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

//! Repair operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const algebraic_mod = @import("algebraic/mod.zig");
const apply_state = @import("derived/apply_state.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const builtin = @import("builtin");
const cleanup_job_owner = @import("cleanup_job_owner.zig");
const derived_types = @import("derived/derived_types.zig");
const docstore_mod = @import("../docstore.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const graph_mod = @import("../../graph/graph.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_generation_manifest = @import("derived/index_generation_manifest.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const index_repair_status = @import("../../common/index_repair_status.zig");
const internal_keys = @import("../internal_keys.zig");
const publication_recovery_owner = @import("publication_recovery_owner.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const shard_mod = @import("../shard.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const types = @import("types.zig");
const Allocator = db_zig.Allocator;
const AsyncContext = db_zig.AsyncContext;
const DerivedCoverageOutcome = db_zig.DerivedCoverageOutcome;
const IndexRepairAdmission = db_zig.IndexRepairAdmission;
const IndexRepairSchedulerDirectory = db_zig.IndexRepairSchedulerDirectory;
const OpenOptions = db_zig.OpenOptions;
const OrderedApplyReceipt = db_zig.OrderedApplyReceipt;
const QueryVisibilityChange = db_zig.QueryVisibilityChange;
const QueryVisibilityHook = db_zig.QueryVisibilityHook;
const appendDerivedBatchRecord = db_zig.appendDerivedBatchRecord;
const artifactRepairIssueKeyForIssueAlloc = db_zig.artifactRepairIssueKeyForIssueAlloc;
const artifactRepairIssueKindKeyForIssueAlloc = db_zig.artifactRepairIssueKindKeyForIssueAlloc;
const bytesToHexAlloc = db_zig.bytesToHexAlloc;
const checkArtifactRepairCancelled = db_zig.checkArtifactRepairCancelled;
const corruptNonEmptyFilesUnderDir = db_zig.corruptNonEmptyFilesUnderDir;
const createUniqueRepairShadowBase = db_zig.createUniqueRepairShadowBase;
const ensureDirPath = db_zig.ensureDirPath;
const expectDenseEmbeddingArtifactValue = db_zig.expectDenseEmbeddingArtifactValue;
const expectedChunkArtifactKeyAlloc = db_zig.expectedChunkArtifactKeyAlloc;
const expectedChunkEmbeddingArtifactKeyAlloc = db_zig.expectedChunkEmbeddingArtifactKeyAlloc;
const expectedDocumentEmbeddingArtifactKeyAlloc = db_zig.expectedDocumentEmbeddingArtifactKeyAlloc;
const graph_repair_rebuild_batch_size = db_zig.graph_repair_rebuild_batch_size;
const graph_split_retire_cursor_key = db_zig.graph_split_retire_cursor_key;
const indexRepairSupportsSnapshotCursor = db_zig.indexRepairSupportsSnapshotCursor;
const lockApply = db_zig.lockApply;
const lockAtomic = db_zig.lockAtomic;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const no_background_test_open = db_zig.no_background_test_open;
const no_replay_no_background_test_open = db_zig.no_replay_no_background_test_open;
const openLegacyDenseTestDB = db_zig.openLegacyDenseTestDB;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const repair_completion_test_options = db_zig.repair_completion_test_options;
const retireSplitEntitySourcedEdgesLocked = db_zig.retireSplitEntitySourcedEdgesLocked;
const retryableIndexRepairTerminalPhase = db_zig.retryableIndexRepairTerminalPhase;
const testManagedGenerationRepairAdmission = db_zig.testManagedGenerationRepairAdmission;
const threadedIo = db_zig.threadedIo;
const ActivatedIndexRepairCertificationFailure = DB.ActivatedIndexRepairCertificationFailure;
const ActivatedIndexRepairReconciliation = DB.ActivatedIndexRepairReconciliation;
const ActivatedIndexRepairRollback = DB.ActivatedIndexRepairRollback;
const IndexRepairAdvanceResult = DB.IndexRepairAdvanceResult;
const IndexRepairInspectionWindow = DB.IndexRepairInspectionWindow;
const IndexRepairIntentUpdate = DB.IndexRepairIntentUpdate;
const IndexRepairLeaseAuthority = DB.IndexRepairLeaseAuthority;
const IndexRepairSchedulerSelection = DB.IndexRepairSchedulerSelection;
const QuarantineIndexPublicationFence = DB.QuarantineIndexPublicationFence;
const RepairWorkingSetPlan = DB.RepairWorkingSetPlan;
const StartupIndexRepairDiscovery = DB.StartupIndexRepairDiscovery;
const StartupIndexRepairResult = DB.StartupIndexRepairResult;
const artifactRepairKindForIndexKind = DB.artifactRepairKindForIndexKind;
const indexRepairIntentBlocksService = DB.indexRepairIntentBlocksService;
const index_repair_inspections_per_execution_slot = DB.index_repair_inspections_per_execution_slot;
const loadDenseArtifactTargetCounter = DB.loadDenseArtifactTargetCounter;
const notifyQueryVisibilityEvent = DB.notifyQueryVisibilityEvent;
const operator_repair_completion_prefix = DB.operator_repair_completion_prefix;
const portableActivationRetryDelayNs = DB.portableActivationRetryDelayNs;
const progressive_index_repair_audit_interval_ms = DB.progressive_index_repair_audit_interval_ms;
const saveIndexStatusSnapshots = DB.saveIndexStatusSnapshots;
const warmNativeCheckpoint = DB.warmNativeCheckpoint;

/// Queue remote acknowledgement without coupling it to DB open or reads.
/// The dedicated owner gives this retry state an independent maintenance
/// probe and makes close a lifetime barrier for every borrowed callback.
pub fn publicationOutboxRecoveryPort(self: *DB) publication_recovery_owner.Owner.Port {
    return .{ .ptr = self, .eligible = outboxRecoveryEligible, .closing = outboxRecoveryClosing, .pending = outboxRecoveryPending, .may_disarm = outboxRecoveryMayDisarm, .now = outboxRecoveryNow, .drain = outboxRecoveryDrain, .retry_delay = outboxRecoveryDelay, .report = outboxRecoveryReport };
}

pub fn scheduleDurableReplicationOutboxRecovery(self: *DB) void {
    self.publication_recovery.schedule(self.publicationOutboxRecoveryPort());
}

pub fn outboxRecoveryEligible(ptr: *anyopaque) bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return self.stable_address and
        !self.async_context.background_closing.load(.acquire) and
        !openModeRequiresReadOnlyBackends(self.open_mode) and !self.backend_runtime.durable_jobs.executesInline();
}

pub fn outboxRecoveryClosing(ptr: *anyopaque) bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return self.async_context.background_closing.load(.acquire);
}

pub fn outboxRecoveryPending(ptr: *anyopaque) bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    if (self.async_context.primary_replication_outbox_pending.load(.acquire)) self.local_execution.durable_replication_outbox_maybe.store(true, .release);
    return self.local_execution.durable_replication_outbox_maybe.load(.acquire);
}

pub fn outboxRecoveryMayDisarm(ptr: *anyopaque) bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return self.local_execution.replication_async_effect_mirror == null;
}

pub fn outboxRecoveryNow(_: *anyopaque) u64 {
    return monotonicTimeNs();
}

pub fn outboxRecoveryDrain(ptr: *anyopaque) !void {
    const self: *DB = @ptrCast(@alignCast(ptr));
    try self.flushDurableReplicationOutboxes();
}

pub fn outboxRecoveryDelay(ptr: *anyopaque, failure_streak: u32) u64 {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return portableActivationRetryDelayNs(self.core.path, @as(u64, @truncate(self.root_incarnation)) ^ self.publication_recovery.owner_id, failure_streak);
}

pub fn outboxRecoveryReport(ptr: *anyopaque, failure: @import("publication_outbox_recovery.zig").Driver.Failure, err: anyerror, failures: u32, delay_ns: u64) void {
    const self: *DB = @ptrCast(@alignCast(ptr));
    std.log.warn("publication recovery {s} failed path={s} err={s} failures={d} next_retry_ms={d}", .{ @tagName(failure), self.core.path, @errorName(err), failures, delay_ns / std.time.ns_per_ms });
}

pub fn requireVectorMigrationRecovery(self: *DB) void {
    self.local_execution.vector_migration_reopen_required.store(true, .release);
    // Transaction-recovery owners share this DocStore but have copied DB
    // wrapper fields. Fence their admission too after an ambiguous commit.
    self.core.store.payload_recovery_required.store(true, .release);
}

/// Rehydrate visibility edges raised before the managed owner attached its
/// hook from durable repair authority. AsyncContext deliberately does not
/// retain borrowed intent strings; reducing those edges to unknown scope,
/// however, turns ordinary initial materialization into a table-wide
/// repair fence. Hook attachment is a control-plane boundary, so paying one
/// bounded checkpoint read here keeps steady-state notification allocation
/// free while preserving exact index/incarnation identity.
pub fn replayPendingIndexRepairVisibility(self: *DB, hook: QueryVisibilityHook) void {
    lockAtomic(&self.async_context.index_repair_control_mutex);
    var state = self.loadIndexRepairState(self.runtime_alloc) catch |err| {
        self.async_context.index_repair_control_mutex.unlock();
        switch (err) {
            // A pending edge may have been cleared before attachment. No
            // durable intent means there is no visibility debt to replay.
            error.FileNotFound => return,
            else => {
                // If durable scope itself cannot be trusted, retain the
                // conservative group-wide invalidation contract.
                std.log.warn("index repair visibility audit failed err={s}", .{@errorName(err)});
                hook.notify(.{ .change = .index_repair_pending });
                return;
            },
        }
    };
    self.async_context.index_repair_control_mutex.unlock();
    defer state.deinit(self.runtime_alloc);

    for (state.entries.items) |entry| {
        const intent = entry.intent;
        if (intent.phase == .terminal or intent.automation != .enabled) continue;
        hook.notify(.{
            .change = .index_repair_pending,
            .repair = .{
                .index_name = intent.index_name,
                .work_class = switch (intent.work_class) {
                    .repair => .repair,
                    .initial_build => .initial_build,
                },
                .repair_id = intent.repair_id,
                .revision = intent.revision,
                .config_hash = intent.config_hash,
                .root_generation = intent.root_generation,
                .admission = indexRepairAdmission(intent),
                .action_required = indexRepairActionRequired(intent),
            },
        });
    }
}

pub fn notifyIndexRepairVisibilityHook(
    ctx: *AsyncContext,
    change: QueryVisibilityChange,
    intent: index_repair_state.IndexRepairIntent,
    persisted_revision: u64,
    previous_admission: IndexRepairAdmission,
    previous_action_required: bool,
    group_repair_debt_remains: bool,
) void {
    std.debug.assert(change == .index_repair_pending or change == .index_repair_cleared);
    const admission: IndexRepairAdmission = if (change == .index_repair_cleared)
        .serviceable
    else
        indexRepairAdmission(intent);
    notifyQueryVisibilityEvent(ctx, .{
        .change = change,
        .repair = .{
            .index_name = intent.index_name,
            .work_class = switch (intent.work_class) {
                .repair => .repair,
                .initial_build => .initial_build,
            },
            .repair_id = intent.repair_id,
            .revision = persisted_revision,
            .config_hash = intent.config_hash,
            .root_generation = intent.root_generation,
            .previous_admission = previous_admission,
            .admission = admission,
            .previous_action_required = previous_action_required,
            .action_required = change != .index_repair_cleared and indexRepairActionRequired(intent),
        },
        .group_repair_debt_remains = group_repair_debt_remains,
    });
}

pub fn recoverOrderedSourceAdmission(self: *DB, request: types.BatchRequest, identity: OrderedApplyReceipt, bypass_write_gate: bool) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, request, identity, bypass_write_gate });
    try self.applyOnlineSourceBatch(request, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = bypass_write_gate,
        .ordered_apply_receipt = identity,
    });
}

/// Only the latest replay receipt may need local source-pin repair.
/// Older retries remain allocation-free and do not decode their payloads.
pub fn recoverReplicatedBatchCut(self: *DB, sequence: u64) !bool {
    if (sequence != try self.replicationAppliedSequence()) return false;
    try self.resumePreparedSourcePin();
    if (try self.restoreStagingStatus(self.alloc)) |stored| {
        var progress = stored;
        defer progress.deinit();
        return progress.value.phase == .validated or progress.value.phase == .published;
    }
    return false;
}

pub fn scheduleInactiveRepairShadowCleanup(self: *DB) void {
    scheduleInactiveRepairShadowCleanupContext(
        self.core.index_manager,
        self.backend_runtime.durable_jobs,
        self.repair_cleanup_owner_id,
    );
}

pub fn scheduleInactiveRepairShadowCleanupContext(
    manager: *index_manager_mod.IndexManager,
    lane: background_runtime_mod.DurableJobLane,
    owner_id: u64,
) void {
    cleanup_job_owner.schedule(.{
        .ptr = manager,
        .state = &manager.repair_cleanup_state,
        .lane = lane,
        .owner_id = owner_id,
        .io = manager.checkpointIo(),
        .advance = advanceRepairShadowCleanup,
        .name = "repair-generation",
    });
}

pub fn advanceRepairShadowCleanup(ptr: *anyopaque) !cleanup_job_owner.Advance {
    const manager: *index_manager_mod.IndexManager = @ptrCast(@alignCast(ptr));
    return if (try manager.cleanupInactiveRepairShadowRootsPage()) .progressed else .idle;
}

pub fn beginIndexRepairLease(self: *DB, index_name: []const u8) !bool {
    lockAtomic(&self.index_repair_mutex);
    defer self.index_repair_mutex.unlock();
    if (self.active_index_repairs.contains(index_name)) return false;
    const owned = try self.alloc.dupe(u8, index_name);
    errdefer self.alloc.free(owned);
    try self.active_index_repairs.put(self.alloc, owned, false);
    return true;
}

pub fn activeIndexRepairCancellationRequested(self: *DB, index_name: []const u8) bool {
    lockAtomic(&self.index_repair_mutex);
    defer self.index_repair_mutex.unlock();
    return self.active_index_repairs.get(index_name) orelse false;
}

pub fn requestActiveIndexRepairCancellation(self: *DB, index_name: []const u8) bool {
    lockAtomic(&self.index_repair_mutex);
    defer self.index_repair_mutex.unlock();
    const requested = self.active_index_repairs.getPtr(index_name) orelse return false;
    requested.* = true;
    return true;
}

pub fn endIndexRepairLease(self: *DB, index_name: []const u8) void {
    lockAtomic(&self.index_repair_mutex);
    defer self.index_repair_mutex.unlock();
    if (self.active_index_repairs.fetchRemove(index_name)) |removed| {
        self.alloc.free(@constCast(removed.key));
    }
}

pub fn repairIssueKeyForIssueAlloc(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
    _ = self;
    return try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
}

pub fn repairIssueKindKeyForIssueAlloc(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
    _ = self;
    return try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
}

pub fn repairCoverageMarkerComplete(self: *DB, alloc: Allocator, marker_key: []const u8) !bool {
    const raw_outcome = self.core.store.get(alloc, marker_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(raw_outcome);
    const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, raw_outcome) orelse
        return error.InvalidDerivedCoverageOutcome;
    return outcome == .produced or outcome == .skipped;
}

pub fn listIndexRepairIssuesPage(
    self: *DB,
    alloc: Allocator,
    req: types.ArtifactRepairListRequest,
) !types.ArtifactRepairListResult {
    const configs = try self.core.listIndexes(alloc);
    defer types.freeIndexConfigs(alloc, configs);
    std.mem.sort(types.IndexConfig, configs, {}, struct {
        fn lessThan(_: void, lhs: types.IndexConfig, rhs: types.IndexConfig) bool {
            return std.mem.order(u8, lhs.name, rhs.name) == .lt;
        }
    }.lessThan);

    var issues = std.ArrayListUnmanaged(types.ArtifactRepairIssue).empty;
    errdefer {
        for (issues.items) |*issue| issue.deinit(alloc);
        issues.deinit(alloc);
    }

    const cursor = req.cursor orelse "";
    var last_returned: ?[]const u8 = null;
    var has_more = false;
    var scanned: u64 = 0;
    for (configs) |cfg| {
        if (cursor.len != 0 and std.mem.order(u8, cfg.name, cursor) != .gt) continue;
        if (req.index_name) |requested| {
            if (!std.mem.eql(u8, requested, cfg.name)) continue;
        }
        const artifact_kind = artifactRepairKindForIndexKind(cfg.kind) orelse continue;
        if (req.artifact_kind) |requested_kind| {
            if (requested_kind != artifact_kind) continue;
        }
        if (!(try self.indexGenerationRepairRequired(alloc, cfg.name))) continue;
        scanned += 1;
        if (req.limit != 0 and issues.items.len >= req.limit) {
            has_more = true;
            break;
        }

        const load_error = self.core.index_manager.loadFailure(cfg.name);
        try issues.append(alloc, .{
            .artifact_kind = artifact_kind,
            .index_name = try alloc.dupe(u8, cfg.name),
            .artifact_name = try alloc.dupe(u8, cfg.name),
            .repairable = true,
            .unsupported_reason = "",
            .reason = if (load_error != null) .unreadable_artifact else .missing_artifact,
            .last_error = if (load_error) |err_name| try alloc.dupe(u8, err_name) else try alloc.dupe(u8, "index_repair_required"),
        });
        last_returned = cfg.name;
    }

    return .{
        .issues = try issues.toOwnedSlice(alloc),
        .limit = req.limit,
        .scanned = scanned,
        .next_cursor = if (has_more and last_returned != null) try alloc.dupe(u8, last_returned.?) else null,
        .has_more = has_more,
    };
}

pub fn indexRepairStateLocation(self: *const DB) !index_repair_state.Location {
    return try indexRepairStateLocationContext(self.async_context);
}

pub fn indexRepairStateLocationContext(ctx: *const AsyncContext) !index_repair_state.Location {
    return ctx.index_repair_checkpoint orelse error.DurableIndexRepairStateUnavailable;
}

pub fn localRepairGroupId(self: *const DB) u64 {
    const namespace = self.core.identity_namespace;
    if (namespace.range_id != 0) return namespace.range_id;
    return namespace.shard_id;
}

/// Requires index_repair_scheduler_mutex.
pub fn publishIndexRepairProgressWaitHint(ctx: *AsyncContext) void {
    ctx.index_repair_progress_wait_pending.store(
        ctx.index_repair_scheduler.initialized and
            ctx.index_repair_scheduler.progress_waiters != 0,
        .release,
    );
}

pub fn invalidateIndexRepairSchedulerDirectory(ctx: *AsyncContext) void {
    _ = ctx.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
    lockAtomic(&ctx.index_repair_scheduler_mutex);
    defer ctx.index_repair_scheduler_mutex.unlock();
    ctx.index_repair_scheduler.deinit(ctx.alloc);
    publishIndexRepairProgressWaitHint(ctx);
}

pub fn noteIndexRepairSchedulerUpsert(
    ctx: *AsyncContext,
    intent: index_repair_state.IndexRepairIntent,
    persisted_revision: u64,
    control_revision: u64,
) void {
    _ = ctx.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
    lockAtomic(&ctx.index_repair_scheduler_mutex);
    defer ctx.index_repair_scheduler_mutex.unlock();
    if (!ctx.index_repair_scheduler.initialized) {
        publishIndexRepairProgressWaitHint(ctx);
        return;
    }
    switch (ctx.index_repair_scheduler.classifyControlMutation(intent.identity(), control_revision)) {
        .stale => return,
        .gap => {
            ctx.index_repair_scheduler.deinit(ctx.alloc);
            publishIndexRepairProgressWaitHint(ctx);
            return;
        },
        .next => {},
    }
    ctx.index_repair_scheduler.upsert(ctx.alloc, intent, persisted_revision) catch {
        // The durable checkpoint remains authoritative. Allocation or a
        // consistency mismatch invalidates only the acceleration layer;
        // the next owner rebuilds it from the committed checkpoint.
        ctx.index_repair_scheduler.deinit(ctx.alloc);
        publishIndexRepairProgressWaitHint(ctx);
        return;
    };
    ctx.index_repair_scheduler.control_revision = control_revision;
    publishIndexRepairProgressWaitHint(ctx);
}

pub fn noteIndexRepairSchedulerRemove(
    ctx: *AsyncContext,
    identity: index_repair_state.ReplicaIdentity,
    repair_id: u128,
    control_revision: u64,
) void {
    _ = ctx.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
    lockAtomic(&ctx.index_repair_scheduler_mutex);
    defer ctx.index_repair_scheduler_mutex.unlock();
    if (!ctx.index_repair_scheduler.initialized) {
        publishIndexRepairProgressWaitHint(ctx);
        return;
    }
    switch (ctx.index_repair_scheduler.classifyControlMutation(identity, control_revision)) {
        .stale => return,
        .gap => {
            ctx.index_repair_scheduler.deinit(ctx.alloc);
            publishIndexRepairProgressWaitHint(ctx);
            return;
        },
        .next => {},
    }
    ctx.index_repair_scheduler.remove(ctx.alloc, repair_id);
    ctx.index_repair_scheduler.control_revision = control_revision;
    publishIndexRepairProgressWaitHint(ctx);
}

pub fn indexRepairNowMs(self: *const DB) u64 {
    return (self.index_repair_clock orelse self.backend_runtime.clock()).nowRealtimeMs();
}

pub fn deferIndexRepairForProgress(
    self: *DB,
    repair_id: u128,
    expected_revision: u64,
    wake_at_sequence: u64,
) bool {
    const fallback_at_ms = self.indexRepairNowMs() +|
        progressive_index_repair_audit_interval_ms;
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    defer self.async_context.index_repair_scheduler_mutex.unlock();
    if (!self.async_context.index_repair_scheduler.initialized) return false;
    const armed = self.async_context.index_repair_scheduler.deferForProgress(
        repair_id,
        expected_revision,
        wake_at_sequence,
        fallback_at_ms,
    );
    if (armed) self.async_context.index_repair_progress_wait_pending.store(true, .release);
    return armed;
}

pub fn deferIndexRepairForAudit(
    self: *DB,
    repair_id: u128,
    expected_revision: u64,
) bool {
    const fallback_at_ms = self.indexRepairNowMs() +|
        progressive_index_repair_audit_interval_ms;
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    defer self.async_context.index_repair_scheduler_mutex.unlock();
    if (!self.async_context.index_repair_scheduler.initialized) return false;
    const deferred = self.async_context.index_repair_scheduler.deferForAudit(
        repair_id,
        expected_revision,
        fallback_at_ms,
    );
    if (deferred) publishIndexRepairProgressWaitHint(self.async_context);
    return deferred;
}

/// Park an initial-build intent on the one watermark that can change the
/// current replay proof. Once that target is already visible, later index
/// publications cannot by themselves prove source-coverage completion;
/// use the bounded audit deadline instead of waking repair for every newly
/// generated artifact.
pub fn deferIndexRepairUntilTargetOrAudit(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    expected_revision: u64,
    index_name: []const u8,
    observed_sequence: u64,
    target_sequence: u64,
) !bool {
    if (observed_sequence >= target_sequence) {
        return self.deferIndexRepairForAudit(repair_id, expected_revision);
    }
    if (!self.deferIndexRepairForProgress(
        repair_id,
        expected_revision,
        target_sequence,
    )) return false;

    // Arm-then-recheck closes the lost-wakeup window between observing the
    // watermark and installing the revision-scoped resident wait.
    const current_sequence = try self.managedIndexAppliedSequence(alloc, index_name);
    if (current_sequence >= target_sequence) {
        _ = wakeIndexRepairForProgressContext(
            self.async_context,
            index_name,
            current_sequence,
        );
    }
    return true;
}

pub fn wakeIndexRepairForProgressContext(
    ctx: *AsyncContext,
    index_name: []const u8,
    applied_sequence: u64,
) ?IndexRepairSchedulerDirectory.ProgressWake {
    if (!ctx.index_repair_progress_wait_pending.load(.acquire)) return null;
    lockAtomic(&ctx.index_repair_scheduler_mutex);
    defer ctx.index_repair_scheduler_mutex.unlock();
    if (!ctx.index_repair_scheduler.initialized) {
        ctx.index_repair_progress_wait_pending.store(false, .release);
        return null;
    }
    const woke = ctx.index_repair_scheduler.wakeForIndexProgress(index_name, applied_sequence);
    if (ctx.index_repair_scheduler.progress_waiters == 0) {
        ctx.index_repair_progress_wait_pending.store(false, .release);
    }
    return woke;
}

pub fn ensureIndexRepairSchedulerDirectory(self: *DB, alloc: Allocator) !void {
    while (true) {
        lockAtomic(&self.async_context.index_repair_scheduler_mutex);
        const initialized = self.async_context.index_repair_scheduler.initialized;
        const current_root = if (self.async_context.index_repair_scheduler.identity) |identity|
            identity.root_generation == self.core.root_generation
        else
            false;
        if (initialized and !current_root) {
            self.async_context.index_repair_scheduler.deinit(self.async_context.alloc);
            publishIndexRepairProgressWaitHint(self.async_context);
            _ = self.async_context.index_repair_scheduler_revision.fetchAdd(1, .acq_rel);
        }
        self.async_context.index_repair_scheduler_mutex.unlock();
        if (initialized and current_root) return;
        if (initialized) continue;

        const observed_revision = self.async_context.index_repair_scheduler_revision.load(.acquire);
        var state = try self.loadOrCreateCurrentIndexRepairState(alloc);
        defer state.deinit(alloc);
        var replacement = try IndexRepairSchedulerDirectory.buildFromState(self.async_context.alloc, state);
        var replacement_owned = true;
        defer if (replacement_owned) replacement.deinit(self.async_context.alloc);

        lockAtomic(&self.async_context.index_repair_scheduler_mutex);
        defer self.async_context.index_repair_scheduler_mutex.unlock();
        if (self.async_context.index_repair_scheduler.initialized) return;
        if (self.async_context.index_repair_scheduler_revision.load(.acquire) != observed_revision) continue;
        self.async_context.index_repair_scheduler = replacement;
        replacement_owned = false;
        publishIndexRepairProgressWaitHint(self.async_context);
        return;
    }
}

pub fn loadIndexRepairState(self: *const DB, alloc: Allocator) !index_repair_state.State {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    return try index_repair_state.loadAt(alloc, try self.indexRepairStateLocation());
}

pub fn refreshIndexRepairAvailabilityGate(self: *DB, alloc: Allocator) !void {
    var state = self.loadIndexRepairState(alloc) catch |err| switch (err) {
        error.FileNotFound, error.DurableIndexRepairStateUnavailable => return,
        error.InvalidIndexRepairState => {
            self.async_context.index_repair_state_corrupt.store(true, .release);
            self.async_context.index_repair_replay_pinned.store(true, .release);
            const configs = try self.core.index_manager.listIndexesPublic(alloc);
            defer {
                for (configs) |*cfg| cfg.deinit(alloc);
                alloc.free(configs);
            }
            for (configs) |cfg| try self.core.index_manager.markUnscopedRepairUnavailable(cfg.name);
            return;
        },
        else => return err,
    };
    defer state.deinit(alloc);
    // Opening already has to inspect every durable intent to install the
    // fail-closed serving gates. Build the exact scheduler projection from
    // that same snapshot so the first maintenance quantum never pays a
    // second O(N) cold-directory reconstruction.
    var scheduler = try IndexRepairSchedulerDirectory.buildFromState(self.async_context.alloc, state);
    var scheduler_owned = true;
    defer if (scheduler_owned) scheduler.deinit(self.async_context.alloc);
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    if (!self.async_context.index_repair_scheduler.initialized) {
        self.async_context.index_repair_scheduler = scheduler;
        scheduler_owned = false;
    }
    self.async_context.index_repair_scheduler_mutex.unlock();
    for (state.entries.items) |entry| {
        var unavailable = indexRepairIntentBlocksService(entry.intent);
        if (!unavailable) {
            if (entry.intent.trigger == .incomplete_bulk_publish) {
                const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.intent.index_name);
                unavailable = checkpoint.status != .clean or checkpoint.config_hash != entry.intent.config_hash;
            }
        }
        if (unavailable) {
            try self.core.index_manager.markUnscopedRepairUnavailable(entry.intent.index_name);
            self.core.index_manager.publishRepairAdmission(
                entry.intent.index_name,
                repairAdmissionVersion(entry.intent, entry.intent.revision, entry.intent.control_revision),
                true,
                false,
            );
        } else self.core.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            repairAdmissionVersion(entry.intent, entry.intent.revision, entry.intent.control_revision),
            false,
            false,
        );
    }
}

/// A writable generation may clear its gate after activating the repaired
/// root. A query-only DB is pinned to the root it opened, so it must retain
/// every startup gate until retirement; durable checkpoint completion can
/// describe a newer root and is not authority to expose the pinned one.
/// Writable DBs recheck only rejected indexes, keeping healthy queries free
/// of checkpoint I/O.
pub fn refreshIndexRepairAvailabilityForIndex(self: *DB, alloc: Allocator, index_name: []const u8) !void {
    const pinned_read_generation = openModeRequiresReadOnlyBackends(self.open_mode);
    var state = self.loadIndexRepairState(alloc) catch |err| switch (err) {
        error.FileNotFound => {
            if (pinned_read_generation) return;
            try self.clearRepairGateIfAdmissionCompleted(alloc, index_name);
            return;
        },
        error.DurableIndexRepairStateUnavailable => return,
        error.InvalidIndexRepairState => {
            self.async_context.index_repair_state_corrupt.store(true, .release);
            self.async_context.index_repair_replay_pinned.store(true, .release);
            return;
        },
        else => return err,
    };
    defer state.deinit(alloc);

    try self.refreshIndexRepairAvailabilityForIndexFromState(alloc, index_name, &state, pinned_read_generation);
}

pub fn refreshIndexRepairAvailabilityForIndexFromState(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    state: *const index_repair_state.State,
    pinned_read_generation: bool,
) !void {
    // A checkpoint from another root generation is not authority to clear
    // this generation's gate. Startup/root-transition repair owns it.
    if (state.identity.root_generation != self.core.root_generation) return;

    const entry_index = state.findIndex(index_name) orelse {
        if (pinned_read_generation) return;
        try self.clearRepairGateIfAdmissionCompleted(alloc, index_name);
        return;
    };
    const intent = state.entries.items[entry_index].intent;
    var unavailable = indexRepairIntentBlocksService(intent);
    const pinned_generation_proved_serviceable = unavailable and
        try self.managedAdmissionGenerationIsQueryable(alloc, intent);
    if (pinned_generation_proved_serviceable) {
        unavailable = false;
    }
    // Durable phase/checkpoint transitions can describe a newer root than
    // the query-only DB has open. Only the proof above inspected this
    // pinned in-memory generation, so it is the sole read-only exception.
    if (pinned_read_generation) {
        if (pinned_generation_proved_serviceable) {
            self.core.index_manager.publishRepairServiceabilityProof(
                index_name,
                repairAdmissionVersion(intent, intent.revision, intent.control_revision),
            );
        }
        return;
    }
    if (!unavailable and intent.trigger == .incomplete_bulk_publish) {
        const checkpoint = try self.core.loadProjectionCheckpoint(alloc, index_name);
        unavailable = checkpoint.status != .clean or checkpoint.config_hash != intent.config_hash;
    }
    if (!unavailable) self.core.index_manager.publishRepairServiceabilityProof(
        index_name,
        repairAdmissionVersion(intent, intent.revision, intent.control_revision),
    );
}

pub fn clearRepairGateIfAdmissionCompleted(self: *DB, alloc: Allocator, index_name: []const u8) !void {
    const key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(key);
    const marker = self.core.store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => {
            self.core.index_manager.clearManagedAdmissionSnapshotForIndex(index_name);
            self.core.index_manager.clearUnscopedRepairUnavailable(index_name);
            return;
        },
        else => return err,
    };
    alloc.free(marker);
}

pub fn indexRepairAdmission(intent: index_repair_state.IndexRepairIntent) IndexRepairAdmission {
    return if (indexRepairIntentBlocksService(intent)) .blocked else .serviceable;
}

pub fn repairAdmissionVersion(
    intent: index_repair_state.IndexRepairIntent,
    persisted_revision: u64,
    control_revision: u64,
) index_manager_mod.IndexManager.RepairAdmissionVersion {
    return .{
        .repair_id = intent.repair_id,
        .control_revision = control_revision,
        .revision = persisted_revision,
        .config_hash = intent.config_hash,
        .root_generation = intent.root_generation,
    };
}

pub fn indexRepairActionRequired(intent: index_repair_state.IndexRepairIntent) bool {
    if (intent.automation == .paused) return true;
    return intent.phase == .terminal and
        retryableIndexRepairTerminalPhase(intent.last_error, intent.trigger) == null;
}

pub fn loadOrCreateCurrentIndexRepairState(self: *DB, alloc: Allocator) !index_repair_state.State {
    const location = try self.indexRepairStateLocation();
    var state = try index_repair_state.loadOrCreateAt(alloc, location, self.core.root_generation);
    var state_owned = true;
    defer if (state_owned) state.deinit(alloc);
    if (state.identity.root_generation == self.core.root_generation) {
        state_owned = false;
        return state;
    }

    // A root-generation transition fences every old repair identity. An
    // inactive candidate can be deleted. A pointer-selected candidate must
    // never become silently serviceable merely because its old intent is
    // discarded: persist fresh, new-root reconstruction debt first and
    // keep that index fail-closed until a new generation is validated.
    var replacement_intents = std.ArrayListUnmanaged(index_repair_state.IndexRepairIntent).empty;
    defer {
        for (replacement_intents.items) |*intent| intent.deinit(alloc);
        replacement_intents.deinit(alloc);
    }
    for (state.entries.items) |entry| {
        const candidate = entry.intent.candidate_relative_path orelse continue;
        try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
        if (try self.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) {
            const cfg = self.core.index_manager.get(entry.intent.index_name) orelse
                return error.InvalidIndexRepairState;
            const now_ms = self.indexRepairNowMs();
            const target_sequence = self.core.nextDerivedSequence();
            var replacement_intent = index_repair_state.IndexRepairIntent{
                .repair_id = try index_repair_state.newRepairId(alloc),
                .db_identity = state.identity.db_identity,
                .group_id = self.localRepairGroupId(),
                .replica_id = state.identity.replica_id,
                .root_generation = self.core.root_generation,
                .index_name = try alloc.dupe(u8, entry.intent.index_name),
                .kind = cfg.kind,
                .config_hash = types.indexConfigHash(cfg.*),
                .trigger = .root_generation_rebuild,
                .detected_sequence = target_sequence,
                .target_sequence = target_sequence,
                .started_at_ms = now_ms,
                .updated_at_ms = now_ms,
                .owner_epoch = 0,
                .last_error = try alloc.dupe(u8, "root_generation_changed_during_activation"),
            };
            var replacement_intent_owned = true;
            errdefer if (replacement_intent_owned) replacement_intent.deinit(alloc);
            try replacement_intents.append(alloc, replacement_intent);
            replacement_intent_owned = false;
            try self.core.index_manager.prepareRepairAdmissionRebind(
                entry.intent.index_name,
                repairAdmissionVersion(replacement_intent, 1, 1),
            );
            continue;
        }
        const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
        const shadow_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.path, candidate[0..separator] });
        defer alloc.free(shadow_root);
        if (self.primary_lsm_storage) |storage| {
            try storage.deleteTree(shadow_root);
        } else if (comptime builtin.os.tag == .freestanding) {
            return error.UnsupportedPlatform;
        } else {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            try std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_root);
        }
    }
    const old_identity = state.identity;
    const replacement = try index_repair_state.resetForRootGenerationWithIntentsAt(
        alloc,
        location,
        old_identity,
        self.core.root_generation,
        replacement_intents.items,
    );
    for (replacement.entries.items) |entry| {
        self.core.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            repairAdmissionVersion(entry.intent, entry.intent.revision, entry.intent.control_revision),
            indexRepairIntentBlocksService(entry.intent),
            false,
        );
    }
    // Only the checkpoint replacement authorizes retirement of old-root
    // admissions. Active candidates were pre-bound to their new identity,
    // so the exact old publication is ignored for them; inactive entries
    // are released without allowing a same-name incarnation to be opened.
    for (state.entries.items) |entry| {
        self.core.index_manager.publishRepairAdmission(
            entry.intent.index_name,
            repairAdmissionVersion(entry.intent, entry.intent.revision +| 1, state.control_revision +| 1),
            false,
            false,
        );
        self.core.index_manager.clearUnscopedRepairUnavailable(entry.intent.index_name);
    }
    state.deinit(alloc);
    state_owned = false;
    invalidateIndexRepairSchedulerDirectory(self.async_context);
    const observed_revision = self.async_context.index_repair_scheduler_revision.load(.acquire);
    var scheduler = try IndexRepairSchedulerDirectory.buildFromState(self.async_context.alloc, replacement);
    var scheduler_owned = true;
    defer if (scheduler_owned) scheduler.deinit(self.async_context.alloc);
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    if (!self.async_context.index_repair_scheduler.initialized and
        self.async_context.index_repair_scheduler_revision.load(.acquire) == observed_revision)
    {
        self.async_context.index_repair_scheduler = scheduler;
        scheduler_owned = false;
    }
    publishIndexRepairProgressWaitHint(self.async_context);
    self.async_context.index_repair_scheduler_mutex.unlock();
    return replacement;
}

pub fn loadIndexRepairEntryById(
    self: *const DB,
    alloc: Allocator,
    repair_id: u128,
) !index_repair_state.Entry {
    var state = try self.loadIndexRepairState(alloc);
    defer state.deinit(alloc);
    for (state.entries.items) |entry| {
        if (entry.intent.repair_id == repair_id) return try entry.clone(alloc);
    }
    return error.IndexRepairIntentNotFound;
}

pub fn indexRepairPhaseHasActivationCertification(phase: index_repair_state.Phase) bool {
    return switch (phase) {
        .activating, .validating, .cleanup, .rolling_back => true,
        else => false,
    };
}

pub fn indexRepairRetryDelayMs(repair_id: u128, attempt_count: u32) u64 {
    const exponent: u6 = @intCast(@min(attempt_count -| 1, 5));
    const base_ms: u64 = 30 * std.time.ms_per_s;
    const maximum_ms: u64 = 10 * std.time.ms_per_min;
    const nominal = @min(base_ms << exponent, maximum_ms);
    // Stable per-attempt +/-10% jitter prevents a node restart from
    // synchronizing every pending repair while keeping the retry time
    // reconstructible and bounded.
    var seed: [20]u8 = undefined;
    std.mem.writeInt(u128, seed[0..16], repair_id, .little);
    std.mem.writeInt(u32, seed[16..20], attempt_count, .little);
    const spread = @max(@as(u64, 1), nominal / 5);
    const jitter = std.hash.Wyhash.hash(0x4944585250525954, &seed) % spread;
    return nominal - nominal / 10 + jitter;
}

pub fn indexRepairIdForIndex(self: *const DB, alloc: Allocator, index_name: []const u8) !?u128 {
    return try indexRepairIdForIndexContext(self.async_context, alloc, index_name);
}

pub fn indexRepairIdForIndexContext(ctx: *const AsyncContext, alloc: Allocator, index_name: []const u8) !?u128 {
    const location = indexRepairStateLocationContext(ctx) catch {
        // Backends without a replica-local durable checkpoint cannot own
        // repair intents, so read-side discovery is equivalent to empty.
        // Intent creation remains fail-closed through the same capability.
        return null;
    };
    var state = index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer state.deinit(alloc);
    const i = state.findIndex(index_name) orelse return null;
    return state.entries.items[i].intent.repair_id;
}

pub fn validateIndexRepairActivationState(self: *DB, alloc: Allocator, repair_id: u128, expected_owner_epoch: u64) !void {
    var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    if (entry.intent.root_generation != self.core.root_generation or
        entry.intent.group_id != self.localRepairGroupId())
    {
        return error.RepairOwnershipLost;
    }
    if (entry.intent.phase == .terminal or entry.intent.automation == .paused) return error.IndexRepairNotRunnable;
    if (expected_owner_epoch != 0 and entry.intent.owner_epoch != expected_owner_epoch) {
        return error.RepairOwnershipLost;
    }
    const cfg = self.core.index_manager.get(entry.intent.index_name) orelse return error.IndexRepairConfigurationChanged;
    if (cfg.kind != entry.intent.kind or types.indexConfigHash(cfg.*) != entry.intent.config_hash) {
        return error.IndexRepairConfigurationChanged;
    }
}

/// Consume a scheduler audit grant by advancing exactly the durable
/// revision selected by that scheduler turn. This is a real control
/// transition, not a read-then-act check: the control mutex plus the
/// checkpoint CAS linearize the grant against pause/resume, owner handoff,
/// replay, and retry mutations. The returned revision is the owned turn's
/// new authority for any wait it publishes after the audit.
pub fn consumeIndexRepairAuditGrant(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    expected_revision: u64,
) !?u64 {
    self.updateIndexRepairIntentWithBindingEffect(
        alloc,
        repair_id,
        .{},
        .retain,
        expected_revision,
    ) catch |err| switch (err) {
        error.RepairTransitionConflict => return null,
        else => return err,
    };
    return expected_revision +| 1;
}

pub fn operatorRepairCompletionKeyAlloc(
    alloc: Allocator,
    index_name: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ operator_repair_completion_prefix, index_name });
}

pub fn operatorRepairAlreadyCompleted(
    self: *DB,
    alloc: Allocator,
    cfg: types.IndexConfig,
    job_id: u64,
    job_created_at_ms: u64,
) !bool {
    if (job_id == 0 or job_created_at_ms == 0) return false;
    const key = try operatorRepairCompletionKeyAlloc(alloc, cfg.name);
    defer alloc.free(key);
    const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != 48) return error.InvalidIndexRepairState;
    var state = try self.loadOrCreateCurrentIndexRepairState(alloc);
    defer state.deinit(alloc);
    const completed_job_id = std.mem.readInt(u64, raw[32..40], .little);
    const completed_created_at_ms = std.mem.readInt(u64, raw[40..48], .little);
    const completion_covers_request = completed_created_at_ms > job_created_at_ms or
        (completed_created_at_ms == job_created_at_ms and completed_job_id >= job_id);
    return completion_covers_request and
        std.mem.readInt(u64, raw[0..8], .little) == types.indexConfigHash(cfg) and
        std.mem.readInt(u128, raw[8..24], .little) == state.identity.db_identity and
        std.mem.readInt(u64, raw[24..32], .little) == state.identity.root_generation;
}

pub fn persistOperatorRepairCompletion(self: *DB, alloc: Allocator, intent: index_repair_state.IndexRepairIntent) !void {
    return try persistOperatorRepairCompletionContext(self.async_context, alloc, intent);
}

pub fn persistOperatorRepairCompletionContext(ctx: *AsyncContext, alloc: Allocator, intent: index_repair_state.IndexRepairIntent) !void {
    if (intent.operator_job_id == 0 or intent.operator_job_created_at_ms == 0) return;
    const key = try operatorRepairCompletionKeyAlloc(alloc, intent.index_name);
    defer alloc.free(key);
    var raw: [48]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], intent.config_hash, .little);
    std.mem.writeInt(u128, raw[8..24], intent.db_identity, .little);
    std.mem.writeInt(u64, raw[24..32], intent.root_generation, .little);
    std.mem.writeInt(u64, raw[32..40], intent.operator_job_id, .little);
    std.mem.writeInt(u64, raw[40..48], intent.operator_job_created_at_ms, .little);
    try ctx.store.put(key, &raw);
}

pub fn attachOperatorRepairRequest(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    operator_job_id: u64,
    operator_job_created_at_ms: u64,
) !void {
    if (operator_job_id == 0 or operator_job_created_at_ms == 0) return;
    var attempts: usize = 0;
    while (attempts < 4) : (attempts += 1) {
        var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        const existing_is_newer = entry.intent.operator_job_created_at_ms > operator_job_created_at_ms or
            (entry.intent.operator_job_created_at_ms == operator_job_created_at_ms and
                entry.intent.operator_job_id >= operator_job_id);
        if (existing_is_newer) return;
        self.updateIndexRepairIntent(alloc, repair_id, .{
            .operator_job_id = operator_job_id,
            .operator_job_created_at_ms = operator_job_created_at_ms,
        }) catch |err| switch (err) {
            error.RepairTransitionConflict => continue,
            else => return err,
        };
        return;
    }
    return error.RepairTransitionConflict;
}

pub fn validateIndexRepairSourcePreflight(self: *DB, alloc: Allocator, cfg: types.IndexConfig) !void {
    if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return;
    const target_count = try loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name);
    if (target_count == null) return error.RepairSourceCoverageIncomplete;
}

pub fn currentIndexRepairIdMatches(self: *const DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !?u128 {
    const repair_id = (try self.indexRepairIdForIndex(alloc, index_name)) orelse {
        if (expected_repair_id != null) return error.StaleIndexRepairControl;
        return null;
    };
    if (expected_repair_id) |expected| {
        if (repair_id != expected) return error.StaleIndexRepairControl;
    }
    return repair_id;
}

pub fn pauseAutomaticIndexRepair(self: *DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
    const repair_id = (try self.currentIndexRepairIdMatches(alloc, index_name, expected_repair_id)) orelse return false;
    try self.updateIndexRepairIntent(alloc, repair_id, .{ .automation = .paused });
    _ = self.requestActiveIndexRepairCancellation(index_name);
    return true;
}

pub fn cancelCurrentIndexRepairAttempt(self: *DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
    _ = (try self.currentIndexRepairIdMatches(alloc, index_name, expected_repair_id)) orelse return false;
    return self.requestActiveIndexRepairCancellation(index_name);
}

pub fn resumeAutomaticIndexRepair(self: *DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
    const repair_id = (try self.currentIndexRepairIdMatches(alloc, index_name, expected_repair_id)) orelse return false;
    try self.updateIndexRepairIntent(alloc, repair_id, .{
        .automation = .enabled,
        .next_retry_at_ms = 0,
        .replace_last_error = true,
    });
    return true;
}

/// Quiesce repair for deletion while retaining its durable gate. The caller
/// owns the returned lease and removes the checkpoint only after catalog
/// absence and marker deletion commit atomically.
pub fn prepareIndexRepairForDeletion(self: *DB, alloc: Allocator, index_name: []const u8) !?u128 {
    const repair_id = (try self.indexRepairIdForIndex(alloc, index_name)) orelse return null;
    if (!(try self.beginIndexRepairLease(index_name))) return error.IndexRepairInProgress;
    errdefer self.endIndexRepairLease(index_name);
    var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    if (entry.intent.candidate_relative_path) |candidate| {
        if (!try self.core.index_manager.isRepairCandidateActive(index_name, candidate)) {
            try self.discardInactiveIndexRepairCandidate(alloc, repair_id);
        }
    }
    return repair_id;
}

pub fn recordIndexRepairAttemptFailure(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    err_name: []const u8,
    terminal: bool,
) !void {
    if (terminal) std.log.warn("index repair failed terminally repair_id={x} err={s}", .{ repair_id, err_name });
    try self.updateIndexRepairIntentWithBindingEffect(
        alloc,
        repair_id,
        .{
            .attempt_failure = .{
                .err_name = err_name,
                .terminal = terminal,
            },
        },
        if (terminal) .release else .retain,
        null,
    );
}

pub fn indexRepairFailureIsTerminal(err: anyerror) bool {
    return switch (err) {
        error.InvalidArgument,
        error.NotFound,
        error.UnsupportedOperation,
        error.InvalidIndexRepairState,
        error.InvalidRepairCandidatePath,
        error.ReplicaIdentityMismatch,
        error.DurableIndexRepairStateUnavailable,
        error.RepairSourceCoverageIncomplete,
        error.IndexRepairConfigurationChanged,
        => true,
        else => false,
    };
}

pub fn discardInactiveIndexRepairCandidate(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
) !void {
    return try self.discardInactiveIndexRepairCandidateWithTerminal(
        alloc,
        repair_id,
        null,
    );
}

/// Deletes only a proven-inactive candidate, then publishes its durable
/// disposition in one repair-control checkpoint. A manual predecessor
/// failure must not expose an intermediate runnable preflight intent if the
/// process stops after filesystem cleanup.
pub fn discardInactiveIndexRepairCandidateWithTerminal(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    terminal_reason: ?[]const u8,
) !void {
    var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    const candidate = entry.intent.candidate_relative_path orelse return;
    try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
    if (try self.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) {
        return error.RepairCandidateAlreadyActive;
    }
    const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
    const shadow_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.path, candidate[0..separator] });
    defer alloc.free(shadow_root);
    if (entry.intent.kind == .algebraic) {
        // Algebraic generations live in the primary store and may be much
        // larger than a scheduler turn. The directory is their durable
        // namespace marker: clear only the writer marker here, detach the
        // intent below, then let paged orphan collection drain the keys.
        try index_manager_mod.IndexManager.clearRepairShadowInProgressMarker(alloc, shadow_root);
    } else if (self.primary_lsm_storage) |storage| {
        try storage.deleteTree(shadow_root);
    } else if (comptime builtin.os.tag == .freestanding) {
        return error.UnsupportedPlatform;
    } else {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        // `deleteTree` is idempotent for an absent root. A cancelled
        // pre-checkpoint build may already have removed its non-reopenable
        // shadow in the unwind path.
        try std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_root);
    }
    const reset = IndexRepairIntentUpdate{
        .phase = .preflight,
        .replace_candidate_path = true,
        .replace_build_resume_key = true,
        .build_reprocessed = 0,
        .candidate_applied_sequence = 0,
        .target_sequence = self.core.nextDerivedSequence(),
        .failure_streak = 0,
        .next_retry_at_ms = 0,
        .replace_last_error = true,
    };
    if (terminal_reason) |reason| {
        var terminal = reset;
        terminal.phase = null;
        terminal.failure_streak = null;
        terminal.next_retry_at_ms = null;
        terminal.replace_last_error = false;
        terminal.attempt_failure = .{
            .err_name = reason,
            .terminal = true,
        };
        try self.updateIndexRepairIntentWithBindingEffect(
            alloc,
            repair_id,
            terminal,
            .release,
            null,
        );
    } else {
        try self.updateIndexRepairIntent(alloc, repair_id, reset);
    }
    if (entry.intent.kind == .algebraic) self.scheduleInactiveRepairShadowCleanup();
}

/// Completes the idempotent half of activation recovery. Certification
/// failures are data outcomes rather than raw scheduler errors: the caller
/// must durably roll the untrusted incarnation back or expose an exact
/// action-required terminal state.
pub fn reconcileActivatedIndexRepair(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
) !ActivatedIndexRepairReconciliation {
    // Restart reconciliation is the second half of the same catalog
    // transaction as ordinary activation. Serialize it with every live
    // index publication and foreground apply, then reload the durable
    // intent inside that fence so pointer, runtime, config, manifest,
    // checkpoint, and admission retirement describe one incarnation.
    if (comptime builtin.is_test) {
        if (DB.test_index_repair_reconcile_fence_hook) |hook| {
            if (hook.before_fence) |before_fence| try before_fence(hook.ptr, self, repair_id);
        }
    }
    var structural_guard = self.beginIndexStructuralMutation("index repair activation reconciliation", "*");
    defer structural_guard.deinit();
    lockApply(self);
    defer self.core.unlockApply();

    var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    switch (entry.intent.phase) {
        .activating, .validating, .cleanup, .rolling_back => {},
        else => return .not_active,
    }
    if (entry.intent.root_generation != self.core.root_generation or
        entry.intent.group_id != self.localRepairGroupId())
    {
        return error.RepairOwnershipLost;
    }
    const candidate = entry.intent.candidate_relative_path orelse return .not_active;
    try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
    const active_pointer = try self.core.index_manager.capturePublishedIndexRootPointerForRecovery(
        entry.intent.index_name,
    );
    defer if (active_pointer) |value| alloc.free(value);
    if (active_pointer == null or !std.mem.eql(u8, active_pointer.?, candidate)) return .not_active;
    const cfg = self.core.index_manager.get(entry.intent.index_name) orelse return .not_active;
    if (cfg.kind != entry.intent.kind or types.indexConfigHash(cfg.*) != entry.intent.config_hash) {
        return error.IndexRepairConfigurationChanged;
    }
    if (!self.core.index_manager.has(entry.intent.index_name) or
        self.core.index_manager.loadFailure(entry.intent.index_name) != null)
    {
        return .not_active;
    }

    const candidate_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.path, candidate });
    defer alloc.free(candidate_path);
    const certification = try index_generation_manifest.certifyReady(
        alloc,
        candidate_path,
        repair_id,
        entry.intent.index_name,
        entry.intent.config_hash,
    );
    const manifest_sequence = switch (certification) {
        .ready => |sequence| sequence,
        .missing => return .{ .certification_failed = .manifest_missing },
        .invalid => return .{ .certification_failed = .manifest_invalid },
        .identity_mismatch => return .{ .certification_failed = .manifest_identity_mismatch },
    };
    if (manifest_sequence != entry.intent.candidate_applied_sequence) {
        return .{ .certification_failed = .manifest_sequence_mismatch };
    }
    if (comptime builtin.is_test) {
        if (DB.test_index_repair_reconcile_fence_hook) |hook| {
            if (hook.after_validation) |after_validation| try after_validation(hook.ptr, self, repair_id);
        }
    }
    try self.core.index_manager.syncIndexByName(entry.intent.index_name, true);
    const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.intent.index_name);
    const applied_sequence = @max(
        checkpoint.applied_sequence,
        @max(entry.intent.candidate_applied_sequence, manifest_sequence),
    );
    if (entry.intent.phase != .cleanup or checkpoint.status != .clean or checkpoint.config_hash != entry.intent.config_hash) {
        const update = apply_state.AppliedSequenceUpdate{
            .index_name = entry.intent.index_name,
            .sequence = applied_sequence,
            .status = .clean,
            .generation = checkpoint.generation +| 1,
            .config_hash = entry.intent.config_hash,
        };
        try saveIndexStatusSnapshots(alloc, self.core.store, self.core.index_manager, &[_]apply_state.AppliedSequenceUpdate{update});
        try self.core.saveAppliedSequence(entry.intent.index_name, applied_sequence);
        try self.core.saveProjectionCheckpoint(entry.intent.index_name, .{
            .applied_sequence = update.sequence,
            .status = update.status,
            .generation = update.generation,
            .config_hash = update.config_hash,
        });
        try self.updateIndexRepairIntent(alloc, repair_id, .{
            .phase = .cleanup,
            .target_sequence = applied_sequence,
            .next_retry_at_ms = 0,
            .replace_last_error = true,
        });
    }
    if (comptime builtin.is_test) {
        if (DB.test_index_repair_reconcile_fence_hook) |hook| {
            if (hook.before_retirement) |before_retirement| try before_retirement(hook.ptr, self, repair_id);
        }
    }
    try self.removeIndexRepairIntentAndPin(alloc, repair_id);
    self.scheduleInactiveRepairShadowCleanup();
    return .committed;
}

pub fn activatedIndexRepairRollbackFailureDisposition(
    action: index_manager_mod.IndexManager.IndexLoadRecoveryAction,
) ActivatedIndexRepairRollback {
    return switch (action) {
        .retry_open => .retry_deferred,
        .rebuild_from_artifacts => .rebuild_required,
        .manual_intervention => .action_required,
    };
}

/// Rolls a failed-to-load activated candidate back to the exact pointer
/// captured before activation. `rolling_back` is checkpointed while the
/// structural/apply fence and exact quarantine claim are both held, before
/// the pointer changes. Restart can therefore distinguish a crash before
/// activation from an interrupted rollback and idempotently finish from
/// either pointer. Slow reopen I/O remains outside the service fence while
/// the exact repair claim prevents discovery or an ordinary retry from
/// publishing underneath it.
pub fn rollbackUnavailableActivatedIndexRepair(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    certification_failure: ?ActivatedIndexRepairCertificationFailure,
) !ActivatedIndexRepairRollback {
    var entry = try self.loadIndexRepairEntryById(alloc, repair_id);
    defer entry.deinit(alloc);
    if (!entry.intent.previous_pointer_captured) return .not_active;
    const candidate = entry.intent.candidate_relative_path orelse return .not_active;
    const rollback_certification_failure = certification_failure orelse blk: {
        if (entry.intent.phase != .rolling_back) break :blk null;
        break :blk ActivatedIndexRepairCertificationFailure.fromReason(
            entry.intent.last_error orelse break :blk null,
        );
    };
    var claim: ?index_manager_mod.IndexManager.FailedIndexLoadRepairClaim = null;
    var predecessor_loaded = false;
    {
        var structural_guard = self.beginIndexStructuralMutation("index repair rollback", entry.intent.index_name);
        defer structural_guard.deinit();
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();

        const active_pointer = try self.core.index_manager.capturePublishedIndexRootPointerForRecovery(
            entry.intent.index_name,
        );
        defer if (active_pointer) |value| alloc.free(value);
        const candidate_active = active_pointer != null and
            std.mem.eql(u8, active_pointer.?, candidate);
        const predecessor_active = if (entry.intent.previous_active_relative_path) |previous|
            active_pointer != null and std.mem.eql(u8, active_pointer.?, previous)
        else
            active_pointer == null;

        if (entry.intent.phase != .rolling_back and !candidate_active) return .not_active;
        if (entry.intent.phase == .rolling_back and !candidate_active and !predecessor_active) {
            return error.RepairOwnershipLost;
        }

        if (self.core.index_manager.failedIndexLoadSnapshot(entry.intent.index_name)) |failure| {
            claim = switch (self.core.index_manager.claimFailedIndexLoadRepair(
                entry.intent.index_name,
                .{ .exact = failure.token },
                entry.intent.repair_id,
            )) {
                .claimed, .already_claimed => |value| value,
                .absent, .mismatch, .claimed_by_other => return error.RepairOwnershipLost,
            };
        } else if (predecessor_active and self.core.index_manager.has(entry.intent.index_name)) {
            // A restart may have already opened the restored predecessor
            // before the durable rollback owner resumes.
            predecessor_loaded = true;
        } else if (!candidate_active or rollback_certification_failure == null) {
            // A loaded active candidate is reconciled by the caller before
            // entering rollback. Absence here has no safe retry identity;
            // an inactive pointer with no loaded predecessor is likewise
            // not evidence that rollback completed.
            return error.RepairOwnershipLost;
        }

        if (entry.intent.phase != .rolling_back) {
            var checkpointed = false;
            defer if (!checkpointed) self.core.index_manager.releaseFailedIndexLoadRepairClaim(
                entry.intent.index_name,
                entry.intent.repair_id,
            );
            try self.updateIndexRepairIntent(alloc, repair_id, .{
                .phase = .rolling_back,
                .next_retry_at_ms = 0,
                .last_error = if (rollback_certification_failure) |failure| failure.reason() else null,
                .replace_last_error = true,
            });
            checkpointed = true;
        }
        if (candidate_active) {
            if (rollback_certification_failure) |failure| {
                claim = try self.core.index_manager.rollbackUncertifiedActiveIndexRoot(
                    entry.intent.index_name,
                    candidate,
                    entry.intent.previous_active_relative_path,
                    entry.intent.config_hash,
                    failure.reason(),
                    repair_id,
                );
            } else {
                try self.core.index_manager.restoreActiveIndexRootPointer(
                    entry.intent.index_name,
                    candidate,
                    entry.intent.previous_active_relative_path,
                );
            }
        }
    }

    if (predecessor_loaded) {
        try self.discardInactiveIndexRepairCandidate(alloc, repair_id);
        return .prior_generation_restored;
    }
    if (comptime builtin.is_test) {
        if (DB.test_index_repair_rollback_retry_hook) |hook| {
            try hook.before_retry(hook.ptr, self, repair_id);
        }
    }
    const retry = try self.retryQuarantinedIndexLoadTarget(.{
        .name = entry.intent.index_name,
        .token = claim.?.token,
        .authority = .{ .repair_claim = entry.intent.repair_id },
    }, true);
    switch (retry.target_outcome) {
        .recovered => {
            try self.discardInactiveIndexRepairCandidate(alloc, repair_id);
            return .prior_generation_restored;
        },
        .attempted_failed => switch (activatedIndexRepairRollbackFailureDisposition(
            retry.target_action orelse return error.RepairOwnershipLost,
        )) {
            .retry_deferred => {
                // Retain both the rollback phase and exact claim. The
                // durable repair backoff owns subsequent retries, avoiding
                // a hot loop and preventing generic retry from racing it.
                try self.recordIndexRepairAttemptFailure(
                    alloc,
                    repair_id,
                    if (rollback_certification_failure) |failure|
                        failure.reason()
                    else
                        "rollback_predecessor_retry_open",
                    false,
                );
                return .retry_deferred;
            },
            .action_required => {
                try self.discardInactiveIndexRepairCandidateWithTerminal(
                    alloc,
                    repair_id,
                    if (rollback_certification_failure) |failure|
                        failure.reason()
                    else
                        "rollback_predecessor_manual_intervention",
                );
                return .action_required;
            },
            .rebuild_required => {
                try self.discardInactiveIndexRepairCandidate(alloc, repair_id);
                return .rebuild_required;
            },
            .not_active, .prior_generation_restored => unreachable,
        },
        // An exact repair-authorized forced retry is always targeted and
        // ignores backoff. Any other outcome means its incarnation or
        // ownership changed while detached I/O was in flight.
        .not_targeted, .absent, .stale, .repair_claimed, .backoff => return error.RepairOwnershipLost,
    }
}

/// Cheap startup discovery. This authorizes and durably records repair
/// debt, but deliberately performs no corpus scan and never touches the
/// quarantined index root.
pub fn discoverRecoverableStartupIndexFailures(
    self: *DB,
    alloc: Allocator,
    limit: usize,
) !StartupIndexRepairDiscovery {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, limit });
    var result: StartupIndexRepairDiscovery = .{};
    try self.ensureIndexRepairSchedulerDirectory(alloc);
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    const state_identity = self.async_context.index_repair_scheduler.identity orelse {
        self.async_context.index_repair_scheduler_mutex.unlock();
        return error.InvalidIndexRepairState;
    };
    self.async_context.index_repair_scheduler_mutex.unlock();
    const location = try self.indexRepairStateLocation();

    const inspection_limit = @max(
        @as(usize, 1),
        std.math.mul(usize, limit, index_repair_inspections_per_execution_slot) catch std.math.maxInt(usize),
    );
    var quantum = try self.core.index_manager.failedIndexLoadRecoveryCandidates(
        alloc,
        .rebuild_from_artifacts,
        inspection_limit,
    );
    defer quantum.deinit(alloc);
    // Classification performs detached durable work. Preserve successful
    // acknowledgements even if a later candidate errors, while exact
    // epoch/revision checks prevent this unwind from certifying stale debt.
    errdefer _ = self.core.index_manager.finishFailedIndexLoadRecoveryQuantum(
        .rebuild_from_artifacts,
        quantum.queue_epoch,
        quantum.candidates,
    );
    result.automatic_remaining = quantum.action_total;
    result.automatic_sweep_complete = quantum.sweep_complete;
    for (quantum.candidates) |*candidate| {
        const cfg = candidate.config;
        const action = candidate.token.action;
        if (!self.core.index_manager.failedIndexLoadRecoveryCandidateIsCurrent(candidate.*)) {
            result.automatic_deferred += 1;
            result.automatic_sweep_complete = false;
            continue;
        }
        // Directory observation and its resident binding effect share the
        // same control transaction as durable phase transitions. The
        // directory remains a cache, but its revision cannot change from
        // pending to terminal (or back) between classification and claim.
        lockAtomic(&self.async_context.index_repair_control_mutex);
        lockAtomic(&self.async_context.index_repair_scheduler_mutex);
        const existing = self.async_context.index_repair_scheduler.lookupByName(cfg.name);
        self.async_context.index_repair_scheduler_mutex.unlock();
        if (existing) |record| {
            if (comptime builtin.is_test) {
                if (DB.test_index_repair_discovery_observation_hook) |hook| {
                    try hook.after_observation(hook.ptr, self, record.repair_id);
                }
            }
            if (record.phase_terminal) {
                self.core.index_manager.releaseFailedIndexLoadRecoveryCandidate(
                    cfg.name,
                    candidate.token,
                    record.repair_id,
                );
                result.terminal += 1;
                result.existing_terminal += 1;
            } else {
                switch (self.core.index_manager.claimFailedIndexLoadRecoveryCandidate(
                    candidate.*,
                    record.repair_id,
                )) {
                    .claimed, .already_claimed => {},
                    .absent, .stale, .claimed_by_other => {
                        self.async_context.index_repair_control_mutex.unlock();
                        result.automatic_deferred += 1;
                        result.automatic_sweep_complete = false;
                        continue;
                    },
                }
                result.already_pending += 1;
            }
            self.async_context.index_repair_control_mutex.unlock();
            candidate.classified = true;
            continue;
        }
        self.async_context.index_repair_control_mutex.unlock();
        switch (action) {
            .retry_open => continue,
            .manual_intervention => {
                result.terminal += 1;
                candidate.classified = true;
                continue;
            },
            .rebuild_from_artifacts => {},
        }
        // Both allowlisted publication failures are currently HBC dense
        // index failures. Do not let future reuse of either error name
        // silently broaden automatic reconstruction.
        if (cfg.kind != .dense_vector) {
            result.terminal += 1;
            candidate.classified = true;
            continue;
        }
        if (result.discovered >= limit) {
            result.automatic_deferred += 1;
            continue;
        }

        const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
        const now_ms = self.indexRepairNowMs();
        const target_sequence = self.core.nextDerivedSequence();
        const repair_id = try index_repair_state.newRepairId(alloc);
        const index_name = try alloc.dupe(u8, cfg.name);
        var intent = index_repair_state.IndexRepairIntent{
            .repair_id = repair_id,
            .db_identity = state_identity.db_identity,
            .group_id = self.localRepairGroupId(),
            .replica_id = state_identity.replica_id,
            .root_generation = state_identity.root_generation,
            .index_name = index_name,
            .kind = cfg.kind,
            .config_hash = types.indexConfigHash(cfg),
            .detected_sequence = target_sequence,
            .target_sequence = target_sequence,
            .started_at_ms = now_ms,
            .updated_at_ms = now_ms,
            .owner_epoch = 0,
        };
        defer intent.deinit(alloc);
        var catalog_incarnation = self.beginIndexCatalogIncarnationGuard();
        defer catalog_incarnation.deinit();
        lockAtomic(&self.async_context.index_repair_control_mutex);
        var repair_control_locked = true;
        defer if (repair_control_locked) self.async_context.index_repair_control_mutex.unlock();
        // Detached config cloning and checkpoint inspection run without the
        // catalog lock. Revalidate under the same short lifecycle boundary
        // used by drop/recreate and quarantine publication so the durable
        // intent cannot bind a stale same-name catalog incarnation.
        if (!self.core.index_manager.failedIndexLoadRecoveryCandidateIsCurrent(candidate.*)) {
            result.automatic_deferred += 1;
            result.automatic_sweep_complete = false;
            continue;
        }
        switch (self.core.index_manager.claimFailedIndexLoadRecoveryCandidate(candidate.*, repair_id)) {
            .claimed, .already_claimed => {},
            .absent, .stale, .claimed_by_other => {
                result.automatic_deferred += 1;
                result.automatic_sweep_complete = false;
                continue;
            },
        }
        var release_repair_claim = true;
        defer if (release_repair_claim) self.core.index_manager.releaseFailedIndexLoadRecoveryCandidate(
            candidate.config.name,
            candidate.token,
            repair_id,
        );
        var admission_fence = try self.core.index_manager.beginRepairAdmissionFence(intent.index_name);
        defer self.core.index_manager.cancelRepairAdmissionFence(&admission_fence, intent.index_name);
        const control_revision = index_repair_state.putEntryAt(
            alloc,
            location,
            state_identity,
            null,
            .{ .intent = intent },
        ) catch |err| switch (err) {
            // Another authorized creator won after the initial directory
            // observation. The exact candidate claim unwinds below and a
            // later bounded pass adopts the committed durable intent.
            error.RepairTransitionConflict => {
                result.automatic_deferred += 1;
                result.automatic_sweep_complete = false;
                continue;
            },
            else => return err,
        };
        // The durable intent now owns this exact quarantine incarnation.
        // Keep the claim until terminal classification or successful
        // replacement removes the failed-load entry.
        release_repair_claim = false;
        self.core.index_manager.finishRepairAdmissionFence(
            &admission_fence,
            intent.index_name,
            repairAdmissionVersion(intent, 1, control_revision),
            true,
            false,
        );
        noteIndexRepairSchedulerUpsert(self.async_context, intent, 1, control_revision);

        // Intent durability precedes the serviceability marker. If this
        // checkpoint write fails, restart still rediscovers the intent and
        // completes the transition without deleting the quarantined root.
        try self.core.saveProjectionCheckpoint(cfg.name, .{
            .applied_sequence = checkpoint.applied_sequence,
            .status = .repair_required,
            .generation = checkpoint.generation,
            .config_hash = types.indexConfigHash(cfg),
        });
        self.async_context.index_repair_control_mutex.unlock();
        repair_control_locked = false;
        catalog_incarnation.release();
        notifyIndexRepairVisibilityHook(
            self.async_context,
            .index_repair_pending,
            intent,
            1,
            .serviceable,
            false,
            false,
        );
        result.discovered += 1;
        candidate.classified = true;
    }
    const final_queue = self.core.index_manager.finishFailedIndexLoadRecoveryQuantum(
        .rebuild_from_artifacts,
        quantum.queue_epoch,
        quantum.candidates,
    );
    result.automatic_remaining = final_queue.action_total;
    result.automatic_sweep_complete = final_queue.sweep_complete;
    return result;
}

pub fn applyCommittedRepairOutcomeToAdvance(
    result: *IndexRepairAdvanceResult,
    repair: types.ArtifactRepairResult,
) void {
    result.has_repair_outcome = true;
    result.artifacts_repaired = repair.repaired;
    result.missing_source_docs = repair.missing_source_docs;
    result.failed = repair.failed;
    result.unsupported = repair.unsupported;
    result.unresolved = repair.unresolved;
    result.in_progress = repair.in_progress;
    result.indexes_rebuilt = repair.indexes_rebuilt;
    result.indexes_degraded_before = repair.indexes_degraded_before;
    result.indexes_degraded_after = repair.indexes_degraded_after;
    result.debt_remaining = repair.debt_remaining;
}

pub fn repairCapacityDomain(options: types.ArtifactRepairRunOptions) resource_manager_mod.CapacityDomainId {
    if (options.capacity_source) |source| return source.domain_id;
    return options.capacity_domain_id;
}

pub fn repairCapacityObservation(options: types.ArtifactRepairRunOptions) !resource_manager_mod.CapacityObservation {
    const observed = if (options.capacity_source) |source|
        source.current() catch |err| switch (err) {
            // Capacity observation is an optional backend capability.
            // Unsupported backends still participate in node-wide byte
            // accounting; transient failures from a supported source
            // remain retryable and fail admission closed.
            error.UnsupportedPlatform => resource_manager_mod.CapacityObservation{},
            else => return err,
        }
    else
        options.capacity_observation;
    return .{
        .available_bytes = observed.available_bytes,
        .capacity_bytes = observed.capacity_bytes,
        .observed_at_ns = observed.observed_at_ns,
        .valid_for_ns = observed.valid_for_ns,
    };
}

pub fn repairWorkingSetPlan(
    alloc: Allocator,
    manager: *resource_manager_mod.ResourceManager,
    cfg: types.IndexConfig,
) !RepairWorkingSetPlan {
    const base_bytes: u64 = 8 * 1024 * 1024;
    if (cfg.kind != .dense_vector) return .{ .reservation_bytes = base_bytes };

    const dims = try index_manager_mod.denseConfigDimensions(alloc, cfg);
    // The snapshot scanner owns one decoded vector while the mapper/HBC
    // boundary may transiently own a second copy. Include keys, artifact
    // identity, list capacity, and allocator overhead per item. HBC/LSM
    // caches and routing work remain independently accounted by their own
    // ResourceManager slices.
    const raw_vector_bytes = std.math.mul(u64, dims, @sizeOf(f32)) catch std.math.maxInt(u64);
    const per_item_bytes = (std.math.mul(u64, raw_vector_bytes, 2) catch std.math.maxInt(u64)) +| 2048;
    const desired_items: u64 = 2048;
    const slice_stats = manager.sliceStats(.dense_repair_working_set);
    const hard_available = if (slice_stats.hard_limit_bytes == 0)
        std.math.maxInt(u64)
    else
        slice_stats.hard_limit_bytes -| slice_stats.used_bytes;
    const minimum_reservation = base_bytes +| per_item_bytes;
    // Preserve ResourceManager's authoritative hard-limit rejection and
    // metric. For admissible work, however, size background batches to the
    // soft budget: reaching the hard limit is a safety failure, not a
    // throughput target.
    if (hard_available < minimum_reservation) {
        return .{
            .reservation_bytes = minimum_reservation,
            .dense_rebuild_batch_items = 1,
        };
    }
    const available = if (slice_stats.soft_limit_bytes == 0)
        hard_available
    else
        slice_stats.soft_limit_bytes -| slice_stats.used_bytes;
    if (available < minimum_reservation) return error.RepairResourceUnavailable;
    const fitting_items = if (available <= base_bytes)
        0
    else
        (available - base_bytes) / @max(@as(u64, 1), per_item_bytes);
    const batch_items = @min(desired_items, fitting_items);
    return .{
        .reservation_bytes = base_bytes +| (per_item_bytes *| batch_items),
        .dense_rebuild_batch_items = if (comptime builtin.is_test)
            db_zig.test_dense_repair_rebuild_batch_size orelse @as(usize, @intCast(batch_items))
        else
            @intCast(batch_items),
    };
}

pub fn indexRepairInspectionWindow(
    candidate_count: usize,
    execution_limit: usize,
    cursor: u64,
) IndexRepairInspectionWindow {
    if (candidate_count == 0 or execution_limit == 0) return .{ .start = 0, .budget = 0 };
    return .{
        .start = @intCast(cursor % candidate_count),
        .budget = @min(
            candidate_count,
            std.math.mul(
                usize,
                execution_limit,
                index_repair_inspections_per_execution_slot,
            ) catch std.math.maxInt(usize),
        ),
    };
}

pub fn selectIndexRepairSchedulerQuantum(
    self: *DB,
    alloc: Allocator,
    execution_limit: usize,
) !IndexRepairSchedulerSelection {
    return self.selectIndexRepairSchedulerQuantumForIndex(alloc, execution_limit, null);
}

pub fn selectIndexRepairSchedulerQuantumForIndex(self: *DB, alloc: Allocator, execution_limit: usize, target_index_name: ?[]const u8) !IndexRepairSchedulerSelection {
    try self.ensureIndexRepairSchedulerDirectory(alloc);
    var selection: IndexRepairSchedulerSelection = .{};
    errdefer selection.deinit(alloc);
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    defer self.async_context.index_repair_scheduler_mutex.unlock();
    const directory = &self.async_context.index_repair_scheduler;
    const summary = directory.summaryForIndex(target_index_name);
    selection.terminal = summary.terminal;
    selection.deferred = summary.paused;
    selection.remaining = summary.runnable + summary.paused + summary.terminal;
    selection.next_retry_at_ms = summary.earliest_retry_at_ms;
    const target_index = if (target_index_name) |name| directory.by_name.get(name) else null;
    if (target_index_name != null and target_index == null) return selection;
    const inspection = if (target_index) |index| IndexRepairInspectionWindow{ .start = index, .budget = @min(execution_limit, 1) } else indexRepairInspectionWindow(
        directory.records.items.len,
        execution_limit,
        @intCast(directory.cursor),
    );
    try selection.repairs.ensureTotalCapacity(alloc, inspection.budget);
    const now_ms = self.indexRepairNowMs();
    while (selection.inspected < inspection.budget) : (selection.inspected += 1) {
        const record_index = (inspection.start + selection.inspected) % directory.records.items.len;
        const record = directory.records.items[record_index];
        if (target_index_name == null) directory.cursor = (record_index + 1) % directory.records.items.len;
        if (record.class != .runnable) continue;
        if (record.next_retry_at_ms > now_ms) {
            selection.deferred += 1;
            if (selection.next_retry_at_ms == 0 or record.next_retry_at_ms < selection.next_retry_at_ms) {
                selection.next_retry_at_ms = record.next_retry_at_ms;
            }
            continue;
        }
        if (record.progress_wait_until_sequence != null) {
            // The slow audit deadline fired without a matching progress
            // callback. Clear the process-local wait before execution so
            // the new observation can arm a fresh token and deadline.
            const mutable = &directory.records.items[record_index];
            mutable.progress_wait_until_sequence = null;
            directory.progress_waiters -= 1;
            mutable.next_retry_at_ms = mutable.durable_next_retry_at_ms;
            directory.rescheduleRunnable(mutable.repair_id);
            if (directory.progress_waiters == 0) {
                self.async_context.index_repair_progress_wait_pending.store(false, .release);
            }
        }
        selection.repairs.appendAssumeCapacity(.{
            .repair_id = record.repair_id,
            .revision = record.revision,
            .audit_due = record.audit_wait_armed,
        });
    }
    return selection;
}

/// Bounded owner-side pass used by managed startup workers and periodic
/// repair scans. Discovery itself is cheap; callers choose when execution
/// is admitted by passing a small `limit` (normally one per node).
pub fn repairRecoverableStartupIndexFailures(
    self: *DB,
    alloc: Allocator,
    limit: usize,
    options: types.ArtifactRepairRunOptions,
) !StartupIndexRepairResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, limit, options });
    var result: StartupIndexRepairResult = .{};
    const discovery = try self.discoverRecoverableStartupIndexFailures(alloc, limit);
    result.discovered = discovery.discovered;
    result.automatic_discovery_pending = discovery.automaticDiscoveryPending();
    // Existing terminal intents are counted from the durable state below.
    // Discovery contributes only terminal load failures for which no intent
    // exists, avoiding double-counting checkpointed failures.
    result.terminal = if (options.target_index_name == null) discovery.terminal - discovery.existing_terminal else 0;

    var selection = try self.selectIndexRepairSchedulerQuantumForIndex(alloc, limit, options.target_index_name);
    defer selection.deinit(alloc);
    result.terminal += selection.terminal;
    result.deferred += selection.deferred;
    result.next_retry_at_ms = selection.next_retry_at_ms;
    result.inspected = selection.inspected;
    result.remaining = selection.remaining;
    var consumed_slots: usize = 0;
    for (selection.repairs.items) |scheduled| {
        if (consumed_slots >= limit) break;
        const advanced = try self.advanceScheduledIndexRepairIntent(
            alloc,
            scheduled.repair_id,
            options,
            scheduled.revision,
            scheduled.audit_due,
        );
        result.attempted += @intFromBool(advanced.attempted);
        result.repaired += @intFromBool(advanced.repaired);
        result.degraded += @intFromBool(advanced.indexes_degraded_after != 0);
        result.deferred += @intFromBool(advanced.deferred);
        result.disk_waits += @intFromBool(advanced.disk_wait);
        result.busy += @intFromBool(advanced.busy);
        result.documents_reprocessed += advanced.documents_reprocessed;
        // Material execution and inspection are independently bounded.
        // Deferred progressive generations do not consume a repair slot,
        // while the rotating cursor prevents them from pinning every
        // future quantum ahead of later damaged indexes.
        if (advanced.attempted or advanced.busy or advanced.terminal) {
            consumed_slots += 1;
        }
    }
    // Mutations update the directory synchronously after their durable
    // checkpoint commit, so this exact count costs O(1) in steady state.
    // If a post-commit projection allocation failed, rebuild from the
    // durable authority before returning rather than misreporting an empty
    // aggregate. That exceptional recovery is allowed to fail explicitly.
    try self.ensureIndexRepairSchedulerDirectory(alloc);
    lockAtomic(&self.async_context.index_repair_scheduler_mutex);
    const directory = &self.async_context.index_repair_scheduler;
    const summary = directory.summaryForIndex(options.target_index_name);
    result.remaining = summary.runnable + summary.paused + summary.terminal;
    result.wake = summary.wake;
    result.next_retry_at_ms = result.wake.retryAtMs();
    self.async_context.index_repair_scheduler_mutex.unlock();
    return result;
}

pub fn repairIndexIssuesWithRequest(
    self: *DB,
    alloc: Allocator,
    req: types.ArtifactRepairRunRequest,
    options: types.ArtifactRepairRunOptions,
    lease_authority: IndexRepairLeaseAuthority,
) !types.ArtifactRepairResult {
    try checkArtifactRepairCancelled(options);
    const requested_index = req.index_name orelse return error.InvalidArgument;
    if (req.cursor != null and req.cursor.?.len != 0) return error.InvalidArgument;
    const limit = if (req.limit == 0) @as(u32, 100) else req.limit;
    var result = types.ArtifactRepairResult{ .limit = limit };
    if (limit == 0) return result;
    if (req.control == null and req.repair_id != null) return error.InvalidArgument;

    const cfg_ptr = self.core.index_manager.get(requested_index) orelse return error.NotFound;
    var cfg = try types.IndexConfig.clone(alloc, cfg_ptr.*);
    defer cfg.deinit(alloc);

    if (req.control) |control| {
        if (req.force or req.cursor != null or req.artifact_kind != null) return error.InvalidArgument;
        const applied = switch (control) {
            .pause_automatic => try self.pauseAutomaticIndexRepair(alloc, requested_index, req.repair_id),
            .resume_automatic => try self.resumeAutomaticIndexRepair(alloc, requested_index, req.repair_id),
            .cancel_current_attempt => try self.cancelCurrentIndexRepairAttempt(alloc, requested_index, req.repair_id),
        };
        result.scanned = 1;
        result.controls_applied = @intFromBool(applied);
        result.debt_remaining = applied;
        result.indexes_degraded_before = @intFromBool(applied);
        result.indexes_degraded_after = @intFromBool(applied);
        return result;
    }

    if (req.artifact_kind) |kind| {
        const matches_kind = switch (cfg.kind) {
            .dense_vector, .sparse_vector => kind == .embedding,
            .graph => kind == .graph,
            .full_text => kind == .full_text,
            .algebraic => kind == .algebraic,
        };
        if (!matches_kind) return error.NotFound;
    }

    const repair_required = try self.indexGenerationRepairRequired(alloc, cfg.name);
    result.indexes_degraded_before = @intFromBool(repair_required);
    if (!repair_required and !req.force) return result;
    if ((req.repair_job_id == null) != (req.repair_job_created_at_ms == null)) return error.InvalidArgument;
    const repair_job_created_at_ms = req.repair_job_created_at_ms orelse 0;
    if (req.force and req.repair_job_id != null and try self.operatorRepairAlreadyCompleted(
        alloc,
        cfg,
        req.repair_job_id.?,
        repair_job_created_at_ms,
    )) {
        result.scanned = 1;
        result.indexes_rebuilt = 1;
        try self.finalizeCommittedIndexRepairOutcome(alloc, cfg.name, 0, &result);
        return result;
    }

    const initial_load_failure = self.core.index_manager.failedIndexLoadSnapshot(cfg.name);
    const had_load_failure = initial_load_failure != null;
    var durable_repair_id = try self.indexRepairIdForIndex(alloc, cfg.name);
    if (initial_load_failure != null and
        initial_load_failure.?.token.action == .rebuild_from_artifacts)
    {
        _ = try self.discoverRecoverableStartupIndexFailures(alloc, 1);
        durable_repair_id = try self.indexRepairIdForIndex(alloc, cfg.name);
    }
    if (try self.classifyCurrentDenseGenerationRepair(alloc, cfg)) |classification| {
        durable_repair_id = try self.ensureAutomaticDenseGenerationRepairIntent(
            alloc,
            cfg,
            classification.trigger,
            classification.last_error,
        );
    } else if (durable_repair_id == null) {
        durable_repair_id = try self.createOperatorGenerationRepairIntent(
            alloc,
            cfg,
            req.repair_job_id orelse 0,
            repair_job_created_at_ms,
        );
    }
    if (durable_repair_id) |repair_id| {
        if (req.force and req.repair_job_id != null) {
            try self.attachOperatorRepairRequest(
                alloc,
                repair_id,
                req.repair_job_id.?,
                repair_job_created_at_ms,
            );
        }
    }

    // Public/operator entry points never execute a durable generation
    // rebuild through this lower-level function. Managed callers enqueue
    // the exact group; standalone callers synchronously enter the same
    // admitted state machine used by the owner-side scheduler. The private
    // lease authority is also the recursion capability: public options
    // cannot bypass the durable owner by setting a data bit.
    if (durable_repair_id) |repair_id| if (lease_authority == .acquire) {
        result.scanned = 1;
        result.indexes_degraded_before = @intFromBool(repair_required);
        result.indexes_degraded_after = 1;
        if (req.force) {
            var current = try self.loadIndexRepairEntryById(alloc, repair_id);
            defer current.deinit(alloc);
            if (current.intent.phase == .terminal) {
                try self.updateIndexRepairIntent(alloc, repair_id, .{
                    .phase = .detected,
                    .attempt_count = current.intent.attempt_count +| 1,
                    .failure_streak = 0,
                    .next_retry_at_ms = 0,
                    .replace_last_error = true,
                });
            }
        }
        if (options.defer_durable_index_repair_execution) {
            result.in_progress = 1;
            result.unresolved = 1;
            result.debt_remaining = true;
            return result;
        }
        const advanced = try self.advanceIndexRepairIntent(alloc, repair_id, options);
        result.reprocessed = advanced.documents_reprocessed;
        if (advanced.has_repair_outcome) {
            result.repaired = advanced.artifacts_repaired;
            result.missing_source_docs = advanced.missing_source_docs;
            result.failed = advanced.failed;
            result.unsupported = advanced.unsupported;
            result.unresolved = advanced.unresolved;
            result.in_progress = advanced.in_progress;
            result.indexes_rebuilt = advanced.indexes_rebuilt;
            result.indexes_degraded_before = advanced.indexes_degraded_before;
            result.indexes_degraded_after = advanced.indexes_degraded_after;
            result.debt_remaining = advanced.debt_remaining;
        } else if (advanced.repaired) {
            result.repaired = 1;
            result.indexes_rebuilt = 1;
        } else if (advanced.terminal) {
            result.failed = 1;
            result.unresolved = 1;
            result.debt_remaining = true;
        } else {
            result.in_progress = 1;
            result.unresolved = 1;
            result.debt_remaining = true;
        }
        return result;
    };

    // Durable state-machine execution owns the lease at its outermost
    // boundary. Standalone non-durable repair retains the same exclusion
    // by acquiring it here. This avoids a nested, non-reentrant lease while
    // keeping all direct rebuild callers serialized with deletion.
    var local_repair_lease = false;
    if (lease_authority == .acquire) {
        if (!(try self.beginIndexRepairLease(cfg.name))) {
            result.scanned += 1;
            result.in_progress += 1;
            result.unresolved += 1;
            // A contended durable repair remains degraded. Do not publish
            // the zero-value gauge as an authoritative healthy observation.
            result.indexes_degraded_after = 1;
            result.debt_remaining = true;
            return result;
        }
        local_repair_lease = true;
    }
    defer if (local_repair_lease) self.endIndexRepairLease(cfg.name);

    result.scanned += 1;
    if (durable_repair_id) |repair_id| {
        var durable_entry = try self.loadIndexRepairEntryById(alloc, repair_id);
        defer durable_entry.deinit(alloc);
        const resumable = durable_entry.intent.candidate_relative_path != null and switch (durable_entry.intent.phase) {
            .building => durable_entry.intent.build_resume_key != null and
                indexRepairSupportsSnapshotCursor(cfg.kind),
            .catching_up, .ready, .waiting_for_convergence => true,
            else => false,
        };
        if (durable_entry.intent.phase == .terminal) {
            try self.updateIndexRepairIntent(alloc, repair_id, .{
                .phase = .detected,
                .attempt_count = durable_entry.intent.attempt_count +| 1,
                .failure_streak = 0,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
            durable_entry.intent.phase = .detected;
        }
        if (!resumable and durable_entry.intent.phase != .preflight) {
            // The scheduler's earlier admission probe precedes capacity
            // checks and source validation. Canonical publication can
            // become queryable in that interval. Serialize the final
            // decision with status: retain a certified partial generation,
            // retire a complete admission, or commit preflight before an
            // observer can publish the old canonical proof as ready.
            lockApply(self);
            defer self.core.unlockApply();
            var current = try self.loadIndexRepairEntryById(alloc, repair_id);
            defer current.deinit(alloc);
            if (try self.managedAdmissionGenerationIsServiceable(alloc, current.intent)) {
                try self.ensureManagedAdmissionCanonicalWorker(alloc, current.intent);
                try self.removeIndexRepairIntentAndPin(alloc, repair_id);
                result.repaired += 1;
                result.indexes_degraded_after = 0;
                return result;
            }
            if (try self.managedAdmissionGenerationIsQueryable(alloc, current.intent)) {
                try self.ensureManagedAdmissionCanonicalWorker(alloc, current.intent);
                result.in_progress += 1;
                result.unresolved += 1;
                result.debt_remaining = true;
                result.indexes_degraded_after = 0;
                return result;
            }
            try self.updateIndexRepairIntent(alloc, repair_id, .{
                .phase = .preflight,
                .attempt_count = @max(@as(u32, 1), current.intent.attempt_count),
                .failure_streak = 0,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
        }
        self.ensureDenseArtifactTargetCounterForRepair(
            alloc,
            cfg,
            repair_id,
            options.cancel_check,
        ) catch |err| switch (err) {
            error.Canceled, error.StaleDenseArtifactCounterBootstrap => {
                result.in_progress += 1;
                result.unresolved += 1;
                result.debt_remaining = true;
                return result;
            },
            else => return err,
        };
        self.validateIndexRepairSourcePreflight(alloc, cfg) catch |err| {
            const err_name = if (err == error.RepairSourceCoverageIncomplete)
                "dense_artifact_coverage_missing"
            else
                @errorName(err);
            try self.recordIndexRepairAttemptFailure(alloc, repair_id, err_name, true);
            result.failed += 1;
            result.unresolved += 1;
            result.debt_remaining = true;
            return result;
        };
    }
    result.indexes_degraded_before = @intFromBool(repair_required);
    result.indexes_degraded_after = 1;
    if (self.core.index_manager.failedIndexLoadSnapshot(cfg.name)) |failure| {
        const recovery_action = failure.token.action;
        switch (recovery_action) {
            .retry_open, .manual_intervention => {
                // Explicit named operator repair may first retry that
                // exact quarantined incarnation. If its identity changes,
                // defer so the next pass re-audits the new policy instead
                // of carrying stale authorization into reconstruction.
                const retry_result = try self.retryQuarantinedIndexLoadTarget(.{
                    .name = cfg.name,
                    .token = failure.token,
                }, true);
                switch (retry_result.target_outcome) {
                    .stale, .repair_claimed, .backoff => {
                        result.in_progress += 1;
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        return result;
                    },
                    .not_targeted => unreachable,
                    .absent, .attempted_failed, .recovered => {},
                }
                const remaining_failure = self.core.index_manager.failedIndexLoadSnapshot(cfg.name);
                if (remaining_failure) |current| {
                    // A failed open advances the resident revision by
                    // design. Re-audit its policy, but retain operator
                    // authorization only for the same durable config.
                    if (current.token.config_hash != failure.token.config_hash) {
                        result.in_progress += 1;
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        return result;
                    }
                    if (recovery_action == .retry_open or
                        current.token.action == .retry_open)
                    {
                        result.failed += 1;
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        return result;
                    }
                }
            },
            .rebuild_from_artifacts => {
                // Build directly from the retained status-only config. The
                // quarantined root remains untouched until the replacement
                // is durable and activated by pointer swap.
            },
        }
    }
    if (had_load_failure and self.core.index_manager.loadFailure(cfg.name) == null and (cfg.kind == .full_text or cfg.kind == .algebraic) and !try self.indexGenerationRepairRequired(alloc, cfg.name)) {
        result.indexes_degraded_after = 0;
        result.repaired += 1;
        return result;
    }

    const rebuilt = self.rebuildIndexWithShadowReplacement(alloc, cfg, options, durable_repair_id) catch |err| switch (err) {
        error.Canceled => {
            result.unresolved += 1;
            result.debt_remaining = true;
            return result;
        },
        error.ShadowIndexCatchUpIncomplete => {
            result.failed += 1;
            result.unresolved += 1;
            result.debt_remaining = true;
            return result;
        },
        else => return err,
    };
    result.reprocessed += rebuilt.reprocessed;
    if (rebuilt.yielded) {
        result.unresolved += rebuilt.unresolved_artifacts;
        result.in_progress += 1;
        result.unresolved += 1;
        result.debt_remaining = true;
        return result;
    }
    result.indexes_rebuilt += 1;
    try self.finalizeCommittedIndexRepairOutcome(alloc, cfg.name, rebuilt.unresolved_artifacts, &result);
    return result;
}

pub fn observeRepairCatchUpCost(previous_ns_per_sequence: u64, from_sequence: u64, to_sequence: u64, elapsed_ns: u64) u64 {
    const advanced = to_sequence -| from_sequence;
    if (advanced == 0) return previous_ns_per_sequence;
    const observed = @max(@as(u64, 1), (elapsed_ns +| advanced -| 1) / advanced);
    // Retain the slower observed rate. Activation admission should be
    // conservative when foreground pressure makes recent replay slower.
    return @max(previous_ns_per_sequence, observed);
}

pub fn repairActivationReplayDeadline(now_ns: u64, activation_deadline_ns: u64, pause_budget_ns: u64) ?u64 {
    if (now_ns >= activation_deadline_ns) return null;
    // Reserve a bounded tail for manifest validation, pointer publication,
    // and clean-checkpoint durability.  Five milliseconds is sufficient
    // for small budgets; large budgets reserve at most fifty milliseconds
    // so replay still receives the majority of the fence interval.
    const reserve_ns = @max(
        5 * std.time.ns_per_ms,
        @min(50 * std.time.ns_per_ms, pause_budget_ns / 5),
    );
    if (activation_deadline_ns - now_ns <= reserve_ns) return null;
    return activation_deadline_ns - reserve_ns;
}

pub fn repairActivationAdmissible(
    gap_sequences: u64,
    observed_ns_per_sequence: u64,
    max_gap_sequences: u64,
    max_pause_ns: u64,
) bool {
    if (gap_sequences == 0) return true;
    if (gap_sequences > max_gap_sequences or observed_ns_per_sequence == 0) return false;
    const estimated_pause_ns = std.math.mul(u64, gap_sequences, observed_ns_per_sequence) catch std.math.maxInt(u64);
    return estimated_pause_ns <= max_pause_ns;
}

pub fn indexGenerationRepairRequired(self: *DB, alloc: Allocator, index_name: []const u8) !bool {
    if (self.core.index_manager.loadFailure(index_name) != null) return true;
    // A durable generation intent is itself authoritative repair debt.
    // Managed catalog admission can intentionally leave the active
    // checkpoint clean while a shadow replacement is pending, so looking
    // only at checkpoint/artifact state would make the ordinary named
    // repair entry point skip work that has already been admitted and is
    // fail-closing service. Healthy indexes still require `force` because
    // they have neither an intent nor any of the conditions below.
    if (try self.indexRepairIdForIndex(alloc, index_name) != null) return true;
    if (try self.core.index_manager.denseNativePhysicalMigrationRequired(index_name)) return true;
    const checkpoint = try self.core.loadProjectionCheckpoint(alloc, index_name);
    switch (checkpoint.status) {
        .degraded, .repair_required => return true,
        .clean, .rebuilding => {},
    }
    if (try self.denseCoverageRegressionRepairRequired(alloc, index_name)) return true;
    // Source artifacts have their own durable repair queue. Treating that
    // queue as generation debt recursively starts another shadow rebuild
    // after a repaired generation activates but before its source artifact
    // is regenerated. Managed catch-up repairs source debt, advances replay,
    // and rebuilds a generation only when one of the structural predicates
    // above still requires it.
    return false;
}

/// Classify a generation after its replacement is durably committed.
/// Both the initial response and crash-idempotent job replay enter here so
/// a completion marker can never be mistaken for proof of healthy coverage.
pub fn finalizeCommittedIndexRepairOutcome(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    observed_unresolved_artifacts: u64,
    result: *types.ArtifactRepairResult,
) !void {
    const repair_summary_ready = try self.artifactRepairSummaryReady(alloc);
    const unresolved_artifacts = if (repair_summary_ready)
        @max(
            observed_unresolved_artifacts,
            (try self.artifactRepairSummaryIndexSnapshot(alloc, index_name, true)).count,
        )
    else
        observed_unresolved_artifacts;
    result.unresolved += unresolved_artifacts;

    // A rebuilding summary cannot prove absence. Preserve debt without a
    // corpus scan on this latency-sensitive completion path; background
    // summary maintenance will publish the exact count.
    result.debt_remaining = result.debt_remaining or !repair_summary_ready or unresolved_artifacts != 0;

    const generation_repair_required = try self.indexGenerationRepairRequired(alloc, index_name);
    const coverage = try self.committedIndexCoverageDisposition(alloc, index_name);
    if (generation_repair_required or coverage != .complete) {
        result.unresolved += 1;
        result.debt_remaining = true;
        result.indexes_degraded_after = 1;
    }

    // "Rebuilt" describes completed work. "Repaired" is the stronger
    // operator promise that no known or indeterminate debt remains.
    result.indexes_degraded_after = @intFromBool(result.debt_remaining);
    if (!result.debt_remaining) {
        result.repaired += 1;
    }
}

pub fn indexKindSupportsManagedGenerationRepair(kind: types.IndexKind) bool {
    return switch (kind) {
        .dense_vector, .sparse_vector, .graph, .full_text, .algebraic => true,
    };
}

pub fn recoverCommittedIndexAdmission(self: *DB, cfg: types.IndexConfig, activation_err: anyerror) !?u128 {
    // Admission remains fail-closed until the durable repair marker is
    // materialized, so this is degraded-but-recoverable state.
    std.log.warn("index catalog admission committed with pending activation index={s} err={s}", .{ cfg.name, @errorName(activation_err) });
    self.requestManagedAdmissionMaterialization();
    self.drainManagedIndexAdmissionsUnderStructuralGuard(self.alloc) catch |materialize_err| {
        // The primary-store marker remains authoritative and startup will
        // retry projection into the repair scheduler.
        std.log.err("committed index activation marker remains pending index={s} err={s}", .{ cfg.name, @errorName(materialize_err) });
        return null;
    };
    const repair_id = self.indexRepairIdForIndex(self.alloc, cfg.name) catch |lookup_err| {
        std.log.err("committed index activation repair lookup failed index={s} err={s}", .{ cfg.name, @errorName(lookup_err) });
        return null;
    };
    return repair_id;
}

/// Re-attempts opening quarantined indexes (artifacts that failed to
/// load at open time). `force` ignores per-index backoff — used by tests
/// and operator-triggered recovery. Runs the same detached-open path as
/// the initial load, so a recovered full-text/sparse/graph index resumes
/// its persisted rebuild state and catches up before registering.
pub fn retryQuarantinedIndexLoads(self: *DB, force: bool) !index_manager_mod.IndexManager.QuarantineRetryResult {
    var publication_fence = QuarantineIndexPublicationFence{ .db = self };
    const execution_limit = if (force)
        self.core.index_manager.failedIndexLoadSummary().total
    else
        @as(usize, 8);
    return try self.core.index_manager.retryFailedIndexLoads(
        self.core.store,
        monotonicTimeNs(),
        force,
        null,
        execution_limit,
        publication_fence.iface(),
    );
}

/// Retry exactly one quarantined index. Ignoring backoff is orthogonal to
/// scope: targeted rollback and operator repair must never inspect, clone,
/// open, or publish unrelated failed indexes.
pub fn retryQuarantinedIndexLoad(
    self: *DB,
    index_name: []const u8,
    force: bool,
) !index_manager_mod.IndexManager.QuarantineRetryResult {
    const failure = self.core.index_manager.failedIndexLoadSnapshot(index_name) orelse
        return .{
            .remaining = self.core.index_manager.failedIndexLoadSummary().total,
            .target_outcome = .absent,
        };
    return try self.retryQuarantinedIndexLoadTarget(.{
        .name = index_name,
        .token = failure.token,
    }, force);
}

pub fn retryQuarantinedIndexLoadTarget(
    self: *DB,
    target: index_manager_mod.IndexManager.FailedIndexLoadRetryTarget,
    force: bool,
) !index_manager_mod.IndexManager.QuarantineRetryResult {
    var publication_fence = QuarantineIndexPublicationFence{ .db = self };
    return try self.core.index_manager.retryFailedIndexLoads(
        self.core.store,
        monotonicTimeNs(),
        force,
        target,
        1,
        publication_fence.iface(),
    );
}

/// Start only after the DB has reached its final address. The scheduler
/// registration retains `self` until its stop/join boundary.
pub fn startQuarantineRetryWorkerIfNeeded(self: *DB) void {
    if (comptime !builtin.is_test and (builtin.single_threaded or builtin.os.tag == .freestanding)) return;
    if (!builtin.is_test and (!self.local_execution.optional_runtime_workers_enabled or self.open_mode != .writer)) return;
    if (self.open_mode == .writer and self.physical_root_mode == .filesystem_managed and self.primary_backend == .lsm and
        self.core.identity_namespace.table_id != 0 and self.backend_runtime.query_cut_repository != null and
        self.backend_runtime.query_cut_repository.?.vtable.warm != null and self.backend_runtime.query_cut_repository.?.limits.max_cuts > 1)
        self.native_query_warming.start(.{ .ptr = self, .runtime = self.backend_runtime, .turn = warmNativeCheckpoint });
    self.quarantine_recovery.start(.{
        .ptr = self,
        .runtime = self.backend_runtime,
        .has_failures = quarantineHasFailures,
        .retry = retryQuarantineFromOwner,
    });
}

pub fn quarantineHasFailures(ptr: *anyopaque) bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return self.core.index_manager.hasLoadFailures();
}

pub fn retryQuarantineFromOwner(ptr: *anyopaque) !usize {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return (try self.retryQuarantinedIndexLoads(false)).remaining;
}

pub fn quarantineRetryWorkerStartedAtCurrentAddressForTest(self: *const DB) bool {
    return builtin.is_test and self.quarantine_recovery.start_address_for_test == @intFromPtr(self);
}

pub fn stopQuarantineRetryWorker(self: *DB) void {
    self.quarantine_recovery.stop(self.backend_runtime);
}

/// A serviceable admission disposition does not by itself prove that a
/// physical generation is installed.
pub fn repairGenerationInstalled(
    self: *DB,
    intent: index_repair_state.IndexRepairIntent,
) bool {
    const cfg = self.core.index_manager.get(intent.index_name) orelse return false;
    if (types.indexConfigHash(cfg.*) != intent.config_hash) return false;
    return switch (intent.kind) {
        .full_text => self.core.index_manager.textIndexEntry(intent.index_name) != null,
        .dense_vector => self.core.index_manager.denseIndex(intent.index_name) != null,
        .sparse_vector => self.core.index_manager.sparseIndex(intent.index_name) != null,
        .graph => self.core.index_manager.graphIndex(intent.index_name) != null,
        .algebraic => self.core.index_manager.algebraicIndex(intent.index_name) != null,
    };
}

pub fn applyDurableIndexRepairStats(
    self: *DB,
    alloc: Allocator,
    state: ?*const index_repair_state.State,
    repair_state_corrupt: bool,
    item: *types.DBIndexStats,
) !void {
    // Compatibility overlays can carry a retired intent. Replace the
    // complete durable lifecycle bundle, including the no-intent case.
    if (item.index_repair_last_error) |value| alloc.free(value);
    const defaults = types.DBIndexStats{ .name = item.name, .kind = item.kind };
    inline for (comptime std.meta.fieldNames(types.DBIndexStats)) |reflected_name| {
        if (comptime std.mem.startsWith(u8, reflected_name, "index_repair_") or
            std.mem.eql(u8, reflected_name, "index_lifecycle_work_class"))
        {
            @field(item, reflected_name) = @field(defaults, reflected_name);
        }
    }
    if (repair_state_corrupt) {
        item.index_lifecycle_work_class = .repair;
        item.index_repair_trigger = "corrupt_local_repair_state";
        item.index_repair_phase = "terminal";
        item.index_repair_wait_reason = "terminal";
        item.index_repair_status = .failed;
        item.index_repair_action_required = true;
        item.repair_degraded = true;
        return;
    }
    const durable = state orelse return;
    const i = durable.findIndex(item.name) orelse return;
    const intent = durable.entries.items[i].intent;
    item.index_repair_id = intent.repair_id;
    item.index_lifecycle_work_class = switch (intent.work_class) {
        .initial_build => .initial_build,
        .repair => .repair,
    };
    item.index_repair_trigger = @tagName(intent.trigger);
    item.index_repair_phase = @tagName(intent.phase);
    item.index_repair_automation = @tagName(intent.automation);
    item.index_repair_attempts = intent.attempt_count;
    item.index_repair_started_at_ms = intent.started_at_ms;
    item.index_repair_updated_at_ms = intent.updated_at_ms;
    item.index_repair_build_floor_sequence = intent.build_floor_sequence;
    item.index_repair_applied_sequence = intent.candidate_applied_sequence;
    item.index_repair_target_sequence = intent.target_sequence;
    item.index_repair_next_retry_at_ms = intent.next_retry_at_ms;
    item.index_repair_last_error = if (intent.last_error) |value| try alloc.dupe(u8, value) else null;
    item.index_repair_wait_reason = if (intent.automation == .paused)
        "paused"
    else if (intent.phase == .terminal)
        "terminal"
    else if (intent.next_retry_at_ms > self.indexRepairNowMs())
        "backoff"
    else if (intent.phase == .rolling_back)
        "rollback"
    else if (intent.phase == .waiting_for_convergence)
        "convergence"
    else
        "none";
    item.index_repair_action_required = indexRepairActionRequired(intent);
    item.index_repair_status = index_repair_status.summarize(
        true,
        item.index_repair_automation,
        item.index_repair_phase,
        item.index_repair_wait_reason,
        item.index_repair_action_required,
    );
    // Keep status available when either optional bounded proof cannot be
    // read; query admission independently retries it and remains closed.
    // A complete active generation makes managed-admission repair debt
    // redundant even before its scheduler quantum removes the intent.
    // Preserve degradation for merely queryable progressive generations:
    // their physical artifact count can still be advancing.
    const active_generation_complete =
        self.managedAdmissionGenerationIsServiceable(alloc, intent) catch false;
    // Use the same intent-scoped exception as query admission. An older
    // publication certificate can still match after damage discovery or
    // after admission hands off to a shadow build. It cannot override that
    // repair's closed gate: only an initial canonical build may establish
    // progressive serviceability without replacement validation.
    const active_generation_queryable =
        self.managedAdmissionGenerationIsQueryable(alloc, intent) catch false;
    const generation_installed = self.repairGenerationInstalled(intent);
    // The resident gate is version/incarnation fenced. Pair its decision
    // with physical presence so neither a missing slot nor a stale config
    // can manufacture serviceability.
    const resident_admission_open = !self.core.index_manager.repairUnavailable(intent.index_name) and
        self.core.index_manager.loadFailure(intent.index_name) == null and
        generation_installed;
    const admitted_serving_generation = indexRepairAdmission(intent) == .serviceable and
        resident_admission_open;
    // Query admission is monotonic for one managed incarnation. Once an
    // authoritative status sample proves the progressive generation safe,
    // clear the same resident gate used by search. A following producer
    // batch can temporarily move the artifact counter ahead of the HBC
    // publication boundary; that must not make status revoke a generation
    // which queries continue serving from its last published snapshot.
    if (active_generation_complete or active_generation_queryable) {
        self.core.index_manager.publishRepairServiceabilityProof(
            intent.index_name,
            repairAdmissionVersion(intent, intent.revision, intent.control_revision),
        );
    }
    item.index_repair_active_generation_serviceable = active_generation_complete or
        active_generation_queryable or
        admitted_serving_generation;
    if (intent.work_class == .initial_build) {
        // A new catalog definition over an existing corpus is normal
        // restartable build work. It remains gated until publication,
        // but is neither corruption nor degraded service.
        item.backfill_active = intent.phase != .terminal;
        item.repair_degraded = false;
    } else switch (intent.trigger) {
        .incomplete_bulk_publish,
        .root_generation_rebuild,
        .projection_generation_invalid,
        .operator_generation_validation,
        .catalog_admission,
        => item.repair_degraded = true,
        .artifact_coverage_mismatch, .artifact_counter_missing => {
            // Source-coverage proof is incomplete or false. Keep the
            // compact degraded state visible while queries fail closed.
            item.backfill_active = intent.phase != .terminal;
            item.repair_degraded = true;
        },
        .replay_artifact_unavailable => {
            item.backfill_active = intent.phase != .terminal;
            item.repair_degraded = true;
        },
        .operator_generation_rebuild, .storage_format_migration, .artifact_baseline_adoption => {
            // The prior generation remains the serving generation during
            // construction. Report background rebuild activity without
            // falsely degrading a healthy index; activation/validation or
            // a terminal replacement failure remains visible as degraded.
            item.backfill_active = intent.phase != .terminal;
            item.repair_degraded = intent.phase == .activating or
                intent.phase == .validating or
                intent.phase == .rolling_back or
                intent.phase == .terminal;
        },
    }
    if (active_generation_complete) item.repair_degraded = false;
}

pub fn loadIndexRepairStateForStats(self: *const DB, alloc: Allocator) !?index_repair_state.State {
    if (comptime builtin.os.tag == .freestanding) {
        return null;
    } else {
        return self.loadIndexRepairState(alloc) catch |err| switch (err) {
            error.FileNotFound, error.DurableIndexRepairStateUnavailable, error.InvalidIndexRepairState => null,
            else => return err,
        };
    }
}

/// Queries that explicitly reference an index whose persisted artifacts
/// failed to load get a distinct error instead of IndexNotFound, so
/// clients can tell "broken, drop+recreate to recover" from "missing".
pub fn failIfIndexQuarantined(self: *DB, index_name: ?[]const u8) !void {
    const name = index_name orelse return;
    switch (self.observeResidentIndexAdmission(self.alloc, name, null)) {
        .admitted => {},
        .rebuilding => return error.IndexRebuilding,
        .unavailable => return error.IndexUnavailable,
    }
}

pub fn ensureRepairActivationDeadline(deadline_ns: u64) !void {
    if (monotonicTimeNs() >= deadline_ns) return error.RepairActivationBudgetExhausted;
}

test "db repair capacity treats unsupported observation as accounting only" {
    const Unsupported = struct {
        fn observe(_: *anyopaque) anyerror!resource_manager_mod.CapacityObservation {
            return error.UnsupportedPlatform;
        }
    };
    var context: u8 = 0;
    const observed = try DB.repairCapacityObservation(.{
        .capacity_source = .{
            .ptr = &context,
            .domain_id = 17,
            .observe = Unsupported.observe,
        },
    });
    try std.testing.expectEqual(@as(?u64, null), observed.available_bytes);
    try std.testing.expectEqual(@as(?u64, null), observed.capacity_bytes);
}

test "db repair completion cannot clear debt behind terminal coverage" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic");
    defer alloc.free(artifact_key);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .embedding,
        .index_name = try alloc.dupe(u8, "semantic"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "semantic"),
        .artifact_key = artifact_key_hex,
        .reason = .enrichment_failed,
        .sequence = 7,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const pending = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    const revision = DB.ArtifactRepairIssueRevision.capture(pending[0]);
    const completion_key = try db.artifactRepairCompletionKeyForIssueAlloc(alloc, pending[0]);
    defer alloc.free(completion_key);
    const completion = try db.artifactRepairCompletionSnapshot(alloc, completion_key);

    const generation = db.core.index_manager.coverageGenerationForIndex("semantic") orelse return error.TestUnexpectedResult;
    const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "semantic", generation, "doc:a");
    defer alloc.free(marker_key);
    try db.core.store.put(marker_key, "terminal_failed");
    try std.testing.expectEqual(
        DB.ArtifactRepairCompletionDisposition.stale,
        try db.completeArtifactRepairIssueIfCurrent(
            alloc,
            pending[0],
            revision,
            completion_key,
            completion.epoch +% 1,
            false,
        ),
    );
    try std.testing.expectEqual(
        DB.ArtifactRepairCompletionDisposition.coverage_incomplete,
        try db.completeArtifactRepairIssueIfCurrent(
            alloc,
            pending[0],
            revision,
            completion_key,
            completion.epoch,
            false,
        ),
    );

    try db.core.store.put(marker_key, "produced");
    try std.testing.expectEqual(
        DB.ArtifactRepairCompletionDisposition.completed,
        try db.completeArtifactRepairIssueIfCurrent(
            alloc,
            pending[0],
            revision,
            completion_key,
            completion.epoch,
            false,
        ),
    );
    const remaining = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);

    // The replay publication path owns ledger retirement. A request resuming
    // after that boundary must observe idempotent success rather than report
    // the repair it initiated as unresolved.
    try std.testing.expectEqual(
        DB.ArtifactRepairCompletionDisposition.completed,
        try db.completeArtifactRepairIssueIfCurrent(
            alloc,
            pending[0],
            revision,
            completion_key,
            completion.epoch,
            false,
        ),
    );
    try std.testing.expect(try db.clearArtifactRepairIssueIfCurrent(alloc, pending[0], revision));
}

test "db index repair requires explicit index and force for healthy rebuild" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

    try std.testing.expectError(error.InvalidArgument, db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .limit = 1,
    }));

    var skipped = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
    });
    defer skipped.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), skipped.scanned);
    try std.testing.expectEqual(@as(u64, 0), skipped.indexes_rebuilt);
    try std.testing.expect(!skipped.debt_remaining);

    var forced = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    });
    defer forced.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), forced.scanned);
    try std.testing.expectEqual(@as(u64, 1), forced.indexes_rebuilt);
    try std.testing.expect(!forced.debt_remaining);

    try std.testing.expectError(error.InvalidArgument, db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .cursor = "unexpected-cursor",
    }));
}

test "db index repair targets one graph index per selected config" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_a", .kind = .graph, .config_json = "{}" });
    try db.addIndex(.{ .name = "graph_b", .kind = .graph, .config_json = "{}" });

    const key_a = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_a", "mentions", "doc:b");
    defer alloc.free(key_a);
    const value_a = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, db.core.index_manager.graphIndex("graph_a").?.config.coverage_generation, 0.7, 0, 0, "");
    defer alloc.free(value_a);
    const key_b = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_b", "mentions", "doc:c");
    defer alloc.free(key_b);
    const value_b = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, db.core.index_manager.graphIndex("graph_b").?.config.coverage_generation, 0.9, 0, 0, "");
    defer alloc.free(value_b);
    try db.core.store.putBatch(&.{
        .{ .key = key_a, .value = value_a },
        .{ .key = key_b, .value = value_b },
    }, &.{});
    const graph_entry = db.core.index_manager.graphIndex("graph_a") orelse return error.TestUnexpectedResult;
    try graph_entry.index.batchApply(&.{.{
        .source = "doc:stale-source",
        .target = "doc:stale",
        .edge_type = "mentions",
        .weight = 1.0,
    }}, &.{});

    {
        const before_edges = try graph_entry.index.getEdges(alloc, "doc:stale-source", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, before_edges);
        try std.testing.expectEqual(@as(usize, 1), before_edges.len);
    }

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_a",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);

    const repaired_graph_entry = db.core.index_manager.graphIndex("graph_a") orelse return error.TestUnexpectedResult;
    const raw_edges_a = try repaired_graph_entry.index.getEdges(alloc, "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, raw_edges_a);
    try std.testing.expectEqual(@as(usize, 1), raw_edges_a.len);
    try std.testing.expectEqualStrings("doc:b", raw_edges_a[0].target);
    const raw_stale_edges = try repaired_graph_entry.index.getEdges(alloc, "doc:stale-source", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, raw_stale_edges);
    try std.testing.expectEqual(@as(usize, 0), raw_stale_edges.len);

    const edges_a = try db.getEdges(alloc, "graph_a", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges_a);
    try std.testing.expectEqual(@as(usize, 1), edges_a.len);
    try std.testing.expectEqualStrings("doc:b", edges_a[0].target);

    const edges_b = try db.getEdges(alloc, "graph_b", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges_b);
    try std.testing.expectEqual(@as(usize, 0), edges_b.len);
}

test "db index repair resets sparse index before rebuilding from artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    const key_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(key_a);
    const value_a = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, null, &.{1}, &.{1.0});
    defer alloc.free(value_a);
    const key_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "sp_v1");
    defer alloc.free(key_b);
    const value_b = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, null, &.{2}, &.{1.0});
    defer alloc.free(value_b);
    try db.core.store.putBatch(&.{
        .{ .key = key_a, .value = value_a },
        .{ .key = key_b, .value = value_b },
    }, &.{});

    const sparse_entry = db.core.index_manager.sparseIndex("sp_v1") orelse return error.TestUnexpectedResult;
    try sparse_entry.index.batch(&.{
        .{ .doc_id = "doc:a", .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc:b", .vec = .{ .indices = &.{2}, .values = &.{1.0} } },
        .{ .doc_id = "doc:stale", .vec = .{ .indices = &.{3}, .values = &.{1.0} } },
    }, &.{});
    try std.testing.expect((try sparse_entry.index.debugDocNumForDocId("doc:stale")) != null);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "sp_v1",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 2), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);

    const repaired_entry = db.core.index_manager.sparseIndex("sp_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expect((try repaired_entry.index.debugDocNumForDocId("doc:a")) != null);
    try std.testing.expect((try repaired_entry.index.debugDocNumForDocId("doc:b")) != null);
    try std.testing.expectEqual(@as(?u32, null), try repaired_entry.index.debugDocNumForDocId("doc:stale"));
}

test "db index repair rebuilds full text index from stored documents" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const text_cfg: types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    try db.addIndex(text_cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha winner\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta winner\"}" },
        },
        .sync_level = .full_index,
    });

    var before = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer before.deinit();
    try std.testing.expectEqual(@as(u32, 1), before.total_hits);

    try db.core.saveProjectionCheckpoint("ft_v1", .{
        .applied_sequence = 0,
        .status = .repair_required,
        .config_hash = types.indexConfigHash(text_cfg),
    });

    const page = try db.listArtifactRepairIssuesPage(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_v1",
        .limit = 1,
    });
    defer {
        types.freeArtifactRepairIssues(alloc, page.issues);
        if (page.next_cursor) |cursor| alloc.free(cursor);
    }
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expect(page.issues[0].repairable);
    try std.testing.expectEqual(types.ArtifactRepairKind.full_text, page.issues[0].artifact_kind);
    try std.testing.expectEqualStrings("ft_v1", page.issues[0].index_name);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 2), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);

    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "ft_v1");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(types.indexConfigHash(text_cfg), checkpoint.config_hash);

    var after = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer after.deinit();
    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db index repair replacement loads named artifact producer dependencies" {
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
    try db.addEnrichment(.{
        .name = "body_chunks",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
    });
    for ([_]types.IndexKind{ .dense_vector, .sparse_vector }) |kind| {
        const dense = kind == .dense_vector;
        const source_name = if (dense) "dense_source" else "sparse_source";
        try db.addEnrichment(.{
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
        const cfg = types.IndexConfig{
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
        var shadow = try index_manager_mod.IndexManager.initWithOptions(alloc, shadow_path, db.index_backends);
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

test "db index repair shadow swap survives reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const text_cfg: types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(text_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha durable\"}" },
                .{ .key = "doc:b", .value = "{\"body\":\"beta durable\"}" },
            },
            .sync_level = .full_index,
        });
        try db.core.saveProjectionCheckpoint("ft_v1", .{
            .applied_sequence = 0,
            .status = .repair_required,
            .config_hash = types.indexConfigHash(text_cfg),
        });

        const stale_canonical_file = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/stale-before-repair", .{std.mem.span(path)});
        defer alloc.free(stale_canonical_file);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{
            .sub_path = stale_canonical_file,
            .data = "stale",
        });

        var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .full_text,
            .index_name = "ft_v1",
            .limit = 1,
        });
        defer repair.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    }

    const abandoned_shadow = try std.fmt.allocPrint(alloc, "{s}/.repair-shadow-abandoned/indexes/ft_v1", .{std.mem.span(path)});
    defer alloc.free(abandoned_shadow);
    try ensureDirPath(abandoned_shadow);
    const in_progress_shadow_root = try std.fmt.allocPrint(alloc, "{s}/.repair-shadow-live-build", .{std.mem.span(path)});
    defer alloc.free(in_progress_shadow_root);
    const in_progress_shadow = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1", .{in_progress_shadow_root});
    defer alloc.free(in_progress_shadow);
    try ensureDirPath(in_progress_shadow);
    try index_manager_mod.IndexManager.writeRepairShadowInProgressMarker(alloc, in_progress_shadow_root);

    {
        var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
        defer reopened.close();

        const checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "ft_v1");
        try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);

        var after = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        });
        defer after.deinit();
        try std.testing.expectEqual(@as(u32, 1), after.total_hits);
        try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
        reopened.backend_runtime.durable_jobs.drainOwner(reopened.repair_cleanup_owner_id);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, abandoned_shadow, .{}));
        try std.Io.Dir.cwd().access(std.testing.io, in_progress_shadow, .{});
        const stale_canonical_file = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/stale-before-repair", .{std.mem.span(path)});
        defer alloc.free(stale_canonical_file);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, stale_canonical_file, .{}));
    }
}

test "db index repair shadow roots are allocated uniquely" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const first = try createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(first);
    const second = try createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(second);
    const third = try createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(third);

    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expect(!std.mem.eql(u8, first, third));
    try std.testing.expect(!std.mem.eql(u8, second, third));
    try std.testing.expect(std.mem.indexOf(u8, first, "/.repair-shadow-") != null);
    try std.testing.expect(std.mem.indexOf(u8, second, "/.repair-shadow-") != null);
    try std.testing.expect(std.mem.indexOf(u8, third, "/.repair-shadow-") != null);

    try std.Io.Dir.cwd().access(std.testing.io, first, .{});
    try std.Io.Dir.cwd().access(std.testing.io, second, .{});
    try std.Io.Dir.cwd().access(std.testing.io, third, .{});
}

test "db index repair shadow swap preserves post snapshot mutations" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const text_cfg: types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(text_cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha before\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta before\"}" },
        },
        .sync_level = .full_index,
    });
    try db.core.saveProjectionCheckpoint("ft_v1", .{
        .applied_sequence = 0,
        .status = .repair_required,
        .config_hash = types.indexConfigHash(text_cfg),
    });

    const HookContext = struct {
        fired: bool = false,
        clean_checkpoint_fired: bool = false,
        observed_floor: u64 = 0,
    };
    const Hook = struct {
        fn afterSnapshotBuild(ptr: *anyopaque, hook_db: *DB, index_name: []const u8, build_floor_sequence: u64) anyerror!void {
            const ctx: *HookContext = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("ft_v1", index_name);
            try std.testing.expect(build_floor_sequence > 0);
            try std.testing.expect(!ctx.fired);
            ctx.fired = true;
            ctx.observed_floor = build_floor_sequence;

            try hook_db.batch(.{
                .writes = &.{
                    .{ .key = "doc:b", .value = "{\"body\":\"gamma after\"}" },
                    .{ .key = "doc:c", .value = "{\"body\":\"alpha after\"}" },
                },
                .deletes = &.{"doc:a"},
                .sync_level = .write,
            });
        }

        fn afterCleanCheckpoint(ptr: *anyopaque, hook_db: *DB, index_name: []const u8) anyerror!void {
            const ctx: *HookContext = @ptrCast(@alignCast(ptr));
            try std.testing.expect(!ctx.clean_checkpoint_fired);
            ctx.clean_checkpoint_fired = true;
            const durable_applied = try hook_db.core.loadAppliedSequence(hook_db.alloc, index_name);
            const worker_applied = hook_db.executor.appliedSequence(index_name) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(durable_applied, worker_applied);
        }
    };
    var hook_ctx = HookContext{};
    db.shadow_index_repair_hook = .{
        .ptr = &hook_ctx,
        .after_snapshot_build = Hook.afterSnapshotBuild,
        .after_clean_checkpoint = Hook.afterCleanCheckpoint,
    };
    defer db.shadow_index_repair_hook = null;

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expect(hook_ctx.fired);
    try std.testing.expect(hook_ctx.clean_checkpoint_fired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "ft_v1");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expect(checkpoint.applied_sequence > hook_ctx.observed_floor);

    var alpha = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer alpha.deinit();
    try std.testing.expectEqual(@as(u32, 1), alpha.total_hits);
    try std.testing.expectEqualStrings("doc:c", alpha.hits[0].id);

    var beta = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "beta" } },
    });
    defer beta.deinit();
    try std.testing.expectEqual(@as(u32, 0), beta.total_hits);

    var gamma = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "gamma" } },
    });
    defer gamma.deinit();
    try std.testing.expectEqual(@as(u32, 1), gamma.total_hits);
    try std.testing.expectEqualStrings("doc:b", gamma.hits[0].id);
}

test "db repair issue list exposes algebraic generation debt as repairable" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const cfg: types.IndexConfig = .{
        .name = "alg_v1",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "table": "docs",
        \\  "schema_version": 1,
        \\  "capability_fingerprint": "test-capability"
        \\}
        ,
    };
    try db.addIndex(cfg);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"category\":\"tools\"}" }},
        .sync_level = .full_index,
    });
    try db.core.saveProjectionCheckpoint("alg_v1", .{
        .applied_sequence = 0,
        .status = .repair_required,
        .config_hash = types.indexConfigHash(cfg),
    });

    var page = try db.listArtifactRepairIssuesPage(alloc, .{
        .target = .index,
        .artifact_kind = .algebraic,
        .index_name = "alg_v1",
        .limit = 1,
    });
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expectEqual(types.ArtifactRepairKind.algebraic, page.issues[0].artifact_kind);
    try std.testing.expect(page.issues[0].repairable);
    try std.testing.expectEqualStrings("", page.issues[0].unsupported_reason);
    try std.testing.expectEqualStrings("index_repair_required", page.issues[0].last_error);

    // This checks repairability and completion, not the production pause SLA.
    var repair = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .algebraic,
        .index_name = "alg_v1",
        .limit = 1,
    }, repair_completion_test_options);
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.unsupported);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.unresolved);
    try std.testing.expect(!repair.debt_remaining);

    while (try db.core.index_manager.cleanupInactiveRepairShadowRootsPage()) {}
    const canonical_prefix = try algebraic_mod.index.storagePrefixAlloc(alloc, "canonical/alg_v1");
    defer alloc.free(canonical_prefix);
    const canonical_rows = try db.core.store.scanPrefix(alloc, canonical_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, canonical_rows);
    try std.testing.expectEqual(@as(usize, 0), canonical_rows.len);
}

test "db index repair serializes duplicate repairs for one index" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });
    try std.testing.expect(try db.beginIndexRepairLease("graph_v1"));
    defer db.endIndexRepairLease("graph_v1");

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.in_progress);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_after);
}

test "db index repair recovers quarantined index load before rebuild" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }

    index_manager_mod.test_inject_index_open_error = error.FileNotFound;
    defer index_manager_mod.test_inject_index_open_error = null;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        const recorded = db.core.index_manager.loadFailure("ft_v1") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("FileNotFound", recorded);

        index_manager_mod.test_inject_index_open_error = null;
        var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .index_name = "ft_v1",
            .limit = 1,
        });
        defer repair.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), repair.scanned);
        try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_before);
        try std.testing.expectEqual(@as(u64, 0), repair.indexes_degraded_after);
        try std.testing.expectEqual(@as(u64, 1), repair.repaired);
        try std.testing.expectEqual(@as(u64, 0), repair.failed);
        try std.testing.expectEqual(@as(u64, 0), repair.unresolved);
        try std.testing.expect(!repair.debt_remaining);
        try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
        try std.testing.expect(db.core.textIndexEntry("ft_v1") != null);
    }
}

test "db index repair rebuilds full text after quarantined root recreation" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const path_slice = std.mem.span(path);

    const text_cfg: types.IndexConfig = .{
        .name = "ft_recreate",
        .kind = .full_text,
        .config_json = "{}",
    };

    {
        var db = try DB.open(alloc, path_slice, .{});
        defer db.close();

        try db.addIndex(text_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha durable\"}" },
                .{ .key = "doc:b", .value = "{\"body\":\"beta durable\"}" },
            },
            .sync_level = .full_index,
        });
        try db.core.saveProjectionCheckpoint("ft_recreate", .{
            .applied_sequence = db.core.nextDerivedSequence(),
            .status = .clean,
            .generation = 2,
            .config_hash = types.indexConfigHash(text_cfg),
        });
    }

    const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_recreate", .{path_slice});
    defer alloc.free(index_path);
    try std.testing.expect((try corruptNonEmptyFilesUnderDir(alloc, index_path)) > 0);

    var reopened = try DB.open(alloc, path_slice, no_replay_no_background_test_open);
    defer reopened.close();

    const recorded = reopened.core.index_manager.loadFailure("ft_recreate") orelse return error.TestUnexpectedResult;
    try std.testing.expect(recorded.len > 0);

    var repair = try reopened.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_recreate",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_before);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_degraded_after);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expect(!repair.debt_remaining);
    try std.testing.expect(reopened.core.index_manager.loadFailure("ft_recreate") == null);

    var after = try reopened.search(alloc, .{
        .index_name = "ft_recreate",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer after.deinit();
    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db index repair streams graph artifact rebuild in batches" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_stream", .kind = .graph, .config_json = "{}" });
    try db.addIndex(.{ .name = "graph_other", .kind = .graph, .config_json = "{}" });

    const total_edges = graph_repair_rebuild_batch_size + 3;
    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        writes.deinit(alloc);
    }
    for (0..total_edges) |i| {
        const source = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
        defer alloc.free(source);
        const target = try std.fmt.allocPrint(alloc, "target:{d:0>5}", .{i});
        defer alloc.free(target);
        const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, source, "graph_stream", "links", target);
        errdefer alloc.free(key);
        const value = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, db.core.index_manager.graphIndex("graph_stream").?.config.coverage_generation, 1.0, 0, 0, "");
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    {
        const other_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "other:a", "graph_other", "links", "other:b");
        errdefer alloc.free(other_key);
        const other_value = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, db.core.index_manager.graphIndex("graph_other").?.config.coverage_generation, 1.0, 0, 0, "");
        errdefer alloc.free(other_value);
        // After append the list owns both allocations, including on failure.
        try writes.append(alloc, .{ .key = other_key, .value = other_value });
    }
    try db.core.store.putBatch(writes.items, &.{});

    // The restore/split entry point uses the same bounded streaming path as
    // online repair; it must neither materialize the complete artifact keyspace
    // nor rescan it for every configured graph index.
    db_zig.test_graph_repair_stream_flushes.store(0, .monotonic);
    db_zig.test_graph_repair_stream_scans.store(0, .monotonic);
    try db.rebuildGraphIndexesForTargetCoverage(alloc);
    try std.testing.expectEqual(@as(u64, 2), db_zig.test_graph_repair_stream_flushes.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), db_zig.test_graph_repair_stream_scans.load(.monotonic));

    const other_edges = try db.getEdges(alloc, "graph_other", "other:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, other_edges);
    try std.testing.expectEqual(@as(usize, 1), other_edges.len);
    try std.testing.expectEqualStrings("other:b", other_edges[0].target);

    db_zig.test_graph_repair_stream_flushes.store(0, .monotonic);
    // Force an activation yield independently of host speed: one millisecond
    // cannot cover the five-millisecond publication reserve. The completed
    // snapshot work must still be reported and remain reusable after reopen.
    var repair = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_stream",
        .limit = 1,
        .force = true,
    }, .{ .max_activation_pause_ms = 1 });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, @intCast(total_edges)), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.in_progress);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expect(repair.debt_remaining);
    try std.testing.expectEqual(@as(u64, 2), db_zig.test_graph_repair_stream_flushes.load(.monotonic));

    const repair_id = (try db.indexRepairIdForIndex(alloc, "graph_stream")) orelse return error.TestUnexpectedResult;
    var pending = try db.loadIndexRepairEntryById(alloc, repair_id);
    defer pending.deinit(alloc);
    try std.testing.expectEqual(@as(u64, total_edges), pending.intent.build_reprocessed);
    try std.testing.expectEqual(@as(u32, 0), pending.intent.failure_streak);
    try std.testing.expectEqual(@as(u64, 0), pending.intent.next_retry_at_ms);
    try std.testing.expect(pending.intent.last_error == null);
    try std.testing.expect(pending.intent.candidate_relative_path != null);

    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{});
    db_zig.test_graph_repair_stream_flushes.store(0, .monotonic);
    const resumed = try db.advanceIndexRepairIntent(alloc, repair_id, repair_completion_test_options);
    try std.testing.expect(resumed.repaired);
    try std.testing.expectEqual(@as(u64, 1), resumed.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 0), resumed.documents_reprocessed);
    try std.testing.expectEqual(@as(u64, 0), db_zig.test_graph_repair_stream_flushes.load(.monotonic));
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
    try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("graph_stream", pending.intent.candidate_relative_path.?));

    const last_source = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{total_edges - 1});
    defer alloc.free(last_source);
    const last_target = try std.fmt.allocPrint(alloc, "target:{d:0>5}", .{total_edges - 1});
    defer alloc.free(last_target);
    const edges = try db.getEdges(alloc, "graph_stream", last_source, "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings(last_target, edges[0].target);

    // Reuse the large artifact fixture to verify that split retirement stays
    // page bounded even when every scanned row is a graph artifact.
    db_zig.test_graph_split_retire_flushes.store(0, .monotonic);
    const synthetic_split: shard_mod.SplitState = .{
        .phase = .prepare,
        .split_key = "doc:",
        .new_shard_id = 2,
        .started_at = 1,
        .original_range_end = "doc;",
    };
    db_zig.test_graph_split_retire_abort_after_flush.store(true, .monotonic);
    defer db_zig.test_graph_split_retire_abort_after_flush.store(false, .monotonic);
    try std.testing.expectError(error.TestInjectedBackfillFailure, retireSplitEntitySourcedEdgesLocked(&db, synthetic_split));
    try std.testing.expectEqual(@as(u64, 1), db_zig.test_graph_split_retire_flushes.load(.monotonic));
    try retireSplitEntitySourcedEdgesLocked(&db, synthetic_split);
    try std.testing.expectEqual(@as(u64, 2), db_zig.test_graph_split_retire_flushes.load(.monotonic));
    const final_cursor = try db.core.store.get(alloc, graph_split_retire_cursor_key);
    defer alloc.free(final_cursor);
    try std.testing.expect(final_cursor.len > 0);
}

test "db repair queue reprocesses corrupt generated dense embedding artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const opts = OpenOptions{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "repair-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), opts);
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_v1\"}}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"repair target text\"}" }},
            .sync_level = .full_index,
        });

        const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &.{},
                },
            },
        };
        appended_sequence = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), opts);
    defer reopened.close();

    {
        const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
        defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
        try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
        try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
        try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    }

    const repair = try reopened.repairEmbeddingArtifactIssues(alloc, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);

    const issues_after = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(dense_applied >= appended_sequence);

    const artifact_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    // Forced repair advances the generation so cached corrupt work cannot be reused.
    try expectDenseEmbeddingArtifactValue(alloc, artifact_value, enrichment_artifact_codec.hashEmbeddingSourceForGeneration("repair target text", "", 1), 3);
}

test "db repair queue reprocesses corrupt chunk generated dense embedding artifacts from parent document" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const opts = OpenOptions{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "repair-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), opts);
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" }},
            .sync_level = .full_index,
        });

        const chunk_key = try expectedChunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
        defer alloc.free(chunk_key);
        const artifact_key = try expectedChunkEmbeddingArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0, "chunk_dense_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = chunk_key,
                    .artifact_key = artifact_key,
                    .vector = &.{},
                },
            },
        };
        appended_sequence = try appendDerivedBatchRecord(&db, derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), opts);
    defer reopened.close();

    {
        const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
        defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
        try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
        try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
        try std.testing.expectEqualStrings("doc:a", issues[0].parent_doc_key);
        try std.testing.expectEqualStrings("body_chunks_v1", issues[0].source_artifact_name);
        try std.testing.expectEqual(@as(?u32, 0), issues[0].chunk_id);
        try std.testing.expectEqualStrings("chunk_dense_v1", issues[0].artifact_name);
    }

    const repair = try reopened.repairEmbeddingArtifactIssues(alloc, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);

    const issues_after = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(dense_applied >= appended_sequence);

    const artifact_key = try expectedChunkEmbeddingArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0, "chunk_dense_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    // Forced repair advances the generation so cached corrupt work cannot be reused.
    try expectDenseEmbeddingArtifactValue(alloc, artifact_value, enrichment_artifact_codec.hashEmbeddingSourceForGeneration("abcdefgh", "", 1), 3);
}

test "db repair activation budget yields without failure backoff and resumes its candidate" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("repair-activation-yield");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), no_background_test_open);
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const repair_id = (try db.admitManagedIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    })) orelse return error.TestUnexpectedResult;
    var yielded = false;
    for (0..8) |_| {
        // One millisecond cannot satisfy the five-millisecond publication
        // reserve, independently of host speed. No wall-clock sleep is needed
        // to force the production activation-budget path.
        const step = try db.advanceIndexRepairIntent(alloc, repair_id, .{ .max_activation_pause_ms = 1 });
        try std.testing.expect(!step.repaired and !step.terminal);
        var entry = try db.loadIndexRepairEntryById(alloc, repair_id);
        defer entry.deinit(alloc);
        try std.testing.expectEqual(@as(u32, 0), entry.intent.failure_streak);
        try std.testing.expectEqual(@as(u64, 0), entry.intent.next_retry_at_ms);
        try std.testing.expect(entry.intent.last_error == null);
        if (step.busy) {
            yielded = true;
            break;
        }
    }
    try std.testing.expect(yielded);
    var pending = try db.loadIndexRepairEntryById(alloc, repair_id);
    defer pending.deinit(alloc);
    try std.testing.expect(pending.intent.candidate_relative_path != null);
    const resumed = try db.advanceIndexRepairIntent(alloc, repair_id, .{ .max_activation_pause_ms = 5_000 });
    try std.testing.expect(resumed.attempted and resumed.repaired);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
    try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("full_text_index_v0", pending.intent.candidate_relative_path.?));
    var result = try db.search(alloc, .{ .index_name = "full_text_index_v0", .query = .{ .match = .{ .field = "_all", .text = "alpha" } }, .limit = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db repair activation admission is time and sequence bounded" {
    try std.testing.expect(DB.repairActivationAdmissible(0, 0, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(!DB.repairActivationAdmissible(201, std.time.ns_per_ms, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(!DB.repairActivationAdmissible(1, 0, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(DB.repairActivationAdmissible(100, std.time.ns_per_ms, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(!DB.repairActivationAdmissible(100, 3 * std.time.ns_per_ms, 200, 250 * std.time.ns_per_ms));

    try std.testing.expectEqual(@as(u64, 10), DB.observeRepairCatchUpCost(0, 10, 20, 100));
    try std.testing.expectEqual(@as(u64, 12), DB.observeRepairCatchUpCost(12, 20, 30, 100));
    try std.testing.expectEqual(@as(u64, 12), DB.observeRepairCatchUpCost(12, 30, 30, 999));

    const deadline = 1_000 * std.time.ns_per_ms;
    try std.testing.expectEqual(
        @as(?u64, deadline - 50 * std.time.ns_per_ms),
        DB.repairActivationReplayDeadline(700 * std.time.ns_per_ms, deadline, 250 * std.time.ns_per_ms),
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        DB.repairActivationReplayDeadline(998 * std.time.ns_per_ms, deadline, 10 * std.time.ns_per_ms),
    );
}

test "db index repair rebuilds dense index with missing native publication" {
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
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    const dense_index_path_z = try alloc.dupeSentinel(u8, dense_index_path, 0);
    defer alloc.free(dense_index_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_index_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        try hbc.markPublishedGenerationIncompleteForTest();
        hbc.close();
    }

    var reopened = try openLegacyDenseTestDB(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const recorded = reopened.core.index_manager.loadFailure("dense_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("IncompleteBulkPublish", recorded);
    const discovery = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    try std.testing.expectEqual(@as(usize, 1), discovery.discovered);
    try std.testing.expect(try reopened.hasPendingIndexRepairIntents(alloc));
    const duplicate_discovery = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    try std.testing.expectEqual(@as(usize, 0), duplicate_discovery.discovered);
    try std.testing.expectEqual(@as(usize, 0), duplicate_discovery.already_pending);
    try std.testing.expect(duplicate_discovery.automatic_sweep_complete);
    var discovered_state = try reopened.loadIndexRepairState(alloc);
    const repair_id = discovered_state.entries.items[0].intent.repair_id;
    try std.testing.expectEqual(index_repair_state.Phase.detected, discovered_state.entries.items[0].intent.phase);
    try std.testing.expect(discovered_state.entries.items[0].pin == null);
    discovered_state.deinit(alloc);

    var pinned_snapshot = try reopened.beginPinnedIndexRepairSnapshot(alloc, repair_id);
    const pinned_floor = pinned_snapshot.build_floor_sequence;
    pinned_snapshot.deinit();
    var pinned_state = try reopened.loadIndexRepairState(alloc);
    try std.testing.expectEqual(@as(?u64, pinned_floor), pinned_state.minimumRetainAfterSequence());
    pinned_state.deinit(alloc);

    // Discovery and pin acquisition must not delete or reopen the poisoned
    // root merely to make rebuilding convenient.
    try std.testing.expectError(
        error.IncompleteBulkPublish,
        hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_index_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{}),
    );
    {
        const status_key = try DB.indexStatusKeyAlloc(alloc, "dense_idx");
        defer alloc.free(status_key);
        var stale_status: [DB.index_status_encoded_len]u8 = undefined;
        DB.encodeIndexStatusSnapshot(.{
            .kind = .dense_vector,
            .doc_count = 99,
            .node_count = 99,
            .root_node = 99,
        }, &stale_status);
        try reopened.core.store.put(status_key, &stale_status);
    }
    {
        const stats = try reopened.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
    }

    const OwnershipFence = struct {
        lost: bool = false,

        fn current(ptr: *anyopaque) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return !self.lost;
        }

        fn afterSnapshot(ptr: *anyopaque, _: *DB, _: []const u8, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lost = true;
        }
    };
    var ownership_fence = OwnershipFence{};
    reopened.shadow_index_repair_hook = .{
        .ptr = &ownership_fence,
        .after_snapshot_build = OwnershipFence.afterSnapshot,
    };
    const ownership_lost = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{
        .owner_epoch = 17,
        .activation_check = .{
            .ptr = &ownership_fence,
            .is_current_owner = OwnershipFence.current,
        },
    });
    reopened.shadow_index_repair_hook = null;
    try std.testing.expect(ownership_lost.attempted);
    try std.testing.expect(ownership_lost.deferred);
    try std.testing.expect(!ownership_lost.repaired);
    try std.testing.expect(try reopened.hasPendingIndexRepairIntents(alloc));
    var reserved_state = try reopened.loadIndexRepairState(alloc);
    try std.testing.expect(reserved_state.entries.items[0].intent.estimated_candidate_bytes > 0);
    try std.testing.expectEqual(@as(u64, 17), reserved_state.entries.items[0].intent.owner_epoch);
    try std.testing.expect(
        reserved_state.entries.items[0].intent.planned_disk_bytes >=
            reserved_state.entries.items[0].intent.estimated_candidate_bytes,
    );
    reserved_state.deinit(alloc);
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
    try std.testing.expectError(
        error.StaleIndexRepairControl,
        reopened.pauseAutomaticIndexRepair(alloc, "dense_idx", repair_id + 1),
    );
    try std.testing.expect(try reopened.resumeAutomaticIndexRepair(alloc, "dense_idx", null));

    const repair = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(repair.attempted);
    try std.testing.expect(repair.repaired);
    // The first attempt reached a durable candidate before ownership loss;
    // retry reopens and catches up that generation instead of rescanning the
    // primary corpus.
    try std.testing.expectEqual(@as(u64, 0), repair.documents_reprocessed);
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    var completed_state = try reopened.loadIndexRepairState(alloc);
    defer completed_state.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), completed_state.entries.items.len);
    {
        const persisted_status = (try reopened.loadIndexStatusSnapshot(alloc, "dense_idx")).?;
        try std.testing.expectEqual(@as(u64, 2), persisted_status.doc_count);
    }

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);

    // The adopted shadow handle must be a normal active runtime, including
    // loader callbacks and subsequent foreground apply; it must not retain a
    // dependency on the destroyed shadow manager.
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" }},
        .sync_level = .full_index,
    });
    var after_adoption = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 1,
        } },
        .limit = 1,
    });
    defer after_adoption.deinit();
    try std.testing.expectEqual(@as(u32, 1), after_adoption.total_hits);
    try std.testing.expectEqualStrings("doc:c", after_adoption.hits[0].id);
}

test "db repair preflight retains a canonical generation completed after scheduler selection" {
    try testManagedGenerationRepairAdmission(.late_completion);
}

test "db repair capacity converts materialized shadow bytes into consumed reservation" {
    const alloc = std.testing.allocator;
    var db_path_tmp = try TestDirectory.init("db");
    defer db_path_tmp.cleanup();
    const db_path = db_path_tmp.path().ptr;
    var candidate_path_tmp = try TestDirectory.init("db");
    defer candidate_path_tmp.cleanup();
    const candidate_path = candidate_path_tmp.path().ptr;
    try ensureDirPath(std.mem.span(candidate_path));

    var db = try DB.open(alloc, std.mem.span(db_path), no_background_test_open);
    defer db.close();

    const Capacity = struct {
        available: u64,

        fn observe(ptr: *anyopaque) anyerror!resource_manager_mod.CapacityObservation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .available_bytes = self.available,
                .capacity_bytes = 100,
                .observed_at_ns = monotonicTimeNs(),
                .valid_for_ns = 5 * std.time.ns_per_s,
            };
        }
    };
    var capacity = Capacity{ .available = 100 };
    var manager = resource_manager_mod.ResourceManager.init(.{
        .disk_safety_floor_bytes = 0,
        .disk_safety_floor_divisor = 0,
    });
    defer manager.deinit(alloc);
    var reservation = try manager.reserveCapacity(alloc, 9, 80, try Capacity.observe(&capacity), monotonicTimeNs());
    defer reservation.release();
    var guard = DB.RepairCapacityGuard{
        .reservation = &reservation,
        .db = &db,
        .alloc = alloc,
        .repair_id = 0,
        .options = .{ .capacity_source = .{ .ptr = &capacity, .domain_id = 9, .observe = Capacity.observe } },
        .admitted_total_bytes = 80,
        .headroom_bytes = 0,
    };
    try DB.RepairCapacityGuard.bindCandidateRoot(&guard, std.mem.span(candidate_path));

    const file_path = try std.fmt.allocPrint(alloc, "{s}/materialized", .{std.mem.span(candidate_path)});
    defer alloc.free(file_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = file_path,
        .data = "0123456789012345678901234567890123456789",
    });
    capacity.available = 60;
    try DB.RepairCapacityGuard.revalidate(&guard);
    try std.testing.expectEqual(@as(u64, 40), manager.capacityStats().reserved_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.capacityStats().denials);
}

test "db repair replay pin applies hard-pressure write backpressure" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lsm_wal_retention)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1,
    };
    var resources = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .resource_manager = &resources,
    });
    defer db.close();

    var tracked: u64 = 0;
    resources.observeUsage(.lsm_wal_retention, &tracked, 2);
    defer resources.observeUsage(.lsm_wal_retention, &tracked, 0);
    db.async_context.index_repair_replay_pinned.store(true, .release);
    try std.testing.expectError(error.DenseRepairBackpressure, db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    }));

    db.async_context.index_repair_replay_pinned.store(false, .release);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
}
