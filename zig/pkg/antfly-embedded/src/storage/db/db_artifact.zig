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

//! Artifact operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDb = db_zig.TestDb;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const appendUniqueOwnedKey = @import("owned_keys.zig").appendUniqueOwnedKey;
const artifact_asset_publication = @import("artifact/asset_publication.zig");
const artifact_authored_acceptance = @import("artifact/authored_acceptance.zig");
const artifact_chunk_cursor = @import("artifact/chunk_cursor.zig");
const artifact_chunk_generation = @import("artifact/chunk_generation.zig");
const artifact_chunk_inventory_progress = @import("artifact/chunk_inventory_progress.zig");
const artifact_chunk_manifest = @import("artifact/chunk_manifest.zig");
const artifact_chunk_scan_position = @import("artifact/chunk_scan_position.zig");
const artifact_chunk_vector_cursor = @import("artifact/chunk_vector_cursor.zig");
const artifact_completion_progress = @import("artifact/completion_progress.zig");
const artifact_footprint_mod = @import("../artifact_footprint.zig");
const artifact_ids = @import("artifact/ids.zig");
const artifact_inventory = @import("artifact/inventory.zig");
const artifact_native_stream = @import("artifact/native_stream.zig");
const artifact_producer_baseline = @import("artifact/producer_baseline.zig");
const artifact_producer_obligations = @import("artifact/producer_obligations.zig");
const artifact_producer_provenance = @import("artifact/producer_provenance.zig");
const artifact_producer_readiness = @import("artifact/producer_readiness.zig");
const artifact_producer_scheduler_mod = @import("artifact/producer_scheduler.zig");
const artifact_producer_validation = @import("artifact/producer_validation.zig");
const artifact_projection_epoch = @import("artifact/projection_epoch.zig");
const artifact_publication = @import("artifact/publication.zig");
const artifact_publication_transport = @import("artifact/publication_transport.zig");
const artifact_publication_transport_codec = @import("artifact/publication_transport_codec.zig");
const artifact_reconcile = @import("artifact/reconcile.zig");
const artifact_reconcile_intent = @import("artifact/reconcile_intent.zig");
const artifact_stream_checkpoint = @import("artifact/stream_checkpoint.zig");
const artifact_stream_progress = @import("artifact/stream_progress.zig");
const artifact_unit_jobs = @import("artifact/unit_jobs.zig");
const artifact_unit_progress = @import("artifact/unit_progress.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const backfill_state_mod = @import("backfill_state.zig");
const background_runtime_mod = @import("../background_runtime.zig");
const builtin = @import("builtin");
const cleanup_job_owner = @import("cleanup_job_owner.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const graph_cleanup_contract_mod = @import("../graph_cleanup_contract.zig");
const graph_edge_contender = @import("graph_edge_contender.zig");
const graph_edge_ttl_tombstone = @import("graph_edge_ttl_tombstone.zig");
const hierarchy_navigation = @import("../hierarchy_navigation.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document/mapper.zig");
const merge_page_contract = @import("merge_page_contract.zig");
const merge_state_mod = @import("merge_state.zig");
const online_integrity_shadow = @import("online_integrity_shadow.zig");
const online_source = @import("online_source.zig");
const online_source_contract = @import("online_source_contract.zig");
const platform = @import("antfly_platform");
const range_protection_mod = @import("../range_protection.zig");
const read_projection = @import("read_projection.zig");
const relational_integrity_handoff = @import("relational_integrity_handoff.zig");
const relational_integrity_topology = @import("relational_integrity_topology.zig");
const replication_effects_mod = @import("replication_effects.zig");
const resolver_catalog_mod = @import("catalog/resolver_catalog.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const restore_staging = @import("restore_staging.zig");
const retained_effects_mod = @import("../retained_effects.zig");
const source_artifact_transfer = @import("source_artifact_transfer.zig");
const source_authority_mod = @import("../source_authority.zig");
const std = @import("std");
const test_index = @import("test_support.zig").test_index;
const storage_engine = @import("../engine.zig");
const types = @import("types.zig");
const Allocator = db_zig.Allocator;
const ArtifactRepairCompletionState = db_zig.ArtifactRepairCompletionState;
const AsyncContext = db_zig.AsyncContext;
const BatchExecutionOptions = db_zig.BatchExecutionOptions;
const CountingDenseEmbedder = db_zig.CountingDenseEmbedder;
const DerivedCoverageCounters = db_zig.DerivedCoverageCounters;
const DerivedCoverageOutcome = db_zig.DerivedCoverageOutcome;
const EnrichmentTerminalFailureMarkerWrite = db_zig.EnrichmentTerminalFailureMarkerWrite;
const HierarchyNavigationArtifact = db_zig.HierarchyNavigationArtifact;
const OpenOptions = db_zig.OpenOptions;
const OrderedApplyReceipt = db_zig.OrderedApplyReceipt;
const RequiredProducerDispatch = db_zig.RequiredProducerDispatch;
const acquireReplicationMutationSharedContext = db_zig.acquireReplicationMutationSharedContext;
const acquireSnapshotReplayContext = db_zig.acquireSnapshotReplayContext;
const appendArtifactRepairSummaryDirtyMarkerForStore = db_zig.appendArtifactRepairSummaryDirtyMarkerForStore;
const appendDerivedBatchRecord = db_zig.appendDerivedBatchRecord;
const appendDerivedBatchRecordContextWithAdmissions = db_zig.appendDerivedBatchRecordContextWithAdmissions;
const appendDerivedBatchRecordContextWithWork = db_zig.appendDerivedBatchRecordContextWithWork;
const appendEnrichmentTerminalFailureMarkerDeletePageForIssue = db_zig.appendEnrichmentTerminalFailureMarkerDeletePageForIssue;
const appendEnrichmentTerminalFailureMarkerDeletesForIssue = db_zig.appendEnrichmentTerminalFailureMarkerDeletesForIssue;
const appendResolutionHandoffDeleteKey = db_zig.appendResolutionHandoffDeleteKey;
const applyArtifactRepairability = db_zig.applyArtifactRepairability;
const artifactRepairIssueIdAlloc = db_zig.artifactRepairIssueIdAlloc;
const artifactRepairIssueKeyForIssueAlloc = db_zig.artifactRepairIssueKeyForIssueAlloc;
const artifactRepairKindHasAutomatedReprocessor = db_zig.artifactRepairKindHasAutomatedReprocessor;
const artifactRepairReasonHasAutomatedReprocessor = db_zig.artifactRepairReasonHasAutomatedReprocessor;
const artifactRepairUnsupportedReasonForIssue = db_zig.artifactRepairUnsupportedReasonForIssue;
const artifact_repair_completion_state_len = db_zig.artifact_repair_completion_state_len;
const artifact_repair_summary_dirty_marker = db_zig.artifact_repair_summary_dirty_marker;
const artifact_repair_summary_invalidation_page_size = db_zig.artifact_repair_summary_invalidation_page_size;
const artifact_repair_summary_ready_marker = db_zig.artifact_repair_summary_ready_marker;
const bytesToHexAlloc = db_zig.bytesToHexAlloc;
const canAdvanceDerivedToTargetAsync = db_zig.canAdvanceDerivedToTargetAsync;
const checkArtifactRepairCancelled = db_zig.checkArtifactRepairCancelled;
const checkHierarchyNavigationCancelled = db_zig.checkHierarchyNavigationCancelled;
const cloneArtifactRepairIssueAlloc = db_zig.cloneArtifactRepairIssueAlloc;
const collectTextReplayDeleteKeys = db_zig.collectTextReplayDeleteKeys;
const containsDeleteKey = db_zig.containsDeleteKey;
const currentTimeNs = db_zig.currentTimeNs;
const decodeArtifactRepairIssueValueAlloc = db_zig.decodeArtifactRepairIssueValueAlloc;
const denseTargetCountForIndexContext = db_zig.denseTargetCountForIndexContext;
const denseTargetCountForIndexContextWithCoverage = db_zig.denseTargetCountForIndexContextWithCoverage;
const documentRangeLowerAlloc = db_zig.documentRangeLowerAlloc;
const documentRangeUpperAlloc = db_zig.documentRangeUpperAlloc;
const documentUnitHierarchyManifestInfo = db_zig.documentUnitHierarchyManifestInfo;
const encodeArtifactRepairCompletionState = db_zig.encodeArtifactRepairCompletionState;
const encodeArtifactRepairIssueValueAlloc = db_zig.encodeArtifactRepairIssueValueAlloc;
const encodeChangeRecordPayload = db_zig.encodeChangeRecordPayload;
const enforceReplicationWriteGateOptional = db_zig.enforceReplicationWriteGateOptional;
const freeJsonValue = db_zig.freeJsonValue;
const hexToBytesAlloc = db_zig.hexToBytesAlloc;
const hierarchy_navigation_cancel_stride = db_zig.hierarchy_navigation_cancel_stride;
const isMergeArtifactKey = db_zig.isMergeArtifactKey;
const loadArtifactRepairCompletionStateFromStore = db_zig.loadArtifactRepairCompletionStateFromStore;
const loadArtifactRepairIssueFromStoreByKey = db_zig.loadArtifactRepairIssueFromStoreByKey;
const loadChunkFieldValueTxn = db_zig.loadChunkFieldValueTxn;
const lockApplyForPortableRuntimeContext = db_zig.lockApplyForPortableRuntimeContext;
const lockAtomic = db_zig.lockAtomic;
const lockAtomicWithBackoff = db_zig.lockAtomicWithBackoff;
const mentionArtifactStateNameAlloc = db_zig.mentionArtifactStateNameAlloc;
const mentionGraphStateNameAlloc = db_zig.mentionGraphStateNameAlloc;
const mergeGenerationFailureAttempts = db_zig.mergeGenerationFailureAttempts;
const missingAsNull = db_zig.missingAsNull;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const nextArtifactRepairIssueTimestamp = db_zig.nextArtifactRepairIssueTimestamp;
const no_background_test_open = db_zig.no_background_test_open;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const orderedApplyDisposition = db_zig.orderedApplyDisposition;
const orderedApplyReceiptWrite = db_zig.orderedApplyReceiptWrite;
const ordered_apply_receipt_value_len = db_zig.ordered_apply_receipt_value_len;
const platform_time = db_zig.platform_time;
const readOrderedApplyReceipt = db_zig.readOrderedApplyReceipt;
const replaceRepairIssueLastError = db_zig.replaceRepairIssueLastError;
const replicationAppliedSequenceWrite = db_zig.replicationAppliedSequenceWrite;
const replication_applied_lsn_value_len = db_zig.replication_applied_lsn_value_len;
const resetDenseIndexForArtifactRebuildForTest = db_zig.resetDenseIndexForArtifactRebuildForTest;
const saveArtifactRepairIssueToStoreWithSummary = db_zig.saveArtifactRepairIssueToStoreWithSummary;
const stageApplyMarkers = db_zig.stageApplyMarkers;
const testEmbeddingArtifactOrigins = db_zig.testEmbeddingArtifactOrigins;
const ArtifactRepairCompletionDisposition = DB.ArtifactRepairCompletionDisposition;
const ArtifactRepairIssueRevision = DB.ArtifactRepairIssueRevision;
const GeneratedArtifactCleanupAdvanceResult = DB.GeneratedArtifactCleanupAdvanceResult;
const IndexRepairLeaseAuthority = DB.IndexRepairLeaseAuthority;
const RetiredArtifactRepairIssueKeyParse = DB.RetiredArtifactRepairIssueKeyParse;
const RetiredArtifactRepairIssueValueDecode = DB.RetiredArtifactRepairIssueValueDecode;
const artifact_repair_metadata_active_poll_ns = DB.artifact_repair_metadata_active_poll_ns;
const decodeManagedIndexAdmissionMarker = DB.decodeManagedIndexAdmissionMarker;
const finalizeClaimedRetiredIndexCleanup = DB.finalizeClaimedRetiredIndexCleanup;
const hasCoordinatedConstraints = DB.hasCoordinatedConstraints;
const initialBuildCanRetireBeforeActivation = DB.initialBuildCanRetireBeforeActivation;
const producerMaintenanceNow = DB.producerMaintenanceNow;
const retired_index_artifact_repair_cleanup_page_size = DB.retired_index_artifact_repair_cleanup_page_size;

/// Reconcile an ordered source inventory before applying its mutation.
/// The store owns the admission fence; callers supply an exact receipt.
pub fn reconcileReplicatedArtifactAdmission(self: *DB, request: types.BatchRequest, entry: OrderedApplyReceipt) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, request, entry });
    const command = request.artifact_catalog orelse return;
    const context = try preflightArtifactAdmission(self, request, entry);
    if (!try reconcileOrderedArtifactCatalogWithContext(self, command, entry.index, context)) return error.ArtifactCatalogDrift;
}

pub fn scheduleGeneratedArtifactCleanup(self: *DB) void {
    scheduleGeneratedArtifactCleanupContext(
        self.async_context,
        self.backend_runtime.durable_jobs,
        self.repair_cleanup_owner_id,
    );
}

pub fn advanceGeneratedArtifactCleanupPage(self: *DB, index_name: ?[]const u8) !GeneratedArtifactCleanupAdvanceResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, index_name });
    return try advanceGeneratedArtifactCleanupContext(self.async_context, index_name);
}

pub fn advanceGeneratedArtifactCleanupContext(
    ctx: *AsyncContext,
    index_name: ?[]const u8,
) !GeneratedArtifactCleanupAdvanceResult {
    if (!ctx.index_artifact_cleanup_mutex.tryLock()) return .busy;
    var cleanup_locked = true;
    defer if (cleanup_locked) ctx.index_artifact_cleanup_mutex.unlock();

    if (index_name) |name| {
        var targeted = try ctx.index_manager.drainGeneratedArtifactCleanupForIndexPage(
            ctx.store,
            name,
        );
        defer targeted.deinit();
        if (targeted.found) {
            if (!targeted.completed) return .progressed;
            return try finalizeClaimedRetiredIndexCleanup(ctx, &cleanup_locked, targeted.index_name, targeted.key);
        }
    }

    var result = try ctx.index_manager.drainGeneratedArtifactCleanupOutboxPage(ctx.store);
    defer result.deinit();
    if (!result.found) return .idle;
    if (!result.completed) return .progressed;
    return try finalizeClaimedRetiredIndexCleanup(ctx, &cleanup_locked, result.index_name, result.key);
}

pub fn scheduleGeneratedArtifactCleanupContext(
    ctx: *AsyncContext,
    lane: background_runtime_mod.DurableJobLane,
    owner_id: u64,
) void {
    cleanup_job_owner.schedule(.{
        .ptr = ctx,
        .state = &ctx.index_manager.artifact_cleanup_state,
        .lane = lane,
        .owner_id = owner_id,
        .io = ctx.io,
        .closing = &ctx.background_closing,
        .advance = advanceArtifactCleanup,
        .name = "generated-artifact",
    });
}

pub fn advanceArtifactCleanup(ptr: *anyopaque) !cleanup_job_owner.Advance {
    const ctx: *AsyncContext = @ptrCast(@alignCast(ptr));
    return try advanceGeneratedArtifactCleanupContext(ctx, null);
}

pub fn parseRetiredArtifactRepairIssueKeyAlloc(
    alloc: Allocator,
    key: []const u8,
) !RetiredArtifactRepairIssueKeyParse {
    const parts = internal_keys.artifactRepairIssueKeyPartsAlloc(alloc, key) catch |err| switch (err) {
        error.OutOfMemory => return err,
        error.InvalidInternalUserKey => return .{ .malformed = err },
    };
    return .{ .valid = parts };
}

pub fn decodeRetiredArtifactRepairIssueAlloc(
    alloc: Allocator,
    raw: []const u8,
) !RetiredArtifactRepairIssueValueDecode {
    const issue = decodeArtifactRepairIssueValueAlloc(alloc, raw) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return .{ .malformed = err },
    };
    return .{ .valid = issue };
}

pub fn drainRetiredIndexArtifactRepairIssuesPageContext(
    ctx: *AsyncContext,
    index_name: []const u8,
) !bool {
    // Summary rebuilds own the apply lock, while issue publication and
    // terminal-marker transitions own the repair mutex. This ordering is
    // shared by derived apply paths and prevents a cleanup batch from
    // racing either authority.
    ctx.apply_mutex.lockExclusive();
    defer ctx.apply_mutex.unlockExclusive();
    lockAtomicWithBackoff(&ctx.artifact_repair_issue_mutex);
    defer ctx.artifact_repair_issue_mutex.unlock();

    const prefix = try internal_keys.artifactRepairIssueIndexPrefixAlloc(ctx.alloc, index_name);
    defer ctx.alloc.free(prefix);
    const rows = try ctx.store.scanPrefixPage(
        ctx.alloc,
        prefix,
        null,
        retired_index_artifact_repair_cleanup_page_size,
    );
    defer docstore_mod.DocStore.freeResults(ctx.alloc, rows);
    if (rows.len == 0) {
        // Primary records are authoritative. Sweep legacy orphaned
        // kind-index rows in bounded pages before releasing admission.
        for (std.meta.tags(types.ArtifactRepairKind)) |kind| {
            const kind_prefix = try internal_keys.artifactRepairIssueKindIndexPrefixAlloc(
                ctx.alloc,
                @tagName(kind),
                index_name,
            );
            defer ctx.alloc.free(kind_prefix);
            const secondary_rows = try ctx.store.scanPrefixPage(
                ctx.alloc,
                kind_prefix,
                null,
                retired_index_artifact_repair_cleanup_page_size,
            );
            defer docstore_mod.DocStore.freeResults(ctx.alloc, secondary_rows);
            if (secondary_rows.len == 0) continue;
            const secondary_deletes = try ctx.alloc.alloc([]const u8, secondary_rows.len);
            defer ctx.alloc.free(secondary_deletes);
            for (secondary_rows, 0..) |row, i| secondary_deletes[i] = row.key;
            try ctx.store.putBatch(&.{}, secondary_deletes);
            return true;
        }
        return false;
    }

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (writes.items) |write| ctx.alloc.free(@constCast(write.value));
        writes.deinit(ctx.alloc);
    }
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(ctx.alloc);
    var owned_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (owned_keys.items) |key| ctx.alloc.free(@constCast(key));
        owned_keys.deinit(ctx.alloc);
    }
    const Batch = struct {
        fn appendOwnedDelete(
            alloc: Allocator,
            out_deletes: *std.ArrayListUnmanaged([]const u8),
            out_owned_keys: *std.ArrayListUnmanaged([]const u8),
            key: []u8,
        ) !void {
            errdefer alloc.free(key);
            const owned_len = out_owned_keys.items.len;
            try out_owned_keys.append(alloc, key);
            errdefer out_owned_keys.shrinkRetainingCapacity(owned_len);
            try out_deletes.append(alloc, key);
        }
    };

    var removed_issue_count: usize = 0;
    for (rows) |row| {
        // A coalesced issue can own arbitrarily many historical failure
        // markers. Drain exactly one marker page and leave the primary
        // issue present until a later pass observes the reverse namespace
        // empty; this is the durable cursor across crashes and reopen.
        const marker_count = try appendEnrichmentTerminalFailureMarkerDeletePageForIssue(
            ctx.alloc,
            ctx.store,
            row.key,
            &deletes,
            &owned_keys,
        );
        if (marker_count != 0) {
            if (removed_issue_count != 0) {
                try appendArtifactRepairSummaryDirtyMarkerForStore(ctx.alloc, &writes, &deletes, &owned_keys);
            }
            try ctx.store.putBatch(writes.items, deletes.items);
            return true;
        }

        try deletes.append(ctx.alloc, row.key);
        removed_issue_count += 1;

        // The marker generation is keyed by the authoritative primary
        // key, so it is safe to retire even when the value is malformed.
        const generation_key = try internal_keys.enrichmentTerminalFailureGenerationKeyAlloc(ctx.alloc, row.key);
        try Batch.appendOwnedDelete(ctx.alloc, &deletes, &owned_keys, generation_key);

        var parts = switch (try parseRetiredArtifactRepairIssueKeyAlloc(ctx.alloc, row.key)) {
            .valid => |value| value,
            .malformed => |err| {
                std.log.warn("retired index cleanup removed malformed artifact repair key index={s} err={s}", .{
                    index_name,
                    @errorName(err),
                });
                continue;
            },
        };
        defer parts.deinit(ctx.alloc);
        if (!std.mem.eql(u8, parts.index_name, index_name)) return error.InvalidInternalUserKey;

        const kind_key = try internal_keys.artifactRepairIssueKindKeyAlloc(
            ctx.alloc,
            parts.repair_artifact_kind,
            parts.index_name,
            parts.issue_id,
        );
        try Batch.appendOwnedDelete(ctx.alloc, &deletes, &owned_keys, kind_key);

        var issue_valid = true;
        var issue = switch (try decodeRetiredArtifactRepairIssueAlloc(ctx.alloc, row.value)) {
            .valid => |value| value,
            .malformed => |err| issue: {
                issue_valid = false;
                std.log.warn("retired index cleanup removed malformed artifact repair issue index={s} err={s}", .{
                    index_name,
                    @errorName(err),
                });
                break :issue types.ArtifactRepairIssue{};
            },
        };
        defer if (issue_valid) issue.deinit(ctx.alloc);
        if (issue_valid) {
            const expected_issue_key = try artifactRepairIssueKeyForIssueAlloc(ctx.alloc, issue);
            defer ctx.alloc.free(expected_issue_key);
            issue_valid = std.mem.eql(u8, issue.index_name, index_name) and
                std.mem.eql(u8, expected_issue_key, row.key);
            if (!issue_valid) {
                issue.deinit(ctx.alloc);
                std.log.warn("retired index cleanup removed mismatched artifact repair issue index={s}", .{index_name});
            }
        }

        const completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(
            ctx.alloc,
            parts.repair_artifact_kind,
            parts.issue_id,
        );
        var completion_key_local = true;
        defer if (completion_key_local) ctx.alloc.free(completion_key);
        if (!issue_valid) {
            // Completion fences are only a single-flight optimization. A
            // corrupt consumer value cannot safely contribute to their
            // shared count, so discard the fence rather than let stale
            // completed state suppress future repair.
            try Batch.appendOwnedDelete(ctx.alloc, &deletes, &owned_keys, completion_key);
            completion_key_local = false;
            continue;
        }
        if (issue.reason != .enrichment_failed) {
            continue;
        }

        const completion = (try loadArtifactRepairCompletionStateFromStore(
            ctx.alloc,
            ctx.store,
            completion_key,
        )) orelse continue;
        var updated_completion = completion;
        updated_completion.pending_issues -|= 1;
        if (updated_completion.pending_issues == 0) {
            try Batch.appendOwnedDelete(ctx.alloc, &deletes, &owned_keys, completion_key);
            completion_key_local = false;
        } else {
            const owned_len = owned_keys.items.len;
            owned_keys.append(ctx.alloc, completion_key) catch |err| {
                return err;
            };
            errdefer owned_keys.shrinkRetainingCapacity(owned_len);
            const encoded = try ctx.alloc.create([artifact_repair_completion_state_len]u8);
            errdefer ctx.alloc.destroy(encoded);
            encodeArtifactRepairCompletionState(encoded, updated_completion);
            try writes.append(ctx.alloc, .{ .key = completion_key, .value = encoded });
            completion_key_local = false;
        }
    }

    try appendArtifactRepairSummaryDirtyMarkerForStore(ctx.alloc, &writes, &deletes, &owned_keys);
    try ctx.store.putBatch(writes.items, deletes.items);
    return true;
}

pub fn getArtifact(self: *DB, alloc: Allocator, artifact_id: []const u8) !?types.ArtifactRecord {
    try self.maybeFinalizePendingRowPolicyPublication();
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    var artifact_ref = (try artifact_ids.decodeArtifactPublicIdAlloc(alloc, artifact_id)) orelse return error.InvalidArgument;
    errdefer artifact_ref.deinit(alloc);

    const internal_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, artifact_ref);
    defer alloc.free(internal_key);

    const value = blk: {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        var input = try @import("artifact/extraction_generation.zig").captureInput(alloc, &read, internal_key);
        defer input.deinit();
        const raw = input.value orelse {
            artifact_ref.deinit(alloc);
            return null;
        };
        break :blk try alloc.dupe(u8, raw);
    };
    errdefer alloc.free(value);

    const id = try alloc.dupe(u8, artifact_id);
    errdefer alloc.free(id);
    try row_policy_lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    return .{
        .id = id,
        .value = value,
        .artifact_ref = artifact_ref,
    };
}

/// Returns an artifact through a public response boundary. Raw artifact
/// values are retained internally because compute/apply transports need
/// storage metadata, while public lookup must never expose hierarchy
/// revision fingerprints or coordinator envelopes.
pub fn getPublicArtifact(self: *DB, alloc: Allocator, artifact_id: []const u8) !?types.ArtifactRecord {
    try self.maybeFinalizePendingRowPolicyPublication();
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    var artifact = (try self.getArtifact(alloc, artifact_id)) orelse return null;
    errdefer artifact.deinit(alloc);
    const public_value = try hierarchy_navigation.publicArtifactPayloadAlloc(alloc, artifact.value);
    alloc.free(artifact.value);
    artifact.value = public_value;
    try row_policy_lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    return artifact;
}

pub fn reprocessEmbeddingArtifactIssue(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
    wait_for_completion: bool,
) !bool {
    var cfg = (try self.getEnrichment(alloc, .embedding, issue.artifact_name)) orelse return false;
    defer cfg.deinit(alloc);

    const source_doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
    const value = try self.get(alloc, source_doc_key) orelse return error.NotFound;
    defer alloc.free(value);

    const writes = [_]types.BatchWrite{.{ .key = source_doc_key, .value = value }};
    const force_artifacts = [_][]const u8{issue.artifact_name};
    try self.batchInternal(.{
        .writes = &writes,
        .sync_level = if (wait_for_completion) .full_index else .write,
    }, null, .{
        .wait_for_sync_level = wait_for_completion,
        .force_generated_artifact_names = &force_artifacts,
    });
    return true;
}

pub fn reprocessChunkArtifactIssue(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
) !bool {
    var cfg = (try self.getEnrichment(alloc, .chunk, issue.artifact_name)) orelse return false;
    defer cfg.deinit(alloc);

    const source_doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
    const value = try self.get(alloc, source_doc_key) orelse return error.NotFound;
    defer alloc.free(value);

    const writes = [_]types.BatchWrite{.{ .key = source_doc_key, .value = value }};
    const force_artifacts = [_][]const u8{issue.artifact_name};
    try self.batchInternal(.{
        .writes = &writes,
        .sync_level = .full_index,
    }, null, .{
        .force_generated_artifact_names = &force_artifacts,
    });
    return true;
}

pub fn artifactRepairCompletionKeyForIssueAlloc(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
) ![]u8 {
    _ = self;
    const issue_id = try artifactRepairIssueIdAlloc(alloc, issue);
    defer alloc.free(issue_id);
    return try internal_keys.artifactRepairCompletionKeyAlloc(alloc, @tagName(issue.artifact_kind), issue_id);
}

pub fn loadArtifactRepairCompletionState(
    self: *DB,
    alloc: Allocator,
    completion_key: []const u8,
) !?ArtifactRepairCompletionState {
    return try loadArtifactRepairCompletionStateFromStore(alloc, self.core.store, completion_key);
}

pub fn encodeArtifactRepairIssueAlloc(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
    _ = self;
    return try encodeArtifactRepairIssueValueAlloc(alloc, issue);
}

pub fn decodeArtifactRepairIssueAlloc(self: *DB, alloc: Allocator, raw: []const u8) !types.ArtifactRepairIssue {
    _ = self;
    return try decodeArtifactRepairIssueValueAlloc(alloc, raw);
}

pub fn loadArtifactRepairIssueByKey(self: *DB, alloc: Allocator, key: []const u8) !?types.ArtifactRepairIssue {
    return try loadArtifactRepairIssueFromStoreByKey(alloc, self.core.store, key);
}

pub fn loadArtifactRepairSummaryCountByKey(self: *DB, alloc: Allocator, key: []const u8) !?u64 {
    const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != @sizeOf(u64)) return error.InvalidArtifactPayload;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn artifactRepairSummaryReady(self: *DB, alloc: Allocator) !bool {
    const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
    defer alloc.free(ready_key);
    const ready = self.core.store.get(alloc, ready_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(ready);
    return std.mem.eql(u8, ready, artifact_repair_summary_ready_marker);
}

pub fn loadArtifactRepairSummaryCountOrScan(
    self: *DB,
    alloc: Allocator,
    key: []const u8,
) !u64 {
    if (try self.artifactRepairSummaryReady(alloc)) {
        return (try self.loadArtifactRepairSummaryCountByKey(alloc, key)) orelse 0;
    }
    return (try self.loadArtifactRepairSummaryCountByKey(alloc, key)) orelse 0;
}

pub fn artifactRepairSummaryRootCount(self: *DB, alloc: Allocator) !u64 {
    return (try self.artifactRepairSummaryRootSnapshot(alloc)).count;
}

pub fn appendArtifactRepairSummaryWrite(
    self: *DB,
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    key: []const u8,
    delta: i64,
) !void {
    const existing = (try self.loadArtifactRepairSummaryCountByKey(alloc, key)) orelse 0;
    const updated = if (delta >= 0) existing +| @as(u64, @intCast(delta)) else existing -| @as(u64, @intCast(-delta));
    if (updated == 0) {
        try deletes.append(alloc, key);
        return;
    }
    const value = try alloc.alloc(u8, @sizeOf(u64));
    std.mem.writeInt(u64, value[0..8], updated, .little);
    try writes.append(alloc, .{ .key = key, .value = value });
}

pub fn appendArtifactRepairSummaryRebuildInvalidation(
    self: *DB,
    alloc: Allocator,
    writes: ?*std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
    errdefer alloc.free(progress_key);
    const owned_len = owned_keys.items.len;
    try owned_keys.append(alloc, progress_key);
    errdefer owned_keys.shrinkRetainingCapacity(owned_len);
    if (writes) |out| {
        const dirty_value = try alloc.dupe(u8, artifact_repair_summary_dirty_marker);
        errdefer alloc.free(dirty_value);
        try out.append(alloc, .{ .key = progress_key, .value = dirty_value });
    } else {
        try deletes.append(alloc, progress_key);
    }

    const rebuild_prefix = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
    defer alloc.free(rebuild_prefix);
    try self.appendKeysForPrefixDelete(alloc, deletes, owned_keys, rebuild_prefix);
}

pub fn appendArtifactRepairSummaryDirty(
    self: *DB,
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
    errdefer alloc.free(ready_key);
    const owned_len = owned_delete_keys.items.len;
    try owned_delete_keys.append(alloc, ready_key);
    errdefer owned_delete_keys.shrinkRetainingCapacity(owned_len);
    try deletes.append(alloc, ready_key);
    try self.appendArtifactRepairSummaryRebuildInvalidation(alloc, writes, deletes, owned_delete_keys);
}

pub fn saveArtifactRepairIssueWithSummary(
    self: *DB,
    alloc: Allocator,
    key: []const u8,
    issue: types.ArtifactRepairIssue,
    new_issue: bool,
    extra_writes: []const docstore_mod.KVPair,
    extra_deletes: []const []const u8,
) !void {
    const kind_key = try self.repairIssueKindKeyForIssueAlloc(alloc, issue);
    defer alloc.free(kind_key);
    const encoded = try encodeArtifactRepairIssueAlloc(self, alloc, issue);
    defer alloc.free(encoded);
    if (!new_issue) {
        var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
        defer writes.deinit(alloc);
        try writes.append(alloc, .{ .key = key, .value = encoded });
        try writes.append(alloc, .{ .key = kind_key, .value = encoded });
        try writes.appendSlice(alloc, extra_writes);
        try self.core.store.putBatch(writes.items, extra_deletes);
        return;
    }

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    var borrowed_write_count: usize = 0;
    defer {
        for (writes.items[borrowed_write_count..]) |item| alloc.free(@constCast(item.value));
        writes.deinit(alloc);
    }
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
        owned_delete_keys.deinit(alloc);
    }

    try writes.append(alloc, .{ .key = key, .value = encoded });
    try writes.append(alloc, .{ .key = kind_key, .value = encoded });
    borrowed_write_count = writes.items.len;
    try writes.appendSlice(alloc, extra_writes);
    borrowed_write_count = writes.items.len;
    try deletes.appendSlice(alloc, extra_deletes);
    try appendArtifactRepairSummaryDirty(self, alloc, &writes, &deletes, &owned_delete_keys);
    try self.core.store.putBatch(writes.items, deletes.items);
}

pub fn deleteArtifactRepairIssueWithSummary(
    self: *DB,
    alloc: Allocator,
    key: []const u8,
    existing_issue: types.ArtifactRepairIssue,
    extra_writes: []const docstore_mod.KVPair,
    extra_deletes: []const []const u8,
) !void {
    const kind_key = try self.repairIssueKindKeyForIssueAlloc(alloc, existing_issue);
    defer alloc.free(kind_key);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    try writes.appendSlice(alloc, extra_writes);
    const borrowed_write_count = writes.items.len;
    defer for (writes.items[borrowed_write_count..]) |item| alloc.free(@constCast(item.value));
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
        owned_delete_keys.deinit(alloc);
    }

    try deletes.append(alloc, key);
    try deletes.append(alloc, kind_key);
    try deletes.appendSlice(alloc, extra_deletes);
    if (existing_issue.reason == .enrichment_failed) {
        try appendEnrichmentTerminalFailureMarkerDeletesForIssue(
            alloc,
            self.core.store,
            key,
            &deletes,
            &owned_delete_keys,
            true,
        );
    }
    try appendArtifactRepairSummaryDirty(self, alloc, &writes, &deletes, &owned_delete_keys);
    try self.core.store.putBatch(writes.items, deletes.items);
}

pub fn clearArtifactRepairIssueWithSummary(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
    const key = try self.repairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(key);

    const existing = (try loadArtifactRepairIssueByKey(self, alloc, key)) orelse {
        const stale_kind_key = try self.repairIssueKindKeyForIssueAlloc(alloc, issue);
        defer alloc.free(stale_kind_key);
        try self.core.store.putBatch(&.{}, &.{stale_kind_key});
        return;
    };
    var existing_issue = existing;
    defer existing_issue.deinit(alloc);
    try deleteArtifactRepairIssueWithSummary(self, alloc, key, existing_issue, &.{}, &.{});
}

pub fn clearArtifactRepairIssueIfCurrent(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
    revision: ArtifactRepairIssueRevision,
) !bool {
    lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
    defer self.async_context.artifact_repair_issue_mutex.unlock();

    const key = try self.repairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(key);
    // Successful replay can retire this exact diagnosis at its publication
    // boundary before the request that initiated replay reaches completion.
    // Absence is therefore an idempotent success; a present, newer revision
    // remains a conflict and must not be cleared below.
    const existing = (try loadArtifactRepairIssueByKey(self, alloc, key)) orelse return true;
    var current = existing;
    defer current.deinit(alloc);
    if (!revision.matches(current)) return false;

    try deleteArtifactRepairIssueWithSummary(self, alloc, key, current, &.{}, &.{});
    return true;
}

pub fn completeArtifactRepairIssueIfCurrent(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
    revision: ArtifactRepairIssueRevision,
    completion_key: []const u8,
    expected_epoch: u64,
    require_completed_fence: bool,
) !ArtifactRepairCompletionDisposition {
    const coverage_marker_key = try self.enrichmentFailureCoverageMarkerKeyAlloc(alloc, issue);
    defer if (coverage_marker_key) |key| alloc.free(key);

    lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
    defer self.async_context.artifact_repair_issue_mutex.unlock();

    const key = try self.repairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(key);
    // Publication owns the authoritative resolution boundary and may have
    // already removed this exact issue while the initiating repair request
    // was replaying it. Treat absence as idempotent completion, but retain
    // the revision and completion-epoch fences for every present record.
    const existing = (try loadArtifactRepairIssueByKey(self, alloc, key)) orelse return .completed;
    var current = existing;
    defer current.deinit(alloc);
    if (!revision.matches(current)) return .stale;

    // Validate the shared execution fence before interpreting this
    // consumer's coverage marker. A sibling failure can advance the epoch
    // without changing this issue revision; treating that stale snapshot
    // as coverage debt would pay for provider work that cannot commit.
    var completion = (try self.loadArtifactRepairCompletionState(alloc, completion_key)) orelse ArtifactRepairCompletionState{};
    if (completion.epoch != expected_epoch) return .stale;
    if (require_completed_fence and completion.completed_sequence < revision.sequence) return .stale;

    // Coverage transitions and repair-ledger completion share this mutex.
    // Re-read the repaired generation while holding it so either repair
    // clears debt first (and stale terminal transitions are rejected) or a
    // terminal transition lands first (and debt remains queued). There is
    // no interleaving that can leave terminal coverage without repair debt.
    if (coverage_marker_key) |marker_key| {
        if (!try self.repairCoverageMarkerComplete(alloc, marker_key)) return .coverage_incomplete;
    }

    completion.completed_sequence = @max(completion.completed_sequence, revision.sequence);
    completion.pending_issues -|= 1;

    var encoded: [artifact_repair_completion_state_len]u8 = undefined;
    const completion_writes: []const docstore_mod.KVPair = if (completion.pending_issues == 0)
        &.{}
    else blk: {
        encodeArtifactRepairCompletionState(&encoded, completion);
        break :blk &.{.{ .key = completion_key, .value = &encoded }};
    };
    const completion_deletes: []const []const u8 = if (completion.pending_issues == 0)
        &.{completion_key}
    else
        &.{};
    try deleteArtifactRepairIssueWithSummary(self, 
        alloc,
        key,
        current,
        completion_writes,
        completion_deletes,
    );
    return .completed;
}

pub fn saveArtifactRepairAttemptIfCurrent(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
    revision: ArtifactRepairIssueRevision,
) !bool {
    lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
    defer self.async_context.artifact_repair_issue_mutex.unlock();
    const key = try self.repairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(key);
    const existing = (try loadArtifactRepairIssueByKey(self, alloc, key)) orelse return false;
    var current = existing;
    defer current.deinit(alloc);
    if (!revision.matches(current)) return false;
    try self.saveArtifactRepairIssueWithSummary(alloc, key, issue, false, &.{}, &.{});
    return true;
}

pub fn drainArtifactRepairSummaryRebuildInvalidationPage(self: *DB, alloc: Allocator) !bool {
    const prefix = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
    defer alloc.free(prefix);
    const rows = try self.core.store.scanPrefixPage(
        alloc,
        prefix,
        null,
        artifact_repair_summary_invalidation_page_size,
    );
    defer docstore_mod.DocStore.freeResults(alloc, rows);
    if (rows.len == 0) return false;

    const deletes = try alloc.alloc([]const u8, rows.len);
    defer alloc.free(deletes);
    for (rows, 0..) |row, i| deletes[i] = row.key;
    try self.core.store.putBatchWithReplayWithOptions(
        null,
        &.{},
        deletes,
        null,
        .{ .defer_commit_flush = true },
    );
    return true;
}

pub fn artifactRepairKindIndexReady(self: *DB, alloc: Allocator) !bool {
    const ready_key = try internal_keys.artifactRepairKindIndexReadyKeyAlloc(alloc);
    defer alloc.free(ready_key);
    const ready = self.core.store.get(alloc, ready_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    alloc.free(ready);
    return true;
}

pub fn recordArtifactRepairIssue(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
) !void {
    lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
    defer self.async_context.artifact_repair_issue_mutex.unlock();
    const key = try self.repairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(key);

    const now_ns = currentTimeNs();
    const existing = try loadArtifactRepairIssueByKey(self, alloc, key);
    const existing_was_pending = if (existing) |loaded| loaded.reason == .enrichment_failed else false;
    const previous_terminal_sequence: ?u64 = if (existing) |loaded|
        if (loaded.reason == .enrichment_failed) loaded.sequence else null
    else
        null;
    var stored = if (existing) |loaded|
        loaded
    else
        try cloneArtifactRepairIssueAlloc(alloc, issue);
    defer stored.deinit(alloc);

    stored.artifact_kind = issue.artifact_kind;
    stored.sequence = issue.sequence;
    stored.reason = issue.reason;
    try applyArtifactRepairability(alloc, &stored);
    if (stored.first_seen_ns == 0) stored.first_seen_ns = now_ns;
    stored.last_seen_ns = nextArtifactRepairIssueTimestamp(stored.last_seen_ns, now_ns);
    if (stored.artifact_key.len == 0 and issue.artifact_key.len > 0) {
        stored.artifact_key = try alloc.dupe(u8, issue.artifact_key);
    }
    if (stored.parent_doc_key.len == 0 and issue.parent_doc_key.len > 0) {
        stored.parent_doc_key = try alloc.dupe(u8, issue.parent_doc_key);
    }
    if (stored.unit_id.len == 0 and issue.unit_id.len > 0) {
        stored.unit_id = try alloc.dupe(u8, issue.unit_id);
    }
    if (stored.source_artifact_name.len == 0 and issue.source_artifact_name.len > 0) {
        stored.source_artifact_name = try alloc.dupe(u8, issue.source_artifact_name);
    }

    const completion_key = try self.artifactRepairCompletionKeyForIssueAlloc(alloc, stored);
    defer alloc.free(completion_key);
    var completion = (try self.loadArtifactRepairCompletionState(alloc, completion_key)) orelse ArtifactRepairCompletionState{
        .pending_issues = @intFromBool(existing_was_pending),
    };
    if (existing_was_pending) completion.pending_issues = @max(completion.pending_issues, 1);
    if (stored.reason == .enrichment_failed) {
        var marker_deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer marker_deletes.deinit(alloc);
        var owned_marker_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (owned_marker_delete_keys.items) |marker_key| alloc.free(@constCast(marker_key));
            owned_marker_delete_keys.deinit(alloc);
        }
        var terminal_marker = try EnrichmentTerminalFailureMarkerWrite.init(
            alloc,
            self.core.store,
            key,
            previous_terminal_sequence,
            stored.sequence,
            &marker_deletes,
            &owned_marker_delete_keys,
        );
        defer terminal_marker.deinit(alloc);
        completion.epoch +%= 1;
        if (completion.epoch == 0) completion.epoch = 1;
        completion.completed_sequence = 0;
        if (!existing_was_pending) completion.pending_issues +|= 1;
        var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
        encodeArtifactRepairCompletionState(&encoded_completion, completion);
        try self.saveArtifactRepairIssueWithSummary(
            alloc,
            key,
            stored,
            existing == null,
            &.{
                .{ .key = completion_key, .value = &encoded_completion },
                .{ .key = &internal_keys.enrichment_terminal_failure_generation_counter_key, .value = &terminal_marker.generation_counter_value },
                .{ .key = terminal_marker.generation_key, .value = &terminal_marker.generation_value },
                .{ .key = terminal_marker.marker.sequence_key, .value = terminal_marker.marker_value },
                .{ .key = terminal_marker.marker.issue_key, .value = terminal_marker.marker.sequence_key },
            },
            marker_deletes.items,
        );
    } else {
        var marker_deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer marker_deletes.deinit(alloc);
        var owned_marker_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (owned_marker_delete_keys.items) |marker_key| alloc.free(@constCast(marker_key));
            owned_marker_delete_keys.deinit(alloc);
        }
        try appendEnrichmentTerminalFailureMarkerDeletesForIssue(
            alloc,
            self.core.store,
            key,
            &marker_deletes,
            &owned_marker_delete_keys,
            true,
        );
        try marker_deletes.append(alloc, completion_key);
        try self.saveArtifactRepairIssueWithSummary(alloc, key, stored, existing == null, &.{}, marker_deletes.items);
    }
}

pub fn recordEmbeddingArtifactRepairIssue(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    artifact_key: []const u8,
    sequence: u64,
    reason: types.ArtifactRepairReason,
) !void {
    lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
    defer self.async_context.artifact_repair_issue_mutex.unlock();
    var identity = (try artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key)) orelse return;
    defer identity.deinit(alloc);

    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    const key = try internal_keys.artifactRepairIssueKeyAlloc(alloc, index_name, "embedding", artifact_key_hex);
    defer alloc.free(key);

    const now_ns = currentTimeNs();
    const existing = try loadArtifactRepairIssueByKey(self, alloc, key);
    const existing_was_pending = if (existing) |loaded| loaded.reason == .enrichment_failed else false;
    const previous_terminal_sequence: ?u64 = if (existing) |loaded|
        if (loaded.reason == .enrichment_failed) loaded.sequence else null
    else
        null;
    var issue = if (existing) |loaded|
        loaded
    else
        types.ArtifactRepairIssue{
            .artifact_kind = .embedding,
            .index_name = try alloc.dupe(u8, index_name),
            .doc_key = try alloc.dupe(u8, identity.doc_key),
            .parent_doc_key = try alloc.dupe(u8, identity.parent_doc_key orelse ""),
            .unit_id = try alloc.dupe(u8, identity.unit_id orelse ""),
            .source_artifact_name = try alloc.dupe(u8, identity.source_artifact_name orelse ""),
            .artifact_name = try alloc.dupe(u8, identity.embedding_name),
            .artifact_key = try alloc.dupe(u8, artifact_key_hex),
            .chunk_id = identity.chunk_id,
            .repairable = artifactRepairReasonHasAutomatedReprocessor(.embedding, reason),
            .first_seen_ns = now_ns,
        };
    defer issue.deinit(alloc);

    issue.sequence = sequence;
    issue.reason = reason;
    issue.chunk_id = identity.chunk_id;
    try applyArtifactRepairability(alloc, &issue);
    issue.last_seen_ns = nextArtifactRepairIssueTimestamp(issue.last_seen_ns, now_ns);
    if (issue.artifact_key.len == 0) {
        issue.artifact_key = try alloc.dupe(u8, artifact_key_hex);
    }
    if (issue.parent_doc_key.len == 0) {
        issue.parent_doc_key = try alloc.dupe(u8, identity.parent_doc_key orelse "");
    }
    if (issue.unit_id.len == 0) {
        issue.unit_id = try alloc.dupe(u8, identity.unit_id orelse "");
    }
    if (issue.source_artifact_name.len == 0) {
        issue.source_artifact_name = try alloc.dupe(u8, identity.source_artifact_name orelse "");
    }

    const completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(alloc, "embedding", artifact_key_hex);
    defer alloc.free(completion_key);
    var completion = (try self.loadArtifactRepairCompletionState(alloc, completion_key)) orelse ArtifactRepairCompletionState{
        .pending_issues = @intFromBool(existing_was_pending),
    };
    if (existing_was_pending) completion.pending_issues = @max(completion.pending_issues, 1);
    if (reason == .enrichment_failed) {
        var marker_deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer marker_deletes.deinit(alloc);
        var owned_marker_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (owned_marker_delete_keys.items) |marker_key| alloc.free(@constCast(marker_key));
            owned_marker_delete_keys.deinit(alloc);
        }
        var terminal_marker = try EnrichmentTerminalFailureMarkerWrite.init(
            alloc,
            self.core.store,
            key,
            previous_terminal_sequence,
            sequence,
            &marker_deletes,
            &owned_marker_delete_keys,
        );
        defer terminal_marker.deinit(alloc);
        completion.epoch +%= 1;
        if (completion.epoch == 0) completion.epoch = 1;
        completion.completed_sequence = 0;
        if (!existing_was_pending) completion.pending_issues +|= 1;
        var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
        encodeArtifactRepairCompletionState(&encoded_completion, completion);
        try self.saveArtifactRepairIssueWithSummary(
            alloc,
            key,
            issue,
            existing == null,
            &.{
                .{ .key = completion_key, .value = &encoded_completion },
                .{ .key = &internal_keys.enrichment_terminal_failure_generation_counter_key, .value = &terminal_marker.generation_counter_value },
                .{ .key = terminal_marker.generation_key, .value = &terminal_marker.generation_value },
                .{ .key = terminal_marker.marker.sequence_key, .value = terminal_marker.marker_value },
                .{ .key = terminal_marker.marker.issue_key, .value = terminal_marker.marker.sequence_key },
            },
            marker_deletes.items,
        );
    } else {
        var marker_deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer marker_deletes.deinit(alloc);
        var owned_marker_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (owned_marker_delete_keys.items) |marker_key| alloc.free(@constCast(marker_key));
            owned_marker_delete_keys.deinit(alloc);
        }
        try appendEnrichmentTerminalFailureMarkerDeletesForIssue(
            alloc,
            self.core.store,
            key,
            &marker_deletes,
            &owned_marker_delete_keys,
            true,
        );
        try marker_deletes.append(alloc, completion_key);
        try self.saveArtifactRepairIssueWithSummary(alloc, key, issue, existing == null, &.{}, marker_deletes.items);
    }
}

pub fn artifactRepairScanLowerBoundAlloc(
    self: *DB,
    alloc: Allocator,
    prefix: []const u8,
    cursor: ?[]const u8,
) ![]u8 {
    _ = self;
    const raw_cursor = cursor orelse return try alloc.dupe(u8, prefix);
    if (raw_cursor.len == 0) return try alloc.dupe(u8, prefix);
    const key = hexToBytesAlloc(alloc, raw_cursor) catch return error.InvalidArgument;
    defer alloc.free(key);
    if (!std.mem.startsWith(u8, key, prefix)) return error.InvalidArgument;
    var lower = try alloc.alloc(u8, key.len + 1);
    @memcpy(lower[0..key.len], key);
    lower[key.len] = 0;
    return lower;
}

pub fn artifactRepairCursorMatchesPrefix(self: *DB, alloc: Allocator, cursor: ?[]const u8, prefix: []const u8) !bool {
    _ = self;
    const raw_cursor = cursor orelse return true;
    if (raw_cursor.len == 0) return true;
    const key = hexToBytesAlloc(alloc, raw_cursor) catch return false;
    defer alloc.free(key);
    return std.mem.startsWith(u8, key, prefix);
}

pub fn artifactRepairPrimaryScanPrefixAlloc(self: *DB, alloc: Allocator, req: types.ArtifactRepairListRequest) ![]u8 {
    _ = self;
    if (req.index_name) |name| return try internal_keys.artifactRepairIssueIndexPrefixAlloc(alloc, name);
    return try internal_keys.artifactRepairIssueRootPrefixAlloc(alloc);
}

pub fn artifactRepairKindScanPrefixAlloc(self: *DB, alloc: Allocator, req: types.ArtifactRepairListRequest, kind: types.ArtifactRepairKind) ![]u8 {
    _ = self;
    if (req.index_name) |name| return try internal_keys.artifactRepairIssueKindIndexPrefixAlloc(alloc, @tagName(kind), name);
    return try internal_keys.artifactRepairIssueKindRootPrefixAlloc(alloc, @tagName(kind));
}

pub fn artifactRepairFallbackScanBudget(limit: u32) u64 {
    const requested: u64 = if (limit == 0) 4096 else @as(u64, limit) * 32;
    return @min(@max(requested, 256), 4096);
}

pub fn artifactRepairKindForIndexKind(kind: types.IndexKind) ?types.ArtifactRepairKind {
    return switch (kind) {
        .dense_vector, .sparse_vector => .embedding,
        .graph => .graph,
        .full_text => .full_text,
        .algebraic => .algebraic,
    };
}

pub fn listArtifactRepairIssues(
    self: *DB,
    alloc: Allocator,
    artifact_kind: ?types.ArtifactRepairKind,
    index_name: ?[]const u8,
    limit: usize,
) ![]types.ArtifactRepairIssue {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, artifact_kind, index_name, limit });
    const page = try self.listArtifactRepairIssuesPage(alloc, .{
        .artifact_kind = artifact_kind,
        .index_name = index_name,
        .limit = @intCast(@min(limit, std.math.maxInt(u32))),
    });
    if (page.next_cursor) |cursor| alloc.free(cursor);
    return page.issues;
}

pub fn listEmbeddingArtifactRepairIssues(
    self: *DB,
    alloc: Allocator,
    index_name: ?[]const u8,
    limit: usize,
) ![]types.EmbeddingArtifactRepairIssue {
    const generic = try self.listArtifactRepairIssues(alloc, .embedding, index_name, limit);
    defer types.freeArtifactRepairIssues(alloc, generic);

    var issues = try alloc.alloc(types.EmbeddingArtifactRepairIssue, generic.len);
    var count: usize = 0;
    errdefer {
        for (issues[0..count]) |*issue| issue.deinit(alloc);
        alloc.free(issues);
    }
    for (generic) |issue| {
        issues[count] = try types.embeddingArtifactRepairIssueFromArtifactAlloc(alloc, issue);
        count += 1;
    }
    return issues;
}

pub fn clearArtifactRepairIssue(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
    try clearArtifactRepairIssueWithSummary(self, alloc, issue);
}

pub fn artifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
    return switch (issue.artifact_kind) {
        .embedding => try embeddingArtifactNowReadable(self, alloc, issue),
        .asset => try assetArtifactNowReadable(self, alloc, issue),
        .chunk, .graph, .full_text, .algebraic => false,
    };
}

pub fn assetArtifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
    const doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
    if (doc_key.len == 0 or issue.artifact_name.len == 0) return false;
    var cfg = (try self.getEnrichment(alloc, .asset, issue.artifact_name)) orelse return false;
    defer cfg.deinit(alloc);
    var producer_cfg = try asset_producer_mod.parseProducerConfig(alloc, cfg.producer_json);
    defer producer_cfg.deinit(alloc);

    const expected_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", issue.artifact_name);
    defer alloc.free(expected_key);
    if (issue.artifact_key.len > 0) {
        const actual_key = try hexToBytesAlloc(alloc, issue.artifact_key);
        defer alloc.free(actual_key);
        if (!std.mem.eql(u8, actual_key, expected_key)) return false;
    }
    if (producer_cfg.type != .document_extraction) {
        const raw = self.core.store.get(alloc, expected_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        defer alloc.free(raw);
        return true;
    }
    var manifest = (self.getDocumentArtifactManifest(alloc, doc_key, issue.artifact_name) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    }) orelse return false;
    defer manifest.deinit(alloc);
    return true;
}

pub fn embeddingArtifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
    if (issue.reason == .enrichment_failed and issue.source_artifact_name.len > 0 and issue.chunk_id == null) {
        // Request-scoped chunk generation may legitimately produce zero,
        // one, or many physical artifacts. The generation-scoped coverage
        // outcome is the authoritative completion fence after full_index
        // repair; probing one synthetic document key can never prove this
        // set healthy.
        const generation = self.core.index_manager.coverageGenerationForIndex(issue.index_name) orelse return false;
        const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, issue.index_name, generation, issue.doc_key);
        defer alloc.free(marker_key);
        const raw_outcome = self.core.store.get(alloc, marker_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        defer alloc.free(raw_outcome);
        const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, raw_outcome) orelse
            return error.InvalidDerivedCoverageOutcome;
        return outcome == .produced or outcome == .skipped;
    }

    const artifact_key = if (issue.artifact_key.len > 0)
        try hexToBytesAlloc(alloc, issue.artifact_key)
    else
        try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, issue.doc_key, issue.artifact_name);
    defer alloc.free(artifact_key);

    const raw = self.core.store.get(alloc, artifact_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(raw);

    if (self.core.index_manager.denseIndex(issue.index_name)) |entry| {
        const view = enrichment_artifact_codec.denseEmbeddingVectorView(raw) catch |err| {
            if (isRecoverableEmbeddingArtifactError(err)) return false;
            return err;
        };
        if (view) |vector| return vector.len == entry.dims;
        const decoded = enrichment_artifact_codec.decodeDenseEmbeddingAlloc(alloc, raw) catch |err| {
            if (isRecoverableEmbeddingArtifactError(err)) return false;
            return err;
        };
        defer alloc.free(decoded);
        return decoded.len == entry.dims;
    }

    if (self.core.index_manager.sparseIndex(issue.index_name) != null) {
        if (enrichment_artifact_codec.sparseEmbeddingVectorView(raw)) |maybe_view| {
            if (maybe_view != null) return true;
        } else |err| {
            if (isRecoverableEmbeddingArtifactError(err)) return false;
            return err;
        }
        var decoded = enrichment_artifact_codec.decodeSparseEmbeddingAlloc(alloc, raw) catch |err| {
            if (isRecoverableEmbeddingArtifactError(err)) return false;
            return err;
        };
        decoded.deinit(alloc);
        return true;
    }
    return false;
}

pub fn reprocessArtifactIssue(
    self: *DB,
    alloc: Allocator,
    issue: types.ArtifactRepairIssue,
) !bool {
    return switch (issue.artifact_kind) {
        .embedding => try self.reprocessEmbeddingArtifactIssue(alloc, issue, true),
        .asset => try self.reprocessDocumentArtifactWithCompletionWait(alloc, if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key, issue.artifact_name, true),
        .chunk => try reprocessChunkArtifactIssue(self, alloc, issue),
        .graph, .full_text, .algebraic => false,
    };
}

pub fn artifactRepairKindHasReprocessor(kind: types.ArtifactRepairKind) bool {
    return artifactRepairKindHasAutomatedReprocessor(kind);
}

/// Receipt migration reuses the published text/graph generation. Once its
/// bounded source pass, producer receipts and native checkpoint agree, a
/// shadow rebuild would only duplicate already published artifacts.
pub fn localArtifactSourceRecoveryIsServiceable(self: *DB, alloc: Allocator, intent: index_repair_state.IndexRepairIntent) !bool {
    if ((intent.kind != .full_text and intent.kind != .graph) or
        intent.work_class != .initial_build or
        (intent.trigger != .replay_artifact_unavailable and intent.trigger != .catalog_admission) or
        intent.source_replay_state == .pending or !initialBuildCanRetireBeforeActivation(intent) or
        intent.root_generation != self.core.root_generation or intent.group_id != self.localRepairGroupId()) return false;
    var minimum_replay = intent.target_sequence;
    if (intent.trigger == .replay_artifact_unavailable and intent.source_replay_state != .complete) return false;
    if (intent.trigger == .catalog_admission) {
        const key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, intent.index_name);
        defer alloc.free(key);
        const raw = self.core.store.get(alloc, key) catch |err| if (err == error.NotFound) return false else return err;
        defer alloc.free(raw);
        const marker = try decodeManagedIndexAdmissionMarker(raw);
        if (marker.disposition != .managed_rebuild or marker.config_hash != intent.config_hash) return false;
        minimum_replay = @max(minimum_replay, marker.replay_target_sequence);
    }
    if (intent.candidate_relative_path) |candidate| {
        if (try self.core.index_manager.isRepairCandidateActive(intent.index_name, candidate)) return false;
    }
    const cfg = self.core.index_manager.get(intent.index_name) orelse return false;
    if (types.indexConfigHash(cfg.*) != intent.config_hash or self.core.index_manager.loadFailure(intent.index_name) != null) return false;
    const checkpoint = try self.core.loadProjectionCheckpoint(alloc, intent.index_name);
    const applied = try self.managedIndexAppliedSequence(alloc, intent.index_name);
    const target = @max(minimum_replay, try self.projectionStatsTargetSequence(alloc, cfg.*, applied));
    if (checkpoint.status != .clean or checkpoint.config_hash != intent.config_hash or
        applied < target or checkpoint.applied_sequence < target) return false;
    const generation = self.core.index_manager.coverageGenerationForIndex(intent.index_name) orelse return false;
    var plan = try self.core.index_manager.acquireWritePlanSnapshot();
    defer plan.release();
    const sources = try self.core.index_manager.artifactSourceNamesForIndexAlloc(alloc, intent.index_name);
    defer {
        for (sources) |source| alloc.free(source);
        alloc.free(sources);
    }
    if (sources.len == 0) return false;
    var read = try self.core.store.beginReadTxn();
    defer read.abort();
    if (try artifact_publication.authority(&read) != null) return false;
    for (sources) |source| {
        const scope = try artifact_producer_readiness.localScopeAlloc(alloc, intent.index_name, source, plan.plan());
        defer alloc.free(scope);
        const counts = try DerivedCoverageCounters.load(alloc, &read, scope, generation);
        const total = counts.source_total orelse return false;
        if (counts.produced == null or counts.skipped == null or counts.terminal_failed != 0 or
            counts.produced.? +| counts.skipped.? != total) return false;
    }
    return true;
}

pub fn repairArtifactIssuesWithRequest(
    self: *DB,
    alloc: Allocator,
    req: types.ArtifactRepairRunRequest,
) !types.ArtifactRepairResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req });
    return try self.repairArtifactIssuesWithRequestOptions(alloc, req, .{});
}

pub fn repairArtifactIssuesWithRequestOptions(
    self: *DB,
    alloc: Allocator,
    req: types.ArtifactRepairRunRequest,
    options: types.ArtifactRepairRunOptions,
) !types.ArtifactRepairResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, options });
    return try self.repairArtifactIssuesWithRequestOptionsInternal(
        alloc,
        req,
        options,
        .acquire,
    );
}

/// Private recursion boundary. Public callers always enter with
/// `.acquire`; only the full-quantum durable owner can supply
/// `.already_held`, so the public options struct cannot forge lease
/// ownership.
pub fn repairArtifactIssuesWithRequestOptionsInternal(
    self: *DB,
    alloc: Allocator,
    req: types.ArtifactRepairRunRequest,
    options: types.ArtifactRepairRunOptions,
    lease_authority: IndexRepairLeaseAuthority,
) !types.ArtifactRepairResult {
    try checkArtifactRepairCancelled(options);
    if (req.target == .index) return try self.repairIndexIssuesWithRequest(
        alloc,
        req,
        options,
        lease_authority,
    );

    const page = try self.listArtifactRepairIssuesPage(alloc, .{
        .artifact_kind = req.artifact_kind,
        .index_name = req.index_name,
        .limit = req.limit,
        .cursor = req.cursor,
    });
    defer {
        types.freeArtifactRepairIssues(alloc, page.issues);
        if (page.next_cursor) |cursor| alloc.free(cursor);
    }

    var result: types.ArtifactRepairResult = .{
        .limit = req.limit,
        .has_more = page.has_more,
        .debt_remaining = page.has_more,
    };
    if (page.next_cursor) |cursor| {
        result.next_cursor = try alloc.dupe(u8, cursor);
    }
    for (page.issues) |*issue| {
        if (options.cancelled()) {
            result.unresolved += 1;
            result.debt_remaining = true;
            return result;
        }
        result.scanned += 1;
        const issue_revision = ArtifactRepairIssueRevision.capture(issue.*);
        issue.attempts += 1;
        issue.last_seen_ns = currentTimeNs();

        if (!issue.repairable or !artifactRepairKindHasReprocessor(issue.artifact_kind)) {
            const unsupported_reason = artifactRepairUnsupportedReasonForIssue(issue.*);
            issue.repairable = false;
            if (issue.unsupported_reason.len == 0) {
                issue.unsupported_reason = try alloc.dupe(u8, unsupported_reason);
            }
            try replaceRepairIssueLastError(alloc, issue, unsupported_reason);
            _ = try saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
            result.unsupported += 1;
            result.unresolved += 1;
            result.debt_remaining = true;
            continue;
        }

        // Consumer-specific debt records for one physical artifact share
        // a single local execution lease and a durable completion fence.
        // This prevents concurrent requests, later cursor pages, and
        // restart from paying for the same provider call more than once.
        const completion_key = try self.artifactRepairCompletionKeyForIssueAlloc(alloc, issue.*);
        defer alloc.free(completion_key);
        if (!(try self.beginIndexRepairLease(completion_key))) {
            result.in_progress += 1;
            result.unresolved += 1;
            result.debt_remaining = true;
            continue;
        }
        defer self.endIndexRepairLease(completion_key);

        const completion_snapshot = if (issue.reason == .enrichment_failed)
            try self.artifactRepairCompletionSnapshot(alloc, completion_key)
        else
            ArtifactRepairCompletionState{};
        if (issue.reason == .enrichment_failed and
            completion_snapshot.completed_sequence >= issue.sequence)
        {
            // Chunk regeneration is set-valued: a valid run may replace,
            // remove, or produce no physical chunks. A completion fence
            // written only after successful full_index regeneration is
            // the authoritative proof for sibling consumer debt.
            if (issue.artifact_kind == .chunk or try artifactNowReadable(self, alloc, issue.*)) {
                switch (try self.completeArtifactRepairIssueIfCurrent(
                    alloc,
                    issue.*,
                    issue_revision,
                    completion_key,
                    completion_snapshot.epoch,
                    true,
                )) {
                    .completed => {
                        result.repaired += 1;
                        continue;
                    },
                    .stale => {
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        continue;
                    },
                    // A sibling consumer completed the shared provider
                    // work, but this consumer still has terminal coverage.
                    // Reprocess it below so the durable fence cannot turn
                    // into a permanent no-progress fast path.
                    .coverage_incomplete => {},
                }
            }
        }

        const reprocessed = reprocessArtifactIssue(self, alloc, issue.*) catch |err| switch (err) {
            error.NotFound => {
                try replaceRepairIssueLastError(alloc, issue, "source_document_missing");
                _ = try saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
                result.missing_source_docs += 1;
                result.unresolved += 1;
                result.debt_remaining = true;
                continue;
            },
            else => {
                try replaceRepairIssueLastError(alloc, issue, @errorName(err));
                _ = try saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
                result.failed += 1;
                result.unresolved += 1;
                result.debt_remaining = true;
                continue;
            },
        };
        if (!reprocessed) {
            const unavailable_error = switch (issue.artifact_kind) {
                .embedding => "embedding_enrichment_unavailable",
                else => "artifact_reprocessor_unavailable",
            };
            try replaceRepairIssueLastError(alloc, issue, unavailable_error);
            _ = try saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
            result.failed += 1;
            result.unresolved += 1;
            result.debt_remaining = true;
            continue;
        }
        result.reprocessed += 1;
        if (issue.artifact_kind == .chunk or try artifactNowReadable(self, alloc, issue.*)) {
            if (issue.reason == .enrichment_failed) {
                switch (try self.completeArtifactRepairIssueIfCurrent(
                    alloc,
                    issue.*,
                    issue_revision,
                    completion_key,
                    completion_snapshot.epoch,
                    false,
                )) {
                    .completed => result.repaired += 1,
                    .stale, .coverage_incomplete => {
                        result.unresolved += 1;
                        result.debt_remaining = true;
                    },
                }
            } else {
                if (try self.clearArtifactRepairIssueIfCurrent(alloc, issue.*, issue_revision)) {
                    result.repaired += 1;
                } else {
                    result.unresolved += 1;
                    result.debt_remaining = true;
                }
            }
        } else {
            try replaceRepairIssueLastError(alloc, issue, "artifact_still_unreadable");
            _ = try saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
            result.failed += 1;
            result.unresolved += 1;
            result.debt_remaining = true;
        }
    }
    if (result.repaired != 0) {
        self.runDerivedUntil(self.core.nextDerivedSequence()) catch |err| switch (err) {
            error.ArtifactRepairRequired => {},
            else => return err,
        };
        try self.runArtifactRepairMetadataMaintenanceUntilIdle();
    }
    return result;
}

pub fn repairArtifactIssues(
    self: *DB,
    alloc: Allocator,
    artifact_kind: ?types.ArtifactRepairKind,
    limit: usize,
) !types.ArtifactRepairResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, artifact_kind, limit });
    return try self.repairArtifactIssuesWithRequest(alloc, .{
        .artifact_kind = artifact_kind,
        .limit = @intCast(@min(limit, std.math.maxInt(u32))),
    });
}

pub fn repairEmbeddingArtifactIssues(self: *DB, alloc: Allocator, limit: usize) !types.EmbeddingArtifactRepairResult {
    return try self.repairArtifactIssues(alloc, .embedding, limit);
}

/// Returns a bounded page of authoritative artifacts under a transition
/// lease. The continuation is an exclusive physical store key. One large
/// row may exceed the byte budget so pagination always makes progress.
pub fn mergeArtifactsPage(
    self: *DB,
    alloc: Allocator,
    byte_range: types.ByteRange,
    after_key: ?[]const u8,
) ![]types.BatchWrite {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, byte_range, after_key });
    // Export only an admitted primary snapshot. Revalidate under the apply
    // lease so recovery publication cannot race the page's admission.
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    const lower = try documentRangeLowerAlloc(alloc, byte_range.start);
    defer alloc.free(lower);
    const upper = if (byte_range.end.len > 0) try documentRangeUpperAlloc(alloc, byte_range.end) else null;
    defer if (upper) |key| alloc.free(key);
    var txn = try self.core.store.beginReadTxn();
    defer txn.abort();
    var cursor = try txn.openCursor();
    defer cursor.close();
    cursor.setUpperBound(upper);
    var rows = std.ArrayListUnmanaged(types.BatchWrite).empty;
    errdefer {
        for (rows.items) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        rows.deinit(alloc);
    }
    var bytes: usize = 0;
    var next = try cursor.seekAtOrAfter(after_key orelse lower);
    while (next) |row| : (next = try cursor.next()) {
        if (upper) |bound| if (std.mem.order(u8, row.key, bound) != .lt) break;
        if (after_key) |key| if (std.mem.order(u8, row.key, key) != .gt) continue;
        if (!isMergeArtifactKey(row.key)) continue;
        if (internal_keys.isGraphRetirementKey(row.key)) _ = try graph_cleanup_contract_mod.retirementGeneration(row.value);
        const value = if (internal_keys.isGraphEdgeArtifactKey(row.key)) graph: {
            const edge = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, row.key)) orelse return error.InvalidBatchRequest;
            defer {
                alloc.free(edge.doc_key);
                alloc.free(edge.index_name);
                alloc.free(edge.edge_type);
                alloc.free(edge.target_doc_key);
                alloc.free(edge.edge_id);
                alloc.free(edge.logical_source);
            }
            const index = self.core.index_manager.graphIndex(edge.index_name) orelse continue;
            var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, row.value);
            defer decoded.deinit(alloc);
            if (enrichment_artifact_codec.isLegacyUnboundGraphEdge(row.value)) continue;
            if (decoded.generation != index.config.coverage_generation and
                !enrichment_artifact_codec.isPortableUnboundGraphEdge(row.value)) continue;
            // Authenticate against the donor generation, then mark the
            // edge portable. Receiver replay binds its own generation;
            // copying the donor generation would silently discard it.
            break :graph try enrichment_artifact_codec.encodePortableUnboundGraphEdgeWithTtlAlloc(
                alloc,
                decoded.weight,
                decoded.created_at,
                decoded.updated_at,
                decoded.ttl_created_ns,
                decoded.metadata_json,
            );
        } else if (internal_keys.isGraphGlobalEdgeContenderKey(row.key)) contender: {
            const generation = try graph_edge_contender.coverageGeneration(row.value);
            var current = false;
            for (self.core.index_manager.graph_indexes.items) |entry| {
                if (entry.config.coverage_generation == generation and
                    internal_keys.matchesGraphGlobalEdgeContenderIndexName(row.key, entry.config.name))
                {
                    const decoded = (try graph_edge_contender.decode(row.value, generation)) orelse return error.InvalidGraphEdgeContender;
                    const expected_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, entry.config.name, generation, decoded.edge_key, decoded.source_priority, decoded.state_key);
                    defer alloc.free(expected_key);
                    if (!std.mem.eql(u8, row.key, expected_key)) return error.InvalidGraphEdgeContender;
                    current = true;
                    break;
                }
            }
            if (!current) continue;
            break :contender try alloc.dupe(u8, row.value);
        } else if (internal_keys.isGraphEdgeTtlLifetimeKey(row.key) or internal_keys.isGraphEdgeTtlTombstoneKey(row.key)) state: {
            const generation = try internal_keys.graphEdgeTtlStateKeyGeneration(row.key);
            var current = false;
            for (self.core.index_manager.graph_indexes.items) |entry| {
                if (entry.config.coverage_generation == generation and
                    internal_keys.matchesGraphEdgeContenderIndexName(row.key, entry.config.name))
                {
                    current = true;
                    break;
                }
            }
            if (!current) continue;
            if (internal_keys.isGraphEdgeTtlLifetimeKey(row.key)) {
                if (row.value.len != 8) return error.InvalidGraphEdgeTtlLifetime;
            } else _ = try graph_edge_ttl_tombstone.Tombstone.decode(row.value);
            break :state try alloc.dupe(u8, row.value);
        } else try alloc.dupe(u8, row.value);
        errdefer alloc.free(value);
        const key = try alloc.dupe(u8, row.key);
        errdefer alloc.free(key);
        try rows.append(alloc, .{ .key = key, .value = value });
        bytes +|= key.len +| value.len;
        if (rows.items.len >= 128 or bytes >= 1024 * 1024) break;
    }
    return rows.toOwnedSlice(alloc);
}

pub fn preflightArtifactAdmission(self: *DB, req: types.BatchRequest, entry: anytype) !artifact_reconcile_intent.Context {
    try artifact_inventory.validateRequest(req);
    try artifact_reconcile.validateDesired(self.alloc, req.artifact_catalog.?);
    if (!std.mem.eql(u8, &req.artifact_catalog.?.namespace, &online_source_contract.namespaceBytes(self.core.identity_namespace))) return error.IdentityNamespaceMismatch;
    if (self.local_execution.row_policy_gate.currentPhase() != .disabled) return error.RowPolicyTopologyUnsupported;
    try range_protection_mod.validateRequest(req);
    try merge_page_contract.validateRequest(req);
    try online_source_contract.validateRequest(req);
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (self.local_execution.row_policy_gate.currentPhase() != .disabled) return error.RowPolicyTopologyUnsupported;
    // Reject before staging the durable intent. The catalog materializer
    // cannot change configuration while source ownership is migrating.
    try self.enforceVectorMigrationConfigurationGate();
    var txn = try self.core.store.beginWriteTxn();
    var preflight_open = true;
    defer if (preflight_open) txn.abort();
    const prepared = try artifact_inventory.prepareOrdered(self.alloc, &txn, req.artifact_catalog.?, entry.index);
    defer if (prepared) |bytes| self.alloc.free(bytes);
    // Speculative admission derives the effect language from the same
    // ordered catalog as final admission. This transaction is aborted
    // before installing the durable reconciliation intent.
    if (prepared) |bytes| try txn.put(artifact_inventory.ordered_key, bytes);
    try restore_staging.requireScope(self.alloc, &txn, null, false);
    const raw = txn.get(merge_state_mod.key) catch |err| switch (err) {
        error.NotFound => txn.get(merge_state_mod.legacy_key) catch |legacy_err| switch (legacy_err) {
            error.NotFound => null,
            else => return legacy_err,
        },
        else => return err,
    };
    var existing = if (raw) |bytes| try merge_state_mod.decodeAlloc(self.alloc, bytes) else null;
    defer if (existing) |*state| state.deinit(self.alloc);
    if (req.online_source) |source| {
        if (source != .admit or source.scope().authority != .raft) return error.InvalidArtifactCatalogCommand;
        const lsm = self.core.primary_store_owner.lsmBackend() orelse return error.BackupSealBackendUnsupported;
        if (lsm.storage) |storage| if (!storage.supportsHostPathGenerationPublication()) return error.NativeBackupStorageBackendUnsupported;
        if (!source.scope().fence.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
        if (try doc_identity.loadNamespaceTxn(&txn)) |stored| {
            if (!stored.eql(source.scope().fence.namespace)) return error.IdentityNamespaceMismatch;
        } else try doc_identity.writeNamespaceTxn(&txn, source.scope().fence.namespace);
        if (existing) |state| if (state.phase != .none and state.phase != .finalized and state.phase != .rolled_back) return error.IntegrityTopologyBusy;
        const authority = source_authority_mod;
        if (try authority.load(&txn) != null) _ = try authority.require(&txn, .raft, source.scope().namespace());
        try online_source.stage(&txn, source, entry.index);
    } else if (req.merge_checkpoint) |checkpoint| {
        if (try relational_integrity_topology.current(&txn) != null) return error.IntegrityTopologyBusy;
        if (checkpoint.page_source) |source| {
            try source.validate();
            if (source.artifact_catalog) |binding| if (!binding.compatible(req.artifact_catalog.?.binding)) return error.InvalidArtifactCatalogCommand;
        }
        if (try retained_effects_mod.load(&txn)) |retained| {
            const namespace = online_source_contract.namespaceBytes(self.core.identity_namespace);
            if (retained.active() and std.mem.eql(u8, &retained.namespace, &namespace)) return error.IntegrityTopologyBusy;
        }
        var coordinated = try online_integrity_shadow.admit(self.alloc, &txn, req, self.core.identity_namespace);
        if (!coordinated) coordinated = try relational_integrity_handoff.admitMergeRequest(self.alloc, &txn, req);
        var schema_view = self.core.schema_registry.acquire();
        defer if (schema_view) |*view| view.release();
        if (hasCoordinatedConstraints(schema_view) and !coordinated) return error.CoordinatedConstraintTopologyUnsupported;
        if (checkpoint.receiver_identity_reassignment_namespace) |namespace| {
            if (!checkpoint.allow_doc_identity_reassignment or !namespace.eql(self.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
        } else if (checkpoint.allow_doc_identity_reassignment) return error.InvalidBatchRequest;
        const plan = try merge_state_mod.planCheckpointApply(self.alloc, if (existing) |*state| state else null, self.core.byteRange(), checkpoint);
        defer plan.deinit(self.alloc);
        const pages = merge_page_contract;
        const page_raw = try missingAsNull(txn.get(pages.key));
        var progress = if (page_raw) |bytes| try pages.decode(self.alloc, bytes) else null;
        defer if (progress) |*value| value.deinit();
        _ = try pages.checkpointPlan(existing, plan.state, checkpoint, if (progress) |value| value.value else null);
        if (checkpoint.page_receiver_namespace) |namespace| if (!namespace.eql(self.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
    }
    // Discard the speculative admission writes, then reserve its exact
    // committed authority while still holding the native apply mutex.
    txn.abort();
    preflight_open = false;
    var intent_txn = try self.core.store.beginWriteTxn();
    errdefer intent_txn.abort();
    const context = try artifact_reconcile_intent.stage(self.alloc, &intent_txn, req, entry);
    try intent_txn.commit();
    return context;
}

pub fn reconcileOrderedArtifactCatalogStep(self: *DB, command: artifact_inventory.Command, applied_index: u64) !bool {
    return reconcileOrderedArtifactCatalogWithContext(self, command, applied_index, null);
}

pub fn reconcileOrderedArtifactCatalogWithContext(self: *DB, command: artifact_inventory.Command, applied_index: u64, context: ?artifact_reconcile_intent.Context) !bool {
    if (!std.mem.eql(u8, &command.namespace, &online_source_contract.namespaceBytes(self.core.identity_namespace))) return error.IdentityNamespaceMismatch;
    {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        const validated = try artifact_inventory.prepareOrdered(self.alloc, &read, command, applied_index);
        defer if (validated) |bytes| self.alloc.free(bytes);
        if (std.mem.eql(u8, &(try artifact_inventory.catalogs(&read)).digest(), &command.binding.digest) and try self.artifactMaterializationsReady(&read, command.catalogs)) return true;
    }
    return artifact_reconcile.step(self, command, context orelse return error.ArtifactCatalogDrift);
}

pub fn alignOrderedArtifactCatalog(self: *DB, command: artifact_inventory.Command, configs: []const types.IndexConfig, context: artifact_reconcile_intent.Context) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, command, configs, context });
    var guard = self.beginIndexStructuralMutation("ordered artifact reconciliation", "");
    defer guard.deinit();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    // Match the other catalog writers' lock order and retain publication
    // ownership until durable bytes and their cached format agree.
    self.core.index_manager.catalog_mutex.lockExclusive();
    defer self.core.index_manager.catalog_mutex.unlockExclusive();
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    try self.requireOnlineArtifactCatalogMutableWithContextLocked(&txn, context);
    try self.requireArtifactReconcileUnfenced(&txn, context);
    const stamp = try self.core.index_manager.alignRowDerivedArtifactCatalogNoLock(&txn, command.catalogs, configs);
    try txn.commit();
    self.core.index_manager.publishCatalogReaderStamp(stamp);
    self.core.index_manager.invalidateWritePlanSnapshot();
}

pub fn reconcileArtifactAddIndex(self: *DB, cfg: types.IndexConfig, context: artifact_reconcile_intent.Context) !?u128 {
    return self.addIndexForReconciliation(cfg, .managed, context, null, null);
}

pub fn reconcileArtifactDeleteIndex(self: *DB, name: []const u8, context: artifact_reconcile_intent.Context) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, name, context });
    return self.deleteIndexForReconciliation(name, context);
}

pub fn reconcileArtifactDeleteResolver(self: *DB, name: []const u8, context: artifact_reconcile_intent.Context) !bool {
    const removed = try self.removeResolverForReconciliation(name, context);
    if (removed) self.stopResolverReplayRuntimesIfUnconfigured();
    return removed;
}

pub fn reconcileArtifactDeleteEnrichment(self: *DB, kind: types.EnrichmentKind, name: []const u8, context: artifact_reconcile_intent.Context) !bool {
    return self.deleteEnrichmentForReconciliation(kind, name, context);
}

pub fn reconcileArtifactAddEnrichment(self: *DB, cfg: types.EnrichmentConfig, context: artifact_reconcile_intent.Context) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg, context });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var replication_mutation = self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    // Exact committed reconciliation provenance authorizes standby
    // materialization; no public/local configuration bypass is exposed.
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    {
        var probe = try self.core.store.beginProbeTxn();
        defer probe.abort();
        try self.requireOnlineArtifactCatalogMutableWithContextLocked(&probe, context);
    }
    try self.enforceVectorMigrationConfigurationGate();
    _ = try self.core.upsertEnrichment(cfg);
}

pub fn artifactInventoryStatus(self: *DB) !artifact_inventory.Status {
    var txn = try self.core.store.beginReadTxn();
    defer txn.abort();
    var namespace: [24]u8 = undefined;
    doc_identity.encodeNamespace(&namespace, self.core.identity_namespace);
    var observed = try artifact_inventory.status(self.alloc, &txn, namespace);
    if (observed.ready) observed.ready = try self.artifactMaterializationsReady(&txn, try artifact_inventory.catalogs(&txn));
    return observed;
}

/// The caller verifies this desired catalog against its metadata read-index
/// before proposal. Ownership of catalogs belongs to the caller.
pub fn artifactInventoryCommand(self: *DB, alloc: Allocator) !artifact_inventory.Command {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    const inventory = artifact_inventory;
    var txn = try self.core.store.beginReadTxn();
    defer txn.abort();
    if (try artifact_reconcile_intent.load(alloc, &txn)) |intent| {
        defer intent.deinit();
        var command = intent.value.command;
        command.catalogs.indexes = try alloc.dupe(u8, command.catalogs.indexes);
        errdefer alloc.free(command.catalogs.indexes);
        command.catalogs.enrichments = try alloc.dupe(u8, command.catalogs.enrichments);
        errdefer alloc.free(command.catalogs.enrichments);
        command.catalogs.resolvers = try alloc.dupe(u8, command.catalogs.resolvers);
        return command;
    }
    var namespace: [24]u8 = undefined;
    doc_identity.encodeNamespace(&namespace, self.core.identity_namespace);
    const observed = try inventory.status(alloc, &txn, namespace);
    const catalogs = try inventory.copyCatalogs(alloc, &txn);
    errdefer {
        var owned = catalogs;
        owned.deinit(alloc);
    }
    const effect_protocol = @max(try catalogs.requiredEffectProtocol(alloc), if (observed.ordered) |binding| binding.effect_protocol else 14);
    if (observed.ready and observed.ordered.?.effect_protocol == effect_protocol) {
        var installed = (try inventory.load(alloc, &txn)).?;
        defer installed.deinit();
        var command = installed.value.command;
        command.catalogs = catalogs;
        return command;
    }
    return .{ .namespace = namespace, .previous = observed.ordered, .binding = .{ .epoch = try std.math.add(u64, if (observed.ordered) |value| value.epoch else 0, 1), .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(alloc), .effect_protocol = effect_protocol }, .catalogs = catalogs };
}

pub fn completeArtifactUpload(txn: anytype, finalization: ?artifact_publication_transport.Finalization, entry: OrderedApplyReceipt) !void {
    if (finalization) |prepared| {
        const upload = prepared.control;
        if (upload.action != .finalize) return error.InvalidBatchRequest;
        try artifact_publication_transport.stageCompletion(txn, upload.namespace, upload.publication_digest, upload.manifest_root, prepared.created_index, entry.index);
    }
}

pub fn applyArtifactPublicationTransportBatch(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    const transport = artifact_publication_transport;
    const control = req.artifact_publication_transport orelse return error.InvalidBatchRequest;
    try transport.validateBatchRequest(req);
    const entry = opts.ordered_apply_receipt orelse return error.InvalidBatchRequest;
    if (entry.term == 0 or entry.index == 0 or opts.artifact_upload_finalize != null) return error.InvalidBatchRequest;
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var namespace: [24]u8 = undefined;
    doc_identity.encodeNamespace(&namespace, self.core.identity_namespace);
    if (control.action != .prune and !std.mem.eql(u8, &namespace, &control.namespace)) return error.IdentityNamespaceMismatch;
    // Assemble against one immutable snapshot outside apply. The final
    // publication transaction checks the same immutable manifest root
    // again, so concurrent pruning cannot publish partially retired work.
    if (control.action == .finalize) assembly: {
        var read = try self.core.store.beginReadTxn();
        var read_open = true;
        defer if (read_open) read.abort();
        if (try transport.terminal(&read, namespace, control.publication_digest)) |decided| {
            if (!std.mem.eql(u8, &decided.root, &control.manifest_root)) return error.ArtifactPublicationUploadChanged;
        } else {
            const raw = read.get(&transport.manifestKey(namespace, control.publication_digest)) catch |err| switch (err) {
                error.NotFound => break :assembly,
                else => return err,
            };
            const manifest = try transport.decodeManifest(raw);
            // A recovered hint is tied to one durable upload incarnation.
            // Prune/recreate makes the hint an inert ordered entry, not
            // authority to consume a different upload with the same root.
            if (control.created_index != 0 and control.created_index != manifest.created_index) break :assembly;
            var reservation: ?resource_manager_mod.Reservation = null;
            defer if (reservation) |*value| value.release();
            // Charge the encoded command, backend read buffers and
            // decoded slice tables before allocating the assembly.
            if (self.core.index_manager.resource_manager) |manager| reservation = try manager.reserveWithoutReclaim(.shard_transition_working_set, @as(u64, manifest.encoded_len) * 2 + 4 * 1024 * 1024);
            var decoded = transport.assembleAlloc(self.alloc, &read, namespace, control.publication_digest, control.manifest_root) catch |err| switch (err) {
                error.ArtifactPublicationUploadMissing, error.ArtifactPublicationUploadIncomplete => break :assembly,
                else => return err,
            };
            defer decoded.deinit();
            if (decoded.command.mode == .activate) return error.InvalidBatchRequest;
            read.abort();
            read_open = false;
            var publication_opts = opts;
            publication_opts.artifact_upload_finalize = .{ .control = control, .created_index = manifest.created_index };
            return self.applyArtifactPublicationBatch(.{ .artifact_publication = decoded.command, .sync_level = req.sync_level }, publication_opts);
        }
    }
    var replication_mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    if (!opts.bypass_replication_write_gate) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.flushDurableReplicationOutboxes();
        try self.preflightReplicationBatchSyncCommit();
    }
    const replication_payload = if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
        try replication_effects_mod.encodeArtifactPublicationTransportMutationRequestAlloc(self.alloc, req, entry)
    else
        null;
    defer if (replication_payload) |value| self.alloc.free(value);
    // Decode at most one chunk before entering the serialized writer.
    const chunk: ?[]u8 = if (control.action == .chunk) blk: {
        const length = std.base64.standard.Decoder.calcSizeForSlice(control.chunk_base64) catch return error.InvalidBatchRequest;
        const bytes = try self.alloc.alloc(u8, length);
        errdefer self.alloc.free(bytes);
        std.base64.standard.Decoder.decode(bytes, control.chunk_base64) catch return error.InvalidBatchRequest;
        break :blk bytes;
    } else null;
    defer if (chunk) |bytes| self.alloc.free(bytes);
    var mutation = self.core.snapshot_admission.acquireMutation();
    defer mutation.release();
    try self.lockApplyForPortableRuntime();
    var apply_held = true;
    defer if (apply_held) self.core.unlockApply();
    if (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry) == .already_applied) return;
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    _ = try source_authority_mod.require(&txn, .raft, namespace);
    switch (control.action) {
        .begin => {
            _ = try transport.pruneOneExpired(&txn, entry.index);
            // Upload backpressure must not wedge an already committed
            // Raft entry. A refused begin creates no receipt or artifact;
            // the durable producer obligation remains pending for retry.
            transport.stageBegin(self.alloc, &txn, control.proposedManifest(), entry.index) catch |err| switch (err) {
                error.ResourceLimitExceeded => {},
                else => return err,
            };
        },
        .chunk => transport.stageChunk(&txn, namespace, control.publication_digest, control.manifest_root, control.ordinal, chunk.?) catch |err| switch (err) {
            error.ArtifactPublicationUploadMissing => {},
            else => return err,
        },
        .finalize => {
            if (try transport.terminal(&txn, namespace, control.publication_digest)) |decided| {
                if (!std.mem.eql(u8, &decided.root, &control.manifest_root)) return error.ArtifactPublicationUploadChanged;
            }
            // Missing/incomplete staging is a pending upload, never a
            // semantic success. Only the common publication writer can
            // create a terminal record and producer receipt.
        },
        .prune => _ = try transport.pruneOneExpired(&txn, entry.index),
        .abandon => _ = try transport.stageAbandon(&txn, control),
    }
    try stageApplyMarkers(&txn, entry, opts.replication_applied_lsn_marker);
    for (opts.extra_store_writes) |write| try txn.put(write.key, write.value);
    for (opts.extra_store_deletes) |key| try txn.delete(key);
    try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
    try txn.commit();
    self.core.unlockApply();
    apply_held = false;
    if (replication_payload != null) try self.flushDurableReplicationOutboxes();
}

pub fn rejectArtifactPublicationAssumeApply(self: *DB, command: artifact_publication.Command, opts: BatchExecutionOptions, replication_payload: ?[]const u8, reason: artifact_publication.Rejection, apply_held: *bool) !void {
    // A source/catalog race is an ordered outcome, not an apply failure.
    // Keep accepted provenance intact and commit the rejection with the
    // applied watermark, so retries cannot wedge a committed Raft entry.
    if (command.completion) |page| if (page.group_producers) std.log.debug("group producer completion deferred reason={s}", .{@tagName(reason)});
    if (command.mode == .publish and command.producer_kind == .enrichment) std.log.debug("producer publication deferred producer={s} reason={s}", .{ command.producer_name, @tagName(reason) });
    const entry = opts.ordered_apply_receipt orelse return error.InvalidBatchRequest;
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    try artifact_publication.stageRejection(&txn, command, entry.index, reason);
    // Baseline readiness is temporary: keep the immutable upload so the
    // same logical publication can finalize after reconciliation. A
    // terminal upload record here would strand its retry forever.
    if (reason != .baseline_pending) try completeArtifactUpload(&txn, opts.artifact_upload_finalize, entry);
    try stageApplyMarkers(&txn, entry, opts.replication_applied_lsn_marker);
    for (opts.extra_store_writes) |write| try txn.put(write.key, write.value);
    for (opts.extra_store_deletes) |key| try txn.delete(key);
    try self.stageRestoreStagingReplicationOutbox(&txn, replication_payload);
    try txn.commit();
    self.core.unlockApply();
    apply_held.* = false;
    if (replication_payload != null) try self.flushDurableReplicationOutboxes();
}

pub fn applyArtifactCatalogBatch(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    const inventory = artifact_inventory;
    try inventory.validateRequest(req);
    const command = req.artifact_catalog orelse return error.InvalidArtifactCatalogCommand;
    const entry = opts.ordered_apply_receipt orelse return error.InvalidArtifactCatalogCommand;
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var replication_mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    if (!opts.bypass_replication_write_gate) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.flushDurableReplicationOutboxes();
        try self.preflightReplicationBatchSyncCommit();
    }
    var payload: ?[]u8 = null;
    defer if (payload) |bytes| self.alloc.free(bytes);
    {
        var admission = self.core.snapshot_admission.acquireMutation();
        defer admission.release();
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        if (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry) == .already_applied) return;
        var namespace: [24]u8 = undefined;
        doc_identity.encodeNamespace(&namespace, self.core.identity_namespace);
        if (!std.mem.eql(u8, &namespace, &command.namespace)) return error.IdentityNamespaceMismatch;
        var txn = try self.core.store.beginWriteTxn();
        errdefer txn.abort();
        // The active source cut pins the existing catalog until release.
        try self.requireOnlineArtifactCatalogMutableLocked(&txn);
        try relational_integrity_topology.requireUnfenced(&txn);
        try inventory.stageOrdered(self.alloc, &txn, command, entry.index);
        for (opts.extra_store_writes) |write| try txn.put(write.key, write.value);
        var marker_bytes: [ordered_apply_receipt_value_len]u8 = undefined;
        const marker = orderedApplyReceiptWrite(entry, &marker_bytes);
        try txn.put(marker.key, marker.value);
        if (opts.replication_applied_lsn_marker) |lsn| {
            var bytes: [replication_applied_lsn_value_len]u8 = undefined;
            const replication_marker = replicationAppliedSequenceWrite(lsn, &bytes);
            try txn.put(replication_marker.key, replication_marker.value);
        }
        if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
            payload = try replication_effects_mod.encodeArtifactCatalogMutationRequestAlloc(self.alloc, req, entry);
        try self.stageRestoreStagingReplicationOutbox(&txn, payload);
        try txn.commit();
    }
    if (payload != null) try self.flushDurableReplicationOutboxes();
}

pub fn describeOnlineSourceArtifact(self: *DB, scope: online_source_contract.Scope, cancellation: types.CancellationToken) !source_artifact_transfer.Descriptor {
    return source_artifact_transfer.describe(self, scope, cancellation);
}

pub fn readOnlineSourceArtifact(self: *DB, alloc: Allocator, descriptor: source_artifact_transfer.Descriptor, offset: u64, cancellation: types.CancellationToken) !source_artifact_transfer.OwnedChunk {
    return source_artifact_transfer.read(self, alloc, descriptor, offset, cancellation);
}

pub fn receiveOnlineSourceArtifact(self: *DB, descriptor: source_artifact_transfer.Descriptor, chunk: source_artifact_transfer.Chunk, cancellation: types.CancellationToken) !source_artifact_transfer.Status {
    return source_artifact_transfer.receive(self, descriptor, chunk, cancellation);
}

pub fn onlineSourceArtifactStatus(self: *DB, descriptor: source_artifact_transfer.Descriptor, cancellation: types.CancellationToken) !source_artifact_transfer.Status {
    return source_artifact_transfer.status(self, descriptor, cancellation);
}

pub fn finishOnlineSourceArtifact(self: *DB, descriptor: source_artifact_transfer.Descriptor, cancellation: types.CancellationToken) !source_artifact_transfer.Status {
    return source_artifact_transfer.finish(self, descriptor, cancellation);
}

/// Artifact and resolver catalog changes alter the source's materialized
/// state contract. REF3 retains primary-row and coordinated-integrity
/// effects, not independently produced graph/vector/enrichment effects.
/// Check after taking the apply lock, which serializes this observation
/// with source admission and terminal release.
pub fn requireOnlineArtifactCatalogMutableLocked(self: *DB, txn: anytype) !void {
    return self.requireOnlineArtifactCatalogMutableWithContextLocked(txn, null);
}

pub fn requireArtifactReconcileUnfenced(self: *DB, txn: anytype, context: ?artifact_reconcile_intent.Context) !void {
    if (context) |value| {
        try artifact_reconcile_intent.requireContext(self.alloc, txn, value);
        if (try relational_integrity_topology.current(txn) != null) return error.IntegrityTopologyBusy;
    } else try relational_integrity_topology.requireUnfenced(txn);
}

pub fn requireOnlineArtifactCatalogMutableWithContextLocked(self: *DB, txn: anytype, context: ?@import("artifact/reconcile_intent.zig").Context) !void {
    var namespace: [24]u8 = undefined;
    doc_identity.encodeNamespace(&namespace, self.core.identity_namespace);
    return @import("artifact/catalog_mutability.zig").requireMutable(self.alloc, txn, namespace, context);
}

pub fn retireResolverArtifactsLocked(self: *DB, cfg: index_manager_mod.ResolverConfig) !?u64 {
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deletes.items) |key| self.alloc.free(@constCast(key));
        deletes.deinit(self.alloc);
    }
    var changed = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (changed.items) |key| self.alloc.free(key);
        changed.deinit(self.alloc);
    }

    const marker_prefix = try internal_keys.assetArtifactSourceIndexPrefixAlloc(self.alloc, cfg.source_artifact);
    defer self.alloc.free(marker_prefix);
    const markers = try self.core.store.scanPrefix(self.alloc, marker_prefix);
    defer docstore_mod.DocStore.freeResults(self.alloc, markers);

    for (markers) |marker| {
        const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(self.alloc, marker.value)) orelse continue;
        defer {
            self.alloc.free(parsed.doc_key);
            self.alloc.free(parsed.artifact_name);
        }
        if (!std.mem.eql(u8, parsed.artifact_name, cfg.source_artifact)) continue;

        const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(self.alloc, parsed.doc_key, cfg.resolution_artifact);
        var resolution_key_owned = true;
        errdefer if (resolution_key_owned) self.alloc.free(resolution_key);
        const exists = if (self.core.store.get(self.alloc, resolution_key)) |raw| exists: {
            self.alloc.free(raw);
            break :exists true;
        } else |err| switch (err) {
            error.NotFound => false,
            else => return err,
        };
        if (exists and !containsDeleteKey(deletes.items, resolution_key)) {
            try deletes.append(self.alloc, resolution_key);
            resolution_key_owned = false;
            try appendUniqueOwnedKey(self.alloc, &changed, resolution_key);
            try appendResolutionHandoffDeleteKey(self.alloc, &deletes, resolution_key);
        } else {
            self.alloc.free(resolution_key);
            resolution_key_owned = false;
        }

        try collectResolverMentionArtifactsForDocLocked(self, cfg, parsed.doc_key, &deletes, &changed);
    }

    const root_lower = [_]u8{internal_keys.user_namespace};
    const root_upper = [_]u8{internal_keys.user_namespace + 1};
    const resolution_rows = try self.core.store.scanRange(self.alloc, root_lower[0..], root_upper[0..]);
    defer docstore_mod.DocStore.freeResults(self.alloc, resolution_rows);
    for (resolution_rows) |row| {
        const parsed = (try internal_keys.parseResolutionArtifactKeyAlloc(self.alloc, row.key)) orelse continue;
        defer {
            self.alloc.free(parsed.doc_key);
            self.alloc.free(parsed.artifact_name);
        }
        if (!std.mem.eql(u8, parsed.artifact_name, cfg.resolution_artifact)) continue;
        if (!containsDeleteKey(deletes.items, row.key)) {
            try deletes.append(self.alloc, try self.alloc.dupe(u8, row.key));
            try appendUniqueOwnedKey(self.alloc, &changed, row.key);
            try appendResolutionHandoffDeleteKey(self.alloc, &deletes, row.key);
        }
        try collectResolverMentionArtifactsForDocLocked(self, cfg, parsed.doc_key, &deletes, &changed);
    }

    if (deletes.items.len == 0 and changed.items.len == 0) return null;

    const sequence = self.core.reserveDerivedAppendSequence();
    const replay_payload = try encodeChangeRecordPayload(&self.batchContext(), .{
        .sequence = sequence,
        .changed_artifact_keys = changed.items,
    }, sequence);
    defer self.alloc.free(replay_payload);

    var backlog_admission = try self.executor.admitBacklogBytes(@intCast(replay_payload.len));
    defer backlog_admission.cancel();
    try self.core.store.putBatchWithReplay(self.backend_runtime.io(), &.{}, deletes.items, .{
        .sequence = sequence,
        .payload = replay_payload,
    });
    self.mirrorReplicationReplayPayloadBestEffort(replay_payload);
    self.executor.commitBacklogAdmission(sequence, &backlog_admission);
    return sequence;
}

pub fn collectResolverMentionArtifactsForDocLocked(
    self: *DB,
    cfg: index_manager_mod.ResolverConfig,
    doc_key: []const u8,
    deletes: *std.ArrayListUnmanaged([]const u8),
    changed: *std.ArrayListUnmanaged([]u8),
) !void {
    for (self.core.graphIndexes()) |graph_entry| {
        for (graph_entry.artifact_sources) |source| {
            if (source.mention_edge_type.len == 0) continue;
            if (!std.mem.eql(u8, source.artifact_name, cfg.source_artifact)) continue;

            const state_name = try mentionGraphStateNameAlloc(self.alloc, cfg.source_artifact, cfg.resolution_artifact);
            defer self.alloc.free(state_name);
            try self.collectGraphStateForDeleteLocked(doc_key, graph_entry.config.name, state_name, graph_entry.config.coverage_generation, deletes, changed);

            const mention_state_name = try mentionArtifactStateNameAlloc(self.alloc, cfg.source_artifact, cfg.resolution_artifact);
            defer self.alloc.free(mention_state_name);
            try self.collectGraphStateForDeleteLocked(doc_key, graph_entry.config.name, mention_state_name, graph_entry.config.coverage_generation, deletes, changed);
        }
    }
}

pub fn artifactRepairMetadataRebuildPending(self: *DB) bool {
    const summary_ready = self.artifactRepairSummaryReady(self.alloc) catch return true;
    if (!summary_ready) return true;
    const kind_index_ready = self.artifactRepairKindIndexReady(self.alloc) catch return true;
    return !kind_index_ready;
}

pub fn runArtifactRepairMetadataMaintenancePass(self: *DB) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    // The mark owns immutable primary/ANN/source leases. Scan before
    // taking apply; only setup, planning, and publication need that fence.
    if (self.local_execution.source_vectors.load(.acquire)) |source| try source.advanceMarkingSnapshot();
    return self.runArtifactRepairMetadataMaintenanceAfterScan();
}

pub fn runArtifactRepairMetadataMaintenanceAfterScan(self: *DB) !bool {
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();

    var more = try self.core.index_manager.runGraphOwnershipCleanupStep();
    more = (try self.rebuildArtifactRepairSummaryIfMissing(self.alloc)) or more;
    more = (try self.rebuildArtifactRepairKindIndexIfMissing(self.alloc)) or more;
    if (self.local_execution.source_vectors.load(.acquire)) |source| {
        const step_bytes = source.backgroundCollectionStepBytes();
        if (step_bytes != 0) {
            try self.refreshSourceVectorOwnershipScopes();
            const prior_generation = source.currentGeneration();
            if (try source.collectBackgroundStepDeferredMark(&self.core.store.runtime_store, step_bytes)) {
                if (source.currentGeneration() != prior_generation) try self.core.index_manager.refreshSourcePayloadGeneration();
            }
            if (source.collectionPending()) more = true;
        }
    }
    return more;
}

pub fn runArtifactRepairMetadataMaintenanceUntilIdle(self: *DB) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    while (try self.runArtifactRepairMetadataMaintenancePass()) {}
}

/// Start only after the DB has reached its final address. DB.open returns
/// by value, so cache owners invoke this after installing that value in a
/// stable heap entry rather than letting an async task capture the open
/// function's temporary stack address.
pub fn startArtifactRepairMetadataWorkerIfNeeded(self: *DB) void {
    if (!self.start_independent_maintenance) return;
    if (comptime builtin.single_threaded or builtin.os.tag == .freestanding) return;
    // Ordinary unit tests drive maintenance explicitly. Borrowed runtimes
    // schedule the production owner deterministically, including VOPR's
    // test-artifact runner; disabling it strands durable graph cleanup.
    if (comptime builtin.is_test) {
        if (!self.backend_runtime.usesBorrowedIo()) return;
    }
    if (!self.start_index_workers) return;
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return;
    self.independent_maintenance.start(self.independentMaintenancePort());
}

pub fn stopArtifactRepairMetadataWorker(self: *DB) void {
    self.independent_maintenance.stop(self.backend_runtime);
}

pub fn artifactRepairMetadataWorkerStep(self: *DB) ?u64 {
    return self.independent_maintenance.step(self.independentMaintenancePort());
}

/// One bounded physical-key page, never under the DB apply lock. Both
/// discovery and the ordinary maintenance scheduler advance the same
/// durable cursor, so large old roots do not require repeated client calls.
pub fn advanceArtifactFootprintPage(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return true;
    var budget = if (self.core.index_manager.resource_manager) |manager|
        resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, self.alloc, 1)
    else
        null;
    defer if (budget) |*value| value.deinit();
    const scratch = if (budget) |*value| value.allocator() else self.alloc;
    const complete = artifact_footprint_mod.reconcilePage(scratch, self.core.store) catch |err| {
        if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
    self.artifact_footprint_pending.store(!complete, .release);
    return complete;
}

/// Discover bounded durable upload facts without payload reads. Release
/// the storage snapshot before invoking the external scheduling owner.
/// Admission remains a hint, never an acknowledgement or seal.
pub fn advanceArtifactUploadRecovery(self: *DB) !bool {
    return self.advanceArtifactUploadRecoveryWithTrigger(.explicit);
}

pub fn advanceArtifactUploadRecoveryWithTrigger(self: *DB, trigger: @FieldType(artifact_publication.UploadRecoveryInvocation, "trigger")) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    const dispatcher = self.local_execution.artifact_publication_dispatcher orelse return false;
    const recovery = dispatcher.upload_recovery orelse return false;
    const now_ns = self.independentMaintenanceNowNs();
    if (!recovery.should_poll(dispatcher.ptr, .{ .now_ns = now_ns, .trigger = trigger })) return false;
    const inventory = blk: {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        const namespace = (try source_authority_mod.externalPublicationNamespace(&read)) orelse return false;
        break :blk try artifact_publication_transport.recoveryInventory(&read, namespace);
    };
    return recovery.recover(dispatcher.ptr, .{
        .io = self.core.index_manager.checkpointIo(),
        .now_ns = now_ns,
        .inventory = inventory,
        .trigger = trigger,
    });
}

/// Append root-producer requests using the existing durable replay journal.
/// The cursor is only a fairness hint: acceptance and sealing remain ordered
/// operations, and restart can safely rediscover every unscheduled row.
pub fn advanceArtifactProducerWorkPage(self: *DB) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    return self.artifact_producer_scheduler.advance(.{ .ptr = self, .now = producerMaintenanceNow, .page = advanceArtifactProducerBudgetedPage });
}

pub fn advanceArtifactProducerBudgetedPage(ptr: *anyopaque) !bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    var budget = if (self.core.index_manager.resource_manager) |manager|
        resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, self.alloc, 1)
    else
        null;
    defer if (budget) |*value| value.deinit();
    const scratch = if (budget) |*value| value.allocator() else self.alloc;
    return advanceArtifactProducerWorkPageWithAllocator(self, scratch) catch |err| {
        if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
}

pub fn refreshArtifactProjections(self: *DB, scratch: Allocator, document: []const u8, plan: *const index_manager_mod.IndexManager.WritePlanSnapshot, limits: artifact_completion_progress.Limits) !void {
    const completion = artifact_completion_progress;
    var requests: completion.AdoptionRequests = .{};
    try completion.refreshProjectionsWithAdoption(scratch, self.core.store, self.core.index_manager, self.core.applied_sequence_checkpoint_path, self.root_incarnation, document, plan, limits, &requests);
    if (requests.count == 0) return;
    // Use the revision-tracked scheduler directory for O(1) deduplication,
    // not a checkpoint-file scan per document. Creation still performs its
    // durable CAS. Existing paused/terminal jobs retain their semantics.
    try self.ensureIndexRepairSchedulerDirectory(scratch);
    const compiled = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    for (requests.ordinals[0..requests.count]) |ordinal| {
        const node = &compiled.nodes[ordinal];
        lockAtomic(&self.async_context.index_repair_scheduler_mutex);
        const existing = self.async_context.index_repair_scheduler.initialized and
            self.async_context.index_repair_scheduler.containsName(node.name);
        self.async_context.index_repair_scheduler_mutex.unlock();
        if (existing) continue;
        var structural = self.beginIndexStructuralMutation("artifact baseline adoption", node.name);
        defer structural.deinit();
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        {
            var read = try self.core.store.beginReadTxn();
            defer read.abort();
            const catalogs = try artifact_inventory.catalogs(&read);
            if (!plan.matchesArtifactInventory(catalogs)) return error.ArtifactCatalogDrift;
            const active = (try artifact_publication.authority(&read)) orelse return error.ArtifactCatalogDrift;
            if (!std.meta.eql(active, requests.authority.?) or !std.mem.eql(u8, &active.catalog_digest, &catalogs.digest())) return error.ArtifactCatalogDrift;
        }
        const cfg = self.core.index_manager.get(node.name) orelse return error.ArtifactCatalogDrift;
        if (cfg.kind != .full_text or cfg.coverage_generation != node.generation) return error.ArtifactCatalogDrift;
        _ = try self.createGenerationRepairIntent(scratch, cfg.*, .artifact_baseline_adoption, 0, 0, null);
        return;
    }
}

pub fn enqueueArtifactCompletion(self: *DB, scratch: Allocator, document: []const u8, plan: *const index_manager_mod.IndexManager.WritePlanSnapshot) !void {
    const dispatcher = self.local_execution.artifact_publication_dispatcher orelse return;
    try self.refreshArtifactProjections(scratch, document, plan, .{});
    var encoded: ?[]u8 = null;
    defer if (encoded) |bytes| scratch.free(bytes);
    var namespace: artifact_publication.Namespace = undefined;
    var action: ?artifact_completion_progress.NextAction = null;
    defer if (action) |*value| value.deinit();
    var jobs: ?artifact_unit_jobs.Admission = null;
    defer if (jobs) |*value| value.deinit();
    {
        var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const owner = (try source_authority_mod.publicationOwner(&read)) orelse return error.ArtifactCatalogDrift;
        if (owner.mode != .ordered) return;
        action = (try artifact_completion_progress.discoverNextAction(scratch, &read, self.root_incarnation, document, plan, .{})) orelse return;
        switch (action.?) {
            .control => |*prepared| {
                const command = try prepared.command();
                namespace = command.namespace;
                encoded = try artifact_publication_transport_codec.encodeAlloc(scratch, command);
            },
            .jobs => |*page| jobs = try artifact_unit_jobs.prepare(scratch, &read, self.root_incarnation, page, .{}),
        }
    }
    if (jobs) |*admission| {
        _ = self.admitArtifactUnitJobs(admission) catch |err| switch (err) {
            // Scope-local headroom must not pin the document sweep. No
            // cursor/job/wakeup committed on refusal; the next metadata
            // round retries after workers have had a chance to drain it.
            error.ResourceBudgetExceeded => return,
            else => return err,
        };
        return;
    }
    // Queue admission does not discharge work or spin the scheduler.
    // Receiver acceptance and the metadata poll drive the next prefix.
    try dispatcher.submit(namespace, encoded.?);
}

/// One bounded receiver-local pass over a document's scoped outbox. Only
/// accepted or obsolete receipts retire jobs; pending callbacks retain
/// their slots. The pinned read is gone before the writer fence; the
/// immutable plan remains pinned for the maintenance turn.
pub fn advanceArtifactUnitReceiptPage(self: *DB, scratch: Allocator, document: []const u8, plan: *const index_manager_mod.IndexManager.WritePlanSnapshot) !bool {
    const unit_jobs = artifact_unit_jobs;
    var document_turn: ?unit_jobs.DocumentTurn = null;
    var work: ?unit_jobs.WorkTurn = null;
    defer if (work) |*turn| turn.deinit();
    var retirements: std.ArrayListUnmanaged(unit_jobs.Retirement) = .empty;
    defer {
        for (retirements.items) |*retirement| retirement.deinit();
        retirements.deinit(scratch);
    }
    {
        var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        document_turn = try unit_jobs.prepareDocumentTurn(&read, self.root_incarnation, document);
        const turn = document_turn orelse return false;
        if (turn.selected) |selected| {
            work = try unit_jobs.prepareTurn(scratch, &read, self.root_incarnation, selected, .{ .visits = 8 });
        }
        if (work != null and work.?.page.items.len != 0) {
            const child = work.?.page.items[0].job.child;
            if (unit_jobs.retirementSession(scratch, &read, self.root_incarnation, document, child, plan)) |session_value| {
                var session = session_value;
                defer session.deinit();
                for (work.?.page.items) |item| {
                    if (!std.mem.eql(u8, item.job.document, document) or !std.mem.eql(u8, item.job.child, child)) return error.ArtifactCatalogCorrupt;
                    if (try session.preparePageItem(scratch, item)) |prepared| {
                        var retirement = prepared;
                        errdefer retirement.deinit();
                        try retirements.append(scratch, retirement);
                    }
                }
            } else |err| switch (err) {
                error.ArtifactPublicationPending, error.EnrichmentSourceChanged => {},
                else => return err,
            }
        }
    }
    const pointers = try scratch.alloc(*const unit_jobs.Retirement, retirements.items.len);
    defer scratch.free(pointers);
    for (retirements.items, pointers) |*retirement, *pointer| pointer.* = retirement;
    const work_ptr: ?*const unit_jobs.WorkTurn = if (work) |*turn| turn else null;
    try self.finishArtifactUnitWorkPage(&document_turn.?, work_ptr, pointers);
    return true;
}

pub fn advanceArtifactProducerWorkPageWithAllocator(self: *DB, scratch: Allocator) !bool {
    const has_ordered_authority = blk: {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        break :blk try artifact_publication.authority(&read) != null;
    };
    if (!has_ordered_authority) {
        if (self.local_source_completion_audit_pending) {
            _ = try self.ensureGeneratedCoverageRecoveryIntents(scratch);
            self.local_source_completion_audit_pending = false;
        }
        return false;
    }
    var plan = try self.core.index_manager.acquireWritePlanSnapshot();
    defer plan.release();
    const readiness_progress = try artifact_producer_readiness.advance(scratch, self.core.store, self.root_incarnation, plan.plan());
    const obligations = artifact_producer_obligations;
    var page = blk: {
        var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const authority = (try artifact_publication.authority(&read)) orelse return false;
        // Protocol-14 owners have no required-stream work registry.
        const state = (try obligations.load(&read)) orelse {
            var ordered = (try artifact_inventory.load(scratch, &read)) orelse return error.ArtifactCatalogDrift;
            defer ordered.deinit();
            if (ordered.value.command.binding.effect_protocol >= 15) return error.ArtifactCatalogCorrupt;
            const pending = try artifact_producer_readiness.pending(scratch, self.core.store, self.root_incarnation);
            self.artifact_producer_scheduler.completeReadinessPage(self.alloc, pending, readiness_progress, self.independentMaintenanceNowNs());
            return readiness_progress;
        };
        try state.requireAuthority(authority);
        if (state.sealed_attempt != null) return false;
        const catalogs = try artifact_inventory.catalogs(&read);
        if (!plan.plan().matchesArtifactInventory(catalogs) or !std.mem.eql(u8, &catalogs.digest(), &authority.catalog_digest)) return error.ArtifactCatalogDrift;
        const cursor = self.artifact_producer_scheduler.cursorForAuthority(self.alloc, authority);
        break :blk try obligations.scanWork(scratch, &read, authority, cursor);
    };
    defer page.deinit();
    const dispatch = @import("artifact/producer_dispatch.zig");
    const retry = @import("artifact/producer_retry.zig");
    const retry_now = self.independentMaintenanceNowNs();
    self.artifact_producer_scheduler.observeAuthority(page.authority);
    const retry_real_now: u64 = @intCast(@max(0, std.Io.Timestamp.now(self.core.index_manager.checkpointIo(), .real).toNanoseconds()));
    // Persisted wall time selects retries only. It prevents cold-owner
    // churn from restarting the grace period forever; runtime deadlines
    // still use the owner's awake clock and cannot discharge any work.
    if (self.artifact_producer_scheduler.needsRetryDelay()) {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        self.artifact_producer_scheduler.setRetryDelay(retry_now, try obligations.retryDelay(&read, page.authority, retry_real_now, retry.interval_ns));
    }
    if (self.artifact_producer_scheduler.needsRetryRound(retry_now, page.items.len != 0, plan.plan().generated_templates.len != 0)) {
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        var txn = try self.core.store.beginWriteTxn();
        errdefer txn.abort();
        const round = try obligations.beginRetryRoundAt(&txn, page.authority, retry_real_now);
        try txn.commit();
        self.artifact_producer_scheduler.startRound(page.authority, round);
    }
    var refs: dispatch.Buffer = undefined;
    const deadline = platform_time.monotonicNs() +| artifact_producer_scheduler_mod.Scheduler.quantum_ns;
    var processed: usize = 0;
    var more_dispatch = false;
    for (page.items) |item| {
        if (processed != 0 and platform_time.monotonicNs() >= deadline) break;
        // Allocate the next fairness cursor before committing, so allocation
        // failure cannot strand an admitted request behind a stale cursor.
        const next = try self.alloc.dupe(u8, item.document);
        errdefer self.alloc.free(next);
        // Native verification is local, resumable evidence, not discharge.
        // It also runs for catalogs with no generated provider templates.
        const native_progress = try artifact_native_stream.advance(scratch, self.core.store, self.root_incarnation, item.document, plan.plan());
        more_dispatch = more_dispatch or native_progress == .progress;
        if (native_progress == .closed) {
            _ = try self.advanceArtifactUnitReceiptPage(scratch, item.document, plan.plan());
            try enqueueArtifactCompletion(self, scratch, item.document, plan.plan());
        }
        if (!item.dispatch_complete and plan.plan().generated_templates.len != 0) {
            const requests = try dispatch.prepare(plan.plan().generated_templates, item.next_template, item.document, &refs);
            var ctx = self.batchContext();
            _ = try appendDerivedBatchRecordContextWithWork(&ctx, .{ .generated_enrichment_refs = requests.items }, false, .{
                .authority = page.authority,
                .item = item,
                .plan_generation = plan.generation(),
                .page = requests.progress,
            });
            more_dispatch = more_dispatch or !requests.progress.complete;
        }
        if (self.artifact_producer_scheduler.retryPage(item, plan.plan().generated_templates.len != 0)) |round| {
            const requests = blk: {
                var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
                defer read.abort();
                break :blk try retry.prepare(scratch, &read, self.root_incarnation, plan.plan(), round.first_template, item.document, &refs);
            };
            const required: RequiredProducerDispatch = .{ .authority = page.authority, .item = item, .plan_generation = plan.generation(), .page = requests.progress, .retry_round = round.number };
            if (requests.items.len != 0) {
                var ctx = self.batchContext();
                _ = try appendDerivedBatchRecordContextWithWork(&ctx, .{ .generated_enrichment_refs = requests.items }, false, required);
            } else {
                // No empty replay record for an already accepted page.
                // This advances scheduling only, not completion credit.
                try self.lockApplyForPortableRuntime();
                defer self.core.unlockApply();
                if (self.core.index_manager.writePlanGeneration() != required.plan_generation) return error.EnrichmentSourceChanged;
                var txn = try self.core.store.beginWriteTxn();
                errdefer txn.abort();
                if (!try obligations.stageRetry(scratch, &txn, page.authority, item, round.number, null, requests.progress)) return error.EnrichmentSourceChanged;
                try txn.commit();
            }
            self.artifact_producer_scheduler.noteRetryProgress(requests.progress.complete);
        }
        self.artifact_producer_scheduler.commitCursor(self.alloc, page.authority, next);
        processed += 1;
    }
    const readiness_pending = try artifact_producer_readiness.pending(scratch, self.core.store, self.root_incarnation);
    self.artifact_producer_scheduler.completeWorkPage(self.alloc, processed == page.items.len and page.at_end, more_dispatch, .{ .pending = readiness_pending, .progressed = readiness_progress }, self.independentMaintenanceNowNs());
    return processed != 0 or readiness_progress;
}

/// Persist scoped jobs and their document wakeup atomically. The thin
/// journal is only a wakeup: exact child/unit/generation identities remain
/// in the root-local outbox until receipt-checked retirement.
pub fn admitArtifactUnitJobs(self: *DB, admission: *const artifact_unit_jobs.Admission) !u64 {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    if (admission.root != self.root_incarnation) return error.DurableRootIncarnationUnavailable;
    var plan = try self.core.index_manager.acquireWritePlanSnapshot();
    defer plan.release();
    const completion = if (plan.plan().completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const child = try completion.unitChild(admission.page.child);
    const ordinal = child.parent_template orelse return error.ArtifactPublicationPending;
    if (ordinal >= plan.plan().generated_templates.len) return error.ArtifactCatalogDrift;
    var ctx = self.batchContext();
    if (admission.jobs.len == 0) {
        // Accepted/metadata-only pages need no provider wakeup or empty
        // replay record. Apply the same authority and snapshot fences.
        var replication_mutation = acquireReplicationMutationSharedContext(&ctx);
        defer if (replication_mutation) |*lease| lease.release();
        try enforceReplicationWriteGateOptional(ctx.replication_write_gate);
        var snapshot_replay = try acquireSnapshotReplayContext(&ctx);
        defer if (snapshot_replay) |*lease| lease.release();
        try lockApplyForPortableRuntimeContext(&ctx);
        defer ctx.apply_mutex.unlockExclusive();
        if (ctx.index_manager.writePlanGeneration() != plan.generation()) return error.EnrichmentSourceChanged;
        var writer = try self.core.store.beginWriteTxn();
        errdefer writer.abort();
        _ = try admission.stage(&writer, self.root_incarnation);
        try writer.commit();
        return 0;
    }
    const parent = plan.plan().generated_templates[ordinal];
    const wake: enrichment_types.GeneratedEnrichmentRef = .{ .kind = parent.kind, .index_name = parent.index_name, .artifact_name = parent.artifact_name, .embedding_name = parent.embedding_name, .doc_key = admission.page.document };
    const sequence = try appendDerivedBatchRecordContextWithAdmissions(&ctx, .{ .generated_enrichment_refs = (&wake)[0..1] }, false, null, .{ .admission = admission, .root = self.root_incarnation, .plan_generation = plan.generation() });
    self.executor.notifySequence(sequence);
    if (self.enrichment_runtime) |runtime| runtime.notifySequence(sequence);
    return sequence;
}

/// Retire only receiver-verified work; callback submission is not an ack.
/// This changes local scheduling metadata, not replay or stream completion.
pub fn retireArtifactUnitJob(self: *DB, retirement: *const artifact_unit_jobs.Retirement) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var ctx = self.batchContext();
    var replication_mutation = acquireReplicationMutationSharedContext(&ctx);
    defer if (replication_mutation) |*lease| lease.release();
    try enforceReplicationWriteGateOptional(ctx.replication_write_gate);
    var snapshot_replay = try acquireSnapshotReplayContext(&ctx);
    defer if (snapshot_replay) |*lease| lease.release();
    try lockApplyForPortableRuntimeContext(&ctx);
    defer ctx.apply_mutex.unlockExclusive();
    var writer = try self.core.store.beginWriteTxn();
    errdefer writer.abort();
    const retired = try retirement.stage(&writer, self.root_incarnation);
    try writer.commit();
    return retired;
}

/// Advance the fair worker continuation without acknowledging any job.
/// The turn owns its scan results; no provider runs under this write gate.
pub fn advanceArtifactUnitWorkTurn(self: *DB, turn: *const artifact_unit_jobs.WorkTurn) !void {
    return self.finishArtifactUnitWorkTurn(null, turn);
}

/// Commit sibling and within-child fairness together after a bounded
/// attempt. A refused/canceled child may advance only the document cursor;
/// neither cursor acknowledges a producer result or releases job headroom.
pub fn finishArtifactUnitWorkTurn(self: *DB, document: ?*const artifact_unit_jobs.DocumentTurn, work: ?*const artifact_unit_jobs.WorkTurn) !void {
    return self.finishArtifactUnitWorkPage(document, work, &.{});
}

/// One durable worker checkpoint: fair continuations and receiver-verified
/// receipts commit together. A callback outcome alone cannot enter this
/// list; every retirement carries its own current accepted-result fence.
pub fn finishArtifactUnitWorkPage(self: *DB, document: ?*const artifact_unit_jobs.DocumentTurn, work: ?*const artifact_unit_jobs.WorkTurn, retirements: []const *const artifact_unit_jobs.Retirement) !void {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    if (document == null and work == null and retirements.len == 0) return error.InvalidBatchRequest;
    if (retirements.len > 128) return error.InvalidBatchRequest;
    if (document) |selected| if (work) |turn| {
        if (selected.selected == null or !std.mem.eql(u8, &selected.selected.?, &turn.selected)) return error.InvalidBatchRequest;
    };
    for (retirements) |retirement| {
        if (retirement.root != self.root_incarnation) return error.DurableRootIncarnationUnavailable;
        if (!std.mem.eql(u8, &retirement.selected, &retirements[0].selected) or
            !std.mem.eql(u8, &retirement.document_scope, &retirements[0].document_scope)) return error.InvalidBatchRequest;
        if (work) |turn| if (!std.mem.eql(u8, &retirement.selected, &turn.selected)) return error.InvalidBatchRequest;
        if (document) |turn| {
            if (turn.selected == null or !std.mem.eql(u8, &retirement.selected, &turn.selected.?) or
                !std.mem.eql(u8, &retirement.document_scope, &turn.document)) return error.InvalidBatchRequest;
        }
    }
    var ctx = self.batchContext();
    var replication_mutation = acquireReplicationMutationSharedContext(&ctx);
    defer if (replication_mutation) |*lease| lease.release();
    try enforceReplicationWriteGateOptional(ctx.replication_write_gate);
    var snapshot_replay = try acquireSnapshotReplayContext(&ctx);
    defer if (snapshot_replay) |*lease| lease.release();
    try lockApplyForPortableRuntimeContext(&ctx);
    defer ctx.apply_mutex.unlockExclusive();
    var writer = try self.core.store.beginWriteTxn();
    errdefer writer.abort();
    if (work) |turn| _ = try turn.stage(&writer, self.root_incarnation);
    if (document) |turn| try turn.stage(&writer, self.root_incarnation);
    for (retirements) |retirement| _ = try retirement.stage(&writer, self.root_incarnation);
    try writer.commit();
}

pub fn advanceArtifactProducerBaselinePage(self: *DB) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return true;
    var budget = if (self.core.index_manager.resource_manager) |manager|
        resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, self.alloc, 1)
    else
        null;
    defer if (budget) |*value| value.deinit();
    const scratch = if (budget) |*value| value.allocator() else self.alloc;
    return advanceArtifactProducerBaselinePageWithAllocator(self, scratch) catch |err| {
        if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
}

pub fn advanceArtifactProducerBaselinePageWithAllocator(self: *DB, scratch: Allocator) !bool {
    const publication_mode = blk: {
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        if (try artifact_publication.authority(&read) == null) {
            self.artifact_producer_baseline_pending.store(false, .release);
            return true;
        }
        const owner = (try source_authority_mod.publicationOwner(&read)) orelse return error.ArtifactCatalogDrift;
        break :blk owner.mode;
    };
    const retired_work = try artifact_producer_obligations.collectObsoleteWorkPage(scratch, self.core.store);
    const retired_proofs = try artifact_producer_provenance.collectObsoletePage(scratch, self.core.store);
    const retired_checkpoints = try artifact_stream_checkpoint.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const retired_streams = try artifact_stream_progress.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const retired_completions = try artifact_completion_progress.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const retired_native = try artifact_native_stream.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const retired_inventories = try artifact_chunk_inventory_progress.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const retired_units = try artifact_unit_progress.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const retired_unit_jobs = try artifact_unit_jobs.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const retired_authored = try artifact_authored_acceptance.collectObsoletePage(scratch, self.core.store, self.root_incarnation);
    const swept_authored = try artifact_authored_acceptance.collectCurrentPage(scratch, self.core.store, self.root_incarnation);
    const retired = retired_work and retired_proofs and retired_checkpoints and retired_streams and retired_completions and retired_native and retired_inventories and retired_units and retired_unit_jobs and retired_authored and swept_authored;
    if (publication_mode == .ordered) {
        var prepared = (try artifact_producer_baseline.prepareRaft(scratch, self.core.store)) orelse {
            var validation = (try artifact_producer_validation.prepareRaft(scratch, self.core.store)) orelse {
                self.artifact_producer_baseline_pending.store(!retired, .release);
                return retired;
            };
            defer validation.deinit();
            const dispatcher = self.local_execution.artifact_publication_dispatcher orelse {
                self.artifact_producer_baseline_pending.store(!retired, .release);
                return false;
            };
            const encoded = try artifact_publication_transport_codec.encodeAlloc(scratch, validation.command);
            defer scratch.free(encoded);
            try dispatcher.enqueue(dispatcher.ptr, validation.command.namespace, encoded);
            self.artifact_producer_baseline_pending.store(true, .release);
            return false;
        };
        defer prepared.deinit();
        const dispatcher = self.local_execution.artifact_publication_dispatcher orelse {
            self.artifact_producer_baseline_pending.store(!retired, .release);
            return false;
        };
        const encoded = try artifact_publication_transport_codec.encodeAlloc(scratch, prepared.command);
        defer scratch.free(encoded);
        try dispatcher.enqueue(dispatcher.ptr, prepared.command.namespace, encoded);
        // Queue admission is not a baseline certificate. The next pass
        // observes only progress installed by the replicated transaction.
        self.artifact_producer_baseline_pending.store(true, .release);
        return false;
    }
    const complete = try artifact_producer_baseline.page(scratch, self.core.store);
    self.artifact_producer_baseline_pending.store(!complete or !retired, .release);
    return complete and retired;
}

pub fn runArtifactRepairMaintenanceTurn(self: *DB) !void {
    if (self.local_execution.source_vectors.load(.acquire)) |source| try source.checkpointMaintenance();
    const independent = if (self.local_execution.source_vectors.load(.acquire)) |source| source.independent_scan else false;
    const now = self.independentMaintenanceNowNs();
    if (!independent or now >= self.artifact_repair_metadata_due_ns)
        self.artifact_repair_metadata_pending = self.artifactRepairMetadataRebuildPending();
    if (independent) {
        const source = self.local_execution.source_vectors.load(.acquire).?;
        try source.advanceMarkingSnapshot();
        // Immutable scan turns bypass apply/catalog work until metadata
        // is due. State survives scheduler yields, not a pinned thread.
        if (now >= self.artifact_repair_metadata_due_ns or !source.continueScanWithoutApply()) {
            self.artifact_repair_metadata_due_ns = now +| artifact_repair_metadata_active_poll_ns;
            self.artifact_repair_metadata_pending = try self.runArtifactRepairMetadataMaintenanceAfterScan();
        }
    } else {
        self.artifact_repair_metadata_pending = try self.runArtifactRepairMetadataMaintenancePass();
    }
}

pub fn isRecoverableEmbeddingArtifactError(err: anyerror) bool {
    return switch (err) {
        error.InvalidArtifactHeader,
        error.InvalidArtifactMagic,
        error.UnsupportedArtifactCodecVersion,
        error.InvalidArtifactKind,
        error.InvalidArtifactPayload,
        error.InvalidVectorDimensions,
        error.InvalidSparseEmbedding,
        => true,
        else => false,
    };
}

pub fn freeSparseArtifactRebuildWrites(alloc: Allocator, writes: *std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite)) void {
    for (writes.items) |write| {
        alloc.free(write.doc_key);
        if (write.artifact_key) |artifact_key| alloc.free(artifact_key);
        if (write.indices.len > 0) alloc.free(write.indices);
        if (write.values.len > 0) alloc.free(write.values);
    }
    writes.clearRetainingCapacity();
}

pub fn artifactSourceTargetSequence(self: *DB, alloc: Allocator, artifact_name: []const u8, published_sequence: u64, aggregate_target_sequence: u64) !u64 {
    const key = try internal_keys.artifactSourceRevisionKeyAlloc(alloc, artifact_name);
    defer alloc.free(key);
    const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
        // Old stores and imported replay tails predate source markers.
        // Attribute their aggregate debt conservatively to every source;
        // this may delay a per-source ready signal but can never produce a
        // false one. New commits immediately establish exact markers.
        error.NotFound => return @max(published_sequence, aggregate_target_sequence),
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != @sizeOf(u64)) return error.InvalidArtifactSourceRevision;
    return @max(published_sequence, std.mem.readInt(u64, raw[0..8], .big));
}

pub fn ensureHierarchyNavigationArtifactsStillPublished(
    self: *DB,
    alloc: Allocator,
    parent_id: []const u8,
    artifacts: []const HierarchyNavigationArtifact,
    req: types.SearchRequest,
) !void {
    for (artifacts, 0..) |artifact, i| {
        if (i % hierarchy_navigation_cancel_stride == 0) try checkHierarchyNavigationCancelled(req);
        const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, parent_id, "asset", artifact.name);
        defer alloc.free(manifest_key);
        const manifest = try self.core.getStoreValue(alloc, manifest_key) orelse
            return error.StorageReadTemporarilyUnavailable;
        defer alloc.free(manifest);
        const info = try documentUnitHierarchyManifestInfo(alloc, manifest);
        if (info.failed) return error.DocumentArtifactPublicationFailed;
        if (info.published_generation == null or info.published_generation.? != artifact.generation) {
            return error.StorageReadTemporarilyUnavailable;
        }
    }
}

test "ordered artifact inventory authored ingress differs from unhashed provider output" {
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, testEmbeddingArtifactOrigins, .{});
}

test "ordered artifact inventory DB authored acceptance survives projection and reopen" {
    const alloc = std.testing.allocator;
    const publication = artifact_publication;
    const acceptance = artifact_authored_acceptance;
    for ([_]bool{ false, true }) |typed| {
        var directory = try TestDirectory.init("authored-acceptance");
        defer directory.cleanup();
        const options: OpenOptions = .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} }, .online_source_authority = .native, .identity_namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 14 } };
        var db = try DB.open(alloc, directory.path(), options);
        defer db.close();
        if (typed) try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"v":{"type":"embedding"}},"additionalProperties":false}}}}
        );
        try db.addIndex(.{ .name = "vector", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2}" });
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            var namespace: publication.Namespace = undefined;
            doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
            const catalog = try artifact_inventory.local(&txn);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = catalog.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try txn.commit();
        }
        const artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "vector");
        defer alloc.free(artifact);
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"v\":[1,2]}" }}, .sync_level = .full_index });
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try acceptance.readCurrent(&read, db.root_incarnation, "doc", artifact)) != null);
        }
        const initial_timestamp = try db.getTimestamp(alloc, "doc");
        const transaction_id = try db.beginTransaction(initial_timestamp + 1);
        try db.writeTransaction(transaction_id, .{ .writes = &.{.{ .key = "doc", .value = "{\"v\":[3,4]}" }} });
        // Prepared intent recovery must reconstruct the same authored work;
        // no borrowed preparation or participant survives this reopen.
        db.close();
        db = try DB.open(alloc, directory.path(), options);
        try db.resolveTransactionIntentsWithSyncLevel(transaction_id, .committed, initial_timestamp + 2, .full_index);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const accepted = (try acceptance.readCurrent(&read, db.root_incarnation, "doc", artifact)) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(initial_timestamp + 2, accepted.source.timestamp);
            const vector = try enrichment_artifact_codec.decodeDenseEmbeddingAlloc(alloc, try read.get(artifact));
            defer alloc.free(vector);
            try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, vector);
        }
        db.close();
        db = try DB.open(alloc, directory.path(), options);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try acceptance.readCurrent(&read, db.root_incarnation, "doc", artifact)) != null);
        }
    }
}

test "artifact repair resource limits remain terminal until input changes" {
    const alloc = std.testing.allocator;
    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .reason = .resource_limit_exceeded,
    };
    defer issue.deinit(alloc);

    try applyArtifactRepairability(alloc, &issue);
    try std.testing.expect(!issue.repairable);
    try std.testing.expectEqualStrings("resource_limit_requires_source_or_config_change", issue.unsupported_reason);

    issue.reason = .corrupt_artifact;
    try applyArtifactRepairability(alloc, &issue);
    try std.testing.expect(issue.repairable);
    try std.testing.expectEqualStrings("", issue.unsupported_reason);
}

test "artifact repair enrichment generation attempts are exact per failure window" {
    try std.testing.expectEqual(@as(u64, 6), mergeGenerationFailureAttempts(
        .enrichment_failed,
        41,
        "EmbedRateLimited",
        6,
        .enrichment_failed,
        41,
        "EmbedRateLimited",
        4,
    ));
    try std.testing.expectEqual(@as(u64, 1), mergeGenerationFailureAttempts(
        .enrichment_failed,
        41,
        "EmbedRateLimited",
        6,
        .enrichment_failed,
        42,
        "EmbedRateLimited",
        1,
    ));
    try std.testing.expectEqual(@as(u64, 2), mergeGenerationFailureAttempts(
        .enrichment_failed,
        42,
        "EmbedRateLimited",
        6,
        .enrichment_failed,
        42,
        "ProviderUnavailable",
        2,
    ));
}

test "db artifact repair summary is persisted for status-only reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 7,
        };
        defer issue.deinit(alloc);
        try db.recordArtifactRepairIssue(alloc, issue);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expect(stats.repair_issue_count_estimated);
        try std.testing.expectEqual(@as(u64, 1), stats.repair_issue_count);
    }

    var status_db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .status_only,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer status_db.close();

    const status_stats = try status_db.stats(alloc);
    defer types.freeDBStats(alloc, status_stats);
    try std.testing.expect(status_stats.repair_degraded);
    try std.testing.expect(!status_stats.repair_summary_ready);
    try std.testing.expect(status_stats.repair_issue_count_estimated);
    try std.testing.expectEqual(@as(u64, 1), status_stats.repair_issue_count);
}

test "db artifact repair summary rebuild is bounded and exact until ready" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const seeded_count: usize = 1030;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        for (0..seeded_count) |i| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
            defer alloc.free(doc_key);
            var issue = types.ArtifactRepairIssue{
                .artifact_kind = .asset,
                .index_name = try alloc.dupe(u8, "document_units_v1"),
                .doc_key = try alloc.dupe(u8, doc_key),
                .artifact_name = try alloc.dupe(u8, "document_units_v1"),
                .reason = .corrupt_artifact,
                .sequence = @intCast(i + 1),
            };
            defer issue.deinit(alloc);
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);
            const value = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
            defer alloc.free(value);
            try db.core.store.put(key, value);
        }
        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
        defer alloc.free(root_summary_key);
        try db.core.store.putBatch(&.{}, &.{ ready_key, progress_key, root_summary_key });
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const progress = try db.core.store.get(alloc, progress_key);
        defer alloc.free(progress);

        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, ready_key));

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expect(stats.repair_issue_count_estimated);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const ready = try db.core.store.get(alloc, ready_key);
        defer alloc.free(ready);

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, progress_key));

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(stats.repair_summary_ready);
        try std.testing.expect(!stats.repair_issue_count_estimated);
        try std.testing.expectEqual(@as(u64, seeded_count), stats.repair_issue_count);
    }
}

test "db artifact repair summary rebuild invalidates partial counters on mutation" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const seeded_count: usize = 1030;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        for (0..seeded_count) |i| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
            defer alloc.free(doc_key);
            var issue = types.ArtifactRepairIssue{
                .artifact_kind = .asset,
                .index_name = try alloc.dupe(u8, "document_units_v1"),
                .doc_key = try alloc.dupe(u8, doc_key),
                .artifact_name = try alloc.dupe(u8, "document_units_v1"),
                .reason = .corrupt_artifact,
                .sequence = @intCast(i + 1),
            };
            defer issue.deinit(alloc);
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);
            const value = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
            defer alloc.free(value);
            try db.core.store.put(key, value);
        }
        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
        defer alloc.free(root_summary_key);
        try db.core.store.putBatch(&.{}, &.{ ready_key, progress_key, root_summary_key });
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);

        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:9999"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 9999,
        };
        defer issue.deinit(alloc);
        try db.recordArtifactRepairIssue(alloc, issue);

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const progress = try db.core.store.get(alloc, progress_key);
        defer alloc.free(progress);
        try std.testing.expectEqualStrings(artifact_repair_summary_dirty_marker, progress);
        const rebuild_root_summary_key = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
        defer alloc.free(rebuild_root_summary_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, rebuild_root_summary_key));

        const dirty_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, dirty_stats);
        try std.testing.expect(dirty_stats.repair_degraded);
        try std.testing.expect(!dirty_stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), dirty_stats.repair_issue_count);
    }

    {
        var status_db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer status_db.close();

        const stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const ready = try db.core.store.get(alloc, ready_key);
        defer alloc.free(ready);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, seeded_count + 1), stats.repair_issue_count);
    }
}

test "db artifact repair summary durably scopes debt to configured source" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .start_optional_runtime_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "semantic_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
        });
        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .embedding,
            .index_name = try alloc.dupe(u8, "semantic_idx"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .source_artifact_name = try alloc.dupe(u8, "body_chunks_v1"),
            .artifact_name = try alloc.dupe(u8, "chunk_dense_v1"),
            .reason = .enrichment_failed,
            .sequence = 7,
        };
        defer issue.deinit(alloc);
        try db.recordArtifactRepairIssue(alloc, issue);

        const dirty = try db.stats(alloc);
        defer types.freeDBStats(alloc, dirty);
        try std.testing.expectEqual(@as(usize, 1), dirty.indexes[0].source_replay.len);
        try std.testing.expect(!dirty.indexes[0].source_replay[0].repair_summary_ready);
        try std.testing.expect(!dirty.indexes[0].source_replay[0].failed);

        try db.runUntilIdle();
        const ready = try db.stats(alloc);
        defer types.freeDBStats(alloc, ready);
        try std.testing.expect(ready.indexes[0].source_replay[0].repair_summary_ready);
        try std.testing.expect(ready.indexes[0].source_replay[0].failed);
        try std.testing.expectEqual(@as(u64, 1), ready.indexes[0].source_replay[0].repair_issue_count);
    }

    // The source-local failure remains authoritative without a live enrichment
    // runtime and after process restart.
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.indexes[0].source_replay[0].failed);
    try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].source_replay[0].repair_issue_count);
}

test "db artifact repair v1 ready marker forces source-summary rebuild" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer t.deinit();
    const db = &t.db;
    const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
    defer alloc.free(ready_key);
    try db.core.store.put(ready_key, "1");
    try std.testing.expect(!try db.artifactRepairSummaryReady(alloc));
    try std.testing.expect(!try db.rebuildArtifactRepairSummaryIfMissing(alloc));
    try std.testing.expect(try db.artifactRepairSummaryReady(alloc));
    const upgraded = try db.core.store.get(alloc, ready_key);
    defer alloc.free(upgraded);
    try std.testing.expectEqualStrings(artifact_repair_summary_ready_marker, upgraded);
}

test "db artifact repair dirty marker drains shadow summaries in bounded pages" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer t.deinit();
    const db = &t.db;

    var encoded_count: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded_count, 1, .little);
    for (0..artifact_repair_summary_invalidation_page_size + 1) |i| {
        const index_name = try std.fmt.allocPrint(alloc, "index-{d:0>4}", .{i});
        defer alloc.free(index_name);
        const key = try internal_keys.artifactRepairSummaryRebuildIndexKeyAlloc(alloc, index_name);
        defer alloc.free(key);
        try db.core.store.put(key, &encoded_count);
    }

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (writes.items) |write| alloc.free(@constCast(write.value));
        writes.deinit(alloc);
    }
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(@constCast(key));
        owned_keys.deinit(alloc);
    }
    try appendArtifactRepairSummaryDirtyMarkerForStore(alloc, &writes, &deletes, &owned_keys);
    try db.core.store.putBatch(writes.items, deletes.items);

    try std.testing.expect(try db.rebuildArtifactRepairSummaryIfMissing(alloc));
    const rebuild_prefix = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
    defer alloc.free(rebuild_prefix);
    {
        const rows = try db.core.store.scanPrefixPage(
            alloc,
            rebuild_prefix,
            null,
            artifact_repair_summary_invalidation_page_size + 1,
        );
        defer docstore_mod.DocStore.freeResults(alloc, rows);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
    }
    const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
    defer alloc.free(progress_key);
    {
        const progress = try db.core.store.get(alloc, progress_key);
        defer alloc.free(progress);
        try std.testing.expectEqualStrings(artifact_repair_summary_dirty_marker, progress);
    }

    try std.testing.expect(try db.rebuildArtifactRepairSummaryIfMissing(alloc));
    {
        const rows = try db.core.store.scanPrefixPage(alloc, rebuild_prefix, null, 1);
        defer docstore_mod.DocStore.freeResults(alloc, rows);
        try std.testing.expectEqual(@as(usize, 0), rows.len);
    }
    try std.testing.expect(!try db.rebuildArtifactRepairSummaryIfMissing(alloc));
    try std.testing.expect(try db.artifactRepairSummaryReady(alloc));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, progress_key));
}

test "db artifact repair summary store writer invalidates partial rebuild" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const seeded_count: usize = 1030;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        for (0..seeded_count) |i| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
            defer alloc.free(doc_key);
            var issue = types.ArtifactRepairIssue{
                .artifact_kind = .asset,
                .index_name = try alloc.dupe(u8, "document_units_v1"),
                .doc_key = try alloc.dupe(u8, doc_key),
                .artifact_name = try alloc.dupe(u8, "document_units_v1"),
                .reason = .corrupt_artifact,
                .sequence = @intCast(i + 1),
            };
            defer issue.deinit(alloc);
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);
            const value = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
            defer alloc.free(value);
            try db.core.store.put(key, value);
        }
        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
        defer alloc.free(root_summary_key);
        try db.core.store.putBatch(&.{}, &.{ ready_key, progress_key, root_summary_key });
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);

        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:0001-extra"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 9999,
        };
        defer issue.deinit(alloc);
        const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
        defer alloc.free(key);
        try saveArtifactRepairIssueToStoreWithSummary(alloc, db.core.store, key, issue, true, &.{}, &.{});

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const progress = try db.core.store.get(alloc, progress_key);
        defer alloc.free(progress);
        try std.testing.expectEqualStrings(artifact_repair_summary_dirty_marker, progress);
        const rebuild_root_summary_key = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
        defer alloc.free(rebuild_root_summary_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, rebuild_root_summary_key));
        const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
        defer alloc.free(root_summary_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, root_summary_key));

        const dirty_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, dirty_stats);
        try std.testing.expect(dirty_stats.repair_degraded);
        try std.testing.expect(!dirty_stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), dirty_stats.repair_issue_count);
    }

    {
        var status_db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer status_db.close();

        const stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const ready = try db.core.store.get(alloc, ready_key);
        defer alloc.free(ready);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, seeded_count + 1), stats.repair_issue_count);
    }
}

test "db artifact repair metadata maintenance drains summary rebuild without restart" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const seeded_count: usize = 2050;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        for (0..seeded_count) |i| {
            const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
            defer alloc.free(doc_key);
            var issue = types.ArtifactRepairIssue{
                .artifact_kind = .asset,
                .index_name = try alloc.dupe(u8, "document_units_v1"),
                .doc_key = try alloc.dupe(u8, doc_key),
                .artifact_name = try alloc.dupe(u8, "document_units_v1"),
                .reason = .corrupt_artifact,
                .sequence = @intCast(i + 1),
            };
            defer issue.deinit(alloc);
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);
            const value = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
            defer alloc.free(value);
            try db.core.store.put(key, value);
        }
        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
        defer alloc.free(root_summary_key);
        try db.core.store.putBatch(&.{}, &.{ ready_key, progress_key, root_summary_key });
    }

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const partial = try db.stats(alloc);
    defer types.freeDBStats(alloc, partial);
    try std.testing.expect(partial.repair_degraded);
    try std.testing.expect(!partial.repair_summary_ready);
    try std.testing.expectEqual(@as(u64, 1024), partial.repair_issue_count);
    try std.testing.expect(db.pendingWorkStats().repair_metadata_rebuild_pending);

    try db.runUntilIdle();

    const drained = try db.stats(alloc);
    defer types.freeDBStats(alloc, drained);
    try std.testing.expect(drained.repair_degraded);
    try std.testing.expect(drained.repair_summary_ready);
    try std.testing.expectEqual(@as(u64, seeded_count), drained.repair_issue_count);
    try std.testing.expect(!db.pendingWorkStats().repair_metadata_rebuild_pending);
}

test "db artifact repair list cursor pages durable repair debt" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    var first_issue = types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "document_units_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_key = try alloc.dupe(u8, "asset:0001"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer first_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, first_issue);

    var second_issue = types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "document_units_v1"),
        .doc_key = try alloc.dupe(u8, "doc:b"),
        .artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_key = try alloc.dupe(u8, "asset:0002"),
        .reason = .corrupt_artifact,
        .sequence = 2,
    };
    defer second_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, second_issue);

    var first_page = try db.listArtifactRepairIssuesPage(alloc, .{ .artifact_kind = .asset, .limit = 1 });
    defer first_page.deinit(alloc);
    try std.testing.expect(first_page.has_more);
    try std.testing.expect(first_page.next_cursor != null);
    try std.testing.expectEqual(@as(usize, 1), first_page.issues.len);
    try std.testing.expectEqualStrings("doc:a", first_page.issues[0].doc_key);

    var second_page = try db.listArtifactRepairIssuesPage(alloc, .{ .artifact_kind = .asset, .limit = 1, .cursor = first_page.next_cursor.? });
    defer second_page.deinit(alloc);
    try std.testing.expect(!second_page.has_more);
    try std.testing.expect(second_page.next_cursor == null);
    try std.testing.expectEqual(@as(usize, 1), second_page.issues.len);
    try std.testing.expectEqualStrings("doc:b", second_page.issues[0].doc_key);
}

test "db artifact repair queue keeps distinct unit artifacts for same document and name" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    var first_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .parent_doc_key = try alloc.dupe(u8, "doc:a"),
        .unit_id = try alloc.dupe(u8, "page:000001"),
        .source_artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_name = try alloc.dupe(u8, "body_chunks_v1"),
        .artifact_key = try alloc.dupe(u8, "aaaa"),
        .chunk_id = 0,
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer first_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, first_issue);

    var second_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .parent_doc_key = try alloc.dupe(u8, "doc:a"),
        .unit_id = try alloc.dupe(u8, "page:000002"),
        .source_artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_name = try alloc.dupe(u8, "body_chunks_v1"),
        .artifact_key = try alloc.dupe(u8, "bbbb"),
        .chunk_id = 0,
        .reason = .corrupt_artifact,
        .sequence = 2,
    };
    defer second_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, second_issue);

    const issues = try db.listArtifactRepairIssues(alloc, .chunk, "body_chunks_v1", 10);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 2), issues.len);
    try std.testing.expect(issues[0].repairable);
    try std.testing.expectEqualStrings("", issues[0].unsupported_reason);
    try std.testing.expectEqualStrings("page:000001", issues[0].unit_id);
    try std.testing.expectEqualStrings("page:000002", issues[1].unit_id);
}

test "db artifact repair fallback issue ids do not collide on delimiter-bearing fields" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    var first_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a\x1fbody"),
        .artifact_name = try alloc.dupe(u8, "chunks"),
        .unit_id = try alloc.dupe(u8, "page:1"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer first_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, first_issue);

    var second_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "body\x1fchunks"),
        .unit_id = try alloc.dupe(u8, "page:1"),
        .reason = .corrupt_artifact,
        .sequence = 2,
    };
    defer second_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, second_issue);

    const issues = try db.listArtifactRepairIssues(alloc, .chunk, "body_chunks_v1", 10);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 2), issues.len);
}

test "db artifact repair kind filter uses selective index after reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var asset_issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 1,
        };
        defer asset_issue.deinit(alloc);
        const asset_key = try artifactRepairIssueKeyForIssueAlloc(alloc, asset_issue);
        defer alloc.free(asset_key);
        const asset_value = try encodeArtifactRepairIssueValueAlloc(alloc, asset_issue);
        defer alloc.free(asset_value);
        try db.core.store.put(asset_key, asset_value);

        var graph_issue = types.ArtifactRepairIssue{
            .artifact_kind = .graph,
            .index_name = try alloc.dupe(u8, "entity_graph_v1"),
            .doc_key = try alloc.dupe(u8, "doc:g"),
            .artifact_name = try alloc.dupe(u8, "entity_graph_v1"),
            .reason = .corrupt_artifact,
            .sequence = 2,
        };
        defer graph_issue.deinit(alloc);
        const graph_key = try artifactRepairIssueKeyForIssueAlloc(alloc, graph_issue);
        defer alloc.free(graph_key);
        const graph_value = try encodeArtifactRepairIssueValueAlloc(alloc, graph_issue);
        defer alloc.free(graph_value);
        try db.core.store.put(graph_key, graph_value);

        const ready_key = try internal_keys.artifactRepairKindIndexReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        try db.core.store.putBatch(&.{}, &.{ready_key});
    }

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var page = try db.listArtifactRepairIssuesPage(alloc, .{ .artifact_kind = .graph, .limit = 1 });
    defer page.deinit(alloc);
    try std.testing.expect(!page.has_more);
    try std.testing.expectEqual(@as(u64, 1), page.scanned);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expectEqual(.graph, page.issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:g", page.issues[0].doc_key);
}

test "db artifact repair kind fallback scan is bounded when kind index is rebuilding" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    const fallback_scan_budget: u64 = 256;
    var i: usize = 0;
    while (i < fallback_scan_budget + 8) : (i += 1) {
        const doc_key = try std.fmt.allocPrint(alloc, "doc:embedding:{d:0>4}", .{i});
        defer alloc.free(doc_key);
        const artifact_key = try std.fmt.allocPrint(alloc, "embedding:{d:0>4}", .{i});
        defer alloc.free(artifact_key);
        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .embedding,
            .index_name = try alloc.dupe(u8, "idx"),
            .doc_key = try alloc.dupe(u8, doc_key),
            .artifact_name = try alloc.dupe(u8, "idx"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .reason = .missing_artifact,
            .sequence = @intCast(i + 1),
        };
        defer issue.deinit(alloc);
        const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
        defer alloc.free(key);
        const value = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
        defer alloc.free(value);
        try db.core.store.put(key, value);
    }

    var graph_issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "idx"),
        .doc_key = try alloc.dupe(u8, "doc:graph"),
        .artifact_name = try alloc.dupe(u8, "idx"),
        .artifact_key = try alloc.dupe(u8, "graph:0001"),
        .reason = .corrupt_artifact,
        .sequence = 1000,
    };
    defer graph_issue.deinit(alloc);
    const graph_key = try artifactRepairIssueKeyForIssueAlloc(alloc, graph_issue);
    defer alloc.free(graph_key);
    const graph_value = try encodeArtifactRepairIssueValueAlloc(alloc, graph_issue);
    defer alloc.free(graph_value);
    try db.core.store.put(graph_key, graph_value);

    const ready_key = try internal_keys.artifactRepairKindIndexReadyKeyAlloc(alloc);
    defer alloc.free(ready_key);
    try db.core.store.putBatch(&.{}, &.{ready_key});

    var first_page = try db.listArtifactRepairIssuesPage(alloc, .{ .artifact_kind = .graph, .index_name = "idx", .limit = 1 });
    defer first_page.deinit(alloc);
    try std.testing.expect(first_page.has_more);
    try std.testing.expect(first_page.next_cursor != null);
    try std.testing.expectEqual(fallback_scan_budget, first_page.scanned);
    try std.testing.expectEqual(@as(usize, 0), first_page.issues.len);

    var second_page = try db.listArtifactRepairIssuesPage(alloc, .{
        .artifact_kind = .graph,
        .index_name = "idx",
        .limit = 1,
        .cursor = first_page.next_cursor,
    });
    defer second_page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_page.issues.len);
    try std.testing.expectEqual(.graph, second_page.issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:graph", second_page.issues[0].doc_key);
}

test "db artifact repair reports unsupported artifact kinds without clearing debt" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "entity_graph_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "entity_graph_v1"),
        .reason = .corrupt_artifact,
        .sequence = 9,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{ .artifact_kind = .graph, .limit = 10 });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expectEqual(@as(u64, 1), repair.unsupported);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(!repair.has_more);
    try std.testing.expect(repair.next_cursor == null);
    try std.testing.expect(repair.debt_remaining);

    const issues = try db.listArtifactRepairIssues(alloc, .graph, "entity_graph_v1", 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(@as(u64, 1), issues[0].attempts);
    try std.testing.expect(!issues[0].repairable);
    try std.testing.expectEqualStrings("graph_reprocessor_unavailable", issues[0].unsupported_reason);
    try std.testing.expectEqualStrings("graph_reprocessor_unavailable", issues[0].last_error);
}

test "db artifact repair issue list reports recorded graph issue" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.graph_v1);

    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "graph_v1"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    var page = try db.listArtifactRepairIssuesPage(alloc, .{
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 10,
    });
    defer page.deinit(alloc);

    try std.testing.expect(!page.has_more);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expectEqual(.graph, page.issues[0].artifact_kind);
    try std.testing.expectEqual(.corrupt_artifact, page.issues[0].reason);
    try std.testing.expectEqualStrings("graph_v1", page.issues[0].index_name);
    try std.testing.expectEqualStrings("", page.issues[0].last_error);
}

test "db artifact repair reports remaining debt when source is missing" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.graph_v1);

    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .doc_key = try alloc.dupe(u8, "doc:missing"),
        .artifact_name = try alloc.dupe(u8, "graph_v1"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), stats.repair_issue_count);
}

test "db artifact repair records corrupt graph edge artifacts during replay" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    const artifact_key = blk: {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(test_index.relations_graph);

        const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
        errdefer alloc.free(key);
        try db.core.store.put(key, "bad-graph-artifact");
        const batch = derived_types.DerivedBatch{
            .changed_artifact_keys = &.{key},
        };
        appended_sequence = try appendDerivedBatchRecord(&db, batch);
        break :blk key;
    };
    defer alloc.free(artifact_key);

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listArtifactRepairIssues(alloc, .graph, "relations_graph", 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.graph, issues[0].artifact_kind);
    try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("mentions:doc:b", issues[0].artifact_name);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    try std.testing.expectEqualStrings(artifact_key_hex, issues[0].artifact_key);

    const raw_artifact = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);
    try std.testing.expectEqualStrings("bad-graph-artifact", raw_artifact);

    const graph_applied = try reopened.core.loadAppliedSequence(alloc, "relations_graph");
    try std.testing.expectEqual(@as(u64, 0), graph_applied);
    try std.testing.expect(appended_sequence > graph_applied);
}

test "db artifact repair records corrupt graph source asset artifacts during replay" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var appended_sequence: u64 = 0;
    const artifact_key = blk: {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(test_index.relations_graph);

        const key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
        errdefer alloc.free(key);
        try db.core.store.put(key, "bad-json");
        const batch = derived_types.DerivedBatch{
            .changed_artifact_keys = &.{key},
        };
        appended_sequence = try appendDerivedBatchRecord(&db, batch);
        break :blk key;
    };
    defer alloc.free(artifact_key);

    var reopened = try DB.open(alloc, std.mem.span(path), no_background_test_open);
    defer reopened.close();

    const issues = try reopened.listArtifactRepairIssues(alloc, .asset, "relations_graph", 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.asset, issues[0].artifact_kind);
    try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("relations_v1", issues[0].artifact_name);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    try std.testing.expectEqualStrings(artifact_key_hex, issues[0].artifact_key);

    const raw_artifact = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);
    try std.testing.expectEqualStrings("bad-json", raw_artifact);

    const graph_applied = try reopened.core.loadAppliedSequence(alloc, "relations_graph");
    try std.testing.expectEqual(@as(u64, 0), graph_applied);
    try std.testing.expect(appended_sequence > graph_applied);
}

test "db artifact dense reset waits for catalog readers before closing storage" {
    if (builtin.single_threaded) return error.SkipZigTest;
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
    try db.addIndex(test_index.dense_idx_dims3_external);

    // Native publication holds this same catalog pin while reading the live
    // posting generation, independently of the DB apply lock.
    var reader = db.tryAcquireIndexCatalogReadLease() orelse return error.TestUnexpectedResult;
    var reader_held = true;
    defer if (reader_held) reader.release();
    const Reset = struct {
        db: *DB,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) !void {
            defer self.done.store(true, .release);
            try resetDenseIndexForArtifactRebuildForTest(self.db, "dense_idx");
        }
    };
    var reset = Reset{ .db = &db };
    var future = try std.testing.io.concurrent(Reset.run, .{&reset});
    var joined = false;
    defer if (!joined) {
        if (reader_held) {
            reader.release();
            reader_held = false;
        }
        future.await(std.testing.io) catch {};
    };
    const deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!db.indexCatalogBarrierActive() and !reset.done.load(.acquire)) {
        if (monotonicTimeNs() >= deadline) return error.TestTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(db.indexCatalogBarrierActive());
    try std.testing.expect(!reset.done.load(.acquire));
    if (db.tryAcquireIndexCatalogReadLease()) |lease| {
        var unexpected = lease;
        unexpected.release();
        return error.TestUnexpectedResult;
    }
    // The old generation remains readable until its final catalog pin leaves.
    try std.testing.expect(db.core.index_manager.denseIndex("dense_idx").?.index.experimentalPostingDurableAppliedSequence() != null);
    reader.release();
    reader_held = false;
    const result = future.await(std.testing.io);
    joined = true;
    try result;
    try std.testing.expect(!db.indexCatalogBarrierActive());
    var reopened_reader = db.tryAcquireIndexCatalogReadLease() orelse return error.TestUnexpectedResult;
    reopened_reader.release();
}

test "artifact text replay deletes only keys owned by its source projection" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 64,
    });
    try db.addEnrichment(.{
        .name = "notes_chunks_v1",
        .kind = .chunk,
        .field = "notes",
        .chunk_size = 64,
    });
    try db.addIndex(.{
        .name = "body_text",
        .kind = .full_text,
        .config_json = "{\"sources\":[{\"artifact\":\"body_chunks_v1\"}]}",
    });

    const body_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc-a", "body_chunks_v1", 0);
    defer alloc.free(body_key);
    const notes_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc-a", "notes_chunks_v1", 0);
    defer alloc.free(notes_key);

    const body_target = [_]derived_types.DerivedTargetRef{.{
        .kind = .full_text,
        .index_name = "body_text",
    }};
    const other_target = [_]derived_types.DerivedTargetRef{.{
        .kind = .full_text,
        .index_name = "notes_text",
    }};
    const replay_deletes = [_][]const u8{ body_key, notes_key, "doc-a" };
    const replay_overwrites = [_][]const u8{ notes_key, "doc-b" };
    const replay_documents = [_]derived_types.DerivedDocument{
        .{ .key = body_key, .action = .upsert, .targets = &body_target },
        .{ .key = notes_key, .action = .upsert, .targets = &other_target },
    };

    var publication = try db.core.index_manager.acquireTextPublicationContext(alloc, "body_text");
    defer publication.deinit();
    var apply_guard = try db.core.index_manager.lockManagedIndexApply(.{
        .name = "body_text",
        .kind = .full_text,
    });
    defer apply_guard.unlock();
    const keys = try collectTextReplayDeleteKeys(
        alloc,
        db.core.index_manager,
        .{
            .deleted_keys = &replay_deletes,
            .overwritten_doc_keys = &replay_overwrites,
            .documents = &replay_documents,
        },
        "body_text",
        publication,
    );
    defer alloc.free(keys);

    try std.testing.expectEqual(@as(usize, 1), keys.len);
    try std.testing.expectEqualStrings(body_key, keys[0]);
}

test "db artifact dense target prefers current incarnation outcomes over stale name counter" {
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

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"body\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dense_idx\"}}",
        .coverage_generation = 42,
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    // Same-name recreation can leave this legacy counter stale while the
    // current generation's outcome accounting is complete.
    const counter_key = try DB.denseArtifactTargetCounterKeyAlloc(alloc, config.name);
    defer alloc.free(counter_key);
    var stale_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &stale_value, 1, .little);
    try db.core.store.put(counter_key, &stale_value);

    try std.testing.expectEqual(
        @as(?u64, 2),
        try denseTargetCountForIndexContext(db.async_context, config.name),
    );

    // A later source has no embedding yet. The certified generation must
    // still advance across source-only replay records; final source coverage
    // and shadow cutover must remain pending on that same durable state.
    db.async_context.enrichment_desired_running.store(false, .release);
    db.enrichment_runtime.?.stop();
    const applied = try db.core.loadAppliedSequence(alloc, config.name);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:pending", .value = "{\"body\":\"provider pending\"}" }},
        .sync_level = .write,
    });
    const target = db.core.nextDerivedSequence();
    try std.testing.expect(target > applied);
    try std.testing.expectEqual(@as(?u64, null), try denseTargetCountForIndexContext(db.async_context, config.name));
    try std.testing.expectEqual(@as(?u64, 2), try denseTargetCountForIndexContextWithCoverage(db.async_context, config.name, .materialized));
    try std.testing.expect(try canAdvanceDerivedToTargetAsync(db.async_context, .{
        .name = config.name,
        .kind = .dense_vector,
    }, applied, target));
}

test "db ordered artifact inventory chunk projection streams pinned snapshots and releases allocation faults" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/chunk-projection", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    const root = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc\x00", "chunks", 0);
    defer alloc.free(root);
    const unit = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc\x00", "chunks", "unit", 0);
    defer alloc.free(unit);
    const other = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc\x00other", "chunks", 0);
    defer alloc.free(other);
    const binary = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc\x00", "escaped\x00", 0);
    defer alloc.free(binary);
    try db.core.store.put(root, "{\"body\":\"old\",\"_source_field\":\"body\",\"_chunk_id\":0}");
    try db.core.store.put(unit, "{\"body\":\"unit\",\"_source_field\":\"body\",\"_chunk_id\":0}");
    try db.core.store.put(other, "{\"body\":\"unrelated\",\"_source_field\":\"body\",\"_chunk_id\":0}");
    try db.core.store.put(binary, "{\"body\":\"binary\",\"_source_field\":\"body\",\"_chunk_id\":0}");
    var read = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    try db.core.store.put(root, "{\"body\":\"new\",\"_source_field\":\"body\",\"_chunk_id\":0}");
    const Harness = struct {
        fn project(a: Allocator, owner: *DB, snapshot: *docstore_mod.DocStore.Txn) !void {
            var value = (try loadChunkFieldValueTxn(owner, a, "doc\x00", snapshot)) orelse return error.TestUnexpectedResult;
            defer freeJsonValue(a, &value);
            const members = value.object.get("chunks").?.array.items;
            try std.testing.expectEqual(@as(usize, 2), members.len);
            try std.testing.expectEqualStrings("old", members[0].object.get("body").?.string);
            try std.testing.expectEqualStrings("unit", members[1].object.get("body").?.string);
            try std.testing.expectEqual(@as(usize, 2), value.object.count());
            try std.testing.expectEqualStrings("binary", value.object.get("escaped\x00").?.array.items[0].object.get("body").?.string);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, Harness.project, .{ &db, &read });
    var current = (try loadChunkFieldValueTxn(&db, alloc, "doc\x00", null)).?;
    defer freeJsonValue(alloc, &current);
    try std.testing.expectEqualStrings("new", current.object.get("chunks").?.array.items[0].object.get("body").?.string);
    try std.testing.expect((try loadChunkFieldValueTxn(&db, alloc, "absent", &read)) == null);
}

test "db ordered artifact inventory staged generations resume and switch snapshot visibility atomically" {
    const alloc = std.testing.allocator;
    const generations = artifact_chunk_generation;
    const chunks = artifact_chunk_manifest;
    const publication = artifact_publication;
    const Guard = struct {
        valid: bool = true,
        pub fn validate(self: @This(), _: anytype) !void {
            if (!self.valid) return error.EnrichmentSourceChanged;
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/staged-generations", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const scope = try chunks.scopedKeyAlloc(alloc, "doc\x00", "chunks", "unit\xff");
    defer alloc.free(scope);
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    var output = chunks.Builder.init();
    try output.append(0, "first");
    try output.append(1, "second");
    const spec = try generations.Spec.init(authority, scope, @splat(3), output.finish(), 1);
    var plan = try generations.Plan.init(alloc, scope, spec);
    defer plan.deinit();
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            // Replica-local old tails may differ. A generation view must not
            // consult those rows for either live ordinals or absent members.
            for ([_]u32{ 0, 99 }) |ordinal| {
                const legacy = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc\x00", "chunks", "unit\xff", ordinal);
                defer alloc.free(legacy);
                try txn.put(legacy, "legacy-local-output");
            }
            try source_authority_mod.bind(&txn, .native, authority.namespace);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try std.testing.expect(try plan.begin(&txn));
            try txn.commit();
        }
        const previous = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk try plan.load(&read);
        };
        var page = try generations.PreparedAppend.init(alloc, &plan, previous, &.{"first"});
        defer page.deinit();
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try std.testing.expect(try page.stage(&plan, &txn));
            try txn.commit();
        }
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try generations.View(docstore_mod.DocStore.Txn).open(alloc, &read, scope)) == null);
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    const previous = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :blk try plan.load(&read);
    };
    try std.testing.expectEqual(@as(u32, 1), previous.progress.count);
    const source_inputs = [_]publication.Source{.{ .document_key = "doc\x00", .content_digest = @splat(0), .timestamp = 1, .input_position = null }};
    const absent_head = [_]publication.ArtifactSource{.{ .key = plan.head_key, .content_digest = null, .input_position = null, .source_index = 0 }};
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try publication.validateArtifactSources(alloc, &read, authority.namespace, &source_inputs, &absent_head);
        // Private staging progress cannot manufacture visible-input revisions.
        try std.testing.expect((try publication.artifactRevision(&read, authority.namespace, scope)) == null);
        try std.testing.expect((try publication.materializationRevision(&read, authority.namespace, "doc\x00")) == null);
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.ArtifactPublicationPending, plan.publish(&txn, null, Guard{}));
    }
    const Check = struct {
        fn prepare(a: Allocator, selected: *const generations.Plan, state: generations.State) !void {
            var page = try generations.PreparedAppend.init(a, selected, state, &.{"second"});
            defer page.deinit();
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, Check.prepare, .{ &plan, previous });
    var page = try generations.PreparedAppend.init(alloc, &plan, previous, &.{"second"});
    defer page.deinit();
    for (0..2) |pass| {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(pass == 0, try page.stage(&plan, &txn));
        try txn.commit();
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.publish(&txn, null, Guard{ .valid = false }));
        try std.testing.expect(try plan.publish(&txn, null, Guard{}));
        // Crash/abort before durable publication must expose no generation.
    }
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try generations.View(docstore_mod.DocStore.Txn).open(alloc, &read, scope)) == null);
        try std.testing.expect((try publication.materializationRevision(&read, authority.namespace, "doc\x00")) == null);
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try plan.publish(&txn, null, Guard{}));
        try txn.commit();
    }
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    var view = (try generations.View(docstore_mod.DocStore.Txn).open(alloc, &pinned, scope)).?;
    defer view.deinit();
    const head_position = (try publication.artifactRevision(&pinned, authority.namespace, plan.head_key)).?;
    try std.testing.expectEqualDeep(head_position, (try publication.artifactRevision(&pinned, authority.namespace, scope)).?);
    try std.testing.expectEqualDeep(head_position, (try publication.materializationRevision(&pinned, authority.namespace, "doc\x00")).?);
    var head_digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(&spec.encode(), &head_digest, .{});
    const head_guard = [_]publication.ArtifactSource{.{ .key = plan.head_key, .content_digest = head_digest, .input_position = head_position, .source_index = 0 }};
    try publication.validateArtifactSources(alloc, &pinned, authority.namespace, &source_inputs, &head_guard);
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateArtifactSources(alloc, &pinned, authority.namespace, &source_inputs, &absent_head));
    const logical = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc\x00", "chunks", "unit\xff", 0);
    defer alloc.free(logical);
    var producer_input: @import("artifact/producer_context.zig").Token = .{
        .arena = std.heap.ArenaAllocator.init(alloc),
        .namespace = authority.namespace,
        .epoch = authority.epoch,
        .catalog_digest = authority.catalog_digest,
        .producer_name = "downstream",
        .producer_kind = .enrichment,
        .producer_generation = 1,
        .artifact_name = "downstream",
        .source = source_inputs[0],
    };
    defer producer_input.deinit();
    {
        var input = try generations.captureInput(alloc, &pinned, logical);
        defer input.deinit();
        try std.testing.expectEqualStrings("first", input.value.?);
        try std.testing.expectEqualStrings(plan.head_key, input.proofKey(logical));
        try input.observe(&producer_input, &pinned, logical);
        try std.testing.expectEqual(@as(usize, 1), producer_input.reads.items.len);
        try publication.validateArtifactSources(alloc, &pinned, authority.namespace, producer_input.sources(), producer_input.reads.items);
        // Storage completion is not a causal producer certificate. A callback
        // must not adopt an old per-member receipt or infer completion itself.
        try std.testing.expectError(error.ArtifactPublicationPending, artifact_asset_publication.readUpstream(alloc, &producer_input, &pinned, logical));
    }
    var identity = (try artifact_ids.decodeArtifactRefAlloc(alloc, logical)).?;
    defer identity.deinit(alloc);
    const public_id = try artifact_ids.artifactPublicIdAlloc(alloc, identity);
    defer alloc.free(public_id);
    const LookupCheck = struct {
        fn run(a: Allocator, owner: *DB, id: []const u8, expected: ?[]const u8) !void {
            var artifact = try owner.getArtifact(a, id);
            defer if (artifact) |*value| value.deinit(a);
            if (expected) |value| {
                try std.testing.expect(artifact != null);
                try std.testing.expectEqualStrings(value, artifact.?.value);
            } else try std.testing.expect(artifact == null);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, LookupCheck.run, .{ &db, public_id, @as(?[]const u8, "first") });
    const empty = try generations.Spec.init(authority, scope, @splat(4), chunks.Builder.init().finish(), 2);
    var replacement = try generations.Plan.init(alloc, scope, empty);
    defer replacement.deinit();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try replacement.begin(&txn);
        try std.testing.expectError(error.EnrichmentSourceChanged, replacement.publish(&txn, null, Guard{}));
        try std.testing.expect(try replacement.publish(&txn, spec.id(), Guard{}));
        try txn.commit();
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.ArtifactPublicationPending, replacement.retire(&txn, authority, Guard{}));
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.retire(&txn, authority, Guard{ .valid = false }));
        try std.testing.expect(try plan.retire(&txn, authority, Guard{}));
        try std.testing.expect(!try plan.retire(&txn, authority, Guard{}));
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.begin(&txn));
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.publish(&txn, empty.id(), Guard{}));
        try std.testing.expectError(error.EnrichmentSourceChanged, page.stage(&plan, &txn));
        try txn.commit();
    }
    for (0..2) |_| {
        const state = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk try plan.load(&read);
        };
        var retired = try generations.PreparedRetirement.init(alloc, &plan, authority, state, 1);
        defer retired.deinit();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try retired.stage(&plan, &txn));
        try std.testing.expect(!try retired.stage(&plan, &txn));
        try txn.commit();
    }
    try std.testing.expectEqualStrings("first", (try view.get(alloc, 0)).?);
    try std.testing.expectEqualStrings("first", (try generations.readMember(alloc, &pinned, logical)).?);
    try std.testing.expectEqualStrings("second", (try view.get(alloc, 1)).?);
    try std.testing.expect((try view.get(alloc, 99)) == null);
    var cursor = try view.openCursor(alloc, 0);
    defer cursor.close();
    try std.testing.expectEqualStrings("first", (try cursor.next()).?.value);
    try std.testing.expectEqualStrings("second", (try cursor.next()).?.value);
    try std.testing.expect((try cursor.next()) == null);
    var current = try db.core.store.beginReadTxn();
    defer current.abort();
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateArtifactSources(alloc, &current, authority.namespace, &source_inputs, &head_guard));
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateArtifactSources(alloc, &current, authority.namespace, producer_input.sources(), producer_input.reads.items));
    try std.testing.expectError(error.ArtifactPublicationPending, artifact_asset_publication.readUpstream(alloc, &producer_input, &current, logical));
    try publication.validateArtifactSources(alloc, &pinned, authority.namespace, &source_inputs, &head_guard);
    var next = (try generations.View(docstore_mod.DocStore.Txn).open(alloc, &current, scope)).?;
    defer next.deinit();
    try std.testing.expect((try next.get(alloc, 0)) == null);
    var empty_cursor = try next.openCursor(alloc, 0);
    defer empty_cursor.close();
    try std.testing.expect((try empty_cursor.next()) == null);
    try std.testing.expect((try generations.readMember(alloc, &current, logical)) == null);
    try std.testing.expectError(error.NotFound, plan.load(&current));
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.begin(&txn));
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.publish(&txn, empty.id(), Guard{}));
        try std.testing.expectError(error.EnrichmentSourceChanged, page.stage(&plan, &txn));
    }
    try platform.allocator.checkAllAllocationFailures(alloc, LookupCheck.run, .{ &db, public_id, @as(?[]const u8, null) });
    try std.testing.expectEqual(artifact_footprint_mod.Family.generated, artifact_footprint_mod.classify(plan.head_key).?);
}

test "db ordered artifact inventory generation retirement resumes after restart without resurrection or tombstone growth" {
    const alloc = std.testing.allocator;
    const generations = artifact_chunk_generation;
    const chunks = artifact_chunk_manifest;
    const publication = artifact_publication;
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/generation-retirement", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    const scope = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", null);
    defer alloc.free(scope);
    var output = chunks.Builder.init();
    for (0..130) |i| try output.append(@intCast(i), "x");
    const spec = try generations.Spec.init(authority, scope, @splat(3), output.finish(), 1);
    var plan = try generations.Plan.init(alloc, scope, spec);
    defer plan.deinit();
    var first_page: ?generations.PreparedRetirement = null;
    defer if (first_page) |*page| page.deinit();
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        var append = try generations.PreparedAppend.init(alloc, &plan, .{ .spec = spec, .progress = chunks.Builder.init().finish() }, &@as([128][]const u8, @splat("x")));
        defer append.deinit();
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            _ = try plan.begin(&txn);
            _ = try append.stage(&plan, &txn);
            _ = try plan.retire(&txn, authority, Guard{});
            try txn.commit();
        }
        const state = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk try plan.load(&read);
        };
        const Check = struct {
            fn run(a: Allocator, selected: *const generations.Plan, owner: publication.Authority, previous: generations.State) !void {
                var page = try generations.PreparedRetirement.init(a, selected, owner, previous, 17);
                defer page.deinit();
            }
        };
        try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{ &plan, authority, state });
        first_page = try generations.PreparedRetirement.init(alloc, &plan, authority, state, 17);
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try first_page.?.stage(&plan, &txn);
        try txn.commit();
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(u32, 17), (try plan.load(&txn)).retired_through);
        try std.testing.expect(!try first_page.?.stage(&plan, &txn));
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.begin(&txn));
    }
    while (true) {
        const state = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk plan.load(&read) catch |err| if (err == error.NotFound) break else return err;
        };
        var page = try generations.PreparedRetirement.init(alloc, &plan, authority, state, 17);
        defer page.deinit();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try page.stage(&plan, &txn));
        try txn.commit();
    }
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.NotFound, plan.load(&read));
        var cursor = try read.openPhysicalCursorAdapter();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(plan.row_prefix)) |entry| try std.testing.expect(!std.mem.startsWith(u8, entry.key, plan.row_prefix));
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(!try first_page.?.stage(&plan, &txn));
        try std.testing.expectError(error.EnrichmentSourceChanged, plan.begin(&txn));
        // Even a different output digest cannot reuse a retired incarnation.
        var reused_spec = spec;
        reused_spec.input_digest = @splat(4);
        var reused = try generations.Plan.init(alloc, scope, reused_spec);
        defer reused.deinit();
        try std.testing.expectError(error.EnrichmentSourceChanged, reused.begin(&txn));
        var fresh_spec = spec;
        fresh_spec.incarnation = 2;
        var fresh = try generations.Plan.init(alloc, scope, fresh_spec);
        defer fresh.deinit();
        try std.testing.expect(try fresh.begin(&txn));
        try std.testing.expect(!try fresh.begin(&txn));
        // Empty, abandoned attempts can retire in one zero-member GC page.
        _ = try fresh.retire(&txn, authority, Guard{});
        var page = try generations.PreparedRetirement.init(alloc, &fresh, authority, try fresh.load(&txn), 17);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 0), page.keys.len);
        _ = try page.stage(&fresh, &txn);
        try txn.commit();
    }
    // Only the per-stream clock remains, not one terminal state per attempt.
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    _ = try read.get(plan.clock_key);
    try std.testing.expectError(error.NotFound, plan.load(&read));
}

test "db ordered artifact inventory generation discovery resumes scoped cursors and races incarnation proposals safely" {
    const alloc = std.testing.allocator;
    const generations = artifact_chunk_generation;
    const recovery = @import("artifact/generation_recovery.zig");
    const chunks = artifact_chunk_manifest;
    const publication = artifact_publication;
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/generation-discovery", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    const scope = try chunks.scopedKeyAlloc(alloc, "doc\xff", "chunks\x00", null);
    defer alloc.free(scope);
    const neighbor = try chunks.scopedKeyAlloc(alloc, "doc\xff", "chunks\x00", "unit\x00");
    defer alloc.free(neighbor);
    var cursor_bytes: [recovery.Cursor.encoded_len]u8 = undefined;
    var seen = std.AutoHashMap(u64, void).init(alloc);
    defer seen.deinit();
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try source_authority_mod.bind(&txn, .native, authority.namespace);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try txn.commit();
        }
        for (1..6) |incarnation| {
            var output = chunks.Builder.init();
            if (incarnation == 2) try output.append(0, "unfinished");
            const spec = try generations.Spec.init(authority, scope, @splat(3), output.finish(), @intCast(incarnation));
            var plan = try generations.Plan.init(alloc, scope, spec);
            defer plan.deinit();
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try std.testing.expectEqual(@as(u64, @intCast(incarnation)), try generations.proposeIncarnation(alloc, &txn, authority, scope));
            _ = try plan.begin(&txn);
            if (incarnation == 1) _ = try plan.publish(&txn, null, Guard{});
            if (incarnation == 3) _ = try plan.retire(&txn, authority, Guard{});
            try txn.commit();
        }
        {
            var other = try generations.Plan.init(alloc, neighbor, try generations.Spec.init(authority, neighbor, @splat(3), chunks.Builder.init().finish(), 99));
            defer other.deinit();
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            _ = try other.begin(&txn);
            try txn.commit();
        }
        var page = try recovery.discover(alloc, db.core.store, scope, null, .{ .rows = 2 });
        defer page.deinit();
        try std.testing.expect(!page.at_end);
        try std.testing.expect(page.selected != null);
        for (page.states) |state| try seen.put(state.spec.incarnation, {});
        cursor_bytes = page.next.?.encode();
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    const saved = try recovery.Cursor.decode(&cursor_bytes);
    var next: ?recovery.Cursor = saved;
    while (next) |position| {
        var page = try recovery.discover(alloc, db.core.store, scope, position, .{ .rows = 2 });
        defer page.deinit();
        for (page.states) |state| {
            try std.testing.expect(state.spec.incarnation <= 5);
            try std.testing.expect(!(try seen.getOrPut(state.spec.incarnation)).found_existing);
        }
        next = page.next;
    }
    try std.testing.expectEqual(@as(usize, 5), seen.count());
    try std.testing.expectError(error.InvalidBatchRequest, recovery.discover(alloc, db.core.store, neighbor, saved, .{}));
    cursor_bytes[100] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, recovery.Cursor.decode(&cursor_bytes));
    const Check = struct {
        fn run(a: Allocator, owner: *DB, stream: []const u8) !void {
            var page = try recovery.discover(a, owner.core.store, stream, null, .{ .rows = 2 });
            defer page.deinit();
            try std.testing.expect(page.states.len > 0 and page.states.len <= 2);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{ &db, scope });
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        const proposal = try generations.proposeIncarnation(alloc, &txn, authority, scope);
        try std.testing.expectEqual(@as(u64, 6), proposal);
        var winner = try generations.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, @splat(3), chunks.Builder.init().finish(), proposal));
        defer winner.deinit();
        var loser = try generations.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, @splat(4), chunks.Builder.init().finish(), proposal));
        defer loser.deinit();
        _ = try winner.begin(&txn);
        try std.testing.expectError(error.EnrichmentSourceChanged, loser.begin(&txn));
        // Aborting releases the proposal; merely observing it never reserves it.
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(u64, 6), try generations.proposeIncarnation(alloc, &txn, authority, scope));
        authority.epoch = 2;
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.commit();
    }
    try std.testing.expectError(error.ArtifactCatalogDrift, recovery.discover(alloc, db.core.store, scope, saved, .{}));
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 1), try generations.proposeIncarnation(alloc, &read, authority, scope));
}

test "db ordered artifact inventory logical chunk projection merges generations without legacy tails" {
    const alloc = std.testing.allocator;
    const generations = artifact_chunk_generation;
    const chunks = artifact_chunk_manifest;
    const publication = artifact_publication;
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/logical-chunks", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    const doc = "doc\x00";
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    try db.addIndex(.{ .name = "chunk_sparse", .kind = .sparse_vector, .config_json = "{\"field\":\"sparse\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\",\"chunk_name\":\"chunks\",\"chunk_size\":8,\"chunk_overlap\":2}}" });
    try db.core.store.put(&internal_keys.identityOrdinalToDocKey(7), doc);
    const sparse_entry = db.core.index_manager.sparseIndex("chunk_sparse").?;
    try std.testing.expectEqualStrings("chunks", sparse_entry.chunk_name.?);
    const Fixture = struct { producer: []const u8 = "chunks", unit: ?[]const u8 = null, value: []const u8 };
    for ([_]Fixture{
        .{ .value = "{\"body\":\"old-root\"}" },
        .{ .unit = "a", .value = "{\"body\":\"old-unit\"}" },
        .{ .unit = "z", .value = "{\"body\":\"legacy-neighbor\"}" },
    }) |fixture| {
        const key = if (fixture.unit) |unit|
            try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc, fixture.producer, unit, 0)
        else
            try internal_keys.chunkArtifactKeyAlloc(alloc, doc, fixture.producer, 0);
        defer alloc.free(key);
        try db.core.store.put(key, fixture.value);
        try db.core.index_manager.indexSparsePreparedWritesByNameWithOptions("chunk_sparse", &.{.{ .doc_id = key, .vec = .{ .indices = &.{1}, .values = &.{1} } }}, .{});
    }
    var old = try db.core.store.beginReadTxn();
    defer old.abort();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        // A malformed stale tail must never be parsed once a head owns its scope.
        const tail = try internal_keys.chunkArtifactKeyAlloc(alloc, doc, "chunks", 9000);
        defer alloc.free(tail);
        try txn.put(tail, "not-json");
        try source_authority_mod.bind(&txn, .native, authority.namespace);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.commit();
    }
    for ([_]Fixture{
        .{ .value = "{\"body\":\"new-root\"}" },
        .{ .unit = "a", .value = "" },
        .{ .producer = "fresh\x00", .unit = "unit\xff", .value = "{\"body\":\"generation-only\"}" },
    }) |fixture| {
        const scope = try chunks.scopedKeyAlloc(alloc, doc, fixture.producer, fixture.unit);
        defer alloc.free(scope);
        var output = chunks.Builder.init();
        if (fixture.value.len != 0) try output.append(0, fixture.value);
        if (fixture.unit == null) try output.append(1, fixture.value);
        const spec = try generations.Spec.init(authority, scope, @splat(3), output.finish(), 1);
        var plan = try generations.Plan.init(alloc, scope, spec);
        defer plan.deinit();
        var append: ?generations.PreparedAppend = if (fixture.value.len != 0)
            try generations.PreparedAppend.init(alloc, &plan, .{ .spec = spec, .progress = chunks.Builder.init().finish() }, if (fixture.unit == null) &.{ fixture.value, fixture.value } else &.{fixture.value})
        else
            null;
        defer if (append) |*page| page.deinit();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try plan.begin(&txn);
        if (append) |*page| _ = try page.stage(&plan, &txn);
        _ = try plan.publish(&txn, null, Guard{});
        try txn.commit();
    }
    var before = (try loadChunkFieldValueTxn(&db, alloc, doc, &old)).?;
    defer freeJsonValue(alloc, &before);
    const previous = before.object.get("chunks").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), previous.len);
    try std.testing.expectEqualStrings("old-root", previous[0].object.get("body").?.string);
    try std.testing.expectEqualStrings("old-unit", previous[1].object.get("body").?.string);
    const Check = struct {
        fn run(a: Allocator, owner: *DB) !void {
            var result = (try loadChunkFieldValueTxn(owner, a, "doc\x00", null)).?;
            defer freeJsonValue(a, &result);
            const mixed = result.object.get("chunks").?.array.items;
            try std.testing.expectEqual(@as(usize, 3), mixed.len);
            try std.testing.expectEqualStrings("new-root", mixed[0].object.get("body").?.string);
            try std.testing.expectEqualStrings("new-root", mixed[1].object.get("body").?.string);
            try std.testing.expectEqualStrings("legacy-neighbor", mixed[2].object.get("body").?.string);
            const generated = result.object.get("fresh\x00").?.array.items;
            try std.testing.expectEqual(@as(usize, 1), generated.len);
            try std.testing.expectEqualStrings("generation-only", generated[0].object.get("body").?.string);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{&db});
    var expected_nums: [2]u32 = undefined;
    for ([_]?[]const u8{ null, "z" }, &expected_nums) |unit, *expected| {
        const key = if (unit) |id|
            try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc, "chunks", id, 0)
        else
            try internal_keys.chunkArtifactKeyAlloc(alloc, doc, "chunks", 0);
        defer alloc.free(key);
        expected.* = (try sparse_entry.index.debugDocNumForDocId(key)).?;
    }
    const ProjectionCheck = struct {
        fn run(a: Allocator, owner: *DB, expected: [2]u32) !void {
            const nums = try owner.core.index_manager.lookupSparseDocNumsForOrdinalsAlloc(a, owner.core.store, "chunk_sparse", &.{ 7, 7 });
            defer a.free(nums);
            // The retired unit still has a live sparse posting. Filtering must
            // follow logical output membership, not the physical artifact tail.
            try std.testing.expectEqualSlices(u32, &expected, nums);
            var read = try owner.core.store.beginReadTxn();
            defer read.abort();
            var binary = try artifact_chunk_cursor.Cursor(docstore_mod.DocStore.Txn).openNamed(a, &read, "doc\x00", "fresh\x00");
            defer binary.close();
            const row = (try binary.next()).?;
            try std.testing.expect(internal_keys.matchesChunkArtifactName(row.key, "fresh\x00"));
            try std.testing.expectEqualStrings("{\"body\":\"generation-only\"}", row.value);
            try binary.seekAfter(row.key);
            try std.testing.expect((try binary.next()) == null);
            var resumed = try artifact_chunk_cursor.Cursor(docstore_mod.DocStore.Txn).openNamed(a, &read, "doc\x00", "chunks");
            defer resumed.close();
            const first = (try resumed.next()).?;
            try std.testing.expectEqualStrings("{\"body\":\"new-root\"}", first.value);
            // The key is borrowed from the cursor itself. Seeking past it must
            // skip the malformed legacy tail and the empty retired unit.
            try resumed.seekAfter(first.key);
            const second = (try resumed.next()).?;
            try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, second.key[second.key.len - 4 ..][0..4], .big));
            try std.testing.expectEqualStrings("{\"body\":\"new-root\"}", second.value);
            try resumed.seekAfter(second.key);
            const next = (try resumed.next()).?;
            try std.testing.expectEqualStrings("{\"body\":\"legacy-neighbor\"}", next.value);
            try resumed.seekAfter(next.key);
            try std.testing.expect((try resumed.next()) == null);
            const tail = try internal_keys.chunkArtifactKeyAlloc(a, "doc\x00", "chunks", std.math.maxInt(u32));
            defer a.free(tail);
            try resumed.seekAfter(tail);
            try std.testing.expectEqualStrings("{\"body\":\"legacy-neighbor\"}", (try resumed.next()).?.value);
            try std.testing.expectError(error.InvalidBatchRequest, binary.seekAfter(tail));
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, ProjectionCheck.run, .{ &db, expected_nums });
    // A selected empty unit and a shadowed root tail still have vector bytes
    // to retire. Reconciliation must enumerate them without exposing their
    // obsolete chunk payloads or confusing another embedding's outputs.
    for ([_]struct { unit: ?[]const u8, ordinal: u32, embedding: []const u8 }{
        .{ .unit = null, .ordinal = 0, .embedding = "model\x00" },
        .{ .unit = null, .ordinal = 9000, .embedding = "unrelated" },
        .{ .unit = null, .ordinal = 9001, .embedding = "model\x00" },
        .{ .unit = "a", .ordinal = 0, .embedding = "model\x00" },
    }) |entry| {
        const member = if (entry.unit) |unit| try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc, "chunks", unit, entry.ordinal) else try internal_keys.chunkArtifactKeyAlloc(alloc, doc, "chunks", entry.ordinal);
        defer alloc.free(member);
        const key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, member, entry.embedding);
        defer alloc.free(key);
        try db.core.store.put(key, "opaque vector");
    }
    const CandidateCheck = struct {
        fn run(a: Allocator, owner: *DB) !void {
            var read = try owner.core.store.beginReadTxn();
            defer read.abort();
            var cursor = try artifact_chunk_vector_cursor.Cursor(docstore_mod.DocStore.Txn).open(a, &read, "doc\x00", "chunks", "model\x00");
            defer cursor.close();
            const expected = [_]struct { unit: ?[]const u8, ordinal: u32 }{
                .{ .unit = null, .ordinal = 0 },
                .{ .unit = null, .ordinal = 1 },
                .{ .unit = null, .ordinal = 9001 },
                .{ .unit = "a", .ordinal = 0 },
                .{ .unit = "z", .ordinal = 0 },
            };
            for (expected) |entry| {
                const key = if (entry.unit) |unit| try internal_keys.documentUnitChunkArtifactKeyAlloc(a, "doc\x00", "chunks", unit, entry.ordinal) else try internal_keys.chunkArtifactKeyAlloc(a, "doc\x00", "chunks", entry.ordinal);
                defer a.free(key);
                const actual = (try cursor.next()).?;
                try std.testing.expectEqualStrings(key, actual);
                // Exercise borrowed resume across generation, tail and unit
                // boundaries, including an accepted member with both inputs.
                try cursor.seekAfter(actual);
            }
            try std.testing.expect((try cursor.next()) == null);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, CandidateCheck.run, .{&db});
    const PollCheck = struct {
        fn run(a: Allocator, owner: *DB, minimum_yields: usize, extra_outputs: usize) !void {
            const candidates = artifact_chunk_vector_cursor;
            const expected = [_]struct { unit: ?[]const u8, ordinal: u32 }{
                .{ .unit = null, .ordinal = 0 },
                .{ .unit = null, .ordinal = 1 },
                .{ .unit = null, .ordinal = 9001 },
                .{ .unit = "a", .ordinal = 0 },
                .{ .unit = "z", .ordinal = 0 },
            };
            var after: ?[]u8 = null;
            defer if (after) |value| a.free(value);
            var scan: ?[]u8 = null;
            defer if (scan) |value| a.free(value);
            var logical_scan: ?[]u8 = null;
            defer if (logical_scan) |value| a.free(value);
            var seen: usize = 0;
            var yields: usize = 0;
            var visits: usize = 0;
            while (true) {
                var read = try owner.core.store.beginReadTxn();
                defer read.abort();
                var cursor = try candidates.Cursor(docstore_mod.DocStore.Txn).open(a, &read, "doc\x00", "chunks", "model\x00");
                defer cursor.close();
                if (after) |key| try cursor.seekAfter(key);
                if (scan) |floor| try cursor.resumePhysical(floor);
                if (logical_scan) |position| try cursor.resumeLogical(position);
                // An expired deadline still permits one forward step, then
                // yields without losing the next unprocessed physical row.
                var budget: candidates.Budget = .{ .max_visits = 2, .max_bytes = 256, .deadline_ns = 0 };
                const step = try cursor.poll(&budget);
                try std.testing.expect(budget.visits <= 3);
                visits += budget.visits;
                try std.testing.expect(visits < 1000);
                switch (step) {
                    .member => |key| {
                        try std.testing.expect(seen < expected.len + extra_outputs);
                        const extra_unit = if (seen >= 4 and seen < 4 + extra_outputs) try std.fmt.allocPrint(a, "a{d:0>3}", .{seen - 4}) else null;
                        defer if (extra_unit) |unit| a.free(unit);
                        const value = if (extra_unit) |unit| @TypeOf(expected[0]){ .unit = unit, .ordinal = 0 } else expected[if (seen < 4) seen else 4];
                        const wanted = if (value.unit) |unit| try internal_keys.documentUnitChunkArtifactKeyAlloc(a, "doc\x00", "chunks", unit, value.ordinal) else try internal_keys.chunkArtifactKeyAlloc(a, "doc\x00", "chunks", value.ordinal);
                        defer a.free(wanted);
                        try std.testing.expectEqualStrings(wanted, key);
                        const next = try a.dupe(u8, key);
                        if (after) |previous_key| a.free(previous_key);
                        after = next;
                        if (scan) |previous_key| a.free(previous_key);
                        scan = null;
                        const next_logical = try cursor.checkpointLogicalAlloc(a);
                        if (logical_scan) |previous_key| a.free(previous_key);
                        logical_scan = next_logical;
                        seen += 1;
                    },
                    .logical_yielded => |position| {
                        if (logical_scan) |previous_key| {
                            const Position = artifact_chunk_scan_position.Position;
                            try std.testing.expect((try Position.decode(position)).advances(try Position.decode(previous_key)));
                        }
                        const next = try a.dupe(u8, position);
                        if (logical_scan) |previous_key| a.free(previous_key);
                        logical_scan = next;
                    },
                    .yielded => |floor| {
                        if (scan) |previous_key| try std.testing.expect(std.mem.order(u8, floor, previous_key) == .gt);
                        const next = try a.dupe(u8, floor);
                        if (scan) |previous_key| a.free(previous_key);
                        scan = next;
                        yields += 1;
                    },
                    .end => break,
                }
            }
            try std.testing.expectEqual(expected.len + extra_outputs, seen);
            try std.testing.expect(yields >= minimum_yields);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, PollCheck.run, .{ &db, 1, 0 });
    // Long obsolete tails are opaque here: malformed payloads and another
    // embedding's keys consume bounded scan work, never provider invocations.
    for (2..66) |ordinal| {
        const member = try internal_keys.chunkArtifactKeyAlloc(alloc, doc, "chunks", @intCast(ordinal));
        defer alloc.free(member);
        const key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, member, "unrelated");
        defer alloc.free(key);
        try db.core.store.put(key, "opaque old output");
    }
    try PollCheck.run(alloc, &db, 10, 0);
    // Empty selected generations must cost bounded work too, including when
    // an older output is returned before a prefetched live logical member.
    for (0..64) |ordinal| {
        const unit = try std.fmt.allocPrint(alloc, "a{d:0>3}", .{ordinal});
        defer alloc.free(unit);
        const scope = try chunks.scopedKeyAlloc(alloc, doc, "chunks", unit);
        defer alloc.free(scope);
        const spec = try generations.Spec.init(authority, scope, @splat(3), chunks.Builder.init().finish(), 1);
        var plan = try generations.Plan.init(alloc, scope, spec);
        defer plan.deinit();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try plan.begin(&txn);
        _ = try plan.publish(&txn, null, Guard{});
        try txn.commit();
    }
    try PollCheck.run(alloc, &db, 10, 0);
    for (0..64) |ordinal| {
        const unit = try std.fmt.allocPrint(alloc, "a{d:0>3}", .{ordinal});
        defer alloc.free(unit);
        const member = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc, "chunks", unit, 0);
        defer alloc.free(member);
        const output_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, member, "model\x00");
        defer alloc.free(output_key);
        try db.core.store.put(output_key, "obsolete");
    }
    // Every result crosses a fresh snapshot. Retaining the prefetched member's
    // predecessor prevents quadratic rescans of the 64 empty heads.
    try PollCheck.run(alloc, &db, 10, 64);
}

test "db ordered artifact inventory local stream checkpoints survive restart without discharging work" {
    const alloc = std.testing.allocator;
    const checkpoints = artifact_stream_checkpoint;
    const Observation = @import("artifact/stream_observation.zig").Observation;
    const publication = artifact_publication;
    const obligations = artifact_producer_obligations;
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    var incarnation: u128 = undefined;
    var selected: checkpoints.Key = undefined;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/stream-checkpoint", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const document = try internal_keys.documentKeyAlloc(alloc, "doc\x00");
    defer alloc.free(document);
    const ScanPosition = artifact_chunk_scan_position.Position;
    const logical_start = try (ScanPosition{ .head = "head\x00", .legacy = "legacy\xff", .members_open = true, .ordinal = 1 }).encodeAlloc(alloc);
    defer alloc.free(logical_start);
    const logical_next = try (ScanPosition{ .head = "head\x00", .legacy = "legacy\xff", .members_open = true, .ordinal = 2 }).encodeAlloc(alloc);
    defer alloc.free(logical_next);
    var first: checkpoints.State = undefined;
    var first_digest: publication.Digest = undefined;
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        incarnation = db.root_incarnation;
        try std.testing.expect(incarnation != 0);
        selected = checkpoints.key(authority, "doc\x00", .{ .root_incarnation = incarnation, .kind = .index, .name = "index", .generation = 1, .artifact = "model" });
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try source_authority_mod.bind(&txn, .native, authority.namespace);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try obligations.begin(alloc, &txn, authority);
            try artifact_producer_validation.begin(alloc, &txn, authority);
            try txn.commit();
        }
        try db.core.store.put(document, "{\"body\":\"one\"}");
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            first = .{ .root_incarnation = incarnation, .observation = try Observation.capture(&read, "doc\x00") };
            // A page may inspect only irrelevant physical rows. Persisting
            // this position must not manufacture an accepted member.
            first.scan_cursor = "physical\x00";
            first.logical_scan_cursor = logical_start;
        }
        const replay = db.core.store.lastReplaySequence(0);
        var prepared = try checkpoints.Prepared.init(alloc, first);
        defer prepared.deinit();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const saved = try checkpoints.stagePrepared(&txn, &selected, null, &prepared, "doc\x00");
        try std.testing.expect(saved.changed);
        var advanced = first;
        advanced.logical_scan_cursor = logical_next;
        const progressed = try checkpoints.stage(alloc, &txn, &selected, saved.digest, advanced, "doc\x00");
        try std.testing.expect(progressed.changed);
        try std.testing.expectError(error.InvalidBatchRequest, checkpoints.stage(alloc, &txn, &selected, progressed.digest, first, "doc\x00"));
        try std.testing.expect(!(try checkpoints.stage(alloc, &txn, &selected, saved.digest, advanced, "doc\x00")).changed);
        first = advanced;
        first_digest = progressed.digest;
        try txn.commit();
        try std.testing.expectEqual(replay, db.core.store.lastReplaySequence(0));
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    try std.testing.expectEqual(incarnation, db.root_incarnation);
    var second: checkpoints.State = undefined;
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const loaded = (try checkpoints.load(&read, &selected)).?;
        try std.testing.expectEqualDeep(first_digest, loaded.digest);
        try std.testing.expectEqualStrings(first.cursor, loaded.state.cursor);
        try std.testing.expectEqualStrings(first.scan_cursor, loaded.state.scan_cursor);
        try std.testing.expectEqualStrings(first.logical_scan_cursor, loaded.state.logical_scan_cursor);
        try std.testing.expectEqual(@as(u64, 0), loaded.state.members);
        try loaded.state.observation.requireCurrent(&read, "doc\x00");
        second = loaded.state;
        // This test isolates persistence/CAS. Runtime tests separately prove
        // that only current accepted producer receipts enter this chain.
        try second.append("scope\x00", @splat(3));
        try second.append("scope\x01", @splat(4));
    }
    var ready_digest: publication.Digest = undefined;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, checkpoints.stage(alloc, &txn, &selected, null, second, "doc\x00"));
        const advanced = try checkpoints.stage(alloc, &txn, &selected, first_digest, second, "doc\x00");
        try std.testing.expect(advanced.changed);
        try std.testing.expect(!(try checkpoints.stage(alloc, &txn, &selected, first_digest, second, "doc\x00")).changed);
        try std.testing.expectError(error.InvalidBatchRequest, checkpoints.stage(alloc, &txn, &selected, advanced.digest, first, "doc\x00"));
        second.enumerated = true;
        ready_digest = (try checkpoints.stage(alloc, &txn, &selected, advanced.digest, second, "doc\x00")).digest;
        try txn.commit();
    }
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const loaded = (try checkpoints.load(&read, &selected)).?;
        try std.testing.expect(loaded.state.enumerated);
        try std.testing.expectEqual(@as(u64, 2), loaded.state.members);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
    }
    {
        // Simulate importing the donor's private store keys into a fresh
        // physical root while preserving namespace, catalog and input stamps.
        // A foreign ready scan must not suppress the receiver's own census.
        const receiver_path = try std.fmt.allocPrint(alloc, "{s}-receiver", .{path});
        defer alloc.free(receiver_path);
        var receiver = try DB.open(alloc, receiver_path, options);
        defer receiver.close();
        try std.testing.expect(receiver.root_incarnation != 0 and receiver.root_incarnation != incarnation);
        const raw = try db.core.store.get(alloc, &selected);
        defer alloc.free(raw);
        try receiver.core.store.put(&selected, raw);
        const local_key = checkpoints.key(authority, "doc\x00", .{ .root_incarnation = receiver.root_incarnation, .kind = .index, .name = "index", .generation = 1, .artifact = "model" });
        {
            var read = try receiver.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect((try checkpoints.load(&read, &local_key)) == null);
        }
        // Re-keying foreign bytes cannot adopt them either: the physical root
        // is authenticated in both the key and checksummed checkpoint value.
        try receiver.core.store.put(&local_key, raw);
        var read = try receiver.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.ArtifactCatalogCorrupt, checkpoints.load(&read, &local_key));
    }
    var stale_prepared = try checkpoints.Prepared.init(alloc, second);
    defer stale_prepared.deinit();
    try db.core.store.put(document, "{\"body\":\"two\"}");
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, checkpoints.stagePrepared(&txn, &selected, ready_digest, &stale_prepared, "doc\x00"));
        try std.testing.expectError(error.EnrichmentSourceChanged, checkpoints.stage(alloc, &txn, &selected, ready_digest, second, "doc\x00"));
        const reset: checkpoints.State = .{ .root_incarnation = incarnation, .observation = try Observation.capture(&txn, "doc\x00"), .enumerated = true };
        try std.testing.expectError(error.EnrichmentSourceChanged, checkpoints.stage(alloc, &txn, &selected, first_digest, reset, "doc\x00"));
        try std.testing.expect((try checkpoints.stage(alloc, &txn, &selected, ready_digest, reset, "doc\x00")).changed);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&txn)).?.pending_documents);
        try txn.commit();
    }
    // Checkpoints are scoped by an ordered epoch prefix, so catalog churn can
    // reclaim old scans with the same bounded machinery as dirty obligations.
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const raw = try (checkpoints.State{ .root_incarnation = incarnation, .observation = try Observation.capture(&txn, "doc\x00") }).encodeAlloc(alloc);
        defer alloc.free(raw);
        for (0..257) |ordinal| {
            const old_key = checkpoints.key(authority, "doc\x00", .{ .root_incarnation = incarnation, .kind = .index, .name = "index", .generation = @intCast(ordinal + 2), .artifact = "model" });
            try txn.put(&old_key, raw);
        }
        try txn.commit();
    }
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    var next_authority = authority;
    next_authority.epoch += 1;
    const current_key = checkpoints.key(next_authority, "doc\x00", .{ .root_incarnation = incarnation, .kind = .index, .name = "index", .generation = 1, .artifact = "model" });
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = next_authority.namespace, .authority_epoch = next_authority.epoch, .catalog_digest = next_authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try obligations.begin(alloc, &txn, next_authority);
        try artifact_producer_validation.begin(alloc, &txn, next_authority);
        const current: checkpoints.State = .{ .root_incarnation = incarnation, .observation = try Observation.capture(&txn, "doc\x00") };
        _ = try checkpoints.stage(alloc, &txn, &current_key, null, current, "doc\x00");
        for ([_]u128{ if (incarnation == 1) 2 else 1, if (incarnation == std.math.maxInt(u128)) std.math.maxInt(u128) - 1 else std.math.maxInt(u128) }) |foreign_root| {
            var imported = current;
            imported.root_incarnation = foreign_root;
            const imported_key = checkpoints.key(next_authority, "doc\x00", .{ .root_incarnation = foreign_root, .kind = .index, .name = "index", .generation = 1, .artifact = "model" });
            const raw = try imported.encodeAlloc(alloc);
            defer alloc.free(raw);
            try txn.put(&imported_key, raw);
        }
        try txn.commit();
    }
    var pages: usize = 0;
    while (true) {
        pages += 1;
        if (try checkpoints.collectObsoletePage(alloc, db.core.store, incarnation)) break;
        try std.testing.expect(pages < 512);
    }
    try std.testing.expect(pages >= 3);
    try std.testing.expect((try checkpoints.load(&pinned, &selected)) != null);
    var current = try db.core.store.beginReadTxn();
    defer current.abort();
    try std.testing.expect((try checkpoints.load(&current, &selected)) == null);
    try std.testing.expect((try checkpoints.load(&current, &current_key)) != null);
    for ([_]u128{ if (incarnation == 1) 2 else 1, if (incarnation == std.math.maxInt(u128)) std.math.maxInt(u128) - 1 else std.math.maxInt(u128) }) |foreign_root| {
        const imported_key = checkpoints.key(next_authority, "doc\x00", .{ .root_incarnation = foreign_root, .kind = .index, .name = "index", .generation = 1, .artifact = "model" });
        try std.testing.expect((try checkpoints.load(&current, &imported_key)) == null);
    }
    try std.testing.expectEqual(@as(u64, 0), (try obligations.load(&current)).?.pending_documents);
}

test "db ordered artifact inventory chunk reconstruction resumes bounded pages and rejects stream races" {
    const alloc = std.testing.allocator;
    const reconstruction = @import("artifact/chunk_reconstruction.zig");
    const chunks = artifact_chunk_manifest;
    const publication = artifact_publication;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/chunk-reconstruction", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const scope: reconstruction.Scope = .{ .document = "doc\x00", .producer = "chunks", .unit = "unit\xff" };
    var encoded_checkpoint: [reconstruction.checkpoint_len]u8 = undefined;
    var expected = chunks.Builder.init();
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        var namespace: publication.Namespace = undefined;
        doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            for (0..257) |ordinal| {
                const key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, scope.document, scope.producer, scope.unit.?, @intCast(ordinal));
                defer alloc.free(key);
                var value: [8]u8 = undefined;
                std.mem.writeInt(u64, &value, ordinal, .little);
                try txn.put(key, &value);
                try expected.append(@intCast(ordinal), &value);
            }
            const other = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, scope.document, scope.producer, "other", 0);
            defer alloc.free(other);
            try txn.put(other, "unrelated");
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try txn.commit();
        }
        var page = (try reconstruction.prepare(alloc, db.core.store, scope, null, .{ .rows = 17 })).?;
        defer page.deinit();
        try std.testing.expect(!page.at_end);
        try std.testing.expect(page.next.manifest.count > 0 and page.next.manifest.count <= 17);
        encoded_checkpoint = try page.next.encode();
        var corrupt = encoded_checkpoint;
        corrupt[80] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, reconstruction.Checkpoint.decode(&corrupt));
    }
    var db = try DB.open(alloc, path, options);
    defer db.close();
    const first = try reconstruction.Checkpoint.decode(&encoded_checkpoint);
    var checkpoint = first;
    var pages: usize = 0;
    while (true) {
        var page = (try reconstruction.prepare(alloc, db.core.store, scope, checkpoint, .{ .rows = 17 })).?;
        defer page.deinit();
        try std.testing.expect(page.next.manifest.count - checkpoint.manifest.count <= 17);
        checkpoint = try reconstruction.Checkpoint.decode(&try page.next.encode());
        pages += 1;
        if (page.at_end) break;
        try std.testing.expect(pages < 257);
    }
    try std.testing.expect(pages > 1);
    try std.testing.expectEqualDeep(expected.finish(), checkpoint.manifest);
    // Discovery, including reaching EOF, creates neither inventory nor proof.
    const manifest_key = try chunks.scopedKeyAlloc(alloc, scope.document, scope.producer, scope.unit);
    defer alloc.free(manifest_key);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.NotFound, read.get(manifest_key));
        try checkpoint.requireCurrent(&read, manifest_key);
    }
    const Harness = struct {
        fn prepare(a: Allocator, store: *docstore_mod.DocStore, selected: reconstruction.Scope, previous: reconstruction.Checkpoint) !void {
            var page = (try reconstruction.prepare(a, store, selected, previous, .{ .rows = 1 })).?;
            defer page.deinit();
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, Harness.prepare, .{ db.core.store, scope, first });
    const other_scope: reconstruction.Scope = .{ .document = scope.document, .producer = scope.producer, .unit = "other" };
    try std.testing.expectError(error.InvalidBatchRequest, reconstruction.prepare(alloc, db.core.store, other_scope, first, .{}));
    // A deletion behind the completed scan invalidates its fixed-size witness.
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, scope.document, scope.producer, scope.unit.?, 0);
        defer alloc.free(key);
        try txn.delete(key);
        var marker: [16]u8 = undefined;
        std.mem.writeInt(u64, marker[0..8], 1, .little);
        std.mem.writeInt(u64, marker[8..16], 1, .little);
        try txn.put(&internal_keys.ordered_document_applied_entry_key, &marker);
        try txn.commit();
    }
    try std.testing.expectError(error.EnrichmentSourceChanged, reconstruction.prepare(alloc, db.core.store, scope, checkpoint, .{}));
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.EnrichmentSourceChanged, checkpoint.requireCurrent(&read, manifest_key));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, reconstruction.prepare(alloc, db.core.store, scope, null, .{}));
    // An immutable generation uses different member keys. Never replace its
    // selected head with an apparently empty legacy-prefix inventory.
    const head = try chunks.scopedKeyAlloc(alloc, other_scope.document, other_scope.producer, other_scope.unit);
    defer alloc.free(head);
    head[internal_keys.findComponentTerminator(head, 1).? + 2] = internal_keys.producer_generation_head_kind;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(head, "selected-generation");
        var marker: [16]u8 = undefined;
        std.mem.writeInt(u64, marker[0..8], 1, .little);
        std.mem.writeInt(u64, marker[8..16], 2, .little);
        try txn.put(&internal_keys.ordered_document_applied_entry_key, &marker);
        try txn.commit();
    }
    try std.testing.expectError(error.OnlineMergeArtifactTailsUnsupported, reconstruction.prepare(alloc, db.core.store, other_scope, null, .{}));
}

test "db ordered artifact inventory chunk replacement rolls back and reopens as one output set" {
    const alloc = std.testing.allocator;
    const chunks = artifact_chunk_manifest;
    const publication = artifact_publication;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/chunk-output-set", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const key = try chunks.keyAlloc(alloc, "doc", "chunks");
    defer alloc.free(key);
    const zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 0);
    defer alloc.free(zero);
    const one = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 1);
    defer alloc.free(one);
    var previous = chunks.Builder.init();
    try previous.append(0, "old zero");
    try previous.append(1, "old one");
    var next = chunks.Builder.init();
    try next.append(0, "new zero");
    const effects = [_]publication.Mutation{
        .{ .family = .document_artifact, .key = zero, .value = "new zero", .source_index = 0 },
        .{ .family = .document_artifact, .key = one, .value = null, .source_index = 0 },
    };
    var prepared = try chunks.PreparedReplacement.init(alloc, "doc", "chunks", previous.finish(), next.finish(), &effects);
    defer prepared.deinit();
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(zero, "old zero");
            try txn.put(one, "old one");
            try txn.put(key, &previous.finish().encode());
            try txn.commit();
        }
        {
            var txn = try db.core.store.beginWriteTxn();
            defer txn.abort();
            try prepared.stage(&txn);
            try txn.put(zero, "new zero");
            // Fault between inventory/member preparation and durable commit.
        }
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(previous.finish(), try chunks.Manifest.decode(try read.get(key)));
            try std.testing.expectEqualStrings("old zero", try read.get(zero));
            try std.testing.expectEqualStrings("old one", try read.get(one));
        }
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try prepared.stage(&txn);
            for (effects) |effect| if (effect.value) |value| try txn.put(effect.key, value) else try txn.delete(effect.key);
            try txn.commit();
        }
    }
    var reopened = try DB.open(alloc, path, options);
    defer reopened.close();
    var read = try reopened.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectEqualDeep(next.finish(), try chunks.Manifest.decode(try read.get(key)));
    try std.testing.expectEqualStrings("new zero", try read.get(zero));
    try std.testing.expectError(error.NotFound, read.get(one));
    try std.testing.expectError(error.EnrichmentSourceChanged, prepared.requireCurrent(&read));
}

test "db ordered artifact inventory producer coverage pages do not idle between productive turns" {
    const alloc = std.testing.allocator;
    const publication = artifact_publication;
    var tmp = try TestDirectory.init("producer-coverage-cadence");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "extraction", .kind = .asset, .field = "body", .content_type = "application/json" });
    for (0..140) |i| {
        const name = try std.fmt.allocPrint(alloc, "doc-{d:0>4}", .{i});
        defer alloc.free(name);
        const key = try internal_keys.documentKeyAlloc(alloc, name);
        defer alloc.free(key);
        try db.core.store.put(key, "{\"body\":\"one\"}");
    }
    var ordered = try db.artifactInventoryCommand(alloc);
    defer ordered.catalogs.deinit(alloc);
    ordered.binding.effect_protocol = 14;
    var namespace: publication.Namespace = undefined;
    doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const catalogs = try artifact_inventory.catalogs(&txn);
        try artifact_inventory.stageOrdered(alloc, &txn, ordered, 1);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = catalogs.digest(), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.commit();
    }
    // No protocol-15 work registry and no clock advancement. Both pages must
    // run immediately; otherwise a large census advances at 64 rows / 5 s.
    try std.testing.expect(try db.advanceArtifactProducerWorkPage());
    try std.testing.expectEqual(@as(u64, 0), db.artifact_producer_scheduler.retry_after_ns.load(.acquire));
    try std.testing.expect(try db.advanceArtifactProducerWorkPage());
    try std.testing.expectEqual(@as(u64, 0), db.artifact_producer_scheduler.retry_after_ns.load(.acquire));
    var turns: usize = 0;
    while (db.artifact_producer_scheduler.retry_after_ns.load(.acquire) == 0 and turns < 1000) : (turns += 1) {
        _ = try db.advanceArtifactProducerWorkPage();
    }
    // Protocol-14 rows cannot fabricate protocol-15 receipts. The census can
    // finish, but an unresolved witness sweep must remain pending and back off.
    try std.testing.expect(turns < 1000);
    try std.testing.expect(db.artifact_producer_scheduler.pending.load(.acquire));
    try std.testing.expect(db.artifact_producer_scheduler.retry_after_ns.load(.acquire) > db.independentMaintenanceNowNs());
    try std.testing.expect(!try db.advanceArtifactProducerWorkPage());
}

test "db ordered artifact inventory producer dispatch is durable and never completes work" {
    const alloc = std.testing.allocator;
    const publication = artifact_publication;
    const obligations = artifact_producer_obligations;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/producer-dispatch", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var expected_sequence: u64 = 0;
    var expected_dispatch_sequence: u64 = 0;
    for (0..2) |pass| {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        // Keep the consumer paused so the wakeup can be inspected without
        // another write or a polling worker concealing a lost notification.
        try db.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
        if (pass == 0) {
            try db.setSchemaJson(alloc, "{}");
            try db.addEnrichment(.{ .name = "extraction", .kind = .asset, .field = "body", .content_type = "application/json" });
            var ordered = try db.artifactInventoryCommand(alloc);
            defer ordered.catalogs.deinit(alloc);
            ordered.binding.effect_protocol = 15;
            var namespace: publication.Namespace = undefined;
            doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
            const authority = blk: {
                var txn = try db.core.store.beginWriteTxn();
                errdefer txn.abort();
                const catalogs = try artifact_inventory.catalogs(&txn);
                const active: publication.Authority = .{ .namespace = namespace, .epoch = 1, .catalog_digest = catalogs.digest() };
                try artifact_inventory.stageOrdered(alloc, &txn, ordered, 1);
                try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = active.epoch, .catalog_digest = active.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
                try obligations.begin(alloc, &txn, active);
                try artifact_producer_validation.begin(alloc, &txn, active);
                _ = try obligations.mark(alloc, &txn, active, "doc", null);
                try txn.commit();
                break :blk active;
            };
            const before = db.core.store.lastReplaySequence(0);
            {
                // A catalog race must not leave either a journal request or
                // a dispatch marker. The next valid pass must still admit it.
                var plan = try db.core.index_manager.acquireWritePlanSnapshot();
                defer plan.release();
                var ctx = db.batchContext();
                try std.testing.expectError(error.EnrichmentSourceChanged, appendDerivedBatchRecordContextWithWork(&ctx, .{}, false, .{
                    .authority = authority,
                    .item = .{ .document = "doc", .position = null, .revision = 1 },
                    .plan_generation = plan.generation() + 1,
                    .page = .{ .next_template = 1, .complete = true },
                }));
                try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
            }
            {
                var snapshot = try db.core.store.beginReadTxn();
                defer snapshot.abort();
                var old = try obligations.scanWork(alloc, &snapshot, authority, null);
                defer old.deinit();
                {
                    var changed = try db.core.store.beginWriteTxn();
                    errdefer changed.abort();
                    _ = try obligations.mark(alloc, &changed, authority, "doc", null);
                    try changed.commit();
                }
                var plan = try db.core.index_manager.acquireWritePlanSnapshot();
                defer plan.release();
                const template = plan.plan().generated_templates[0];
                var ctx = db.batchContext();
                // Same catalog and primary position, but a newer dependency
                // obligation: the journal and its dispatch marker both abort.
                try std.testing.expectError(error.EnrichmentSourceChanged, appendDerivedBatchRecordContextWithWork(&ctx, .{
                    .generated_enrichment_refs = &.{.{ .kind = template.kind, .index_name = template.index_name, .artifact_name = template.artifact_name, .embedding_name = template.embedding_name, .doc_key = "doc" }},
                }, false, .{ .authority = authority, .item = old.items[0], .plan_generation = plan.generation(), .page = .{ .next_template = 1, .complete = true } }));
                try std.testing.expectEqual(before, db.core.store.lastReplaySequence(0));
            }
            try std.testing.expect(try db.advanceArtifactProducerWorkPage());
            expected_sequence = db.core.store.lastReplaySequence(0);
            expected_dispatch_sequence = expected_sequence;
            try std.testing.expect(expected_sequence > before);
            try std.testing.expectEqual(expected_sequence, db.enrichment_runtime.?.stats().target_sequence);
        }
        _ = try db.advanceArtifactProducerWorkPage();
        try std.testing.expectEqual(expected_sequence, db.core.store.lastReplaySequence(0));
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const authority = (try publication.authority(&read)).?;
            var page = try obligations.scanWork(alloc, &read, authority, null);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.items.len);
            try std.testing.expectEqual(expected_dispatch_sequence, page.items[0].dispatch_sequence);
            try std.testing.expectEqual(@as(u64, 2), page.items[0].revision);
            try std.testing.expectEqual(@as(u64, 2), (try obligations.load(&read)).?.work_revision);
            try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
            try std.testing.expectError(error.ArtifactCatalogDrift, obligations.requireDrained(&read, authority));
        }
        if (pass == 0) {
            // Lose the producer's queue/upload entirely: the durable original
            // dispatch is complete, but no accepted output exists. A bounded
            // retry sweep must append work without reopening that dispatch.
            db.artifact_producer_scheduler.producer_retry_after_ns = 0;
            db.artifact_producer_scheduler.retry_after_ns.store(0, .release);
            try std.testing.expect(try db.advanceArtifactProducerWorkPage());
            const retried_sequence = db.core.store.lastReplaySequence(0);
            try std.testing.expect(retried_sequence > expected_sequence);
            try std.testing.expectEqual(retried_sequence, db.enrichment_runtime.?.stats().target_sequence);
            expected_sequence = retried_sequence;
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const authority = (try publication.authority(&read)).?;
            const current = (try obligations.lookupWork(alloc, &read, authority, "doc")).?;
            try std.testing.expectEqual(@as(u64, 2), current.retry_round);
            try std.testing.expectEqual(retried_sequence, current.retry_sequence);
            try std.testing.expectEqual(expected_dispatch_sequence, current.dispatch_sequence);
            try std.testing.expect(current.dispatch_complete);
            try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
        }
        if (pass == 1) {
            // Crash before the retry transaction commits: neither its round
            // allocation nor its per-document cursor may survive.
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                const authority = (try publication.authority(&txn)).?;
                const current = (try obligations.lookupWork(alloc, &txn, authority, "doc")).?;
                try std.testing.expectEqual(expected_sequence, current.retry_sequence);
                const round = try obligations.beginRetryRound(&txn, authority);
                try std.testing.expectEqual(@as(u64, 3), round);
                try std.testing.expect(try obligations.stageRetry(alloc, &txn, authority, current, round, expected_sequence + 1, .{ .next_template = 1, .complete = true }));
            }
            db.artifact_producer_scheduler.producer_retry_after_ns = 0;
            db.artifact_producer_scheduler.retry_after_ns.store(0, .release);
            try std.testing.expect(try db.advanceArtifactProducerWorkPage());
            expected_sequence = db.core.store.lastReplaySequence(0);
            {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const authority = (try publication.authority(&read)).?;
                const current = (try obligations.lookupWork(alloc, &read, authority, "doc")).?;
                try std.testing.expectEqual(@as(u64, 3), current.retry_round);
                try std.testing.expectEqual(expected_sequence, current.retry_sequence);
                try std.testing.expectEqual(expected_dispatch_sequence, current.dispatch_sequence);
            }
            // A completed local retry round cannot spin while other work
            // keeps the maintenance scheduler on its active cadence.
            db.artifact_producer_scheduler.retry_after_ns.store(0, .release);
            _ = try db.advanceArtifactProducerWorkPage();
            try std.testing.expectEqual(expected_sequence, db.core.store.lastReplaySequence(0));
            // A dependency repair can retain the primary position. It must
            // reopen durable dispatch even after a process restart.
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            const authority = (try publication.authority(&txn)).?;
            _ = try obligations.mark(alloc, &txn, authority, "doc", null);
            try txn.commit();
            db.artifact_producer_scheduler.retry_after_ns.store(0, .release);
            try std.testing.expect(try db.advanceArtifactProducerWorkPage());
            try std.testing.expect(db.core.store.lastReplaySequence(0) > expected_sequence);
        }
    }
}

test "db ordered artifact inventory producer pages resume after restart without completing obligations" {
    const alloc = std.testing.allocator;
    const publication = artifact_publication;
    const obligations = artifact_producer_obligations;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/paged-producer-dispatch", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const document: [12000]u8 = @splat('x');
    var previous_sequence: u64 = 0;
    for (0..4) |pass| {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        if (pass == 0) {
            try db.setSchemaJson(alloc, "{}");
            for ([_][]const u8{ "first", "second", "third" }) |name| try db.addEnrichment(.{ .name = name, .kind = .asset, .field = "body", .content_type = "application/json" });
            var ordered = try db.artifactInventoryCommand(alloc);
            defer ordered.catalogs.deinit(alloc);
            ordered.binding.effect_protocol = 15;
            var namespace: publication.Namespace = undefined;
            doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            const catalogs = try artifact_inventory.catalogs(&txn);
            const authority: publication.Authority = .{ .namespace = namespace, .epoch = 1, .catalog_digest = catalogs.digest() };
            try artifact_inventory.stageOrdered(alloc, &txn, ordered, 1);
            try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            try obligations.begin(alloc, &txn, authority);
            try artifact_producer_validation.begin(alloc, &txn, authority);
            _ = try obligations.mark(alloc, &txn, authority, &document, null);
            try txn.commit();
        }
        _ = try db.advanceArtifactProducerWorkPage();
        const sequence = db.core.store.lastReplaySequence(0);
        if (pass < 3) try std.testing.expect(sequence > previous_sequence) else try std.testing.expectEqual(previous_sequence, sequence);
        previous_sequence = sequence;
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const authority = (try publication.authority(&read)).?;
        var work = try obligations.scanWork(alloc, &read, authority, null);
        defer work.deinit();
        try std.testing.expectEqual(@as(usize, 1), work.items.len);
        try std.testing.expectEqual(@as(u32, @intCast(@min(pass + 1, 3))), work.items[0].next_template);
        try std.testing.expectEqual(pass >= 2, work.items[0].dispatch_complete);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
        try std.testing.expectError(error.ArtifactCatalogDrift, obligations.requireDrained(&read, authority));
    }
}

test "db ordered artifact inventory row reconciliation publishes envelopes and preserves reader floors" {
    const alloc = std.testing.allocator;
    const inventory = artifact_inventory;
    var tmp = try TestDirectory.init("row-inventory-envelope");
    defer tmp.cleanup();
    const Check = struct {
        fn ready(db: *DB, command: inventory.Command) !void {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u32, 4), try resolver_catalog_mod.readMinimumReaderVersion(&read, 4));
            try std.testing.expectEqualStrings(command.catalogs.resolvers, (try inventory.catalogs(&read)).resolvers);
            try std.testing.expect((try inventory.status(std.testing.allocator, &read, command.namespace)).ready);
            try std.testing.expect(try db.core.index_manager.matchesArtifactInventory(command.catalogs));
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            try std.testing.expect(plan.plan().matchesArtifactInventory(command.catalogs));
        }
    };
    var expected: ?inventory.Catalogs = null;
    defer if (expected) |*catalogs| catalogs.deinit(alloc);
    var committed: ?inventory.Command = null;
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        var old = try db.core.index_manager.acquireWritePlanSnapshot();
        defer old.release();
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            expected = try inventory.copyCatalogs(alloc, &read);
        }
        const envelope = try resolver_catalog_mod.versionedCatalog(alloc, "[]", 4);
        alloc.free(expected.?.resolvers);
        expected.?.resolvers = envelope;
        const command: inventory.Command = .{
            .namespace = online_source_contract.namespaceBytes(db.core.identity_namespace),
            .binding = .{ .epoch = 1, .digest = expected.?.digest(), .semantic_digest = try expected.?.semanticDigest(alloc) },
            .catalogs = expected.?,
        };
        const request: types.BatchRequest = .{ .artifact_catalog = command };
        const receipt: OrderedApplyReceipt = .{ .term = 1, .index = 1 };
        try db.reconcileReplicatedArtifactAdmission(request, receipt);
        // The production caller completes this reconciliation with its
        // compound source/merge admission. Acknowledge only the catalog side
        // in this fixture; a standalone catalog mutation cannot bypass an intent.
        {
            var ack = try db.core.store.beginWriteTxn();
            errdefer ack.abort();
            try inventory.stageOrdered(alloc, &ack, command, receipt.index);
            try artifact_reconcile_intent.clear(alloc, &ack, command, receipt.index);
            try ack.commit();
        }
        try Check.ready(&db, command);
        try std.testing.expect(!old.plan().matchesArtifactInventory(command.catalogs));
        committed = command;
        // A later, otherwise valid row-only recipe may not erase the permanent
        // reader fence merely because it has no resolver definitions.
        var legacy = command;
        legacy.previous = command.binding;
        legacy.binding.epoch = 2;
        legacy.catalogs.resolvers = "[]";
        legacy.binding.digest = legacy.catalogs.digest();
        legacy.binding.semantic_digest = try legacy.catalogs.semanticDigest(alloc);
        try std.testing.expectError(error.ArtifactCatalogDrift, db.reconcileReplicatedArtifactAdmission(.{ .artifact_catalog = legacy }, .{ .term = 1, .index = 2 }));
        try Check.ready(&db, command);
        {
            var cleanup = try db.core.store.beginWriteTxn();
            errdefer cleanup.abort();
            try artifact_reconcile_intent.clear(alloc, &cleanup, legacy, 2);
            try cleanup.commit();
        }
    }
    var reopened = try DB.open(alloc, tmp.path(), .{ .open_mode = .query_readonly });
    defer reopened.close();
    try Check.ready(&reopened, committed.?);
}

test "db ordered artifact inventory reader epoch upgrade refreshes digest and immutable plans" {
    const alloc = std.testing.allocator;
    const inventory = artifact_inventory;
    var tmp = try TestDirectory.init("reader-upgrade-inventory");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    // Establish a certified old catalog and retain its immutable foreground view.
    const before = before: {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try inventory.refresh(&txn);
        const binding = try inventory.local(&txn);
        try txn.commit();
        break :before binding;
    };
    var old = try db.core.index_manager.acquireWritePlanSnapshot();
    defer old.release();
    try db.core.index_manager.aggregateAuthoritativeReaderVersion(db.core.store, 4);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    const after = try inventory.local(&read);
    try std.testing.expectEqual(before.epoch + 1, after.epoch);
    const catalogs = try inventory.catalogs(&read);
    try std.testing.expectEqualStrings("{\"version\":4,\"resolvers\":[]}", catalogs.resolvers);
    try std.testing.expect(try db.core.index_manager.matchesArtifactInventory(catalogs));
    var current = try db.core.index_manager.acquireWritePlanSnapshot();
    defer current.release();
    try std.testing.expect(current.plan() != old.plan());
    try std.testing.expect(current.plan().matchesArtifactInventory(catalogs));
    try std.testing.expect(!old.plan().matchesArtifactInventory(catalogs));
    // The old plan remains valid for its pinned generation; a no-op upgrade
    // neither advances the inventory epoch nor rebuilds the new plan.
    try std.testing.expect(old.plan().matchesArtifactInventory(old.plan().artifact_catalogs.?));
    try db.core.index_manager.aggregateAuthoritativeReaderVersion(db.core.store, 4);
    var again = try db.core.index_manager.acquireWritePlanSnapshot();
    defer again.release();
    try std.testing.expect(again.plan() == current.plan());
    var latest = try db.core.store.beginReadTxn();
    defer latest.abort();
    try std.testing.expectEqual(after.epoch, (try inventory.local(&latest)).epoch);
}

test "db ordered artifact inventory graph TTL definitions retain reader envelope across mutations and reopen" {
    const alloc = std.testing.allocator;
    const inventory = artifact_inventory;
    var tmp = try TestDirectory.init("ttl-inventory-envelope");
    defer tmp.cleanup();
    const Check = struct {
        fn loaded(db: *DB) !void {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            _ = try inventory.local(&read);
            const catalogs = try inventory.catalogs(&read);
            // Shard transition admission consumes both the exact binding and
            // its definition identity, not just loaded-index matching.
            const command: inventory.Command = .{
                .namespace = @splat(7),
                .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(std.testing.allocator) },
                .catalogs = catalogs,
            };
            try command.validate();
            try std.testing.expectEqual(@as(u32, 4), try resolver_catalog_mod.catalogReaderVersion(std.testing.allocator, catalogs.resolvers));
            try std.testing.expect(try db.core.index_manager.matchesArtifactInventory(catalogs));
            var plan = try db.core.index_manager.acquireWritePlanSnapshot();
            defer plan.release();
            try std.testing.expect(plan.plan().matchesArtifactInventory(catalogs));
        }
    };
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try Check.loaded(&db);
        try db.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "body" });
        try Check.loaded(&db);
        try std.testing.expect(try db.deleteIndex("g"));
        // Removing the last TTL index cannot erase a permanent format fence.
        try Check.loaded(&db);
    }
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try Check.loaded(&db);
    }
    var readonly = try DB.open(alloc, tmp.path(), .{ .open_mode = .query_readonly });
    defer readonly.close();
    try Check.loaded(&readonly);
}

test "db ordered artifact inventory obsolete producer work is bounded and preserves pinned readers" {
    const alloc = std.testing.allocator;
    const publication = artifact_publication;
    const obligations = artifact_producer_obligations;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/producer-work-gc", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    var namespace: publication.Namespace = undefined;
    doc_identity.encodeNamespace(&namespace, db.core.identity_namespace);
    const active: publication.Authority = .{ .namespace = namespace, .epoch = 2, .catalog_digest = @splat(3) };
    var old = active;
    old.epoch = 1;
    const prefix = obligations.workPrefix(old);
    const first = try std.mem.concat(alloc, u8, &.{ &prefix, "doc0000" });
    defer alloc.free(first);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = namespace, .authority_epoch = active.epoch, .catalog_digest = active.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try obligations.begin(alloc, &txn, active);
        _ = try obligations.mark(alloc, &txn, active, "current\x00\xff", null);
        for (0..257) |i| {
            const key = try std.fmt.allocPrint(alloc, "{s}doc{d:0>4}", .{ prefix, i });
            defer alloc.free(key);
            // Obsolete values are not consumed as work; retirement need not
            // decode corrupt data that cannot belong to the active epoch.
            try txn.put(key, "retired");
        }
        try txn.commit();
    }
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    var pages: usize = 0;
    while (pages < 1024) {
        pages += 1;
        if (try obligations.collectObsoleteWorkPage(alloc, db.core.store)) break;
    }
    try std.testing.expect(pages >= 3 and pages < 1024);
    try std.testing.expectEqualStrings("retired", try pinned.get(first));
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(first));
    try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
    var work = try obligations.scanWork(alloc, &read, active, null);
    defer work.deinit();
    try std.testing.expectEqual(@as(usize, 1), work.items.len);
    try std.testing.expectEqualStrings("current\x00\xff", work.items[0].document);
    try std.testing.expectError(error.ArtifactCatalogDrift, obligations.scanWork(alloc, &read, active, .{ .authority = old, .document = "" }));
}

test "db ordered artifact inventory resolver commit invalidates pinned producer catalog" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/producer-catalog-resolver", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "extraction", .kind = .asset, .field = "body", .content_type = "application/json" });
    var before = try db.core.index_manager.acquireWritePlanSnapshot();
    defer before.release();
    try db.addResolver(.{ .name = "people", .table = "entities", .source_artifact = "extraction", .resolution_artifact = "resolved", .key_template = "{{canonical_name}}", .config_generation = 7 });
    var after = try db.core.index_manager.acquireWritePlanSnapshot();
    defer after.release();
    try std.testing.expect(after.generation() > before.generation());
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    const current = try artifact_inventory.catalogs(&read);
    try std.testing.expect(!before.plan().matchesArtifactInventory(current));
    try std.testing.expect(after.plan().matchesArtifactInventory(current));
    try std.testing.expect(before.plan().matchesArtifactInventory(before.plan().artifact_catalogs.?));
}

test "db ordered artifact inventory writer reopen revokes external projection evidence but readonly does not" {
    const alloc = std.testing.allocator;
    const publication = artifact_publication;
    const epoch = artifact_projection_epoch;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/projection-reopen", .{tmp.sub_path});
    defer alloc.free(path);
    const options: OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try epoch.revoke(&txn);
        try std.testing.expectEqual(@as(u64, 1), try epoch.load(&txn));
        try txn.commit();
    }
    {
        var readonly_options = options;
        readonly_options.open_mode = .status_only;
        var db = try DB.open(alloc, path, readonly_options);
        defer db.close();
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), try epoch.load(&read));
    }
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 2), try epoch.load(&read));
    }
}

test "db ordered artifact inventory full text reset revokes before destructive work" {
    const alloc = std.testing.allocator;
    const publication = artifact_publication;
    const epoch = artifact_projection_epoch;
    const rebuild = backfill_state_mod;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/projection-reset-order", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"keep me\"}" }} });
    const cfg: types.IndexConfig = .{ .name = "text", .kind = .full_text, .config_json = "{}" };
    try db.addIndex(cfg);
    const manager = db.core.index_manager;
    const entry = manager.textIndexEntry(cfg.name).?;
    const count = entry.persistent.snapshot().liveDocCount();
    try std.testing.expect(count > 0);
    const before = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.commit();
    }
    rebuild.test_publish_fault = .after_temp_sync;
    defer rebuild.test_publish_fault = null;
    try std.testing.expectError(error.TestRebuildStateCrash, manager.resetFullTextIndexForArtifactRebuild(db.core.store, cfg.name));
    rebuild.test_publish_fault = null;
    // The checkpoint is already revoked, but storage was not cleared when
    // durable restart-marker publication failed.
    try std.testing.expectEqual(count, entry.persistent.snapshot().liveDocCount());
    const failed = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try std.testing.expectEqual(.rebuilding, failed.status);
    try std.testing.expectEqual(before.generation + 1, failed.generation);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try epoch.load(&read) > 0);
    }
    try std.testing.expectEqual(count, try manager.resetFullTextIndexForArtifactRebuild(db.core.store, cfg.name));
    const complete = try db.core.loadProjectionCheckpoint(alloc, cfg.name);
    try std.testing.expectEqual(.clean, complete.status);
    try std.testing.expectEqual(failed.generation + 1, complete.generation);
    try std.testing.expectEqual(types.indexConfigHash(cfg), complete.config_hash);
}

test "artifact projection construction releases failures with real local resources" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("projection-ownership");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addEnrichment(.{ .name = "structured", .kind = .asset, .field = "body", .content_type = "application/json" });
    const Check = struct {
        fn run(failing: Allocator, source: read_projection.Source, kind: types.ArtifactKind, raw: []const u8) !void {
            var value = try read_projection.artifactProjectionValue(source, failing, .{
                .document_id = @constCast("doc"),
                .name = @constCast("structured"),
                .kind = kind,
                .chunk_id = if (kind == .chunk) 0 else null,
            }, raw, null);
            defer freeJsonValue(failing, &value);
            try std.testing.expectEqualStrings("ready", value.object.get("status").?.string);
            try std.testing.expectEqualStrings("doc", value.object.get("artifact_ref").?.object.get("document_id").?.string);
        }
    };
    const source = read_projection.Source{ .core = db.core };
    try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{ source, types.ArtifactKind.asset, @as([]const u8, "{\"entities\":[{\"text\":\"Antfly\"}]}") });
    try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{ source, types.ArtifactKind.chunk, @as([]const u8, "{\"body\":\"hello\",\"_chunk_id\":0}") });
    try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{ source, types.ArtifactKind.chunk, @as([]const u8, "plain chunk") });
}
