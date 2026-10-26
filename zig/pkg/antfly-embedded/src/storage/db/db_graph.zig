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

//! Graph operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDb = db_zig.TestDb;
const GraphTtlSha256 = @import("antfly_hash").Sha256;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const algebraic_mod = @import("algebraic/mod.zig");
const appendUniqueOwnedKey = @import("owned_keys.zig").appendUniqueOwnedKey;
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const builtin = @import("builtin");
const change_journal_mod = @import("derived/change_journal.zig");
const db_query_graph = @import("query/graph_exec.zig");
const db_query_result_shape = @import("query/result_shape.zig");
const derived_executor_mod = @import("derived/derived_executor.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_set = @import("doc_set.zig");
const docstore_mod = @import("../docstore.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const graph_asset_state = @import("graph_asset_state.zig");
const graph_cleanup_contract_mod = @import("../graph_cleanup_contract.zig");
const graph_cleanup_owner_mod = @import("graph_cleanup_owner.zig");
const graph_edge_contender = @import("graph_edge_contender.zig");
const graph_edge_ttl_expiration = @import("graph_edge_ttl_expiration.zig");
const graph_edge_ttl_tombstone = @import("graph_edge_ttl_tombstone.zig");
const graph_metadata_tables = @import("../../graph/metadata_tables.zig");
const graph_metric_rerank = @import("../../graph/metric_rerank.zig");
const graph_metric_runtime_mod = @import("maintenance/graph_metric_runtime.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_pattern_mod = @import("../../graph/pattern.zig");
const graph_query_mod = @import("../../graph/query.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const lease_mod = @import("lease.zig");
const mapper = @import("document/mapper.zig");
const paths_mod = @import("../../graph/paths.zig");
const platform = @import("antfly_platform");
const portable_backup = @import("../portable_backup.zig");
const relationship_filter_mod = @import("../../graph/relationship_filter.zig");
const resolver_catalog_mod = @import("catalog/resolver_catalog.zig");
const schema_registry_mod = @import("schema_registry.zig");
const shard_mod = @import("../shard.zig");
const std = @import("std");
const test_enrichment = @import("test_support.zig").test_enrichment;
const test_index = @import("test_support.zig").test_index;
const storage_engine = @import("../engine.zig");
const table_catalog_mod = @import("table_catalog.zig");
const transactions_mod = @import("../transactions.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const ttl_mod = @import("../ttl.zig");
const types = @import("types.zig");
const Allocator = db_zig.Allocator;
const AsyncContext = db_zig.AsyncContext;
const BatchExecutionContext = db_zig.BatchExecutionContext;
const BatchExecutionOptions = db_zig.BatchExecutionOptions;
const BatchProfile = db_zig.BatchProfile;
const DocumentArtifactChildRangeDispatch = db_zig.DocumentArtifactChildRangeDispatch;
const DocumentArtifactChildRangeDispatcher = db_zig.DocumentArtifactChildRangeDispatcher;
const GateAssetProducer = db_zig.GateAssetProducer;
const GraphArtifactRestoreMaterializationPage = db_zig.GraphArtifactRestoreMaterializationPage;
const GraphPrimaryPublicationTest = db_zig.GraphPrimaryPublicationTest;
const GraphTtlCandidate = db_zig.GraphTtlCandidate;
const IndexTargetVisibility = db_zig.IndexTargetVisibility;
const Io = db_zig.Io;
const ManagedSyncTargets = db_zig.ManagedSyncTargets;
const NamedResultSet = db_zig.NamedResultSet;
const NodeAdmission = db_zig.NodeAdmission;
const PrimaryBackend = db_zig.PrimaryBackend;
const QueryVisibilityEvent = db_zig.QueryVisibilityEvent;
const TtlCleanupContext = db_zig.TtlCleanupContext;
const acquireNeighborContextGraphSource = db_zig.acquireNeighborContextGraphSource;
const appendGraphAssetStateSegmentDeleteKeys = db_zig.appendGraphAssetStateSegmentDeleteKeys;
const applyDerivedBatchToIndexAsync = db_zig.applyDerivedBatchToIndexAsync;
const applyDerivedBatchToIndexContextProfiled = db_zig.applyDerivedBatchToIndexContextProfiled;
const applyGraphArtifactMutationPages = db_zig.applyGraphArtifactMutationPages;
const applyGraphIntersection = db_zig.applyGraphIntersection;
const applyGraphUnion = db_zig.applyGraphUnion;
const applySplitGraphArtifactsInRange = db_zig.applySplitGraphArtifactsInRange;
const augmentExtractedWriteWithGraphFieldEdgesParsed = db_zig.augmentExtractedWriteWithGraphFieldEdgesParsed;
const batchAdvancesManagedIndexApplyStateForReplay = db_zig.batchAdvancesManagedIndexApplyStateForReplay;
const batchAffectsManagedIndex = db_zig.batchAffectsManagedIndex;
const batchAffectsManagedIndexForReplay = db_zig.batchAffectsManagedIndexForReplay;
const bytesToHexAlloc = db_zig.bytesToHexAlloc;
const castOwnedKeysToConst = db_zig.castOwnedKeysToConst;
const collectGraphMutationsForArtifacts = db_zig.collectGraphMutationsForArtifacts;
const collectManagedSyncTargetsForRecord = db_zig.collectManagedSyncTargetsForRecord;
const collectManagedSyncTargetsForRecordWithBatch = db_zig.collectManagedSyncTargetsForRecordWithBatch;
const commitGraphContenderReconcilePage = db_zig.commitGraphContenderReconcilePage;
const containsDeleteKey = db_zig.containsDeleteKey;
const currentTimeNs = db_zig.currentTimeNs;
const decodeArtifactRefIfKnownAlloc = db_zig.decodeArtifactRefIfKnownAlloc;
const directGraphNeighborContextHintsAlloc = db_zig.directGraphNeighborContextHintsAlloc;
const documentRangeLowerAlloc = db_zig.documentRangeLowerAlloc;
const documentRangeUpperAlloc = db_zig.documentRangeUpperAlloc;
const encodeChangeRecordPayload = db_zig.encodeChangeRecordPayload;
const encodeDirectChangeRecordPayload = db_zig.encodeDirectChangeRecordPayload;
const encodeGraphEdgeArtifactWithTtlAlloc = db_zig.encodeGraphEdgeArtifactWithTtlAlloc;
const encodeGraphSourceEdgeArtifactWithTtlAlloc = db_zig.encodeGraphSourceEdgeArtifactWithTtlAlloc;
const encodeStoreLookupKeyAlloc = db_zig.encodeStoreLookupKeyAlloc;
const encodeThinReplayRecordPayload = db_zig.encodeThinReplayRecordPayload;
const executeDeleteBatchContext = db_zig.executeDeleteBatchContext;
const expectPhysicalSplitRetirements = db_zig.expectPhysicalSplitRetirements;
const expireDirectGraphTtlCandidateContext = db_zig.expireDirectGraphTtlCandidateContext;
const expireGraphTtlCandidateContext = db_zig.expireGraphTtlCandidateContext;
const filterExpiredSearchResult = db_zig.filterExpiredSearchResult;
const freeGraphWrites = db_zig.freeGraphWrites;
const freeOptionalOwnedBytes = db_zig.freeOptionalOwnedBytes;
const freeOwnedConstKeySlice = db_zig.freeOwnedConstKeySlice;
const freeOwnedKeySlice = db_zig.freeOwnedKeySlice;
const graphArtifactStateNameAlloc = db_zig.graphArtifactStateNameAlloc;
const graphAssetStateKeyAlloc = db_zig.graphAssetStateKeyAlloc;
const graphCleanupReplayWritesAlloc = db_zig.graphCleanupReplayWritesAlloc;
const graphEndpointCleanupDeletesAlloc = db_zig.graphEndpointCleanupDeletesAlloc;
const graphTestApplyOrdered = db_zig.graphTestApplyOrdered;
const graphWritesFromArtifactParsedPageAlloc = db_zig.graphWritesFromArtifactParsedPageAlloc;
const graphWritesFromArtifactValueAlloc = db_zig.graphWritesFromArtifactValueAlloc;
const graph_edge_ttl_due_rebuild_marker = db_zig.graph_edge_ttl_due_rebuild_marker;
const initStoppedTtlRuntimeForTest = db_zig.initStoppedTtlRuntimeForTest;
const isMetadataKey = db_zig.isMetadataKey;
const journalRecordHasHint = db_zig.journalRecordHasHint;
const loadGraphAssetStateKeysAlloc = db_zig.loadGraphAssetStateKeysAlloc;
const loadManagedAppliedSequenceContext = db_zig.loadManagedAppliedSequenceContext;
const loadStoredSearchDocumentCallback = db_zig.loadStoredSearchDocumentCallback;
const loadStoredSearchDocumentManyCallback = db_zig.loadStoredSearchDocumentManyCallback;
const loadStoredSearchDocumentsMany = db_zig.loadStoredSearchDocumentsMany;
const lockApply = db_zig.lockApply;
const lockApplyShared = db_zig.lockApplyShared;
const lockAtomicWithBackoff = db_zig.lockAtomicWithBackoff;
const materializeGraphSourceArtifactRestorePage = db_zig.materializeGraphSourceArtifactRestorePage;
const materializeGraphSourceArtifactsForIndex = db_zig.materializeGraphSourceArtifactsForIndex;
const missingAsNull = db_zig.missingAsNull;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const no_background_test_open = db_zig.no_background_test_open;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const platform_clock = db_zig.platform_clock;
const platform_time = db_zig.platform_time;
const prepareDirectChangeRecord = db_zig.prepareDirectChangeRecord;
const pruneStaleDirectGraphTtlCandidateContext = db_zig.pruneStaleDirectGraphTtlCandidateContext;
const readRestoreStateForPathAllocWithIo = db_zig.readRestoreStateForPathAllocWithIo;
const readU32Big = db_zig.readU32Big;
const rebuildGraphReplacementDocumentsWithRead = db_zig.rebuildGraphReplacementDocumentsWithRead;
const reconcileSingleGraphStateContenders = db_zig.reconcileSingleGraphStateContenders;
const recordArtifactRepairIssueContextForIndexSource = db_zig.recordArtifactRepairIssueContextForIndexSource;
const relationalColumns = db_zig.relationalColumns;
const releaseReplicationMutationShared = db_zig.releaseReplicationMutationShared;
const retireSplitEntitySourcedEdgesLocked = db_zig.retireSplitEntitySourcedEdgesLocked;
const rollbackGraphSourceArtifactRestoreStatePage = db_zig.rollbackGraphSourceArtifactRestoreStatePage;
const sleepNs = db_zig.sleepNs;
const testChildRangeDestination = db_zig.testChildRangeDestination;
const threadedIo = db_zig.threadedIo;
const validateGraphEdgeMetadataJson = db_zig.validateGraphEdgeMetadataJson;
const writeRestoreStateForPathWithIo = db_zig.writeRestoreStateForPathWithIo;
const GeneratedEnrichmentReplayPageResult = DB.GeneratedEnrichmentReplayPageResult;
const GraphEndpointCleanupBatch = DB.GraphEndpointCleanupBatch;
const GraphKeyAdmission = DB.GraphKeyAdmission;
const GraphMetricServiceMaintenanceRequest = DB.GraphMetricServiceMaintenanceRequest;
const GraphNodeAdmissionCache = DB.GraphNodeAdmissionCache;
const GraphOwnershipRestoreCursor = DB.GraphOwnershipRestoreCursor;
const GraphOwnershipRestorePage = DB.GraphOwnershipRestorePage;
const GraphPredicateExecutionContext = DB.GraphPredicateExecutionContext;
const GraphReplacementDrive = DB.GraphReplacementDrive;
const GraphReplacementIdentity = DB.GraphReplacementIdentity;
const PreparedGraphReplacement = DB.PreparedGraphReplacement;
const cloneGraphMetricStatusFromGraph = DB.cloneGraphMetricStatusFromGraph;
const executeKShortestPathsCallback = DB.executeKShortestPathsCallback;
const executeKShortestPathsWithAdmissionCallback = DB.executeKShortestPathsWithAdmissionCallback;
const executeShortestPathCallback = DB.executeShortestPathCallback;
const executeShortestPathWithAdmissionCallback = DB.executeShortestPathWithAdmissionCallback;
const graph_ownership_restore_cursor_magic = DB.graph_ownership_restore_cursor_magic;
const graph_ownership_restore_cursor_magic_v1 = DB.graph_ownership_restore_cursor_magic_v1;
const graph_ownership_restore_cursor_magic_v2 = DB.graph_ownership_restore_cursor_magic_v2;
const graph_ownership_restore_cursor_magic_v3 = DB.graph_ownership_restore_cursor_magic_v3;
const loadProjectedSearchDocumentCallback = DB.loadProjectedSearchDocumentCallback;
const loadProjectedSearchDocumentManyCallback = DB.loadProjectedSearchDocumentManyCallback;
const lookupLiveDocOrdinalNoLockCallback = DB.lookupLiveDocOrdinalNoLockCallback;
const replacementRetryable = DB.replacementRetryable;
const replacement_max_backoff_ns = DB.replacement_max_backoff_ns;
const replacement_total_retry_limit = DB.replacement_total_retry_limit;
const resolveDocIdsToDocSetCallback = DB.resolveDocIdsToDocSetCallback;
const resolveDocSetDocIdsCallback = DB.resolveDocSetDocIdsCallback;
const resolveSearchHitsToDocSetCallback = DB.resolveSearchHitsToDocSetCallback;

pub fn initOptionalGraphMetricRuntime(self: *DB, cfg: graph_metric_runtime_mod.Config) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cfg });
    if (comptime builtin.os.tag == .freestanding) return;
    if (!self.start_index_workers) return;
    // Manual backends with only filesystem authority cannot own a
    // background worker. Their caller drives maintenance explicitly.
    if (self.backend_runtime.io() == null) return;
    const resources = self.core.asyncResources();
    self.graph_metric_owner = try @TypeOf(self.graph_metric_owner).create(self.runtime_alloc, .{
        self.runtime_alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        self.backend_runtime,
        cfg.forDbOwner(),
    });
    const runtime = self.graph_metric_owner.runtime.?;
    self.graph_metric_runtime = runtime;
    resources.index_manager.graph_metric_notify = .{ .ptr = runtime, .notify = graph_metric_runtime_mod.GraphMetricRuntime.notifyOpaque };
}

pub fn drainStandaloneGraphEndpointCleanup(self: *DB) !bool {
    return drainStandaloneGraphEndpointCleanupWithOptions(self, .{}, std.math.maxInt(usize));
}

pub fn drainStandaloneGraphEndpointCleanupWithOptions(self: *DB, opts: BatchExecutionOptions, max_pages: usize) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    // Most batches create no endpoint jobs. Avoid authority probes and
    // maintenance admission altogether on their completion path.
    if (!try self.core.store.hasGraphEndpointCleanup()) return false;
    if (!try canRunLocalGraphEndpointCleanup(self)) return false;
    var pages: usize = 0;
    while (pages < max_pages and try self.core.store.hasGraphEndpointCleanup()) {
        if (!try canRunLocalGraphEndpointCleanup(self)) break;
        // The primary mutation is already durable. Cancellation can leave
        // queued maintenance for the resident owner, but must never interrupt
        // a page halfway through its atomic commit. Explicit drains use .none.
        if (opts.visibility_cancellation.isCancelled()) return error.EnrichmentWaitCanceled;
        try self.batchInternal(.{ .graph_endpoint_cleanup = true, .sync_level = .write }, null, .{
            .document_child_range_dispatcher = opts.document_child_range_dispatcher,
            .committed_batch_effects_observer = opts.committed_batch_effects_observer,
        });
        pages += 1;
    }
    return pages != 0;
}

/// Shared completion for ordinary, profiled, callback and transaction
/// batches, after the serialized primary apply section has finished.
/// Replicated apply must only execute its exact command; its owner drives
/// cleanup independently through subsequent ordered Raft entries.
pub fn finishBatchGraphEndpointCleanup(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    if (req.graph_endpoint_cleanup or !opts.wait_for_sync_level or
        opts.ordered_apply_receipt != null or opts.replication_applied_lsn_marker != null or
        opts.native_initial_child_entry != null or opts.restore_staging != null or
        req.restore_staging_scope != null or req.restore_staging != null) return;
    // Weak sync levels pay for at most one bounded maintenance page.
    // Durable jobs continue through the resident scheduler; full_index
    // and explicit maintenance retain complete cleanup guarantees.
    const max_pages: usize = if (req.sync_level == .full_index) std.math.maxInt(usize) else 1;
    const drained = try drainStandaloneGraphEndpointCleanupWithOptions(self, opts, max_pages);
    // A resident worker can finish before the foreground queue probe.
    // Deletes/transforms must still include its newly committed replay cut.
    if (req.sync_level == .full_index and (drained or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0))
        try self.waitForCurrentSyncLevelWithCancellation(.full_index, opts.visibility_cancellation);
}

/// Local maintenance follows the same captured write admission as user writes.
/// A retained standby/fenced generation may open and replay exact commands,
/// but cannot choose cleanup effects or mutate its local directory on its own.
pub fn graphEndpointCleanupWriteAuthority(self: *DB) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    return if (self.local_execution.replication_write_gate) |gate| gate.allowsBackgroundWrite() else true;
}

pub fn canRunLocalGraphEndpointCleanup(self: *DB) !bool {
    if (!try graphEndpointCleanupWriteAuthority(self)) return false;
    return !try self.maintenanceRequiresOrderedApply();
}

pub fn startGraphEndpointCleanupWorker(self: *DB) void {
    if (comptime builtin.os.tag == .freestanding) return;
    if (!self.stable_address or !self.local_execution.optional_runtime_workers_enabled or self.open_mode != .writer) return;
    if (!(canRunLocalGraphEndpointCleanup(self) catch false)) return;
    self.graph_cleanup_owner.start(graphCleanupPort(self));
}

pub fn graphCleanupPort(self: *DB) graph_cleanup_owner_mod.Owner.Port {
    return .{ .ptr = self, .runtime = self.backend_runtime, .run = graphCleanupPage };
}

pub fn graphCleanupPage(ptr: *anyopaque) !bool {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return self.runStandaloneGraphEndpointCleanupStep();
}

pub fn stopGraphEndpointCleanupWorker(self: *DB) void {
    self.graph_cleanup_owner.stop(self.backend_runtime.io());
}

pub fn graphEndpointCleanupWorkerStep(self: *DB) ?u64 {
    return self.graph_cleanup_owner.step(graphCleanupPort(self));
}

/// Every enqueue path is serviced independently of subsequent foreground
/// writes. Each scheduler turn commits at most one bounded page.
pub fn runStandaloneGraphEndpointCleanupStep(self: *DB) !bool {
    if (!try canRunLocalGraphEndpointCleanup(self)) return false;
    if (!try self.core.store.hasGraphEndpointCleanup()) {
        // A ready directory needs no work. Idle owners must not contend
        // with user mutations for the apply lock every scheduler turn.
        if (try self.core.store.graphIncomingDirectoryReady()) return false;
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        if (!try canRunLocalGraphEndpointCleanup(self)) return false;
        if (self.core.index_manager.hasGraphIndexes() and !try self.core.store.graphIncomingDirectoryReady())
            return !try self.core.store.backfillGraphIncomingDirectoryPage();
        return false;
    }
    try self.batchInternal(.{ .graph_endpoint_cleanup = true, .sync_level = .write }, null, .{});
    return true;
}

/// The leader selects exact identities before proposal. Followers apply
/// these afterimages rather than consulting their local directory progress.
pub fn prepareGraphEndpointCleanupBatch(self: *DB, alloc: Allocator) !?GraphEndpointCleanupBatch {
    if (!try graphEndpointCleanupWriteAuthority(self)) return null;
    if (!try self.core.store.hasGraphEndpointCleanup() and try self.core.store.graphIncomingDirectoryReady()) return null;
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (!try graphEndpointCleanupWriteAuthority(self)) return null;
    if (!try self.core.store.hasGraphEndpointCleanup()) {
        // Build local directory coverage ahead of future endpoint deletes,
        // without proposing empty Raft maintenance entries.
        if (self.core.index_manager.hasGraphIndexes() and !try self.core.store.graphIncomingDirectoryReady())
            _ = try self.core.store.backfillGraphIncomingDirectoryPage();
        return null;
    }
    var page = (try self.core.store.prepareGraphEndpointCleanupPage(alloc)) orelse return null;
    errdefer page.deinit();
    const replay_writes = try graphCleanupReplayWritesAlloc(alloc, page);
    errdefer alloc.free(replay_writes);
    return .{ .page = page, .graph_deletes = try graphEndpointCleanupDeletesAlloc(alloc, page), .replay_writes = replay_writes };
}

pub fn requireGraphEndpointCleanupReady(self: *DB) !void {
    if (!try self.core.store.graphEndpointCleanupBlocksReads()) return;
    return error.StorageBusy;
}

pub fn updateRestoreGraphOwnershipCursorWithIo(
    self: *DB,
    alloc: Allocator,
    io: Io,
    cursor: []const u8,
) !void {
    var state = (try readRestoreStateForPathAllocWithIo(alloc, io, self.core.path)) orelse return error.InvalidRestoreState;
    defer state.deinit(alloc);
    if (!std.mem.eql(u8, state.phase, "rebuild_graph") or state.runtime_repair_complete) {
        return error.InvalidRestoreState;
    }
    const encoded_cursor = try bytesToHexAlloc(alloc, cursor);
    alloc.free(state.graph_ownership_cursor);
    state.graph_ownership_cursor = encoded_cursor;
    try writeRestoreStateForPathWithIo(alloc, io, self.core.path, state);
}

pub fn rebuildGraphDerivedState(self: *DB) !usize {
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    // Portable restore imports graph edge artifacts before the table's
    // configured indexes are opened. Materialize those durable artifacts
    // before repairing reverse edges; rebuilding reverse state alone would
    // silently publish an empty graph for an otherwise valid backup.
    try applySplitGraphArtifactsInRange(
        self.alloc,
        self.getRange().start,
        self.getRange().end,
        self.core.store,
        self.core.index_manager,
    );
    return try self.core.index_manager.rebuildGraphSplitDestination(
        self.getRange().start,
        self.getRange().end,
    );
}

pub fn decodeGraphOwnershipRestoreCursor(cursor: ?[]const u8) !?GraphOwnershipRestoreCursor {
    const raw = cursor orelse return null;
    if (std.mem.startsWith(u8, raw, graph_ownership_restore_cursor_magic_v1)) {
        const header_len = graph_ownership_restore_cursor_magic_v1.len + @sizeOf(u32);
        if (raw.len <= header_len) return error.InvalidRestoreState;
        return .{
            .artifact_key = raw[header_len..],
            .graph_index_offset = std.mem.readInt(u32, raw[graph_ownership_restore_cursor_magic_v1.len..header_len], .big),
        };
    }
    const v2_header_len = graph_ownership_restore_cursor_magic_v2.len + 4 * @sizeOf(u32);
    if (std.mem.startsWith(u8, raw, graph_ownership_restore_cursor_magic_v2)) {
        if (raw.len <= v2_header_len) return error.InvalidRestoreState;
        var pos = graph_ownership_restore_cursor_magic_v2.len;
        return .{
            .artifact_key = raw[v2_header_len..],
            .graph_index_offset = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
            .item_offset = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
            .segment_index = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
            .key_count = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
        };
    }
    const v3_header_len = graph_ownership_restore_cursor_magic_v3.len + 5 * @sizeOf(u32);
    if (std.mem.startsWith(u8, raw, graph_ownership_restore_cursor_magic_v3)) {
        if (raw.len <= v3_header_len) return error.InvalidRestoreState;
        var pos = graph_ownership_restore_cursor_magic_v3.len;
        return .{
            .artifact_key = raw[v3_header_len..],
            .graph_index_offset = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
            .item_offset = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
            .segment_index = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
            .key_count = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
            .manifest_bytes = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
        };
    }
    const header_len = graph_ownership_restore_cursor_magic.len + 1 + 5 * @sizeOf(u32);
    if (std.mem.startsWith(u8, raw, graph_ownership_restore_cursor_magic)) {
        if (raw.len <= header_len) return error.InvalidRestoreState;
    } else {
        return null;
    }
    var pos = graph_ownership_restore_cursor_magic.len;
    const mode = raw[pos];
    pos += 1;
    if (mode > 1) return error.InvalidRestoreState;
    return .{
        .artifact_key = raw[header_len..],
        .graph_index_offset = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
        .item_offset = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
        .segment_index = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
        .key_count = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
        .manifest_bytes = readU32Big(raw, &pos) catch return error.InvalidRestoreState,
        .rollback = mode == 1,
    };
}

pub fn encodeGraphOwnershipRestoreCursorAlloc(
    alloc: Allocator,
    artifact_key: []const u8,
    graph_index_offset: usize,
    item_offset: usize,
    segment_index: usize,
    key_count: usize,
    manifest_bytes: usize,
    rollback: bool,
) ![]u8 {
    if (graph_index_offset > std.math.maxInt(u32) or item_offset > std.math.maxInt(u32) or
        segment_index > std.math.maxInt(u32) or key_count > std.math.maxInt(u32) or
        manifest_bytes > std.math.maxInt(u32)) return error.InvalidRestoreState;
    const header_len = graph_ownership_restore_cursor_magic.len + 1 + 5 * @sizeOf(u32);
    const out = try alloc.alloc(u8, header_len + artifact_key.len);
    @memcpy(out[0..graph_ownership_restore_cursor_magic.len], graph_ownership_restore_cursor_magic);
    var pos = graph_ownership_restore_cursor_magic.len;
    out[pos] = if (rollback) 1 else 0;
    pos += 1;
    for ([_]usize{ graph_index_offset, item_offset, segment_index, key_count, manifest_bytes }) |value| {
        std.mem.writeInt(u32, out[pos..][0..4], @intCast(value), .big);
        pos += 4;
    }
    @memcpy(out[header_len..], artifact_key);
    return out;
}

pub fn rebuildOneGraphArtifactOwnershipForRestore(
    self: *DB,
    alloc: Allocator,
    artifact_key: []const u8,
    graph_index_offset: usize,
    item_offset: usize,
    segment_index: usize,
    key_count: usize,
    manifest_bytes: usize,
    rollback: bool,
) !GraphOwnershipRestorePage {
    const graph_indexes = self.core.index_manager.graphIndexes();
    if (graph_index_offset >= graph_indexes.len) return error.InvalidRestoreState;
    const index_name = graph_indexes[graph_index_offset].config.name;

    if (rollback) {
        var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse return error.InvalidRestoreState;
        defer artifact_ref.deinit(alloc);
        const more = try rollbackGraphSourceArtifactRestoreStatePage(self, alloc, index_name, artifact_ref);
        return .{
            .replayed = 1,
            .next_cursor = if (more)
                try encodeGraphOwnershipRestoreCursorAlloc(alloc, artifact_key, graph_index_offset, 0, 0, 0, 0, true)
            else if (graph_index_offset + 1 < graph_indexes.len)
                try encodeGraphOwnershipRestoreCursorAlloc(alloc, artifact_key, graph_index_offset + 1, 0, 0, 0, 0, false)
            else
                try alloc.dupe(u8, artifact_key),
        };
    }

    // Runtime restore repair operates on an unpublished staged generation
    // with optional workers disabled. Parsing, contender reconstruction,
    // and mutation filtering can be proportional to one admitted artifact;
    // do not hold the shard apply lock across that work. The durable restore
    // cursor serializes these units and advances only after putBatch returns.
    // Legacy v0.2 artifacts are monolithic. Admit a bounded relation and
    // byte window so a single restored document cannot monopolize memory
    // or produce an unbounded store batch. Invalid/oversized inputs are
    // durable source-local repair debt: they must not brick the remainder
    // of an otherwise healthy restore.
    const materialized: ?GraphArtifactRestoreMaterializationPage = materializeGraphSourceArtifactRestorePage(
        self,
        alloc,
        artifact_key,
        index_name,
        item_offset,
        segment_index,
        key_count,
        manifest_bytes,
    ) catch |err| switch (err) {
        error.ArtifactRepairRequired => null,
        error.ResourceLimitExceeded, error.InvalidGraphArtifact => {
            var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse return error.InvalidRestoreState;
            defer artifact_ref.deinit(alloc);
            const source = self.core.index_manager.graphArtifactSourceForArtifact(
                index_name,
                artifact_ref.name,
            ) orelse return error.InvalidRestoreState;
            try recordArtifactRepairIssueContextForIndexSource(
                self.async_context,
                .graph,
                index_name,
                artifact_ref.document_id,
                "",
                artifact_ref.unit_id orelse "",
                source.artifact_name,
                if (artifact_ref.source) |source_ref| source_ref.name else "",
                artifact_ref.name,
                artifact_key,
                artifact_ref.chunk_id,
                0,
                if (err == error.ResourceLimitExceeded) .resource_limit_exceeded else .corrupt_artifact,
            );
            const more = try rollbackGraphSourceArtifactRestoreStatePage(self, alloc, index_name, artifact_ref);
            return .{
                .replayed = 1,
                .next_cursor = if (more)
                    try encodeGraphOwnershipRestoreCursorAlloc(alloc, artifact_key, graph_index_offset, 0, 0, 0, 0, true)
                else if (graph_index_offset + 1 < graph_indexes.len)
                    try encodeGraphOwnershipRestoreCursorAlloc(alloc, artifact_key, graph_index_offset + 1, 0, 0, 0, 0, false)
                else
                    try alloc.dupe(u8, artifact_key),
            };
        },
        else => return err,
    };

    if (materialized) |page| {
        if (page.next_item_offset) |next_item_offset| return .{
            .replayed = 1,
            .next_cursor = try encodeGraphOwnershipRestoreCursorAlloc(
                alloc,
                artifact_key,
                graph_index_offset,
                next_item_offset,
                page.next_segment_index,
                page.key_count,
                page.manifest_bytes,
                false,
            ),
        };
    }

    return .{
        .replayed = 1,
        .next_cursor = if (graph_index_offset + 1 < graph_indexes.len)
            try encodeGraphOwnershipRestoreCursorAlloc(alloc, artifact_key, graph_index_offset + 1, 0, 0, 0, 0, false)
        else
            try alloc.dupe(u8, artifact_key),
    };
}

pub fn rebuildGraphArtifactOwnershipForRestorePage(
    self: *DB,
    alloc: Allocator,
    cursor: ?[]const u8,
) !GraphOwnershipRestorePage {
    if (!self.core.index_manager.hasGraphIndexes()) return .{ .complete = true };
    if (try decodeGraphOwnershipRestoreCursor(cursor)) |continuation| {
        return try rebuildOneGraphArtifactOwnershipForRestore(
            self,
            alloc,
            continuation.artifact_key,
            continuation.graph_index_offset,
            continuation.item_offset,
            continuation.segment_index,
            continuation.key_count,
            continuation.manifest_bytes,
            continuation.rollback,
        );
    }

    // Scanning is bounded independently from materialization. Once an
    // artifact is found, process exactly one graph index and persist a
    // continuation for the remaining indexes. This prevents source count,
    // graph index count, and artifact size from multiplying in one quantum.
    const scan_page_size: usize = 128;
    const user_prefix = [_]u8{internal_keys.user_namespace};
    const keys = try self.core.store.scanPrefixKeysPage(alloc, &user_prefix, cursor, scan_page_size);
    defer freeOwnedKeySlice(alloc, keys);
    if (keys.len == 0) {
        if (self.graph_restore_parse_cache) |*cache| cache.deinit(self.alloc);
        self.graph_restore_parse_cache = null;
        return .{ .complete = true };
    }

    var consumed_rows: usize = 0;
    for (keys) |key| {
        const artifact_row = internal_keys.isAssetArtifactKey(key) or
            internal_keys.isChunkArtifactRecordKey(key) or
            internal_keys.isResolutionArtifactKey(key);
        consumed_rows += 1;
        if (artifact_row) {
            return try rebuildOneGraphArtifactOwnershipForRestore(self, alloc, key, 0, 0, 0, 0, 0, false);
        }
    }

    return .{
        .replayed = 0,
        .next_cursor = if (consumed_rows < keys.len or keys.len == scan_page_size)
            try alloc.dupe(u8, keys[consumed_rows - 1])
        else
            null,
        .complete = consumed_rows == keys.len and keys.len < scan_page_size,
    };
}

/// Endpoint deletion is a document mutation, including TTL and deletes
/// prepared by another transaction. Reuse the durable shared read guards
/// so it cannot enqueue cleanup after an inline relationship votes prepared.
/// The guards survive restart and are released atomically with resolution.
/// Fact-owned projections deliberately have an independent lifecycle.
pub fn prepareGraphEndpointDependenciesLocked(
    self: *DB,
    alloc: Allocator,
    scratch: Allocator,
    txn_id: types.TxnId,
    intents: []const transactions_mod.WriteIntent,
    schema_view: ?schema_registry_mod.SchemaView,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
) !void {
    if (!self.core.hasGraphIndexes()) return;
    var manager = try self.core.initTxnManager();
    defer manager.deinit();
    var targets = std.StringHashMapUnmanaged(void).empty;
    defer targets.deinit(scratch);
    var read = try self.core.store.beginProbeTxn();
    defer read.abort();
    const owning_table: ?[]const u8 = self.local_execution.row_policy_table_name orelse (read.get(internal_keys.graph_owning_table_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    });
    var rows = std.heap.ArenaAllocator.init(alloc);
    defer rows.deinit();
    const row_alloc = rows.allocator();
    for (intents) |intent| {
        if (isMetadataKey(intent.key)) continue;
        const value = intent.value orelse continue;
        defer _ = rows.reset(.retain_capacity);
        var prepared: ?mapper.PreparedRelationalWrite = if (intent.prepared_row != null) prepared_row: {
            const view = schema_view orelse return error.PreparedGenerationChanged;
            break :prepared_row try mapper.PreparedRelationalWrite.initFromIntent(row_alloc, intent.key, value, view.validator(), view.tableSchema().*, view.physicalLayout(), intent.prepared_row);
        } else null;
        defer if (prepared) |*row| row.deinit(row_alloc);
        var parsed: ?std.json.Parsed(std.json.Value) = if (prepared == null)
            try std.json.parseFromSlice(std.json.Value, row_alloc, value, .{ .parse_numbers = false })
        else
            null;
        defer if (parsed) |*root| root.deinit();
        if (prepared) |*row| {
            // AROW columns are lazy; materialize only fields consumed by
            // graph mappings before planning durable endpoint guards.
            for (self.core.graphIndexes()) |entry| {
                for (entry.edge_type_configs) |edge_cfg| {
                    if (edge_cfg.field_name) |name| try row.requireLogicalField(name);
                }
            }
        }
        const root = if (prepared) |*row| row.parsedValue() else parsed.?.value;
        var extracted = if (prepared) |*row| row.takeExtracted() else try mapper.extractWriteFromParsed(row_alloc, intent.key, value, root);
        defer extracted.deinit(row_alloc);
        try augmentExtractedWriteWithGraphFieldEdgesParsed(self, row_alloc, intent.key, root, &extracted);
        for (extracted.graph_writes) |edge| {
            if (edge.owner_document.len != 0 and !std.mem.eql(u8, edge.owner_document, edge.source)) continue;
            if (edge.owner.len != 0 and !std.mem.eql(u8, edge.owner, edge.source)) continue;
            var table_scratch = graph_metadata_tables.Scratch.init(row_alloc, null);
            defer table_scratch.deinit();
            const source_table = try table_scratch.table(edge.metadata_json, "source_table");
            const target_table = try table_scratch.table(edge.metadata_json, "target_table");
            if (!graph_metadata_tables.inlineEndpointsAreLocal(source_table, target_table, owning_table)) continue;
            if (targets.contains(edge.target)) continue;
            const target = try scratch.dupe(u8, edge.target);
            // Request preparation owns all predicate bytes until prepare.
            const job = try internal_keys.graphEndpointCleanupKeyAlloc(scratch, target);
            _ = read.get(job) catch |err| switch (err) {
                error.NotFound => {
                    if (!try manager.hasSharedReadDependency(txn_id, target)) {
                        if (predicates.items.len >= transactions_mod.max_read_guards_per_transaction) return error.TransactionTooLarge;
                        const version = (try ttl_mod.readTimestamp(self.core.store, alloc, target)) orelse 0;
                        try predicates.append(alloc, .{ .key = target, .expected_version = version });
                    }
                    try targets.put(scratch, target, {});
                    continue;
                },
                else => return err,
            };
            return error.IntegrityTopologyBusy;
        }
    }
}

/// Physical graph reads retain the same admission, identity fence, and
/// scan budget through selection and the owned result.
pub fn graphEdgesForInternalReadBoundedAt(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    key: []const u8,
    edge_types: []const []const u8,
    direction: graph_mod.EdgeDirection,
    identity_read_generation: ?u64,
    ttl_now_ns: u64,
    max_edges: usize,
    max_owned_bytes: usize,
    max_scanned_rows: usize,
) !graph_mod.GraphIndex.BoundedEdgeResult {
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    _ = try self.currentIdentityReadGenerationForRequest(identity_read_generation);
    const entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
    const result = try entry.index.getEdgesByTypesBoundedWithStatsAt(alloc, key, edge_types, direction, max_edges, max_owned_bytes, max_scanned_rows, if (ttl_now_ns == 0) entry.index.clock.nowRealtimeNs() else ttl_now_ns);
    errdefer graph_mod.GraphIndex.freeEdges(alloc, result.edges);
    try row_policy_lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    return result;
}

pub fn graphResultNodePathAlloc(alloc: Allocator, node: graph_query_mod.GraphResultNode) !?paths_mod.Path {
    const node_path = node.path orelse return null;
    const edge_path = node.path_edges orelse return null;
    const nodes = try alloc.alloc([]const u8, node_path.len);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[0..initialized_nodes]) |item| alloc.free(item);
        alloc.free(nodes);
    }
    for (node_path, 0..) |item, i| {
        nodes[i] = try alloc.dupe(u8, item);
        initialized_nodes += 1;
    }

    const edges = try alloc.alloc(paths_mod.PathEdge, edge_path.len);
    var initialized_edges: usize = 0;
    errdefer {
        for (edges[0..initialized_edges]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.edge_id.len > 0) alloc.free(edge.edge_id);
            if (edge.owner_document.len > 0) alloc.free(edge.owner_document);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        alloc.free(edges);
    }
    for (edge_path, 0..) |item, i| {
        edges[i] = try paths_mod.clonePathEdge(alloc, item);
        initialized_edges += 1;
    }
    const total_weight = try paths_mod.sumPathEdgeWeights(edges);

    return .{
        .nodes = nodes,
        .edges = edges,
        .total_weight = total_weight,
        .length = @intCast(edges.len),
    };
}

pub fn graphInputSetHitsAlloc(
    self: *DB,
    alloc: Allocator,
    hit_ids: []const []const u8,
    generation: ?u64,
) ![]types.SearchHit {
    const ordinals = try self.lookupLiveDocOrdinalsNoLock(alloc, hit_ids, generation);
    defer alloc.free(ordinals);

    const hits = try alloc.alloc(types.SearchHit, hit_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }
    for (hit_ids, 0..) |hit_id, j| {
        hits[j] = .{
            .id = try alloc.dupe(u8, hit_id),
            .doc_ordinal = ordinals[j],
            .score = null,
            .stored_data = null,
            .chunk_hits = &.{},
        };
        initialized += 1;
    }
    return hits;
}

pub fn executeNamedGraphQueries(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    graph_queries: []const types.NamedGraphQuery,
    input_sets: []const types.NamedGraphInputSet,
) ![]types.GraphSearchResult {
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    if (req.identity_read_generation == null) {
        for (input_sets) |input_set| {
            if (input_set.hit_ids.len > 0) return error.UnsupportedQueryRequest;
        }
    }
    const snapshot_req = try self.searchRequestAtCurrentIdentityGeneration(req);

    var named_sets = try alloc.alloc(NamedResultSet, input_sets.len);
    defer alloc.free(named_sets);
    var temp_hits = try alloc.alloc([]types.SearchHit, input_sets.len);
    defer alloc.free(temp_hits);
    var resolved_sets = try alloc.alloc(?doc_set.ResolvedDocSet, input_sets.len);
    defer {
        for (resolved_sets) |*maybe_set| {
            if (maybe_set.*) |*set| set.deinit(alloc);
        }
        if (resolved_sets.len > 0) alloc.free(resolved_sets);
    }
    @memset(resolved_sets, null);
    var initialized_sets: usize = 0;

    for (input_sets, 0..) |input_set, i| {
        resolved_sets[i] = try self.resolveDocSetForIdsNoLockAtGenerationAlloc(alloc, input_set.hit_ids, snapshot_req.identity_read_generation);
        temp_hits[i] = try self.graphInputSetHitsAlloc(alloc, input_set.hit_ids, snapshot_req.identity_read_generation);
        const resolved_doc_set = if (resolved_sets[i]) |*set| set else null;
        named_sets[i] = .{
            .name = input_set.name,
            .hits = temp_hits[i],
            .total_hits = if (input_set.total_hits > 0) input_set.total_hits else @intCast(input_set.hit_ids.len),
            .resolved_doc_set = resolved_doc_set,
            .resolved_doc_set_complete = input_set.total_hits == 0 or @as(u64, input_set.total_hits) <= input_set.hit_ids.len,
        };
        initialized_sets += 1;
    }
    defer {
        for (temp_hits[0..initialized_sets]) |hits| {
            for (hits) |*hit| hit.deinit(alloc);
            if (hits.len > 0) alloc.free(hits);
        }
    }

    return try executeGraphQueriesWithSets(self, alloc, snapshot_req, graph_queries, named_sets);
}

/// Field-derived edges are part of prepared document effects. Preserve
/// their catalog until durable prepares resolve, including prepares made
/// while no graph index existed. Schema leases already survive recovery.
pub fn requireGraphCatalogMutableForTransactions(self: *DB) !void {
    var manager = try self.core.initTxnManager();
    defer manager.deinit();
    if (try manager.hasSchemaLeases()) return error.SchemaInUse;
}

pub fn graphReplacementCutoverBudgetNs(activation_attempts: u32) u64 {
    return (if (activation_attempts >= 8) @as(u64, 1000) else 100) * std.time.ns_per_ms;
}

/// Register a hidden shadow and return. The maintenance pipeline owns its
/// bounded build, replay, durable checkpoints and atomic activation. Return
/// the admitted generation so waits cannot follow a reused target name.
pub fn replaceGraphIndex(self: *DB, old: []const u8, cfg: types.IndexConfig) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, old, cfg });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var owner = self.acquireReplicationMutationShared();
    defer if (owner) |*lease| lease.release();
    try self.enforceReplicationWriteGate();
    const generation = blk: {
        var activity = try self.acquireResolverCatalogActivity(true);
        defer activity.deinit();
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        self.core.repair_replay_mutex.lockUncancelable(self.core.index_manager.checkpointIo());
        defer self.core.repair_replay_mutex.unlock(self.core.index_manager.checkpointIo());
        var probe = try self.core.store.beginProbeTxn();
        defer probe.abort();
        try self.requireOnlineArtifactCatalogMutableLocked(&probe);
        // Manual owners can disable optional runtime construction at open.
        // A staged resolver still needs a synchronous replay driver, even
        // on an empty database with no journal sequence to wake a worker.
        var declaration = try std.json.parseFromSlice(std.json.Value, self.alloc, cfg.config_json, .{});
        defer declaration.deinit();
        if (declaration.value == .object and declaration.value.object.contains("resolvers")) {
            if (self.resolution_runtime == null) try self.initResolutionRuntime();
            if (self.promotion_runtime == null) try self.initPromotionRuntime();
        }
        var stored = try self.core.index_manager.prepareGraphReplacementConfig(self.core.store, old, cfg, self.core.nextDerivedSequence());
        defer stored.deinit(self.core.index_manager.alloc);
        break :blk stored.coverage_generation;
    };
    try self.startResolverReplayRuntimesIfConfigured();
    if (self.resolution_runtime) |runtime| runtime.notifyBackfill();
    self.startArtifactRepairMetadataWorkerIfNeeded();
    return generation;
}

pub fn hasPendingGraphReplacements(self: *DB) bool {
    const jobs = self.core.index_manager.graphReplacementBuildsAlloc(self.core.store, self.alloc) catch return true;
    defer jobs.deinit();
    for (jobs.value) |job| if (job.phase != .failed) return true;
    return false;
}

pub fn graphReplacementStatusAlloc(self: *DB, alloc: Allocator) !std.json.Parsed([]index_manager_mod.GraphReplacementBuild) {
    return self.core.index_manager.graphReplacementBuildsAlloc(self.core.store, alloc);
}

/// Caller owns the returned names and slice. Include upstream artifact
/// producers so diagnostics and source replay describe the same dependency.
pub fn graphReplacementProducerNamesAlloc(self: *DB, a: Allocator, job: index_manager_mod.GraphReplacementBuild) ![][]u8 {
    const cfg = job.config orelse return a.alloc([]u8, 0);
    self.core.index_manager.catalog_mutex.lockShared();
    defer self.core.index_manager.catalog_mutex.unlockShared();
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    var graph = try index_manager_mod.parseGraphConfig(a, cfg.config_json);
    defer graph.deinit(a);
    for (graph.artifact_sources) |source| try names.append(a, source.artifact_name);
    for (self.core.index_manager.resolvers.items) |resolver| {
        if (std.mem.eql(u8, resolver.replacement_index, cfg.name)) try names.append(a, resolver.source_artifact);
    }
    return self.core.index_manager.artifactProducerNamesForArtifactsAlloc(a, names.items);
}

pub fn graphReplacementContext(self: *DB, manager: *index_manager_mod.IndexManager) AsyncContext {
    return .{
        .alloc = self.alloc,
        .io = self.backend_runtime.io(),
        .store = self.core.store,
        .relational_base_rows = relationalColumns(self) != null,
        .index_manager = manager,
        .apply_mutex = self.async_context.apply_mutex,
        .dense_bulk_session_scope = .external,
        .require_graph_resolution_contract = true,
    };
}

/// One fair maintenance quantum. Opening a shadow reuses its durable graph;
/// the saved source cursor and replay watermark never restart the corpus.
pub fn advanceGraphReplacement(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    const pending = try self.advanceGraphReplacementUntil(null);
    if (pending) try self.core.index_manager.checkpointIo().sleep(.fromMilliseconds(1), .awake);
    return pending;
}

pub fn graphReplacementWorkNow(_: *DB) u64 {
    if (builtin.is_test) if (DB.test_graph_replacement_work_now_ns) |now| return now;
    return monotonicTimeNs();
}

pub fn advanceGraphReplacementUntil(self: *DB, deadline: ?u64) !bool {
    return self.advanceGraphReplacementForIdentityUntil(deadline, null, .manual);
}

pub fn advanceGraphReplacementForIdentityUntil(self: *DB, deadline: ?u64, identity: ?GraphReplacementIdentity, drive: GraphReplacementDrive) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    if (deadline) |end| if (self.graphReplacementWorkNow() >= end) return self.hasPendingGraphReplacements();
    if (!self.graph_replacement_mutex.tryLock()) return self.hasPendingGraphReplacements();
    defer self.graph_replacement_mutex.unlock(self.core.index_manager.checkpointIo());
    const jobs = try self.graphReplacementStatusAlloc(self.alloc);
    defer jobs.deinit();
    for (jobs.value) |original| {
        if (identity) |wanted| {
            if (!std.mem.eql(u8, original.name, wanted.name) or original.generation != wanted.generation) continue;
        }
        if (original.phase == .failed) continue;
        var saved = original;
        const retry_now = self.replacementRetryNow();
        if (saved.retry_after_ns > retry_now +| replacement_max_backoff_ns) {
            // Clock correction is local maintenance, not a replicated
            // command. Leave queued jobs untouched when writes are denied
            // or source authority requires ordered application.
            if (!try canRunLocalGraphEndpointCleanup(self)) continue;
            // Persist the correction once. Taking min(now + limit) only
            // in memory on every poll would slide the deadline forever.
            var owner = self.acquireReplicationMutationShared();
            defer if (owner) |*lease| lease.release();
            try self.enforceReplicationWriteGate();
            try self.lockApplyForPortableRuntime();
            defer self.core.unlockApply();
            saved.retry_after_ns = retry_now +| replacement_max_backoff_ns;
            try self.core.index_manager.saveGraphReplacementBuild(self.core.store, saved);
        }
        if (saved.retry_after_ns > retry_now) continue;
        // The native cutover shares the tail's finite escalation policy.
        // Select its budget from this already-loaded durable reservation.
        const work_deadline = if (drive == .cutover)
            self.graphReplacementWorkNow() +| graphReplacementCutoverBudgetNs(saved.activation_attempts)
        else
            deadline;
        return self.advanceGraphReplacementOnce(work_deadline, saved, drive == .detached) catch |err| {
            if (err == error.Canceled or err == error.Cancelled or err == error.EnrichmentWaitCanceled) {
                // A stale detached snapshot yields without failing the job
                // or consuming its retry budget. Interrupting this owner
                // is also not cancelling the durable
                // job. Discard volatile work and preserve its checkpoint.
                self.clearGraphReplacementManager();
                return err;
            }
            if (self.core.index_manager.get(saved.name) == null) {
                if (err == error.Busy or err == error.IntegrityTopologyBusy) return true;
                var owner = self.acquireReplicationMutationShared();
                defer if (owner) |*lease| lease.release();
                try self.enforceReplicationWriteGate();
                var activity = try self.acquireResolverCatalogActivity(true);
                defer activity.deinit();
                try self.lockApplyForPortableRuntime();
                defer self.core.unlockApply();
                // Activation can fail after a newer checkpoint committed.
                // Account against that durable progress, never overwrite it
                // with the scheduler's pre-quantum copy.
                const durable = try self.graphReplacementStatusAlloc(self.alloc);
                defer durable.deinit();
                var job = for (durable.value) |current| {
                    if (std.mem.eql(u8, current.name, saved.name)) break current;
                } else return self.hasPendingGraphReplacements();
                job.last_error = @errorName(err);
                if (replacementRetryable(err) and job.retry_count < 4 and job.total_retry_count < replacement_total_retry_limit) {
                    // Persist wall-clock backoff across owner changes/restarts.
                    // Discard volatile projection state and resume the last
                    // acknowledged checkpoint, including partial tail work.
                    job.retry_after_ns = self.replacementRetryNow() +| (@as(u64, 50) << @intCast(job.retry_count * 2)) * std.time.ns_per_ms;
                    job.retry_count += 1;
                    job.total_retry_count += 1;
                    try self.core.index_manager.saveGraphReplacementBuild(self.core.store, job);
                } else {
                    job.phase = .failed;
                    job.retry_after_ns = 0;
                    try self.core.index_manager.failGraphReplacement(self.core.store, job);
                }
                if (self.resolution_runtime) |runtime| runtime.notifyBackfill();
            }
            self.clearGraphReplacementManager();
            return self.hasPendingGraphReplacements();
        };
    }
    // Sleep only after releasing replacement ownership: cancellation,
    // deletion and other ready jobs must not queue behind retry backoff.
    return self.hasPendingGraphReplacements();
}

pub fn clearGraphReplacementProgress(self: *DB) void {
    if (self.graph_replacement_progress) |progress| if (progress.job.resume_key_b64) |key| self.alloc.free(key);
    self.graph_replacement_progress = null;
}

pub fn clearGraphReplacementManager(self: *DB) void {
    clearGraphReplacementProgress(self);
    if (self.graph_replacement_manager) |manager| {
        // Only checkpointed shadow state is reusable. Never flush an
        // abandoned or retired generation over a newer connection view.
        manager.deinitImmutableSnapshot();
        self.alloc.destroy(manager);
        self.graph_replacement_manager = null;
    }
}

/// Construct the synchronous driver even when optional runtimes were not
/// opened. Serialize publication with producer reconfiguration, outside
/// apply/snapshot admission so joining another owner cannot deadlock.
pub fn ensureGraphReplacementEnrichmentRuntime(self: *DB) !void {
    const created = blk: {
        lockAtomicWithBackoff(&self.async_context.enrichment_lifecycle_mutex);
        defer self.async_context.enrichment_lifecycle_mutex.unlock();
        if (self.enrichment_runtime != null) break :blk false;
        var config: enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
        try self.initOptionalEnrichmentRuntime(&config);
        break :blk true;
    };
    if (created) try self.restartEnrichmentAfterStructuralMutation("graph replacement replay", "");
}

pub fn advanceGraphReplacementOnce(self: *DB, deadline: ?u64, saved: index_manager_mod.GraphReplacementBuild, bounded_replay: bool) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    var owner = self.acquireReplicationMutationShared();
    defer if (owner) |*lease| lease.release();
    try self.enforceReplicationWriteGate();
    if (!saved.producer_replay_complete and self.core.index_manager.minimum_reader_version.load(.acquire) < 3) {
        // Upgrade legacy reservations before appending any producer work.
        // A crash must never leave new prerequisites open to epoch-2 writers.
        try self.lockApplyForPortableRuntime();
        defer self.core.unlockApply();
        try self.core.index_manager.saveGraphReplacementBuild(self.core.store, saved);
    }
    if (saved.producer_replay_complete and saved.producer_replay_target > 0) {
        releaseReplicationMutationShared(&owner);
        try self.ensureGraphReplacementEnrichmentRuntime();
        owner = self.acquireReplicationMutationShared();
        try self.enforceReplicationWriteGate();
    }
    var build_snapshot_lease = try self.acquireSnapshotReplayMutation();
    var snapshot_held = true;
    defer if (snapshot_held) build_snapshot_lease.release();
    var job = saved;
    const cfg = job.config orelse return error.InvalidIndexConfig;
    if (!job.producer_replay_complete) {
        // Reuse the managed-admission source replay owner: one bounded
        // primary page per quantum, with a durable cursor after its append.
        // A crash can repeat a page but cannot skip source documents.
        // The append owns its own role/snapshot admission. Release ours
        // first so a waiting snapshot cannot deadlock a nested reader.
        build_snapshot_lease.release();
        snapshot_held = false;
        releaseReplicationMutationShared(&owner);
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const producer_resume = if (job.producer_replay_resume_b64) |encoded| blk: {
            const bytes = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
            try std.base64.standard.Decoder.decode(bytes, encoded);
            break :blk bytes;
        } else null;
        const producers = try self.graphReplacementProducerNamesAlloc(a, job);
        var page = if (producers.len == 0)
            GeneratedEnrichmentReplayPageResult{ .complete = true }
        else blk: {
            try self.ensureGraphReplacementEnrichmentRuntime();
            break :blk try self.appendGeneratedEnrichmentsFromStoredDocsPage(a, producers, true, producer_resume, 32, .{
                .deadline_ns = @min(deadline orelse std.math.maxInt(u64), self.graphReplacementWorkNow() +| 100 * std.time.ns_per_ms),
            });
        };
        defer page.deinit(a);
        job.producer_replay_complete = page.complete;
        job.producer_replay_target = @max(job.producer_replay_target, page.target_sequence);
        job.producer_replay_resume_b64 = if (page.next_resume_key) |key| blk: {
            const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(key.len));
            break :blk std.base64.standard.Encoder.encode(encoded, key);
        } else null;
        owner = self.acquireReplicationMutationShared();
        try self.enforceReplicationWriteGate();
        build_snapshot_lease = try self.acquireSnapshotReplayMutation();
        snapshot_held = true;
        try self.core.index_manager.saveGraphReplacementBuild(self.core.store, job);
        return true;
    }
    if (job.producer_replay_target > 0) {
        const runtime = self.enrichment_runtime orelse return error.EnrichmentRuntimeUnavailable;
        runtime.notifySequence(job.producer_replay_target);
        if (runtime.stats().applied_sequence < job.producer_replay_target) return true;
    }
    const catalog_generation = self.core.index_manager.write_plan_generation.load(.acquire);
    if (self.graph_replacement_manager) |manager| if (manager.get(job.name) == null or self.graph_replacement_catalog_generation != catalog_generation) self.clearGraphReplacementManager();
    if (self.graph_replacement_manager == null) {
        const manager = try self.alloc.create(index_manager_mod.IndexManager);
        errdefer self.alloc.destroy(manager);
        manager.* = try index_manager_mod.IndexManager.initWithOptions(self.core.index_manager.alloc, self.core.index_manager.base_path, self.index_backends);
        errdefer manager.deinit();
        // Repeated bounded pages revisit the same checkpoint runs. Use
        // the backend's existing bounded cache rather than publishing a
        // fresh native immutable-source lease on every block read.
        manager.graph_reverse_lsm_options.local_block_cache_enabled = true;
        // This unpublished manager has no independent compactor. Let
        // ordinary bounded write-pressure maintenance prevent checkpoint
        // runs accumulating without limit during a long build.
        manager.graph_reverse_lsm_options.foreground_soft_compaction = true;
        manager.source_payload_store = self.core.index_manager.source_payload_store;
        manager.setIo(self.backend_runtime.io());
        try manager.registerReplacementIndex(self.core.store, cfg);
        self.graph_replacement_manager = manager;
        self.graph_replacement_catalog_generation = catalog_generation;
    }
    const private = self.graph_replacement_manager.?;
    if (self.graph_replacement_progress) |progress| {
        job.resume_key_b64 = progress.job.resume_key_b64;
        job.scanned = progress.job.scanned;
        job.applied_sequence = progress.job.applied_sequence;
        job.tail_source_cursor = progress.job.tail_source_cursor;
        job.phase = progress.job.phase;
    }
    var ctx = self.graphReplacementContext(private);
    defer ctx.deinit(self.alloc);
    var arena = std.heap.ArenaAllocator.init(self.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    if (job.phase == .building) {
        if (builtin.is_test) if (DB.test_graph_replacement_before_build) |hook| try hook(self);
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const source_lower = try documentRangeLowerAlloc(a, "");
        const source_upper = (try documentRangeUpperAlloc(a, "")).?;
        const build_resume = if (job.resume_key_b64) |encoded| blk: {
            const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
            const bytes = try a.alloc(u8, size);
            try std.base64.standard.Decoder.decode(bytes, encoded);
            break :blk bytes;
        } else source_lower;
        var row = try cursor.seekAtOrAfter(if (std.mem.lessThan(u8, build_resume, source_lower)) source_lower else build_resume);
        if (job.resume_key_b64 != null and row != null and std.mem.eql(u8, row.?.key, build_resume)) row = try cursor.next();
        var documents: std.ArrayList([]const u8) = .empty;
        var artifacts: std.ArrayList([]const u8) = .empty;
        var count: usize = 0;
        var visited: usize = 0;
        var source_bytes: usize = 0;
        // Detached pages amortize reopening and durably checkpointing the
        // private native graph. Keep the 1 MiB/100 ms scan caps for large
        // rows; foreground waits retain their smaller document quantum.
        const document_limit: usize = if (bounded_replay) 256 else 32;
        const build_deadline = @min(deadline orelse std.math.maxInt(u64), monotonicTimeNs() +| 100 * std.time.ns_per_ms);
        while (row) |kv| : (row = try cursor.next()) {
            if (!std.mem.lessThan(u8, kv.key, source_upper)) {
                row = null;
                break;
            }
            if (count == document_limit or visited == 4096) break;
            if (visited != 0 and source_bytes +| kv.value.len > 1024 * 1024) break;
            // Setup can consume a short caller quantum. Admit one
            // bounded scan unit so repeated waits cannot starve the job.
            if (visited != 0 and monotonicTimeNs() >= build_deadline) break;
            visited += 1;
            source_bytes +|= kv.value.len;
            if (internal_keys.isStoredDocumentRowKey(kv.key)) {
                const key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(a, kv.key)).?;
                count += 1;
                try documents.append(a, key);
            } else if (internal_keys.isAssetArtifactKey(kv.key) or internal_keys.isChunkArtifactRecordKey(kv.key) or internal_keys.isResolutionArtifactKey(kv.key)) {
                const owner_key = (try internal_keys.decodeDocumentComponentAlloc(a, kv.key)).?;
                const primary = if (relationalColumns(self) != null)
                    try internal_keys.relationalRowKeyAlloc(a, owner_key)
                else
                    try internal_keys.documentKeyAlloc(a, owner_key);
                const primary_value = try missingAsNull(read.get(primary));
                if (primary_value == null) {
                    count += 1;
                    try artifacts.append(a, try a.dupe(u8, kv.key));
                }
            }
            const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(kv.key.len));
            job.resume_key_b64 = std.base64.standard.Encoder.encode(encoded, kv.key);
            job.scanned += 1;
        }
        if (documents.items.len > 0) try rebuildGraphReplacementDocumentsWithRead(&ctx, cfg.name, documents.items, &read);
        if (artifacts.items.len > 0) {
            const changed = try materializeGraphSourceArtifactsForIndex(a, ctx.store, private, artifacts.items, cfg.name, .{ .require_resolution_contract = true });
            try applyDerivedBatchToIndexContextProfiled(&ctx, .{ .changed_artifact_keys = changed }, .{ .name = cfg.name, .kind = .graph }, null, false, null);
        }
        if (row == null) job.phase = .catching_up;
    }
    const target = self.core.nextDerivedSequence();
    const before_catchup = job.applied_sequence;
    // Construction setup may exhaust a short caller deadline. Background
    // replay gets its own finite quantum after the base page, so a moving
    // journal tip cannot monopolize construction or discard its progress.
    const catchup_deadline = if (bounded_replay) deadline orelse (self.graphReplacementWorkNow() +| 100 * std.time.ns_per_ms) else deadline;
    job.applied_sequence = try self.catchUpGraphReplacement(private, cfg.name, job.applied_sequence, target, 1024, catchup_deadline);
    if (job.applied_sequence > before_catchup) job.tail_source_cursor = 0;
    // Yielding is not a durability boundary. Keep volatile progress with
    // the resident shadow, retaining the journal at the last durable cut.
    // Checkpoint the first quantum, at most once per second thereafter,
    // and always before activation. A crash repeats only unacknowledged
    // work, never an entire completed build.
    const now = monotonicTimeNs();
    const previous_checkpoint = if (self.graph_replacement_progress) |progress| progress.checkpoint_ns else 0;
    const checkpoint = previous_checkpoint == 0 or now -| previous_checkpoint >= std.time.ns_per_s or job.phase == .catching_up;
    var progress_job = job;
    progress_job.name = "";
    progress_job.old = "";
    progress_job.config = null;
    progress_job.last_error = null;
    progress_job.resume_key_b64 = if (job.resume_key_b64) |key| try self.alloc.dupe(u8, key) else null;
    var progress_transferred = false;
    errdefer if (!progress_transferred) {
        if (progress_job.resume_key_b64) |key| self.alloc.free(key);
    };
    if (checkpoint) {
        if (builtin.is_test) if (DB.test_graph_replacement_before_checkpoint) |hook| hook(self);
        try private.checkpointLsmWalForManagedIndex(.{ .name = cfg.name, .kind = .graph });
        try private.syncIndexByName(cfg.name, true);
        if (job.scanned > saved.scanned or job.applied_sequence > saved.applied_sequence or job.tail_source_cursor > saved.tail_source_cursor or job.phase != saved.phase) {
            job.retry_count = 0;
            job.retry_after_ns = 0;
            job.last_error = null;
            progress_job.retry_count = 0;
            progress_job.retry_after_ns = 0;
        }
        try self.core.index_manager.saveGraphReplacementBuild(self.core.store, job);
    }
    clearGraphReplacementProgress(self);
    self.graph_replacement_progress = .{ .job = progress_job, .checkpoint_ns = if (checkpoint) now else previous_checkpoint };
    progress_transferred = true;
    if (job.phase != .catching_up or bounded_replay) return true;
    // Activation acquires its own structural/role admission. Release the
    // construction leases first so a waiting snapshot/role fence cannot
    // deadlock a nested acquisition by this owner.
    build_snapshot_lease.release();
    snapshot_held = false;
    releaseReplicationMutationShared(&owner);
    if (builtin.is_test) if (DB.test_graph_replacement_before_activation) |hook| try hook(self);
    var prepared = PreparedGraphReplacement{ .manager = private, .sequence = job.applied_sequence, .deadline_ns = deadline, .tail_source_cursor = job.tail_source_cursor, .activation_attempts = job.activation_attempts };
    defer if (prepared.entry) |*entry| private.destroyDetachedReplacementIndex(entry);
    _ = self.addIndexForReconciliation(cfg, .ordinary, null, job.old, &prepared) catch |err| switch (err) {
        error.Busy, error.IntegrityTopologyBusy => {
            // The bounded fence may have advanced the durable intra-entry
            // cursor; refresh from that cut on the next maintenance turn.
            clearGraphReplacementProgress(self);
            return true;
        },
        else => return err,
    };
    return self.hasPendingGraphReplacements();
}

pub fn graphReplacementGenerationCommitted(self: *DB, name: []const u8, generation: u64) !bool {
    const actual = self.indexGeneration(name) orelse return false;
    if (actual != generation) return error.Cancelled;
    return true;
}

pub fn driveManualGraphReplacementReplay(self: *DB, producer_target: u64, deadline: u64) !void {
    // A wait observes accepted work even when this owner cannot choose
    // local effects. Leave denied/ordered work queued, as the replacement
    // scheduler does, rather than turning the observation into a write.
    if (monotonicTimeNs() >= deadline or !try canRunLocalGraphEndpointCleanup(self)) return;
    try self.enforceReplicationWriteGate();
    if (self.enrichment_runtime) |runtime| {
        if (!self.local_execution.optional_runtime_workers_enabled and !runtime.isStarted() and producer_target > runtime.stats().applied_sequence) {
            // The enrichment guard uses its own real-time clock; the API
            // wait deadline is monotonic. Convert the remaining duration.
            const remaining = deadline -| monotonicTimeNs();
            const provider_deadline = runtime.deadline_clock.nowRealtimeNs() +| remaining;
            runtime.catchUpUntilWithVisibilityDeadline(producer_target, .none, provider_deadline) catch |err| switch (err) {
                error.EnrichmentWaitTimeout, error.EnrichmentRetryInProgress => return,
                else => return err,
            };
        }
    }
    if (monotonicTimeNs() >= deadline) return;
    if (self.hasConfiguredResolvers()) {
        if (self.resolution_runtime == null) try self.initResolutionRuntime();
        const resolution = self.resolution_runtime.?;
        resolution.notifySequence(self.core.nextDerivedSequence());
        if ((!self.local_execution.optional_runtime_workers_enabled or !self.resolver_workers_enabled) and !resolution.worker_started.load(.acquire)) try resolution.catchUpOnce(deadline);
    }
}

/// Wait for the generation returned by registration, including across
/// caller-managed wait quanta. Reusing its name never completes this wait.
pub fn waitGraphReplacement(self: *DB, name: []const u8, generation: u64, timeout_ms: u64) !void {
    if (generation == 0) return error.Cancelled;
    const deadline = monotonicTimeNs() +| timeout_ms *| std.time.ns_per_ms;
    while (true) {
        // Publication appends the new runtime entry before committing the
        // catalog. Its exclusive catalog lease spans commit and retirement;
        // a shared observation cannot acknowledge that provisional entry.
        if (try graphReplacementGenerationCommitted(self, name, generation)) return;
        if (builtin.is_test) if (DB.test_graph_replacement_wait_observation) |hook| try hook(self);
        const jobs = try self.graphReplacementStatusAlloc(self.alloc);
        defer jobs.deinit();
        const pending = for (jobs.value) |job| {
            if (std.mem.eql(u8, job.name, name)) break job;
        } else {
            // The swap may have removed the marker after the first
            // catalog observation. Missing work alone is not cancellation.
            if (try graphReplacementGenerationCommitted(self, name, generation)) return;
            return error.Cancelled;
        };
        if (pending.generation != generation) return error.Cancelled;
        if (pending.phase == .failed) return error.IndexReplacementFailed;
        if (monotonicTimeNs() >= deadline) {
            if (try graphReplacementGenerationCommitted(self, name, generation)) return;
            return error.Busy;
        }
        // A named wait advances its own reservation, avoiding unrelated
        // corpus builds. Recheck its generation under scheduler ownership
        // so cancellation/name reuse cannot redirect this quantum.
        try driveManualGraphReplacementReplay(self, pending.producer_replay_target, deadline);
        _ = try self.advanceGraphReplacementForIdentityUntil(deadline, .{ .name = name, .generation = generation }, .manual);
        // The control IO also exists for manual/inline runtimes. Never
        // spin when there is no independent background IO runtime.
        try self.core.index_manager.checkpointIo().sleep(.fromMilliseconds(1), .awake);
    }
}

pub fn collectGraphStateForDeleteLocked(
    self: *DB,
    doc_key: []const u8,
    index_name: []const u8,
    state_name: []const u8,
    expected_generation: u64,
    deletes: *std.ArrayListUnmanaged([]const u8),
    changed: *std.ArrayListUnmanaged([]u8),
) !void {
    const state_key = try graphAssetStateKeyAlloc(self.alloc, doc_key, index_name, state_name);
    var state_key_owned = true;
    errdefer if (state_key_owned) self.alloc.free(state_key);

    if (try loadGraphAssetStateKeysAlloc(self.alloc, self.core.store, state_key, expected_generation)) |previous_keys| {
        defer freeOwnedConstKeySlice(self.alloc, previous_keys);
        for (previous_keys) |previous_key| {
            if (containsDeleteKey(deletes.items, previous_key)) continue;
            const owned_key = try self.alloc.dupe(u8, previous_key);
            try deletes.append(self.alloc, owned_key);
            try appendUniqueOwnedKey(self.alloc, changed, previous_key);
        }
    }
    try appendGraphAssetStateSegmentDeleteKeys(self.alloc, self.core.store, state_key, deletes);

    const state_exists = if (self.core.store.get(self.alloc, state_key)) |raw| exists: {
        self.alloc.free(raw);
        break :exists true;
    } else |err| switch (err) {
        error.NotFound => false,
        else => return err,
    };
    if (state_exists and !containsDeleteKey(deletes.items, state_key)) {
        try deletes.append(self.alloc, state_key);
        state_key_owned = false;
    } else {
        self.alloc.free(state_key);
        state_key_owned = false;
    }
}

pub fn syncTargetsIncludeGraph(self: *DB, index_names: []const []const u8) bool {
    for (index_names) |index_name| {
        if (self.core.graphIndex(index_name) != null) return true;
    }
    return false;
}

pub fn runGraphMetricMaintenanceForIdle(self: *DB) !usize {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    while (true) {
        lockApply(self);
        if (!self.core.index_manager.graphRetirementAdmissionOpen()) {
            self.core.unlockApply();
            return error.IntegrityTopologyBusy;
        }
        const more = self.core.index_manager.runGraphOwnershipCleanupStep() catch |err| {
            self.core.unlockApply();
            return err;
        };
        self.core.unlockApply();
        if (!more) break;
    }
    // Planned maintenance uses the same catalog pins and transaction
    // fences as background workers. Never hold the ingest lock while
    // draining graph computation; graph writes may supersede a build.
    switch (self.graph_metric_idle_maintenance) {
        .auto => return runGraphMetricPlannedAutoMaintenanceForIdle(self),
        .planned => return drainGraphMetricPlannedIdle(self),
        else => {},
    }
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    return switch (self.graph_metric_idle_maintenance) {
        .legacy => try self.core.index_manager.runGraphMetricMaintenance(),
        .planned, .auto => unreachable,
        .degree_canary => try runGraphMetricDegreeCanaryMaintenanceForIdleLocked(self),
    };
}

pub fn graphMetricPlannedProgress(result: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult) usize {
    return result.planning_steps + result.builds_started + result.pages_completed + result.phases_advanced + result.published;
}

pub fn drainGraphMetricPlannedIdle(self: *DB) !usize {
    const result = try self.core.index_manager.runGraphMetricPlannedMaintenance(self.graph_metric_idle_planned_options);
    if (result.budget_exhausted) return error.RunUntilIdleDidNotConverge;
    return graphMetricPlannedProgress(result);
}

pub fn runGraphMetricPlannedAutoMaintenanceForIdle(self: *DB) !usize {
    const result = try self.core.index_manager.runGraphMetricPlannedAutoMaintenance(
        self.graph_metric_idle_planned_options,
        self.graph_metric_idle_auto_options,
    );
    if (result.budget_exhausted) return error.RunUntilIdleDidNotConverge;
    const progressed = graphMetricPlannedProgress(result);
    const after = try self.core.index_manager.graphMetricPlannedAutoIdleDecision(self.graph_metric_idle_auto_options);
    if (!after.shouldRunPlanned() and after.ineligible_queued != 0) {
        // Admission caps must not silently select unlimited local compute.
        return error.RunUntilIdleDidNotConverge;
    }
    return progressed;
}

pub fn runGraphMetricDegreeCanaryMaintenanceForIdleLocked(self: *DB) !usize {
    const decision = try self.core.index_manager.graphMetricDegreeCanaryDecision(self.graph_metric_idle_degree_canary_options);
    if (decision.shouldRunPlanned()) return try drainGraphMetricPlannedIdle(self);
    if (decision.active_degree_builds != 0 or decision.blocked_active_non_degree != 0) {
        return error.RunUntilIdleDidNotConverge;
    }
    return try self.core.index_manager.runGraphMetricMaintenance();
}

pub fn runGraphMetricPlannedMaintenanceForIdle(
    self: *DB,
    options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    return try self.core.index_manager.runGraphMetricPlannedMaintenance(options);
}

pub fn runGraphMetricServiceMaintenanceJsonAlloc(self: *DB, alloc: Allocator, body: []const u8) ![]u8 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, body });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var parsed = std.json.parseFromSlice(GraphMetricServiceMaintenanceRequest, alloc, if (body.len == 0) "{}" else body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch return error.InvalidGraphMetricRuntimeConfig;
    defer parsed.deinit();

    var manual_clock = platform_clock.ManualClock{};
    if (parsed.value.now_ms) |now_ms| manual_clock.setRealtimeNs(now_ms *| std.time.ns_per_ms);
    const resources = self.core.asyncResources();
    var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        self.backend_runtime,
        .{
            .start_background_loop = false,
            .role = parsed.value.role,
            .runtime_id = parsed.value.runtime_id,
            .lease_owned = parsed.value.lease_owned,
            .owner_id = parsed.value.owner_id,
            .lease_ttl_ms = parsed.value.lease_ttl_ms,
            .coordinator_start_background_builds = parsed.value.start_background_builds,
            .planned_options = .{
                .worker_id = parsed.value.worker_id orelse "",
                .worker_ids = parsed.value.worker_ids orelse &.{},
                .max_rounds = parsed.value.max_rounds,
                .max_metrics_per_round = parsed.value.max_metrics_per_round,
                .max_pages_per_round = parsed.value.max_pages_per_round,
            },
            .clock = if (parsed.value.now_ms != null) manual_clock.clock() else platform_clock.Clock.real(),
        },
    );
    var preserve_lease = false;
    defer if (preserve_lease) runtime.deinitPreserveLease() else runtime.deinit();

    if (parsed.value.action == .release) {
        const released = try runtime.ownership.releaseHeldLease();
        var current_lease = try runtime.ownership.loadLease(alloc);
        defer if (current_lease) |*lease| lease_mod.deinitRecord(alloc, lease);
        var runtime_stats = runtime.stats();
        runtime_stats.shutdown = true;
        return try std.json.Stringify.valueAlloc(alloc, .{
            .released = released,
            .lease_owner_id_hash = if (current_lease) |lease| graph_metric_runtime_mod.identityHash(lease.owner_id) else 0,
            .lease_expires_at_ms = if (current_lease) |lease| lease.expires_at_ms else 0,
            .stats = runtime_stats,
        }, .{ .emit_null_optional_fields = false });
    }

    const result: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult = if (parsed.value.action == .tick) try runtime.runOnceDetailed() else .{};
    const runtime_stats = runtime.stats();
    preserve_lease = parsed.value.preserve_lease_after_tick and parsed.value.lease_owned and parsed.value.action == .tick and runtime_stats.has_lease;
    return try std.json.Stringify.valueAlloc(alloc, .{
        .result = result,
        .stats = runtime_stats,
    }, .{ .emit_null_optional_fields = false });
}

pub fn refreshGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    const entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
    try entry.index.enableGraphMetric(metric_name);
    var status = try entry.index.runGraphMetric(metric_name);
    defer status.deinit(entry.index.alloc);
    const cloned = try cloneGraphMetricStatusFromGraph(alloc, status);
    if (self.graph_metric_runtime) |runtime| runtime.notify();
    return cloned;
}

pub fn rebuildGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    const entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
    try entry.index.enableGraphMetric(metric_name);
    var status = try entry.index.runGraphMetric(metric_name);
    defer status.deinit(entry.index.alloc);
    const cloned = try cloneGraphMetricStatusFromGraph(alloc, status);
    if (self.graph_metric_runtime) |runtime| runtime.notify();
    return cloned;
}

/// Durably enqueue metric work and return immediately. Public control-plane
/// actions use this path so request latency is independent of graph size;
/// the bounded maintenance runtime performs and checkpoints the build.
pub fn scheduleGraphMetricBuild(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    metric_name: []const u8,
    force: bool,
) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name, force });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    const owned_status = blk: {
        lockApply(self);
        defer self.core.unlockApply();
        try self.requireGraphEndpointCleanupReady();
        if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
        const entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
        var current = try entry.index.graphMetricStatus(metric_name);
        defer current.deinit(entry.index.alloc);
        if (current.state == .disabled) {
            try entry.index.enableGraphMetric(metric_name);
            current.deinit(entry.index.alloc);
            current = try entry.index.graphMetricStatus(metric_name);
        }
        if (!force and current.state == .fresh) {
            break :blk try cloneGraphMetricStatusFromGraph(alloc, current);
        }
        const target_generation = @max(current.edge_generation, current.target_edge_generation);
        // Force means "build another immutable score epoch", not
        // "unpublish first". Repeated requests remain idempotent while a
        // build is active, and readers keep the verified prior epoch until
        // the new pointer is atomically published.
        var scheduled = entry.index.queueGraphMetricBuild(metric_name, target_generation) catch |err| switch (err) {
            // A newer edge snapshot may be queued while the prior bounded
            // build is still active. Treat repeated control-plane actions
            // as accepted and expose the active/queued generations in the
            // returned status instead of turning a safe retry into a 500.
            error.GraphMetricBuildAlreadyRunning => break :blk try cloneGraphMetricStatusFromGraph(alloc, current),
            else => return err,
        };
        defer scheduled.deinit(self.core.index_manager.alloc);
        break :blk try cloneGraphMetricStatusFromGraph(alloc, scheduled);
    };
    if (self.graph_metric_runtime) |runtime| runtime.notify();
    return owned_status;
}

pub fn deleteGraphMetricMaterialization(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    const entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
    try entry.index.deleteGraphMetricMaterialization(metric_name);
    var status = try entry.index.graphMetricStatus(metric_name);
    defer status.deinit(entry.index.alloc);
    const cloned = try cloneGraphMetricStatusFromGraph(alloc, status);
    if (self.graph_metric_runtime) |runtime| runtime.notify();
    return cloned;
}

pub fn pauseGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    const entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
    var status = try entry.index.pauseGraphMetricMaintenance(metric_name);
    defer status.deinit(entry.index.alloc);
    return try cloneGraphMetricStatusFromGraph(alloc, status);
}

pub fn resumeGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    const entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
    var status = try entry.index.resumeGraphMetricMaintenance(metric_name);
    defer status.deinit(entry.index.alloc);
    return try cloneGraphMetricStatusFromGraph(alloc, status);
}

pub fn ensureGraphMetricPlannedBuild(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    metric_name: []const u8,
    target_generation: u64,
) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name, target_generation });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    var status = try self.core.index_manager.ensureGraphMetricPlannedBuild(index_name, metric_name, target_generation);
    defer status.deinit(self.core.index_manager.alloc);
    return try cloneGraphMetricStatusFromGraph(alloc, status);
}

pub fn runGraphMetricPlannedWorkerPageStep(self: *DB, index_name: []const u8, metric_name: []const u8, worker_id: []const u8) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    return try self.core.index_manager.runGraphMetricPlannedWorkerPageStep(index_name, metric_name, worker_id);
}

pub fn runGraphMetricPlannedWorkerPageStepAt(self: *DB, index_name: []const u8, metric_name: []const u8, worker_id: []const u8, now_ms: u64) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, index_name, metric_name, worker_id, now_ms });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    return try self.core.index_manager.runGraphMetricPlannedWorkerPageStepAt(index_name, metric_name, worker_id, now_ms);
}

pub fn runGraphMetricPlannedCoordinatorStep(self: *DB, index_name: []const u8, metric_name: []const u8) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    return try self.core.index_manager.runGraphMetricPlannedCoordinatorStep(index_name, metric_name);
}

pub fn runGraphMetricPlannedCoordinatorStepAt(self: *DB, index_name: []const u8, metric_name: []const u8, now_ms: u64) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, index_name, metric_name, now_ms });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    return try self.core.index_manager.runGraphMetricPlannedCoordinatorStepAt(index_name, metric_name, now_ms);
}

pub fn failGraphMetricPlannedBuild(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8, err: anyerror) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name, @errorName(err) });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    var status = try self.core.index_manager.failGraphMetricPlannedBuild(index_name, metric_name, err);
    defer status.deinit(self.core.index_manager.alloc);
    return try cloneGraphMetricStatusFromGraph(alloc, status);
}

pub fn runGraphMetricPlannedDrain(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8, target_generation: u64, options: graph_mod.GraphIndex.GraphMetricPlannedDrainOptions) !types.GraphMetricStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, metric_name, target_generation, options });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    lockApply(self);
    defer self.core.unlockApply();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    var status = try self.core.index_manager.runGraphMetricPlannedDrain(index_name, metric_name, target_generation, options);
    defer status.deinit(self.core.index_manager.alloc);
    return try cloneGraphMetricStatusFromGraph(alloc, status);
}

pub fn runGraphMetricPlannedCoordinatorSweep(self: *DB, options: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    // Planned graph work is generation-fenced and storage-transactional.
    // IndexManager pins catalog lifetime without blocking foreground apply.
    return try self.core.index_manager.runGraphMetricPlannedCoordinatorSweep(options);
}

pub fn runGraphMetricPlannedWorkerSweep(self: *DB, options: index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    return try self.core.index_manager.runGraphMetricPlannedWorkerSweep(options);
}

/// Called only by a Lite owner holding its live coordinator and path
/// publication fence. Producer replay runs in the separate detached turn.
pub fn finishBackgroundGraphReplacement(self: *DB, name: []const u8, generation: u64) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, name, generation });
    return self.advanceGraphReplacementForIdentityUntil(null, .{ .name = name, .generation = generation }, .cutover);
}

pub fn rebuildGraphIndexesForTargetCoverage(self: *DB, alloc: Allocator) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc });
    var snapshot_replay = try self.acquireSnapshotReplayMutation();
    defer snapshot_replay.release();
    try applySplitGraphArtifactsInRange(
        alloc,
        "",
        "",
        self.core.store,
        self.core.index_manager,
    );
}

pub fn populateGraphMetricStatusStats(alloc: Allocator, item: *types.DBIndexStats, graph_index: *graph_mod.GraphIndex) !void {
    if (graph_index.metric_configs.len == 0) return;
    const statuses = try alloc.alloc(types.GraphMetricStatus, graph_index.metric_configs.len);
    var initialized: usize = 0;
    errdefer {
        for (statuses[0..initialized]) |*status| status.deinit(alloc);
        alloc.free(statuses);
    }
    for (graph_index.metric_configs, 0..) |cfg, i| {
        var status = try graph_index.graphMetricStatus(cfg.name);
        defer status.deinit(graph_index.alloc);
        statuses[i] = try cloneGraphMetricStatusFromGraph(alloc, status);
        initialized += 1;
    }
    item.graph_metric_status = statuses;
}

pub fn graphMetricRuntimeStats(self: *DB) types.GraphMetricRuntimeStats {
    const runtime = self.graph_metric_runtime orelse return .{};
    const runtime_snapshot = runtime.stats();
    const total = runtime_snapshot.total_result;
    const last = runtime_snapshot.last_result;
    return .{
        .enabled = runtime_snapshot.enabled,
        .role = switch (runtime_snapshot.role) {
            .combined => .combined,
            .coordinator => .coordinator,
            .worker => .worker,
            .worker_pool => .worker_pool,
        },
        .runtime_id_hash = runtime_snapshot.runtime_id_hash,
        .owner_id_hash = runtime_snapshot.owner_id_hash,
        .lease_key_hash = runtime_snapshot.lease_key_hash,
        .worker_id_hash = runtime_snapshot.worker_id_hash,
        .worker_count = @intCast(runtime_snapshot.worker_count),
        .lease_owned = runtime_snapshot.lease_owned,
        .has_lease = runtime_snapshot.has_lease,
        .acquisition_count = runtime_snapshot.acquisition_count,
        .takeover_count = runtime_snapshot.takeover_count,
        .lease_acquire_failures = runtime_snapshot.lease_acquire_failures,
        .lost_leases = runtime_snapshot.lost_leases,
        .last_acquired_ms = runtime_snapshot.last_acquired_ms,
        .lease_expires_at_ms = runtime_snapshot.lease_expires_at_ms,
        .lease_renew_after_ms = runtime_snapshot.lease_renew_after_ms,
        .renewal_count = runtime_snapshot.renewal_count,
        .started = runtime_snapshot.started,
        .shutdown = runtime_snapshot.shutdown,
        .notified = runtime_snapshot.notified,
        .ticks_started = runtime_snapshot.ticks_started,
        .ticks_completed = runtime_snapshot.ticks_completed,
        .durable_progress_ticks = runtime_snapshot.durable_progress_ticks,
        .idle_ticks = runtime_snapshot.idle_ticks,
        .error_ticks = runtime_snapshot.error_ticks,
        .last_error_name = if (runtime_snapshot.last_error_name) |name| types.RuntimeErrorName.init(name) else null,
        .total_metrics_scanned = @intCast(total.metrics_scanned),
        .total_active_builds = @intCast(total.active_builds),
        .total_builds_started = @intCast(total.builds_started),
        .total_worker_steps = @intCast(total.worker_steps),
        .total_coordinator_steps = @intCast(total.coordinator_steps),
        .total_retired_input_records = @intCast(total.retired_input_records),
        .total_pages_claimed = @intCast(total.pages_claimed),
        .total_pages_completed = @intCast(total.pages_completed),
        .total_phases_advanced = @intCast(total.phases_advanced),
        .total_published = @intCast(total.published),
        .total_failed_builds = @intCast(total.failed_builds),
        .last_metrics_scanned = @intCast(last.metrics_scanned),
        .last_active_builds = @intCast(last.active_builds),
        .last_builds_started = @intCast(last.builds_started),
        .last_worker_steps = @intCast(last.worker_steps),
        .last_coordinator_steps = @intCast(last.coordinator_steps),
        .last_retired_input_records = @intCast(last.retired_input_records),
        .last_pages_claimed = @intCast(last.pages_claimed),
        .last_pages_completed = @intCast(last.pages_completed),
        .last_phases_advanced = @intCast(last.phases_advanced),
        .last_published = @intCast(last.published),
        .last_failed_builds = @intCast(last.failed_builds),
        .last_budget_exhausted = last.budget_exhausted,
    };
}

pub fn executeGraphMetricQueries(
    self: *DB,
    alloc: Allocator,
    queries: []const types.NamedGraphMetricQuery,
    cancellation: ?types.CancellationToken,
) ![]types.GraphMetricResult {
    if (queries.len == 0) return &.{};
    const results = try alloc.alloc(types.GraphMetricResult, queries.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(alloc);
        alloc.free(results);
    }
    for (queries, 0..) |named, i| {
        results[i] = try executeGraphMetricQuery(self, alloc, named, cancellation);
        initialized += 1;
    }
    return results;
}

pub fn applyGraphMetricRerank(self: *DB, result: *types.SearchResult, req: types.SearchRequest) !void {
    const rerank = req.graph_metric_rerank orelse return;
    if (req.count_only) return error.UnsupportedQueryRequest;
    if (rerank.damping != null and rerank.seed_nodes.len == 0) return error.InvalidQueryRequest;
    const entry = self.core.graphIndex(rerank.index_name) orelse return error.IndexNotFound;
    const node_ids = try result.alloc.alloc([]const u8, result.hits.len);
    defer result.alloc.free(node_ids);
    for (result.hits, 0..) |hit, i| node_ids[i] = hit.id;
    var score_snapshot = if (rerank.seed_nodes.len != 0) blk: {
        // Personalized blends are computed fresh from the current edge
        // snapshot; a published generation would silently return
        // unpersonalized scores, so seeds require fresh and skip the
        // publication requirements below.
        if (rerank.freshness != .fresh) return error.GraphMetricPersonalizationRequiresFresh;
        break :blk try entry.index.personalizedPageRankScoreSnapshotAllocWithControls(
            rerank.metric_name,
            rerank.seed_nodes,
            rerank.damping,
            node_ids,
            .{ .cancellation = req.cancellation orelse .none, .direction = @fromBackingInt(@intCast(@backingInt(rerank.direction))) },
        );
    } else try entry.index.graphMetricScoreSnapshotWithPolicyAlloc(rerank.metric_name, node_ids, .{
        .require_published = true,
        .require_fresh = rerank.freshness == .fresh,
    });
    defer score_snapshot.deinit(entry.index.alloc);
    if (rerank.seed_nodes.len == 0) {
        if (score_snapshot.status.published_generation == 0) return error.MetricNotReady;
        if (rerank.freshness == .fresh and score_snapshot.status.state != .fresh) return error.MetricStale;
    }

    var result_status = try cloneGraphMetricStatusFromGraph(result.alloc, score_snapshot.status);
    errdefer result_status.deinit(result.alloc);
    const selected = try graph_metric_rerank.selectPageAlloc(
        result.alloc,
        result.hits,
        score_snapshot.scores,
        .{
            .base_weight = rerank.base_weight,
            .metric_weight = rerank.weight,
            .missing_score = rerank.missing_score,
        },
        req.offset,
        req.limit,
    );
    defer result.alloc.free(selected);
    for (selected) |selection| {
        const hit = &result.hits[selection.original_index];
        var details = types.GraphMetricRerankScoreDetails{
            .index_name = try result.alloc.dupe(u8, rerank.index_name),
            .metric_name = undefined,
            .base_score = selection.base_score,
            .base_weight = rerank.base_weight,
            .metric_score = selection.metric_score,
            .metric_score_used = selection.metric_score_used,
            .metric_weight = rerank.weight,
            .missing_score_used = selection.metric_score == null,
            .final_score = selection.final_score,
            .published_generation = score_snapshot.status.published_generation,
        };
        errdefer result.alloc.free(details.index_name);
        details.metric_name = try result.alloc.dupe(u8, rerank.metric_name);
        if (hit.score_details) |*old| old.deinit(result.alloc);
        hit.score_details = details;
        hit.score = selection.final_score;
    }
    const old_hits = result.hits;
    const retained = try result.alloc.alloc(bool, old_hits.len);
    defer result.alloc.free(retained);
    const kept = try result.alloc.alloc(types.SearchHit, selected.len);
    @memset(retained, false);
    for (selected, 0..) |selection, i| {
        retained[selection.original_index] = true;
        kept[i] = old_hits[selection.original_index];
        old_hits[selection.original_index] = undefined;
    }
    for (old_hits, retained) |*hit, keep| if (!keep) hit.deinit(result.alloc);
    if (old_hits.len > 0) result.alloc.free(old_hits);
    result.hits = kept;
    if (result.graph_metric_rerank_status) |*old| old.deinit(result.alloc);
    result.graph_metric_rerank_status = result_status;
}

pub fn executeGraphMetricQuery(
    self: *DB,
    alloc: Allocator,
    named: types.NamedGraphMetricQuery,
    cancellation: ?types.CancellationToken,
) !types.GraphMetricResult {
    try self.requireGraphEndpointCleanupReady();
    const entry = self.core.graphIndex(named.query.index_name) orelse return error.IndexNotFound;
    if (named.query.damping != null and named.query.seed_nodes.len == 0) return error.InvalidQueryRequest;
    var metric_snapshot = if (named.query.seed_nodes.len != 0) blk: {
        // Personalized rankings are computed fresh from the current edge
        // snapshot; published generations are global-only, so seeded
        // reads against published freshness fail closed and the fresh
        // publication-state check below does not apply. The fresh
        // computation scans the filtered edge snapshot BEFORE kernel
        // admission, so it carries the request's cancellation and the
        // default scan caps (PersonalizedReadControls).
        if (named.query.freshness != .fresh) return error.GraphMetricPersonalizationRequiresFresh;
        break :blk try entry.index.personalizedPageRankTopKSnapshotAllocWithControls(
            named.query.metric_name,
            named.query.seed_nodes,
            named.query.damping,
            named.query.top_k,
            .{ .cancellation = cancellation orelse .none, .direction = @fromBackingInt(@intCast(@backingInt(named.query.direction))) },
        );
    } else try entry.index.graphMetricTopKSnapshotAlloc(
        named.query.metric_name,
        named.query.top_k,
    );
    defer metric_snapshot.deinit(entry.index.alloc);
    if (named.query.seed_nodes.len == 0 and named.query.freshness == .fresh and metric_snapshot.status.state != .fresh) return error.MetricStale;

    const raw_scores = metric_snapshot.scores;
    const scores = try alloc.alloc(types.GraphMetricScore, raw_scores.len);
    var initialized_scores: usize = 0;
    errdefer {
        for (scores[0..initialized_scores]) |*score| score.deinit(alloc);
        alloc.free(scores);
    }
    for (raw_scores, 0..) |score, i| {
        scores[i] = .{ .node = try alloc.dupe(u8, score.node), .score = score.score };
        initialized_scores += 1;
    }
    const name = try alloc.dupe(u8, named.name);
    errdefer alloc.free(name);
    const index_name = try alloc.dupe(u8, named.query.index_name);
    errdefer alloc.free(index_name);
    const metric_name = try alloc.dupe(u8, named.query.metric_name);
    errdefer alloc.free(metric_name);
    var owned_status = try cloneGraphMetricStatusFromGraph(alloc, metric_snapshot.status);
    errdefer owned_status.deinit(alloc);
    return .{
        .name = name,
        .index_name = index_name,
        .metric_name = metric_name,
        .scores = scores,
        .status = owned_status,
    };
}

pub fn attachGraphResultsCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    base: *types.SearchResult,
    named_sets: []const NamedResultSet,
) anyerror!void {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    base.graph_results = try executeGraphQueriesWithSets(self, alloc, req, req.graph_queries, named_sets);
    try self.applyGraphExpandStrategy(alloc, base, req.expand_strategy);
}

/// Owner-local graph admission and hydration after the caller has crossed
/// the Raft read gate. Candidate filtering and document loading are
/// batched, avoiding one probe transaction per graph node.
pub fn graphHydrateKeysForInternalRead(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    keys: []const []const u8,
) ![]types.SearchHit {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, keys });
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;

    const snapshot_req = try self.searchRequestAtCurrentIdentityGeneration(req);
    var admission = try filterGraphKeysWithOrdinalsAlloc(
        self,
        alloc,
        snapshot_req,
        keys,
    );
    defer admission.deinit(alloc);

    var hits = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }
    try hits.ensureTotalCapacity(alloc, keys.len);
    for (keys, admission.allowed, admission.ordinals, 0..) |key, admitted, maybe_ordinal, i| {
        if (!admitted) continue;
        const ordinal = maybe_ordinal orelse return error.InvalidGraphNodeAdmissionResult;
        var stored_data: ?[]u8 = null;
        if (admission.stored_data) |items| {
            stored_data = items[i] orelse return error.InvalidGraphNodeAdmissionResult;
            items[i] = null;
        }
        const id = try alloc.dupe(u8, key);
        errdefer alloc.free(id);
        hits.appendAssumeCapacity(.{
            .id = id,
            .doc_ordinal = ordinal,
            .stored_data = stored_data,
        });
    }
    return try hits.toOwnedSlice(alloc);
}

/// Owner-local graph root probe after the caller has crossed the Raft read
/// gate. The graph index performs the whole batch in one reverse snapshot.
pub fn graphHasIncomingEdgesForInternalRead(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    keys: []const []const u8,
    expected: index_manager_mod.IndexManager.CoverageIdentity,
    identity_read_generation: ?u64,
) ![]bool {
    const result = try self.graphHasIncomingEdgesForInternalReadBoundedAt(alloc, index_name, keys, expected, identity_read_generation, 0, @import("../../graph/work_budget.zig").default_max_explored_edges);
    alloc.free(result.has_physical_incoming);
    return result.has_incoming;
}

pub fn graphHasIncomingEdgesForInternalReadBoundedAt(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    keys: []const []const u8,
    expected: index_manager_mod.IndexManager.CoverageIdentity,
    identity_read_generation: ?u64,
    ttl_now_ns: u64,
    max_scanned_rows: usize,
) !graph_mod.GraphIndex.IncomingProbeResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, keys, expected, identity_read_generation, ttl_now_ns, max_scanned_rows });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    try self.requireGraphEndpointCleanupReady();
    if (!self.core.index_manager.graphRetirementAdmissionOpen()) return error.IntegrityTopologyBusy;
    // Validate under the same apply lease as the reverse snapshot. A
    // out-of-lease check can race index replacement and certify an
    // old index's negative answers under the new incarnation's cache key.
    if (expected.generation == 0 or expected.config_fingerprint == null)
        return error.InvalidArgument;
    const actual = self.core.index_manager.coverageIdentityForIndex(index_name) orelse
        return error.IndexGenerationMismatch;
    if (actual.generation != expected.generation or actual.config_fingerprint != expected.config_fingerprint)
        return error.IndexGenerationMismatch;
    // Reverse-only probes skip document hydration, but their routing
    // cache keys still bind the source shard's document/read generation.
    _ = try self.currentIdentityReadGenerationForRequest(identity_read_generation);
    const graph_entry = self.core.graphIndex(index_name) orelse return error.IndexNotFound;
    return try graph_entry.index.hasIncomingEdgesManyAllocBoundedAt(alloc, keys, if (ttl_now_ns == 0) graph_entry.index.clock.nowRealtimeNs() else ttl_now_ns, max_scanned_rows);
}

/// Local graph execution scope from the request: the API read source
/// sets `graph_owning_table` and `graph_index_complete_snapshot` when it
/// admits a single-group query for local (non-coordinated) execution.
pub fn graphExecutionScope(req: types.SearchRequest) graph_query_mod.ExecutionScope {
    return .{
        .ttl_now_ns = if (req.graph_ttl_now_ns == 0) null else req.graph_ttl_now_ns,
        .owning_table = req.graph_owning_table,
        .expand_cross_table_local = req.graph_index_complete_snapshot,
    };
}

pub fn searchGraph(self: *DB, alloc: Allocator, req: types.SearchRequest, graph_query: graph_query_mod.GraphQuery, base_hits: ?[]const types.SearchHit) !types.SearchResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, graph_query, base_hits });
    _ = req.index_name;
    const predicate_aware = graphRequestRequiresAdmission(req, graph_query.params.node_filter);
    var admission = GraphNodeAdmissionCache.init(self, alloc, req, graph_query.index_name, graph_query.params.node_filter);
    defer admission.deinit();
    var execution = GraphPredicateExecutionContext{
        .db = self,
        .admission = &admission,
    };
    var raw = try db_query_graph.executeSearchGraph(alloc, req, graph_query, base_hits, .{
        .ctx = self,
        .graph_ctx = if (predicate_aware) &execution else null,
        .predicate_aware = predicate_aware,
        .scope = graphExecutionScope(req),
        .execute_graph_query = if (predicate_aware)
            executeSearchGraphQueryWithAdmissionCallback
        else
            executeSearchGraphQueryCallback,
        .load_projected_document = loadProjectedSearchDocumentCallback,
        .load_projected_documents = loadProjectedSearchDocumentManyCallback,
        .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
        .filter_keys = filterGraphKeysCallback,
    });
    errdefer raw.deinit();
    try self.annotateSearchHitOrdinalsNoLock(alloc, req, raw.hits);
    return try filterExpiredSearchResult(self, alloc, raw);
}

pub fn executeGraphQueries(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    graph_queries: []const types.NamedGraphQuery,
    base_hits: []const types.SearchHit,
    base_total_hits: u32,
) ![]types.GraphSearchResult {
    return try db_query_graph.executeGraphQueries(alloc, req, graph_queries, base_hits, base_total_hits, .{
        .ctx = self,
        .func = executeSingleGraphQueryWithSetsCallback,
        .resolve_hits_to_doc_set = resolveSearchHitsToDocSetCallback,
        .resolve_nodes_to_doc_set = resolveGraphNodesToDocSetCallback,
    });
}

pub fn executeGraphQueriesWithSets(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    graph_queries: []const types.NamedGraphQuery,
    named_sets: []const NamedResultSet,
) ![]types.GraphSearchResult {
    return try db_query_graph.executeGraphQueriesWithSets(alloc, req, graph_queries, named_sets, .{
        .ctx = self,
        .func = executeSingleGraphQueryWithSetsCallback,
        .resolve_hits_to_doc_set = resolveSearchHitsToDocSetCallback,
        .resolve_nodes_to_doc_set = resolveGraphNodesToDocSetCallback,
    });
}

pub fn executeSingleGraphQueryWithSets(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    named: *const types.NamedGraphQuery,
    named_sets: []const NamedResultSet,
    budgets: db_query_graph.RequestGraphBudgets,
) !types.GraphSearchResult {
    try self.requireGraphEndpointCleanupReady();
    const predicate_aware = graphRequestRequiresAdmission(req, named.query.params.node_filter);
    var admission = GraphNodeAdmissionCache.init(self, alloc, req, named.query.index_name, named.query.params.node_filter);
    defer admission.deinit();
    var execution = GraphPredicateExecutionContext{
        .db = self,
        .admission = &admission,
    };
    var result = switch (named.query.query_type) {
        .pattern => try self.executeSinglePatternQueryWithSetsWithContext(
            alloc,
            req,
            named,
            named_sets,
            &execution,
            predicate_aware,
            budgets,
        ),
        else => try db_query_graph.executeSingleNonPatternQueryWithSetsWithBudgets(alloc, req, named, named_sets, .{
            .ctx = self,
            .graph_ctx = if (predicate_aware) &execution else null,
            .predicate_aware = predicate_aware,
            .scope = graphExecutionScope(req),
            .find_shortest_path = if (predicate_aware)
                executeShortestPathWithAdmissionCallback
            else
                executeShortestPathCallback,
            .find_k_shortest_paths = if (predicate_aware)
                executeKShortestPathsWithAdmissionCallback
            else
                executeKShortestPathsCallback,
            .execute_graph_query = if (predicate_aware)
                executeGraphQueryWithAdmissionCallback
            else
                executeGraphQueryCallback,
            .load_projected_document = loadProjectedSearchDocumentCallback,
            .load_projected_documents = loadProjectedSearchDocumentManyCallback,
            .resolve_doc_set_doc_ids = resolveDocSetDocIdsForGraphCallback,
            .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
            .filter_keys = filterGraphKeysCallback,
        }, budgets),
    };
    errdefer result.deinit(alloc);
    try self.annotateSearchHitOrdinalsNoLock(alloc, req, result.hits);
    return result;
}

pub fn resolveGraphNodesToDocSetCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    nodes: []const graph_query_mod.GraphResultNode,
) anyerror!doc_set.ResolvedDocSet {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    var doc_ids = try alloc.alloc([]const u8, nodes.len);
    defer alloc.free(doc_ids);
    for (nodes, 0..) |node, i| {
        if (node.table != null) return error.UnsupportedQueryRequest;
        doc_ids[i] = node.key;
    }
    return try self.resolveDocSetForIdsNoLockAtGenerationAlloc(alloc, doc_ids, req.identity_read_generation);
}

pub fn resolveDocSetDocIdsForGraphCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    set: *const doc_set.ResolvedDocSet,
    generation: ?u64,
) anyerror!?[][]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const ids = (try self.docIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, set, generation)) orelse return null;
    defer alloc.free(@constCast(ids));
    errdefer for (ids) |id| alloc.free(@constCast(id));

    const out = try alloc.alloc([]u8, ids.len);
    for (ids, 0..) |id, i| out[i] = @constCast(id);
    return out;
}

pub fn executeSingleGraphQueryWithSetsCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    named: *const types.NamedGraphQuery,
    named_sets: []const NamedResultSet,
    budgets: db_query_graph.RequestGraphBudgets,
) anyerror!types.GraphSearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try executeSingleGraphQueryWithSets(self, alloc, req, named, named_sets, budgets);
}

pub fn filterGraphKeysCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    keys: []const []const u8,
) anyerror![]bool {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.filterGraphKeysAlloc(alloc, req, keys);
}

pub fn filterGraphKeysAlloc(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    keys: []const []const u8,
) ![]bool {
    var filter_req = req;
    filter_req.include_stored = false;
    const admission = try filterGraphKeysWithOrdinalsAlloc(self, alloc, filter_req, keys);
    alloc.free(admission.ordinals);
    return admission.allowed;
}

pub fn filterGraphKeysWithOrdinalsAlloc(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    keys: []const []const u8,
) !GraphKeyAdmission {
    const ordinals = try self.lookupLiveDocOrdinalsNoLock(
        alloc,
        keys,
        req.identity_read_generation,
    );
    errdefer alloc.free(ordinals);

    var live_count: usize = 0;
    for (ordinals) |ordinal| {
        if (ordinal != null) live_count += 1;
    }

    const mask = try alloc.alloc(bool, keys.len);
    errdefer alloc.free(mask);
    @memset(mask, false);
    if (live_count == 0) return .{
        .allowed = mask,
        .ordinals = ordinals,
    };

    const loaded = if (req.include_stored)
        try loadStoredSearchDocumentsMany(self, alloc, keys, null)
    else
        null;
    defer if (loaded) |items| freeOptionalOwnedBytes(alloc, items);

    var hits = try alloc.alloc(types.SearchHit, live_count);
    var initialized: usize = 0;
    var hits_owned = true;
    errdefer if (hits_owned) {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    };
    for (keys, ordinals, 0..) |key, maybe_ordinal, i| {
        const ordinal = maybe_ordinal orelse continue;
        const id = try alloc.dupe(u8, key);
        errdefer alloc.free(id);
        const stored_data = if (loaded) |items| blk: {
            const value = items[i] orelse return error.InvalidGraphNodeAdmissionResult;
            items[i] = null;
            break :blk value;
        } else null;
        hits[initialized] = .{
            .id = id,
            .doc_ordinal = ordinal,
            .score = 1.0,
            .stored_data = stored_data,
        };
        initialized += 1;
    }

    var filtered = try db_query_result_shape.applyStoredSearchPatternFilters(alloc, req, .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits.len),
        .graph_results = &.{},
    }, .{
        .ctx = self,
        .load_stored = loadStoredSearchDocumentCallback,
        .load_many_stored = loadStoredSearchDocumentManyCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
    });
    hits_owned = false;
    defer filtered.deinit();

    const stored_data = if (req.include_stored) blk: {
        const values = try alloc.alloc(?[]u8, keys.len);
        @memset(values, null);
        break :blk values;
    } else null;
    errdefer if (stored_data) |items| freeOptionalOwnedBytes(alloc, items);

    var filtered_index: usize = 0;
    for (keys, ordinals, 0..) |key, maybe_ordinal, i| {
        if (maybe_ordinal == null) continue;
        const allowed = filtered_index < filtered.hits.len and
            std.mem.eql(u8, key, filtered.hits[filtered_index].id);
        mask[i] = allowed;
        if (!allowed) continue;
        if (stored_data) |items| {
            items[i] = filtered.hits[filtered_index].stored_data orelse
                return error.InvalidGraphNodeAdmissionResult;
            filtered.hits[filtered_index].stored_data = null;
        }
        filtered_index += 1;
    }
    if (filtered_index != filtered.hits.len) return error.InvalidQueryResult;
    return .{
        .allowed = mask,
        .ordinals = ordinals,
        .stored_data = stored_data,
    };
}

pub fn graphRequestRequiresAdmission(
    req: types.SearchRequest,
    node_filter: graph_pattern_mod.NodeFilter,
) bool {
    return db_query_graph.searchRequestHasGraphPredicates(req) or
        req.graph_table_read_authorizer != null or
        graph_query_mod.nodeFilterActive(node_filter);
}

pub fn executeGraphQueryCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    named: *const types.NamedGraphQuery,
    start_key_refs: []const []const u8,
    target_keys: [][]u8,
    work_budget: *graph_pattern_mod.WorkBudget,
    scope: graph_query_mod.ExecutionScope,
) anyerror!graph_query_mod.GraphQueryResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try executeGraphQueryWithTargets(self, alloc, named.query, start_key_refs, target_keys, work_budget, scope);
}

pub fn executeGraphQueryWithAdmissionCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    named: *const types.NamedGraphQuery,
    start_key_refs: []const []const u8,
    target_keys: [][]u8,
    work_budget: *graph_pattern_mod.WorkBudget,
    scope: graph_query_mod.ExecutionScope,
) anyerror!graph_query_mod.GraphQueryResult {
    const execution: *GraphPredicateExecutionContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try executeGraphQueryWithAdmission(
        execution.db,
        alloc,
        named.query,
        start_key_refs,
        target_keys,
        execution.admission.iface(),
        work_budget,
        scope,
    );
}

pub fn executeSearchGraphQueryCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    graph_query: graph_query_mod.GraphQuery,
    start_key_refs: []const []const u8,
    target_keys: [][]u8,
    scope: graph_query_mod.ExecutionScope,
) anyerror!graph_query_mod.GraphQueryResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try executeGraphQueryWithTargets(self, alloc, graph_query, start_key_refs, target_keys, null, scope);
}

pub fn executeSearchGraphQueryWithAdmissionCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    graph_query: graph_query_mod.GraphQuery,
    start_key_refs: []const []const u8,
    target_keys: [][]u8,
    scope: graph_query_mod.ExecutionScope,
) anyerror!graph_query_mod.GraphQueryResult {
    const execution: *GraphPredicateExecutionContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try executeGraphQueryWithAdmission(
        execution.db,
        alloc,
        graph_query,
        start_key_refs,
        target_keys,
        execution.admission.iface(),
        null,
        scope,
    );
}

pub fn searchGraphWithSets(self: *DB, alloc: Allocator, req: types.SearchRequest, graph_query: graph_query_mod.GraphQuery, named_sets: []const NamedResultSet) !types.SearchResult {
    _ = req.index_name;

    if (graph_query.query_type == .pattern) {
        const named_query: types.NamedGraphQuery = .{
            .name = "__graph",
            .query = graph_query,
        };
        const graph_results = try alloc.alloc(types.GraphSearchResult, 1);
        errdefer alloc.free(graph_results);
        var work_budget = graph_pattern_mod.WorkBudget.init(
            graph_pattern_mod.default_max_explored_nodes,
            graph_pattern_mod.default_max_explored_edges,
        );
        var distinct_budget = graph_pattern_mod.DistinctBudget.init(
            graph_pattern_mod.default_max_distinct_identities,
            graph_pattern_mod.default_max_distinct_state_bytes,
        );
        graph_results[0] = try self.executeSinglePatternQueryWithSets(
            alloc,
            req,
            &named_query,
            named_sets,
            .{ .work = &work_budget, .distinct = &distinct_budget },
        );
        return .{
            .alloc = alloc,
            .hits = &.{},
            .total_hits = graph_results[0].total_hits,
            .graph_results = graph_results,
        };
    }
    const predicate_aware = graphRequestRequiresAdmission(req, graph_query.params.node_filter);
    var admission = GraphNodeAdmissionCache.init(self, alloc, req, graph_query.index_name, graph_query.params.node_filter);
    defer admission.deinit();
    var execution = GraphPredicateExecutionContext{
        .db = self,
        .admission = &admission,
    };
    const raw = try db_query_graph.executeSearchGraphWithSets(alloc, req, graph_query, named_sets, .{
        .ctx = self,
        .graph_ctx = if (predicate_aware) &execution else null,
        .predicate_aware = predicate_aware,
        .scope = graphExecutionScope(req),
        .execute_graph_query = if (predicate_aware)
            executeSearchGraphQueryWithAdmissionCallback
        else
            executeSearchGraphQueryCallback,
        .load_projected_document = loadProjectedSearchDocumentCallback,
        .load_projected_documents = loadProjectedSearchDocumentManyCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsForGraphCallback,
        .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
        .filter_keys = filterGraphKeysCallback,
    });
    return try filterExpiredSearchResult(self, alloc, raw);
}

pub fn applyGraphExpandStrategy(self: *DB, alloc: Allocator, result: *types.SearchResult, strategy: ?graph_query_mod.ExpandStrategy) !void {
    _ = self;
    try db_query_graph.applyGraphExpandStrategy(alloc, result, strategy);
}

pub fn executeGraphQueryWithTargets(
    self: *DB,
    alloc: Allocator,
    graph_query: graph_query_mod.GraphQuery,
    start_key_refs: []const []const u8,
    target_keys: [][]u8,
    work_budget: ?*graph_pattern_mod.WorkBudget,
    scope: graph_query_mod.ExecutionScope,
) !graph_query_mod.GraphQueryResult {
    const entry = self.core.graphIndex(graph_query.index_name) orelse {
        try self.failIfIndexQuarantined(graph_query.index_name);
        return error.IndexNotFound;
    };
    if (entry.index.supportsAlgebraicSemiringTraversal()) {
        try self.proveGraphTraversalProgram(graph_query.index_name, target_keys.len > 0);
    }

    var resolved_query = graph_query;
    if (graph_query.target_nodes != null) {
        resolved_query.target_nodes = .{ .keys = try castOwnedKeysToConst(alloc, target_keys) };
    }
    defer if (graph_query.target_nodes != null) {
        switch (resolved_query.target_nodes.?) {
            .keys => |owned| alloc.free(owned),
            .identities => unreachable,
            .result_ref => unreachable,
        }
    };

    var graph_engine = graph_query_mod.GraphQueryEngine{
        .alloc = alloc,
        .work_budget = work_budget,
        .ttl_now_ns = scope.ttl_now_ns,
        .scope = scope,
    };
    return try graph_engine.execute(&entry.index, resolved_query, start_key_refs);
}

pub fn executeGraphQueryWithAdmission(
    self: *DB,
    alloc: Allocator,
    graph_query: graph_query_mod.GraphQuery,
    start_key_refs: []const []const u8,
    target_keys: [][]u8,
    admission: NodeAdmission,
    work_budget: ?*graph_pattern_mod.WorkBudget,
    scope: graph_query_mod.ExecutionScope,
) !graph_query_mod.GraphQueryResult {
    try self.requireGraphEndpointCleanupReady();
    const entry = self.core.graphIndex(graph_query.index_name) orelse {
        try self.failIfIndexQuarantined(graph_query.index_name);
        return error.IndexNotFound;
    };

    var resolved_query = graph_query;
    if (graph_query.target_nodes != null) {
        resolved_query.target_nodes = .{ .keys = try castOwnedKeysToConst(alloc, target_keys) };
    }
    defer if (graph_query.target_nodes != null) {
        switch (resolved_query.target_nodes.?) {
            .keys => |owned| alloc.free(owned),
            .identities => unreachable,
            .result_ref => unreachable,
        }
    };

    var graph_engine = graph_query_mod.GraphQueryEngine{
        .alloc = alloc,
        .node_admission = admission,
        .work_budget = work_budget,
        .ttl_now_ns = scope.ttl_now_ns,
        .scope = scope,
    };
    return try graph_engine.execute(&entry.index, resolved_query, start_key_refs);
}

pub fn proveGraphTraversalProgram(self: *DB, index_name: []const u8, constrained_targets: bool) !void {
    const graph_access_path = self.core.index_manager.graphTraversalAccessPath(index_name) orelse return error.InvalidIndexConfig;
    var tensor_program = (try algebraic_mod.planner.planGraphTraversalTensorProgramAlloc(self.alloc, index_name, constrained_targets)) orelse return error.InvalidIndexConfig;
    defer tensor_program.deinit(self.alloc);
    if (tensor_program.access_paths.len != 1 or
        tensor_program.access_paths[0].layout != .graph_edges or
        !std.mem.eql(u8, tensor_program.access_paths[0].owner, index_name) or
        !std.mem.eql(algebraic_mod.ir.Dimension, tensor_program.access_paths[0].output_dims, graph_access_path.output_dims))
    {
        return error.InvalidIndexConfig;
    }
    if (!algebraic_mod.ir.graphTraversalProgramMatchesTarget(tensor_program.asProgram(), index_name, constrained_targets)) {
        return error.InvalidIndexConfig;
    }
}

test "graph ttl creation at zero clock instant uses a durable timestamp" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("graph-ttl-zero-clock");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph", "links", "doc:b");
    defer alloc.free(key);
    const write: types.GraphEdgeWrite = .{ .index_name = "graph", .source = "doc:a", .target = "doc:b", .edge_type = "links" };
    const direct = try encodeGraphEdgeArtifactWithTtlAlloc(alloc, db.core.store, key, 1, std.time.ns_per_hour, 0, write);
    defer alloc.free(direct);
    var direct_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, direct);
    defer direct_edge.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), direct_edge.ttl_created_ns);
    const source = try encodeGraphSourceEdgeArtifactWithTtlAlloc(alloc, db.core.store, key, 1, std.time.ns_per_hour, 0, write);
    defer alloc.free(source);
    var source_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, source);
    defer source_edge.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), source_edge.ttl_created_ns);
}

test "graph edge metadata accepts only the public object shape" {
    const alloc = std.testing.allocator;
    try validateGraphEdgeMetadataJson(alloc, "");
    try validateGraphEdgeMetadataJson(alloc, "{}");
    try validateGraphEdgeMetadataJson(alloc, "{\"kind\":\"citation\"}");
    try std.testing.expectError(error.InvalidGraphEdges, validateGraphEdgeMetadataJson(alloc, "null"));
    try std.testing.expectError(error.InvalidGraphEdges, validateGraphEdgeMetadataJson(alloc, "[]"));
    try std.testing.expectError(error.InvalidGraphEdges, validateGraphEdgeMetadataJson(alloc, "\"legacy\""));
    try std.testing.expectError(error.InvalidGraphEdges, validateGraphEdgeMetadataJson(alloc, "{"));
}

test "db graph runtime count snapshots preserve pending state and read released snapshots" {
    var encoded: [DB.index_status_encoded_len]u8 = undefined;
    DB.encodeIndexStatusSnapshot(.{ .kind = .graph, .edge_count = 12, .graph_counts_pending = true }, &encoded);
    const current = try DB.decodeIndexStatusSnapshot(&encoded);
    try std.testing.expect(current.graph_counts_pending);
    try std.testing.expectEqual(@as(u64, 12), current.edge_count);
    std.mem.writeInt(u64, encoded[64..72], 2, .little);
    try std.testing.expectError(error.InvalidIndexStatusSnapshot, DB.decodeIndexStatusSnapshot(&encoded));
    var released: [64]u8 = encoded[0..64].*;
    std.mem.writeInt(u64, released[0..8], DB.index_status_magic_v1, .little);
    const old = try DB.decodeIndexStatusSnapshot(&released);
    try std.testing.expect(!old.graph_counts_pending);
    try std.testing.expectEqual(@as(u64, 12), old.edge_count);
}

test "graph rerank read pins apply across retirement seal admission" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("graph-retirement-read-pin");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();

    var request: types.SearchRequest = .{ .dense = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 } };
    try std.testing.expect(DB.publishedDenseRequestEligible(request));
    request.graph_metric_rerank = .{ .index_name = "links", .metric_name = "rank" };
    try std.testing.expect(!DB.publishedDenseRequestEligible(request));
    {
        const access = db.beginDenseSearchAccess(request);
        defer db.endDenseSearchAccess(access);
        try std.testing.expectEqual(DB.DenseSearchAccess.apply_shared, access);
        // The Raft seal takes apply-exclusive before closing the graph gate;
        // it cannot cross this in-flight graph read after its gate check.
        try std.testing.expect(!db.core.tryLockApplyExclusive());
    }
    try std.testing.expect(db.core.tryLockApplyExclusive());
    db.core.unlockApply();

    request.graph_metric_rerank = null;
    request.graph_metric_queries = &.{.{ .name = "metric", .query = .{ .index_name = "links", .metric_name = "rank" } }};
    try std.testing.expect(!DB.publishedDenseRequestEligible(request));
    request.graph_metric_queries = &.{};
    const anchor: u8 = 0;
    request.graph_query_transport = .{ .dialect = .canonical, .operations_json = "{}", .admitted_operations_ptr = &anchor, .admitted_operations_len = 0 };
    try std.testing.expect(!DB.publishedDenseRequestEligible(request));
}

test "db graph fact documents project arbitrary endpoints and retain parallel facts" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("fact-projection");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{
        .name = "facts",
        .kind = .graph,
        .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$","nodes":{"source":"{{ _item.source }}","target":"{{ _item.target }}"},"edge":{"edge_id":"{{ _doc.key }}","type":"RELATES_TO"}},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
        ,
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "fact:works", .value = "{\"relations\":[{\"source\":\"alice\",\"target\":\"acme\",\"weight\":0.5}]}" },
            .{ .key = "fact:founded", .value = "{\"relations\":[{\"source\":\"alice\",\"target\":\"acme\",\"weight\":0.9}]}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();
    const graph_entry = db.core.index_manager.graphIndex("facts") orelse return error.IndexNotFound;
    const graph = &graph_entry.index;
    {
        const edges = try graph.getEdges(alloc, "alice", "RELATES_TO", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 2), edges.len);
        for (edges) |edge| {
            try std.testing.expectEqualStrings("acme", edge.target);
            try std.testing.expectEqualStrings(edge.edge_id, edge.owner_document);
        }
    }
    try db.batch(.{ .deletes = &.{"fact:works"}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const remaining = try graph.getEdges(alloc, "acme", "RELATES_TO", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualStrings("fact:founded", remaining[0].edge_id);
    try db.batch(.{ .writes = &.{.{ .key = "alice", .value = "{\"name\":\"Alice\"}" }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    {
        const after_entity_update = try graph.getEdges(alloc, "alice", "RELATES_TO", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, after_entity_update);
        try std.testing.expectEqual(@as(usize, 1), after_entity_update.len);
    }
    try db.batch(.{ .writes = &.{.{ .key = "fact:founded", .value = "{\"relations\":[{\"source\":\"bob\",\"target\":\"acme\",\"weight\":0.9}]}" }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    {
        const old_source = try graph.getEdges(alloc, "alice", "RELATES_TO", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, old_source);
        try std.testing.expectEqual(@as(usize, 0), old_source.len);
        const new_source = try graph.getEdges(alloc, "bob", "RELATES_TO", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, new_source);
        try std.testing.expectEqual(@as(usize, 1), new_source.len);
        try std.testing.expectEqualStrings("fact:founded", new_source[0].edge_id);
    }
}

test "db graph index materializes relation asset artifacts into graph edge artifacts" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.relations_graph);

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

test "db graph index materializes unit-derived chunk artifacts into graph edge artifacts" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addEnrichment(test_enrichment.document_units_v1);
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 64,
    });
    try db.addIndex(.{
        .name = "chunk_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"document_chunks_v1","format":"extraction_relation",
        \\    "nodes":{"target":"{{ _artifact.value.text }}"},
        \\    "edge":{"type":"mentions","metadata":{"unit":"{{ _artifact.value._parent_unit_id }}","artifact":"{{ _artifact.name }}"}}}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,ZG9jOmI=\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const edges = try db.getEdges(alloc, "chunk_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"unit\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"artifact\":\"document_chunks_v1\"") != null);
}

test "db graph replay blocks resolution artifact without resolver contract" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{ .start_index_workers = false });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.prov_graph);

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

    try std.testing.expect(!batchAffectsManagedIndex(db.core.index_manager, batch, index_ref));
    try std.testing.expect(try batchAffectsManagedIndexForReplay(db.core.index_manager, batch, index_ref));
    try std.testing.expect(try batchAdvancesManagedIndexApplyStateForReplay(db.core.index_manager, batch, index_ref));
    try std.testing.expectError(
        error.MissingResolverArtifactContract,
        materializeGraphSourceArtifactsForIndex(
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
        materializeGraphSourceArtifactsForIndex(
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

    try std.testing.expect(batchAffectsManagedIndex(db.core.index_manager, batch, index_ref));
    try std.testing.expect(try batchAdvancesManagedIndexApplyStateForReplay(db.core.index_manager, batch, index_ref));
}

test "db graph replay ignores resolution artifacts bound to another source contract" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{ .start_index_workers = false });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "graph_a",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_a","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_a","kind":"asset","source":{"type":"field","value":"relations_a"},"content_type":"application/json"}
        \\}
        ,
    });
    try db.addIndex(.{
        .name = "graph_b",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_b","mention_edge_type":"mentions",
        \\    "format":"extraction_relation","path":"$.relations[*]"},
        \\  "artifact":{"name":"relations_b","kind":"asset","source":{"type":"field","value":"relations_b"},"content_type":"application/json"}
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
    try std.testing.expect(!batchAffectsManagedIndex(db.core.index_manager, batch, graph_a));
    try std.testing.expect(!try batchAffectsManagedIndexForReplay(db.core.index_manager, batch, graph_a));
    try std.testing.expect(!try batchAdvancesManagedIndexApplyStateForReplay(db.core.index_manager, batch, graph_a));

    const graph_b = index_manager_mod.ManagedIndexRef{ .name = "graph_b", .kind = .graph };
    try std.testing.expect(batchAffectsManagedIndex(db.core.index_manager, batch, graph_b));
    try std.testing.expect(try batchAffectsManagedIndexForReplay(db.core.index_manager, batch, graph_b));
    try std.testing.expect(try batchAdvancesManagedIndexApplyStateForReplay(db.core.index_manager, batch, graph_b));
}

test "db graph hydration rejects table-qualified entity nodes in local snapshots" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.prov_graph);
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
            \\{"tenant":"visible","relations":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"}]}}
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

    const Authorizer = struct {
        fn authorize(
            _: ?*const anyopaque,
            _: Allocator,
            _: []const u8,
        ) !types.GraphTableReadAuthorization {
            return .{ .allowed = false };
        }
    };
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        db.search(alloc, .{
            .graph_queries = &.{.{ .name = "m", .query = mention_query }},
            .graph_table_read_authorizer = .{
                .ctx = null,
                .authorize_table = Authorizer.authorize,
            },
        }),
    );

    // A table-local DB snapshot cannot hydrate the entity binding. Reject the
    // request instead of returning a successful response with a missing
    // document; coordinator-backed execution owns cross-table hydration.
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        db.search(alloc, .{ .graph_queries = &.{.{ .name = "m", .query = mention_query }} }),
    );

    // Resolver targets are cross-table even when the graph's default node model
    // is document. Reverse traversal admits that external start by edge role,
    // while still enforcing the source-table predicate on the reached document.
    {
        var result = try db.search(alloc, .{
            .graph_queries = &.{.{
                .name = "reverse_mentions",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "prov_graph",
                    .start_nodes = .{ .keys = &.{"person/ada_lovelace"} },
                    .params = .{
                        .edge_types = &.{"mentions"},
                        .direction = .in,
                        .max_depth = 1,
                    },
                },
            }},
            .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
        try std.testing.expectEqualStrings("doc:a", result.graph_results[0].nodes[0].key);
    }
    {
        var result = try db.search(alloc, .{
            .graph_queries = &.{.{
                .name = "bidirectional_mentions",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "prov_graph",
                    .start_nodes = .{ .keys = &.{"person/ada_lovelace"} },
                    .params = .{
                        .edge_types = &.{"mentions"},
                        .direction = .both,
                        .max_depth = 1,
                    },
                },
            }},
            .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
        try std.testing.expectEqualStrings("doc:a", result.graph_results[0].nodes[0].key);
    }

    // Even a coincidentally equal key in the source table must not satisfy an
    // entities-qualified identity. The coordinator must read the named table.
    try db.batch(.{
        .writes = &.{.{ .key = "person/ada_lovelace", .value =
        \\{"entity_type":"person","canonical_name":"Ada Lovelace"}
        }},
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        db.search(alloc, .{ .graph_queries = &.{.{ .name = "m", .query = mention_query }} }),
    );
}

test "db graph relation artifact materializer uses mapping templates" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.items[*]","format":"extraction_relation",
        \\    "nodes":{"target":"{{ _item.to }}"},
        \\    "edge":{"type":"{{ _item.rel }}","weight":"{{ default _item.score 1.0 }}","metadata":{"evidence":"{{ _item.evidence }}","ordinal":"{{ _item_index }}","tenant":"{{ _doc.value.tenant_id }}"}},
        \\    "context":{"doc_fields":["tenant_id"]}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
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

test "graph artifact materializer rejects non-finite mapped weights" {
    const alloc = std.testing.allocator;
    const source = index_manager_mod.GraphArtifactSource{
        .artifact_name = @constCast("relations_v1"),
        .mapping = .{ .weight_template = @constCast("{{ _item.score }}") },
    };
    for ([_][]const u8{ "NaN", "Inf", "-Inf" }) |weight| {
        const raw = try std.fmt.allocPrint(
            alloc,
            "{{\"type\":\"mentions\",\"target\":\"doc:b\",\"score\":\"{s}\"}}",
            .{weight},
        );
        defer alloc.free(raw);
        try std.testing.expectError(error.InvalidGraphEdges, graphWritesFromArtifactValueAlloc(
            alloc,
            "relations_graph",
            "doc:a",
            raw,
            source,
            "application/json",
            null,
            null,
            1,
        ));
    }
}

test "graph artifact parser enforces its independent raw item safety limit" {
    const alloc = std.testing.allocator;
    const source = index_manager_mod.GraphArtifactSource{
        .artifact_name = @constCast("relations_v1"),
    };
    const raw = "[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}},{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:c\"}}]";
    try std.testing.expectError(error.ResourceLimitExceeded, graphWritesFromArtifactValueAlloc(
        alloc,
        "relations_graph",
        "doc:a",
        raw,
        source,
        "application/json",
        null,
        null,
        1,
    ));
}

test "graph relation endpoints canonicalize through injected resolutions" {
    const alloc = std.testing.allocator;
    const source = index_manager_mod.GraphArtifactSource{
        .artifact_name = @constCast("kg_v1"),
        .format = .extraction_graph,
    };
    const raw =
        \\{"entities":[
        \\  {"id":"e0","label":"person","text":"Ada Lovelace"},
        \\  {"id":"v0","label":"event","text":"Ada writes the first program"}
        \\],"relations":[
        \\  {"type":"participates_in","source":"e0","target":"v0"},
        \\  {"type":"mentions_external","source":"e0","target":"ext-node"}
        \\]}
    ;

    // With a resolver targeting the artifact but no resolution landed (an
    // empty "_entities" map), any endpoint referencing an extraction entity
    // — source or target — drops the edge (no durable node exists yet; the
    // resolution replay re-renders it canonically). Both relations here
    // carry the extraction-entity source "e0", so nothing materializes.
    {
        const writes = try graphWritesFromArtifactValueAlloc(alloc, "kg", "doc:a", raw, source, "application/json", null, "{}", 100);
        defer freeGraphWrites(alloc, writes);
        try std.testing.expectEqual(@as(usize, 0), writes.len);
    }

    // With no resolver configured, canonical keys never arrive, so an
    // unresolved source keeps the owning document instead of losing the
    // edge. An unresolved extraction-entity target still drops ("v0"), but
    // the external target survives.
    {
        const writes = try graphWritesFromArtifactValueAlloc(alloc, "kg", "doc:a", raw, source, "application/json", null, null, 100);
        defer freeGraphWrites(alloc, writes);
        try std.testing.expectEqual(@as(usize, 1), writes.len);
        try std.testing.expectEqualStrings("doc:a", writes[0].source);
        try std.testing.expectEqualStrings("ext-node", writes[0].target);
    }

    // With resolutions injected, resolved local ids render the resolver's
    // canonical keys for BOTH endpoints: the topological source becomes the
    // canonical entity while the owning document moves to `owner` for
    // artifact-key routing. External endpoints keep the passthrough with the
    // document as source.
    {
        const resolutions =
            \\{"e0":{"key":"person/ada_lovelace","table":"entities"},"v0":{"key":"event/7f3a","table":"events"}}
        ;
        const writes = try graphWritesFromArtifactValueAlloc(alloc, "kg", "doc:a", raw, source, "application/json", null, resolutions, 100);
        defer freeGraphWrites(alloc, writes);
        try std.testing.expectEqual(@as(usize, 2), writes.len);
        try std.testing.expectEqualStrings("person/ada_lovelace", writes[0].source);
        try std.testing.expectEqualStrings("event/7f3a", writes[0].target);
        try std.testing.expectEqualStrings("doc:a", writes[0].owner);
        try std.testing.expectEqualStrings("person/ada_lovelace", writes[1].source);
        try std.testing.expectEqualStrings("ext-node", writes[1].target);

        // The artifact key routes by owner while embedding the source.
        const key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, writes[0].owner, writes[0].index_name, writes[0].edge_type, writes[0].target, writes[0].source);
        defer alloc.free(key);
        const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, key)).?;
        defer {
            alloc.free(parsed.doc_key);
            alloc.free(parsed.index_name);
            alloc.free(parsed.edge_type);
            alloc.free(parsed.target_doc_key);
            alloc.free(parsed.edge_id);
            if (parsed.source_node) |s| alloc.free(s);
        }
        try std.testing.expectEqualStrings("doc:a", parsed.doc_key);
        try std.testing.expectEqualStrings("person/ada_lovelace", parsed.source_node.?);
    }

    // An inline mention that matches no extraction entity has no canonical
    // identity. It must not manufacture an edge from the owning document.
    {
        const legacy_raw =
            \\{"relations":[{"type":"mentions","source":{"id":"inline","label":"person","text":"Ada"},"target":"ext-node"}]}
        ;
        const writes = try graphWritesFromArtifactValueAlloc(alloc, "kg", "doc:a", legacy_raw, source, "application/json", null, null, 100);
        defer freeGraphWrites(alloc, writes);
        try std.testing.expectEqual(@as(usize, 0), writes.len);
    }
    // An explicit document reference is already canonical and remains usable
    // even when it is absent from the extraction entity list.
    {
        const explicit_raw =
            \\{"relations":[{"type":"mentions","source":{"document_id":"person/ada"},"target":"ext-node"}]}
        ;
        const writes = try graphWritesFromArtifactValueAlloc(alloc, "kg", "doc:a", explicit_raw, source, "application/json", null, null, 100);
        defer freeGraphWrites(alloc, writes);
        try std.testing.expectEqual(@as(usize, 1), writes.len);
        try std.testing.expectEqualStrings("person/ada", writes[0].source);
        try std.testing.expectEqualStrings("doc:a", writes[0].owner);
    }
}

test "db graph visible edge limit applies after identity deduplication" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "max_edges_per_document":1,
        \\  "source":{"artifact":"relations_v1","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"relations":[{"type":"mentions","target":"doc:b","weight":0.5},{"type":"mentions","target":"doc:b","weight":0.9}]}
            ,
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), edges[0].weight, 0.0001);
}

test "db graph relation artifact materializer resolves entity refs and artifact template values" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_graph",
        \\    "nodes":{"target":"{{ _item.target.doc_ref.key }}"},
        \\    "edge":{"type":"{{ _item.type }}","metadata":{"artifact":"{{ _artifact.name }}","content_type":"{{ _artifact.content_type }}","source_text":"{{ _item.source.text }}","target_text":"{{ _item.target.text }}"}}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
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

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.relations_graph);

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

test "db graph config requires stable ids for arbitrary source endpoints" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;
    try std.testing.expectError(error.InvalidIndexConfig, db.addIndex(.{
        .name = "entity_graph",
        .kind = .graph,
        .config_json =
        \\{"sources":[{"artifact":"relations_v1","nodes":{"source":"{{ _artifact.value.owner }}"}}]}
        ,
    }));
}

test "db graph relation artifact materializer deletes edges when asset source disappears" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.relations_graph);

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

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "relations_graph_a",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
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
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
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

test "db graph artifact source reuses user enrichment and rejects incompatible shorthand" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"}
        \\}
        ,
    });

    try std.testing.expectError(error.ConflictingEnrichmentConfig, db.addIndex(.{
        .name = "conflicting_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"body"},"content_type":"application/json"}
        \\}
        ,
    }));
}

test "db graph source artifact deletion clears materialized graph edges" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.relations_graph);

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

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.relations_graph);

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

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation",
        \\    "nodes":{"model":"external","target":"{{ _item.target.entity_id }}"}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"tenant\":\"visible\",\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"entity_id\":\"entity:person:ada_lovelace\"}}]}}" },
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

    var reverse = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "mentioned_by",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"entity:person:ada_lovelace"} },
                    .params = .{ .direction = .in, .edge_types = &.{"mentions"} },
                    .include_documents = true,
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer reverse.deinit();

    try std.testing.expectEqual(@as(u32, 1), reverse.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:a", reverse.graph_results[0].hits[0].id);
    try std.testing.expect(reverse.graph_results[0].hits[0].stored_data != null);

    var bidirectional = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "connected",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"entity:person:ada_lovelace"} },
                    .params = .{ .direction = .both, .edge_types = &.{"mentions"} },
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer bidirectional.deinit();

    try std.testing.expectEqual(@as(u32, 1), bidirectional.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:a", bidirectional.graph_results[0].nodes[0].key);
}

test "db graph artifact source replay catches up after reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(test_index.relations_graph);

        const doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(doc_key);
        const artifact_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
        defer alloc.free(artifact_key);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try encodeChangeRecordPayload(&ctx, .{
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

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        const graph_value = try enrichment_artifact_codec.encodePortableUnboundGraphEdgeAlloc(alloc, 0.8, 0, 0, "");
        defer alloc.free(graph_value);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try encodeChangeRecordPayload(&ctx, .{
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

    const graph_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(graph_key);
    const bound_value = try reopened.core.store.get(alloc, graph_key);
    defer alloc.free(bound_value);
    try std.testing.expect(!enrichment_artifact_codec.isPortableUnboundGraphEdge(bound_value));
    const graph_generation = reopened.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, bound_value);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(graph_generation, decoded.generation);

    var recreated_generation = try collectGraphMutationsForArtifacts(
        alloc,
        reopened.core.store,
        &.{graph_key},
        "relations_graph",
        .{ .expected_generation = graph_generation + 1 },
    );
    defer recreated_generation.deinit();
    try std.testing.expectEqual(@as(usize, 0), recreated_generation.writes.len);
    try std.testing.expectEqual(@as(usize, 1), recreated_generation.deletes.len);
    try std.testing.expectEqual(@as(usize, 0), recreated_generation.generation_bindings.len);
}

test "db graph generation repair preserves corrupt artifact debt after rebuild" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json = "{}",
    });

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-graph-artifact");

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "relations_graph",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    // The generation rebuild completed, but the corrupt source artifact remains
    // durable debt. `repaired` is the stronger healthy outcome, not a duplicate
    // count of completed rebuild work.
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);

    const issues = try db.listArtifactRepairIssues(alloc, .graph, "relations_graph", 10);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("mentions:doc:b", issues[0].artifact_name);

    const raw_artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);
    try std.testing.expectEqualStrings("bad-graph-artifact", raw_artifact);
}

test "db graph methods expose edges, neighbors, and shortest path" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "citations",
        .kind = .graph,
        .config_json = "{}",
    });
    try db.addIndex(test_index.citations_alg);

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
        },
        .sync_level = .full_index,
    });

    const edges = try db.getEdges(alloc, "citations", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 2), edges.len);

    const neighbors = try db.getNeighbors(alloc, "citations", "a", "cites", .out);
    defer traversal_mod.freeOwnedResults(alloc, neighbors);
    try std.testing.expectEqual(@as(usize, 2), neighbors.len);

    const traversed = try db.traverseEdges(alloc, "citations", "a", .{
        .direction = .out,
        .edge_types = &.{"cites"},
        .max_depth = 2,
    });
    defer traversal_mod.freeOwnedResults(alloc, traversed);
    try std.testing.expectEqual(@as(usize, 3), traversed.len);

    const shortest = (try db.findShortestPath(alloc, "citations", "a", "d", &.{"cites"}, .out, .min_hops, 8, null, null)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 2), shortest.length);
    try std.testing.expectEqual(@as(usize, 3), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("d", shortest.nodes[2]);

    const algebraic_shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 8, null, null)).?;
    defer paths_mod.freePath(alloc, algebraic_shortest);
    try std.testing.expectEqual(@as(u32, 2), algebraic_shortest.length);
    try std.testing.expectEqual(@as(usize, 3), algebraic_shortest.nodes.len);
    try std.testing.expectEqualStrings("a", algebraic_shortest.nodes[0]);
    try std.testing.expectEqualStrings("b", algebraic_shortest.nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_shortest.nodes[2]);
    try std.testing.expectEqual(@as(usize, 2), algebraic_shortest.edges.len);
    try std.testing.expectEqualStrings("cites", algebraic_shortest.edges[0].edge_type);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 8, null, null);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 2), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[2]);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var found_alg_stats = false;
    for (stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, "citations_alg")) continue;
        found_alg_stats = true;
        try std.testing.expect(item.algebraic_graph_traversal_attempt_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_proven_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_result_node_count > 0);
    }
    try std.testing.expect(found_alg_stats);
}

test "db graph algebraic shortest path applies exact min-hop edge weight filters" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.citations_alg);

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 0.5 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "e", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "e", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "d", .edge_type = "cites", .weight = 5.0 },
        },
        .sync_level = .full_index,
    });

    const shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 3), shortest.length);
    try std.testing.expectEqual(@as(usize, 4), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("c", shortest.nodes[1]);
    try std.testing.expectEqualStrings("e", shortest.nodes[2]);
    try std.testing.expectEqualStrings("d", shortest.nodes[3]);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 3), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("c", algebraic_k_one[0].nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[3]);
}

test "db graph index routes neighbor queries and doc deletes clean edges" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
    });

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"parent\",\"topology\":\"tree\"}]}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:c", .edge_type = "links", .weight = 2.0 },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].id);

    try db.batch(.{
        .deletes = &.{"doc:b"},
        .sync_level = .full_index,
    });

    var after_delete = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer after_delete.deinit();

    try std.testing.expectEqual(@as(u32, 1), after_delete.total_hits);
    try std.testing.expectEqualStrings("doc:c", after_delete.hits[0].id);
}

test "db graph index routes neighbor queries and doc deletes clean edges with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer t.deinit();
    const db = &t.db;

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
    });

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"parent\",\"topology\":\"tree\"}]}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:c", .edge_type = "links", .weight = 2.0 },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].id);

    try db.batch(.{
        .deletes = &.{"doc:b"},
        .sync_level = .full_index,
    });

    var after_delete = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer after_delete.deinit();

    try std.testing.expectEqual(@as(u32, 1), after_delete.total_hits);
    try std.testing.expectEqualStrings("doc:c", after_delete.hits[0].id);
}

test "db graph ttl document and relational cleanup retire all graph deadlines" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |relational| {
        var tmp = try TestDirectory.init("db-graph-ttl-document-gc");
        defer tmp.cleanup();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.setSchema(.{
            .version = 1,
            .storage_mode = if (relational) .relational else .document,
            .ttl_duration_ns = std.time.ns_per_s,
            .relational_columns = if (relational) &.{.{ .name = "relations", .path = "relations", .column_type = .json, .is_json = true, .json_kind = .any, .allows_null = true }} else &.{},
        });
        const key = try GraphPrimaryPublicationTest.seed(&db, true);
        defer alloc.free(key);
        _ = try applyDerivedBatchToIndexAsync(db.async_context, .{ .changed_artifact_keys = &.{key} }, .{ .name = "g", .kind = .graph }, .{});
        try db.addIndex(.{ .name = "direct", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "direct", .source = "doc:a", .target = "doc:c", .edge_type = "links" }}, .sync_level = .full_index });
        const before = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, before);
        try std.testing.expectEqual(@as(usize, 2), before.len);
        const timestamp = (try ttl_mod.readTimestamp(db.core.store, alloc, "doc:a")).?;
        var clock = platform_clock.ManualClock{};
        clock.setRealtimeNs(timestamp + 2 * std.time.ns_per_s);
        try initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = clock.clock() });
        try db.ttl_runtime.?.runOnce();
        try std.testing.expectEqual(@as(u64, 1), db.ttl_runtime.?.stats().deleted_docs);
        const after = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, after);
        try std.testing.expectEqual(@as(usize, 0), after.len);
        try std.testing.expect((try db.get(alloc, "doc:a")) == null);
        db.close();
        db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        const reopened = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, reopened);
        try std.testing.expectEqual(@as(usize, 0), reopened.len);
    }
}

test "db graph primary publication serializes ordinary writes with source accounting" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |timed| {
        var tmp = try TestDirectory.init("db-graph-publication-write");
        defer tmp.cleanup();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        const key = try GraphPrimaryPublicationTest.seed(&db, timed);
        defer alloc.free(key);
        var pending = GraphPrimaryPublicationTest.Pending{ .db = &db, .artifact_key = key, .sync_level = if (timed) .full_index else .write };
        try GraphPrimaryPublicationTest.run(&pending);
        try GraphPrimaryPublicationTest.expectCount(&db, 2);
        try std.testing.expectError(error.ResourceLimitExceeded, db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:d", .edge_type = "links" }} }));
        try GraphPrimaryPublicationTest.expectCount(&db, 2);
        // Replacing an edge at the limit is valid: admission uses the final
        // batch state rather than rejecting an intermediate addition.
        try db.batch(.{
            .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:d", .edge_type = "links" }},
            .graph_deletes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:c", .edge_type = "links" }},
            .sync_level = .full_index,
        });
        try GraphPrimaryPublicationTest.expectCount(&db, 2);
        db.close();
        db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        try GraphPrimaryPublicationTest.expectCount(&db, 2);
        const edges = try db.getEdges(alloc, "g", "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 2), edges.len);
        try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:d", .edge_type = "links" }}, .sync_level = .full_index });
        try GraphPrimaryPublicationTest.expectCount(&db, 1);
        try db.batch(.{ .deletes = &.{"doc:a"}, .sync_level = .full_index });
        const globals_prefix = try internal_keys.graphGlobalEdgeContenderRootPrefixAlloc(alloc, "doc:a");
        defer alloc.free(globals_prefix);
        const globals = try db.core.store.scanPrefix(alloc, globals_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, globals);
        try std.testing.expectEqual(@as(usize, 0), globals.len);
        const due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, due);
        try std.testing.expectEqual(@as(usize, 0), due.len);
    }
}

test "db graph primary publication serializes ttl cleanup with source accounting" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("db-graph-publication-gc");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const key = try GraphPrimaryPublicationTest.seed(&db, true);
    defer alloc.free(key);
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:c", .edge_type = "links" }}, .sync_level = .full_index });
    const due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due);
    try std.testing.expectEqual(@as(usize, 1), due.len);
    var pending = GraphPrimaryPublicationTest.Pending{ .db = &db, .artifact_key = key, .expire = (try graph_edge_ttl_expiration.decodeDue(due[0].value)).source };
    try GraphPrimaryPublicationTest.run(&pending);
    try std.testing.expect(pending.expired);
    try GraphPrimaryPublicationTest.expectCount(&db, 1);
    const remaining_due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, remaining_due);
    try std.testing.expectEqual(@as(usize, 1), remaining_due.len);
    db.close();
    db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
    try GraphPrimaryPublicationTest.expectCount(&db, 1);
    const edges = try db.getEdges(alloc, "g", "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
}

test "db graph search filters result nodes and hidden traversal intermediates" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.gr_v1);

    try db.batch(.{
        .writes = &.{
            .{ .key = "n:a", .value = "{\"tenant\":\"visible\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:b\"},{\"target\":\"n:d\"}]}}}" },
            .{ .key = "n:b", .value = "{\"tenant\":\"hidden\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:c\"}]}}}" },
            .{ .key = "n:c", .value = "{\"tenant\":\"visible\"}" },
            .{ .key = "n:d", .value = "{\"tenant\":\"visible\"}" },
        },
        .sync_level = .full_index,
    });

    var visible = try db.search(alloc, .{
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer visible.deinit();
    try std.testing.expectEqual(@as(u32, 3), visible.total_hits);

    const graph_key_mask = try filterGraphKeysCallback(
        db,
        alloc,
        .{ .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}" },
        &.{ "n:a", "n:b", "n:c", "n:d", "n:missing" },
    );
    defer alloc.free(graph_key_mask);
    try std.testing.expectEqualSlices(bool, &.{ true, false, true, true, false }, graph_key_mask);

    const hydrated = try db.graphHydrateKeysForInternalRead(
        alloc,
        .{
            .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
            .include_stored = false,
        },
        &.{ "n:a", "n:b", "n:c", "n:d", "n:missing" },
    );
    defer {
        for (hydrated) |*hit| hit.deinit(alloc);
        alloc.free(hydrated);
    }
    try std.testing.expectEqual(@as(usize, 3), hydrated.len);
    try std.testing.expectEqualStrings("n:a", hydrated[0].id);
    try std.testing.expectEqualStrings("n:c", hydrated[1].id);
    try std.testing.expectEqualStrings("n:d", hydrated[2].id);
    for (hydrated) |hit| {
        try std.testing.expect(hit.doc_ordinal != null);
        try std.testing.expect(hit.stored_data == null);
    }

    const hydrated_with_stored = try db.graphHydrateKeysForInternalRead(
        alloc,
        .{
            .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
            .include_stored = true,
        },
        &.{ "n:a", "n:b", "n:c", "n:d", "n:missing" },
    );
    defer {
        for (hydrated_with_stored) |*hit| hit.deinit(alloc);
        alloc.free(hydrated_with_stored);
    }
    try std.testing.expectEqual(@as(usize, 3), hydrated_with_stored.len);
    for (hydrated_with_stored) |hit| {
        try std.testing.expect(hit.doc_ordinal != null);
        try std.testing.expect(hit.stored_data != null);
        try std.testing.expect(std.mem.indexOf(u8, hit.stored_data.?, "\"tenant\":\"visible\"") != null);
    }

    const graph_identity = db.core.index_manager.coverageIdentityForIndex("gr_v1").?;
    var stale_graph_identity = graph_identity;
    stale_graph_identity.generation ^= 1;
    try std.testing.expectError(error.IndexGenerationMismatch, db.graphHasIncomingEdgesForInternalRead(alloc, "gr_v1", &.{"n:b"}, stale_graph_identity, null));
    stale_graph_identity = graph_identity;
    stale_graph_identity.config_fingerprint = graph_identity.config_fingerprint.? ^ 1;
    try std.testing.expectError(error.IndexGenerationMismatch, db.graphHasIncomingEdgesForInternalRead(alloc, "gr_v1", &.{"n:b"}, stale_graph_identity, null));
    try std.testing.expectError(error.IndexGenerationMismatch, db.graphHasIncomingEdgesForInternalRead(alloc, "missing", &.{"n:b"}, graph_identity, null));
    try std.testing.expectError(error.InvalidArgument, db.graphHasIncomingEdgesForInternalRead(alloc, "gr_v1", &.{"n:b"}, .{ .generation = 0, .config_fingerprint = null }, null));
    const read_generation = try db.currentIdentityReadGenerationForRequest(null);
    try std.testing.expectError(error.IdentityReadGenerationChanged, db.graphHasIncomingEdgesForInternalRead(alloc, "gr_v1", &.{"n:b"}, graph_identity, read_generation ^ 1));
    const incoming = try db.graphHasIncomingEdgesForInternalRead(
        alloc,
        "gr_v1",
        &.{ "n:a", "n:b", "n:c", "n:d", "n:missing" },
        graph_identity,
        read_generation,
    );
    defer alloc.free(incoming);
    try std.testing.expectEqualSlices(
        bool,
        &.{ false, true, true, true, false },
        incoming,
    );

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "links",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:a"} },
                    .params = .{
                        .direction = .out,
                        .edge_types = &.{"links"},
                        .max_depth = 2,
                        .include_paths = false,
                    },
                    .include_documents = true,
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    const graph_result = result.graph_results[0];
    try std.testing.expectEqual(@as(u32, 1), graph_result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), graph_result.nodes.len);
    try std.testing.expectEqualStrings("n:d", graph_result.nodes[0].key);
    try std.testing.expect(graph_result.nodes[0].path == null);
    try std.testing.expectEqual(@as(usize, 1), graph_result.hits.len);
    try std.testing.expectEqualStrings("n:d", graph_result.hits[0].id);
    try std.testing.expect(graph_result.hits[0].stored_data != null);

    var direct = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .traverse,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"n:a"} },
            .params = .{
                .direction = .out,
                .edge_types = &.{"links"},
                .max_depth = 2,
                .include_paths = false,
            },
            .include_documents = true,
        } },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer direct.deinit();

    try std.testing.expectEqual(@as(u32, 1), direct.total_hits);
    try std.testing.expectEqual(@as(usize, 1), direct.hits.len);
    try std.testing.expectEqualStrings("n:d", direct.hits[0].id);
    try std.testing.expect(direct.hits[0].stored_data != null);

    var query_scoped = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "query_scoped_visible",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:a"} },
                    .params = .{
                        .direction = .out,
                        .edge_types = &.{"links"},
                        .max_depth = 2,
                        .node_filter = .{
                            .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
                        },
                    },
                },
            },
        },
        .limit = 10,
    });
    defer query_scoped.deinit();
    try std.testing.expectEqual(@as(u32, 1), query_scoped.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("n:d", query_scoped.graph_results[0].nodes[0].key);

    var bounded = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "bounded_links",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:a"} },
                    .params = .{
                        .direction = .out,
                        .edge_types = &.{"links"},
                        .max_depth = 1,
                        .max_results = 1,
                    },
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer bounded.deinit();
    try std.testing.expectEqual(@as(u32, 1), bounded.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("n:d", bounded.graph_results[0].nodes[0].key);

    var pattern = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "bounded_pattern",
                .query = .{
                    .query_type = .pattern,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:a"} },
                    .params = .{ .max_results = 1 },
                    .pattern = &.{
                        .{ .alias = "source" },
                        .{
                            .alias = "target",
                            .edge = .{
                                .direction = .out,
                                .types = &.{"links"},
                            },
                        },
                    },
                    .return_aliases = &.{ "source", "target" },
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer pattern.deinit();
    try std.testing.expectEqual(@as(u32, 1), pattern.graph_results[0].total_hits);
    try std.testing.expectEqual(@as(usize, 1), pattern.graph_results[0].matches.len);
    var matched_visible_target = false;
    for (pattern.graph_results[0].matches[0].bindings) |binding| {
        if (std.mem.eql(u8, binding.alias, "target")) {
            try std.testing.expectEqualStrings("n:d", binding.node.key);
            matched_visible_target = true;
        }
    }
    try std.testing.expect(matched_visible_target);

    var hidden_start = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "hidden_start",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:b"} },
                    .params = .{
                        .direction = .out,
                        .edge_types = &.{"links"},
                    },
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer hidden_start.deinit();
    try std.testing.expectEqual(@as(u32, 0), hidden_start.graph_results[0].total_hits);
}

test "db graph shortest path searches through admitted alternatives" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(test_index.gr_v1);
    try db.batch(.{
        .writes = &.{
            .{ .key = "n:a", .value = "{\"tenant\":\"visible\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:b\"},{\"target\":\"n:d\"}]}}}" },
            .{ .key = "n:b", .value = "{\"tenant\":\"hidden\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:c\"}]}}}" },
            .{ .key = "n:c", .value = "{\"tenant\":\"visible\"}" },
            .{ .key = "n:d", .value = "{\"tenant\":\"visible\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:e\"}]}}}" },
            .{ .key = "n:e", .value = "{\"tenant\":\"visible\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:c\"}]}}}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "visible_path",
                .query = .{
                    .query_type = .shortest_path,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:a"} },
                    .target_nodes = .{ .keys = &.{"n:c"} },
                    .params = .{
                        .direction = .out,
                        .edge_types = &.{"links"},
                        .max_depth = 4,
                    },
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results[0].paths.len);
    try std.testing.expectEqual(@as(u32, 3), result.graph_results[0].paths[0].length);
    try std.testing.expectEqual(@as(usize, 4), result.graph_results[0].paths[0].nodes.len);
    try std.testing.expectEqualStrings("n:a", result.graph_results[0].paths[0].nodes[0]);
    try std.testing.expectEqualStrings("n:d", result.graph_results[0].paths[0].nodes[1]);
    try std.testing.expectEqualStrings("n:e", result.graph_results[0].paths[0].nodes[2]);
    try std.testing.expectEqualStrings("n:c", result.graph_results[0].paths[0].nodes[3]);
}

test "db graph result merging preserves artifact refs" {
    const alloc = std.testing.allocator;

    var result = types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
        .graph_results = try alloc.alloc(types.GraphSearchResult, 1),
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.0,
    };

    result.graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
    };
    result.graph_results[0].hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:Ym9keV9jaHVua3NfdjE:0"),
        .score = 0.75,
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "body_chunks_v1"),
            .kind = .chunk,
            .chunk_id = 0,
        },
    };

    try applyGraphUnion(alloc, &result);

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    const merged_ref = result.hits[1].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.ArtifactKind.chunk, merged_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), merged_ref.chunk_id);
    try std.testing.expectEqualStrings("doc:a", merged_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", merged_ref.name);
}

test "db graph intersection preserves artifact refs" {
    const alloc = std.testing.allocator;

    var result = types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
        .graph_results = try alloc.alloc(types.GraphSearchResult, 1),
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:Ym9keV9jaHVua3NfdjE:0"),
        .score = 0.75,
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "body_chunks_v1"),
            .kind = .chunk,
            .chunk_id = 0,
        },
    };

    result.graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
    };
    result.graph_results[0].hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:Ym9keV9jaHVua3NfdjE:0"),
        .score = 0.75,
    };

    try applyGraphIntersection(alloc, &result);

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    const kept_ref = result.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.ArtifactKind.chunk, kept_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), kept_ref.chunk_id);
    try std.testing.expectEqualStrings("doc:a", kept_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", kept_ref.name);
}

test "db graph index reloads on reopen for neighbor queries" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.gr_v1);

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
            },
        });

        try db.batch(.{
            .graph_writes = &.{
                .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
                .{ .index_name = "gr_v1", .source = "doc:b", .target = "doc:c", .edge_type = "links", .weight = 1.0 },
            },
        });
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .query = .{ .graph = .{
                .query_type = .neighbors,
                .index_name = "gr_v1",
                .start_nodes = .{ .keys = &.{"doc:a"} },
                .params = .{ .edge_types = &.{"links"} },
            } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }
}

test "db graph edge ttl is stamped by server and hidden after reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-edge-ttl");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "ttl_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1ns\"}}" });
        try db.batch(.{ .writes = &.{
            .{ .key = "doc:a", .value = "{}" },
            .{ .key = "doc:b", .value = "{}" },
        } });
        try db.batch(.{ .graph_writes = &.{.{
            .index_name = "ttl_graph",
            .source = "doc:a",
            .target = "doc:b",
            .edge_type = "links",
        }} });
        const artifact_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "ttl_graph", "links", "doc:b", "doc:a");
        defer alloc.free(artifact_key);
        const first_raw = try db.core.store.get(alloc, artifact_key);
        defer alloc.free(first_raw);
        var first_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, first_raw);
        defer first_edge.deinit(alloc);
        try std.testing.expect(first_edge.ttl_created_ns != 0);
        try db.batch(.{ .graph_writes = &.{.{
            .index_name = "ttl_graph",
            .source = "doc:a",
            .target = "doc:b",
            .edge_type = "links",
            .weight = 2,
        }} });
        const second_raw = try db.core.store.get(alloc, artifact_key);
        defer alloc.free(second_raw);
        var second_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, second_raw);
        defer second_edge.deinit(alloc);
        try std.testing.expectEqual(first_edge.ttl_created_ns, second_edge.ttl_created_ns);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    var result = try reopened.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "ttl_graph",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
}

test "db graph document clear removes an expired physical edge" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-expired-graph-clear");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "ttl_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
    const entry = db.core.index_manager.graphIndex("ttl_graph") orelse return error.TestExpectedGraphIndex;
    const now_ns = currentTimeNs();
    try entry.index.batchApply(&.{
        .{ .source = "doc:a", .target = "doc:b", .edge_type = "links", .owner = "doc:a", .ttl_created_ns = 1 },
        .{ .source = "doc:a", .target = "doc:c", .edge_type = "links", .owner = "doc:a", .ttl_created_ns = now_ns },
    }, &.{});
    try entry.index.replaceContributionSnapshots(&.{
        .{ .source = "doc:a", .target = "doc:b", .edge_type = "links", .owner = "doc:a", .contributions = &.{.{ .state_key = "old", .source_priority = 0, .weight = 1, .created_at = 0, .updated_at = 0, .ttl_created_ns = 1, .metadata_json = "" }} },
        .{ .source = "doc:a", .target = "doc:c", .edge_type = "links", .owner = "doc:a", .contributions = &.{.{ .state_key = "old", .source_priority = 0, .weight = 1, .created_at = 0, .updated_at = 0, .ttl_created_ns = now_ns, .metadata_json = "" }} },
    });
    const visible = try entry.index.getEdges(alloc, "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, visible);
    try std.testing.expectEqual(@as(usize, 1), visible.len);
    const physical_before = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, physical_before);
    try std.testing.expectEqual(@as(usize, 2), physical_before.len);
    try std.testing.expect((try entry.index.nextContributionExpirationAtOrAfter(0)) != null);
    graph_mod.test_abort_doc_clear_after_forward_commit = true;
    defer graph_mod.test_abort_doc_clear_after_forward_commit = false;
    try std.testing.expectError(error.TestInjectedGraphClearFailure, db.core.index_manager.deleteGraphDocsByName("ttl_graph", &.{"doc:a"}));
    graph_mod.test_abort_doc_clear_after_forward_commit = false;
    const retry_edges = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:a", "links", .both);
    defer graph_mod.GraphIndex.freeEdges(alloc, retry_edges);
    try std.testing.expectEqual(@as(usize, 2), retry_edges.len);
    try db.core.index_manager.deleteGraphDocsByName("ttl_graph", &.{"doc:a"});
    const physical_after = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, physical_after);
    try std.testing.expectEqual(@as(usize, 0), physical_after.len);
    try std.testing.expectEqual(@as(?u64, null), try entry.index.nextContributionExpirationAtOrAfter(0));
    var old_member_key = std.ArrayListUnmanaged(u8).empty;
    defer old_member_key.deinit(alloc);
    try old_member_key.appendSlice(alloc, "meta:owner_member:v1:");
    for ([_][]const u8{ "doc:a", "links", "doc:c", "doc:a" }) |part|
        try internal_keys.appendEncodedComponent(&old_member_key, alloc, part);
    {
        var member_read = try entry.index.reverse_store.beginRead();
        defer member_read.abort();
        try std.testing.expectError(error.NotFound, member_read.get(old_member_key.items));
    }

    // A later owner must not inherit the deleted document's contender or
    // membership row for the same logical edge.
    try entry.index.batchApply(&.{.{ .source = "doc:a", .target = "doc:c", .edge_type = "links", .owner = "doc:other", .weight = 2, .ttl_created_ns = now_ns }}, &.{});
    try entry.index.replaceContributionSnapshots(&.{.{ .source = "doc:a", .target = "doc:c", .edge_type = "links", .owner = "doc:other", .contributions = &.{.{ .state_key = "new", .source_priority = 1, .weight = 2, .created_at = 0, .updated_at = 0, .ttl_created_ns = now_ns, .metadata_json = "" }} }});
    const replacement = try entry.index.getEdges(alloc, "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, replacement);
    try std.testing.expectEqual(@as(usize, 1), replacement.len);
    try std.testing.expectEqual(@as(f64, 2), replacement[0].weight);
    try entry.index.batchApply(&.{}, &.{.{ .source = "doc:a", .target = "doc:c", .edge_type = "links", .owner = "doc:other" }});
    try entry.index.replaceContributionSnapshots(&.{.{ .source = "doc:a", .target = "doc:c", .edge_type = "links", .owner = "doc:other", .contributions = &.{} }});
    const after_withdrawal = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, after_withdrawal);
    try std.testing.expectEqual(@as(usize, 0), after_withdrawal.len);
}

test "db graph document clear retries an unmarked edge after forward commit" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |with_ttl| {
        var path_tmp = try TestDirectory.init("db-graph-clear-intent");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        const config_json = if (with_ttl) "{\"ttl\":{\"duration\":\"1h\"}}" else "{}";
        {
            var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
            defer db.close();
            try db.addIndex(.{ .name = "graph", .kind = .graph, .config_json = config_json });
            const entry = db.core.index_manager.graphIndex("graph") orelse return error.TestExpectedGraphIndex;
            // Exercise a stored edge with neither an owner row nor a TTL
            // contribution marker; neither can serve as a recovery cursor.
            try entry.index.batchApply(&.{.{
                .source = "doc:a",
                .target = "doc:b",
                .edge_type = "links",
                .ttl_created_ns = if (with_ttl) currentTimeNs() else 0,
            }}, &.{});
            graph_mod.test_abort_doc_clear_after_forward_commit = true;
            defer graph_mod.test_abort_doc_clear_after_forward_commit = false;
            try std.testing.expectError(error.TestInjectedGraphClearFailure, db.core.index_manager.deleteGraphDocsByName("graph", &.{"doc:a"}));
            graph_mod.test_abort_doc_clear_after_forward_commit = false;
            const pending = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:a", "links", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, pending);
            try std.testing.expectEqual(@as(usize, 1), pending.len);
        }
        var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer reopened.close();
        const entry = reopened.core.index_manager.graphIndex("graph") orelse return error.TestExpectedGraphIndex;
        const pending = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, pending);
        try std.testing.expectEqual(@as(usize, 1), pending.len);
        try reopened.core.index_manager.deleteGraphDocsByName("graph", &.{"doc:a"});
        const incoming = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:b", "links", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, incoming);
        try std.testing.expectEqual(@as(usize, 0), incoming.len);
        const after = try entry.index.getPhysicalEdgesForDeletion(alloc, "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, after);
        try std.testing.expectEqual(@as(usize, 0), after.len);
    }
}

test "db graph untimed migration restores source order and direct contributor precedence" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-untimed-order");
    defer path_tmp.cleanup();
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addEnrichment(.{ .name = "primary_relations_v1", .kind = .asset, .field = "primary_relations", .content_type = "application/json" });
        try db.addEnrichment(.{ .name = "fallback_relations_v1", .kind = .asset, .field = "fallback_relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
            \\{"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
        });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
        }}, .sync_level = .enrichments });
        try db.runUntilIdle();
        const initial = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, initial);
        try std.testing.expectEqual(@as(f64, 2), initial[0].weight);
        try std.testing.expectEqual(@as(u64, 1), initial[0].winner_rank);
        // Simulate an older private projection: adjacency is present, but
        // neither contributor snapshots nor the completion marker exist.
        try db.core.index_manager.resetGraphIndexForArtifactRebuild("relations_graph");
        const entry = db.core.index_manager.graphIndex("relations_graph").?;
        try entry.index.batchApply(&.{.{ .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .weight = 99 }}, &.{});
        // A partially replayed owner snapshot must also be overwritten when
        // writable open resumes the migration without its completion marker.
        try entry.index.replaceContributionSnapshots(&.{.{ .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .owner = "doc:a", .contributions = &.{
            .{ .state_key = "partial", .source_priority = 1, .weight = 99, .created_at = 0, .updated_at = 0, .ttl_created_ns = 0, .metadata_json = "" },
        } }});
        try std.testing.expect(!try entry.index.contributionOrderingReady());
        try entry.index.sync(true);
    }
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    const entry = db.core.index_manager.graphIndex("relations_graph") orelse return error.TestExpectedGraphIndex;
    try std.testing.expect(try entry.index.contributionOrderingReady());
    const migrated = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, migrated);
    try std.testing.expectEqual(@as(usize, 1), migrated.len);
    try std.testing.expectEqual(@as(f64, 2), migrated[0].weight);
    try std.testing.expectEqual(@as(u64, 1), migrated[0].winner_rank);
    try std.testing.expect(migrated[0].winner_key_hex.len > 0);
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "relations_graph", .source = "doc:a", .target = "doc:b", .edge_type = "mentions", .weight = 4 }}, .sync_level = .full_index });
    // An asset revision cannot displace an explicit direct contribution.
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":5},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const direct = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, direct);
    try std.testing.expectEqual(@as(f64, 4), direct[0].weight);
    try std.testing.expectEqual(@as(u64, 0), direct[0].winner_rank);
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "relations_graph", .source = "doc:a", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
    // Explicit relationship deletion retires the complete assertion, including
    // retained source inputs. Only a later owner lifecycle may revive it.
    const retired = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, retired);
    try std.testing.expectEqual(@as(usize, 0), retired.len);
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":5},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"revived":true}
    }}, .sync_level = .full_index });
    const restored = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, restored);
    try std.testing.expectEqual(@as(usize, 1), restored.len);
    try std.testing.expectEqual(@as(f64, 5), restored[0].weight);
    try std.testing.expectEqual(@as(u64, 1), restored[0].winner_rank);
    const due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due);
    try std.testing.expectEqual(@as(usize, 0), due.len);
}

test "db graph untimed migration restores source order and direct contributor precedence for legacy writes" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("db-legacy-direct-order");
    defer tmp.cleanup();
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"relations_v1\"}]}" });
        try db.batch(.{ .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":2}}" },
            .{ .key = "doc:c", .value = "{}" },
            .{ .key = "doc:e", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":3}}" },
        }, .sync_level = .enrichments });
        try db.runUntilIdle();
        for ([_][]const u8{ "doc:a", "doc:c" }) |owner| {
            try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = owner, .target = "doc:b", .edge_type = "mentions", .weight = 4 }}, .sync_level = .full_index });
            const generation = db.core.index_manager.graphIndex("g").?.config.coverage_generation;
            const edge_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, owner, "g", "mentions", "doc:b", owner);
            defer alloc.free(edge_key);
            const state_key = try internal_keys.graphDirectStateKeyAlloc(alloc, owner, "g");
            defer alloc.free(state_key);
            const global = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, "g", generation, edge_key, @intCast(graph_mod.direct_source_priority), state_key);
            defer alloc.free(global);
            const local = try internal_keys.graphEdgeContenderKeyAlloc(alloc, owner, "g", edge_key, state_key);
            defer alloc.free(local);
            // Old direct writes persisted the artifact but no contributor.
            // The asset contender for doc:a remains unchanged.
            const count_key = try internal_keys.graphEdgeContenderCountKeyAlloc(alloc, owner, "g");
            defer alloc.free(count_key);
            const count = try graph_edge_contender.encodeVisibleCount(generation, if (std.mem.eql(u8, owner, "doc:a")) 1 else 0);
            try db.core.store.putBatch(&.{.{ .key = count_key, .value = &count }}, &.{ global, local });
        }
        try db.core.index_manager.resetGraphIndexForArtifactRebuild("g");
        const graph = &db.core.index_manager.graphIndex("g").?.index;
        for ([_][]const u8{ "doc:a", "doc:c", "doc:e" }) |owner| {
            try graph.batchApply(&.{.{ .source = owner, .target = "doc:b", .edge_type = "mentions", .weight = if (std.mem.eql(u8, owner, "doc:e")) 3 else 4 }}, &.{});
        }
        try db.core.store.sync(true);
        try graph.sync(true);
    }
    // Repeating migration after discarding the projection also exercises
    // durable recovery and idempotent membership/cardinality publication.
    for (0..2) |pass| {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        for ([_][]const u8{ "doc:a", "doc:c", "doc:e" }) |owner| {
            const edges = try db.getEdges(alloc, "g", owner, "mentions", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
            const source_only = std.mem.eql(u8, owner, "doc:e");
            try std.testing.expectEqual(@as(f64, if (source_only) 3 else 4), edges[0].weight);
            try std.testing.expectEqual(@as(u64, if (source_only) 1 else 0), edges[0].winner_rank);
            const count_key = try internal_keys.graphEdgeContenderCountKeyAlloc(alloc, owner, "g");
            defer alloc.free(count_key);
            const raw = try db.core.store.get(alloc, count_key);
            defer alloc.free(raw);
            try std.testing.expectEqual(@as(?usize, 1), try graph_edge_contender.decodeVisibleCount(raw, db.core.index_manager.graphIndex("g").?.config.coverage_generation));
        }
        if (pass == 0) {
            try db.core.index_manager.resetGraphIndexForArtifactRebuild("g");
            const graph = &db.core.index_manager.graphIndex("g").?.index;
            for ([_][]const u8{ "doc:a", "doc:c", "doc:e" }) |owner| {
                try graph.batchApply(&.{.{ .source = owner, .target = "doc:b", .edge_type = "mentions", .weight = if (std.mem.eql(u8, owner, "doc:e")) 3 else 4 }}, &.{});
            }
            try graph.sync(true);
        } else {
            try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":5}}" }}, .sync_level = .enrichments });
            try db.runUntilIdle();
            const direct = try db.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, direct);
            try std.testing.expectEqual(@as(f64, 4), direct[0].weight);
            try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
            const retired = try db.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, retired);
            try std.testing.expectEqual(@as(usize, 0), retired.len);
            // Historical direct-contributor migration preserves deletion
            // authority too; a new owner lifecycle can replay its asset input.
            try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"relations\":{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"},\"weight\":5},\"revived\":true}" }}, .sync_level = .full_index });
            const revived = try db.getEdges(alloc, "g", "doc:a", "mentions", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, revived);
            try std.testing.expectEqual(@as(usize, 1), revived.len);
            try std.testing.expectEqual(@as(f64, 5), revived[0].weight);
            try std.testing.expectEqual(@as(u64, 1), revived[0].winner_rank);
            try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "doc:c", .target = "doc:b", .edge_type = "mentions" }}, .sync_level = .full_index });
            const deleted = try db.getEdges(alloc, "g", "doc:c", "mentions", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, deleted);
            try std.testing.expectEqual(@as(usize, 0), deleted.len);
        }
    }
}

test "db graph ttl projects source contenders across reopen" {
    const alloc = std.testing.allocator;
    var original_primary_timestamp: u64 = 0;
    var path_tmp = try TestDirectory.init("db-source-edge-ttl");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addEnrichment(.{ .name = "primary_relations_v1", .kind = .asset, .field = "primary_relations", .content_type = "application/json" });
        try db.addEnrichment(.{ .name = "fallback_relations_v1", .kind = .asset, .field = "fallback_relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
            \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"primary_relations_v1"},{"artifact":"fallback_relations_v1"}]}
        });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
        }}, .sync_level = .enrichments });
        try db.runUntilIdle();
        const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
        const artifact_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
        defer alloc.free(artifact_key);
        const contender_prefix = try internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, artifact_key);
        defer alloc.free(contender_prefix);
        const initial_contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, initial_contenders);
        try std.testing.expectEqual(@as(usize, 1), initial_contenders.len);
        const initial_view = (try graph_edge_contender.decode(initial_contenders[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
        var initial_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, initial_view.payload);
        defer initial_edge.deinit(alloc);
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
        }}, .sync_level = .enrichments });
        try db.runUntilIdle();
        const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqual(@as(f64, 2), edges[0].weight);

        const contenders = try db.core.store.scanPrefix(alloc, contender_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, contenders);
        var primary_timestamp: u64 = 0;
        var fallback_timestamp: u64 = 0;
        for (contenders) |contender_row| {
            const contender = (try graph_edge_contender.decode(contender_row.value, generation)) orelse continue;
            var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, contender.payload);
            defer edge.deinit(alloc);
            if (contender.source_priority == 0) {
                try std.testing.expectEqualStrings(initial_view.state_key, contender.state_key);
                primary_timestamp = edge.ttl_created_ns;
            }
            if (contender.source_priority == 1) fallback_timestamp = edge.ttl_created_ns;
        }
        try std.testing.expect(primary_timestamp != 0);
        original_primary_timestamp = primary_timestamp;
        try std.testing.expectEqual(initial_edge.ttl_created_ns, primary_timestamp);
        try std.testing.expect(fallback_timestamp > primary_timestamp);
        var changed_primary = false;
        for (contenders) |contender_row| {
            const contender = (try graph_edge_contender.decode(contender_row.value, generation)) orelse continue;
            if (contender.source_priority != 0) continue;
            const header = try enrichment_artifact_codec.decodeHeader(contender.payload);
            var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, contender.payload);
            defer edge.deinit(alloc);
            const expired_at = currentTimeNs() - 2 * std.time.ns_per_hour;
            const expired_payload = try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(alloc, if (header.flags.has_source_hash) header.source_hash else null, generation, edge.weight, edge.created_at, edge.updated_at, expired_at, edge.metadata_json);
            defer alloc.free(expired_payload);
            const changed = try graph_edge_contender.encodeAlloc(alloc, generation, contender.source_priority, artifact_key, contender.state_key, expired_payload);
            defer alloc.free(changed);
            try db.core.store.putBatch(&.{.{ .key = contender_row.key, .value = changed }}, &.{});
            changed_primary = true;
            break;
        }
        try std.testing.expect(changed_primary);
        try db.core.index_manager.applyGraphMutationsByName("relations_graph", &.{.{
            .index_name = "relations_graph",
            .source = "doc:a",
            .target = "doc:b",
            .edge_type = "mentions",
            .weight = 2,
            .ttl_created_ns = currentTimeNs() - 2 * std.time.ns_per_hour,
        }}, &.{});
        const fallback = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, fallback);
        try std.testing.expectEqual(@as(usize, 1), fallback.len);
        try std.testing.expectEqual(@as(f64, 3), fallback[0].weight);
    }
    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqual(@as(f64, 3), edges[0].weight);
    try reopened.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try reopened.runUntilIdle();
    try reopened.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback_relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try reopened.runUntilIdle();
    const generation = reopened.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const artifact_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(artifact_key);
    const contender_prefix = try internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, artifact_key);
    defer alloc.free(contender_prefix);
    const new_contenders = try reopened.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, new_contenders);
    var restarted_primary_timestamp: u64 = 0;
    for (new_contenders) |row| {
        const contender = (try graph_edge_contender.decode(row.value, generation)) orelse continue;
        if (contender.source_priority != 0) continue;
        var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, contender.payload);
        defer edge.deinit(alloc);
        restarted_primary_timestamp = edge.ttl_created_ns;
    }
    try std.testing.expect(restarted_primary_timestamp > original_primary_timestamp);
}

test "db graph ttl tombstone suppresses unchanged source replay and admits changed revision" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-source-edge-tombstone");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();

    const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const edge_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(edge_key);
    const contender_prefix = try internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, edge_key);
    defer alloc.free(contender_prefix);
    const initial = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, initial);
    try std.testing.expectEqual(@as(usize, 1), initial.len);
    const view = (try graph_edge_contender.decode(initial[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
    const state_key = try alloc.dupe(u8, view.state_key);
    defer alloc.free(state_key);
    const original_payload = try alloc.dupe(u8, view.payload);
    defer alloc.free(original_payload);
    const digest = try graph_edge_ttl_tombstone.sourceDigest(alloc, original_payload);
    const tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, "relations_graph", generation, state_key);
    defer alloc.free(tombstone_key);
    const tombstone = (graph_edge_ttl_tombstone.Tombstone{ .deadline_ns = currentTimeNs(), .source_digest = digest }).encode();
    try db.core.store.put(tombstone_key, &tombstone);

    {
        var reconciled = try reconcileSingleGraphStateContenders(
            alloc,
            db.core.store,
            "doc:a",
            "relations_graph",
            state_key,
            &.{edge_key},
            &.{.{ .key = edge_key, .value = original_payload }},
            db.core.index_manager.graphArtifactSources("relations_graph"),
            generation,
            std.time.ns_per_hour,
        );
        defer reconciled.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), reconciled.visible_count);
        try std.testing.expect(reconciled.winners.map.get(edge_key) == null);
        try commitGraphContenderReconcilePage(alloc, db.core.store, &.{edge_key}, &reconciled, &.{}, &.{});
    }
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, edge_key));
    const suppressed = try db.core.store.scanPrefix(alloc, contender_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, suppressed);
    try std.testing.expectEqual(@as(usize, 0), suppressed.len);

    const header = try enrichment_artifact_codec.decodeHeader(original_payload);
    var original = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, original_payload);
    defer original.deinit(alloc);
    const new_timestamp = currentTimeNs();
    const changed_payload = try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(
        alloc,
        if (header.flags.has_source_hash) header.source_hash else null,
        generation,
        3,
        original.created_at,
        original.updated_at,
        new_timestamp,
        original.metadata_json,
    );
    defer alloc.free(changed_payload);
    {
        var reconciled = try reconcileSingleGraphStateContenders(
            alloc,
            db.core.store,
            "doc:a",
            "relations_graph",
            state_key,
            &.{edge_key},
            &.{.{ .key = edge_key, .value = changed_payload }},
            db.core.index_manager.graphArtifactSources("relations_graph"),
            generation,
            std.time.ns_per_hour,
        );
        defer reconciled.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), reconciled.visible_count);
        try std.testing.expect(reconciled.winners.map.get(edge_key) != null);
        try commitGraphContenderReconcilePage(alloc, db.core.store, &.{edge_key}, &reconciled, &.{}, &.{});
    }
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, tombstone_key));
    const restored_raw = try db.core.store.get(alloc, edge_key);
    defer alloc.free(restored_raw);
    var restored = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, restored_raw);
    defer restored.deinit(alloc);
    try std.testing.expectEqual(@as(f64, 3), restored.weight);
    try std.testing.expectEqual(new_timestamp, restored.ttl_created_ns);
}

test "db graph ttl guarded cleanup removes one source and replays fallback" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-ttl-guarded-cleanup");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addEnrichment(.{ .name = "primary_v1", .kind = .asset, .field = "primary", .content_type = "application/json" });
    try db.addEnrichment(.{ .name = "fallback_v1", .kind = .asset, .field = "fallback", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"primary_v1"},{"artifact":"fallback_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();

    const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const edge_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(edge_key);
    const prefix = try internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, "relations_graph", generation, edge_key);
    defer alloc.free(prefix);
    const contenders = try db.core.store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, contenders);
    try std.testing.expectEqual(@as(usize, 2), contenders.len);
    var candidate: ?GraphTtlCandidate = null;
    for (contenders) |row| {
        const view = (try graph_edge_contender.decode(row.value, generation)) orelse continue;
        if (view.source_priority != 0) continue;
        var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, view.payload);
        defer edge.deinit(alloc);
        var digest: [32]u8 = undefined;
        GraphTtlSha256.hash(row.value, &digest, .{});
        candidate = .{
            .index_name = "relations_graph",
            .generation = generation,
            .edge_key = edge_key,
            .state_key = view.state_key,
            .source_priority = view.source_priority,
            .deadline_ns = edge.ttl_created_ns + std.time.ns_per_hour,
            .contender_digest = digest,
        };
    }
    const due = candidate orelse return error.TestExpectedGraphEdgeContender;
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(due.deadline_ns + 1);
    var ttl_ctx = TtlCleanupContext{ .batch = db.batchContext(), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(try expireGraphTtlCandidateContext(&ttl_ctx, due));
    try std.testing.expect(!(try expireGraphTtlCandidateContext(&ttl_ctx, due)));

    const remaining = try db.core.store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    const survivor = (try graph_edge_contender.decode(remaining[0].value, generation)) orelse return error.TestExpectedGraphEdgeContender;
    try std.testing.expectEqual(@as(usize, 1), survivor.source_priority);
    const tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, "relations_graph", generation, due.state_key);
    defer alloc.free(tombstone_key);
    const tombstone_raw = try db.core.store.get(alloc, tombstone_key);
    defer alloc.free(tombstone_raw);
    try std.testing.expectEqual(due.deadline_ns, (try graph_edge_ttl_tombstone.Tombstone.decode(tombstone_raw)).deadline_ns);
    const fallback = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, fallback);
    try std.testing.expectEqual(@as(usize, 1), fallback.len);
    try std.testing.expectEqual(@as(f64, 3), fallback[0].weight);

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"fallback":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const unchanged = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, unchanged);
    try std.testing.expectEqual(@as(f64, 3), unchanged[0].weight);
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"primary":{"type":"mentions","target":{"document_id":"doc:b"},"weight":4},"fallback":{"type":"mentions","target":{"document_id":"doc:b"},"weight":3},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, tombstone_key));
    const revived = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, revived);
    try std.testing.expectEqual(@as(f64, 4), revived[0].weight);
}

test "db graph ttl shared worker expires graph source without document ttl" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-ttl-shared-worker");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const due = try graph_edge_ttl_expiration.decode(due_rows[0].value);
    try std.testing.expectEqual(due.deadline_ns, try graph_edge_ttl_expiration.deadlineFromKey(due_rows[0].key));
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(due.deadline_ns + 1);
    try initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = manual_clock.clock() });
    try db.ttl_runtime.?.runOnce();
    const stats = db.ttl_runtime.?.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.scanned_graph_candidates);
    try std.testing.expectEqual(@as(u64, 1), stats.expired_graph_sources);
    const due_after = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_after);
    try std.testing.expectEqual(@as(usize, 0), due_after.len);
    // Simulate a stale copied deadline row after the source has gone away.
    try db.core.store.put(due_rows[0].key, due_rows[0].value);
    try db.ttl_runtime.?.runOnce();
    const pruned = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, pruned);
    try std.testing.expectEqual(@as(usize, 0), pruned.len);
    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
    const doc = try db.get(alloc, "doc:a");
    defer if (doc) |raw| alloc.free(raw);
    try std.testing.expect(doc != null);
}

test "db graph ttl source retirement ignores abandoned replay reservations without index workers" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-ttl-abandoned-replay-reservation");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const batch_ctx = db.batchContext();
    try std.testing.expect(!batch_ctx.executor.hasWorkers());
    const committed_tip = blk: {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        break :blk try db.core.store.lastReplaySequenceFromTxn(&txn, 0);
    };
    try std.testing.expect(committed_tip > 0);
    try std.testing.expectEqual(committed_tip, try loadManagedAppliedSequenceContext(alloc, db.core.index_manager, db.core.store, batch_ctx.applied_sequence_checkpoint_path, .{ .name = "relations_graph", .kind = .graph }));
    // A mutation can reserve its sequence and fail before the primary commit.
    // With no workers, no executor can certify this nonexistent replay debt.
    const abandoned = db.core.store.reserveNextReplaySequence(1);
    try std.testing.expect(abandoned > committed_tip);
    try std.testing.expect(db.core.store.nextReplaySequence(1) - 1 > committed_tip);
    {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(committed_tip, try db.core.store.lastReplaySequenceFromTxn(&txn, 0));
    }
    const due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const due = try graph_edge_ttl_expiration.decode(due_rows[0].value);
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(due.deadline_ns + 1);
    try initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = manual_clock.clock() });
    try db.ttl_runtime.?.runOnce();
    try std.testing.expectEqual(@as(u64, 1), db.ttl_runtime.?.stats().expired_graph_sources);
    const due_after = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_after);
    try std.testing.expectEqual(@as(usize, 0), due_after.len);
    const tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, due.edge_key, due.index_name, due.generation, due.state_key);
    defer alloc.free(tombstone_key);
    const tombstone = try db.core.store.get(alloc, tombstone_key);
    defer alloc.free(tombstone);
    try std.testing.expectEqual(due.deadline_ns, (try graph_edge_ttl_tombstone.Tombstone.decode(tombstone)).deadline_ns);
    const doc = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedDocument;
    defer alloc.free(doc);
    // A later committed revision crosses the sequence gap. Replaying the same
    // source content must still honor retirement rather than renew its lifetime.
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2},"note":1}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db graph ttl shared worker conditionally expires direct edge artifact" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-direct-ttl-worker");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.addIndex(.{ .name = "direct_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"keep","_edges":{"direct_graph":{"links":[{"target":"doc:b","weight":2}]}}}
    }}, .sync_level = .full_index });
    const due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 1), due_rows.len);
    const due = (try graph_edge_ttl_expiration.decodeDue(due_rows[0].value)).direct;
    try std.testing.expectEqual(due.deadline_ns, try graph_edge_ttl_expiration.deadlineFromKey(due_rows[0].key));
    var scanned_rows: usize = 0;
    var expired_read = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = .{
            .query_type = .neighbors,
            .index_name = "direct_graph",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .direction = .out, .edge_types = &.{"links"} },
        } }},
        .graph_ttl_now_ns = due.deadline_ns + 1,
        .graph_physical_scan_observation = &scanned_rows,
    });
    defer expired_read.deinit();
    try std.testing.expectEqual(@as(usize, 0), expired_read.graph_results[0].nodes.len);
    // The canonical row and its private direct contributor both count.
    try std.testing.expectEqual(@as(usize, 2), scanned_rows);
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(due.deadline_ns + 1);
    try initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = manual_clock.clock() });
    try db.ttl_runtime.?.runOnce();
    try std.testing.expectEqual(@as(u64, 1), db.ttl_runtime.?.stats().expired_graph_artifacts);
    const due_after = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_after);
    try std.testing.expectEqual(@as(usize, 0), due_after.len);
    try db.core.store.put(due_rows[0].key, due_rows[0].value);
    try db.ttl_runtime.?.runOnce();
    const pruned = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, pruned);
    try std.testing.expectEqual(@as(usize, 0), pruned.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, due.artifact_key));
    const edges = try db.getEdges(alloc, "direct_graph", "doc:a", "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
    const doc = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedDocument;
    defer alloc.free(doc);
}

test "db graph ttl direct contribution yields to surviving asset source" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-mixed-source-ttl");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{}" }}, .sync_level = .full_index });
    try db.batch(.{ .graph_writes = &.{.{
        .index_name = "relations_graph",
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "mentions",
        .weight = 3,
    }}, .sync_level = .full_index });
    const direct_due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, direct_due_rows);
    try std.testing.expectEqual(@as(usize, 1), direct_due_rows.len);
    const direct_due = (try graph_edge_ttl_expiration.decodeDue(direct_due_rows[0].value)).source;
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    db.close();
    db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    const before = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, before);
    try std.testing.expectEqual(@as(usize, 1), before.len);
    try std.testing.expectEqual(@as(f64, 3), before[0].weight);
    const due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_rows);
    try std.testing.expectEqual(@as(usize, 2), due_rows.len);
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(direct_due.deadline_ns + 1);
    try initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = manual_clock.clock() });
    try db.ttl_runtime.?.runOnce();
    const after = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, after);
    try std.testing.expectEqual(@as(usize, 1), after.len);
    try std.testing.expectEqual(@as(f64, 2), after[0].weight);
    try db.batch(.{ .graph_writes = &.{.{
        .index_name = "relations_graph",
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "mentions",
        .weight = 3,
    }}, .sync_level = .full_index });
    const revived = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, revived);
    try std.testing.expectEqual(@as(usize, 1), revived.len);
    try std.testing.expectEqual(@as(f64, 3), revived[0].weight);
    try db.batch(.{ .graph_deletes = &.{.{
        .index_name = "relations_graph",
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "mentions",
    }}, .sync_level = .full_index });
    const restored = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, restored);
    // Expiration withdrew only the direct source above. The later explicit
    // relationship deletion also suppresses the retained asset assertion.
    try std.testing.expectEqual(@as(usize, 0), restored.len);
    const generation = db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation;
    const edge_key = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b", "doc:a");
    defer alloc.free(edge_key);
    const direct_state_key = try internal_keys.graphDirectStateKeyAlloc(alloc, "doc:a", "relations_graph");
    defer alloc.free(direct_state_key);
    const direct_lifetime_key = try internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, edge_key, "relations_graph", generation, direct_state_key);
    defer alloc.free(direct_lifetime_key);
    const direct_tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, "relations_graph", generation, direct_state_key);
    defer alloc.free(direct_tombstone_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, direct_lifetime_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, direct_tombstone_key));
}

test "db graph ttl direct cleanup cannot delete a replaced artifact" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-direct-ttl-replacement");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.addIndex(.{ .name = "direct_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","_edges":{"direct_graph":{"links":[{"target":"doc:b","weight":2}]}}}
    }}, .sync_level = .full_index });
    const old_due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, old_due_rows);
    try std.testing.expectEqual(@as(usize, 1), old_due_rows.len);
    const old_due = (try graph_edge_ttl_expiration.decodeDue(old_due_rows[0].value)).direct;
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"title":"owner","_edges":{"direct_graph":{"links":[{"target":"doc:b","weight":3}]}}}
    }}, .sync_level = .full_index });
    const due_after_replacement = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_after_replacement);
    try std.testing.expectEqual(@as(usize, 1), due_after_replacement.len);
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(old_due.deadline_ns + 1);
    var ttl_ctx = TtlCleanupContext{ .batch = db.batchContext(), .grace_period_ns = 0, .clock = manual_clock.clock() };
    try std.testing.expect(!(try expireDirectGraphTtlCandidateContext(&ttl_ctx, old_due)));
    try pruneStaleDirectGraphTtlCandidateContext(&ttl_ctx, old_due);
    const current_due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, current_due);
    try std.testing.expectEqual(@as(usize, 1), current_due.len);
    const current = (try graph_edge_ttl_expiration.decodeDue(current_due[0].value)).direct;
    try std.testing.expect(!std.mem.eql(u8, &old_due.artifact_digest, &current.artifact_digest));
    const artifact = try db.core.store.get(alloc, current.artifact_key);
    defer alloc.free(artifact);
    var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, artifact);
    defer edge.deinit(alloc);
    try std.testing.expectEqual(@as(f64, 3), edge.weight);
}

test "db graph ttl migration cold writer fences legacy expiry without changing readonly files" {
    const alloc = std.testing.allocator;
    const resolver_catalog = resolver_catalog_mod;
    const migration = @import("maintenance/graph_ttl_migration.zig");
    var path_tmp = try TestDirectory.init("db-graph-ttl-legacy-namespace");
    defer path_tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const resolution = try internal_keys.resolutionArtifactKeyAlloc(scratch, "doc:a", "resolver");
    var promotion: std.ArrayList(u8) = .empty;
    try promotion.appendSlice(scratch, &internal_keys.promoted_keys_state_prefix);
    try internal_keys.appendEncodedComponent(&promotion, scratch, resolution);
    var deadline: u64 = 0;
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"_edges\":{\"g\":{\"links\":[{\"target\":\"doc:b\"}]}}}" }}, .sync_level = .full_index });
        const rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, rows);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        deadline = try graph_edge_ttl_expiration.deadlineFromKey(rows[0].key);
        const legacy = try scratch.dupe(u8, rows[0].key);
        @memcpy(legacy[0..internal_keys.legacy_graph_edge_expiration_index_prefix.len], &internal_keys.legacy_graph_edge_expiration_index_prefix);
        // Reconstruct the old writer's durable representation, including the
        // mixed family and pre-migration reader fence. No worker is running.
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(legacy, rows[0].value);
        try txn.delete(rows[0].key);
        try txn.put(promotion.items, "promotion state");
        try txn.put(resolver_catalog.minimum_reader_key, "3");
        try txn.delete("\x00\x00__metadata__:resolvers");
        try txn.delete(migration.progress_key);
        try txn.commit();
    }
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .open_mode = .query_readonly, .start_optional_runtimes = false });
        defer db.close();
        const floor = try db.core.store.get(alloc, resolver_catalog.minimum_reader_key);
        defer alloc.free(floor);
        try std.testing.expectEqualStrings("3", floor);
        const rows = try db.core.store.scanPrefix(alloc, &internal_keys.legacy_graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, rows);
        try std.testing.expectEqual(@as(usize, 2), rows.len);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, migration.progress_key));
    }
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, resolver_catalog.readMinimumReaderVersion(&read, 3));
        }
        var clock = platform_clock.ManualClock{};
        clock.setRealtimeNs(deadline + 1);
        try initStoppedTtlRuntimeForTest(&db, .{ .enabled = true, .grace_period_ns = 0, .clock = clock.clock() });
        try db.ttl_runtime.?.runOnce();
        try std.testing.expectEqual(@as(u64, 1), db.ttl_runtime.?.stats().expired_graph_artifacts);
        try std.testing.expectEqual(@as(u64, 0), db.ttl_runtime.?.stats().error_count);
        const preserved = try db.core.store.get(alloc, promotion.items);
        defer alloc.free(preserved);
        try std.testing.expectEqualStrings("promotion state", preserved);
        const edges = try db.getEdges(alloc, "g", "doc:a", "links", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
    }
}

test "db graph ttl due index rebuild resumes after reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-ttl-due-rebuild");
    defer path_tmp.cleanup();
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
        try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
            \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
        });
        try db.addIndex(.{ .name = "direct_graph", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
            \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"},"weight":2}}
        }}, .sync_level = .enrichments });
        try db.batch(.{ .writes = &.{.{ .key = "doc:c", .value =
            \\{"title":"direct","_edges":{"direct_graph":{"links":[{"target":"doc:d"}]}}}
        }}, .sync_level = .full_index });
        try db.runUntilIdle();
        const due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, due_rows);
        try std.testing.expectEqual(@as(usize, 2), due_rows.len);
        for (due_rows) |row| try db.core.store.delete(row.key);
        try db.core.store.put(graph_edge_ttl_due_rebuild_marker, "1");
    }
    {
        var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
        defer db.close();
        const due_rows = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, due_rows);
        try std.testing.expectEqual(@as(usize, 2), due_rows.len);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, graph_edge_ttl_due_rebuild_marker));
        var saw_source = false;
        var saw_direct = false;
        for (due_rows) |row| switch (try graph_edge_ttl_expiration.decodeDue(row.value)) {
            .source => |due| {
                saw_source = true;
                try std.testing.expectEqual(due.deadline_ns, try graph_edge_ttl_expiration.deadlineFromKey(row.key));
            },
            .direct => |due| {
                saw_direct = true;
                try std.testing.expectEqual(due.deadline_ns, try graph_edge_ttl_expiration.deadlineFromKey(row.key));
            },
        };
        try std.testing.expect(saw_source and saw_direct);
    }
}

test "db graph index reloads on reopen for neighbor queries with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const primary_backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addIndex(test_index.gr_v1);

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
            },
        });

        try db.batch(.{
            .graph_writes = &.{
                .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
                .{ .index_name = "gr_v1", .source = "doc:b", .target = "doc:c", .edge_type = "links", .weight = 1.0 },
            },
        });
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .query = .{ .graph = .{
                .query_type = .neighbors,
                .index_name = "gr_v1",
                .start_nodes = .{ .keys = &.{"doc:a"} },
                .params = .{ .edge_types = &.{"links"} },
            } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }
}

test "db graph reverse rebuild resumes after interrupted reopen" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(test_index.gr_v1);

        var graph_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
        defer {
            for (graph_writes.items) |item| {
                alloc.free(@constCast(item.source));
                alloc.free(@constCast(item.target));
                alloc.free(@constCast(item.edge_type));
            }
            graph_writes.deinit(alloc);
        }

        for (0..1500) |i| {
            try graph_writes.append(alloc, .{
                .index_name = "gr_v1",
                .source = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .target = try alloc.dupe(u8, "doc:target"),
                .edge_type = try alloc.dupe(u8, "links"),
            });
        }

        try db.batch(.{
            .graph_writes = graph_writes.items,
            .sync_level = .full_index,
        });
    }

    const reverse_path = try std.fmt.allocPrint(alloc, "{s}/indexes/gr_v1/reverse", .{std.mem.span(path)});
    defer alloc.free(reverse_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), reverse_path) catch |err| switch (err) {
        error.NotDir => try std.Io.Dir.cwd().deleteFile(io_impl.io(), reverse_path),
        else => return err,
    };
    var reverse_dir = std.Io.Dir.cwd().openDir(io_impl.io(), reverse_path, .{}) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (reverse_dir) |*dir| {
        dir.close(io_impl.io());
        return error.TestUnexpectedResult;
    }

    graph_mod.test_abort_reverse_rebuild_after_batches = 1;
    defer graph_mod.test_abort_reverse_rebuild_after_batches = null;
    var state_path: ?[]u8 = null;
    defer if (state_path) |owned| alloc.free(owned);
    {
        // A per-index load failure no longer fails the whole open; the index
        // is quarantined with its error recorded and retried on next open.
        var interrupted = try DB.open(alloc, std.mem.span(path), .{});
        defer interrupted.close();
        const recorded = interrupted.core.index_manager.loadFailure("gr_v1") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("TestInjectedBackfillFailure", recorded);

        const cfg = interrupted.core.index_manager.get("gr_v1") orelse return error.IndexNotFound;
        const rebuild_root = try interrupted.denseIndexRebuildStatePathAlloc(alloc, cfg.name);
        defer alloc.free(rebuild_root);
        const rebuild_state = interrupted.core.index_manager.rebuildState(.graph, rebuild_root, cfg.*);
        state_path = try rebuild_state.pathAlloc(alloc);
    }

    const interrupted_state = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path.?, alloc, .limited(1024));
    defer alloc.free(interrupted_state);
    try std.testing.expect(interrupted_state.len > 0);

    graph_mod.test_abort_reverse_rebuild_after_batches = null;

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path.?, alloc, .limited(1024)));

    const incoming = try reopened.getEdges(alloc, "gr_v1", "doc:target", "links", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, incoming);
    try std.testing.expectEqual(@as(usize, 1500), incoming.len);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    for (stats.indexes) |entry| {
        if (std.mem.eql(u8, entry.name, "gr_v1")) {
            try std.testing.expectEqual(false, entry.backfill_active);
            try std.testing.expectEqual(@as(u64, 1500), entry.edge_count);
            return;
        }
    }
    return error.TestExpectedEqual;
}

test "db graph push and pull transforms update projected edges across restart" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "graph",
            .kind = .graph,
            .config_json = "{}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "a", .value = "{\"title\":\"A\",\"_edges\":{\"graph\":{\"FRIEND\":[{\"target\":\"b\",\"weight\":2,\"metadata\":{\"since\":2024}}]}}}" },
                .{ .key = "b", .value = "{\"title\":\"B\"}" },
                .{ .key = "c", .value = "{\"title\":\"C\"}" },
            },
            .sync_level = .full_index,
        });

        try db.batch(.{
            .transforms = &.{.{
                .key = "a",
                .operations = &.{
                    .{ .op = .push, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"c\",\"weight\":3}" },
                    .{ .op = .set, .path = "title", .value_json = "\"A updated\"" },
                },
            }},
            .sync_level = .full_index,
        });

        const stored = (try db.get(alloc, "a")) orelse return error.TestExpectedEqual;
        defer alloc.free(stored);
        try std.testing.expect(std.mem.indexOf(u8, stored, "A updated") != null);

        const edges = try db.getEdges(alloc, "graph", "a", "FRIEND", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 2), edges.len);
        var saw_b = false;
        var saw_c = false;
        for (edges) |edge| {
            if (std.mem.eql(u8, edge.target, "b")) {
                saw_b = true;
                try std.testing.expectEqual(@as(f64, 2), edge.weight);
                try std.testing.expect(std.mem.indexOf(u8, edge.metadata, "2024") != null);
            }
            if (std.mem.eql(u8, edge.target, "c")) {
                saw_c = true;
                try std.testing.expectEqual(@as(f64, 3), edge.weight);
            }
        }
        try std.testing.expect(saw_b and saw_c);

        const c_artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "a", "graph", "FRIEND", "c");
        defer alloc.free(c_artifact_key);
        const c_artifact_before_pull = try db.core.store.get(alloc, c_artifact_key);
        alloc.free(c_artifact_before_pull);

        try db.batch(.{
            .transforms = &.{.{
                .key = "a",
                .operations = &.{.{
                    .op = .pull,
                    .path = "$._edges.graph.FRIEND",
                    .value_json = "{\"target\":\"b\"}",
                }},
            }},
            .sync_level = .full_index,
        });
        const c_artifact_after_pull = try db.core.store.get(alloc, c_artifact_key);
        alloc.free(c_artifact_after_pull);
        const after_pull = try db.getEdges(alloc, "graph", "a", "FRIEND", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, after_pull);
        try std.testing.expectEqual(@as(usize, 1), after_pull.len);
        try std.testing.expectEqualStrings("c", after_pull[0].target);

        try db.batch(.{
            .transforms = &.{.{
                .key = "a",
                .operations = &.{
                    .{ .op = .add_to_set, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\"}" },
                    .{ .op = .pull, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\"}" },
                    .{ .op = .pull, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"c\"}" },
                    .{ .op = .add_to_set, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"c\",\"weight\":4}" },
                },
            }},
            .sync_level = .full_index,
        });
        const ordered = try db.getEdges(alloc, "graph", "a", "FRIEND", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, ordered);
        try std.testing.expectEqual(@as(usize, 1), ordered.len);
        try std.testing.expectEqualStrings("c", ordered[0].target);
        try std.testing.expectEqual(@as(f64, 4), ordered[0].weight);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const reopened_edges = try reopened.getEdges(alloc, "graph", "a", "FRIEND", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, reopened_edges);
    try std.testing.expectEqual(@as(usize, 1), reopened_edges.len);
    try std.testing.expectEqualStrings("c", reopened_edges[0].target);
    try std.testing.expectEqual(@as(f64, 4), reopened_edges[0].weight);
}

test "db graph transforms fail closed for replacement operations" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(.{ .name = "graph", .kind = .graph, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "a", .value = "{\"title\":\"A\",\"_edges\":{\"graph\":{\"FRIEND\":[{\"target\":\"b\"}]}}}" }},
        .sync_level = .full_index,
    });

    try std.testing.expectError(error.UnsupportedTransformOperation, db.batch(.{
        .transforms = &.{.{
            .key = "a",
            .operations = &.{.{ .op = .set, .path = "$._edges.graph.FRIEND", .value_json = "[]" }},
        }},
        .sync_level = .full_index,
    }));

    // Validation is state-independent: an invalid projected edge cannot be
    // acknowledged as a no-op just because its source document is absent.
    try std.testing.expectError(error.InvalidGraphEdges, db.batch(.{
        .transforms = &.{.{
            .key = "missing",
            .operations = &.{.{ .op = .push, .path = "$._edges.graph.FRIEND", .value_json = "{\"weight\":2}" }},
        }},
        .sync_level = .full_index,
    }));

    const edges = try db.getEdges(alloc, "graph", "a", "FRIEND", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("b", edges[0].target);
}

test "db graph ownership restore cursor resumes one artifact index exactly" {
    const alloc = std.testing.allocator;
    const encoded = try encodeGraphOwnershipRestoreCursorAlloc(alloc, "\x01artifact:key", 37, 4096, 2, 3072, 65536, true);
    defer alloc.free(encoded);
    const decoded = (try decodeGraphOwnershipRestoreCursor(encoded)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("\x01artifact:key", decoded.artifact_key);
    try std.testing.expectEqual(@as(u32, 37), decoded.graph_index_offset);
    try std.testing.expectEqual(@as(u32, 4096), decoded.item_offset);
    try std.testing.expectEqual(@as(u32, 2), decoded.segment_index);
    try std.testing.expectEqual(@as(u32, 3072), decoded.key_count);
    try std.testing.expectEqual(@as(u32, 65536), decoded.manifest_bytes);
    try std.testing.expect(decoded.rollback);
    try std.testing.expect(try decodeGraphOwnershipRestoreCursor("\x01legacy-scan-cursor") == null);
    try std.testing.expectError(error.InvalidRestoreState, decodeGraphOwnershipRestoreCursor("GRC2truncated"));
    try std.testing.expectError(error.InvalidRestoreState, decodeGraphOwnershipRestoreCursor("GRC3truncated"));
    try std.testing.expectError(error.InvalidRestoreState, decodeGraphOwnershipRestoreCursor("GRC4truncated"));
    try std.testing.expectError(error.InvalidRestoreState, decodeGraphOwnershipRestoreCursor("GRC1"));
}

test "db graph ownership restore materializes large artifacts in published segments" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    const t = try TestDb.open(alloc, .{ .start_index_workers = false });
    defer t.deinit();
    const db = &t.db;
    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
        ,
    });

    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(alloc);
    try raw.appendSlice(alloc, "{\"relations\":[");
    for (0..2050) |i| {
        if (i > 0) try raw.append(alloc, ',');
        const item = try std.fmt.allocPrint(alloc, "{{\"type\":\"mentions\",\"target\":\"doc:{d}\"}}", .{i});
        defer alloc.free(item);
        try raw.appendSlice(alloc, item);
    }
    try raw.appendSlice(alloc, "]}");
    const artifact_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, raw.items);

    const first = try materializeGraphSourceArtifactRestorePage(
        db,
        alloc,
        artifact_key,
        "relations_graph",
        0,
        0,
        0,
        0,
    );
    try std.testing.expectEqual(@as(?usize, 2048), first.next_item_offset);
    try std.testing.expectEqual(@as(usize, 1), first.next_segment_index);
    try std.testing.expectEqual(@as(usize, 2048), first.key_count);

    // A failed later page rolls back one committed segment atomically. With a
    // single segment there is no continuation, and neither the visible edge
    // nor its ownership manifest may survive.
    var partial_ref = (try decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse return error.TestUnexpectedResult;
    defer partial_ref.deinit(alloc);
    try std.testing.expect(!try rollbackGraphSourceArtifactRestoreStatePage(db, alloc, "relations_graph", partial_ref));
    const partial_state_name = try graphArtifactStateNameAlloc(alloc, partial_ref);
    defer alloc.free(partial_state_name);
    const partial_state_key = try graphAssetStateKeyAlloc(alloc, "doc:a", "relations_graph", partial_state_name);
    defer alloc.free(partial_state_key);
    const partial_segment_key = try internal_keys.graphAssetStateSegmentKeyAlloc(alloc, partial_state_key, 0);
    defer alloc.free(partial_segment_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, partial_segment_key));
    const partial_edge_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:0");
    defer alloc.free(partial_edge_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, partial_edge_key));

    const restarted_first = try materializeGraphSourceArtifactRestorePage(
        db,
        alloc,
        artifact_key,
        "relations_graph",
        0,
        0,
        0,
        0,
    );

    const second = try materializeGraphSourceArtifactRestorePage(
        db,
        alloc,
        artifact_key,
        "relations_graph",
        restarted_first.next_item_offset.?,
        restarted_first.next_segment_index,
        restarted_first.key_count,
        restarted_first.manifest_bytes,
    );
    try std.testing.expectEqual(@as(?usize, null), second.next_item_offset);
    try std.testing.expectEqual(@as(usize, 2), second.next_segment_index);
    try std.testing.expectEqual(@as(usize, 2050), second.key_count);
    try std.testing.expectEqual(@as(usize, 1), db.graph_restore_parse_count_for_test);

    var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse return error.TestUnexpectedResult;
    defer artifact_ref.deinit(alloc);
    const state_name = try graphArtifactStateNameAlloc(alloc, artifact_ref);
    defer alloc.free(state_name);
    const state_key = try graphAssetStateKeyAlloc(alloc, "doc:a", "relations_graph", state_name);
    defer alloc.free(state_key);
    const root_raw = try db.core.store.get(alloc, state_key);
    defer alloc.free(root_raw);
    try std.testing.expectEqual(graph_asset_state.Format.v5, try graph_asset_state.format(root_raw));
    const restored_keys = (try loadGraphAssetStateKeysAlloc(alloc, db.core.store, state_key, db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation)).?;
    defer freeOwnedConstKeySlice(alloc, restored_keys);
    try std.testing.expectEqual(@as(usize, 2050), restored_keys.len);

    try db.core.store.put(artifact_key, "{\"relations\":[{\"type\":\"mentions\",\"target\":\"doc:replacement\"}]}");
    const changed = try materializeGraphSourceArtifactsForIndex(
        alloc,
        db.core.store,
        db.core.index_manager,
        &.{artifact_key},
        "relations_graph",
        .{},
    );
    defer freeOwnedKeySlice(alloc, changed);
    const first_segment_key = try internal_keys.graphAssetStateSegmentKeyAlloc(alloc, state_key, 0);
    defer alloc.free(first_segment_key);
    const second_segment_key = try internal_keys.graphAssetStateSegmentKeyAlloc(alloc, state_key, 1);
    defer alloc.free(second_segment_key);
    const replacement_segment = try db.core.store.get(alloc, first_segment_key);
    defer alloc.free(replacement_segment);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, second_segment_key));
    const replacement_root = try db.core.store.get(alloc, state_key);
    defer alloc.free(replacement_root);
    try std.testing.expectEqual(graph_asset_state.Format.v5, try graph_asset_state.format(replacement_root));
    const replacement_keys = (try loadGraphAssetStateKeysAlloc(
        alloc,
        db.core.store,
        state_key,
        db.core.index_manager.graphIndex("relations_graph").?.config.coverage_generation,
    )).?;
    defer freeOwnedConstKeySlice(alloc, replacement_keys);
    try std.testing.expectEqual(@as(usize, 1), replacement_keys.len);
}

test "db graph fact projections survive logical snapshot restore and reopen" {
    const alloc = std.testing.allocator;
    var source_dir = try TestDirectory.init("fact-snapshot-source");
    defer source_dir.cleanup();
    var dest_dir = try TestDirectory.init("fact-snapshot-dest");
    defer dest_dir.cleanup();
    const primary: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/facts", .{source_dir.path()});
    defer alloc.free(snapshot_root);
    defer {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
    }
    {
        var db = try DB.open(alloc, std.mem.span(source_dir.path().ptr), .{ .primary_backend = primary });
        defer db.close();
        try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
        try db.batch(.{
            .writes = &.{ .{ .key = "fact:1", .value = "{\"fact\":\"Alice works at Acme\"}" }, .{ .key = "fact:2", .value = "{\"fact\":\"Alice founded Acme\"}" } },
            .graph_writes = &.{
                .{ .index_name = "facts", .source = "alice", .target = "acme", .edge_type = "RELATES_TO", .edge_id = "fact:1", .owner_document = "fact:1" },
                .{ .index_name = "facts", .source = "alice", .target = "acme", .edge_type = "RELATES_TO", .edge_id = "fact:2", .owner_document = "fact:2" },
                .{ .index_name = "facts", .source = "fact:2", .target = "acme", .edge_type = "RELATES_TO", .edge_id = "source-owned", .owner_document = "fact:2" },
            },
            .sync_level = .enrichments,
        });
        _ = try db.snapshot("facts");
    }
    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(dest_dir.path().ptr), .{ .primary_backend = primary });
    for (0..2) |iteration| {
        var db = try DB.open(alloc, std.mem.span(dest_dir.path().ptr), .{ .primary_backend = primary });
        defer db.close();
        const edges = try db.getEdges(alloc, "facts", "alice", "RELATES_TO", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, if (iteration == 0) 2 else 1), edges.len);
        for (edges) |edge| try std.testing.expectEqualStrings(edge.edge_id, edge.owner_document);
        const source_owned = try db.getEdges(alloc, "facts", "fact:2", "RELATES_TO", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, source_owned);
        try std.testing.expectEqual(@as(usize, if (iteration == 0) 1 else 0), source_owned.len);
        if (iteration == 0) {
            try std.testing.expectEqualStrings("", source_owned[0].owner_document);
            try db.batch(.{
                .graph_deletes = &.{.{ .index_name = "facts", .source = "fact:2", .target = "acme", .edge_type = "RELATES_TO", .edge_id = "source-owned", .owner_document = "fact:2" }},
                .deletes = &.{"fact:1"},
                .sync_level = .full_index,
            });
            try db.runUntilIdle();
        } else try std.testing.expectEqualStrings("fact:2", edges[0].edge_id);
    }
}

test "db graph stale generation cleanup retires the exact fact identity" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("fact-generation");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{ .{ .key = "a", .value = "{}" }, .{ .key = "fact:one", .value = "{}" } },
        .graph_writes = &.{
            .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R" },
            .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "one", .owner_document = "fact:one" },
        },
        .sync_level = .full_index,
    });
    const key = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "fact:one", "facts", "R", "b", "a", "one");
    defer alloc.free(key);
    const generation = db.core.index_manager.graphIndex("facts").?.config.coverage_generation;
    const FailureCase = struct {
        fn run(allocator: Allocator, store: *docstore_mod.DocStore, artifact_key: []const u8, expected: u64) !void {
            var current = try collectGraphMutationsForArtifacts(allocator, store, &.{artifact_key}, "facts", .{ .expected_generation = expected });
            defer current.deinit();
            var stale_copy = try collectGraphMutationsForArtifacts(allocator, store, &.{artifact_key}, "facts", .{ .expected_generation = expected + 1 });
            defer stale_copy.deinit();
            try std.testing.expectEqual(@as(usize, 1), current.writes.len);
            try std.testing.expectEqual(@as(usize, 1), stale_copy.deletes.len);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, FailureCase.run, .{ db.core.store, key, generation });
    var stale = try collectGraphMutationsForArtifacts(alloc, db.core.store, &.{key}, "facts", .{ .expected_generation = generation + 1 });
    defer stale.deinit();
    try std.testing.expectEqual(@as(usize, 1), stale.deletes.len);
    try std.testing.expectEqualStrings("a", stale.deletes[0].source);
    try std.testing.expectEqualStrings("one", stale.deletes[0].edge_id);
    try std.testing.expectEqualStrings("fact:one", stale.deletes[0].owner_document);
    try db.core.index_manager.applyGraphMutationsByName("facts", stale.writes, stale.deletes);
    const remaining = try db.getEdges(alloc, "facts", "a", "R", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expectEqualStrings("", remaining[0].edge_id);
}

test "db graph endpoint deletion retires inline identities and preserves independent facts" {
    const alloc = std.testing.allocator;
    for ([_][]const []const u8{ &.{"a"}, &.{"b"}, &.{ "a", "b" } }) |deleted_endpoints| {
        const endpoint = deleted_endpoints[0];
        var directory = try TestDirectory.init("endpoint-identity-cleanup");
        defer directory.cleanup();
        var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
        defer db.close();
        try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
        try db.batch(.{
            .writes = &.{ .{ .key = "a", .value = "{}" }, .{ .key = "b", .value = "{}" }, .{ .key = "fact:1", .value = "{}" } },
            .graph_writes = &.{
                .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "LEGACY" },
                .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "inline:1" },
                .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "inline:2" },
                .{ .index_name = "facts", .source = endpoint, .target = endpoint, .edge_type = "R", .edge_id = "self" },
                .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "fact:1", .owner_document = "fact:1" },
            },
            .sync_level = .write,
        });
        if (std.mem.eql(u8, endpoint, "b")) {
            // Simulate a pre-directory store and leave graph projection pending:
            // cleanup must derive identities from primary data, never adjacency.
            const refs = try db.core.store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, refs);
            for (refs) |ref| try db.core.store.delete(ref.key);
            try db.core.store.delete(internal_keys.graph_incoming_ready_key);
        } else try db.runUntilIdle();
        if (std.mem.eql(u8, endpoint, "b")) {
            var ctx = db.batchContext();
            _ = try executeDeleteBatchContext(&ctx, deleted_endpoints, .full_index, null);
        } else try db.batch(.{ .deletes = deleted_endpoints, .sync_level = .full_index });
        try db.runUntilIdle();
        _ = try db.rebuildGraphDerivedState();
        const graph = &(db.core.index_manager.graphIndex("facts") orelse return error.IndexNotFound).index;
        for ([_]graph_mod.EdgeDirection{ .out, .in }) |direction| {
            const edges = try graph.getEdges(alloc, if (direction == .out) "a" else "b", "", direction);
            defer graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
            try std.testing.expectEqualStrings("fact:1", edges[0].edge_id);
            try std.testing.expectEqualStrings("fact:1", edges[0].owner_document);
        }
        var restored_directory = try TestDirectory.init("endpoint-identity-restore");
        defer restored_directory.cleanup();
        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/facts", .{directory.path()});
        defer alloc.free(snapshot_root);
        defer {
            var io_impl = threadedIo();
            defer io_impl.deinit();
            std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
        }
        _ = try db.snapshot("facts");
        try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restored_directory.path().ptr), .{});
        for (0..2) |iteration| {
            var restored = try DB.open(alloc, std.mem.span(restored_directory.path().ptr), .{});
            defer restored.close();
            const restored_edges = try restored.getEdges(alloc, "facts", "a", "", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, restored_edges);
            try std.testing.expectEqual(@as(usize, if (iteration == 0) 1 else 0), restored_edges.len);
            if (iteration == 0) {
                try std.testing.expectEqualStrings("fact:1", restored_edges[0].edge_id);
                try std.testing.expectEqualStrings("fact:1", restored_edges[0].owner_document);
                try restored.batch(.{ .deletes = &.{"fact:1"}, .sync_level = .full_index });
                try restored.runUntilIdle();
            }
        }
        try db.batch(.{ .deletes = &.{"fact:1"}, .sync_level = .full_index });
        try db.runUntilIdle();
        const edges = try graph.getEdges(alloc, "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
    }
}

test "db graph projected endpoint retirement survives restore" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("projected-retirement");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"}}]}}" },
        .{ .key = "b", .value = "{}" },
    }, .sync_level = .full_index });
    try db.runUntilIdle();
    {
        const edges = try db.getEdges(alloc, "facts", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }
    try db.batch(.{ .deletes = &.{"b"}, .sync_level = .full_index });
    try db.runUntilIdle();
    {
        const edges = try db.getEdges(alloc, "facts", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
    }
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"}}]}}" }}, .sync_level = .full_index });
    try db.runUntilIdle();
    var restored_directory = try TestDirectory.init("projected-retirement-restore");
    defer restored_directory.cleanup();
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/facts", .{directory.path()});
    defer alloc.free(snapshot_root);
    defer {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
    }
    _ = try db.snapshot("facts");
    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restored_directory.path().ptr), .{});
    for (0..3) |iteration| {
        var restored = try DB.open(alloc, std.mem.span(restored_directory.path().ptr), .{});
        defer restored.close();
        const edges = try restored.getEdges(alloc, "facts", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, if (iteration == 1) 1 else 0), edges.len);
        if (iteration == 0) {
            // An intentional new relationship write revives exactly this identity.
            try restored.batch(.{ .graph_writes = &.{.{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
            try restored.runUntilIdle();
        } else if (iteration == 1) {
            var ctx = restored.batchContext();
            _ = try executeDeleteBatchContext(&ctx, &.{"b"}, .full_index, null);
            try restored.runUntilIdle();
        }
    }
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"},\"weight\":2}]}}" }}, .sync_level = .full_index });
    try db.runUntilIdle();
    const renewed = try db.getEdges(alloc, "facts", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, renewed);
    try std.testing.expectEqual(@as(usize, 1), renewed.len);
    try std.testing.expectEqual(@as(f64, 2), renewed[0].weight);
}

test "db graph projected endpoint retirement survives restore index recreation" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("review-retirement-index");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    const cfg = types.IndexConfig{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    };
    try db.addIndex(cfg);
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"}}]}}" },
        .{ .key = "b", .value = "{}" },
    }, .sync_level = .full_index });
    try db.runUntilIdle();
    try db.batch(.{ .deletes = &.{"b"}, .sync_level = .full_index });
    try db.runUntilIdle();
    const input_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "relations_v1");
    defer alloc.free(input_key);
    const input_value = try db.core.store.get(alloc, input_key);
    defer alloc.free(input_value);
    try std.testing.expect(try db.deleteIndex("facts"));
    try db.runUntilIdle();
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{}" }}, .sync_level = .full_index });
    try db.addIndex(cfg);
    try db.core.store.put(input_key, input_value);
    const changed = try materializeGraphSourceArtifactsForIndex(alloc, db.core.store, db.core.index_manager, &.{input_key}, "facts", .{});
    defer freeOwnedKeySlice(alloc, changed);
    try applyGraphArtifactMutationPages(db.async_context, "facts", &.{}, changed, 0);
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "facts", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
}

test "db graph projected endpoint retirement survives restore explicit deletion" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("review-retirement-explicit");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    const cfg = types.IndexConfig{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    };
    try db.addIndex(cfg);
    try db.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"}}]}}" },
        .{ .key = "b", .value = "{}" },
    }, .sync_level = .full_index });
    try db.runUntilIdle();
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    try db.runUntilIdle();
    {
        const deleted = try db.getEdges(alloc, "facts", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, deleted);
        try std.testing.expectEqual(@as(usize, 0), deleted.len);
    }
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/facts", .{directory.path()});
    defer alloc.free(snapshot_root);
    defer {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
    }
    _ = try db.snapshot("facts");
    var restored_directory = try TestDirectory.init("review-explicit-restore");
    defer restored_directory.cleanup();
    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restored_directory.path().ptr), .{});
    var restored = try DB.open(alloc, std.mem.span(restored_directory.path().ptr), .{});
    defer restored.close();
    const edges = try restored.getEdges(alloc, "facts", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db graph projected endpoint retirement survives restore independent fact deletion" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("fact-explicit-retirement");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$","nodes":{"source":"{{ _item.source }}","target":"{{ _item.target }}"},"edge":{"edge_id":"{{ _doc.key }}","type":"RELATES_TO"}},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    try db.batch(.{ .writes = &.{
        .{ .key = "fact:one", .value = "{\"relations\":[{\"source\":\"alice\",\"target\":\"acme\"}]}" },
        .{ .key = "fact:two", .value = "{\"relations\":[{\"source\":\"alice\",\"target\":\"acme\"}]}" },
    }, .sync_level = .full_index });
    try db.runUntilIdle();
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "facts", .source = "alice", .target = "acme", .edge_type = "RELATES_TO", .edge_id = "fact:one", .owner_document = "fact:one" }}, .sync_level = .full_index });
    try db.runUntilIdle();
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/facts", .{directory.path()});
    defer alloc.free(snapshot_root);
    defer {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
    }
    _ = try db.snapshot("facts");
    var restored_directory = try TestDirectory.init("fact-explicit-retirement-restored");
    defer restored_directory.cleanup();
    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(restored_directory.path().ptr), .{});
    var restored = try DB.open(alloc, std.mem.span(restored_directory.path().ptr), .{});
    defer restored.close();
    {
        const edges = try restored.getEdges(alloc, "facts", "alice", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqualStrings("fact:two", edges[0].edge_id);
    }
    try std.testing.expect(try restored.core.store.hasGraphRetirements());
    try restored.batch(.{ .graph_writes = &.{.{ .index_name = "facts", .source = "alice", .target = "acme", .edge_type = "RELATES_TO", .edge_id = "fact:one", .owner_document = "fact:one" }}, .sync_level = .full_index });
    try restored.runUntilIdle();
    try std.testing.expect(!try restored.core.store.hasGraphRetirements());
    const revived = try restored.getEdges(alloc, "facts", "alice", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, revived);
    try std.testing.expectEqual(@as(usize, 2), revived.len);
}

test "db graph projected endpoint retirement survives restore review merge export" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("retirement-merge-review");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{}" }}, .graph_writes = &.{.{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    const rows = try db.mergeArtifactsPage(alloc, .{ .start = "a", .end = "b" }, null);
    defer {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expect(internal_keys.isGraphRetirementKey(rows[0].key));
}

test "db graph projected endpoint retirement survives restore review merge validation" {
    const alloc = std.testing.allocator;
    const artifact = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "a", "facts", "R", "b");
    defer alloc.free(artifact);
    const retired = try internal_keys.graphRetirementKeyAlloc(alloc, artifact);
    defer alloc.free(retired);
    try types.validateMergeArtifacts(.{ .merge_replication = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = .{} }, .merge_artifacts = &.{.{ .key = retired, .value = "1" }} });
}

test "db graph projected endpoint retirement survives restore review physical split" {
    try expectPhysicalSplitRetirements(.{ .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } } });
}

test "db graph projected endpoint retirement survives restore merge receiver replay" {
    const alloc = std.testing.allocator;
    var donor_dir = try TestDirectory.init("retirement-donor");
    defer donor_dir.cleanup();
    var donor = try DB.open(alloc, std.mem.span(donor_dir.path().ptr), .{});
    defer donor.close();
    const config: types.IndexConfig = .{ .name = "facts", .kind = .graph, .config_json = "{}" };
    try donor.addIndex(config);
    const write: types.GraphEdgeWrite = .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R" };
    try donor.batch(.{ .writes = &.{.{ .key = "a", .value = "{}" }}, .graph_writes = &.{write}, .sync_level = .full_index });
    try donor.batch(.{ .graph_deletes = &.{.{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    const rows = try donor.mergeArtifactsPage(alloc, .{ .start = "", .end = "" }, null);
    defer {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
    for ([_]bool{ false, true }) |direct| {
        var receiver_dir = try TestDirectory.init("retirement-receiver");
        defer receiver_dir.cleanup();
        {
            var receiver = try DB.open(alloc, std.mem.span(receiver_dir.path().ptr), .{});
            defer receiver.close();
            try receiver.addIndex(config);
            try receiver.batch(.{ .writes = &.{.{ .key = "a", .value = "{}" }}, .graph_writes = &.{write}, .sync_level = .full_index });
            if (direct) {
                try receiver.importMergeRangeFromTransitionDonor(&donor, .{ .start = "", .end = "" });
            } else {
                try receiver.updateRange(.{ .start = "m", .end = "" });
                try receiver.batch(.{ .merge_checkpoint = .{ .kind = .accept, .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .receiver_base_start = "m", .receiver_base_end = "", .merged_start = "", .merged_end = "" } });
                const req: types.BatchRequest = .{ .merge_replication = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = receiver.core.identity_namespace }, .merge_artifacts = rows };
                try graphTestApplyOrdered(&receiver, req, .{ .term = 1, .index = 10 });
                try graphTestApplyOrdered(&receiver, req, .{ .term = 1, .index = 10 });
            }
            try receiver.runUntilIdle();
            const edges = try receiver.getEdges(alloc, "facts", "a", "", .out);
            defer graph_mod.GraphIndex.freeEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 0), edges.len);
        }
        var reopened = try DB.open(alloc, std.mem.span(receiver_dir.path().ptr), .{});
        defer reopened.close();
        try reopened.runUntilIdle();
        const edges = try reopened.getEdges(alloc, "facts", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
        try std.testing.expect(try reopened.core.store.hasGraphRetirements());
    }
}

test "db graph projected endpoint retirement survives restore local owner remote endpoints" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("fact-range-review");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
    try db.updateRange(.{ .start = "m", .end = "" });
    try db.batch(.{ .writes = &.{.{ .key = "z", .value = "{}" }}, .graph_writes = &.{.{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .owner_document = "z", .edge_id = "fact" }}, .sync_level = .full_index });
    const edges = try db.getEdges(alloc, "facts", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("z", edges[0].owner_document);
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .owner_document = "z", .edge_id = "fact" }}, .sync_level = .full_index });
    const deleted = try db.getEdges(alloc, "facts", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, deleted);
    try std.testing.expectEqual(@as(usize, 0), deleted.len);
}

test "db graph projected endpoint retirement survives restore remote owner local endpoints" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("fact-range-review");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
    try db.updateRange(.{ .start = "m", .end = "" });
    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{ .graph_writes = &.{.{ .index_name = "facts", .source = "y", .target = "z", .edge_type = "R", .owner_document = "a", .edge_id = "fact" }}, .sync_level = .full_index }));
}

test "db graph projected endpoint retirement survives restore rejects remote delete owner" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("fact-delete-range");
    defer directory.cleanup();
    var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
    try db.updateRange(.{ .start = "m", .end = "" });
    try std.testing.expectError(error.KeyOutOfRange, db.batch(.{ .graph_deletes = &.{.{ .index_name = "facts", .source = "y", .target = "z", .edge_type = "R", .owner_document = "a", .edge_id = "fact" }} }));
}

test "graph projection preserves numeric literals in direct materialization" {
    const alloc = std.testing.allocator;
    const writes = try graphWritesFromArtifactValueAlloc(alloc, "g", "fact", "{\"source\":\"a\",\"target\":\"b\",\"type\":\"R\",\"uuid\":\"id\",\"score\":1.0000000000000001}", .{
        .artifact_name = @constCast("facts"),
        .mapping = .{ .source_template = @constCast("{{ _item.source }}"), .edge_id_template = @constCast("{{ _item.uuid }}") },
    }, "application/json", null, null, 10);
    defer freeGraphWrites(alloc, writes);
    try std.testing.expectEqual(@as(usize, 1), writes.len);
    const filter = relationship_filter_mod.Filter{ .properties = &.{.{ .field = "/metadata/score", .op = .eq, .value_json = "1.0000000000000001" }} };
    const edge = graph_mod.Edge{ .source = writes[0].source, .target = writes[0].target, .edge_type = writes[0].edge_type, .weight = writes[0].weight, .metadata = writes[0].metadata_json, .created_at = 0, .updated_at = 0 };
    try std.testing.expect(try filter.matches(alloc, edge));
}

test "graph projection pages account for complete relationship identities" {
    const alloc = std.testing.allocator;
    const id = try alloc.alloc(u8, 10000);
    defer alloc.free(id);
    @memset(id, 'x');
    const raw = try std.fmt.allocPrint(alloc, "[{{\"source\":\"a\",\"target\":\"b\",\"type\":\"R\",\"uuid\":\"{s}1\"}},{{\"source\":\"a\",\"target\":\"b\",\"type\":\"R\",\"uuid\":\"{s}2\"}}]", .{ id, id });
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const page = try graphWritesFromArtifactParsedPageAlloc(alloc, "g", "fact", parsed.value, .{
        .artifact_name = @constCast("facts"),
        .mapping = .{ .source_template = @constCast("{{ _item.source }}"), .edge_id_template = @constCast("{{ _item.uuid }}"), .metadata_template_json = @constCast("{}") },
    }, "application/json", null, 0, 2048, 4096, 100);
    defer freeGraphWrites(alloc, page.writes);
    try std.testing.expectEqual(@as(usize, 1), page.writes.len);
    try std.testing.expectEqual(@as(?usize, 1), page.next_item_offset);
    const next = try graphWritesFromArtifactParsedPageAlloc(alloc, "g", "fact", parsed.value, .{
        .artifact_name = @constCast("facts"),
        .mapping = .{ .source_template = @constCast("{{ _item.source }}"), .edge_id_template = @constCast("{{ _item.uuid }}"), .metadata_template_json = @constCast("{}") },
    }, "application/json", null, page.next_item_offset.?, 2048, 4096, 100);
    defer freeGraphWrites(alloc, next.writes);
    try std.testing.expectEqual(@as(usize, 1), next.writes.len);
    try std.testing.expectEqual(@as(?usize, null), next.next_item_offset);
    try std.testing.expect(!std.mem.eql(u8, page.writes[0].edge_id, next.writes[0].edge_id));
    // Owner bytes alone must also force the page boundary.
    const owned = try graphWritesFromArtifactParsedPageAlloc(alloc, "g", id, parsed.value, .{
        .artifact_name = @constCast("facts"),
        .mapping = .{ .source_template = @constCast("{{ _item.source }}"), .edge_id_template = @constCast("{{ _item_index }}"), .metadata_template_json = @constCast("{}") },
    }, "application/json", null, 0, 2048, 4096, 100);
    defer freeGraphWrites(alloc, owned.writes);
    try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
    try std.testing.expectEqualStrings(id, owned.writes[0].owner_document);
    try std.testing.expectEqual(@as(?usize, 1), owned.next_item_offset);
}

test "graph projection preserves numeric literals through field artifacts repair snapshot and reopen" {
    const alloc = std.testing.allocator;
    var source_dir = try TestDirectory.init("numeric-projection-source");
    defer source_dir.cleanup();
    var dest_dir = try TestDirectory.init("numeric-projection-restore");
    defer dest_dir.cleanup();
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/facts", .{source_dir.path()});
    defer alloc.free(snapshot_root);
    defer {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
    }
    const Check = struct {
        fn edges(db: *DB, allocator: Allocator) !void {
            const found = try db.getEdges(allocator, "facts", "alice", "R", .out);
            defer graph_mod.GraphIndex.freeEdges(allocator, found);
            try std.testing.expectEqual(@as(usize, 1), found.len);
            try std.testing.expectEqualStrings("18446744073709551615", found[0].edge_id);
            try std.testing.expectEqual(@as(f64, 0.75), found[0].weight);
            const filter = relationship_filter_mod.Filter{ .properties = &.{
                .{ .field = "/metadata/score", .op = .eq, .value_json = "1.0000000000000001" },
                .{ .field = "/metadata/large", .op = .eq, .value_json = "18446744073709551615" },
            } };
            try std.testing.expect(try filter.matches(allocator, found[0]));
        }
    };
    {
        var db = try DB.open(alloc, std.mem.span(source_dir.path().ptr), .{});
        defer db.close();
        try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json =
            \\{"source":{"artifact":"relations_v1","path":"$","nodes":{"source":"{{ _item.source }}","target":"{{ _item.target }}"},"edge":{"edge_id":"{{ _doc.value.uuid }}","type":"R"},"context":{"doc_fields":["uuid"]}},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
        });
        try db.batch(.{ .writes = &.{.{ .key = "fact", .value =
            \\{"uuid":18446744073709551615,"relations":[{"source":"alice","target":"acme","weight":0.75,"score":1.0000000000000001,"large":18446744073709551615}]}
        }}, .sync_level = .enrichments });
        try db.runUntilIdle();
        try Check.edges(&db, alloc);
        _ = try db.rebuildGraphDerivedState();
        try Check.edges(&db, alloc);
        _ = try db.snapshot("facts");
    }
    try DB.restoreSnapshotTo(alloc, snapshot_root, std.mem.span(dest_dir.path().ptr), .{});
    for (0..2) |_| {
        var restored = try DB.open(alloc, std.mem.span(dest_dir.path().ptr), .{});
        defer restored.close();
        try Check.edges(&restored, alloc);
    }
}

test "db graph endpoint cleanup pages are replicated bounded and restartable" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var directory = try TestDirectory.init("endpoint-cleanup-pages");
    defer directory.cleanup();
    var follower_directory = try TestDirectory.init("endpoint-cleanup-follower");
    defer follower_directory.cleanup();
    var follower = try DB.open(alloc, std.mem.span(follower_directory.path().ptr), .{});
    defer follower.close();
    try follower.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    {
        var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(types.GraphEdgeWrite, 600);
        for (writes, 0..) |*write, i| write.* = .{ .index_name = "g", .source = try std.fmt.allocPrint(scratch, "source:{d:0>4}", .{i}), .target = "hub", .edge_type = "R" };
        try graphTestApplyOrdered(&db, .{ .graph_writes = writes, .sync_level = .full_index }, .{ .term = 1, .index = 1 });
        try graphTestApplyOrdered(&follower, .{ .graph_writes = writes, .sync_level = .write }, .{ .term = 1, .index = 1 });
        try graphTestApplyOrdered(&follower, .{ .deletes = &.{"hub"}, .sync_level = .write }, .{ .term = 1, .index = 2 });
        // Local directory readiness differs across replicas. The follower must
        // still apply the leader-selected identities, without replanning.
        try follower.core.store.invalidateGraphDirectories();
        try graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"}, .sync_level = .write }, .{ .term = 1, .index = 2 });
        try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
        var archive = std.ArrayListUnmanaged(u8).empty;
        defer archive.deinit(alloc);
        try std.testing.expectError(error.StorageBusy, portable_backup.exportPortable(alloc, db.core.store, &archive));
        var reply = (try db.lookup(alloc, "", .{ .relational_topology_json = "{\"mode\":\"graph_endpoint_cleanup\"}" })).?;
        defer reply.deinit(alloc);
        var plan = try std.json.parseFromSlice(types.GraphEndpointCleanupStatus, alloc, reply.json, .{});
        defer plan.deinit();
        try std.testing.expect(plan.value.pending);
        try std.testing.expectEqual(@as(usize, 256), plan.value.graph_deletes.len);
        try std.testing.expectEqual(@as(usize, 0), plan.value.deletes.len);
        try std.testing.expectError(error.StorageBusy, db.getEdges(alloc, "g", "source:0000", "", .out));
        try std.testing.expectError(error.IntegrityTopologyBusy, graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "new", .target = "hub", .edge_type = "R" }}, .sync_level = .write }, .{ .term = 1, .index = 3 }));
        var first_page = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
        defer first_page.deinit();
        try graphTestApplyOrdered(&db, first_page.request(), .{ .term = 1, .index = 3 });
        try graphTestApplyOrdered(&follower, first_page.request(), .{ .term = 1, .index = 3 });
        const remaining = try db.core.store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, remaining);
        try std.testing.expectEqual(@as(usize, 344), remaining.len);
        // An already-applied Raft entry cannot consume a second page.
        try graphTestApplyOrdered(&db, first_page.request(), .{ .term = 1, .index = 3 });
        const unchanged = try db.core.store.scanPrefix(alloc, internal_keys.graph_incoming_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, unchanged);
        try std.testing.expectEqual(remaining.len, unchanged.len);
    }
    {
        var db = try DB.open(alloc, std.mem.span(directory.path().ptr), .{});
        defer db.close();
        try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
        var second_page = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
        defer second_page.deinit();
        try graphTestApplyOrdered(&db, second_page.request(), .{ .term = 2, .index = 4 });
        try graphTestApplyOrdered(&follower, second_page.request(), .{ .term = 2, .index = 4 });
        try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
        var final_page = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
        defer final_page.deinit();
        try graphTestApplyOrdered(&db, final_page.request(), .{ .term = 2, .index = 5 });
        try graphTestApplyOrdered(&follower, final_page.request(), .{ .term = 2, .index = 5 });
        try std.testing.expect(!try follower.core.store.hasGraphEndpointCleanup());
        try follower.runUntilIdle();
        _ = try follower.rebuildGraphDerivedState();
        const follower_edges = try follower.getEdges(alloc, "g", "hub", "", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, follower_edges);
        try std.testing.expectEqual(@as(usize, 0), follower_edges.len);
        try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
        try db.runUntilIdle();
        _ = try db.rebuildGraphDerivedState();
        const edges = try db.getEdges(alloc, "g", "hub", "", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
        try graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "new", .target = "hub", .edge_type = "R" }}, .sync_level = .full_index }, .{ .term = 2, .index = 6 });
        // Raft apply deliberately returns before derived visibility; the
        // public writer waits for its requested sync level separately.
        try db.runUntilIdle();
        const revived = try db.getEdges(alloc, "g", "hub", "", .in);
        defer graph_mod.GraphIndex.freeEdges(alloc, revived);
        try std.testing.expectEqual(@as(usize, 1), revived.len);
    }
}

test "db graph endpoint cleanup pages finish on constrained tables" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("constrained-cleanup");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" }} }, .{ .term = 1, .index = 1 });
    // Seed the durable state produced by a coordinated document deletion.
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
    defer alloc.free(job);
    try db.core.store.put(job, "hub");
    var cleanup = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer cleanup.deinit();
    try std.testing.expectEqual(@as(usize, 1), cleanup.page.deletes.len);
    try graphTestApplyOrdered(&db, cleanup.request(), .{ .term = 1, .index = 2 });
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
    try std.testing.expectError(error.InvalidBatchRequest, graphTestApplyOrdered(&db, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .deletes = &.{"document"} }, .{ .term = 1, .index = 3 }));
    try std.testing.expectError(error.ForeignKeyCoordinationRequired, graphTestApplyOrdered(&db, .{ .deletes = &.{"document"} }, .{ .term = 1, .index = 3 }));
}

test "db graph endpoint cleanup pages progress after standalone ttl without another write" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("ttl-cleanup-worker");
    defer directory.cleanup();
    const db = try DB.openOwned(alloc, directory.path(), .{ .start_index_workers = false });
    defer db.closeOwned();
    try std.testing.expect(db.graph_cleanup_owner.registration != null);
    try db.setSchema(.{ .version = 1, .default_type = "_default", .ttl_duration_ns = std.time.ns_per_s });
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "hub", .value = "{}" }}, .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" }}, .timestamp_ns = currentTimeNs() - 2 * std.time.ns_per_s, .sync_level = .full_index });
    try initStoppedTtlRuntimeForTest(db, .{ .enabled = true, .grace_period_ns = 0 });
    try db.ttl_runtime.?.runOnce();
    try std.testing.expectEqual(@as(u64, 0), try db.getTimestamp(alloc, "hub"));
    const deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (try db.core.store.hasGraphEndpointCleanup()) {
        if (monotonicTimeNs() >= deadline) return error.CleanupWorkerDidNotProgress;
        sleepNs(std.time.ns_per_ms);
    }
    // With explicit worker suppression the foreground maintenance API remains
    // available; autonomous owners instead make progress without another write.
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db graph endpoint cleanup pages batch unrelated deletes without blocking graph reads" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("empty-cleanup-pages");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index }, .{ .term = 1, .index = 1 });
    try db.runUntilIdle();
    try db.core.store.ensureGraphIncomingDirectory();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const deletes = try scratch.alloc([]const u8, 600);
    for (deletes, 0..) |*key, i| key.* = try std.fmt.allocPrint(scratch, "isolated:{d}", .{i});
    try graphTestApplyOrdered(&db, .{ .deletes = deletes }, .{ .term = 1, .index = 2 });
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    try std.testing.expect(!try db.core.store.graphEndpointCleanupBlocksReads());
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    for ([_]usize{ 256, 256, 88 }, 0..) |size, i| {
        var page = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 0), page.graph_deletes.len);
        try std.testing.expectEqual(size, page.page.deletes.len);
        try graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = 3 + i });
    }
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
}

test "db graph endpoint cleanup pages respect raft ownership before first applied marker" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("raft-cleanup-authority");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .online_source_authority = .raft, .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
    defer alloc.free(job);
    try db.core.store.put(job, "hub");
    try std.testing.expect(!try db.runStandaloneGraphEndpointCleanupStep());
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
}

test "db graph endpoint cleanup pages full index completion covers every local write entry point" {
    const alloc = std.testing.allocator;
    const Capture = struct {
        calls: usize = 0,
        fn observe(ptr: *anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
        fn dispatch(_: *anyopaque, _: Allocator, _: DocumentArtifactChildRangeDispatch) !void {}
    };
    for (0..6) |mode| {
        var directory = try TestDirectory.init("completion-entry-points");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.batch(.{ .writes = &.{.{ .key = "hub", .value = "{}" }}, .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" }}, .timestamp_ns = 1, .sync_level = .full_index });
        const req: types.BatchRequest = .{ .deletes = &.{"hub"}, .sync_level = .full_index };
        var capture = Capture{};
        const dispatcher = DocumentArtifactChildRangeDispatcher{ .ptr = &capture, .select_destination = testChildRangeDestination, .apply = Capture.dispatch };
        switch (mode) {
            0 => try db.batchTransactionCompatible(req),
            1 => {
                var profile = BatchProfile{};
                try db.batchProfiled(req, &profile);
            },
            2 => try db.batchWithDocumentArtifactChildRangeDispatcher(req, dispatcher),
            3 => {
                try db.batchWithDocumentArtifactChildRangeDispatcherAndCommittedEffectsObserver(req, dispatcher, .{ .ptr = &capture, .apply = Capture.observe });
                // The observer sees both the primary delete and its cleanup.
                try std.testing.expect(capture.calls >= 2);
            },
            4 => {
                const txn = try db.beginTransaction(10);
                try db.writeTransaction(txn, .{ .deletes = req.deletes });
                try db.resolveTransactionIntentsWithSyncLevel(txn, .committed, 20, .full_index);
            },
            5 => try db.batchWithoutRangeValidation(req),
            else => unreachable,
        }
        try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
        const edges = try db.getEdges(alloc, "g", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
    }
}

test "db graph endpoint cleanup pages ordinary writes do not drain directory migration" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |graph_index| {
        var directory = try TestDirectory.init("write-migration-bounded");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        if (graph_index) try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        // Seed unrelated historical rows directly: the regression exercises
        // the next DB write, not extraction/replay for the fixture corpus.
        const writes = try scratch.alloc(docstore_mod.KVPair, 600);
        for (writes, 0..) |*write, i| {
            const key = try std.fmt.allocPrint(scratch, "doc:{d}", .{i});
            write.* = .{ .key = try encodeStoreLookupKeyAlloc(&db, scratch, key), .value = "{}" };
        }
        try db.core.store.putBatch(writes, &.{});
        // Existing pre-migration stores lack a ready marker.
        try db.core.store.delete(internal_keys.graph_incoming_ready_key);
        try db.batch(.{ .writes = &.{.{ .key = "new", .value = "{}" }}, .sync_level = .write });
        try std.testing.expect(!try db.core.store.graphIncomingDirectoryReady());
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, internal_keys.graph_incoming_cursor_key));
        if (graph_index) {
            try std.testing.expect(try db.runStandaloneGraphEndpointCleanupStep());
            try std.testing.expect(!try db.core.store.graphIncomingDirectoryReady());
            const cursor = try db.core.store.get(alloc, internal_keys.graph_incoming_cursor_key);
            alloc.free(cursor);
            // Another ordinary write cannot consume the remaining pages.
            try db.batch(.{ .writes = &.{.{ .key = "other", .value = "{}" }}, .sync_level = .write });
            try std.testing.expect(!try db.core.store.graphIncomingDirectoryReady());
            while (try db.runStandaloneGraphEndpointCleanupStep()) {}
            try std.testing.expect(try db.core.store.graphIncomingDirectoryReady());
        }
    }
}

test "db graph endpoint cleanup pages incomplete directory preserves owner retirement lifecycle" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("retirement-owner-fallback");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"v\":1}" }}, .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "a", "g", "R", "b", "a", "");
    defer alloc.free(artifact);
    try std.testing.expect(try db.core.store.graphRelationshipRetired(artifact));
    try db.core.store.delete(internal_keys.graph_incoming_ready_key);
    // A semantic no-op must retain the retirement even without a summary.
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"v\":1}" }}, .sync_level = .write });
    try std.testing.expect(try db.core.store.graphRelationshipRetired(artifact));
    // Only this owner's changed lifecycle removes its marker.
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"v\":2}" }}, .sync_level = .write });
    try std.testing.expect(!try db.core.store.graphRelationshipRetired(artifact));
    // Weak-sync completion may finish a small directory migration while the
    // bounded owner job remains pending. It does not enumerate owner history.
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    while (try db.runStandaloneGraphEndpointCleanupStep()) {}
    try std.testing.expect(try db.core.store.graphIncomingDirectoryReady());
}

test "db graph endpoint cleanup pages progress through row policy phases without user admission" {
    const alloc = std.testing.allocator;
    for ([_]table_catalog_mod.RowPolicyPhase{ .preparing, .active }) |phase| {
        for (0..3) |mode| {
            var directory = try TestDirectory.init("policy-cleanup-admission");
            defer directory.cleanup();
            var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
            defer db.close();
            try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            const edge: types.GraphEdgeWrite = .{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" };
            if (mode == 1)
                try graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 1 })
            else
                try db.batch(.{ .graph_writes = &.{edge}, .sync_level = .full_index });
            const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
            defer alloc.free(job);
            try db.core.store.put(job, "hub");
            db.local_execution.row_policy_gate.phase.store(@backingInt(phase), .release);
            defer db.local_execution.row_policy_gate.phase.store(@backingInt(table_catalog_mod.RowPolicyPhase.disabled), .release);
            // Maintenance must not acquire a user admission lease, including
            // during preparing. Invalid mixed commands never gain exemption.
            try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .graph_endpoint_cleanup = true, .writes = &.{.{ .key = "user", .value = "{}" }} }));
            try std.testing.expectError(error.RowPolicyAuthenticationRequired, db.batch(.{ .writes = &.{.{ .key = "user", .value = "{}" }} }));
            try std.testing.expectError(error.RowPolicyAuthenticationRequired, graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 2 }));
            switch (mode) {
                0 => try std.testing.expect(try db.runStandaloneGraphEndpointCleanupStep()),
                1 => {
                    var page = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
                    defer page.deinit();
                    try graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = 2 });
                },
                2 => try db.batchInternal(.{ .graph_endpoint_cleanup = true, .sync_level = .write }, null, .{ .replication_applied_lsn_marker = 1, .bypass_replication_write_gate = true }),
                else => unreachable,
            }
            try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
            // Authentication failures and cleanup cannot create a user row.
            try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "user"));
        }
    }
}

test "db graph endpoint cleanup pages weak sync has one page budget and full index drains" {
    const alloc = std.testing.allocator;
    const Capture = struct {
        calls: usize = 0,
        fn observe(ptr: *anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
        fn dispatch(_: *anyopaque, _: Allocator, _: DocumentArtifactChildRangeDispatch) !void {}
    };
    for ([_]types.SyncLevel{ .propose, .write, .full_text, .enrichments, .full_index }) |level| {
        var directory = try TestDirectory.init("cleanup-foreground-budget");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{}" }}, .sync_level = .write });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const jobs = try scratch.alloc(docstore_mod.KVPair, 600);
        for (jobs, 0..) |*job, i| {
            const endpoint = try std.fmt.allocPrint(scratch, "isolated:{d}", .{i});
            job.* = .{ .key = try internal_keys.graphEndpointCleanupKeyAlloc(scratch, endpoint), .value = endpoint };
        }
        try db.core.store.putBatch(jobs, &.{});
        var capture = Capture{};
        try db.batchWithDocumentArtifactChildRangeDispatcherAndCommittedEffectsObserver(.{ .writes = &.{.{ .key = "unrelated", .value = "{}" }}, .sync_level = level }, .{ .ptr = &capture, .select_destination = testChildRangeDestination, .apply = Capture.dispatch }, .{ .ptr = &capture, .apply = Capture.observe });
        const pending = try db.core.store.scanPrefix(alloc, internal_keys.graph_endpoint_cleanup_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, pending);
        if (level == .full_index) {
            try std.testing.expectEqual(@as(usize, 0), pending.len);
            try std.testing.expectEqual(@as(usize, 4), capture.calls);
        } else {
            try std.testing.expectEqual(@as(usize, 344), pending.len);
            try std.testing.expectEqual(@as(usize, 2), capture.calls);
            // Each scheduler turn remains bounded and eventually clears jobs.
            try std.testing.expect(try db.runStandaloneGraphEndpointCleanupStep());
            try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
            try std.testing.expect(try db.runStandaloneGraphEndpointCleanupStep());
            try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
        }
    }
}

test "db graph endpoint cleanup pages cancellation stops between commits" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("review-cleanup-cancel");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{}" }}, .sync_level = .write });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const jobs = try scratch.alloc(docstore_mod.KVPair, 257);
    for (jobs, 0..) |*job, i| {
        const endpoint = try std.fmt.allocPrint(scratch, "isolated:{d}", .{i});
        job.* = .{ .key = try internal_keys.graphEndpointCleanupKeyAlloc(scratch, endpoint), .value = endpoint };
    }
    try db.core.store.putBatch(jobs, &.{});
    const Capture = struct {
        calls: usize = 0,
        canceled: std.atomic.Value(bool) = .init(false),
        fn observe(ptr: *anyopaque, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 2) self.canceled.store(true, .release);
        }
    };
    var capture = Capture{};
    try std.testing.expectError(error.EnrichmentWaitCanceled, db.batchInternal(.{ .writes = &.{.{ .key = "unrelated", .value = "{}" }}, .sync_level = .full_index }, null, .{ .committed_batch_effects_observer = .{ .ptr = &capture, .apply = Capture.observe }, .visibility_cancellation = .fromAtomic(&capture.canceled) }));
    try std.testing.expectEqual(@as(usize, 2), capture.calls);
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    const committed_key = try encodeStoreLookupKeyAlloc(&db, alloc, "unrelated");
    defer alloc.free(committed_key);
    const committed = try db.core.store.get(alloc, committed_key);
    alloc.free(committed);
    // Cancellation preserves the primary commit and the unfinished durable job.
    try std.testing.expect(try db.runStandaloneGraphEndpointCleanupStep());
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
}

test "db graph endpoint cleanup pages stale plan preserves revived relationship" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("review-stale-graph-plan");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const write = types.GraphEdgeWrite{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R", .edge_id = "one" };
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{write}, .sync_level = .write }, .{ .term = 1, .index = 1 });
    try graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"}, .sync_level = .write }, .{ .term = 1, .index = 2 });
    var plan = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    try graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 3 });
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{write}, .sync_level = .write }, .{ .term = 1, .index = 4 });
    try db.runUntilIdle();
    {
        const edges = try db.getEdges(alloc, "g", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }
    // The same selected page arrives as a distinct later ordered command,
    // rather than redelivery of an already-applied Raft entry.
    try graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 5 });
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
}

test "db graph endpoint cleanup pages fence a later deletion of the same endpoint" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("cleanup-job-aba");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const edge = types.GraphEdgeWrite{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" };
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 1 });
    try graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 2 });
    var old = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer old.deinit();
    try graphTestApplyOrdered(&db, old.request(), .{ .term = 1, .index = 3 });
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{edge} }, .{ .term = 1, .index = 4 });
    try graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 5 });
    var fresh = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer fresh.deinit();
    try std.testing.expect(fresh.page.guards[0].generation > old.page.guards[0].generation);
    try graphTestApplyOrdered(&db, old.request(), .{ .term = 1, .index = 6 });
    try std.testing.expect(try db.core.store.hasGraphEndpointCleanup());
    var unchanged = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer unchanged.deinit();
    try std.testing.expectEqual(fresh.page.guards[0].generation, unchanged.page.guards[0].generation);
    try std.testing.expectEqual(@as(usize, 1), unchanged.graph_deletes.len);
    try graphTestApplyOrdered(&db, fresh.request(), .{ .term = 1, .index = 7 });
    try db.runUntilIdle();
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db graph endpoint cleanup pages filter stale and live jobs independently" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("cleanup-mixed-generations");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{
        .{ .index_name = "g", .source = "a", .target = "hub-a", .edge_type = "R", .edge_id = "one" },
        .{ .index_name = "g", .source = "b", .target = "hub-b", .edge_type = "R", .edge_id = "two" },
    } }, .{ .term = 1, .index = 1 });
    try graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub-a", "hub-b" } }, .{ .term = 1, .index = 2 });
    var plan = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.page.guards.len);
    try std.testing.expectEqual(@as(usize, 2), plan.request().graph_deletes.len);
    const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub-a");
    defer alloc.free(job);
    const guard = if (std.mem.eql(u8, plan.page.guards[0].endpoint, "hub-a")) plan.page.guards[0] else plan.page.guards[1];
    const edges = plan.request().graph_deletes;
    const edge = if (std.mem.eql(u8, edges[0].target, "hub-a")) edges[0] else edges[1];
    try graphTestApplyOrdered(&db, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .graph_endpoint_cleanup_guards = &.{guard}, .graph_deletes = &.{edge}, .deletes = &.{job} }, .{ .term = 1, .index = 3 });
    try graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub-a", .edge_type = "R", .edge_id = "one" }} }, .{ .term = 1, .index = 4 });
    try graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 5 });
    try std.testing.expect(try db.orderedMutationAlreadyApplied(.{ .term = 1, .index = 5 }));
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
    try db.runUntilIdle();
    const revived = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, revived);
    const retired = try db.getEdges(alloc, "g", "b", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, retired);
    try std.testing.expectEqual(@as(usize, 1), revived.len);
    try std.testing.expectEqual(@as(usize, 0), retired.len);
}

test "db graph endpoint cleanup pages transaction dependencies survive reopen" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("graph-prepared-endpoint");
    defer directory.cleanup();
    var txn_id: types.TxnId = undefined;
    {
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}" });
        txn_id = try db.beginTransaction(21_000);
        try db.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "b", .value = "{\"links\":[\"field-target\"],\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\"}]}}}" }} });
        try std.testing.expectError(error.IntentConflict, graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub", "field-target" } }, .{ .term = 1, .index = 1 }));
        try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
        try std.testing.expectError(error.SchemaInUse, db.deleteIndex("g"));
        try std.testing.expectError(error.SchemaInUse, db.addIndex(.{ .name = "another", .kind = .graph, .config_json = "{}" }));
    }
    {
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try std.testing.expectError(error.IntentConflict, graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub", "field-target" } }, .{ .term = 1, .index = 1 }));
        const deleting = try db.beginTransaction(22_000);
        try std.testing.expectError(error.IntentConflict, db.writeTransaction(deleting, .{ .deletes = &.{ "hub", "field-target" } }));
        try db.abortTransaction(deleting, 22_001);
        try db.commitTransaction(txn_id, 21_001);
        try graphTestApplyOrdered(&db, .{ .deletes = &.{ "hub", "field-target" } }, .{ .term = 1, .index = 1 });
        while (try db.prepareGraphEndpointCleanupBatch(alloc)) |selected| {
            var plan = selected;
            defer plan.deinit();
            try db.batch(plan.request());
        }
        try db.runUntilIdle();
        const edges = try db.getEdges(alloc, "g", "b", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 0), edges.len);
    }
}

test "db graph endpoint cleanup pages transaction admission rejects active cleanup and abort releases guards" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("graph-prepare-cleanup-admission");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\"}]}}}";
    const aborted = try db.beginTransaction(1);
    try db.writeIntents(aborted, &.{.{ .key = "a", .value = value }}, &.{});
    try std.testing.expectError(error.IntentConflict, graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 }));
    try db.abortTransaction(aborted, 2);
    try graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 });
    const rejected = try db.beginTransaction(3);
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeIntents(rejected, &.{.{ .key = "b", .value = value }}, &.{}));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeTransaction(rejected, .{ .writes = &.{.{ .key = "b", .value = value }} }));
    try db.abortTransaction(rejected, 4);
    var plan = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    try graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 2 });
    const accepted = try db.beginTransaction(5);
    try db.writeIntents(accepted, &.{.{ .key = "c", .value = value }}, &.{});
    try db.commitTransaction(accepted, 6);
}

test "db graph endpoint cleanup pages transaction dependencies cover relational fields" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("graph-relational-prepared-endpoint");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "links", .path = "links", .column_type = .json, .is_json = true, .json_kind = .any, .allows_null = true }} });
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}" });
    const txn_id = try db.beginTransaction(1);
    try db.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "a", .value = "{\"links\":[\"hub\"]}" }} });
    try std.testing.expectError(error.IntentConflict, graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 }));
    try db.commitTransaction(txn_id, 2);
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("hub", edges[0].target);
    try graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 });
}

test "db graph endpoint cleanup pages transaction dependencies retain stronger caller read guards" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("graph-caller-read-dependency");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "hub", .value = "{\"title\":\"hub\"}" }}, .timestamp_ns = 99 });
    const key = try internal_keys.documentKeyAlloc(alloc, "hub");
    defer alloc.free(key);
    const value = try db.core.store.get(alloc, key);
    defer alloc.free(value);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
    const txn_id = try db.beginTransaction(1);
    try db.writeIntents(txn_id, &.{}, &.{.{ .key = "hub", .expected_version = 99, .expected_content_digest = digest }});
    try db.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\"}]}}}" }} });
    try std.testing.expectError(error.IntentConflict, graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 }));
    try db.commitTransaction(txn_id, 2);
}

test "db graph endpoint cleanup pages rejected index deletion restores worker and replay" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("graph-delete-prepare-race");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try std.testing.expect(db.executor.appliedSequence("g") != null);
    const Race = struct {
        var txn_id: types.TxnId = undefined;
        fn prepare(owner: *DB) !void {
            txn_id = try owner.beginTransaction(1);
            try owner.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "prepared", .value = "{}" }} });
        }
    };
    DB.test_index_delete_after_worker_paused = Race.prepare;
    defer DB.test_index_delete_after_worker_paused = null;
    try std.testing.expectError(error.SchemaInUse, db.deleteIndex("g"));
    DB.test_index_delete_after_worker_paused = null;
    try std.testing.expect(db.core.index_manager.graphIndex("g") != null);
    try std.testing.expect(db.executor.appliedSequence("g") != null);
    try db.abortTransaction(Race.txn_id, 2);
    try db.batch(.{ .writes = &.{.{ .key = "source", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"target\"}]}}}" }}, .sync_level = .full_index });
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "source", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("target", edges[0].target);
}

test "db graph endpoint cleanup pages rejected deletion retains replay across another worker" {
    const alloc = std.testing.allocator;
    for ([_]derived_executor_mod.Backend{ .manual, .io_threaded }) |backend| {
        var directory = try TestDirectory.init("graph-delete-replay-retention");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .executor = .{ .backend = backend } });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.addIndex(.{ .name = "other", .kind = .graph, .config_json = "{}" });
        const Race = struct {
            var txn_id: types.TxnId = undefined;
            var manual: bool = false;
            fn writeAndPrepare(owner: *DB) !void {
                try owner.batch(.{ .writes = &.{.{ .key = "source", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"target\"}]}}}" }}, .sync_level = .write });
                const sequence = owner.core.nextDerivedSequence();
                try owner.runDerivedUntilTargets(sequence, &.{"other"});
                // A visibility wait must not report success for a paused index.
                const expected_error = if (manual) error.DerivedWorkerPaused else error.EnrichmentWaitTimeout;
                try std.testing.expectError(expected_error, owner.executor.waitForIndexesWithVisibilityWait(sequence, &.{"g"}, .{ .deadline_ns = 0 }));
                txn_id = try owner.beginTransaction(1);
                try owner.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "prepared", .value = "{}" }} });
            }
        };
        Race.manual = backend == .manual;
        DB.test_index_delete_after_worker_paused = Race.writeAndPrepare;
        defer DB.test_index_delete_after_worker_paused = null;
        try std.testing.expectError(error.SchemaInUse, db.deleteIndex("g"));
        DB.test_index_delete_after_worker_paused = null;
        try db.abortTransaction(Race.txn_id, 2);
        try db.runUntilIdle();
        const edges = try db.getEdges(alloc, "g", "source", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqualStrings("target", edges[0].target);
        try std.testing.expectEqual(db.core.nextDerivedSequence(), db.executor.appliedSequence("g").?);
        // Successful retirement must also dispose of the paused registration.
        try std.testing.expect(try db.deleteIndex("g"));
        try std.testing.expect(db.executor.appliedSequence("g") == null);
        try std.testing.expect(db.core.index_manager.graphIndex("g") == null);
    }
}

test "db graph endpoint cleanup pages rejected deletion resumes without rollback allocations" {
    const alloc = std.testing.allocator;
    for ([_]derived_executor_mod.Backend{ .manual, .io_threaded }) |backend| {
        var directory = try TestDirectory.init("graph-delete-allocation-free-rollback");
        defer directory.cleanup();
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .executor = .{ .backend = backend } });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.addIndex(.{ .name = "other", .kind = .graph, .config_json = "{}" });
        const Race = struct {
            var txn_id: types.TxnId = undefined;
            var failing: std.testing.FailingAllocator = undefined;
            fn prepare(owner: *DB) !void {
                txn_id = try owner.beginTransaction(1);
                try owner.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "prepared", .value = "{}" }} });
                failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
                owner.alloc = failing.allocator();
            }
        };
        DB.test_index_delete_after_worker_paused = Race.prepare;
        defer DB.test_index_delete_after_worker_paused = null;
        const result = db.deleteIndex("g");
        db.alloc = alloc;
        DB.test_index_delete_after_worker_paused = null;
        try std.testing.expectError(error.SchemaInUse, result);
        try std.testing.expect(!Race.failing.has_induced_failure);
        try std.testing.expectEqual(@as(usize, 0), Race.failing.alloc_index);
        try std.testing.expect(db.executor.appliedSequence("g") != null);
        try db.executor.failIfUnhealthy();
        try db.abortTransaction(Race.txn_id, 2);
        try db.batch(.{ .writes = &.{.{ .key = "source", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"target\"}]}}}" }}, .sync_level = .full_index });
        try db.runUntilIdle();
        const edges = try db.getEdges(alloc, "g", "source", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
        try std.testing.expectEqualStrings("target", edges[0].target);
    }
}

test "graph projection routing writer honors root tags without nested suppression or duplicates" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "", "{\"evidence\":{\"source_table\":\"wrong\",\"target_table\":\"wrong\"},\"score\":1.00000000000000000000001}", "{\"source_table\":\"custom_people\",\"target_table\":\"custom_companies\"}" }) |metadata_template| {
        const writes = try graphWritesFromArtifactValueAlloc(alloc, "facts", "fact",
            \\{"entities":[{"id":"person","document_id":"same","table":"people"},{"id":"company","document_id":"same","table":"companies"}],"relations":[{"uuid":"one","type":"R","source":"person","target":"company","source_table":"stale_people","target_table":"stale_companies","evidence":{"source_table":"wrong","target_table":"wrong"},"score":1.00000000000000000000001}]}
        , .{ .artifact_name = @constCast("facts"), .format = .extraction_graph, .mapping = .{
            .edge_id_template = @constCast("{{ _item.uuid }}"),
            .metadata_template_json = @constCast(metadata_template),
        } }, "application/json", null, null, 10);
        defer freeGraphWrites(alloc, writes);
        try std.testing.expectEqual(@as(usize, 1), writes.len);
        var scratch = graph_metadata_tables.Scratch.init(alloc, null);
        defer scratch.deinit();
        const custom = std.mem.indexOf(u8, metadata_template, "custom_people") != null;
        try std.testing.expectEqualStrings(if (custom) "custom_people" else "people", (try scratch.table(writes[0].metadata_json, "source_table")).?);
        try std.testing.expectEqualStrings(if (custom) "custom_companies" else "companies", (try scratch.table(writes[0].metadata_json, "target_table")).?);
        if (!custom) try std.testing.expect(std.mem.indexOf(u8, writes[0].metadata_json, "1.00000000000000000000001") != null);
    }
}

test "db graph qualified cleanup preserves foreign identities across rebuild and reopen" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("qualified-cleanup-lifecycle");
    defer directory.cleanup();
    for (0..2) |iteration| {
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        db.local_execution.row_policy_table_name = "facts";
        if (iteration == 0) {
            try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            try db.batch(.{ .graph_writes = &.{
                .{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "local" },
                .{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "foreign-target", .metadata_json = "{\"target_table\":\"people\"}" },
                .{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "foreign-source", .owner_document = "a", .metadata_json = "{\"source_table\":\"people\"}" },
            }, .sync_level = .full_index });
            try db.core.store.ensureGraphIncomingDirectory();
            try graphTestApplyOrdered(&db, .{ .deletes = &.{"shared"} }, .{ .term = 1, .index = 1 });
            var old = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
            defer old.deinit();
            try std.testing.expectEqual(@as(usize, 1), old.graph_deletes.len);
            // Same storage identity, new foreign destination. The selected
            // page must check its current routing before publishing retirement.
            try graphTestApplyOrdered(&db, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "shared", .edge_type = "R", .edge_id = "local", .metadata_json = "{\"target_table\":\"people\"}" }} }, .{ .term = 1, .index = 2 });
            try graphTestApplyOrdered(&db, old.request(), .{ .term = 1, .index = 3 });
            try db.runUntilIdle();
        }
        _ = try db.rebuildGraphDerivedState();
        const edges = try db.getEdges(alloc, "g", "a", "", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 3), edges.len);
    }
}

test "db graph owner revival bounds foreground history and fences stale pages and newer retirements" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("owner-revival-lifecycle");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const contract = graph_cleanup_contract_mod;
    const history = try a.alloc(docstore_mod.KVPair, 601);
    for (history, 0..) |*row, i| {
        const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(a, "a", "g", if (i == 600) "R" else "OLD", if (i == 600) "b" else "target", "a", if (i == 600) "" else try std.fmt.allocPrint(a, "{d:0>4}", .{i}));
        row.* = .{ .key = try internal_keys.graphRetirementKeyAlloc(a, artifact), .value = "1" };
    }
    try db.core.store.putBatch(history, &.{});
    try graphTestApplyOrdered(&db, .{ .writes = &.{.{ .key = "a", .value = "{\"v\":1,\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\"}]}}}" }} }, .{ .term = 1, .index = 1 });
    const prefix = try internal_keys.graphRetirementPrefixAlloc(a, "a");
    const retained = try db.core.store.scanPrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, retained);
    // The explicitly reauthored inline relationship clears its own marker.
    // Every unrelated historical marker remains for bounded maintenance.
    try std.testing.expectEqual(@as(usize, 600), retained.len);
    try std.testing.expect(try db.core.store.graphEndpointCleanupBlocksReads());
    var stale = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer stale.deinit();
    try std.testing.expect(stale.page.inspected <= 255);
    try graphTestApplyOrdered(&db, .{ .writes = &.{.{ .key = "a", .value = "{\"v\":2,\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\"}]}}}" }} }, .{ .term = 1, .index = 2 });
    const job_key = try contract.ownerJobKeyAlloc(a, "a");
    const current_job = try db.core.store.get(alloc, job_key);
    defer alloc.free(current_job);
    try graphTestApplyOrdered(&db, stale.request(), .{ .term = 1, .index = 3 });
    const after_stale = try db.core.store.get(alloc, job_key);
    defer alloc.free(after_stale);
    try std.testing.expectEqualSlices(u8, current_job, after_stale);
    var fresh = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer fresh.deinit();
    const newer = contract.retirementValue(fresh.page.guards[0].generation + 1);
    const retained_key = fresh.page.deletes[0];
    try db.core.store.put(retained_key, &newer);
    try graphTestApplyOrdered(&db, fresh.request(), .{ .term = 1, .index = 4 });
    var index: u64 = 5;
    var pages: usize = 1;
    while (try db.prepareGraphEndpointCleanupBatch(alloc)) |selected| {
        var page = selected;
        defer page.deinit();
        try std.testing.expect(page.page.inspected <= 255);
        try std.testing.expect(page.page.bytes <= 256 * 1024);
        try graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = index });
        index += 1;
        pages += 1;
    }
    try std.testing.expect(pages >= 4);
    const marker = try db.core.store.get(alloc, retained_key);
    defer alloc.free(marker);
    try std.testing.expectEqualSlices(u8, &newer, marker);
    try db.runUntilIdle();
    const edges = try db.getEdges(alloc, "g", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("b", edges[0].target);
}

test "db graph owner revival retained input replay cannot overwrite a newer input" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("owner-replay-input-race");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"relations\":{\"relations\":[{\"type\":\"R\",\"target\":{\"document_id\":\"b\"}}]}}" }}, .sync_level = .full_index });
    const contract = graph_cleanup_contract_mod;
    const job = try contract.ownerJobKeyAlloc(alloc, "a");
    defer alloc.free(job);
    const value = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "a", .generation = 10, .phase = .inputs });
    defer alloc.free(value);
    try db.core.store.put(job, value);
    var plan = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer plan.deinit();
    const input_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "relations_v1");
    defer alloc.free(input_key);
    const old = try db.core.store.get(alloc, input_key);
    defer alloc.free(old);
    const newer = try std.mem.concat(alloc, u8, &.{ old, " " });
    defer alloc.free(newer);
    try db.core.store.put(input_key, newer);
    try graphTestApplyOrdered(&db, plan.request(), .{ .term = 1, .index = 1 });
    const actual = try db.core.store.get(alloc, input_key);
    defer alloc.free(actual);
    try std.testing.expectEqualSlices(u8, newer, actual);
    var sequence: u64 = 2;
    while (try db.prepareGraphEndpointCleanupBatch(alloc)) |selected| {
        var page = selected;
        defer page.deinit();
        try graphTestApplyOrdered(&db, page.request(), .{ .term = 1, .index = sequence });
        sequence += 1;
    }
    try std.testing.expect(!try db.core.store.hasGraphEndpointCleanup());
}

test "db graph cleanup lookup carries and applies owner replay afterimages" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("fresh-owner-wire");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const contract = graph_cleanup_contract_mod;
    const key = try contract.ownerJobKeyAlloc(alloc, "owner");
    defer alloc.free(key);
    const value = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "owner", .generation = 7, .phase = .inputs });
    defer alloc.free(value);
    try db.core.store.put(key, value);
    var page = (try db.prepareGraphEndpointCleanupBatch(alloc)).?;
    defer page.deinit();
    try std.testing.expect(page.request().merge_artifacts.len > 0);
    var response = (try db.lookupRelationalTopology(alloc, "{\"mode\":\"graph_endpoint_cleanup\"}", .{})).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(types.GraphEndpointCleanupStatus, alloc, response.json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(page.replay_writes.len, parsed.value.merge_artifacts.len);
    const req = parsed.value.request();
    try types.validateGraphEndpointCleanupCommand(req);
    for (page.replay_writes, req.merge_artifacts) |expected, actual| {
        try std.testing.expectEqualSlices(u8, expected.key, actual.key);
        try std.testing.expectEqualSlices(u8, expected.value, actual.value);
    }
    try db.batch(req);
    const advanced = try db.core.store.get(alloc, key);
    defer alloc.free(advanced);
    try std.testing.expectEqual(contract.OwnerPhase.chunks, (try contract.decodeOwnerJob(key, advanced)).phase);
    // Duplicate ordered maintenance pages cannot roll back a newer checkpoint.
    try db.batch(req);
    const replayed = try db.core.store.get(alloc, key);
    defer alloc.free(replayed);
    try std.testing.expectEqualSlices(u8, advanced, replayed);
}

test "db graph endpoint cleanup transaction guards respect physical endpoint tables" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("qualified-transaction-guards");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    db.local_execution.row_policy_table_name = "facts";
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const foreign = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\",\"metadata\":{\"target_table\":\"people\"}}]}}}";
    const first = try db.beginTransaction(1);
    try db.writeTransaction(first, .{ .writes = &.{.{ .key = "a", .value = foreign }} });
    // A foreign dependency must not lock the same document key in this table.
    try graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 1 });
    try db.abortTransaction(first, 2);
    // Subsequent admission also works from the durable table identity when
    // the caller has no execution-time table override.
    db.local_execution.row_policy_table_name = null;
    for ([_][]const u8{
        "{\"target_table\":\"people\"}",
        "{\"source_table\":\"people\"}",
        "{\"source_table\":\"facts\",\"target_table\":\"people\"}",
        "{\"source_table\":\"people\",\"target_table\":\"facts\"}",
    }) |metadata| {
        const value = try std.fmt.allocPrint(alloc, "{{\"_edges\":{{\"g\":{{\"R\":[{{\"target\":\"hub\",\"metadata\":{s}}}]}}}}}}", .{metadata});
        defer alloc.free(value);
        for (0..2) |entrypoint| {
            const txn_id = try db.beginTransaction(3);
            if (entrypoint == 0)
                try db.writeIntents(txn_id, &.{.{ .key = "a", .value = value }}, &.{})
            else
                try db.writeTransaction(txn_id, .{ .writes = &.{.{ .key = "a", .value = value }} });
            try db.abortTransaction(txn_id, 4);
        }
    }
    const local = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"hub\",\"metadata\":{\"source_table\":\"facts\",\"target_table\":\"facts\"}}]}}}";
    const rejected = try db.beginTransaction(5);
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeTransaction(rejected, .{ .writes = &.{.{ .key = "a", .value = local }} }));
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeIntents(rejected, &.{.{ .key = "a", .value = local }}, &.{}));
    try db.abortTransaction(rejected, 6);
    while (try db.prepareGraphEndpointCleanupBatch(alloc)) |selected| {
        var plan = selected;
        defer plan.deinit();
        try db.batch(plan.request());
    }
    const accepted = try db.beginTransaction(7);
    try db.writeTransaction(accepted, .{ .writes = &.{.{ .key = "a", .value = local }} });
    try std.testing.expectError(error.IntentConflict, graphTestApplyOrdered(&db, .{ .deletes = &.{"hub"} }, .{ .term = 1, .index = 2 }));
    try db.abortTransaction(accepted, 8);
}

test "db graph fact split retirement recovery preserves complete identities" {
    const alloc = std.testing.allocator;
    for ([_]bool{ true, false }) |before_cursor| {
        var tmp = try TestDirectory.init("split-fact-identities");
        defer tmp.cleanup();
        {
            var db = try DB.open(alloc, tmp.path(), .{});
            defer db.close();
            try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            try db.batch(.{ .graph_writes = &.{
                .{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "R", .weight = 7 },
                .{ .index_name = "g", .source = "doc:a", .target = "doc:b", .edge_type = "R", .edge_id = "fact", .owner_document = "doc:z", .weight = 2 },
                .{ .index_name = "g", .source = "doc:z", .target = "doc:b", .edge_type = "R", .edge_id = "inline", .weight = 3 },
            }, .sync_level = .full_index });
            const split: shard_mod.SplitState = .{ .phase = .prepare, .split_key = "doc:m", .new_shard_id = 2, .started_at = 1, .original_range_end = "" };
            try db.core.setSplitState(split);
            if (before_cursor) db_zig.test_graph_split_retire_abort_before_cursor.store(true, .monotonic) else db_zig.test_graph_split_retire_abort_after_flush.store(true, .monotonic);
            defer db_zig.test_graph_split_retire_abort_before_cursor.store(false, .monotonic);
            defer db_zig.test_graph_split_retire_abort_after_flush.store(false, .monotonic);
            try std.testing.expectError(error.TestInjectedBackfillFailure, retireSplitEntitySourcedEdgesLocked(&db, split));
        }
        var db = try DB.open(alloc, tmp.path(), .{});
        defer db.close();
        const retained = try db.getEdges(alloc, "g", "doc:a", "R", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, retained);
        try std.testing.expectEqual(@as(usize, 2), retained.len);
        for (retained) |edge| {
            try std.testing.expectEqual(@as(f64, if (edge.edge_id.len == 0) 7 else 2), edge.weight);
            if (edge.edge_id.len > 0) try std.testing.expectEqualStrings("doc:z", edge.owner_document);
        }
        const moved = try db.getEdges(alloc, "g", "doc:z", "R", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, moved);
        try std.testing.expectEqual(@as(usize, 1), moved.len);
        try std.testing.expectEqualStrings("inline", moved[0].edge_id);
    }
}

test "db graph fact ownership modes reject mixed native mutations atomically" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("fact-ownership-admission");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{});
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    const write: types.GraphEdgeWrite = .{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "one", .owner_document = "one", .owner = "one" };
    try std.testing.expectError(error.InvalidGraphEdges, db.batch(.{ .writes = &.{.{ .key = "untouched", .value = "{}" }}, .graph_writes = &.{write} }));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "untouched"));
    var valid = write;
    valid.owner = "";
    try db.batch(.{ .graph_writes = &.{valid}, .sync_level = .full_index });
    const deletion: types.GraphEdgeDelete = .{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "one", .owner_document = "one", .owner = "one" };
    try std.testing.expectError(error.InvalidGraphEdges, db.batch(.{ .deletes = &.{"one"}, .graph_deletes = &.{deletion} }));
    const edges = try db.getEdges(alloc, "g", "a", "R", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    var index = db.core.index_manager.graphIndex("g").?.index;
    try std.testing.expectError(error.InvalidGraphEdges, index.batchApply(&.{.{ .source = "a", .target = "b", .edge_type = "R", .edge_id = "two", .owner = "two" }}, &.{}));
    try std.testing.expectError(error.InvalidGraphEdges, index.batchApply(&.{}, &.{.{ .source = "a", .target = "b", .edge_type = "R", .edge_id = "one", .owner_document = "one", .owner = "one" }}));
    const unchanged = try db.getEdges(alloc, "g", "a", "R", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, unchanged);
    try std.testing.expectEqual(@as(usize, 1), unchanged.len);
}

test "db graph fact neighbor context schedules qualified endpoints in both replay formats" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("fact-neighbor-replay");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const manager = db.core.index_manager;
    try manager.enrichments.append(alloc, try @import("catalog/enrichment_catalog.zig").EnrichmentConfig.clone(alloc, .{
        .name = "context",
        .kind = .asset,
        .source_field = "body",
        .neighbor_context_json = "{\"graph_index\":\"g\",\"direction\":\"both\"}",
    }));
    try db.core.store.put(internal_keys.graph_owning_table_key, "facts");
    const cases = [_]struct { metadata: []const u8, previous_metadata: ?[]const u8 = null, source: bool, target: bool }{
        .{ .metadata = "{}", .source = true, .target = true },
        .{ .metadata = "{\"source_table\":\"facts\",\"target_table\":\"facts\"}", .source = true, .target = true },
        .{ .metadata = "{\"source_table\":\"people\"}", .source = false, .target = true },
        .{ .metadata = "{\"target_table\":\"companies\"}", .source = true, .target = false },
        .{ .metadata = "{\"source_table\":\"people\",\"target_table\":\"companies\"}", .source = false, .target = false },
        .{ .metadata = "{\"source_table\":\"people\",\"target_table\":\"companies\"}", .previous_metadata = "{}", .source = true, .target = true },
    };
    for ([_]struct { json: []const u8, out: bool, in: bool }{
        .{ .json = "{\"graph_index\":\"g\",\"direction\":\"out\"}", .out = true, .in = false },
        .{ .json = "{\"graph_index\":\"g\",\"direction\":\"in\"}", .out = false, .in = true },
        .{ .json = "{\"graph_index\":\"g\",\"direction\":\"both\"}", .out = true, .in = true },
    }) |direction| {
        const configured = try alloc.dupe(u8, direction.json);
        alloc.free(manager.enrichments.items[0].neighbor_context_json);
        manager.enrichments.items[0].neighbor_context_json = configured;
        for (cases) |case| {
            const write: types.GraphEdgeWrite = .{ .index_name = "g", .source = "alice", .target = "acme", .edge_type = "R", .edge_id = "fact", .owner_document = "fact", .metadata_json = case.metadata };
            const deletion: types.GraphEdgeDelete = .{ .index_name = "g", .source = "alice", .target = "acme", .edge_type = "R", .edge_id = "fact", .owner_document = "fact" };
            const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "fact", "g", "R", "acme", "alice", "fact");
            defer alloc.free(artifact);
            const raw = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 0, 1, 0, 0, case.previous_metadata orelse case.metadata);
            defer alloc.free(raw);
            try db.core.store.put(artifact, raw);
            for ([_]bool{ false, true }) |delete| for ([_]bool{ false, true }) |thin| {
                const req: types.BatchRequest = if (delete) .{ .graph_deletes = &.{deletion} } else .{ .graph_writes = &.{ write, write } };
                var targets: ManagedSyncTargets = .{};
                defer targets.deinit(alloc);
                const payload = if (thin)
                    try encodeThinReplayRecordPayload(alloc, req, &.{}, &.{}, &.{}, &.{}, &.{}, 1, false, manager, &targets, null)
                else
                    try encodeDirectChangeRecordPayload(&db.batchContext(), .{ .graph_writes = req.graph_writes, .graph_deletes = req.graph_deletes }, 1, req, null);
                defer alloc.free(payload);
                var decoded = try change_journal_mod.decodeRecord(alloc, payload);
                defer decoded.deinit();
                var sources: usize = 0;
                var targets_count: usize = 0;
                var owners: usize = 0;
                for (decoded.record.changed_doc_keys) |key| {
                    sources += @intFromBool(std.mem.eql(u8, key, "alice"));
                    targets_count += @intFromBool(std.mem.eql(u8, key, "acme"));
                    owners += @intFromBool(std.mem.eql(u8, key, "fact"));
                }
                try std.testing.expectEqual(@as(usize, @intFromBool(case.source and direction.out)), sources);
                try std.testing.expectEqual(@as(usize, @intFromBool(case.target and direction.in)), targets_count);
                try std.testing.expectEqual(@as(usize, 1), owners);
                try std.testing.expectEqual((case.source and direction.out) or (case.target and direction.in), journalRecordHasHint(decoded.record, .enrichment));
            };
        }
    }
}

test "db graph fact neighbor replay releases partial allocations" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var manager = try index_manager_mod.IndexManager.init(alloc, ".");
            // Borrow immutable catalog strings so allocation injection targets
            // replay construction, independently of catalog cloning.
            defer {
                manager.enrichments.clearRetainingCapacity();
                manager.deinit();
            }
            try manager.enrichments.append(alloc, .{
                .name = "context",
                .kind = .asset,
                .source_field = "body",
                .neighbor_context_json = "{\"graph_index\":\"g\",\"direction\":\"both\"}",
            });
            const write: types.GraphEdgeWrite = .{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "fact", .owner_document = "fact", .metadata_json = "{\"source_table\":\"f\\u0061cts\"}" };
            const req: types.BatchRequest = .{ .graph_writes = &.{ write, write } };
            var ctx: BatchExecutionContext = undefined;
            ctx.alloc = alloc;
            ctx.index_manager = &manager;
            const materialized = try encodeDirectChangeRecordPayload(&ctx, .{ .graph_writes = req.graph_writes }, 1, req, "facts");
            defer alloc.free(materialized);
            var targets: ManagedSyncTargets = .{};
            defer targets.deinit(alloc);
            const thin = try encodeThinReplayRecordPayload(alloc, req, &.{}, &.{}, &.{}, &.{}, &.{}, 1, false, &manager, &targets, "facts");
            defer alloc.free(thin);
            const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "fact", "g", "R", "b", "a", "fact");
            defer alloc.free(artifact);
            var mapped = try directGraphNeighborContextHintsAlloc(alloc, .{ .writes = &.{.{ .key = "fact", .value = "{}" }} }, &.{write}, &.{artifact}, &manager, "facts");
            defer mapped.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), mapped.keys.len);
            var generated = try directGraphNeighborContextHintsAlloc(alloc, .{}, &.{}, &.{artifact}, &manager, "facts");
            defer generated.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), generated.keys.len);
        }
    };
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "db graph fact transform preserves exact metadata" {
    const alloc = std.testing.allocator;
    var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    defer {
        for (writes.items) |*w| DB.deinitOwnedGraphEdgeWrite(alloc, w);
        writes.deinit(alloc);
    }
    var deletes = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
    defer deletes.deinit(alloc);
    try DB.appendGraphTransformWrite(alloc, &writes, &deletes, "a", .{ .index_name = "g", .edge_type = "R" }, "{\"target\":\"b\",\"edge_id\":\"f\",\"metadata\":{\"n\":1.5000000000000001}}");
    try std.testing.expect(std.mem.indexOf(u8, writes.items[0].metadata_json, "1.5000000000000001") != null);

    var tmp = try TestDirectory.init("transform-exact-numbers");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{}" }}, .sync_level = .full_index });
    try db.batch(.{
        .transforms = &.{.{ .key = "a", .operations = &.{.{ .op = .push, .path = "$._edges.g.R", .value_json = "{\"target\":\"b\",\"edge_id\":\"f\",\"weight\":2.25,\"metadata\":{\"n\":1.5000000000000001,\"large\":9007199254740993}}" }} }},
        .sync_level = .full_index,
    });
    const path = try db.findShortestPathWithOptions(alloc, "g", "a", "b", .{
        .edge_filter = .{ .properties = &.{
            .{ .field = "/metadata/n", .op = .eq, .value_json = "1.5000000000000001" },
            .{ .field = "/metadata/large", .op = .eq, .value_json = "9007199254740993" },
        } },
    });
    defer if (path) |value| paths_mod.freePath(alloc, value);
    try std.testing.expect(path != null);
    try std.testing.expectEqual(@as(f64, 2.25), path.?.total_weight);
}

test "db graph fact materialized scopes match journal" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("review-scope");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    try db.addIndex(.{ .name = "h", .kind = .graph, .config_json = "{\"sources\":[{\"artifact\":\"context\"}]}" });
    try db.addIndex(.{ .name = "plain", .kind = .full_text, .config_json = "{}" });
    const req: types.BatchRequest = .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f" }} };
    const batch: derived_types.DerivedBatch = .{ .graph_writes = req.graph_writes };
    const payload = try encodeDirectChangeRecordPayload(&db.batchContext(), batch, 1, req, null);
    defer alloc.free(payload);
    var record = try change_journal_mod.decodeRecord(alloc, payload);
    defer record.deinit();
    var planned = try prepareDirectChangeRecord(&db.batchContext(), batch, 1, req, null);
    defer change_journal_mod.deinitRecord(alloc, &planned);
    var from_batch = try collectManagedSyncTargetsForRecordWithBatch(alloc, db.core.index_manager, planned, batch);
    defer from_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), from_batch.full_text_indexes.len);
    var from_record = try collectManagedSyncTargetsForRecord(alloc, db.core.index_manager, record.record);
    defer from_record.deinit(alloc);
    var has_batch = false;
    for (from_batch.target_identities) |identity| try std.testing.expect(!std.mem.eql(u8, identity.index_name, "plain"));
    for (from_batch.target_identities) |i| {
        if (std.mem.eql(u8, i.index_name, "h")) {
            has_batch = true;
            try std.testing.expectEqual(IndexTargetVisibility.ServingSetEffect.may_reduce, i.serving_set_effect);
        }
    }
    var has_record = false;
    for (from_record.target_identities) |i| {
        if (std.mem.eql(u8, i.index_name, "h")) has_record = true;
    }
    try std.testing.expect(has_record);
    try std.testing.expectEqual(has_record, has_batch);

    const Observer = struct {
        saw_generated_consumer: bool = false,
        scope_known: bool = false,
        fn changed(ptr: *anyopaque, event: QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (event.target_sequence == null) return;
            self.scope_known = event.target_scope_known;
            for (event.target_indexes) |identity| {
                if (std.mem.eql(u8, identity.index_name, "h") and identity.serving_set_effect == .may_reduce)
                    self.saw_generated_consumer = true;
            }
        }
    };
    var observer = Observer{};
    db.setQueryVisibilityHook(.{ .ptr = &observer, .on_change = Observer.changed });
    defer db.setQueryVisibilityHook(null);
    try db.batch(.{ .graph_writes = req.graph_writes, .sync_level = .write });
    try std.testing.expect(observer.scope_known);
    try std.testing.expect(observer.saw_generated_consumer);
}

test "db graph fact enrichment observes pending graph replay" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("review-neighbor-order");
    defer tmp.cleanup();
    var producer = GateAssetProducer{};
    producer.allowAll();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var config: enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.initOptionalEnrichmentRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alice\"}" }}, .sync_level = .write });
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const previous = producer.successful_requests.load(.acquire);
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f" }}, .sync_level = .write });
    const pending = db.core.nextDerivedSequence();
    const graph_checkpoint = try db.core.loadAppliedSequence(alloc, "g");
    try db.enrichment_runtime.?.catchUpUntilForDrain(pending);
    // Source refresh must not certify this consumer while its producer runs.
    try std.testing.expectEqual(graph_checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
    try std.testing.expect(producer.successful_requests.load(.acquire) > previous);
    const RefreshAllocationFixture = struct {
        fn run(failing_alloc: Allocator, resident: *DB) !void {
            var ctx = resident.async_context.*;
            ctx.alloc = failing_alloc;
            var append_ctx = resident.enrichment_append_context.?.*;
            append_ctx.async_context = &ctx;
            resident.core.index_manager.graphIndex("g").?.invalidateNeighborSource();
            while (true) {
                const lease = acquireNeighborContextGraphSource(&append_ctx, "g") catch |err| {
                    if (err == error.GraphSourceReplayPending) continue;
                    return err;
                };
                lease.release();
                break;
            }
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, RefreshAllocationFixture.run, .{&db});
    // Simulate an older consumer window withdrawing this owner after a source
    // refresh. Invalidation must make the next sample restore current primary
    // effects even though no new primary revision has arrived.
    try db.core.index_manager.deleteGraphDocsByName("g", &.{"f"});
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.graphIndex("g").?.neighbor_source_sequence);
    {
        const source_cap = db.enrichment_runtime.?.config.neighbor_context_graph_source.?;
        const lease = try source_cap.acquire_fn(source_cap.ptr, "g");
        defer lease.release();
        const restored = try db.core.index_manager.graphIndex("g").?.index.getEdges(alloc, "a", "R", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, restored);
        try std.testing.expectEqual(@as(usize, 1), restored.len);
    }
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    const current = producer.successful_requests.load(.acquire);

    try std.testing.expect(current > previous);

    try db.batch(.{ .graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f" }}, .sync_level = .write });
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try std.testing.expect(producer.successful_requests.load(.acquire) > current);
    const edges = try db.core.index_manager.graphIndex("g").?.index.getEdges(alloc, "a", "R", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}

test "db graph fact qualified path bypasses algebraic shortcut" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("review-qualified-path");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\",\"enabled\":true}}}" });
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }}, .sync_level = .full_index });
    const path = try db.findShortestPathWithOptions(alloc, "g", "a", "b", .{ .target_table = "foreign", .max_depth = 3 });
    defer if (path) |value| paths_mod.freePath(alloc, value);
    try std.testing.expect(path == null);
    const paths = try db.findKShortestPathsWithOptions(alloc, "g", "a", "b", 1, .{ .target_table = "foreign", .max_depth = 3 });
    defer paths_mod.freePaths(alloc, paths);
    try std.testing.expectEqual(@as(usize, 0), paths.len);
}

test "db graph fact inline mutation wakes incoming producers for additions replacement removal and deletion" {
    const Fixture = struct {
        fn produceBounded(ptr: *anyopaque, alloc: Allocator, request: asset_producer_mod.Request, context: asset_producer_mod.InvocationContext) ![]u8 {
            try context.check();
            return GateAssetProducer.produce(ptr, alloc, request);
        }
        fn drain(db: *DB) !void {
            try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
            try db.runDerivedUntil(db.core.nextDerivedSequence());
            try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        }
        fn hasNeighbor(db: *DB, doc: []const u8) !bool {
            const key = try internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "context");
            defer db.alloc.free(key);
            const value = try db.core.store.get(db.alloc, key);
            defer db.alloc.free(value);
            return std.mem.indexOf(u8, value, "\"target\":\"a\"") != null;
        }
    };
    const alloc = std.testing.allocator;
    for ([_]types.SyncLevel{ .write, .full_index }) |sync_level| {
        var tmp = try TestDirectory.init("inline-neighbor-lifecycle");
        defer tmp.cleanup();
        var producer = GateAssetProducer{};
        producer.allowAll();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .in } });
        var config: enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &producer, .vtable = &.{
            .produce = GateAssetProducer.produce,
            .produce_with_context = Fixture.produceBounded,
            .foreground_bounded = true,
            .invocation_memory_for_requests = GateAssetProducer.invocationMemory,
        } } };
        try db.initOptionalEnrichmentRuntime(&config);
        try db.batch(.{ .writes = &.{ .{ .key = "b", .value = "{\"body\":\"Bob\"}" }, .{ .key = "c", .value = "{\"body\":\"Carol\"}" } }, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));
        // A synchronous source-field update must retain committed adjacency
        // in producer input and its skip hash, just like asynchronous replay.
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"body\":\"Bob revised\"}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"c\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
        try std.testing.expect(try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .deletes = &.{"a"}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
    }
}

test "db graph fact neighbor context excludes foreign source namespace" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("review-foreign-neighbor");
    defer tmp.cleanup();
    var producer = GateAssetProducer{};
    producer.allowAll();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.core.store.put(internal_keys.graph_owning_table_key, "here");
    db.local_execution.row_policy_table_name = "here";
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var config: enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.initOptionalEnrichmentRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alpha\"}" }}, .sync_level = .write });
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "foreign-source-neighbor", .edge_type = "R", .edge_id = "f", .owner_document = "f", .metadata_json = "{\"source_table\":\"foreign\",\"target_table\":\"here\"}" }}, .sync_level = .write });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const path = try db.findShortestPathWithOptions(alloc, "g", "a", "foreign-source-neighbor", .{ .owning_table = "here" });
    defer if (path) |value| paths_mod.freePath(alloc, value);
    try std.testing.expect(path == null);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alpha revised\"}" }}, .sync_level = .write });
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    const key = try internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "context");
    defer alloc.free(key);
    const output = try db.core.store.get(alloc, key);
    defer alloc.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "foreign-source-neighbor") == null);
}

test "db graph fact neighbor refresh resumes bounded turns without certifying consumer coverage" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("neighbor-refresh-turns");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var producer = GateAssetProducer{};
    producer.allowAll();
    var config: enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.initOptionalEnrichmentRuntime(&config);
    const record_count = 17;
    for (0..record_count) |i| {
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "f", .owner_document = "f", .weight = @floatFromInt(i + 1) }}, .sync_level = .write });
    }
    const checkpoint = try db.core.loadAppliedSequence(alloc, "g");
    const source = db.enrichment_runtime.?.config.neighbor_context_graph_source.?;
    const entry = db.core.index_manager.graphIndex("g").?;
    var previous = entry.neighbor_source_sequence;
    var turns: usize = 0;
    while (true) {
        const lease = source.acquire_fn(source.ptr, "g") catch |err| {
            try std.testing.expectEqual(error.GraphSourceReplayPending, err);
            try std.testing.expect(entry.neighbor_source_sequence > previous);
            previous = entry.neighbor_source_sequence;
            turns += 1;
            try std.testing.expect(turns <= record_count);
            // The acquisition released publication/catalog/index locks on yield.
            var publication = db.core.index_manager.beginGraphPrimaryMutation();
            publication.release();
            try std.testing.expectEqual(checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
            continue;
        };
        lease.release();
        break;
    }
    try std.testing.expect(turns >= 4);
    try std.testing.expectEqual(db.core.store.lastReplaySequence(0), entry.neighbor_source_sequence);
    try std.testing.expectEqual(checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
    const edges = try entry.index.getEdges(alloc, "a", "R", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqual(@as(f64, record_count), edges[0].weight);
}

test "db graph fact neighbor context resolves equal-key qualified reverse orientation" {
    const alloc = std.testing.allocator;
    for ([_]types.EnrichmentNeighborContextConfig{ .{ .graph_index = "g", .direction = .in }, .{ .graph_index = "g", .direction = .both } }) |neighbor_context| {
        var tmp = try TestDirectory.init("qualified-reverse-neighbor");
        defer tmp.cleanup();
        var producer = GateAssetProducer{};
        producer.allowAll();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.core.store.put(internal_keys.graph_owning_table_key, "here");
        db.local_execution.row_policy_table_name = "here";
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = neighbor_context });
        var config: enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
        try db.initOptionalEnrichmentRuntime(&config);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"Alpha\"}" }}, .sync_level = .write });
        try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        try db.runDerivedUntil(db.core.nextDerivedSequence());
        try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "a", .edge_type = "R", .edge_id = "f", .owner_document = "f", .metadata_json = "{\"source_table\":\"foreign\",\"target_table\":\"h\\u0065re\"}" }}, .sync_level = .write });
        try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        const key = try internal_keys.artifactNamedPrefixAlloc(alloc, "a", "asset", "context");
        defer alloc.free(key);
        const output = try db.core.store.get(alloc, key);
        defer alloc.free(output);
        try std.testing.expect(std.mem.indexOf(u8, output, "\"direction\":\"in\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, output, "\"direction\":\"out\"") == null);
    }
}

test "db graph fact synchronous neighbor downstream chain" {
    const Fixture = struct {
        fn produceBounded(ptr: *anyopaque, alloc: Allocator, request: asset_producer_mod.Request, context: asset_producer_mod.InvocationContext) ![]u8 {
            try context.check();
            return GateAssetProducer.produce(ptr, alloc, request);
        }
        fn drain(db: *DB) !void {
            try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
            try db.runDerivedUntil(db.core.nextDerivedSequence());
            try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        }
        fn hasNeighbor(db: *DB, doc: []const u8) !bool {
            const key = try internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "context");
            defer db.alloc.free(key);
            const value = try db.core.store.get(db.alloc, key);
            defer db.alloc.free(value);
            const downstream_key = try internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "copied_context");
            defer db.alloc.free(downstream_key);
            const downstream = try db.core.store.get(db.alloc, downstream_key);
            defer db.alloc.free(downstream);
            try std.testing.expectEqualStrings(value, downstream);
            const mirror_key = try internal_keys.artifactNamedPrefixAlloc(db.alloc, doc, "asset", "mirrored_context");
            defer db.alloc.free(mirror_key);
            const mirror = try db.core.store.get(db.alloc, mirror_key);
            defer db.alloc.free(mirror);
            try std.testing.expectEqualStrings(value, mirror);
            return std.mem.indexOf(u8, value, "\"target\":\"a\"") != null;
        }
    };
    const alloc = std.testing.allocator;
    for ([_]types.SyncLevel{.full_index}) |sync_level| {
        var tmp = try TestDirectory.init("inline-neighbor-lifecycle");
        defer tmp.cleanup();
        var producer = GateAssetProducer{};
        producer.allowAll();
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
        try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .content_type = "text/plain", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .in } });
        try db.addEnrichment(.{ .name = "copied_context", .kind = .asset, .source_artifact_name = "context", .producer_json = "{\"type\":\"copy\"}" });
        try db.addEnrichment(.{ .name = "mirrored_context", .kind = .asset, .source_artifact_name = "copied_context", .producer_json = "{\"type\":\"copy\"}" });
        var config: enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &producer, .vtable = &.{
            .produce = GateAssetProducer.produce,
            .produce_with_context = Fixture.produceBounded,
            .foreground_bounded = true,
            .invocation_memory_for_requests = GateAssetProducer.invocationMemory,
        } } };
        try db.initOptionalEnrichmentRuntime(&config);
        try db.batch(.{ .writes = &.{ .{ .key = "b", .value = "{\"body\":\"Bob\"}" }, .{ .key = "c", .value = "{\"body\":\"Carol\"}" } }, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));

        // A synchronous source-field update must retain committed adjacency
        // in producer input and its skip hash, just like asynchronous replay.
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"body\":\"Bob revised\"}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(try Fixture.hasNeighbor(&db, "b"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"c\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
        try std.testing.expect(try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "c"));
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"_edges\":{\"g\":{\"R\":[{\"target\":\"b\",\"edge_id\":\"f\"}]}}}" }}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try db.batch(.{ .deletes = &.{"a"}, .sync_level = sync_level });
        try Fixture.drain(&db);
        try std.testing.expect(!try Fixture.hasNeighbor(&db, "b"));
    }
}

test "db graph fact generated projection wakes endpoint context" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("review-generated-fact-context");
    defer tmp.cleanup();
    var producer = GateAssetProducer{};
    producer.allowAll();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$","nodes":{"source":"{{ _item.source }}","target":"{{ _item.target }}"},"edge":{"edge_id":"{{ _doc.key }}","type":"RELATES_TO"}},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "facts", .direction = .out } });
    var config: enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.initOptionalEnrichmentRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "alice", .value = "{\"body\":\"Alice\"}" }}, .sync_level = .write });
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.batch(.{ .writes = &.{.{ .key = "fact:works", .value = "{\"relations\":[{\"source\":\"alice\",\"target\":\"acme\"}]}" }}, .sync_level = .write });
    for (0..3) |_| {
        try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        try db.runDerivedUntil(db.core.nextDerivedSequence());
    }
    const entry = db.core.index_manager.graphIndex("facts").?;
    const edges = try entry.index.getEdges(alloc, "alice", "RELATES_TO", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    const key = try internal_keys.artifactNamedPrefixAlloc(alloc, "alice", "asset", "context");
    defer alloc.free(key);
    const output = try db.core.store.get(alloc, key);
    defer alloc.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "acme") != null);
}

test "db graph fact refresh bounds artifact work within one large record" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var tmp = try TestDirectory.init("review-large-record-refresh");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    var producer = GateAssetProducer{};
    producer.allowAll();
    var config: enrichment_runtime_mod.Config = .{ .asset_producer = producer.interface() };
    try db.initOptionalEnrichmentRuntime(&config);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const writes = try a.alloc(types.GraphEdgeWrite, 1025);
    for (writes, 0..) |*write, i| write.* = .{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = try std.fmt.allocPrint(a, "f{d}", .{i}), .owner_document = "owner" };
    try db.batch(.{ .graph_writes = writes, .sync_level = .write });
    const before_entry = db.core.index_manager.graphIndex("g").?;
    const before_edges = try before_entry.index.getEdges(alloc, "a", "R", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, before_edges);
    try std.testing.expectEqual(@as(usize, 0), before_edges.len);
    const tip = db.core.store.lastReplaySequence(0);
    try std.testing.expect((try db.core.loadAppliedSequence(alloc, "g")) < tip);
    try std.testing.expect((try before_entry.index.artifactRebuildSequence()) < tip);
    const source = db.enrichment_runtime.?.config.neighbor_context_graph_source.?;
    const checkpoint = try db.core.loadAppliedSequence(alloc, "g");
    try std.testing.expectError(error.GraphSourceReplayPending, source.acquire_fn(source.ptr, "g"));
    try std.testing.expect(before_entry.neighbor_refresh.sequence != 0);
    // An ordinary replay mutation discards the partial source view. A restart
    // must replay the record from its beginning rather than skipping lost rows.
    var stale = writes[0];
    stale.weight = 9;
    try db.core.index_manager.applyGraphMutationsByName("g", &.{stale}, &.{});
    try std.testing.expectEqual(@as(u64, 0), before_entry.neighbor_refresh.sequence);
    var turns: usize = 0;
    while (turns < 32) : (turns += 1) {
        const lease = source.acquire_fn(source.ptr, "g") catch |err| {
            if (err == error.GraphSourceReplayPending) continue;
            return err;
        };
        lease.release();
        break;
    }
    try std.testing.expect(turns < 32);
    try std.testing.expectEqual(checkpoint, try db.core.loadAppliedSequence(alloc, "g"));
    try std.testing.expectEqual(@as(u64, 0), before_entry.neighbor_refresh.sequence);
    const entry = db.core.index_manager.graphIndex("g").?;
    const edges = try entry.index.getEdges(alloc, "a", "R", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1025), edges.len);
}

test "db graph fact neighbor dependency admission rejects indirect feedback" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("neighbor-feedback");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addEnrichment(.{ .name = "relations", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "facts", .direction = .out } });
    try db.addEnrichment(.{ .name = "copied_relations", .kind = .asset, .source_artifact_name = "relations", .producer_json = "{\"type\":\"copy\"}" });
    try std.testing.expectError(error.InvalidEnrichmentConfig, db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{\"source\":{\"artifact\":\"copied_relations\"}}" }));
    try std.testing.expect(db.core.index_manager.graphIndex("facts") == null);
}

test "db graph fact failed neighbor producer blocks downstream stale artifact coverage" {
    const Fixture = struct {
        fail: bool = false,
        fn produce(ptr: *anyopaque, alloc: Allocator, request: asset_producer_mod.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.InvalidExtractorResponse;
            return std.fmt.allocPrint(alloc, "asset:{s}", .{request.source_text});
        }
    };
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("neighbor-chain-failure");
    defer tmp.cleanup();
    var fixture: Fixture = .{};
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "context", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}", .neighbor_context = .{ .graph_index = "g", .direction = .out } });
    try db.addEnrichment(.{ .name = "copied_context", .kind = .asset, .source_artifact_name = "context", .producer_json = "{\"type\":\"copy\"}" });
    var config: enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &fixture, .vtable = &.{ .produce = Fixture.produce, .invocation_memory_for_requests = GateAssetProducer.invocationMemory } } };
    try db.initOptionalEnrichmentRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"old\"}" }}, .sync_level = .write });
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    fixture.fail = true;
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"new\"}" }}, .sync_level = .write });
    try std.testing.expectError(error.EnrichmentWorkerFailed, db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence()));
    const issues = try db.listArtifactRepairIssues(alloc, .asset, null, 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    const downstream_failed = for (issues) |issue| {
        if (std.mem.eql(u8, issue.artifact_name, "copied_context") and issue.reason == .enrichment_failed and std.mem.eql(u8, issue.generation_error, "ArtifactDependencyFailed")) break true;
    } else false;
    try std.testing.expect(downstream_failed);
}

test "db graph fact orphan stages recover after reopen with no replay debt" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("graph-stage-recovery");
    defer tmp.cleanup();
    const prefix = "\x00\x00__graph_stage__:v1:";
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        // More than two pages of abandoned preparations from earlier owners.
        // The legacy stage format has no manifest and remains recoverable.
        var batch = try db.core.store.beginWriteBatch();
        errdefer batch.abort();
        for (0..513) |i| {
            var buf: [80]u8 = undefined;
            const key = try std.fmt.bufPrint(&buf, "{s}{d:0>8}", .{ prefix, i });
            try batch.put(key, "unpublished graph payload");
        }
        try batch.put("unrelated", "keep");
        try batch.commit();
    }
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    var config: enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
    try db.initOptionalEnrichmentRuntime(&config);
    const runtime = db.enrichment_runtime.?;
    const before = runtime.stats();
    try std.testing.expect(runtime.graph_stage_recovery_pending);
    try std.testing.expectError(error.EnrichmentWaitTimeout, runtime.catchUpUntilWithVisibilityDeadline(0, .{}, 0));
    try std.testing.expect(runtime.graph_stage_recovery_pending);
    try runtime.catchUpUntil(0);
    try std.testing.expect(!runtime.graph_stage_recovery_pending);
    var page = try @import("../backend_scan.zig").scanPrefixKeysPage(alloc, &runtime.store, prefix, 1);
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), page.keys.len);
    const kept = try db.core.store.get(alloc, "unrelated");
    defer alloc.free(kept);
    try std.testing.expectEqualStrings("keep", kept);
    const after = runtime.stats();
    try std.testing.expectEqual(before.applied_sequence, after.applied_sequence);
    try std.testing.expectEqual(before.processed_requests, after.processed_requests);
    try std.testing.expectEqual(before.error_count, after.error_count);
    try std.testing.expectEqual(before.retryable_error_count, after.retryable_error_count);
    try std.testing.expectEqual(@as(u64, 0), db.core.store.lastReplaySequence(0));
}

test "db graph fact orphan stage maintenance preserves terminal worker failure" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("graph-stage-failed-owner");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const stage = "\x00\x00__graph_stage__:v1:old";
    try db.core.store.put(stage, "unpublished");
    var config: enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
    try db.initOptionalEnrichmentRuntime(&config);
    const runtime = db.enrichment_runtime.?;
    runtime.worker_failed = true;
    runtime.last_error_name = "InvalidExtractorResponse";
    runtime.target_sequence = 1;
    try std.testing.expectError(error.EnrichmentWorkerFailed, runtime.catchUpUntil(1));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stage));
    try std.testing.expect(runtime.worker_failed);
    try std.testing.expectEqualStrings("InvalidExtractorResponse", runtime.last_error_name.?);
    try std.testing.expectEqual(@as(u64, 0), runtime.applied_sequence);
}

test "db graph fact generated replacement retains overlap and retires withdrawn edges" {
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("graph-indexed-replacement");
    defer tmp.cleanup();
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "facts", .kind = .graph, .config_json =
        \\{"source":{"artifact":"relations_v1","path":"$","nodes":{"source":"{{ _item.source }}","target":"{{ _item.target }}"},"edge":{"edge_id":"{{ _doc.key }}","type":"RELATES_TO"}},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"}}
    });
    var config: enrichment_runtime_mod.Config = .{ .enable_without_producers = true };
    try db.initOptionalEnrichmentRuntime(&config);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const rows = try a.alloc(struct { source: []const u8, target: []const u8 }, 96);
    for (0..2) |phase| {
        for (rows, 0..) |*row, i| row.* = .{ .source = "alice", .target = try std.fmt.allocPrint(a, "target:{d}", .{i + phase * 48}) };
        const raw = try std.json.Stringify.valueAlloc(a, .{ .relations = rows }, .{});
        try db.batch(.{ .writes = &.{.{ .key = "facts-owner", .value = raw }}, .sync_level = .write });
        try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
        try db.runDerivedUntil(db.core.nextDerivedSequence());
        const entry = db.core.index_manager.graphIndex("facts").?;
        const edges = try entry.index.getEdges(alloc, "alice", "RELATES_TO", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(rows.len, edges.len);
        var seen: [96]bool = @splat(false);
        for (edges) |edge| {
            const number = try std.fmt.parseInt(usize, edge.target["target:".len..], 10);
            try std.testing.expect(number >= phase * 48 and number < phase * 48 + rows.len);
            const offset = number - phase * 48;
            try std.testing.expect(!seen[offset]);
            seen[offset] = true;
        }
    }
    try db.batch(.{ .deletes = &.{"facts-owner"}, .sync_level = .write });
    try db.enrichment_runtime.?.catchUpUntilForDrain(db.core.nextDerivedSequence());
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const edges = try db.core.index_manager.graphIndex("facts").?.index.getEdges(alloc, "alice", "RELATES_TO", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
}
