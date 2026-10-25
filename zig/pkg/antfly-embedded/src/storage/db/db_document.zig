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

//! Document operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDb = db_zig.TestDb;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const allocation_bench_support_mod = @import("../../allocation_bench_support.zig");
const ant_json = @import("antfly-json");
const artifact_ids = @import("artifact/ids.zig");
const builtin = @import("builtin");
const derived_types = @import("derived/derived_types.zig");
const docstore_mod = @import("../docstore.zig");
const document_extraction_mod = @import("enrichment/document_extraction.zig");
const document_mutation_revision_mod = @import("../document_mutation_revision.zig");
const document_unit_fingerprint = @import("enrichment/document_unit_fingerprint.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const graph_edge_ttl_expiration = @import("graph_edge_ttl_expiration.zig");
const graph_query_mod = @import("../../graph/query.zig");
const hierarchy_navigation = @import("../hierarchy_navigation.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const introducer_mod = @import("../../introducer.zig");
const mapper = @import("document/mapper.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const platform = @import("antfly_platform");
const public_table_schema = @import("../../schema/mod.zig");
const range_state_mod = @import("range_state.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store = @import("relational_store.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const retained_effects_mod = @import("../retained_effects.zig");
const schema_mod = @import("../schema.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const types = @import("types.zig");
const Allocator = db_zig.Allocator;
const BatchProfile = db_zig.BatchProfile;
const CommittedBatchEffectsObserver = db_zig.CommittedBatchEffectsObserver;
const CountingDenseEmbedder = db_zig.CountingDenseEmbedder;
const DocumentArtifactChildRangeApplyBatch = db_zig.DocumentArtifactChildRangeApplyBatch;
const DocumentArtifactChildRangeDispatcher = db_zig.DocumentArtifactChildRangeDispatcher;
const DocumentChildRangeDispatchGroup = db_zig.DocumentChildRangeDispatchGroup;
const DocumentChildRangeRoutingSnapshot = db_zig.DocumentChildRangeRoutingSnapshot;
const DocumentExtractionUnitDescriptor = db_zig.DocumentExtractionUnitDescriptor;
const ManagedIndexBatchApplicability = db_zig.ManagedIndexBatchApplicability;
const OwnedEmbeddingArtifactWriteIdentity = db_zig.OwnedEmbeddingArtifactWriteIdentity;
const PortableRuntimeBatchPrelockTestHook = db_zig.PortableRuntimeBatchPrelockTestHook;
const TestAssetProducer = db_zig.TestAssetProducer;
const acquireReplicationMutationSharedContext = db_zig.acquireReplicationMutationSharedContext;
const acquireSnapshotReplayContext = db_zig.acquireSnapshotReplayContext;
const appendArtifactSourceRevisionWritesFromReplay = db_zig.appendArtifactSourceRevisionWritesFromReplay;
const appendAssetArtifactSourceIndexMutations = db_zig.appendAssetArtifactSourceIndexMutations;
const appendDocumentExtractionNavigationBackfill = db_zig.appendDocumentExtractionNavigationBackfill;
const appendJsonFieldU64 = db_zig.appendJsonFieldU64;
const appendJsonFieldUsize = db_zig.appendJsonFieldUsize;
const appendJsonString = db_zig.appendJsonString;
const appendUniqueBorrowedKeyWithSet = db_zig.appendUniqueBorrowedKeyWithSet;
const appendUniqueOwnedKeyIndexed = db_zig.appendUniqueOwnedKeyIndexed;
const applyDerivedBacklogPressureContext = db_zig.applyDerivedBacklogPressureContext;
const applyDerivedBatchProfiled = db_zig.applyDerivedBatchProfiled;
const applyDerivedBatchTargetsProfiled = db_zig.applyDerivedBatchTargetsProfiled;
const assetStateKeyAlloc = db_zig.assetStateKeyAlloc;
const benchMetricsEnabled = db_zig.benchMetricsEnabled;
const buildDocumentExtractionCatalogView = db_zig.buildDocumentExtractionCatalogView;
const collectDenseEmbeddingWritesForBatch = db_zig.collectDenseEmbeddingWritesForBatch;
const collectDocumentWritesProfiled = db_zig.collectDocumentWritesProfiled;
const collectManagedSyncTargets = db_zig.collectManagedSyncTargets;
const collectSparseEmbeddingWritesForBatch = db_zig.collectSparseEmbeddingWritesForBatch;
const collectSparseFieldWritesProfiled = db_zig.collectSparseFieldWritesProfiled;
const collectTextDocumentWritesForIndex = db_zig.collectTextDocumentWritesForIndex;
const contentPartsJsonAlloc = db_zig.contentPartsJsonAlloc;
const currentTimeNs = db_zig.currentTimeNs;
const decodeArtifactRefIfKnownAlloc = db_zig.decodeArtifactRefIfKnownAlloc;
const decodeEmbeddingArtifactWriteIdentityAlloc = db_zig.decodeEmbeddingArtifactWriteIdentityAlloc;
const decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc = db_zig.decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc;
const documentArtifactManifestFromJsonAlloc = db_zig.documentArtifactManifestFromJsonAlloc;
const documentExtractionFailureManifestPayloadAlloc = db_zig.documentExtractionFailureManifestPayloadAlloc;
const documentExtractionManifestPayloadAlloc = db_zig.documentExtractionManifestPayloadAlloc;
const documentExtractionStateChunkKeysAlloc = db_zig.documentExtractionStateChunkKeysAlloc;
const documentExtractionStateValueAlloc = db_zig.documentExtractionStateValueAlloc;
const documentExtractionStoredUnitFingerprintAlloc = db_zig.documentExtractionStoredUnitFingerprintAlloc;
const documentExtractionUnitDescriptorsFromKeysAlloc = db_zig.documentExtractionUnitDescriptorsFromKeysAlloc;
const documentExtractionUnitFingerprintAlloc = db_zig.documentExtractionUnitFingerprintAlloc;
const documentExtractionUnitRangeCount = db_zig.documentExtractionUnitRangeCount;
const documentExtractionUnitRangeIndex = db_zig.documentExtractionUnitRangeIndex;
const documentRangeLowerAlloc = db_zig.documentRangeLowerAlloc;
const documentRangeUpperAlloc = db_zig.documentRangeUpperAlloc;
const documentUnitHierarchyManifestInfo = db_zig.documentUnitHierarchyManifestInfo;
const documentUnitPayloadAlloc = db_zig.documentUnitPayloadAlloc;
const embeddingIdentityAllocationBenchmark = db_zig.embeddingIdentityAllocationBenchmark;
const embeddingWriteIdentitySweep = db_zig.embeddingWriteIdentitySweep;
const encodeChangeRecordPayload = db_zig.encodeChangeRecordPayload;
const enforceReplicationWriteGateOptional = db_zig.enforceReplicationWriteGateOptional;
const expectedChunkEmbeddingArtifactKeyAlloc = db_zig.expectedChunkEmbeddingArtifactKeyAlloc;
const expectedDocumentEmbeddingArtifactKeyAlloc = db_zig.expectedDocumentEmbeddingArtifactKeyAlloc;
const filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay = db_zig.filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay;
const filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay = db_zig.filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay;
const freeOptionalOwnedBytes = db_zig.freeOptionalOwnedBytes;
const freeOwnedConstKeySlice = db_zig.freeOwnedConstKeySlice;
const generatedEmbedBatchItems = db_zig.generatedEmbedBatchItems;
const hierarchyNavigationArtifactDigestAlloc = db_zig.hierarchyNavigationArtifactDigestAlloc;
const hierarchyNavigationBlockCount = db_zig.hierarchyNavigationBlockCount;
const hierarchy_navigation_unit_fingerprint_field = db_zig.hierarchy_navigation_unit_fingerprint_field;
const jsonOutputAllocationBenchmark = db_zig.jsonOutputAllocationBenchmark;
const jsonTestNumber = db_zig.jsonTestNumber;
const loadStoredSearchDocumentsMany = db_zig.loadStoredSearchDocumentsMany;
const lockApplyForPortableRuntimeContext = db_zig.lockApplyForPortableRuntimeContext;
const lockApplyShared = db_zig.lockApplyShared;
const lockAtomicWithBackoff = db_zig.lockAtomicWithBackoff;
const logBatchProfile = db_zig.logBatchProfile;
const managedIndexBatchApplicability = db_zig.managedIndexBatchApplicability;
const managedIndexRecordApplicability = db_zig.managedIndexRecordApplicability;
const missingAsNull = db_zig.missingAsNull;
const monotonicTimeNs = db_zig.monotonicTimeNs;
const no_background_test_open = db_zig.no_background_test_open;
const notifyExecutorForSyncLevelWithDenseBulkDeferral = db_zig.notifyExecutorForSyncLevelWithDenseBulkDeferral;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const ordinalProjectionFields = db_zig.ordinalProjectionFields;
const partitionRemoteDerivedDocuments = db_zig.partitionRemoteDerivedDocuments;
const platform_time = db_zig.platform_time;
const prepareGeneratedEnrichments = db_zig.prepareGeneratedEnrichments;
const projectLookupStoredBytes = db_zig.projectLookupStoredBytes;
const putLeakyJsonStringField = db_zig.putLeakyJsonStringField;
const putLeakyJsonU64Field = db_zig.putLeakyJsonU64Field;
const replayDocumentStoreKeyAlloc = db_zig.replayDocumentStoreKeyAlloc;
const replicationWriteGateAllowsBackgroundWork = db_zig.replicationWriteGateAllowsBackgroundWork;
const scraping = db_zig.scraping;
const shouldAppendSplitDelta = db_zig.shouldAppendSplitDelta;
const slow_test_wait_attempts = db_zig.slow_test_wait_attempts;
const sparseCollectorOwnershipSweep = db_zig.sparseCollectorOwnershipSweep;
const syncLevelRequiresDerivedVisibility = db_zig.syncLevelRequiresDerivedVisibility;
const testChildRangeDestination = db_zig.testChildRangeDestination;
const testChunkArtifactCount = db_zig.testChunkArtifactCount;
const testDefaultTextHitCount = db_zig.testDefaultTextHitCount;
const testLargeHtmlDataUrlAlloc = db_zig.testLargeHtmlDataUrlAlloc;
const testSourceDocumentJsonAlloc = db_zig.testSourceDocumentJsonAlloc;
const textCollectorFailureSweep = db_zig.textCollectorFailureSweep;
const textualAssetFullTextProjectionAlloc = db_zig.textualAssetFullTextProjectionAlloc;
const waitForDenseIndexResultsWithAttempts = db_zig.waitForDenseIndexResultsWithAttempts;
const waitForSearchResult = db_zig.waitForSearchResult;
const DocumentReadSession = DB.DocumentReadSession;
const appendDenseArtifactCounterMutations = DB.appendDenseArtifactCounterMutations;
const notifyQueryVisibilityTargetAdvanced = DB.notifyQueryVisibilityTargetAdvanced;
const notifyQueryVisibilityTargetAdvancedScoped = DB.notifyQueryVisibilityTargetAdvancedScoped;

pub fn batchWithDocumentArtifactChildRangeDispatcher(
    self: *DB,
    req: types.BatchRequest,
    dispatcher: DocumentArtifactChildRangeDispatcher,
) anyerror!void {
    if (benchMetricsEnabled()) {
        var profile = BatchProfile{};
        try self.batchInternal(req, &profile, .{ .document_child_range_dispatcher = dispatcher });
        logBatchProfile(req, profile);
    } else {
        try self.batchInternal(req, null, .{ .document_child_range_dispatcher = dispatcher });
    }
}

pub fn batchWithDocumentArtifactChildRangeDispatcherAndCommittedEffectsObserver(
    self: *DB,
    req: types.BatchRequest,
    dispatcher: ?DocumentArtifactChildRangeDispatcher,
    observer: CommittedBatchEffectsObserver,
) anyerror!void {
    if (benchMetricsEnabled()) {
        var profile = BatchProfile{};
        try self.batchInternal(req, &profile, .{
            .document_child_range_dispatcher = dispatcher,
            .committed_batch_effects_observer = observer,
        });
        logBatchProfile(req, profile);
    } else {
        try self.batchInternal(req, null, .{
            .document_child_range_dispatcher = dispatcher,
            .committed_batch_effects_observer = observer,
        });
    }
}

pub fn applyDocumentArtifactChildRangeBatch(self: *DB, child_batch: DocumentArtifactChildRangeApplyBatch) anyerror!u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, child_batch });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    if (child_batch.artifact_writes.len == 0 and
        child_batch.artifact_delete_keys.len == 0 and
        child_batch.documents.len == 0 and
        child_batch.dense_embeddings.len == 0 and
        child_batch.sparse_embeddings.len == 0 and
        child_batch.generated_enrichment_refs.len == 0)
    {
        return 0;
    }

    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    // Recheck after lock acquisition to close publication racing the fast
    // preflight performed by the caller.
    try self.lockApplyForPortableRuntime();
    var apply_mutex_held = true;
    errdefer if (apply_mutex_held) self.core.unlockApply();

    var store_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer store_writes.deinit(self.alloc);
    var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer delete_keys.deinit(self.alloc);
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
    var owned_delete_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_delete_keys.items) |key| self.alloc.free(key);
        owned_delete_keys.deinit(self.alloc);
    }
    var changed_artifact_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (changed_artifact_keys.items) |key| self.alloc.free(key);
        changed_artifact_keys.deinit(self.alloc);
    }
    var changed_artifact_key_set = std.StringHashMapUnmanaged(void).empty;
    defer changed_artifact_key_set.deinit(self.alloc);
    for (child_batch.artifact_writes) |write| {
        try store_writes.append(self.alloc, .{
            .key = write.key,
            .value = write.value,
        });
        try appendUniqueOwnedKeyIndexed(self.alloc, &changed_artifact_keys, &changed_artifact_key_set, write.key);
    }
    for (child_batch.artifact_delete_keys) |key| {
        try delete_keys.append(self.alloc, key);
        try appendUniqueOwnedKeyIndexed(self.alloc, &changed_artifact_keys, &changed_artifact_key_set, key);
    }

    // Graph artifact expansion belongs to the durable derived replay. Keep
    // foreground apply proportional to the admitted artifact bytes: the
    // replay record below owns graph materialization, retries, and the
    // sync-level visibility fence after this primary-store commit.
    if (child_batch.artifact_writes.len > 0) {
        try self.core.appendArtifactPresenceMarker(&store_writes);
    }
    try appendAssetArtifactSourceIndexMutations(
        self.alloc,
        &store_writes,
        child_batch.artifact_delete_keys,
        &delete_keys,
        &owned_store_keys,
        &owned_store_values,
        &owned_delete_keys,
    );
    const sequence = self.core.reserveDerivedAppendSequence();
    const derived_seed = derived_types.DerivedBatch{
        .sequence = sequence,
        .documents = child_batch.documents,
        .deleted_keys = child_batch.artifact_delete_keys,
        .changed_artifact_keys = changed_artifact_keys.items,
        .dense_embeddings = child_batch.dense_embeddings,
        .sparse_embeddings = child_batch.sparse_embeddings,
        .generated_enrichment_refs = child_batch.generated_enrichment_refs,
    };
    var derived_batch = try derived_types.cloneBatch(self.alloc, derived_seed);
    defer derived_types.deinitDerivedBatch(self.alloc, &derived_batch);
    derived_batch.sequence = sequence;

    var sync_targets = try collectManagedSyncTargets(self.alloc, self.core.index_manager, derived_batch);
    defer sync_targets.deinit(self.alloc);
    const replay_payload = try encodeChangeRecordPayload(&self.batchContext(), derived_batch, sequence);
    defer self.alloc.free(replay_payload);

    try appendArtifactSourceRevisionWritesFromReplay(
        self.alloc,
        replay_payload,
        sequence,
        &store_writes,
        &owned_store_keys,
        &owned_store_values,
    );

    try appendDenseArtifactCounterMutations(
        self.alloc,
        self.core.store,
        self.core.index_manager,
        &store_writes,
        delete_keys.items,
        &owned_store_keys,
        &owned_store_values,
    );
    var backlog_admission = try self.executor.admitBacklogBytes(@intCast(replay_payload.len));
    defer backlog_admission.cancel();
    try self.core.store.putBatchWithReplay(
        self.backend_runtime.io(),
        store_writes.items,
        delete_keys.items,
        .{
            .sequence = sequence,
            .payload = replay_payload,
        },
    );
    self.mirrorReplicationReplayPayloadBestEffort(replay_payload);
    if (shouldAppendSplitDelta(self)) {
        try self.core.appendSplitDelta(currentTimeNs(), store_writes.items, delete_keys.items);
    }

    self.executor.commitBacklogAdmission(sequence, &backlog_admission);
    self.core.unlockApply();
    apply_mutex_held = false;
    snapshot_mutation.release();
    notifyQueryVisibilityTargetAdvancedScoped(
        self.async_context,
        sequence,
        sync_targets.target_identities,
        sync_targets.target_scope_known,
    );

    var pressure_ctx = self.batchContext();
    try self.markPrecomputedEnrichmentAppliedForSync(child_batch.sync_level, sequence);
    try applyDerivedBacklogPressureContext(&pressure_ctx, sequence, child_batch.sync_level, sync_targets);
    if (self.executor.hasWorkers()) {
        notifyExecutorForSyncLevelWithDenseBulkDeferral(self.async_context, self.executor, child_batch.sync_level, sequence, sync_targets);
        try self.waitForSyncLevel(child_batch.sync_level, sequence, sync_targets);
    } else {
        if (syncLevelRequiresDerivedVisibility(child_batch.sync_level)) {
            if (child_batch.sync_level == .full_text) {
                try applyDerivedBatchTargetsProfiled(self, derived_batch, sync_targets.full_text_indexes, null);
            } else {
                try applyDerivedBatchProfiled(self, derived_batch, null);
            }
        }
        try self.waitForSyncLevel(child_batch.sync_level, sequence, sync_targets);
    }
    if (self.enrichment_runtime) |runtime| runtime.notifySequence(sequence);
    self.notifyResolverReplayRuntimes(sequence);
    return sequence;
}

pub fn getDocumentArtifactManifest(
    self: *DB,
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
) !?types.DocumentArtifactManifest {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, doc_key, artifact_name });
    try self.maybeFinalizePendingRowPolicyPublication();
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    lockApplyShared(self);
    defer self.core.unlockApplyShared();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", artifact_name);
    defer alloc.free(manifest_key);
    const manifest = try self.core.getStoreValue(alloc, manifest_key) orelse return null;
    var manifest_owned = true;
    errdefer if (manifest_owned) alloc.free(manifest);

    const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_name);
    defer alloc.free(state_key);
    const state = try missingAsNull(self.core.getStoreValue(alloc, state_key));
    var state_owned = true;
    errdefer if (state_owned) if (state) |value| alloc.free(value);

    manifest_owned = false;
    state_owned = false;
    var result = try documentArtifactManifestFromJsonAlloc(alloc, doc_key, artifact_name, manifest, state);
    errdefer result.deinit(alloc);
    try row_policy_lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    return result;
}

pub fn listDocumentArtifactManifests(
    self: *DB,
    alloc: Allocator,
    doc_key: []const u8,
) !types.DocumentArtifactManifestList {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, doc_key });
    try self.maybeFinalizePendingRowPolicyPublication();
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    defer row_policy_lease.release();
    lockApplyShared(self);
    defer self.core.unlockApplyShared();

    const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "asset");
    defer alloc.free(prefix);
    const rows = try self.core.scanStorePrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, rows);

    var artifacts = std.ArrayListUnmanaged(types.DocumentArtifactManifest).empty;
    errdefer {
        for (artifacts.items) |*artifact| artifact.deinit(alloc);
        artifacts.deinit(alloc);
    }

    for (rows) |row| {
        var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, row.key)) orelse continue;
        defer artifact_ref.deinit(alloc);
        if (artifact_ref.kind != .asset) continue;
        if (artifact_ref.unit_id != null) continue;
        if (!std.mem.eql(u8, artifact_ref.document_id, doc_key)) continue;

        const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_ref.name);
        defer alloc.free(state_key);
        const state = try missingAsNull(self.core.getStoreValue(alloc, state_key));
        errdefer if (state) |value| alloc.free(value);

        const manifest_json = try alloc.dupe(u8, row.value);
        errdefer alloc.free(manifest_json);
        try artifacts.append(
            alloc,
            try documentArtifactManifestFromJsonAlloc(alloc, doc_key, artifact_ref.name, manifest_json, state),
        );
    }

    var result: types.DocumentArtifactManifestList = .{
        .document_id = try alloc.dupe(u8, doc_key),
        .artifacts = try artifacts.toOwnedSlice(alloc),
    };
    errdefer result.deinit(alloc);
    try row_policy_lease.checkAt(@intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    return result;
}

pub fn updateDocumentArtifactChildRangePlacement(
    self: *DB,
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    update: types.DocumentArtifactChildRangePlacementUpdate,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, doc_key, artifact_name, update });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    try self.lockApplyForPortableRuntime();
    var apply_mutex_held = true;
    errdefer if (apply_mutex_held) self.core.unlockApply();

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", artifact_name);
    defer alloc.free(manifest_key);
    const manifest = try self.core.getStoreValue(alloc, manifest_key) orelse {
        self.core.unlockApply();
        apply_mutex_held = false;
        return false;
    };
    defer alloc.free(manifest);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    var manifest_value = try std.json.parseFromSliceLeaky(std.json.Value, arena_alloc, manifest, .{ .allocate = .alloc_always });
    if (manifest_value != .object) return error.InvalidArgument;
    const child_ranges = manifest_value.object.getPtr("child_ranges") orelse return error.InvalidArgument;
    if (child_ranges.* != .array) return error.InvalidArgument;

    var changed = false;
    for (child_ranges.array.items) |*item| {
        if (item.* != .object) return error.InvalidArgument;
        const range_id = item.object.get("range_id") orelse return error.InvalidArgument;
        if (range_id != .string) return error.InvalidArgument;
        if (!std.mem.eql(u8, range_id.string, update.range_id)) continue;

        try putLeakyJsonStringField(arena_alloc, &item.object, "placement", update.placement);
        if (update.owner_group_id) |owner_group_id| try putLeakyJsonU64Field(arena_alloc, &item.object, "owner_group_id", owner_group_id);
        if (update.placement_generation) |placement_generation| try putLeakyJsonU64Field(arena_alloc, &item.object, "placement_generation", placement_generation);
        if (update.route_status) |route_status| try putLeakyJsonStringField(arena_alloc, &item.object, "route_status", route_status);
        if (update.split_eligible) |split_eligible| try item.object.put(arena_alloc, "split_eligible", .{ .bool = split_eligible });
        changed = true;
        break;
    }

    if (!changed) {
        self.core.unlockApply();
        apply_mutex_held = false;
        return false;
    }

    const updated_manifest = try std.json.Stringify.valueAlloc(alloc, manifest_value, .{});
    defer alloc.free(updated_manifest);

    const sequence = self.core.reserveDerivedAppendSequence();
    const changed_artifact_keys = [_][]const u8{manifest_key};
    const replay_payload = try encodeChangeRecordPayload(&self.batchContext(), .{
        .sequence = sequence,
        .changed_artifact_keys = changed_artifact_keys[0..],
    }, sequence);
    defer alloc.free(replay_payload);

    const writes = [_]docstore_mod.KVPair{.{
        .key = manifest_key,
        .value = updated_manifest,
    }};
    var backlog_admission = try self.executor.admitBacklogBytes(@intCast(replay_payload.len));
    defer backlog_admission.cancel();
    try self.core.store.putBatchWithReplay(self.backend_runtime.io(), writes[0..], &.{}, .{
        .sequence = sequence,
        .payload = replay_payload,
    });
    self.mirrorReplicationReplayPayloadBestEffort(replay_payload);
    self.executor.commitBacklogAdmission(sequence, &backlog_admission);
    self.core.unlockApply();
    apply_mutex_held = false;
    snapshot_mutation.release();
    notifyQueryVisibilityTargetAdvanced(self.async_context, sequence);

    if (self.executor.hasWorkers()) {
        self.executor.forceSequence(sequence);
    } else {
        self.executor.notifySequence(sequence);
    }
    self.notifyResolverReplayRuntimes(sequence);
    return true;
}

/// Public reprocessing acknowledges durable submission, not completion of
/// every enrichment/index on the source. Completion is observed through
/// artifact status; unrelated retrying providers must not fail submission.
pub fn reprocessDocumentArtifact(
    self: *DB,
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, doc_key, artifact_name });
    return self.reprocessDocumentArtifactWithCompletionWait(alloc, doc_key, artifact_name, false);
}

pub fn reprocessDocumentArtifactWithCompletionWait(
    self: *DB,
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    wait_for_completion: bool,
) !bool {
    var cfg = (try self.getEnrichment(alloc, .asset, artifact_name)) orelse
        return self.reprocessEmbeddingArtifactIssue(alloc, .{
            .artifact_kind = .embedding,
            .doc_key = doc_key,
            .artifact_name = artifact_name,
        }, wait_for_completion);
    defer cfg.deinit(alloc);
    // A missing producer definition means this artifact cannot currently be
    // regenerated. A missing source document is a distinct, actionable
    // repair outcome and must reach the repair API as `missing_source_docs`.
    const value = try self.get(alloc, doc_key) orelse return error.NotFound;
    defer alloc.free(value);

    const writes = [_]types.BatchWrite{.{ .key = doc_key, .value = value }};
    const force_artifacts = [_][]const u8{artifact_name};
    // Preserve forced generation's precommit planning. A write-only sync
    // level would lose the force flag for unchanged cached asset outputs.
    // Public acceptance only skips the postcommit, table-wide visibility wait.
    try self.batchInternal(.{
        .writes = &writes,
        .sync_level = .full_index,
    }, null, .{
        .wait_for_sync_level = wait_for_completion,
        .force_generated_artifact_names = &force_artifacts,
    });
    return true;
}

pub fn reprocessDocumentEmbeddingArtifact(
    self: *DB,
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
) !bool {
    const issue = types.ArtifactRepairIssue{
        .artifact_kind = .embedding,
        .doc_key = doc_key,
        .artifact_name = artifact_name,
    };
    return self.reprocessEmbeddingArtifactIssue(alloc, issue, false) catch |err| switch (err) {
        error.NotFound => false,
        else => return err,
    };
}

pub fn reprocessDocumentArtifactRange(
    self: *DB,
    alloc: Allocator,
    artifact_name: []const u8,
    req: types.DocumentArtifactTableReprocessRequest,
) !types.DocumentArtifactTableReprocessResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, artifact_name, req });
    var cfg = (try self.getEnrichment(alloc, .asset, artifact_name)) orelse
        (try self.getEnrichment(alloc, .embedding, artifact_name)) orelse return error.NotFound;
    defer cfg.deinit(alloc);

    var effective_req = req;
    if (req.shard_cursors.len > 0) {
        if (req.shard_cursors.len != 1) return error.InvalidArgument;
        const cursor = req.shard_cursors[0];
        if (cursor.group_id != null) return error.InvalidArgument;
        effective_req = .{
            .from_key = cursor.next_key,
            .to_key = req.to_key,
            .limit = if (cursor.limit != 0) cursor.limit else req.limit,
        };
    }

    const limit = if (effective_req.limit == 0) @as(u32, 100) else effective_req.limit;
    const scan_limit = if (limit == std.math.maxInt(u32)) limit else limit + 1;
    var scanned = try self.scan(alloc, effective_req.from_key, effective_req.to_key, .{
        .include_documents = true,
        .limit = scan_limit,
    });
    defer scanned.deinit(alloc);

    var failures = std.ArrayListUnmanaged(types.DocumentArtifactReprocessFailure).empty;
    errdefer {
        for (failures.items) |*failure| failure.deinit(alloc);
        failures.deinit(alloc);
    }

    var result = types.DocumentArtifactTableReprocessResult{
        .limit = limit,
    };
    errdefer result.deinit(alloc);

    const process_len = @min(scanned.documents.len, @as(usize, @intCast(limit)));
    for (scanned.documents[0..process_len]) |doc| {
        result.scanned += 1;
        const handled = self.reprocessDocumentArtifact(alloc, doc.id, artifact_name) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                result.failed += 1;
                try failures.append(alloc, .{
                    .key = try alloc.dupe(u8, doc.id),
                    .error_code = try alloc.dupe(u8, @errorName(err)),
                });
                continue;
            },
        };
        if (handled) {
            result.reprocessed += 1;
        } else {
            result.skipped += 1;
        }
    }

    if (scanned.documents.len > process_len and process_len > 0) {
        result.next_key = try alloc.dupe(u8, scanned.documents[process_len - 1].id);
    }
    result.failures = try failures.toOwnedSlice(alloc);
    return result;
}

pub fn getDocument(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, key, opts });
    return try self.lookup(alloc, key, opts);
}

/// Replaces all primary documents and their range in a
/// single durable commit. Derived indexes consume the same replacement
/// batch, so stale documents are removed instead of surviving restore.
pub fn replacePrimaryDocumentsAndRange(
    self: *DB,
    alloc: Allocator,
    byte_range: types.ByteRange,
    writes: []const types.BatchWrite,
) !void {
    lockAtomicWithBackoff(&self.generation_replace_mutex);
    defer self.generation_replace_mutex.unlock();
    if (byte_range.start.len > 0 and byte_range.end.len > 0 and
        std.mem.order(u8, byte_range.start, byte_range.end) != .lt)
    {
        return error.InvalidAppliedDataRange;
    }
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.ensureTotalCapacity(alloc, @intCast(writes.len));
    for (writes) |write| {
        if (!byte_range.contains(write.key)) return error.KeyOutOfRange;
        const result = try seen.getOrPut(alloc, write.key);
        if (result.found_existing) return error.DuplicateDocumentKey;
    }

    const lower = try internal_keys.documentRangeLowerAlloc(alloc, "");
    defer alloc.free(lower);
    const upper = (try internal_keys.documentRangeUpperAlloc(alloc, "")) orelse return error.InvalidAppliedDataRange;
    defer alloc.free(upper);
    lockApplyShared(self);
    const existing_documents = self.core.scanStoreRange(alloc, lower, upper) catch |err| {
        self.core.unlockApplyShared();
        return err;
    };
    self.core.unlockApplyShared();
    defer docstore_mod.DocStore.freeResults(alloc, existing_documents);

    var deletes = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deletes.items) |key| alloc.free(key);
        deletes.deinit(alloc);
    }
    for (existing_documents) |entry| {
        const logical_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, entry.key)) orelse continue;
        if (seen.contains(logical_key)) {
            alloc.free(logical_key);
            continue;
        }
        errdefer alloc.free(logical_key);
        try deletes.append(alloc, logical_key);
    }

    const range_value = try range_state_mod.encodeRangeAlloc(alloc, byte_range);
    defer alloc.free(range_value);
    const range_write: docstore_mod.KVPair = .{ .key = range_state_mod.range_key, .value = range_value };
    try self.batchInternal(.{
        .writes = writes,
        .deletes = deletes.items,
        .sync_level = .write,
    }, null, .{
        .validate_range_ownership = false,
        .wait_for_sync_level = false,
        .bypass_replication_write_gate = true,
        .extra_store_writes = &.{range_write},
    });
    try self.refreshSplitBootstrapRangeInMemory(byte_range);
}

/// Raw primary outcomes for a transition snapshot. Do not use public scan
/// projection here: it can hydrate derived fields into the returned JSON.
pub fn mergeDocumentsPage(self: *DB, alloc: Allocator, byte_range: types.ByteRange, after_key: ?[]const u8) ![]types.BatchWrite {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, byte_range, after_key });
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    const lower = if (after_key) |key| try internal_keys.documentKeyAlloc(alloc, key) else try documentRangeLowerAlloc(alloc, byte_range.start);
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
    var next = try cursor.seekAtOrAfter(lower);
    while (next) |row| : (next = try cursor.next()) {
        if (upper) |bound| if (std.mem.order(u8, row.key, bound) != .lt) break;
        if (!internal_keys.isStoredDocumentRowKey(row.key)) continue;
        const key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, row.key)).?;
        errdefer alloc.free(key);
        if (after_key) |after| if (std.mem.order(u8, key, after) != .gt) {
            alloc.free(key);
            continue;
        };
        const value = if (internal_keys.isRelationalRowKey(row.key)) blk: {
            const version = try relational_store.rowSchemaVersion(row.value);
            var view = (try self.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            defer view.release();
            const typed = try relational_row_codec.ordinalRowView(row.value, view.tableSchema().*, view.physicalLayout());
            break :blk try typed.reconstructValueAlloc(alloc);
        } else try alloc.dupe(u8, row.value);
        errdefer alloc.free(value);
        try rows.append(alloc, .{ .key = key, .value = value });
        bytes +|= key.len +| value.len;
        if (rows.items.len >= 128 or bytes >= 1024 * 1024) break;
    }
    return rows.toOwnedSlice(alloc);
}

/// Internal search-kernel indexing boundary. Source documents are already
/// projected and are published only to the production full-text index.
pub fn indexTextKernelDocuments(
    self: *DB,
    index_name: []const u8,
    docs: []const introducer_mod.TextDocument,
) !usize {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    try self.enforcePortableRuntimeGate();
    const reservation_limit = if (self.text_merge_runtime) |runtime| runtime.producerSegmentReservationLimit() else std.math.maxInt(usize);
    var publication_plan = try self.core.index_manager.planTextKernelPublication(self.alloc, docs, reservation_limit);
    defer publication_plan.deinit();
    var segment_count: usize = 0;
    var start: usize = 0;
    for (publication_plan.chunks) |planned| {
        const chunk = docs[start..planned.end];
        {
            var merge_permit: ?text_merge_runtime_mod.TextMergeRuntime.ProducerPermit = null;
            if (self.text_merge_runtime) |runtime| {
                merge_permit = try runtime.acquireProducerPermit(index_name, planned.estimate.segment_count, planned.estimate.byte_count);
            }
            defer if (merge_permit) |*permit| permit.release();
            var snapshot_replay = try self.acquireSnapshotReplayMutation();
            defer snapshot_replay.release();
            try self.lockApplyForPortableRuntime();
            const published = self.core.index_manager.indexTextKernelDocuments(index_name, chunk) catch |err| {
                self.core.unlockApply();
                return err;
            };
            self.core.unlockApply();
            segment_count += published;
            if (self.text_merge_runtime) |runtime| runtime.notify();
        }
        start = planned.end;
    }
    std.debug.assert(start == docs.len);
    return segment_count;
}

/// Local derived-cache metadata, not a user mutation or replication fact.
/// Leadership and root-image gates cover the short publication transaction;
/// hashing and scanning happen beforehand under a disposable read snapshot.
pub fn advanceDocumentAuthorityBackfillPage(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call("advanceDocumentAuthorityBackfillPage", .{self});
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return true;
    if (!replicationWriteGateAllowsBackgroundWork(self.local_execution.replication_write_gate)) return false;
    if (!self.document_authority_backfill_mutex.tryLock()) return false;
    defer self.document_authority_backfill_mutex.unlock();
    if (self.document_authority_repairs.pending.load(.acquire) and !self.document_authority_repairs_yield_sweep) {
        var page = self.document_authority_repairs.take() orelse return false;
        defer page.deinit(self.alloc);
        errdefer if (!page.retry(&self.document_authority_repairs, self.alloc)) self.document_authority_backfill_requested.store(true, .release);
        var repair_ctx = self.batchContext();
        var repair_mutation = acquireReplicationMutationSharedContext(&repair_ctx);
        defer if (repair_mutation) |*lease| lease.release();
        try enforceReplicationWriteGateOptional(repair_ctx.replication_write_gate);
        var repair_replay = try acquireSnapshotReplayContext(&repair_ctx);
        defer if (repair_replay) |*lease| lease.release();
        try lockApplyForPortableRuntimeContext(&repair_ctx);
        defer repair_ctx.apply_mutex.unlockExclusive();
        var writer = try self.core.store.beginWriteTxn();
        errdefer writer.abort();
        _ = try page.stage(&writer);
        try writer.commit();
        self.document_authority_repairs_yield_sweep = self.document_authority_backfill_active.load(.acquire) or self.document_authority_backfill_requested.load(.acquire);
        return !self.document_authority_repairs_yield_sweep and !self.document_authority_repairs.pending.load(.acquire);
    }
    self.document_authority_repairs_yield_sweep = false;
    const restart = if (!self.document_authority_backfill_active.load(.acquire)) self.document_authority_backfill_requested.swap(false, .acq_rel) else false;
    if (!self.document_authority_backfill_active.load(.acquire) and !restart) return !self.document_authority_repairs.pending.load(.acquire);
    self.document_authority_backfill_active.store(true, .release);
    self.document_authority_backfill_restart = self.document_authority_backfill_restart or restart;
    var ctx = self.batchContext();
    var replication_mutation = acquireReplicationMutationSharedContext(&ctx);
    defer if (replication_mutation) |*lease| lease.release();
    try enforceReplicationWriteGateOptional(ctx.replication_write_gate);
    var snapshot_replay = try acquireSnapshotReplayContext(&ctx);
    defer if (snapshot_replay) |*lease| lease.release();
    var budget = if (self.core.index_manager.resource_manager) |manager|
        resource_manager_mod.BudgetedAllocator.init(manager, .relational_preparation_working_set, self.alloc, 1)
    else
        null;
    defer if (budget) |*value| value.deinit();
    const scratch = if (budget) |*value| value.allocator() else self.alloc;
    var prepared = (document_mutation_revision_mod.BackfillPage.prepare(scratch, self.core.store, self.document_authority_backfill_restart) catch |err| {
        if (err == error.OutOfMemory) if (budget) |*value| if (value.denied()) return error.ResourceBudgetExceeded;
        return err;
    }) orelse {
        self.document_authority_backfill_active.store(false, .release);
        return !self.document_authority_repairs.pending.load(.acquire);
    };
    defer prepared.deinit();
    try lockApplyForPortableRuntimeContext(&ctx);
    defer ctx.apply_mutex.unlockExclusive();
    var writer = try self.core.store.beginWriteTxn();
    errdefer writer.abort();
    _ = try prepared.stage(&writer);
    try writer.commit();
    self.document_authority_backfill_restart = false;
    self.document_authority_backfill_active.store(!prepared.complete, .release);
    return prepared.complete and !self.document_authority_repairs.pending.load(.acquire);
}

pub fn openDocumentReadSession(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !*DocumentReadSession {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, from_key, to_key, opts });
    try self.maybeFinalizePendingRowPolicyPublication();
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    errdefer row_policy_lease.release();
    try self.lockApplySharedForPortableRuntime();
    var locked = true;
    defer if (locked) self.core.unlockApplyShared();
    var schema = self.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    const borrowed = self.core.byteRange();
    const start = try alloc.dupe(u8, borrowed.start);
    defer alloc.free(start);
    const end = try alloc.dupe(u8, borrowed.end);
    defer alloc.free(end);
    const txn = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
    const now = currentTimeNs();
    self.core.unlockApplyShared();
    locked = false;
    const session = try DocumentReadSession.openSnapshot(alloc, self.core.store, txn, schema, .{ .start = start, .end = end }, from_key, to_key, opts, now);
    session.row_policy_lease = row_policy_lease;
    return session;
}

pub fn attachDistributedDocumentAuthority(self: *DB, alloc: Allocator, req: types.SearchRequest, result: *types.SearchResult) !void {
    if (req.document_authority_mode == .none) return;
    if (req.document_authority_group == 0 or req.include_stored or req.return_mode != .parent or req.highlight != null or req.graph_queries.len != 0 or req.aggregations_json.len != 0) return error.InvalidQueryRequest;
    var schema_view = self.core.acquireSchemaView();
    defer if (schema_view) |*view| view.release();
    try self.attachDistributedDocumentAuthorityAtSnapshot(alloc, req, result, null, schema_view);
}

pub fn loadPatternProjectedDocumentCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    query: graph_query_mod.GraphQuery,
    key: []const u8,
) anyerror!?[]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return if (try self.loadStoredSearchDocumentProbe(alloc, key)) |stored|
        try projectLookupStoredBytes(self, alloc, key, stored, .{
            .fields = query.fields,
            .include_all_fields = query.include_all_fields,
        })
    else
        null;
}

pub fn loadPatternProjectedDocumentsCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    query: graph_query_mod.GraphQuery,
    keys: []const []const u8,
) anyerror![]?[]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    const loaded = try loadStoredSearchDocumentsMany(self, alloc, keys, ordinalProjectionFields(query.fields, query.include_all_fields));
    errdefer freeOptionalOwnedBytes(alloc, loaded);
    for (loaded, keys, 0..) |maybe_stored, key, i| {
        const stored = maybe_stored orelse continue;
        const projected = try projectLookupStoredBytes(self, alloc, key, stored, .{
            .fields = query.fields,
            .include_all_fields = query.include_all_fields,
        });
        alloc.free(stored);
        loaded[i] = projected;
    }
    return loaded;
}

test "document extraction inline source size uses remote content limit" {
    const security = scraping.ContentSecurityConfig{ .max_download_size_bytes = 4 };
    var remote_content = scraping.RemoteContentConfig{ .security = security };
    try std.testing.expect(try document_extraction_mod.inlineDataUriSourceTooLarge(&remote_content, "data:text/plain;base64,aGVsbG8="));
    try std.testing.expect(!try document_extraction_mod.inlineDataUriSourceTooLarge(&remote_content, "data:text/plain;base64,Zm9v"));
    try std.testing.expect(!try document_extraction_mod.inlineDataUriSourceTooLarge(&remote_content, "data:"));
}

test "document extraction templated inline source size is rejected before persistence" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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

test "db document extraction OCR default is scoped to PDF routes" {
    const alloc = std.testing.allocator;
    var defaults = try document_extraction_mod.parseConfig(alloc, "{}");
    defer defaults.deinit(alloc);
    try std.testing.expect(document_extraction_mod.ocrEnabledForRoute(defaults, "pdf"));
    try std.testing.expect(!document_extraction_mod.ocrEnabledForRoute(defaults, "image"));

    var explicit_image = try document_extraction_mod.parseConfig(alloc, "{\"ocr\":true}");
    defer explicit_image.deinit(alloc);
    try std.testing.expect(document_extraction_mod.ocrEnabledForRoute(explicit_image, "image"));
}

test "db document extraction failure manifest preserves prior artifacts" {
    const alloc = std.testing.allocator;
    const unit_keys = [_][]const u8{"unit:1"};
    const chunk_keys = [_][]const u8{"chunk:1"};
    const manifest = try documentExtractionFailureManifestPayloadAlloc(
        alloc,
        "doc:1",
        "document_units_v1",
        "https://example.test/document.pdf",
        &unit_keys,
        &chunk_keys,
        &.{},
        4,
        5,
        "RemoteDocumentFetchFailed",
        "remote content download failed",
        "remote_content_download",
    );
    defer alloc.free(manifest);

    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"unit_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"chunk_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"start_key\":\"unit:1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"start_key\":\"chunk:1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"last_error\":{\"code\":\"RemoteDocumentFetchFailed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"stage\":\"remote_content_download\"") != null);
}

test "document extraction template prompt failure is rejected before persistence" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .template = "<<<error:status=413 message=StreamTooLong>>> fallback text",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });

    try std.testing.expectError(error.PermanentPromptFailure, db.batch(.{
        .writes = &.{.{
            .key = "doc:templated-prompt-failure",
            .value = "{\"url\":\"data:text/plain;base64,Zm9v\"}",
        }},
        .sync_level = .write,
    }));
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

    const fingerprint = try documentExtractionUnitFingerprintAlloc(alloc, unit);
    defer alloc.free(fingerprint);
    const legacy_fingerprint = try document_unit_fingerprint.legacyFingerprintAlloc(alloc, unit);
    defer alloc.free(legacy_fingerprint);
    try std.testing.expect(document_unit_fingerprint.isCurrent(fingerprint));
    try std.testing.expect(!std.mem.eql(u8, fingerprint, legacy_fingerprint));
    const payload = try documentUnitPayloadAlloc(alloc, "doc:a", "document_units_v1", unit, fingerprint, "data:application/pdf;base64,AA==", "application/pdf", .{ .range_id = "range:000000" });
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

    // Pre-marker payloads remain readable during rolling upgrades. Their
    // fingerprint is reconstructed with the legacy encoding from the stored
    // unit fields,
    // while malformed legacy provenance is rejected instead of trapping.
    _ = parsed.value.object.orderedRemove(hierarchy_navigation_unit_fingerprint_field);
    const legacy_payload = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(legacy_payload);
    const reconstructed = try documentExtractionStoredUnitFingerprintAlloc(alloc, legacy_payload);
    defer alloc.free(reconstructed);
    try std.testing.expectEqualStrings(legacy_fingerprint, reconstructed);

    const malformed_payload =
        \\{"unit_id":"page:000001","unit_type":"page","text":"hello","ocr_attempted":false,"provenance":{"method":"pdf_text","ocr_used":false,"transcript_used":false,"text_regions":[{"span":["bad",5],"bbox":[72,700,120,712]}]}}
    ;
    try std.testing.expectError(
        error.InvalidDocumentExtractionState,
        documentExtractionStoredUnitFingerprintAlloc(alloc, malformed_payload),
    );
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

    const fingerprint = try documentExtractionUnitFingerprintAlloc(alloc, unit);
    defer alloc.free(fingerprint);
    const payload = try documentUnitPayloadAlloc(alloc, "doc:a", "document_units_v1", unit, fingerprint, "data:application/pdf;base64,AA==", "application/pdf", .{ .range_id = "range:000000" });
    defer alloc.free(payload);
    try std.testing.expect(document_unit_fingerprint.isCurrent(fingerprint));

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

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
    // A source may have multiple asset enrichments. Opaque non-document
    // payloads must never be sniffed as extraction manifests by traversal.
    try db.addEnrichment(.{
        .name = "source_copy_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "text/plain",
        .producer_json = "{\"type\":\"copy\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\"}",
        }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 0), fake.calls);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const state_key = try assetStateKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(state_key);
    const navigation_summary_key = try internal_keys.documentUnitNavigationSummaryKeyAlloc(alloc, "doc:a", "document_units_v1");
    defer alloc.free(navigation_summary_key);
    const navigation_block_key = try internal_keys.documentUnitNavigationBlockKeyAlloc(alloc, "doc:a", "document_units_v1", 0);
    defer alloc.free(navigation_block_key);

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
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_artifact_unit_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"_hierarchy\"") == null);

    const state = try db.core.store.get(alloc, state_key);
    defer alloc.free(state);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"kind\":\"document_extraction_state_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "\"unit_descriptors\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, state, "document:000001") != null);

    const navigation_summary = try db.core.store.get(alloc, navigation_summary_key);
    defer alloc.free(navigation_summary);
    try ant_json.testing.expectSubsetJsonText(
        alloc,
        "{\"kind\":\"document_unit_navigation_summary_v1\",\"generation\":1,\"unit_count\":1,\"block_count\":1}",
        navigation_summary,
    );

    const navigation_block = try db.core.store.get(alloc, navigation_block_key);
    defer alloc.free(navigation_block);
    try std.testing.expect(std.mem.indexOf(u8, navigation_block, "\"kind\":\"document_unit_navigation_block_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, navigation_block, "document:000001") != null);

    // Simulate an artifact retained from before compact navigation metadata.
    // An unchanged-source enrichment pass must backfill from descriptors while
    // preserving the manifest generation, rather than downloading/extracting.
    var legacy_state_parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
    defer legacy_state_parsed.deinit();
    _ = legacy_state_parsed.value.object.orderedRemove("navigation_digest");
    _ = legacy_state_parsed.value.object.orderedRemove("navigation_block_count");
    _ = legacy_state_parsed.value.object.orderedRemove("navigation_block_size");
    const legacy_state = try std.json.Stringify.valueAlloc(alloc, legacy_state_parsed.value, .{});
    defer alloc.free(legacy_state);
    try db.core.store.put(state_key, legacy_state);
    try db.core.store.delete(navigation_summary_key);
    try db.core.store.delete(navigation_block_key);
    var backfill_writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (backfill_writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        backfill_writes.deinit(alloc);
    }
    var backfill_deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (backfill_deletes.items) |key| alloc.free(@constCast(key));
        backfill_deletes.deinit(alloc);
    }
    try appendDocumentExtractionNavigationBackfill(
        alloc,
        &db,
        "doc:a",
        "document_units_v1",
        "6c1146dc037bb3ca77d4248754322b14697b236ddaddf5203b13ab97f133fb78",
        legacy_state,
        (try documentUnitHierarchyManifestInfo(alloc, manifest)).published_generation.?,
        &backfill_writes,
        &backfill_deletes,
    );
    for (backfill_writes.items) |write| try db.core.store.put(write.key, write.value);
    for (backfill_deletes.items) |key| try db.core.store.delete(key);
    const backfilled_manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(backfilled_manifest);
    try std.testing.expectEqualStrings(manifest, backfilled_manifest);
    const backfilled_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(backfilled_state);
    const backfilled_summary = try db.core.store.get(alloc, navigation_summary_key);
    defer alloc.free(backfilled_summary);
    const backfilled_block = try db.core.store.get(alloc, navigation_block_key);
    defer alloc.free(backfilled_block);
    try std.testing.expect(try hierarchy_navigation.indexMetadataMatches(alloc, backfilled_state, backfilled_summary, 1));

    const navigation_order = [_]types.SortField{
        .{ .field = "_hierarchy.position" },
        .{ .field = "_id" },
    };

    // A missing summary is only eligible for legacy fallback when the state
    // predates all navigation metadata. Current-format state must never be
    // traversed through that fallback during a partial publication.
    try db.core.store.delete(navigation_summary_key);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(navigation_summary_key, backfilled_summary);

    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .query = .{ .match_none = {} },
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .include_all_fields = false,
    }));
    var navigation = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
        .fields = &.{ "unit_id", "unit_type", "text", hierarchy_navigation_unit_fingerprint_field },
    });
    defer navigation.deinit();
    try std.testing.expectEqual(@as(u32, 1), navigation.total_hits);
    try std.testing.expectEqual(@as(usize, 1), navigation.hits.len);
    try std.testing.expectEqual(@as(usize, 2), navigation.hits[0].sort_values.len);
    try std.testing.expect(std.mem.startsWith(u8, navigation.hits[0].sort_values[0].string, "hn3/"));
    try std.testing.expect(std.mem.indexOf(u8, navigation.hits[0].sort_values[0].string, "/646f63756d656e745f756e6974735f7631/") != null);
    try std.testing.expectEqualStrings("document:000001", navigation.hits[0].artifact_ref.?.unit_id.?);
    var projected_unit = try std.json.parseFromSlice(std.json.Value, alloc, navigation.hits[0].stored_data.?, .{});
    defer projected_unit.deinit();
    try ant_json.testing.expectSubsetJsonValue(
        alloc,
        "{\"unit_id\":\"document:000001\",\"text\":\"alpha beta\"}",
        projected_unit.value,
    );
    try std.testing.expect(projected_unit.value.object.get("_hierarchy") == null);
    try std.testing.expect(projected_unit.value.object.get(hierarchy_navigation_unit_fingerprint_field) == null);

    // A reader that races an extraction must never combine the old summary
    // with artifacts being rewritten under an in-progress manifest.
    try db.core.store.put(
        manifest_key,
        "{\"artifact_type\":\"document_units\",\"generation\":1," ++
            "\"merge_plan\":{\"from_generation\":1,\"to_generation\":2,\"status\":\"in_progress\"}}",
    );
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(
        manifest_key,
        "{\"artifact_type\":\"document_units\",\"generation\":2," ++
            "\"merge_plan\":{\"from_generation\":1,\"to_generation\":2,\"status\":\"failed\"}}",
    );
    try std.testing.expectError(error.DocumentArtifactPublicationFailed, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(manifest_key, manifest);

    // Independently read records are cross-checked by revision so even an
    // unexpected non-atomic store implementation fails closed.
    var mismatched_summary_parsed = try std.json.parseFromSlice(std.json.Value, alloc, backfilled_summary, .{});
    defer mismatched_summary_parsed.deinit();
    mismatched_summary_parsed.value.object.getPtr("generation").?.* = .{ .integer = 2 };
    const mismatched_summary = try std.json.Stringify.valueAlloc(alloc, mismatched_summary_parsed.value, .{});
    defer alloc.free(mismatched_summary);
    try db.core.store.put(navigation_summary_key, mismatched_summary);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.core.store.put(navigation_summary_key, backfilled_summary);

    var identity_navigation = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_stored = false,
        .include_all_fields = false,
        .fields = &.{},
    });
    defer identity_navigation.deinit();
    try std.testing.expectEqual(@as(usize, 1), identity_navigation.hits.len);
    try std.testing.expect(identity_navigation.hits[0].stored_data == null);
    try std.testing.expectEqualStrings(
        navigation.hits[0].sort_values[0].string,
        identity_navigation.hits[0].sort_values[0].string,
    );

    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Cancelled, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
        .cancellation = types.CancellationToken.fromAtomic(&cancelled),
    }));

    // The parent-owned state remains authoritative when a unit payload is on a
    // child-range shard. The local planner must retain the unit and let the
    // table coordinator hydrate the selected page through routed lookups.
    try db.core.store.delete(unit_key);
    var remote_unit_page = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .defer_hierarchy_child_hydration = true,
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    });
    defer remote_unit_page.deinit();
    try std.testing.expectEqual(@as(u32, 1), remote_unit_page.total_hits);
    try std.testing.expectEqual(@as(usize, 1), remote_unit_page.hits.len);
    try std.testing.expect(remote_unit_page.hits[0].stored_data == null);
    try db.core.store.put(unit_key, unit_payload);

    const stale_cursor = [_]std.json.Value{
        .{ .string = navigation.hits[0].sort_values[0].string },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    const malformed_cursor = [_]std.json.Value{
        .{ .string = "hn1/not-a-source-revision/document_units_v1/00000000000000000001/00000000000000000000/fingerprint" },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    const legacy_cursor = [_]std.json.Value{
        .{ .string = "hn2/retired" },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    const short_cursor = [_]std.json.Value{.{ .string = navigation.hits[0].sort_values[0].string }};
    const non_string_cursor = [_]std.json.Value{
        .{ .integer = 1 },
        .{ .string = navigation.hits[0].sort_values[1].string },
    };
    for ([_][]const std.json.Value{ &short_cursor, &non_string_cursor }) |invalid_cursor| {
        try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
            .hierarchy_children = .{ .parent_id = "doc:a" },
            .return_mode = .unit,
            .order_by = &navigation_order,
            .search_after = invalid_cursor,
            .limit = 20,
            .include_all_fields = false,
        }));
    }
    for ([_]u32{ 0, types.max_canonical_hierarchy_groups + 1 }) |invalid_limit| {
        try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
            .hierarchy_children = .{ .parent_id = "doc:a" },
            .return_mode = .unit,
            .order_by = &navigation_order,
            .limit = invalid_limit,
            .include_all_fields = false,
        }));
    }
    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &malformed_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &legacy_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,Z2FtbWEgZGVsdGE=\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &stale_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .enrichments,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, manifest_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, state_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, navigation_summary_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, navigation_block_key));

    // Row policy must not turn parent existence into a cursor-response oracle.
    // Exercise the same initial/valid/malformed cursor matrix below while the
    // source still exists but an internal authorization filter excludes it.
    const hidden_parent_request = types.SearchRequest{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
        .filter_doc_ids = &.{"doc:other"},
        .filter_doc_ids_positive = true,
    };
    var hidden_source = try db.search(alloc, hidden_parent_request);
    defer hidden_source.deinit();
    try std.testing.expectEqual(@as(u32, 0), hidden_source.total_hits);
    try std.testing.expectEqual(@as(usize, 0), hidden_source.hits.len);
    var hidden_valid_cursor = hidden_parent_request;
    hidden_valid_cursor.search_after = &stale_cursor;
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, hidden_valid_cursor));
    var hidden_malformed_cursor = hidden_parent_request;
    hidden_malformed_cursor.search_after = &malformed_cursor;
    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, hidden_malformed_cursor));

    // Deleting the parent after issuing a page cursor invalidates the complete
    // hierarchy revision. Initial traversal remains an empty collection, while
    // continuation returns exactly the same cursor classifications as the
    // authorization-hidden source above.
    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .enrichments,
    });
    var missing_source = try db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .limit = 20,
        .include_all_fields = false,
    });
    defer missing_source.deinit();
    try std.testing.expectEqual(@as(u32, 0), missing_source.total_hits);
    try std.testing.expectEqual(@as(usize, 0), missing_source.hits.len);
    const missing_shard_request = types.SearchRequest{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &stale_cursor,
        .limit = 20,
        .include_all_fields = false,
        .defer_hierarchy_child_hydration = true,
    };
    var missing_shard = try db.search(alloc, missing_shard_request);
    defer missing_shard.deinit();
    try std.testing.expectEqual(@as(u32, 0), missing_shard.total_hits);
    try std.testing.expectEqual(@as(usize, 0), missing_shard.hits.len);
    try std.testing.expectError(error.HierarchyCursorStale, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &stale_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
    try std.testing.expectError(error.InvalidQueryRequest, db.search(alloc, .{
        .hierarchy_children = .{ .parent_id = "doc:a" },
        .return_mode = .unit,
        .order_by = &navigation_order,
        .search_after = &malformed_cursor,
        .limit = 20,
        .include_all_fields = false,
    }));
}

test "db document extraction routes mixed files using source metadata fields" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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
        .sync_level = .enrichments,
    });

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

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

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
        .sync_level = .enrichments,
    });

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

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

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
        .sync_level = .enrichments,
    });

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

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

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
        .sync_level = .enrichments,
    });

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

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .enrichments,
    });

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

test "db document extraction attempts forced OCR for a scanned PDF" {
    // The real PDF implementation is covered by lib-pdf-test and the
    // production-binary PDF/OCR E2E suite. The aggregate Antfly test root uses
    // the lightweight PDF stub to stay below constrained CI memory limits.
    if (!document_extraction_mod.pdf_runtime_available) return error.SkipZigTest;

    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var fake = TestAssetProducer{ .reader_output = "[{\"text\":\"scanned PDF transcription with enough text to remain selected\"}]" };
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
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"mode\":\"always\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    });

    const pdf_bytes = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "lib/pdf/testdata/scanned_table_fixture.pdf",
        alloc,
        .limited(4 * 1024 * 1024),
    );
    defer alloc.free(pdf_bytes);
    const encoded_len = std.base64.standard.Encoder.calcSize(pdf_bytes.len);
    const encoded = try alloc.alloc(u8, encoded_len);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, pdf_bytes);
    const value = try std.fmt.allocPrint(
        alloc,
        "{{\"filename\":\"scan.pdf\",\"mime_type\":\"application/pdf\",\"url\":\"data:application/pdf;base64,{s}\"}}",
        .{encoded},
    );
    defer alloc.free(value);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:pdf-ocr", .value = value }},
        .sync_level = .enrichments,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.reader_calls);
    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:pdf-ocr", "document_units_v1", "page:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_attempted\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_used\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "\"ocr_render_dpi\":150") != null);
    try std.testing.expect(std.mem.indexOf(u8, unit_payload, "scanned PDF transcription") != null);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:pdf-ocr", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"ocr_attempted_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"ocr_selected_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"ocr_failed_count\":0") != null);
}

test "db document extraction stores structured OCR confidence and coordinates" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var fake = TestAssetProducer{
        .reader_output = "[{\"text\":\"invoice total\",\"confidence\":0.92,\"bbox\":[1,2,101,42],\"warning\":\"low contrast\"}]",
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
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"ocr\":{\"enabled\":true,\"config\":{\"provider\":\"antfly\",\"model\":\"test-reader\"}}}}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:image-ocr-structured",
            .value = "{\"filename\":\"scan.png\",\"mime_type\":\"image/png\",\"url\":\"data:image/png;base64,iVBORw0KGgppbWFnZSBieXRlcw==\"}",
        }},
        .sync_level = .enrichments,
    });

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
    try std.testing.expectApproxEqAbs(@as(f64, 1), jsonTestNumber(bbox[0]), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 42), jsonTestNumber(bbox[3]), 0.0001);
    try std.testing.expectEqualStrings("low contrast", parsed.value.object.get("extraction_warning").?.string);
    const provenance = parsed.value.object.get("provenance").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("confidence").?.float, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.92), provenance.get("ocr_confidence").?.float, 0.0001);
    try std.testing.expectEqualStrings("low contrast", provenance.get("extraction_warning").?.string);
}

test "db document extraction completes audio transcription with transcriber producer" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        .sync_level = .enrichments,
    });

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

test "db document extraction transcript segments time chunk artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var fake = TestAssetProducer{
        .transcriber_output = "{\"text\":\"first phrase here. second phrase there.\",\"confidence\":0.9,\"duration_ms\":2500,\"segments\":[{\"text\":\"first phrase here.\",\"start_ms\":0,\"end_ms\":1200},{\"text\":\"second phrase there.\",\"start_ms\":1300,\"end_ms\":2500}]}",
    };
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
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"transcription\":{\"enabled\":true,\"config\":{\"provider\":\"mock-transcriber\"}}}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 20,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:timed-audio",
            .value = "{\"filename\":\"audio.mp3\",\"mime_type\":\"audio/mpeg\",\"url\":\"data:audio/mpeg;base64,SUQzYXVkaW8gYnl0ZXM=\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:timed-audio", "document_units_v1", "audio:000001");
    defer alloc.free(unit_key);
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed_unit = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed_unit.deinit();
    try std.testing.expectEqualStrings("first phrase here. second phrase there.", parsed_unit.value.object.get("text").?.string);
    const spans = parsed_unit.value.object.get("provenance").?.object.get("transcript_spans").?.array;
    try std.testing.expectEqual(@as(usize, 2), spans.items.len);
    try std.testing.expectEqual(@as(i64, 0), spans.items[0].object.get("char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 18), spans.items[0].object.get("char_end").?.integer);
    try std.testing.expectEqual(@as(i64, 1200), spans.items[0].object.get("end_ms").?.integer);
    try std.testing.expectEqual(@as(i64, 19), spans.items[1].object.get("char_start").?.integer);
    try std.testing.expectEqual(@as(i64, 1300), spans.items[1].object.get("start_ms").?.integer);
    try std.testing.expectEqual(@as(i64, 2500), spans.items[1].object.get("end_ms").?.integer);

    // Every chunk cut from the transcript carries the offsets of the phrases
    // it overlaps, and the first chunk starts where the recording does.
    var chunk_index: u32 = 0;
    var timed_chunks: usize = 0;
    var last_end_ms: f64 = 0;
    while (true) : (chunk_index += 1) {
        const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:timed-audio", "document_chunks_v1", "audio:000001", chunk_index);
        defer alloc.free(chunk_key);
        const chunk_payload = db.core.store.get(alloc, chunk_key) catch |err| switch (err) {
            error.NotFound => break,
            else => return err,
        };
        defer alloc.free(chunk_payload);
        var parsed_chunk = try std.json.parseFromSlice(std.json.Value, alloc, chunk_payload, .{});
        defer parsed_chunk.deinit();
        const start_ms = jsonTestNumber(parsed_chunk.value.object.get("_start_time_ms").?);
        const end_ms = jsonTestNumber(parsed_chunk.value.object.get("_end_time_ms").?);
        if (chunk_index == 0) try std.testing.expectEqual(@as(f64, 0), start_ms);
        try std.testing.expect(end_ms >= start_ms);
        try std.testing.expect(start_ms >= last_end_ms or start_ms == 0 or start_ms == 1300);
        last_end_ms = end_ms;
        timed_chunks += 1;
    }
    try std.testing.expect(timed_chunks >= 2);
    try std.testing.expectEqual(@as(f64, 2500), last_end_ms);
}

test "db document extraction keeps diarized speakers through reopen" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var fake = TestAssetProducer{
        .transcriber_output = "{\"text\":\"alpha alpha alpha alpha alpha. beta beta beta beta beta.\",\"confidence\":0.9,\"duration_ms\":6000,\"speakers\":[\"SPEAKER_00\",\"SPEAKER_01\"],\"segments\":[{\"text\":\"alpha alpha alpha alpha alpha.\",\"start_ms\":0,\"end_ms\":3000,\"speaker\":\"SPEAKER_00\"},{\"text\":\"beta beta beta beta beta.\",\"start_ms\":3100,\"end_ms\":6000,\"speaker\":\"SPEAKER_01\"}]}",
    };

    {
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
            .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"filename_field\":\"filename\",\"content_type_field\":\"mime_type\"},\"transcription\":{\"enabled\":true,\"config\":{\"provider\":\"mock-transcriber\",\"diarization\":true}}}}",
        });
        try db.addEnrichment(.{
            .name = "document_chunks_v1",
            .kind = .chunk,
            .field = "text",
            .source_artifact_name = "document_units_v1",
            .chunk_size = 20,
        });

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:diarized-audio",
                .value = "{\"filename\":\"call.webm\",\"mime_type\":\"video/webm\",\"url\":\"data:video/webm;base64,SUQzYXVkaW8gYnl0ZXM=\"}",
            }},
            .sync_level = .full_index,
        });
        try std.testing.expectEqual(@as(usize, 1), fake.transcriber_calls);
    }

    // Reopen: the speaker attribution has to survive as durable artifact
    // state, not just as something the enrichment pass held in memory.
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
        .start_index_workers = false,
    });
    defer reopened.close();

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:diarized-audio", "document_units_v1", "audio:000001");
    defer alloc.free(unit_key);
    const unit_payload = try reopened.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed_unit = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed_unit.deinit();

    // Every phrase kept the speaker who said it, numbered in the order they
    // first spoke.
    const spans = parsed_unit.value.object.get("provenance").?.object.get("transcript_spans").?.array;
    try std.testing.expect(spans.items.len >= 2);
    try std.testing.expectEqual(@as(i64, 0), spans.items[0].object.get("speaker_index").?.integer);
    try std.testing.expectEqual(@as(i64, 1), spans.items[spans.items.len - 1].object.get("speaker_index").?.integer);

    // Chunks cut from the transcript carry the speaker's label, and a chunk
    // that straddles the turn carries none.
    var chunk_index: u32 = 0;
    var first_speaker_chunks: usize = 0;
    var second_speaker_chunks: usize = 0;
    while (true) : (chunk_index += 1) {
        const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:diarized-audio", "document_chunks_v1", "audio:000001", chunk_index);
        defer alloc.free(chunk_key);
        const chunk_payload = reopened.core.store.get(alloc, chunk_key) catch |err| switch (err) {
            error.NotFound => break,
            else => return err,
        };
        defer alloc.free(chunk_payload);
        var parsed_chunk = try std.json.parseFromSlice(std.json.Value, alloc, chunk_payload, .{});
        defer parsed_chunk.deinit();
        const speaker = parsed_chunk.value.object.get("_speaker") orelse continue;
        if (std.mem.eql(u8, speaker.string, "SPEAKER_00")) {
            first_speaker_chunks += 1;
        } else if (std.mem.eql(u8, speaker.string, "SPEAKER_01")) {
            second_speaker_chunks += 1;
        } else {
            return error.UnexpectedSpeakerLabel;
        }
    }
    try std.testing.expect(first_speaker_chunks >= 1);
    try std.testing.expect(second_speaker_chunks >= 1);
}

test "db document extraction stores rfc822 email units" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

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
        .sync_level = .enrichments,
    });

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

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

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
        .sync_level = .enrichments,
    });

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

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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
        .sync_level = .full_index,
    });

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

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });
    try std.testing.expect(db.core.index_manager.denseIndex("dv_document_chunks").?.supports_unit_grouping);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .full_index,
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

test "db document artifact child range batch atomically tracks dense artifact counters" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, no_background_test_open);
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
    });
    const artifact_key = try expectedChunkEmbeddingArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0, "chunk_dense_v1");
    defer alloc.free(artifact_key);
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 0, 0 });
    defer alloc.free(payload);

    const first_sequence = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_writes = &.{.{ .key = artifact_key, .value = payload }},
        .sync_level = .write,
    });
    const source_revision_key = try internal_keys.artifactSourceRevisionKeyAlloc(alloc, "chunk_dense_v1");
    defer alloc.free(source_revision_key);
    {
        const raw = try db.core.store.get(alloc, source_revision_key);
        defer alloc.free(raw);
        try std.testing.expectEqual(first_sequence, std.mem.readInt(u64, raw[0..8], .big));
    }
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "semantic_idx"),
    );

    const replacement_payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 0, 1, 0 });
    defer alloc.free(replacement_payload);
    _ = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_writes = &.{.{ .key = artifact_key, .value = replacement_payload }},
        .artifact_delete_keys = &.{artifact_key},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "semantic_idx"),
    );

    const delete_sequence = try db.applyDocumentArtifactChildRangeBatch(.{
        .artifact_delete_keys = &.{artifact_key},
        .sync_level = .write,
    });
    {
        const raw = try db.core.store.get(alloc, source_revision_key);
        defer alloc.free(raw);
        try std.testing.expectEqual(delete_sequence, std.mem.readInt(u64, raw[0..8], .big));
    }
    try std.testing.expectEqual(
        @as(?u64, 0),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "semantic_idx"),
    );
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var semantic_stats: ?types.DBIndexStats = null;
    for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, "semantic_idx")) semantic_stats = item;
    }
    const semantic = semantic_stats orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), semantic.source_replay.len);
    try std.testing.expectEqualStrings("chunk_dense_v1", semantic.source_replay[0].artifact_name);
    try std.testing.expectEqual(delete_sequence, semantic.source_replay[0].target_sequence);
    try std.testing.expect(semantic.source_replay[0].published_sequence < semantic.source_replay[0].target_sequence);
}

test "document child range partition preserves single ownership on allocation failure" {
    const alloc = std.testing.allocator;
    const remote_key = try internal_keys.documentUnitArtifactKeyAlloc(
        alloc,
        "doc:a",
        "document_units_v1",
        "page:000001",
    );
    defer alloc.free(remote_key);

    try platform.allocator.checkAllAllocationFailures(
        alloc,
        struct {
            fn run(failing_alloc: Allocator, routed_key: []const u8) !void {
                const range = types.DocumentArtifactChildRange{
                    .range_id = @constCast("range:000000"),
                    .range_kind = @constCast("unit"),
                    .artifact_name = @constCast("document_units_v1"),
                    .split_boundary = @constCast("unit"),
                    .placement = @constCast("remote"),
                    .owner_group_id = 7002,
                    .placement_generation = 7,
                    .route_status = @constCast("remote_committed"),
                    .split_eligible = true,
                    .start_key = @constCast(routed_key),
                    .end_key_exclusive = @constCast(""),
                    .last_key = @constCast(routed_key),
                    .child_count = 1,
                };
                const snapshot = DocumentChildRangeRoutingSnapshot{
                    .doc_key = @constCast("doc:a"),
                    .manifest_artifact_name = @constCast("document_units_v1"),
                    .child_ranges = @constCast(&[_]types.DocumentArtifactChildRange{range}),
                };

                var source = try derived_types.cloneBatch(failing_alloc, .{
                    .documents = &.{
                        .{ .key = routed_key, .targets = &.{.{ .kind = .full_text, .index_name = "full_text_index_v0" }} },
                        .{ .key = "doc:local", .targets = &.{.{ .kind = .graph, .index_name = "graph_v1" }} },
                    },
                });
                var documents = source.documents;
                source.documents = &.{};
                defer {
                    var local_batch = derived_types.DerivedBatch{ .documents = documents };
                    derived_types.deinitDerivedBatch(failing_alloc, &local_batch);
                    derived_types.deinitDerivedBatch(failing_alloc, &source);
                }

                var groups = std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup).empty;
                defer {
                    for (groups.items) |*group| group.deinit(failing_alloc);
                    groups.deinit(failing_alloc);
                }

                try partitionRemoteDerivedDocuments(
                    failing_alloc,
                    &.{snapshot},
                    .{ .ptr = &source, .select = testChildRangeDestination },
                    &documents,
                    &groups,
                );
                try std.testing.expectEqual(@as(usize, 1), documents.len);
                try std.testing.expectEqual(@as(usize, 1), groups.items.len);
                try std.testing.expectEqual(@as(usize, 1), groups.items[0].documents.items.len);
            }
        }.run,
        .{remote_key},
    );
}

test "db document extraction manifest inspection and reprocess API" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        .name = "dv_document_chunks",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
        }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const first_calls = counting.calls;
    try std.testing.expectEqual(@as(usize, 1), first_calls);

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
    try std.testing.expectEqual(first_calls, counting.calls);

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.generation);
    try std.testing.expectEqualStrings("remote", after.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7001), after.child_ranges[0].owner_group_id);
    try std.testing.expectEqual(@as(?u64, 2), after.child_ranges[0].placement_generation);
    try std.testing.expectEqualStrings("remote_committed", after.child_ranges[0].route_status.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"op\":\"keep\"") != null);
    const routed_unit_payload = try db.core.store.get(alloc, after.child_ranges[0].start_key);
    defer alloc.free(routed_unit_payload);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_route_status\":\"remote_committed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, routed_unit_payload, "\"_artifact_owner_group_id\":7001") != null);

    try std.testing.expectError(error.NotFound, db.reprocessDocumentArtifact(alloc, "doc:missing", "document_units_v1"));
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
        .sync_level = .full_index,
    });

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
    try std.testing.expectEqual(@as(u64, 3), range_after_a.generation);

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

test "db document extraction failure preserves last-known-good children and recovers" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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
    try std.testing.expectEqual(@as(usize, 1), after.unit_count);
    try std.testing.expectEqual(@as(usize, 1), after.child_range_count);
    try std.testing.expect(after.state_json != null);
    try std.testing.expect(after.last_error_code != null);
    try std.testing.expectEqualStrings("InvalidDataUri", after.last_error_code.?);
    try std.testing.expect(after.last_error_message != null);
    try std.testing.expectEqualStrings("remote content download failed", after.last_error_message.?);
    try std.testing.expect(std.mem.indexOf(u8, after.manifest_json, "\"last_error\"") != null);
    {
        const retained_unit = try db.core.store.get(alloc, stale_unit_key);
        defer alloc.free(retained_unit);
    }

    var list = try db.listDocumentArtifactManifests(alloc, "doc:a");
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), list.artifacts.len);
    try std.testing.expectEqualStrings("InvalidDataUri", list.artifacts[0].last_error_code.?);

    // A failure manifest must remain eligible for repair even when the restored
    // source matches the retained state fingerprint.
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    var repaired = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer repaired.deinit(alloc);
    try std.testing.expectEqual(@as(u64, after.generation + 1), repaired.generation);
    try std.testing.expectEqualStrings("text", repaired.route_type);
    try std.testing.expectEqualStrings("converged", repaired.merge_status);
    try std.testing.expectEqual(@as(usize, 1), repaired.unit_count);
    try std.testing.expect(repaired.last_error_code == null);
}

test "db document extraction skips stable unit local rewrites while replaying full text from stored artifacts" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        .config_json = "{\"field\":\"text\",\"sources\":[{\"artifact\":\"document_units_v1\"}]}",
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
    const chunk_vector_indexes = try db.core.index_manager.vectorIndexesForChunk(alloc, "document_chunks_v1");
    defer {
        for (chunk_vector_indexes) |index_name| alloc.free(index_name);
        alloc.free(chunk_vector_indexes);
    }
    try std.testing.expectEqual(@as(usize, 1), chunk_vector_indexes.len);
    try std.testing.expectEqualStrings("dv_document_chunks", chunk_vector_indexes[0]);
    const asset_vector_indexes = try db.core.index_manager.vectorIndexesDependingOnArtifact(alloc, "document_units_v1");
    defer {
        for (asset_vector_indexes) |index_name| alloc.free(index_name);
        alloc.free(asset_vector_indexes);
    }
    try std.testing.expectEqual(@as(usize, 1), asset_vector_indexes.len);
    try std.testing.expectEqualStrings("dv_document_chunks", asset_vector_indexes[0]);
    const asset_consumers = try db.core.index_manager.indexesDependingOnArtifact(alloc, "document_units_v1");
    defer {
        for (asset_consumers) |index_name| alloc.free(index_name);
        alloc.free(asset_consumers);
    }
    try std.testing.expectEqual(@as(usize, 3), asset_consumers.len);
    var found_units_text = false;
    var found_chunks_text = false;
    var found_chunks_dense = false;
    for (asset_consumers) |index_name| {
        found_units_text = found_units_text or std.mem.eql(u8, index_name, "ft_document_units");
        found_chunks_text = found_chunks_text or std.mem.eql(u8, index_name, "ft_document_chunks");
        found_chunks_dense = found_chunks_dense or std.mem.eql(u8, index_name, "dv_document_chunks");
    }
    try std.testing.expect(found_units_text and found_chunks_text and found_chunks_dense);

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
    var precomputed = try prepareGeneratedEnrichments(&db, alloc, .{
        .writes = &.{.{ .key = "doc:a", .value = second_value }},
    }, &extracted, .all, &.{}, null, null);
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

    // A full-index merge precomputes this successful empty extraction before
    // generated replay. The replay's stable-state shortcut must still account
    // for the source document so artifact-backed vector coverage can converge.
    try db.batch(.{
        .writes = &.{.{ .key = "doc:empty", .value = "{\"url\":\"data:text/plain,\"}" }},
        .sync_level = .full_index,
    });
    try db.runUntilIdle();
    const coverage_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, coverage_stats);
    var saw_vector_coverage = false;
    for (coverage_stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "dv_document_chunks")) continue;
        saw_vector_coverage = true;
        try std.testing.expect(index_stats.coverage_summary_ready);
        try std.testing.expectEqual(@as(u64, 1), index_stats.coverage_produced_count);
        try std.testing.expectEqual(@as(u64, 1), index_stats.coverage_skipped_count);
        try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_terminal_failed_count);
    }
    try std.testing.expect(saw_vector_coverage);

    var unit_result = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .unit,
    });
    defer unit_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), unit_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", unit_result.hits[0].artifact_ref.?.document_id);
    try std.testing.expectEqualStrings("document_units_v1", unit_result.hits[0].artifact_ref.?.name);
    try std.testing.expectEqualStrings("document:000001", unit_result.hits[0].artifact_ref.?.unit_id.?);

    var all_unit_members = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match_all = {} },
        .return_mode = .chunk,
    });
    defer all_unit_members.deinit();
    try std.testing.expectEqual(@as(u32, 2), all_unit_members.total_hits);
    for (all_unit_members.hits) |hit| {
        try std.testing.expectEqual(types.ArtifactKind.asset, hit.artifact_ref.?.kind);
        try std.testing.expect(hit.artifact_ref.?.unit_id != null);
    }

    // The artifact contains operational strings such as unit_type in addition
    // to text. A selective artifact index must not leak those fields into its
    // postings, including through the schema-less _all projection.
    var excluded_metadata = try db.search(alloc, .{
        .index_name = "ft_document_units",
        .full_text = .{ .match = .{ .field = "unit_type", .text = "document" } },
        .return_mode = .chunk,
    });
    defer excluded_metadata.deinit();
    try std.testing.expectEqual(@as(u32, 0), excluded_metadata.total_hits);

    var chunk_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .chunk,
    });
    defer chunk_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);

    var chunk_unit_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "gamma" } },
        .return_mode = .unit,
    });
    defer chunk_unit_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_unit_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", chunk_unit_result.hits[0].artifact_ref.?.document_id);
    try std.testing.expectEqualStrings("document_units_v1", chunk_unit_result.hits[0].artifact_ref.?.name);
    try std.testing.expectEqualStrings("document:000001", chunk_unit_result.hits[0].artifact_ref.?.unit_id.?);
}

test "db document extraction chunks units through source artifact enrichment" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
        .name = "document_chunk_caption_dense_v1",
        .kind = .embedding,
        .field = "caption",
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
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });
    try db.addIndex(.{
        .name = "document_caption_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_caption_dense_v1\"}",
    });
    try db.addIndex(.{
        .name = "document_chunk_sparse_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\",\"embedding_name\":\"document_chunk_sparse_v1\"}",
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
    try std.testing.expectEqualStrings("document_vectors", dense_consumers[0]);
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
        "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"source document decoy\"}",
        &.{},
        &.{},
    );
    defer enrichment_types.deinitGeneratedRequests(alloc, planned);
    var saw_document_asset = false;
    var saw_document_chunk_dense = false;
    var saw_document_chunk_caption_dense = false;
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
        if (request.kind == .dense_embedding and
            std.mem.eql(u8, request.artifact_name, "document_chunks_v1") and
            std.mem.eql(u8, request.embedding_name, "document_chunk_caption_dense_v1") and
            request.chunk_size == 256)
        {
            saw_document_chunk_caption_dense = true;
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
    try std.testing.expect(saw_document_chunk_dense);
    try std.testing.expect(saw_document_chunk_caption_dense);
    try std.testing.expect(saw_document_chunk_sparse);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"source document decoy\"}",
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
    const unit_payload = try db.core.store.get(alloc, unit_key);
    defer alloc.free(unit_payload);
    var parsed_unit_payload = try std.json.parseFromSlice(std.json.Value, alloc, unit_payload, .{});
    defer parsed_unit_payload.deinit();
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
    try std.testing.expectEqualStrings(
        parsed_unit_payload.value.object.get(hierarchy_navigation_unit_fingerprint_field).?.string,
        parsed_chunk_payload.value.object.get(hierarchy_navigation_unit_fingerprint_field).?.string,
    );
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
    try ant_json.testing.expectSubsetJsonText(
        alloc,
        "{\"chunk_unit_fingerprint_version\":2}",
        state,
    );

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
    const dense_index = db.core.index_manager.denseIndex("document_vectors") orelse return error.IndexNotFound;
    var direct = try waitForDenseIndexResultsWithAttempts(dense_index.index, query_vec, 3, 1, slow_test_wait_attempts);
    defer direct.deinit();
    const dense_internal_id = if (direct.takeMetadata(0)) |metadata|
        metadata
    else blk: {
        const hit = direct.getHits()[0];
        break :blk (try dense_index.index.getMetadata(hit.vector_id)) orelse return error.TestUnexpectedResult;
    };
    defer alloc.free(dense_internal_id);
    try std.testing.expectEqualStrings(chunk_key, dense_internal_id);

    var semantic_rollup = try db.search(alloc, .{
        .index_name = "document_vectors",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .limit = 1,
        .include_stored = false,
    });
    defer semantic_rollup.deinit();
    try std.testing.expectEqual(@as(u32, 1), semantic_rollup.total_hits);
    try std.testing.expectEqualStrings("doc:a", semantic_rollup.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), semantic_rollup.hits[0].chunk_hits.len);
    try std.testing.expectEqualStrings(chunk_public_id_for_rollup.id, semantic_rollup.hits[0].chunk_hits[0].id);
    const semantic_chunk_ref = semantic_rollup.hits[0].chunk_hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("doc:a", semantic_chunk_ref.document_id);
    try std.testing.expectEqualStrings("document_chunks_v1", semantic_chunk_ref.name);
    try std.testing.expectEqual(types.ArtifactKind.chunk, semantic_chunk_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), semantic_chunk_ref.chunk_id);
    try std.testing.expectEqualStrings("document:000001", semantic_chunk_ref.unit_id.?);

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

    var composed_ancestors = try db.search(alloc, .{
        .full_text_queries = &.{.{ .name = "only", .index_name = "ft_document_chunks", .query = .{ .match = .{ .field = "text", .text = "gamma" } } }},
        .return_mode = .member,
        .limit = 1,
        .include_stored = false,
        .hierarchy_include_source = true,
        .hierarchy_include_unit = true,
    });
    defer composed_ancestors.deinit();
    try std.testing.expectEqual(@as(usize, 1), composed_ancestors.hits.len);
    try std.testing.expect(composed_ancestors.hits[0].ancestor_source_data != null);
    try std.testing.expect(composed_ancestors.hits[0].ancestor_unit_data != null);
    try std.testing.expectEqualStrings(sparse_result.hits[0].ancestor_source_data.?, composed_ancestors.hits[0].ancestor_source_data.?);
    try std.testing.expectEqualStrings(sparse_result.hits[0].ancestor_unit_data.?, composed_ancestors.hits[0].ancestor_unit_data.?);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBkZWx0YQ==\",\"text\":\"updated source document decoy\"}",
        }},
        .sync_level = .write,
    });
    try db.waitForCurrentSyncLevel(.full_index);
    const updated_manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(updated_manifest);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"from_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated_manifest, "\"to_generation\":2") != null);
    const updated_chunk_payload = try db.core.store.get(alloc, chunk_key);
    defer alloc.free(updated_chunk_payload);
    try std.testing.expect(std.mem.indexOf(u8, updated_chunk_payload, "\"text\":\"alpha beta delta\"") != null);
    const refreshed_dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(refreshed_dense_artifact_payload);
    const refreshed_sparse_artifact_payload = try db.core.store.get(alloc, sparse_artifact_key);
    defer alloc.free(refreshed_sparse_artifact_payload);

    // A live source/unit must not resurrect an orphaned member when grouped.
    // Presence applies to the raw member before either grouping or hydration.
    try db.core.store.delete(chunk_key);
    for (std.enums.values(types.ReturnMode)) |mode| {
        for ([_]bool{ false, true }) |include_stored| {
            var missing = try db.search(alloc, .{
                .index_name = "ft_document_chunks",
                .full_text = .{ .match = .{ .field = "text", .text = "delta" } },
                .return_mode = mode,
                .include_stored = include_stored,
                .limit = 1,
            });
            defer missing.deinit();
            try std.testing.expectEqual(@as(usize, 0), missing.hits.len);
            try std.testing.expectEqual(@as(u32, 0), missing.total_hits);
        }
    }
    var missing_dense = try db.search(alloc, .{ .index_name = "document_vectors", .dense = .{ .vector = query_vec, .k = 1 }, .limit = 1, .include_stored = false });
    defer missing_dense.deinit();
    // Vectors are backed by their embedding artifacts, independently of the
    // optional stored chunk payload used by the full-text index above.
    try std.testing.expectEqual(@as(usize, 1), missing_dense.hits.len);
    var updated_sparse_query = try deterministic_sparse.interface().embedSparse(alloc, "document_chunk_sparse_v1", "alpha beta delta");
    defer updated_sparse_query.deinit(alloc);
    var missing_sparse = try db.search(alloc, .{ .index_name = "document_chunk_sparse_v1", .query = .{ .sparse_knn = .{ .indices = updated_sparse_query.indices, .values = updated_sparse_query.values, .k = 1 } }, .limit = 1, .include_stored = false });
    defer missing_sparse.deinit();
    try std.testing.expectEqual(@as(usize, 1), missing_sparse.hits.len);
    try db.core.store.delete(dense_artifact_key);
    try db.core.store.delete(sparse_artifact_key);
    var orphaned_dense = try db.search(alloc, .{ .index_name = "document_vectors", .dense = .{ .vector = query_vec, .k = 1 }, .limit = 1, .include_stored = false });
    defer orphaned_dense.deinit();
    try std.testing.expectEqual(@as(usize, 0), orphaned_dense.hits.len);
    var orphaned_sparse = try db.search(alloc, .{ .index_name = "document_chunk_sparse_v1", .query = .{ .sparse_knn = .{ .indices = updated_sparse_query.indices, .values = updated_sparse_query.values, .k = 1 } }, .limit = 1, .include_stored = false });
    defer orphaned_sparse.deinit();
    try std.testing.expectEqual(@as(usize, 0), orphaned_sparse.hits.len);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"\"}",
        }},
        .sync_level = .full_index,
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unit_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, chunk_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, dense_artifact_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, sparse_artifact_key));
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("document_vectors").?.index.metadata.active_count);
}

test "db document extraction concurrent same-key writes converge without stale precompute" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
    try db.addIndex(.{
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    var hook = PortableRuntimeBatchPrelockTestHook{};
    db_zig.test_portable_runtime_batch_prelock_hook = &hook;
    defer db_zig.test_portable_runtime_batch_prelock_hook = null;

    // doc:a does not exist yet: thread_a's pre-lock precompute (hoisted by
    // this change) reads that absence and plans a from_generation: 0 commit,
    // then parks at the prelock test hook, just before the apply lock.
    const Writer = struct {
        db: *DB,
        value: []const u8,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{ .key = "doc:a", .value = self.value }},
                .sync_level = .enrichments,
            }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer_a = Writer{
        .db = &db,
        .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"decoy\"}",
    };
    const thread_a = try std.Thread.spawn(.{}, Writer.run, .{&writer_a});
    var joined_a = false;
    defer if (!joined_a) {
        hook.release.store(true, .release);
        thread_a.join();
    };

    const prelock_deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!hook.entered.load(.acquire) and monotonicTimeNs() < prelock_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(hook.entered.load(.acquire));

    // Commit different content for the same key while thread_a is parked.
    // Clear the hook first so this write is not itself gated by it.
    db_zig.test_portable_runtime_batch_prelock_hook = null;
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YSBnYW1tYSBkZWx0YQ==\",\"text\":\"decoy\"}",
        }},
        .sync_level = .full_index,
    });

    // Release thread_a. Its stale pre-lock precompute (built against a
    // nonexistent prior document) must be detected and retried against the
    // now-current state, not silently committed on top of it -- which would
    // either corrupt the generation counter or leave orphaned chunk/embedding
    // artifacts from the intermediate commit.
    hook.release.store(true, .release);
    thread_a.join();
    joined_a = true;
    try std.testing.expectEqual(@as(?anyerror, null), writer_a.err);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"from_generation\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"to_generation\":2") != null);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    var last_chunk_payload: []const u8 = "";
    for (chunk_entries) |entry| if (internal_keys.isChunkArtifactRecordKey(entry.key)) {
        chunk_count += 1;
        last_chunk_payload = entry.value;
    };
    // Exactly one surviving chunk, matching thread_a's final ("alpha beta
    // gamma") content -- no orphaned chunk from the intermediate ("beta
    // gamma delta") generation this write raced against.
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expect(std.mem.indexOf(u8, last_chunk_payload, "alpha beta gamma") != null);
}

test "db document extraction pre-lock precompute survives a concurrent catalog mutation without touching freed memory" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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
    try db.addIndex(.{
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    var hook = PortableRuntimeBatchPrelockTestHook{};
    db_zig.test_portable_runtime_batch_prelock_hook = &hook;
    defer db_zig.test_portable_runtime_batch_prelock_hook = null;

    // thread_a's pre-lock precompute (extraction against the owned write-plan
    // snapshot, then chunk/embedding derivation) runs to completion and
    // parks at the prelock test hook, just before the apply lock.
    const Writer = struct {
        db: *DB,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{
                    .key = "doc:a",
                    .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\"}",
                }},
                .sync_level = .enrichments,
            }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer_a = Writer{ .db = &db };
    const thread_a = try std.Thread.spawn(.{}, Writer.run, .{&writer_a});
    var joined_a = false;
    defer if (!joined_a) {
        hook.release.store(true, .release);
        thread_a.join();
    };

    const prelock_deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!hook.entered.load(.acquire) and monotonicTimeNs() < prelock_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(hook.entered.load(.acquire));

    // Replace the CHUNK enrichment doc:a's own pre-lock extraction actually
    // used -- with a different chunk_size -- while thread_a is parked just
    // before the apply lock. By this point thread_a's
    // DocumentExtractionCatalogView (built once, under
    // catalog_mutex.lockShared(), in computeAssetRequestDerived) has
    // already been fully read -- its chunk/embedding derivation runs
    // entirely pre-lock, before this hook -- so this exercises the same
    // class of bug the review blocker identified: upsertEnrichment frees
    // the live "document_chunks_v1" entry's strings (chunker_json,
    // source_field, ...) and replaces them in place; thread_a's view, and
    // everything it derived from "document_chunks_v1" and
    // "document_chunk_dense_v1" (which consumes it), must be fully owned,
    // not a borrowed pointer into what this upsert frees. Deleting the
    // chunk enrichment outright is rejected with error.EnrichmentInUse
    // while the embedding still names it as source_artifact_name, so this
    // uses the allowed in-place replace instead. A catalog publish bumps
    // the write-plan generation thread_a pinned, so this also forces the
    // existing PreparedGenerationChanged retry path to run against the
    // replaced catalog entry.
    db_zig.test_portable_runtime_batch_prelock_hook = null;
    _ = try db.upsertEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 4,
    });

    hook.release.store(true, .release);
    thread_a.join();
    joined_a = true;
    try std.testing.expectEqual(@as(?anyerror, null), writer_a.err);

    // Whether this landed from thread_a's own pre-lock result (if its
    // pinned generation happened to still match) or a generation-changed
    // retry against the replaced chunk_size, the testing allocator above
    // would already have caught any read of, or double free of, memory the
    // upsert's replace freed. Every surviving chunk still gets its
    // "document_chunk_dense_v1" embedding artifact -- the replace never
    // touched that enrichment, so this is the same convergence check as
    // the "concurrent same-key writes" test above, just with a catalog
    // replace as the interference instead of another write.
    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    var dense_artifact_count: usize = 0;
    for (chunk_entries) |entry| {
        if (!internal_keys.isChunkArtifactRecordKey(entry.key)) continue;
        chunk_count += 1;
        const dense_artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, entry.key, "document_chunk_dense_v1");
        defer alloc.free(dense_artifact_key);
        const dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
        alloc.free(dense_artifact_payload);
        dense_artifact_count += 1;
    }
    try std.testing.expect(chunk_count > 0);
    try std.testing.expectEqual(chunk_count, dense_artifact_count);
}

test "db document extraction unit chunks embed in provider batches, not per chunk" {
    const alloc = std.testing.allocator;
    const BatchProbe = struct {
        deterministic: embedder_mod.DeterministicDenseEmbedder = .{},
        batch_calls: usize = 0,
        items: usize = 0,

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedDenseBatch(
            raw: *anyopaque,
            batch_alloc: Allocator,
            embedding_name: []const u8,
            texts: []const []const u8,
            dims: u32,
        ) ![]const []const f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.batch_calls += 1;
            self.items += texts.len;
            const vectors = try batch_alloc.alloc([]const f32, texts.len);
            var initialized: usize = 0;
            errdefer {
                for (vectors[0..initialized]) |vector| batch_alloc.free(@constCast(vector));
                batch_alloc.free(vectors);
            }
            for (texts, vectors) |text, *vector| {
                vector.* = try embedder_mod.DeterministicDenseEmbedder.embedDense(
                    &self.deterministic,
                    batch_alloc,
                    embedding_name,
                    text,
                    dims,
                );
                initialized += 1;
            }
            return vectors;
        }

        fn interface(self: *@This()) embedder_mod.DenseEmbedder {
            return .{
                .ptr = self,
                .dense_embed_fn = embedDense,
                .dense_embed_batch_fn = embedDenseBatch,
                .deinit_fn = null,
            };
        }
    };

    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const batch_items_env = "ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS";
    try std.testing.expectEqual(@as(c_int, 0), c.setenv(batch_items_env, "3", 1));
    defer _ = c.unsetenv(batch_items_env);

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var probe = BatchProbe{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{ .owner_id = "worker-a", .dense_embedder = probe.interface() },
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
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    // ~2.5 KB of text, base64-encoded, so chunk_size: 256 produces well over
    // one provider batch (batch items pinned to 3 above) from a single
    // document_extraction unit.
    const doc_value = "{\"url\":\"data:text/plain;base64," ++
        "YWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4gYWxwaGEgYmV0YSBnYW1tYSBkZWx0YSBlcHNpbG9uIHpldGEgZXRhIHRoZXRhIGlvdGEga2FwcGEgbGFtYmRhIG11IG51IHhpIG9taWNyb24gcGkgcmhvIHNpZ21hIHRhdSB1cHNpbG9uIHBoaSBjaGkgcHNpIG9tZWdhLiBhbHBoYSBiZXRhIGdhbW1hIGRlbHRhIGVwc2lsb24gemV0YSBldGEgdGhldGEgaW90YSBrYXBwYSBsYW1iZGEgbXUgbnUgeGkgb21pY3JvbiBwaSByaG8gc2lnbWEgdGF1IHVwc2lsb24gcGhpIGNoaSBwc2kgb21lZ2EuIGFscGhhIGJldGEgZ2FtbWEgZGVsdGEgZXBzaWxvbiB6ZXRhIGV0YSB0aGV0YSBpb3RhIGthcHBhIGxhbWJkYSBtdSBudSB4aSBvbWljcm9uIHBpIHJobyBzaWdtYSB0YXUgdXBzaWxvbiBwaGkgY2hpIHBzaSBvbWVnYS4g" ++
        "\"}";

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = doc_value }},
        .sync_level = .enrichments,
    });

    // Well more than one provider batch's worth of chunks, but far fewer
    // batch calls than chunks: the old per-chunk path would have called the
    // single-item embedDense (which always errors here) once per chunk.
    try std.testing.expect(probe.items >= 8);
    try std.testing.expect(probe.batch_calls < probe.items);
    const max_batch_items = generatedEmbedBatchItems();
    const expected_batch_calls = (probe.items + max_batch_items - 1) / max_batch_items;
    try std.testing.expectEqual(expected_batch_calls, probe.batch_calls);

    // Every chunk this document produced has its own derived dense embedding
    // artifact -- batching the provider calls did not drop or misattribute
    // any chunk's embedding.
    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    for (chunk_entries) |entry| {
        if (!internal_keys.isChunkArtifactRecordKey(entry.key)) continue;
        chunk_count += 1;
        const dense_artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, entry.key, "document_chunk_dense_v1");
        defer alloc.free(dense_artifact_key);
        const dense_artifact_payload = try db.core.store.get(alloc, dense_artifact_key);
        alloc.free(dense_artifact_payload);
    }
    try std.testing.expectEqual(probe.items, chunk_count);
}

test "db document extraction state round-trips binary chunk keys beyond one byte ids" {
    const alloc = std.testing.allocator;

    var unit_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (unit_keys.items) |key| alloc.free(@constCast(key));
        unit_keys.deinit(alloc);
    }
    var unit_fingerprints = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (unit_fingerprints.items) |fingerprint| alloc.free(@constCast(fingerprint));
        unit_fingerprints.deinit(alloc);
    }
    var chunk_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (chunk_keys.items) |key| alloc.free(@constCast(key));
        chunk_keys.deinit(alloc);
    }

    try unit_keys.append(alloc, try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:large", "document_units_v1", "document:000001"));
    try unit_fingerprints.append(alloc, try alloc.dupe(u8, "unit-fingerprint"));
    for (0..320) |i| {
        try chunk_keys.append(alloc, try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:large", "document_chunks_v1", "document:000001", @intCast(i)));
    }

    const descriptors = try documentExtractionUnitDescriptorsFromKeysAlloc(alloc, unit_keys.items, unit_fingerprints.items);
    defer alloc.free(descriptors);
    const navigation_digest = try hierarchyNavigationArtifactDigestAlloc(alloc, descriptors);
    defer alloc.free(navigation_digest);
    const state = try documentExtractionStateValueAlloc(
        alloc,
        "source-fingerprint",
        unit_keys.items,
        descriptors,
        chunk_keys.items,
        navigation_digest,
        hierarchyNavigationBlockCount(@intCast(descriptors.len)),
        true,
    );
    defer alloc.free(state);

    const parsed_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, state);
    defer freeOwnedConstKeySlice(alloc, parsed_chunk_keys);
    try std.testing.expectEqual(chunk_keys.items.len, parsed_chunk_keys.len);
    for (chunk_keys.items, parsed_chunk_keys) |expected, actual| {
        try std.testing.expectEqualSlices(u8, expected, actual);
    }
}

test "db document extraction changed version updates large chunked source document" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"etag_field\":\"sha256\",\"version_field\":\"sha256\",\"content_type_field\":\"mime_type\",\"filename_field\":\"filename\"}}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 96,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try db.addIndex(.{
        .name = "ft_document_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"document_chunks_v1\"}",
    });

    const url_v1 = try testLargeHtmlDataUrlAlloc(alloc, "v1", "firstversiontoken", 360);
    defer alloc.free(url_v1);
    const doc_v1 = try testSourceDocumentJsonAlloc(alloc, url_v1, "sha-v1");
    defer alloc.free(doc_v1);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:large", .value = doc_v1 }},
        .sync_level = .full_index,
    });

    const state_key = try assetStateKeyAlloc(alloc, "doc:large", "document_units_v1");
    defer alloc.free(state_key);
    const first_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(first_state);
    const first_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, first_state);
    defer freeOwnedConstKeySlice(alloc, first_chunk_keys);
    try std.testing.expect(first_chunk_keys.len > 300);

    const url_v2 = try testLargeHtmlDataUrlAlloc(alloc, "v2", "secondversiontoken", 360);
    defer alloc.free(url_v2);
    const doc_v2 = try testSourceDocumentJsonAlloc(alloc, url_v2, "sha-v2");
    defer alloc.free(doc_v2);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:large", .value = doc_v2 }},
        .sync_level = .full_index,
    });

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:large", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":2") != null);

    const second_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(second_state);
    const second_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, second_state);
    defer freeOwnedConstKeySlice(alloc, second_chunk_keys);
    try std.testing.expect(second_chunk_keys.len > 300);

    var second_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "secondversiontoken" } },
        .return_mode = .chunk,
    });
    defer second_result.deinit();
    try std.testing.expect(second_result.total_hits > 0);

    var first_result = try db.search(alloc, .{
        .index_name = "ft_document_chunks",
        .full_text = .{ .match = .{ .field = "text", .text = "firstversiontoken" } },
        .return_mode = .chunk,
    });
    defer first_result.deinit();
    try std.testing.expectEqual(@as(u32, 0), first_result.total_hits);
}

test "db document extraction chunk enrichment lifecycle rechunks adds and retires chunk artifacts" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

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
        .chunk_size = 64,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try db.addIndex(.{ .name = "ft_default", .kind = .full_text, .config_json = "{}" });

    var url = std.ArrayListUnmanaged(u8).empty;
    defer url.deinit(alloc);
    try url.appendSlice(alloc, "data:text/plain,");
    for (0..40) |i| {
        const word = try std.fmt.allocPrint(alloc, "harbor{d}%20ledger%20window%20", .{i});
        defer alloc.free(word);
        try url.appendSlice(alloc, word);
    }
    try url.appendSlice(alloc, "zygomorphic");
    const doc_value = try std.fmt.allocPrint(alloc, "{{\"url\":\"{s}\"}}", .{url.items});
    defer alloc.free(doc_value);

    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = doc_value }}, .sync_level = .full_index });
    try db.runUntilIdle();
    const small_chunks = try testChunkArtifactCount(alloc, db, "doc:a", "document_chunks_v1");
    try std.testing.expect(small_chunks > 4);
    try std.testing.expectEqual(@as(u32, 1), try testDefaultTextHitCount(alloc, db, "zygomorphic"));

    // Growing the chunk size keeps the old chunk ids 0..n but must replace
    // their text, so the tail term moves into the new last chunk.
    _ = try db.upsertEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 4096,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), try testChunkArtifactCount(alloc, db, "doc:a", "document_chunks_v1"));
    try std.testing.expectEqual(@as(u32, 1), try testDefaultTextHitCount(alloc, db, "zygomorphic"));
    try std.testing.expectEqual(@as(u32, 1), try testDefaultTextHitCount(alloc, db, "harbor0"));

    try db.addEnrichment(.{
        .name = "document_chunks_v2",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 4096,
        .chunk_overlap = 0,
        .full_text_index = true,
    });
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 1), try testChunkArtifactCount(alloc, db, "doc:a", "document_chunks_v2"));
    try std.testing.expectEqual(@as(u32, 2), try testDefaultTextHitCount(alloc, db, "zygomorphic"));

    try std.testing.expect(try db.deleteEnrichment(.chunk, "document_chunks_v2"));
    try std.testing.expect(try db.reprocessDocumentArtifact(alloc, "doc:a", "document_units_v1"));
    try db.runUntilIdle();
    try std.testing.expectEqual(@as(usize, 0), try testChunkArtifactCount(alloc, db, "doc:a", "document_chunks_v2"));
    try std.testing.expectEqual(@as(u32, 1), try testDefaultTextHitCount(alloc, db, "zygomorphic"));
}

test "db document extraction update recovers corrupt previous extraction state" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{\"source\":{\"etag_field\":\"sha256\",\"version_field\":\"sha256\",\"content_type_field\":\"mime_type\",\"filename_field\":\"filename\"}}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 96,
        .chunk_overlap = 0,
        .full_text_index = true,
    });

    const url_v1 = try testLargeHtmlDataUrlAlloc(alloc, "v1", "corruptoldtoken", 80);
    defer alloc.free(url_v1);
    const doc_v1 = try testSourceDocumentJsonAlloc(alloc, url_v1, "sha-v1");
    defer alloc.free(doc_v1);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:corrupt", .value = doc_v1 }},
        .sync_level = .full_index,
    });

    const state_key = try assetStateKeyAlloc(alloc, "doc:corrupt", "document_units_v1");
    defer alloc.free(state_key);
    try db.core.store.put(state_key, "{\"kind\":\"document_extraction_state_v1\",\"unit_keys\":[7],\"unit_descriptors\":{},\"chunk_keys\":[false]}");

    const url_v2 = try testLargeHtmlDataUrlAlloc(alloc, "v2", "corruptnewtoken", 80);
    defer alloc.free(url_v2);
    const doc_v2 = try testSourceDocumentJsonAlloc(alloc, url_v2, "sha-v2");
    defer alloc.free(doc_v2);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:corrupt", .value = doc_v2 }},
        .sync_level = .full_index,
    });

    const recovered_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(recovered_state);
    const recovered_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, recovered_state);
    defer freeOwnedConstKeySlice(alloc, recovered_chunk_keys);
    try std.testing.expect(recovered_chunk_keys.len > 0);

    try db.core.store.put(state_key, "{");

    const url_v3 = try testLargeHtmlDataUrlAlloc(alloc, "v3", "corruptthirdtoken", 80);
    defer alloc.free(url_v3);
    const doc_v3 = try testSourceDocumentJsonAlloc(alloc, url_v3, "sha-v3");
    defer alloc.free(doc_v3);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:corrupt", .value = doc_v3 }},
        .sync_level = .full_index,
    });

    const recovered_truncated_state = try db.core.store.get(alloc, state_key);
    defer alloc.free(recovered_truncated_state);
    const recovered_truncated_chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, recovered_truncated_state);
    defer freeOwnedConstKeySlice(alloc, recovered_truncated_chunk_keys);
    try std.testing.expect(recovered_truncated_chunk_keys.len > 0);
}

test "db document _edges reconcile graph state and preserve base document" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\",\"weight\":1.0},{\"target\":\"doc:c\",\"weight\":0.5}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_edges\"") == null);
    const graph_prefix_before = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:a", "gr_v1");
    defer alloc.free(graph_prefix_before);
    const graph_artifacts_before = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_before);
    try std.testing.expectEqual(@as(usize, 2), graph_artifacts_before.len);

    var before = try waitForSearchResult(alloc, db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 2);
    defer before.deinit();

    try std.testing.expectEqual(@as(u32, 2), before.total_hits);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\",\"weight\":0.9}]}}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_edges\"") == null);
    const graph_artifacts_after = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_after);
    try std.testing.expectEqual(@as(usize, 1), graph_artifacts_after.len);

    var after = try waitForSearchResult(alloc, db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 1);
    defer after.deinit();

    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:c", after.hits[0].id);
}

test "db document _edges reconcile graph state and preserve base document with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\",\"weight\":1.0},{\"target\":\"doc:c\",\"weight\":0.5}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_edges\"") == null);
    const graph_prefix_before = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:a", "gr_v1");
    defer alloc.free(graph_prefix_before);
    const graph_artifacts_before = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_before);
    try std.testing.expectEqual(@as(usize, 2), graph_artifacts_before.len);

    var before = try waitForSearchResult(alloc, db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 2);
    defer before.deinit();

    try std.testing.expectEqual(@as(u32, 2), before.total_hits);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\",\"weight\":0.9}]}}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_edges\"") == null);
    const graph_artifacts_after = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_after);
    try std.testing.expectEqual(@as(usize, 1), graph_artifacts_after.len);

    var after = try waitForSearchResult(alloc, db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 1);
    defer after.deinit();

    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:c", after.hits[0].id);
}

test "db document _embeddings update vector index and strip stored special fields" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{});
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_embeddings\"") == null);

    var before = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer before.deinit();

    try std.testing.expectEqualStrings("doc:a", before.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dv_v1\":[0,0,1]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_embeddings\"") == null);

    var after = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer after.deinit();

    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db document _embeddings external vector coverage survives updates deletes and reopen" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |sparse| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;

        const ExpectCoverage = struct {
            fn run(db_value: *DB, source: u64, produced: u64, is_sparse: bool) !void {
                const stats = try db_value.stats(std.testing.allocator);
                defer types.freeDBStats(std.testing.allocator, stats);
                try std.testing.expectEqual(source, stats.source_doc_count);
                for (stats.indexes) |index_stats| {
                    if (!std.mem.eql(u8, index_stats.name, "external_v1")) continue;
                    try std.testing.expectEqual(produced, index_stats.doc_count);
                    try std.testing.expectEqual(produced, index_stats.coverage_produced_count);
                    try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_skipped_count);
                    var result = try db_value.search(std.testing.allocator, .{
                        .index_name = "external_v1",
                        .query = if (is_sparse) .{ .sparse_knn = .{ .indices = &.{7}, .values = &.{1}, .k = 10 } } else .{ .dense_knn = .{ .vector = &.{ 0, 0, 1 }, .k = 10 } },
                        .limit = 10,
                    });
                    defer result.deinit();
                    try std.testing.expectEqual(produced, result.hits.len);
                    if (produced != 0) try std.testing.expectEqualStrings("doc:covered", result.hits[0].id);
                    return;
                }
                return error.IndexNotFound;
            }
        };

        {
            var db = try DB.open(alloc, std.mem.span(path), .{});
            defer db.close();
            try db.addIndex(.{
                .name = "external_v1",
                .kind = if (sparse) .sparse_vector else .dense_vector,
                .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
            });
            try std.testing.expect(if (sparse) db.core.index_manager.sparseIndexUsesExternalCoverage("external_v1") else db.core.index_manager.denseIndexUsesExternalCoverage("external_v1"));

            // Source-only work must bypass vector replay, including journal
            // catch-up, while real document deletion still targets the index.
            const ref = index_manager_mod.ManagedIndexRef{ .name = "external_v1", .kind = if (sparse) .sparse_vector else .dense_vector };
            try std.testing.expectEqual(ManagedIndexBatchApplicability.irrelevant, managedIndexBatchApplicability(db.core.index_manager, .{
                .documents = &.{.{ .key = "doc:covered", .action = .upsert }},
                .overwritten_doc_keys = &.{"doc:covered"},
            }, ref));
            try std.testing.expectEqual(ManagedIndexBatchApplicability.irrelevant, managedIndexRecordApplicability(db.core.index_manager, .{
                .sequence = 1,
                .changed_doc_keys = &.{"doc:covered"},
                .overwritten_doc_keys = &.{"doc:covered"},
            }, ref));
            try std.testing.expectEqual(ManagedIndexBatchApplicability.relevant, managedIndexRecordApplicability(db.core.index_manager, .{
                .sequence = 1,
                .deleted_doc_keys = &.{"doc:covered"},
            }, ref));

            try db.batch(.{
                .writes = &.{
                    .{ .key = "doc:covered", .value = if (sparse) "{\"title\":\"covered\",\"_embeddings\":{\"external_v1\":{\"indices\":[7],\"values\":[1]}}}" else "{\"title\":\"covered\",\"_embeddings\":{\"external_v1\":[1,0,0]}}" },
                    .{ .key = "doc:pending", .value = "{\"title\":\"pending\"}" },
                },
                .sync_level = .full_index,
            });
            try ExpectCoverage.run(&db, 2, 1, sparse);
            const vector_id = if (sparse) null else try db.core.index_manager.lookupDenseVectorId(db.core.store, "external_v1", "doc:covered");

            try db.batch(.{ .writes = &.{.{ .key = "doc:covered", .value = "{\"title\":\"vector retained\"}" }}, .sync_level = .full_index });
            try ExpectCoverage.run(&db, 2, 1, sparse);
            try db.batch(.{ .transforms = &.{.{ .key = "doc:covered", .operations = &.{.{ .op = .set, .path = "caption", .value_json = "\"captioned\"" }} }}, .sync_level = .full_index });
            try ExpectCoverage.run(&db, 2, 1, sparse);
            // An explicit replacement with changed source JSON must upsert the
            // existing member, retaining its dense identity without a delete gap.
            try db.batch(.{ .writes = &.{.{ .key = "doc:covered", .value = if (sparse) "{\"title\":\"replaced\",\"_embeddings\":{\"external_v1\":{\"indices\":[7],\"values\":[3]}}}" else "{\"title\":\"replaced\",\"_embeddings\":{\"external_v1\":[0,0,1]}}" }}, .sync_level = .full_index });
            try ExpectCoverage.run(&db, 2, 1, sparse);
            if (!sparse) try std.testing.expectEqual(vector_id, try db.core.index_manager.lookupDenseVectorId(db.core.store, "external_v1", "doc:covered"));
            try db.batch(.{ .writes = &.{.{ .key = "doc:pending", .value = if (sparse) "{\"_embeddings\":{\"external_v1\":{\"indices\":[7],\"values\":[2]}}}" else "{\"_embeddings\":{\"external_v1\":[0,1,0]}}" }}, .sync_level = .full_index });
            try ExpectCoverage.run(&db, 2, 2, sparse);
        }

        {
            var reopened = try DB.open(alloc, std.mem.span(path), .{});
            defer reopened.close();
            try ExpectCoverage.run(&reopened, 2, 2, sparse);
            try reopened.batch(.{ .deletes = &.{"doc:pending"}, .sync_level = .full_index });
            try ExpectCoverage.run(&reopened, 1, 1, sparse);
            // Exercise the thin durable replay lane as well as full_index.
            try reopened.batch(.{ .writes = &.{.{ .key = "doc:covered", .value = "{\"_embeddings\":{\"external_v1\":null}}" }}, .sync_level = .write });
            try reopened.runUntilIdle();
            try ExpectCoverage.run(&reopened, 1, 0, sparse);
            try reopened.batch(.{ .writes = &.{.{ .key = "doc:covered", .value = "{\"_embeddings\":{\"external_v1\":null}}" }}, .sync_level = .full_index });
            try ExpectCoverage.run(&reopened, 1, 0, sparse);
        }
        {
            var reopened = try DB.open(alloc, std.mem.span(path), .{});
            defer reopened.close();
            try ExpectCoverage.run(&reopened, 1, 0, sparse);
        }
    }
}

test "db document _embeddings external indexes ignore source vector fields and isolate sibling mutations" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |sparse| {
        const t = try TestDb.open(alloc, .{});
        defer t.deinit();
        const db = &t.db;
        for ([_][]const u8{ "a", "b" }) |name| try db.addIndex(.{
            .name = name,
            .kind = if (sparse) .sparse_vector else .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        var plan = try db.core.index_manager.acquireWritePlanSnapshot();
        defer plan.release();
        try std.testing.expectEqual(@as(usize, 0), plan.plan().dense_fields.len);
        try std.testing.expectEqual(@as(usize, 0), plan.plan().sparse_fields.len);
        const a_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc", "a");
        defer alloc.free(a_key);
        const b_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc", "b");
        defer alloc.free(b_key);
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = if (sparse)
            "{\"title\":\"initial\",\"_embeddings\":{\"a\":{\"indices\":[7],\"values\":[1]},\"b\":{\"indices\":[7],\"values\":[2]}}}"
        else
            "{\"title\":\"initial\",\"_embeddings\":{\"a\":[1,0,0],\"b\":[0,1,0]}}" }}, .sync_level = .full_index });
        const a_before = try db.core.store.get(alloc, a_key);
        defer alloc.free(a_before);
        const b_before = try db.core.store.get(alloc, b_key);
        defer alloc.free(b_before);
        const source = if (sparse) "{\"embedding\":{\"indices\":[7],\"values\":[9]}}" else "{\"embedding\":[0,0,9]}";
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = source }}, .sync_level = .full_index });
        const a_after = try db.core.store.get(alloc, a_key);
        defer alloc.free(a_after);
        const b_after = try db.core.store.get(alloc, b_key);
        defer alloc.free(b_after);
        try std.testing.expectEqualStrings(a_before, a_after);
        try std.testing.expectEqualStrings(b_before, b_after);
        const stored = (try db.get(alloc, "doc")).?;
        defer alloc.free(stored);
        try std.testing.expectEqualStrings(source, stored);
        // A source field must not conflict with the explicit deletion, and
        // omitting the sibling index must preserve its exact artifact payload.
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = if (sparse)
            "{\"embedding\":{\"indices\":[7],\"values\":[8]},\"_embeddings\":{\"a\":null}}"
        else
            "{\"embedding\":[0,0,8],\"_embeddings\":{\"a\":null}}" }}, .sync_level = .full_index });
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, a_key));
        const b_retained = try db.core.store.get(alloc, b_key);
        defer alloc.free(b_retained);
        try std.testing.expectEqualStrings(b_before, b_retained);
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        for (stats.indexes) |index| {
            const expected: u64 = if (std.mem.eql(u8, index.name, "a")) 0 else 1;
            try std.testing.expectEqual(expected, index.doc_count);
            try std.testing.expectEqual(expected, index.coverage_produced_count);
        }
        // Ordinary application data need not be parseable as a vector.
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"embedding\":\"application data\"}" }}, .sync_level = .full_index });
    }
}

test "db document _embeddings transaction null reserves retained artifact tombstones" {
    const alloc = std.testing.allocator;
    const retention = retained_effects_mod;
    for ([_]bool{ false, true }) |sparse| {
        var dir = try TestDirectory.init("external-null-transaction");
        defer dir.cleanup();
        var db = try DB.open(alloc, dir.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "vec", .kind = if (sparse) .sparse_vector else .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}" });
        const document: [256]u8 = @splat('x');
        try db.batch(.{ .writes = &.{.{ .key = &document, .value = if (sparse) "{\"title\":\"initial\",\"_embeddings\":{\"vec\":{\"indices\":[7],\"values\":[1]}}}" else "{\"title\":\"initial\",\"_embeddings\":{\"vec\":[1,0,0]}}" }}, .timestamp_ns = 10, .sync_level = .full_index });
        const key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, &document, "vec");
        defer alloc.free(key);
        const before = try db.core.store.get(alloc, key);
        defer alloc.free(before);
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            const ns = (try txn.get(&internal_keys.identity_namespace_key))[0..24].*;
            _ = try retention.admitWithDirectVectors(&txn, ns, 1, @splat(9), retention.default_limit, true);
            try txn.commit();
        }
        const edit = try db.beginTransaction(20);
        try db.writeTransaction(edit, .{ .transforms = &.{.{ .key = &document, .operations = &.{.{ .op = .set, .path = "caption", .value_json = "\"edited\"" }} }} });
        try db.resolveTransactionIntentsWithSyncLevel(edit, .committed, 30, .full_index);
        const after_edit = try db.core.store.get(alloc, key);
        defer alloc.free(after_edit);
        try std.testing.expectEqualStrings(before, after_edit);
        const abort = try db.beginTransaction(40);
        try db.writeTransaction(abort, .{ .writes = &.{.{ .key = &document, .value = "{\"_embeddings\":{\"vec\":null}}" }} });
        try db.resolveTransactionIntents(abort, .aborted, 50);
        const after_abort = try db.core.store.get(alloc, key);
        defer alloc.free(after_abort);
        try std.testing.expectEqualStrings(before, after_abort);
        const remove = try db.beginTransaction(60);
        try db.writeTransaction(remove, .{ .writes = &.{.{ .key = &document, .value = "{\"title\":\"removed\",\"_embeddings\":{\"vec\":null}}" }} });
        var reserved: u64 = 0;
        var retained_before: u64 = 0;
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            reserved = (try retention.loadReservations(&read)).?.bytes;
            retained_before = (try retention.load(&read)).?.retained_bytes;
        }
        try db.resolveReplicatedTransactionAtOrderedReceipt(remove, .committed, 70, .full_index, .none, .{ .term = 1, .index = 1 }, null);
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const actual = (try retention.load(&read)).?.retained_bytes - retained_before;
            try std.testing.expect(actual <= reserved);
        }
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, key));
    }
}

test "db document _embeddings stale replay skips external null with surviving source" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |sparse| {
        var dir = try TestDirectory.init("external-null-window");
        defer dir.cleanup();
        var db = try DB.open(alloc, dir.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "vec", .kind = if (sparse) .sparse_vector else .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = if (sparse) "{\"_embeddings\":{\"vec\":{\"indices\":[7],\"values\":[3]}}}" else "{\"_embeddings\":{\"vec\":[0,0,1]}}" }}, .sync_level = .write });
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"title\":\"survives\",\"_embeddings\":{\"vec\":null}}" }}, .sync_level = .write });
        const artifact = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc", "vec");
        defer alloc.free(artifact);
        // Model an earlier replay window that contains only the stale upsert.
        // The later null has already committed, but its journal record will be
        // consumed in a subsequent window. Source document existence is irrelevant.
        if (sparse) {
            var writes = try collectSparseEmbeddingWritesForBatch(alloc, db.core.index_manager, &.{}, &.{artifact}, "vec");
            defer writes.deinit();
            try filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay(db.async_context, "vec", &writes, 1);
            try std.testing.expectEqual(@as(usize, 0), writes.writes.len);
        } else {
            var writes = try collectDenseEmbeddingWritesForBatch(alloc, db.core.index_manager, &.{}, &.{artifact}, "vec");
            defer writes.deinit();
            try filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay(db.async_context, "vec", 3, &writes, 1);
            try std.testing.expectEqual(@as(usize, 0), writes.writes.len);
        }
        const issues = try db.listEmbeddingArtifactRepairIssues(alloc, "vec", 0);
        defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 0), issues.len);
        try db.runUntilIdle();
    }
}

test "db document _embeddings queued null replacement and omission converge" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |sparse| {
        var dir = try TestDirectory.init("external-null-queued");
        defer dir.cleanup();
        var db = try DB.open(alloc, dir.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        try db.addIndex(.{ .name = "vec", .kind = if (sparse) .sparse_vector else .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}" });
        const value = if (sparse) "{\"title\":\"vector\",\"_embeddings\":{\"vec\":{\"indices\":[7],\"values\":[3]}}}" else "{\"title\":\"vector\",\"_embeddings\":{\"vec\":[0,0,1]}}";
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = value }}, .sync_level = .write });
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"_embeddings\":{\"vec\":null}}" }}, .sync_level = .write });
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = value }}, .sync_level = .write });
        try db.batch(.{ .transforms = &.{.{ .key = "doc", .operations = &.{.{ .op = .set, .path = "caption", .value_json = "\"preserved\"" }} }}, .sync_level = .write });
        try db.runUntilIdle();
        {
            var result = try db.search(alloc, .{ .index_name = "vec", .query = if (sparse) .{ .sparse_knn = .{ .indices = &.{7}, .values = &.{1}, .k = 10 } } else .{ .dense_knn = .{ .vector = &.{ 0, 0, 1 }, .k = 10 } }, .limit = 10 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        }
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = value }}, .sync_level = .write });
        try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"_embeddings\":{\"vec\":null}}" }}, .sync_level = .write });
        db.close();
        db = try DB.open(alloc, dir.path(), .{});
        try db.runUntilIdle();
        var result = try db.search(alloc, .{ .index_name = "vec", .query = if (sparse) .{ .sparse_knn = .{ .indices = &.{7}, .values = &.{1}, .k = 10 } } else .{ .dense_knn = .{ .vector = &.{ 0, 0, 1 }, .k = 10 } }, .limit = 10 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 0), result.hits.len);
    }
}

test "db document _embeddings update vector index and strip stored special fields with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    const t = try TestDb.open(alloc, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer t.deinit();
    const db = &t.db;

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_embeddings\"") == null);

    var before = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer before.deinit();

    try std.testing.expectEqualStrings("doc:a", before.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dv_v1\":[0,0,1]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_embeddings\"") == null);

    var after = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer after.deinit();

    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db document deletion retires graph source contender and due entry" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db-graph-source-owner-delete");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.addEnrichment(.{ .name = "relations_v1", .kind = .asset, .field = "relations", .content_type = "application/json" });
    try db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json =
        \\{"ttl":{"duration":"1h"},"sources":[{"artifact":"relations_v1"}]}
    });
    try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"relations":{"type":"mentions","target":{"document_id":"doc:b"}}}
    }}, .sync_level = .enrichments });
    try db.runUntilIdle();
    const due_before = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_before);
    try std.testing.expectEqual(@as(usize, 1), due_before.len);
    const candidate = try graph_edge_ttl_expiration.decode(due_before[0].value);
    const contender_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, candidate.index_name, candidate.generation, candidate.edge_key, candidate.source_priority, candidate.state_key);
    defer alloc.free(contender_key);
    try db.batch(.{ .deletes = &.{"doc:a"} });
    const due_after = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due_after);
    try std.testing.expectEqual(@as(usize, 0), due_after.len);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, contender_key));
}

test "document extraction catalog snapshot releases every allocation on failure" {
    const Harness = struct {
        const Entry = struct {
            kind: enum { chunk, embedding },
            name: []const u8,
            source_artifact_name: []const u8,
            source_field: []const u8 = "text",
            expected_dims: u32 = 0,
            producer_json: []const u8 = "{\"model\":\"test\"}",
            chunker_json: []const u8 = "{}",
            full_text_index: bool = true,
            chunk_size: u32 = 10,
            chunk_overlap: u32 = 2,
        };
        const Lock = struct {
            held: bool = false,
            pub fn lockShared(self: *@This()) void {
                std.debug.assert(!self.held);
                self.held = true;
            }
            pub fn unlockShared(self: *@This()) void {
                std.debug.assert(self.held);
                self.held = false;
            }
        };
        const Manager = struct {
            catalog_mutex: Lock = .{},
            enrichments: struct { items: []const Entry } = .{ .items = &.{
                .{ .kind = .chunk, .name = "chunks", .source_artifact_name = "asset" },
                .{ .kind = .embedding, .name = "dense", .source_artifact_name = "chunks", .expected_dims = 2 },
                .{ .kind = .embedding, .name = "sparse", .source_artifact_name = "chunks" },
                .{ .kind = .chunk, .name = "other_chunks", .source_artifact_name = "asset" },
                .{ .kind = .embedding, .name = "other_dense", .source_artifact_name = "other_chunks", .expected_dims = 2 },
                .{ .kind = .embedding, .name = "other_sparse", .source_artifact_name = "other_chunks" },
            } },
            fn indexes(self: *@This(), alloc: Allocator) ![][]u8 {
                std.debug.assert(self.catalog_mutex.held);
                const names = try alloc.alloc([]u8, 1);
                errdefer alloc.free(names);
                names[0] = try alloc.dupe(u8, "consumer");
                return names;
            }
            pub fn denseIndexesForEmbedding(self: *@This(), alloc: Allocator, _: []const u8, _: u32) ![][]u8 {
                return self.indexes(alloc);
            }
            pub fn sparseIndexesForEmbedding(self: *@This(), alloc: Allocator, _: []const u8) ![][]u8 {
                return self.indexes(alloc);
            }
            pub fn textIndexesForChunk(self: *@This(), alloc: Allocator, _: []const u8, _: bool) ![][]u8 {
                return self.indexes(alloc);
            }
        };
        fn run(alloc: Allocator) !void {
            var manager = Manager{};
            defer std.debug.assert(!manager.catalog_mutex.held);
            const db = .{ .core = .{ .index_manager = &manager } };
            var snapshot = try buildDocumentExtractionCatalogView(alloc, db, "asset");
            defer snapshot.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), snapshot.chunks.len);
            for (snapshot.chunks) |chunk| {
                try std.testing.expectEqual(@as(usize, 1), chunk.dense_embeddings.len);
                try std.testing.expectEqual(@as(usize, 1), chunk.sparse_embeddings.len);
                try std.testing.expectEqualStrings("consumer", chunk.text_indexes[0]);
                try std.testing.expectEqualStrings("consumer", chunk.dense_embeddings[0].consumer_indexes[0]);
                try std.testing.expectEqualStrings("consumer", chunk.sparse_embeddings[0].consumer_indexes[0]);
            }
        }
    };
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "db document extraction concurrent writes with unchanged TTL timestamps converge" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

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

    try db.setSchema(.{ .version = 1, .ttl_duration_ns = 60 * std.time.ns_per_s, .ttl_field = "expires" });

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
        .name = "document_vectors",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"embedding_name\":\"document_chunk_dense_v1\"}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"decoy\",\"expires\":4102444800000000000}" }},
        .sync_level = .full_index,
    });

    var hook = PortableRuntimeBatchPrelockTestHook{};
    db_zig.test_portable_runtime_batch_prelock_hook = &hook;
    defer db_zig.test_portable_runtime_batch_prelock_hook = null;

    // Precompute against the original alpha artifacts, then park before
    // commit. Both concurrent writes retain the same TTL field value.
    const Writer = struct {
        db: *DB,
        value: []const u8,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.db.batch(.{
                .writes = &.{.{ .key = "doc:a", .value = self.value }},
                .sync_level = .enrichments,
            }) catch |err| {
                self.err = err;
            };
        }
    };
    var writer_a = Writer{
        .db = &db,
        .value = "{\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YSBnYW1tYQ==\",\"text\":\"decoy\",\"expires\":4102444800000000000}",
    };
    const thread_a = try std.Thread.spawn(.{}, Writer.run, .{&writer_a});
    var joined_a = false;
    defer if (!joined_a) {
        hook.release.store(true, .release);
        thread_a.join();
    };

    const prelock_deadline = monotonicTimeNs() + 5 * std.time.ns_per_s;
    while (!hook.entered.load(.acquire) and monotonicTimeNs() < prelock_deadline) {
        platform.time.yieldNow();
    }
    try std.testing.expect(hook.entered.load(.acquire));

    // Commit different content for the same key while thread_a is parked.
    // Clear the hook first so this write is not itself gated by it.
    db_zig.test_portable_runtime_batch_prelock_hook = null;
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YmV0YSBnYW1tYSBkZWx0YQ==\",\"text\":\"decoy\",\"expires\":4102444800000000000}",
        }},
        .sync_level = .full_index,
    });

    // Release thread_a. Its stale pre-lock precompute (built against the
    // original alpha document) must be detected and retried against the
    // now-current state, not silently committed on top of it -- which would
    // either corrupt the generation counter or leave orphaned chunk/embedding
    // artifacts from the intermediate commit.
    hook.release.store(true, .release);
    thread_a.join();
    joined_a = true;
    try std.testing.expectEqual(@as(?anyerror, null), writer_a.err);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const manifest = try db.core.store.get(alloc, manifest_key);
    defer alloc.free(manifest);

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "document_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_entries = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_entries);
    var chunk_count: usize = 0;
    var last_chunk_payload: []const u8 = "";
    for (chunk_entries) |entry| if (internal_keys.isChunkArtifactRecordKey(entry.key)) {
        chunk_count += 1;
        last_chunk_payload = entry.value;
    };
    // Exactly one surviving chunk, matching thread_a's final ("alpha beta
    // gamma") content -- no orphaned chunk from the intermediate ("beta
    // gamma delta") generation this write raced against.
    const final_primary = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedEqual;
    defer alloc.free(final_primary);
    try std.testing.expect(std.mem.indexOf(u8, final_primary, "YWxwaGEgYmV0YSBnYW1tYQ==") != null);
    try std.testing.expect(std.mem.indexOf(u8, last_chunk_payload, "alpha beta gamma") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"generation\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"from_generation\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"to_generation\":3") != null);
    try std.testing.expectEqual(@as(usize, 1), chunk_count);
    try std.testing.expect(std.mem.indexOf(u8, last_chunk_payload, "alpha beta gamma") != null);
}

test "db document lookup allocation benchmark" {
    const Counter = allocation_bench_support_mod.Counter;
    const Case = enum { short, long, missing, sparse, sparse_large, text_large, embedding_identity, embedding_identity_chunk, text, text_asset, text_asset_large, text_asset_escaped, text_mixed, text_missing, delete_set, relational, json_numbers, json_media };
    const case_name = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_CASE")) |raw| std.mem.span(raw) else "short";
    const fixture = std.meta.stringToEnum(Case, case_name) orelse return error.InvalidBenchmarkCase;
    // Setup is outside measurement. Use the C allocator here to avoid
    // debug-allocator bookkeeping dominating large fixture construction;
    // measured collectors use the counter or production smp allocator.
    const alloc = std.heap.c_allocator;
    const count = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_DOCUMENTS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 50_000;
    if (count == 0) return error.InvalidBenchmarkDocuments;
    const batch_size = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_BATCH")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 256;
    if (batch_size == 0) return error.InvalidBenchmarkBatch;
    if (fixture == .json_numbers or fixture == .json_media)
        return jsonOutputAllocationBenchmark(case_name, count, batch_size);
    if (fixture == .embedding_identity or fixture == .embedding_identity_chunk)
        return embeddingIdentityAllocationBenchmark(case_name, count, batch_size);
    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore_mod.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var directory = try TestDirectory.init("collector-benchmark");
    defer directory.cleanup();
    const text_fixture = fixture == .text_large or fixture == .text or fixture == .text_asset or fixture == .text_asset_large or fixture == .text_asset_escaped or fixture == .text_mixed or fixture == .text_missing;
    var db: ?DB = if (text_fixture or fixture == .relational) try DB.open(alloc, directory.path(), .{ .start_index_workers = false }) else null;
    defer if (db) |*database| database.close();
    const sparse_fixture = fixture == .sparse or fixture == .sparse_large;
    const value_bytes = if (std.c.getenv("ANTFLY_DOCUMENT_BENCH_VALUE_BYTES")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 4096;
    var large_value: ?[]u8 = null;
    defer if (large_value) |bytes| alloc.free(bytes);
    if (fixture == .text_large or fixture == .sparse_large) {
        const prefix = if (sparse_fixture) "{\"vec\":{\"indices\":[1,3],\"values\":[0.5,0.25]},\"padding\":\"" else "{\"title\":\"";
        const len = try std.math.add(usize, prefix.len + 2, value_bytes);
        const bytes = try alloc.alloc(u8, len);
        @memcpy(bytes[0..prefix.len], prefix);
        @memset(bytes[prefix.len..][0..value_bytes], 'x');
        @memcpy(bytes[len - 2 ..], "\"}");
        large_value = bytes;
    }
    const value = large_value orelse if (sparse_fixture) "{\"vec\":{\"indices\":[1,3],\"values\":[0.5,0.25]}}" else "{\"title\":\"document value\"}";
    const relational_schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"additionalProperties\":false}}}}";
    var packed_value: ?[]u8 = null;
    defer if (packed_value) |packed_bytes| alloc.free(packed_bytes);
    if (fixture == .relational) {
        try db.?.setSchemaJson(alloc, relational_schema);
        var parsed_schema = try public_table_schema.parseValidatedTableSchema(alloc, relational_schema);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer schema_mod.freeSchema(alloc, runtime_schema);
        var parsed_value = try std.json.parseFromSlice(std.json.Value, alloc, value, .{});
        defer parsed_value.deinit();
        packed_value = try mapper.buildRelationalRowValueForSchemaFromParsedAlloc(alloc, parsed_value.value, runtime_schema);
    }
    if (text_fixture) try db.?.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    const asset_name = if (fixture == .text_asset_escaped) "cap\x00tion" else "caption";
    const large_asset_text = comptime blk: {
        const pattern = "quote\" newline\n null\x00 café 🚀";
        var text: [pattern.len * 128]u8 = undefined;
        for (0..128) |i| @memcpy(text[i * pattern.len ..][0..pattern.len], pattern);
        break :blk text;
    };
    const asset_raw: []const u8 = if (fixture == .text_asset_large) &large_asset_text else "projected value";
    const asset_projected = try std.fmt.allocPrint(alloc, "{{\"caption\":{f}}}", .{std.json.fmt(asset_raw, .{})});
    defer alloc.free(asset_projected);
    if (text_fixture) try db.?.addEnrichment(.{ .name = asset_name, .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const targets = [_]derived_types.DerivedTargetRef{.{ .kind = .full_text, .index_name = "text" }};
    const names = try alloc.alloc([544]u8, count);
    defer alloc.free(names);
    const docs = try alloc.alloc(derived_types.DerivedDocument, count);
    defer alloc.free(docs);
    const stored = try alloc.alloc(docstore_mod.KVPair, count);
    defer alloc.free(stored);
    var initialized: usize = 0;
    defer for (stored[0..initialized]) |write| alloc.free(write.key);
    for (names, docs, stored, 0..) |*name, *doc, *write, i| {
        const prefix = try std.fmt.bufPrint(name, "document-{d:0>10}", .{i});
        const key = if (fixture == .long) blk: {
            @memset(name[prefix.len..512], 'x');
            // Exercise escaped component encoding and multiple key blocks.
            name[100] = 0;
            break :blk name[0..512];
        } else prefix;
        const is_asset = fixture == .text_asset or fixture == .text_asset_large or fixture == .text_asset_escaped or (fixture == .text_mixed and i % 8 == 0);
        const stored_key = if (is_asset) try internal_keys.artifactNamedPrefixAlloc(alloc, key, "asset", asset_name) else try replayDocumentStoreKeyAlloc(alloc, key, fixture == .relational);
        doc.* = .{ .key = if (is_asset) stored_key else key, .action = .upsert, .targets = &targets };
        write.* = .{ .key = stored_key, .value = if (is_asset) asset_raw else packed_value orelse value };
        initialized += 1;
    }
    const expected_count = if (fixture == .missing or fixture == .text_missing) count / 5 else count;
    try store.putBatch(stored[0..expected_count], &.{});
    for (0..2) |measurement| {
        for (0..2) |sample| {
            var counter: Counter = .{};
            const run_alloc = if (measurement == 0) counter.allocator() else std.heap.smp_allocator;
            var checksum: usize = 0;
            var output_count: usize = 0;
            const started = monotonicTimeNs();
            var offset: usize = 0;
            while (offset < count) {
                const end = @min(count, offset + batch_size);
                if (fixture == .delete_set) {
                    var keys = std.ArrayListUnmanaged([]const u8).empty;
                    defer keys.deinit(run_alloc);
                    var seen = std.StringHashMapUnmanaged(void).empty;
                    defer seen.deinit(run_alloc);
                    // Exactly six keys fill the default map's first load limit.
                    const unique = @min(@as(usize, 6), end - offset);
                    for (docs[offset..][0..unique]) |doc| try appendUniqueBorrowedKeyWithSet(run_alloc, &keys, &seen, doc.key);
                    for (0..128) |_| for (docs[offset..][0..unique]) |doc| try appendUniqueBorrowedKeyWithSet(run_alloc, &keys, &seen, doc.key);
                    try std.testing.expectEqual(unique, keys.items.len);
                    for (keys.items) |key| for (key) |byte| {
                        checksum +%= byte;
                    };
                    output_count += end - offset;
                } else if (sparse_fixture) {
                    var writes = try collectSparseFieldWritesProfiled(run_alloc, &store, null, docs[offset..end], .{ .start = "", .end = "" }, "vec", .{}, null);
                    defer writes.deinit();
                    try std.testing.expectEqual(end - offset, writes.items.len);
                    for (writes.items, docs[offset..end]) |write, doc| {
                        try std.testing.expectEqualStrings(doc.key, write.doc_id);
                        try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, write.vec.indices);
                        try std.testing.expectEqualSlices(f32, &.{ 0.5, 0.25 }, write.vec.values);
                        for (write.doc_id) |byte| checksum +%= byte;
                    }
                    output_count += writes.items.len;
                } else if (text_fixture) {
                    var writes = try collectTextDocumentWritesForIndex(run_alloc, &store, db.?.core.index_manager, docs[offset..end], "text", false, .{ .start = "", .end = "" }, .{});
                    defer writes.deinit();
                    const available = @min(end, expected_count) -| offset;
                    try std.testing.expectEqual(available, writes.docs.items.len);
                    for (writes.docs.items, docs[offset..][0..available]) |write, doc| {
                        try std.testing.expectEqualStrings(doc.key, write.key);
                        try std.testing.expectEqualStrings(if (internal_keys.isAssetArtifactKey(doc.key)) asset_projected else value, write.value);
                        for (write.key) |byte| checksum +%= byte;
                    }
                    output_count += writes.docs.items.len;
                } else {
                    var writes = try collectDocumentWritesProfiled(run_alloc, &store, if (db) |database| database.core.index_manager else null, docs[offset..end], .{ .start = "", .end = "" }, .{ .relational_base_rows = fixture == .relational }, null);
                    defer writes.deinit();
                    const available = @min(end, expected_count) -| offset;
                    try std.testing.expectEqual(available, writes.items.len);
                    for (writes.items, docs[offset .. offset + available]) |write, doc| {
                        try std.testing.expectEqualStrings(doc.key, write.key);
                        try std.testing.expectEqualStrings(if (internal_keys.isAssetArtifactKey(doc.key)) asset_projected else value, write.value);
                        for (write.key) |byte| checksum +%= byte;
                    }
                    output_count += writes.items.len;
                }
                offset = end;
            }
            const elapsed = monotonicTimeNs() - started;
            try std.testing.expectEqual(expected_count, output_count);
            try std.testing.expectEqual(@as(usize, 0), counter.live);
            if (sample != 0) std.debug.print("document_lookup_bench {{\"case\":\"{s}\",\"documents\":{d},\"batch\":{d},\"measurement\":\"{s}\",\"elapsed_ns\":{d},\"allocations\":{d},\"resize_calls\":{d},\"remap_calls\":{d},\"moving_remaps\":{d},\"moved_bytes\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"checksum\":{d}}}\n", .{ @tagName(fixture), count, batch_size, if (measurement == 0) "counted" else "timing", elapsed, counter.calls, counter.resize_calls, counter.remap_calls, counter.moving_remaps, counter.moved_bytes, counter.bytes, counter.peak, checksum });
        }
    }
}

test "document collectors release text projections and materialized values on allocation failures" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("text-collector-failures");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{});
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "caption", .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
    defer alloc.free(asset_key);
    const key = try replayDocumentStoreKeyAlloc(alloc, "normal", false);
    defer alloc.free(key);
    try db.core.store.putBatch(&.{ .{ .key = asset_key, .value = "projected value" }, .{ .key = key, .value = "{\"title\":\"normal\"}" } }, &.{});
    const targets = [_]derived_types.DerivedTargetRef{.{ .kind = .full_text, .index_name = "text" }};
    const docs = [_]derived_types.DerivedDocument{
        .{ .key = asset_key, .action = .upsert, .targets = &targets, .cleaned_value = "projected value" },
        .{ .key = "normal", .action = .upsert, .targets = &targets, .cleaned_value = "{\"title\":\"normal\"}" },
    };
    for ([_]bool{ false, true }) |inline_values| {
        // Mixed batches exercise per-value ownership; ordinary batches also
        // exhaust failures before and after the exact-size slab allocation.
        try platform.allocator.checkAllAllocationFailures(alloc, textCollectorFailureSweep, .{ db.core.store, db.core.index_manager, @as([]const derived_types.DerivedDocument, &docs), inline_values });
        try platform.allocator.checkAllAllocationFailures(alloc, textCollectorFailureSweep, .{ db.core.store, db.core.index_manager, @as([]const derived_types.DerivedDocument, docs[1..]), inline_values });
    }
}

test "document collectors duplicate delete keys survive growth failure at map capacity" {
    const alloc = std.testing.allocator;
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    defer keys.deinit(alloc);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f" }) |key|
        try appendUniqueBorrowedKeyWithSet(alloc, &keys, &seen, key);
    try std.testing.expectEqual(@as(u32, 8), seen.capacity());
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    for (0..128) |_| try appendUniqueBorrowedKeyWithSet(failing.allocator(), &keys, &seen, "a");
    try std.testing.expectEqual(@as(usize, 6), keys.items.len);
    try std.testing.expectEqual(@as(u32, 8), seen.capacity());
    try std.testing.expectError(error.OutOfMemory, appendUniqueBorrowedKeyWithSet(failing.allocator(), &keys, &seen, "g"));
    try std.testing.expectEqual(@as(usize, 6), keys.items.len);
    try std.testing.expect(failing.has_induced_failure);
    // The normal allocator still permits pre-probe map growth. This test
    // promises recovery under pressure, not allocation-free normal insertion.
    try appendUniqueBorrowedKeyWithSet(alloc, &keys, &seen, "a");
    try std.testing.expect(seen.capacity() > 8);
    try std.testing.expectEqual(@as(usize, 6), keys.items.len);
}

test "document collectors asset projection borrows ordinary names and preserves escaped names" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("asset-name-projection");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_index_workers = false });
    defer db.close();
    try db.addEnrichment(.{ .name = "json", .kind = .asset, .field = "caption", .content_type = "application/json" });
    try db.addEnrichment(.{ .name = "cap\x00tion", .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const ordinary = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc\x00id", "asset", "json");
    defer alloc.free(ordinary);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectEqual(@as(?[]u8, null), try textualAssetFullTextProjectionAlloc(failing.allocator(), db.core.index_manager, ordinary, "{}"));
    const escaped = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc\x00id", "asset", "cap\x00tion");
    defer alloc.free(escaped);
    var long_name = @as([512]u8, @splat('x'));
    long_name[1] = 0;
    const long_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", &long_name);
    defer alloc.free(long_key);
    try std.testing.expectError(error.OutOfMemory, textualAssetFullTextProjectionAlloc(failing.allocator(), db.core.index_manager, long_key, "value"));
    try std.testing.expectEqual(@as(?[]u8, null), try textualAssetFullTextProjectionAlloc(alloc, db.core.index_manager, long_key, "value"));
    const Check = struct {
        fn run(a: Allocator, manager: *index_manager_mod.IndexManager, key: []const u8) !void {
            const result = (try textualAssetFullTextProjectionAlloc(a, manager, key, "projected value")).?;
            defer a.free(result);
            try std.testing.expectEqualStrings("{\"caption\":\"projected value\"}", result);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{ db.core.index_manager, @as([]const u8, escaped) });
}

test "document collectors ordinary slab excludes projected assets" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("text-materialization-hint");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "caption", .kind = .asset, .field = "caption", .content_type = "text/plain" });
    const asset = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
    defer alloc.free(asset);
    const normal = try replayDocumentStoreKeyAlloc(alloc, "normal", false);
    defer alloc.free(normal);
    try db.core.store.putBatch(&.{ .{ .key = asset, .value = "projected value" }, .{ .key = normal, .value = "{\"title\":\"normal\"}" } }, &.{});
    const empty = try replayDocumentStoreKeyAlloc(alloc, "empty", false);
    defer alloc.free(empty);
    const second = try replayDocumentStoreKeyAlloc(alloc, "second", false);
    defer alloc.free(second);
    try db.core.store.putBatch(&.{ .{ .key = empty, .value = "" }, .{ .key = second, .value = "{\"title\":\"second document\"}" } }, &.{});
    const targets = [_]derived_types.DerivedTargetRef{.{ .kind = .full_text, .index_name = "text" }};
    var docs = @as([32]derived_types.DerivedDocument, @splat(.{ .key = asset, .action = .upsert, .targets = &targets }));
    docs[29].key = "normal";
    docs[30].key = "empty";
    docs[31].key = "second";
    var result = try collectTextDocumentWritesForIndex(alloc, db.core.store, db.core.index_manager, docs[29..], "text", false, .{ .start = "", .end = "" }, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.docs.items.len);
    try std.testing.expectEqual(@as(usize, 0), result.materialized_documents.items.len);
    try std.testing.expectEqualStrings("{\"title\":\"normal\"}{\"title\":\"second document\"}", result.document_bytes);
    // The collector has already closed its read transaction. Replacing the
    // source document must not change the returned owned slice.
    try db.core.store.putBatch(&.{.{ .key = normal, .value = "{}" }}, &.{});
    try std.testing.expectEqualStrings("{\"title\":\"normal\"}", result.docs.items[1].value);
    try std.testing.expectEqualStrings("", result.docs.items[0].value);
    try std.testing.expectEqualStrings("{\"title\":\"second document\"}", result.docs.items[2].value);
    try std.testing.expectEqual(result.document_bytes.ptr, result.docs.items[1].value.ptr);
    try std.testing.expectEqual(result.document_bytes.ptr + result.docs.items[1].value.len, result.docs.items[2].value.ptr);
    var mixed = try collectTextDocumentWritesForIndex(alloc, db.core.store, db.core.index_manager, &docs, "text", false, .{ .start = "", .end = "" }, .{});
    defer mixed.deinit();
    try std.testing.expectEqual(@as(usize, 32), mixed.docs.items.len);
    try std.testing.expectEqual(@as(usize, 0), mixed.document_bytes.len);
    try std.testing.expectEqual(@as(usize, 3), mixed.materialized_documents.items.len);
    try std.testing.expectEqualStrings("{}", mixed.docs.items[30].value);
    try std.testing.expectEqualStrings("{\"caption\":\"projected value\"}", mixed.docs.items[0].value);
}

test "document collectors JSON string writing preserves escaping and output ownership on failure" {
    const Check = struct {
        fn run(a: Allocator, input: []const u8, expected: []const u8) !void {
            var output = std.ArrayListUnmanaged(u8).empty;
            defer output.deinit(a);
            try output.appendSlice(a, "prefix:");
            try appendJsonString(a, &output, input);
            try std.testing.expectEqualStrings("prefix:", output.items[0..7]);
            try std.testing.expectEqualStrings(expected, output.items[7..]);
        }
    };
    const inputs = [_][]const u8{ "", "ordinary", "quote\" slash\\ newline\n null\x00", "café 🚀", &(@as([512]u8, @splat(0))) };
    for (inputs) |input| {
        const expected = try std.fmt.allocPrint(std.testing.allocator, "{f}", .{std.json.fmt(input, .{})});
        defer std.testing.allocator.free(expected);
        try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{ input, expected });
    }
    var output = std.ArrayListUnmanaged(u8).empty;
    defer output.deinit(std.testing.allocator);
    try output.ensureTotalCapacityPrecise(std.testing.allocator, 128);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try appendJsonString(failing.allocator(), &output, "quote\" newline\n");
    try std.testing.expectEqualStrings("\"quote\\\" newline\\n\"", output.items);
    try std.testing.expect(!failing.has_induced_failure);
}

test "document collectors JSON numeric fields preserve boundaries and ownership on failure" {
    const Check = struct {
        fn run(a: Allocator) !void {
            var out: std.ArrayListUnmanaged(u8) = .empty;
            defer out.deinit(a);
            try out.append(a, '{');
            var first = true;
            try appendJsonFieldU64(a, &out, &first, "zero", 0);
            try appendJsonFieldU64(a, &out, &first, "max", std.math.maxInt(u64));
            try appendJsonFieldUsize(a, &out, &first, "size", 42);
            try out.append(a, '}');
            try std.testing.expectEqualStrings("{\"zero\":0,\"max\":18446744073709551615,\"size\":42}", out.items);
        }
    };
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    var out: std.ArrayListUnmanaged(u8) = .empty;
    defer out.deinit(std.testing.allocator);
    try out.ensureTotalCapacityPrecise(std.testing.allocator, 128);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var first = true;
    try appendJsonFieldU64(failing.allocator(), &out, &first, "number", std.math.maxInt(u64));
    try std.testing.expectEqualStrings("\"number\":18446744073709551615", out.items);
    try std.testing.expect(!failing.has_induced_failure);
}

test "document collectors JSON media encoding preserves padding escaping and failure ownership" {
    const Check = struct {
        fn run(a: Allocator, data: []const u8, expected: []const u8) !void {
            const result = try contentPartsJsonAlloc(a, &.{ .{ .text = "prefix\n" }, .{ .binary = .{ .mime_type = "quoted\"type", .data = data } }, .{ .media_url = "suffix" } });
            defer a.free(result);
            try std.testing.expectEqualStrings(expected, result);
        }
    };
    const alloc = std.testing.allocator;
    var bytes: [16384]u8 = undefined;
    for (&bytes, 0..) |*byte, i| byte.* = @truncate(i);
    for ([_]usize{ 0, 1, 2, 3, 4, 5, 6, 256, 16384 }) |len| {
        const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(len));
        defer alloc.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, bytes[0..len]);
        const expected = try std.fmt.allocPrint(alloc, "[{{\"type\":\"text\",\"text\":\"prefix\\n\"}},{{\"type\":\"media\",\"mime_type\":\"quoted\\\"type\",\"data\":\"{s}\"}},{{\"type\":\"media\",\"url\":\"suffix\"}}]", .{encoded});
        defer alloc.free(expected);
        try platform.allocator.checkAllAllocationFailures(alloc, Check.run, .{ @as([]const u8, bytes[0..len]), @as([]const u8, expected) });
    }
}

test "document collectors sparse borrowed JSON owns numeric outputs and cleans every failure" {
    const alloc = std.testing.allocator;
    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore_mod.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const values = [_][]const u8{
        "{\"vec\":{\"indices\":[1,3],\"values\":[0.5,0.25]}}",
        "{\"v\\u0065c\":{\"indices\":[1,3],\"values\":[0.5,0.25]}}",
        "{\"vec\":{\"metadata\":{},\"indices\":[1,3],\"values\":[0.5,0.25]}}",
    };
    const ids = [_][]const u8{ "raw", "escaped", "metadata" };
    var docs: [3]derived_types.DerivedDocument = undefined;
    for (ids, values, &docs) |id, value, *doc| {
        const key = try internal_keys.documentKeyAlloc(alloc, id);
        defer alloc.free(key);
        try store.put(key, value);
        doc.* = .{ .key = id, .action = .upsert };
    }
    try platform.allocator.checkAllAllocationFailures(alloc, sparseCollectorOwnershipSweep, .{ &store, @as(?*index_manager_mod.IndexManager, null), @as([]const derived_types.DerivedDocument, &docs), false });
    var result = try collectSparseFieldWritesProfiled(alloc, &store, null, &docs, .{ .start = "", .end = "" }, "vec", .{}, null);
    defer result.deinit();
    // Returned numeric arrays outlive the collector's read transaction.
    for (ids) |id| {
        const key = try internal_keys.documentKeyAlloc(alloc, id);
        defer alloc.free(key);
        try store.put(key, "{}");
    }
    for (result.items) |write| {
        try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, write.vec.indices);
        try std.testing.expectEqualSlices(f32, &.{ 0.5, 0.25 }, write.vec.values);
    }
    var absent = try collectSparseFieldWritesProfiled(alloc, &store, null, &docs, .{ .start = "", .end = "" }, "vec", .{}, null);
    defer absent.deinit();
    try std.testing.expectEqual(@as(usize, 0), absent.items.len);
    const key = try internal_keys.documentKeyAlloc(alloc, ids[0]);
    defer alloc.free(key);
    try store.put(key, "{\"vec\":{\"indices\":[1,3],\"values\":[0.5]}}");
    try std.testing.expectError(error.InvalidSparseVector, collectSparseFieldWritesProfiled(alloc, &store, null, docs[0..1], .{ .start = "", .end = "" }, "vec", .{}, null));
}

test "document collectors sparse relational rows still decode under allocation failures" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("sparse-relational-ownership");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_index_workers = false });
    defer db.close();
    const schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"vec\":{\"type\":\"json\"}},\"additionalProperties\":false}}}}";
    try db.setSchemaJson(alloc, schema_json);
    var parsed_schema = try public_table_schema.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const schema = try public_table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, schema);
    var parsed_value = try std.json.parseFromSlice(std.json.Value, alloc, "{\"vec\":{\"indices\":[1,3],\"values\":[0.5,0.25]}}", .{});
    defer parsed_value.deinit();
    const packed_row = try mapper.buildRelationalRowValueForSchemaFromParsedAlloc(alloc, parsed_value.value, schema);
    defer alloc.free(packed_row);
    const key = try relational_store.keyAlloc(alloc, "row");
    defer alloc.free(key);
    try db.core.store.put(key, packed_row);
    const docs = [_]derived_types.DerivedDocument{.{ .key = "row", .action = .upsert }};
    try platform.allocator.checkAllAllocationFailures(alloc, sparseCollectorOwnershipSweep, .{ db.core.store, @as(?*index_manager_mod.IndexManager, db.core.index_manager), @as([]const derived_types.DerivedDocument, &docs), true });
}

test "document collectors embedding identity accepted and rejected keys clean every failure" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("embedding-write-identity");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "dense", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2}" });
    const chunk = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc\x00id", "chunks", "unit", 3);
    defer alloc.free(chunk);
    const doc = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc\x00id", "dense");
    defer alloc.free(doc);
    const derived = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk, "dense");
    defer alloc.free(derived);
    const rejected = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "other");
    defer alloc.free(rejected);
    for ([_][]const u8{ doc, derived, rejected }) |key| {
        for ([_]?*index_manager_mod.IndexManager{ null, db.core.index_manager }) |manager|
            try platform.allocator.checkAllAllocationFailures(alloc, embeddingWriteIdentitySweep, .{ key, @as([]const u8, "dense"), manager });
    }
    try std.testing.expectEqual(@as(?OwnedEmbeddingArtifactWriteIdentity, null), try decodeEmbeddingArtifactWriteIdentityAlloc(alloc, rejected, "dense"));
    try std.testing.expectEqual(@as(?OwnedEmbeddingArtifactWriteIdentity, null), try decodeEmbeddingArtifactWriteIdentityForManagedIndexAlloc(alloc, db.core.index_manager, .{ .name = "dense", .kind = .dense_vector }, rejected));
}
