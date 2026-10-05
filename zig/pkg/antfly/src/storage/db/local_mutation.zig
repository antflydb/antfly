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

//! Shared local mutation receiver and compile-time pipeline composition.
//! Preparation, commit and materialization have distinct source owners.
//! Public DB and synchronous recovery share these algorithms. Owning close,
//! resident scheduling and caller acknowledgement remain on their receivers.

const execution_resources = @import("execution_resources.zig");

const background_runtime_mod = @import("../background_runtime.zig");
const build_options = @import("build_options");
const builtin = @import("builtin");

const common_secrets = @import("../../common/secrets.zig");
const db_config = @import("config.zig");
const db_core = @import("core.zig");

const derived_executor_mod = @import("derived/derived_executor.zig");

const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");

const enrichment_types = @import("enrichment/enrichment_types.zig");

const internal_keys = @import("../internal_keys.zig");

const lsm_backend_mod = @import("../lsm_backend/mod.zig");

const promotion_runtime_mod = @import("promotion_runtime.zig");

const resolution_runtime_mod = @import("resolution_runtime.zig");

const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("scraping_stub.zig")
else
    @import("antfly_scraping");

const sparse_compaction_runtime_mod = @import("maintenance/sparse_compaction_runtime.zig");

const std = @import("std");

const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const transactions_mod = @import("../transactions.zig");

const types = @import("types.zig");

pub fn ImplementationFor(comptime D: type) type {
    return struct {
        const Implementation = @This();
        const preparation = @import("mutation_preparation.zig").ImplementationFor(Implementation, D);
        const commit = @import("mutation_commit.zig").ImplementationFor(Implementation, D);
        const materialization = @import("mutation_materialization.zig").ImplementationFor(Implementation, D);
        const Allocator = execution_resources.Allocator;
        const Io = std.Io;

        const AsyncContext = execution_resources.AsyncContext;

        const BatchExecutionOptions = execution_resources.BatchExecutionOptions;

        const ChunkCacheEntry = execution_resources.ChunkCacheEntry;

        const GraphRestoreParseCache = execution_resources.GraphRestoreParseCache;

        pub const InlineChunkEmbeddingCleanup = struct {
            groups: std.ArrayListUnmanaged(Group) = .empty,

            const Group = struct {
                artifact_name: []const u8,
                embeddings: std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)) = .empty,
            };

            pub fn deinit(self: *InlineChunkEmbeddingCleanup, alloc: Allocator) void {
                for (self.groups.items) |*group| {
                    var iterator = group.embeddings.iterator();
                    while (iterator.next()) |entry| {
                        var keys = entry.value_ptr.iterator();
                        while (keys.next()) |key| alloc.free(key.key_ptr.*);
                        entry.value_ptr.deinit(alloc);
                    }
                    group.embeddings.deinit(alloc);
                }
                self.groups.deinit(alloc);
            }

            pub fn add(
                self: *InlineChunkEmbeddingCleanup,
                alloc: Allocator,
                db: anytype,
                doc_value: []const u8,
                request: enrichment_types.GeneratedEnrichmentRequest,
                cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
            ) !void {
                const artifact_name = requestArtifactName(request);
                var group: *Group = undefined;
                for (self.groups.items) |*candidate| {
                    if (std.mem.eql(u8, candidate.artifact_name, artifact_name)) {
                        group = candidate;
                        break;
                    }
                } else {
                    try self.groups.append(alloc, .{ .artifact_name = artifact_name });
                    group = &self.groups.items[self.groups.items.len - 1];
                }

                const embedding_name = requestEmbeddingName(request);
                const entry = try group.embeddings.getOrPut(alloc, embedding_name);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                var chunks_created: usize = 0;
                const sources = try chunkEmbeddingSourcesForRequest(alloc, db, doc_value, request, cache, &chunks_created);
                defer freeChunkEmbeddingSources(alloc, sources);
                for (sources) |source| {
                    const key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, source.key, embedding_name);
                    errdefer alloc.free(key);
                    const desired_entry = try entry.value_ptr.getOrPut(alloc, key);
                    if (desired_entry.found_existing) alloc.free(key);
                }
            }

            pub fn flush(
                self: *const InlineChunkEmbeddingCleanup,
                alloc: Allocator,
                db: anytype,
                doc_key: []const u8,
                artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
            ) !void {
                for (self.groups.items) |group| {
                    const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "chunk", group.artifact_name);
                    defer alloc.free(prefix);
                    var cursor: ?[]u8 = null;
                    defer if (cursor) |key| alloc.free(key);
                    while (true) {
                        const existing = try db.core.store.scanPrefixKeysPage(alloc, prefix, cursor, 256);
                        defer freeOwnedKeySlice(alloc, existing);
                        if (existing.len == 0) break;
                        for (existing) |key| {
                            if (!internal_keys.isDerivedEmbeddingArtifactKey(key)) continue;
                            const embedding_name = (try internal_keys.artifactNameView(key)) orelse blk: {
                                // Escaped binary names cannot be borrowed from the key.
                                var names = group.embeddings.iterator();
                                while (names.next()) |entry| {
                                    if (internal_keys.matchesDerivedEmbeddingArtifactName(key, entry.key_ptr.*))
                                        break :blk entry.key_ptr.*;
                                }
                                continue;
                            };
                            const desired = group.embeddings.get(embedding_name) orelse continue;
                            if (!desired.contains(key)) {
                                const deleted_key = try alloc.dupe(u8, key);
                                errdefer alloc.free(deleted_key);
                                try artifact_delete_keys.append(alloc, deleted_key);
                            }
                        }
                        const next_cursor = try alloc.dupe(u8, existing[existing.len - 1]);
                        if (cursor) |key| alloc.free(key);
                        cursor = next_cursor;
                        if (existing.len < 256) break;
                    }
                }
            }
        };
        const LocalExecutionState = execution_resources.LocalExecutionState;

        const ManagedSyncTargets = execution_resources.ManagedSyncTargets;

        const OpenOptions = D.OpenOptions;

        const PrimaryBackend = execution_resources.PrimaryBackend;

        const ShadowState = execution_resources.ShadowState;

        pub const TransactionRecoveryLocalContext = struct {
            execution: ?Context = null,
            /// A recovery call borrows replaceable providers for its entire mutation.
            /// Reconfiguration takes this lock before retiring any provider runtime.
            provider_mutex: Io.Mutex = .init,
            /// Borrowed split state published under the core apply lock. The public DB
            /// wrapper is movable, so its `shadow` field is not a stable source for the
            /// recovery owner allocated during open.
            split_shadow: ?*ShadowState = null,
        };
        const TtlCleanupContext = execution_resources.TtlCleanupContext;

        const freeChunkEmbeddingSources = D.freeChunkEmbeddingSources;

        pub const CoalescedKeyValueRequest = preparation.CoalescedKeyValueRequest;
        pub const DenseArtifactCounterBootstrap = materialization.DenseArtifactCounterBootstrap;
        pub const DenseArtifactCounterCatalog = materialization.DenseArtifactCounterCatalog;
        pub const DenseArtifactCounterTarget = materialization.DenseArtifactCounterTarget;
        pub const DenseArtifactTargetKey = materialization.DenseArtifactTargetKey;
        pub const DenseArtifactTargetKeyContext = materialization.DenseArtifactTargetKeyContext;
        pub const FinalDenseArtifactMutation = materialization.FinalDenseArtifactMutation;
        pub const GeneratedWriteReadSnapshot = preparation.GeneratedWriteReadSnapshot;
        pub const PendingDenseArtifactCounterMutation = materialization.PendingDenseArtifactCounterMutation;
        pub const PreparedMergeArtifacts = preparation.PreparedMergeArtifacts;
        pub const TransformReadSnapshot = preparation.TransformReadSnapshot;
        pub const acquireReplicationMutationShared = commit.acquireReplicationMutationShared;
        pub const acquireTransactionSchemaView = materialization.acquireTransactionSchemaView;
        pub const appendDenseArtifactCounterBootstrapWrite = materialization.appendDenseArtifactCounterBootstrapWrite;
        pub const appendDenseArtifactCounterMutations = materialization.appendDenseArtifactCounterMutations;
        pub const appendDenseArtifactCounterMutationsWithPromotions = materialization.appendDenseArtifactCounterMutationsWithPromotions;
        pub const appendDenseArtifactTargetCounterWrite = materialization.appendDenseArtifactTargetCounterWrite;
        pub const appendGraphTransformDelete = materialization.appendGraphTransformDelete;
        pub const appendGraphTransformWrite = materialization.appendGraphTransformWrite;
        pub const applyDenseArtifactCounterDelta = materialization.applyDenseArtifactCounterDelta;
        pub const artifactMaterializationsReady = materialization.artifactMaterializationsReady;
        pub const batchContext = commit.batchContext;
        pub const batchInternalPrepared = commit.batchInternalPrepared;
        pub const batchInternalWithPreparationAllocator = commit.batchInternalWithPreparationAllocator;
        pub const captureGeneratedWriteReadSnapshot = preparation.captureGeneratedWriteReadSnapshot;
        pub const captureTransformReadSnapshot = preparation.captureTransformReadSnapshot;
        pub const childRangeManifestReader = materialization.childRangeManifestReader;
        pub const clearActiveIndexRepairsLocked = commit.clearActiveIndexRepairsLocked;
        pub const clearBulkIngestIdentityAllNewLocked = commit.clearBulkIngestIdentityAllNewLocked;
        pub const clearDurableReplicationOutbox = commit.clearDurableReplicationOutbox;
        pub const clearLiveDocSetCache = commit.clearLiveDocSetCache;
        pub const clearNonVisibleDocSetCache = commit.clearNonVisibleDocSetCache;
        pub const coalesceKeyValueRequest = preparation.coalesceKeyValueRequest;
        pub const decodeDenseArtifactCounterBootstrap = materialization.decodeDenseArtifactCounterBootstrap;
        pub const deinitOwnedGraphEdgeDelete = materialization.deinitOwnedGraphEdgeDelete;
        pub const deinitOwnedGraphEdgeWrite = materialization.deinitOwnedGraphEdgeWrite;
        pub const deleteDocumentArtifactChildRangeOutboxEntry = materialization.deleteDocumentArtifactChildRangeOutboxEntry;
        pub const denseArtifactCounterBootstrapKeyAlloc = materialization.denseArtifactCounterBootstrapKeyAlloc;
        pub const denseArtifactNameForEntry = materialization.denseArtifactNameForEntry;
        pub const denseArtifactTargetCounterKeyAlloc = materialization.denseArtifactTargetCounterKeyAlloc;
        pub const denseRepairWriteBackpressured = materialization.denseRepairWriteBackpressured;
        pub const dense_artifact_counter_bootstrap_encoded_len = materialization.dense_artifact_counter_bootstrap_encoded_len;
        pub const dense_artifact_counter_bootstrap_magic = materialization.dense_artifact_counter_bootstrap_magic;
        pub const dense_artifact_counter_bootstrap_prefix = materialization.dense_artifact_counter_bootstrap_prefix;
        pub const dense_artifact_target_counter_prefix = materialization.dense_artifact_target_counter_prefix;
        pub const derivedCoverageAppliesToIndex = materialization.derivedCoverageAppliesToIndex;
        pub const drainDocumentArtifactChildRangeOutbox = materialization.drainDocumentArtifactChildRangeOutbox;
        pub const encodeDenseArtifactCounterBootstrap = materialization.encodeDenseArtifactCounterBootstrap;
        pub const enforcePortableRuntimeGate = commit.enforcePortableRuntimeGate;
        pub const enforceReplicationWriteGate = commit.enforceReplicationWriteGate;
        pub const enforceRowPolicyMutationLocked = commit.enforceRowPolicyMutationLocked;
        pub const ensureDurableReplicationStartupBarrier = commit.ensureDurableReplicationStartupBarrier;
        pub const failIfIdentityOrdinalExhaustedForNewUpserts = commit.failIfIdentityOrdinalExhaustedForNewUpserts;
        pub const finalizePendingRowPolicyReceiptLocked = commit.finalizePendingRowPolicyReceiptLocked;
        pub const flushDurableReplicationOutboxes = commit.flushDurableReplicationOutboxes;
        pub const flushDurableReplicationOutboxesLocked = commit.flushDurableReplicationOutboxesLocked;
        pub const flushTransactionReplicationOutbox = commit.flushTransactionReplicationOutbox;
        pub const generatedPrecomputeModeForSyncLevel = preparation.generatedPrecomputeModeForSyncLevel;
        pub const graphMutationIdentityEql = materialization.graphMutationIdentityEql;
        pub const hasConfiguredResolvers = materialization.hasConfiguredResolvers;
        pub const hasCoordinatedConstraints = commit.hasCoordinatedConstraints;
        pub const identityUpsertStoreWritesAreNew = commit.identityUpsertStoreWritesAreNew;
        pub const isProtectedIntegrityKey = commit.isProtectedIntegrityKey;
        pub const isProtectedRangeWriteKey = commit.isProtectedRangeWriteKey;
        pub const loadDenseArtifactCounterBootstrap = materialization.loadDenseArtifactCounterBootstrap;
        pub const loadDenseArtifactTargetCounter = materialization.loadDenseArtifactTargetCounter;
        pub const lockApplyForPortableRuntime = commit.lockApplyForPortableRuntime;
        pub const maintenanceRequiresOrderedApply = commit.maintenanceRequiresOrderedApply;
        pub const markPrecomputedEnrichmentAppliedForSync = commit.markPrecomputedEnrichmentAppliedForSync;
        pub const maybeFinalizePendingRowPolicyPublication = commit.maybeFinalizePendingRowPolicyPublication;
        pub const mirrorReplicationEncodedBatchMutationCommit = commit.mirrorReplicationEncodedBatchMutationCommit;
        pub const mirrorReplicationReplayPayloadCommit = commit.mirrorReplicationReplayPayloadCommit;
        pub const noPendingEnrichmentReplayThrough = commit.noPendingEnrichmentReplayThrough;
        pub const notifyQueryVisibilityEvent = commit.notifyQueryVisibilityEvent;
        pub const notifyQueryVisibilityTargetAdvancedScoped = commit.notifyQueryVisibilityTargetAdvancedScoped;
        pub const notifyResolverReplayRuntimes = commit.notifyResolverReplayRuntimes;
        pub const notifyResolverReplayRuntimesForced = commit.notifyResolverReplayRuntimesForced;
        pub const pendingRowPolicyReceipt = commit.pendingRowPolicyReceipt;
        pub const preflightReplicationBatchSyncCommit = commit.preflightReplicationBatchSyncCommit;
        pub const prepareMergeArtifactEffects = preparation.prepareMergeArtifactEffects;
        pub const projectedBatchLsmAdmissionBytes = preparation.projectedBatchLsmAdmissionBytes;
        pub const rememberBulkIngestAllNewIdentityUpserts = commit.rememberBulkIngestAllNewIdentityUpserts;
        pub const replicationMutationBarrier = commit.replicationMutationBarrier;
        pub const resetCoalescedEntryToDelete = preparation.resetCoalescedEntryToDelete;
        pub const resolveTransactionIntentsInternal = commit.resolveTransactionIntentsInternal;
        pub const resolveTransactionIntentsPrepared = commit.resolveTransactionIntentsPrepared;
        pub const restoreStagingStatus = commit.restoreStagingStatus;
        pub const setCoalescedEntryToBorrowedWrite = preparation.setCoalescedEntryToBorrowedWrite;
        pub const setCoalescedEntryToOwnedWrite = preparation.setCoalescedEntryToOwnedWrite;
        pub const shouldApplySplitReplicationLocked = commit.shouldApplySplitReplicationLocked;
        pub const splitMarkerMatches = commit.splitMarkerMatches;
        pub const storedDocumentValuesEqual = preparation.storedDocumentValuesEqual;
        pub const thinReplayInputsHaveDerivedWork = commit.thinReplayInputsHaveDerivedWork;
        pub const unchangedDerivedReplayTargetsServiceable = commit.unchangedDerivedReplayTargetsServiceable;
        pub const validateGeneratedWriteReadSnapshot = preparation.validateGeneratedWriteReadSnapshot;
        pub const validateLiveReplicationIntegrityEffects = commit.validateLiveReplicationIntegrityEffects;
        pub const validateMergeCleanupPageLocked = commit.validateMergeCleanupPageLocked;
        pub const validatePreparedSchemaViewLocked = preparation.validatePreparedSchemaViewLocked;
        pub const validateResolvedKeyOwnership = commit.validateResolvedKeyOwnership;
        pub const validateRestoreStagingReplicationEffects = commit.validateRestoreStagingReplicationEffects;
        pub const validateTransformReadSnapshot = preparation.validateTransformReadSnapshot;
        pub const waitForResolvedTransactionSync = commit.waitForResolvedTransactionSync;
        pub const accountDenseCoverage = materialization.accountDenseCoverage;
        pub const accountSparseCoverage = materialization.accountSparseCoverage;
        pub const acquireSnapshotReplayAsyncContext = materialization.acquireSnapshotReplayAsyncContext;
        pub const activeSplitShadow = commit.activeSplitShadow;
        pub const addHbcWriteProfileDelta = materialization.addHbcWriteProfileDelta;
        pub const addPrecomputeAssetProducerBytes = materialization.addPrecomputeAssetProducerBytes;
        pub const appendArtifactRepairSummaryDirtyForStore = materialization.appendArtifactRepairSummaryDirtyForStore;
        pub const appendArtifactRepairSummaryRebuildInvalidationForStore = materialization.appendArtifactRepairSummaryRebuildInvalidationForStore;
        pub const appendArtifactSourceRevisionWritesFromReplay = materialization.appendArtifactSourceRevisionWritesFromReplay;
        pub const appendAssetArtifactSourceIndexDelete = materialization.appendAssetArtifactSourceIndexDelete;
        pub const appendAssetArtifactSourceIndexMutations = materialization.appendAssetArtifactSourceIndexMutations;
        pub const appendAssetArtifactSourceIndexWrite = materialization.appendAssetArtifactSourceIndexWrite;
        pub const appendChunkArtifactWrites = materialization.appendChunkArtifactWrites;
        pub const appendChunkToPendingDenseChunkEmbedding = materialization.appendChunkToPendingDenseChunkEmbedding;
        pub const appendChunkToPendingSparseChunkEmbedding = materialization.appendChunkToPendingSparseChunkEmbedding;
        pub const appendDerivedDenseEmbeddingForConsumers = materialization.appendDerivedDenseEmbeddingForConsumers;
        pub const appendDerivedSparseEmbeddingForConsumers = materialization.appendDerivedSparseEmbeddingForConsumers;
        pub const appendDerivedTargetRefAlloc = materialization.appendDerivedTargetRefAlloc;
        pub const appendDirectGraphTtlDueWrite = materialization.appendDirectGraphTtlDueWrite;
        pub const appendDirectVectorArtifactOutcomes = materialization.appendDirectVectorArtifactOutcomes;
        pub const appendDocumentExtractionDeleteKeys = materialization.appendDocumentExtractionDeleteKeys;
        pub const appendDocumentExtractionFailureManifest = materialization.appendDocumentExtractionFailureManifest;
        pub const appendDocumentExtractionKeyRanges = materialization.appendDocumentExtractionKeyRanges;
        pub const appendDocumentExtractionMergeOperation = materialization.appendDocumentExtractionMergeOperation;
        pub const appendDocumentExtractionNavigationBackfill = materialization.appendDocumentExtractionNavigationBackfill;
        pub const appendDocumentExtractionRangeDescriptors = materialization.appendDocumentExtractionRangeDescriptors;
        pub const appendDocumentExtractionRangePolicy = materialization.appendDocumentExtractionRangePolicy;
        pub const appendDocumentExtractionUnitMergeOperation = materialization.appendDocumentExtractionUnitMergeOperation;
        pub const appendDocumentUnitChunkWrites = materialization.appendDocumentUnitChunkWrites;
        pub const appendDocumentUnitStoredChunkFullTextDocuments = materialization.appendDocumentUnitStoredChunkFullTextDocuments;
        pub const appendDocumentUnitStoredFullTextDocuments = materialization.appendDocumentUnitStoredFullTextDocuments;
        pub const appendEmbeddingArtifactWrite = materialization.appendEmbeddingArtifactWrite;
        pub const appendEnrichmentTerminalFailureMarkerDeletePageForIssue = materialization.appendEnrichmentTerminalFailureMarkerDeletePageForIssue;
        pub const appendEnrichmentTerminalFailureMarkerDeletesForIssue = materialization.appendEnrichmentTerminalFailureMarkerDeletesForIssue;
        pub const appendFullTextDeleteDocument = materialization.appendFullTextDeleteDocument;
        pub const appendGeneratedEnrichmentRef = materialization.appendGeneratedEnrichmentRef;
        pub const appendGraphAssetStateSegmentDeleteKeys = materialization.appendGraphAssetStateSegmentDeleteKeys;
        pub const appendGraphContenderChange = materialization.appendGraphContenderChange;
        pub const appendGraphEdgeArtifactWrite = materialization.appendGraphEdgeArtifactWrite;
        pub const appendGraphEndpointRetirements = materialization.appendGraphEndpointRetirements;
        pub const appendGraphLifecycleGeneration = materialization.appendGraphLifecycleGeneration;
        pub const appendImportedGraphContenderMutations = materialization.appendImportedGraphContenderMutations;
        pub const appendInlineFullTextDocument = materialization.appendInlineFullTextDocument;
        pub const appendJsonFieldBool = materialization.appendJsonFieldBool;
        pub const appendJsonFieldName = materialization.appendJsonFieldName;
        pub const appendJsonFieldString = materialization.appendJsonFieldString;
        pub const appendJsonFieldU64 = materialization.appendJsonFieldU64;
        pub const appendJsonFieldUsize = materialization.appendJsonFieldUsize;
        pub const appendJsonUnsigned = materialization.appendJsonUnsigned;
        pub const appendKeysForPrefixDeleteInStore = materialization.appendKeysForPrefixDeleteInStore;
        pub const appendManagedTargetIdentity = materialization.appendManagedTargetIdentity;
        pub const appendMaterializedChunkSourceToBatch = materialization.appendMaterializedChunkSourceToBatch;
        pub const appendMentionEvidenceArtifactsFromResolution = materialization.appendMentionEvidenceArtifactsFromResolution;
        pub const appendMixedDirectGraphContenderMutations = materialization.appendMixedDirectGraphContenderMutations;
        pub const appendNeighborContextHintsToRecord = commit.appendNeighborContextHintsToRecord;
        pub const appendOwnedConstBytes = materialization.appendOwnedConstBytes;
        pub const appendOwnedManagedIndexName = materialization.appendOwnedManagedIndexName;
        pub const appendPendingDocumentUnitChunkSource = materialization.appendPendingDocumentUnitChunkSource;
        pub const appendPrecomputeAssetProducerBatchItem = materialization.appendPrecomputeAssetProducerBatchItem;
        pub const appendPrecomputedArtifactCoverageOutcomes = materialization.appendPrecomputedArtifactCoverageOutcomes;
        pub const appendPrecomputedCoverageCandidate = materialization.appendPrecomputedCoverageCandidate;
        pub const appendPrecomputedCoverageOutcomeMutations = materialization.appendPrecomputedCoverageOutcomeMutations;
        pub const appendPrecomputedEmbeddingCoverageOutcomes = materialization.appendPrecomputedEmbeddingCoverageOutcomes;
        pub const appendPreparedGraphEdgeArtifactWrite = materialization.appendPreparedGraphEdgeArtifactWrite;
        pub const appendRelationItemsFromPath = materialization.appendRelationItemsFromPath;
        pub const appendRelationValueItems = materialization.appendRelationValueItems;
        pub const appendReplicationBatchMutationCommitLockedContext = commit.appendReplicationBatchMutationCommitLockedContext;
        pub const appendReplicationEncodedBatchMutationCommitLockedContext = commit.appendReplicationEncodedBatchMutationCommitLockedContext;
        pub const appendReplicationEncodedBatchMutationCommitLockedContextStrict = commit.appendReplicationEncodedBatchMutationCommitLockedContextStrict;
        pub const appendReplicationReplayPayloadCommitLockedContext = commit.appendReplicationReplayPayloadCommitLockedContext;
        pub const appendRetiredDirectGraphTtlDueDeletes = materialization.appendRetiredDirectGraphTtlDueDeletes;
        pub const appendSparseEmbeddingArtifactWrite = materialization.appendSparseEmbeddingArtifactWrite;
        pub const appendStaleChunkArtifactDeleteKeys = materialization.appendStaleChunkArtifactDeleteKeys;
        pub const appendStalePrecomputedChunkEmbeddingDeletes = materialization.appendStalePrecomputedChunkEmbeddingDeletes;
        pub const appendStoredFullTextDocument = materialization.appendStoredFullTextDocument;
        pub const appendUniqueBorrowedKeyWithSet = materialization.appendUniqueBorrowedKeyWithSet;
        pub const appendUniqueOwnedConstKeyIndexed = materialization.appendUniqueOwnedConstKeyIndexed;
        pub const appendUniqueOwnedKeyIndexed = materialization.appendUniqueOwnedKeyIndexed;
        pub const appendUniqueReplayRecordHint = materialization.appendUniqueReplayRecordHint;
        pub const appendUniqueReplayRecordKeyWithSet = materialization.appendUniqueReplayRecordKeyWithSet;
        pub const appliedSequenceUpdatesWithConfigHashes = commit.appliedSequenceUpdatesWithConfigHashes;
        pub const applyArtifactRepairability = materialization.applyArtifactRepairability;
        pub const applyCommittedBatchToShadow = commit.applyCommittedBatchToShadow;
        pub const applyCommittedBatchToShadowOrdered = commit.applyCommittedBatchToShadowOrdered;
        pub const applyDerivedBacklogPressureContext = materialization.applyDerivedBacklogPressureContext;
        pub const applyDerivedBatchContextProfiled = materialization.applyDerivedBatchContextProfiled;
        pub const applyDerivedBatchProfiled = materialization.applyDerivedBatchProfiled;
        pub const applyDerivedBatchTargetsContextProfiled = materialization.applyDerivedBatchTargetsContextProfiled;
        pub const applyDerivedBatchTargetsProfiled = materialization.applyDerivedBatchTargetsProfiled;
        pub const applyDerivedBatchToIndexContext = materialization.applyDerivedBatchToIndexContext;
        pub const applyDerivedBatchToIndexContextProfiled = materialization.applyDerivedBatchToIndexContextProfiled;
        pub const applyGraphArtifactMutationPages = materialization.applyGraphArtifactMutationPages;
        pub const applyGraphDocClearsForIndex = materialization.applyGraphDocClearsForIndex;
        pub const applyPrecomputeAssetProducerOutput = materialization.applyPrecomputeAssetProducerOutput;
        pub const artifactKindFromInternalLabel = materialization.artifactKindFromInternalLabel;
        pub const artifactRepairIssueIdAlloc = materialization.artifactRepairIssueIdAlloc;
        pub const artifactRepairIssueIdHashOptionalU64 = materialization.artifactRepairIssueIdHashOptionalU64;
        pub const artifactRepairIssueIdHashString = materialization.artifactRepairIssueIdHashString;
        pub const artifactRepairIssueKindKeyForIssueAlloc = materialization.artifactRepairIssueKindKeyForIssueAlloc;
        pub const artifactRepairKindHasAutomatedReprocessor = materialization.artifactRepairKindHasAutomatedReprocessor;
        pub const artifactRepairReasonHasAutomatedReprocessor = materialization.artifactRepairReasonHasAutomatedReprocessor;
        pub const artifactRepairUnsupportedReason = materialization.artifactRepairUnsupportedReason;
        pub const artifactRepairUnsupportedReasonForIssue = materialization.artifactRepairUnsupportedReasonForIssue;
        pub const artifactSourcesContainGeneratedEnrichment = materialization.artifactSourcesContainGeneratedEnrichment;
        pub const assetStateKeyAlloc = materialization.assetStateKeyAlloc;
        pub const assetStateValueAlloc = materialization.assetStateValueAlloc;
        pub const asyncIndexProfileEnabled = commit.asyncIndexProfileEnabled;
        pub const atomicMaxU64 = commit.atomicMaxU64;
        pub const attachPreparedUpsertDocumentProjections = preparation.attachPreparedUpsertDocumentProjections;
        pub const augmentExtractedWriteWithGraphFieldEdges = preparation.augmentExtractedWriteWithGraphFieldEdges;
        pub const augmentExtractedWriteWithGraphFieldEdgesFromSnapshotParsed = preparation.augmentExtractedWriteWithGraphFieldEdgesFromSnapshotParsed;
        pub const augmentExtractedWriteWithGraphFieldEdgesParsed = preparation.augmentExtractedWriteWithGraphFieldEdgesParsed;
        pub const batchAdvancesManagedIndexApplyState = materialization.batchAdvancesManagedIndexApplyState;
        pub const batchAffectsManagedIndex = materialization.batchAffectsManagedIndex;
        pub const batchHasEmbeddingArtifactForManagedIndex = materialization.batchHasEmbeddingArtifactForManagedIndex;
        pub const benchMetricsEnabled = commit.benchMetricsEnabled;
        pub const boundaryFailureErrorName = commit.boundaryFailureErrorName;
        pub const buildChunkArtifactPayloadAlloc = materialization.buildChunkArtifactPayloadAlloc;
        pub const buildDerivedBatch = materialization.buildDerivedBatch;
        pub const buildDocumentExtractionCatalogView = materialization.buildDocumentExtractionCatalogView;
        pub const buildDocumentExtractionEmbeddingViews = materialization.buildDocumentExtractionEmbeddingViews;
        pub const buildDocumentUnitChunkPayloadAlloc = materialization.buildDocumentUnitChunkPayloadAlloc;
        pub const buildOverwrittenDocKeys = preparation.buildOverwrittenDocKeys;
        pub const bytesToHexAlloc = materialization.bytesToHexAlloc;
        pub const cachedEnvUsize = commit.cachedEnvUsize;
        pub const cachedOptionalEnvUsize = commit.cachedOptionalEnvUsize;
        pub const chunkArtifactKeysForChunksAlloc = materialization.chunkArtifactKeysForChunksAlloc;
        pub const chunkCacheTupleKeyAlloc = materialization.chunkCacheTupleKeyAlloc;
        pub const chunkEmbeddingSourcesForRequest = materialization.chunkEmbeddingSourcesForRequest;
        pub const clampReplayTruncationForRepairPins = commit.clampReplayTruncationForRepairPins;
        pub const clearGeneratedDenseMemoJobs = preparation.clearGeneratedDenseMemoJobs;
        pub const clearGeneratedSparseMemoJobs = preparation.clearGeneratedSparseMemoJobs;
        pub const clearGraphArtifactStatePaged = materialization.clearGraphArtifactStatePaged;
        pub const clearPrecomputeAssetProducerBatchItems = materialization.clearPrecomputeAssetProducerBatchItems;
        pub const clearPublishedEmbeddingArtifactRepairIssueContext = materialization.clearPublishedEmbeddingArtifactRepairIssueContext;
        pub const clearPublishedEmbeddingArtifactRepairIssuesContext = materialization.clearPublishedEmbeddingArtifactRepairIssuesContext;
        pub const collectDeleteKeysForPrefix = materialization.collectDeleteKeysForPrefix;
        pub const collectDocumentExtractionDesiredKeys = materialization.collectDocumentExtractionDesiredKeys;
        pub const collectEnrichmentArtifactDeleteKeysForDocContext = materialization.collectEnrichmentArtifactDeleteKeysForDocContext;
        pub const collectEnrichmentArtifactDeletesForBatch = materialization.collectEnrichmentArtifactDeletesForBatch;
        pub const collectGraphArtifactsForDocIndex = materialization.collectGraphArtifactsForDocIndex;
        pub const collectGraphDeletes = materialization.collectGraphDeletes;
        pub const collectGraphMutationsForArtifacts = materialization.collectGraphMutationsForArtifacts;
        pub const collectGraphWrites = materialization.collectGraphWrites;
        pub const collectManagedIndexCandidates = materialization.collectManagedIndexCandidates;
        pub const collectManagedSyncTargetsForRecord = materialization.collectManagedSyncTargetsForRecord;
        pub const collectManagedSyncTargetsForRecordWithBatch = materialization.collectManagedSyncTargetsForRecordWithBatch;
        pub const collectManagedSyncTargetsWithGeneratedSource = materialization.collectManagedSyncTargetsWithGeneratedSource;
        pub const collectPendingDocumentUnitDenseChunkEmbeddings = materialization.collectPendingDocumentUnitDenseChunkEmbeddings;
        pub const collectPendingDocumentUnitSparseChunkEmbeddings = materialization.collectPendingDocumentUnitSparseChunkEmbeddings;
        pub const collectTextReplayDeleteKeys = materialization.collectTextReplayDeleteKeys;
        pub const collectVectorReplayDeleteKeys = materialization.collectVectorReplayDeleteKeys;
        pub const commitGraphContenderReconcilePage = materialization.commitGraphContenderReconcilePage;
        pub const computeAssetRequestDerived = materialization.computeAssetRequestDerived;
        pub const computeChunkRequestDerived = materialization.computeChunkRequestDerived;
        pub const computeDenseMaterializedChunkRequestImpl = materialization.computeDenseMaterializedChunkRequestImpl;
        pub const computeDenseRequestDerived = materialization.computeDenseRequestDerived;
        pub const computeDenseRequestImpl = materialization.computeDenseRequestImpl;
        pub const computeDocumentExtractionAssetRequestDerived = materialization.computeDocumentExtractionAssetRequestDerived;
        pub const computeSparseMaterializedChunkRequest = materialization.computeSparseMaterializedChunkRequest;
        pub const computeSparseRequestDerived = materialization.computeSparseRequestDerived;
        pub const concatKVPairSlices = materialization.concatKVPairSlices;
        pub const considerGraphEdgeWinner = materialization.considerGraphEdgeWinner;
        pub const containsDeleteKey = materialization.containsDeleteKey;
        pub const containsName = materialization.containsName;
        pub const containsOwnedKey = materialization.containsOwnedKey;
        pub const containsStoreWriteKey = materialization.containsStoreWriteKey;
        pub const contentPartsJsonAlloc = materialization.contentPartsJsonAlloc;
        pub const countKeysNotIn = materialization.countKeysNotIn;
        pub const countUnitDescriptorsByFingerprintMatch = materialization.countUnitDescriptorsByFingerprintMatch;
        pub const currentTimeNs = commit.currentTimeNs;
        pub const decisionName = materialization.decisionName;
        pub const decodeArtifactRefIfKnownAlloc = materialization.decodeArtifactRefIfKnownAlloc;
        pub const decodeArtifactRefViewForGraphApplicability = materialization.decodeArtifactRefViewForGraphApplicability;
        pub const decodeArtifactRepairIssueValueAlloc = materialization.decodeArtifactRepairIssueValueAlloc;
        pub const deferExternalBulkExecutorNotification = commit.deferExternalBulkExecutorNotification;
        pub const deleteDerivedCoverageForDocKeys = materialization.deleteDerivedCoverageForDocKeys;
        pub const denseApplyUsesLocalStreamingSession = materialization.denseApplyUsesLocalStreamingSession;
        pub const denseCatchUpBulkRebuildHbcLeafMinMembers = materialization.denseCatchUpBulkRebuildHbcLeafMinMembers;
        pub const denseCatchUpDeferredHbcLeafSplitMembersPerPublish = materialization.denseCatchUpDeferredHbcLeafSplitMembersPerPublish;
        pub const denseCatchUpDeferredHbcLeafSplitsPerPublish = materialization.denseCatchUpDeferredHbcLeafSplitsPerPublish;
        pub const denseCatchUpDeferredL0Limit = materialization.denseCatchUpDeferredL0Limit;
        pub const denseCatchUpFinishOptions = materialization.denseCatchUpFinishOptions;
        pub const denseEmbeddingWriteSourceDocumentExists = materialization.denseEmbeddingWriteSourceDocumentExists;
        pub const denseIndexIsArtifactBacked = materialization.denseIndexIsArtifactBacked;
        pub const derivedCoverageOutcomeCounterValueForStore = materialization.derivedCoverageOutcomeCounterValueForStore;
        pub const directGraphNeighborContextHintsAlloc = materialization.directGraphNeighborContextHintsAlloc;
        pub const documentExtractionFailureManifestPayloadAlloc = materialization.documentExtractionFailureManifestPayloadAlloc;
        pub const documentExtractionFingerprintAlloc = materialization.documentExtractionFingerprintAlloc;
        pub const documentExtractionKeyIndex = materialization.documentExtractionKeyIndex;
        pub const documentExtractionManifestGeneration = materialization.documentExtractionManifestGeneration;
        pub const documentExtractionManifestHasLastError = materialization.documentExtractionManifestHasLastError;
        pub const documentExtractionManifestPayloadAlloc = materialization.documentExtractionManifestPayloadAlloc;
        pub const documentExtractionRangeEnd = materialization.documentExtractionRangeEnd;
        pub const documentExtractionRangeIdAlloc = materialization.documentExtractionRangeIdAlloc;
        pub const documentExtractionRangeRoute = materialization.documentExtractionRangeRoute;
        pub const documentExtractionSplitBoundary = materialization.documentExtractionSplitBoundary;
        pub const documentExtractionStateByteSliceAlloc = materialization.documentExtractionStateByteSliceAlloc;
        pub const documentExtractionStateChunkKeysAlloc = materialization.documentExtractionStateChunkKeysAlloc;
        pub const documentExtractionStateFingerprintMatches = materialization.documentExtractionStateFingerprintMatches;
        pub const documentExtractionStateHasChunkUnitFingerprints = materialization.documentExtractionStateHasChunkUnitFingerprints;
        pub const documentExtractionStateKeysAlloc = materialization.documentExtractionStateKeysAlloc;
        pub const documentExtractionStateNavigationBlockCount = materialization.documentExtractionStateNavigationBlockCount;
        pub const documentExtractionStateUnitDescriptorFallbackAlloc = materialization.documentExtractionStateUnitDescriptorFallbackAlloc;
        pub const documentExtractionStateUnitDescriptorsAlloc = materialization.documentExtractionStateUnitDescriptorsAlloc;
        pub const documentExtractionStateUnitKeysAlloc = materialization.documentExtractionStateUnitKeysAlloc;
        pub const documentExtractionStateValueAlloc = materialization.documentExtractionStateValueAlloc;
        pub const documentExtractionStoredUnitFingerprintAlloc = materialization.documentExtractionStoredUnitFingerprintAlloc;
        pub const documentExtractionUnitDescriptorsFromKeysAlloc = materialization.documentExtractionUnitDescriptorsFromKeysAlloc;
        pub const documentExtractionUnitFingerprintAlloc = materialization.documentExtractionUnitFingerprintAlloc;
        pub const documentExtractionUnitRangeCount = materialization.documentExtractionUnitRangeCount;
        pub const documentExtractionUnitRangeIndex = materialization.documentExtractionUnitRangeIndex;
        pub const documentRangeLowerAlloc = materialization.documentRangeLowerAlloc;
        pub const documentUnitCanSkipLocalWrites = materialization.documentUnitCanSkipLocalWrites;
        pub const documentUnitChunkEmbeddingArtifactsPresent = materialization.documentUnitChunkEmbeddingArtifactsPresent;
        pub const documentUnitConfidence = materialization.documentUnitConfidence;
        pub const documentUnitPayloadAlloc = materialization.documentUnitPayloadAlloc;
        pub const dupeConsumerIndexNames = materialization.dupeConsumerIndexNames;
        pub const elapsedSince = commit.elapsedSince;
        pub const embeddingArtifactKeyForBaseAlloc = materialization.embeddingArtifactKeyForBaseAlloc;
        pub const encodeArtifactRepairCompletionState = materialization.encodeArtifactRepairCompletionState;
        pub const encodeArtifactRepairIssueValueAlloc = materialization.encodeArtifactRepairIssueValueAlloc;
        pub const encodeGraphAssetStateKeysAlloc = materialization.encodeGraphAssetStateKeysAlloc;
        pub const encodeGraphEdgeArtifactWithTtlAlloc = materialization.encodeGraphEdgeArtifactWithTtlAlloc;
        pub const encodeGraphSourceEdgeArtifactWithTtlAlloc = materialization.encodeGraphSourceEdgeArtifactWithTtlAlloc;
        pub const encodeStoreLookupKeyAlloc = commit.encodeStoreLookupKeyAlloc;
        pub const encodeStoreLookupKeyWithPinnedSchemaAlloc = commit.encodeStoreLookupKeyWithPinnedSchemaAlloc;
        pub const encodeThinReplayRecordPayload = commit.encodeThinReplayRecordPayload;
        pub const encodeTimestampValue = preparation.encodeTimestampValue;
        pub const enforcePortableRuntimeGateOptional = commit.enforcePortableRuntimeGateOptional;
        pub const envBoolEnabled = commit.envBoolEnabled;
        pub const extractAssetSourceValue = materialization.extractAssetSourceValue;
        pub const extractDocumentDownloadedAlloc = materialization.extractDocumentDownloadedAlloc;
        pub const extractStringField = preparation.extractStringField;
        pub const extractionConfidenceForLocalId = materialization.extractionConfidenceForLocalId;
        pub const extractionEntityForLocalId = materialization.extractionEntityForLocalId;
        pub const filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay = materialization.filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay;
        pub const filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay = materialization.filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay;
        pub const filterChangedGraphMaterializationBatch = materialization.filterChangedGraphMaterializationBatch;
        pub const findDocumentArtifactChildRange = materialization.findDocumentArtifactChildRange;
        pub const findOrAppendMentionEdgeAggregate = materialization.findOrAppendMentionEdgeAggregate;
        pub const finishManagedSyncTargets = materialization.finishManagedSyncTargets;
        pub const flushGeneratedDenseChunkBatch = preparation.flushGeneratedDenseChunkBatch;
        pub const flushGeneratedDenseChunkSourceBatch = preparation.flushGeneratedDenseChunkSourceBatch;
        pub const flushGeneratedDenseMemoJobs = preparation.flushGeneratedDenseMemoJobs;
        pub const flushGeneratedSparseChunkBatch = preparation.flushGeneratedSparseChunkBatch;
        pub const flushGeneratedSparseChunkSourceBatch = preparation.flushGeneratedSparseChunkSourceBatch;
        pub const flushGeneratedSparseMemoJobs = preparation.flushGeneratedSparseMemoJobs;
        pub const flushPendingDenseChunkEmbedding = materialization.flushPendingDenseChunkEmbedding;
        pub const flushPendingSparseChunkEmbedding = materialization.flushPendingSparseChunkEmbedding;
        pub const flushPrecomputeAssetProducerBatch = materialization.flushPrecomputeAssetProducerBatch;
        pub const flushPrecomputeAssetProducerBatchSequential = materialization.flushPrecomputeAssetProducerBatchSequential;
        pub const freeChunkArtifactKeys = materialization.freeChunkArtifactKeys;
        pub const freeDocumentExtractionUnitDescriptors = preparation.freeDocumentExtractionUnitDescriptors;
        pub const freeGraphWrites = materialization.freeGraphWrites;
        pub const freeOwnedConstKeySlice = preparation.freeOwnedConstKeySlice;
        pub const freeOwnedKeySlice = preparation.freeOwnedKeySlice;
        pub const freePrecomputeAssetProducerBatchItem = materialization.freePrecomputeAssetProducerBatchItem;
        pub const fullTextTargetRefsAlloc = materialization.fullTextTargetRefsAlloc;
        pub const generatedConsumerSetsEqual = materialization.generatedConsumerSetsEqual;
        pub const generatedEmbedBatchBytes = preparation.generatedEmbedBatchBytes;
        pub const generatedEmbedBatchItems = preparation.generatedEmbedBatchItems;
        pub const getOrCreateChunks = materialization.getOrCreateChunks;
        pub const getenv = commit.getenv;
        pub const graphArtifactContentType = materialization.graphArtifactContentType;
        pub const graphArtifactRefUsesDocumentWideFallback = materialization.graphArtifactRefUsesDocumentWideFallback;
        pub const graphArtifactSourceConsumesArtifactKey = materialization.graphArtifactSourceConsumesArtifactKey;
        pub const graphArtifactSourceConsumesRef = materialization.graphArtifactSourceConsumesRef;
        pub const graphArtifactSourceConsumesRefView = materialization.graphArtifactSourceConsumesRefView;
        pub const graphArtifactStateNameAlloc = materialization.graphArtifactStateNameAlloc;
        pub const graphAssetSourceConsumesAssetRef = materialization.graphAssetSourceConsumesAssetRef;
        pub const graphAssetSourceConsumesAssetRefView = materialization.graphAssetSourceConsumesAssetRefView;
        pub const graphAssetStateKeyAlloc = materialization.graphAssetStateKeyAlloc;
        pub const graphCleanupReplayWritesAlloc = materialization.graphCleanupReplayWritesAlloc;
        pub const graphContenderStateChanged = materialization.graphContenderStateChanged;
        pub const graphEndpointCleanupDeletesAlloc = materialization.graphEndpointCleanupDeletesAlloc;
        pub const graphEndpointResolutionsJsonAlloc = materialization.graphEndpointResolutionsJsonAlloc;
        pub const graphStateSourcePriorityAlloc = materialization.graphStateSourcePriorityAlloc;
        pub const graphWritesFromArtifactValueAlloc = materialization.graphWritesFromArtifactValueAlloc;
        pub const hexBytesAlloc = materialization.hexBytesAlloc;
        pub const hierarchyNavigationArtifactDigestAlloc = materialization.hierarchyNavigationArtifactDigestAlloc;
        pub const hierarchyNavigationBlockCount = materialization.hierarchyNavigationBlockCount;
        pub const hierarchyNavigationBlockValueAlloc = materialization.hierarchyNavigationBlockValueAlloc;
        pub const hierarchyNavigationDigestIsValid = materialization.hierarchyNavigationDigestIsValid;
        pub const hierarchyNavigationSummaryValueAlloc = materialization.hierarchyNavigationSummaryValueAlloc;
        pub const indexNameInSlice = materialization.indexNameInSlice;
        pub const indexPendingArtifactWrites = materialization.indexPendingArtifactWrites;
        pub const injectGraphEndpointResolutions = materialization.injectGraphEndpointResolutions;
        pub const isMergeArtifactKey = materialization.isMergeArtifactKey;
        pub const isMetadataKey = commit.isMetadataKey;
        pub const isRetryableAssetProducerError = materialization.isRetryableAssetProducerError;
        pub const isSplitMetadataKey = commit.isSplitMetadataKey;
        pub const journalRecordHasHint = commit.journalRecordHasHint;
        pub const keyAfterAlloc = materialization.keyAfterAlloc;
        pub const loadArtifactRepairCompletionStateFromStore = materialization.loadArtifactRepairCompletionStateFromStore;
        pub const loadArtifactRepairIssueFromStoreByKey = materialization.loadArtifactRepairIssueFromStoreByKey;
        pub const loadDerivedCoverageOutcomeCounterFromStore = materialization.loadDerivedCoverageOutcomeCounterFromStore;
        pub const loadDocumentExtractionPreviousState = materialization.loadDocumentExtractionPreviousState;
        pub const loadDocumentExtractionPreviousStateFromJson = materialization.loadDocumentExtractionPreviousStateFromJson;
        pub const loadGraphAssetStateKeysAlloc = materialization.loadGraphAssetStateKeysAlloc;
        pub const loadManagedAppliedSequenceContext = commit.loadManagedAppliedSequenceContext;
        pub const loadSourceExtractionForResolution = materialization.loadSourceExtractionForResolution;
        pub const lockApply = commit.lockApply;
        pub const lockAtomicWithBackoff = commit.lockAtomicWithBackoff;
        pub const lockAtomicWithBackoffProfiled = commit.lockAtomicWithBackoffProfiled;
        pub const logSparseWriteProfileDelta = materialization.logSparseWriteProfileDelta;
        pub const makeChunkCacheKey = materialization.makeChunkCacheKey;
        pub const makeTimestampKey = preparation.makeTimestampKey;
        pub const managedIndexBatchApplicability = materialization.managedIndexBatchApplicability;
        pub const managedIndexBatchApplicabilityWithEmbeddingNames = materialization.managedIndexBatchApplicabilityWithEmbeddingNames;
        pub const managedIndexBatchServingSetEffect = materialization.managedIndexBatchServingSetEffect;
        pub const managedIndexConsumesEmbeddingName = materialization.managedIndexConsumesEmbeddingName;
        pub const managedIndexDeleteKeyAffectsProjection = materialization.managedIndexDeleteKeyAffectsProjection;
        pub const managedIndexDeleteKeysAffectProjection = materialization.managedIndexDeleteKeysAffectProjection;
        pub const managedIndexRecordApplicability = materialization.managedIndexRecordApplicability;
        pub const managedIndexRecordServingSetEffect = materialization.managedIndexRecordServingSetEffect;
        pub const materializeGraphArtifactValuePaged = materialization.materializeGraphArtifactValuePaged;
        pub const materializeGraphSourceArtifactsForIndex = materialization.materializeGraphSourceArtifactsForIndex;
        pub const materializeMentionEdgesForResolutionKey = materialization.materializeMentionEdgesForResolutionKey;
        pub const mentionArtifactStateNameAlloc = materialization.mentionArtifactStateNameAlloc;
        pub const mentionEdgeMetadataJsonAlloc = materialization.mentionEdgeMetadataJsonAlloc;
        pub const mentionEdgeWritesFromResolutionAlloc = materialization.mentionEdgeWritesFromResolutionAlloc;
        pub const mentionEvidencePayloadAlloc = materialization.mentionEvidencePayloadAlloc;
        pub const mentionGraphStateNameAlloc = materialization.mentionGraphStateNameAlloc;
        pub const mergeGenerationFailureAttempts = materialization.mergeGenerationFailureAttempts;
        pub const mirrorReplicationEncodedBatchMutationCommitContext = commit.mirrorReplicationEncodedBatchMutationCommitContext;
        pub const mirrorReplicationReplayPayloadCommitContext = commit.mirrorReplicationReplayPayloadCommitContext;
        pub const monotonicTimeNs = commit.monotonicTimeNs;
        pub const nextArtifactRepairIssueTimestamp = materialization.nextArtifactRepairIssueTimestamp;
        pub const notifyExecutorForSyncLevel = commit.notifyExecutorForSyncLevel;
        pub const notifyExecutorForSyncLevelWithDenseBulkDeferral = commit.notifyExecutorForSyncLevelWithDenseBulkDeferral;
        pub const nsToMs = commit.nsToMs;
        pub const openModeRequiresReadOnlyBackends = commit.openModeRequiresReadOnlyBackends;
        pub const orderedCoverageActive = commit.orderedCoverageActive;
        pub const overwriteProbeLessThan = preparation.overwriteProbeLessThan;
        pub const parsePatternRfc3339ToNs = preparation.parsePatternRfc3339ToNs;
        pub const pendingGeneratedCoverageDocKeysForIndexAlloc = materialization.pendingGeneratedCoverageDocKeysForIndexAlloc;
        pub const planGeneratedEnrichmentsForRows = preparation.planGeneratedEnrichmentsForRows;
        pub const precomputeAssetProducerBatchBytes = materialization.precomputeAssetProducerBatchBytes;
        pub const precomputeAssetProducerBatchItemBytes = materialization.precomputeAssetProducerBatchItemBytes;
        pub const precomputedCoverageOutcomePriority = materialization.precomputedCoverageOutcomePriority;
        pub const precomputedEmbeddingCoverageOutcome = materialization.precomputedEmbeddingCoverageOutcome;
        pub const preflightReplicationMirrorSyncCommitContext = commit.preflightReplicationMirrorSyncCommitContext;
        pub const prepareDirectChangeRecord = commit.prepareDirectChangeRecord;
        pub const prepareGeneratedEnrichments = preparation.prepareGeneratedEnrichments;
        pub const prepareGraphContenderReconcilePage = materialization.prepareGraphContenderReconcilePage;
        pub const preparePreservedEmbeddingSources = preparation.preparePreservedEmbeddingSources;
        pub const prewarmGeneratedDenseMemo = preparation.prewarmGeneratedDenseMemo;
        pub const prewarmGeneratedMemoFromArtifact = preparation.prewarmGeneratedMemoFromArtifact;
        pub const prewarmGeneratedSparseMemo = preparation.prewarmGeneratedSparseMemo;
        pub const profileDelta = commit.profileDelta;
        pub const readEnvUsize = commit.readEnvUsize;
        pub const reconcileGlobalGraphEdgeWinner = materialization.reconcileGlobalGraphEdgeWinner;
        pub const reconcileGraphEdgeContenders = materialization.reconcileGraphEdgeContenders;
        pub const reconcileGraphEdgeContendersWithLifetimePolicy = materialization.reconcileGraphEdgeContendersWithLifetimePolicy;
        pub const reconcileGraphEdgeContendersWithOverlay = materialization.reconcileGraphEdgeContendersWithOverlay;
        pub const reconcileSingleGraphStateContenders = materialization.reconcileSingleGraphStateContenders;
        pub const recordArtifactRepairIssueContext = materialization.recordArtifactRepairIssueContext;
        pub const recordArtifactRepairIssueContextDetailed = materialization.recordArtifactRepairIssueContextDetailed;
        pub const recordArtifactRepairIssueContextForIndexSource = materialization.recordArtifactRepairIssueContextForIndexSource;
        pub const recordArtifactRepairIssueForRefContext = materialization.recordArtifactRepairIssueForRefContext;
        pub const recordEmbeddingArtifactRepairIssueContext = materialization.recordEmbeddingArtifactRepairIssueContext;
        pub const recordGraphResolutionResourceLimitForRepair = materialization.recordGraphResolutionResourceLimitForRepair;
        pub const recordGraphResourceLimitForRepair = materialization.recordGraphResourceLimitForRepair;
        pub const recordProfileNs = commit.recordProfileNs;
        pub const recoverDurableReplicationOutboxContext = commit.recoverDurableReplicationOutboxContext;
        pub const relationalColumns = materialization.relationalColumns;
        pub const releaseReplicationMutationShared = commit.releaseReplicationMutationShared;
        pub const removePendingDeleteKey = materialization.removePendingDeleteKey;
        pub const renderSourceParts = materialization.renderSourceParts;
        pub const renderSourcePartsJson = materialization.renderSourcePartsJson;
        pub const renderSourceTemplateParts = materialization.renderSourceTemplateParts;
        pub const renderSourceTemplateText = materialization.renderSourceTemplateText;
        pub const repairKindFromArtifactKind = materialization.repairKindFromArtifactKind;
        pub const replayRecordHasTargetHint = commit.replayRecordHasTargetHint;
        pub const replaySourceDocumentExists = materialization.replaySourceDocumentExists;
        pub const replicationCommitContext = commit.replicationCommitContext;
        pub const replicationTransitionMutexFromContext = commit.replicationTransitionMutexFromContext;
        pub const requestArtifactName = materialization.requestArtifactName;
        pub const requestEmbeddingName = materialization.requestEmbeddingName;
        pub const requestHasChunking = materialization.requestHasChunking;
        pub const requestUsesChunkSource = materialization.requestUsesChunkSource;
        pub const requestUsesPinnedMaterializedChunkArtifact = materialization.requestUsesPinnedMaterializedChunkArtifact;
        pub const requireRelationalConsumerFields = preparation.requireRelationalConsumerFields;
        pub const reserveSplitShadowApplyTicket = commit.reserveSplitShadowApplyTicket;
        pub const resolutionDecisionCreatesCanonicalEdge = materialization.resolutionDecisionCreatesCanonicalEdge;
        pub const resolutionMentionArtifactKeyAlloc = materialization.resolutionMentionArtifactKeyAlloc;
        pub const resolutionMentionArtifactNameAlloc = materialization.resolutionMentionArtifactNameAlloc;
        pub const resolutionMentionStateKeysForGraphSourceAlloc = materialization.resolutionMentionStateKeysForGraphSourceAlloc;
        pub const resolutionOwningAssetArtifactKeyAlloc = materialization.resolutionOwningAssetArtifactKeyAlloc;
        pub const resolveWriteTimestampForSchemaValue = preparation.resolveWriteTimestampForSchemaValue;
        pub const resolveWriteTimestampFromValue = preparation.resolveWriteTimestampFromValue;
        pub const resolveWriteTimestampNs = preparation.resolveWriteTimestampNs;
        pub const resolverConfigForResolution = materialization.resolverConfigForResolution;
        pub const resolverConfigForResolutionArtifact = materialization.resolverConfigForResolutionArtifact;
        pub const resolverReplayRetentionRequired = materialization.resolverReplayRetentionRequired;
        pub const retainPreparedTextRoots = preparation.retainPreparedTextRoots;
        pub const rollbackGraphStateSegmentPage = materialization.rollbackGraphStateSegmentPage;
        pub const samePrecomputeAssetProducerBatchKey = materialization.samePrecomputeAssetProducerBatchKey;
        pub const saveAppliedSequencesBatchContext = commit.saveAppliedSequencesBatchContext;
        pub const saveArtifactRepairIssueToStoreWithSummary = materialization.saveArtifactRepairIssueToStoreWithSummary;
        pub const scanDerivedCoverageOutcomeFromStore = materialization.scanDerivedCoverageOutcomeFromStore;
        pub const scanDocumentExtractionPreviousStateFromStore = materialization.scanDocumentExtractionPreviousStateFromStore;
        pub const scanMaterializedChunkSourceStoreBatch = materialization.scanMaterializedChunkSourceStoreBatch;
        pub const setDerivedCoverageOutcomes = materialization.setDerivedCoverageOutcomes;
        pub const shouldAppendSplitDelta = commit.shouldAppendSplitDelta;
        pub const shouldDeferBacklogPressureForExternalDenseBulk = commit.shouldDeferBacklogPressureForExternalDenseBulk;
        pub const shouldPrecomputeGeneratedRequest = preparation.shouldPrecomputeGeneratedRequest;
        pub const shouldStoreChunkArtifacts = materialization.shouldStoreChunkArtifacts;
        pub const shouldWriteTimestamp = preparation.shouldWriteTimestamp;
        pub const sleepNs = commit.sleepNs;
        pub const sliceContainsDocKeyPrefix = materialization.sliceContainsDocKeyPrefix;
        pub const sliceContainsKey = materialization.sliceContainsKey;
        pub const sliceContainsKeyPrefix = materialization.sliceContainsKeyPrefix;
        pub const sliceContainsWriteKey = materialization.sliceContainsWriteKey;
        pub const sliceContainsWriteKeyPrefix = materialization.sliceContainsWriteKeyPrefix;
        pub const sourceArtifactKeyForResolutionAlloc = materialization.sourceArtifactKeyForResolutionAlloc;
        pub const sourceArtifactKeyFromResolutionScopeAlloc = materialization.sourceArtifactKeyFromResolutionScopeAlloc;
        pub const splitShadowRequiresMaterializedDerivedBatch = commit.splitShadowRequiresMaterializedDerivedBatch;
        pub const storeContainsKey = materialization.storeContainsKey;
        pub const storeDocumentValueForGraphSource = materialization.storeDocumentValueForGraphSource;
        pub const storeHasUserDataBounded = materialization.storeHasUserDataBounded;
        pub const storeKeyExists = materialization.storeKeyExists;
        pub const storeValueDiffers = materialization.storeValueDiffers;
        pub const storedOrPendingEmbeddingSourceHash = materialization.storedOrPendingEmbeddingSourceHash;
        pub const storedUnitIntegerValue = materialization.storedUnitIntegerValue;
        pub const storedUnitOptionalBbox = materialization.storedUnitOptionalBbox;
        pub const storedUnitOptionalBool = materialization.storedUnitOptionalBool;
        pub const storedUnitOptionalFloat = materialization.storedUnitOptionalFloat;
        pub const storedUnitOptionalInteger = materialization.storedUnitOptionalInteger;
        pub const storedUnitOptionalString = materialization.storedUnitOptionalString;
        pub const storedUnitRequiredBool = materialization.storedUnitRequiredBool;
        pub const storedUnitRequiredString = materialization.storedUnitRequiredString;
        pub const storedUnitTextRegionsAlloc = materialization.storedUnitTextRegionsAlloc;
        pub const strippedStoredDocumentValueAlloc = preparation.strippedStoredDocumentValueAlloc;
        pub const syncLevelParticipatesInDerivedBacklogPressure = commit.syncLevelParticipatesInDerivedBacklogPressure;
        pub const syncLevelRequiresDerivedVisibility = commit.syncLevelRequiresDerivedVisibility;
        pub const takeOwnedSlice = materialization.takeOwnedSlice;
        pub const truncateReplayJournalIfSafeContext = commit.truncateReplayJournalIfSafeContext;
        pub const truncateReplayLogs = commit.truncateReplayLogs;
        pub const ttlTimestampNsFromDocumentValue = preparation.ttlTimestampNsFromDocumentValue;
        pub const ttlTimestampNsFromJsonValue = preparation.ttlTimestampNsFromJsonValue;
        pub const ttlTimestampNsFromString = preparation.ttlTimestampNsFromString;
        pub const unitDescriptorFingerprintMatches = materialization.unitDescriptorFingerprintMatches;
        pub const unlockProfiledApply = commit.unlockProfiledApply;
        pub const upsertOwnedStoreWrite = materialization.upsertOwnedStoreWrite;
        pub const upsertOwnedStoreWriteDupeKey = materialization.upsertOwnedStoreWriteDupeKey;
        pub const validateDocumentExtractionInlineSources = materialization.validateDocumentExtractionInlineSources;
        pub const validateDocumentExtractionInlineSourcesParsed = materialization.validateDocumentExtractionInlineSourcesParsed;
        pub const validateDocumentExtractionInlineSourcesSnapshotParsed = materialization.validateDocumentExtractionInlineSourcesSnapshotParsed;
        pub const validateGraphEdgeMetadataJson = materialization.validateGraphEdgeMetadataJson;
        pub const vectorIndexConsumesChunkKey = materialization.vectorIndexConsumesChunkKey;
        pub const waitForCachedBool = commit.waitForCachedBool;
        pub const Execution = struct {
            local_execution: *LocalExecutionState,
            rewrite_program_cache: @import("../rewrite_program_cache.zig").Cache = .{},
            rewrite_tail_cache: @import("../rewrite_tail_spool.zig").Cache = .{},
            restore_decoder_cache: @import("../restore_decoder_cache.zig").Cache = .{},
            alloc: Allocator,
            runtime_alloc: Allocator,
            open_mode: OpenOptions.OpenMode,
            primary_backend: PrimaryBackend,
            primary_lsm_storage: ?lsm_backend_mod.Storage,
            physical_root_mode: OpenOptions.PhysicalRootMode,
            index_backends: db_config.IndexBackendOptions,
            core: *db_core.DBCore,
            root_incarnation: u128 = 0,
            async_context: *AsyncContext,
            backend_runtime: *background_runtime_mod.BackendRuntime,
            backend_owner_id: u64,
            repair_cleanup_owner_id: u64,
            executor: *derived_executor_mod.Executor,
            secret_store: ?*common_secrets.FileStore,
            remote_content: ?*const scraping.RemoteContentConfig,
            enrichment_runtime: ?*enrichment_runtime_mod.EnrichmentRuntime,
            resolution_runtime: ?*resolution_runtime_mod.ResolutionRuntime = null,
            promotion_runtime: ?*promotion_runtime_mod.PromotionRuntime = null,
            ttl_cleanup_context: ?*TtlCleanupContext,
            transaction_recovery_identity_context: ?*db_core.TransactionRecoveryIdentityContext,
            transaction_recovery_local_context: ?*TransactionRecoveryLocalContext,
            text_merge_runtime: ?*text_merge_runtime_mod.TextMergeRuntime,
            sparse_compaction_runtime: ?*sparse_compaction_runtime_mod.SparseCompactionRuntime,
            last_run_until_idle_no_progress: ?execution_resources.NoProgressDiagnostic = null,
            shadow: ?*ShadowState,
            bulk_identity: @import("bulk_ingest_session.zig").IdentityScratch = .{},
            embedding_activity: @import("embedding_activity_cache.zig").Owner = .{},
            active_index_repairs: std.StringHashMapUnmanaged(bool) = .{},
            graph_restore_parse_cache: ?GraphRestoreParseCache = null,

            pub const derivedCoverageAppliesToIndex = Implementation.derivedCoverageAppliesToIndex;
            pub const acquireReplicationMutationShared = Implementation.acquireReplicationMutationShared;
            pub const acquireTransactionSchemaView = Implementation.acquireTransactionSchemaView;
            pub const artifactMaterializationsReady = Implementation.artifactMaterializationsReady;
            pub const maintenanceRequiresOrderedApply = Implementation.maintenanceRequiresOrderedApply;
            pub fn finishBatchGraphEndpointCleanup(_: *@This(), _: types.BatchRequest, _: BatchExecutionOptions) !void {
                // Recovery owns only the committed mutation. The resident DB services
                // its durable jobs independently after recovery releases admission.
            }
            pub const batchContext = Implementation.batchContext;
            pub const batchInternalPrepared = Implementation.batchInternalPrepared;
            pub const batchInternalWithPreparationAllocator = Implementation.batchInternalWithPreparationAllocator;
            pub const captureTransformReadSnapshot = Implementation.captureTransformReadSnapshot;
            pub const captureGeneratedWriteReadSnapshot = Implementation.captureGeneratedWriteReadSnapshot;
            pub const validateGeneratedWriteReadSnapshot = Implementation.validateGeneratedWriteReadSnapshot;
            pub const GeneratedWriteReadSnapshot = Implementation.GeneratedWriteReadSnapshot;
            pub const clearActiveIndexRepairsLocked = Implementation.clearActiveIndexRepairsLocked;
            pub const clearBulkIngestIdentityAllNewLocked = Implementation.clearBulkIngestIdentityAllNewLocked;
            pub const clearDurableReplicationOutbox = Implementation.clearDurableReplicationOutbox;
            pub const clearLiveDocSetCache = Implementation.clearLiveDocSetCache;
            pub const clearNonVisibleDocSetCache = Implementation.clearNonVisibleDocSetCache;
            pub const deleteDocumentArtifactChildRangeOutboxEntry = Implementation.deleteDocumentArtifactChildRangeOutboxEntry;
            pub const denseRepairWriteBackpressured = Implementation.denseRepairWriteBackpressured;
            pub const drainDocumentArtifactChildRangeOutbox = Implementation.drainDocumentArtifactChildRangeOutbox;
            pub const enforcePortableRuntimeGate = Implementation.enforcePortableRuntimeGate;
            pub const enforceReplicationWriteGate = Implementation.enforceReplicationWriteGate;
            pub const enforceRowPolicyMutationLocked = Implementation.enforceRowPolicyMutationLocked;
            pub const ensureDurableReplicationStartupBarrier = Implementation.ensureDurableReplicationStartupBarrier;
            pub const failIfIdentityOrdinalExhaustedForNewUpserts = Implementation.failIfIdentityOrdinalExhaustedForNewUpserts;
            pub const finalizePendingRowPolicyReceiptLocked = Implementation.finalizePendingRowPolicyReceiptLocked;
            pub const flushDurableReplicationOutboxes = Implementation.flushDurableReplicationOutboxes;
            pub const flushDurableReplicationOutboxesLocked = Implementation.flushDurableReplicationOutboxesLocked;
            pub const flushTransactionReplicationOutbox = Implementation.flushTransactionReplicationOutbox;
            pub const hasConfiguredResolvers = Implementation.hasConfiguredResolvers;
            pub const lockApplyForPortableRuntime = Implementation.lockApplyForPortableRuntime;
            pub const markPrecomputedEnrichmentAppliedForSync = Implementation.markPrecomputedEnrichmentAppliedForSync;
            pub const maybeFinalizePendingRowPolicyPublication = Implementation.maybeFinalizePendingRowPolicyPublication;
            pub const mirrorReplicationEncodedBatchMutationCommit = Implementation.mirrorReplicationEncodedBatchMutationCommit;
            pub const mirrorReplicationReplayPayloadCommit = Implementation.mirrorReplicationReplayPayloadCommit;
            pub const noPendingEnrichmentReplayThrough = Implementation.noPendingEnrichmentReplayThrough;
            pub const notifyResolverReplayRuntimes = Implementation.notifyResolverReplayRuntimes;
            pub const notifyResolverReplayRuntimesForced = Implementation.notifyResolverReplayRuntimesForced;
            pub const pendingRowPolicyReceipt = Implementation.pendingRowPolicyReceipt;
            pub const preflightReplicationBatchSyncCommit = Implementation.preflightReplicationBatchSyncCommit;
            pub const prepareMergeArtifactEffects = Implementation.prepareMergeArtifactEffects;
            pub const rememberBulkIngestAllNewIdentityUpserts = Implementation.rememberBulkIngestAllNewIdentityUpserts;
            pub const replicationMutationBarrier = Implementation.replicationMutationBarrier;
            pub const resolveTransactionIntentsInternal = Implementation.resolveTransactionIntentsInternal;
            pub const resolveTransactionIntentsPrepared = Implementation.resolveTransactionIntentsPrepared;
            pub const restoreStagingStatus = Implementation.restoreStagingStatus;
            pub const shouldApplySplitReplicationLocked = Implementation.shouldApplySplitReplicationLocked;
            pub const unchangedDerivedReplayTargetsServiceable = Implementation.unchangedDerivedReplayTargetsServiceable;
            pub const validateLiveReplicationIntegrityEffects = Implementation.validateLiveReplicationIntegrityEffects;
            pub const validateMergeCleanupPageLocked = Implementation.validateMergeCleanupPageLocked;
            pub const validatePreparedSchemaViewLocked = Implementation.validatePreparedSchemaViewLocked;
            pub const validateResolvedKeyOwnership = Implementation.validateResolvedKeyOwnership;
            pub const validateRestoreStagingReplicationEffects = Implementation.validateRestoreStagingReplicationEffects;
            pub const validateTransformReadSnapshot = Implementation.validateTransformReadSnapshot;
            pub const waitForResolvedTransactionSync = Implementation.waitForResolvedTransactionSync;

            // Recovery leaves resident retry scheduling to the serving owner.
            pub fn scheduleDurableReplicationOutboxRecovery(_: *@This()) void {}

            pub fn resolveTransaction(self: *@This(), txn_id: transactions_mod.TxnId, status: transactions_mod.TxnStatus, commit_version: u64) !void {
                try self.resolveTransactionIntentsInternal(txn_id, status, commit_version, .propose, .none, null, null);
            }

            pub fn waitForResolvedTransactionSyncWithCancellation(self: *@This(), sync_level: types.SyncLevel, _: u64, _: types.CancellationToken) !void {
                std.debug.assert(sync_level == .propose);
                try self.executor.failIfUnhealthy();
            }

            pub fn waitForSyncLevelWithCancellation(self: *@This(), sync_level: types.SyncLevel, _: u64, _: ManagedSyncTargets, _: types.CancellationToken, _: bool) !void {
                std.debug.assert(sync_level == .propose);
                try self.executor.failIfUnhealthy();
            }

            pub fn bulkSessionActive(_: *@This()) bool {
                return false;
            }

            pub fn deinitScratch(self: *@This()) void {
                const filesystem_io = self.backend_runtime.filesystemIo() orelse std.Options.debug_io;
                self.restore_decoder_cache.deinit(filesystem_io);
                self.rewrite_program_cache.deinit(filesystem_io);
                self.rewrite_tail_cache.deinit(filesystem_io);
                if (self.graph_restore_parse_cache) |*cache| cache.deinit(self.alloc);
                if (self.last_run_until_idle_no_progress) |*diagnostic| diagnostic.deinit(self.alloc);
                self.embedding_activity.deinit(self.alloc);
                self.clearActiveIndexRepairsLocked();
                self.active_index_repairs.deinit(self.alloc);
                self.bulk_identity.deinit(self.alloc);
            }

            comptime {
                // Borrowed execution must never gain DB destruction or resident
                // callbacks: its address is valid only for this synchronous invocation.
                for (.{ "close", "closeOwned", "deinitWrapperState", "startResidentBackgroundWorkersIfNeeded", "reconcilePublishedSchemaIndexes", "reconcileSchemaPass" }) |operation| {
                    if (@hasDecl(@This(), operation)) @compileError("borrowed mutation receiver exposes an owning operation: " ++ operation);
                }
                for (.{ "generation_read_lease", "owned_backend_runtime", "owned_resource_manager", "source_vector_storage", "stable_address" }) |field| {
                    if (@hasField(@This(), field)) @compileError("borrowed mutation receiver retains DB ownership: " ++ field);
                }
            }
        };
        pub const Context = struct {
            local_execution: *LocalExecutionState,
            alloc: Allocator,
            runtime_alloc: Allocator,
            open_mode: OpenOptions.OpenMode,
            primary_backend: PrimaryBackend,
            primary_lsm_storage: ?lsm_backend_mod.Storage,
            physical_root_mode: OpenOptions.PhysicalRootMode,
            index_backends: db_config.IndexBackendOptions,
            core: *db_core.DBCore,
            root_incarnation: u128,
            async_context: *AsyncContext,
            backend_runtime: *background_runtime_mod.BackendRuntime,
            backend_owner_id: u64,
            repair_cleanup_owner_id: u64,
            executor: *derived_executor_mod.Executor,
            ttl_cleanup_context: ?*TtlCleanupContext,
            transaction_recovery_identity_context: ?*db_core.TransactionRecoveryIdentityContext,
            secret_store: ?*common_secrets.FileStore,
            remote_content: ?*const scraping.RemoteContentConfig,

            pub fn borrow(db: anytype) @This() {
                return .{
                    .local_execution = db.local_execution,
                    .alloc = db.alloc,
                    .runtime_alloc = db.runtime_alloc,
                    .open_mode = db.open_mode,
                    .primary_backend = db.primary_backend,
                    .primary_lsm_storage = db.primary_lsm_storage,
                    .physical_root_mode = db.physical_root_mode,
                    .index_backends = db.index_backends,
                    .core = db.core,
                    .root_incarnation = db.root_incarnation,
                    .async_context = db.async_context,
                    .backend_runtime = db.backend_runtime,
                    .backend_owner_id = db.backend_owner_id,
                    .repair_cleanup_owner_id = db.repair_cleanup_owner_id,
                    .executor = db.executor,
                    .ttl_cleanup_context = db.ttl_cleanup_context,
                    .transaction_recovery_identity_context = db.transaction_recovery_identity_context,
                    .secret_store = db.secret_store,
                    .remote_content = db.remote_content,
                };
            }

            /// Bind the dedicated synchronous receiver. It owns
            /// no DB resources; mutable admission and publication state stays shared.
            /// Its scratch allocations are released before returning to the driver.
            pub fn mutation(self: *const @This()) Execution {
                return .{
                    .local_execution = self.local_execution,
                    .alloc = self.alloc,
                    .runtime_alloc = self.runtime_alloc,
                    .open_mode = self.open_mode,
                    .primary_backend = self.primary_backend,
                    .primary_lsm_storage = self.primary_lsm_storage,
                    .physical_root_mode = self.physical_root_mode,
                    .index_backends = self.index_backends,
                    .core = self.core,
                    .root_incarnation = self.root_incarnation,
                    .async_context = self.async_context,
                    .backend_runtime = self.backend_runtime,
                    .backend_owner_id = self.backend_owner_id,
                    .repair_cleanup_owner_id = self.repair_cleanup_owner_id,
                    .executor = self.executor,
                    .secret_store = self.secret_store,
                    .remote_content = self.remote_content,
                    .enrichment_runtime = self.async_context.enrichment_runtime,
                    .resolution_runtime = self.async_context.resolution_runtime,
                    .promotion_runtime = self.async_context.promotion_runtime,
                    .ttl_cleanup_context = self.ttl_cleanup_context,
                    .transaction_recovery_identity_context = self.transaction_recovery_identity_context,
                    .transaction_recovery_local_context = null,
                    .text_merge_runtime = self.async_context.text_merge_runtime,
                    .sparse_compaction_runtime = self.async_context.sparse_compaction_runtime,
                    .shadow = null,
                };
            }
        };
        pub const prepareRelationalRows = preparation.prepareRelationalRows;

        pub const relationalPreparationWorkers = preparation.relationalPreparationWorkers;
    };
}
