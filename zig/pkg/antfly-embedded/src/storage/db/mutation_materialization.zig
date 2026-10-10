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

//! Derived effects, artifact production and replay materialization.
//! Receivers borrow local resources; lifetime and scheduling remain with DB.

const CollectSparseFieldWritesProfile = @import("replay_vector_collectors.zig").CollectSparseFieldWritesProfile;
const GraphTtlSha256 = @import("antfly_hash").Sha256;
pub const IndexTargetVisibility = @import("query_visibility.zig").IndexTargetVisibility;
const OwnedDenseEmbeddingWrites = @import("replay_vector_collectors.zig").OwnedDenseEmbeddingWrites;
const OwnedSparseEmbeddingWrites = @import("replay_vector_collectors.zig").OwnedSparseEmbeddingWrites;
const ant_json = @import("antfly-json");
const appendOwnedKey = @import("owned_keys.zig").appendOwnedKey;
const appendUniqueOwnedKey = @import("owned_keys.zig").appendUniqueOwnedKey;
const apply_state = @import("derived/apply_state.zig");
const artifact_ids = @import("artifact_ids.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const backend_types = @import("../backend_types.zig");
const build_options = @import("build_options");
const builtin = @import("builtin");
const change_journal_mod = @import("derived/change_journal.zig");
const chunk_artifact_mod = @import("../../chunking/chunk.zig");
const chunker_mod = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("enrichment/chunker_stub.zig")
else
    @import("enrichment/chunker.zig");
const chunking_types_mod = @import("../../chunking/types.zig");
const collectDenseEmbeddingWritesForBatch = @import("replay_vector_collectors.zig").collectDenseEmbeddingWritesForBatch;
const collectSparseEmbeddingWritesForBatch = @import("replay_vector_collectors.zig").collectSparseEmbeddingWritesForBatch;
const collectSparseFieldWritesProfiled = @import("replay_vector_collectors.zig").collectSparseFieldWritesProfiled;
const denseEmbeddingDocKeySet = @import("replay_vector_collectors.zig").denseEmbeddingDocKeySet;
const derived_types = @import("derived/derived_types.zig");
const docstore_mod = @import("../docstore.zig");
const document_child_range_effects = @import("document_child_range_effects.zig");
const document_child_range_outbox = @import("document_child_range_outbox.zig");
const document_extraction_mod = @import("enrichment/document_extraction.zig");
const document_unit_fingerprint = @import("enrichment/document_unit_fingerprint.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const execution_resources = @import("execution_resources.zig");
const filterDeletedEmbeddingWrites = @import("replay_vector_collectors.zig").filterDeletedEmbeddingWrites;
const graph_asset_state = @import("graph_asset_state.zig");
const graph_edge_contender = @import("graph_edge_contender.zig");
const graph_edge_ttl_expiration = @import("graph_edge_ttl_expiration.zig");
const graph_edge_ttl_tombstone = @import("graph_edge_ttl_tombstone.zig");
const graph_metadata_tables = @import("../../graph/metadata_tables.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_state_name = @import("graph_state_name.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const hierarchy_navigation = @import("../hierarchy_navigation.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document_mapper.zig");
const relational_store = @import("relational_store.zig");
const resolver_lib = @import("antfly_resolver");
const resource_manager_mod = @import("../resource_manager.zig");
const result_collectors = @import("result_collectors.zig");
const runtime_failure_abi = @import("runtime_failure_abi");
const schema_mod = @import("../schema.zig");
const schema_registry_mod = @import("schema_registry.zig");
const snapshot_admission_mod = @import("snapshot_admission.zig");
const sparseEmbeddingDocKeySet = @import("replay_vector_collectors.zig").sparseEmbeddingDocKeySet;
const sparse_mod = if (builtin.os.tag == .freestanding)
    @import("sparse_stub.zig")
else
    @import("../../sparse/sparse.zig");
const std = @import("std");
const template_mod = if (builtin.os.tag == .freestanding or builtin.os.tag == .wasi or builtin.is_test or build_options.bench_minimal_deps)
    @import("template_stub.zig")
else
    @import("../../template.zig");
const template_remote = if (builtin.os.tag == .freestanding or builtin.os.tag == .wasi or builtin.is_test or build_options.bench_minimal_deps)
    @import("template_remote_stub.zig")
else
    @import("../../template_remote.zig");
const text_merge_runtime_mod = @import("maintenance/text_merge_runtime.zig");
const transactions_mod = @import("../transactions.zig");
const transform_mod = @import("transform.zig");
const types = @import("types.zig");

pub fn ImplementationFor(comptime S: type, comptime D: type) type {
    return struct {
        const Implementation = S;
        const Allocator = execution_resources.Allocator;
        const ArtifactRepairCompletionState = execution_resources.ArtifactRepairCompletionState;
        const AsyncContext = execution_resources.AsyncContext;
        const BatchExecutionContext = execution_resources.BatchExecutionContext;
        const BatchProfile = execution_resources.BatchProfile;
        const BorrowedGraphMaterializationBatch = execution_resources.BorrowedGraphMaterializationBatch;
        const ChunkCacheEntry = execution_resources.ChunkCacheEntry;
        const ChunkEmbeddingSource = execution_resources.ChunkEmbeddingSource;
        const DerivedCoverageDocOutcome = execution_resources.DerivedCoverageDocOutcome;
        const DerivedCoverageOutcome = execution_resources.DerivedCoverageOutcome;
        const DocumentArtifactChildRangeDispatcher = execution_resources.DocumentArtifactChildRangeDispatcher;
        const DocumentArtifactChildRangeOutboxDrainResult = execution_resources.DocumentArtifactChildRangeOutboxDrainResult;
        const DocumentExtractionCatalogView = execution_resources.DocumentExtractionCatalogView;
        const DocumentExtractionChunkView = execution_resources.DocumentExtractionChunkView;
        const DocumentExtractionEmbeddingView = execution_resources.DocumentExtractionEmbeddingView;
        const DocumentExtractionPreviousState = execution_resources.DocumentExtractionPreviousState;
        const DocumentExtractionRangeRoute = execution_resources.DocumentExtractionRangeRoute;
        const DocumentExtractionUnitDescriptor = execution_resources.DocumentExtractionUnitDescriptor;
        const EmbeddingArtifactOrigin = execution_resources.EmbeddingArtifactOrigin;
        const EnrichmentTerminalFailureMarkerWrite = execution_resources.EnrichmentTerminalFailureMarkerWrite;
        const GeneratedEmbeddingMemo = execution_resources.GeneratedEmbeddingMemo;
        const GeneratedEnrichmentNameLookup = execution_resources.GeneratedEnrichmentNameLookup;
        const GraphArtifactRefView = execution_resources.GraphArtifactRefView;
        const GraphContenderChange = execution_resources.GraphContenderChange;
        const GraphContenderChanges = execution_resources.GraphContenderChanges;
        const GraphContenderMutation = execution_resources.GraphContenderMutation;
        const GraphContenderReconcileResult = execution_resources.GraphContenderReconcileResult;
        const GraphEdgeWinners = execution_resources.GraphEdgeWinners;
        const GraphMaterializationOptions = execution_resources.GraphMaterializationOptions;
        const GraphMutationCollectionOptions = execution_resources.GraphMutationCollectionOptions;
        const GraphTtlCandidate = execution_resources.GraphTtlCandidate;
        const ManagedIndexBatchApplicability = execution_resources.ManagedIndexBatchApplicability;
        const ManagedIndexCandidate = execution_resources.ManagedIndexCandidate;
        const ManagedSyncTargets = execution_resources.ManagedSyncTargets;
        const MentionEdgeAggregate = execution_resources.MentionEdgeAggregate;
        const NeighborContextReplayHints = execution_resources.NeighborContextReplayHints;
        const OwnedGraphMutations = execution_resources.OwnedGraphMutations;
        const PendingArtifactWriteIndex = execution_resources.PendingArtifactWriteIndex;
        const PendingDocumentUnitDenseChunkEmbedding = execution_resources.PendingDocumentUnitDenseChunkEmbedding;
        const PendingDocumentUnitSparseChunkEmbedding = execution_resources.PendingDocumentUnitSparseChunkEmbedding;
        const PendingGraphContenderOverlay = execution_resources.PendingGraphContenderOverlay;
        const PrecomputeAssetProducerBatchItem = execution_resources.PrecomputeAssetProducerBatchItem;
        const PrecomputedCoverageCandidate = execution_resources.PrecomputedCoverageCandidate;
        const PrecomputedCoverageOutcome = execution_resources.PrecomputedCoverageOutcome;
        const StoreWritePositions = execution_resources.StoreWritePositions;
        const appendJsonString = D.appendJsonString;
        const appendRelationItem = D.appendRelationItem;
        const artifact_repair_completion_state_len = D.artifact_repair_completion_state_len;
        const artifact_repair_summary_dirty_marker = D.artifact_repair_summary_dirty_marker;
        const chunkPayloadTextAlloc = D.chunkPayloadTextAlloc;
        const clearChunkEmbeddingSourceList = execution_resources.clearChunkEmbeddingSourceList;
        const collectDocumentWritesProfiled = D.collectDocumentWritesProfiled;
        const collectTextDocumentWritesForIndex = D.collectTextDocumentWritesForIndex;
        const denseEmbeddingArtifactRepairReasonFromRaw = D.denseEmbeddingArtifactRepairReasonFromRaw;
        const dense_catch_up_default_deferred_hbc_leaf_split_members_per_publish = D.dense_catch_up_default_deferred_hbc_leaf_split_members_per_publish;
        const dense_catch_up_default_deferred_hbc_leaf_splits_per_publish = D.dense_catch_up_default_deferred_hbc_leaf_splits_per_publish;
        const dense_catch_up_default_deferred_l0_limit = D.dense_catch_up_default_deferred_l0_limit;
        const documentArtifactChildRangesFromManifestJsonAlloc = D.documentArtifactChildRangesFromManifestJsonAlloc;
        const documentTargetsTextIndex = D.documentTargetsTextIndex;
        const document_extraction_client = execution_resources.document_extraction_client;
        const document_extraction_range_target_children = D.document_extraction_range_target_children;
        const document_extraction_range_target_text_bytes = D.document_extraction_range_target_text_bytes;
        const freeChunkEmbeddingSources = D.freeChunkEmbeddingSources;
        const freeDocumentArtifactChildRanges = D.freeDocumentArtifactChildRanges;
        const freeGraphWriteFields = D.freeGraphWriteFields;
        const graphWritesFromArtifactParsedPageAlloc = D.graphWritesFromArtifactParsedPageAlloc;
        const hierarchy_navigation_block_size = D.hierarchy_navigation_block_size;
        const hierarchy_navigation_unit_fingerprint_field = D.hierarchy_navigation_unit_fingerprint_field;
        const jsonStringField = D.jsonStringField;
        const remoteRenderConfig = D.remoteRenderConfig;
        const replayDocumentStoreKeyAlloc = D.replayDocumentStoreKeyAlloc;
        const selectGraphArtifactPath = D.selectGraphArtifactPath;
        const sparseEmbeddingArtifactRepairReason = D.sparseEmbeddingArtifactRepairReason;
        const InlineChunkEmbeddingCleanup = S.InlineChunkEmbeddingCleanup;
        const benchMetricsEnabled = S.benchMetricsEnabled;
        const boundaryFailureErrorName = S.boundaryFailureErrorName;
        const cachedEnvUsize = S.cachedEnvUsize;
        const cachedOptionalEnvUsize = S.cachedOptionalEnvUsize;
        const currentTimeNs = S.currentTimeNs;
        const extractStringField = S.extractStringField;
        const flushGeneratedDenseChunkBatch = S.flushGeneratedDenseChunkBatch;
        const flushGeneratedDenseChunkSourceBatch = S.flushGeneratedDenseChunkSourceBatch;
        const flushGeneratedSparseChunkBatch = S.flushGeneratedSparseChunkBatch;
        const flushGeneratedSparseChunkSourceBatch = S.flushGeneratedSparseChunkSourceBatch;
        const freeDocumentExtractionUnitDescriptors = S.freeDocumentExtractionUnitDescriptors;
        const freeOwnedConstKeySlice = S.freeOwnedConstKeySlice;
        const freeOwnedKeySlice = S.freeOwnedKeySlice;
        const generatedEmbedBatchBytes = S.generatedEmbedBatchBytes;
        const generatedEmbedBatchItems = S.generatedEmbedBatchItems;
        const isMetadataKey = S.isMetadataKey;
        const lockAtomicWithBackoff = S.lockAtomicWithBackoff;
        const monotonicTimeNs = S.monotonicTimeNs;
        const nsToMs = S.nsToMs;
        const openModeRequiresReadOnlyBackends = S.openModeRequiresReadOnlyBackends;
        const orderedCoverageActive = S.orderedCoverageActive;
        const preparePreservedEmbeddingSources = S.preparePreservedEmbeddingSources;
        const prewarmGeneratedMemoFromArtifact = S.prewarmGeneratedMemoFromArtifact;
        const profileDelta = S.profileDelta;
        const recordProfileNs = S.recordProfileNs;
        const replayRecordHasTargetHint = S.replayRecordHasTargetHint;
        const saveAppliedSequencesBatchContext = S.saveAppliedSequencesBatchContext;
        const shouldDeferBacklogPressureForExternalDenseBulk = S.shouldDeferBacklogPressureForExternalDenseBulk;
        const syncLevelParticipatesInDerivedBacklogPressure = S.syncLevelParticipatesInDerivedBacklogPressure;
        const truncateReplayJournalIfSafeContext = S.truncateReplayJournalIfSafeContext;
        pub const DenseArtifactCounterBootstrap = struct {
            repair_id: u128,
            attempt_id: u128,
            delta: i64 = 0,
        };

        pub const DenseArtifactCounterCatalog = struct {
            targets: std.ArrayListUnmanaged(DenseArtifactCounterTarget) = .empty,
            by_artifact: std.HashMapUnmanaged(DenseArtifactTargetKey, std.ArrayListUnmanaged(usize), DenseArtifactTargetKeyContext, 80) = .empty,

            pub fn deinit(self: *@This(), alloc: Allocator) void {
                var values = self.by_artifact.valueIterator();
                while (values.next()) |indices| indices.deinit(alloc);
                self.by_artifact.deinit(alloc);
                for (self.targets.items) |*target| target.deinit(alloc);
                self.targets.deinit(alloc);
                self.* = .{};
            }

            fn indexTarget(self: *const @This(), index_name: []const u8) ?usize {
                for (self.targets.items, 0..) |target, i| {
                    if (std.mem.eql(u8, target.index_name, index_name)) return i;
                }
                return null;
            }

            fn add(
                self: *@This(),
                alloc: Allocator,
                index_name: []const u8,
                artifact_name: []const u8,
                dims: u32,
            ) !void {
                if (self.indexTarget(index_name)) |target_idx| {
                    const target = &self.targets.items[target_idx];
                    for (target.artifact_names.items) |configured| {
                        if (std.mem.eql(u8, configured, artifact_name)) return;
                    }
                    const owned_name = try alloc.dupe(u8, artifact_name);
                    errdefer alloc.free(owned_name);
                    try target.artifact_names.append(alloc, owned_name);
                    const key: DenseArtifactTargetKey = .{ .artifact_name = owned_name, .dims = dims };
                    var entry = try self.by_artifact.getOrPut(alloc, key);
                    if (!entry.found_existing) entry.value_ptr.* = .empty;
                    try entry.value_ptr.append(alloc, target_idx);
                    return;
                }
                var owned_index_name: ?[]u8 = try alloc.dupe(u8, index_name);
                errdefer if (owned_index_name) |value| alloc.free(value);
                var owned_artifact_name: ?[]u8 = try alloc.dupe(u8, artifact_name);
                errdefer if (owned_artifact_name) |value| alloc.free(value);
                var artifact_names = std.ArrayListUnmanaged([]u8).empty;
                errdefer artifact_names.deinit(alloc);
                try artifact_names.append(alloc, owned_artifact_name.?);
                try self.targets.append(alloc, .{
                    .index_name = owned_index_name.?,
                    .artifact_names = artifact_names,
                    .dims = dims,
                });
                artifact_names = .empty;
                owned_index_name = null;
                owned_artifact_name = null;
                var keep_target = false;
                errdefer if (!keep_target) {
                    var removed = self.targets.pop().?;
                    removed.deinit(alloc);
                };

                const target_idx = self.targets.items.len - 1;
                const target = &self.targets.items[target_idx];
                const key: DenseArtifactTargetKey = .{
                    .artifact_name = target.artifact_names.items[0],
                    .dims = target.dims,
                };
                var entry = try self.by_artifact.getOrPut(alloc, key);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                errdefer if (!entry.found_existing) {
                    entry.value_ptr.deinit(alloc);
                    _ = self.by_artifact.remove(key);
                };
                try entry.value_ptr.append(alloc, target_idx);
                keep_target = true;
            }

            fn init(
                alloc: Allocator,
                index_manager: *const index_manager_mod.IndexManager,
            ) !DenseArtifactCounterCatalog {
                var catalog: DenseArtifactCounterCatalog = .{};
                errdefer catalog.deinit(alloc);

                for (index_manager.dense_indexes.items) |*entry| {
                    const artifact_backed = entry.external or entry.chunk_name != null or entry.embedding_name != null or entry.embedding_names.len > 0;
                    if (!artifact_backed) continue;
                    if (entry.embedding_names.len > 0) {
                        for (entry.embedding_names) |artifact_name| try catalog.add(alloc, entry.config.name, artifact_name, entry.dims);
                    } else {
                        try catalog.add(alloc, entry.config.name, denseArtifactNameForEntry(entry), entry.dims);
                    }
                }
                for (index_manager.status_only_index_configs) |cfg| {
                    if (cfg.kind != .dense_vector or catalog.indexTarget(cfg.name) != null) continue;
                    const requires_coverage = index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        // Invalid/unsupported quarantined configs are deliberately
                        // isolated from healthy indexes. They cannot have a valid
                        // counter target and must not make unrelated writes fail.
                        else => continue,
                    };
                    if (!requires_coverage) continue;
                    const artifact_names = index_manager_mod.denseConfigArtifactNamesAlloc(alloc, cfg) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => continue,
                    };
                    defer {
                        for (artifact_names) |name| alloc.free(name);
                        alloc.free(artifact_names);
                    }
                    const dims = index_manager_mod.denseConfigDimensions(alloc, cfg) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => continue,
                    };
                    for (artifact_names) |artifact_name| try catalog.add(alloc, cfg.name, artifact_name, dims);
                }
                return catalog;
            }

            fn targetsFor(
                self: *const @This(),
                artifact_name: []const u8,
                dims: u32,
            ) []const usize {
                const indices = self.by_artifact.get(.{
                    .artifact_name = artifact_name,
                    .dims = dims,
                }) orelse return &.{};
                return indices.items;
            }
        };

        pub const DenseArtifactCounterTarget = struct {
            index_name: []u8,
            artifact_names: std.ArrayListUnmanaged([]u8) = .empty,
            dims: u32,

            pub fn deinit(self: *@This(), alloc: Allocator) void {
                alloc.free(self.index_name);
                for (self.artifact_names.items) |name| alloc.free(name);
                self.artifact_names.deinit(alloc);
                self.* = undefined;
            }
        };

        pub const DenseArtifactTargetKey = struct {
            artifact_name: []const u8,
            dims: u32,
        };

        pub const DenseArtifactTargetKeyContext = struct {
            pub fn hash(_: @This(), key: DenseArtifactTargetKey) u64 {
                var hasher = std.hash.Wyhash.init(0);
                const artifact_name_len: u64 = @intCast(key.artifact_name.len);
                hasher.update(std.mem.asBytes(&artifact_name_len));
                hasher.update(key.artifact_name);
                hasher.update(std.mem.asBytes(&key.dims));
                return hasher.final();
            }

            pub fn eql(_: @This(), lhs: DenseArtifactTargetKey, rhs: DenseArtifactTargetKey) bool {
                return lhs.dims == rhs.dims and std.mem.eql(u8, lhs.artifact_name, rhs.artifact_name);
            }
        };

        pub const FinalDenseArtifactMutation = union(enum) {
            deleted,
            write: usize,
            promotion: usize,
        };

        pub const PendingDenseArtifactCounterMutation = union(enum) {
            counter: u64,
            bootstrap: DenseArtifactCounterBootstrap,
            unavailable,
        };

        pub fn acquireTransactionSchemaView(self: anytype, alloc: Allocator, binding: ?transactions_mod.SchemaBinding) !?schema_registry_mod.SchemaView {
            if (binding) |pinned| return if (pinned.version) |version|
                try self.core.acquireSchemaVersionWriteView(alloc, version)
            else
                null;
            return self.core.acquireSchemaView();
        }

        pub fn appendDenseArtifactCounterBootstrapWrite(
            alloc: Allocator,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            index_name: []const u8,
            bootstrap: DenseArtifactCounterBootstrap,
        ) !void {
            const key = try denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
            errdefer alloc.free(key);
            const value = try alloc.alloc(u8, dense_artifact_counter_bootstrap_encoded_len);
            errdefer alloc.free(value);
            encodeDenseArtifactCounterBootstrap(bootstrap, value[0..dense_artifact_counter_bootstrap_encoded_len]);
            try owned_keys.append(alloc, key);
            try owned_values.append(alloc, value);
            try store_writes.append(alloc, .{ .key = key, .value = value });
        }

        pub fn appendDenseArtifactCounterMutations(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *const index_manager_mod.IndexManager,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            delete_keys: []const []const u8,
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            try appendDenseArtifactCounterMutationsWithPromotions(
                alloc,
                store,
                index_manager,
                store_writes,
                delete_keys,
                &.{},
                owned_keys,
                owned_values,
            );
        }

        pub fn appendDenseArtifactCounterMutationsWithPromotions(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *const index_manager_mod.IndexManager,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            delete_keys: []const []const u8,
            promotions: []const enrichment_runtime_mod.GeneratedArtifactPromotion,
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            var may_mutate_embedding_artifact = false;
            for (delete_keys) |key| {
                if (internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key)) {
                    may_mutate_embedding_artifact = true;
                    break;
                }
            }
            if (!may_mutate_embedding_artifact) {
                for (store_writes.items) |write| {
                    if (internal_keys.isEmbeddingArtifactKey(write.key) or internal_keys.isDerivedEmbeddingArtifactKey(write.key)) {
                        may_mutate_embedding_artifact = true;
                        break;
                    }
                }
            }
            if (!may_mutate_embedding_artifact) {
                for (promotions) |promotion| {
                    if (internal_keys.isEmbeddingArtifactKey(promotion.final_key) or
                        internal_keys.isDerivedEmbeddingArtifactKey(promotion.final_key))
                    {
                        may_mutate_embedding_artifact = true;
                        break;
                    }
                }
            }
            // Ordinary document-only writes stay allocation-free in counter
            // routing. Build the catalog lookup only for commits that can actually
            // change embedding-artifact coverage.
            if (!may_mutate_embedding_artifact) return;

            var catalog = try DenseArtifactCounterCatalog.init(alloc, index_manager);
            defer catalog.deinit(alloc);
            if (catalog.targets.items.len == 0) return;
            var mutations = std.AutoHashMapUnmanaged(usize, PendingDenseArtifactCounterMutation){};
            defer mutations.deinit(alloc);
            var final_artifact_mutations = std.StringHashMapUnmanaged(FinalDenseArtifactMutation).empty;
            defer final_artifact_mutations.deinit(alloc);

            for (delete_keys) |key| {
                if (!internal_keys.isEmbeddingArtifactKey(key) and !internal_keys.isDerivedEmbeddingArtifactKey(key)) continue;
                const gop = try final_artifact_mutations.getOrPut(alloc, key);
                if (!gop.found_existing) gop.value_ptr.* = .deleted;
            }
            for (store_writes.items, 0..) |write, write_idx| {
                if (!internal_keys.isEmbeddingArtifactKey(write.key) and !internal_keys.isDerivedEmbeddingArtifactKey(write.key)) continue;
                try final_artifact_mutations.put(alloc, write.key, .{ .write = write_idx });
            }
            for (promotions, 0..) |promotion, promotion_idx| {
                if (!internal_keys.isEmbeddingArtifactKey(promotion.final_key) and
                    !internal_keys.isDerivedEmbeddingArtifactKey(promotion.final_key)) continue;
                try final_artifact_mutations.put(alloc, promotion.final_key, .{ .promotion = promotion_idx });
            }

            // Resolve the committed side of every overwrite through one sorted
            // probe. Point-reading here made the globally serialized commit section
            // perform O(artifact mutations) independent backend transactions and
            // repeatedly decode the same LSM blocks.
            const artifact_keys = try alloc.alloc([]const u8, final_artifact_mutations.count());
            defer alloc.free(artifact_keys);
            var final_it = final_artifact_mutations.iterator();
            var artifact_index: usize = 0;
            while (final_it.next()) |entry| : (artifact_index += 1) {
                artifact_keys[artifact_index] = entry.key_ptr.*;
            }
            std.mem.sort([]const u8, artifact_keys, {}, struct {
                fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.lessThan);
            const old_values = try alloc.alloc(?[]const u8, artifact_keys.len);
            defer alloc.free(old_values);
            var artifact_probe = try store.beginProbeTxn();
            defer artifact_probe.abort();
            try artifact_probe.getManySorted(artifact_keys, old_values);

            for (artifact_keys, old_values) |artifact_key, old_value| {
                const final_mutation = final_artifact_mutations.get(artifact_key).?;
                if (old_value) |value| {
                    try applyDenseArtifactCounterDelta(alloc, store, &catalog, &mutations, artifact_key, value, -1);
                }
                switch (final_mutation) {
                    .deleted => {},
                    .write => |write_idx| {
                        const write = store_writes.items[write_idx];
                        try applyDenseArtifactCounterDelta(alloc, store, &catalog, &mutations, write.key, write.value, 1);
                    },
                    .promotion => |promotion_idx| {
                        const promotion = promotions[promotion_idx];
                        const staged_value = store.get(alloc, promotion.staged_key) catch |err| switch (err) {
                            error.NotFound => return error.InvalidGeneratedArtifactPromotion,
                            else => return err,
                        };
                        defer alloc.free(staged_value);
                        try applyDenseArtifactCounterDelta(alloc, store, &catalog, &mutations, promotion.final_key, staged_value, 1);
                    },
                }
            }

            var it = mutations.iterator();
            while (it.next()) |entry| {
                const target = &catalog.targets.items[entry.key_ptr.*];
                switch (entry.value_ptr.*) {
                    .counter => |count| try appendDenseArtifactTargetCounterWrite(
                        alloc,
                        store_writes,
                        owned_keys,
                        owned_values,
                        target.index_name,
                        count,
                    ),
                    .bootstrap => |bootstrap| try appendDenseArtifactCounterBootstrapWrite(
                        alloc,
                        store_writes,
                        owned_keys,
                        owned_values,
                        target.index_name,
                        bootstrap,
                    ),
                    .unavailable => {},
                }
            }
        }

        pub fn appendDenseArtifactTargetCounterWrite(
            alloc: Allocator,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            index_name: []const u8,
            count: u64,
        ) !void {
            const key = try denseArtifactTargetCounterKeyAlloc(alloc, index_name);
            errdefer alloc.free(key);
            const value = try alloc.alloc(u8, 8);
            errdefer alloc.free(value);
            std.mem.writeInt(u64, value[0..8], count, .little);
            try owned_keys.append(alloc, key);
            try owned_values.append(alloc, value);
            try store_writes.append(alloc, .{
                .key = key,
                .value = value,
            });
        }

        pub fn appendGraphTransformDelete(
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
            deletes: *std.ArrayListUnmanaged(types.GraphEdgeDelete),
            source: []const u8,
            path: transform_mod.GraphProjectionPath,
            value_json: []const u8,
        ) !void {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{ .parse_numbers = false });
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidGraphEdges;
            const target_value = parsed.value.object.get("target") orelse return error.InvalidGraphEdges;
            if (target_value != .string or target_value.string.len == 0) return error.InvalidGraphEdges;

            const index_name = try alloc.dupe(u8, path.index_name);
            errdefer alloc.free(index_name);
            const owned_source = try alloc.dupe(u8, source);
            errdefer alloc.free(owned_source);
            const target = try alloc.dupe(u8, target_value.string);
            errdefer alloc.free(target);
            const edge_type = try alloc.dupe(u8, path.edge_type);
            errdefer alloc.free(edge_type);
            const edge_id = if (parsed.value.object.get("edge_id")) |value| blk: {
                if (value != .string or value.string.len == 0) return error.InvalidGraphEdges;
                break :blk try alloc.dupe(u8, value.string);
            } else "";
            errdefer if (edge_id.len > 0) alloc.free(edge_id);

            // A later pull overrides every earlier projected write for the same
            // physical relationship before the split graph mutation batch is built.
            var write_index = writes.items.len;
            while (write_index > 0) {
                write_index -= 1;
                const prior = writes.items[write_index];
                if (!graphMutationIdentityEql(prior, .{
                    .index_name = index_name,
                    .source = owned_source,
                    .target = target,
                    .edge_type = edge_type,
                    .edge_id = edge_id,
                })) continue;
                var removed = writes.orderedRemove(write_index);
                deinitOwnedGraphEdgeWrite(alloc, &removed);
            }
            try deletes.append(alloc, .{
                .index_name = index_name,
                .source = owned_source,
                .target = target,
                .edge_type = edge_type,
                .edge_id = edge_id,
            });
        }

        pub fn appendGraphTransformWrite(
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
            deletes: *std.ArrayListUnmanaged(types.GraphEdgeDelete),
            source: []const u8,
            path: transform_mod.GraphProjectionPath,
            value_json: []const u8,
        ) !void {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{ .parse_numbers = false });
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidGraphEdges;

            const target_value = parsed.value.object.get("target") orelse return error.InvalidGraphEdges;
            if (target_value != .string or target_value.string.len == 0) return error.InvalidGraphEdges;

            const weight: f64 = if (parsed.value.object.get("weight")) |value| switch (value) {
                .integer => |integer| @floatFromInt(integer),
                .float => |float| float,
                .number_string => |number| try std.fmt.parseFloat(f64, number),
                else => return error.InvalidGraphEdges,
            } else 1.0;
            if (!std.math.isFinite(weight)) return error.InvalidGraphEdges;

            const metadata_json: []const u8 = if (parsed.value.object.get("metadata")) |metadata| blk: {
                // Edge metadata has one stable public shape across ingestion,
                // storage, graph queries, and generated SDKs. Reject scalar or
                // array values before they become durable artifacts rather than
                // allowing a later response to violate the object contract.
                if (metadata != .object) return error.InvalidGraphEdges;
                break :blk try std.json.Stringify.valueAlloc(alloc, metadata, .{});
            } else "";
            errdefer if (metadata_json.len > 0) alloc.free(@constCast(metadata_json));

            const index_name = try alloc.dupe(u8, path.index_name);
            errdefer alloc.free(index_name);
            const owned_source = try alloc.dupe(u8, source);
            errdefer alloc.free(owned_source);
            const target = try alloc.dupe(u8, target_value.string);
            errdefer alloc.free(target);
            const edge_type = try alloc.dupe(u8, path.edge_type);
            errdefer alloc.free(edge_type);
            const edge_id = if (parsed.value.object.get("edge_id")) |value| blk: {
                if (value != .string or value.string.len == 0) return error.InvalidGraphEdges;
                break :blk try alloc.dupe(u8, value.string);
            } else "";
            errdefer if (edge_id.len > 0) alloc.free(edge_id);

            // Transform operations are ordered. Since the storage batch carries
            // graph writes and deletes in separate slices (deletes execute first),
            // discard any earlier projected delete for this identity so a later
            // push/addToSet remains the final operation.
            var delete_index = deletes.items.len;
            while (delete_index > 0) {
                delete_index -= 1;
                const prior = deletes.items[delete_index];
                if (!graphMutationIdentityEql(prior, .{
                    .index_name = index_name,
                    .source = owned_source,
                    .target = target,
                    .edge_type = edge_type,
                    .edge_id = edge_id,
                })) continue;
                var removed = deletes.orderedRemove(delete_index);
                deinitOwnedGraphEdgeDelete(alloc, &removed);
            }

            try writes.append(alloc, .{
                .index_name = index_name,
                .source = owned_source,
                .target = target,
                .edge_type = edge_type,
                .edge_id = edge_id,
                .weight = weight,
                .metadata_json = metadata_json,
            });
        }

        pub fn applyDenseArtifactCounterDelta(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            catalog: *const DenseArtifactCounterCatalog,
            mutations: *std.AutoHashMapUnmanaged(usize, PendingDenseArtifactCounterMutation),
            artifact_key: []const u8,
            artifact_value: ?[]const u8,
            delta: i64,
        ) !void {
            if (delta == 0) return;
            var identity = (artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key) catch |err| switch (err) {
                error.InvalidInternalUserKey => return,
                else => return err,
            }) orelse return;
            defer identity.deinit(alloc);
            const value = artifact_value orelse return;
            const dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch return;
            if (dims == 0) return;

            for (catalog.targetsFor(identity.embedding_name, dims)) |target_idx| {
                const target = &catalog.targets.items[target_idx];
                const gop = try mutations.getOrPut(alloc, target_idx);
                if (!gop.found_existing) {
                    if (try loadDenseArtifactTargetCounter(alloc, store, target.index_name)) |count| {
                        gop.value_ptr.* = .{ .counter = count };
                    } else if (try loadDenseArtifactCounterBootstrap(alloc, store, target.index_name)) |bootstrap| {
                        gop.value_ptr.* = .{ .bootstrap = bootstrap };
                    } else {
                        // A missing counter without an active bootstrap is metadata
                        // debt. Do not manufacture a partial counter from the
                        // next mutation; repair will establish an authoritative
                        // snapshot plus concurrent signed delta.
                        gop.value_ptr.* = .unavailable;
                    }
                }
                switch (gop.value_ptr.*) {
                    .counter => |*count| {
                        if (delta > 0) {
                            count.* +|= @as(u64, @intCast(delta));
                        } else {
                            count.* -|= @as(u64, @intCast(-delta));
                        }
                    },
                    .bootstrap => |*bootstrap| {
                        bootstrap.delta = std.math.add(i64, bootstrap.delta, delta) catch
                            return error.DenseArtifactCounterBootstrapOverflow;
                    },
                    .unavailable => {},
                }
            }
        }

        pub fn artifactMaterializationsReady(self: anytype, txn: anytype, catalogs: @import("artifact_inventory.zig").Catalogs) !bool {
            if (!try self.core.index_manager.matchesArtifactInventory(catalogs)) return false;
            const prefix = try internal_keys.indexArtifactCleanupRootPrefixAlloc(self.alloc);
            defer self.alloc.free(prefix);
            var cursor = try txn.openCursor();
            defer cursor.close();
            const first = try cursor.seekAtOrAfter(prefix) orelse return true;
            return !std.mem.startsWith(u8, first.key, prefix);
        }

        pub fn childRangeManifestReader(self: anytype) document_child_range_effects.Reader {
            const Read = struct {
                fn get(ptr: *anyopaque, alloc: Allocator, key: []const u8) !?[]u8 {
                    const owner: @TypeOf(self) = @ptrCast(@alignCast(ptr));
                    return owner.core.getStoreValue(alloc, key);
                }
            };
            return .{ .ptr = self, .get = Read.get };
        }

        pub fn decodeDenseArtifactCounterBootstrap(raw: []const u8) !DenseArtifactCounterBootstrap {
            if (raw.len != dense_artifact_counter_bootstrap_encoded_len or
                !std.mem.eql(u8, raw[0..dense_artifact_counter_bootstrap_magic.len], dense_artifact_counter_bootstrap_magic))
            {
                return error.InvalidDenseArtifactCounterBootstrap;
            }
            var offset = dense_artifact_counter_bootstrap_magic.len;
            const repair_id = std.mem.readInt(u128, raw[offset..][0..@sizeOf(u128)], .little);
            offset += @sizeOf(u128);
            const attempt_id = std.mem.readInt(u128, raw[offset..][0..@sizeOf(u128)], .little);
            offset += @sizeOf(u128);
            return .{
                .repair_id = repair_id,
                .attempt_id = attempt_id,
                .delta = std.mem.readInt(i64, raw[offset..][0..@sizeOf(i64)], .little),
            };
        }

        pub fn deinitOwnedGraphEdgeDelete(alloc: Allocator, delete: *types.GraphEdgeDelete) void {
            alloc.free(@constCast(delete.index_name));
            alloc.free(@constCast(delete.source));
            alloc.free(@constCast(delete.target));
            alloc.free(@constCast(delete.edge_type));
            if (delete.edge_id.len > 0) alloc.free(@constCast(delete.edge_id));
            if (delete.owner_document.len > 0) alloc.free(@constCast(delete.owner_document));
            if (delete.owner.len > 0) alloc.free(@constCast(delete.owner));
            delete.* = undefined;
        }

        pub fn deinitOwnedGraphEdgeWrite(alloc: Allocator, write: *types.GraphEdgeWrite) void {
            alloc.free(@constCast(write.index_name));
            alloc.free(@constCast(write.source));
            alloc.free(@constCast(write.target));
            alloc.free(@constCast(write.edge_type));
            if (write.edge_id.len > 0) alloc.free(@constCast(write.edge_id));
            if (write.owner_document.len > 0) alloc.free(@constCast(write.owner_document));
            if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
            if (write.owner.len > 0) alloc.free(@constCast(write.owner));
            write.* = undefined;
        }

        pub fn deleteDocumentArtifactChildRangeOutboxEntry(self: anytype, key: []const u8) !void {
            var replication_mutation = self.acquireReplicationMutationShared();
            defer if (replication_mutation) |*lease| lease.release();
            try self.enforceReplicationWriteGate();
            try self.lockApplyForPortableRuntime();
            defer self.core.unlockApply();
            const deletes = [_][]const u8{key};
            try self.core.store.putBatch(&.{}, deletes[0..]);
        }

        pub fn denseArtifactCounterBootstrapKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}{s}", .{ dense_artifact_counter_bootstrap_prefix, index_name });
        }

        pub fn denseArtifactNameForEntry(entry: anytype) []const u8 {
            return entry.embedding_name orelse entry.config.name;
        }

        pub fn denseArtifactTargetCounterKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}{s}", .{ dense_artifact_target_counter_prefix, index_name });
        }

        pub fn denseRepairWriteBackpressured(self: anytype) bool {
            if (!self.async_context.index_repair_replay_pinned.load(.acquire)) return false;
            const manager = self.core.index_manager.resource_manager orelse return false;
            return manager.denseRepairReplayPressureIsHard();
        }

        pub const dense_artifact_counter_bootstrap_encoded_len = dense_artifact_counter_bootstrap_magic.len + 2 * @sizeOf(u128) + @sizeOf(i64);

        pub const dense_artifact_counter_bootstrap_magic = "AFDCB001";

        pub const dense_artifact_counter_bootstrap_prefix = "\x00\x00__metadata__:dense_artifact_counter_bootstrap:";

        pub const dense_artifact_target_counter_prefix = "\x00\x00__metadata__:dense_artifact_target_count:";

        pub fn derivedCoverageAppliesToIndex(self: anytype, kind: types.IndexKind, index_name: []const u8) bool {
            return switch (kind) {
                .graph => blk: {
                    for (self.core.graphIndexes()) |entry| {
                        if (std.mem.eql(u8, entry.config.name, index_name))
                            break :blk entry.artifact_sources.len > 0;
                    }
                    break :blk false;
                },
                .full_text => blk: {
                    for (self.core.index_manager.text_indexes.items) |entry| {
                        if (std.mem.eql(u8, entry.config.name, index_name))
                            break :blk entry.chunk_name != null or entry.source_artifact_names.len > 0;
                    }
                    break :blk false;
                },
                else => true,
            };
        }

        pub fn drainDocumentArtifactChildRangeOutbox(
            self: anytype,
            dispatcher: DocumentArtifactChildRangeDispatcher,
            limit: usize,
        ) anyerror!DocumentArtifactChildRangeOutboxDrainResult {
            if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            var replication_mutation = self.acquireReplicationMutationShared();
            defer if (replication_mutation) |*lease| lease.release();
            try self.enforceReplicationWriteGate();

            const prefix = try internal_keys.documentChildRangeOutboxRootPrefixAlloc(self.alloc);
            defer self.alloc.free(prefix);

            try self.lockApplyForPortableRuntime();
            var apply_mutex_held = true;
            errdefer if (apply_mutex_held) self.core.unlockApply();
            const scanned = try self.core.scanStorePrefix(self.alloc, prefix);
            self.core.unlockApply();
            apply_mutex_held = false;
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            const Removal = struct {
                fn remove(ptr: *anyopaque, key: []const u8) !void {
                    const owner: @TypeOf(self) = @ptrCast(@alignCast(ptr));
                    return owner.deleteDocumentArtifactChildRangeOutboxEntry(key);
                }
            };
            return document_child_range_outbox.drain(self.alloc, scanned, dispatcher, limit, self, Removal.remove);
        }

        pub fn encodeDenseArtifactCounterBootstrap(value: DenseArtifactCounterBootstrap, out: *[dense_artifact_counter_bootstrap_encoded_len]u8) void {
            @memcpy(out[0..dense_artifact_counter_bootstrap_magic.len], dense_artifact_counter_bootstrap_magic);
            var offset = dense_artifact_counter_bootstrap_magic.len;
            std.mem.writeInt(u128, out[offset..][0..@sizeOf(u128)], value.repair_id, .little);
            offset += @sizeOf(u128);
            std.mem.writeInt(u128, out[offset..][0..@sizeOf(u128)], value.attempt_id, .little);
            offset += @sizeOf(u128);
            std.mem.writeInt(i64, out[offset..][0..@sizeOf(i64)], value.delta, .little);
        }

        pub fn graphMutationIdentityEql(left: anytype, right: anytype) bool {
            return std.mem.eql(u8, left.index_name, right.index_name) and
                std.mem.eql(u8, left.source, right.source) and
                std.mem.eql(u8, left.target, right.target) and
                std.mem.eql(u8, left.edge_type, right.edge_type) and
                std.mem.eql(u8, left.edge_id, if (@hasField(@TypeOf(right), "edge_id")) right.edge_id else "") and
                std.mem.eql(u8, left.owner_document, if (@hasField(@TypeOf(right), "owner_document")) right.owner_document else "");
        }

        pub fn hasConfiguredResolvers(self: anytype) bool {
            return self.core.index_manager.resolvers.items.len > 0;
        }

        pub fn loadDenseArtifactCounterBootstrap(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
        ) !?DenseArtifactCounterBootstrap {
            const key = try denseArtifactCounterBootstrapKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            const raw = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            return try decodeDenseArtifactCounterBootstrap(raw);
        }

        pub fn loadDenseArtifactTargetCounter(alloc: Allocator, store: *docstore_mod.DocStore, index_name: []const u8) !?u64 {
            const key = try denseArtifactTargetCounterKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            const raw = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            if (raw.len != 8) return error.InvalidDenseArtifactTargetCounter;
            return std.mem.readInt(u64, raw[0..8], .little);
        }

        pub fn accountDenseCoverage(
            ctx: *const AsyncContext,
            index_name: []const u8,
            batch: derived_types.DerivedBatch,
            writes: []const mapper.DenseEmbeddingWrite,
        ) !void {
            const external = ctx.index_manager.denseIndexUsesExternalCoverage(index_name);
            const direct = ctx.index_manager.denseIndexUsesManagedDirectField(index_name);
            var deleted_artifacts = std.StringHashMapUnmanaged(void).empty;
            defer deleted_artifacts.deinit(ctx.alloc);
            for (batch.deleted_keys) |key| try deleted_artifacts.put(ctx.alloc, key, {});
            var produced = std.StringHashMapUnmanaged(void).empty;
            defer produced.deinit(ctx.alloc);
            for (writes) |write| {
                // Replay represents an artifact deletion as an empty lazy write for
                // the same key so the projection can remove its prior member. That is
                // negative evidence, not a produced source outcome. Positive cached
                // artifact loads use the same empty-vector representation but never
                // carry their artifact key in the batch deletion set.
                if (write.artifact_key) |artifact_key| {
                    if (deleted_artifacts.contains(artifact_key)) continue;
                } else if (write.vector.len == 0) continue;
                try produced.put(ctx.alloc, write.parent_doc_key orelse write.doc_key, {});
            }
            var outcomes = std.ArrayListUnmanaged(DerivedCoverageDocOutcome).empty;
            defer outcomes.deinit(ctx.alloc);
            if (!external and direct) {
                for (batch.documents) |doc| {
                    if (doc.action == .delete or internal_keys.isInternalUserKey(doc.key) or !ctx.index_manager.byte_range.contains(doc.key)) continue;
                    const was_produced = produced.contains(doc.key);
                    try outcomes.append(ctx.alloc, .{
                        .doc_key = doc.key,
                        .outcome = if (was_produced) .produced else .skipped,
                    });
                }
                try appendDirectVectorArtifactOutcomes(ctx, index_name, writes, &deleted_artifacts, &produced, &outcomes);
            } else {
                // Generated indexes have a distinct producer lifecycle. A replay
                // window can contain source documents before, or independently from,
                // their generated artifacts; absence from that window is therefore
                // pending work, never terminal skip evidence. The enrichment producer
                // owns all generated produced/skipped/failed transitions. Replay may
                // repeat positive produced evidence, which is idempotent and also
                // covers external indexes that have no managed producer.
                var iter = produced.keyIterator();
                while (iter.next()) |doc_key| {
                    if (internal_keys.isInternalUserKey(doc_key.*) or !ctx.index_manager.byte_range.contains(doc_key.*)) continue;
                    try outcomes.append(ctx.alloc, .{ .doc_key = doc_key.*, .outcome = .produced });
                }
            }
            try setDerivedCoverageOutcomes(ctx.alloc, ctx.store, ctx.index_manager, index_name, outcomes.items);
        }

        pub fn accountSparseCoverage(
            ctx: *const AsyncContext,
            index_name: []const u8,
            batch: derived_types.DerivedBatch,
            writes: []const mapper.SparseEmbeddingWrite,
        ) !void {
            const external = ctx.index_manager.sparseIndexUsesExternalCoverage(index_name);
            const direct = ctx.index_manager.sparseIndexUsesManagedDirectField(index_name);
            var deleted_artifacts = std.StringHashMapUnmanaged(void).empty;
            defer deleted_artifacts.deinit(ctx.alloc);
            for (batch.deleted_keys) |key| try deleted_artifacts.put(ctx.alloc, key, {});
            var produced = std.StringHashMapUnmanaged(void).empty;
            defer produced.deinit(ctx.alloc);
            for (writes) |write| {
                if (write.artifact_key) |artifact_key| {
                    if (deleted_artifacts.contains(artifact_key)) continue;
                } else if (write.indices.len == 0) continue;
                try produced.put(ctx.alloc, write.doc_key, {});
            }
            var outcomes = std.ArrayListUnmanaged(DerivedCoverageDocOutcome).empty;
            defer outcomes.deinit(ctx.alloc);
            if (!external and direct) {
                for (batch.documents) |doc| {
                    if (doc.action == .delete or internal_keys.isInternalUserKey(doc.key) or !ctx.index_manager.byte_range.contains(doc.key)) continue;
                    const was_produced = produced.contains(doc.key);
                    try outcomes.append(ctx.alloc, .{
                        .doc_key = doc.key,
                        .outcome = if (was_produced) .produced else .skipped,
                    });
                }
                try appendDirectVectorArtifactOutcomes(ctx, index_name, writes, &deleted_artifacts, &produced, &outcomes);
            } else {
                var iter = produced.keyIterator();
                while (iter.next()) |doc_key| {
                    if (internal_keys.isInternalUserKey(doc_key.*) or !ctx.index_manager.byte_range.contains(doc_key.*)) continue;
                    try outcomes.append(ctx.alloc, .{ .doc_key = doc_key.*, .outcome = .produced });
                }
            }
            try setDerivedCoverageOutcomes(ctx.alloc, ctx.store, ctx.index_manager, index_name, outcomes.items);
        }

        pub fn acquireSnapshotReplayAsyncContext(ctx: *const AsyncContext) !?snapshot_admission_mod.SnapshotAdmission.MutationLease {
            const admission = ctx.snapshot_replay_admission orelse return null;
            if (ctx.io) |io| {
                return try admission.acquireMutationIo(io, @as(?types.CancellationToken, null));
            }
            return admission.acquireMutation();
        }

        pub fn addHbcWriteProfileDelta(total: *BatchProfile, before: hbc_mod.WriteProfile, after: hbc_mod.WriteProfile) void {
            total.hbc_insert_calls += profileDelta(after.insert_calls, before.insert_calls);
            total.hbc_batch_route_calls += profileDelta(after.batch_route_calls, before.batch_route_calls);
            total.hbc_batch_route_internal_nodes += profileDelta(after.batch_route_internal_nodes, before.batch_route_internal_nodes);
            total.hbc_batch_route_leaf_groups += profileDelta(after.batch_route_leaf_groups, before.batch_route_leaf_groups);
            total.hbc_batch_route_items += profileDelta(after.batch_route_items, before.batch_route_items);
            total.hbc_batch_route_quantized_nodes += profileDelta(after.batch_route_quantized_nodes, before.batch_route_quantized_nodes);
            total.hbc_batch_route_exact_child_scores += profileDelta(after.batch_route_exact_child_scores, before.batch_route_exact_child_scores);
            total.hbc_batch_route_fallback_nodes += profileDelta(after.batch_route_fallback_nodes, before.batch_route_fallback_nodes);
            total.hbc_grouped_items += profileDelta(after.grouped_items, before.grouped_items);
            total.hbc_grouped_fallback_items += profileDelta(after.grouped_fallback_items, before.grouped_fallback_items);
            total.hbc_grouped_leaf_groups += profileDelta(after.grouped_leaf_groups, before.grouped_leaf_groups);
            total.hbc_grouped_split_candidates += profileDelta(after.grouped_split_candidates, before.grouped_split_candidates);
            total.hbc_grouped_recursive_splits += profileDelta(after.grouped_recursive_splits, before.grouped_recursive_splits);
            total.hbc_grouped_split_scan_iterations += profileDelta(after.grouped_split_scan_iterations, before.grouped_split_scan_iterations);
            total.hbc_grouped_split_queue_peak_total += profileDelta(after.grouped_split_queue_peak_total, before.grouped_split_queue_peak_total);
            total.hbc_grouped_leaf_range_writes += profileDelta(after.grouped_leaf_range_writes, before.grouped_leaf_range_writes);
            total.hbc_grouped_ancestor_range_refreshes += profileDelta(after.grouped_ancestor_range_refreshes, before.grouped_ancestor_range_refreshes);
            total.hbc_grouped_ancestor_range_nodes += profileDelta(after.grouped_ancestor_range_nodes, before.grouped_ancestor_range_nodes);
            total.hbc_grouped_node_body_writes += profileDelta(after.grouped_node_body_writes, before.grouped_node_body_writes);
            total.hbc_grouped_vec_leaf_writes += profileDelta(after.grouped_vec_leaf_writes, before.grouped_vec_leaf_writes);
            total.hbc_split_leaf_input_members_total += profileDelta(after.split_leaf_input_members_total, before.split_leaf_input_members_total);
            total.hbc_split_leaf_input_overflow_members_total += profileDelta(after.split_leaf_input_overflow_members_total, before.split_leaf_input_overflow_members_total);
            total.hbc_save_node_calls += profileDelta(after.save_node_calls, before.save_node_calls);
            total.hbc_split_leaf_calls += profileDelta(after.split_leaf_calls, before.split_leaf_calls);
            total.hbc_split_internal_calls += profileDelta(after.split_internal_calls, before.split_internal_calls);
            total.hbc_range_put_calls += profileDelta(after.range_put_calls, before.range_put_calls);
            total.hbc_range_delete_calls += profileDelta(after.range_delete_calls, before.range_delete_calls);
            total.hbc_nodes_put_calls += profileDelta(after.ns_nodes_put_calls, before.ns_nodes_put_calls);
            total.hbc_nodes_append_calls += profileDelta(after.ns_nodes_append_calls, before.ns_nodes_append_calls);
            total.hbc_nodes_delete_calls += profileDelta(after.ns_nodes_delete_calls, before.ns_nodes_delete_calls);
            total.hbc_meta_put_calls += profileDelta(after.ns_meta_put_calls, before.ns_meta_put_calls);
            total.hbc_meta_append_calls += profileDelta(after.ns_meta_append_calls, before.ns_meta_append_calls);
            total.hbc_meta_delete_calls += profileDelta(after.ns_meta_delete_calls, before.ns_meta_delete_calls);
            total.hbc_quant_put_calls += profileDelta(after.ns_quant_put_calls, before.ns_quant_put_calls);
            total.hbc_quant_append_calls += profileDelta(after.ns_quant_append_calls, before.ns_quant_append_calls);
            total.hbc_quant_delete_calls += profileDelta(after.ns_quant_delete_calls, before.ns_quant_delete_calls);
            total.hbc_vecs_put_calls += profileDelta(after.ns_vecs_put_calls, before.ns_vecs_put_calls);
            total.hbc_vecs_append_calls += profileDelta(after.ns_vecs_append_calls, before.ns_vecs_append_calls);
            total.hbc_vecs_delete_calls += profileDelta(after.ns_vecs_delete_calls, before.ns_vecs_delete_calls);
            total.hbc_insert_transform_ns += profileDelta(after.insert_transform_ns, before.insert_transform_ns);
            total.hbc_insert_store_vector_ns += profileDelta(after.insert_store_vector_ns, before.insert_store_vector_ns);
            total.hbc_insert_find_leaf_ns += profileDelta(after.insert_find_leaf_ns, before.insert_find_leaf_ns);
            total.hbc_insert_mutate_leaf_ns += profileDelta(after.insert_mutate_leaf_ns, before.insert_mutate_leaf_ns);
            total.hbc_insert_flush_metadata_ns += profileDelta(after.insert_flush_metadata_ns, before.insert_flush_metadata_ns);
            total.hbc_insert_commit_ns += profileDelta(after.insert_commit_ns, before.insert_commit_ns);
            total.hbc_save_node_ns += profileDelta(after.save_node_ns, before.save_node_ns);
            total.hbc_save_split_range_ns += profileDelta(after.save_split_range_ns, before.save_split_range_ns);
            total.hbc_update_parent_ns += profileDelta(after.update_parent_ns, before.update_parent_ns);
            total.hbc_split_leaf_ns += profileDelta(after.split_leaf_ns, before.split_leaf_ns);
            total.hbc_split_leaf_vector_load_ns += profileDelta(after.split_leaf_vector_load_ns, before.split_leaf_vector_load_ns);
            total.hbc_split_leaf_partition_ns += profileDelta(after.split_leaf_partition_ns, before.split_leaf_partition_ns);
            total.hbc_split_leaf_finalize_ns += profileDelta(after.split_leaf_finalize_ns, before.split_leaf_finalize_ns);
            total.hbc_split_internal_ns += profileDelta(after.split_internal_ns, before.split_internal_ns);
            total.hbc_refresh_quantized_ns += profileDelta(after.refresh_quantized_ns, before.refresh_quantized_ns);
            total.hbc_quantized_vector_load_ns += profileDelta(after.quantized_vector_load_ns, before.quantized_vector_load_ns);
            total.hbc_quantized_compute_ns += profileDelta(after.quantized_compute_ns, before.quantized_compute_ns);
            total.hbc_quantized_store_ns += profileDelta(after.quantized_store_ns, before.quantized_store_ns);
            total.hbc_quantized_encode_ns += profileDelta(after.quantized_encode_ns, before.quantized_encode_ns);
            total.hbc_quantized_put_ns += profileDelta(after.quantized_put_ns, before.quantized_put_ns);
            total.hbc_bulk_build_store_ns += profileDelta(after.bulk_build_store_ns, before.bulk_build_store_ns);
            total.hbc_bulk_build_tree_ns += profileDelta(after.bulk_build_tree_ns, before.bulk_build_tree_ns);
            total.hbc_posting_maintenance_scanned_nodes += profileDelta(after.posting_maintenance_scanned_nodes, before.posting_maintenance_scanned_nodes);
            total.hbc_posting_maintenance_scanned_postings += profileDelta(after.posting_maintenance_scanned_postings, before.posting_maintenance_scanned_postings);
            total.hbc_posting_maintenance_dirty_postings += profileDelta(after.posting_maintenance_dirty_postings, before.posting_maintenance_dirty_postings);
            total.hbc_posting_maintenance_repaired_postings += profileDelta(after.posting_maintenance_repaired_postings, before.posting_maintenance_repaired_postings);
            total.hbc_posting_maintenance_centroid_refreshed += profileDelta(after.posting_maintenance_centroid_refreshed, before.posting_maintenance_centroid_refreshed);
            total.hbc_posting_maintenance_payload_refreshed += profileDelta(after.posting_maintenance_payload_refreshed, before.posting_maintenance_payload_refreshed);
            total.hbc_posting_maintenance_ancestor_refresh_roots += profileDelta(after.posting_maintenance_ancestor_refresh_roots, before.posting_maintenance_ancestor_refresh_roots);
            total.hbc_posting_maintenance_split_postings += profileDelta(after.posting_maintenance_split_postings, before.posting_maintenance_split_postings);
            total.hbc_posting_maintenance_merged_postings += profileDelta(after.posting_maintenance_merged_postings, before.posting_maintenance_merged_postings);
            total.hbc_posting_maintenance_boundary_reassigned_vectors += profileDelta(after.posting_maintenance_boundary_reassigned_vectors, before.posting_maintenance_boundary_reassigned_vectors);
            total.hbc_posting_lazy_centroid_deferrals += profileDelta(after.posting_lazy_centroid_deferrals, before.posting_lazy_centroid_deferrals);
            total.hbc_posting_lazy_payload_deferrals += profileDelta(after.posting_lazy_payload_deferrals, before.posting_lazy_payload_deferrals);
            total.hbc_posting_lazy_ancestor_deferrals += profileDelta(after.posting_lazy_ancestor_deferrals, before.posting_lazy_ancestor_deferrals);
        }

        pub fn addPrecomputeAssetProducerBytes(lhs: usize, rhs: usize) usize {
            return std.math.add(usize, lhs, rhs) catch std.math.maxInt(usize);
        }

        pub fn appendArtifactRepairSummaryDirtyForStore(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
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
            try appendArtifactRepairSummaryRebuildInvalidationForStore(alloc, store, writes, deletes, owned_delete_keys);
        }

        pub fn appendArtifactRepairSummaryRebuildInvalidationForStore(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
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
            try appendKeysForPrefixDeleteInStore(alloc, store, deletes, owned_keys, rebuild_prefix);
        }

        pub fn appendArtifactSourceRevisionWritesFromReplay(
            alloc: Allocator,
            payload: []const u8,
            sequence: u64,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            var decoded = try change_journal_mod.decodeBinaryRecordBorrowed(alloc, payload);
            defer decoded.deinit();
            if (decoded.record.sequence != sequence) return error.InvalidDerivedSequence;

            var artifact_names = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (artifact_names.items) |name| alloc.free(name);
                artifact_names.deinit(alloc);
            }
            for (decoded.record.changed_artifact_keys) |artifact_key| {
                if (try internal_keys.artifactNameView(artifact_key)) |artifact_name| {
                    try appendUniqueOwnedKey(alloc, &artifact_names, artifact_name);
                    continue;
                }
                var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse continue;
                defer artifact_ref.deinit(alloc);
                try appendUniqueOwnedKey(alloc, &artifact_names, artifact_ref.name);
            }
            std.mem.sort([]u8, artifact_names.items, {}, struct {
                fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.lessThan);

            for (artifact_names.items) |artifact_name| {
                const key = try internal_keys.artifactSourceRevisionKeyAlloc(alloc, artifact_name);
                var key_transferred = false;
                errdefer if (!key_transferred) alloc.free(key);
                const value = try alloc.alloc(u8, @sizeOf(u64));
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value);
                std.mem.writeInt(u64, value[0..8], sequence, .big);
                try owned_keys.append(alloc, key);
                key_transferred = true;
                try owned_values.append(alloc, value);
                value_transferred = true;
                // Preserve the key order required by the bulk-ingest append fast path.
                // Replay metadata sorts between user rows and identity metadata.
                var insert_at: usize = 0;
                while (insert_at < writes.items.len and std.mem.order(u8, writes.items[insert_at].key, key) != .gt) : (insert_at += 1) {}
                try writes.insert(alloc, insert_at, .{ .key = key, .value = value });
            }
        }

        pub fn appendAssetArtifactSourceIndexDelete(
            alloc: Allocator,
            artifact_key: []const u8,
            store_writes: []const docstore_mod.KVPair,
            delete_keys: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
        ) !void {
            const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(alloc, artifact_key)) orelse return;
            defer {
                alloc.free(parsed.doc_key);
                alloc.free(parsed.artifact_name);
            }

            const marker_key = try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, parsed.artifact_name, parsed.doc_key);
            var marker_key_owned = false;
            defer if (!marker_key_owned) alloc.free(marker_key);
            if (containsStoreWriteKey(store_writes, marker_key)) return;
            if (containsOwnedKey(owned_delete_keys.items, marker_key)) return;

            try owned_delete_keys.append(alloc, marker_key);
            marker_key_owned = true;
            try delete_keys.append(alloc, marker_key);
        }

        pub fn appendAssetArtifactSourceIndexMutations(
            alloc: Allocator,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            deleted_artifact_keys: []const []const u8,
            delete_keys: *std.ArrayListUnmanaged([]const u8),
            owned_store_keys: *std.ArrayListUnmanaged([]u8),
            owned_store_values: *std.ArrayListUnmanaged([]u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
        ) !void {
            const original_write_count = store_writes.items.len;
            var write_index: usize = 0;
            while (write_index < original_write_count) : (write_index += 1) {
                const write = store_writes.items[write_index];
                try appendAssetArtifactSourceIndexWrite(alloc, write.key, store_writes, owned_store_keys, owned_store_values);
            }

            const original_delete_count = delete_keys.items.len;
            var delete_index: usize = 0;
            while (delete_index < original_delete_count) : (delete_index += 1) {
                const key = delete_keys.items[delete_index];
                try appendAssetArtifactSourceIndexDelete(alloc, key, store_writes.items, delete_keys, owned_delete_keys);
            }
            for (deleted_artifact_keys) |key| {
                try appendAssetArtifactSourceIndexDelete(alloc, key, store_writes.items, delete_keys, owned_delete_keys);
            }
        }

        pub fn appendAssetArtifactSourceIndexWrite(
            alloc: Allocator,
            artifact_key: []const u8,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_store_keys: *std.ArrayListUnmanaged([]u8),
            owned_store_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(alloc, artifact_key)) orelse return;
            defer {
                alloc.free(parsed.doc_key);
                alloc.free(parsed.artifact_name);
            }

            const marker_key = try internal_keys.assetArtifactSourceIndexKeyAlloc(alloc, parsed.artifact_name, parsed.doc_key);
            var marker_key_owned = false;
            defer if (!marker_key_owned) alloc.free(marker_key);
            if (containsStoreWriteKey(store_writes.items, marker_key)) return;

            const marker_value = try alloc.dupe(u8, artifact_key);
            var marker_value_owned = false;
            defer if (!marker_value_owned) alloc.free(marker_value);

            try owned_store_keys.append(alloc, marker_key);
            marker_key_owned = true;
            try owned_store_values.append(alloc, marker_value);
            marker_value_owned = true;
            try store_writes.append(alloc, .{
                .key = marker_key,
                .value = marker_value,
            });
        }

        pub fn appendChunkArtifactWrites(
            alloc: Allocator,
            doc_key: []const u8,
            source_field: []const u8,
            artifact_name: []const u8,
            chunks: []const chunker_mod.Chunk,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            include_payload: bool,
        ) !void {
            var arena_state = std.heap.ArenaAllocator.init(alloc);
            defer arena_state.deinit();
            const scratch = arena_state.allocator();

            for (chunks) |chunk| {
                const key = try internal_keys.chunkArtifactKeyAlloc(alloc, doc_key, artifact_name, @intCast(chunk.chunk_id));
                defer alloc.free(key);
                const payload = try buildChunkArtifactPayloadAlloc(scratch, doc_key, artifact_name, source_field, chunk, include_payload);

                try result_collectors.appendArtifact(alloc, artifact_writes, key, payload);

                _ = arena_state.reset(.retain_capacity);
            }
        }

        pub fn appendChunkToPendingDenseChunkEmbedding(
            alloc: Allocator,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            dense_embedder: embedder_mod.DenseEmbedder,
            doc_key: []const u8,
            chunk_key: []const u8,
            chunk_text: []const u8,
            pending: *PendingDocumentUnitDenseChunkEmbedding,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
        ) !void {
            const max_batch_items = generatedEmbedBatchItems();
            const max_batch_bytes = generatedEmbedBatchBytes();
            if (pending.chunk_texts.items.len > 0 and
                (pending.chunk_texts.items.len >= max_batch_items or pending.batch_source_bytes + chunk_text.len > max_batch_bytes))
            {
                try flushPendingDenseChunkEmbedding(alloc, runtime, dense_embedder, doc_key, pending, artifact_writes, dense_embeddings);
            }
            try appendPendingDocumentUnitChunkSource(alloc, pending, chunk_key, chunk_text);
            if (pending.chunk_texts.items.len >= max_batch_items or pending.batch_source_bytes >= max_batch_bytes) {
                try flushPendingDenseChunkEmbedding(alloc, runtime, dense_embedder, doc_key, pending, artifact_writes, dense_embeddings);
            }
        }

        pub fn appendChunkToPendingSparseChunkEmbedding(
            alloc: Allocator,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            sparse_embedder: embedder_mod.SparseEmbedder,
            chunk_key: []const u8,
            chunk_text: []const u8,
            pending: *PendingDocumentUnitSparseChunkEmbedding,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
        ) !void {
            const max_batch_items = generatedEmbedBatchItems();
            const max_batch_bytes = generatedEmbedBatchBytes();
            if (pending.chunk_texts.items.len > 0 and
                (pending.chunk_texts.items.len >= max_batch_items or pending.batch_source_bytes + chunk_text.len > max_batch_bytes))
            {
                try flushPendingSparseChunkEmbedding(alloc, runtime, sparse_embedder, pending, artifact_writes, sparse_embeddings);
            }
            try appendPendingDocumentUnitChunkSource(alloc, pending, chunk_key, chunk_text);
            if (pending.chunk_texts.items.len >= max_batch_items or pending.batch_source_bytes >= max_batch_bytes) {
                try flushPendingSparseChunkEmbedding(alloc, runtime, sparse_embedder, pending, artifact_writes, sparse_embeddings);
            }
        }

        pub fn appendDerivedDenseEmbeddingForConsumers(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
            doc_key: []const u8,
            parent_doc_key: ?[]const u8,
            artifact_key: []const u8,
            vector: []const f32,
            consumer_indexes: []const []const u8,
        ) !void {
            _ = vector;
            for (consumer_indexes) |index_name| {
                try out.ensureUnusedCapacity(alloc, 1);
                const owned_name = try alloc.dupe(u8, index_name);
                errdefer alloc.free(owned_name);
                const owned_doc = try alloc.dupe(u8, doc_key);
                errdefer alloc.free(owned_doc);
                const owned_artifact = try alloc.dupe(u8, artifact_key);
                errdefer alloc.free(owned_artifact);
                const owned_parent = if (parent_doc_key) |key| try alloc.dupe(u8, key) else null;
                out.appendAssumeCapacity(.{ .index_name = owned_name, .parent_doc_key = owned_parent, .doc_key = owned_doc, .artifact_key = owned_artifact, .vector = &.{} });
            }
        }

        pub fn appendDerivedSparseEmbeddingForConsumers(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
            doc_key: []const u8,
            artifact_key: []const u8,
            indices: []const u32,
            values: []const f32,
            consumer_indexes: []const []const u8,
        ) !void {
            _ = indices;
            _ = values;
            for (consumer_indexes) |index_name| {
                try out.ensureUnusedCapacity(alloc, 1);
                const owned_name = try alloc.dupe(u8, index_name);
                errdefer alloc.free(owned_name);
                const owned_doc = try alloc.dupe(u8, doc_key);
                errdefer alloc.free(owned_doc);
                const owned_artifact = try alloc.dupe(u8, artifact_key);
                errdefer alloc.free(owned_artifact);
                out.appendAssumeCapacity(.{ .index_name = owned_name, .doc_key = owned_doc, .artifact_key = owned_artifact, .indices = &.{}, .values = &.{} });
            }
        }

        pub fn appendDerivedTargetRefAlloc(
            alloc: Allocator,
            targets: *std.ArrayListUnmanaged(derived_types.DerivedTargetRef),
            kind: derived_types.DerivedTarget,
            index_name: []const u8,
        ) !void {
            const owned_index_name = try alloc.dupe(u8, index_name);
            errdefer alloc.free(owned_index_name);
            try targets.append(alloc, .{
                .kind = kind,
                .index_name = owned_index_name,
            });
        }

        pub fn appendDirectGraphTtlDueWrite(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            write: types.BatchWrite,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            if (!internal_keys.isGraphEdgeArtifactKey(write.key)) return;
            const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, write.key)) orelse return error.InvalidGraphEdgeArtifact;
            defer {
                alloc.free(parsed.doc_key);
                alloc.free(parsed.index_name);
                alloc.free(parsed.edge_type);
                alloc.free(parsed.target_doc_key);
                alloc.free(parsed.edge_id);
                if (parsed.source_node) |source| alloc.free(source);
            }
            const entry = index_manager.graphIndex(parsed.index_name) orelse return;
            if (entry.ttl_duration_ns == 0 or index_manager.graphArtifactSources(parsed.index_name).len != 0) return;
            var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, write.value);
            defer edge.deinit(alloc);
            if (edge.ttl_created_ns == 0 or edge.generation != entry.config.coverage_generation)
                return error.GraphEdgeTtlMigrationRequired;
            const deadline_ns = std.math.add(u64, edge.ttl_created_ns, entry.ttl_duration_ns) catch std.math.maxInt(u64);
            var digest: [32]u8 = undefined;
            GraphTtlSha256.hash(write.value, &digest, .{});
            const due_key = try graph_edge_ttl_expiration.directIndexKeyAlloc(alloc, deadline_ns, write.key);
            var due_key_unowned = true;
            errdefer if (due_key_unowned) alloc.free(due_key);
            const due_value = try graph_edge_ttl_expiration.encodeDirectAlloc(alloc, .{
                .index_name = parsed.index_name,
                .generation = entry.config.coverage_generation,
                .artifact_key = write.key,
                .deadline_ns = deadline_ns,
                .artifact_digest = digest,
            });
            var due_value_unowned = true;
            errdefer if (due_value_unowned) alloc.free(due_value);
            try owned_keys.append(alloc, due_key);
            due_key_unowned = false;
            try owned_values.append(alloc, due_value);
            due_value_unowned = false;
            try store_writes.append(alloc, .{ .key = due_key, .value = due_value });
        }

        pub fn appendDirectVectorArtifactOutcomes(ctx: *const AsyncContext, index_name: []const u8, writes: anytype, deleted: *const std.StringHashMapUnmanaged(void), produced: *const std.StringHashMapUnmanaged(void), outcomes: *std.ArrayListUnmanaged(DerivedCoverageDocOutcome)) !void {
            for (writes) |write| {
                const artifact_key = write.artifact_key orelse continue;
                const owner = if (@hasField(@TypeOf(write), "parent_doc_key")) write.parent_doc_key orelse write.doc_key else write.doc_key;
                if (internal_keys.isInternalUserKey(owner) or !ctx.index_manager.byte_range.contains(owner)) continue;
                if (!try replaySourceDocumentExists(ctx, owner)) {
                    try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_name, &.{owner});
                    continue;
                }
                if (produced.contains(owner)) {
                    try outcomes.append(ctx.alloc, .{ .doc_key = owner, .outcome = .produced });
                } else if (deleted.contains(artifact_key)) {
                    try outcomes.append(ctx.alloc, .{ .doc_key = owner, .outcome = .skipped });
                }
            }
        }

        pub fn appendDocumentExtractionDeleteKeys(
            alloc: Allocator,
            db: anytype,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            artifact_name: []const u8,
            manifest_key: []const u8,
            artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
        ) !void {
            try appendOwnedKey(alloc, artifact_delete_keys, manifest_key);
            const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_name);
            errdefer alloc.free(state_key);
            const existing_state = db.core.getStoreValue(alloc, state_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (existing_state) |value| alloc.free(value);
            if (existing_state != null) {
                var previous_state = try loadDocumentExtractionPreviousState(alloc, db, view, doc_key, artifact_name, existing_state);
                defer previous_state.deinit(alloc);
                for (previous_state.unit_keys) |previous_key| {
                    try appendOwnedKey(alloc, artifact_delete_keys, previous_key);
                }
                for (previous_state.chunk_keys) |previous_key| {
                    try appendOwnedKey(alloc, artifact_delete_keys, previous_key);
                }
                var block_index: u32 = 0;
                while (block_index < previous_state.navigation_block_count) : (block_index += 1) {
                    try artifact_delete_keys.ensureUnusedCapacity(alloc, 1);
                    artifact_delete_keys.appendAssumeCapacity(try internal_keys.documentUnitNavigationBlockKeyAlloc(alloc, doc_key, artifact_name, block_index));
                }
            }
            try artifact_delete_keys.ensureUnusedCapacity(alloc, 1);
            artifact_delete_keys.appendAssumeCapacity(try internal_keys.documentUnitNavigationSummaryKeyAlloc(alloc, doc_key, artifact_name));
            try artifact_delete_keys.append(alloc, state_key);
        }

        pub fn appendDocumentExtractionFailureManifest(
            alloc: Allocator,
            db: anytype,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            artifact_name: []const u8,
            source_url: []const u8,
            manifest_key: []const u8,
            existing_state: ?[]const u8,
            previous_child_ranges: []const types.DocumentArtifactChildRange,
            from_generation: u64,
            to_generation: u64,
            error_code: []const u8,
            error_message: []const u8,
            error_stage: []const u8,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
        ) !void {
            var previous_state = DocumentExtractionPreviousState{};
            defer previous_state.deinit(alloc);
            if (existing_state != null) {
                previous_state = try loadDocumentExtractionPreviousState(alloc, db, view, doc_key, artifact_name, existing_state);
            }

            const manifest = try documentExtractionFailureManifestPayloadAlloc(
                alloc,
                doc_key,
                artifact_name,
                source_url,
                previous_state.unit_keys,
                previous_state.chunk_keys,
                previous_child_ranges,
                from_generation,
                to_generation,
                error_code,
                error_message,
                error_stage,
            );
            defer alloc.free(manifest);
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, manifest_key),
                .value = try alloc.dupe(u8, manifest),
            });
            // Preserve the last known-good state and child artifacts. The failure
            // manifest makes the source stale and repairable without creating a search
            // outage or losing the prior key set needed for a correct future diff.
        }

        pub fn appendDocumentExtractionKeyRanges(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(u8),
            first_range: *bool,
            range_index: *usize,
            range_kind: []const u8,
            artifact_name: []const u8,
            keys: []const []const u8,
            units: []const document_extraction_mod.Unit,
            previous_child_ranges: []const types.DocumentArtifactChildRange,
        ) !void {
            var start: usize = 0;
            while (start < keys.len) {
                const end = documentExtractionRangeEnd(keys.len, units, start);
                if (first_range.*) {
                    first_range.* = false;
                } else {
                    try out.append(alloc, ',');
                }
                var first = true;
                try out.append(alloc, '{');
                const range_id = try std.fmt.allocPrint(alloc, "range:{d:0>6}", .{range_index.*});
                defer alloc.free(range_id);
                const previous_range = findDocumentArtifactChildRange(previous_child_ranges, range_id, range_kind, artifact_name);
                try appendJsonFieldString(alloc, out, &first, "range_id", range_id);
                try appendJsonFieldString(alloc, out, &first, "range_kind", range_kind);
                try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
                try appendJsonFieldString(alloc, out, &first, "split_boundary", documentExtractionSplitBoundary(range_kind));
                try appendJsonFieldString(alloc, out, &first, "placement", if (previous_range) |range| range.placement else "parent");
                try appendJsonFieldU64(alloc, out, &first, "owner_group_id", if (previous_range) |range| range.owner_group_id orelse 0 else 0);
                try appendJsonFieldU64(alloc, out, &first, "placement_generation", if (previous_range) |range| range.placement_generation orelse 0 else 0);
                try appendJsonFieldString(alloc, out, &first, "route_status", if (previous_range) |range| range.route_status orelse "local_committed" else "local_committed");
                try appendJsonFieldBool(alloc, out, &first, "split_eligible", if (previous_range) |range| range.split_eligible orelse (end - start > 1) else end - start > 1);
                try appendJsonFieldString(alloc, out, &first, "start_key", keys[start]);
                try appendJsonFieldString(alloc, out, &first, "end_key_exclusive", if (end < keys.len) keys[end] else "");
                try appendJsonFieldString(alloc, out, &first, "last_key", keys[end - 1]);
                try appendJsonFieldUsize(alloc, out, &first, "child_count", end - start);
                if (units.len >= end) {
                    var text_bytes: usize = 0;
                    for (units[start..end]) |unit| text_bytes += unit.text.len;
                    try appendJsonFieldUsize(alloc, out, &first, "text_bytes", text_bytes);
                }
                try out.append(alloc, '}');
                range_index.* += 1;
                start = end;
            }
        }

        pub fn appendDocumentExtractionMergeOperation(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(u8),
            first_operation: *bool,
            op: []const u8,
            range_kind: []const u8,
            artifact_name: []const u8,
            keys: []const []const u8,
            exclude_keys: []const []const u8,
        ) !void {
            const count = countKeysNotIn(keys, exclude_keys);
            if (count == 0) return;

            var first_key: ?[]const u8 = null;
            var last_key: ?[]const u8 = null;
            for (keys) |key| {
                if (containsDeleteKey(exclude_keys, key)) continue;
                if (first_key == null) first_key = key;
                last_key = key;
            }

            if (first_operation.*) {
                first_operation.* = false;
            } else {
                try out.append(alloc, ',');
            }

            var first = true;
            try out.append(alloc, '{');
            try appendJsonFieldString(alloc, out, &first, "op", op);
            try appendJsonFieldString(alloc, out, &first, "range_kind", range_kind);
            try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
            try appendJsonFieldString(alloc, out, &first, "first_key", first_key.?);
            try appendJsonFieldString(alloc, out, &first, "last_key", last_key.?);
            try appendJsonFieldUsize(alloc, out, &first, "key_count", count);
            try out.append(alloc, '}');
        }

        pub fn appendDocumentExtractionNavigationBackfill(
            alloc: Allocator,
            db: anytype,
            doc_key: []const u8,
            artifact_name: []const u8,
            source_fingerprint: []const u8,
            state: []const u8,
            generation: u64,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
        ) !void {
            const summary_key = try internal_keys.documentUnitNavigationSummaryKeyAlloc(alloc, doc_key, artifact_name);
            defer alloc.free(summary_key);
            const existing_summary = try db.core.getStoreValue(alloc, summary_key);
            defer if (existing_summary) |value| alloc.free(value);
            if (existing_summary) |summary| {
                if (try hierarchy_navigation.indexMetadataMatches(alloc, state, summary, generation)) return;
            }

            const unit_keys = try documentExtractionStateUnitKeysAlloc(alloc, state);
            defer freeOwnedConstKeySlice(alloc, unit_keys);
            const chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, state);
            defer freeOwnedConstKeySlice(alloc, chunk_keys);
            const descriptors = try documentExtractionStateUnitDescriptorsAlloc(alloc, state);
            defer freeDocumentExtractionUnitDescriptors(alloc, descriptors);
            if (descriptors.len != unit_keys.len) return error.InvalidDocumentExtractionState;

            for (descriptors, unit_keys) |*descriptor, unit_key| {
                if (!std.mem.eql(u8, descriptor.key, unit_key)) return error.InvalidDocumentExtractionState;
                var unit_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, descriptor.key)) orelse
                    return error.InvalidDocumentExtractionState;
                defer unit_ref.deinit(alloc);
                if (unit_ref.kind != .asset or unit_ref.unit_id == null or
                    !std.mem.eql(u8, unit_ref.document_id, doc_key) or
                    !std.mem.eql(u8, unit_ref.name, artifact_name))
                {
                    return error.InvalidDocumentExtractionState;
                }
                if (descriptor.fingerprint.len == 0) {
                    const stored = try db.core.getStoreValue(alloc, descriptor.key) orelse
                        return error.InvalidDocumentExtractionState;
                    defer alloc.free(stored);
                    descriptor.fingerprint = try documentExtractionStoredUnitFingerprintAlloc(alloc, stored);
                }
            }

            const digest = try hierarchy_navigation.artifactDigestAlloc(alloc, descriptors);
            defer alloc.free(digest);
            const unit_count = std.math.cast(u32, descriptors.len) orelse return error.InvalidDocumentExtractionState;
            const block_count = hierarchy_navigation.blockCount(unit_count);
            const indexed_state = try documentExtractionStateValueAlloc(
                alloc,
                source_fingerprint,
                unit_keys,
                descriptors,
                chunk_keys,
                digest,
                block_count,
                documentExtractionStateHasChunkUnitFingerprints(alloc, state),
            );
            defer alloc.free(indexed_state);
            const summary = try hierarchy_navigation.summaryValueAlloc(alloc, generation, digest, unit_count, block_count);
            defer alloc.free(summary);
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, summary_key),
                .value = try alloc.dupe(u8, summary),
            });

            var block_index: u32 = 0;
            while (block_index < block_count) : (block_index += 1) {
                const start = @as(usize, block_index) * hierarchy_navigation.block_size;
                const end = @min(start + hierarchy_navigation.block_size, descriptors.len);
                const block_key = try internal_keys.documentUnitNavigationBlockKeyAlloc(alloc, doc_key, artifact_name, block_index);
                defer alloc.free(block_key);
                const block_value = try hierarchy_navigation.blockValueAlloc(alloc, block_index, descriptors[start..end]);
                defer alloc.free(block_value);
                try artifact_writes.append(alloc, .{
                    .key = try alloc.dupe(u8, block_key),
                    .value = try alloc.dupe(u8, block_value),
                });
            }

            const previous_block_count = try documentExtractionStateNavigationBlockCount(alloc, state);
            var obsolete_block = block_count;
            while (obsolete_block < previous_block_count) : (obsolete_block += 1) {
                try artifact_delete_keys.append(
                    alloc,
                    try internal_keys.documentUnitNavigationBlockKeyAlloc(alloc, doc_key, artifact_name, obsolete_block),
                );
            }
            const state_key = try assetStateKeyAlloc(alloc, doc_key, artifact_name);
            defer alloc.free(state_key);
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, state_key),
                .value = try alloc.dupe(u8, indexed_state),
            });
        }

        pub fn appendDocumentExtractionRangeDescriptors(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(u8),
            artifact_name: []const u8,
            unit_keys: []const []const u8,
            chunk_keys: []const []const u8,
            units: []const document_extraction_mod.Unit,
            previous_child_ranges: []const types.DocumentArtifactChildRange,
        ) !void {
            var first_range = true;
            var range_index: usize = 0;
            try appendDocumentExtractionKeyRanges(alloc, out, &first_range, &range_index, "unit", artifact_name, unit_keys, units, previous_child_ranges);
            try appendDocumentExtractionKeyRanges(alloc, out, &first_range, &range_index, "chunk", "derived_chunks", chunk_keys, &.{}, previous_child_ranges);
        }

        pub fn appendDocumentExtractionRangePolicy(alloc: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
            var first = true;
            try out.append(alloc, '{');
            try appendJsonFieldU64(alloc, out, &first, "policy_version", 1);
            try appendJsonFieldUsize(alloc, out, &first, "unit_target_children", document_extraction_range_target_children);
            try appendJsonFieldUsize(alloc, out, &first, "unit_target_text_bytes", document_extraction_range_target_text_bytes);
            try appendJsonFieldUsize(alloc, out, &first, "chunk_target_children", document_extraction_range_target_children);
            try appendJsonFieldString(alloc, out, &first, "oversized_unit_policy", "single_unit_range");
            try out.append(alloc, '}');
        }

        pub fn appendDocumentExtractionUnitMergeOperation(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(u8),
            first_operation: *bool,
            op: []const u8,
            artifact_name: []const u8,
            descriptors: []const DocumentExtractionUnitDescriptor,
            comparison: []const DocumentExtractionUnitDescriptor,
            want_fingerprint_match: bool,
        ) !void {
            const count = countUnitDescriptorsByFingerprintMatch(descriptors, comparison, want_fingerprint_match);
            if (count == 0) return;

            var first_key: ?[]const u8 = null;
            var last_key: ?[]const u8 = null;
            for (descriptors) |descriptor| {
                const matched = unitDescriptorFingerprintMatches(comparison, descriptor.key, descriptor.fingerprint);
                if (matched != want_fingerprint_match) continue;
                if (first_key == null) first_key = descriptor.key;
                last_key = descriptor.key;
            }

            if (first_operation.*) {
                first_operation.* = false;
            } else {
                try out.append(alloc, ',');
            }

            var first = true;
            try out.append(alloc, '{');
            try appendJsonFieldString(alloc, out, &first, "op", op);
            try appendJsonFieldString(alloc, out, &first, "range_kind", "unit");
            try appendJsonFieldString(alloc, out, &first, "artifact_name", artifact_name);
            try appendJsonFieldString(alloc, out, &first, "first_key", first_key.?);
            try appendJsonFieldString(alloc, out, &first, "last_key", last_key.?);
            try appendJsonFieldUsize(alloc, out, &first, "key_count", count);
            try appendJsonFieldBool(alloc, out, &first, "fingerprint_match", want_fingerprint_match);
            try out.append(alloc, '}');
        }

        pub fn appendDocumentUnitChunkWrites(
            alloc: Allocator,
            db: anytype,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            source_artifact_name: []const u8,
            unit_key: []const u8,
            unit_fingerprint: []const u8,
            unit: document_extraction_mod.Unit,
            desired_chunk_keys: []const []const u8,
            chunk_range_base_index: usize,
            previous_child_ranges: []const types.DocumentArtifactChildRange,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
        ) !void {
            for (view.chunks) |entry| {
                const chunks = if (entry.chunker_json.len > 0)
                    try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
                else
                    try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
                defer chunker_mod.freeChunks(alloc, chunks);
                if (chunks.len == 0) continue;
                document_extraction_mod.applyTranscriptTiming(unit, chunks);

                const text_indexes = entry.text_indexes;

                // Resolve, once per chunk-producing entry rather than once per chunk,
                // which embedding enrichments consume these chunks. The per-chunk
                // loop below only appends chunk text to these accumulators; the
                // provider is invoked in batches as thresholds are crossed and once
                // more after the loop for any remainder.
                var dense_pending = std.ArrayListUnmanaged(PendingDocumentUnitDenseChunkEmbedding).empty;
                defer {
                    for (dense_pending.items) |*pending| pending.deinit(alloc);
                    dense_pending.deinit(alloc);
                }
                try collectPendingDocumentUnitDenseChunkEmbeddings(alloc, db, &entry, &dense_pending);
                var sparse_pending = std.ArrayListUnmanaged(PendingDocumentUnitSparseChunkEmbedding).empty;
                defer {
                    for (sparse_pending.items) |*pending| pending.deinit(alloc);
                    sparse_pending.deinit(alloc);
                }
                try collectPendingDocumentUnitSparseChunkEmbeddings(alloc, db, &entry, &sparse_pending);
                const runtime = db.enrichment_runtime;
                const dense_embedder = if (runtime) |rt| rt.config.dense_embedder else null;
                const sparse_embedder = if (runtime) |rt| rt.config.sparse_embedder else null;

                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const scratch = arena_state.allocator();

                for (chunks) |chunk| {
                    if (!chunk.isText()) continue;
                    const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id));
                    defer alloc.free(chunk_key);
                    const chunk_key_index = documentExtractionKeyIndex(desired_chunk_keys, chunk_key) orelse return error.DocumentExtractionChunkRangeMissing;
                    const chunk_range_id = try documentExtractionRangeIdAlloc(scratch, chunk_range_base_index + (chunk_key_index / document_extraction_range_target_children));
                    const chunk_route = documentExtractionRangeRoute(previous_child_ranges, chunk_range_id, "chunk", "derived_chunks");
                    const payload = try buildDocumentUnitChunkPayloadAlloc(scratch, doc_key, unit_key, unit_fingerprint, entry.name, source_artifact_name, entry.source_field, unit, chunk, true, chunk_route);
                    try artifact_writes.append(alloc, .{
                        .key = try alloc.dupe(u8, chunk_key),
                        .value = try alloc.dupe(u8, payload),
                    });

                    if (text_indexes.len > 0) {
                        const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
                        errdefer {
                            for (targets) |target| alloc.free(target.index_name);
                            alloc.free(targets);
                        }
                        for (text_indexes, 0..) |index_name, i| {
                            targets[i] = .{
                                .kind = .full_text,
                                .index_name = try alloc.dupe(u8, index_name),
                            };
                        }
                        try documents.append(alloc, .{
                            .key = try alloc.dupe(u8, chunk_key),
                            .action = .upsert,
                            .cleaned_value = try alloc.dupe(u8, payload),
                            .targets = targets,
                        });
                    }

                    if (chunk.text) |chunk_text| {
                        if (dense_embedder) |embedder| {
                            for (dense_pending.items) |*pending| {
                                try appendChunkToPendingDenseChunkEmbedding(alloc, runtime.?, embedder, doc_key, chunk_key, chunk_text, pending, artifact_writes, dense_embeddings);
                            }
                        }
                        if (sparse_embedder) |embedder| {
                            for (sparse_pending.items) |*pending| {
                                try appendChunkToPendingSparseChunkEmbedding(alloc, runtime.?, embedder, chunk_key, chunk_text, pending, artifact_writes, sparse_embeddings);
                            }
                        }
                    }

                    _ = arena_state.reset(.retain_capacity);
                }

                for (dense_pending.items) |*pending| {
                    try flushPendingDenseChunkEmbedding(alloc, runtime.?, dense_embedder.?, doc_key, pending, artifact_writes, dense_embeddings);
                }
                for (sparse_pending.items) |*pending| {
                    try flushPendingSparseChunkEmbedding(alloc, runtime.?, sparse_embedder.?, pending, artifact_writes, sparse_embeddings);
                }
            }
        }

        pub fn appendDocumentUnitStoredChunkFullTextDocuments(
            alloc: Allocator,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            unit: document_extraction_mod.Unit,
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
        ) !void {
            for (view.chunks) |entry| {
                if (entry.text_indexes.len == 0) continue;

                const chunks = if (entry.chunker_json.len > 0)
                    try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
                else
                    try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
                defer chunker_mod.freeChunks(alloc, chunks);
                document_extraction_mod.applyTranscriptTiming(unit, chunks);

                for (chunks) |chunk| {
                    if (!chunk.isText()) continue;
                    const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id));
                    defer alloc.free(chunk_key);
                    try appendStoredFullTextDocument(alloc, documents, chunk_key, entry.text_indexes);
                }
            }
        }

        pub fn appendDocumentUnitStoredFullTextDocuments(
            alloc: Allocator,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            unit_key: []const u8,
            unit: document_extraction_mod.Unit,
            unit_text_indexes: []const []const u8,
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
        ) !void {
            try appendStoredFullTextDocument(alloc, documents, unit_key, unit_text_indexes);
            try appendDocumentUnitStoredChunkFullTextDocuments(alloc, view, doc_key, unit, documents);
        }

        pub fn appendEmbeddingArtifactWrite(
            alloc: Allocator,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            base_key: []const u8,
            parent_doc_key: []const u8,
            artifact_name: []const u8,
            source_field: []const u8,
            source_key: ?[]const u8,
            origin: EmbeddingArtifactOrigin,
            vector: []const f32,
        ) ![]u8 {
            _ = parent_doc_key;
            _ = source_field;
            _ = source_key;
            const key = if (internal_keys.isInternalUserKey(base_key))
                try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, base_key, artifact_name)
            else
                try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, base_key, artifact_name);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);
            const payload = switch (origin) {
                .authored => try enrichment_artifact_codec.encodeAuthoredDenseEmbeddingAlloc(alloc, vector),
                .generated => |hash| try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, hash, vector),
            };
            var payload_owned = true;
            errdefer if (payload_owned) alloc.free(payload);
            const artifact_key = try alloc.dupe(u8, key);
            errdefer alloc.free(artifact_key);
            try artifact_writes.append(alloc, .{
                .key = key,
                .value = payload,
            });
            key_owned = false;
            payload_owned = false;
            return artifact_key;
        }

        pub const appendEnrichmentTerminalFailureMarkerDeletePageForIssue = execution_resources.appendEnrichmentTerminalFailureMarkerDeletePageForIssue;

        pub const appendEnrichmentTerminalFailureMarkerDeletesForIssue = execution_resources.appendEnrichmentTerminalFailureMarkerDeletesForIssue;

        pub fn appendFullTextDeleteDocument(
            alloc: Allocator,
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            key: []const u8,
            text_indexes: []const []const u8,
        ) !void {
            if (text_indexes.len == 0) return;
            const targets = try fullTextTargetRefsAlloc(alloc, text_indexes);
            errdefer {
                for (targets) |target| alloc.free(target.index_name);
                alloc.free(targets);
            }
            try documents.append(alloc, .{
                .key = try alloc.dupe(u8, key),
                .action = .delete,
                .targets = targets,
            });
        }

        pub fn appendGeneratedEnrichmentRef(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef),
            request: enrichment_types.GeneratedEnrichmentRequest,
        ) !void {
            const ref = try enrichment_types.requestToRef(alloc, request);
            errdefer enrichment_types.freeGeneratedRef(alloc, ref);
            try out.append(alloc, ref);
        }

        pub fn appendGraphAssetStateSegmentDeleteKeys(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            state_key: []const u8,
            deletes: *std.ArrayListUnmanaged([]const u8),
        ) !void {
            const raw = store.get(alloc, state_key) catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            defer alloc.free(raw);
            if (try graph_asset_state.format(raw) != .v5) return;
            const root = try graph_asset_state.segmentedRoot(raw);
            for (0..root.segment_count) |segment_index| {
                const key = try internal_keys.graphAssetStateSegmentKeyAlloc(alloc, state_key, @intCast(segment_index));
                if (containsDeleteKey(deletes.items, key)) {
                    alloc.free(key);
                } else {
                    try deletes.append(alloc, key);
                }
            }
        }

        pub fn appendGraphContenderChange(
            alloc: Allocator,
            changes: *GraphContenderChanges,
            edge_key: []const u8,
            state_key: []const u8,
            source_priority: usize,
            payload: ?[]const u8,
        ) !void {
            const gop = try changes.getOrPut(alloc, edge_key);
            if (!gop.found_existing) gop.value_ptr.* = .empty;
            for (gop.value_ptr.items) |*change| {
                if (!std.mem.eql(u8, change.state_key, state_key)) continue;
                change.source_priority = source_priority;
                change.payload = payload;
                return;
            }
            try gop.value_ptr.append(alloc, .{
                .state_key = state_key,
                .source_priority = source_priority,
                .payload = payload,
            });
        }

        pub fn appendGraphEdgeArtifactWrite(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            write: types.GraphEdgeWrite,
            generation: u64,
            ttl_duration_ns: u64,
            timestamp_ns: u64,
        ) !void {
            try validateGraphEdgeMetadataJson(alloc, write.metadata_json);
            return try appendPreparedGraphEdgeArtifactWrite(alloc, store, artifact_writes, write, generation, ttl_duration_ns, timestamp_ns);
        }

        pub fn appendGraphEndpointRetirements(
            alloc: Allocator,
            _: *docstore_mod.DocStore,
            generation: u64,
            deleted_docs: []const []const u8,
            enqueue_endpoints: bool,
            deleted_artifacts: []const []const u8,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            var deleted_owners = std.StringHashMapUnmanaged(void).empty;
            defer deleted_owners.deinit(alloc);
            for (deleted_docs) |doc| try deleted_owners.put(alloc, doc, {});
            for (deleted_docs) |doc| {
                if (!enqueue_endpoints or isMetadataKey(doc)) continue;
                const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, doc);
                owned_keys.append(alloc, job) catch |err| {
                    alloc.free(job);
                    return err;
                };
                const value = try @import("../graph_cleanup_contract.zig").encodeAlloc(alloc, doc, generation);
                owned_values.append(alloc, value) catch |err| {
                    alloc.free(value);
                    return err;
                };
                try writes.append(alloc, .{ .key = job, .value = value });
            }
            for (deleted_artifacts) |artifact| {
                if (internal_keys.graphInlineTargetComponent(artifact) == null) continue;
                const owner_end = internal_keys.findComponentTerminator(artifact, 1).?;
                const owner = try internal_keys.decodeBodyAlloc(alloc, artifact[1..owner_end]);
                defer alloc.free(owner);
                if (deleted_owners.contains(owner)) continue;
                const retired = try internal_keys.graphRetirementKeyAlloc(alloc, artifact);
                owned_keys.append(alloc, retired) catch |err| {
                    alloc.free(retired);
                    return err;
                };
                const stamp = @import("../graph_cleanup_contract.zig").retirementValue(generation);
                const value = try alloc.dupe(u8, &stamp);
                owned_values.append(alloc, value) catch |err| {
                    alloc.free(value);
                    return err;
                };
                try writes.append(alloc, .{ .key = retired, .value = value });
            }
        }

        pub fn appendGraphLifecycleGeneration(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            ordered_index: u64,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !u64 {
            const raw = store.get(alloc, internal_keys.graph_endpoint_cleanup_generation_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (raw) |value| alloc.free(value);
            if (raw != null and raw.?.len != 8) return error.InvalidGraphSegment;
            const previous = if (raw) |value| std.mem.readInt(u64, value[0..8], .little) else 0;
            const generation = @max(try std.math.add(u64, previous, 1), ordered_index);
            const value = try alloc.alloc(u8, 8);
            owned_values.append(alloc, value) catch |err| {
                alloc.free(value);
                return err;
            };
            std.mem.writeInt(u64, value[0..8], generation, .little);
            try writes.append(alloc, .{ .key = internal_keys.graph_endpoint_cleanup_generation_key, .value = value });
            return generation;
        }

        pub fn appendImportedGraphContenderMutations(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            manager: *index_manager_mod.IndexManager,
            imported: []const types.BatchWrite,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            deletes: *std.ArrayListUnmanaged([]const u8),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            owned_deletes: *std.ArrayListUnmanaged([]u8),
            changed: *std.ArrayListUnmanaged([]u8),
            changed_set: *std.StringHashMapUnmanaged(void),
        ) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            const Group = struct {
                owner: []const u8,
                index: *index_manager_mod.IndexManager.GraphIndex,
                changes: GraphContenderChanges = .empty,
                extra_deletes: std.ArrayListUnmanaged([]const u8) = .empty,
            };
            const Capture = struct {
                alloc: Allocator,
                backing_alloc: Allocator,
                store: *docstore_mod.DocStore,
                writes: []const docstore_mod.KVPair,
                positions: *const StoreWritePositions,
                groups: std.ArrayListUnmanaged(Group) = .empty,
                group_positions: std.StringHashMapUnmanaged(usize) = .empty,

                fn group(self: *@This(), owner: []const u8, index: *index_manager_mod.IndexManager.GraphIndex) !*Group {
                    const key = try internal_keys.graphEdgeContenderCountKeyAlloc(self.alloc, owner, index.config.name);
                    const pos = try self.group_positions.getOrPut(self.alloc, key);
                    if (!pos.found_existing) {
                        pos.value_ptr.* = self.groups.items.len;
                        try self.groups.append(self.alloc, .{ .owner = try self.alloc.dupe(u8, owner), .index = index });
                    }
                    return &self.groups.items[pos.value_ptr.*];
                }

                fn value(self: *@This(), value_alloc: Allocator, key: []const u8) !?[]const u8 {
                    if (self.positions.get(key)) |pos| return self.writes[pos].value;
                    return self.store.get(value_alloc, key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                }

                fn contender(self: *@This(), index: *index_manager_mod.IndexManager.GraphIndex, key: []const u8, raw: []const u8, state: ?docstore_mod.KVPair) !void {
                    // An edge prefix can contain arbitrarily many source units. Keep
                    // validation allocations scoped to one candidate, and copy only
                    // matching mutation state into the page arena.
                    var probe_arena = std.heap.ArenaAllocator.init(self.backing_alloc);
                    defer probe_arena.deinit();
                    const probe = probe_arena.allocator();
                    const view = (try graph_edge_contender.decode(raw, index.config.coverage_generation)) orelse return;
                    const expected = try internal_keys.graphGlobalEdgeContenderKeyAlloc(probe, index.config.name, index.config.coverage_generation, view.edge_key, view.source_priority, view.state_key);
                    if (!std.mem.eql(u8, key, expected)) return error.InvalidGraphEdgeContender;
                    const owner = (try internal_keys.decodeDocumentComponentAlloc(probe, view.edge_key)) orelse return error.InvalidGraphEdgeArtifact;
                    const tomb_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(probe, view.edge_key, index.config.name, index.config.coverage_generation, view.state_key);
                    const lifetime_key = try internal_keys.graphEdgeTtlLifetimeKeyAlloc(probe, view.edge_key, index.config.name, index.config.coverage_generation, view.state_key);
                    if (state) |row| {
                        const expected_state = if (internal_keys.isGraphEdgeTtlTombstoneKey(row.key)) tomb_key else lifetime_key;
                        if (!std.mem.eql(u8, row.key, expected_state)) return;
                    }
                    const tomb_raw = try self.value(probe, tomb_key);
                    const suppressed = if (tomb_raw) |tomb| blk: {
                        const decoded = try graph_edge_ttl_tombstone.Tombstone.decode(tomb);
                        const digest = try graph_edge_ttl_tombstone.sourceDigest(probe, view.payload);
                        break :blk std.mem.eql(u8, &digest, &decoded.source_digest);
                    } else false;
                    const g = try self.group(owner, index);
                    if (suppressed) {
                        try appendGraphContenderChange(self.alloc, &g.changes, try self.alloc.dupe(u8, view.edge_key), try self.alloc.dupe(u8, view.state_key), view.source_priority, null);
                        try g.extra_deletes.append(self.alloc, try self.alloc.dupe(u8, lifetime_key));
                        return;
                    }
                    // A changed revision retires an obsolete imported tombstone too.
                    if (tomb_raw != null) try g.extra_deletes.append(self.alloc, try self.alloc.dupe(u8, tomb_key));
                    var payload = try self.alloc.dupe(u8, view.payload);
                    if (state) |row| if (internal_keys.isGraphEdgeTtlLifetimeKey(row.key)) {
                        if (row.value.len != 8) return error.InvalidGraphEdgeTtlLifetime;
                        const timestamp = std.mem.readInt(u64, row.value[0..8], .big);
                        if (timestamp == 0) return error.InvalidGraphEdgeTtlLifetime;
                        var edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(probe, payload);
                        defer edge.deinit(probe);
                        payload = try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(self.alloc, null, index.config.coverage_generation, edge.weight, edge.created_at, edge.updated_at, timestamp, edge.metadata_json);
                    };
                    try appendGraphContenderChange(self.alloc, &g.changes, try self.alloc.dupe(u8, view.edge_key), try self.alloc.dupe(u8, view.state_key), view.source_priority, payload);
                }
            };
            var positions = StoreWritePositions.empty;
            for (writes.items, 0..) |write, i| try positions.put(scratch, write.key, i);
            var capture = Capture{ .alloc = scratch, .backing_alloc = alloc, .store = store, .writes = writes.items, .positions = &positions };
            for (imported) |row| {
                const global = internal_keys.isGraphGlobalEdgeContenderKey(row.key);
                const ttl_state = internal_keys.isGraphEdgeTtlLifetimeKey(row.key) or internal_keys.isGraphEdgeTtlTombstoneKey(row.key);
                if (!global and !ttl_state) continue;
                var index: ?*index_manager_mod.IndexManager.GraphIndex = null;
                for (manager.graph_indexes.items) |*entry| {
                    if (if (global) internal_keys.matchesGraphGlobalEdgeContenderIndexName(row.key, entry.config.name) else internal_keys.matchesGraphEdgeContenderIndexName(row.key, entry.config.name)) {
                        index = entry;
                        break;
                    }
                }
                const entry = index orelse return error.IndexNotFound;
                if (global) {
                    const generation = try graph_edge_contender.coverageGeneration(row.value);
                    const view = (try graph_edge_contender.decode(row.value, generation)) orelse return error.InvalidGraphEdgeContender;
                    const key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(scratch, entry.config.name, entry.config.coverage_generation, view.edge_key, view.source_priority, view.state_key);
                    try capture.contender(entry, key, (try capture.value(scratch, key)) orelse return error.InvalidGraphEdgeContender, null);
                } else {
                    const key = try internal_keys.rebindGraphEdgeTtlStateKeyGenerationAlloc(scratch, row.key, entry.config.coverage_generation);
                    const state: docstore_mod.KVPair = .{ .key = key, .value = row.value };
                    const prefix = try internal_keys.graphGlobalEdgeContenderPrefixForTtlStateAlloc(scratch, key);
                    var txn = try store.beginReadTxn();
                    defer txn.abort();
                    var cursor = try txn.openCursor();
                    defer cursor.close();
                    var next = try cursor.seekAtOrAfter(prefix);
                    while (next) |existing| : (next = try cursor.next()) {
                        if (!std.mem.startsWith(u8, existing.key, prefix)) break;
                        if (positions.contains(existing.key)) continue;
                        try capture.contender(entry, existing.key, existing.value, state);
                    }
                    for (writes.items) |pending| {
                        if (std.mem.startsWith(u8, pending.key, prefix))
                            try capture.contender(entry, pending.key, pending.value, state);
                    }
                }
            }
            for (capture.groups.items) |*g| {
                var reconciled = try reconcileGraphEdgeContendersWithLifetimePolicy(alloc, store, g.owner, g.index.config.name, g.index.config.coverage_generation, g.index.ttl_duration_ns, &g.changes, writes.items, deletes.items, true);
                defer reconciled.deinit(alloc);
                const affected = try scratch.alloc([]const u8, g.changes.count());
                var it = g.changes.keyIterator();
                for (affected) |*key| key.* = it.next().?.*;
                var mutation = try prepareGraphContenderReconcilePage(alloc, affected, &reconciled, &.{}, g.extra_deletes.items);
                defer mutation.deinit(alloc);
                try owned_deletes.ensureUnusedCapacity(alloc, mutation.deletes.items.len);
                try deletes.ensureUnusedCapacity(alloc, mutation.deletes.items.len);
                try owned_keys.ensureUnusedCapacity(alloc, mutation.writes.items.len);
                try owned_values.ensureUnusedCapacity(alloc, mutation.writes.items.len);
                try writes.ensureUnusedCapacity(alloc, mutation.writes.items.len);
                for (affected) |key| try appendUniqueOwnedKeyIndexed(alloc, changed, changed_set, key);
                for (mutation.deletes.items) |key| {
                    // Removed imported rows must not also survive as pending puts.
                    var i: usize = 0;
                    while (i < writes.items.len) {
                        if (std.mem.eql(u8, writes.items[i].key, key)) _ = writes.orderedRemove(i) else i += 1;
                    }
                    owned_deletes.appendAssumeCapacity(@constCast(key));
                    deletes.appendAssumeCapacity(key);
                }
                mutation.deletes.clearRetainingCapacity();
                for (mutation.writes.items) |write| {
                    removePendingDeleteKey(alloc, deletes, owned_deletes, write.key);
                    owned_keys.appendAssumeCapacity(@constCast(write.key));
                    owned_values.appendAssumeCapacity(@constCast(write.value));
                    writes.appendAssumeCapacity(write);
                }
                mutation.writes.clearRetainingCapacity();
            }
        }

        pub fn appendInlineFullTextDocument(
            alloc: Allocator,
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            key: []const u8,
            value: []const u8,
            text_indexes: []const []const u8,
        ) !void {
            if (text_indexes.len == 0) return;
            const targets = try fullTextTargetRefsAlloc(alloc, text_indexes);
            errdefer {
                for (targets) |target| alloc.free(target.index_name);
                alloc.free(targets);
            }
            try documents.append(alloc, .{
                .key = try alloc.dupe(u8, key),
                .action = .upsert,
                .cleaned_value = try alloc.dupe(u8, value),
                .targets = targets,
            });
        }

        pub fn appendJsonFieldBool(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: bool) !void {
            try appendJsonFieldName(alloc, out, first, name);
            try out.appendSlice(alloc, if (value) "true" else "false");
        }

        pub fn appendJsonFieldName(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8) !void {
            if (first.*) {
                first.* = false;
            } else {
                try out.append(alloc, ',');
            }
            try appendJsonString(alloc, out, name);
            try out.append(alloc, ':');
        }

        pub fn appendJsonFieldString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: []const u8) !void {
            try appendJsonFieldName(alloc, out, first, name);
            try appendJsonString(alloc, out, value);
        }

        pub fn appendJsonFieldU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: u64) !void {
            try appendJsonFieldName(alloc, out, first, name);
            try appendJsonUnsigned(alloc, out, value);
        }

        pub fn appendJsonFieldUsize(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: usize) !void {
            try appendJsonFieldName(alloc, out, first, name);
            try appendJsonUnsigned(alloc, out, value);
        }

        pub fn appendJsonUnsigned(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
            var writer = std.Io.Writer.Allocating.fromArrayList(alloc, out);
            defer out.* = writer.toArrayList();
            writer.writer.print("{d}", .{value}) catch return error.OutOfMemory;
        }

        pub fn appendKeysForPrefixDeleteInStore(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            deletes: *std.ArrayListUnmanaged([]const u8),
            owned_keys: *std.ArrayListUnmanaged([]const u8),
            prefix: []const u8,
        ) !void {
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |buf| alloc.free(buf);
            const ScanState = struct {
                alloc: Allocator,
                deletes: *std.ArrayListUnmanaged([]const u8),
                owned_keys: *std.ArrayListUnmanaged([]const u8),

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    const key_copy = try state.alloc.dupe(u8, key);
                    errdefer state.alloc.free(key_copy);
                    const owned_len = state.owned_keys.items.len;
                    try state.owned_keys.append(state.alloc, key_copy);
                    errdefer state.owned_keys.shrinkRetainingCapacity(owned_len);
                    try state.deletes.append(state.alloc, key_copy);
                    return .@"continue";
                }
            };
            var state = ScanState{ .alloc = alloc, .deletes = deletes, .owned_keys = owned_keys };
            try store.scanWithContext(prefix, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);
        }

        pub fn appendManagedTargetIdentity(
            alloc: Allocator,
            identities: *std.ArrayListUnmanaged(IndexTargetVisibility),
            cfg: *const types.IndexConfig,
            serving_set_effect: IndexTargetVisibility.ServingSetEffect,
            scope_known: *bool,
        ) !void {
            if (!scope_known.*) return;
            const fingerprint = cfg.coverage_config_fingerprint orelse {
                scope_known.* = false;
                return;
            };
            const owned_name = try alloc.dupe(u8, cfg.name);
            errdefer alloc.free(owned_name);
            try identities.append(alloc, .{
                .index_name = owned_name,
                .kind = cfg.kind,
                .incarnation = internal_keys.derivedCoverageGenerationForConfig(
                    cfg.coverage_generation,
                    cfg.config_json,
                ),
                .config_hash = fingerprint,
                .serving_set_effect = serving_set_effect,
            });
        }

        pub fn appendMaterializedChunkSourceToBatch(
            alloc: Allocator,
            sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
            batch_source_bytes: *usize,
            key: []const u8,
            value: []const u8,
            source_field: []const u8,
        ) !bool {
            const text = (try chunkPayloadTextAlloc(alloc, value, source_field)) orelse return false;
            var text_owned = true;
            errdefer if (text_owned) alloc.free(text);
            try sources.append(alloc, .{
                .key = try alloc.dupe(u8, key),
                .text = text,
            });
            text_owned = false;
            batch_source_bytes.* += text.len;
            return true;
        }

        pub fn appendMentionEvidenceArtifactsFromResolution(
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            changed: *std.ArrayListUnmanaged([]u8),
            doc_key: []const u8,
            resolution_key: []const u8,
            resolution_raw: []const u8,
            extraction_raw: ?[]const u8,
            cfg: *const index_manager_mod.ResolverConfig,
        ) !void {
            var parsed_resolution = resolver_lib.parseResolution(alloc, resolution_raw) catch return;
            defer parsed_resolution.deinit();
            var parsed_extraction: ?resolver_lib.ParsedEntities = if (extraction_raw) |raw|
                resolver_lib.parseExtractionEntities(alloc, raw) catch null
            else
                null;
            defer if (parsed_extraction) |*parsed| parsed.deinit();
            const source_artifact_key = try sourceArtifactKeyForResolutionAlloc(alloc, doc_key, cfg.source_artifact);
            defer alloc.free(source_artifact_key);

            for (parsed_resolution.entities) |entity| {
                if (!resolutionDecisionCreatesCanonicalEdge(entity.decision)) continue;
                if (entity.doc_ref.key.len == 0) continue;
                const extraction_entity = if (parsed_extraction) |parsed|
                    extractionEntityForLocalId(parsed.entities, entity.local_id)
                else
                    null;
                const key = try resolutionMentionArtifactKeyAlloc(alloc, doc_key, cfg.source_artifact, cfg.resolution_artifact, entity.local_id);
                var key_owned = true;
                errdefer if (key_owned) alloc.free(key);
                const payload = try mentionEvidencePayloadAlloc(alloc, doc_key, key, source_artifact_key, resolution_key, cfg, entity, extraction_entity);
                var payload_owned = true;
                errdefer if (payload_owned) alloc.free(payload);
                try writes.append(alloc, .{ .key = key, .value = payload });
                key_owned = false;
                payload_owned = false;
                try appendUniqueOwnedKey(alloc, changed, key);
            }
        }

        pub fn appendMixedDirectGraphContenderMutations(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            direct_writes: []const types.BatchWrite,
            store_writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            store_deletes: *std.ArrayListUnmanaged([]const u8),
            owned_write_keys: *std.ArrayListUnmanaged([]u8),
            owned_write_values: *std.ArrayListUnmanaged([]u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
        ) !void {
            const Group = struct {
                doc_key: []u8,
                index_name: []u8,
                state_key: []u8,
                generation: u64,
                ttl_duration_ns: u64,
                edge_limit: usize,
                changes: GraphContenderChanges = .empty,

                pub fn deinit(self: *@This(), allocator: Allocator) void {
                    var it = self.changes.valueIterator();
                    while (it.next()) |items| items.deinit(allocator);
                    self.changes.deinit(allocator);
                    allocator.free(self.doc_key);
                    allocator.free(self.index_name);
                    allocator.free(self.state_key);
                }
            };
            const Collect = struct {
                alloc: Allocator,
                index_manager: *index_manager_mod.IndexManager,
                groups: *std.ArrayListUnmanaged(Group),
                positions: *std.StringHashMapUnmanaged(usize),

                fn add(self: *@This(), edge_key: []const u8, payload: ?[]const u8) !void {
                    if (!internal_keys.isGraphEdgeArtifactKey(edge_key)) return;
                    const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(self.alloc, edge_key)) orelse return error.InvalidGraphEdgeArtifact;
                    defer {
                        self.alloc.free(parsed.doc_key);
                        self.alloc.free(parsed.index_name);
                        self.alloc.free(parsed.edge_type);
                        self.alloc.free(parsed.target_doc_key);
                        self.alloc.free(parsed.edge_id);
                        self.alloc.free(parsed.logical_source);
                    }
                    const entry = self.index_manager.graphIndex(parsed.index_name) orelse return;
                    if (self.index_manager.graphArtifactSources(parsed.index_name).len == 0) return;
                    const state_key = try internal_keys.graphDirectStateKeyAlloc(self.alloc, parsed.doc_key, parsed.index_name);
                    var state_key_owned = true;
                    defer if (state_key_owned) self.alloc.free(state_key);
                    const gop = try self.positions.getOrPut(self.alloc, state_key);
                    if (!gop.found_existing) {
                        const doc_key = try self.alloc.dupe(u8, parsed.doc_key);
                        errdefer self.alloc.free(doc_key);
                        const index_name = try self.alloc.dupe(u8, parsed.index_name);
                        errdefer self.alloc.free(index_name);
                        gop.value_ptr.* = self.groups.items.len;
                        try self.groups.append(self.alloc, .{
                            .doc_key = doc_key,
                            .index_name = index_name,
                            .state_key = state_key,
                            .generation = entry.config.coverage_generation,
                            .ttl_duration_ns = entry.ttl_duration_ns,
                            .edge_limit = graph_asset_state.effectiveEdgeLimit(entry.max_edges_per_document),
                        });
                        state_key_owned = false;
                    }
                    const group = &self.groups.items[gop.value_ptr.*];
                    try appendGraphContenderChange(self.alloc, &group.changes, edge_key, group.state_key, @intCast(graph_mod.direct_source_priority), payload);
                }
            };
            var groups = std.ArrayListUnmanaged(Group).empty;
            defer {
                for (groups.items) |*group| group.deinit(alloc);
                groups.deinit(alloc);
            }
            var positions = std.StringHashMapUnmanaged(usize).empty;
            defer positions.deinit(alloc);
            var collect = Collect{ .alloc = alloc, .index_manager = index_manager, .groups = &groups, .positions = &positions };
            var written_keys = std.StringHashMapUnmanaged(void).empty;
            defer written_keys.deinit(alloc);
            for (direct_writes) |write| {
                try written_keys.put(alloc, write.key, {});
                try collect.add(write.key, write.value);
            }
            const initial_delete_count = store_deletes.items.len;
            for (store_deletes.items[0..initial_delete_count]) |edge_key| {
                if (written_keys.contains(edge_key)) continue;
                try collect.add(edge_key, null);
            }
            for (groups.items) |*group| {
                const affected = try alloc.alloc([]const u8, group.changes.count());
                defer alloc.free(affected);
                var it = group.changes.keyIterator();
                var i: usize = 0;
                while (it.next()) |key| : (i += 1) affected[i] = key.*;
                for (affected) |edge_key| {
                    const changes = group.changes.get(edge_key).?;
                    const tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, group.index_name, group.generation, group.state_key);
                    try owned_delete_keys.append(alloc, tombstone_key);
                    try store_deletes.append(alloc, tombstone_key);
                    if (changes.items[0].payload == null) {
                        const lifetime_key = try internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, edge_key, group.index_name, group.generation, group.state_key);
                        try owned_delete_keys.append(alloc, lifetime_key);
                        try store_deletes.append(alloc, lifetime_key);
                    }
                }
                var reconciled = try reconcileGraphEdgeContenders(alloc, store, group.doc_key, group.index_name, group.generation, group.ttl_duration_ns, &group.changes, store_writes.items, store_deletes.items);
                defer reconciled.deinit(alloc);
                // Direct contributors share the source edge budget. Check the final
                // primary state, including every addition/removal in this batch,
                // before publishing any mutation or replay record.
                if (reconciled.visible_count > group.edge_limit) return error.ResourceLimitExceeded;
                var mutation = try prepareGraphContenderReconcilePage(alloc, affected, &reconciled, &.{}, &.{});
                defer mutation.deinit(alloc);
                for (mutation.writes.items) |write| {
                    const key = try alloc.dupe(u8, write.key);
                    var key_owned = true;
                    errdefer if (key_owned) alloc.free(key);
                    const value = try alloc.dupe(u8, write.value);
                    var value_owned = true;
                    errdefer if (value_owned) alloc.free(value);
                    try owned_write_keys.append(alloc, key);
                    key_owned = false;
                    try owned_write_values.append(alloc, value);
                    value_owned = false;
                    try store_writes.append(alloc, .{ .key = key, .value = value });
                }
                for (mutation.deletes.items) |key| {
                    const owned = try alloc.dupe(u8, key);
                    try owned_delete_keys.append(alloc, owned);
                    try store_deletes.append(alloc, owned);
                }
            }
        }

        pub fn appendOwnedConstBytes(
            alloc: Allocator,
            values: *std.ArrayListUnmanaged([]const u8),
            value: []const u8,
        ) !void {
            const owned = try alloc.dupe(u8, value);
            errdefer alloc.free(owned);
            try values.append(alloc, owned);
        }

        pub fn appendOwnedManagedIndexName(
            alloc: Allocator,
            items: *std.ArrayListUnmanaged([]const u8),
            value: []const u8,
        ) !void {
            // IndexManager.managedIndexes() returns one entry per index name, so each
            // filtered projection is already unique. Preserve that invariant here
            // instead of turning target discovery into an O(indexes^2) duplicate scan.
            const owned = try alloc.dupe(u8, value);
            errdefer alloc.free(owned);
            try items.append(alloc, owned);
        }

        pub fn appendPendingDocumentUnitChunkSource(
            alloc: Allocator,
            pending: anytype,
            chunk_key: []const u8,
            chunk_text: []const u8,
        ) !void {
            try pending.sources.ensureUnusedCapacity(alloc, 1);
            try pending.chunk_texts.ensureUnusedCapacity(alloc, 1);
            try pending.source_indexes.ensureUnusedCapacity(alloc, 1);
            const key = try alloc.dupe(u8, chunk_key);
            errdefer alloc.free(key);
            const text = try alloc.dupe(u8, chunk_text);
            const source_index = pending.sources.items.len;
            pending.sources.appendAssumeCapacity(.{ .key = key, .text = text });
            pending.chunk_texts.appendAssumeCapacity(text);
            pending.source_indexes.appendAssumeCapacity(source_index);
            pending.batch_source_bytes += chunk_text.len;
        }

        pub fn appendPrecomputeAssetProducerBatchItem(
            alloc: Allocator,
            db: anytype,
            items: *std.ArrayListUnmanaged(PrecomputeAssetProducerBatchItem),
            item: PrecomputeAssetProducerBatchItem,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            coverage_outcomes: *std.ArrayListUnmanaged(PrecomputedCoverageOutcome),
        ) !void {
            const policy = enrichment_types.parseExecutionPolicyJson(alloc, item.request.execution_json) catch enrichment_types.ExecutionPolicy{};
            const max_items = @max(@as(usize, 1), policy.batch_items orelse 1);
            const max_bytes = @max(@as(usize, 1), policy.batch_bytes orelse std.math.maxInt(usize));
            if (items.items.len > 0) {
                const current_bytes = precomputeAssetProducerBatchBytes(items.items);
                const item_bytes = precomputeAssetProducerBatchItemBytes(item);
                if (!samePrecomputeAssetProducerBatchKey(items.items[0], item) or
                    items.items.len >= max_items or
                    addPrecomputeAssetProducerBytes(current_bytes, item_bytes) > max_bytes)
                {
                    try flushPrecomputeAssetProducerBatch(alloc, db, items, artifact_writes, documents, coverage_outcomes);
                }
            }
            try items.append(alloc, item);
        }

        pub fn appendPrecomputedArtifactCoverageOutcomes(
            db: anytype,
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(PrecomputedCoverageOutcome),
            request: enrichment_types.GeneratedEnrichmentRequest,
            outcome: DerivedCoverageOutcome,
        ) !void {
            // `indexesDependingOnArtifact` and `derivedCoverageAppliesToIndex` both
            // walk live `enrichments`/`dense_indexes`/`sparse_indexes`/`graph_indexes`/
            // `text_indexes` arrays on the IndexManager. This runs from the pre-lock
            // precompute path (no exclusive apply lock held), so take the catalog's
            // own shared lock for this short, allocation-only scan -- never held
            // across chunking, rendering, or model inference -- instead of reading
            // those arrays while a concurrent catalog publish could replace or free
            // them.
            var pinned = try db.core.index_manager.acquireWritePlanSnapshot();
            defer pinned.release();
            const requirements = if (pinned.plan().completion_plan) |*value| value else return error.ArtifactCatalogDrift;
            _ = requirements.providerFor(request) catch return error.EnrichmentSourceChanged;
            db.core.index_manager.catalog_mutex.lockShared();
            defer db.core.index_manager.catalog_mutex.unlockShared();
            const consumers = try db.core.index_manager.indexesDependingOnArtifact(alloc, requestArtifactName(request));
            defer {
                for (consumers) |name| alloc.free(name);
                alloc.free(consumers);
            }
            for (consumers) |index_name| {
                // A produced artifact says nothing about its dense/sparse
                // consumers, whose embedding lanes settle their own outcomes; only
                // graph and full_text consumers are settled by the producer.
                const applies = db.derivedCoverageAppliesToIndex(.graph, index_name) or
                    db.derivedCoverageAppliesToIndex(.full_text, index_name);
                if (!applies) continue;
                const owned_index_name = try alloc.dupe(u8, index_name);
                errdefer alloc.free(owned_index_name);
                const owned_doc_key = try alloc.dupe(u8, request.doc_key);
                errdefer alloc.free(owned_doc_key);
                try out.append(alloc, .{
                    .index_name = owned_index_name,
                    .doc_key = owned_doc_key,
                    .outcome = outcome,
                });
                const readiness = @import("artifact_producer_readiness.zig");
                const sources = try db.core.index_manager.artifactSourceNamesForIndexAlloc(alloc, index_name);
                defer {
                    for (sources) |source| alloc.free(source);
                    alloc.free(sources);
                }
                for (sources) |source| {
                    if (!readiness.localSourceProduced(pinned.plan(), requestArtifactName(request), source)) continue;
                    const scope = try readiness.localScopeAlloc(alloc, index_name, source, pinned.plan());
                    errdefer alloc.free(scope);
                    const document = try alloc.dupe(u8, request.doc_key);
                    errdefer alloc.free(document);
                    try out.append(alloc, .{ .index_name = scope, .doc_key = document, .outcome = outcome });
                }
            }
        }

        pub fn appendPrecomputedCoverageCandidate(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(PrecomputedCoverageCandidate),
            request: enrichment_types.GeneratedEnrichmentRequest,
            produced: bool,
        ) !void {
            const cloned = try enrichment_types.cloneGeneratedRequest(alloc, request);
            errdefer enrichment_types.freeGeneratedRequest(alloc, cloned);
            try out.append(alloc, .{ .request = cloned, .produced = produced });
        }

        pub fn appendPrecomputedCoverageOutcomeMutations(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            outcomes: []const PrecomputedCoverageOutcome,
            source_sequence: u64,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            if (outcomes.len == 0) return;
            if (try orderedCoverageActive(store)) return;

            // Group once so commit preparation stays O(number of outcomes), even when
            // one batch feeds many indexes. Keys borrow from `outcomes` for this call.
            var grouped = std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(DerivedCoverageOutcome)).empty;
            defer {
                var values = grouped.valueIterator();
                while (values.next()) |value| value.deinit(alloc);
                grouped.deinit(alloc);
            }
            for (outcomes) |candidate| {
                const group = try grouped.getOrPut(alloc, candidate.index_name);
                if (!group.found_existing) group.value_ptr.* = .empty;
                const entry = try group.value_ptr.getOrPut(alloc, candidate.doc_key);
                if (!entry.found_existing or
                    precomputedCoverageOutcomePriority(candidate.outcome) > precomputedCoverageOutcomePriority(entry.value_ptr.*))
                {
                    entry.value_ptr.* = candidate.outcome;
                }
            }

            var groups = grouped.iterator();
            while (groups.next()) |group| {
                const index_name = group.key_ptr.*;
                const generation = index_manager.coverageGenerationForIndex(@import("artifact_producer_readiness.zig").localScopeIndex(index_name)) orelse continue;
                // Multiple generated requests may feed one index for the same source
                // document. The strongest exact terminal result wins deterministically.
                const final_outcomes = group.value_ptr;

                const tags = std.meta.tags(DerivedCoverageOutcome);
                var counter_counts: [tags.len]u64 = undefined;
                inline for (tags, 0..) |outcome, i| {
                    counter_counts[i] = try derivedCoverageOutcomeCounterValueForStore(
                        alloc,
                        store,
                        index_name,
                        generation,
                        @tagName(outcome),
                    );
                }

                var changed = false;
                var outcome_it = final_outcomes.iterator();
                while (outcome_it.next()) |entry| {
                    const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(
                        alloc,
                        index_name,
                        generation,
                        entry.key_ptr.*,
                    );
                    errdefer alloc.free(marker_key);
                    if (!std.mem.eql(u8, index_name, @import("artifact_producer_readiness.zig").localScopeIndex(index_name))) {
                        const revision_key = try @import("artifact_producer_readiness.zig").localReceiptRevisionKeyAlloc(alloc, marker_key);
                        owned_keys.append(alloc, revision_key) catch |err| {
                            alloc.free(revision_key);
                            return err;
                        };
                        const revision_value = try alloc.alloc(u8, 8);
                        std.mem.writeInt(u64, revision_value[0..8], source_sequence, .little);
                        owned_values.append(alloc, revision_value) catch |err| {
                            alloc.free(revision_value);
                            return err;
                        };
                        try writes.append(alloc, .{ .key = revision_key, .value = revision_value });
                    }
                    const existing = store.get(alloc, marker_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    const existing_outcome: ?DerivedCoverageOutcome = if (existing) |value| blk: {
                        defer alloc.free(value);
                        break :blk std.meta.stringToEnum(DerivedCoverageOutcome, value) orelse
                            return error.InvalidDerivedCoverageOutcome;
                    } else null;
                    const target = entry.value_ptr.*;
                    if (existing_outcome != null and existing_outcome.? == target) {
                        alloc.free(marker_key);
                        continue;
                    }
                    if (existing_outcome) |previous| {
                        const previous_index = @backingInt(previous);
                        if (counter_counts[previous_index] == 0) return error.InvalidDerivedCoverageCounter;
                        counter_counts[previous_index] -= 1;
                    }
                    counter_counts[@backingInt(target)] +|= 1;
                    try owned_keys.append(alloc, marker_key);
                    try writes.append(alloc, .{ .key = marker_key, .value = @tagName(target) });
                    changed = true;
                }
                if (!changed) continue;

                inline for (tags, 0..) |outcome, i| {
                    const counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(
                        alloc,
                        index_name,
                        generation,
                        @tagName(outcome),
                    );
                    errdefer alloc.free(counter_key);
                    const counter_value = try alloc.alloc(u8, @sizeOf(u64));
                    errdefer alloc.free(counter_value);
                    std.mem.writeInt(u64, counter_value[0..8], counter_counts[i], .little);
                    try owned_keys.append(alloc, counter_key);
                    try owned_values.append(alloc, counter_value);
                    try writes.append(alloc, .{ .key = counter_key, .value = counter_value });
                }
            }
        }

        pub fn appendPrecomputedEmbeddingCoverageOutcomes(
            db: anytype,
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(PrecomputedCoverageOutcome),
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: []const types.BatchWrite,
            produced: bool,
        ) !bool {
            const consumer_indexes: []const []const u8 = switch (request.kind) {
                .dense_embedding, .sparse_embedding => request.consumer_indexes,
                .asset, .chunk_text => return true,
            };

            const outcome = try precomputedEmbeddingCoverageOutcome(db, alloc, request, artifact_writes, produced) orelse return false;
            for (consumer_indexes) |index_name| {
                const owned_index_name = try alloc.dupe(u8, index_name);
                errdefer alloc.free(owned_index_name);
                const owned_doc_key = try alloc.dupe(u8, request.doc_key);
                errdefer alloc.free(owned_doc_key);
                try out.append(alloc, .{
                    .index_name = owned_index_name,
                    .doc_key = owned_doc_key,
                    .outcome = outcome,
                });
            }
            return true;
        }

        pub fn appendPreparedGraphEdgeArtifactWrite(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            write: types.GraphEdgeWrite,
            generation: u64,
            ttl_duration_ns: u64,
            timestamp_ns: u64,
        ) !void {
            const key = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, if (write.owner_document.len > 0) write.owner_document else if (write.owner.len > 0) write.owner else write.source, write.index_name, write.edge_type, write.target, write.source, write.edge_id);
            defer alloc.free(key);
            const payload = try encodeGraphEdgeArtifactWithTtlAlloc(alloc, store, key, generation, ttl_duration_ns, timestamp_ns, write);
            var payload_owned = true;
            errdefer if (payload_owned) alloc.free(payload);
            const owned_key = try alloc.dupe(u8, key);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(owned_key);
            try artifact_writes.append(alloc, .{
                .key = owned_key,
                .value = payload,
            });
            key_owned = false;
            payload_owned = false;
        }

        pub fn appendRelationItemsFromPath(
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
            index_name: []const u8,
            doc_key: []const u8,
            doc_value: ?std.json.Value,
            root: std.json.Value,
            path: []const u8,
            mapping: index_manager_mod.GraphArtifactMapping,
            artifact_name: []const u8,
            artifact_content_type: []const u8,
            artifact_value: std.json.Value,
            edge_limit: usize,
        ) !void {
            if (path.len == 0 or std.mem.eql(u8, path, "$")) return appendRelationValueItems(alloc, writes, index_name, doc_key, doc_value, root, mapping, artifact_name, artifact_content_type, artifact_value, edge_limit);
            const selected = selectGraphArtifactPath(root, path) orelse return;
            try appendRelationValueItems(alloc, writes, index_name, doc_key, doc_value, selected, mapping, artifact_name, artifact_content_type, artifact_value, edge_limit);
        }

        pub fn appendRelationValueItems(
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
            index_name: []const u8,
            doc_key: []const u8,
            doc_value: ?std.json.Value,
            value: std.json.Value,
            mapping: index_manager_mod.GraphArtifactMapping,
            artifact_name: []const u8,
            artifact_content_type: []const u8,
            artifact_value: std.json.Value,
            edge_limit: usize,
        ) !void {
            if (value == .array) {
                for (value.array.items, 0..) |item, i| try appendRelationItem(alloc, writes, index_name, doc_key, doc_value, item, i, mapping, artifact_name, artifact_content_type, artifact_value, edge_limit);
            } else {
                try appendRelationItem(alloc, writes, index_name, doc_key, doc_value, value, 0, mapping, artifact_name, artifact_content_type, artifact_value, edge_limit);
            }
        }

        pub fn appendRetiredDirectGraphTtlDueDeletes(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            writes: []const docstore_mod.KVPair,
            deletes: []const []const u8,
            store_deletes: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
        ) !void {
            // Callers append to the same list supplying deletes. Preserve its slice
            // table before growth can invalidate the borrowed input allocation.
            const input_deletes = try alloc.dupe([]const u8, deletes);
            defer alloc.free(input_deletes);
            var due_writes = std.StringHashMapUnmanaged(void).empty;
            defer due_writes.deinit(alloc);
            for (writes) |write| try due_writes.put(alloc, write.key, {});
            var due_deletes = std.StringHashMapUnmanaged(void).empty;
            defer due_deletes.deinit(alloc);
            for (owned_delete_keys.items) |key| try due_deletes.put(alloc, key, {});
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            var artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer artifact_keys.deinit(alloc);
            for (writes) |write| {
                if (!internal_keys.isGraphEdgeArtifactKey(write.key)) continue;
                const gop = try seen.getOrPut(alloc, write.key);
                if (!gop.found_existing) try artifact_keys.append(alloc, write.key);
            }
            for (input_deletes) |key| {
                if (!internal_keys.isGraphEdgeArtifactKey(key)) continue;
                const gop = try seen.getOrPut(alloc, key);
                if (!gop.found_existing) try artifact_keys.append(alloc, key);
            }
            for (artifact_keys.items) |key| {
                const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, key)) orelse continue;
                defer {
                    alloc.free(parsed.doc_key);
                    alloc.free(parsed.index_name);
                    alloc.free(parsed.edge_type);
                    alloc.free(parsed.target_doc_key);
                    alloc.free(parsed.edge_id);
                    if (parsed.source_node) |source| alloc.free(source);
                }
                const entry = index_manager.graphIndex(parsed.index_name) orelse continue;
                if (entry.ttl_duration_ns == 0 or index_manager.graphArtifactSources(parsed.index_name).len != 0) continue;
                const old_raw = store.get(alloc, key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                defer alloc.free(old_raw);
                var old_edge = enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, old_raw) catch continue;
                defer old_edge.deinit(alloc);
                if (old_edge.ttl_created_ns == 0) continue;
                const deadline = std.math.add(u64, old_edge.ttl_created_ns, entry.ttl_duration_ns) catch std.math.maxInt(u64);
                const due_key = try graph_edge_ttl_expiration.directIndexKeyAlloc(alloc, deadline, key);
                var due_key_unowned = true;
                errdefer if (due_key_unowned) alloc.free(due_key);
                if (due_writes.contains(due_key) or due_deletes.contains(due_key)) {
                    alloc.free(due_key);
                    continue;
                }
                try due_deletes.put(alloc, due_key, {});
                try owned_delete_keys.append(alloc, due_key);
                due_key_unowned = false;
                try store_deletes.append(alloc, due_key);
            }
            // A document delete can also retire producer contenders directly. Their
            // deadline keys live outside the document range and must join the same
            // primary mutation instead of waiting for the worker's stale-key prune.
            for (input_deletes) |key| {
                if (!internal_keys.isGraphGlobalEdgeContenderKey(key)) continue;
                const raw = store.get(alloc, key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                defer alloc.free(raw);
                const generation = graph_edge_contender.coverageGeneration(raw) catch continue;
                const contender = (graph_edge_contender.decode(raw, generation) catch continue) orelse continue;
                var duration_ns: u64 = 0;
                for (index_manager.graph_indexes.items) |entry| {
                    if (internal_keys.matchesGraphGlobalEdgeContenderIndexName(key, entry.config.name)) {
                        duration_ns = entry.ttl_duration_ns;
                        break;
                    }
                }
                if (duration_ns == 0) continue;
                var edge = enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, contender.payload) catch continue;
                defer edge.deinit(alloc);
                if (edge.ttl_created_ns == 0) continue;
                const deadline = std.math.add(u64, edge.ttl_created_ns, duration_ns) catch std.math.maxInt(u64);
                const due_key = try graph_edge_ttl_expiration.indexKeyAlloc(alloc, deadline, key);
                var due_key_unowned = true;
                errdefer if (due_key_unowned) alloc.free(due_key);
                if (due_writes.contains(due_key) or due_deletes.contains(due_key)) {
                    alloc.free(due_key);
                    continue;
                }
                try due_deletes.put(alloc, due_key, {});
                try owned_delete_keys.append(alloc, due_key);
                due_key_unowned = false;
                try store_deletes.append(alloc, due_key);
            }
        }

        pub fn appendSparseEmbeddingArtifactWrite(
            alloc: Allocator,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            base_key: []const u8,
            artifact_name: []const u8,
            origin: EmbeddingArtifactOrigin,
            indices: []const u32,
            values: []const f32,
        ) ![]u8 {
            const key = if (internal_keys.isInternalUserKey(base_key))
                try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, base_key, artifact_name)
            else
                try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, base_key, artifact_name);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);
            const payload = switch (origin) {
                .authored => try enrichment_artifact_codec.encodeAuthoredSparseEmbeddingAlloc(alloc, indices, values),
                .generated => |hash| try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, hash, indices, values),
            };
            var payload_owned = true;
            errdefer if (payload_owned) alloc.free(payload);
            const artifact_key = try alloc.dupe(u8, key);
            errdefer alloc.free(artifact_key);
            try artifact_writes.append(alloc, .{
                .key = key,
                .value = payload,
            });
            key_owned = false;
            payload_owned = false;
            return artifact_key;
        }

        pub fn appendStaleChunkArtifactDeleteKeys(
            alloc: Allocator,
            db: anytype,
            doc_key: []const u8,
            artifact_name: []const u8,
            desired_chunk_keys: []const []u8,
            artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
        ) !void {
            const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "chunk", artifact_name);
            defer alloc.free(prefix);
            var desired = std.StringHashMapUnmanaged(void).empty;
            defer desired.deinit(alloc);
            for (desired_chunk_keys) |key| try desired.put(alloc, key, {});
            var deleted = std.StringHashMapUnmanaged(void).empty;
            defer deleted.deinit(alloc);
            for (artifact_delete_keys.items) |key| try deleted.put(alloc, key, {});

            var cursor: ?[]u8 = null;
            defer if (cursor) |key| alloc.free(key);
            while (true) {
                const existing = try db.core.store.scanPrefixKeysPage(alloc, prefix, cursor, 256);
                defer freeOwnedKeySlice(alloc, existing);
                if (existing.len == 0) break;
                for (existing) |entry| {
                    // A chunk producer may finish before a deferred embedding provider.
                    // Keep derived embeddings until their own consumer has successfully
                    // prepared the replacement in this commit or a later replay window.
                    if (internal_keys.isDerivedEmbeddingArtifactKey(entry) or desired.contains(entry) or deleted.contains(entry)) continue;
                    const key = try alloc.dupe(u8, entry);
                    errdefer alloc.free(key);
                    try deleted.put(alloc, key, {});
                    errdefer _ = deleted.remove(key);
                    try artifact_delete_keys.append(alloc, key);
                }
                const next_cursor = try alloc.dupe(u8, existing[existing.len - 1]);
                if (cursor) |key| alloc.free(key);
                cursor = next_cursor;
                if (existing.len < 256) break;
            }
        }

        pub fn appendStalePrecomputedChunkEmbeddingDeletes(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
            pending_writes: *const PendingArtifactWriteIndex,
            chunk_deletes: []const []const u8,
            artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
            inline_cleanup: *InlineChunkEmbeddingCleanup,
        ) !void {
            if (!requestUsesChunkSource(request)) return;

            if (requestUsesPinnedMaterializedChunkArtifact(request)) {
                // A materialized producer owns chunk deletion. Once its embedding
                // consumer has prepared successfully, retire only embeddings whose
                // source disappears in this commit. A replacement write wins over a
                // delete of the same chunk key.
                const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, request.doc_key, "chunk", requestArtifactName(request));
                defer alloc.free(prefix);
                for (chunk_deletes) |key| {
                    if (!std.mem.startsWith(u8, key, prefix) or
                        !internal_keys.matchesChunkArtifactName(key, requestArtifactName(request))) continue;
                    if (pending_writes.get(key)) |value| {
                        const text = try chunkPayloadTextAlloc(alloc, value, request.source_field);
                        defer if (text) |owned| alloc.free(owned);
                        if (text != null) continue;
                    }
                    try artifact_delete_keys.append(alloc, try embeddingArtifactKeyForBaseAlloc(alloc, key, requestEmbeddingName(request)));
                }
                return;
            }

            try inline_cleanup.add(alloc, db, doc_value, request, cache);
        }

        pub fn appendStoredFullTextDocument(
            alloc: Allocator,
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            key: []const u8,
            text_indexes: []const []const u8,
        ) !void {
            if (text_indexes.len == 0) return;
            const targets = try fullTextTargetRefsAlloc(alloc, text_indexes);
            errdefer {
                for (targets) |target| alloc.free(target.index_name);
                alloc.free(targets);
            }
            try documents.append(alloc, .{
                .key = try alloc.dupe(u8, key),
                .action = .upsert,
                .targets = targets,
            });
        }

        pub fn appendUniqueBorrowedKeyWithSet(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged([]const u8),
            seen: *std.StringHashMapUnmanaged(void),
            key: []const u8,
        ) !void {
            if (key.len == 0) return;
            // Preserve the normal insertion/probing path. At capacity, getOrPut can
            // fail while trying to grow even for an existing key; a duplicate still
            // needs no additional storage and may succeed under that pressure.
            const entry = seen.getOrPut(alloc, key) catch |err| {
                if (seen.contains(key)) return;
                return err;
            };
            if (entry.found_existing) return;
            errdefer _ = seen.remove(key);
            try out.append(alloc, key);
        }

        pub fn appendUniqueOwnedConstKeyIndexed(
            alloc: Allocator,
            list: *std.ArrayListUnmanaged([]const u8),
            set: *std.StringHashMapUnmanaged(void),
            key: []const u8,
        ) !void {
            if (set.contains(key)) return;
            const owned = try alloc.dupe(u8, key);
            errdefer alloc.free(owned);
            try list.append(alloc, owned);
            errdefer _ = list.pop();
            try set.put(alloc, owned, {});
        }

        pub fn appendUniqueOwnedKeyIndexed(
            alloc: Allocator,
            list: *std.ArrayListUnmanaged([]u8),
            set: *std.StringHashMapUnmanaged(void),
            key: []const u8,
        ) !void {
            if (set.contains(key)) return;
            const owned = try alloc.dupe(u8, key);
            errdefer alloc.free(owned);
            try list.append(alloc, owned);
            errdefer _ = list.pop();
            try set.put(alloc, owned, {});
        }

        pub fn appendUniqueReplayRecordHint(
            alloc: Allocator,
            list: *std.ArrayListUnmanaged(change_journal_mod.TargetHint),
            hint: change_journal_mod.TargetHint,
        ) !void {
            for (list.items) |existing| {
                if (existing == hint) return;
            }
            try list.append(alloc, hint);
        }

        pub fn appendUniqueReplayRecordKeyWithSet(
            alloc: Allocator,
            list: *std.ArrayListUnmanaged([]const u8),
            seen: *std.StringHashMapUnmanaged(void),
            key: []const u8,
        ) !void {
            if (key.len == 0) return;
            if (seen.contains(key)) return;
            const owned = try alloc.dupe(u8, key);
            errdefer alloc.free(owned);
            try seen.put(alloc, owned, {});
            errdefer _ = seen.remove(owned);
            try list.append(alloc, owned);
        }

        pub fn applyArtifactRepairability(alloc: Allocator, issue: *types.ArtifactRepairIssue) !void {
            issue.repairable = artifactRepairReasonHasAutomatedReprocessor(issue.artifact_kind, issue.reason);
            const desired = if (issue.repairable) "" else artifactRepairUnsupportedReasonForIssue(issue.*);
            if (std.mem.eql(u8, issue.unsupported_reason, desired)) return;
            const owned = if (desired.len > 0) try alloc.dupe(u8, desired) else "";
            if (issue.unsupported_reason.len > 0) alloc.free(@constCast(issue.unsupported_reason));
            issue.unsupported_reason = owned;
        }

        pub fn applyDerivedBacklogPressureContext(ctx: *const BatchExecutionContext, sequence: u64, sync_level: types.SyncLevel, sync_targets: ManagedSyncTargets) !void {
            if (!syncLevelParticipatesInDerivedBacklogPressure(sync_level)) return;
            const throttle_target = ctx.executor.backlogThrottleTargetSequence() orelse return;
            _ = sync_targets;
            // Retention capacity was reserved before the source WAL commit. Crossing
            // the sequence or soft-byte watermark is therefore an urgency signal for
            // the derived queue, never permission to make the committed HTTP request
            // perform corpus-scale HBC catch-up. Explicit `.full_text`/`.full_index`
            // visibility is still enforced by waitForSyncLevel below.
            if (shouldDeferBacklogPressureForExternalDenseBulk(ctx, sync_level)) return;
            ctx.executor.forceSequence(@min(sequence, throttle_target));
        }

        pub fn applyDerivedBatchContextProfiled(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch, profile: ?*BatchProfile) !void {
            try applyDerivedBatchTargetsContextProfiled(ctx, batch, &.{}, profile);
        }

        pub fn applyDerivedBatchProfiled(self: anytype, batch: derived_types.DerivedBatch, profile: ?*BatchProfile) !void {
            var ctx = self.batchContext();
            try applyDerivedBatchContextProfiled(&ctx, batch, profile);
            if (self.text_merge_runtime) |runtime| {
                runtime.notify();
            }
            if (self.sparse_compaction_runtime) |runtime| runtime.notify();
        }

        pub fn applyDerivedBatchTargetsContextProfiled(ctx: *const BatchExecutionContext, batch: derived_types.DerivedBatch, index_names: []const []const u8, profile: ?*BatchProfile) !void {
            const managed_indexes = try ctx.index_manager.managedIndexes(ctx.alloc);
            defer {
                for (managed_indexes) |index_ref| ctx.alloc.free(@constCast(index_ref.name));
                ctx.alloc.free(managed_indexes);
            }
            var updates = std.ArrayListUnmanaged(apply_state.AppliedSequenceUpdate).empty;
            defer updates.deinit(ctx.alloc);
            for (managed_indexes) |index_ref| {
                if (index_names.len != 0 and !indexNameInSlice(index_ref.name, index_names)) continue;
                if (try batchAdvancesManagedIndexApplyState(ctx.index_manager, batch, index_ref)) {
                    const async_ctx = AsyncContext{
                        .alloc = ctx.alloc,
                        .io = ctx.io,
                        .store = ctx.store,
                        .applied_sequence_checkpoint_path = ctx.applied_sequence_checkpoint_path,
                        .index_manager = ctx.index_manager,
                        .apply_mutex = ctx.apply_mutex,
                        .snapshot_replay_admission = ctx.snapshot_replay_admission,
                        .dense_bulk_session_scope = ctx.dense_bulk_session_scope,
                        .text_merge_runtime = if (ctx.async_context) |active| active.text_merge_runtime else null,
                    };
                    try applyDerivedBatchToIndexContextProfiled(&async_ctx, batch, index_ref, profile, false, null);
                    const index_sync_start_ns = monotonicTimeNs();
                    try ctx.index_manager.syncReplayStateByName(ctx.store, index_ref.name);
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.index_sync_ns, index_sync_start_ns);
                    try updates.append(ctx.alloc, .{
                        .index_name = index_ref.name,
                        .sequence = batch.sequence,
                    });
                }
            }
            const applied_sequence_start_ns = monotonicTimeNs();
            try saveAppliedSequencesBatchContext(ctx, updates.items);
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.applied_sequence_save_ns, applied_sequence_start_ns);
            const truncate_start_ns = monotonicTimeNs();
            try truncateReplayJournalIfSafeContext(ctx);
            if (profile) |active_profile| recordProfileNs(profile, &active_profile.replay_journal_truncate_ns, truncate_start_ns);
        }

        pub fn applyDerivedBatchTargetsProfiled(self: anytype, batch: derived_types.DerivedBatch, index_names: []const []const u8, profile: ?*BatchProfile) !void {
            var ctx = self.batchContext();
            try applyDerivedBatchTargetsContextProfiled(&ctx, batch, index_names, profile);
            if (self.text_merge_runtime) |runtime| {
                runtime.notify();
            }
            if (self.sparse_compaction_runtime) |runtime| runtime.notify();
        }

        pub fn applyDerivedBatchToIndexContext(ctx: *const AsyncContext, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !void {
            try applyDerivedBatchToIndexContextProfiled(ctx, batch, index_ref, null, false, null);
        }

        pub fn applyDerivedBatchToIndexContextProfiled(
            ctx: *const AsyncContext,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
            profile: ?*BatchProfile,
            borrow_active_source_capture: bool,
            admitted_replay: ?*const snapshot_admission_mod.SnapshotAdmission.MutationLease,
        ) !void {
            // Generated files and their publication metadata are one physical
            // generation. Native capture takes the exclusive side of this admission
            // while copying, so no async worker may rewrite an artifact mid-copy.
            var snapshot_replay = if (admitted_replay) |lease| blk: {
                std.debug.assert(lease.admission == ctx.snapshot_replay_admission);
                std.debug.assert(lease.active);
                // The callback keeps its retained lease alive for this entire call.
                // Borrow it directly instead of adding another shared-count round trip.
                break :blk @as(?snapshot_admission_mod.SnapshotAdmission.MutationLease, null);
            } else try acquireSnapshotReplayAsyncContext(ctx);
            defer if (snapshot_replay) |*lease| lease.release();
            if (index_ref.kind == .full_text) {
                const apply_start_ns = monotonicTimeNs();
                const text_replay_options: index_manager_mod.IndexBatchOptions = .{
                    .compact_text = false,
                    .compact_text_segment_threshold = null,
                    .defer_text_compaction = true,
                };
                var publication_context = try ctx.index_manager.acquireTextPublicationContext(ctx.alloc, index_ref.name);
                defer publication_context.deinit();
                if (!ctx.projection_only) try invalidateLocalArtifactProducerCoverage(ctx, batch, index_ref);
                // A document that can never gain visible content (issue #938: an
                // `_edges`-only document reconstructed by replay as a synthetic
                // full-text candidate) would otherwise make this exact window retry
                // error.ReplayDocumentNotVisible forever. Past a bounded number of
                // consecutive failures here, give up on the still-missing documents
                // in this window instead of blocking the worker -- and anything
                // waiting on it to drain, like Lite's run_until_idle() -- forever.
                // Keyed by this owner's IndexManager address (PR #957 review on the
                // #938 escalation commit): the ResourceManager below is shared
                // process-wide across every storage owner, and same-named indexes
                // (the default `full_text_index_v0` is the common case) or
                // coincidentally equal sequence numbers across owners must not
                // combine or reset each other's retry counts.
                const replay_owner = resource_manager_mod.replayOwnerIdFromPtr(ctx.index_manager);
                const tolerate_missing_replay_documents = if (ctx.index_manager.resource_manager) |manager|
                    manager.shouldEscalateReplayDocumentNotVisible(replay_owner, index_ref.name, batch.sequence)
                else
                    false;
                var collected = try collectTextDocumentWritesForIndex(
                    ctx.alloc,
                    ctx.store,
                    ctx.index_manager,
                    batch.documents,
                    index_ref.name,
                    publication_context.chunk_backed,
                    ctx.index_manager.byte_range,
                    .{
                        .prefer_inline_when_store_tip_matches_sequence = batch.sequence,
                        .relational_base_rows = ctx.relational_base_rows,
                        .tolerate_missing_replay_documents = tolerate_missing_replay_documents,
                    },
                );
                defer collected.deinit();
                if (collected.missing_required != 0) return error.ReplayDocumentNotVisible;
                if (ctx.index_manager.resource_manager) |manager|
                    manager.clearReplayDocumentNotVisibleEscalation(replay_owner, index_ref.name, batch.sequence);

                const reservation_limit = if (ctx.text_merge_runtime) |runtime| runtime.producerSegmentReservationLimit() else std.math.maxInt(usize);
                var write_start: usize = 0;
                var applied_first_chunk = false;
                while (!applied_first_chunk or write_start < collected.docs.items.len) {
                    const plan_base = write_start;
                    var publication_plan = try ctx.index_manager.planTextMapperDocsPublication(
                        index_ref.name,
                        &publication_context,
                        collected.docs.items[plan_base..],
                        reservation_limit,
                    );
                    defer publication_plan.deinit();

                    var relative_start: usize = 0;
                    var replan_suffix = false;
                    for (publication_plan.chunks) |planned| {
                        const write_end = plan_base + planned.end;
                        const write_chunk = collected.docs.items[plan_base + relative_start .. write_end];
                        var applied_chunk = false;
                        var merge_permit: ?text_merge_runtime_mod.TextMergeRuntime.ProducerPermit = null;
                        if (ctx.text_merge_runtime) |runtime| {
                            merge_permit = try runtime.acquireProducerPermit(
                                index_ref.name,
                                planned.estimate.segment_count,
                                planned.estimate.byte_count,
                            );
                        }
                        defer if (merge_permit) |*permit| permit.release();

                        // Capacity is reserved before retaining immutable segment
                        // bytes. Projection, tokenization, sorting, and encoding run
                        // without the per-index apply guard; the read-only catalog and
                        // analysis lease inside preparation pins every borrowed config
                        // pointer for the duration of the build.
                        const prepare_start_ns = monotonicTimeNs();
                        var prepared_publication = ctx.index_manager.prepareTextMapperDocsPublication(
                            ctx.alloc,
                            ctx.store,
                            index_ref.name,
                            &publication_context,
                            write_chunk,
                        ) catch |err| switch (err) {
                            error.TextProjectionChanged => {
                                if (profile) |active_profile| recordProfileNs(profile, &active_profile.full_text_prepare_ns, prepare_start_ns);
                                replan_suffix = true;
                                break;
                            },
                            else => return err,
                        };
                        if (profile) |active_profile| recordProfileNs(profile, &active_profile.full_text_prepare_ns, prepare_start_ns);
                        defer prepared_publication.deinit();

                        const apply_lock_wait_start_ns = monotonicTimeNs();
                        var index_apply_guard = try ctx.index_manager.lockManagedIndexApply(index_ref);
                        if (profile) |active_profile| recordProfileNs(profile, &active_profile.full_text_apply_lock_wait_ns, apply_lock_wait_start_ns);
                        const apply_lock_held_start_ns = monotonicTimeNs();
                        defer {
                            index_apply_guard.unlock();
                            if (profile) |active_profile| recordProfileNs(profile, &active_profile.full_text_apply_lock_held_ns, apply_lock_held_start_ns);
                        }

                        // Admission may wait without holding catalog/apply locks. A
                        // same-instance schema or analyzer update invalidates only
                        // the unpublished suffix, which is replanned below. An index
                        // replacement still fails closed through IndexNotFound.
                        const before_apply = try ctx.index_manager.refreshTextPublicationContextAssumeCatalogLocked(
                            index_ref.name,
                            &publication_context,
                        );
                        if (before_apply == .projection_changed) {
                            replan_suffix = true;
                        } else {
                            const delete_keys = if (applied_first_chunk)
                                &.{}
                            else
                                try collectTextReplayDeleteKeys(
                                    ctx.alloc,
                                    ctx.index_manager,
                                    batch,
                                    index_ref.name,
                                    publication_context,
                                );
                            defer if (delete_keys.len > 0) ctx.alloc.free(delete_keys);
                            try ctx.index_manager.applyPreparedTextMapperPublicationByNameWithOptions(
                                ctx.store,
                                index_ref.name,
                                delete_keys,
                                &prepared_publication,
                                text_replay_options,
                            );
                            applied_chunk = true;
                            const after_apply = try ctx.index_manager.refreshTextPublicationContextAssumeCatalogLocked(
                                index_ref.name,
                                &publication_context,
                            );
                            replan_suffix = after_apply == .projection_changed;
                            if (ctx.text_merge_runtime) |runtime| runtime.notify();
                        }

                        if (!applied_chunk) break;
                        applied_first_chunk = true;
                        write_start = write_end;
                        relative_start = planned.end;
                        if (replan_suffix and write_start < collected.docs.items.len) break;
                    }

                    if (write_start == collected.docs.items.len) break;
                    std.debug.assert(replan_suffix);
                }
                std.debug.assert(applied_first_chunk and write_start == collected.docs.items.len);
                if (profile) |active_profile| recordProfileNs(profile, &active_profile.full_text_apply_ns, apply_start_ns);
                return;
            }

            if (builtin.is_test and index_ref.kind == .graph) if (D.test_before_graph_replay_apply.*) |hook| hook.call(hook.ctx);
            // Enter before the catalog lease: imports may need catalog readers while
            // holding exclusive publication. A queued worker must not pin the catalog
            // while waiting for that import to finish.
            var graph_publication = if (index_ref.kind == .graph)
                ctx.index_manager.beginGraphSourceReplay()
            else
                null;
            defer if (graph_publication) |*lease| lease.release();
            if (index_ref.kind == .graph and ctx.primary_replication_append_pending.load(.acquire)) return error.ReplicationPublisherUnavailable;
            var index_apply_guard = try ctx.index_manager.lockManagedIndexApply(index_ref);
            defer index_apply_guard.unlock();
            if (index_ref.kind == .graph) {
                // Acquire the catalog/apply guard before checking publication. A
                // queued callback may have waited while import replaced the graph.
                if (ctx.index_manager.graph_artifact_rebuild_pending) return error.GraphMaintenanceInProgress;
                const entry = ctx.index_manager.graphIndex(index_ref.name) orelse return error.IndexNotFound;
                if (batch.sequence != 0 and batch.sequence <= try entry.index.artifactRebuildSequence()) return;
            }
            switch (index_ref.kind) {
                .full_text => unreachable,
                .dense_vector => {
                    // A native publication may certify a newer source tip
                    // before a lagging journal cursor reaches it. Under the
                    // per-index apply guard, older windows are already covered
                    // and must not roll back vectors or open a capture below
                    // its committed base. Sequence zero remains unversioned.
                    if (batch.sequence != 0 and ctx.index_manager.densePostingWalAuthoritativeByName(index_ref.name)) {
                        const covered = ctx.index_manager.denseIndex(index_ref.name).?.index.experimentalPostingDurableAppliedSequence() orelse 0;
                        if (batch.sequence < covered) return;
                    }
                    const dense_apply_start_ns = monotonicTimeNs();
                    const dense_finish_options = denseCatchUpFinishOptions();
                    const borrow_source_capture = borrow_active_source_capture;
                    const posting_capture = try ctx.index_manager.beginDensePostingSidecarCaptureLeaseByNameWithOptions(
                        index_ref.name,
                        .{ .borrow_active_source = borrow_source_capture },
                    );
                    var posting_capture_owned = if (posting_capture) |lease| lease.ownsLifecycle() else false;
                    errdefer if (posting_capture_owned) if (posting_capture) |lease| {
                        ctx.index_manager.cancelDensePostingSidecarCaptureLeaseByName(index_ref.name, lease) catch |err| {
                            if (err != error.PostingWalCaptureSuperseded and
                                err != error.ExperimentalPostingCaptureNotActive)
                            {
                                std.log.err("dense posting capture abort failed index={s} err={s}", .{ index_ref.name, @errorName(err) });
                            }
                        };
                    };
                    if (posting_capture) |lease| {
                        try ctx.index_manager.recordDensePostingCaptureMutationSequence(index_ref.name, lease, batch.sequence);
                    }
                    // A borrowed source capture already owns the HBC streaming
                    // session even when this apply uses a short-lived context whose
                    // counters are empty. Never nest a local session inside it.
                    const use_local_streaming_session = !borrow_source_capture and
                        denseApplyUsesLocalStreamingSession(ctx, index_ref.name);
                    var dense_streaming_session_open = false;
                    if (use_local_streaming_session) {
                        try ctx.index_manager.beginDenseStreamingReplaySessionByName(index_ref.name);
                        dense_streaming_session_open = true;
                        errdefer if (dense_streaming_session_open) ctx.index_manager.abortDenseStreamingReplaySessionByName(index_ref.name);
                    }
                    const before_hbc_profile = if (profile != null) ctx.index_manager.denseWriteProfileByName(index_ref.name) else null;
                    const batch_options: backend_types.BatchOptions = .{ .mode = .bulk_ingest };
                    var dense_embeddings = try collectDenseEmbeddingWritesForBatch(
                        ctx.alloc,
                        ctx.index_manager,
                        batch.dense_embeddings,
                        batch.changed_artifact_keys,
                        index_ref.name,
                    );
                    defer dense_embeddings.deinit();
                    try filterDeletedEmbeddingWrites(&dense_embeddings, batch.deleted_keys);
                    if (!ctx.projection_only) {
                        if (ctx.index_manager.denseIndex(index_ref.name)) |entry| {
                            try filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay(ctx, index_ref.name, entry.dims, &dense_embeddings, batch.sequence);
                        }
                    }
                    const dense_delete_start_ns = monotonicTimeNs();
                    const replay_delete_keys = try collectVectorReplayDeleteKeys(
                        ctx.alloc,
                        ctx.index_manager,
                        index_ref,
                        batch.deleted_keys,
                    );
                    defer freeOwnedKeySlice(ctx.alloc, replay_delete_keys);
                    // Exact-member upsert retains vector identity and replaces changed
                    // payloads in one mutation. Only real removals enter the delete lane;
                    // pre-deleting replacements publishes artificial cardinality gaps.
                    try ctx.index_manager.deleteDenseBatchByNameWithOptions(ctx.store, index_ref.name, replay_delete_keys, batch_options);
                    // Dense apply is already an exact-member upsert: it retains the
                    // existing vector id, skips identical payloads, and replaces a
                    // changed payload in one HBC mutation batch. A separate pre-delete
                    // would publish an artificial lower-cardinality checkpoint from a
                    // streaming replay session before the upsert restores the member.
                    // Reserve the delete lane for actual source/artifact removals.
                    const chunk_backed = if (ctx.index_manager.denseIndex(index_ref.name)) |entry|
                        entry.chunk_name != null
                    else
                        false;
                    // Chunk-backed vectors have deterministic artifact identities and
                    // a complete stale-chunk delete lifecycle. Erasing every vector
                    // for an overwritten parent races asynchronous enrichment replay,
                    // causes needless HBC churn, and can leave a ready chunked index
                    // with only a partial physical corpus. Non-chunked generated and
                    // inline vectors still require parent-wide replacement to retire
                    // provisional or previous identities.
                    if (!chunk_backed) {
                        try ctx.index_manager.deleteDenseBatchByNameWithOptions(ctx.store, index_ref.name, batch.overwritten_doc_keys, batch_options);
                    }
                    if (!ctx.projection_only) {
                        try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.deleted_keys);
                        if (ctx.index_manager.denseIndexUsesManagedDirectField(index_ref.name) or
                            ctx.index_manager.denseIndexUsesExternalCoverage(index_ref.name))
                        {
                            try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.overwritten_doc_keys);
                        } else {
                            // Producer-owned exact outcomes are committed atomically with
                            // the source update. Only a request retained for asynchronous
                            // replay makes the prior generated outcome pending.
                            const pending_doc_keys = try pendingGeneratedCoverageDocKeysForIndexAlloc(
                                ctx.alloc,
                                ctx.index_manager,
                                index_ref.name,
                                .dense_vector,
                                batch.generated_enrichment_refs,
                            );
                            defer ctx.alloc.free(pending_doc_keys);
                            try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, pending_doc_keys);
                        }
                    }
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.dense_delete_ns, dense_delete_start_ns);

                    // Artifact-only replay has no documents to filter with this set.
                    var dense_embedding_doc_keys = if (batch.documents.len == 0)
                        std.StringHashMapUnmanaged(void).empty
                    else
                        try denseEmbeddingDocKeySet(ctx.alloc, dense_embeddings.writes);
                    defer dense_embedding_doc_keys.deinit(ctx.alloc);

                    var index_writes = try collectDocumentWritesProfiled(
                        ctx.alloc,
                        ctx.store,
                        ctx.index_manager,
                        batch.documents,
                        ctx.index_manager.byte_range,
                        .{
                            .skip_doc_keys = &dense_embedding_doc_keys,
                            .relational_base_rows = ctx.relational_base_rows,
                        },
                        null,
                    );
                    defer index_writes.deinit();
                    if (index_writes.missing_required != 0) return error.ReplayDocumentNotVisible;
                    const dense_doc_index_start_ns = monotonicTimeNs();
                    try ctx.index_manager.indexDenseBatchByNameWithOptions(ctx.store, index_ref.name, index_writes.items, batch_options);
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.dense_doc_index_ns, dense_doc_index_start_ns);

                    const dense_embedding_start_ns = monotonicTimeNs();
                    try ctx.index_manager.applyDenseEmbeddingWritesByNameWithOptions(ctx.store, index_ref.name, dense_embeddings.writes, batch_options);
                    if (!ctx.projection_only)
                        try accountDenseCoverage(ctx, index_ref.name, batch, dense_embeddings.writes);
                    if (profile) |active_profile| {
                        recordProfileNs(profile, &active_profile.dense_embedding_apply_ns, dense_embedding_start_ns);
                        if (before_hbc_profile) |before| {
                            if (ctx.index_manager.denseWriteProfileByName(index_ref.name)) |after| {
                                addHbcWriteProfileDelta(active_profile, before, after);
                            }
                        }
                    }
                    if (use_local_streaming_session) {
                        try ctx.index_manager.finishDenseStreamingReplaySessionByNameWithOptions(index_ref.name, dense_finish_options);
                        dense_streaming_session_open = false;
                        // A locally-owned source capture is one transaction with this
                        // locally-owned streaming session. Publish it before releasing
                        // the managed-index apply guard; otherwise the session depth
                        // reaches zero while the capture remains live until a later
                        // applied-watermark callback, and an enrichment/repair replay
                        // can neither join it nor make progress. Catch-up and external
                        // bulk windows own their capture outside this batch and take
                        // the `posting_capture_started == false` path.
                        if (posting_capture_owned) {
                            try ctx.index_manager.finishDensePostingSidecarCaptureLeaseByName(
                                index_ref.name,
                                posting_capture.?,
                                batch.sequence,
                            );
                            posting_capture_owned = false;
                        }
                    }
                    if (!ctx.projection_only)
                        try clearPublishedEmbeddingArtifactRepairIssuesContext(ctx, index_ref.name, dense_embeddings.writes, batch.sequence);
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.dense_apply_ns, dense_apply_start_ns);
                },
                .sparse_vector => {
                    const apply_start_ns = monotonicTimeNs();
                    const emit_sparse_write_profile = benchMetricsEnabled();
                    const before_sparse_profile = if (emit_sparse_write_profile) ctx.index_manager.sparseWriteProfileByName(index_ref.name) else null;
                    const batch_options: backend_types.BatchOptions = .{ .mode = .bulk_ingest };
                    var collect_doc_profile: CollectSparseFieldWritesProfile = .{};
                    var sparse_delete_ns: u64 = 0;
                    var sparse_collect_doc_ns: u64 = 0;
                    var sparse_doc_index_ns: u64 = 0;
                    var sparse_collect_embedding_ns: u64 = 0;
                    var sparse_embedding_apply_ns: u64 = 0;
                    const sparse_collect_embedding_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    var sparse_embeddings = try collectSparseEmbeddingWritesForBatch(
                        ctx.alloc,
                        ctx.index_manager,
                        batch.sparse_embeddings,
                        batch.changed_artifact_keys,
                        index_ref.name,
                    );
                    defer sparse_embeddings.deinit();
                    try filterDeletedEmbeddingWrites(&sparse_embeddings, batch.deleted_keys);
                    if (emit_sparse_write_profile) sparse_collect_embedding_ns = monotonicTimeNs() - sparse_collect_embedding_start_ns;
                    if (!ctx.projection_only)
                        try filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay(ctx, index_ref.name, &sparse_embeddings, batch.sequence);
                    try ctx.index_manager.validateSparseEmbeddingArtifactsByName(ctx.store, index_ref.name, sparse_embeddings.writes);

                    const sparse_delete_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    const replay_delete_keys = try collectVectorReplayDeleteKeys(
                        ctx.alloc,
                        ctx.index_manager,
                        index_ref,
                        batch.deleted_keys,
                    );
                    defer freeOwnedKeySlice(ctx.alloc, replay_delete_keys);
                    try ctx.index_manager.deleteSparseBatchByNameWithOptions(index_ref.name, replay_delete_keys, batch_options);
                    try ctx.index_manager.deleteSparseBatchByNameWithOptions(index_ref.name, batch.overwritten_doc_keys, batch_options);
                    if (!ctx.projection_only) {
                        try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.deleted_keys);
                        if (ctx.index_manager.sparseIndexUsesManagedDirectField(index_ref.name) or
                            ctx.index_manager.sparseIndexUsesExternalCoverage(index_ref.name))
                        {
                            try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.overwritten_doc_keys);
                        } else {
                            const pending_doc_keys = try pendingGeneratedCoverageDocKeysForIndexAlloc(
                                ctx.alloc,
                                ctx.index_manager,
                                index_ref.name,
                                .sparse_vector,
                                batch.generated_enrichment_refs,
                            );
                            defer ctx.alloc.free(pending_doc_keys);
                            try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, pending_doc_keys);
                        }
                    }
                    if (emit_sparse_write_profile) sparse_delete_ns = monotonicTimeNs() - sparse_delete_start_ns;

                    var sparse_embedding_doc_keys = if (batch.documents.len == 0)
                        std.StringHashMapUnmanaged(void).empty
                    else
                        try sparseEmbeddingDocKeySet(ctx.alloc, sparse_embeddings.writes);
                    defer sparse_embedding_doc_keys.deinit(ctx.alloc);

                    const sparse_collect_doc_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    const sparse_field_name = ctx.index_manager.sparseFieldNameByName(index_ref.name) orelse return error.IndexNotFound;
                    var index_writes = try collectSparseFieldWritesProfiled(
                        ctx.alloc,
                        ctx.store,
                        ctx.index_manager,
                        batch.documents,
                        ctx.index_manager.byte_range,
                        sparse_field_name,
                        .{
                            .prefer_inline_when_store_tip_matches_sequence = batch.sequence,
                            .prefer_available_inline_values = true,
                            .skip_doc_keys = &sparse_embedding_doc_keys,
                            .relational_base_rows = ctx.relational_base_rows,
                        },
                        if (emit_sparse_write_profile) &collect_doc_profile else null,
                    );
                    defer index_writes.deinit();
                    if (emit_sparse_write_profile) sparse_collect_doc_ns = monotonicTimeNs() - sparse_collect_doc_start_ns;
                    if (index_writes.missing_required != 0) return error.ReplayDocumentNotVisible;
                    const sparse_doc_index_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    try ctx.index_manager.indexSparsePreparedWritesByNameWithOptions(index_ref.name, index_writes.items, batch_options);
                    if (emit_sparse_write_profile) sparse_doc_index_ns = monotonicTimeNs() - sparse_doc_index_start_ns;

                    const sparse_embedding_apply_start_ns = if (emit_sparse_write_profile) monotonicTimeNs() else 0;
                    try ctx.index_manager.applySparseEmbeddingWritesByNameWithOptions(ctx.store, index_ref.name, sparse_embeddings.writes, batch_options);
                    if (!ctx.projection_only) {
                        try accountSparseCoverage(ctx, index_ref.name, batch, sparse_embeddings.writes);
                        try clearPublishedEmbeddingArtifactRepairIssuesContext(ctx, index_ref.name, sparse_embeddings.writes, batch.sequence);
                    }
                    if (emit_sparse_write_profile) sparse_embedding_apply_ns = monotonicTimeNs() - sparse_embedding_apply_start_ns;
                    if (before_sparse_profile) |before| {
                        if (ctx.index_manager.sparseWriteProfileByName(index_ref.name)) |after| {
                            logSparseWriteProfileDelta(index_ref.name, sparse_mod.WriteProfile.delta(after, before));
                        }
                    }
                    if (emit_sparse_write_profile) {
                        std.log.info(
                            "antfly_bench_sparse_doc_replay index={s} sequence={} documents={d} sparse_embeddings={d} total_ms={d} delete_ms={d} collect_doc_ms={d} collect_doc_scan_ms={d} collect_doc_sort_ms={d} collect_doc_read_ms={d} collect_doc_extract_ms={d} collect_doc_pending={d} collect_doc_output={d} collect_doc_store_hits={d} collect_doc_inline_hits={d} collect_doc_missing={d} collect_doc_no_vector={d} doc_index_ms={d} collect_embedding_ms={d} embedding_apply_ms={d}",
                            .{
                                index_ref.name,
                                batch.sequence,
                                batch.documents.len,
                                batch.sparse_embeddings.len,
                                nsToMs(monotonicTimeNs() - apply_start_ns),
                                nsToMs(sparse_delete_ns),
                                nsToMs(sparse_collect_doc_ns),
                                nsToMs(collect_doc_profile.scan_ns),
                                nsToMs(collect_doc_profile.sort_ns),
                                nsToMs(collect_doc_profile.read_ns),
                                nsToMs(collect_doc_profile.extract_ns),
                                collect_doc_profile.pending_documents,
                                collect_doc_profile.output_writes,
                                collect_doc_profile.store_hits,
                                collect_doc_profile.inline_hits,
                                collect_doc_profile.missing_required,
                                collect_doc_profile.skipped_without_vector,
                                nsToMs(sparse_doc_index_ns),
                                nsToMs(sparse_collect_embedding_ns),
                                nsToMs(sparse_embedding_apply_ns),
                            },
                        );
                    }
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.sparse_apply_ns, apply_start_ns);
                },
                .graph => {
                    const apply_start_ns = monotonicTimeNs();
                    if (!ctx.projection_only) try invalidateLocalArtifactProducerCoverage(ctx, batch, index_ref);
                    try ctx.index_manager.deleteGraphDocsByName(index_ref.name, batch.deleted_keys);
                    try applyGraphDocClearsForIndex(ctx, batch.graph_doc_clears, index_ref.name);

                    // Under ordered producer authority replay consumes committed graph
                    // effects only. Asset replay must not mutate primary graph state
                    // outside its Raft/native publication transaction. The asset
                    // producer retry plans its graph consumer and waits for acceptance.
                    const materialized_artifact_keys = if (ctx.allow_graph_materialization and !try orderedCoverageActive(ctx.store))
                        try materializeGraphSourceArtifactsForIndex(
                            ctx.alloc,
                            ctx.store,
                            ctx.index_manager,
                            batch.changed_artifact_keys,
                            index_ref.name,
                            .{
                                .require_resolution_contract = ctx.require_graph_resolution_contract,
                                .repair_ctx = ctx,
                                .sequence = batch.sequence,
                            },
                        )
                    else
                        try ctx.alloc.alloc([]u8, 0);
                    defer freeOwnedKeySlice(ctx.alloc, materialized_artifact_keys);

                    if (batch.graph_writes.len > 0 or batch.graph_deletes.len > 0) {
                        const graph_deletes = try collectGraphDeletes(ctx.alloc, batch.graph_deletes, index_ref.name);
                        defer if (graph_deletes.len > 0) ctx.alloc.free(graph_deletes);

                        const graph_writes = try collectGraphWrites(ctx.alloc, batch.graph_writes, index_ref.name);
                        defer if (graph_writes.len > 0) ctx.alloc.free(graph_writes);
                        try ctx.index_manager.applyGraphMutationsByName(index_ref.name, graph_writes, graph_deletes);
                    }
                    if (batch.changed_artifact_keys.len > 0 or materialized_artifact_keys.len > 0) {
                        try applyGraphArtifactMutationPages(
                            ctx,
                            index_ref.name,
                            batch.changed_artifact_keys,
                            materialized_artifact_keys,
                            batch.sequence,
                        );
                    }
                    if (profile) |active_profile| recordProfileNs(profile, &active_profile.graph_apply_ns, apply_start_ns);
                },
                .algebraic => {
                    if (ctx.projection_only) return error.InvalidShadowIndexKind;
                    try ctx.index_manager.applyAlgebraicBatchByNameWithOptions(ctx.store, index_ref.name, batch, .{ .mode = .bulk_ingest });
                },
            }
        }

        pub fn applyGraphArtifactMutationPages(
            ctx: *const AsyncContext,
            index_name: []const u8,
            primary_keys: []const []const u8,
            materialized_keys: []const []u8,
            sequence: u64,
        ) !void {
            const page_size: usize = 2048;
            var key_page: [page_size][]const u8 = undefined;
            var primary_pos: usize = 0;
            var materialized_pos: usize = 0;
            const expected_generation = (ctx.index_manager.graphIndex(index_name) orelse return error.IndexNotFound).config.coverage_generation;
            while (primary_pos < primary_keys.len or materialized_pos < materialized_keys.len) {
                var count: usize = 0;
                while (count < key_page.len and primary_pos < primary_keys.len) : (count += 1) {
                    key_page[count] = primary_keys[primary_pos];
                    primary_pos += 1;
                }
                while (count < key_page.len and materialized_pos < materialized_keys.len) : (count += 1) {
                    key_page[count] = materialized_keys[materialized_pos];
                    materialized_pos += 1;
                }
                var graph_mutations = try collectGraphMutationsForArtifacts(ctx.alloc, ctx.store, key_page[0..count], index_name, .{
                    .expected_generation = expected_generation,
                    .repair_ctx = if (ctx.projection_only) null else ctx,
                    .sequence = sequence,
                });
                defer graph_mutations.deinit();
                if (!ctx.projection_only and graph_mutations.generation_bindings.len > 0) {
                    try ctx.store.putBatch(graph_mutations.generation_bindings, &.{});
                }
                try ctx.index_manager.applyGraphMutationsByName(index_name, graph_mutations.writes, graph_mutations.deletes);
            }
        }

        pub fn applyGraphDocClearsForIndex(ctx: *const AsyncContext, clears: []const derived_types.DerivedGraphDocClear, index_name: []const u8) !void {
            for (clears) |clear| {
                for (clear.index_names) |clear_index_name| {
                    if (!std.mem.eql(u8, clear_index_name, index_name)) continue;
                    try ctx.index_manager.deleteGraphDocInIndexes(clear.key, &.{index_name});
                    break;
                }
            }
        }

        pub fn applyPrecomputeAssetProducerOutput(
            alloc: Allocator,
            db: anytype,
            item: PrecomputeAssetProducerBatchItem,
            produced: []const u8,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            coverage_outcomes: *std.ArrayListUnmanaged(PrecomputedCoverageOutcome),
        ) !void {
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, item.artifact_key),
                .value = try alloc.dupe(u8, produced),
            });
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, item.state_key),
                .value = try alloc.dupe(u8, item.state_value),
            });

            const text_indexes: []const []const u8 = item.request.consumer_indexes;
            try appendInlineFullTextDocument(alloc, documents, item.artifact_key, produced, text_indexes);
            try appendPrecomputedArtifactCoverageOutcomes(db, alloc, coverage_outcomes, item.request, .produced);
        }

        pub fn artifactKindFromInternalLabel(raw_kind: []const u8) !types.ArtifactKind {
            if (std.mem.eql(u8, raw_kind, "chunk")) return .chunk;
            if (std.mem.eql(u8, raw_kind, "asset")) return .asset;
            if (std.mem.eql(u8, raw_kind, "embedding")) return .embedding;
            return error.InvalidInternalUserKey;
        }

        pub fn artifactRepairIssueIdAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
            if (issue.artifact_key.len > 0) return try alloc.dupe(u8, issue.artifact_key);
            var hasher = std.crypto.hash.sha2.Sha256.init(.{});
            artifactRepairIssueIdHashString(&hasher, issue.doc_key);
            artifactRepairIssueIdHashString(&hasher, issue.parent_doc_key);
            artifactRepairIssueIdHashString(&hasher, issue.source_artifact_name);
            artifactRepairIssueIdHashString(&hasher, issue.artifact_name);
            artifactRepairIssueIdHashString(&hasher, issue.unit_id);
            artifactRepairIssueIdHashOptionalU64(&hasher, if (issue.chunk_id) |chunk_id| @as(u64, chunk_id) else null);
            artifactRepairIssueIdHashString(&hasher, @tagName(issue.artifact_kind));
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            hasher.final(&digest);
            const hex = try bytesToHexAlloc(alloc, &digest);
            defer alloc.free(hex);
            return try std.fmt.allocPrint(alloc, "tuple-sha256:{s}", .{hex});
        }

        pub fn artifactRepairIssueIdHashOptionalU64(hasher: *std.crypto.hash.sha2.Sha256, value: ?u64) void {
            hasher.update(if (value == null) "\x00" else "\x01");
            if (value) |raw| {
                var buf: [8]u8 = undefined;
                std.mem.writeInt(u64, &buf, raw, .little);
                hasher.update(&buf);
            }
        }

        pub fn artifactRepairIssueIdHashString(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
            var len_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &len_buf, value.len, .little);
            hasher.update(&len_buf);
            hasher.update(value);
        }

        pub fn artifactRepairIssueKindKeyForIssueAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
            const issue_id = try artifactRepairIssueIdAlloc(alloc, issue);
            defer alloc.free(issue_id);
            return try internal_keys.artifactRepairIssueKindKeyAlloc(alloc, @tagName(issue.artifact_kind), issue.index_name, issue_id);
        }

        pub fn artifactRepairKindHasAutomatedReprocessor(kind: types.ArtifactRepairKind) bool {
            return switch (kind) {
                .embedding, .asset, .chunk => true,
                .graph, .full_text, .algebraic => false,
            };
        }

        pub fn artifactRepairReasonHasAutomatedReprocessor(kind: types.ArtifactRepairKind, reason: types.ArtifactRepairReason) bool {
            return artifactRepairKindHasAutomatedReprocessor(kind) and reason != .resource_limit_exceeded;
        }

        pub fn artifactRepairUnsupportedReason(kind: types.ArtifactRepairKind) []const u8 {
            return switch (kind) {
                .embedding, .asset, .chunk => "",
                .graph => "graph_reprocessor_unavailable",
                .full_text => "full_text_reprocessor_unavailable",
                .algebraic => "algebraic_artifact_reprocessor_unavailable",
            };
        }

        pub fn artifactRepairUnsupportedReasonForIssue(issue: types.ArtifactRepairIssue) []const u8 {
            if (issue.reason == .resource_limit_exceeded) return "resource_limit_requires_source_or_config_change";
            return artifactRepairUnsupportedReason(issue.artifact_kind);
        }

        pub fn artifactSourcesContainGeneratedEnrichment(
            lookup: *const std.StringHashMapUnmanaged(void),
            names: []const []const u8,
        ) bool {
            for (names) |name| {
                if (lookup.contains(name)) return true;
            }
            return false;
        }

        pub fn assetStateKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
            var list = std.ArrayListUnmanaged(u8).empty;
            defer list.deinit(alloc);
            try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
            try list.append(alloc, internal_keys.asset_state_kind);
            try internal_keys.appendEncodedComponent(&list, alloc, artifact_name);
            return try list.toOwnedSlice(alloc);
        }

        pub fn assetStateValueAlloc(
            alloc: Allocator,
            source_text: []const u8,
            source_parts_json: ?[]const u8,
            producer_json: []const u8,
        ) ![]u8 {
            var hasher = std.crypto.hash.sha2.Sha256.init(.{});
            hasher.update(source_text);
            if (source_parts_json) |parts| hasher.update(parts);
            hasher.update(producer_json);
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            hasher.final(&digest);
            return try alloc.dupe(u8, &digest);
        }

        pub fn batchAdvancesManagedIndexApplyState(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) !bool {
            if (!batchAffectsManagedIndex(index_manager, batch, index_ref)) return false;

            switch (index_ref.kind) {
                .dense_vector, .sparse_vector => {
                    if (batch.deleted_keys.len > 0 or batch.overwritten_doc_keys.len > 0) return true;
                    const artifact_backed_dense = if (index_ref.kind == .dense_vector)
                        if (index_manager.denseIndex(index_ref.name)) |entry| denseIndexIsArtifactBacked(entry) else false
                    else
                        false;
                    if (!try index_manager.requiresEnrichmentReplay(index_ref.name) and !artifact_backed_dense) return true;
                    if (batch.changed_artifact_keys.len > 0) return true;
                    if (index_ref.kind == .dense_vector) {
                        for (batch.dense_embeddings) |embedding| {
                            if (std.mem.eql(u8, embedding.index_name, index_ref.name)) return true;
                        }
                        return false;
                    }
                    for (batch.sparse_embeddings) |embedding| {
                        if (std.mem.eql(u8, embedding.index_name, index_ref.name)) return true;
                    }
                    return false;
                },
                else => return true,
            }
        }

        pub fn batchAffectsManagedIndex(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) bool {
            return managedIndexBatchApplicability(index_manager, batch, index_ref) == .relevant;
        }

        pub fn batchHasEmbeddingArtifactForManagedIndex(
            index_manager: *index_manager_mod.IndexManager,
            index_ref: index_manager_mod.ManagedIndexRef,
            artifact_keys: []const []const u8,
        ) bool {
            const alloc = index_manager.alloc;
            for (artifact_keys) |artifact_key| {
                var identity = artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key) catch |err| switch (err) {
                    error.InvalidInternalUserKey => continue,
                    else => continue,
                } orelse continue;
                defer identity.deinit(alloc);
                if (managedIndexConsumesEmbeddingName(index_manager, index_ref, identity.embedding_name)) return true;
            }

            return false;
        }

        pub fn buildChunkArtifactPayloadAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
            source_field: []const u8,
            chunk: chunker_mod.Chunk,
            include_payload: bool,
        ) ![]u8 {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, try alloc.dupe(u8, "_parent_doc_key"), .{ .string = try alloc.dupe(u8, doc_key) });
            try obj.put(alloc, try alloc.dupe(u8, "_artifact_name"), .{ .string = try alloc.dupe(u8, artifact_name) });
            try obj.put(alloc, try alloc.dupe(u8, "_source_field"), .{ .string = try alloc.dupe(u8, source_field) });
            try chunk_artifact_mod.appendArtifactFields(alloc, &obj, source_field, chunk, include_payload);
            return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = obj }, .{});
        }

        pub fn buildDerivedBatch(
            alloc: Allocator,
            req: types.BatchRequest,
            extracted: []const mapper.ExtractedWrite,
            deleted_artifact_keys: []const []const u8,
            changed_artifact_keys: []const []u8,
        ) !derived_types.DerivedBatch {
            var documents = try alloc.alloc(derived_types.DerivedDocument, req.writes.len);
            var initialized: usize = 0;
            errdefer {
                for (documents[0..initialized]) |doc| derived_types.deinitDerivedDocument(alloc, doc);
                if (documents.len > 0) alloc.free(documents);
            }

            for (req.writes, 0..) |write, i| {
                var targets = std.ArrayListUnmanaged(derived_types.DerivedTargetRef).empty;
                defer targets.deinit(alloc);
                errdefer for (targets.items) |target| alloc.free(target.index_name);

                if (extracted[i].hasDocument()) {
                    try appendDerivedTargetRefAlloc(alloc, &targets, .full_text, "*");
                }
                for (extracted[i].dense_embeddings) |embedding| {
                    try appendDerivedTargetRefAlloc(alloc, &targets, .dense_vector, embedding.index_name);
                }
                for (extracted[i].sparse_embeddings) |embedding| {
                    try appendDerivedTargetRefAlloc(alloc, &targets, .sparse_vector, embedding.index_name);
                }
                for (extracted[i].mentioned_graph_indexes) |index_name| {
                    try appendDerivedTargetRefAlloc(alloc, &targets, .graph, index_name);
                }
                for (req.graph_writes) |graph_write| {
                    if (std.mem.eql(u8, graph_write.producingDocument(), write.key)) {
                        try appendDerivedTargetRefAlloc(alloc, &targets, .graph, graph_write.index_name);
                    }
                }

                const key = try alloc.dupe(u8, write.key);
                errdefer alloc.free(key);
                const owned_targets = try targets.toOwnedSlice(alloc);
                documents[i] = .{
                    .key = key,
                    .action = if (!extracted[i].hasDocument()) .preserve_base_document else .upsert,
                    .cleaned_value = null,
                    .targets = owned_targets,
                };
                initialized += 1;
            }

            var deleted_keys = try alloc.alloc([]const u8, req.deletes.len + deleted_artifact_keys.len);
            var deleted_initialized: usize = 0;
            errdefer {
                for (deleted_keys[0..deleted_initialized]) |key| alloc.free(key);
                alloc.free(deleted_keys);
            }
            for (req.deletes, 0..) |key, i| {
                deleted_keys[i] = try alloc.dupe(u8, key);
                deleted_initialized += 1;
            }
            for (deleted_artifact_keys) |key| {
                deleted_keys[deleted_initialized] = try alloc.dupe(u8, key);
                deleted_initialized += 1;
            }

            var overwritten_doc_keys_list = std.ArrayListUnmanaged([]const u8).empty;
            defer overwritten_doc_keys_list.deinit(alloc);
            errdefer for (overwritten_doc_keys_list.items) |key| alloc.free(@constCast(key));
            for (req.writes, 0..) |write, i| {
                if (extracted[i].hasDocument()) {
                    try appendOwnedConstBytes(alloc, &overwritten_doc_keys_list, write.key);
                }
            }

            var changed_artifact_keys_list = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (changed_artifact_keys_list.items) |key| alloc.free(@constCast(key));
                changed_artifact_keys_list.deinit(alloc);
            }
            for (changed_artifact_keys) |key| {
                try appendOwnedConstBytes(alloc, &changed_artifact_keys_list, key);
            }
            for (deleted_artifact_keys) |key| {
                if (!internal_keys.isAssetArtifactKey(key) and
                    !internal_keys.isChunkArtifactRecordKey(key) and
                    !internal_keys.isGraphEdgeArtifactKey(key)) continue;
                try appendOwnedConstBytes(alloc, &changed_artifact_keys_list, key);
            }

            var graph_doc_clears = std.ArrayListUnmanaged(derived_types.DerivedGraphDocClear).empty;
            errdefer {
                for (graph_doc_clears.items) |clear| derived_types.deinitDerivedGraphDocClear(alloc, clear);
                graph_doc_clears.deinit(alloc);
            }
            for (req.writes, 0..) |write, i| {
                if (extracted[i].mentioned_graph_indexes.len == 0) continue;
                const clear = try derived_types.cloneDerivedGraphDocClear(alloc, .{
                    .key = write.key,
                    .index_names = extracted[i].mentioned_graph_indexes,
                });
                errdefer derived_types.deinitDerivedGraphDocClear(alloc, clear);
                try graph_doc_clears.append(alloc, clear);
            }

            var graph_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
            errdefer {
                for (graph_writes.items) |write| derived_types.deinitDerivedGraphWrite(alloc, write);
                graph_writes.deinit(alloc);
            }
            for (extracted) |item| {
                for (item.graph_writes) |write| {
                    const cloned = try derived_types.cloneDerivedGraphWrite(alloc, write);
                    errdefer derived_types.deinitDerivedGraphWrite(alloc, cloned);
                    try graph_writes.append(alloc, cloned);
                }
            }
            for (req.graph_writes) |write| {
                const cloned = try derived_types.cloneDerivedGraphWrite(alloc, write);
                errdefer derived_types.deinitDerivedGraphWrite(alloc, cloned);
                try graph_writes.append(alloc, cloned);
            }

            var graph_deletes = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
            errdefer {
                for (graph_deletes.items) |delete| derived_types.deinitDerivedGraphDelete(alloc, delete);
                graph_deletes.deinit(alloc);
            }
            for (req.graph_deletes) |delete| {
                const cloned = try derived_types.cloneDerivedGraphDelete(alloc, delete);
                errdefer derived_types.deinitDerivedGraphDelete(alloc, cloned);
                try graph_deletes.append(alloc, cloned);
            }

            var dense_embeddings = std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite).empty;
            errdefer {
                for (dense_embeddings.items) |embedding|
                    derived_types.deinitDerivedDenseEmbedding(alloc, embedding);
                dense_embeddings.deinit(alloc);
            }
            for (extracted) |item| {
                for (item.dense_embeddings) |embedding| {
                    const cloned = try derived_types.cloneDerivedDenseEmbedding(alloc, .{
                        .index_name = embedding.index_name,
                        .doc_key = embedding.doc_key,
                        .artifact_key = embedding.artifact_key,
                        .vector = if (embedding.artifact_key != null) &.{} else embedding.vector,
                    });
                    errdefer derived_types.deinitDerivedDenseEmbedding(alloc, cloned);
                    try dense_embeddings.append(alloc, cloned);
                }
            }

            var sparse_embeddings = std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite).empty;
            errdefer {
                for (sparse_embeddings.items) |embedding|
                    derived_types.deinitDerivedSparseEmbedding(alloc, embedding);
                sparse_embeddings.deinit(alloc);
            }
            for (extracted) |item| {
                for (item.sparse_embeddings) |embedding| {
                    const cloned = try derived_types.cloneDerivedSparseEmbedding(alloc, .{
                        .index_name = embedding.index_name,
                        .doc_key = embedding.doc_key,
                        .artifact_key = embedding.artifact_key,
                        .indices = embedding.indices,
                        .values = embedding.values,
                    });
                    errdefer derived_types.deinitDerivedSparseEmbedding(alloc, cloned);
                    try sparse_embeddings.append(alloc, cloned);
                }
            }

            const overwritten_doc_keys = try overwritten_doc_keys_list.toOwnedSlice(alloc);
            errdefer {
                for (overwritten_doc_keys) |key| alloc.free(@constCast(key));
                if (overwritten_doc_keys.len > 0) alloc.free(overwritten_doc_keys);
            }
            const owned_changed_artifact_keys = try changed_artifact_keys_list.toOwnedSlice(alloc);
            errdefer {
                for (owned_changed_artifact_keys) |key| alloc.free(@constCast(key));
                if (owned_changed_artifact_keys.len > 0) alloc.free(owned_changed_artifact_keys);
            }
            const owned_graph_doc_clears = try graph_doc_clears.toOwnedSlice(alloc);
            errdefer {
                for (owned_graph_doc_clears) |clear| derived_types.deinitDerivedGraphDocClear(alloc, clear);
                if (owned_graph_doc_clears.len > 0) alloc.free(owned_graph_doc_clears);
            }
            const owned_dense_embeddings = try dense_embeddings.toOwnedSlice(alloc);
            errdefer {
                for (owned_dense_embeddings) |embedding|
                    derived_types.deinitDerivedDenseEmbedding(alloc, embedding);
                if (owned_dense_embeddings.len > 0) alloc.free(owned_dense_embeddings);
            }
            const owned_sparse_embeddings = try sparse_embeddings.toOwnedSlice(alloc);
            errdefer {
                for (owned_sparse_embeddings) |embedding|
                    derived_types.deinitDerivedSparseEmbedding(alloc, embedding);
                if (owned_sparse_embeddings.len > 0) alloc.free(owned_sparse_embeddings);
            }
            const owned_graph_writes = try graph_writes.toOwnedSlice(alloc);
            errdefer {
                for (owned_graph_writes) |write| derived_types.deinitDerivedGraphWrite(alloc, write);
                if (owned_graph_writes.len > 0) alloc.free(owned_graph_writes);
            }
            const owned_graph_deletes = try graph_deletes.toOwnedSlice(alloc);
            errdefer {
                for (owned_graph_deletes) |delete| derived_types.deinitDerivedGraphDelete(alloc, delete);
                if (owned_graph_deletes.len > 0) alloc.free(owned_graph_deletes);
            }

            return .{
                .sequence = 0,
                .documents = documents,
                .deleted_keys = deleted_keys,
                .overwritten_doc_keys = overwritten_doc_keys,
                .changed_artifact_keys = owned_changed_artifact_keys,
                .graph_doc_clears = owned_graph_doc_clears,
                .dense_embeddings = owned_dense_embeddings,
                .sparse_embeddings = owned_sparse_embeddings,
                .generated_enrichment_refs = &.{},
                .graph_writes = owned_graph_writes,
                .graph_deletes = owned_graph_deletes,
            };
        }

        pub fn buildDocumentExtractionCatalogView(
            alloc: Allocator,
            db: anytype,
            artifact_name: []const u8,
        ) !DocumentExtractionCatalogView {
            db.core.index_manager.catalog_mutex.lockShared();
            defer db.core.index_manager.catalog_mutex.unlockShared();

            var chunks = std.ArrayListUnmanaged(DocumentExtractionChunkView).empty;
            errdefer {
                for (chunks.items) |*chunk| chunk.deinit(alloc);
                chunks.deinit(alloc);
            }
            for (db.core.index_manager.enrichments.items) |entry| {
                if (entry.kind != .chunk) continue;
                if (!std.mem.eql(u8, entry.source_artifact_name, artifact_name)) continue;

                var chunk = DocumentExtractionChunkView{};
                errdefer chunk.deinit(alloc);
                chunk.name = try alloc.dupe(u8, entry.name);
                chunk.source_field = try alloc.dupe(u8, entry.source_field);
                chunk.chunker_json = if (entry.chunker_json.len > 0) try alloc.dupe(u8, entry.chunker_json) else "";
                chunk.chunk_size = entry.chunk_size;
                chunk.chunk_overlap = entry.chunk_overlap;
                const include_default_full_text = entry.full_text_index or
                    try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, entry.chunker_json);
                chunk.text_indexes = try db.core.index_manager.textIndexesForChunk(alloc, entry.name, include_default_full_text);

                var dense = std.ArrayListUnmanaged(DocumentExtractionEmbeddingView).empty;
                defer {
                    for (dense.items) |*embedding| embedding.deinit(alloc);
                    dense.deinit(alloc);
                }
                var sparse = std.ArrayListUnmanaged(DocumentExtractionEmbeddingView).empty;
                defer {
                    for (sparse.items) |*embedding| embedding.deinit(alloc);
                    sparse.deinit(alloc);
                }
                try buildDocumentExtractionEmbeddingViews(alloc, db, entry.name, &dense, &sparse);
                // Each detached slice moves directly into the chunk's cleanup scope,
                // including failures in the second detach or in the outer append.
                chunk.dense_embeddings = try dense.toOwnedSlice(alloc);
                chunk.sparse_embeddings = try sparse.toOwnedSlice(alloc);
                try chunks.append(alloc, chunk);
            }
            return .{ .chunks = try chunks.toOwnedSlice(alloc) };
        }

        pub fn buildDocumentExtractionEmbeddingViews(
            alloc: Allocator,
            db: anytype,
            chunk_artifact_name: []const u8,
            dense: *std.ArrayListUnmanaged(DocumentExtractionEmbeddingView),
            sparse: *std.ArrayListUnmanaged(DocumentExtractionEmbeddingView),
        ) !void {
            for (db.core.index_manager.enrichments.items) |entry| {
                if (entry.kind != .embedding) continue;
                if (!std.mem.eql(u8, entry.source_artifact_name, chunk_artifact_name)) continue;
                // cloneFromEnrichment takes ownership only on success. End the
                // consumer cleanup scope before the view becomes the sole owner.
                var view = blk: {
                    const consumers = if (entry.expected_dims > 0)
                        try db.core.index_manager.denseIndexesForEmbedding(alloc, entry.name, entry.expected_dims)
                    else
                        try db.core.index_manager.sparseIndexesForEmbedding(alloc, entry.name);
                    errdefer {
                        for (consumers) |name| alloc.free(name);
                        alloc.free(consumers);
                    }
                    break :blk try DocumentExtractionEmbeddingView.cloneFromEnrichment(alloc, entry, consumers);
                };
                errdefer view.deinit(alloc);
                const target = if (entry.expected_dims > 0) dense else sparse;
                try target.append(alloc, view);
            }
        }

        pub fn buildDocumentUnitChunkPayloadAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            unit_key: []const u8,
            unit_fingerprint: []const u8,
            artifact_name: []const u8,
            source_artifact_name: []const u8,
            source_field: []const u8,
            unit: document_extraction_mod.Unit,
            chunk: chunker_mod.Chunk,
            include_payload: bool,
            route: DocumentExtractionRangeRoute,
        ) ![]u8 {
            const owner_group_id = std.math.cast(i64, route.owner_group_id) orelse return error.InvalidDocumentExtractionManifest;
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, try alloc.dupe(u8, "_parent_doc_key"), .{ .string = try alloc.dupe(u8, doc_key) });
            try obj.put(alloc, try alloc.dupe(u8, "_parent_unit_key"), .{ .string = try alloc.dupe(u8, unit_key) });
            try obj.put(alloc, try alloc.dupe(u8, hierarchy_navigation_unit_fingerprint_field), .{ .string = try alloc.dupe(u8, unit_fingerprint) });
            try obj.put(alloc, try alloc.dupe(u8, "_parent_unit_id"), .{ .string = try alloc.dupe(u8, unit.unit_id) });
            try obj.put(alloc, try alloc.dupe(u8, "_artifact_name"), .{ .string = try alloc.dupe(u8, artifact_name) });
            try obj.put(alloc, try alloc.dupe(u8, "_source_artifact_name"), .{ .string = try alloc.dupe(u8, source_artifact_name) });
            try obj.put(alloc, try alloc.dupe(u8, "_source_field"), .{ .string = try alloc.dupe(u8, source_field) });
            try obj.put(alloc, try alloc.dupe(u8, "_artifact_range_id"), .{ .string = try alloc.dupe(u8, route.range_id) });
            try obj.put(alloc, try alloc.dupe(u8, "_artifact_range_kind"), .{ .string = try alloc.dupe(u8, "chunk") });
            try obj.put(alloc, try alloc.dupe(u8, "_artifact_route_status"), .{ .string = try alloc.dupe(u8, route.route_status) });
            try obj.put(alloc, try alloc.dupe(u8, "_artifact_owner_group_id"), .{ .integer = owner_group_id });
            try chunk_artifact_mod.appendArtifactFieldsWithProvenance(alloc, &obj, source_field, chunk, include_payload, .{
                .scope = .unit,
                .parent_doc_key = doc_key,
                .parent_unit_key = unit_key,
                .parent_unit_id = unit.unit_id,
                .source_artifact_name = source_artifact_name,
                .document_char_base = unit.char_start,
                .page_number = unit.page_number,
                .page_label = unit.page_label,
                .page_bbox = unit.page_bbox,
                .page_rotation = unit.page_rotation,
                .extraction_method = unit.method,
                .extraction_status = unit.extraction_status,
                .confidence = documentUnitConfidence(unit),
                .ocr_used = unit.ocr_used,
                .ocr_attempted = unit.ocr_attempted,
                .ocr_render_dpi = unit.ocr_render_dpi,
                .ocr_effective_render_dpi = unit.ocr_effective_render_dpi,
                .ocr_rendered_width = unit.ocr_rendered_width,
                .ocr_rendered_height = unit.ocr_rendered_height,
                .ocr_rendered_bytes = unit.ocr_rendered_bytes,
                .ocr_failure_stage = unit.ocr_failure_stage,
                .ocr_failure_retryable = unit.ocr_failure_retryable,
                .ocr_trigger_reasons = unit.ocr_trigger_reasons,
                .ocr_embedded_quality = unit.ocr_embedded_quality,
                .ocr_output_quality = unit.ocr_output_quality,
                .ocr_confidence = unit.ocr_confidence,
                .ocr_bbox = unit.ocr_bbox,
                .transcript_used = unit.transcript_used,
                .transcript_confidence = unit.transcript_confidence,
                .extraction_warning = unit.extraction_warning,
            });
            return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = obj }, .{});
        }

        pub fn bytesToHexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
            const out = try alloc.alloc(u8, bytes.len * 2);
            for (bytes, 0..) |byte, idx| {
                out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
                out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
            }
            return out;
        }

        pub fn chunkArtifactKeysForChunksAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
            chunks: []const chunker_mod.Chunk,
        ) ![][]u8 {
            const keys = try alloc.alloc([]u8, chunks.len);
            var initialized: usize = 0;
            errdefer {
                for (keys[0..initialized]) |key| alloc.free(key);
                alloc.free(keys);
            }
            for (chunks, 0..) |chunk, i| {
                keys[i] = try internal_keys.chunkArtifactKeyAlloc(alloc, doc_key, artifact_name, @intCast(chunk.chunk_id));
                initialized += 1;
            }
            return keys;
        }

        pub fn chunkCacheTupleKeyAlloc(alloc: Allocator, components: []const []const u8) ![]u8 {
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);

            for (components) |component| {
                if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
                var len_buf: [@sizeOf(u32)]u8 = undefined;
                std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
                try out.appendSlice(alloc, &len_buf);
                try out.appendSlice(alloc, component);
            }

            return try out.toOwnedSlice(alloc);
        }

        pub fn chunkEmbeddingSourcesForRequest(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
            chunks_created: *usize,
        ) ![]ChunkEmbeddingSource {
            std.debug.assert(request.input_kind == .inline_chunks);
            chunks_created.* = 0;
            const artifact_name = requestArtifactName(request);
            var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
            errdefer {
                for (sources.items) |source| {
                    alloc.free(source.key);
                    alloc.free(source.text);
                }
                sources.deinit(alloc);
            }

            const chunks = try getOrCreateChunks(alloc, db, doc_value, request, cache);
            for (chunks) |chunk| {
                const chunk_text = chunk.text orelse continue;
                if (chunk_text.len == 0) continue;
                try sources.ensureUnusedCapacity(alloc, 1);
                const key = try internal_keys.chunkArtifactKeyAlloc(alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
                errdefer alloc.free(key);
                const text = try alloc.dupe(u8, chunk_text);
                sources.appendAssumeCapacity(.{ .key = key, .text = text });
                chunks_created.* += 1;
            }
            // Inline chunks are derived from this document revision. An empty set is
            // intentional; falling back to stored rows would resurrect the previous
            // revision's chunks while this commit is deleting them.
            return try sources.toOwnedSlice(alloc);
        }

        pub fn clearGraphArtifactStatePaged(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            doc_key: []const u8,
            index_name: []const u8,
            state_key: []const u8,
            generation: u64,
            retire_lifetimes: bool,
            changed: *std.ArrayListUnmanaged([]u8),
            changed_set: *std.StringHashMapUnmanaged(void),
        ) !void {
            const raw_state = store.get(alloc, state_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (raw_state) |raw| alloc.free(raw);
            if (raw_state) |raw| {
                if (try graph_asset_state.coverageGeneration(raw) != generation) {
                    // A new index incarnation cannot reconcile old-generation
                    // contenders into its projection. Remove the obsolete manifests
                    // in bounded key-only pages; index-generation installation owns
                    // the corresponding projection reset.
                    const segment_prefix = try internal_keys.graphAssetStateSegmentPrefixAlloc(alloc, state_key);
                    defer alloc.free(segment_prefix);
                    while (true) {
                        const segment_keys = try store.scanPrefixKeysPage(alloc, segment_prefix, null, 2048);
                        defer freeOwnedKeySlice(alloc, segment_keys);
                        if (segment_keys.len == 0) break;
                        try store.putBatch(&.{}, segment_keys);
                    }
                    try store.putBatch(&.{}, &.{state_key});
                    return;
                } else if (try graph_asset_state.format(raw) == .v4) {
                    const previous_keys = try graph_asset_state.decodeKeysAlloc(alloc, raw);
                    defer freeOwnedConstKeySlice(alloc, previous_keys);
                    var start: usize = 0;
                    while (start < previous_keys.len) {
                        const end = @min(previous_keys.len, start + 2048);
                        const page = previous_keys[start..end];
                        var reconciled = try reconcileSingleGraphStateContenders(
                            alloc,
                            store,
                            doc_key,
                            index_name,
                            state_key,
                            page,
                            &.{},
                            index_manager.graphArtifactSources(index_name),
                            generation,
                            (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).ttl_duration_ns,
                        );
                        defer reconciled.deinit(alloc);
                        for (page) |edge_key| try appendUniqueOwnedKeyIndexed(alloc, changed, changed_set, edge_key);
                        var scratch_state = std.heap.ArenaAllocator.init(alloc);
                        defer scratch_state.deinit();
                        const scratch = scratch_state.allocator();
                        var deletes = std.ArrayListUnmanaged([]const u8).empty;
                        if (end == previous_keys.len) try deletes.append(scratch, state_key);
                        if (retire_lifetimes) for (page) |edge_key| {
                            try deletes.append(scratch, try internal_keys.graphEdgeTtlLifetimeKeyAlloc(scratch, edge_key, index_name, generation, state_key));
                            try deletes.append(scratch, try internal_keys.graphEdgeTtlTombstoneKeyAlloc(scratch, edge_key, index_name, generation, state_key));
                        };
                        try commitGraphContenderReconcilePage(
                            alloc,
                            store,
                            page,
                            &reconciled,
                            &.{},
                            deletes.items,
                        );
                        start = end;
                    }
                    if (previous_keys.len == 0) try store.putBatch(&.{}, &.{state_key});
                }
            }

            // V5 states and crash-left unpublished segments share the same bounded
            // rollback path. A missing root is intentional while a new view is staged.
            while (try rollbackGraphStateSegmentPage(
                alloc,
                store,
                index_manager,
                doc_key,
                index_name,
                state_key,
                generation,
                retire_lifetimes,
                changed,
                changed_set,
            )) {}
        }

        pub fn clearPrecomputeAssetProducerBatchItems(
            alloc: Allocator,
            items: *std.ArrayListUnmanaged(PrecomputeAssetProducerBatchItem),
        ) void {
            for (items.items) |item| freePrecomputeAssetProducerBatchItem(alloc, item);
            items.clearRetainingCapacity();
        }

        pub fn clearPublishedEmbeddingArtifactRepairIssueContext(
            ctx: *const AsyncContext,
            index_name: []const u8,
            artifact_key: []const u8,
            published_sequence: u64,
        ) !void {
            const artifact_key_hex = try bytesToHexAlloc(ctx.alloc, artifact_key);
            defer ctx.alloc.free(artifact_key_hex);
            const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(ctx.alloc, index_name, "embedding", artifact_key_hex);
            defer ctx.alloc.free(issue_key);

            const mutable_ctx = @constCast(ctx);
            lockAtomicWithBackoff(&mutable_ctx.artifact_repair_issue_mutex);
            defer mutable_ctx.artifact_repair_issue_mutex.unlock();

            const loaded = (try loadArtifactRepairIssueFromStoreByKey(ctx.alloc, ctx.store, issue_key)) orelse return;
            var issue = loaded;
            defer issue.deinit(ctx.alloc);
            if (issue.sequence > published_sequence) return;

            var completion_key: ?[]u8 = null;
            defer if (completion_key) |key| ctx.alloc.free(key);
            var completion: ArtifactRepairCompletionState = .{};
            var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
            if (issue.reason == .enrichment_failed) {
                if (ctx.index_manager.denseIndex(index_name) == null and
                    ctx.index_manager.sparseIndex(index_name) == null)
                {
                    return;
                }
                const generation = ctx.index_manager.coverageGenerationForIndex(index_name) orelse return;
                const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(ctx.alloc, index_name, generation, issue.doc_key);
                defer ctx.alloc.free(marker_key);
                const raw_outcome = ctx.store.get(ctx.alloc, marker_key) catch |err| switch (err) {
                    error.NotFound => return,
                    else => return err,
                };
                defer ctx.alloc.free(raw_outcome);
                const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, raw_outcome) orelse
                    return error.InvalidDerivedCoverageOutcome;
                if (outcome != .produced and outcome != .skipped) return;

                completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(ctx.alloc, "embedding", artifact_key_hex);
                completion = (try loadArtifactRepairCompletionStateFromStore(ctx.alloc, ctx.store, completion_key.?)) orelse .{};
                completion.completed_sequence = @max(completion.completed_sequence, published_sequence);
                completion.pending_issues -|= 1;
            }

            const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(ctx.alloc, issue);
            defer ctx.alloc.free(kind_key);
            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            var borrowed_write_count: usize = 0;
            defer {
                for (writes.items[borrowed_write_count..]) |write| ctx.alloc.free(@constCast(write.value));
                writes.deinit(ctx.alloc);
            }
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(ctx.alloc);
            var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (owned_delete_keys.items) |key| ctx.alloc.free(@constCast(key));
                owned_delete_keys.deinit(ctx.alloc);
            }

            try deletes.append(ctx.alloc, issue_key);
            try deletes.append(ctx.alloc, kind_key);
            if (issue.reason == .enrichment_failed) {
                try appendEnrichmentTerminalFailureMarkerDeletesForIssue(
                    ctx.alloc,
                    ctx.store,
                    issue_key,
                    &deletes,
                    &owned_delete_keys,
                    true,
                );
            }
            if (completion_key) |key| {
                if (completion.pending_issues == 0) {
                    try deletes.append(ctx.alloc, key);
                } else {
                    encodeArtifactRepairCompletionState(&encoded_completion, completion);
                    try writes.append(ctx.alloc, .{ .key = key, .value = &encoded_completion });
                }
            }
            borrowed_write_count = writes.items.len;
            try appendArtifactRepairSummaryDirtyForStore(ctx.alloc, ctx.store, &writes, &deletes, &owned_delete_keys);
            try ctx.store.putBatch(writes.items, deletes.items);
        }

        pub fn clearPublishedEmbeddingArtifactRepairIssuesContext(
            ctx: *const AsyncContext,
            index_name: []const u8,
            writes: anytype,
            published_sequence: u64,
        ) !void {
            const ArtifactRepairProbe = struct {
                issue_key: []u8,
                artifact_key: []const u8,

                fn lessThan(_: void, lhs: @This(), rhs: @This()) bool {
                    return std.mem.lessThan(u8, lhs.issue_key, rhs.issue_key);
                }
            };

            var probes = std.ArrayListUnmanaged(ArtifactRepairProbe).empty;
            defer {
                for (probes.items) |probe| ctx.alloc.free(probe.issue_key);
                probes.deinit(ctx.alloc);
            }
            for (writes) |write| {
                const artifact_key = write.artifact_key orelse continue;
                const artifact_key_hex = try bytesToHexAlloc(ctx.alloc, artifact_key);
                defer ctx.alloc.free(artifact_key_hex);
                const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(ctx.alloc, index_name, "embedding", artifact_key_hex);
                errdefer ctx.alloc.free(issue_key);
                try probes.append(ctx.alloc, .{ .issue_key = issue_key, .artifact_key = artifact_key });
            }
            if (probes.items.len == 0) return;
            std.mem.sort(ArtifactRepairProbe, probes.items, {}, ArtifactRepairProbe.lessThan);

            const keys = try ctx.alloc.alloc([]const u8, probes.items.len);
            defer ctx.alloc.free(keys);
            const values = try ctx.alloc.alloc(?[]const u8, probes.items.len);
            defer ctx.alloc.free(values);
            for (probes.items, 0..) |probe, i| keys[i] = probe.issue_key;

            var existing_artifacts = std.ArrayListUnmanaged([]const u8).empty;
            defer existing_artifacts.deinit(ctx.alloc);
            {
                // Healthy replay has no issue for almost every artifact. Batch those
                // negative lookups without retaining their LSM blocks or taking the
                // repair mutex once per vector. The rare positive case keeps the
                // existing serialized correctness path below.
                var issue_probe = try ctx.store.beginProbeTxnWithBlockCacheAdmission(.transient);
                defer issue_probe.abort();
                try issue_probe.getManySorted(keys, values);
                for (probes.items, values) |probe, value| {
                    if (value != null) try existing_artifacts.append(ctx.alloc, probe.artifact_key);
                }
            }
            for (existing_artifacts.items) |artifact_key| {
                try clearPublishedEmbeddingArtifactRepairIssueContext(ctx, index_name, artifact_key, published_sequence);
            }
        }

        pub fn collectDeleteKeysForPrefix(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            prefix: []const u8,
            deletes: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
            recorded: ?*std.ArrayListUnmanaged([]u8),
        ) !void {
            const existing = try store.scanPrefixKeysPage(alloc, prefix, null, std.math.maxInt(usize));
            defer {
                for (existing) |key| alloc.free(key);
                alloc.free(existing);
            }
            for (existing) |entry| {
                const owned = try alloc.dupe(u8, entry);
                errdefer alloc.free(owned);
                try owned_delete_keys.append(alloc, owned);
                errdefer _ = owned_delete_keys.pop();
                try deletes.append(alloc, owned);
                errdefer _ = deletes.pop();
                if (recorded) |out| try out.append(alloc, owned);
            }
        }

        pub fn collectDocumentExtractionDesiredKeys(
            alloc: Allocator,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            artifact_name: []const u8,
            units: []const document_extraction_mod.Unit,
            desired_unit_keys: *std.ArrayListUnmanaged([]const u8),
            desired_unit_fingerprints: *std.ArrayListUnmanaged([]const u8),
            desired_chunk_keys: *std.ArrayListUnmanaged([]const u8),
        ) !void {
            for (units) |unit| {
                try desired_unit_keys.append(alloc, try internal_keys.documentUnitArtifactKeyAlloc(alloc, doc_key, artifact_name, unit.unit_id));
                try desired_unit_fingerprints.append(alloc, try documentExtractionUnitFingerprintAlloc(alloc, unit));
                for (view.chunks) |entry| {
                    const chunks = if (entry.chunker_json.len > 0)
                        try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
                    else
                        try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
                    defer chunker_mod.freeChunks(alloc, chunks);
                    for (chunks) |chunk| {
                        try desired_chunk_keys.append(alloc, try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id)));
                    }
                }
            }
        }

        pub fn collectEnrichmentArtifactDeleteKeysForDocContext(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            doc_key: []const u8,
            delete_keys: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
            deleted_artifact_keys: *std.ArrayListUnmanaged([]u8),
            selected_artifacts: *std.StringHashMapUnmanaged(void),
        ) !void {
            const document_prefix = try internal_keys.documentExactPrefixAlloc(alloc, doc_key);
            defer alloc.free(document_prefix);
            const artifact_prefix = try internal_keys.artifactRootPrefixAlloc(alloc, doc_key);
            defer alloc.free(artifact_prefix);
            const asset_state_prefix = try internal_keys.assetStateRootPrefixAlloc(alloc, doc_key);
            defer alloc.free(asset_state_prefix);
            const graph_asset_state_prefix = try internal_keys.graphAssetStateRootPrefixAlloc(alloc, doc_key);
            defer alloc.free(graph_asset_state_prefix);
            const graph_edge_contender_prefix = try internal_keys.graphEdgeContenderRootPrefixAlloc(alloc, doc_key);
            defer alloc.free(graph_edge_contender_prefix);
            const graph_global_contender_prefix = try internal_keys.graphGlobalEdgeContenderRootPrefixAlloc(alloc, doc_key);
            defer alloc.free(graph_global_contender_prefix);
            // All document-owned records are contiguous. Stream borrowed keys directly
            // from one cursor and retain only keys selected for deletion. In particular,
            // never copy potentially large chunk/vector/asset payloads merely to delete
            // their owning records.
            const upper = try internal_keys.nextPrefixAlloc(alloc, document_prefix);
            defer if (upper) |key| alloc.free(key);
            const ScanContext = struct {
                alloc: Allocator,
                artifact_prefix: []const u8,
                asset_state_prefix: []const u8,
                graph_asset_state_prefix: []const u8,
                graph_edge_contender_prefix: []const u8,
                graph_global_contender_prefix: []const u8,
                delete_keys: *std.ArrayListUnmanaged([]const u8),
                owned_delete_keys: *std.ArrayListUnmanaged([]u8),
                deleted_artifact_keys: *std.ArrayListUnmanaged([]u8),
                selected_artifacts: *std.StringHashMapUnmanaged(void),

                fn visit(raw: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    _ = value;
                    const ctx: *@This() = @ptrCast(@alignCast(raw orelse return error.InvalidArgument));
                    const is_artifact = std.mem.startsWith(u8, key, ctx.artifact_prefix);
                    if (!is_artifact and
                        !std.mem.startsWith(u8, key, ctx.asset_state_prefix) and
                        !std.mem.startsWith(u8, key, ctx.graph_asset_state_prefix) and
                        !std.mem.startsWith(u8, key, ctx.graph_edge_contender_prefix) and
                        !std.mem.startsWith(u8, key, ctx.graph_global_contender_prefix)) return .@"continue";
                    if (is_artifact and ctx.selected_artifacts.contains(key)) return .@"continue";
                    const owned = try ctx.alloc.dupe(u8, key);
                    errdefer ctx.alloc.free(owned);
                    try ctx.owned_delete_keys.append(ctx.alloc, owned);
                    errdefer _ = ctx.owned_delete_keys.pop();
                    try ctx.delete_keys.append(ctx.alloc, owned);
                    errdefer _ = ctx.delete_keys.pop();
                    if (is_artifact) {
                        try ctx.deleted_artifact_keys.append(ctx.alloc, owned);
                        try ctx.selected_artifacts.put(ctx.alloc, owned, {});
                    }
                    return .@"continue";
                }
            };
            var scan_context = ScanContext{
                .alloc = alloc,
                .artifact_prefix = artifact_prefix,
                .asset_state_prefix = asset_state_prefix,
                .graph_asset_state_prefix = graph_asset_state_prefix,
                .graph_edge_contender_prefix = graph_edge_contender_prefix,
                .graph_global_contender_prefix = graph_global_contender_prefix,
                .delete_keys = delete_keys,
                .owned_delete_keys = owned_delete_keys,
                .deleted_artifact_keys = deleted_artifact_keys,
                .selected_artifacts = selected_artifacts,
            };
            try store.scanWithContext(document_prefix, upper orelse "", .{}, &scan_context, ScanContext.visit);
            const retired_prefix = try internal_keys.graphRetirementPrefixAlloc(alloc, doc_key);
            defer alloc.free(retired_prefix);
            try collectDeleteKeysForPrefix(alloc, store, retired_prefix, delete_keys, owned_delete_keys, null);

            // Incoming artifacts can span an arbitrarily large number of owners.
            // The primary delete records a durable job instead of retaining them here.

        }

        pub fn collectEnrichmentArtifactDeletesForBatch(
            self: anytype,
            req: types.BatchRequest,
            artifact_effects: []const @import("merge_page_contract.zig").IntegrityEffect,
            extracted: []const mapper.ExtractedWrite,
            delete_keys: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
        ) ![][]u8 {
            _ = extracted;
            const vector_effects = artifact_effects;
            if (!self.core.hasArtifactCleanupMaybe() and vector_effects.len == 0) return try self.alloc.alloc([]u8, 0);

            var deleted = std.ArrayListUnmanaged([]u8).empty;
            errdefer deleted.deinit(self.alloc);
            var seen_docs = std.StringHashMapUnmanaged(void).empty;
            defer seen_docs.deinit(self.alloc);
            var selected_artifacts = std.StringHashMapUnmanaged(void).empty;
            defer selected_artifacts.deinit(self.alloc);

            for (req.deletes) |key| {
                const gop = try seen_docs.getOrPut(self.alloc, key);
                if (gop.found_existing) continue;
                try collectEnrichmentArtifactDeleteKeysForDocContext(
                    self.alloc,
                    self.core.store,
                    key,
                    delete_keys,
                    owned_delete_keys,
                    &deleted,
                    &selected_artifacts,
                );
            }
            // Explicit tombstones reach derived replay even if the corresponding
            // insertion is not materialized yet. Do not count the same deletion twice
            // when this fragment also deletes its primary document.
            var seen_artifacts = std.StringHashMapUnmanaged(void).empty;
            defer seen_artifacts.deinit(self.alloc);
            for (deleted.items) |key| try seen_artifacts.put(self.alloc, key, {});
            for (vector_effects) |effect| if (effect.value == null) {
                const gop = try seen_artifacts.getOrPut(self.alloc, effect.key);
                if (gop.found_existing) continue;
                const owned_key = try self.alloc.dupe(u8, effect.key);
                owned_delete_keys.append(self.alloc, owned_key) catch |err| {
                    self.alloc.free(owned_key);
                    return err;
                };
                try delete_keys.append(self.alloc, owned_key);
                try deleted.append(self.alloc, owned_key);
            };
            return try deleted.toOwnedSlice(self.alloc);
        }

        pub fn collectGraphArtifactsForDocIndex(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            doc_key: []const u8,
            index_name: []const u8,
        ) ![]docstore_mod.OwnedKVPair {
            const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, doc_key, index_name);
            defer alloc.free(prefix);
            return try store.scanPrefix(alloc, prefix);
        }

        pub fn collectGraphDeletes(alloc: Allocator, deletes: []const types.GraphEdgeDelete, index_name: []const u8) ![]types.GraphEdgeDelete {
            var filtered = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
            defer filtered.deinit(alloc);

            for (deletes) |delete| {
                if (!std.mem.eql(u8, delete.index_name, index_name)) continue;
                try filtered.append(alloc, delete);
            }

            return try filtered.toOwnedSlice(alloc);
        }

        pub fn collectGraphMutationsForArtifacts(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            artifact_keys: []const []const u8,
            index_name: []const u8,
            options: GraphMutationCollectionOptions,
        ) !OwnedGraphMutations {
            var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
            errdefer {
                for (writes.items) |write| {
                    alloc.free(@constCast(write.index_name));
                    alloc.free(@constCast(write.source));
                    alloc.free(@constCast(write.target));
                    alloc.free(@constCast(write.edge_type));
                    if (write.edge_id.len > 0) alloc.free(@constCast(write.edge_id));
                    if (write.owner_document.len > 0) alloc.free(@constCast(write.owner_document));
                    if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
                    if (write.owner.len > 0) alloc.free(@constCast(write.owner));
                }
                writes.deinit(alloc);
            }
            var deletes = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
            errdefer {
                for (deletes.items) |delete| {
                    alloc.free(@constCast(delete.index_name));
                    alloc.free(@constCast(delete.source));
                    alloc.free(@constCast(delete.target));
                    alloc.free(@constCast(delete.edge_type));
                    if (delete.edge_id.len > 0) alloc.free(@constCast(delete.edge_id));
                    if (delete.owner_document.len > 0) alloc.free(@constCast(delete.owner_document));
                    if (delete.owner.len > 0) alloc.free(@constCast(delete.owner));
                }
                deletes.deinit(alloc);
            }
            var generation_bindings = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            errdefer {
                for (generation_bindings.items) |binding| {
                    alloc.free(@constCast(binding.key));
                    alloc.free(@constCast(binding.value));
                }
                generation_bindings.deinit(alloc);
            }

            var txn = try store.beginReadTxn();
            defer txn.abort();

            for (artifact_keys) |artifact_key| {
                const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, artifact_key)) orelse continue;
                defer {
                    alloc.free(parsed.doc_key);
                    alloc.free(parsed.index_name);
                    alloc.free(parsed.edge_type);
                    alloc.free(parsed.target_doc_key);
                    alloc.free(parsed.edge_id);
                    alloc.free(parsed.logical_source);
                }
                if (!std.mem.eql(u8, parsed.index_name, index_name)) continue;
                // The key's owner routes and retires the row; the applied edge starts
                // from the explicit source node when one is embedded (entity-sourced
                // relations, zig/AUTOSCHEMA.md).

                const raw = txn.get(artifact_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (raw) |value| {
                    var decoded = enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, value) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => {
                            if (options.repair_ctx) |repair_ctx| {
                                const artifact_name = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ parsed.edge_type, parsed.target_doc_key });
                                defer alloc.free(artifact_name);
                                try recordArtifactRepairIssueContextForIndexSource(
                                    repair_ctx,
                                    .graph,
                                    parsed.index_name,
                                    parsed.doc_key,
                                    "",
                                    "",
                                    "",
                                    "",
                                    artifact_name,
                                    artifact_key,
                                    null,
                                    options.sequence,
                                    .corrupt_artifact,
                                );
                                return error.ArtifactRepairRequired;
                            }
                            return err;
                        },
                    };
                    defer decoded.deinit(alloc);
                    // Raw v0.2 edge keys carry no incarnation proof. Only explicitly
                    // portable records may be rebound without a current-generation
                    // state manifest authenticating the edge.
                    if (enrichment_artifact_codec.isLegacyUnboundGraphEdge(value)) {
                        continue;
                    }
                    const generation_unbound = enrichment_artifact_codec.isPortableUnboundGraphEdge(value);
                    if (decoded.generation != options.expected_generation and !generation_unbound) {
                        var owned = try (types.GraphEdgeDelete{
                            .index_name = parsed.index_name,
                            .source = if (parsed.logical_source.len > 0) parsed.logical_source else parsed.doc_key,
                            .owner = if (parsed.edge_id.len == 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                            .edge_id = parsed.edge_id,
                            .owner_document = if (parsed.edge_id.len > 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                            .target = parsed.target_doc_key,
                            .edge_type = parsed.edge_type,
                        }).cloneAlloc(alloc);
                        errdefer owned.deinit(alloc);
                        try deletes.append(alloc, owned);
                        continue;
                    }
                    if (generation_unbound) {
                        const bound_value = try enrichment_artifact_codec.bindGraphEdgeGenerationAlloc(
                            alloc,
                            value,
                            options.expected_generation,
                        );
                        errdefer alloc.free(bound_value);
                        const bound_key = try alloc.dupe(u8, artifact_key);
                        errdefer alloc.free(bound_key);
                        try generation_bindings.append(alloc, .{ .key = bound_key, .value = bound_value });
                    }
                    var owned = try (types.GraphEdgeWrite{
                        .index_name = parsed.index_name,
                        .source = if (parsed.logical_source.len > 0) parsed.logical_source else parsed.doc_key,
                        .owner = if (parsed.edge_id.len == 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                        .edge_id = parsed.edge_id,
                        .owner_document = if (parsed.edge_id.len > 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                        .target = parsed.target_doc_key,
                        .edge_type = parsed.edge_type,
                        .weight = decoded.weight,
                        .created_at = decoded.created_at,
                        .updated_at = decoded.updated_at,
                        .ttl_created_ns = decoded.ttl_created_ns,
                        .metadata_json = "",
                    }).cloneAlloc(alloc);
                    errdefer owned.deinit(alloc);
                    try writes.append(alloc, owned);
                    writes.items[writes.items.len - 1].metadata_json = decoded.metadata_json;
                    decoded.metadata_json = &.{};
                } else {
                    var owned = try (types.GraphEdgeDelete{
                        .index_name = parsed.index_name,
                        .source = if (parsed.logical_source.len > 0) parsed.logical_source else parsed.doc_key,
                        .owner = if (parsed.edge_id.len == 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                        .edge_id = parsed.edge_id,
                        .owner_document = if (parsed.edge_id.len > 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                        .target = parsed.target_doc_key,
                        .edge_type = parsed.edge_type,
                    }).cloneAlloc(alloc);
                    errdefer owned.deinit(alloc);
                    try deletes.append(alloc, owned);
                }
            }

            var result = OwnedGraphMutations{ .alloc = alloc };
            errdefer result.deinit();
            result.writes = try writes.toOwnedSlice(alloc);
            result.deletes = try deletes.toOwnedSlice(alloc);
            result.generation_bindings = try generation_bindings.toOwnedSlice(alloc);
            return result;
        }

        pub fn collectGraphWrites(alloc: Allocator, writes: []const types.GraphEdgeWrite, index_name: []const u8) ![]types.GraphEdgeWrite {
            var filtered = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
            defer filtered.deinit(alloc);

            for (writes) |write| {
                if (!std.mem.eql(u8, write.index_name, index_name)) continue;
                try filtered.append(alloc, write);
            }

            return try filtered.toOwnedSlice(alloc);
        }

        pub fn collectManagedIndexCandidates(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            classify_generated_dependencies: bool,
        ) ![]ManagedIndexCandidate {
            var generated_names = if (classify_generated_dependencies)
                try GeneratedEnrichmentNameLookup.init(alloc, index_manager)
            else
                GeneratedEnrichmentNameLookup{};
            defer generated_names.deinit(alloc);

            var candidates = std.ArrayListUnmanaged(ManagedIndexCandidate).empty;
            errdefer candidates.deinit(alloc);
            try candidates.ensureTotalCapacity(alloc, index_manager.count());

            for (index_manager.text_indexes.items) |*entry| {
                const generated = (entry.chunk_name != null and generated_names.all.contains(entry.chunk_name.?)) or
                    artifactSourcesContainGeneratedEnrichment(&generated_names.all, entry.source_artifact_names);
                candidates.appendAssumeCapacity(.{
                    .ref = .{ .name = entry.config.name, .kind = .full_text },
                    .config = &entry.config,
                    .consumes_generated_enrichment = generated,
                });
            }
            for (index_manager.dense_indexes.items) |*entry| {
                const generated = artifactSourcesContainGeneratedEnrichment(
                    &generated_names.embeddings,
                    entry.embedding_names,
                ) or
                    (!entry.external and entry.embedding_name != null and
                        generated_names.embeddings.contains(entry.embedding_name.?));
                const working_set_factor = index_manager.denseReplayWorkingSetFactor();
                candidates.appendAssumeCapacity(.{
                    .ref = .{
                        .name = entry.config.name,
                        .kind = .dense_vector,
                        .estimated_dense_vector_bytes = @as(u64, entry.dims) * @sizeOf(f32) *| working_set_factor,
                        .dense_replay_working_set_factor = working_set_factor,
                    },
                    .config = &entry.config,
                    .consumes_generated_enrichment = generated,
                });
            }
            for (index_manager.sparse_indexes.items) |*entry| {
                const generated = artifactSourcesContainGeneratedEnrichment(
                    &generated_names.embeddings,
                    entry.embedding_names,
                ) or
                    (!entry.external and entry.embedding_name != null and
                        generated_names.embeddings.contains(entry.embedding_name.?));
                candidates.appendAssumeCapacity(.{
                    .ref = .{ .name = entry.config.name, .kind = .sparse_vector },
                    .config = &entry.config,
                    .consumes_generated_enrichment = generated,
                });
            }
            for (index_manager.graph_indexes.items) |*entry| {
                var generated = false;
                for (entry.artifact_sources) |source| {
                    if (generated_names.all.contains(source.artifact_name)) {
                        generated = true;
                        break;
                    }
                }
                candidates.appendAssumeCapacity(.{
                    .ref = .{ .name = entry.config.name, .kind = .graph },
                    .config = &entry.config,
                    .consumes_generated_enrichment = generated,
                });
            }
            for (index_manager.algebraic_indexes.items) |*entry| {
                candidates.appendAssumeCapacity(.{
                    .ref = .{ .name = entry.config.name, .kind = .algebraic },
                    .config = &entry.config,
                    .consumes_generated_enrichment = false,
                });
            }
            for (index_manager.status_only_index_configs) |*cfg| {
                if (index_manager.loadFailure(cfg.name) != null) continue;
                candidates.appendAssumeCapacity(.{
                    .ref = .{ .name = cfg.name, .kind = cfg.kind },
                    .config = cfg,
                    .consumes_generated_enrichment = false,
                });
            }
            return try candidates.toOwnedSlice(alloc);
        }

        pub fn collectManagedSyncTargetsForRecord(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            record: change_journal_mod.Record,
        ) !ManagedSyncTargets {
            const generated_source_advanced = replayRecordHasTargetHint(record, .enrichment);
            const managed_indexes = try collectManagedIndexCandidates(
                alloc,
                index_manager,
                generated_source_advanced,
            );
            defer alloc.free(managed_indexes);

            var full_text_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (full_text_indexes.items) |name| alloc.free(@constCast(name));
                full_text_indexes.deinit(alloc);
            }
            var all_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (all_indexes.items) |name| alloc.free(@constCast(name));
                all_indexes.deinit(alloc);
            }
            var target_identities = std.ArrayListUnmanaged(IndexTargetVisibility).empty;
            errdefer {
                for (target_identities.items) |identity| alloc.free(@constCast(identity.index_name));
                target_identities.deinit(alloc);
            }

            var target_scope_known = true;
            for (managed_indexes) |candidate| {
                const applicability = managedIndexRecordApplicability(index_manager, record, candidate.ref);
                switch (applicability) {
                    .irrelevant, .missing_dependency => {},
                    .relevant => {
                        try appendOwnedManagedIndexName(alloc, &all_indexes, candidate.ref.name);
                        if (candidate.ref.kind == .full_text)
                            try appendOwnedManagedIndexName(alloc, &full_text_indexes, candidate.ref.name);
                    },
                }
                if (applicability != .irrelevant or
                    (generated_source_advanced and candidate.consumes_generated_enrichment))
                {
                    try appendManagedTargetIdentity(
                        alloc,
                        &target_identities,
                        candidate.config,
                        // Producer reruns can replace or remove generated relationships,
                        // postings, and vectors even when the direct mutation only adds.
                        if (generated_source_advanced and candidate.consumes_generated_enrichment)
                            .may_reduce
                        else
                            managedIndexRecordServingSetEffect(index_manager, record, candidate.ref),
                        &target_scope_known,
                    );
                }
            }

            return try finishManagedSyncTargets(
                alloc,
                &full_text_indexes,
                &all_indexes,
                &target_identities,
                target_scope_known,
            );
        }

        pub fn collectManagedSyncTargetsForRecordWithBatch(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            record: change_journal_mod.Record,
            batch: derived_types.DerivedBatch,
        ) !ManagedSyncTargets {
            // The journal's scheduling decision is authoritative for generated
            // consumers. Keep batch operation detail and predecoded artifact routing
            // for direct consumers, avoiding irrelevant text work or O(indexes*artifacts)
            // embedding identity decoding in the materialized commit path.
            return collectManagedSyncTargetsWithGeneratedSource(alloc, index_manager, batch, replayRecordHasTargetHint(record, .enrichment));
        }

        pub fn collectManagedSyncTargetsWithGeneratedSource(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            generated_source_advanced: bool,
        ) !ManagedSyncTargets {
            // Decode artifact identities once for the commit plan. Previously every
            // dense and sparse index reparsed and allocated the same identity for every
            // changed artifact while the primary apply fence was held.
            var routing_arena = std.heap.ArenaAllocator.init(alloc);
            defer routing_arena.deinit();
            var changed_embedding_names = std.StringHashMapUnmanaged(void).empty;
            defer changed_embedding_names.deinit(alloc);
            for (batch.changed_artifact_keys) |artifact_key| {
                var identity = (artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key) catch continue) orelse continue;
                defer identity.deinit(alloc);
                if (changed_embedding_names.contains(identity.embedding_name)) continue;
                try changed_embedding_names.put(alloc, try routing_arena.allocator().dupe(u8, identity.embedding_name), {});
            }

            const managed_indexes = try collectManagedIndexCandidates(
                alloc,
                index_manager,
                generated_source_advanced,
            );
            defer alloc.free(managed_indexes);

            var full_text_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (full_text_indexes.items) |name| alloc.free(@constCast(name));
                full_text_indexes.deinit(alloc);
            }
            var all_indexes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (all_indexes.items) |name| alloc.free(@constCast(name));
                all_indexes.deinit(alloc);
            }
            var target_identities = std.ArrayListUnmanaged(IndexTargetVisibility).empty;
            errdefer {
                for (target_identities.items) |identity| alloc.free(@constCast(identity.index_name));
                target_identities.deinit(alloc);
            }

            var target_scope_known = true;
            for (managed_indexes) |candidate| {
                const applicability = managedIndexBatchApplicabilityWithEmbeddingNames(index_manager, batch, candidate.ref, &changed_embedding_names);
                switch (applicability) {
                    .irrelevant, .missing_dependency => {},
                    .relevant => {
                        try appendOwnedManagedIndexName(alloc, &all_indexes, candidate.ref.name);
                        if (candidate.ref.kind == .full_text)
                            try appendOwnedManagedIndexName(alloc, &full_text_indexes, candidate.ref.name);
                    },
                }
                if (applicability != .irrelevant or
                    (generated_source_advanced and candidate.consumes_generated_enrichment))
                {
                    try appendManagedTargetIdentity(
                        alloc,
                        &target_identities,
                        candidate.config,
                        if (generated_source_advanced and candidate.consumes_generated_enrichment)
                            .may_reduce
                        else
                            managedIndexBatchServingSetEffect(index_manager, batch, candidate.ref),
                        &target_scope_known,
                    );
                }
            }

            return try finishManagedSyncTargets(
                alloc,
                &full_text_indexes,
                &all_indexes,
                &target_identities,
                target_scope_known,
            );
        }

        pub fn collectPendingDocumentUnitDenseChunkEmbeddings(
            alloc: Allocator,
            db: anytype,
            chunk_view: *const DocumentExtractionChunkView,
            out: *std.ArrayListUnmanaged(PendingDocumentUnitDenseChunkEmbedding),
        ) !void {
            const runtime = db.enrichment_runtime orelse return;
            if (runtime.config.dense_embedder == null) return;
            for (chunk_view.dense_embeddings) |embed_entry| {
                if (embed_entry.consumer_indexes.len == 0) continue;
                // `embedding_name`/`source_field`/`producer_json` borrow from
                // `chunk_view`, which outlives this pending struct (both live only
                // within one computeDocumentExtractionAssetRequestDerived call);
                // `consumer_indexes` is duplicated because PendingDocumentUnit*
                // ChunkEmbedding.deinit frees it, and it must not free memory the
                // view's own deinit also owns.
                const consumer_indexes = try dupeConsumerIndexNames(alloc, embed_entry.consumer_indexes);
                errdefer {
                    for (consumer_indexes) |name| alloc.free(name);
                    alloc.free(consumer_indexes);
                }
                try out.append(alloc, .{
                    .embedding_name = embed_entry.name,
                    .source_field = embed_entry.source_field,
                    .expected_dims = embed_entry.expected_dims,
                    .producer_json = embed_entry.producer_json,
                    .consumer_indexes = consumer_indexes,
                });
            }
        }

        pub fn collectPendingDocumentUnitSparseChunkEmbeddings(
            alloc: Allocator,
            db: anytype,
            chunk_view: *const DocumentExtractionChunkView,
            out: *std.ArrayListUnmanaged(PendingDocumentUnitSparseChunkEmbedding),
        ) !void {
            const runtime = db.enrichment_runtime orelse return;
            if (runtime.config.sparse_embedder == null) return;
            for (chunk_view.sparse_embeddings) |embed_entry| {
                if (embed_entry.consumer_indexes.len == 0) continue;
                const consumer_indexes = try dupeConsumerIndexNames(alloc, embed_entry.consumer_indexes);
                errdefer {
                    for (consumer_indexes) |name| alloc.free(name);
                    alloc.free(consumer_indexes);
                }
                try out.append(alloc, .{
                    .embedding_name = embed_entry.name,
                    .producer_json = embed_entry.producer_json,
                    .consumer_indexes = consumer_indexes,
                });
            }
        }

        pub fn collectTextReplayDeleteKeys(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_name: []const u8,
            publication_context: index_manager_mod.TextPublicationContext,
        ) ![]const []const u8 {
            var keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer keys.deinit(alloc);
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);

            for (batch.deleted_keys) |key| {
                if (!try index_manager.textPublicationContextRetiresDeletedKeyAssumeCatalogLocked(index_name, publication_context, key)) continue;
                try appendUniqueBorrowedKeyWithSet(alloc, &keys, &seen, key);
            }
            for (batch.overwritten_doc_keys) |key| {
                if (!try index_manager.textPublicationContextConsumesKeyAssumeCatalogLocked(index_name, publication_context, key)) continue;
                try appendUniqueBorrowedKeyWithSet(alloc, &keys, &seen, key);
            }
            for (batch.documents) |doc| {
                if (!documentTargetsTextIndex(doc, index_name, publication_context.chunk_backed)) continue;
                try appendUniqueBorrowedKeyWithSet(alloc, &keys, &seen, doc.key);
            }

            return try keys.toOwnedSlice(alloc);
        }

        pub fn collectVectorReplayDeleteKeys(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            index_ref: index_manager_mod.ManagedIndexRef,
            deleted_keys: []const []const u8,
        ) ![][]u8 {
            std.debug.assert(index_ref.kind == .dense_vector or index_ref.kind == .sparse_vector);
            var keys = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (keys.items) |key| alloc.free(key);
                keys.deinit(alloc);
            }
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            const embedding_names: []const []const u8 = switch (index_ref.kind) {
                .dense_vector => if (index_manager.denseIndex(index_ref.name)) |entry| entry.embedding_names else &.{},
                .sparse_vector => if (index_manager.sparseIndex(index_ref.name)) |entry| entry.embedding_names else &.{},
                else => unreachable,
            };

            for (deleted_keys) |key| {
                if (!internal_keys.isEmbeddingArtifactKey(key) and !internal_keys.isDerivedEmbeddingArtifactKey(key)) {
                    if (!managedIndexDeleteKeyAffectsProjection(index_manager, index_ref, key)) continue;
                    if (embedding_names.len > 0 and internal_keys.isChunkArtifactRecordKey(key)) {
                        // Multi-source projections key members by embedding artifact,
                        // not by chunk. Derive the retired identities from the bound
                        // producers; the source rows no longer exist at replay time.
                        for (embedding_names) |name| {
                            const producer = index_manager.getEnrichment(.embedding, name) orelse continue;
                            if (!internal_keys.matchesChunkArtifactName(key, producer.source_artifact_name)) continue;
                            const member_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, key, name);
                            defer alloc.free(member_key);
                            try appendUniqueOwnedKeyIndexed(alloc, &keys, &seen, member_key);
                        }
                        continue;
                    }
                    try appendUniqueOwnedKeyIndexed(alloc, &keys, &seen, key);
                    continue;
                }

                var identity = artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, key) catch |err| switch (err) {
                    error.InvalidInternalUserKey => continue,
                    else => return err,
                } orelse continue;
                defer identity.deinit(alloc);
                if (!managedIndexConsumesEmbeddingName(index_manager, index_ref, identity.embedding_name)) continue;
                try appendUniqueOwnedKeyIndexed(alloc, &keys, &seen, if (embedding_names.len > 0) key else identity.doc_key);
            }
            return try keys.toOwnedSlice(alloc);
        }

        pub fn commitGraphContenderReconcilePage(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            affected_edge_keys: []const []const u8,
            reconciled: *const GraphContenderReconcileResult,
            extra_writes: []const docstore_mod.KVPair,
            extra_deletes: []const []const u8,
        ) !void {
            var mutation = try prepareGraphContenderReconcilePage(alloc, affected_edge_keys, reconciled, extra_writes, extra_deletes);
            defer mutation.deinit(alloc);
            var changed_batch = try filterChangedGraphMaterializationBatch(alloc, store, mutation.writes.items, mutation.deletes.items);
            defer changed_batch.deinit(alloc);
            if (changed_batch.writes.len > 0 or changed_batch.deletes.len > 0) {
                if (builtin.is_test) if (D.test_before_graph_contender_commit.*) |hook| {
                    for (changed_batch.writes) |write| {
                        if (internal_keys.isGraphGlobalEdgeContenderKey(write.key)) {
                            hook.call(hook.ctx);
                            break;
                        }
                    }
                };
                try store.putBatch(changed_batch.writes, changed_batch.deletes);
            }
        }

        pub fn computeAssetRequestDerived(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
            deferred_asset_producer_items: ?*std.ArrayListUnmanaged(PrecomputeAssetProducerBatchItem),
            force_reprocess: bool,
            document_execution: ?*enrichment_runtime_mod.PrecommitDocumentExecution,
            coverage_outcomes: *std.ArrayListUnmanaged(PrecomputedCoverageOutcome),
        ) !void {
            var producer_cfg = try asset_producer_mod.parseProducerConfig(alloc, request.producer_json);
            defer producer_cfg.deinit(alloc);

            const artifact_name = requestArtifactName(request);
            const key = try internal_keys.artifactNamedPrefixAlloc(alloc, request.doc_key, "asset", artifact_name);
            defer alloc.free(key);

            // Resolved once, under IndexManager.catalog_mutex.lockShared(), before
            // any chunking/inference work below; both the "no source text" delete
            // path and computeDocumentExtractionAssetRequestDerived need it, and
            // neither may read the live catalog arrays themselves afterward.
            var document_extraction_view: DocumentExtractionCatalogView = .{};
            defer document_extraction_view.deinit(alloc);
            if (producer_cfg.type == .document_extraction) {
                document_extraction_view = try buildDocumentExtractionCatalogView(alloc, db, artifact_name);
            }

            const text_indexes: []const []const u8 = request.consumer_indexes;

            // Asset-consumes-asset: the source is another asset's produced bytes.
            // Requests are planned upstream-first, so a producer computed earlier in
            // this same batch sits in `artifact_writes` rather than the store; the
            // overlay scan (newest first) keeps the chain convergent within one
            // synchronous pass. A missing upstream takes the delete path below and
            // is re-driven by the per-document replay record once the upstream lands.
            const consumes_upstream = request.upstream_artifact_name.len > 0 and
                producer_cfg.type != .document_extraction;
            const source_text: ?[]const u8 = if (consumes_upstream) blk: {
                const upstream_key = try internal_keys.artifactNamedPrefixAlloc(alloc, request.doc_key, "asset", request.upstream_artifact_name);
                defer alloc.free(upstream_key);
                var i = artifact_writes.items.len;
                while (i > 0) {
                    i -= 1;
                    const write = artifact_writes.items[i];
                    if (std.mem.eql(u8, write.key, upstream_key)) {
                        break :blk try alloc.dupe(u8, write.value);
                    }
                }
                for (artifact_delete_keys.items) |delete_key| {
                    if (std.mem.eql(u8, delete_key, upstream_key)) break :blk null;
                }
                break :blk try db.core.getStoreValue(alloc, upstream_key);
            } else try extractAssetSourceValue(alloc, db, doc_value, request);
            if (source_text == null or source_text.?.len == 0) {
                if (source_text) |s| alloc.free(s);
                try appendFullTextDeleteDocument(alloc, documents, key, text_indexes);
                if (producer_cfg.type == .document_extraction) {
                    try appendDocumentExtractionDeleteKeys(alloc, db, &document_extraction_view, request.doc_key, artifact_name, key, artifact_delete_keys);
                    try appendPrecomputedArtifactCoverageOutcomes(db, alloc, coverage_outcomes, request, .skipped);
                    return;
                }
                try appendOwnedKey(alloc, artifact_delete_keys, key);
                const state_key = try assetStateKeyAlloc(alloc, request.doc_key, artifact_name);
                errdefer alloc.free(state_key);
                try artifact_delete_keys.append(alloc, state_key);
                try appendPrecomputedArtifactCoverageOutcomes(db, alloc, coverage_outcomes, request, .skipped);
                return;
            }
            defer alloc.free(source_text.?);

            if (producer_cfg.type == .document_extraction) {
                try computeDocumentExtractionAssetRequestDerived(
                    alloc,
                    db,
                    &document_extraction_view,
                    doc_value,
                    source_text.?,
                    request,
                    producer_cfg.config_json,
                    key,
                    artifact_writes,
                    artifact_delete_keys,
                    documents,
                    dense_embeddings,
                    sparse_embeddings,
                    force_reprocess,
                    document_execution,
                );
                var selected_manifest: ?[]const u8 = null;
                var i = artifact_writes.items.len;
                while (i > 0) {
                    i -= 1;
                    const write = artifact_writes.items[i];
                    if (!std.mem.eql(u8, write.key, key)) continue;
                    selected_manifest = write.value;
                    break;
                }
                const retained_manifest = if (selected_manifest == null) try db.core.getStoreValue(alloc, key) else null;
                defer if (retained_manifest) |value| alloc.free(value);
                if (selected_manifest orelse retained_manifest) |manifest| {
                    const failed = try documentExtractionManifestHasLastError(alloc, manifest) or
                        try enrichment_runtime_mod.documentExtractionEmptyCoverageIsTerminalFailure(alloc, manifest);
                    try appendPrecomputedArtifactCoverageOutcomes(db, alloc, coverage_outcomes, request, if (failed) .terminal_failed else .produced);
                }
                return;
            }

            const source_parts_json = if (producer_cfg.type != .copy and request.source_template.len > 0)
                try renderSourcePartsJson(alloc, db, doc_value, request)
            else
                null;
            defer if (source_parts_json) |value| alloc.free(value);

            const state_key = if (producer_cfg.type != .copy)
                try assetStateKeyAlloc(alloc, request.doc_key, artifact_name)
            else
                null;
            defer if (state_key) |value| alloc.free(value);
            const state_value = if (producer_cfg.type != .copy)
                try assetStateValueAlloc(alloc, source_text.?, source_parts_json, request.producer_json)
            else
                null;
            defer if (state_value) |value| alloc.free(value);
            if (!force_reprocess and state_key != null and state_value != null) {
                const existing_state = try db.core.getStoreValue(alloc, state_key.?);
                defer if (existing_state) |value| alloc.free(value);
                if (existing_state != null and std.mem.eql(u8, existing_state.?, state_value.?)) {
                    const existing_asset = try db.core.getStoreValue(alloc, key);
                    defer if (existing_asset) |value| alloc.free(value);
                    if (existing_asset) |value| {
                        try artifact_writes.append(alloc, .{
                            .key = try alloc.dupe(u8, key),
                            .value = try alloc.dupe(u8, value),
                        });
                        try appendInlineFullTextDocument(alloc, documents, key, value, text_indexes);
                        try appendPrecomputedArtifactCoverageOutcomes(db, alloc, coverage_outcomes, request, .produced);
                        return;
                    }
                }
            }

            if (producer_cfg.type != .copy) {
                if (deferred_asset_producer_items) |items| {
                    const request_clone = try enrichment_types.cloneGeneratedRequest(alloc, request);
                    errdefer enrichment_types.freeGeneratedRequest(alloc, request_clone);
                    const config_json = producer_cfg.config_json;
                    producer_cfg.config_json = "";
                    errdefer if (config_json.len > 0) alloc.free(@constCast(config_json));
                    const item_source_text = try alloc.dupe(u8, source_text.?);
                    errdefer alloc.free(item_source_text);
                    const item_source_parts_json = if (source_parts_json) |parts| try alloc.dupe(u8, parts) else null;
                    errdefer if (item_source_parts_json) |parts| alloc.free(parts);
                    const item_artifact_key = try alloc.dupe(u8, key);
                    errdefer alloc.free(item_artifact_key);
                    const item_state_key = try alloc.dupe(u8, state_key.?);
                    errdefer alloc.free(item_state_key);
                    const item_state_value = try alloc.dupe(u8, state_value.?);
                    errdefer alloc.free(item_state_value);
                    const item = PrecomputeAssetProducerBatchItem{
                        .request = request_clone,
                        .producer_type = producer_cfg.type,
                        .config_json = @constCast(config_json),
                        .source_text = item_source_text,
                        .source_parts_json = item_source_parts_json,
                        .artifact_key = item_artifact_key,
                        .state_key = item_state_key,
                        .state_value = item_state_value,
                    };
                    try appendPrecomputeAssetProducerBatchItem(alloc, db, items, item, artifact_writes, documents, coverage_outcomes);
                    return;
                }
            }

            const value = if (producer_cfg.type == .copy) source_text.? else blk: {
                const runtime = db.enrichment_runtime orelse return error.MissingAssetProducer;
                const producer = runtime.config.asset_producer orelse return error.MissingAssetProducer;
                break :blk try producer.produce(alloc, .{
                    .producer_type = producer_cfg.type,
                    .config_json = producer_cfg.config_json,
                    .source_text = source_text.?,
                    .source_parts_json = source_parts_json,
                    .content_type = request.content_type,
                });
            };
            defer if (producer_cfg.type != .copy) alloc.free(value);

            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, key),
                .value = try alloc.dupe(u8, value),
            });
            try appendInlineFullTextDocument(alloc, documents, key, value, text_indexes);

            try appendPrecomputedArtifactCoverageOutcomes(db, alloc, coverage_outcomes, request, .produced);

            if (producer_cfg.type != .copy) {
                try artifact_writes.append(alloc, .{
                    .key = try alloc.dupe(u8, state_key.?),
                    .value = try alloc.dupe(u8, state_value.?),
                });
            }
        }

        pub fn computeChunkRequestDerived(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
        ) !void {
            if (!requestHasChunking(request)) return;

            const artifact_name = requestArtifactName(request);
            const chunks = try getOrCreateChunks(alloc, db, doc_value, request, cache);
            const desired_chunk_keys = try chunkArtifactKeysForChunksAlloc(alloc, request.doc_key, artifact_name, chunks);
            defer freeChunkArtifactKeys(alloc, desired_chunk_keys);

            const persist_chunks = try shouldStoreChunkArtifacts(alloc, request);
            if (persist_chunks) {
                try appendChunkArtifactWrites(alloc, request.doc_key, request.source_field, artifact_name, chunks, artifact_writes, true);
            }
            try appendStaleChunkArtifactDeleteKeys(
                alloc,
                db,
                request.doc_key,
                artifact_name,
                desired_chunk_keys,
                artifact_delete_keys,
            );
            if (chunks.len == 0) return;

            const text_indexes: []const []const u8 = request.consumer_indexes;
            if (text_indexes.len == 0) return;

            var arena_state = std.heap.ArenaAllocator.init(alloc);
            defer arena_state.deinit();
            const scratch = arena_state.allocator();

            for (chunks) |chunk| {
                if (!chunk.isText()) continue;
                const key = try internal_keys.chunkArtifactKeyAlloc(alloc, request.doc_key, artifact_name, @intCast(chunk.chunk_id));
                defer alloc.free(key);

                const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
                errdefer {
                    for (targets) |target| alloc.free(target.index_name);
                    alloc.free(targets);
                }
                for (text_indexes, 0..) |index_name, i| {
                    targets[i] = .{
                        .kind = .full_text,
                        .index_name = try alloc.dupe(u8, index_name),
                    };
                }
                const payload = try buildChunkArtifactPayloadAlloc(scratch, request.doc_key, artifact_name, request.source_field, chunk, true);

                try documents.append(alloc, .{
                    .key = try alloc.dupe(u8, key),
                    .action = .upsert,
                    .cleaned_value = try alloc.dupe(u8, payload),
                    .targets = targets,
                });

                _ = arena_state.reset(.retain_capacity);
            }
        }

        pub fn computeDenseMaterializedChunkRequestImpl(
            alloc: Allocator,
            db: anytype,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            pending_deletes: *const std.StringHashMapUnmanaged(void),
            dense_embeddings: anytype,
            skip_unchanged_artifacts: bool,
            shared_pending_writes: ?*const PendingArtifactWriteIndex,
            comptime appendForConsumers: anytype,
            dense_embedder: embedder_mod.DenseEmbedder,
            embedding_name: []const u8,
            consumer_indexes: []const []const u8,
        ) !void {
            const artifact_name = requestArtifactName(request);
            const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, request.doc_key, "chunk", artifact_name);
            defer alloc.free(prefix);
            var local_pending_writes = if (shared_pending_writes == null)
                try PendingArtifactWriteIndex.init(alloc, artifact_writes.items)
            else
                PendingArtifactWriteIndex{};
            defer local_pending_writes.deinit(alloc);
            const pending_writes = shared_pending_writes orelse &local_pending_writes;
            const pending_lookup: ?*const PendingArtifactWriteIndex = if (skip_unchanged_artifacts) pending_writes else null;

            const max_batch_items = generatedEmbedBatchItems();
            const max_batch_bytes = generatedEmbedBatchBytes();
            var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
            defer {
                clearChunkEmbeddingSourceList(alloc, &sources);
                sources.deinit(alloc);
            }
            var batch_source_bytes: usize = 0;
            var pending_chunk_keys = std.StringHashMapUnmanaged(void).empty;
            defer pending_chunk_keys.deinit(alloc);

            // The view retains stable write slices, even when embedding writes grow and
            // relocate the batch list during a provider flush.
            for (pending_writes.chunkWritesForDoc(request.doc_key)) |write| {
                if (!std.mem.startsWith(u8, write.key, prefix) or
                    !internal_keys.matchesChunkArtifactName(write.key, artifact_name)) continue;
                if (pending_chunk_keys.contains(write.key)) continue;
                try pending_chunk_keys.put(alloc, write.key, {});
                _ = try appendMaterializedChunkSourceToBatch(alloc, &sources, &batch_source_bytes, write.key, write.value, request.source_field);
                if (sources.items.len >= max_batch_items or batch_source_bytes >= max_batch_bytes) {
                    try flushGeneratedDenseChunkSourceBatch(alloc, db, runtime, dense_embedder, embedding_name, request, artifact_writes, dense_embeddings, &sources, consumer_indexes, skip_unchanged_artifacts, pending_lookup, appendForConsumers);
                    batch_source_bytes = 0;
                }
            }
            try flushGeneratedDenseChunkSourceBatch(alloc, db, runtime, dense_embedder, embedding_name, request, artifact_writes, dense_embeddings, &sources, consumer_indexes, skip_unchanged_artifacts, pending_lookup, appendForConsumers);
            batch_source_bytes = 0;

            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |key| alloc.free(key);
            const upper_bound = if (upper) |key| key else "";
            var lower = try alloc.dupe(u8, prefix);
            defer alloc.free(lower);
            while (true) {
                const next_lower = try scanMaterializedChunkSourceStoreBatch(alloc, db, prefix, upper_bound, lower, request.source_field, &pending_chunk_keys, pending_deletes, &sources, &batch_source_bytes, max_batch_items, max_batch_bytes);
                try flushGeneratedDenseChunkSourceBatch(alloc, db, runtime, dense_embedder, embedding_name, request, artifact_writes, dense_embeddings, &sources, consumer_indexes, skip_unchanged_artifacts, pending_lookup, appendForConsumers);
                batch_source_bytes = 0;
                if (next_lower) |owned_next| {
                    alloc.free(lower);
                    lower = owned_next;
                    continue;
                }
                break;
            }
        }

        pub fn computeDenseRequestDerived(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            pending_deletes: *const std.StringHashMapUnmanaged(void),
            dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
            memo: ?*GeneratedEmbeddingMemo,
            shared_pending_writes: ?*const PendingArtifactWriteIndex,
        ) !void {
            return computeDenseRequestImpl(alloc, db, doc_value, request, artifact_writes, pending_deletes, dense_embeddings, cache, true, memo, shared_pending_writes, appendDerivedDenseEmbeddingForConsumers);
        }

        pub fn computeDenseRequestImpl(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            pending_deletes: *const std.StringHashMapUnmanaged(void),
            dense_embeddings: anytype,
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
            skip_unchanged_artifacts: bool,
            memo: ?*GeneratedEmbeddingMemo,
            shared_pending_writes: ?*const PendingArtifactWriteIndex,
            comptime appendForConsumers: anytype,
        ) !void {
            if (memo) |preservation| if (preservation.reuse_stored_artifacts) {
                if (try preparePreservedEmbeddingSources(alloc, db, doc_value, request, artifact_writes.items, pending_deletes, cache, shared_pending_writes)) |sources| {
                    defer freeChunkEmbeddingSources(alloc, sources);
                    for (sources) |source| {
                        const artifact_key = try embeddingArtifactKeyForBaseAlloc(alloc, source.key, requestEmbeddingName(request));
                        defer alloc.free(artifact_key);
                        try appendForConsumers(alloc, dense_embeddings, source.key, if (requestUsesChunkSource(request)) request.doc_key else null, artifact_key, &.{}, request.consumer_indexes);
                    }
                    return;
                }
            };
            const runtime = db.enrichment_runtime orelse return error.MissingDenseEmbedder;
            const dense_embedder = runtime.config.dense_embedder orelse return error.MissingDenseEmbedder;

            const embedding_name = requestEmbeddingName(request);
            const consumer_indexes: []const []const u8 = request.consumer_indexes;
            if (consumer_indexes.len == 0) return;

            if (requestUsesChunkSource(request)) {
                if (requestUsesPinnedMaterializedChunkArtifact(request)) {
                    try computeDenseMaterializedChunkRequestImpl(alloc, db, runtime, request, artifact_writes, pending_deletes, dense_embeddings, skip_unchanged_artifacts, shared_pending_writes, appendForConsumers, dense_embedder, embedding_name, consumer_indexes);
                    return;
                }
                var chunks_created: usize = 0;
                const sources = try chunkEmbeddingSourcesForRequest(alloc, db, doc_value, request, cache, &chunks_created);
                defer freeChunkEmbeddingSources(alloc, sources);
                if (sources.len == 0) return;
                enrichment_runtime_mod.noteIndexChunksCreated(runtime, consumer_indexes, chunks_created);
                var local_pending_writes = if (skip_unchanged_artifacts and shared_pending_writes == null)
                    try PendingArtifactWriteIndex.init(alloc, artifact_writes.items)
                else
                    PendingArtifactWriteIndex{};
                defer local_pending_writes.deinit(alloc);
                const pending_lookup: ?*const PendingArtifactWriteIndex = if (skip_unchanged_artifacts) shared_pending_writes orelse &local_pending_writes else null;

                var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
                defer chunk_texts.deinit(alloc);
                var source_indexes = std.ArrayListUnmanaged(usize).empty;
                defer source_indexes.deinit(alloc);
                const max_batch_items = generatedEmbedBatchItems();
                const max_batch_bytes = generatedEmbedBatchBytes();
                var batch_source_bytes: usize = 0;
                for (sources, 0..) |source, i| {
                    const source_hash = enrichment_artifact_codec.hashEmbeddingSource(source.text, request.producer_json);
                    const artifact_key = try embeddingArtifactKeyForBaseAlloc(alloc, source.key, embedding_name);
                    defer alloc.free(artifact_key);
                    if (skip_unchanged_artifacts) {
                        if (try storedOrPendingEmbeddingSourceHash(db, pending_lookup, artifact_key)) |existing_hash| {
                            if (existing_hash == source_hash) {
                                try appendForConsumers(alloc, dense_embeddings, source.key, request.doc_key, artifact_key, &.{}, consumer_indexes);
                                continue;
                            }
                        }
                    }
                    if (chunk_texts.items.len > 0 and
                        (chunk_texts.items.len >= max_batch_items or batch_source_bytes + source.text.len > max_batch_bytes))
                    {
                        try flushGeneratedDenseChunkBatch(alloc, runtime, dense_embedder, embedding_name, request, artifact_writes, dense_embeddings, sources, &source_indexes, &chunk_texts, consumer_indexes, appendForConsumers);
                        batch_source_bytes = 0;
                    }
                    try chunk_texts.append(alloc, source.text);
                    try source_indexes.append(alloc, i);
                    batch_source_bytes += source.text.len;
                    if (chunk_texts.items.len >= max_batch_items or batch_source_bytes >= max_batch_bytes) {
                        try flushGeneratedDenseChunkBatch(alloc, runtime, dense_embedder, embedding_name, request, artifact_writes, dense_embeddings, sources, &source_indexes, &chunk_texts, consumer_indexes, appendForConsumers);
                        batch_source_bytes = 0;
                    }
                }
                try flushGeneratedDenseChunkBatch(alloc, runtime, dense_embedder, embedding_name, request, artifact_writes, dense_embeddings, sources, &source_indexes, &chunk_texts, consumer_indexes, appendForConsumers);
                return;
            }

            if (request.source_template.len > 0 and dense_embedder.supportsParts()) {
                const source_parts = try renderSourceParts(
                    alloc,
                    db,
                    doc_value,
                    request,
                    dense_embedder.mediaPartLimit(embedding_name),
                );
                if (source_parts) |parts| {
                    defer template_mod.freeContentParts(alloc, parts);

                    const vector = try enrichment_runtime_mod.embedDensePartsTracked(runtime, consumer_indexes, alloc, dense_embedder, embedding_name, parts, request.expected_dims);
                    defer alloc.free(vector);
                    const artifact_key = try appendEmbeddingArtifactWrite(
                        alloc,
                        artifact_writes,
                        request.doc_key,
                        request.doc_key,
                        embedding_name,
                        request.source_field,
                        null,
                        .{ .generated = null },
                        vector,
                    );
                    defer alloc.free(artifact_key);
                    try appendForConsumers(alloc, dense_embeddings, request.doc_key, null, artifact_key, vector, consumer_indexes);
                    return;
                }
            }

            const source_text = if (request.source_template.len > 0)
                renderSourceTemplateText(alloc, db, request.source_template, doc_value) catch |err| switch (err) {
                    error.OutOfMemory, error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                    else => null,
                }
            else
                try extractStringField(alloc, doc_value, request.source_field);
            if (source_text == null or source_text.?.len == 0) {
                if (source_text) |s| alloc.free(s);
                return;
            }
            defer alloc.free(source_text.?);

            const memo_key = GeneratedEmbeddingMemo.key(
                .dense_embedding,
                embedding_name,
                request.producer_json,
                request.execution_json,
                request.expected_dims,
                source_text.?,
            );
            // Document tables build their generated plan under apply rather than in
            // relational prewarm. Use the same persisted-cache admission in either
            // path before falling back to the external provider.
            if (memo) |cache_memo| if (!cache_memo.dense.contains(memo_key)) {
                _ = try prewarmGeneratedMemoFromArtifact(db, cache_memo, request, source_text.?, memo_key);
            };
            var uncached_vector: ?[]f32 = null;
            defer if (uncached_vector) |owned| alloc.free(owned);
            const vector: []const f32 = if (memo) |cache_memo|
                cache_memo.dense.get(memo_key) orelse blk: {
                    const computed = try enrichment_runtime_mod.embedDenseTracked(
                        runtime,
                        consumer_indexes,
                        cache_memo.alloc,
                        dense_embedder,
                        embedding_name,
                        source_text.?,
                        request.expected_dims,
                    );
                    errdefer cache_memo.alloc.free(computed);
                    break :blk try cache_memo.adoptDense(memo_key, computed);
                }
            else blk: {
                uncached_vector = try enrichment_runtime_mod.embedDenseTracked(runtime, consumer_indexes, alloc, dense_embedder, embedding_name, source_text.?, request.expected_dims);
                break :blk uncached_vector.?;
            };
            const artifact_key = try appendEmbeddingArtifactWrite(
                alloc,
                artifact_writes,
                request.doc_key,
                request.doc_key,
                embedding_name,
                request.source_field,
                null,
                .{ .generated = enrichment_artifact_codec.hashEmbeddingSource(source_text.?, request.producer_json) },
                vector,
            );
            defer alloc.free(artifact_key);
            try appendForConsumers(alloc, dense_embeddings, request.doc_key, null, artifact_key, vector, consumer_indexes);
        }

        pub fn computeDocumentExtractionAssetRequestDerived(
            alloc: Allocator,
            db: anytype,
            view: *const DocumentExtractionCatalogView,
            doc_value: []const u8,
            source_url: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            config_json: []const u8,
            manifest_key: []const u8,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            artifact_delete_keys: *std.ArrayListUnmanaged([]const u8),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
            force_reprocess: bool,
            document_execution: ?*enrichment_runtime_mod.PrecommitDocumentExecution,
        ) !void {
            const artifact_name = requestArtifactName(request);
            var config = try document_extraction_mod.parseConfig(alloc, config_json);
            defer config.deinit(alloc);
            enrichment_runtime_mod.applyDocumentExtractionRuntimePolicy(&config);
            try document_extraction_mod.applySourceMetadataFromJson(alloc, &config, doc_value);

            const state_key = try assetStateKeyAlloc(alloc, request.doc_key, artifact_name);
            defer alloc.free(state_key);
            const existing_state = db.core.getStoreValue(alloc, state_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (existing_state) |value| alloc.free(value);
            const existing_manifest = db.core.getStoreValue(alloc, manifest_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (existing_manifest) |value| alloc.free(value);
            var previous_child_ranges: []types.DocumentArtifactChildRange = &.{};
            defer freeDocumentArtifactChildRanges(alloc, previous_child_ranges);
            if (existing_manifest) |value| {
                previous_child_ranges = documentArtifactChildRangesFromManifestJsonAlloc(alloc, value) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => &.{},
                };
            }

            const from_generation = if (existing_manifest) |value|
                documentExtractionManifestGeneration(alloc, value) catch 0
            else
                0;
            const to_generation = from_generation + 1;

            const metadata_fingerprint = try document_extraction_mod.metadataFingerprintAlloc(alloc, source_url, config_json, config);
            defer if (metadata_fingerprint) |fingerprint| alloc.free(fingerprint);
            if (!force_reprocess) {
                if (metadata_fingerprint) |fingerprint| {
                    if (existing_state) |state| {
                        if (documentExtractionStateFingerprintMatches(alloc, state, fingerprint) and
                            documentExtractionStateHasChunkUnitFingerprints(alloc, state))
                        {
                            if (existing_manifest) |value| {
                                const manifest_has_last_error = documentExtractionManifestHasLastError(alloc, value) catch |err| switch (err) {
                                    error.OutOfMemory => return err,
                                    else => true,
                                };
                                if (!manifest_has_last_error) {
                                    try appendDocumentExtractionNavigationBackfill(
                                        alloc,
                                        db,
                                        request.doc_key,
                                        artifact_name,
                                        fingerprint,
                                        state,
                                        from_generation,
                                        artifact_writes,
                                        artifact_delete_keys,
                                    );
                                    try artifact_writes.append(alloc, .{
                                        .key = try alloc.dupe(u8, manifest_key),
                                        .value = try alloc.dupe(u8, value),
                                    });
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            const extraction_resource_manager = if (db.enrichment_runtime) |runtime|
                runtime.config.resource_manager orelse runtime.index_manager.resource_manager
            else
                db.core.index_manager.resource_manager;
            var extraction_budgeted: ?resource_manager_mod.BudgetedAllocator = if (extraction_resource_manager) |manager|
                resource_manager_mod.BudgetedAllocator.initReclaiming(manager, .document_extraction_working_set, alloc, 1)
            else
                null;
            defer if (extraction_budgeted) |*budgeted| budgeted.deinit();
            const extraction_alloc = if (extraction_budgeted) |*budgeted| budgeted.allocator() else alloc;

            const fetched = template_remote.downloadRemoteContentOutcomeAllocWithRenderConfig(
                extraction_alloc,
                remoteRenderConfig(db, null),
                source_url,
                if (config.credentials.len > 0) config.credentials else null,
            ) catch |err| switch (err) {
                error.OutOfMemory => if (extraction_budgeted != null and extraction_budgeted.?.denied()) return enrichment_runtime_mod.documentExtractionBudgetDenialError(&extraction_budgeted.?) else return err,
                else => {
                    if (!document_extraction_mod.remoteContentErrorIsPermanent(err)) return err;
                    try appendDocumentExtractionFailureManifest(alloc, db, view, request.doc_key, artifact_name, source_url, manifest_key, existing_state, previous_child_ranges, from_generation, to_generation, @errorName(err), "remote content download failed", "remote_content_download", artifact_writes);
                    return;
                },
            };
            const downloaded = switch (fetched) {
                .ok => |content| content,
                .http_error => |http_error| {
                    if (document_extraction_mod.remoteHttpStatusIsTransient(http_error.status))
                        return error.RemoteDocumentFetchFailed;
                    const message = try std.fmt.allocPrint(alloc, "{s}: HTTP {d}", .{ http_error.message, http_error.status });
                    defer alloc.free(message);
                    try appendDocumentExtractionFailureManifest(alloc, db, view, request.doc_key, artifact_name, source_url, manifest_key, existing_state, previous_child_ranges, from_generation, to_generation, "RemoteDocumentFetchFailed", message, "remote_content_http", artifact_writes);
                    return;
                },
            };
            var downloaded_mut = downloaded;
            defer downloaded_mut.deinit(extraction_alloc);

            // Downloaded bytes, inspection state, and decoded extraction results are
            // charged at their actual live allocator size. OCR render scratch plus
            // provider/output memory are admitted atomically for each page window in
            // enrichment_runtime and released before the next window.
            const source_is_pdf = document_extraction_mod.resolvesToPdf(config, source_url, if (config.content_type.len > 0) config.content_type else downloaded_mut.content_type, downloaded_mut.data);
            const pdf_inspection_bytes: usize = if (source_is_pdf) config.pdf_decode_limits.max_working_set_bytes else 0;

            var pdf_inspection_reservation = enrichment_runtime_mod.ReservedWorkingSetAllocator.init(extraction_alloc, pdf_inspection_bytes);
            const document_extraction_alloc = if (source_is_pdf)
                pdf_inspection_reservation.allocator()
            else
                extraction_alloc;
            var extraction_config = config;
            if (source_is_pdf) {
                extraction_config.pdf_decode_limits.max_working_set_bytes = pdf_inspection_bytes;
                extraction_config.pdf_decode_limits.max_decoded_stream_bytes = @min(extraction_config.pdf_decode_limits.max_decoded_stream_bytes, pdf_inspection_bytes);
            }
            var extraction_failure: runtime_failure_abi.FailureIdentity = .{};
            var extraction = extractDocumentDownloadedAlloc(document_extraction_alloc, downloaded_mut, source_url, extraction_config, config_json, doc_value, &extraction_failure) catch |err| switch (err) {
                error.OutOfMemory => if (pdf_inspection_reservation.limit_exceeded)
                    return error.DocumentExtractionWorkingSetTooLarge
                else if (extraction_budgeted != null and extraction_budgeted.?.denied())
                    return enrichment_runtime_mod.documentExtractionBudgetDenialError(&extraction_budgeted.?)
                else
                    return err,
                else => {
                    const exact_error_name = boundaryFailureErrorName(&extraction_failure, err);
                    try appendDocumentExtractionFailureManifest(alloc, db, view, request.doc_key, artifact_name, source_url, manifest_key, existing_state, previous_child_ranges, from_generation, to_generation, exact_error_name, "document extraction failed", document_extraction_mod.failureStageFromErrorName(exact_error_name, "document_extraction"), artifact_writes);
                    return;
                },
            };
            defer extraction.deinit(document_extraction_alloc);
            if (db.enrichment_runtime) |runtime| {
                try enrichment_runtime_mod.completeDocumentExtractionGeneratedTextForRequestWithMemory(
                    if (document_execution) |execution| &execution.runtime else runtime,
                    document_extraction_alloc,
                    request,
                    config,
                    source_url,
                    downloaded_mut.data,
                    extraction.content_type,
                    &extraction,
                    .{
                        .native_backing_alloc = alloc,
                    },
                );
            } else if (document_extraction_mod.ocrEnabledForRoute(config, extraction.route_type) or config.transcription_enabled) {
                return error.MissingAssetProducer;
            }
            document_extraction_mod.rebaseUnitCharOffsets(extraction.units);

            const byte_source_fingerprint = if (metadata_fingerprint == null)
                try documentExtractionFingerprintAlloc(alloc, source_url, config_json, config.content_type, config.filename, downloaded_mut.content_type, downloaded_mut.data)
            else
                null;
            defer if (byte_source_fingerprint) |fingerprint| alloc.free(fingerprint);
            const source_fingerprint = metadata_fingerprint orelse byte_source_fingerprint.?;

            var desired_unit_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (desired_unit_keys.items) |key| alloc.free(@constCast(key));
                desired_unit_keys.deinit(alloc);
            }
            var desired_unit_fingerprints = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (desired_unit_fingerprints.items) |fingerprint| alloc.free(@constCast(fingerprint));
                desired_unit_fingerprints.deinit(alloc);
            }
            var desired_chunk_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (desired_chunk_keys.items) |key| alloc.free(@constCast(key));
                desired_chunk_keys.deinit(alloc);
            }

            try collectDocumentExtractionDesiredKeys(alloc, view, request.doc_key, artifact_name, extraction.units, &desired_unit_keys, &desired_unit_fingerprints, &desired_chunk_keys);

            const desired_unit_descriptors = try documentExtractionUnitDescriptorsFromKeysAlloc(alloc, desired_unit_keys.items, desired_unit_fingerprints.items);
            defer alloc.free(desired_unit_descriptors);

            const navigation_digest = try hierarchyNavigationArtifactDigestAlloc(alloc, desired_unit_descriptors);
            defer alloc.free(navigation_digest);
            const navigation_unit_count = std.math.cast(u32, desired_unit_descriptors.len) orelse
                return error.InvalidDocumentExtractionState;
            const navigation_block_count = hierarchyNavigationBlockCount(navigation_unit_count);

            const new_state = try documentExtractionStateValueAlloc(
                alloc,
                source_fingerprint,
                desired_unit_keys.items,
                desired_unit_descriptors,
                desired_chunk_keys.items,
                navigation_digest,
                navigation_block_count,
                true,
            );
            defer alloc.free(new_state);
            const navigation_summary_key = try internal_keys.documentUnitNavigationSummaryKeyAlloc(alloc, request.doc_key, artifact_name);
            defer alloc.free(navigation_summary_key);
            const existing_navigation_summary = try db.core.getStoreValue(alloc, navigation_summary_key);
            defer if (existing_navigation_summary) |value| alloc.free(value);

            var previous_state = DocumentExtractionPreviousState{};
            defer previous_state.deinit(alloc);

            if (existing_state) |state| {
                if (!force_reprocess and existing_navigation_summary != null and
                    std.mem.eql(u8, state, new_state) and
                    try hierarchy_navigation.indexMetadataMatches(alloc, state, existing_navigation_summary.?, from_generation))
                {
                    if (existing_manifest) |value| {
                        if (!(try documentExtractionManifestHasLastError(alloc, value))) {
                            try artifact_writes.append(alloc, .{
                                .key = try alloc.dupe(u8, manifest_key),
                                .value = try alloc.dupe(u8, value),
                            });
                            return;
                        }
                    }
                }

                previous_state = try loadDocumentExtractionPreviousState(alloc, db, view, request.doc_key, artifact_name, state);
                for (previous_state.unit_keys) |previous_key| {
                    if (containsDeleteKey(desired_unit_keys.items, previous_key)) continue;
                    try appendOwnedKey(alloc, artifact_delete_keys, previous_key);
                }
                for (previous_state.chunk_keys) |previous_key| {
                    if (containsDeleteKey(desired_chunk_keys.items, previous_key)) continue;
                    try appendOwnedKey(alloc, artifact_delete_keys, previous_key);
                }
                if (previous_state.recovered_from_store_scan) {
                    try appendOwnedKey(alloc, artifact_delete_keys, state_key);
                }
            }

            // Persist compact, directly addressable hierarchy metadata with the same
            // derived write batch as the manifest and unit payloads. Cursor requests
            // can then validate one summary and fetch only the blocks covering the
            // requested page instead of reparsing every unit descriptor.
            const navigation_summary = try hierarchyNavigationSummaryValueAlloc(
                alloc,
                to_generation,
                navigation_digest,
                navigation_unit_count,
                navigation_block_count,
            );
            defer alloc.free(navigation_summary);
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, navigation_summary_key),
                .value = try alloc.dupe(u8, navigation_summary),
            });
            var navigation_block_index: u32 = 0;
            while (navigation_block_index < navigation_block_count) : (navigation_block_index += 1) {
                const start = @as(usize, navigation_block_index) * hierarchy_navigation_block_size;
                const end = @min(start + hierarchy_navigation_block_size, desired_unit_descriptors.len);
                const block_key = try internal_keys.documentUnitNavigationBlockKeyAlloc(
                    alloc,
                    request.doc_key,
                    artifact_name,
                    navigation_block_index,
                );
                defer alloc.free(block_key);
                const block_value = try hierarchyNavigationBlockValueAlloc(
                    alloc,
                    navigation_block_index,
                    desired_unit_descriptors[start..end],
                );
                defer alloc.free(block_value);
                try artifact_writes.append(alloc, .{
                    .key = try alloc.dupe(u8, block_key),
                    .value = try alloc.dupe(u8, block_value),
                });
            }
            var obsolete_navigation_block = navigation_block_count;
            while (obsolete_navigation_block < previous_state.navigation_block_count) : (obsolete_navigation_block += 1) {
                try artifact_delete_keys.append(
                    alloc,
                    try internal_keys.documentUnitNavigationBlockKeyAlloc(
                        alloc,
                        request.doc_key,
                        artifact_name,
                        obsolete_navigation_block,
                    ),
                );
            }

            const text_indexes: []const []const u8 = request.consumer_indexes;

            const chunk_range_base_index = documentExtractionUnitRangeCount(extraction.units);
            for (extraction.units, desired_unit_descriptors, 0..) |unit, unit_descriptor, unit_index| {
                const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, request.doc_key, artifact_name, unit.unit_id);
                defer alloc.free(unit_key);
                const unit_range_id = try documentExtractionRangeIdAlloc(alloc, documentExtractionUnitRangeIndex(extraction.units, unit_index));
                defer alloc.free(unit_range_id);
                const unit_route = documentExtractionRangeRoute(previous_child_ranges, unit_range_id, "unit", artifact_name);
                const unit_unchanged = std.mem.eql(u8, unit_descriptor.key, unit_key) and
                    unitDescriptorFingerprintMatches(previous_state.unit_descriptors, unit_key, unit_descriptor.fingerprint);
                if (unit_unchanged and
                    try documentUnitCanSkipLocalWrites(alloc, db, view, request.doc_key, artifact_name, unit_key, unit_descriptor.fingerprint, unit, desired_chunk_keys.items, chunk_range_base_index, previous_child_ranges))
                {
                    if (force_reprocess) {
                        const payload = try documentUnitPayloadAlloc(alloc, request.doc_key, artifact_name, unit, unit_descriptor.fingerprint, source_url, extraction.content_type, unit_route);
                        defer alloc.free(payload);
                        try artifact_writes.append(alloc, .{
                            .key = try alloc.dupe(u8, unit_key),
                            .value = try alloc.dupe(u8, payload),
                        });
                        if (text_indexes.len > 0) {
                            const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
                            errdefer {
                                for (targets) |target| alloc.free(target.index_name);
                                alloc.free(targets);
                            }
                            for (text_indexes, 0..) |index_name, i| {
                                targets[i] = .{
                                    .kind = .full_text,
                                    .index_name = try alloc.dupe(u8, index_name),
                                };
                            }
                            try documents.append(alloc, .{
                                .key = try alloc.dupe(u8, unit_key),
                                .action = .upsert,
                                .cleaned_value = try alloc.dupe(u8, payload),
                                .targets = targets,
                            });
                        }
                        try appendDocumentUnitStoredChunkFullTextDocuments(alloc, view, request.doc_key, unit, documents);
                    } else {
                        try appendDocumentUnitStoredFullTextDocuments(alloc, view, request.doc_key, unit_key, unit, text_indexes, documents);
                    }
                    continue;
                }
                const payload = try documentUnitPayloadAlloc(alloc, request.doc_key, artifact_name, unit, unit_descriptor.fingerprint, source_url, extraction.content_type, unit_route);
                defer alloc.free(payload);
                try artifact_writes.append(alloc, .{
                    .key = try alloc.dupe(u8, unit_key),
                    .value = try alloc.dupe(u8, payload),
                });
                if (text_indexes.len > 0) {
                    const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
                    errdefer {
                        for (targets) |target| alloc.free(target.index_name);
                        alloc.free(targets);
                    }
                    for (text_indexes, 0..) |index_name, i| {
                        targets[i] = .{
                            .kind = .full_text,
                            .index_name = try alloc.dupe(u8, index_name),
                        };
                    }
                    try documents.append(alloc, .{
                        .key = try alloc.dupe(u8, unit_key),
                        .action = .upsert,
                        .cleaned_value = try alloc.dupe(u8, payload),
                        .targets = targets,
                    });
                }

                try appendDocumentUnitChunkWrites(alloc, db, view, request.doc_key, artifact_name, unit_key, unit_descriptor.fingerprint, unit, desired_chunk_keys.items, chunk_range_base_index, previous_child_ranges, artifact_writes, documents, dense_embeddings, sparse_embeddings);
            }

            const manifest = try documentExtractionManifestPayloadAlloc(
                alloc,
                request.doc_key,
                artifact_name,
                source_url,
                source_fingerprint,
                extraction,
                desired_unit_keys.items,
                desired_unit_descriptors,
                desired_chunk_keys.items,
                previous_child_ranges,
                previous_state.unit_keys,
                previous_state.unit_descriptors,
                previous_state.chunk_keys,
                to_generation,
                from_generation,
                to_generation,
                "converged",
            );
            defer alloc.free(manifest);
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, manifest_key),
                .value = try alloc.dupe(u8, manifest),
            });
            try artifact_writes.append(alloc, .{
                .key = try alloc.dupe(u8, state_key),
                .value = try alloc.dupe(u8, new_state),
            });
        }

        pub fn computeSparseMaterializedChunkRequest(
            alloc: Allocator,
            db: anytype,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            pending_deletes: *const std.StringHashMapUnmanaged(void),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
            sparse_embedder: embedder_mod.SparseEmbedder,
            embedding_name: []const u8,
            consumer_indexes: []const []const u8,
            shared_pending_writes: ?*const PendingArtifactWriteIndex,
        ) !void {
            const artifact_name = requestArtifactName(request);
            const prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, request.doc_key, "chunk", artifact_name);
            defer alloc.free(prefix);
            var local_pending_writes = if (shared_pending_writes == null)
                try PendingArtifactWriteIndex.init(alloc, artifact_writes.items)
            else
                PendingArtifactWriteIndex{};
            defer local_pending_writes.deinit(alloc);
            const pending_writes = shared_pending_writes orelse &local_pending_writes;

            const max_batch_items = generatedEmbedBatchItems();
            const max_batch_bytes = generatedEmbedBatchBytes();
            var sources = std.ArrayListUnmanaged(ChunkEmbeddingSource).empty;
            defer {
                clearChunkEmbeddingSourceList(alloc, &sources);
                sources.deinit(alloc);
            }
            var batch_source_bytes: usize = 0;
            var pending_chunk_keys = std.StringHashMapUnmanaged(void).empty;
            defer pending_chunk_keys.deinit(alloc);

            for (pending_writes.chunkWritesForDoc(request.doc_key)) |write| {
                if (!std.mem.startsWith(u8, write.key, prefix) or
                    !internal_keys.matchesChunkArtifactName(write.key, artifact_name)) continue;
                if (pending_chunk_keys.contains(write.key)) continue;
                try pending_chunk_keys.put(alloc, write.key, {});
                _ = try appendMaterializedChunkSourceToBatch(alloc, &sources, &batch_source_bytes, write.key, write.value, request.source_field);
                if (sources.items.len >= max_batch_items or batch_source_bytes >= max_batch_bytes) {
                    try flushGeneratedSparseChunkSourceBatch(alloc, db, runtime, sparse_embedder, embedding_name, request.producer_json, artifact_writes, sparse_embeddings, &sources, consumer_indexes, pending_writes);
                    batch_source_bytes = 0;
                }
            }
            try flushGeneratedSparseChunkSourceBatch(alloc, db, runtime, sparse_embedder, embedding_name, request.producer_json, artifact_writes, sparse_embeddings, &sources, consumer_indexes, pending_writes);
            batch_source_bytes = 0;

            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |key| alloc.free(key);
            const upper_bound = if (upper) |key| key else "";
            var lower = try alloc.dupe(u8, prefix);
            defer alloc.free(lower);
            while (true) {
                const next_lower = try scanMaterializedChunkSourceStoreBatch(alloc, db, prefix, upper_bound, lower, request.source_field, &pending_chunk_keys, pending_deletes, &sources, &batch_source_bytes, max_batch_items, max_batch_bytes);
                try flushGeneratedSparseChunkSourceBatch(alloc, db, runtime, sparse_embedder, embedding_name, request.producer_json, artifact_writes, sparse_embeddings, &sources, consumer_indexes, pending_writes);
                batch_source_bytes = 0;
                if (next_lower) |owned_next| {
                    alloc.free(lower);
                    lower = owned_next;
                    continue;
                }
                break;
            }
        }

        pub fn computeSparseRequestDerived(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            pending_deletes: *const std.StringHashMapUnmanaged(void),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
            memo: ?*GeneratedEmbeddingMemo,
            shared_pending_writes: ?*const PendingArtifactWriteIndex,
        ) !void {
            if (memo) |preservation| if (preservation.reuse_stored_artifacts) {
                if (try preparePreservedEmbeddingSources(alloc, db, doc_value, request, artifact_writes.items, pending_deletes, cache, shared_pending_writes)) |sources| {
                    defer freeChunkEmbeddingSources(alloc, sources);
                    for (sources) |source| {
                        const artifact_key = try embeddingArtifactKeyForBaseAlloc(alloc, source.key, requestEmbeddingName(request));
                        defer alloc.free(artifact_key);
                        try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, source.key, artifact_key, &.{}, &.{}, request.consumer_indexes);
                    }
                    return;
                }
            };
            const runtime = db.enrichment_runtime orelse return error.MissingSparseEmbedder;
            const sparse_embedder = runtime.config.sparse_embedder orelse return error.MissingSparseEmbedder;

            const embedding_name = requestEmbeddingName(request);
            const consumer_indexes: []const []const u8 = request.consumer_indexes;
            if (consumer_indexes.len == 0) return;

            if (requestUsesChunkSource(request)) {
                if (requestUsesPinnedMaterializedChunkArtifact(request)) {
                    try computeSparseMaterializedChunkRequest(alloc, db, runtime, request, artifact_writes, pending_deletes, sparse_embeddings, sparse_embedder, embedding_name, consumer_indexes, shared_pending_writes);
                    return;
                }
                var chunks_created: usize = 0;
                const sources = try chunkEmbeddingSourcesForRequest(alloc, db, doc_value, request, cache, &chunks_created);
                defer freeChunkEmbeddingSources(alloc, sources);
                if (sources.len == 0) return;
                enrichment_runtime_mod.noteIndexChunksCreated(runtime, consumer_indexes, chunks_created);
                var local_pending_writes = if (shared_pending_writes == null)
                    try PendingArtifactWriteIndex.init(alloc, artifact_writes.items)
                else
                    PendingArtifactWriteIndex{};
                defer local_pending_writes.deinit(alloc);
                const pending_writes = shared_pending_writes orelse &local_pending_writes;

                var chunk_texts = std.ArrayListUnmanaged([]const u8).empty;
                defer chunk_texts.deinit(alloc);
                var source_indexes = std.ArrayListUnmanaged(usize).empty;
                defer source_indexes.deinit(alloc);
                const max_batch_items = generatedEmbedBatchItems();
                const max_batch_bytes = generatedEmbedBatchBytes();
                var batch_source_bytes: usize = 0;
                for (sources, 0..) |source, i| {
                    const source_hash = enrichment_artifact_codec.hashEmbeddingSource(source.text, request.producer_json);
                    const artifact_key = try embeddingArtifactKeyForBaseAlloc(alloc, source.key, embedding_name);
                    defer alloc.free(artifact_key);
                    if (try storedOrPendingEmbeddingSourceHash(db, pending_writes, artifact_key)) |existing_hash| {
                        if (existing_hash == source_hash) {
                            try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, source.key, artifact_key, &.{}, &.{}, consumer_indexes);
                            continue;
                        }
                    }
                    if (chunk_texts.items.len > 0 and
                        (chunk_texts.items.len >= max_batch_items or batch_source_bytes + source.text.len > max_batch_bytes))
                    {
                        try flushGeneratedSparseChunkBatch(alloc, runtime, sparse_embedder, embedding_name, request.producer_json, artifact_writes, sparse_embeddings, sources, &source_indexes, &chunk_texts, consumer_indexes);
                        batch_source_bytes = 0;
                    }
                    try chunk_texts.append(alloc, source.text);
                    try source_indexes.append(alloc, i);
                    batch_source_bytes += source.text.len;
                    if (chunk_texts.items.len >= max_batch_items or batch_source_bytes >= max_batch_bytes) {
                        try flushGeneratedSparseChunkBatch(alloc, runtime, sparse_embedder, embedding_name, request.producer_json, artifact_writes, sparse_embeddings, sources, &source_indexes, &chunk_texts, consumer_indexes);
                        batch_source_bytes = 0;
                    }
                }
                try flushGeneratedSparseChunkBatch(alloc, runtime, sparse_embedder, embedding_name, request.producer_json, artifact_writes, sparse_embeddings, sources, &source_indexes, &chunk_texts, consumer_indexes);
                return;
            }

            const source_text = if (request.source_template.len > 0)
                renderSourceTemplateText(alloc, db, request.source_template, doc_value) catch |err| switch (err) {
                    error.OutOfMemory, error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                    else => null,
                }
            else
                try extractStringField(alloc, doc_value, request.source_field);
            if (source_text == null or source_text.?.len == 0) {
                if (source_text) |s| alloc.free(s);
                return;
            }
            defer alloc.free(source_text.?);

            const memo_key = GeneratedEmbeddingMemo.key(
                .sparse_embedding,
                embedding_name,
                request.producer_json,
                request.execution_json,
                0,
                source_text.?,
            );
            if (memo) |cache_memo| if (!cache_memo.sparse.contains(memo_key)) {
                _ = try prewarmGeneratedMemoFromArtifact(db, cache_memo, request, source_text.?, memo_key);
            };
            var uncached_sparse: ?embedder_mod.SparseEmbedding = null;
            defer if (uncached_sparse) |*owned| owned.deinit(alloc);
            const sparse = if (memo) |cache_memo|
                cache_memo.sparse.get(memo_key) orelse blk: {
                    var computed = try enrichment_runtime_mod.embedSparseTracked(runtime, consumer_indexes, cache_memo.alloc, sparse_embedder, embedding_name, source_text.?);
                    errdefer computed.deinit(cache_memo.alloc);
                    break :blk try cache_memo.adoptSparse(memo_key, computed);
                }
            else blk: {
                uncached_sparse = try enrichment_runtime_mod.embedSparseTracked(runtime, consumer_indexes, alloc, sparse_embedder, embedding_name, source_text.?);
                break :blk GeneratedEmbeddingMemo.SparseValue{
                    .indices = uncached_sparse.?.indices,
                    .values = uncached_sparse.?.values,
                };
            };
            const artifact_key = try appendSparseEmbeddingArtifactWrite(
                alloc,
                artifact_writes,
                request.doc_key,
                embedding_name,
                .{ .generated = enrichment_artifact_codec.hashEmbeddingSource(source_text.?, request.producer_json) },
                sparse.indices,
                sparse.values,
            );
            defer alloc.free(artifact_key);
            try appendDerivedSparseEmbeddingForConsumers(alloc, sparse_embeddings, request.doc_key, artifact_key, sparse.indices, sparse.values, consumer_indexes);
        }

        pub fn concatKVPairSlices(alloc: Allocator, lhs: []const docstore_mod.KVPair, rhs: []const docstore_mod.KVPair) ![]docstore_mod.KVPair {
            const out = try alloc.alloc(docstore_mod.KVPair, lhs.len + rhs.len);
            @memcpy(out[0..lhs.len], lhs);
            for (rhs, 0..) |write, i| out[lhs.len + i] = write;
            return out;
        }

        pub fn considerGraphEdgeWinner(
            alloc: Allocator,
            winners: *GraphEdgeWinners,
            edge_key: []const u8,
            state_key: []const u8,
            source_priority: usize,
            payload: []const u8,
        ) !void {
            if (winners.map.getPtr(edge_key)) |winner| {
                if (graph_mod.sourcePriorityRank(@intCast(source_priority)) > graph_mod.sourcePriorityRank(@intCast(winner.source_priority)) or
                    (source_priority == winner.source_priority and std.mem.order(u8, state_key, winner.owner_state_key) != .lt)) return;
                const owner = try alloc.dupe(u8, state_key);
                errdefer alloc.free(owner);
                const owned_payload = try alloc.dupe(u8, payload);
                alloc.free(winner.owner_state_key);
                alloc.free(winner.payload);
                winner.* = .{ .owner_state_key = owner, .payload = owned_payload, .source_priority = source_priority };
                return;
            }
            const owned_edge = try alloc.dupe(u8, edge_key);
            errdefer alloc.free(owned_edge);
            const owner = try alloc.dupe(u8, state_key);
            errdefer alloc.free(owner);
            const owned_payload = try alloc.dupe(u8, payload);
            errdefer alloc.free(owned_payload);
            try winners.map.put(alloc, owned_edge, .{
                .owner_state_key = owner,
                .payload = owned_payload,
                .source_priority = source_priority,
            });
        }

        pub fn containsDeleteKey(list: []const []const u8, key: []const u8) bool {
            for (list) |existing| {
                if (std.mem.eql(u8, existing, key)) return true;
            }
            return false;
        }

        pub fn containsName(names: []const []const u8, name: []const u8) bool {
            for (names) |existing| {
                if (std.mem.eql(u8, existing, name)) return true;
            }
            return false;
        }

        pub fn containsOwnedKey(list: []const []u8, key: []const u8) bool {
            for (list) |existing| {
                if (std.mem.eql(u8, existing, key)) return true;
            }
            return false;
        }

        pub fn containsStoreWriteKey(list: []const docstore_mod.KVPair, key: []const u8) bool {
            for (list) |item| {
                if (std.mem.eql(u8, item.key, key)) return true;
            }
            return false;
        }

        pub fn contentPartsJsonAlloc(alloc: Allocator, parts: []const template_mod.ContentPart) ![]u8 {
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);
            try out.append(alloc, '[');
            for (parts, 0..) |part, i| {
                if (i > 0) try out.append(alloc, ',');
                switch (part) {
                    .text => |text| {
                        try out.appendSlice(alloc, "{\"type\":\"text\",\"text\":");
                        try appendJsonString(alloc, &out, text);
                        try out.append(alloc, '}');
                    },
                    .media_url => |url| {
                        try out.appendSlice(alloc, "{\"type\":\"media\",\"url\":");
                        try appendJsonString(alloc, &out, url);
                        try out.append(alloc, '}');
                    },
                    .binary => |binary| {
                        try out.appendSlice(alloc, "{\"type\":\"media\",\"mime_type\":");
                        try appendJsonString(alloc, &out, binary.mime_type);
                        try out.appendSlice(alloc, ",\"data\":");
                        // Standard base64 has no JSON escape characters. Encode into
                        // the final owned buffer after reserving the complete suffix.
                        const encoded_len = try std.math.mul(usize, (try std.math.add(usize, binary.data.len, 2)) / 3, 4);
                        const suffix_len = try std.math.add(usize, encoded_len, 2);
                        try out.ensureUnusedCapacity(alloc, suffix_len);
                        out.appendAssumeCapacity('"');
                        const offset = out.items.len;
                        out.items.len += encoded_len;
                        _ = std.base64.standard.Encoder.encode(out.items[offset..], binary.data);
                        out.appendAssumeCapacity('"');
                        try out.append(alloc, '}');
                    },
                }
            }
            try out.append(alloc, ']');
            return try out.toOwnedSlice(alloc);
        }

        pub fn countKeysNotIn(keys: []const []const u8, exclude_keys: []const []const u8) usize {
            var count: usize = 0;
            for (keys) |key| {
                if (!containsDeleteKey(exclude_keys, key)) count += 1;
            }
            return count;
        }

        pub fn countUnitDescriptorsByFingerprintMatch(
            descriptors: []const DocumentExtractionUnitDescriptor,
            comparison: []const DocumentExtractionUnitDescriptor,
            want_match: bool,
        ) usize {
            var count: usize = 0;
            for (descriptors) |descriptor| {
                const matched = unitDescriptorFingerprintMatches(comparison, descriptor.key, descriptor.fingerprint);
                if (matched == want_match) count += 1;
            }
            return count;
        }

        pub fn decisionName(decision: resolver_lib.Decision) []const u8 {
            return switch (decision) {
                .new => "new",
                .match => "match",
                .review => "review",
            };
        }

        pub fn decodeArtifactRefIfKnownAlloc(alloc: Allocator, key: []const u8) !?types.ArtifactRef {
            return artifact_ids.decodeArtifactRefAlloc(alloc, key) catch |err| switch (err) {
                error.InvalidInternalUserKey => null,
                else => return err,
            };
        }

        pub fn decodeArtifactRefViewForGraphApplicability(key: []const u8) !?GraphArtifactRefView {
            if (!internal_keys.isInternalUserKey(key)) return null;

            const doc_term = internal_keys.findComponentTerminator(key, 1) orelse return null;
            var pos = doc_term + 2;
            if (pos >= key.len or key[pos] != internal_keys.artifact_kind) return null;
            pos += 1;

            const type_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
            const raw_kind = (try internal_keys.decodeBodyView(key[pos..type_term])) orelse return null;
            const kind = try artifactKindFromInternalLabel(raw_kind);
            pos = type_term + 2;

            const name_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
            const name = (try internal_keys.decodeBodyView(key[pos..name_term])) orelse return null;
            pos = name_term + 2;

            var unit_id_present = false;
            if (kind == .asset and pos < key.len and key[pos] == internal_keys.document_unit_record_kind) {
                pos += 1;
                const unit_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
                if ((try internal_keys.decodeBodyView(key[pos..unit_term])) == null) return null;
                pos = unit_term + 2;
                if (pos == key.len) return .{ .name = name, .kind = .asset, .unit_id_present = true };
            } else if (kind == .chunk) {
                if (pos < key.len and key[pos] == internal_keys.document_unit_record_kind) {
                    pos += 1;
                    const unit_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
                    if ((try internal_keys.decodeBodyView(key[pos..unit_term])) == null) return null;
                    pos = unit_term + 2;
                    unit_id_present = true;
                }
                if (pos + 1 + @sizeOf(u32) > key.len or key[pos] != internal_keys.chunk_record_kind) return error.InvalidInternalUserKey;
                pos += 1 + @sizeOf(u32);
                if (pos == key.len) return .{ .name = name, .kind = .chunk, .unit_id_present = unit_id_present };
            }

            if (pos == key.len) return .{ .name = name, .kind = kind, .unit_id_present = unit_id_present };
            if (key[pos] != internal_keys.derived_embedding_kind) return error.InvalidInternalUserKey;
            pos += 1;
            const derived_name_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
            const derived_name = (try internal_keys.decodeBodyView(key[pos..derived_name_term])) orelse return null;
            if (derived_name_term + 2 != key.len) return error.InvalidInternalUserKey;
            return .{ .name = derived_name, .kind = .embedding };
        }

        pub fn decodeArtifactRepairIssueValueAlloc(alloc: Allocator, raw: []const u8) !types.ArtifactRepairIssue {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidArtifactPayload;
            const obj = parsed.value.object;
            const Fields = struct {
                fn string(object: std.json.ObjectMap, name: []const u8) []const u8 {
                    const value = object.get(name) orelse return "";
                    if (value != .string) return "";
                    return value.string;
                }
                fn u64Value(object: std.json.ObjectMap, name: []const u8) u64 {
                    const value = object.get(name) orelse return 0;
                    if (value != .integer or value.integer < 0) return 0;
                    return std.math.cast(u64, value.integer) orelse 0;
                }
                fn optionalU32(object: std.json.ObjectMap, name: []const u8) ?u32 {
                    const value = object.get(name) orelse return null;
                    if (value == .null) return null;
                    if (value != .integer or value.integer < 0) return null;
                    return std.math.cast(u32, value.integer);
                }
                fn boolValue(object: std.json.ObjectMap, name: []const u8, default: bool) bool {
                    const value = object.get(name) orelse return default;
                    if (value != .bool) return default;
                    return value.bool;
                }
            };
            const kind = std.meta.stringToEnum(types.ArtifactRepairKind, Fields.string(obj, "artifact_kind")) orelse .embedding;
            const reason = std.meta.stringToEnum(types.ArtifactRepairReason, Fields.string(obj, "reason")) orelse .missing_artifact;
            const default_repairable = artifactRepairKindHasAutomatedReprocessor(kind);
            var issue = types.ArtifactRepairIssue{
                .artifact_kind = kind,
                .chunk_id = Fields.optionalU32(obj, "chunk_id"),
                .repairable = Fields.boolValue(obj, "repairable", default_repairable),
                .sequence = Fields.u64Value(obj, "sequence"),
                .reason = reason,
                .generation_attempts = Fields.u64Value(obj, "generation_attempts"),
                .attempts = Fields.u64Value(obj, "attempts"),
                .first_seen_ns = Fields.u64Value(obj, "first_seen_ns"),
                .last_seen_ns = Fields.u64Value(obj, "last_seen_ns"),
            };
            errdefer issue.deinit(alloc);
            issue.index_name = try alloc.dupe(u8, Fields.string(obj, "index_name"));
            issue.doc_key = try alloc.dupe(u8, Fields.string(obj, "doc_key"));
            issue.parent_doc_key = try alloc.dupe(u8, Fields.string(obj, "parent_doc_key"));
            issue.unit_id = try alloc.dupe(u8, Fields.string(obj, "unit_id"));
            issue.index_source_artifact_name = try alloc.dupe(u8, Fields.string(obj, "index_source_artifact_name"));
            issue.source_artifact_name = try alloc.dupe(u8, Fields.string(obj, "source_artifact_name"));
            issue.artifact_name = try alloc.dupe(u8, Fields.string(obj, "artifact_name"));
            issue.artifact_key = try alloc.dupe(u8, Fields.string(obj, "artifact_key"));
            issue.unsupported_reason = try alloc.dupe(u8, Fields.string(obj, "unsupported_reason"));
            issue.generation_error = try alloc.dupe(u8, Fields.string(obj, "generation_error"));
            issue.last_error = try alloc.dupe(u8, Fields.string(obj, "last_error"));
            try applyArtifactRepairability(alloc, &issue);
            return issue;
        }

        fn invalidateLocalArtifactProducerCoverage(ctx: *const AsyncContext, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !void {
            try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.deleted_keys);
            try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.overwritten_doc_keys);
            const pending = try pendingGeneratedCoverageDocKeysForIndexAlloc(ctx.alloc, ctx.index_manager, index_ref.name, index_ref.kind, batch.generated_enrichment_refs);
            defer ctx.alloc.free(pending);
            try deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, pending);
        }

        pub fn deleteDerivedCoverageForDocKeys(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            index_name: []const u8,
            doc_keys: []const []const u8,
        ) !void {
            if (doc_keys.len == 0) return;
            if (try orderedCoverageActive(store)) return;
            var local_coverage_guard = index_manager.lockLocalCoverage();
            defer local_coverage_guard.release();
            const generation = index_manager.coverageGenerationForIndex(index_name) orelse return;
            try deleteDerivedCoverageScopeForDocKeys(alloc, store, index_name, generation, doc_keys);
            var pinned = try index_manager.acquireWritePlanSnapshot();
            defer pinned.release();
            const sources = try index_manager.artifactSourceNamesForIndexAlloc(alloc, index_name);
            defer {
                for (sources) |source| alloc.free(source);
                alloc.free(sources);
            }
            for (sources) |source| {
                const scope = try @import("artifact_producer_readiness.zig").localScopeAlloc(alloc, index_name, source, pinned.plan());
                defer alloc.free(scope);
                try deleteDerivedCoverageScopeForDocKeys(alloc, store, scope, generation, doc_keys);
            }
        }

        fn derivedCoverageOutcomeCounterValueForTxn(
            alloc: Allocator,
            txn: *docstore_mod.DocStore.Txn,
            index_name: []const u8,
            generation: u64,
            outcome: []const u8,
        ) !u64 {
            const key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, outcome);
            defer alloc.free(key);
            const raw = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
            if (raw) |value| return try internal_keys.decodeDerivedCoverageOutcomeCount(value);
            // Only legacy tuples without maintained counters need a prefix scan.
            const prefix = try internal_keys.derivedCoverageOutcomeMarkerPrefixAlloc(alloc, index_name, generation);
            defer alloc.free(prefix);
            var cursor = try txn.openCursor();
            defer cursor.close();
            var row = try cursor.seekAtOrAfter(prefix);
            var count: u64 = 0;
            while (row) |entry| {
                if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                if (std.mem.eql(u8, entry.value, outcome)) count += 1;
                row = try cursor.next();
            }
            return count;
        }

        fn deleteDerivedCoverageScopeForDocKeys(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
            generation: u64,
            doc_keys: []const []const u8,
        ) !void {
            // Read receipts, revalidate their primary revisions, and retire their
            // counters in one writer transaction. A missing marker observed before
            // acquiring the writer must never erase a concurrent completion.
            var txn = try store.beginWriteTxn();
            var committed = false;
            defer if (!committed) txn.abort();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            const outcomes = std.meta.tags(DerivedCoverageOutcome);
            var counts: [outcomes.len]u64 = undefined;
            inline for (outcomes, 0..) |outcome, i| {
                counts[i] = try derivedCoverageOutcomeCounterValueForTxn(scratch, &txn, index_name, generation, @tagName(outcome));
            }
            var removed_counts = @as([outcomes.len]u64, @splat(0));
            const readiness = @import("artifact_producer_readiness.zig");
            const source_specific = !std.mem.eql(u8, index_name, readiness.localScopeIndex(index_name));
            for (doc_keys) |doc_key| {
                const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(scratch, index_name, generation, doc_key);
                if (seen.contains(marker_key)) continue;
                try seen.put(scratch, marker_key, {});
                const existing = txn.get(marker_key) catch |err| if (err == error.NotFound) null else return err;
                const value = existing orelse continue;
                const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, value) orelse return error.InvalidDerivedCoverageOutcome;
                if (source_specific) {
                    const revision_key = try readiness.localReceiptRevisionKeyAlloc(scratch, marker_key);
                    const revision = txn.get(revision_key) catch |err| if (err == error.NotFound) null else return err;
                    const ordinal = try @import("doc_identity.zig").lookupOrdinalTxn(scratch, &txn, doc_key);
                    const identity = if (ordinal) |id| try @import("doc_identity.zig").lookupStateTxn(&txn, id) else null;
                    if (revision) |bytes| {
                        if (bytes.len != 8) return error.InvalidDerivedCoverageOutcome;
                        if (identity) |state| {
                            // Untouched older documents have no primary stamp yet.
                            // New primary writes always install it atomically.
                            const primary = (try readiness.localPrimaryRevision(scratch, &txn, doc_key)) orelse state.created_generation;
                            if (state.isLive() and std.mem.readInt(u64, bytes[0..8], .little) >= primary) continue;
                        }
                    }
                    txn.delete(revision_key) catch |err| if (err != error.NotFound) return err;
                }
                try txn.delete(marker_key);
                removed_counts[@backingInt(outcome)] += 1;
            }
            var removed: u64 = 0;
            for (outcomes, removed_counts, 0..) |outcome, count, i| {
                if (count == 0) continue;
                removed += count;
                if (counts[i] < count) return error.InvalidDerivedCoverageCounter;
                const key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(scratch, index_name, generation, @tagName(outcome));
                var bytes: [8]u8 = undefined;
                try txn.put(key, internal_keys.encodeDerivedCoverageOutcomeCount(&bytes, counts[i] - count));
            }
            if (removed == 0) return;
            try txn.commit();
            committed = true;
        }

        pub fn denseApplyUsesLocalStreamingSession(ctx: *const AsyncContext, index_name: []const u8) bool {
            _ = index_name;
            if (ctx.dense_bulk_session_scope == .external) return false;
            if (ctx.dense_admission.external_sessions.load(.acquire) != 0) return false;
            if (ctx.dense_admission.sessions.active.load(.acquire) != 0) return false;
            return true;
        }

        pub fn denseCatchUpBulkRebuildHbcLeafMinMembers() ?usize {
            return cachedOptionalEnvUsize(
                &D.dense_catch_up_bulk_rebuild_hbc_leaf_min_members_cache.*,
                "ANTFLY_DENSE_CATCH_UP_BULK_REBUILD_HBC_LEAF_MIN_MEMBERS",
            );
        }

        pub fn denseCatchUpDeferredHbcLeafSplitMembersPerPublish() usize {
            return cachedEnvUsize(
                &D.dense_catch_up_deferred_hbc_leaf_split_members_cache.*,
                "ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_HBC_LEAF_SPLIT_MEMBERS_PER_PUBLISH",
                dense_catch_up_default_deferred_hbc_leaf_split_members_per_publish,
            );
        }

        pub fn denseCatchUpDeferredHbcLeafSplitsPerPublish() usize {
            return cachedEnvUsize(
                &D.dense_catch_up_deferred_hbc_leaf_splits_cache.*,
                "ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_HBC_LEAF_SPLITS_PER_PUBLISH",
                dense_catch_up_default_deferred_hbc_leaf_splits_per_publish,
            );
        }

        pub fn denseCatchUpDeferredL0Limit() usize {
            return cachedEnvUsize(
                &D.dense_catch_up_deferred_l0_limit_cache.*,
                "ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_L0_RUNS",
                dense_catch_up_default_deferred_l0_limit,
            );
        }

        pub fn denseCatchUpFinishOptions() backend_types.BulkIngestFinishOptions {
            return .{
                .compact = false,
                .flush = true,
                .max_deferred_l0_runs = denseCatchUpDeferredL0Limit(),
                .max_deferred_hbc_leaf_splits_per_publish = denseCatchUpDeferredHbcLeafSplitsPerPublish(),
                .max_deferred_hbc_leaf_split_members_per_publish = denseCatchUpDeferredHbcLeafSplitMembersPerPublish(),
                .bulk_rebuild_hbc_leaf_min_members = denseCatchUpBulkRebuildHbcLeafMinMembers(),
            };
        }

        pub fn denseEmbeddingWriteSourceDocumentExists(
            ctx: *const AsyncContext,
            write: mapper.DenseEmbeddingWrite,
        ) !bool {
            return try replaySourceDocumentExists(ctx, write.parent_doc_key orelse write.doc_key);
        }

        pub fn denseIndexIsArtifactBacked(entry: anytype) bool {
            return entry.external or entry.chunk_name != null or entry.embedding_name != null or entry.embedding_names.len > 0;
        }

        pub fn derivedCoverageOutcomeCounterValueForStore(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
            generation: u64,
            outcome: []const u8,
        ) !u64 {
            return (try loadDerivedCoverageOutcomeCounterFromStore(alloc, store, index_name, generation, outcome)) orelse
                try scanDerivedCoverageOutcomeFromStore(alloc, store, index_name, generation, outcome);
        }

        pub fn directGraphNeighborContextHintsAlloc(
            alloc: Allocator,
            req: types.BatchRequest,
            writes: []const types.GraphEdgeWrite,
            artifact_keys: []const []const u8,
            manager: ?*index_manager_mod.IndexManager,
            owning_table: ?[]const u8,
        ) !NeighborContextReplayHints {
            const active = manager orelse return .{};
            if (!active.hasAssetNeighborContext()) return .{};
            if (writes.len == 0 and req.graph_deletes.len == 0) {
                var has_graph_artifacts = false;
                for (artifact_keys) |key| if (internal_keys.isGraphEdgeArtifactKey(key)) {
                    has_graph_artifacts = true;
                    break;
                };
                if (!has_graph_artifacts) return .{};
            }
            // Artifact afterimages include removals omitted from the mapped write list.
            // Direct batches restrict artifact removals to their affected owners.
            // Generated effects admit arbitrary endpoints; catalog dependency admission
            // prevents feedback through the sampled graph.
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch_alloc = arena.allocator();
            var direct_owners = std.StringHashMapUnmanaged(void).empty;
            for (req.writes) |write| try direct_owners.put(scratch_alloc, write.key, {});
            for (req.deletes) |key| try direct_owners.put(scratch_alloc, key, {});
            var removed = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
            for (artifact_keys) |key| {
                const identity = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(scratch_alloc, key)) orelse continue;
                if (direct_owners.count() != 0 and !direct_owners.contains(identity.doc_key)) continue;
                try removed.append(scratch_alloc, .{
                    .index_name = identity.index_name,
                    .source = if (identity.logical_source.len != 0) identity.logical_source else identity.doc_key,
                    .target = identity.target_doc_key,
                    .edge_type = identity.edge_type,
                    .edge_id = identity.edge_id,
                    .owner_document = identity.doc_key,
                });
            }
            var keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (keys.items) |key| alloc.free(key);
                keys.deinit(alloc);
            }
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            var stored_table: ?[]u8 = null;
            defer if (stored_table) |table| alloc.free(table);
            var table = owning_table;
            var table_loaded = table != null;
            var directions = std.StringHashMapUnmanaged(index_manager_mod.IndexManager.NeighborContextDirections).empty;
            defer directions.deinit(alloc);
            inline for (.{ writes, req.graph_deletes, removed.items }) |mutations| {
                for (mutations) |mutation| {
                    const slot = try directions.getOrPut(alloc, mutation.index_name);
                    if (!slot.found_existing) slot.value_ptr.* = try active.assetNeighborContextDirectionsForGraphIndex(alloc, mutation.index_name);
                    const oriented = slot.value_ptr.*;
                    if (!oriented.any()) continue;
                    if (!table_loaded) {
                        if (active.primary_store) |store| {
                            stored_table = store.get(alloc, internal_keys.graph_owning_table_key) catch |err| switch (err) {
                                error.NotFound => null,
                                else => return err,
                            };
                            table = stored_table;
                        }
                        table_loaded = true;
                    }
                    var routing = graph_metadata_tables.Scratch.init(alloc, null);
                    defer routing.deinit();
                    var persisted: ?[]u8 = null;
                    defer if (persisted) |raw| alloc.free(raw);
                    // Read only this relationship's pre-commit artifact. A routing
                    // update must wake local endpoints that lose adjacency as well as
                    // those that gain it. Deletes carry no metadata on the wire.
                    const previous_metadata: ?[]const u8 = previous: {
                        const store = active.primary_store orelse break :previous null;
                        const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, mutation.producingDocument(), mutation.index_name, mutation.edge_type, mutation.target, mutation.source, mutation.edge_id);
                        defer alloc.free(artifact);
                        persisted = store.get(alloc, artifact) catch |err| switch (err) {
                            error.NotFound => null,
                            else => return err,
                        };
                        const raw = persisted orelse break :previous null;
                        break :previous (try enrichment_artifact_codec.decodeGraphEdgeBorrowed(raw)).metadata_json;
                    };
                    const metadata = if (@hasField(@TypeOf(mutation), "metadata_json")) mutation.metadata_json else previous_metadata orelse "";
                    if (oriented.out and (graph_metadata_tables.inlineEndpointsAreLocal(try routing.table(metadata, "source_table"), null, table) or
                        (if (previous_metadata) |old| graph_metadata_tables.inlineEndpointsAreLocal(try routing.table(old, "source_table"), null, table) else false)))
                        try appendUniqueReplayRecordKeyWithSet(alloc, &keys, &seen, mutation.source);
                    if (oriented.in and (graph_metadata_tables.inlineEndpointsAreLocal(null, try routing.table(metadata, "target_table"), table) or
                        (if (previous_metadata) |old| graph_metadata_tables.inlineEndpointsAreLocal(null, try routing.table(old, "target_table"), table) else false)))
                        try appendUniqueReplayRecordKeyWithSet(alloc, &keys, &seen, mutation.target);
                }
            }
            return .{ .keys = try keys.toOwnedSlice(alloc) };
        }

        pub fn documentExtractionFailureManifestPayloadAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
            source_url: []const u8,
            unit_keys: []const []const u8,
            chunk_keys: []const []const u8,
            previous_child_ranges: []const types.DocumentArtifactChildRange,
            from_generation: u64,
            to_generation: u64,
            error_code: []const u8,
            error_message: []const u8,
            error_stage: []const u8,
        ) ![]u8 {
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);
            var first = true;
            try out.append(alloc, '{');
            try appendJsonFieldString(alloc, &out, &first, "_parent_doc_key", doc_key);
            try appendJsonFieldString(alloc, &out, &first, "_artifact_name", artifact_name);
            try appendJsonFieldString(alloc, &out, &first, "artifact_type", "document_units");
            try appendJsonFieldU64(alloc, &out, &first, "manifest_version", 2);
            try appendJsonFieldU64(alloc, &out, &first, "generation", to_generation);
            try appendJsonFieldString(alloc, &out, &first, "source_url", source_url);
            try appendJsonFieldString(alloc, &out, &first, "source_fingerprint", "");
            try appendJsonFieldString(alloc, &out, &first, "content_type", "");
            try appendJsonFieldString(alloc, &out, &first, "route_type", "error");
            try appendJsonFieldUsize(alloc, &out, &first, "unit_count", unit_keys.len);
            try appendJsonFieldUsize(alloc, &out, &first, "chunk_count", chunk_keys.len);
            try appendJsonFieldName(alloc, &out, &first, "child_ranges");
            try out.append(alloc, '[');
            try appendDocumentExtractionRangeDescriptors(alloc, &out, artifact_name, unit_keys, chunk_keys, &.{}, previous_child_ranges);
            try out.append(alloc, ']');
            try appendJsonFieldName(alloc, &out, &first, "merge_plan");
            try out.append(alloc, '{');
            var merge_first = true;
            try appendJsonFieldU64(alloc, &out, &merge_first, "plan_version", 1);
            try appendJsonFieldU64(alloc, &out, &merge_first, "from_generation", from_generation);
            try appendJsonFieldU64(alloc, &out, &merge_first, "to_generation", to_generation);
            try appendJsonFieldString(alloc, &out, &merge_first, "status", "failed");
            try appendJsonFieldString(alloc, &out, &merge_first, "operation_granularity", "unit_fingerprint");
            try appendJsonFieldName(alloc, &out, &merge_first, "operations");
            try out.appendSlice(alloc, "[]");
            try out.append(alloc, '}');
            try appendJsonFieldName(alloc, &out, &first, "last_error");
            try out.append(alloc, '{');
            var error_first = true;
            try appendJsonFieldString(alloc, &out, &error_first, "code", error_code);
            try appendJsonFieldString(alloc, &out, &error_first, "message", error_message);
            try appendJsonFieldString(alloc, &out, &error_first, "stage", error_stage);
            try out.append(alloc, '}');
            try out.append(alloc, '}');
            return try out.toOwnedSlice(alloc);
        }

        pub fn documentExtractionFingerprintAlloc(
            alloc: Allocator,
            source_url: []const u8,
            config_json: []const u8,
            configured_content_type: []const u8,
            configured_filename: []const u8,
            downloaded_content_type: []const u8,
            data: []const u8,
        ) ![]u8 {
            var hasher = std.crypto.hash.sha2.Sha256.init(.{});
            hasher.update(source_url);
            hasher.update(config_json);
            hasher.update(configured_content_type);
            hasher.update(configured_filename);
            hasher.update(downloaded_content_type);
            hasher.update(data);
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            hasher.final(&digest);
            return try hexBytesAlloc(alloc, &digest);
        }

        pub fn documentExtractionKeyIndex(keys: []const []const u8, key: []const u8) ?usize {
            for (keys, 0..) |candidate, i| {
                if (std.mem.eql(u8, candidate, key)) return i;
            }
            return null;
        }

        pub fn documentExtractionManifestGeneration(alloc: Allocator, manifest: []const u8) !u64 {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, manifest, .{}) catch return 0;
            defer parsed.deinit();
            if (parsed.value != .object) return 0;
            const generation = parsed.value.object.get("generation") orelse return 0;
            if (generation != .integer or generation.integer < 0) return error.InvalidDocumentExtractionManifest;
            return std.math.cast(u64, generation.integer) orelse return error.InvalidDocumentExtractionManifest;
        }

        pub fn documentExtractionManifestHasLastError(alloc: Allocator, manifest_json: []const u8) !bool {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, manifest_json, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return false;
            return parsed.value.object.get("last_error") != null;
        }

        pub fn documentExtractionManifestPayloadAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
            source_url: []const u8,
            fingerprint: []const u8,
            extraction: document_extraction_mod.Result,
            unit_keys: []const []const u8,
            unit_descriptors: []const DocumentExtractionUnitDescriptor,
            chunk_keys: []const []const u8,
            previous_child_ranges: []const types.DocumentArtifactChildRange,
            previous_unit_keys: []const []const u8,
            previous_unit_descriptors: []const DocumentExtractionUnitDescriptor,
            previous_chunk_keys: []const []const u8,
            manifest_generation: u64,
            from_generation: u64,
            to_generation: u64,
            merge_status: []const u8,
        ) ![]u8 {
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);
            var first = true;
            try out.append(alloc, '{');
            try appendJsonFieldString(alloc, &out, &first, "_parent_doc_key", doc_key);
            try appendJsonFieldString(alloc, &out, &first, "_artifact_name", artifact_name);
            try appendJsonFieldString(alloc, &out, &first, "artifact_type", "document_units");
            try appendJsonFieldU64(alloc, &out, &first, "manifest_version", 2);
            try appendJsonFieldU64(alloc, &out, &first, "generation", manifest_generation);
            try appendJsonFieldString(alloc, &out, &first, "source_url", source_url);
            try appendJsonFieldString(alloc, &out, &first, "source_fingerprint", fingerprint);
            try appendJsonFieldString(alloc, &out, &first, "content_type", extraction.content_type);
            try appendJsonFieldString(alloc, &out, &first, "route_type", extraction.route_type);
            if (extraction.unsupported_reason.len > 0) {
                try appendJsonFieldString(alloc, &out, &first, "unsupported_reason", extraction.unsupported_reason);
            }
            try appendJsonFieldUsize(alloc, &out, &first, "unit_count", extraction.units.len);
            try appendJsonFieldUsize(alloc, &out, &first, "chunk_count", chunk_keys.len);
            var ocr_attempted_count: usize = 0;
            var ocr_selected_count: usize = 0;
            var ocr_retained_embedded_count: usize = 0;
            var ocr_failed_count: usize = 0;
            var failed_pages: [32]u32 = undefined;
            var failed_pages_len: usize = 0;
            for (extraction.units) |unit| {
                if (!unit.ocr_attempted) continue;
                ocr_attempted_count += 1;
                if (unit.ocr_used) ocr_selected_count += 1;
                if (unit.extraction_status) |status| {
                    if (std.mem.eql(u8, status, "completed_embedded_preferred")) ocr_retained_embedded_count += 1;
                    if (std.mem.eql(u8, status, "failed_ocr")) {
                        ocr_failed_count += 1;
                        if (failed_pages_len < failed_pages.len) if (unit.page_number) |page| {
                            failed_pages[failed_pages_len] = page;
                            failed_pages_len += 1;
                        };
                    }
                }
            }
            try appendJsonFieldUsize(alloc, &out, &first, "ocr_attempted_count", ocr_attempted_count);
            try appendJsonFieldUsize(alloc, &out, &first, "ocr_selected_count", ocr_selected_count);
            try appendJsonFieldUsize(alloc, &out, &first, "ocr_retained_embedded_count", ocr_retained_embedded_count);
            try appendJsonFieldUsize(alloc, &out, &first, "ocr_failed_count", ocr_failed_count);
            try appendJsonFieldName(alloc, &out, &first, "ocr_failed_page_numbers");
            try out.append(alloc, '[');
            for (failed_pages[0..failed_pages_len], 0..) |page, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonUnsigned(alloc, &out, page);
            }
            try out.append(alloc, ']');
            try appendJsonFieldBool(alloc, &out, &first, "ocr_failed_pages_truncated", ocr_failed_count > failed_pages_len);
            try appendJsonFieldName(alloc, &out, &first, "ocr_failure_details");
            try out.append(alloc, '[');
            var failure_detail_count: usize = 0;
            for (extraction.units) |unit| {
                const status = unit.extraction_status orelse continue;
                if (!std.mem.eql(u8, status, "failed_ocr")) continue;
                if (failure_detail_count >= 32) break;
                if (failure_detail_count > 0) try out.append(alloc, ',');
                failure_detail_count += 1;
                try out.append(alloc, '{');
                var detail_first = true;
                if (unit.page_number) |page| try appendJsonFieldU64(alloc, &out, &detail_first, "page_number", page);
                try appendJsonFieldString(alloc, &out, &detail_first, "unit_id", unit.unit_id);
                try appendJsonFieldString(alloc, &out, &detail_first, "retained_method", unit.method);
                try appendJsonFieldString(alloc, &out, &detail_first, "error_message", unit.extraction_warning orelse "OCR failed without a recorded cause");
                if (unit.ocr_failure_stage) |stage| try appendJsonFieldString(alloc, &out, &detail_first, "failure_stage", stage);
                try appendJsonFieldBool(alloc, &out, &detail_first, "retryable", unit.ocr_failure_retryable orelse false);
                try out.append(alloc, '}');
            }
            try out.append(alloc, ']');
            try appendJsonFieldName(alloc, &out, &first, "child_ranges");
            try out.append(alloc, '[');
            try appendDocumentExtractionRangeDescriptors(alloc, &out, artifact_name, unit_keys, chunk_keys, extraction.units, previous_child_ranges);
            try out.append(alloc, ']');
            try appendJsonFieldName(alloc, &out, &first, "range_policy");
            try appendDocumentExtractionRangePolicy(alloc, &out);
            try appendJsonFieldName(alloc, &out, &first, "merge_plan");
            try out.append(alloc, '{');
            var merge_first = true;
            try appendJsonFieldU64(alloc, &out, &merge_first, "plan_version", 1);
            try appendJsonFieldU64(alloc, &out, &merge_first, "from_generation", from_generation);
            try appendJsonFieldU64(alloc, &out, &merge_first, "to_generation", to_generation);
            try appendJsonFieldString(alloc, &out, &merge_first, "status", merge_status);
            try appendJsonFieldString(alloc, &out, &merge_first, "operation_granularity", "unit_fingerprint");
            try appendJsonFieldName(alloc, &out, &merge_first, "operations");
            try out.append(alloc, '[');
            var first_operation = true;
            try appendDocumentExtractionUnitMergeOperation(alloc, &out, &first_operation, "keep", artifact_name, unit_descriptors, previous_unit_descriptors, true);
            try appendDocumentExtractionUnitMergeOperation(alloc, &out, &first_operation, "upsert", artifact_name, unit_descriptors, previous_unit_descriptors, false);
            try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "upsert", "chunk", "derived_chunks", chunk_keys, &.{});
            try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "delete", "unit", artifact_name, previous_unit_keys, unit_keys);
            try appendDocumentExtractionMergeOperation(alloc, &out, &first_operation, "delete", "chunk", "derived_chunks", previous_chunk_keys, chunk_keys);
            try out.append(alloc, ']');
            try out.append(alloc, '}');
            try appendJsonFieldName(alloc, &out, &first, "coverage_plan");
            try out.append(alloc, '{');
            var coverage_first = true;
            try appendJsonFieldU64(alloc, &out, &coverage_first, "plan_version", 1);
            try appendJsonFieldString(alloc, &out, &coverage_first, "full_text_replay", "stored_artifact_required");
            try appendJsonFieldBool(alloc, &out, &coverage_first, "full_text_replay_suppressed", false);
            try appendJsonFieldBool(alloc, &out, &coverage_first, "watermark_required_before_suppression", true);
            try out.append(alloc, '}');
            try out.append(alloc, '}');
            return try out.toOwnedSlice(alloc);
        }

        pub fn documentExtractionRangeEnd(
            key_count: usize,
            units: []const document_extraction_mod.Unit,
            start: usize,
        ) usize {
            var end = start;
            var text_bytes: usize = 0;
            const use_text_limit = units.len == key_count;
            while (end < key_count and end - start < document_extraction_range_target_children) {
                if (use_text_limit) {
                    const unit_bytes = units[end].text.len;
                    if (end > start and text_bytes + unit_bytes > document_extraction_range_target_text_bytes) break;
                    text_bytes += unit_bytes;
                }
                end += 1;
            }
            return end;
        }

        pub fn documentExtractionRangeIdAlloc(alloc: Allocator, range_index: usize) ![]u8 {
            return try std.fmt.allocPrint(alloc, "range:{d:0>6}", .{range_index});
        }

        pub fn documentExtractionRangeRoute(
            ranges: []const types.DocumentArtifactChildRange,
            range_id: []const u8,
            range_kind: []const u8,
            artifact_name: []const u8,
        ) DocumentExtractionRangeRoute {
            const range = findDocumentArtifactChildRange(ranges, range_id, range_kind, artifact_name) orelse return .{ .range_id = range_id };
            return .{
                .range_id = range_id,
                .route_status = range.route_status orelse "local_committed",
                .owner_group_id = range.owner_group_id orelse 0,
            };
        }

        pub fn documentExtractionSplitBoundary(range_kind: []const u8) []const u8 {
            if (std.mem.eql(u8, range_kind, "chunk")) return "chunk";
            return "unit";
        }

        pub fn documentExtractionStateByteSliceAlloc(alloc: Allocator, value: std.json.Value) ![]const u8 {
            switch (value) {
                .string => |string| return try alloc.dupe(u8, string),
                .array => |array| {
                    const out = try alloc.alloc(u8, array.items.len);
                    errdefer alloc.free(out);
                    for (array.items, 0..) |item, i| {
                        if (item != .integer) return error.InvalidDocumentExtractionState;
                        out[i] = std.math.cast(u8, item.integer) orelse return error.InvalidDocumentExtractionState;
                    }
                    return out;
                },
                else => return error.InvalidDocumentExtractionState,
            }
        }

        pub fn documentExtractionStateChunkKeysAlloc(alloc: Allocator, state: []const u8) ![]const []const u8 {
            return try documentExtractionStateKeysAlloc(alloc, state, "chunk_keys");
        }

        pub fn documentExtractionStateFingerprintMatches(alloc: Allocator, state: []const u8, fingerprint: []const u8) bool {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, state, .{}) catch return false;
            defer parsed.deinit();
            if (parsed.value != .object) return false;
            const value = parsed.value.object.get("fingerprint") orelse return false;
            return value == .string and std.mem.eql(u8, value.string, fingerprint);
        }

        pub fn documentExtractionStateHasChunkUnitFingerprints(alloc: Allocator, state: []const u8) bool {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, state, .{}) catch return false;
            defer parsed.deinit();
            if (parsed.value != .object) return false;
            const version = parsed.value.object.get("chunk_unit_fingerprint_version") orelse return false;
            return document_unit_fingerprint.stateVersionIsCurrent(version);
        }

        pub fn documentExtractionStateKeysAlloc(alloc: Allocator, state: []const u8, field_name: []const u8) ![]const []const u8 {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return try alloc.alloc([]const u8, 0);
            const keys_value = parsed.value.object.get(field_name) orelse return try alloc.alloc([]const u8, 0);
            if (keys_value != .array) return try alloc.alloc([]const u8, 0);
            const out = try alloc.alloc([]const u8, keys_value.array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |key| alloc.free(@constCast(key));
                alloc.free(out);
            }
            for (keys_value.array.items, 0..) |item, i| {
                out[i] = try documentExtractionStateByteSliceAlloc(alloc, item);
                initialized += 1;
            }
            return out;
        }

        pub fn documentExtractionStateNavigationBlockCount(alloc: Allocator, state: []const u8) !u32 {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return 0;
            const value = parsed.value.object.get("navigation_block_count") orelse return 0;
            if (value != .integer) return error.InvalidDocumentExtractionState;
            return std.math.cast(u32, value.integer) orelse error.InvalidDocumentExtractionState;
        }

        pub fn documentExtractionStateUnitDescriptorFallbackAlloc(alloc: Allocator, object: std.json.ObjectMap) ![]DocumentExtractionUnitDescriptor {
            const keys_value = object.get("unit_keys") orelse return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
            if (keys_value != .array) return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
            const out = try alloc.alloc(DocumentExtractionUnitDescriptor, keys_value.array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |descriptor| {
                    alloc.free(@constCast(descriptor.key));
                    if (descriptor.fingerprint.len > 0) alloc.free(@constCast(descriptor.fingerprint));
                }
                alloc.free(out);
            }
            for (keys_value.array.items, 0..) |item, i| {
                out[i] = .{
                    .key = try documentExtractionStateByteSliceAlloc(alloc, item),
                    .fingerprint = "",
                };
                initialized += 1;
            }
            return out;
        }

        pub fn documentExtractionStateUnitDescriptorsAlloc(alloc: Allocator, state: []const u8) ![]DocumentExtractionUnitDescriptor {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, state, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return try alloc.alloc(DocumentExtractionUnitDescriptor, 0);
            const descriptors_value = parsed.value.object.get("unit_descriptors") orelse return documentExtractionStateUnitDescriptorFallbackAlloc(alloc, parsed.value.object);
            if (descriptors_value != .array) return error.InvalidDocumentExtractionState;
            const out = try alloc.alloc(DocumentExtractionUnitDescriptor, descriptors_value.array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |descriptor| {
                    alloc.free(@constCast(descriptor.key));
                    alloc.free(@constCast(descriptor.fingerprint));
                }
                alloc.free(out);
            }
            for (descriptors_value.array.items, 0..) |item, i| {
                if (item != .object) return error.InvalidDocumentExtractionState;
                const key_value = item.object.get("key") orelse return error.InvalidDocumentExtractionState;
                const fingerprint_value = item.object.get("fingerprint") orelse return error.InvalidDocumentExtractionState;
                if (fingerprint_value != .string) return error.InvalidDocumentExtractionState;
                const key = try documentExtractionStateByteSliceAlloc(alloc, key_value);
                errdefer alloc.free(@constCast(key));
                const fingerprint = try alloc.dupe(u8, fingerprint_value.string);
                errdefer alloc.free(fingerprint);
                out[i] = .{
                    .key = key,
                    .fingerprint = fingerprint,
                };
                initialized += 1;
            }
            return out;
        }

        pub fn documentExtractionStateUnitKeysAlloc(alloc: Allocator, state: []const u8) ![]const []const u8 {
            return try documentExtractionStateKeysAlloc(alloc, state, "unit_keys");
        }

        pub fn documentExtractionStateValueAlloc(
            alloc: Allocator,
            fingerprint: []const u8,
            unit_keys: []const []const u8,
            unit_descriptors: []const DocumentExtractionUnitDescriptor,
            chunk_keys: []const []const u8,
            navigation_digest: []const u8,
            navigation_block_count: u32,
            chunk_unit_fingerprints: bool,
        ) ![]u8 {
            return try std.json.Stringify.valueAlloc(alloc, .{
                .kind = "document_extraction_state_v1",
                .fingerprint = fingerprint,
                .unit_keys = unit_keys,
                .unit_descriptors = unit_descriptors,
                .chunk_keys = chunk_keys,
                .navigation_digest = navigation_digest,
                .navigation_block_count = navigation_block_count,
                .navigation_block_size = hierarchy_navigation_block_size,
                .chunk_unit_fingerprint_version = if (chunk_unit_fingerprints) document_unit_fingerprint.current_state_version else @as(u8, 0),
            }, .{});
        }

        pub fn documentExtractionStoredUnitFingerprintAlloc(alloc: Allocator, stored: []const u8) ![]u8 {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            var parsed = try std.json.parseFromSlice(std.json.Value, scratch, stored, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidDocumentExtractionState;
            const object = parsed.value.object;
            const provenance_value = object.get("provenance") orelse return error.InvalidDocumentExtractionState;
            if (provenance_value != .object) return error.InvalidDocumentExtractionState;
            const provenance = provenance_value.object;

            const text_regions = try storedUnitTextRegionsAlloc(scratch, provenance.get("text_regions"));
            const unit = document_extraction_mod.Unit{
                .unit_id = @constCast(try storedUnitRequiredString(object, "unit_id")),
                .unit_type = @constCast(try storedUnitRequiredString(object, "unit_type")),
                .text = @constCast(try storedUnitRequiredString(object, "text")),
                .method = @constCast(try storedUnitRequiredString(provenance, "method")),
                .source_path = if (try storedUnitOptionalString(object, "source_path")) |value| @constCast(value) else null,
                .extraction_status = if (try storedUnitOptionalString(object, "extraction_status")) |value| @constCast(value) else null,
                .source_sha256 = if (try storedUnitOptionalString(object, "source_sha256")) |value| @constCast(value) else null,
                .byte_length = try storedUnitOptionalInteger(u64, object, "byte_length"),
                .ocr_used = try storedUnitRequiredBool(provenance, "ocr_used"),
                .ocr_attempted = try storedUnitRequiredBool(object, "ocr_attempted"),
                .ocr_render_dpi = try storedUnitOptionalInteger(u16, object, "ocr_render_dpi"),
                .ocr_effective_render_dpi = try storedUnitOptionalInteger(u16, object, "ocr_effective_render_dpi"),
                .ocr_rendered_width = try storedUnitOptionalInteger(u32, object, "ocr_rendered_width"),
                .ocr_rendered_height = try storedUnitOptionalInteger(u32, object, "ocr_rendered_height"),
                .ocr_rendered_bytes = try storedUnitOptionalInteger(u64, object, "ocr_rendered_bytes"),
                .ocr_failure_stage = if (try storedUnitOptionalString(object, "ocr_failure_stage")) |value| @constCast(value) else null,
                .ocr_failure_retryable = try storedUnitOptionalBool(object, "ocr_failure_retryable"),
                .ocr_trigger_reasons = if (try storedUnitOptionalString(object, "ocr_trigger_reasons")) |value| @constCast(value) else null,
                .ocr_embedded_quality = if (try storedUnitOptionalString(object, "ocr_embedded_quality")) |value| @constCast(value) else null,
                .ocr_output_quality = if (try storedUnitOptionalString(object, "ocr_output_quality")) |value| @constCast(value) else null,
                .ocr_confidence = try storedUnitOptionalFloat(object, "ocr_confidence"),
                .ocr_bbox = try storedUnitOptionalBbox(object, "ocr_bbox"),
                .transcript_used = try storedUnitRequiredBool(provenance, "transcript_used"),
                .transcript_confidence = try storedUnitOptionalFloat(object, "transcript_confidence"),
                .extraction_warning = if (try storedUnitOptionalString(object, "extraction_warning")) |value| @constCast(value) else null,
                .page_number = try storedUnitOptionalInteger(u32, provenance, "page_number"),
                .page_label = if (try storedUnitOptionalString(provenance, "page_label")) |value| @constCast(value) else null,
                .page_bbox = try storedUnitOptionalBbox(provenance, "page_bbox"),
                .page_rotation = try storedUnitOptionalInteger(i32, provenance, "page_rotation"),
                .text_regions = text_regions,
                .char_start = try storedUnitOptionalInteger(u32, provenance, "char_start"),
                .char_end = try storedUnitOptionalInteger(u32, provenance, "char_end"),
            };
            // A payload without the marker predates the canonical fingerprint
            // encoding. Its state descriptor therefore contains the legacy digest;
            // reconstruct that exact value for rolling-upgrade validation. Current
            // payloads always validate through their persisted `duf2:` marker.
            return try document_unit_fingerprint.legacyFingerprintAlloc(alloc, unit);
        }

        pub fn documentExtractionUnitDescriptorsFromKeysAlloc(
            alloc: Allocator,
            unit_keys: []const []const u8,
            fingerprints: []const []const u8,
        ) ![]DocumentExtractionUnitDescriptor {
            if (unit_keys.len != fingerprints.len) return error.InvalidDocumentExtractionState;
            const out = try alloc.alloc(DocumentExtractionUnitDescriptor, unit_keys.len);
            for (unit_keys, fingerprints, 0..) |key, fingerprint, i| {
                out[i] = .{
                    .key = key,
                    .fingerprint = fingerprint,
                };
            }
            return out;
        }

        pub fn documentExtractionUnitFingerprintAlloc(alloc: Allocator, unit: document_extraction_mod.Unit) ![]u8 {
            return try document_unit_fingerprint.fingerprintAlloc(alloc, unit);
        }

        pub fn documentExtractionUnitRangeCount(units: []const document_extraction_mod.Unit) usize {
            var count: usize = 0;
            var start: usize = 0;
            while (start < units.len) {
                count += 1;
                start = documentExtractionRangeEnd(units.len, units, start);
            }
            return count;
        }

        pub fn documentExtractionUnitRangeIndex(units: []const document_extraction_mod.Unit, unit_index: usize) usize {
            var range_index: usize = 0;
            var start: usize = 0;
            while (start < units.len) : (range_index += 1) {
                const end = documentExtractionRangeEnd(units.len, units, start);
                if (unit_index < end) return range_index;
                start = end;
            }
            return range_index;
        }

        pub fn documentRangeLowerAlloc(alloc: Allocator, raw_key: []const u8) ![]u8 {
            return try internal_keys.documentRangeLowerAlloc(alloc, raw_key);
        }

        pub fn documentUnitCanSkipLocalWrites(
            alloc: Allocator,
            db: anytype,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            source_artifact_name: []const u8,
            unit_key: []const u8,
            unit_fingerprint: []const u8,
            unit: document_extraction_mod.Unit,
            desired_chunk_keys: []const []const u8,
            chunk_range_base_index: usize,
            previous_child_ranges: []const types.DocumentArtifactChildRange,
        ) !bool {
            if (!(try storeKeyExists(alloc, db, unit_key))) return false;

            for (view.chunks) |entry| {
                const chunks = if (entry.chunker_json.len > 0)
                    try chunker_mod.chunkTextWithConfigJson(alloc, unit.text, entry.chunker_json)
                else
                    try chunker_mod.chunkText(alloc, unit.text, entry.chunk_size, entry.chunk_overlap);
                defer chunker_mod.freeChunks(alloc, chunks);
                document_extraction_mod.applyTranscriptTiming(unit, chunks);

                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const scratch = arena_state.allocator();

                for (chunks) |chunk| {
                    if (!chunk.isText()) continue;
                    defer _ = arena_state.reset(.retain_capacity);
                    const chunk_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(scratch, doc_key, entry.name, unit.unit_id, @intCast(chunk.chunk_id));
                    const chunk_key_index = documentExtractionKeyIndex(desired_chunk_keys, chunk_key) orelse return false;
                    const stored = (db.core.getStoreValue(scratch, chunk_key) catch |err| switch (err) {
                        error.NotFound => return false,
                        else => return err,
                    }) orelse return false;
                    const chunk_range_id = try documentExtractionRangeIdAlloc(scratch, chunk_range_base_index + (chunk_key_index / document_extraction_range_target_children));
                    const chunk_route = documentExtractionRangeRoute(previous_child_ranges, chunk_range_id, "chunk", "derived_chunks");
                    const expected = try buildDocumentUnitChunkPayloadAlloc(scratch, doc_key, unit_key, unit_fingerprint, entry.name, source_artifact_name, entry.source_field, unit, chunk, true, chunk_route);
                    if (!std.mem.eql(u8, stored, expected)) return false;
                    if (!(try documentUnitChunkEmbeddingArtifactsPresent(alloc, db, &entry, chunk_key))) return false;
                }
            }
            return true;
        }

        pub fn documentUnitChunkEmbeddingArtifactsPresent(
            alloc: Allocator,
            db: anytype,
            chunk_view: *const DocumentExtractionChunkView,
            chunk_key: []const u8,
        ) !bool {
            const runtime = db.enrichment_runtime;
            for (chunk_view.dense_embeddings) |entry| {
                if (entry.consumer_indexes.len == 0 or runtime == null or runtime.?.config.dense_embedder == null) continue;
                const artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, entry.name);
                defer alloc.free(artifact_key);
                if (!(try storeKeyExists(alloc, db, artifact_key))) return false;
            }
            for (chunk_view.sparse_embeddings) |entry| {
                if (entry.consumer_indexes.len == 0 or runtime == null or runtime.?.config.sparse_embedder == null) continue;
                const artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, entry.name);
                defer alloc.free(artifact_key);
                if (!(try storeKeyExists(alloc, db, artifact_key))) return false;
            }
            return true;
        }

        pub fn documentUnitConfidence(unit: document_extraction_mod.Unit) ?f64 {
            return unit.ocr_confidence orelse unit.transcript_confidence;
        }

        pub fn documentUnitPayloadAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
            unit: document_extraction_mod.Unit,
            unit_fingerprint: []const u8,
            source_url: []const u8,
            content_type: []const u8,
            route: DocumentExtractionRangeRoute,
        ) ![]u8 {
            const owner_group_id = std.math.cast(i64, route.owner_group_id) orelse return error.InvalidDocumentExtractionManifest;
            return try std.json.Stringify.valueAlloc(alloc, .{
                ._parent_doc_key = doc_key,
                ._artifact_name = artifact_name,
                ._artifact_range_id = route.range_id,
                ._artifact_range_kind = "unit",
                ._artifact_route_status = route.route_status,
                ._artifact_owner_group_id = owner_group_id,
                ._artifact_unit_fingerprint = unit_fingerprint,
                .unit_id = unit.unit_id,
                .unit_type = unit.unit_type,
                .text = unit.text,
                .content_type = "text/plain",
                .language = "",
                .source_path = unit.source_path,
                .extraction_status = unit.extraction_status,
                .source_sha256 = unit.source_sha256,
                .byte_length = unit.byte_length,
                .confidence = documentUnitConfidence(unit),
                .ocr_attempted = unit.ocr_attempted,
                .ocr_render_dpi = unit.ocr_render_dpi,
                .ocr_effective_render_dpi = unit.ocr_effective_render_dpi,
                .ocr_rendered_width = unit.ocr_rendered_width,
                .ocr_rendered_height = unit.ocr_rendered_height,
                .ocr_rendered_bytes = unit.ocr_rendered_bytes,
                .ocr_failure_stage = unit.ocr_failure_stage,
                .ocr_failure_retryable = unit.ocr_failure_retryable,
                .ocr_trigger_reasons = unit.ocr_trigger_reasons,
                .ocr_embedded_quality = unit.ocr_embedded_quality,
                .ocr_output_quality = unit.ocr_output_quality,
                .ocr_confidence = unit.ocr_confidence,
                .ocr_bbox = unit.ocr_bbox,
                .transcript_confidence = unit.transcript_confidence,
                .extraction_warning = unit.extraction_warning,
                .provenance = .{
                    .source_url = source_url,
                    .source_path = unit.source_path,
                    .method = unit.method,
                    .extraction_status = unit.extraction_status,
                    .source_sha256 = unit.source_sha256,
                    .byte_length = unit.byte_length,
                    .confidence = documentUnitConfidence(unit),
                    .ocr_used = unit.ocr_used,
                    .ocr_attempted = unit.ocr_attempted,
                    .ocr_render_dpi = unit.ocr_render_dpi,
                    .ocr_effective_render_dpi = unit.ocr_effective_render_dpi,
                    .ocr_rendered_width = unit.ocr_rendered_width,
                    .ocr_rendered_height = unit.ocr_rendered_height,
                    .ocr_rendered_bytes = unit.ocr_rendered_bytes,
                    .ocr_failure_stage = unit.ocr_failure_stage,
                    .ocr_failure_retryable = unit.ocr_failure_retryable,
                    .ocr_trigger_reasons = unit.ocr_trigger_reasons,
                    .ocr_embedded_quality = unit.ocr_embedded_quality,
                    .ocr_output_quality = unit.ocr_output_quality,
                    .ocr_confidence = unit.ocr_confidence,
                    .ocr_bbox = unit.ocr_bbox,
                    .transcript_used = unit.transcript_used,
                    .transcript_confidence = unit.transcript_confidence,
                    .transcript_spans = if (unit.transcript_spans.len > 0) unit.transcript_spans else null,
                    .extraction_warning = unit.extraction_warning,
                    .page_number = unit.page_number,
                    .page_label = unit.page_label,
                    .page_bbox = unit.page_bbox,
                    .page_rotation = unit.page_rotation,
                    .text_regions = unit.text_regions,
                    .char_start = unit.char_start,
                    .char_end = unit.char_end,
                    .source_content_type = content_type,
                    .format_provenance = .{
                        .schema = "antfly.document_format_provenance.v1",
                        .source_content_type = content_type,
                        .source_path = unit.source_path,
                        .coordinate_system = "source_page_points",
                        .extraction_method = unit.method,
                        .extraction_status = unit.extraction_status,
                        .source_sha256 = unit.source_sha256,
                        .byte_length = unit.byte_length,
                        .confidence = documentUnitConfidence(unit),
                        .ocr_used = unit.ocr_used,
                        .ocr_attempted = unit.ocr_attempted,
                        .ocr_render_dpi = unit.ocr_render_dpi,
                        .ocr_effective_render_dpi = unit.ocr_effective_render_dpi,
                        .ocr_rendered_width = unit.ocr_rendered_width,
                        .ocr_rendered_height = unit.ocr_rendered_height,
                        .ocr_rendered_bytes = unit.ocr_rendered_bytes,
                        .ocr_failure_stage = unit.ocr_failure_stage,
                        .ocr_failure_retryable = unit.ocr_failure_retryable,
                        .ocr_trigger_reasons = unit.ocr_trigger_reasons,
                        .ocr_embedded_quality = unit.ocr_embedded_quality,
                        .ocr_output_quality = unit.ocr_output_quality,
                        .ocr_confidence = unit.ocr_confidence,
                        .ocr_bbox = unit.ocr_bbox,
                        .transcript_used = unit.transcript_used,
                        .transcript_confidence = unit.transcript_confidence,
                        .extraction_warning = unit.extraction_warning,
                        .page_number = unit.page_number,
                        .page_label = unit.page_label,
                        .page_bbox = unit.page_bbox,
                        .page_rotation = unit.page_rotation,
                        .text_regions = unit.text_regions,
                    },
                },
            }, .{});
        }

        pub fn dupeConsumerIndexNames(alloc: Allocator, names: []const []const u8) ![][]u8 {
            const out = try alloc.alloc([]u8, names.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |name| alloc.free(name);
                alloc.free(out);
            }
            for (names, 0..) |name, i| {
                out[i] = try alloc.dupe(u8, name);
                initialized += 1;
            }
            return out;
        }

        pub fn embeddingArtifactKeyForBaseAlloc(alloc: Allocator, base_key: []const u8, artifact_name: []const u8) ![]u8 {
            return if (internal_keys.isInternalUserKey(base_key))
                try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, base_key, artifact_name)
            else
                try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, base_key, artifact_name);
        }

        pub fn encodeArtifactRepairCompletionState(
            out: *[artifact_repair_completion_state_len]u8,
            state: ArtifactRepairCompletionState,
        ) void {
            std.mem.writeInt(u64, out[0..8], state.epoch, .little);
            std.mem.writeInt(u64, out[8..16], state.completed_sequence, .little);
            std.mem.writeInt(u64, out[16..24], state.pending_issues, .little);
        }

        pub fn encodeArtifactRepairIssueValueAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
            return try std.json.Stringify.valueAlloc(alloc, issue, .{ .emit_null_optional_fields = false });
        }

        pub fn encodeGraphAssetStateKeysAlloc(alloc: Allocator, generation: u64, writes: []const docstore_mod.KVPair) ![]u8 {
            return try graph_asset_state.encodeAlloc(alloc, generation, writes);
        }

        pub fn encodeGraphEdgeArtifactWithTtlAlloc(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            key: []const u8,
            generation: u64,
            ttl_duration_ns: u64,
            timestamp_ns: u64,
            write: types.GraphEdgeWrite,
        ) ![]u8 {
            var ttl_created_ns = write.ttl_created_ns;
            if (ttl_duration_ns != 0 and ttl_created_ns == 0) {
                ttl_created_ns = @max(1, timestamp_ns);
                const existing = store.get(alloc, key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (existing) |raw| {
                    defer alloc.free(raw);
                    var previous = enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, raw) catch null;
                    if (previous) |*edge| {
                        defer edge.deinit(alloc);
                        if (edge.generation == generation and edge.ttl_created_ns != 0) ttl_created_ns = edge.ttl_created_ns;
                    }
                }
            }
            return try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(
                alloc,
                null,
                generation,
                write.weight,
                write.created_at,
                write.updated_at,
                ttl_created_ns,
                write.metadata_json,
            );
        }

        pub fn encodeGraphSourceEdgeArtifactWithTtlAlloc(
            alloc: Allocator,
            _: *docstore_mod.DocStore,
            _: []const u8,
            generation: u64,
            ttl_duration_ns: u64,
            timestamp_ns: u64,
            write: types.GraphEdgeWrite,
        ) ![]u8 {
            return try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(
                alloc,
                null,
                generation,
                write.weight,
                write.created_at,
                write.updated_at,
                if (ttl_duration_ns != 0 and write.ttl_created_ns == 0) @max(1, timestamp_ns) else write.ttl_created_ns,
                write.metadata_json,
            );
        }

        pub fn extractAssetSourceValue(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
        ) !?[]u8 {
            if (request.source_template.len > 0) {
                const rendered = renderSourceTemplateText(alloc, db, request.source_template, doc_value) catch |err| switch (err) {
                    error.OutOfMemory, error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                    else => return null,
                };
                errdefer alloc.free(rendered);
                try document_extraction_mod.validateInlineSourceSize(db.remote_content, rendered);
                return @constCast(rendered);
            }

            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{ .parse_numbers = false });
            defer parsed.deinit();
            if (parsed.value != .object) return null;
            const source = parsed.value.object.get(request.source_field) orelse return null;
            return switch (source) {
                .null => null,
                .string => |value| blk: {
                    try document_extraction_mod.validateInlineSourceSize(db.remote_content, value);
                    break :blk try alloc.dupe(u8, value);
                },
                else => blk: {
                    const rendered = try std.json.Stringify.valueAlloc(alloc, source, .{});
                    errdefer alloc.free(rendered);
                    try document_extraction_mod.validateInlineSourceSize(db.remote_content, rendered);
                    break :blk rendered;
                },
            };
        }

        pub fn extractDocumentDownloadedAlloc(
            alloc: Allocator,
            downloaded: anytype,
            source_url: []const u8,
            config: document_extraction_mod.Config,
            config_json: []const u8,
            raw_document_json: []const u8,
            out_failure: *runtime_failure_abi.FailureIdentity,
        ) !document_extraction_mod.Result {
            out_failure.* = .{};
            if (comptime !builtin.is_test and build_options.linked_storage) {
                return document_extraction_client.extractDownloadedAllocWithLimitsWithFailure(
                    alloc,
                    downloaded,
                    source_url,
                    config_json,
                    raw_document_json,
                    config.pdf_decode_limits,
                    out_failure,
                );
            }
            return document_extraction_mod.extractDownloadedAlloc(alloc, downloaded, source_url, config);
        }

        pub fn extractionConfidenceForLocalId(entities: []const resolver_lib.ExtractedEntity, local_id: []const u8) ?f64 {
            for (entities) |entity| {
                if (std.mem.eql(u8, entity.local_id, local_id)) return entity.confidence;
            }
            return null;
        }

        pub fn extractionEntityForLocalId(entities: []const resolver_lib.ExtractedEntity, local_id: []const u8) ?resolver_lib.ExtractedEntity {
            for (entities) |entity| {
                if (std.mem.eql(u8, entity.local_id, local_id)) return entity;
            }
            return null;
        }

        pub fn filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay(
            ctx: *const AsyncContext,
            index_name: []const u8,
            dims: u32,
            owned: *OwnedDenseEmbeddingWrites,
            sequence: u64,
        ) !void {
            const ArtifactProbe = struct {
                artifact_key: []const u8,
                write_index: usize,

                fn lessThan(_: void, lhs: @This(), rhs: @This()) bool {
                    return switch (std.mem.order(u8, lhs.artifact_key, rhs.artifact_key)) {
                        .lt => true,
                        .eq => lhs.write_index < rhs.write_index,
                        .gt => false,
                    };
                }
            };

            var probes = std.ArrayListUnmanaged(ArtifactProbe).empty;
            defer probes.deinit(ctx.alloc);
            for (owned.writes, 0..) |write, write_index| {
                const artifact_key = write.artifact_key orelse continue;
                try probes.append(ctx.alloc, .{ .artifact_key = artifact_key, .write_index = write_index });
            }
            if (probes.items.len == 0) return;
            std.mem.sort(ArtifactProbe, probes.items, {}, ArtifactProbe.lessThan);

            const keys = try ctx.alloc.alloc([]const u8, probes.items.len);
            defer ctx.alloc.free(keys);
            const values = try ctx.alloc.alloc(?[]const u8, probes.items.len);
            defer ctx.alloc.free(values);
            for (probes.items, 0..) |probe, index| keys[index] = probe.artifact_key;

            // Replay consumes every artifact exactly once. Retaining these large
            // vector-bearing blocks both pollutes the foreground cache and duplicates
            // HBC's bounded native vector workspace. One sorted probe also avoids a
            // transaction and point lookup per embedding.
            var artifact_txn = try ctx.store.beginProbeTxnWithBlockCacheAdmission(.transient);
            defer artifact_txn.abort();
            try artifact_txn.getManySorted(keys, values);

            var removed_indices = std.ArrayListUnmanaged(usize).empty;
            defer removed_indices.deinit(ctx.alloc);
            for (probes.items, values) |probe, maybe_raw| {
                const write = owned.writes[probe.write_index];
                const reason = if (maybe_raw) |raw|
                    try denseEmbeddingArtifactRepairReasonFromRaw(ctx.alloc, dims, raw)
                else
                    types.ArtifactRepairReason.missing_artifact;
                if (reason) |repair_reason| {
                    // A delete can commit after this journal record but before replay
                    // reads its artifact. That is an ordinary supersession, not
                    // corruption: advance past the stale upsert and let the following
                    // delete record remove any indexed value. This also prevents TTL
                    // cleanup from wedging replay on an artifact it correctly removed.
                    if (repair_reason == .missing_artifact and !try denseEmbeddingWriteSourceDocumentExists(ctx, write)) {
                        try removed_indices.append(ctx.alloc, probe.write_index);
                        continue;
                    }
                    try recordEmbeddingArtifactRepairIssueContext(ctx, index_name, probe.artifact_key, sequence, repair_reason);
                }
            }
            if (removed_indices.items.len == 0) return;
            std.mem.sort(usize, removed_indices.items, {}, std.sort.asc(usize));

            const kept_len = owned.writes.len - removed_indices.items.len;
            const kept = try owned.alloc.alloc(mapper.DenseEmbeddingWrite, kept_len);
            var kept_idx: usize = 0;
            var removed_idx: usize = 0;
            for (owned.writes, 0..) |write, write_idx| {
                if (removed_idx < removed_indices.items.len and removed_indices.items[removed_idx] == write_idx) {
                    removed_idx += 1;
                    if (owned.owns_doc_keys) {
                        owned.alloc.free(@constCast(write.doc_key));
                        if (write.parent_doc_key) |parent_doc_key| owned.alloc.free(@constCast(parent_doc_key));
                    }
                    continue;
                }
                kept[kept_idx] = write;
                kept_idx += 1;
            }
            if (owned.allocation_len > 0) owned.alloc.free(owned.writes.ptr[0..owned.allocation_len]);
            owned.writes = kept;
            owned.allocation_len = kept.len;
        }

        pub fn filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay(
            ctx: *const AsyncContext,
            index_name: []const u8,
            owned: *OwnedSparseEmbeddingWrites,
            sequence: u64,
        ) !void {
            var removed_indices = std.ArrayListUnmanaged(usize).empty;
            defer removed_indices.deinit(ctx.alloc);
            for (owned.writes, 0..) |write, write_idx| {
                const artifact_key = write.artifact_key orelse continue;
                if (try sparseEmbeddingArtifactRepairReason(ctx, artifact_key)) |reason| {
                    // Sparse artifact cleanup follows the same primary/journal
                    // ordering as dense cleanup. If a later delete already removed
                    // both the source and artifact, this stale upsert is superseded;
                    // the later delete record remains responsible for index removal.
                    if (reason == .missing_artifact and !try replaySourceDocumentExists(ctx, write.doc_key)) {
                        try removed_indices.append(ctx.alloc, write_idx);
                        continue;
                    }
                    try recordEmbeddingArtifactRepairIssueContext(ctx, index_name, artifact_key, sequence, reason);
                }
            }
            if (removed_indices.items.len == 0) return;

            const kept_len = owned.writes.len - removed_indices.items.len;
            const kept = try owned.alloc.alloc(mapper.SparseEmbeddingWrite, kept_len);
            var kept_idx: usize = 0;
            var removed_idx: usize = 0;
            for (owned.writes, 0..) |write, write_idx| {
                if (removed_idx < removed_indices.items.len and removed_indices.items[removed_idx] == write_idx) {
                    removed_idx += 1;
                    continue;
                }
                kept[kept_idx] = write;
                kept_idx += 1;
            }
            if (owned.allocation_len > 0) owned.alloc.free(owned.writes.ptr[0..owned.allocation_len]);
            owned.writes = kept;
            owned.allocation_len = kept.len;
        }

        pub fn filterChangedGraphMaterializationBatch(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            writes: []const docstore_mod.KVPair,
            deletes: []const []const u8,
        ) !BorrowedGraphMaterializationBatch {
            var changed_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            errdefer changed_writes.deinit(alloc);
            for (writes) |write| {
                if (try storeValueDiffers(alloc, store, write.key, write.value)) {
                    try changed_writes.append(alloc, write);
                }
            }

            var changed_deletes = std.ArrayListUnmanaged([]const u8).empty;
            errdefer changed_deletes.deinit(alloc);
            for (deletes) |key| {
                if (containsStoreWriteKey(writes, key)) continue;
                if (try storeContainsKey(alloc, store, key)) {
                    try changed_deletes.append(alloc, key);
                }
            }

            return .{
                .writes = try changed_writes.toOwnedSlice(alloc),
                .deletes = try changed_deletes.toOwnedSlice(alloc),
            };
        }

        pub fn findDocumentArtifactChildRange(
            ranges: []const types.DocumentArtifactChildRange,
            range_id: []const u8,
            range_kind: []const u8,
            artifact_name: []const u8,
        ) ?*const types.DocumentArtifactChildRange {
            for (ranges) |*range| {
                if (std.mem.eql(u8, range.range_id, range_id) and
                    std.mem.eql(u8, range.range_kind, range_kind) and
                    std.mem.eql(u8, range.artifact_name, artifact_name))
                {
                    return range;
                }
            }
            return null;
        }

        pub fn findOrAppendMentionEdgeAggregate(
            alloc: Allocator,
            aggregates: *std.ArrayListUnmanaged(MentionEdgeAggregate),
            target: []const u8,
            target_table: []const u8,
        ) !*MentionEdgeAggregate {
            for (aggregates.items) |*existing| {
                if (std.mem.eql(u8, existing.target, target)) return existing;
            }
            const target_owned = try alloc.dupe(u8, target);
            var target_owned_live = true;
            errdefer if (target_owned_live) alloc.free(target_owned);
            const table_owned = try alloc.dupe(u8, target_table);
            var table_owned_live = true;
            errdefer if (table_owned_live) alloc.free(table_owned);
            try aggregates.append(alloc, .{
                .target = target_owned,
                .target_table = table_owned,
                .mention_confidence = 0,
            });
            target_owned_live = false;
            table_owned_live = false;
            return &aggregates.items[aggregates.items.len - 1];
        }

        pub fn finishManagedSyncTargets(
            alloc: Allocator,
            full_text_indexes: *std.ArrayListUnmanaged([]const u8),
            all_indexes: *std.ArrayListUnmanaged([]const u8),
            target_identities: *std.ArrayListUnmanaged(IndexTargetVisibility),
            target_scope_known: bool,
        ) !ManagedSyncTargets {
            const owned_full_text = try full_text_indexes.toOwnedSlice(alloc);
            errdefer {
                for (owned_full_text) |name| alloc.free(@constCast(name));
                if (owned_full_text.len > 0) alloc.free(owned_full_text);
            }
            const owned_all = try all_indexes.toOwnedSlice(alloc);
            errdefer {
                for (owned_all) |name| alloc.free(@constCast(name));
                if (owned_all.len > 0) alloc.free(owned_all);
            }
            var identities: []IndexTargetVisibility = &.{};
            if (target_scope_known) {
                identities = try target_identities.toOwnedSlice(alloc);
            } else {
                for (target_identities.items) |identity| alloc.free(@constCast(identity.index_name));
                target_identities.deinit(alloc);
                target_identities.* = .empty;
            }

            return .{
                .full_text_indexes = owned_full_text,
                .all_indexes = owned_all,
                .target_identities = identities,
                .target_scope_known = target_scope_known,
            };
        }

        pub fn flushPendingDenseChunkEmbedding(
            alloc: Allocator,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            dense_embedder: embedder_mod.DenseEmbedder,
            doc_key: []const u8,
            pending: *PendingDocumentUnitDenseChunkEmbedding,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            dense_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite),
        ) !void {
            if (pending.chunk_texts.items.len == 0) return;
            const synthetic_request: enrichment_types.GeneratedEnrichmentRequest = .{
                .kind = .dense_embedding,
                .index_name = "",
                .doc_key = doc_key,
                .source_field = pending.source_field,
                .expected_dims = pending.expected_dims,
                .producer_json = pending.producer_json,
            };
            try flushGeneratedDenseChunkBatch(
                alloc,
                runtime,
                dense_embedder,
                pending.embedding_name,
                synthetic_request,
                artifact_writes,
                dense_embeddings,
                pending.sources.items,
                &pending.source_indexes,
                &pending.chunk_texts,
                pending.consumer_indexes,
                appendDerivedDenseEmbeddingForConsumers,
            );
            pending.batch_source_bytes = 0;
        }

        pub fn flushPendingSparseChunkEmbedding(
            alloc: Allocator,
            runtime: *enrichment_runtime_mod.EnrichmentRuntime,
            sparse_embedder: embedder_mod.SparseEmbedder,
            pending: *PendingDocumentUnitSparseChunkEmbedding,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            sparse_embeddings: *std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite),
        ) !void {
            if (pending.chunk_texts.items.len == 0) return;
            try flushGeneratedSparseChunkBatch(
                alloc,
                runtime,
                sparse_embedder,
                pending.embedding_name,
                pending.producer_json,
                artifact_writes,
                sparse_embeddings,
                pending.sources.items,
                &pending.source_indexes,
                &pending.chunk_texts,
                pending.consumer_indexes,
            );
            pending.batch_source_bytes = 0;
        }

        pub fn flushPrecomputeAssetProducerBatch(
            alloc: Allocator,
            db: anytype,
            items: *std.ArrayListUnmanaged(PrecomputeAssetProducerBatchItem),
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            coverage_outcomes: *std.ArrayListUnmanaged(PrecomputedCoverageOutcome),
        ) !void {
            if (items.items.len == 0) return;
            defer clearPrecomputeAssetProducerBatchItems(alloc, items);

            const runtime = db.enrichment_runtime orelse return error.MissingAssetProducer;
            const producer = runtime.config.asset_producer orelse return error.MissingAssetProducer;
            const requests = try alloc.alloc(asset_producer_mod.Request, items.items.len);
            defer alloc.free(requests);
            for (items.items, 0..) |*item, idx| requests[idx] = item.asRequest();

            const can_batch = producer.canProduceBatch(alloc, requests) catch |err| {
                if (err == error.OutOfMemory) return err;
                if (isRetryableAssetProducerError(err)) return err;
                return try flushPrecomputeAssetProducerBatchSequential(alloc, producer, items.items, db, artifact_writes, documents, coverage_outcomes);
            };
            if (!can_batch)
                return try flushPrecomputeAssetProducerBatchSequential(alloc, producer, items.items, db, artifact_writes, documents, coverage_outcomes);

            var produced = producer.produceBatch(alloc, requests) catch |err| {
                if (err == error.OutOfMemory) return err;
                if (isRetryableAssetProducerError(err)) return err;
                return try flushPrecomputeAssetProducerBatchSequential(alloc, producer, items.items, db, artifact_writes, documents, coverage_outcomes);
            };
            if (produced.len != items.items.len) {
                for (produced) |output| {
                    if (output.len > 0) alloc.free(output);
                }
                alloc.free(produced);
                return try flushPrecomputeAssetProducerBatchSequential(alloc, producer, items.items, db, artifact_writes, documents, coverage_outcomes);
            }

            defer alloc.free(produced);
            errdefer {
                for (produced) |output| {
                    if (output.len > 0) alloc.free(output);
                }
            }

            for (items.items, produced, 0..) |item, output, idx| {
                applyPrecomputeAssetProducerOutput(alloc, db, item, output, artifact_writes, documents, coverage_outcomes) catch |err| {
                    alloc.free(output);
                    produced[idx] = "";
                    if (err == error.OutOfMemory) return err;
                    if (isRetryableAssetProducerError(err)) return err;
                    return err;
                };
                alloc.free(output);
                produced[idx] = "";
            }
        }

        pub fn flushPrecomputeAssetProducerBatchSequential(
            alloc: Allocator,
            producer: asset_producer_mod.Producer,
            items: []const PrecomputeAssetProducerBatchItem,
            db: anytype,
            artifact_writes: *std.ArrayListUnmanaged(types.BatchWrite),
            documents: *std.ArrayListUnmanaged(derived_types.DerivedDocument),
            coverage_outcomes: *std.ArrayListUnmanaged(PrecomputedCoverageOutcome),
        ) !void {
            for (items) |item| {
                const produced = producer.produce(alloc, item.asRequest()) catch |err| {
                    if (err == error.OutOfMemory) return err;
                    if (isRetryableAssetProducerError(err)) return err;
                    return err;
                };
                defer alloc.free(produced);
                applyPrecomputeAssetProducerOutput(alloc, db, item, produced, artifact_writes, documents, coverage_outcomes) catch |err| {
                    if (err == error.OutOfMemory) return err;
                    if (isRetryableAssetProducerError(err)) return err;
                    return err;
                };
            }
        }

        pub fn freeChunkArtifactKeys(alloc: Allocator, keys: []const []u8) void {
            for (keys) |key| alloc.free(key);
            alloc.free(keys);
        }

        pub fn freeGraphWrites(alloc: Allocator, writes: []types.GraphEdgeWrite) void {
            for (writes) |write| freeGraphWriteFields(alloc, write);
            if (writes.len > 0) alloc.free(writes);
        }

        pub fn freePrecomputeAssetProducerBatchItem(alloc: Allocator, item: PrecomputeAssetProducerBatchItem) void {
            enrichment_types.freeGeneratedRequest(alloc, item.request);
            if (item.config_json.len > 0) alloc.free(item.config_json);
            alloc.free(item.source_text);
            if (item.source_parts_json) |parts| alloc.free(parts);
            alloc.free(item.artifact_key);
            alloc.free(item.state_key);
            alloc.free(item.state_value);
        }

        pub fn fullTextTargetRefsAlloc(
            alloc: Allocator,
            text_indexes: []const []const u8,
        ) ![]derived_types.DerivedTargetRef {
            const targets = try alloc.alloc(derived_types.DerivedTargetRef, text_indexes.len);
            errdefer {
                for (targets) |target| alloc.free(target.index_name);
                alloc.free(targets);
            }
            for (text_indexes, 0..) |index_name, i| {
                targets[i] = .{
                    .kind = .full_text,
                    .index_name = try alloc.dupe(u8, index_name),
                };
            }
            return targets;
        }

        pub fn generatedConsumerSetsEqual(left: []const []const u8, right: []const []const u8) bool {
            if (left.len != right.len) return false;
            for (left, right) |a, b| if (!std.mem.eql(u8, a, b)) return false;
            return true;
        }

        pub fn getOrCreateChunks(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            cache: *std.ArrayListUnmanaged(ChunkCacheEntry),
        ) ![]chunker_mod.Chunk {
            const cache_key = try makeChunkCacheKey(alloc, request);
            errdefer alloc.free(cache_key);

            for (cache.items) |entry| {
                if (std.mem.eql(u8, entry.key, cache_key)) {
                    alloc.free(cache_key);
                    return entry.chunks;
                }
            }

            const source_text = if (request.source_template.len > 0)
                renderSourceTemplateText(alloc, db, request.source_template, doc_value) catch |err| switch (err) {
                    error.OutOfMemory, error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                    else => null,
                }
            else
                try extractStringField(alloc, doc_value, request.source_field);
            if (source_text == null or source_text.?.len == 0) {
                if (source_text) |s| alloc.free(s);
                const empty = try alloc.alloc(chunker_mod.Chunk, 0);
                try cache.append(alloc, .{
                    .key = cache_key,
                    .chunks = empty,
                });
                return cache.items[cache.items.len - 1].chunks;
            }
            defer alloc.free(source_text.?);

            const chunks = if (request.chunker_json.len > 0)
                try chunker_mod.chunkTextWithConfigJson(alloc, source_text.?, request.chunker_json)
            else
                try chunker_mod.chunkText(alloc, source_text.?, request.chunk_size, request.chunk_overlap);
            errdefer chunker_mod.freeChunks(alloc, chunks);
            try cache.append(alloc, .{
                .key = cache_key,
                .chunks = chunks,
            });
            return cache.items[cache.items.len - 1].chunks;
        }

        pub fn graphArtifactContentType(index_manager: *const index_manager_mod.IndexManager, artifact_name: []const u8) []const u8 {
            if (index_manager.getEnrichment(.asset, artifact_name)) |cfg| return cfg.content_type;
            if (index_manager.getEnrichment(.chunk, artifact_name) != null) return "application/json";
            return "";
        }

        pub fn graphArtifactRefUsesDocumentWideFallback(artifact_ref: types.ArtifactRef) bool {
            return artifact_ref.kind == .asset and artifact_ref.unit_id == null and artifact_ref.chunk_id == null and artifact_ref.source == null;
        }

        pub fn graphArtifactSourceConsumesArtifactKey(
            index_manager: *const index_manager_mod.IndexManager,
            source: index_manager_mod.GraphArtifactSource,
            artifact_key: []const u8,
        ) bool {
            if (decodeArtifactRefViewForGraphApplicability(artifact_key) catch null) |artifact_ref| {
                return graphArtifactSourceConsumesRefView(index_manager, source, artifact_ref);
            }

            var artifact_ref = (artifact_ids.decodeArtifactRefAlloc(index_manager.alloc, artifact_key) catch return false) orelse return false;
            defer artifact_ref.deinit(index_manager.alloc);
            return graphArtifactSourceConsumesRef(index_manager, source, artifact_ref);
        }

        pub fn graphArtifactSourceConsumesRef(
            index_manager: *const index_manager_mod.IndexManager,
            source: index_manager_mod.GraphArtifactSource,
            artifact_ref: types.ArtifactRef,
        ) bool {
            if (!std.mem.eql(u8, source.artifact_name, artifact_ref.name)) return false;
            return switch (artifact_ref.kind) {
                .asset => graphAssetSourceConsumesAssetRef(index_manager, artifact_ref),
                .chunk => index_manager.getEnrichment(.chunk, artifact_ref.name) != null,
                .embedding => false,
            };
        }

        pub fn graphArtifactSourceConsumesRefView(
            index_manager: *const index_manager_mod.IndexManager,
            source: index_manager_mod.GraphArtifactSource,
            artifact_ref: GraphArtifactRefView,
        ) bool {
            if (!std.mem.eql(u8, source.artifact_name, artifact_ref.name)) return false;
            return switch (artifact_ref.kind) {
                .asset => graphAssetSourceConsumesAssetRefView(index_manager, artifact_ref),
                .chunk => index_manager.getEnrichment(.chunk, artifact_ref.name) != null,
                .embedding => false,
            };
        }

        pub fn graphArtifactStateNameAlloc(alloc: Allocator, artifact_ref: types.ArtifactRef) ![]u8 {
            return try graph_state_name.artifactAlloc(alloc, artifact_ref);
        }

        pub fn graphAssetSourceConsumesAssetRef(index_manager: *const index_manager_mod.IndexManager, artifact_ref: types.ArtifactRef) bool {
            if (index_manager.getEnrichment(.asset, artifact_ref.name) == null) return false;
            if (artifact_ref.unit_id != null) return true;
            const cfg = index_manager.getEnrichment(.asset, artifact_ref.name) orelse return false;
            var producer_cfg = asset_producer_mod.parseProducerConfig(index_manager.alloc, cfg.producer_json) catch return true;
            defer producer_cfg.deinit(index_manager.alloc);
            return producer_cfg.type != .document_extraction;
        }

        pub fn graphAssetSourceConsumesAssetRefView(index_manager: *const index_manager_mod.IndexManager, artifact_ref: GraphArtifactRefView) bool {
            if (index_manager.getEnrichment(.asset, artifact_ref.name) == null) return false;
            if (artifact_ref.unit_id_present) return true;
            const cfg = index_manager.getEnrichment(.asset, artifact_ref.name) orelse return false;
            var producer_cfg = asset_producer_mod.parseProducerConfig(index_manager.alloc, cfg.producer_json) catch return true;
            defer producer_cfg.deinit(index_manager.alloc);
            return producer_cfg.type != .document_extraction;
        }

        pub fn graphAssetStateKeyAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8, artifact_name: []const u8) ![]u8 {
            var list = std.ArrayListUnmanaged(u8).empty;
            defer list.deinit(alloc);
            try internal_keys.appendDocumentPrefix(&list, alloc, doc_key);
            try list.append(alloc, internal_keys.graph_asset_state_kind);
            try internal_keys.appendEncodedComponent(&list, alloc, index_name);
            try internal_keys.appendEncodedComponent(&list, alloc, artifact_name);
            return try list.toOwnedSlice(alloc);
        }

        pub fn graphCleanupReplayWritesAlloc(alloc: Allocator, page: docstore_mod.DocStore.GraphEndpointCleanupPage) ![]types.BatchWrite {
            const writes = try alloc.alloc(types.BatchWrite, page.replay_writes.len);
            for (page.replay_writes, writes) |source, *target| target.* = .{ .key = source.key, .value = source.value };
            return writes;
        }

        pub fn graphContenderStateChanged(changes: []const GraphContenderChange, state_key: []const u8) bool {
            for (changes) |change| {
                if (std.mem.eql(u8, change.state_key, state_key)) return true;
            }
            return false;
        }

        pub fn graphEndpointCleanupDeletesAlloc(alloc: Allocator, page: docstore_mod.DocStore.GraphEndpointCleanupPage) ![]types.GraphEdgeDelete {
            const items = try alloc.alloc(types.GraphEdgeDelete, page.writes.len);
            var initialized: usize = 0;
            errdefer {
                for (items[0..initialized]) |*item| item.deinit(alloc);
                alloc.free(items);
            }
            for (page.writes, items) |row, *item| {
                const artifact = try internal_keys.graphRetirementArtifactKeyAlloc(alloc, row.key);
                defer alloc.free(artifact);
                const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, artifact)).?;
                item.* = .{
                    .index_name = parsed.index_name,
                    .source = if (parsed.logical_source.len > 0) parsed.logical_source else parsed.doc_key,
                    .target = parsed.target_doc_key,
                    .edge_type = parsed.edge_type,
                    .edge_id = parsed.edge_id,
                    .owner_document = if (parsed.edge_id.len > 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                    .owner = if (parsed.edge_id.len == 0 and parsed.logical_source.len > 0) parsed.doc_key else "",
                };
                initialized += 1;
            }
            return items;
        }

        pub fn graphEndpointResolutionsJsonAlloc(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            doc_key: []const u8,
            source_artifact_name: []const u8,
        ) !?[]u8 {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var map: std.json.ObjectMap = .empty;
            var resolver_targets_artifact = false;

            for (index_manager.resolvers.items) |*cfg| {
                if (!std.mem.eql(u8, cfg.source_artifact, source_artifact_name)) continue;
                resolver_targets_artifact = true;
                const res_key = try internal_keys.resolutionArtifactKeyAlloc(a, doc_key, cfg.resolution_artifact);
                const raw = store.get(a, res_key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch continue;
                if (parsed != .object) continue;
                const entities = parsed.object.get("entities") orelse continue;
                if (entities != .array) continue;
                for (entities.array.items) |entity| {
                    if (entity != .object) continue;
                    // Same human-review gate as the mention-edge path
                    // (resolutionDecisionCreatesCanonicalEdge): a review-band entry
                    // carries a PROVISIONAL doc key, and admitting it here would make
                    // relations traversable through that identity before a curator
                    // approves it. Only canonical decisions become endpoints; the
                    // relation stays absent (drop-before-resolution) and the
                    // approval's re-resolution replay renders it.
                    const decision = jsonStringField(entity, "decision") orelse continue;
                    if (!std.mem.eql(u8, decision, "new") and !std.mem.eql(u8, decision, "match")) continue;
                    const local_id = jsonStringField(entity, "local_id") orelse continue;
                    const doc_ref = entity.object.get("doc_ref") orelse continue;
                    if (doc_ref != .object) continue;
                    const key = jsonStringField(doc_ref, "key") orelse continue;
                    if (key.len == 0) continue;
                    var ref: std.json.ObjectMap = .empty;
                    try ref.put(a, "key", .{ .string = key });
                    if (jsonStringField(doc_ref, "table")) |table| try ref.put(a, "table", .{ .string = table });
                    try map.put(a, local_id, .{ .object = ref });
                }
            }
            // No resolver targets this artifact: return null so relation sources
            // keep their owning document (see appendRelationItem). A targeting
            // resolver with nothing landed yet still yields an (empty) map, which
            // tells the materializer canonical keys are coming.
            if (!resolver_targets_artifact) return null;
            return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = map }, .{});
        }

        pub fn graphStateSourcePriorityAlloc(
            alloc: Allocator,
            state_key: []const u8,
            state_prefix: []const u8,
            sources: []const index_manager_mod.GraphArtifactSource,
        ) !?usize {
            if (!std.mem.startsWith(u8, state_key, state_prefix)) return null;
            const terminator = internal_keys.findComponentTerminator(state_key, state_prefix.len) orelse return null;
            const state_name = try internal_keys.decodeBodyAlloc(alloc, state_key[state_prefix.len..terminator]);
            defer alloc.free(state_name);

            if (std.mem.eql(u8, state_name, internal_keys.graph_direct_state_name)) return @intCast(graph_mod.direct_source_priority);
            return graph_state_name.materializedSourcePriority(state_name, sources);
        }

        pub fn graphWritesFromArtifactValueAlloc(
            alloc: Allocator,
            index_name: []const u8,
            doc_key: []const u8,
            raw: []const u8,
            source: index_manager_mod.GraphArtifactSource,
            artifact_content_type: []const u8,
            raw_doc: ?[]const u8,
            endpoint_resolutions_raw: ?[]const u8,
            edge_limit: usize,
        ) ![]types.GraphEdgeWrite {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .parse_numbers = false });
            defer parsed.deinit();
            if (endpoint_resolutions_raw) |res_raw| try injectGraphEndpointResolutions(&parsed, res_raw);
            var parsed_doc = if (raw_doc) |doc| try std.json.parseFromSlice(std.json.Value, alloc, doc, .{ .parse_numbers = false }) else null;
            defer if (parsed_doc) |*doc| doc.deinit();
            const doc_value: ?std.json.Value = if (parsed_doc) |doc| doc.value else null;

            var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
            errdefer {
                for (writes.items) |write| freeGraphWriteFields(alloc, write);
                writes.deinit(alloc);
            }

            switch (source.format) {
                .extraction_relation => try appendRelationItemsFromPath(alloc, &writes, index_name, doc_key, doc_value, parsed.value, source.path, source.mapping, source.artifact_name, artifact_content_type, parsed.value, edge_limit),
                .extraction_graph => {
                    if (source.path.len > 0) {
                        try appendRelationItemsFromPath(alloc, &writes, index_name, doc_key, doc_value, parsed.value, source.path, source.mapping, source.artifact_name, artifact_content_type, parsed.value, edge_limit);
                    } else if (parsed.value == .object) {
                        if (parsed.value.object.get("relations")) |relations| try appendRelationValueItems(alloc, &writes, index_name, doc_key, doc_value, relations, source.mapping, source.artifact_name, artifact_content_type, parsed.value, edge_limit);
                        if (parsed.value.object.get("edges")) |edges| try appendRelationValueItems(alloc, &writes, index_name, doc_key, doc_value, edges, source.mapping, source.artifact_name, artifact_content_type, parsed.value, edge_limit);
                    }
                },
            }

            return try writes.toOwnedSlice(alloc);
        }

        pub fn hexBytesAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
            const out = try alloc.alloc(u8, bytes.len * 2);
            for (bytes, 0..) |byte, idx| {
                out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
                out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
            }
            return out;
        }

        pub fn hierarchyNavigationArtifactDigestAlloc(
            alloc: Allocator,
            units: []const DocumentExtractionUnitDescriptor,
        ) ![]u8 {
            var hasher = std.crypto.hash.sha2.Sha256.init(.{});
            hasher.update("antfly-hierarchy-artifact-v1");
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, @intCast(units.len), .big);
            hasher.update(&encoded);
            for (units) |unit| {
                std.mem.writeInt(u64, &encoded, @intCast(unit.key.len), .big);
                hasher.update(&encoded);
                hasher.update(unit.key);
                std.mem.writeInt(u64, &encoded, @intCast(unit.fingerprint.len), .big);
                hasher.update(&encoded);
                hasher.update(unit.fingerprint);
            }
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            hasher.final(&digest);
            return try hexBytesAlloc(alloc, &digest);
        }

        pub fn hierarchyNavigationBlockCount(unit_count: u32) u32 {
            if (unit_count == 0) return 0;
            return std.math.divCeil(u32, unit_count, hierarchy_navigation_block_size) catch unreachable;
        }

        pub fn hierarchyNavigationBlockValueAlloc(
            alloc: Allocator,
            block_index: u32,
            units: []const DocumentExtractionUnitDescriptor,
        ) ![]u8 {
            return try std.json.Stringify.valueAlloc(alloc, .{
                .kind = "document_unit_navigation_block_v1",
                .block_index = block_index,
                .units = units,
            }, .{});
        }

        pub fn hierarchyNavigationDigestIsValid(digest: []const u8) bool {
            if (digest.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
            for (digest) |byte| {
                if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return false;
            }
            return true;
        }

        pub fn hierarchyNavigationSummaryValueAlloc(
            alloc: Allocator,
            generation: u64,
            digest: []const u8,
            unit_count: u32,
            block_count: u32,
        ) ![]u8 {
            if (generation == 0 or !hierarchyNavigationDigestIsValid(digest) or
                block_count != hierarchyNavigationBlockCount(unit_count))
            {
                return error.InvalidDocumentExtractionState;
            }
            return try std.json.Stringify.valueAlloc(alloc, .{
                .kind = "document_unit_navigation_summary_v1",
                .generation = generation,
                .digest = digest,
                .unit_count = unit_count,
                .block_count = block_count,
                .block_size = hierarchy_navigation_block_size,
            }, .{});
        }

        pub fn indexNameInSlice(name: []const u8, index_names: []const []const u8) bool {
            for (index_names) |candidate| {
                if (std.mem.eql(u8, name, candidate)) return true;
            }
            return false;
        }

        pub fn indexPendingArtifactWrites(
            alloc: Allocator,
            index: *PendingArtifactWriteIndex,
            writes: []const types.BatchWrite,
            indexed_count: *usize,
        ) !void {
            for (writes[indexed_count.*..]) |write| try index.add(alloc, write);
            indexed_count.* = writes.len;
        }

        pub fn injectGraphEndpointResolutions(parsed: *std.json.Parsed(std.json.Value), resolutions_raw: []const u8) !void {
            if (parsed.value != .object) return;
            const a = parsed.arena.allocator();
            const res = std.json.parseFromSliceLeaky(std.json.Value, a, resolutions_raw, .{}) catch return;
            if (res != .object) return;
            try parsed.value.object.put(a, "_entities", res);
        }

        pub fn isMergeArtifactKey(key: []const u8) bool {
            return internal_keys.isGraphOwnerReplayJobKey(key) or
                internal_keys.isGraphEdgeArtifactKey(key) or
                internal_keys.isGraphRetirementKey(key) or
                internal_keys.isGraphGlobalEdgeContenderKey(key) or
                internal_keys.isGraphEdgeTtlLifetimeKey(key) or
                internal_keys.isGraphEdgeTtlTombstoneKey(key) or
                internal_keys.isAssetArtifactKey(key) or
                internal_keys.isChunkArtifactRecordKey(key) or internal_keys.isResolutionArtifactKey(key) or
                internal_keys.isEmbeddingArtifactKey(key) or
                internal_keys.isDerivedEmbeddingArtifactKey(key);
        }

        pub fn isRetryableAssetProducerError(err: anyerror) bool {
            return switch (err) {
                error.EmbedRateLimited,
                error.EmbedTransientFailure,
                error.ModelNotFound,
                error.ConnectionRefused,
                error.ConnectionResetByPeer,
                error.ConnectionTimedOut,
                error.Timeout,
                error.NetworkUnreachable,
                error.HostLacksNetworkAddresses,
                error.TemporaryNameServerFailure,
                error.NameServerFailure,
                error.UnexpectedReadFailure,
                error.SendFailed,
                error.RecvFailed,
                error.ResourceBudgetExceeded,
                error.GenerateBatchTransientFailure,
                error.ReadTransientFailure,
                => true,
                else => false,
            };
        }

        pub fn keyAfterAlloc(alloc: Allocator, key: []const u8) ![]u8 {
            const out = try alloc.alloc(u8, key.len + 1);
            @memcpy(out[0..key.len], key);
            out[key.len] = 0;
            return out;
        }

        pub fn loadArtifactRepairCompletionStateFromStore(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            key: []const u8,
        ) !?ArtifactRepairCompletionState {
            const raw = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            // This fence is an optimization, never authoritative repair debt. Treat a
            // malformed value as absent so metadata corruption cannot wedge repair;
            // the next issue transition overwrites or deletes it atomically.
            if (raw.len != artifact_repair_completion_state_len) return null;
            return .{
                .epoch = std.mem.readInt(u64, raw[0..8], .little),
                .completed_sequence = std.mem.readInt(u64, raw[8..16], .little),
                .pending_issues = std.mem.readInt(u64, raw[16..24], .little),
            };
        }

        pub fn loadArtifactRepairIssueFromStoreByKey(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            key: []const u8,
        ) !?types.ArtifactRepairIssue {
            const raw = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            return try decodeArtifactRepairIssueValueAlloc(alloc, raw);
        }

        pub fn loadDerivedCoverageOutcomeCounterFromStore(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
            generation: u64,
            outcome: []const u8,
        ) !?u64 {
            const counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, outcome);
            defer alloc.free(counter_key);
            const raw = store.get(alloc, counter_key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            return try internal_keys.decodeDerivedCoverageOutcomeCount(raw);
        }

        pub fn loadDocumentExtractionPreviousState(
            alloc: Allocator,
            db: anytype,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            artifact_name: []const u8,
            existing_state: ?[]const u8,
        ) !DocumentExtractionPreviousState {
            if (existing_state) |state| {
                if (loadDocumentExtractionPreviousStateFromJson(alloc, state)) |parsed| {
                    return parsed;
                } else |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => {},
                }
            }
            var recovered = try scanDocumentExtractionPreviousStateFromStore(alloc, db, view, doc_key, artifact_name);
            recovered.recovered_from_store_scan = existing_state != null;
            return recovered;
        }

        pub fn loadDocumentExtractionPreviousStateFromJson(alloc: Allocator, state: []const u8) !DocumentExtractionPreviousState {
            var out = DocumentExtractionPreviousState{};
            errdefer out.deinit(alloc);
            out.unit_keys = try documentExtractionStateUnitKeysAlloc(alloc, state);
            out.unit_descriptors = try documentExtractionStateUnitDescriptorsAlloc(alloc, state);
            out.chunk_keys = try documentExtractionStateChunkKeysAlloc(alloc, state);
            out.navigation_block_count = try documentExtractionStateNavigationBlockCount(alloc, state);
            return out;
        }

        pub fn loadGraphAssetStateKeysAlloc(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            state_key: []const u8,
            expected_generation: u64,
        ) !?[][]u8 {
            const raw = store.get(alloc, state_key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            if (try graph_asset_state.coverageGeneration(raw) != expected_generation) return null;
            return switch (try graph_asset_state.format(raw)) {
                .v4 => try graph_asset_state.decodeKeysAlloc(alloc, raw),
                .v5 => blk: {
                    const root = try graph_asset_state.segmentedRoot(raw);
                    const root_key_count: usize = root.key_count;
                    const all = try alloc.alloc([]u8, root_key_count);
                    var initialized: usize = 0;
                    var encoded_bytes: usize = 0;
                    errdefer {
                        for (all[0..initialized]) |key| alloc.free(key);
                        if (all.len > 0) alloc.free(all);
                    }
                    for (0..root.segment_count) |segment_index| {
                        const segment_key = try internal_keys.graphAssetStateSegmentKeyAlloc(alloc, state_key, @intCast(segment_index));
                        defer alloc.free(segment_key);
                        const segment_raw = store.get(alloc, segment_key) catch |err| switch (err) {
                            error.NotFound => return error.InvalidGraphAssetState,
                            else => return err,
                        };
                        defer alloc.free(segment_raw);
                        encoded_bytes = std.math.add(usize, encoded_bytes, segment_raw.len) catch return error.ResourceLimitExceeded;
                        if (encoded_bytes > graph_asset_state.hard_max_manifest_bytes) return error.ResourceLimitExceeded;
                        const segment_keys = try graph_asset_state.decodeSegmentKeysAlloc(alloc, segment_raw, expected_generation);
                        defer if (segment_keys.len > 0) alloc.free(segment_keys);
                        if (initialized > root_key_count or segment_keys.len > root_key_count - initialized) {
                            return error.InvalidGraphAssetState;
                        }
                        @memcpy(all[initialized..][0..segment_keys.len], segment_keys);
                        initialized += segment_keys.len;
                    }
                    if (initialized != root.key_count) return error.InvalidGraphAssetState;
                    break :blk all;
                },
            };
        }

        pub fn loadSourceExtractionForResolution(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            doc_key: []const u8,
            source_artifact: []const u8,
        ) !?[]u8 {
            if (try sourceArtifactKeyFromResolutionScopeAlloc(alloc, doc_key, source_artifact)) |source_key| {
                defer alloc.free(source_key);
                return store.get(alloc, source_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
            }
            const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", source_artifact);
            defer alloc.free(extraction_key);
            return store.get(alloc, extraction_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        }

        pub fn logSparseWriteProfileDelta(index_name: []const u8, delta: sparse_mod.WriteProfile) void {
            std.log.info(
                "antfly_bench_sparse_write index={s} batch_calls={d} incremental_calls={d} bulk_append_calls={d} bulk_append_fallbacks={d} writes={d} deletes={d} postings={d} terms={d} reserve_ms={d} dedupe_ms={d} existence_check_ms={d} doc_num_ms={d} fwd_rev_put_ms={d} posting_collect_ms={d} posting_sort_ms={d} posting_write_ms={d} chunk_read_ms={d} chunk_encode_ms={d} chunk_put_ms={d} range_meta_encode_ms={d} range_meta_put_ms={d} term_meta_ms={d} commit_ms={d} incremental_delete_ms={d} incremental_insert_ms={d} incremental_refresh_ms={d} incremental_commit_ms={d}",
                .{
                    index_name,
                    delta.batch_calls,
                    delta.incremental_calls,
                    delta.bulk_append_calls,
                    delta.bulk_append_fallbacks,
                    delta.writes,
                    delta.deletes,
                    delta.postings,
                    delta.terms,
                    nsToMs(delta.reserve_ns),
                    nsToMs(delta.dedupe_ns),
                    nsToMs(delta.existence_check_ns),
                    nsToMs(delta.doc_num_ns),
                    nsToMs(delta.fwd_rev_put_ns),
                    nsToMs(delta.posting_collect_ns),
                    nsToMs(delta.posting_sort_ns),
                    nsToMs(delta.posting_write_ns),
                    nsToMs(delta.chunk_read_ns),
                    nsToMs(delta.chunk_encode_ns),
                    nsToMs(delta.chunk_put_ns),
                    nsToMs(delta.range_meta_encode_ns),
                    nsToMs(delta.range_meta_put_ns),
                    nsToMs(delta.term_meta_ns),
                    nsToMs(delta.commit_ns),
                    nsToMs(delta.incremental_delete_ns),
                    nsToMs(delta.incremental_insert_ns),
                    nsToMs(delta.incremental_refresh_ns),
                    nsToMs(delta.incremental_commit_ns),
                },
            );
        }

        pub fn makeChunkCacheKey(alloc: Allocator, request: enrichment_types.GeneratedEnrichmentRequest) ![]u8 {
            var chunk_size: [@sizeOf(u32)]u8 = undefined;
            var chunk_overlap: [@sizeOf(u32)]u8 = undefined;
            std.mem.writeInt(u32, &chunk_size, request.chunk_size, .big);
            std.mem.writeInt(u32, &chunk_overlap, request.chunk_overlap, .big);
            return try chunkCacheTupleKeyAlloc(alloc, &.{
                request.doc_key,
                requestArtifactName(request),
                request.source_field,
                &chunk_size,
                &chunk_overlap,
                request.chunker_json,
            });
        }

        pub fn managedIndexBatchApplicability(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) ManagedIndexBatchApplicability {
            return managedIndexBatchApplicabilityWithEmbeddingNames(index_manager, batch, index_ref, null);
        }

        pub fn managedIndexBatchApplicabilityWithEmbeddingNames(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
            changed_embedding_names: ?*std.StringHashMapUnmanaged(void),
        ) ManagedIndexBatchApplicability {
            switch (index_ref.kind) {
                .full_text, .algebraic => {
                    if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, batch.deleted_keys) or
                        batch.overwritten_doc_keys.len > 0) return .relevant;
                    for (batch.documents) |doc| {
                        if (doc.action == .upsert) return .relevant;
                    }
                    return .irrelevant;
                },
                .dense_vector => {
                    if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, batch.deleted_keys) or
                        batch.overwritten_doc_keys.len > 0) return .relevant;
                    const uses_artifact_members = index_manager.denseIndexUsesArtifactMembers(index_ref.name);
                    if (!uses_artifact_members) {
                        for (batch.documents) |doc| {
                            if (doc.action == .upsert) return .relevant;
                        }
                    }
                    for (batch.dense_embeddings) |embedding| {
                        if (!std.mem.eql(u8, embedding.index_name, index_ref.name)) continue;
                        if (!uses_artifact_members) return .relevant;
                        const artifact_key = embedding.artifact_key orelse continue;
                        if (index_manager.denseIndexAcceptsArtifactKey(index_ref.name, artifact_key)) return .relevant;
                    }
                    if (changed_embedding_names) |names| {
                        var iterator = names.keyIterator();
                        while (iterator.next()) |name| {
                            if (managedIndexConsumesEmbeddingName(index_manager, index_ref, name.*)) return .relevant;
                        }
                    } else if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, batch.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .sparse_vector => {
                    if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, batch.deleted_keys) or
                        batch.overwritten_doc_keys.len > 0) return .relevant;
                    const uses_artifact_members = index_manager.sparseIndexUsesArtifactMembers(index_ref.name);
                    if (!uses_artifact_members) {
                        for (batch.documents) |doc| {
                            if (doc.action == .upsert) return .relevant;
                        }
                    }
                    for (batch.sparse_embeddings) |embedding| {
                        if (!std.mem.eql(u8, embedding.index_name, index_ref.name)) continue;
                        if (!uses_artifact_members) return .relevant;
                        const artifact_key = embedding.artifact_key orelse continue;
                        if (index_manager.sparseIndexAcceptsArtifactKey(index_ref.name, artifact_key)) return .relevant;
                    }
                    if (changed_embedding_names) |names| {
                        var iterator = names.keyIterator();
                        while (iterator.next()) |name| {
                            if (managedIndexConsumesEmbeddingName(index_manager, index_ref, name.*)) return .relevant;
                        }
                    } else if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, batch.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .graph => {
                    if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, batch.deleted_keys)) return .relevant;
                    for (batch.graph_doc_clears) |clear| {
                        for (clear.index_names) |index_name| {
                            if (std.mem.eql(u8, index_name, index_ref.name)) return .relevant;
                        }
                    }
                    for (batch.graph_writes) |write| {
                        if (std.mem.eql(u8, write.index_name, index_ref.name)) return .relevant;
                    }
                    for (batch.graph_deletes) |delete| {
                        if (std.mem.eql(u8, delete.index_name, index_ref.name)) return .relevant;
                    }
                    for (batch.changed_artifact_keys) |artifact_key| {
                        if (!internal_keys.isResolutionArtifactKey(artifact_key) and !internal_keys.isGraphEdgeArtifactKey(artifact_key)) {
                            for (index_manager.graphArtifactSources(index_ref.name)) |source| {
                                if (graphArtifactSourceConsumesArtifactKey(index_manager, source, artifact_key)) return .relevant;
                            }
                            continue;
                        }
                        if (internal_keys.isResolutionArtifactKey(artifact_key)) {
                            const parsed = (internal_keys.parseResolutionArtifactKeyAlloc(index_manager.alloc, artifact_key) catch continue) orelse continue;
                            defer {
                                index_manager.alloc.free(parsed.doc_key);
                                index_manager.alloc.free(parsed.artifact_name);
                            }
                            for (index_manager.graphArtifactSources(index_ref.name)) |source| {
                                if (source.mention_edge_type.len == 0) continue;
                                if (resolverConfigForResolution(index_manager, source.artifact_name, parsed.artifact_name) != null) return .relevant;
                            }
                            if (resolverConfigForResolutionArtifact(index_manager, parsed.artifact_name) != null) continue;
                            return .missing_dependency;
                        }
                        if (internal_keys.isGraphEdgeArtifactKey(artifact_key)) {
                            const parsed = (internal_keys.parseGraphEdgeArtifactKeyAlloc(index_manager.alloc, artifact_key) catch continue) orelse continue;
                            defer {
                                index_manager.alloc.free(parsed.doc_key);
                                index_manager.alloc.free(parsed.index_name);
                                index_manager.alloc.free(parsed.edge_type);
                                index_manager.alloc.free(parsed.target_doc_key);
                                index_manager.alloc.free(parsed.edge_id);
                                index_manager.alloc.free(parsed.logical_source);
                            }
                            if (std.mem.eql(u8, parsed.index_name, index_ref.name)) return .relevant;
                        }
                    }
                    return .irrelevant;
                },
            }
        }

        pub fn managedIndexBatchServingSetEffect(
            index_manager: *index_manager_mod.IndexManager,
            batch: derived_types.DerivedBatch,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) IndexTargetVisibility.ServingSetEffect {
            if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, batch.deleted_keys) or
                batch.overwritten_doc_keys.len != 0)
                return .may_reduce;
            if (index_ref.kind == .graph and
                (batch.graph_doc_clears.len != 0 or batch.graph_deletes.len != 0))
                return .may_reduce;
            return .additive_only;
        }

        pub fn managedIndexConsumesEmbeddingName(
            index_manager: *index_manager_mod.IndexManager,
            index_ref: index_manager_mod.ManagedIndexRef,
            embedding_name: []const u8,
        ) bool {
            return switch (index_ref.kind) {
                .dense_vector => index_manager.denseIndexConsumesEmbedding(index_ref.name, embedding_name),
                .sparse_vector => index_manager.sparseIndexConsumesEmbedding(index_ref.name, embedding_name),
                else => false,
            };
        }

        pub fn managedIndexDeleteKeyAffectsProjection(
            index_manager: *index_manager_mod.IndexManager,
            index_ref: index_manager_mod.ManagedIndexRef,
            key: []const u8,
        ) bool {
            // Public document identities are carried unencoded in ordinary replay;
            // primary-store document keys may appear in replicated/internal paths.
            // Either form can remove a member from every document-backed projection.
            if (isMetadataKey(key) or internal_keys.isInternalMetadataKey(key)) return false;
            if (!internal_keys.isInternalUserKey(key) or internal_keys.isPrimaryDocumentKey(key)) return true;

            // Internal cleanup keys are not generic document deletions. Only the
            // projection that consumes the exact artifact may treat one as reduction
            // authority; manifests, retry state, and sibling artifacts are no-ops.
            return switch (index_ref.kind) {
                .full_text => index_manager.textIndexAcceptsArtifactKey(index_ref.name, key),
                // Chunk retirement removes its logical members even when the surviving
                // embeddings are cache hits and publish no replacement writes.
                .dense_vector, .sparse_vector => vectorIndexConsumesChunkKey(index_manager, index_ref, key) or batchHasEmbeddingArtifactForManagedIndex(
                    index_manager,
                    index_ref,
                    &.{key},
                ),
                .algebraic => false,
                .graph => blk: {
                    if (internal_keys.isGraphEdgeArtifactKey(key)) {
                        const parsed = (internal_keys.parseGraphEdgeArtifactKeyAlloc(index_manager.alloc, key) catch break :blk false) orelse break :blk false;
                        defer {
                            index_manager.alloc.free(parsed.doc_key);
                            index_manager.alloc.free(parsed.index_name);
                            index_manager.alloc.free(parsed.edge_type);
                            index_manager.alloc.free(parsed.target_doc_key);
                            index_manager.alloc.free(parsed.edge_id);
                            index_manager.alloc.free(parsed.logical_source);
                        }
                        if (std.mem.eql(u8, parsed.index_name, index_ref.name)) break :blk true;
                    }
                    for (index_manager.graphArtifactSources(index_ref.name)) |source| {
                        if (graphArtifactSourceConsumesArtifactKey(index_manager, source, key)) break :blk true;
                    }
                    break :blk false;
                },
            };
        }

        pub fn managedIndexDeleteKeysAffectProjection(
            index_manager: *index_manager_mod.IndexManager,
            index_ref: index_manager_mod.ManagedIndexRef,
            keys: []const []const u8,
        ) bool {
            for (keys) |key| {
                if (managedIndexDeleteKeyAffectsProjection(index_manager, index_ref, key)) return true;
            }
            return false;
        }

        pub fn managedIndexRecordApplicability(
            index_manager: *index_manager_mod.IndexManager,
            record: change_journal_mod.Record,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) ManagedIndexBatchApplicability {
            switch (index_ref.kind) {
                .full_text, .algebraic => {
                    if (record.changed_doc_keys.len > 0 or
                        managedIndexDeleteKeysAffectProjection(index_manager, index_ref, record.deleted_doc_keys) or
                        record.overwritten_doc_keys.len > 0) return .relevant;
                    return .irrelevant;
                },
                .dense_vector => {
                    if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, record.deleted_doc_keys) or
                        record.overwritten_doc_keys.len > 0) return .relevant;
                    // Artifact-backed indexes consume generated artifact records, not
                    // the source document record that scheduled enrichment. Treating
                    // that source record as perpetually applicable prevents a clean
                    // target advance after an idempotent index recreate reuses already
                    // durable artifacts. The artifact target counter and active index
                    // cardinality below remain the completeness gate.
                    if (record.changed_doc_keys.len > 0) {
                        const entry = index_manager.denseIndex(index_ref.name);
                        if (entry == null or !denseIndexIsArtifactBacked(entry.?)) return .relevant;
                    }
                    if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, record.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .sparse_vector => {
                    if (record.changed_doc_keys.len > 0 or
                        managedIndexDeleteKeysAffectProjection(index_manager, index_ref, record.deleted_doc_keys) or
                        record.overwritten_doc_keys.len > 0) return .relevant;
                    if (batchHasEmbeddingArtifactForManagedIndex(index_manager, index_ref, record.changed_artifact_keys)) return .relevant;
                    return .irrelevant;
                },
                .graph => {
                    if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, record.deleted_doc_keys)) return .relevant;
                    for (record.changed_artifact_keys) |artifact_key| {
                        if (!internal_keys.isResolutionArtifactKey(artifact_key) and !internal_keys.isGraphEdgeArtifactKey(artifact_key)) {
                            for (index_manager.graphArtifactSources(index_ref.name)) |source| {
                                if (graphArtifactSourceConsumesArtifactKey(index_manager, source, artifact_key)) return .relevant;
                            }
                            continue;
                        }
                        if (internal_keys.isResolutionArtifactKey(artifact_key)) {
                            const parsed = (internal_keys.parseResolutionArtifactKeyAlloc(index_manager.alloc, artifact_key) catch continue) orelse continue;
                            defer {
                                index_manager.alloc.free(parsed.doc_key);
                                index_manager.alloc.free(parsed.artifact_name);
                            }
                            for (index_manager.graphArtifactSources(index_ref.name)) |source| {
                                if (source.mention_edge_type.len == 0) continue;
                                if (resolverConfigForResolution(index_manager, source.artifact_name, parsed.artifact_name) != null) return .relevant;
                            }
                            if (resolverConfigForResolutionArtifact(index_manager, parsed.artifact_name) != null) continue;
                            return .missing_dependency;
                        }
                        if (internal_keys.isGraphEdgeArtifactKey(artifact_key)) {
                            const parsed = (internal_keys.parseGraphEdgeArtifactKeyAlloc(index_manager.alloc, artifact_key) catch continue) orelse continue;
                            defer {
                                index_manager.alloc.free(parsed.doc_key);
                                index_manager.alloc.free(parsed.index_name);
                                index_manager.alloc.free(parsed.edge_type);
                                index_manager.alloc.free(parsed.target_doc_key);
                                index_manager.alloc.free(parsed.edge_id);
                                index_manager.alloc.free(parsed.logical_source);
                            }
                            if (std.mem.eql(u8, parsed.index_name, index_ref.name)) return .relevant;
                        }
                    }
                    return .irrelevant;
                },
            }
        }

        pub fn managedIndexRecordServingSetEffect(
            index_manager: *index_manager_mod.IndexManager,
            record: change_journal_mod.Record,
            index_ref: index_manager_mod.ManagedIndexRef,
        ) IndexTargetVisibility.ServingSetEffect {
            if (managedIndexDeleteKeysAffectProjection(index_manager, index_ref, record.deleted_doc_keys) or
                record.overwritten_doc_keys.len != 0)
                return .may_reduce;
            // The compact replay record intentionally coalesces graph writes and
            // deletes into changed artifact identities. Without the original batch's
            // operation tags a relevant graph mutation must remain conservative.
            if (index_ref.kind == .graph and record.changed_artifact_keys.len != 0)
                return .may_reduce;
            return .additive_only;
        }

        pub fn materializeGraphArtifactValuePaged(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            index_name: []const u8,
            artifact_ref: types.ArtifactRef,
            source: index_manager_mod.GraphArtifactSource,
            state_key: []const u8,
            raw: []const u8,
            options: GraphMaterializationOptions,
            changed: *std.ArrayListUnmanaged([]u8),
            changed_set: *std.StringHashMapUnmanaged(void),
        ) !void {
            if (raw.len > options.max_input_bytes) return error.ResourceLimitExceeded;
            const generation = (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).config.coverage_generation;
            const configured_edge_limit = graph_asset_state.effectiveEdgeLimit((index_manager.graphIndex(index_name) orelse return error.IndexNotFound).max_edges_per_document);

            var parsed_artifact = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .parse_numbers = false });
            defer parsed_artifact.deinit();
            const endpoint_resolutions = try graphEndpointResolutionsJsonAlloc(alloc, store, index_manager, artifact_ref.document_id, source.artifact_name);
            defer if (endpoint_resolutions) |value| alloc.free(value);
            if (endpoint_resolutions) |res_raw| try injectGraphEndpointResolutions(&parsed_artifact, res_raw);
            const raw_doc = try storeDocumentValueForGraphSource(
                alloc,
                store,
                index_manager,
                artifact_ref.document_id,
                if (options.repair_ctx) |ctx| ctx.relational_base_rows else false,
            );
            defer if (raw_doc) |value| alloc.free(value);
            var parsed_document = if (raw_doc) |value| try std.json.parseFromSlice(std.json.Value, alloc, value, .{ .parse_numbers = false }) else null;
            defer if (parsed_document) |*document| document.deinit();

            var item_offset: usize = 0;
            var segment_index: usize = 0;
            var key_count: usize = 0;
            var manifest_bytes: usize = 0;
            while (true) {
                const page = graphWritesFromArtifactParsedPageAlloc(
                    alloc,
                    index_name,
                    artifact_ref.document_id,
                    parsed_artifact.value,
                    source,
                    graphArtifactContentType(index_manager, source.artifact_name),
                    if (parsed_document) |document| document.value else null,
                    item_offset,
                    2048,
                    4 * 1024 * 1024,
                    options.max_relation_items,
                ) catch |err| switch (err) {
                    error.OutOfMemory, error.ResourceLimitExceeded => return err,
                    else => return error.InvalidGraphArtifact,
                };
                defer freeGraphWrites(alloc, page.writes);

                var graph_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer {
                    for (graph_writes.items) |write| {
                        alloc.free(@constCast(write.key));
                        alloc.free(@constCast(write.value));
                    }
                    graph_writes.deinit(alloc);
                }
                var graph_write_positions = StoreWritePositions.empty;
                defer graph_write_positions.deinit(alloc);
                for (page.writes) |write| {
                    const key = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, if (write.owner_document.len > 0) write.owner_document else if (write.owner.len > 0) write.owner else write.source, write.index_name, write.edge_type, write.target, write.source, write.edge_id);
                    var key_owned = true;
                    errdefer if (key_owned) alloc.free(key);
                    const graph_entry = index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
                    const payload = try encodeGraphSourceEdgeArtifactWithTtlAlloc(alloc, store, key, generation, graph_entry.ttl_duration_ns, currentTimeNs(), write);
                    var payload_owned = true;
                    errdefer if (payload_owned) alloc.free(payload);
                    try upsertOwnedStoreWrite(alloc, &graph_writes, &graph_write_positions, key, payload);
                    key_owned = false;
                    payload_owned = false;
                }

                var reconciled = try reconcileSingleGraphStateContenders(
                    alloc,
                    store,
                    artifact_ref.document_id,
                    index_name,
                    state_key,
                    &.{},
                    graph_writes.items,
                    index_manager.graphArtifactSources(index_name),
                    generation,
                    (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).ttl_duration_ns,
                );
                defer reconciled.deinit(alloc);
                if (reconciled.visible_count > @min(configured_edge_limit, options.max_materialized_edges)) {
                    return error.ResourceLimitExceeded;
                }

                var extra_writes: [2]docstore_mod.KVPair = undefined;
                var extra_write_count: usize = 0;
                var segment_key: ?[]u8 = null;
                defer if (segment_key) |key| alloc.free(key);
                var segment_value: ?[]u8 = null;
                defer if (segment_value) |value| alloc.free(value);
                if (graph_writes.items.len > 0) {
                    if (segment_index > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
                    segment_key = try internal_keys.graphAssetStateSegmentKeyAlloc(alloc, state_key, @intCast(segment_index));
                    segment_value = try graph_asset_state.encodeSegmentAlloc(alloc, generation, graph_writes.items);
                    manifest_bytes = std.math.add(usize, manifest_bytes, segment_value.?.len) catch return error.ResourceLimitExceeded;
                    if (manifest_bytes > graph_asset_state.hard_max_manifest_bytes) return error.ResourceLimitExceeded;
                    key_count = std.math.add(usize, key_count, graph_writes.items.len) catch return error.ResourceLimitExceeded;
                    if (key_count > graph_asset_state.hard_max_edges_per_document) return error.ResourceLimitExceeded;
                    extra_writes[extra_write_count] = .{ .key = segment_key.?, .value = segment_value.? };
                    extra_write_count += 1;
                    segment_index += 1;
                }
                var root_value: ?[]u8 = null;
                defer if (root_value) |value| alloc.free(value);
                if (page.next_item_offset == null) {
                    root_value = try graph_asset_state.encodeSegmentedRootAlloc(alloc, generation, segment_index, key_count);
                    extra_writes[extra_write_count] = .{ .key = state_key, .value = root_value.? };
                    extra_write_count += 1;
                }

                for (graph_writes.items) |write| try appendUniqueOwnedKeyIndexed(alloc, changed, changed_set, write.key);
                const affected = try alloc.alloc([]const u8, graph_writes.items.len);
                defer if (affected.len > 0) alloc.free(affected);
                for (graph_writes.items, 0..) |write, i| affected[i] = write.key;
                try commitGraphContenderReconcilePage(
                    alloc,
                    store,
                    affected,
                    &reconciled,
                    extra_writes[0..extra_write_count],
                    &.{},
                );

                item_offset = page.next_item_offset orelse break;
            }
        }

        pub fn materializeGraphSourceArtifactsForIndex(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            changed_artifact_keys: []const []const u8,
            index_name: []const u8,
            options: GraphMaterializationOptions,
        ) ![][]u8 {
            const generation = (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).config.coverage_generation;
            const sources = index_manager.graphArtifactSources(index_name);
            if (sources.len == 0) return try alloc.alloc([]u8, 0);

            var changed = std.ArrayListUnmanaged([]u8).empty;
            errdefer freeOwnedKeySlice(alloc, changed.items);
            var changed_set = std.StringHashMapUnmanaged(void).empty;
            defer changed_set.deinit(alloc);

            for (changed_artifact_keys) |artifact_key| {
                if (internal_keys.isResolutionArtifactKey(artifact_key)) {
                    for (sources) |source| {
                        if (source.mention_edge_type.len == 0) continue;
                        materializeMentionEdgesForResolutionKey(alloc, store, index_manager, &changed, index_name, source, artifact_key, options) catch |err| switch (err) {
                            error.ResourceLimitExceeded => {
                                if (try recordGraphResolutionResourceLimitForRepair(alloc, options, index_name, source, artifact_key)) return error.ArtifactRepairRequired;
                                return err;
                            },
                            else => return err,
                        };
                    }
                    for (changed.items) |key| try changed_set.put(alloc, key, {});
                    // Relation edges materialize when the extraction artifact is
                    // written, which is before its resolution artifacts exist, so
                    // their first render carries local-id endpoints. A landed
                    // resolution re-renders the owning extraction artifact with
                    // canonical endpoints (graphEndpointResolutionsJsonAlloc); the
                    // existing replacement semantics retire the stale local-id
                    // edges. Depth-one recursion: the synthesized key is an asset
                    // artifact, never another resolution key.
                    if (try resolutionOwningAssetArtifactKeyAlloc(alloc, index_manager, artifact_key)) |asset_key| {
                        defer alloc.free(asset_key);
                        const rerendered = try materializeGraphSourceArtifactsForIndex(alloc, store, index_manager, &.{asset_key}, index_name, options);
                        defer alloc.free(rerendered);
                        var idx: usize = 0;
                        errdefer for (rerendered[idx..]) |key| alloc.free(key);
                        while (idx < rerendered.len) {
                            const key = rerendered[idx];
                            if (changed_set.contains(key)) {
                                alloc.free(key);
                                idx += 1;
                                continue;
                            }
                            try changed.append(alloc, key);
                            idx += 1;
                            try changed_set.put(alloc, key, {});
                        }
                    }
                    continue;
                }
                var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse continue;
                defer artifact_ref.deinit(alloc);
                const source = blk: {
                    for (sources) |candidate| {
                        if (graphArtifactSourceConsumesRef(index_manager, candidate, artifact_ref)) break :blk candidate;
                    }
                    continue;
                };

                const raw = store.get(alloc, artifact_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                defer if (raw) |value| alloc.free(value);

                var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer {
                    for (writes.items) |write| {
                        alloc.free(@constCast(write.key));
                        alloc.free(@constCast(write.value));
                    }
                    writes.deinit(alloc);
                }

                if (raw) |value| {
                    const raw_doc = try storeDocumentValueForGraphSource(
                        alloc,
                        store,
                        index_manager,
                        artifact_ref.document_id,
                        if (options.repair_ctx) |ctx| ctx.relational_base_rows else false,
                    );
                    defer if (raw_doc) |doc_value| alloc.free(doc_value);
                    const endpoint_resolutions = try graphEndpointResolutionsJsonAlloc(alloc, store, index_manager, artifact_ref.document_id, source.artifact_name);
                    defer if (endpoint_resolutions) |res_raw| alloc.free(res_raw);
                    const graph_writes = graphWritesFromArtifactValueAlloc(
                        alloc,
                        index_name,
                        artifact_ref.document_id,
                        value,
                        source,
                        graphArtifactContentType(index_manager, source.artifact_name),
                        raw_doc,
                        endpoint_resolutions,
                        options.max_relation_items,
                    ) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => {
                            if (options.repair_ctx) |repair_ctx| {
                                try recordArtifactRepairIssueForRefContext(repair_ctx, index_name, artifact_ref, artifact_key, options.sequence, .corrupt_artifact);
                                return error.ArtifactRepairRequired;
                            }
                            return err;
                        },
                    };
                    defer freeGraphWrites(alloc, graph_writes);
                    for (graph_writes) |write| {
                        const key = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, if (write.owner_document.len > 0) write.owner_document else if (write.owner.len > 0) write.owner else write.source, write.index_name, write.edge_type, write.target, write.source, write.edge_id);
                        var key_owned = true;
                        errdefer if (key_owned) alloc.free(key);
                        const graph_entry = index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
                        const payload = try encodeGraphSourceEdgeArtifactWithTtlAlloc(alloc, store, key, generation, graph_entry.ttl_duration_ns, currentTimeNs(), write);
                        var payload_owned = true;
                        errdefer if (payload_owned) alloc.free(payload);
                        try writes.append(alloc, .{ .key = key, .value = payload });
                        key_owned = false;
                        payload_owned = false;
                        try appendUniqueOwnedKey(alloc, &changed, key);
                    }
                }

                const state_name = try graphArtifactStateNameAlloc(alloc, artifact_ref);
                defer alloc.free(state_name);
                const state_key = try graphAssetStateKeyAlloc(alloc, artifact_ref.document_id, index_name, state_name);
                defer alloc.free(state_key);

                const had_state = store.get(alloc, state_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                defer if (had_state) |value| alloc.free(value);
                if (had_state == null and sources.len <= 1 and graphArtifactRefUsesDocumentWideFallback(artifact_ref)) {
                    const protected_keys = try resolutionMentionStateKeysForGraphSourceAlloc(alloc, store, index_manager, artifact_ref.document_id, index_name, source);
                    defer freeOwnedConstKeySlice(alloc, protected_keys);
                    const existing = try collectGraphArtifactsForDocIndex(alloc, store, artifact_ref.document_id, index_name);
                    defer docstore_mod.DocStore.freeResults(alloc, existing);
                    var legacy_deletes = std.ArrayListUnmanaged([]const u8).empty;
                    defer legacy_deletes.deinit(alloc);
                    const direct_state = try internal_keys.graphDirectStateKeyAlloc(alloc, artifact_ref.document_id, index_name);
                    defer alloc.free(direct_state);
                    for (existing) |entry| {
                        if (containsDeleteKey(protected_keys, entry.key)) continue;
                        // Legacy fallback owns source outputs only. Explicit writes
                        // already registered in the contributor machinery must survive
                        // a source withdrawal, including replay after a snapshot import.
                        const direct_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, index_name, generation, entry.key, @intCast(graph_mod.direct_source_priority), direct_state);
                        defer alloc.free(direct_key);
                        const direct_raw = store.get(alloc, direct_key) catch |err| switch (err) {
                            error.NotFound => null,
                            else => return err,
                        };
                        if (direct_raw) |raw_direct| {
                            defer alloc.free(raw_direct);
                            const direct = (try graph_edge_contender.decode(raw_direct, generation)) orelse return error.InvalidGraphEdgeContender;
                            if (!std.mem.eql(u8, direct.edge_key, entry.key) or !std.mem.eql(u8, direct.state_key, direct_state) or direct.source_priority != graph_mod.direct_source_priority) return error.InvalidGraphEdgeContender;
                            continue;
                        }
                        try legacy_deletes.append(alloc, entry.key);
                        try appendUniqueOwnedKeyIndexed(alloc, &changed, &changed_set, entry.key);
                    }
                    var start: usize = 0;
                    while (start < legacy_deletes.items.len) {
                        const end = @min(legacy_deletes.items.len, start + 2048);
                        try store.putBatch(&.{}, legacy_deletes.items[start..end]);
                        start = end;
                    }
                }

                clearGraphArtifactStatePaged(
                    alloc,
                    store,
                    index_manager,
                    artifact_ref.document_id,
                    index_name,
                    state_key,
                    generation,
                    raw == null,
                    &changed,
                    &changed_set,
                ) catch |err| switch (err) {
                    error.ResourceLimitExceeded => {
                        if (try recordGraphResourceLimitForRepair(options, index_name, artifact_ref, artifact_key)) return error.ArtifactRepairRequired;
                        return err;
                    },
                    else => return err,
                };

                if (raw) |value| materializeGraphArtifactValuePaged(
                    alloc,
                    store,
                    index_manager,
                    index_name,
                    artifact_ref,
                    source,
                    state_key,
                    value,
                    options,
                    &changed,
                    &changed_set,
                ) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    error.ResourceLimitExceeded => {
                        if (try recordGraphResourceLimitForRepair(options, index_name, artifact_ref, artifact_key)) return error.ArtifactRepairRequired;
                        return err;
                    },
                    else => {
                        if (options.repair_ctx) |repair_ctx| {
                            try recordArtifactRepairIssueForRefContext(repair_ctx, index_name, artifact_ref, artifact_key, options.sequence, .corrupt_artifact);
                            return error.ArtifactRepairRequired;
                        }
                        return err;
                    },
                };
            }

            return try changed.toOwnedSlice(alloc);
        }

        pub fn materializeMentionEdgesForResolutionKey(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            changed: *std.ArrayListUnmanaged([]u8),
            index_name: []const u8,
            source: index_manager_mod.GraphArtifactSource,
            resolution_key: []const u8,
            options: GraphMaterializationOptions,
        ) !void {
            if (source.mention_edge_type.len == 0) return;
            const generation = (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).config.coverage_generation;
            const parsed_key = (try internal_keys.parseResolutionArtifactKeyAlloc(alloc, resolution_key)) orelse return;
            defer alloc.free(parsed_key.doc_key);
            defer alloc.free(parsed_key.artifact_name);

            const raw_resolution = store.get(alloc, resolution_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (raw_resolution) |raw| alloc.free(raw);
            if (raw_resolution) |raw| {
                if (raw.len > options.max_input_bytes) {
                    if (try recordGraphResolutionResourceLimitForRepair(alloc, options, index_name, source, resolution_key)) return error.ArtifactRepairRequired;
                    return error.ResourceLimitExceeded;
                }
            }

            const cfg = resolverConfigForResolution(index_manager, source.artifact_name, parsed_key.artifact_name) orelse {
                // A multi-source graph pairs every changed resolution key with every
                // mention-edge source. A resolution artifact owned by a different
                // source's resolver (e.g. label-routed autoschema layouts with one
                // resolver pair per extraction artifact) is that sibling source's to
                // materialize, not a missing contract; only a resolution artifact no
                // resolver owns fails closed.
                if (resolverConfigForResolutionArtifact(index_manager, parsed_key.artifact_name) != null) return;
                if (options.require_resolution_contract) return error.MissingResolverArtifactContract;
                return;
            };
            const state_name = try mentionGraphStateNameAlloc(alloc, source.artifact_name, cfg.resolution_artifact);
            defer alloc.free(state_name);
            const state_key = try graphAssetStateKeyAlloc(alloc, parsed_key.doc_key, index_name, state_name);
            defer alloc.free(state_key);

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (writes.items) |write| {
                    alloc.free(@constCast(write.key));
                    alloc.free(@constCast(write.value));
                }
                writes.deinit(alloc);
            }
            var write_positions = StoreWritePositions.empty;
            defer write_positions.deinit(alloc);
            var mention_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (mention_writes.items) |write| {
                    alloc.free(@constCast(write.key));
                    alloc.free(@constCast(write.value));
                }
                mention_writes.deinit(alloc);
            }
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (deletes.items) |key| alloc.free(@constCast(key));
                deletes.deinit(alloc);
            }

            if (raw_resolution) |raw| {
                const raw_extraction = loadSourceExtractionForResolution(alloc, store, parsed_key.doc_key, cfg.source_artifact) catch null;
                defer if (raw_extraction) |raw_src| alloc.free(raw_src);
                const mention_edge_writes = try mentionEdgeWritesFromResolutionAlloc(
                    alloc,
                    index_name,
                    parsed_key.doc_key,
                    raw,
                    raw_extraction,
                    source.mention_edge_type,
                    cfg,
                );
                defer freeGraphWrites(alloc, mention_edge_writes);
                for (mention_edge_writes) |write| {
                    const key = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, if (write.owner_document.len > 0) write.owner_document else if (write.owner.len > 0) write.owner else write.source, write.index_name, write.edge_type, write.target, write.source, write.edge_id);
                    var key_owned = true;
                    errdefer if (key_owned) alloc.free(key);
                    const graph_entry = index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
                    const payload = try encodeGraphSourceEdgeArtifactWithTtlAlloc(alloc, store, key, generation, graph_entry.ttl_duration_ns, currentTimeNs(), write);
                    var payload_owned = true;
                    errdefer if (payload_owned) alloc.free(payload);
                    try appendUniqueOwnedKey(alloc, changed, key);
                    try upsertOwnedStoreWrite(alloc, &writes, &write_positions, key, payload);
                    key_owned = false;
                    payload_owned = false;
                }
                try appendMentionEvidenceArtifactsFromResolution(
                    alloc,
                    &mention_writes,
                    changed,
                    parsed_key.doc_key,
                    resolution_key,
                    raw,
                    raw_extraction,
                    cfg,
                );
            }

            const previous_keys = try loadGraphAssetStateKeysAlloc(alloc, store, state_key, generation);
            defer if (previous_keys) |keys| freeOwnedConstKeySlice(alloc, keys);
            try appendGraphAssetStateSegmentDeleteKeys(alloc, store, state_key, &deletes);
            const graph_write_count = writes.items.len;
            const state_value = try encodeGraphAssetStateKeysAlloc(alloc, generation, writes.items[0..graph_write_count]);
            var state_value_owned = true;
            defer if (state_value_owned) alloc.free(state_value);
            var reconciled = try reconcileSingleGraphStateContenders(
                alloc,
                store,
                parsed_key.doc_key,
                index_name,
                state_key,
                previous_keys orelse &.{},
                writes.items[0..graph_write_count],
                index_manager.graphArtifactSources(index_name),
                generation,
                (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).ttl_duration_ns,
            );
            defer reconciled.deinit(alloc);
            const configured_edge_limit = graph_asset_state.effectiveEdgeLimit((index_manager.graphIndex(index_name) orelse return error.IndexNotFound).max_edges_per_document);
            if (raw_resolution != null and reconciled.visible_count > @min(configured_edge_limit, options.max_materialized_edges)) {
                if (try recordGraphResolutionResourceLimitForRepair(alloc, options, index_name, source, resolution_key)) return error.ArtifactRepairRequired;
                return error.ResourceLimitExceeded;
            }
            var affected = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (affected.items) |key| alloc.free(key);
                affected.deinit(alloc);
            }
            if (previous_keys) |keys| for (keys) |key| try appendUniqueOwnedKey(alloc, &affected, key);
            for (writes.items[0..graph_write_count]) |write| try appendUniqueOwnedKey(alloc, &affected, write.key);
            for (affected.items) |edge_key| {
                if (reconciled.winners.map.get(edge_key)) |winner| {
                    const payload = try alloc.dupe(u8, winner.payload);
                    var payload_owned = true;
                    errdefer if (payload_owned) alloc.free(payload);
                    try upsertOwnedStoreWriteDupeKey(alloc, &writes, &write_positions, edge_key, payload);
                    payload_owned = false;
                } else if (!containsDeleteKey(deletes.items, edge_key)) {
                    try appendOwnedKey(alloc, &deletes, edge_key);
                    try appendUniqueOwnedKey(alloc, changed, edge_key);
                }
            }
            try upsertOwnedStoreWriteDupeKey(alloc, &writes, &write_positions, state_key, state_value);
            state_value_owned = false;
            for (reconciled.writes.items) |write| {
                const value = try alloc.dupe(u8, write.value);
                var value_owned = true;
                errdefer if (value_owned) alloc.free(value);
                try upsertOwnedStoreWriteDupeKey(alloc, &writes, &write_positions, write.key, value);
                value_owned = false;
            }
            for (reconciled.deletes.items) |key| {
                if (containsStoreWriteKey(writes.items, key) or containsDeleteKey(deletes.items, key)) continue;
                try appendOwnedKey(alloc, &deletes, key);
            }
            const mention_state_name = try mentionArtifactStateNameAlloc(alloc, source.artifact_name, cfg.resolution_artifact);
            defer alloc.free(mention_state_name);
            const mention_state_key = try graphAssetStateKeyAlloc(alloc, parsed_key.doc_key, index_name, mention_state_name);
            defer alloc.free(mention_state_key);
            if (try loadGraphAssetStateKeysAlloc(alloc, store, mention_state_key, generation)) |previous_mention_keys| {
                defer freeOwnedConstKeySlice(alloc, previous_mention_keys);
                for (previous_mention_keys) |previous_key| {
                    if (containsStoreWriteKey(mention_writes.items, previous_key)) continue;
                    try appendOwnedKey(alloc, &deletes, previous_key);
                    try appendUniqueOwnedKey(alloc, changed, previous_key);
                }
            }
            const mention_state_value = try encodeGraphAssetStateKeysAlloc(alloc, generation, mention_writes.items);
            var mention_state_value_owned = true;
            defer if (mention_state_value_owned) alloc.free(mention_state_value);
            try mention_writes.append(alloc, .{
                .key = try alloc.dupe(u8, mention_state_key),
                .value = mention_state_value,
            });
            mention_state_value_owned = false;

            if (writes.items.len > 0 or mention_writes.items.len > 0 or deletes.items.len > 0) {
                const combined = try concatKVPairSlices(alloc, writes.items, mention_writes.items);
                defer alloc.free(combined);
                var changed_batch = try filterChangedGraphMaterializationBatch(alloc, store, combined, deletes.items);
                defer changed_batch.deinit(alloc);
                if (changed_batch.writes.len > 0 or changed_batch.deletes.len > 0) {
                    try store.putBatch(changed_batch.writes, changed_batch.deletes);
                }
            }
        }

        pub fn mentionArtifactStateNameAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
            return try graph_state_name.mentionArtifactAlloc(alloc, source_artifact, resolution_artifact);
        }

        pub fn mentionEdgeMetadataJsonAlloc(alloc: Allocator, aggregate: MentionEdgeAggregate) ![]u8 {
            // Record the resolved DocRef target table so the endpoint can be hydrated
            // cross-table; same-table hydration ignores it. The mention artifact keys
            // are the durable evidence rollup behind the deduplicated entity edge.
            return try std.json.Stringify.valueAlloc(alloc, .{
                .target_table = aggregate.target_table,
                .mention_count = aggregate.mention_artifact_keys.items.len,
                .mention_artifact_keys = aggregate.mention_artifact_keys.items,
            }, .{});
        }

        pub fn mentionEdgeWritesFromResolutionAlloc(
            alloc: Allocator,
            index_name: []const u8,
            doc_key: []const u8,
            resolution_raw: []const u8,
            extraction_raw: ?[]const u8,
            mention_edge_type: []const u8,
            cfg: *const index_manager_mod.ResolverConfig,
        ) ![]types.GraphEdgeWrite {
            var parsed_resolution = resolver_lib.parseResolution(alloc, resolution_raw) catch return try alloc.alloc(types.GraphEdgeWrite, 0);
            defer parsed_resolution.deinit();
            var parsed_extraction: ?resolver_lib.ParsedEntities = if (extraction_raw) |raw|
                resolver_lib.parseExtractionEntities(alloc, raw) catch null
            else
                null;
            defer if (parsed_extraction) |*parsed| parsed.deinit();

            var aggregates = std.ArrayListUnmanaged(MentionEdgeAggregate).empty;
            defer {
                for (aggregates.items) |*aggregate| aggregate.deinit(alloc);
                aggregates.deinit(alloc);
            }
            for (parsed_resolution.entities) |entity| {
                if (!resolutionDecisionCreatesCanonicalEdge(entity.decision)) continue;
                if (entity.doc_ref.key.len == 0) continue;
                const mention_confidence = if (parsed_extraction) |parsed|
                    extractionConfidenceForLocalId(parsed.entities, entity.local_id) orelse entity.confidence
                else
                    entity.confidence;
                const mention_artifact_key = try resolutionMentionArtifactKeyAlloc(alloc, doc_key, cfg.source_artifact, cfg.resolution_artifact, entity.local_id);
                var mention_key_owned = true;
                errdefer if (mention_key_owned) alloc.free(mention_artifact_key);
                const aggregate = try findOrAppendMentionEdgeAggregate(alloc, &aggregates, entity.doc_ref.key, entity.doc_ref.table);
                try aggregate.appendMentionArtifactKey(alloc, mention_artifact_key);
                mention_key_owned = false;
                aggregate.mention_confidence = @max(aggregate.mention_confidence, mention_confidence);
            }

            var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
            errdefer freeGraphWrites(alloc, writes.items);
            for (aggregates.items) |aggregate| {
                const metadata = try mentionEdgeMetadataJsonAlloc(alloc, aggregate);
                errdefer alloc.free(metadata);
                try writes.append(alloc, .{
                    .index_name = try alloc.dupe(u8, index_name),
                    .source = try alloc.dupe(u8, doc_key),
                    .target = try alloc.dupe(u8, aggregate.target),
                    .edge_type = try alloc.dupe(u8, mention_edge_type),
                    .weight = cfg.fusedMentionWeight(aggregate.mention_confidence),
                    .metadata_json = metadata,
                });
            }
            return try writes.toOwnedSlice(alloc);
        }

        pub fn mentionEvidencePayloadAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            mention_artifact_key: []const u8,
            source_artifact_key: []const u8,
            resolution_key: []const u8,
            cfg: *const index_manager_mod.ResolverConfig,
            entity: resolver_lib.ResolvedEntity,
            extraction_entity: ?resolver_lib.ExtractedEntity,
        ) ![]u8 {
            const mention_label = if (extraction_entity) |source| source.label else entity.label;
            const mention_text = if (extraction_entity) |source| source.text else entity.surface_form;
            const mention_confidence = if (extraction_entity) |source| source.confidence else entity.confidence;
            return try std.json.Stringify.valueAlloc(alloc, .{
                ._schema = "antfly.resolution_mention.v1",
                ._parent_doc_key = doc_key,
                ._artifact_kind = "resolution_mention",
                ._artifact_key = mention_artifact_key,
                .source_artifact = cfg.source_artifact,
                .source_artifact_key = source_artifact_key,
                .resolution_artifact = cfg.resolution_artifact,
                .resolution_artifact_key = resolution_key,
                .resolver = cfg.name,
                .resolver_table = cfg.table,
                .config_generation = cfg.config_generation,
                .local_id = entity.local_id,
                .decision = decisionName(entity.decision),
                .confidence = entity.confidence,
                .canonical = .{
                    .table = entity.doc_ref.table,
                    .key = entity.doc_ref.key,
                    .name = entity.canonical_name,
                    .label = entity.label,
                },
                .mention = .{
                    .text = mention_text,
                    .label = mention_label,
                    .confidence = mention_confidence,
                },
            }, .{});
        }

        pub fn mentionGraphStateNameAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
            return try graph_state_name.mentionAlloc(alloc, source_artifact, resolution_artifact);
        }

        pub fn mergeGenerationFailureAttempts(
            previous_reason: types.ArtifactRepairReason,
            previous_sequence: u64,
            previous_error: []const u8,
            previous_attempts: u64,
            reason: types.ArtifactRepairReason,
            sequence: u64,
            generation_error: []const u8,
            generation_attempts: u64,
        ) u64 {
            const same_window = previous_reason == .enrichment_failed and
                reason == .enrichment_failed and
                previous_sequence == sequence and
                std.mem.eql(u8, previous_error, generation_error);
            return if (same_window) @max(previous_attempts, generation_attempts) else generation_attempts;
        }

        pub fn nextArtifactRepairIssueTimestamp(previous: u64, observed_now: u64) u64 {
            // The realtime clock may have coarse resolution. Keep this revision field
            // strictly monotonic per issue so concurrent publication can never compare
            // equal to the stale revision a repair captured before provider work.
            return @max(observed_now, previous +| 1);
        }

        pub fn pendingGeneratedCoverageDocKeysForIndexAlloc(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            index_name: []const u8,
            kind: types.IndexKind,
            refs: []const enrichment_types.GeneratedEnrichmentRef,
        ) ![]const []const u8 {
            var keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer keys.deinit(alloc);
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);

            for (refs) |ref| {
                const embedding_name = if (ref.embedding_name.len > 0) ref.embedding_name else ref.index_name;
                const targets_index = switch (kind) {
                    .dense_vector => ref.kind == .dense_embedding and
                        index_manager.denseIndexConsumesEmbedding(index_name, embedding_name),
                    .sparse_vector => ref.kind == .sparse_embedding and
                        index_manager.sparseIndexConsumesEmbedding(index_name, embedding_name),
                    .full_text, .graph => blk: {
                        if (ref.kind != .asset and ref.kind != .chunk_text) break :blk false;
                        const consumers = try index_manager.indexesDependingOnArtifact(alloc, ref.artifact_name);
                        defer {
                            for (consumers) |consumer| alloc.free(consumer);
                            alloc.free(consumers);
                        }
                        break :blk indexNameInSlice(index_name, consumers);
                    },
                    else => false,
                };
                if (!targets_index or seen.contains(ref.doc_key)) continue;
                try seen.put(alloc, ref.doc_key, {});
                try keys.append(alloc, ref.doc_key);
            }
            return try keys.toOwnedSlice(alloc);
        }

        pub fn precomputeAssetProducerBatchBytes(items: []const PrecomputeAssetProducerBatchItem) usize {
            var total: usize = 0;
            for (items) |item| total = addPrecomputeAssetProducerBytes(total, precomputeAssetProducerBatchItemBytes(item));
            return total;
        }

        pub fn precomputeAssetProducerBatchItemBytes(item: PrecomputeAssetProducerBatchItem) usize {
            return addPrecomputeAssetProducerBytes(
                addPrecomputeAssetProducerBytes(item.config_json.len, item.source_text.len),
                if (item.source_parts_json) |parts| parts.len else 0,
            );
        }

        pub fn precomputedCoverageOutcomePriority(outcome: DerivedCoverageOutcome) u8 {
            return switch (outcome) {
                .skipped => 0,
                .produced => 1,
                .terminal_failed => 2,
            };
        }

        pub fn precomputedEmbeddingCoverageOutcome(
            db: anytype,
            alloc: Allocator,
            request: enrichment_types.GeneratedEnrichmentRequest,
            artifact_writes: []const types.BatchWrite,
            produced: bool,
        ) !?DerivedCoverageOutcome {
            if (produced) return .produced;
            const chunk_artifact_name = request.artifact_name;
            if (chunk_artifact_name.len == 0 or
                !requestUsesPinnedMaterializedChunkArtifact(request) or
                request.upstream_artifact_name.len == 0) return .skipped;

            const manifest_key = try internal_keys.artifactNamedPrefixAlloc(
                alloc,
                request.doc_key,
                "asset",
                request.upstream_artifact_name,
            );
            defer alloc.free(manifest_key);
            var manifest: ?[]const u8 = null;
            var owned_manifest: ?[]u8 = null;
            defer if (owned_manifest) |value| alloc.free(value);
            var i = artifact_writes.len;
            while (i > 0) {
                i -= 1;
                const write = artifact_writes[i];
                if (std.mem.eql(u8, write.key, manifest_key)) {
                    manifest = write.value;
                    break;
                }
            }
            if (manifest == null) {
                owned_manifest = db.core.store.get(alloc, manifest_key) catch |err| switch (err) {
                    error.NotFound => return null,
                    else => return err,
                };
                manifest = owned_manifest.?;
            }
            return if (try enrichment_runtime_mod.documentExtractionEmptyCoverageIsTerminalFailure(alloc, manifest.?))
                .terminal_failed
            else
                .skipped;
        }

        pub fn prepareGraphContenderReconcilePage(
            alloc: Allocator,
            affected_edge_keys: []const []const u8,
            reconciled: *const GraphContenderReconcileResult,
            extra_writes: []const docstore_mod.KVPair,
            extra_deletes: []const []const u8,
        ) !GraphContenderMutation {
            var mutation = GraphContenderMutation{};
            errdefer mutation.deinit(alloc);
            var write_positions = StoreWritePositions.empty;
            defer write_positions.deinit(alloc);
            var delete_set = std.StringHashMapUnmanaged(void).empty;
            defer delete_set.deinit(alloc);

            for (affected_edge_keys) |edge_key| {
                if (reconciled.winners.map.get(edge_key)) |winner| {
                    const payload = try alloc.dupe(u8, winner.payload);
                    var payload_owned = true;
                    errdefer if (payload_owned) alloc.free(payload);
                    try upsertOwnedStoreWriteDupeKey(alloc, &mutation.writes, &write_positions, edge_key, payload);
                    payload_owned = false;
                } else {
                    try appendUniqueOwnedConstKeyIndexed(alloc, &mutation.deletes, &delete_set, edge_key);
                }
            }
            for (reconciled.writes.items) |write| {
                const value = try alloc.dupe(u8, write.value);
                var value_owned = true;
                errdefer if (value_owned) alloc.free(value);
                try upsertOwnedStoreWriteDupeKey(alloc, &mutation.writes, &write_positions, write.key, value);
                value_owned = false;
            }
            for (reconciled.deletes.items) |key| {
                if (write_positions.contains(key)) continue;
                try appendUniqueOwnedConstKeyIndexed(alloc, &mutation.deletes, &delete_set, key);
            }
            for (extra_writes) |write| {
                const value = try alloc.dupe(u8, write.value);
                var value_owned = true;
                errdefer if (value_owned) alloc.free(value);
                try upsertOwnedStoreWriteDupeKey(alloc, &mutation.writes, &write_positions, write.key, value);
                value_owned = false;
            }
            for (extra_deletes) |key| {
                if (write_positions.contains(key)) continue;
                try appendUniqueOwnedConstKeyIndexed(alloc, &mutation.deletes, &delete_set, key);
            }
            return mutation;
        }

        pub fn reconcileGlobalGraphEdgeWinner(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
            expected_generation: u64,
            ttl_duration_ns: u64,
            edge_key: []const u8,
            edge_changes: []const GraphContenderChange,
            pending_writes: []const docstore_mod.KVPair,
            pending: *const PendingGraphContenderOverlay,
            result: *GraphContenderReconcileResult,
            preserve_incoming_lifetimes: bool,
        ) !void {
            const prefix = try internal_keys.graphGlobalEdgeContenderEdgePrefixAlloc(alloc, index_name, expected_generation, edge_key);
            defer alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |value| alloc.free(value);

            const ScanState = struct {
                alloc: Allocator,
                index_name: []const u8,
                generation: u64,
                edge_key: []const u8,
                edge_changes: []const GraphContenderChange,
                pending: *const PendingGraphContenderOverlay,
                winners: *GraphEdgeWinners,

                fn scan(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    if (state.pending.delete_keys.contains(key) or state.pending.write_positions.contains(key)) return .@"continue";
                    const view = (try graph_edge_contender.decode(value, state.generation)) orelse return .@"continue";
                    if (!std.mem.eql(u8, view.edge_key, state.edge_key)) return error.InvalidGraphEdgeContender;
                    const expected_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(
                        state.alloc,
                        state.index_name,
                        state.generation,
                        state.edge_key,
                        view.source_priority,
                        view.state_key,
                    );
                    defer state.alloc.free(expected_key);
                    if (!std.mem.eql(u8, key, expected_key)) return error.InvalidGraphEdgeContender;
                    if (graphContenderStateChanged(state.edge_changes, view.state_key)) return .@"continue";
                    const authenticated = (try enrichment_artifact_codec.authenticateGraphEdgeGenerationAlloc(state.alloc, view.payload, state.generation)) orelse return error.InvalidGraphEdgeContender;
                    defer state.alloc.free(authenticated);
                    try considerGraphEdgeWinner(state.alloc, state.winners, state.edge_key, view.state_key, view.source_priority, authenticated);
                    // Global keys are ordered by priority and state identity. The first
                    // unchanged record is the best persisted candidate.
                    return .stop;
                }
            };
            // Persisted asset contenders retain their original 0..63 priorities.
            // The direct contributor has a dedicated key ordered after them, so probe
            // it exactly before the ordered asset scan can stop at its first winner.
            const direct_doc_key = (try internal_keys.decodeDocumentComponentAlloc(alloc, edge_key)) orelse return error.InvalidGraphEdgeArtifact;
            defer alloc.free(direct_doc_key);
            const direct_state_key = try internal_keys.graphDirectStateKeyAlloc(alloc, direct_doc_key, index_name);
            defer alloc.free(direct_state_key);
            const direct_contender_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, index_name, expected_generation, edge_key, @intCast(graph_mod.direct_source_priority), direct_state_key);
            defer alloc.free(direct_contender_key);
            if (!pending.delete_keys.contains(direct_contender_key) and
                !pending.write_positions.contains(direct_contender_key) and
                !graphContenderStateChanged(edge_changes, direct_state_key))
            {
                const direct_raw = store.get(alloc, direct_contender_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                defer if (direct_raw) |raw| alloc.free(raw);
                if (direct_raw) |raw| {
                    const direct = (try graph_edge_contender.decode(raw, expected_generation)) orelse return error.InvalidGraphEdgeContender;
                    if (!std.mem.eql(u8, direct.edge_key, edge_key) or
                        !std.mem.eql(u8, direct.state_key, direct_state_key) or
                        direct.source_priority != graph_mod.direct_source_priority) return error.InvalidGraphEdgeContender;
                    const authenticated = (try enrichment_artifact_codec.authenticateGraphEdgeGenerationAlloc(alloc, direct.payload, expected_generation)) orelse return error.InvalidGraphEdgeContender;
                    defer alloc.free(authenticated);
                    try considerGraphEdgeWinner(alloc, &result.winners, edge_key, direct_state_key, @intCast(graph_mod.direct_source_priority), authenticated);
                }
            }
            var scan_state = ScanState{
                .alloc = alloc,
                .index_name = index_name,
                .generation = expected_generation,
                .edge_key = edge_key,
                .edge_changes = edge_changes,
                .pending = pending,
                .winners = &result.winners,
            };
            try store.scanWithContext(prefix, if (upper) |value| value else "", .{}, &scan_state, ScanState.scan);

            // Reconciliation groups in one document batch are processed sequentially.
            // Fold earlier groups' not-yet-committed global contenders into this winner
            // selection so same-batch cross-document collisions are atomic.
            const pending_indexes: []const usize = if (pending.global_writes_by_edge.get(edge_key)) |indexes| indexes.items else &.{};
            for (pending_indexes) |pending_index| {
                const write = pending_writes[pending_index];
                const latest_position = pending.write_positions.get(write.key) orelse continue;
                if (latest_position != pending_index or pending.delete_keys.contains(write.key)) continue;
                if (!std.mem.startsWith(u8, write.key, prefix)) return error.InvalidGraphEdgeContender;
                const view = (try graph_edge_contender.decode(write.value, expected_generation)) orelse continue;
                if (!std.mem.eql(u8, view.edge_key, edge_key)) return error.InvalidGraphEdgeContender;
                if (graphContenderStateChanged(edge_changes, view.state_key)) continue;
                const expected_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, index_name, expected_generation, edge_key, view.source_priority, view.state_key);
                defer alloc.free(expected_key);
                if (!std.mem.eql(u8, write.key, expected_key)) return error.InvalidGraphEdgeContender;
                const authenticated = (try enrichment_artifact_codec.authenticateGraphEdgeGenerationAlloc(alloc, view.payload, expected_generation)) orelse return error.InvalidGraphEdgeContender;
                defer alloc.free(authenticated);
                try considerGraphEdgeWinner(alloc, &result.winners, edge_key, view.state_key, view.source_priority, authenticated);
            }

            for (edge_changes) |change| {
                const contender_key = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, index_name, expected_generation, edge_key, change.source_priority, change.state_key);
                if (ttl_duration_ns != 0) {
                    const previous_owned = store.get(alloc, contender_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    defer if (previous_owned) |value| alloc.free(value);
                    // An imported pending put is the afterimage. Its donor deadline
                    // cannot identify the receiver deadline that this page replaces.
                    const previous_raw: ?[]const u8 = if (preserve_incoming_lifetimes and previous_owned != null)
                        previous_owned.?
                    else if (pending.write_positions.get(contender_key)) |position|
                        pending_writes[position].value
                    else if (previous_owned) |value| value else null;
                    if (previous_raw) |raw| {
                        const previous = (try graph_edge_contender.decode(raw, expected_generation)) orelse return error.InvalidGraphEdgeContender;
                        var previous_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, previous.payload);
                        defer previous_edge.deinit(alloc);
                        if (previous_edge.ttl_created_ns == 0) return error.GraphEdgeTtlMigrationRequired;
                        const old_deadline = std.math.add(u64, previous_edge.ttl_created_ns, ttl_duration_ns) catch std.math.maxInt(u64);
                        try result.deletes.append(alloc, try graph_edge_ttl_expiration.indexKeyAlloc(alloc, old_deadline, contender_key));
                    }
                }
                if (change.payload) |payload| {
                    const authenticated = (try enrichment_artifact_codec.authenticateGraphEdgeGenerationAlloc(alloc, payload, expected_generation)) orelse return error.InvalidGraphEdgeContender;
                    defer alloc.free(authenticated);
                    var preserved_payload: ?[]u8 = null;
                    defer if (preserved_payload) |value| alloc.free(value);
                    var incoming_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, authenticated);
                    defer incoming_edge.deinit(alloc);
                    if (incoming_edge.ttl_created_ns != 0) {
                        const tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, edge_key, index_name, expected_generation, change.state_key);
                        defer alloc.free(tombstone_key);
                        const tombstone_raw = if (pending.delete_keys.contains(tombstone_key)) null else store.get(alloc, tombstone_key) catch |err| switch (err) {
                            error.NotFound => null,
                            else => return err,
                        };
                        defer if (tombstone_raw) |value| alloc.free(value);
                        const changed_source_revision = if (tombstone_raw) |raw| blk: {
                            const tombstone = try graph_edge_ttl_tombstone.Tombstone.decode(raw);
                            const digest = try graph_edge_ttl_tombstone.sourceDigest(alloc, authenticated);
                            if (std.mem.eql(u8, &digest, &tombstone.source_digest)) return error.InvalidGraphEdgeTtlTombstone;
                            break :blk true;
                        } else false;
                        if (changed_source_revision) try appendOwnedKey(alloc, &result.deletes, tombstone_key);
                        const lifetime_key = try internal_keys.graphEdgeTtlLifetimeKeyAlloc(alloc, edge_key, index_name, expected_generation, change.state_key);
                        var lifetime_key_owned = true;
                        errdefer if (lifetime_key_owned) alloc.free(lifetime_key);
                        const lifetime_raw = store.get(alloc, lifetime_key) catch |err| switch (err) {
                            error.NotFound => null,
                            else => return err,
                        };
                        defer if (lifetime_raw) |value| alloc.free(value);
                        var source_timestamp = incoming_edge.ttl_created_ns;
                        if (lifetime_raw != null and !changed_source_revision and !preserve_incoming_lifetimes) {
                            const raw = lifetime_raw.?;
                            if (raw.len != 8) return error.InvalidGraphEdgeTtlLifetime;
                            source_timestamp = std.mem.readInt(u64, raw[0..8], .big);
                            if (source_timestamp == 0) return error.InvalidGraphEdgeTtlLifetime;
                        } else if (!changed_source_revision and !preserve_incoming_lifetimes) {
                            const previous_owned = store.get(alloc, contender_key) catch |err| switch (err) {
                                error.NotFound => null,
                                else => return err,
                            };
                            defer if (previous_owned) |value| alloc.free(value);
                            const previous_raw: ?[]const u8 = if (previous_owned) |raw| raw else if (pending.write_positions.get(contender_key)) |position| pending_writes[position].value else null;
                            if (previous_raw) |raw| {
                                const prior = (try graph_edge_contender.decode(raw, expected_generation)) orelse return error.InvalidGraphEdgeContender;
                                if (!std.mem.eql(u8, prior.edge_key, edge_key) or !std.mem.eql(u8, prior.state_key, change.state_key) or prior.source_priority != change.source_priority)
                                    return error.InvalidGraphEdgeContender;
                                var prior_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, prior.payload);
                                defer prior_edge.deinit(alloc);
                                if (prior_edge.ttl_created_ns == 0) return error.GraphEdgeTtlMigrationRequired;
                                source_timestamp = prior_edge.ttl_created_ns;
                            }
                        }
                        var timestamp_buf: [8]u8 = undefined;
                        std.mem.writeInt(u64, &timestamp_buf, source_timestamp, .big);
                        const lifetime_value = try alloc.dupe(u8, &timestamp_buf);
                        var lifetime_value_owned = true;
                        errdefer if (lifetime_value_owned) alloc.free(lifetime_value);
                        try result.writes.append(alloc, .{ .key = lifetime_key, .value = lifetime_value });
                        lifetime_value_owned = false;
                        lifetime_key_owned = false;
                        if (source_timestamp != incoming_edge.ttl_created_ns) {
                            const header = try enrichment_artifact_codec.decodeHeader(authenticated);
                            preserved_payload = try enrichment_artifact_codec.encodeGraphEdgeWithTtlAlloc(
                                alloc,
                                if (header.flags.has_source_hash) header.source_hash else null,
                                expected_generation,
                                incoming_edge.weight,
                                incoming_edge.created_at,
                                incoming_edge.updated_at,
                                source_timestamp,
                                incoming_edge.metadata_json,
                            );
                        }
                    }
                    const source_payload = preserved_payload orelse authenticated;
                    const contender_value = try graph_edge_contender.encodeAlloc(alloc, expected_generation, change.source_priority, edge_key, change.state_key, source_payload);
                    if (ttl_duration_ns != 0) {
                        var final_edge = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, source_payload);
                        defer final_edge.deinit(alloc);
                        if (final_edge.ttl_created_ns == 0) return error.GraphEdgeTtlMigrationRequired;
                        const deadline = std.math.add(u64, final_edge.ttl_created_ns, ttl_duration_ns) catch std.math.maxInt(u64);
                        var contender_digest: [32]u8 = undefined;
                        GraphTtlSha256.hash(contender_value, &contender_digest, .{});
                        const candidate: GraphTtlCandidate = .{
                            .index_name = index_name,
                            .generation = expected_generation,
                            .edge_key = edge_key,
                            .state_key = change.state_key,
                            .source_priority = change.source_priority,
                            .deadline_ns = deadline,
                            .contender_digest = contender_digest,
                        };
                        const expiration_key = try graph_edge_ttl_expiration.indexKeyAlloc(alloc, deadline, contender_key);
                        var expiration_key_owned = true;
                        errdefer if (expiration_key_owned) alloc.free(expiration_key);
                        const expiration_value = try graph_edge_ttl_expiration.encodeAlloc(alloc, candidate);
                        var expiration_value_owned = true;
                        errdefer if (expiration_value_owned) alloc.free(expiration_value);
                        try result.writes.append(alloc, .{ .key = expiration_key, .value = expiration_value });
                        expiration_key_owned = false;
                        expiration_value_owned = false;
                    }
                    try result.writes.append(alloc, .{ .key = contender_key, .value = contender_value });
                    try considerGraphEdgeWinner(alloc, &result.winners, edge_key, change.state_key, change.source_priority, source_payload);
                } else {
                    try result.deletes.append(alloc, contender_key);
                    // Retiring any one contender -- direct or asset-derived -- must
                    // still fall back to the next-best surviving contender the scan
                    // above already selected; that is the whole point of priority
                    // ordering. "db graph untimed migration restores source order
                    // and direct contributor precedence" and its legacy-writes
                    // sibling both depend on an explicit direct-priority delete
                    // falling back to a surviving asset contender.
                }
            }
        }

        pub fn reconcileGraphEdgeContenders(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            doc_key: []const u8,
            index_name: []const u8,
            expected_generation: u64,
            ttl_duration_ns: u64,
            changes: *GraphContenderChanges,
            pending_writes: []const docstore_mod.KVPair,
            pending_deletes: []const []const u8,
        ) !GraphContenderReconcileResult {
            return reconcileGraphEdgeContendersWithLifetimePolicy(alloc, store, doc_key, index_name, expected_generation, ttl_duration_ns, changes, pending_writes, pending_deletes, false);
        }

        pub fn reconcileGraphEdgeContendersWithLifetimePolicy(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            doc_key: []const u8,
            index_name: []const u8,
            expected_generation: u64,
            ttl_duration_ns: u64,
            changes: *GraphContenderChanges,
            pending_writes: []const docstore_mod.KVPair,
            pending_deletes: []const []const u8,
            preserve_incoming_lifetimes: bool,
        ) !GraphContenderReconcileResult {
            var pending = try PendingGraphContenderOverlay.init(alloc, pending_writes, pending_deletes, expected_generation);
            defer pending.deinit(alloc);
            return reconcileGraphEdgeContendersWithOverlay(alloc, store, doc_key, index_name, expected_generation, ttl_duration_ns, changes, pending_writes, &pending, preserve_incoming_lifetimes);
        }

        pub fn reconcileGraphEdgeContendersWithOverlay(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            doc_key: []const u8,
            index_name: []const u8,
            expected_generation: u64,
            ttl_duration_ns: u64,
            changes: *GraphContenderChanges,
            pending_writes: []const docstore_mod.KVPair,
            pending: *const PendingGraphContenderOverlay,
            preserve_incoming_lifetimes: bool,
        ) !GraphContenderReconcileResult {
            var result = GraphContenderReconcileResult{};
            errdefer result.deinit(alloc);

            const count_key = try internal_keys.graphEdgeContenderCountKeyAlloc(alloc, doc_key, index_name);
            defer alloc.free(count_key);
            // Counts and membership must observe the same batch-local view. Primary
            // deletion retires every local witness before direct-source reconciliation;
            // reading the old count here would recreate nonempty debt for a dead row.
            const overlay_count = pending.write_positions.contains(count_key) or pending.delete_keys.contains(count_key);
            const owned_count = if (!overlay_count) store.get(alloc, count_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            } else null;
            defer if (owned_count) |raw| alloc.free(raw);
            const raw_count = if (pending.write_positions.get(count_key)) |pos| pending_writes[pos].value else owned_count;
            const count_present = raw_count != null and (try graph_edge_contender.decodeVisibleCount(raw_count.?, expected_generation)) != null;
            result.visible_count = if (raw_count) |raw| (try graph_edge_contender.decodeVisibleCount(raw, expected_generation)) orelse 0 else 0;
            var saw_current_contender = false;
            var bulk_existing_edges = std.StringHashMapUnmanaged(void).empty;
            defer bulk_existing_edges.deinit(alloc);
            var bulk_surviving_edges = std.StringHashMapUnmanaged(void).empty;
            defer bulk_surviving_edges.deinit(alloc);
            const bulk_scan = count_present and graph_edge_contender.shouldBulkScan(changes.count(), result.visible_count);
            if (bulk_scan) {
                const index_prefix = try internal_keys.graphEdgeContenderIndexPrefixAlloc(alloc, doc_key, index_name);
                defer alloc.free(index_prefix);
                const existing = try store.scanPrefix(alloc, index_prefix);
                defer docstore_mod.DocStore.freeResults(alloc, existing);
                for (existing) |contender| {
                    if (std.mem.eql(u8, contender.key, count_key)) continue;
                    if (internal_keys.isGraphEdgeTtlLifetimeKey(contender.key) or internal_keys.isGraphEdgeTtlTombstoneKey(contender.key)) continue;
                    const view = (try graph_edge_contender.decode(contender.value, expected_generation)) orelse continue;
                    const edge_key = changes.getKey(view.edge_key) orelse continue;
                    const edge_changes = changes.get(edge_key).?;
                    const expected_key = try internal_keys.graphEdgeContenderKeyAlloc(alloc, doc_key, index_name, edge_key, view.state_key);
                    defer alloc.free(expected_key);
                    if (!std.mem.eql(u8, contender.key, expected_key)) return error.InvalidGraphEdgeContender;
                    const replaced = pending.delete_keys.contains(contender.key) or pending.write_positions.contains(contender.key);
                    if (overlay_count and replaced) continue;
                    saw_current_contender = true;
                    try bulk_existing_edges.put(alloc, edge_key, {});
                    if (replaced or graphContenderStateChanged(edge_changes.items, view.state_key)) continue;
                    try bulk_surviving_edges.put(alloc, edge_key, {});
                }
            }

            var it = changes.iterator();
            while (it.next()) |entry| {
                const edge_key = entry.key_ptr.*;
                const edge_changes = entry.value_ptr.items;
                var existed_before = bulk_existing_edges.contains(edge_key);
                var exists_after = bulk_surviving_edges.contains(edge_key);
                if (!bulk_scan and count_present) {
                    const prefix = try internal_keys.graphEdgeContenderEdgePrefixAlloc(alloc, doc_key, index_name, edge_key);
                    defer alloc.free(prefix);
                    const existing = try store.scanPrefix(alloc, prefix);
                    defer docstore_mod.DocStore.freeResults(alloc, existing);
                    for (existing) |contender| {
                        const view = (try graph_edge_contender.decode(contender.value, expected_generation)) orelse continue;
                        if (!std.mem.eql(u8, view.edge_key, edge_key)) return error.InvalidGraphEdgeContender;
                        const expected_key = try internal_keys.graphEdgeContenderKeyAlloc(alloc, doc_key, index_name, edge_key, view.state_key);
                        defer alloc.free(expected_key);
                        if (!std.mem.eql(u8, contender.key, expected_key)) return error.InvalidGraphEdgeContender;
                        const replaced = pending.delete_keys.contains(contender.key) or pending.write_positions.contains(contender.key);
                        if (overlay_count and replaced) continue;
                        saw_current_contender = true;
                        existed_before = true;
                        if (replaced or graphContenderStateChanged(edge_changes, view.state_key)) continue;
                        exists_after = true;
                    }
                }

                if (pending.local_writes_by_edge.get(edge_key)) |positions| for (positions.items) |pos| {
                    const write = pending_writes[pos];
                    const view = (try graph_edge_contender.decode(write.value, expected_generation)) orelse continue;
                    const expected_key = try internal_keys.graphEdgeContenderKeyAlloc(alloc, doc_key, index_name, edge_key, view.state_key);
                    defer alloc.free(expected_key);
                    // Other documents/indexes can contribute to the same logical edge.
                    if (!std.mem.eql(u8, write.key, expected_key)) continue;
                    if (overlay_count) existed_before = true;
                    if (!graphContenderStateChanged(edge_changes, view.state_key)) exists_after = true;
                };

                for (edge_changes) |change| {
                    const contender_key = try internal_keys.graphEdgeContenderKeyAlloc(alloc, doc_key, index_name, edge_key, change.state_key);
                    if (change.payload != null) {
                        const contender_value = try graph_edge_contender.encodeAlloc(alloc, expected_generation, change.source_priority, edge_key, change.state_key, "");
                        try result.writes.append(alloc, .{ .key = contender_key, .value = contender_value });
                        exists_after = true;
                    } else {
                        try result.deletes.append(alloc, contender_key);
                    }
                }

                if (!existed_before and exists_after) {
                    result.visible_count = std.math.add(usize, result.visible_count, 1) catch return error.ResourceLimitExceeded;
                } else if (existed_before and !exists_after) {
                    if (result.visible_count == 0) return error.InvalidGraphEdgeContenderCount;
                    result.visible_count -= 1;
                }
            }
            if (saw_current_contender and !count_present) return error.InvalidGraphEdgeContenderCount;

            const encoded_count = try graph_edge_contender.encodeVisibleCount(expected_generation, result.visible_count);
            try result.writes.append(alloc, .{
                .key = try alloc.dupe(u8, count_key),
                .value = try alloc.dupe(u8, &encoded_count),
            });

            // The document-local sidecars above provide scalable bulk accounting and
            // deletion. Winner selection must use the logical-edge-global sidecars so
            // two documents that map to the same source/type/target cannot clobber one
            // another. Reset the local winner projection before selecting globally.
            result.winners.deinit(alloc);
            result.winners = .{};
            var global_it = changes.iterator();
            while (global_it.next()) |entry| {
                try reconcileGlobalGraphEdgeWinner(
                    alloc,
                    store,
                    index_name,
                    expected_generation,
                    ttl_duration_ns,
                    entry.key_ptr.*,
                    entry.value_ptr.items,
                    pending_writes,
                    pending,
                    &result,
                    preserve_incoming_lifetimes,
                );
            }
            return result;
        }

        pub fn reconcileSingleGraphStateContenders(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            doc_key: []const u8,
            index_name: []const u8,
            state_key: []const u8,
            previous_keys: []const []const u8,
            graph_writes: []const docstore_mod.KVPair,
            sources: []const index_manager_mod.GraphArtifactSource,
            expected_generation: u64,
            ttl_duration_ns: u64,
        ) !GraphContenderReconcileResult {
            var changes = GraphContenderChanges.empty;
            defer {
                var it = changes.valueIterator();
                while (it.next()) |items| items.deinit(alloc);
                changes.deinit(alloc);
            }
            const state_prefix = try internal_keys.graphAssetStateIndexPrefixAlloc(alloc, doc_key, index_name);
            defer alloc.free(state_prefix);
            const source_priority = try graphStateSourcePriorityAlloc(alloc, state_key, state_prefix, sources) orelse return error.InvalidGraphStateName;
            for (previous_keys) |edge_key| {
                try appendGraphContenderChange(alloc, &changes, edge_key, state_key, source_priority, null);
            }
            for (graph_writes) |write| {
                const tombstone_key = try internal_keys.graphEdgeTtlTombstoneKeyAlloc(alloc, write.key, index_name, expected_generation, state_key);
                defer alloc.free(tombstone_key);
                const tombstone_raw = store.get(alloc, tombstone_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                defer if (tombstone_raw) |raw| alloc.free(raw);
                const suppressed = if (tombstone_raw) |raw| blk: {
                    const tombstone = try graph_edge_ttl_tombstone.Tombstone.decode(raw);
                    const digest = try graph_edge_ttl_tombstone.sourceDigest(alloc, write.value);
                    break :blk std.mem.eql(u8, &digest, &tombstone.source_digest);
                } else false;
                try appendGraphContenderChange(alloc, &changes, write.key, state_key, source_priority, if (suppressed) null else write.value);
            }
            return try reconcileGraphEdgeContenders(alloc, store, doc_key, index_name, expected_generation, ttl_duration_ns, &changes, &.{}, &.{});
        }

        pub fn recordArtifactRepairIssueContext(
            ctx: *const AsyncContext,
            artifact_kind: types.ArtifactRepairKind,
            index_name: []const u8,
            doc_key: []const u8,
            parent_doc_key: []const u8,
            unit_id: []const u8,
            source_artifact_name: []const u8,
            artifact_name: []const u8,
            artifact_key: []const u8,
            chunk_id: ?u32,
            sequence: u64,
            reason: types.ArtifactRepairReason,
        ) !void {
            return recordArtifactRepairIssueContextDetailed(ctx, artifact_kind, index_name, doc_key, parent_doc_key, unit_id, artifact_name, source_artifact_name, artifact_name, artifact_key, chunk_id, sequence, reason, 0, "");
        }

        pub fn recordArtifactRepairIssueContextDetailed(
            ctx: *const AsyncContext,
            artifact_kind: types.ArtifactRepairKind,
            index_name: []const u8,
            doc_key: []const u8,
            parent_doc_key: []const u8,
            unit_id: []const u8,
            index_source_artifact_name: []const u8,
            source_artifact_name: []const u8,
            artifact_name: []const u8,
            artifact_key: []const u8,
            chunk_id: ?u32,
            sequence: u64,
            reason: types.ArtifactRepairReason,
            generation_attempts: u64,
            generation_error: []const u8,
        ) !void {
            const kind_name = @tagName(artifact_kind);
            const artifact_key_hex = if (artifact_key.len > 0)
                try bytesToHexAlloc(ctx.alloc, artifact_key)
            else
                try ctx.alloc.dupe(u8, "");
            defer ctx.alloc.free(artifact_key_hex);
            const issue_id = try artifactRepairIssueIdAlloc(ctx.alloc, .{
                .artifact_kind = artifact_kind,
                .index_name = index_name,
                .doc_key = doc_key,
                .parent_doc_key = parent_doc_key,
                .unit_id = unit_id,
                .source_artifact_name = source_artifact_name,
                .artifact_name = artifact_name,
                .artifact_key = artifact_key_hex,
                .chunk_id = chunk_id,
            });
            defer ctx.alloc.free(issue_id);
            const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(ctx.alloc, index_name, kind_name, issue_id);
            defer ctx.alloc.free(issue_key);

            const mutable_ctx = @constCast(ctx);
            lockAtomicWithBackoff(&mutable_ctx.artifact_repair_issue_mutex);
            defer mutable_ctx.artifact_repair_issue_mutex.unlock();

            const now_ns = currentTimeNs();
            const existing = try loadArtifactRepairIssueFromStoreByKey(ctx.alloc, ctx.store, issue_key);
            const existing_was_pending = if (existing) |loaded| loaded.reason == .enrichment_failed else false;
            const previous_terminal_sequence: ?u64 = if (existing) |loaded|
                if (loaded.reason == .enrichment_failed) loaded.sequence else null
            else
                null;
            var issue = if (existing) |loaded|
                loaded
            else
                types.ArtifactRepairIssue{
                    .artifact_kind = artifact_kind,
                    .index_name = try ctx.alloc.dupe(u8, index_name),
                    .doc_key = try ctx.alloc.dupe(u8, doc_key),
                    .parent_doc_key = try ctx.alloc.dupe(u8, parent_doc_key),
                    .unit_id = try ctx.alloc.dupe(u8, unit_id),
                    .index_source_artifact_name = try ctx.alloc.dupe(u8, index_source_artifact_name),
                    .source_artifact_name = try ctx.alloc.dupe(u8, source_artifact_name),
                    .artifact_name = try ctx.alloc.dupe(u8, artifact_name),
                    .artifact_key = if (artifact_key_hex.len > 0) try ctx.alloc.dupe(u8, artifact_key_hex) else "",
                    .chunk_id = chunk_id,
                    .repairable = artifactRepairReasonHasAutomatedReprocessor(artifact_kind, reason),
                    .first_seen_ns = now_ns,
                };
            defer issue.deinit(ctx.alloc);

            const merged_generation_attempts = mergeGenerationFailureAttempts(
                issue.reason,
                issue.sequence,
                issue.generation_error,
                issue.generation_attempts,
                reason,
                sequence,
                generation_error,
                generation_attempts,
            );
            issue.artifact_kind = artifact_kind;
            issue.sequence = sequence;
            issue.reason = reason;
            issue.generation_attempts = merged_generation_attempts;
            issue.chunk_id = chunk_id;
            try applyArtifactRepairability(ctx.alloc, &issue);
            issue.last_seen_ns = nextArtifactRepairIssueTimestamp(issue.last_seen_ns, now_ns);
            if (issue.artifact_key.len == 0 and artifact_key_hex.len > 0) {
                issue.artifact_key = try ctx.alloc.dupe(u8, artifact_key_hex);
            }
            if (issue.parent_doc_key.len == 0 and parent_doc_key.len > 0) {
                issue.parent_doc_key = try ctx.alloc.dupe(u8, parent_doc_key);
            }
            if (issue.unit_id.len == 0 and unit_id.len > 0) {
                issue.unit_id = try ctx.alloc.dupe(u8, unit_id);
            }
            if (issue.index_source_artifact_name.len == 0 and index_source_artifact_name.len > 0) {
                issue.index_source_artifact_name = try ctx.alloc.dupe(u8, index_source_artifact_name);
            }
            if (issue.source_artifact_name.len == 0 and source_artifact_name.len > 0) {
                issue.source_artifact_name = try ctx.alloc.dupe(u8, source_artifact_name);
            }
            if (!std.mem.eql(u8, issue.generation_error, generation_error)) {
                const owned_generation_error = if (generation_error.len > 0)
                    try ctx.alloc.dupe(u8, generation_error)
                else
                    "";
                if (issue.generation_error.len > 0) ctx.alloc.free(@constCast(issue.generation_error));
                issue.generation_error = owned_generation_error;
            }

            const completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(ctx.alloc, kind_name, issue_id);
            defer ctx.alloc.free(completion_key);
            if (reason == .enrichment_failed) {
                var marker_deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer marker_deletes.deinit(ctx.alloc);
                var owned_marker_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
                defer {
                    for (owned_marker_delete_keys.items) |key| ctx.alloc.free(@constCast(key));
                    owned_marker_delete_keys.deinit(ctx.alloc);
                }
                var terminal_marker = try EnrichmentTerminalFailureMarkerWrite.init(
                    ctx.alloc,
                    ctx.store,
                    issue_key,
                    previous_terminal_sequence,
                    sequence,
                    &marker_deletes,
                    &owned_marker_delete_keys,
                );
                defer terminal_marker.deinit(ctx.alloc);
                var completion = (try loadArtifactRepairCompletionStateFromStore(ctx.alloc, ctx.store, completion_key)) orelse ArtifactRepairCompletionState{
                    .pending_issues = @intFromBool(existing_was_pending),
                };
                if (existing_was_pending) completion.pending_issues = @max(completion.pending_issues, 1);
                completion.epoch +%= 1;
                if (completion.epoch == 0) completion.epoch = 1;
                completion.completed_sequence = 0;
                if (!existing_was_pending) completion.pending_issues +|= 1;
                var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
                encodeArtifactRepairCompletionState(&encoded_completion, completion);
                try saveArtifactRepairIssueToStoreWithSummary(
                    ctx.alloc,
                    ctx.store,
                    issue_key,
                    issue,
                    existing == null,
                    &.{
                        .{ .key = completion_key, .value = &encoded_completion },
                        .{ .key = &internal_keys.enrichment_terminal_failure_generation_counter_key, .value = &terminal_marker.generation_counter_value },
                        .{ .key = terminal_marker.generation_key, .value = &terminal_marker.generation_value },
                        // The primary key is ordered by source sequence for exact
                        // visibility checks. The reverse key owns its lifecycle and
                        // points back to the primary key for bounded retirement.
                        .{ .key = terminal_marker.marker.sequence_key, .value = terminal_marker.marker_value },
                        .{ .key = terminal_marker.marker.issue_key, .value = terminal_marker.marker.sequence_key },
                    },
                    marker_deletes.items,
                );
            } else {
                // A non-enrichment diagnosis supersedes the shared success fence. It
                // must be invalidated in the same metadata transaction as the issue so
                // neither crashes nor concurrent repair can leave a stale marker.
                var marker_deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer marker_deletes.deinit(ctx.alloc);
                var owned_marker_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
                defer {
                    for (owned_marker_delete_keys.items) |key| ctx.alloc.free(@constCast(key));
                    owned_marker_delete_keys.deinit(ctx.alloc);
                }
                try appendEnrichmentTerminalFailureMarkerDeletesForIssue(
                    ctx.alloc,
                    ctx.store,
                    issue_key,
                    &marker_deletes,
                    &owned_marker_delete_keys,
                    true,
                );
                try marker_deletes.append(ctx.alloc, completion_key);
                try saveArtifactRepairIssueToStoreWithSummary(
                    ctx.alloc,
                    ctx.store,
                    issue_key,
                    issue,
                    existing == null,
                    &.{},
                    marker_deletes.items,
                );
            }
            if (ctx.repair_issue_counter) |counter| _ = counter.fetchAdd(1, .monotonic);
        }

        pub fn recordArtifactRepairIssueContextForIndexSource(
            ctx: *const AsyncContext,
            artifact_kind: types.ArtifactRepairKind,
            index_name: []const u8,
            doc_key: []const u8,
            parent_doc_key: []const u8,
            unit_id: []const u8,
            index_source_artifact_name: []const u8,
            source_artifact_name: []const u8,
            artifact_name: []const u8,
            artifact_key: []const u8,
            chunk_id: ?u32,
            sequence: u64,
            reason: types.ArtifactRepairReason,
        ) !void {
            return recordArtifactRepairIssueContextDetailed(ctx, artifact_kind, index_name, doc_key, parent_doc_key, unit_id, index_source_artifact_name, source_artifact_name, artifact_name, artifact_key, chunk_id, sequence, reason, 0, "");
        }

        pub fn recordArtifactRepairIssueForRefContext(
            ctx: *const AsyncContext,
            index_name: []const u8,
            artifact_ref: types.ArtifactRef,
            artifact_key: []const u8,
            sequence: u64,
            reason: types.ArtifactRepairReason,
        ) !void {
            const unit_id = artifact_ref.unit_id orelse if (artifact_ref.source) |source| source.unit_id orelse "" else "";
            const parent_doc_key = if (unit_id.len > 0) artifact_ref.document_id else "";
            const source_artifact_name = if (artifact_ref.source) |source| source.name else "";
            try recordArtifactRepairIssueContext(
                ctx,
                repairKindFromArtifactKind(artifact_ref.kind),
                index_name,
                artifact_ref.document_id,
                parent_doc_key,
                unit_id,
                source_artifact_name,
                artifact_ref.name,
                artifact_key,
                artifact_ref.chunk_id,
                sequence,
                reason,
            );
        }

        pub fn recordEmbeddingArtifactRepairIssueContext(
            ctx: *const AsyncContext,
            index_name: []const u8,
            artifact_key: []const u8,
            sequence: u64,
            reason: types.ArtifactRepairReason,
        ) !void {
            const mutable_ctx = @constCast(ctx);
            lockAtomicWithBackoff(&mutable_ctx.artifact_repair_issue_mutex);
            defer mutable_ctx.artifact_repair_issue_mutex.unlock();
            var identity = (try artifact_ids.decodeEmbeddingArtifactIdentityAlloc(ctx.alloc, artifact_key)) orelse return;
            defer identity.deinit(ctx.alloc);

            const artifact_key_hex = try bytesToHexAlloc(ctx.alloc, artifact_key);
            defer ctx.alloc.free(artifact_key_hex);
            const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(ctx.alloc, index_name, "embedding", artifact_key_hex);
            defer ctx.alloc.free(issue_key);

            const now_ns = currentTimeNs();
            const existing = try loadArtifactRepairIssueFromStoreByKey(ctx.alloc, ctx.store, issue_key);
            var issue = if (existing) |loaded|
                loaded
            else
                types.ArtifactRepairIssue{
                    .artifact_kind = .embedding,
                    .index_name = try ctx.alloc.dupe(u8, index_name),
                    .doc_key = try ctx.alloc.dupe(u8, identity.doc_key),
                    .parent_doc_key = try ctx.alloc.dupe(u8, identity.parent_doc_key orelse ""),
                    .unit_id = try ctx.alloc.dupe(u8, identity.unit_id orelse ""),
                    .source_artifact_name = try ctx.alloc.dupe(u8, identity.source_artifact_name orelse ""),
                    .artifact_name = try ctx.alloc.dupe(u8, identity.embedding_name),
                    .artifact_key = try ctx.alloc.dupe(u8, artifact_key_hex),
                    .chunk_id = identity.chunk_id,
                    .repairable = artifactRepairReasonHasAutomatedReprocessor(.embedding, reason),
                    .first_seen_ns = now_ns,
                };
            defer issue.deinit(ctx.alloc);

            issue.sequence = sequence;
            issue.reason = reason;
            issue.chunk_id = identity.chunk_id;
            try applyArtifactRepairability(ctx.alloc, &issue);
            issue.last_seen_ns = nextArtifactRepairIssueTimestamp(issue.last_seen_ns, now_ns);
            if (issue.artifact_key.len == 0) {
                issue.artifact_key = try ctx.alloc.dupe(u8, artifact_key_hex);
            }
            if (issue.parent_doc_key.len == 0) {
                issue.parent_doc_key = try ctx.alloc.dupe(u8, identity.parent_doc_key orelse "");
            }
            if (issue.unit_id.len == 0) {
                issue.unit_id = try ctx.alloc.dupe(u8, identity.unit_id orelse "");
            }
            if (issue.source_artifact_name.len == 0) {
                issue.source_artifact_name = try ctx.alloc.dupe(u8, identity.source_artifact_name orelse "");
            }

            const completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(ctx.alloc, "embedding", artifact_key_hex);
            defer ctx.alloc.free(completion_key);
            try saveArtifactRepairIssueToStoreWithSummary(
                ctx.alloc,
                ctx.store,
                issue_key,
                issue,
                existing == null,
                &.{},
                &.{completion_key},
            );
            if (ctx.repair_issue_counter) |counter| _ = counter.fetchAdd(1, .monotonic);
        }

        pub fn recordGraphResolutionResourceLimitForRepair(
            alloc: Allocator,
            options: GraphMaterializationOptions,
            index_name: []const u8,
            source: index_manager_mod.GraphArtifactSource,
            resolution_key: []const u8,
        ) !bool {
            const repair_ctx = options.repair_ctx orelse return false;
            const parsed = (try internal_keys.parseResolutionArtifactKeyAlloc(alloc, resolution_key)) orelse return false;
            defer alloc.free(parsed.doc_key);
            defer alloc.free(parsed.artifact_name);
            try recordArtifactRepairIssueContextForIndexSource(
                repair_ctx,
                .graph,
                index_name,
                parsed.doc_key,
                "",
                "",
                source.artifact_name,
                source.artifact_name,
                parsed.artifact_name,
                resolution_key,
                null,
                options.sequence,
                .resource_limit_exceeded,
            );
            return true;
        }

        pub fn recordGraphResourceLimitForRepair(
            options: GraphMaterializationOptions,
            index_name: []const u8,
            artifact_ref: types.ArtifactRef,
            artifact_key: []const u8,
        ) !bool {
            const repair_ctx = options.repair_ctx orelse return false;
            try recordArtifactRepairIssueForRefContext(
                repair_ctx,
                index_name,
                artifact_ref,
                artifact_key,
                options.sequence,
                .resource_limit_exceeded,
            );
            return true;
        }

        pub fn relationalColumns(self: anytype) ?[]const schema_mod.RelationalColumn {
            const schema = self.core.schema orelse return null;
            if (schema.storage_mode != .relational) return null;
            return schema.relational_columns;
        }

        pub fn removePendingDeleteKey(
            alloc: Allocator,
            delete_keys: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]u8),
            key: []const u8,
        ) void {
            var i: usize = 0;
            while (i < delete_keys.items.len) {
                if (std.mem.eql(u8, delete_keys.items[i], key)) {
                    _ = delete_keys.orderedRemove(i);
                } else {
                    i += 1;
                }
            }
            i = 0;
            while (i < owned_delete_keys.items.len) {
                if (std.mem.eql(u8, owned_delete_keys.items[i], key)) {
                    alloc.free(owned_delete_keys.orderedRemove(i));
                } else {
                    i += 1;
                }
            }
        }

        pub fn renderSourceParts(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
            max_media_parts: ?usize,
        ) !?[]template_mod.ContentPart {
            if (request.source_template.len == 0) return null;
            const parts = renderSourceTemplateParts(alloc, db, request.source_template, doc_value, max_media_parts) catch |err| switch (err) {
                error.OutOfMemory, error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                else => return null,
            };
            if (parts.len == 0) {
                template_mod.freeContentParts(alloc, parts);
                return null;
            }
            return parts;
        }

        pub fn renderSourcePartsJson(
            alloc: Allocator,
            db: anytype,
            doc_value: []const u8,
            request: enrichment_types.GeneratedEnrichmentRequest,
        ) !?[]u8 {
            const parts = try renderSourceParts(alloc, db, doc_value, request, null) orelse return null;
            defer template_mod.freeContentParts(alloc, parts);
            return try contentPartsJsonAlloc(alloc, parts);
        }

        pub fn renderSourceTemplateParts(
            alloc: Allocator,
            db: anytype,
            template_source: []const u8,
            doc_value: []const u8,
            max_media_parts: ?usize,
        ) ![]template_mod.ContentPart {
            if (comptime @hasDecl(template_remote, "renderJsonToPartsWithConfig")) {
                return try template_remote.renderJsonToPartsWithConfig(
                    alloc,
                    template_source,
                    doc_value,
                    remoteRenderConfig(db, max_media_parts),
                );
            }
            return try template_remote.renderJsonToParts(alloc, template_source, doc_value);
        }

        pub fn renderSourceTemplateText(
            alloc: Allocator,
            db: anytype,
            template_source: []const u8,
            doc_value: []const u8,
        ) ![]const u8 {
            if (comptime @hasDecl(template_remote, "renderJsonToValidatedTextWithConfig")) {
                return try template_remote.renderJsonToValidatedTextWithConfig(
                    alloc,
                    template_source,
                    doc_value,
                    remoteRenderConfig(db, null),
                );
            }
            return try template_remote.renderJsonToTextWithConfig(
                alloc,
                template_source,
                doc_value,
                remoteRenderConfig(db, null),
            );
        }

        pub fn repairKindFromArtifactKind(kind: types.ArtifactKind) types.ArtifactRepairKind {
            return switch (kind) {
                .asset => .asset,
                .chunk => .chunk,
                .embedding => .embedding,
            };
        }

        pub fn replaySourceDocumentExists(
            ctx: *const AsyncContext,
            source_doc_key: []const u8,
        ) !bool {
            const store_key = try replayDocumentStoreKeyAlloc(ctx.alloc, source_doc_key, ctx.relational_base_rows);
            defer ctx.alloc.free(store_key);
            const raw = ctx.store.get(ctx.alloc, store_key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            ctx.alloc.free(raw);
            return true;
        }

        pub fn requestArtifactName(request: enrichment_types.GeneratedEnrichmentRequest) []const u8 {
            return if (request.artifact_name.len > 0) request.artifact_name else request.index_name;
        }

        pub fn requestEmbeddingName(request: enrichment_types.GeneratedEnrichmentRequest) []const u8 {
            return if (request.embedding_name.len > 0) request.embedding_name else request.index_name;
        }

        pub fn requestHasChunking(request: enrichment_types.GeneratedEnrichmentRequest) bool {
            return request.chunk_size > 0 or request.chunker_json.len > 0;
        }

        pub fn requestUsesChunkSource(request: enrichment_types.GeneratedEnrichmentRequest) bool {
            return request.input_kind != .document;
        }

        pub fn requestUsesPinnedMaterializedChunkArtifact(request: enrichment_types.GeneratedEnrichmentRequest) bool {
            return request.input_kind == .materialized_chunks;
        }

        pub fn resolutionDecisionCreatesCanonicalEdge(decision: resolver_lib.Decision) bool {
            return switch (decision) {
                .new, .match => true,
                .review => false,
            };
        }

        pub fn resolutionMentionArtifactKeyAlloc(
            alloc: Allocator,
            doc_key: []const u8,
            source_artifact: []const u8,
            resolution_artifact: []const u8,
            local_id: []const u8,
        ) ![]u8 {
            const name = try resolutionMentionArtifactNameAlloc(alloc, source_artifact, resolution_artifact, local_id);
            defer alloc.free(name);
            return try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", name);
        }

        pub fn resolutionMentionArtifactNameAlloc(
            alloc: Allocator,
            source_artifact: []const u8,
            resolution_artifact: []const u8,
            local_id: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "_resolution_mention\x1f{s}\x1f{s}\x1f{s}", .{ source_artifact, resolution_artifact, local_id });
        }

        pub fn resolutionMentionStateKeysForGraphSourceAlloc(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            doc_key: []const u8,
            index_name: []const u8,
            source: index_manager_mod.GraphArtifactSource,
        ) ![][]const u8 {
            if (source.mention_edge_type.len == 0) return try alloc.alloc([]const u8, 0);
            const generation = (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).config.coverage_generation;

            var protected = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (protected.items) |key| alloc.free(@constCast(key));
                protected.deinit(alloc);
            }

            for (index_manager.resolvers.items) |cfg| {
                if (!std.mem.eql(u8, cfg.source_artifact, source.artifact_name)) continue;

                const state_name = try mentionGraphStateNameAlloc(alloc, source.artifact_name, cfg.resolution_artifact);
                defer alloc.free(state_name);
                const state_key = try graphAssetStateKeyAlloc(alloc, doc_key, index_name, state_name);
                defer alloc.free(state_key);

                const state_keys = try loadGraphAssetStateKeysAlloc(alloc, store, state_key, generation) orelse continue;
                defer freeOwnedConstKeySlice(alloc, state_keys);
                for (state_keys) |key| {
                    try appendOwnedKey(alloc, &protected, key);
                }
            }

            return try protected.toOwnedSlice(alloc);
        }

        pub fn resolutionOwningAssetArtifactKeyAlloc(
            alloc: Allocator,
            index_manager: *index_manager_mod.IndexManager,
            resolution_key: []const u8,
        ) !?[]u8 {
            const parsed_key = (try internal_keys.parseResolutionArtifactKeyAlloc(alloc, resolution_key)) orelse return null;
            defer alloc.free(parsed_key.doc_key);
            defer alloc.free(parsed_key.artifact_name);
            const cfg = resolverConfigForResolutionArtifact(index_manager, parsed_key.artifact_name) orelse return null;
            return try internal_keys.artifactNamedPrefixAlloc(alloc, parsed_key.doc_key, "asset", cfg.source_artifact);
        }

        pub fn resolverConfigForResolution(
            index_manager: *index_manager_mod.IndexManager,
            source_artifact: []const u8,
            resolution_artifact: []const u8,
        ) ?*const index_manager_mod.ResolverConfig {
            for (index_manager.resolvers.items) |*cfg| {
                if (std.mem.eql(u8, cfg.source_artifact, source_artifact) and
                    std.mem.eql(u8, cfg.resolution_artifact, resolution_artifact))
                {
                    return cfg;
                }
            }
            return null;
        }

        pub fn resolverConfigForResolutionArtifact(
            index_manager: *index_manager_mod.IndexManager,
            resolution_artifact: []const u8,
        ) ?*const index_manager_mod.ResolverConfig {
            for (index_manager.resolvers.items) |*cfg| {
                if (std.mem.eql(u8, cfg.resolution_artifact, resolution_artifact)) return cfg;
            }
            return null;
        }

        pub fn resolverReplayRetentionRequired(index_manager: *const index_manager_mod.IndexManager, stats: types.ReplayStageStats) bool {
            if (stats.blocked) return true;
            return index_manager.resolvers.items.len > 0;
        }

        pub fn rollbackGraphStateSegmentPage(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            doc_key: []const u8,
            index_name: []const u8,
            state_key: []const u8,
            generation: u64,
            retire_lifetimes: bool,
            changed: *std.ArrayListUnmanaged([]u8),
            changed_set: *std.StringHashMapUnmanaged(void),
        ) !bool {
            const segment_prefix = try internal_keys.graphAssetStateSegmentPrefixAlloc(alloc, state_key);
            defer alloc.free(segment_prefix);
            const segment_upper = try internal_keys.nextPrefixAlloc(alloc, segment_prefix);
            defer if (segment_upper) |upper| alloc.free(upper);

            const SegmentScan = struct {
                alloc: Allocator,
                key: ?[]u8 = null,
                value: ?[]u8 = null,
                has_more: bool = false,

                pub fn deinit(state: *@This()) void {
                    if (state.key) |key| state.alloc.free(key);
                    if (state.value) |value| state.alloc.free(value);
                    state.* = undefined;
                }

                fn scan(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    if (state.key != null) {
                        state.has_more = true;
                        return .stop;
                    }
                    state.key = try state.alloc.dupe(u8, key);
                    errdefer {
                        state.alloc.free(state.key.?);
                        state.key = null;
                    }
                    state.value = try state.alloc.dupe(u8, value);
                    return .@"continue";
                }
            };
            var segment = SegmentScan{ .alloc = alloc };
            defer segment.deinit();
            try store.scanWithContext(
                segment_prefix,
                if (segment_upper) |upper| upper else "",
                .{},
                &segment,
                SegmentScan.scan,
            );

            if (segment.key == null) {
                try store.putBatch(&.{}, &.{state_key});
                return false;
            }

            const previous_keys = try graph_asset_state.decodeSegmentKeysAlloc(alloc, segment.value.?, generation);
            defer freeOwnedConstKeySlice(alloc, previous_keys);
            var reconciled = try reconcileSingleGraphStateContenders(
                alloc,
                store,
                doc_key,
                index_name,
                state_key,
                previous_keys,
                &.{},
                index_manager.graphArtifactSources(index_name),
                generation,
                (index_manager.graphIndex(index_name) orelse return error.IndexNotFound).ttl_duration_ns,
            );
            defer reconciled.deinit(alloc);
            for (previous_keys) |edge_key| try appendUniqueOwnedKeyIndexed(alloc, changed, changed_set, edge_key);
            var scratch_state = std.heap.ArenaAllocator.init(alloc);
            defer scratch_state.deinit();
            const scratch = scratch_state.allocator();
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            try deletes.append(scratch, segment.key.?);
            if (!segment.has_more) try deletes.append(scratch, state_key);
            if (retire_lifetimes) for (previous_keys) |edge_key| {
                try deletes.append(scratch, try internal_keys.graphEdgeTtlLifetimeKeyAlloc(scratch, edge_key, index_name, generation, state_key));
                try deletes.append(scratch, try internal_keys.graphEdgeTtlTombstoneKeyAlloc(scratch, edge_key, index_name, generation, state_key));
            };
            try commitGraphContenderReconcilePage(alloc, store, previous_keys, &reconciled, &.{}, deletes.items);
            return segment.has_more;
        }

        pub fn samePrecomputeAssetProducerBatchKey(lhs: PrecomputeAssetProducerBatchItem, rhs: PrecomputeAssetProducerBatchItem) bool {
            return lhs.producer_type == rhs.producer_type and
                std.mem.eql(u8, lhs.config_json, rhs.config_json) and
                std.mem.eql(u8, lhs.request.content_type, rhs.request.content_type) and
                std.mem.eql(u8, lhs.request.execution_json, rhs.request.execution_json);
        }

        pub fn saveArtifactRepairIssueToStoreWithSummary(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            key: []const u8,
            issue: types.ArtifactRepairIssue,
            new_issue: bool,
            extra_writes: []const docstore_mod.KVPair,
            extra_deletes: []const []const u8,
        ) !void {
            const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
            defer alloc.free(kind_key);
            const encoded = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
            defer alloc.free(encoded);
            if (!new_issue) {
                var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer writes.deinit(alloc);
                try writes.append(alloc, .{ .key = key, .value = encoded });
                try writes.append(alloc, .{ .key = kind_key, .value = encoded });
                try writes.appendSlice(alloc, extra_writes);
                try store.putBatch(writes.items, extra_deletes);
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
            try appendArtifactRepairSummaryDirtyForStore(alloc, store, &writes, &deletes, &owned_delete_keys);
            try store.putBatch(writes.items, deletes.items);
        }

        pub fn scanDerivedCoverageOutcomeFromStore(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
            generation: u64,
            outcome: []const u8,
        ) !u64 {
            const lower = try internal_keys.derivedCoverageOutcomeMarkerPrefixAlloc(alloc, index_name, generation);
            defer alloc.free(lower);
            const upper = try internal_keys.nextPrefixAlloc(alloc, lower);
            defer if (upper) |key| alloc.free(key);
            const upper_bound = if (upper) |key| key else "";

            var skipped: u64 = 0;
            const CountState = struct {
                count: *u64,
                outcome_name: []const u8,

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    _ = key;
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    if (std.mem.eql(u8, value, state.outcome_name)) state.count.* += 1;
                    return .@"continue";
                }
            };

            var state = CountState{ .count = &skipped, .outcome_name = outcome };
            try store.scanWithContext(lower, upper_bound, .{}, &state, CountState.scanEntry);
            return skipped;
        }

        pub fn scanDocumentExtractionPreviousStateFromStore(
            alloc: Allocator,
            db: anytype,
            view: *const DocumentExtractionCatalogView,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) !DocumentExtractionPreviousState {
            var out = DocumentExtractionPreviousState{};
            errdefer out.deinit(alloc);

            var unit_keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (unit_keys.items) |key| alloc.free(@constCast(key));
                unit_keys.deinit(alloc);
            }
            const unit_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", artifact_name);
            defer alloc.free(unit_prefix);
            const unit_rows = try db.core.store.scanPrefix(alloc, unit_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, unit_rows);
            for (unit_rows) |entry| {
                if (std.mem.eql(u8, entry.key, unit_prefix)) continue;
                if (internal_keys.isDerivedEmbeddingArtifactKey(entry.key)) continue;
                try appendOwnedKey(alloc, &unit_keys, entry.key);
            }

            var chunk_keys = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (chunk_keys.items) |key| alloc.free(@constCast(key));
                chunk_keys.deinit(alloc);
            }
            for (view.chunks) |entry| {
                const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "chunk", entry.name);
                defer alloc.free(chunk_prefix);
                const chunk_rows = try db.core.store.scanPrefix(alloc, chunk_prefix);
                defer docstore_mod.DocStore.freeResults(alloc, chunk_rows);
                for (chunk_rows) |row| {
                    if (!internal_keys.isChunkArtifactRecordKey(row.key)) continue;
                    try appendOwnedKey(alloc, &chunk_keys, row.key);
                }
            }

            out.unit_keys = try unit_keys.toOwnedSlice(alloc);
            out.chunk_keys = try chunk_keys.toOwnedSlice(alloc);
            out.unit_descriptors = try alloc.alloc(DocumentExtractionUnitDescriptor, out.unit_keys.len);
            for (out.unit_descriptors) |*descriptor| {
                descriptor.* = .{ .key = "", .fingerprint = "" };
            }
            for (out.unit_descriptors, out.unit_keys) |*descriptor, key| {
                descriptor.* = .{
                    .key = try alloc.dupe(u8, key),
                    .fingerprint = "",
                };
            }
            return out;
        }

        pub fn scanMaterializedChunkSourceStoreBatch(
            alloc: Allocator,
            db: anytype,
            prefix: []const u8,
            upper_bound: []const u8,
            lower: []const u8,
            source_field: []const u8,
            pending_chunk_keys: *const std.StringHashMapUnmanaged(void),
            pending_deletes: *const std.StringHashMapUnmanaged(void),
            sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
            batch_source_bytes: *usize,
            max_batch_items: usize,
            max_batch_bytes: usize,
        ) !?[]u8 {
            const ScanCtx = struct {
                alloc: Allocator,
                prefix: []const u8,
                source_field: []const u8,
                pending_chunk_keys: *const std.StringHashMapUnmanaged(void),
                pending_deletes: *const std.StringHashMapUnmanaged(void),
                sources: *std.ArrayListUnmanaged(ChunkEmbeddingSource),
                batch_source_bytes: *usize,
                max_batch_items: usize,
                max_batch_bytes: usize,
                stopped_key: ?[]u8 = null,

                fn consume(ctx_ptr: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr orelse return error.InvalidArgument));
                    if (!std.mem.startsWith(u8, key, ctx.prefix)) return .stop;
                    if (!internal_keys.isChunkArtifactRecordKey(key)) return .@"continue";
                    if (ctx.pending_chunk_keys.contains(key)) return .@"continue";
                    if (ctx.pending_deletes.contains(key)) return .@"continue";
                    if (!try appendMaterializedChunkSourceToBatch(ctx.alloc, ctx.sources, ctx.batch_source_bytes, key, value, ctx.source_field)) return .@"continue";
                    if (ctx.sources.items.len >= ctx.max_batch_items or ctx.batch_source_bytes.* >= ctx.max_batch_bytes) {
                        ctx.stopped_key = try ctx.alloc.dupe(u8, key);
                        return .stop;
                    }
                    return .@"continue";
                }
            };

            var scan_ctx = ScanCtx{
                .alloc = alloc,
                .prefix = prefix,
                .source_field = source_field,
                .pending_chunk_keys = pending_chunk_keys,
                .pending_deletes = pending_deletes,
                .sources = sources,
                .batch_source_bytes = batch_source_bytes,
                .max_batch_items = max_batch_items,
                .max_batch_bytes = max_batch_bytes,
            };
            try db.core.store.scanWithContext(lower, upper_bound, .{}, &scan_ctx, ScanCtx.consume);
            if (scan_ctx.stopped_key) |key| {
                defer alloc.free(key);
                return try keyAfterAlloc(alloc, key);
            }
            return null;
        }

        pub fn setDerivedCoverageOutcomes(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            index_name: []const u8,
            outcomes: []const DerivedCoverageDocOutcome,
        ) !void {
            if (outcomes.len == 0) return;
            if (try orderedCoverageActive(store)) return;
            const generation = index_manager.coverageGenerationForIndex(index_name) orelse return;
            const tags = std.meta.tags(DerivedCoverageOutcome);

            var counter_counts: [tags.len]u64 = undefined;
            var counter_keys: [tags.len][]u8 = undefined;
            var counter_missing = @as([tags.len]bool, @splat(false));
            var initialized_counters: usize = 0;
            defer for (counter_keys[0..initialized_counters]) |key| alloc.free(key);

            var owned_marker_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_marker_keys.items) |key| alloc.free(key);
                owned_marker_keys.deinit(alloc);
            }
            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer writes.deinit(alloc);
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            var changed = false;

            // Coverage counters and per-document markers are replay bookkeeping, not
            // foreground query data. Bypass cache retention: the hot counters
            // otherwise admit one physical block from every newly published L0 run
            // and fill the shared cache during sustained ingest.
            {
                var counter_probe = try store.beginProbeTxnWithBlockCacheAdmission(.transient);
                defer counter_probe.abort();

                inline for (tags, 0..) |outcome, i| {
                    counter_keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, @tagName(outcome));
                    initialized_counters += 1;
                    const raw = counter_probe.get(counter_keys[i]) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (raw) |value| {
                        counter_counts[i] = try internal_keys.decodeDerivedCoverageOutcomeCount(value);
                    } else {
                        counter_missing[i] = true;
                    }
                }
            }
            // A legacy generation can have markers but no materialized counters. Run
            // that one-time migration scan only after closing the point-read txn; LMDB
            // does not universally permit nested read transactions on one thread.
            inline for (tags, 0..) |outcome, i| {
                if (counter_missing[i]) {
                    counter_counts[i] = try scanDerivedCoverageOutcomeFromStore(alloc, store, index_name, generation, @tagName(outcome));
                }
            }

            {
                var marker_probe = try store.beginProbeTxnWithBlockCacheAdmission(.transient);
                defer marker_probe.abort();
                for (outcomes) |transition| {
                    if (seen.contains(transition.doc_key)) continue;
                    try seen.put(alloc, transition.doc_key, {});
                    const target_index = @backingInt(transition.outcome);
                    const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, index_name, generation, transition.doc_key);
                    owned_marker_keys.append(alloc, marker_key) catch |err| {
                        alloc.free(marker_key);
                        return err;
                    };
                    const existing = marker_probe.get(marker_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    const existing_outcome: ?DerivedCoverageOutcome = if (existing) |value|
                        std.meta.stringToEnum(DerivedCoverageOutcome, value) orelse return error.InvalidDerivedCoverageOutcome
                    else
                        null;
                    if (existing_outcome == null or existing_outcome.? != transition.outcome) {
                        if (existing_outcome) |previous| {
                            const previous_index = @backingInt(previous);
                            if (counter_counts[previous_index] == 0) return error.InvalidDerivedCoverageCounter;
                            counter_counts[previous_index] -= 1;
                        }
                        counter_counts[target_index] +|= 1;
                        try writes.append(alloc, .{ .key = marker_key, .value = @tagName(transition.outcome) });
                        changed = true;
                    }
                }
            }
            if (!changed) return;

            var counter_values: [tags.len][8]u8 = undefined;
            inline for (tags, 0..) |_, i| {
                try writes.append(alloc, .{
                    .key = counter_keys[i],
                    .value = internal_keys.encodeDerivedCoverageOutcomeCount(&counter_values[i], counter_counts[i]),
                });
            }
            try store.putBatch(writes.items, &.{});
        }

        pub fn shouldStoreChunkArtifacts(alloc: Allocator, request: enrichment_types.GeneratedEnrichmentRequest) !bool {
            if (request.persist_artifact) return true;
            if (request.full_text_index) return true;
            if (request.chunker_json.len == 0) return true;
            if (try chunking_types_mod.parseHasFullTextIndexFromSlice(alloc, request.chunker_json)) return true;
            return try chunking_types_mod.parseStoreChunksFromSlice(alloc, request.chunker_json);
        }

        pub fn sliceContainsDocKeyPrefix(docs: []const derived_types.DerivedDocument, prefix: []const u8) bool {
            for (docs) |doc| if (std.mem.startsWith(u8, doc.key, prefix)) return true;
            return false;
        }

        pub fn sliceContainsKey(keys: []const []const u8, key: []const u8) bool {
            for (keys) |candidate| if (std.mem.eql(u8, candidate, key)) return true;
            return false;
        }

        pub fn sliceContainsKeyPrefix(keys: []const []const u8, prefix: []const u8) bool {
            for (keys) |candidate| if (std.mem.startsWith(u8, candidate, prefix)) return true;
            return false;
        }

        pub fn sliceContainsWriteKey(writes: []const types.BatchWrite, key: []const u8) bool {
            for (writes) |write| if (std.mem.eql(u8, write.key, key)) return true;
            return false;
        }

        pub fn sliceContainsWriteKeyPrefix(writes: []const types.BatchWrite, prefix: []const u8) bool {
            for (writes) |write| if (std.mem.startsWith(u8, write.key, prefix)) return true;
            return false;
        }

        pub fn sourceArtifactKeyForResolutionAlloc(alloc: Allocator, doc_key: []const u8, source_artifact: []const u8) ![]u8 {
            if (try sourceArtifactKeyFromResolutionScopeAlloc(alloc, doc_key, source_artifact)) |source_key| return source_key;
            return try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", source_artifact);
        }

        pub fn sourceArtifactKeyFromResolutionScopeAlloc(alloc: Allocator, doc_key: []const u8, source_artifact: []const u8) !?[]u8 {
            var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, doc_key)) orelse return null;
            defer artifact_ref.deinit(alloc);
            if (artifact_ref.kind != .asset and artifact_ref.kind != .chunk) return null;
            if (!std.mem.eql(u8, artifact_ref.name, source_artifact)) return null;
            return try alloc.dupe(u8, doc_key);
        }

        pub fn storeContainsKey(alloc: Allocator, store: *docstore_mod.DocStore, key: []const u8) !bool {
            const existing = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            alloc.free(existing);
            return true;
        }

        pub fn storeDocumentValueForGraphSource(
            alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_manager: *index_manager_mod.IndexManager,
            doc_key: []const u8,
            relational_base_rows: bool,
        ) !?[]u8 {
            const internal_doc_key = if (relational_base_rows)
                try relational_store.keyAlloc(alloc, doc_key)
            else
                try internal_keys.documentKeyAlloc(alloc, doc_key);
            defer alloc.free(internal_doc_key);
            const raw = store.get(alloc, internal_doc_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (raw == null) return null;
            defer alloc.free(raw.?);
            return try index_manager.materializeStoredValueAlloc(alloc, internal_doc_key, raw.?);
        }

        pub fn storeHasUserDataBounded(store: *docstore_mod.DocStore) !bool {
            var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
            defer read.abort();
            var cursor = try read.openCursor();
            defer cursor.close();
            const first = try cursor.seekAtOrAfter(&.{internal_keys.user_namespace});
            return if (first) |entry| internal_keys.isInternalUserKey(entry.key) else false;
        }

        pub fn storeKeyExists(alloc: Allocator, db: anytype, key: []const u8) !bool {
            const value = db.core.getStoreValue(alloc, key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            const owned = value orelse return false;
            alloc.free(owned);
            return true;
        }

        pub fn storeValueDiffers(alloc: Allocator, store: *docstore_mod.DocStore, key: []const u8, value: []const u8) !bool {
            const existing = store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return true,
                else => return err,
            };
            defer alloc.free(existing);
            return !std.mem.eql(u8, existing, value);
        }

        pub fn storedOrPendingEmbeddingSourceHash(
            db: anytype,
            pending_writes: ?*const PendingArtifactWriteIndex,
            artifact_key: []const u8,
        ) !?u64 {
            if (pending_writes) |index| {
                if (index.get(artifact_key)) |value| {
                    return enrichment_artifact_codec.sourceHash(value) catch null;
                }
            }
            const metadata = db.core.store.getArtifactMetadata(artifact_key) catch |err| switch (err) {
                error.NotFound,
                error.InvalidArtifactHeader,
                error.InvalidArtifactMagic,
                error.InvalidArtifactKind,
                error.UnsupportedArtifactCodecVersion,
                error.InvalidArtifactPayload,
                error.InvalidVectorDimensions,
                => return null,
                else => return err,
            };
            return metadata.sourceHash();
        }

        pub fn storedUnitIntegerValue(comptime T: type, value: std.json.Value) !T {
            return switch (value) {
                .integer => |integer| std.math.cast(T, integer) orelse error.InvalidDocumentExtractionState,
                .number_string => |text| std.fmt.parseInt(T, text, 10) catch error.InvalidDocumentExtractionState,
                else => error.InvalidDocumentExtractionState,
            };
        }

        pub fn storedUnitOptionalBbox(object: std.json.ObjectMap, name: []const u8) !?[4]f64 {
            const value = object.get(name) orelse return null;
            if (value == .null) return null;
            if (value != .array or value.array.items.len != 4) return error.InvalidDocumentExtractionState;
            var bbox: [4]f64 = undefined;
            for (value.array.items, 0..) |coordinate, i| {
                bbox[i] = switch (coordinate) {
                    .integer => |integer| @floatFromInt(integer),
                    .float => |float| if (std.math.isFinite(float)) float else return error.InvalidDocumentExtractionState,
                    .number_string => |text| std.fmt.parseFloat(f64, text) catch return error.InvalidDocumentExtractionState,
                    else => return error.InvalidDocumentExtractionState,
                };
                if (!std.math.isFinite(bbox[i])) return error.InvalidDocumentExtractionState;
            }
            return bbox;
        }

        pub fn storedUnitOptionalBool(object: std.json.ObjectMap, name: []const u8) !?bool {
            const value = object.get(name) orelse return null;
            return switch (value) {
                .null => null,
                .bool => |boolean| boolean,
                else => error.InvalidDocumentExtractionState,
            };
        }

        pub fn storedUnitOptionalFloat(object: std.json.ObjectMap, name: []const u8) !?f64 {
            const value = object.get(name) orelse return null;
            return switch (value) {
                .null => null,
                .integer => |integer| @floatFromInt(integer),
                .float => |float| if (std.math.isFinite(float)) float else error.InvalidDocumentExtractionState,
                .number_string => |text| blk: {
                    const parsed = std.fmt.parseFloat(f64, text) catch return error.InvalidDocumentExtractionState;
                    if (!std.math.isFinite(parsed)) return error.InvalidDocumentExtractionState;
                    break :blk parsed;
                },
                else => error.InvalidDocumentExtractionState,
            };
        }

        pub fn storedUnitOptionalInteger(comptime T: type, object: std.json.ObjectMap, name: []const u8) !?T {
            const value = object.get(name) orelse return null;
            return switch (value) {
                .null => null,
                .integer => |integer| std.math.cast(T, integer) orelse error.InvalidDocumentExtractionState,
                .number_string => |text| std.fmt.parseInt(T, text, 10) catch error.InvalidDocumentExtractionState,
                else => error.InvalidDocumentExtractionState,
            };
        }

        pub fn storedUnitOptionalString(object: std.json.ObjectMap, name: []const u8) !?[]const u8 {
            const value = object.get(name) orelse return null;
            return switch (value) {
                .null => null,
                .string => |text| text,
                else => error.InvalidDocumentExtractionState,
            };
        }

        pub fn storedUnitRequiredBool(object: std.json.ObjectMap, name: []const u8) !bool {
            return (try storedUnitOptionalBool(object, name)) orelse error.InvalidDocumentExtractionState;
        }

        pub fn storedUnitRequiredString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
            return (try storedUnitOptionalString(object, name)) orelse error.InvalidDocumentExtractionState;
        }

        pub fn storedUnitTextRegionsAlloc(alloc: Allocator, value: ?std.json.Value) ![]document_extraction_mod.TextRegion {
            const regions_value = value orelse return &.{};
            if (regions_value == .null) return &.{};
            if (regions_value != .array) return error.InvalidDocumentExtractionState;
            const regions = try alloc.alloc(document_extraction_mod.TextRegion, regions_value.array.items.len);
            for (regions_value.array.items, 0..) |item, i| {
                if (item != .object) return error.InvalidDocumentExtractionState;
                const span_value = item.object.get("span") orelse return error.InvalidDocumentExtractionState;
                if (span_value != .array or span_value.array.items.len != 2) return error.InvalidDocumentExtractionState;
                const bbox = (try storedUnitOptionalBbox(item.object, "bbox")) orelse return error.InvalidDocumentExtractionState;
                regions[i] = .{
                    .span = .{
                        try storedUnitIntegerValue(u32, span_value.array.items[0]),
                        try storedUnitIntegerValue(u32, span_value.array.items[1]),
                    },
                    .bbox = bbox,
                };
            }
            return regions;
        }

        pub fn takeOwnedSlice(comptime T: type, alloc: Allocator, existing: []const T, incoming: *[]const T) ![]const T {
            if (incoming.*.len == 0) return existing;
            if (existing.len == 0) {
                const out = incoming.*;
                incoming.* = &.{};
                return out;
            }

            const out = try alloc.alloc(T, existing.len + incoming.*.len);
            @memcpy(out[0..existing.len], existing);
            @memcpy(out[existing.len..], incoming.*);
            alloc.free(existing);
            alloc.free(incoming.*);
            incoming.* = &.{};
            return out;
        }

        pub fn unitDescriptorFingerprintMatches(descriptors: []const DocumentExtractionUnitDescriptor, key: []const u8, fingerprint: []const u8) bool {
            if (fingerprint.len == 0) return false;
            for (descriptors) |descriptor| {
                if (std.mem.eql(u8, descriptor.key, key) and std.mem.eql(u8, descriptor.fingerprint, fingerprint)) return true;
            }
            return false;
        }

        pub fn upsertOwnedStoreWrite(
            alloc: Allocator,
            list: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            positions: *StoreWritePositions,
            key: []u8,
            value: []u8,
        ) !void {
            if (positions.get(key)) |position| {
                alloc.free(key);
                alloc.free(@constCast(list.items[position].value));
                list.items[position].value = value;
                return;
            }
            const position = list.items.len;
            try list.append(alloc, .{ .key = key, .value = value });
            errdefer _ = list.pop();
            try positions.put(alloc, key, position);
        }

        pub fn upsertOwnedStoreWriteDupeKey(
            alloc: Allocator,
            list: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            positions: *StoreWritePositions,
            key: []const u8,
            value: []u8,
        ) !void {
            const owned_key = try alloc.dupe(u8, key);
            errdefer alloc.free(owned_key);
            try upsertOwnedStoreWrite(alloc, list, positions, owned_key, value);
        }

        pub fn validateDocumentExtractionInlineSources(db: anytype, doc_value: []const u8) !void {
            var has_document_extraction_asset = false;
            for (db.core.index_manager.enrichments.items) |entry| {
                if (entry.kind != .asset) continue;
                var producer_cfg = asset_producer_mod.parseProducerConfig(db.alloc, entry.producer_json) catch continue;
                defer producer_cfg.deinit(db.alloc);
                if (producer_cfg.type == .document_extraction) {
                    has_document_extraction_asset = true;
                    break;
                }
            }
            if (!has_document_extraction_asset) return;

            const parsed = try std.json.parseFromSlice(std.json.Value, db.alloc, doc_value, .{});
            defer parsed.deinit();
            return try validateDocumentExtractionInlineSourcesParsed(db, parsed.value, doc_value);
        }

        pub fn validateDocumentExtractionInlineSourcesParsed(
            db: anytype,
            value: std.json.Value,
            doc_value: []const u8,
        ) !void {
            if (value != .object) return;

            for (db.core.index_manager.enrichments.items) |entry| {
                if (entry.kind != .asset) continue;
                var producer_cfg = asset_producer_mod.parseProducerConfig(db.alloc, entry.producer_json) catch continue;
                defer producer_cfg.deinit(db.alloc);
                if (producer_cfg.type != .document_extraction) continue;

                if (entry.source_template.len > 0) {
                    const rendered = renderSourceTemplateText(db.alloc, db, entry.source_template, doc_value) catch |err| switch (err) {
                        error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                        else => continue,
                    };
                    defer db.alloc.free(rendered);
                    try document_extraction_mod.validateInlineSourceSize(db.remote_content, rendered);
                    continue;
                }

                const source = value.object.get(entry.source_field) orelse continue;
                if (source != .string) continue;
                try document_extraction_mod.validateInlineSourceSize(db.remote_content, source.string);
            }
        }

        pub fn validateDocumentExtractionInlineSourcesSnapshotParsed(
            alloc: Allocator,
            db: anytype,
            plan: index_manager_mod.IndexManager.WritePlanSnapshot,
            value: std.json.Value,
            document: mapper.ExtractedWrite,
        ) !void {
            if (value != .object) return;
            for (plan.generated_templates) |request| {
                if (request.kind != .asset) continue;
                var producer_cfg = asset_producer_mod.parseProducerConfig(alloc, request.producer_json) catch continue;
                defer producer_cfg.deinit(alloc);
                if (producer_cfg.type != .document_extraction) continue;

                if (request.source_template.len > 0) {
                    const rendered = renderSourceTemplateText(alloc, db, request.source_template, (try document.logicalJson()).?) catch |err| switch (err) {
                        error.PermanentPromptFailure, error.TransientPromptFailure => return err,
                        else => continue,
                    };
                    defer alloc.free(rendered);
                    try document_extraction_mod.validateInlineSourceSize(db.remote_content, rendered);
                    continue;
                }
                const source = value.object.get(request.source_field) orelse continue;
                if (source != .string) continue;
                try document_extraction_mod.validateInlineSourceSize(db.remote_content, source.string);
            }
        }

        pub fn validateGraphEdgeMetadataJson(alloc: Allocator, metadata_json: []const u8) !void {
            if (metadata_json.len == 0) return;
            var parsed = ant_json.parseFromSlice(std.json.Value, alloc, metadata_json, .{}) catch
                return error.InvalidGraphEdges;
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidGraphEdges;
        }

        pub fn vectorIndexConsumesChunkKey(
            index_manager: *index_manager_mod.IndexManager,
            index_ref: index_manager_mod.ManagedIndexRef,
            key: []const u8,
        ) bool {
            if (!internal_keys.isChunkArtifactRecordKey(key)) return false;
            const chunk_name, const embedding_names = switch (index_ref.kind) {
                .dense_vector => blk: {
                    const entry = index_manager.denseIndex(index_ref.name) orelse return false;
                    break :blk .{ entry.chunk_name, entry.embedding_names };
                },
                .sparse_vector => blk: {
                    const entry = index_manager.sparseIndex(index_ref.name) orelse return false;
                    break :blk .{ entry.chunk_name, entry.embedding_names };
                },
                else => return false,
            };
            if (chunk_name) |name| {
                if (internal_keys.matchesChunkArtifactName(key, name)) return true;
            }
            // Multi-source indexes cache only their first source's chunk name. Resolve
            // the remaining configured producers, without inspecting artifact storage:
            // the chunk and its embedding rows may already have been deleted.
            for (embedding_names) |name| {
                const producer = index_manager.getEnrichment(.embedding, name) orelse continue;
                if (producer.source_artifact_name.len > 0 and
                    internal_keys.matchesChunkArtifactName(key, producer.source_artifact_name)) return true;
            }
            return false;
        }
    };
}
