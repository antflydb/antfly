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
const Allocator = std.mem.Allocator;
const db_config = @import("config.zig");
const apply_rw_lock_mod = @import("apply_rw_lock.zig");
const apply_state = @import("derived/apply_state.zig");
const doc_identity = @import("doc_identity.zig");
const internal_keys = @import("../internal_keys.zig");
const docstore_mod = @import("../docstore.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const mapper = @import("document_mapper.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const replay_source_mod = @import("derived/replay_source.zig");
const transaction_runtime_mod = @import("maintenance/transaction_runtime.zig");
const mem_backend_mod = @import("../mem_backend.zig");
const persistent_mod = @import("../persistent.zig");
const range_state_mod = @import("range_state.zig");
const schema_mod = @import("../schema.zig");
const shard_mod = @import("../shard.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const ttl_mod = @import("../ttl.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const lsm_table_file = @import("../lsm/table_file.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_pattern_mod = @import("../../graph/pattern.zig");
const paths_mod = @import("../../graph/paths.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");

pub const PrimaryBackendKind = db_config.PrimaryBackendKind;
pub const PrimaryBackend = db_config.PrimaryBackend;
pub const CoreOpenOptions = db_config.CoreOpenOptions;

pub const PendingWorkStats = struct {
    derived_target_sequence: u64,
    has_async_indexes: bool,
    enrichment: types.EnrichmentStats,
    text_merge: types.TextMergeStats = .{},
};

pub const MaintenanceDriver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        pending_work_stats: *const fn (ptr: *anyopaque) PendingWorkStats,
        run_derived_until: *const fn (ptr: *anyopaque, sequence: u64) anyerror!void,
        run_enrichment_until: *const fn (ptr: *anyopaque, sequence: u64) anyerror!void,
        run_maintenance_until: *const fn (ptr: *anyopaque, sequence: u64) anyerror!void,
        run_until_idle: *const fn (ptr: *anyopaque) anyerror!void,
    };

    pub fn pendingWorkStats(self: MaintenanceDriver) PendingWorkStats {
        return self.vtable.pending_work_stats(self.ptr);
    }

    pub fn runDerivedUntil(self: MaintenanceDriver, sequence: u64) !void {
        return try self.vtable.run_derived_until(self.ptr, sequence);
    }

    pub fn runEnrichmentUntil(self: MaintenanceDriver, sequence: u64) !void {
        return try self.vtable.run_enrichment_until(self.ptr, sequence);
    }

    pub fn runMaintenanceUntil(self: MaintenanceDriver, sequence: u64) !void {
        return try self.vtable.run_maintenance_until(self.ptr, sequence);
    }

    pub fn runUntilIdle(self: MaintenanceDriver) !void {
        return try self.vtable.run_until_idle(self.ptr);
    }
};

pub const Services = struct {
    engine: Engine,
    maintenance: MaintenanceDriver,
};

pub const PrimaryStoreOwner = union(enum) {
    none,
    mem: *mem_backend_mod.Backend,
    lsm: struct {
        handle: lsm_backend_mod.BackendHandle,
        split_options: ?lsm_backend_mod.Options,
    },

    pub fn close(self: *PrimaryStoreOwner, alloc: Allocator) void {
        switch (self.*) {
            .none => {},
            .mem => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*owner| owner.handle.close(),
        }
        self.* = .none;
    }

    pub fn prepareSplitRightToDir(self: *PrimaryStoreOwner, split_key: []const u8, dest_dir: []const u8) !bool {
        return switch (self.*) {
            .none, .mem => false,
            .lsm => |owner| blk: {
                if (owner.split_options) |split_opts| {
                    _ = try owner.handle.backend.prepareSplitRightToDir(split_key, dest_dir, split_opts);
                    break :blk true;
                }
                break :blk false;
            },
        };
    }

    pub fn rewriteLeftInPlace(self: *PrimaryStoreOwner, split_key: []const u8) !bool {
        return switch (self.*) {
            .none, .mem => false,
            .lsm => |owner| blk: {
                if (owner.split_options == null) break :blk false;
                break :blk try owner.handle.backend.rewriteLeftInPlace(split_key);
            },
        };
    }

    pub fn lsmMaintenanceScore(self: *const PrimaryStoreOwner) u64 {
        return switch (self.*) {
            .none, .mem => 0,
            .lsm => |owner| owner.handle.backend.maintenanceScore(),
        };
    }

    pub fn lsmMaintenanceDebtHint(self: *const PrimaryStoreOwner) u64 {
        return switch (self.*) {
            .none, .mem => 0,
            .lsm => |owner| owner.handle.backend.maintenanceDebtHint(),
        };
    }

    pub fn refreshLsmMaintenanceDebtHint(self: *PrimaryStoreOwner) void {
        switch (self.*) {
            .none, .mem => {},
            .lsm => |owner| owner.handle.backend.refreshMaintenanceDebtHint(),
        }
    }

    pub fn snapshotLsmMaintenanceStats(self: *const PrimaryStoreOwner) ?lsm_backend_mod.Backend.MaintenanceStats {
        return switch (self.*) {
            .none, .mem => null,
            .lsm => |owner| owner.handle.backend.snapshotMaintenanceStats(),
        };
    }

    pub fn snapshotLsmWriteStats(self: *const PrimaryStoreOwner) ?lsm_backend_mod.Backend.WriteStats {
        return switch (self.*) {
            .none, .mem => null,
            .lsm => |owner| owner.handle.backend.snapshotWriteStats(),
        };
    }

    pub fn snapshotLsmNativeStorageStats(self: *const PrimaryStoreOwner) ?lsm_backend_mod.NativeStorageStats {
        return switch (self.*) {
            .none, .mem => null,
            .lsm => |owner| owner.handle.backend.snapshotNativeStorageStats(),
        };
    }

    pub fn runLsmMaintenanceStep(self: *PrimaryStoreOwner) !bool {
        return switch (self.*) {
            .none, .mem => false,
            .lsm => |owner| try owner.handle.backend.runMaintenanceStep(),
        };
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *PrimaryStoreOwner) !bool {
        return switch (self.*) {
            .none, .mem => false,
            .lsm => |owner| try owner.handle.backend.runMaintenanceStepBestEffort(),
        };
    }
};

pub const OpenedPrimaryStore = struct {
    store: docstore_mod.DocStore,
    owner: PrimaryStoreOwner = .none,
};

pub const OpenedCoreResources = struct {
    path: []u8,
    applied_sequence_checkpoint_path: ?[]u8,
    store: *docstore_mod.DocStore,
    primary_store_owner: PrimaryStoreOwner,
    change_journal: *change_journal_mod.Journal,
    shard_manager: *shard_mod.ShardManager,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    log_mutex: *std.atomic.Mutex,
    schema: ?schema_mod.TableSchema,
    identity_namespace: doc_identity.Namespace,
    artifact_cleanup_maybe: bool,

    pub fn deinit(self: *OpenedCoreResources, alloc: Allocator) void {
        if (self.schema) |schema| schema_mod.freeSchema(alloc, schema);
        self.log_mutex.* = undefined;
        alloc.destroy(self.log_mutex);
        self.apply_mutex.* = undefined;
        alloc.destroy(self.apply_mutex);
        self.index_manager.deinit();
        alloc.destroy(self.index_manager);
        self.shard_manager.deinit();
        alloc.destroy(self.shard_manager);
        self.change_journal.close();
        alloc.destroy(self.change_journal);
        self.store.close();
        alloc.destroy(self.store);
        self.primary_store_owner.close(alloc);
        if (self.applied_sequence_checkpoint_path) |checkpoint_path| alloc.free(checkpoint_path);
        alloc.free(self.path);
        self.* = undefined;
    }
};

pub const AsyncResources = struct {
    store: *docstore_mod.DocStore,
    applied_sequence_checkpoint_path: ?[]const u8,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
};

pub const BatchExecutionResources = struct {
    store: *docstore_mod.DocStore,
    applied_sequence_checkpoint_path: ?[]const u8,
    shard_manager: *shard_mod.ShardManager,
    change_journal: *change_journal_mod.Journal,
    replay_source: replay_source_mod.Source,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    log_mutex: *std.atomic.Mutex,
    identity_namespace: doc_identity.Namespace,
    artifact_cleanup_maybe: *std.atomic.Value(bool),
};

pub const SplitIndexHandoffs = struct {
    dense: []index_manager_mod.DenseSplitHandoff,
    text: []index_manager_mod.TextSplitHandoff,
    sparse: []index_manager_mod.SparseSplitHandoff,

    pub fn deinit(self: *SplitIndexHandoffs, alloc: Allocator) void {
        for (self.dense) |*handoff| handoff.deinit(alloc);
        alloc.free(self.dense);
        for (self.text) |*handoff| handoff.deinit(alloc);
        alloc.free(self.text);
        for (self.sparse) |*handoff| handoff.deinit(alloc);
        alloc.free(self.sparse);
        self.* = undefined;
    }
};

pub const DBCore = struct {
    alloc: Allocator,
    path: []u8,
    applied_sequence_checkpoint_path: ?[]u8,
    store: *docstore_mod.DocStore,
    primary_store_owner: PrimaryStoreOwner,
    change_journal: *change_journal_mod.Journal,
    shard_manager: *shard_mod.ShardManager,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    log_mutex: *std.atomic.Mutex,
    schema: ?schema_mod.TableSchema,
    identity_namespace: doc_identity.Namespace,
    artifact_cleanup_maybe: std.atomic.Value(bool),

    pub fn fromOpened(alloc: Allocator, opened: OpenedCoreResources) DBCore {
        return .{
            .alloc = alloc,
            .path = opened.path,
            .applied_sequence_checkpoint_path = opened.applied_sequence_checkpoint_path,
            .store = opened.store,
            .primary_store_owner = opened.primary_store_owner,
            .change_journal = opened.change_journal,
            .shard_manager = opened.shard_manager,
            .index_manager = opened.index_manager,
            .apply_mutex = opened.apply_mutex,
            .log_mutex = opened.log_mutex,
            .schema = opened.schema,
            .identity_namespace = opened.identity_namespace,
            .artifact_cleanup_maybe = .init(opened.artifact_cleanup_maybe),
        };
    }

    pub fn deinit(self: *DBCore) void {
        if (self.schema) |schema| schema_mod.freeSchema(self.alloc, schema);
        self.log_mutex.* = undefined;
        self.alloc.destroy(self.log_mutex);
        self.apply_mutex.* = undefined;
        self.alloc.destroy(self.apply_mutex);
        self.index_manager.deinit();
        self.alloc.destroy(self.index_manager);
        self.shard_manager.deinit();
        self.alloc.destroy(self.shard_manager);
        self.change_journal.close();
        self.alloc.destroy(self.change_journal);
        self.store.close();
        self.alloc.destroy(self.store);
        self.primary_store_owner.close(self.alloc);
        if (self.applied_sequence_checkpoint_path) |checkpoint_path| self.alloc.free(checkpoint_path);
        self.alloc.free(self.path);
        self.* = undefined;
    }

    pub fn services(self: *DBCore, engine: Engine, maintenance: MaintenanceDriver) Services {
        _ = self;
        return .{
            .engine = engine,
            .maintenance = maintenance,
        };
    }

    pub fn asyncResources(self: *DBCore) AsyncResources {
        return .{
            .store = self.store,
            .applied_sequence_checkpoint_path = self.applied_sequence_checkpoint_path,
            .index_manager = self.index_manager,
            .apply_mutex = self.apply_mutex,
        };
    }

    pub fn batchExecutionResources(self: *DBCore) BatchExecutionResources {
        return .{
            .store = self.store,
            .applied_sequence_checkpoint_path = self.applied_sequence_checkpoint_path,
            .shard_manager = self.shard_manager,
            .change_journal = self.change_journal,
            .replay_source = self.replaySource(),
            .index_manager = self.index_manager,
            .apply_mutex = self.apply_mutex,
            .log_mutex = self.log_mutex,
            .identity_namespace = self.identity_namespace,
            .artifact_cleanup_maybe = &self.artifact_cleanup_maybe,
        };
    }

    pub fn lockApply(self: *DBCore) void {
        self.lockApplyExclusive();
    }

    pub fn unlockApply(self: *DBCore) void {
        self.unlockApplyExclusive();
    }

    pub fn lockApplyExclusive(self: *DBCore) void {
        self.apply_mutex.lockExclusive();
    }

    pub fn tryLockApplyExclusive(self: *DBCore) bool {
        return self.apply_mutex.tryLockExclusive();
    }

    pub fn unlockApplyExclusive(self: *DBCore) void {
        self.apply_mutex.unlockExclusive();
    }

    pub fn lockApplyShared(self: *DBCore) void {
        self.apply_mutex.lockShared();
    }

    pub fn tryLockApplyShared(self: *DBCore) bool {
        return self.apply_mutex.tryLockShared();
    }

    pub fn unlockApplyShared(self: *DBCore) void {
        self.apply_mutex.unlockShared();
    }

    pub fn byteRange(self: *DBCore) types.ByteRange {
        return self.shard_manager.getByteRange();
    }

    pub fn splitState(self: *DBCore) ?shard_mod.SplitState {
        return self.shard_manager.getSplitState();
    }

    pub fn refreshIndexRange(self: *DBCore) void {
        self.index_manager.updateRange(self.shard_manager.getByteRange());
    }

    pub fn setIndexOpenParallelism(self: *DBCore, parallelism: ?usize) void {
        self.index_manager.setLoadParallelism(parallelism);
    }

    pub fn updateRange(self: *DBCore, byte_range: types.ByteRange) !void {
        try self.shard_manager.setByteRange(byte_range);
        self.refreshIndexRange();
        try range_state_mod.saveRange(self.store, self.shard_manager.getByteRange());
    }

    pub fn nextDerivedSequence(self: *DBCore) u64 {
        return self.store.lastReplaySequence(0);
    }

    pub fn nextEnrichmentSequence(self: *DBCore) u64 {
        return self.store.lastReplaySequence(0);
    }

    pub fn nextDerivedAppendSequence(self: *DBCore) u64 {
        return self.store.nextReplaySequence(1);
    }

    pub fn reserveDerivedAppendSequence(self: *DBCore) u64 {
        return self.store.reserveNextReplaySequence(1);
    }

    pub fn replaySource(self: *DBCore) replay_source_mod.Source {
        return replay_source_mod.Source.fromPrimaryStore(self.store, null, self.index_manager.resource_manager);
    }

    pub fn setSplitState(self: *DBCore, state: ?shard_mod.SplitState) !void {
        try self.shard_manager.setSplitState(state);
        self.refreshIndexRange();
    }

    pub fn prepareSplit(self: *DBCore, split_key: []const u8) !void {
        try self.shard_manager.prepareSplit(split_key);
    }

    pub fn splitDeltaSequence(self: *DBCore) u64 {
        return self.shard_manager.getDeltaSequence();
    }

    pub fn listSplitDeltasAfter(self: *DBCore, alloc: Allocator, after_seq: u64) ![]shard_mod.SplitDelta {
        return try self.shard_manager.listDeltasAfter(alloc, after_seq);
    }

    pub fn clearSplitDeltas(self: *DBCore) !void {
        try self.shard_manager.clearSplitDeltas();
    }

    pub fn appendSplitDelta(self: *DBCore, timestamp_ns: u64, writes: []const docstore_mod.KVPair, deletes: []const []const u8) !void {
        try self.shard_manager.appendSplitDelta(timestamp_ns, writes, deletes);
    }

    pub fn completeSplitTransition(self: *DBCore, new_shard_id: u64, split_key: []const u8) !void {
        try self.shard_manager.split(new_shard_id, split_key);
        self.refreshIndexRange();
        try range_state_mod.saveRange(self.store, self.shard_manager.getByteRange());
    }

    pub fn finalizeSplitState(self: *DBCore) !void {
        try self.shard_manager.finalizeSplit();
        self.refreshIndexRange();
        try range_state_mod.saveRange(self.store, self.shard_manager.getByteRange());
        try self.clearSplitDeltaFinalSeq();
    }

    pub fn loadSplitDeltaFinalSeq(self: *DBCore, alloc: Allocator) !u64 {
        return try range_state_mod.loadSplitDeltaFinalSeq(alloc, self.store);
    }

    pub fn saveSplitDeltaFinalSeq(self: *DBCore, seq: u64) !void {
        try range_state_mod.saveSplitDeltaFinalSeq(self.store, seq);
    }

    pub fn clearSplitDeltaFinalSeq(self: *DBCore) !void {
        try range_state_mod.clearSplitDeltaFinalSeq(self.store);
    }

    pub fn addIndex(self: *DBCore, cfg: types.IndexConfig) !u64 {
        try self.index_manager.add(self.store, cfg);
        const applied = if (try self.index_manager.requiresEnrichmentReplay(cfg.name))
            0
        else
            self.nextDerivedSequence();
        try self.saveAppliedSequence(cfg.name, applied);
        return applied;
    }

    pub fn addEnrichment(self: *DBCore, cfg: types.EnrichmentConfig) !void {
        try self.index_manager.addEnrichment(self.store, cfg);
    }

    pub fn hasIndex(self: *DBCore, name: []const u8) bool {
        return self.index_manager.has(name);
    }

    pub fn listIndexes(self: *DBCore, alloc: Allocator) ![]types.IndexConfig {
        return try self.index_manager.listIndexesPublic(alloc);
    }

    pub fn compactTextIndexes(self: *DBCore) !void {
        try self.index_manager.compactAllTextIndexes();
    }

    pub fn drainScheduledTextMerges(self: *DBCore) !void {
        try self.index_manager.drainScheduledTextMerges();
    }

    pub fn forceCompactTextIndexes(self: *DBCore) !void {
        try self.index_manager.forceCompactAllTextIndexes();
    }

    pub fn bestEffortForceCompactTextIndexes(self: *DBCore) !void {
        try self.index_manager.bestEffortForceCompactAllTextIndexes();
    }

    pub fn registerShadowIndexes(
        self: *DBCore,
        alloc: Allocator,
        shadow_manager: *index_manager_mod.IndexManager,
    ) !void {
        const configs = try self.listIndexes(alloc);
        defer types.freeIndexConfigs(alloc, configs);
        for (configs) |cfg| {
            try shadow_manager.registerShadowIndex(self.store, cfg);
        }
    }

    pub fn indexCount(self: *DBCore) usize {
        return self.index_manager.count();
    }

    pub fn getEnrichment(self: *DBCore, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) !?types.EnrichmentConfig {
        return try self.index_manager.getEnrichmentPublic(alloc, kind, name);
    }

    pub fn listEnrichments(self: *DBCore, alloc: Allocator) ![]types.EnrichmentConfig {
        return try self.index_manager.listEnrichmentsPublic(alloc);
    }

    pub fn deleteIndex(self: *DBCore, name: []const u8) !bool {
        return try self.index_manager.remove(self.store, name);
    }

    pub fn deleteEnrichment(self: *DBCore, kind: types.EnrichmentKind, name: []const u8) !bool {
        return try self.index_manager.removeEnrichment(self.store, kind, name);
    }

    pub fn planGeneratedEnrichments(
        self: *DBCore,
        alloc: Allocator,
        doc_key: []const u8,
        cleaned: []const u8,
        dense_embeddings: []const types.EnrichmentDenseEmbeddingWrite,
        sparse_embeddings: []const types.EnrichmentSparseEmbeddingWrite,
    ) ![]enrichment_types.GeneratedEnrichmentRequest {
        var explicit_dense = try alloc.alloc(mapper.DenseEmbeddingWrite, dense_embeddings.len);
        defer alloc.free(explicit_dense);
        for (dense_embeddings, 0..) |embedding, i| {
            explicit_dense[i] = .{
                .index_name = embedding.index_name,
                .doc_key = embedding.doc_key,
                .artifact_key = null,
                .vector = embedding.vector,
            };
        }

        var explicit_sparse = try alloc.alloc(mapper.SparseEmbeddingWrite, sparse_embeddings.len);
        defer alloc.free(explicit_sparse);
        for (sparse_embeddings, 0..) |embedding, i| {
            explicit_sparse[i] = .{
                .index_name = embedding.index_name,
                .doc_key = embedding.doc_key,
                .indices = embedding.indices,
                .values = embedding.values,
            };
        }

        return try self.index_manager.planGeneratedEnrichments(
            alloc,
            doc_key,
            cleaned,
            explicit_dense,
            explicit_sparse,
        );
    }

    pub fn hasGeneratedEnrichmentTargets(self: *DBCore) bool {
        return self.index_manager.hasGeneratedEnrichmentTargets();
    }

    pub fn textIndexEntry(self: *DBCore, name: ?[]const u8) ?*index_manager_mod.IndexManager.TextIndex {
        return self.index_manager.textIndexEntry(name);
    }

    pub fn textIndex(self: *DBCore, name: ?[]const u8) ?*persistent_mod.PersistentIndex {
        return self.index_manager.textIndex(name);
    }

    pub fn selectedTextChunkName(self: *DBCore, name: ?[]const u8) ?[]const u8 {
        return self.index_manager.selectedTextChunkName(name);
    }

    pub fn textIndexIsChunkBacked(self: *DBCore, alloc: Allocator, name: ?[]const u8) !bool {
        return try self.index_manager.textIndexIsChunkBacked(alloc, name);
    }

    pub fn denseIndex(self: *DBCore, name: ?[]const u8) ?*index_manager_mod.IndexManager.DenseIndex {
        return self.index_manager.denseIndex(name);
    }

    pub fn sparseIndex(self: *DBCore, name: ?[]const u8) ?*index_manager_mod.IndexManager.SparseIndex {
        return self.index_manager.sparseIndex(name);
    }

    pub fn graphIndex(self: *DBCore, name: ?[]const u8) ?*index_manager_mod.IndexManager.GraphIndex {
        return self.index_manager.graphIndex(name);
    }

    pub fn hasGraphIndexes(self: *DBCore) bool {
        return self.index_manager.hasGraphIndexes();
    }

    pub fn graphIndexes(self: *DBCore) []const index_manager_mod.IndexManager.GraphIndex {
        return self.index_manager.graphIndexes();
    }

    pub fn hasManagedIndexes(self: *DBCore) bool {
        return self.index_manager.hasManagedIndexes();
    }

    pub fn managedIndexes(self: *DBCore, alloc: Allocator) ![]index_manager_mod.ManagedIndexRef {
        return try self.index_manager.managedIndexes(alloc);
    }

    pub fn loadAppliedSequence(self: *DBCore, alloc: Allocator, index_name: []const u8) !u64 {
        return try apply_state.loadAppliedSequenceWithCheckpoint(
            alloc,
            self.store,
            self.applied_sequence_checkpoint_path,
            index_name,
        );
    }

    pub fn indexRequiresEnrichmentReplay(self: *DBCore, index_name: []const u8) !bool {
        return try self.index_manager.requiresEnrichmentReplay(index_name);
    }

    pub fn saveAppliedSequence(self: *DBCore, index_name: []const u8, sequence: u64) !void {
        try apply_state.saveAppliedSequenceWithCheckpoint(
            self.alloc,
            self.store,
            self.applied_sequence_checkpoint_path,
            index_name,
            sequence,
        );
    }

    pub fn hasArtifactCleanupMaybe(self: *const DBCore) bool {
        return self.artifact_cleanup_maybe.load(.acquire) or
            self.index_manager.hasGeneratedEnrichmentTargets();
    }

    pub fn appendArtifactPresenceMarker(
        self: *DBCore,
        writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    ) !void {
        self.artifact_cleanup_maybe.store(true, .release);
        try writes.append(self.alloc, .{
            .key = internal_keys.artifact_presence_key[0..],
            .value = "1",
        });
    }

    pub fn collectAndDeleteEnrichmentArtifactsForDocContext(
        self: *DBCore,
        alloc: Allocator,
        doc_key: []const u8,
        deleted: *std.ArrayListUnmanaged([]u8),
    ) !void {
        if (!self.hasArtifactCleanupMaybe()) return;

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(alloc);
        var unrecorded_delete_keys = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (unrecorded_delete_keys.items) |key| alloc.free(key);
            unrecorded_delete_keys.deinit(alloc);
        }

        const artifact_prefix = try internal_keys.artifactRootPrefixAlloc(alloc, doc_key);
        defer alloc.free(artifact_prefix);
        try self.collectDeleteKeysForPrefix(alloc, artifact_prefix, &deletes, deleted, &unrecorded_delete_keys);

        const asset_state_prefix = try internal_keys.assetStateRootPrefixAlloc(alloc, doc_key);
        defer alloc.free(asset_state_prefix);
        try self.collectDeleteKeysForPrefix(alloc, asset_state_prefix, &deletes, null, &unrecorded_delete_keys);

        const graph_asset_state_prefix = try internal_keys.graphAssetStateRootPrefixAlloc(alloc, doc_key);
        defer alloc.free(graph_asset_state_prefix);
        try self.collectDeleteKeysForPrefix(alloc, graph_asset_state_prefix, &deletes, null, &unrecorded_delete_keys);

        if (deletes.items.len > 0) {
            try self.putStoreBatch(&.{}, deletes.items);
        }
    }

    fn collectDeleteKeysForPrefix(
        self: *DBCore,
        alloc: Allocator,
        prefix: []const u8,
        deletes: *std.ArrayListUnmanaged([]const u8),
        recorded: ?*std.ArrayListUnmanaged([]u8),
        unrecorded: *std.ArrayListUnmanaged([]u8),
    ) !void {
        const existing = try self.scanStorePrefix(alloc, prefix);
        defer docstore_mod.DocStore.freeResults(alloc, existing);
        for (existing) |entry| {
            const owned = try alloc.dupe(u8, entry.key);
            errdefer alloc.free(owned);
            try deletes.append(alloc, owned);
            if (recorded) |out| {
                try out.append(alloc, owned);
            } else {
                try unrecorded.append(alloc, owned);
            }
        }
    }

    pub fn loadIndexes(self: *DBCore) !void {
        try self.index_manager.load(self.store);
    }

    pub fn loadIndexesNoBackfill(self: *DBCore) !void {
        try self.index_manager.loadNoBackfill(self.store);
    }

    pub fn loadIndexCatalogOnly(self: *DBCore) !void {
        try self.index_manager.loadCatalogOnly(self.store);
    }

    pub fn runTransactionRecoveryOnce(
        self: *DBCore,
        alloc: Allocator,
        config: transaction_runtime_mod.Config,
    ) !types.TransactionRecoveryStats {
        var identity_ctx = TransactionRecoveryIdentityContext{
            .store = self.store,
            .identity_namespace = self.identity_namespace,
            .alloc = alloc,
        };
        var effective_config = config;
        effective_config.resolution_extra_hooks = transactionRecoveryIdentityHooks(&identity_ctx);
        return try transaction_runtime_mod.recoverOnce(alloc, self.store, effective_config);
    }

    pub fn writeSnapshot(self: *DBCore, snapshot_root: []const u8) !u64 {
        var total: u64 = 0;
        total += try writeStoreSnapshot(self.alloc, self.store, snapshot_root);
        return total;
    }

    pub fn syncStore(self: *DBCore, full: bool) !void {
        try self.store.sync(full);
    }

    pub fn getStoreValue(self: *DBCore, alloc: Allocator, key: []const u8) !?[]u8 {
        return self.store.get(alloc, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }

    pub fn putStoreBatch(self: *DBCore, writes: []const docstore_mod.KVPair, deletes: []const []const u8) !void {
        try self.store.putBatch(writes, deletes);
    }

    pub fn scanStorePrefix(self: *DBCore, alloc: Allocator, prefix: []const u8) ![]docstore_mod.OwnedKVPair {
        return try self.store.scanPrefix(alloc, prefix);
    }

    pub fn scanStoreRange(self: *DBCore, alloc: Allocator, lower: []const u8, upper: []const u8) ![]docstore_mod.OwnedKVPair {
        return try self.store.scanRange(alloc, lower, upper);
    }

    pub fn findMedianStoreKey(
        self: *DBCore,
        alloc: Allocator,
        lower: []const u8,
        upper: []const u8,
        options: docstore_mod.DocStore.ScanOptions,
    ) ![]u8 {
        return try self.store.findMedianKey(alloc, lower, upper, options);
    }

    pub fn readTimestamp(self: *DBCore, alloc: Allocator, key: []const u8) !u64 {
        return (try ttl_mod.readTimestamp(self.store, alloc, key)) orelse 0;
    }

    pub fn setSchema(self: *DBCore, table_schema: schema_mod.TableSchema) !void {
        try schema_mod.saveSchema(self.store, self.alloc, table_schema);
        if (self.schema) |existing| schema_mod.freeSchema(self.alloc, existing);
        self.schema = try schema_mod.loadSchema(self.store, self.alloc);
    }

    pub fn saveSchemaCloneTo(self: *DBCore, dest_store: *docstore_mod.DocStore) !void {
        try schema_mod.copySchemas(self.store, dest_store, self.alloc);
    }

    pub fn pruneSplitRangeFromPrimaryIndexes(
        self: *DBCore,
        split_key: []const u8,
        original_range_end: []const u8,
    ) !void {
        try self.index_manager.pruneTextSplitRange(split_key);
        try self.index_manager.pruneDenseSplitRange(self.store, split_key);
        try self.index_manager.pruneSparseSplitRange(split_key, original_range_end);
        try self.index_manager.pruneGraphSplitRange(split_key, original_range_end);
    }

    pub fn splitRightStoreToDir(self: *DBCore, split_lower: []const u8, dest_dir: []const u8) !bool {
        return self.store.splitRightToDir(split_lower, dest_dir) catch |err| switch (err) {
            error.Incompatible => false,
            error.Unsupported => false,
            else => return err,
        };
    }

    pub fn rewriteLeftStoreInPlace(self: *DBCore, split_lower: []const u8) !bool {
        return self.store.rewriteLeftInPlace(split_lower) catch |err| switch (err) {
            error.Incompatible => false,
            error.Unsupported => false,
            else => return err,
        };
    }

    pub fn collectSplitIndexHandoffs(
        self: *DBCore,
        dest_indexes: *index_manager_mod.IndexManager,
        dest_store: *docstore_mod.DocStore,
        split_doc_frontier: []const []const u8,
        byte_range: types.ByteRange,
        collect_skip_doc_keys: bool,
    ) !SplitIndexHandoffs {
        return .{
            .dense = try dest_indexes.handoffDenseFrom(
                self.index_manager,
                dest_store,
                byte_range.start,
                collect_skip_doc_keys,
            ),
            .text = try dest_indexes.handoffRightOnlyTextSegmentsFrom(
                self.index_manager,
                byte_range.start,
                collect_skip_doc_keys,
            ),
            .sparse = if (split_doc_frontier.len > 0)
                try dest_indexes.handoffSparseFromPreparedDocIds(
                    self.index_manager,
                    split_doc_frontier,
                    byte_range.start,
                    byte_range.end,
                    collect_skip_doc_keys,
                )
            else
                try dest_indexes.handoffSparseFrom(
                    self.index_manager,
                    byte_range.start,
                    byte_range.end,
                    collect_skip_doc_keys,
                ),
        };
    }

    pub fn graphGetEdges(
        self: *DBCore,
        alloc: Allocator,
        index_name: []const u8,
        key: []const u8,
        edge_type: []const u8,
        direction: graph_mod.EdgeDirection,
    ) ![]graph_mod.Edge {
        const entry = self.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
        return try entry.index.getEdges(alloc, key, edge_type, direction);
    }

    pub fn graphTraverseEdges(
        self: *DBCore,
        alloc: Allocator,
        index_name: []const u8,
        start_key: []const u8,
        rules: traversal_mod.TraversalRules,
    ) ![]traversal_mod.TraversalResult {
        const entry = self.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
        return try traversal_mod.traverse(alloc, &entry.index, start_key, rules);
    }

    pub fn graphFindShortestPath(
        self: *DBCore,
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
        const entry = self.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
        return try paths_mod.findShortestPath(alloc, &entry.index, source, target, .{
            .weight_mode = weight_mode,
            .edge_types = edge_types,
            .direction = direction,
            .max_depth = max_depth,
            .min_weight = min_weight,
            .max_weight = max_weight,
        });
    }

    pub fn graphFindKShortestPaths(
        self: *DBCore,
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
        const entry = self.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
        return try paths_mod.findKShortestPaths(alloc, &entry.index, source, target, k, .{
            .weight_mode = weight_mode,
            .edge_types = edge_types,
            .direction = direction,
            .max_depth = max_depth,
            .min_weight = min_weight,
            .max_weight = max_weight,
        });
    }

    pub fn graphMatchPattern(
        self: *DBCore,
        alloc: Allocator,
        index_name: []const u8,
        start_keys: []const []const u8,
        pattern: []const graph_pattern_mod.PatternStep,
        opts: graph_pattern_mod.MatchOptions,
    ) ![]graph_pattern_mod.PatternMatch {
        const entry = self.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
        return try graph_pattern_mod.matchPattern(alloc, &entry.index, start_keys, pattern, opts);
    }

    pub fn documentRangeLowerAlloc(self: *DBCore, raw_key: []const u8) ![]u8 {
        return try internal_keys.documentRangeLowerAlloc(self.alloc, raw_key);
    }

    pub fn documentRangeUpperAlloc(self: *DBCore, raw_key: []const u8) !?[]u8 {
        return try internal_keys.documentRangeUpperAlloc(self.alloc, raw_key);
    }

    pub fn validateKeyOwnership(self: *DBCore, key: []const u8) !void {
        self.shard_manager.validateKeyOwnership(key) catch |err| switch (err) {
            error.KeyOutOfRange => {
                const split_state = self.shard_manager.getSplitState() orelse return error.KeyOutOfRange;
                if (split_state.phase == .splitting) {
                    const original_range = docstore_mod.ByteRange{
                        .start = self.shard_manager.getByteRange().start,
                        .end = split_state.original_range_end,
                    };
                    if (original_range.contains(key)) return;
                }
                return error.KeyOutOfRange;
            },
            error.SplitInProgress => return error.SplitInProgress,
        };
    }

    pub fn validateBatchRangeOwnership(self: *DBCore, req: types.BatchRequest) !void {
        for (req.writes) |write| {
            try self.validateKeyOwnership(write.key);
        }
        for (req.deletes) |key| {
            try self.validateKeyOwnership(key);
        }
        for (req.graph_writes) |write| {
            try self.validateKeyOwnership(write.source);
            try self.validateKeyOwnership(write.target);
        }
        for (req.graph_deletes) |delete| {
            try self.validateKeyOwnership(delete.source);
            try self.validateKeyOwnership(delete.target);
        }
        for (req.predicates) |predicate| {
            try self.validateKeyOwnership(predicate.key);
        }
    }

    fn initTxnManager(self: *DBCore) !transactions_mod.TxnManager {
        return try transactions_mod.TxnManager.init(self.alloc, self.store);
    }

    pub fn beginTransactionWithParticipants(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        timestamp_ns: u64,
        participants: []const []const u8,
    ) !transactions_mod.TxnId {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.initTransactionWithParticipants(txn_id, timestamp_ns, participants);
        return txn_id;
    }

    pub fn writeIntents(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        intents: []const transactions_mod.WriteIntent,
        predicates: []const transactions_mod.VersionPredicate,
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.writeIntents(txn_id, intents, predicates);
    }

    pub fn checkVersionPredicates(
        self: *DBCore,
        predicates: []const transactions_mod.VersionPredicate,
        exclude_txn_id: ?transactions_mod.TxnId,
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.checkVersionPredicates(predicates, exclude_txn_id);
    }

    pub fn resolveTransactionIntents(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        status: transactions_mod.TxnStatus,
        commit_version: u64,
    ) !void {
        try self.resolveTransactionIntentsWithExtraBatch(txn_id, status, commit_version, .{});
    }

    pub fn resolveTransactionIntentsWithExtraBatch(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        status: transactions_mod.TxnStatus,
        commit_version: u64,
        extra_batch: transactions_mod.ResolutionExtraBatch,
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.resolveIntentsWithExtraBatch(txn_id, status, commit_version, extra_batch);
    }

    pub fn collectTransactionIntentDocumentKeys(
        self: *DBCore,
        alloc: Allocator,
        txn_id: transactions_mod.TxnId,
        upserts: *std.ArrayListUnmanaged([]const u8),
        deletes: *std.ArrayListUnmanaged([]const u8),
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.collectIntentDocumentKeys(alloc, txn_id, upserts, deletes);
    }

    pub fn getTransactionStatus(self: *DBCore, txn_id: transactions_mod.TxnId) !transactions_mod.TxnStatus {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.getTransactionStatus(txn_id);
    }

    pub fn getCommitVersion(self: *DBCore, txn_id: transactions_mod.TxnId) !u64 {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.getCommitVersion(txn_id);
    }

    pub fn markTransactionParticipantResolved(self: *DBCore, txn_id: transactions_mod.TxnId, participant: []const u8) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.markParticipantResolved(txn_id, participant);
    }

    pub fn getTransactionParticipants(self: *DBCore, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.getParticipants(alloc, txn_id);
    }

    pub fn getUnresolvedTransactionParticipants(self: *DBCore, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.getUnresolvedParticipants(alloc, txn_id);
    }

    pub fn recoverTransactions(self: *DBCore, cutoff_timestamp: u64, resolution_timestamp: u64) !transactions_mod.RecoveryStats {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        var identity_ctx = TransactionRecoveryIdentityContext{
            .store = self.store,
            .identity_namespace = self.identity_namespace,
            .alloc = self.alloc,
        };
        return try manager.recoverTransactionsWithExtraBatchHooks(
            cutoff_timestamp,
            resolution_timestamp,
            transactionRecoveryIdentityHooks(&identity_ctx),
        );
    }
};

pub const TransactionRecoveryIdentityContext = struct {
    store: *docstore_mod.DocStore,
    identity_namespace: doc_identity.Namespace,
    alloc: Allocator,
};

pub fn transactionRecoveryIdentityHooks(ctx: *TransactionRecoveryIdentityContext) transactions_mod.TxnManager.RecoveryExtraBatchHooks {
    return .{
        .ctx = ctx,
        .build = buildTransactionRecoveryIdentityExtraBatch,
        .cleanup = cleanupTransactionRecoveryIdentityExtraBatch,
    };
}

fn transactionIdentityMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
        std.mem.startsWith(u8, key, "splitstate:") or
        std.mem.startsWith(u8, key, "splitdelta:") or
        internal_keys.isTtlKey(key);
}

fn buildTransactionRecoveryIdentityExtraBatch(
    ctx: ?*anyopaque,
    manager: *transactions_mod.TxnManager,
    txn_id: transactions_mod.TxnId,
    status: transactions_mod.TxnStatus,
    timestamp: u64,
) anyerror!transactions_mod.ResolutionExtraBatch {
    _ = timestamp;
    if (status != .committed) return .{};
    const identity_ctx: *TransactionRecoveryIdentityContext = @ptrCast(@alignCast(ctx.?));
    const alloc = identity_ctx.alloc;

    var raw_upserts = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (raw_upserts.items) |key| alloc.free(@constCast(key));
        raw_upserts.deinit(alloc);
    }
    var raw_deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (raw_deletes.items) |key| alloc.free(@constCast(key));
        raw_deletes.deinit(alloc);
    }
    try manager.collectIntentDocumentKeys(alloc, txn_id, &raw_upserts, &raw_deletes);

    var identity_upserts = std.ArrayListUnmanaged([]const u8).empty;
    defer identity_upserts.deinit(alloc);
    var identity_deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer identity_deletes.deinit(alloc);
    for (raw_upserts.items) |key| {
        if (!transactionIdentityMetadataKey(key)) try identity_upserts.append(alloc, key);
    }
    for (raw_deletes.items) |key| {
        if (!transactionIdentityMetadataKey(key)) try identity_deletes.append(alloc, key);
    }

    var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    errdefer {
        for (identity_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        identity_writes.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataForNamespaceAlloc(
        alloc,
        identity_ctx.store,
        identity_ctx.identity_namespace,
        identity_ctx.store.lastReplaySequence(0),
        &identity_writes,
        identity_upserts.items,
        identity_deletes.items,
    );
    if (identity_writes.items.len == 0) return .{};
    return .{
        .writes = try identity_writes.toOwnedSlice(alloc),
    };
}

fn cleanupTransactionRecoveryIdentityExtraBatch(ctx: ?*anyopaque, batch: transactions_mod.ResolutionExtraBatch) void {
    const identity_ctx: *TransactionRecoveryIdentityContext = @ptrCast(@alignCast(ctx.?));
    const alloc = identity_ctx.alloc;
    for (batch.writes) |item| {
        alloc.free(@constCast(item.key));
        alloc.free(@constCast(item.value));
    }
    if (batch.writes.len > 0) alloc.free(@constCast(batch.writes));
    if (batch.deletes.len > 0) alloc.free(@constCast(batch.deletes));
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        std.atomic.spinLoopHint();
    }
}

pub const Engine = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        batch: *const fn (ptr: *anyopaque, req: types.BatchRequest) anyerror!void,
        lookup: *const fn (ptr: *anyopaque, alloc: Allocator, key: []const u8, opts: types.LookupOptions) anyerror!?types.LookupResult,
        scan: *const fn (ptr: *anyopaque, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) anyerror!types.ScanResult,
        search: *const fn (ptr: *anyopaque, alloc: Allocator, req: types.SearchRequest) anyerror!types.SearchResult,
        stats: *const fn (ptr: *anyopaque, alloc: Allocator) anyerror!types.DBStats,
        list_indexes: *const fn (ptr: *anyopaque, alloc: Allocator) anyerror![]types.IndexConfig,
        list_enrichments: *const fn (ptr: *anyopaque, alloc: Allocator) anyerror![]types.EnrichmentConfig,
    };

    pub fn batch(self: Engine, req: types.BatchRequest) !void {
        return try self.vtable.batch(self.ptr, req);
    }

    pub fn lookup(self: Engine, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
        return try self.vtable.lookup(self.ptr, alloc, key, opts);
    }

    pub fn scan(self: Engine, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !types.ScanResult {
        return try self.vtable.scan(self.ptr, alloc, from_key, to_key, opts);
    }

    pub fn search(self: Engine, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
        return try self.vtable.search(self.ptr, alloc, req);
    }

    pub fn stats(self: Engine, alloc: Allocator) !types.DBStats {
        return try self.vtable.stats(self.ptr, alloc);
    }

    pub fn listIndexes(self: Engine, alloc: Allocator) ![]types.IndexConfig {
        return try self.vtable.list_indexes(self.ptr, alloc);
    }

    pub fn listEnrichments(self: Engine, alloc: Allocator) ![]types.EnrichmentConfig {
        return try self.vtable.list_enrichments(self.ptr, alloc);
    }
};

const IndexBackendOptions = db_config.IndexBackendOptions;

pub fn changeJournalOpenOptionsForPrimaryKind(
    map_size: usize,
    no_sync: bool,
    primary_backend_kind: PrimaryBackendKind,
    primary_lsm_storage: ?lsm_backend_mod.Storage,
    backend_override: ?change_journal_mod.StorageBackend,
    storage_override: ?lsm_backend_mod.Storage,
) change_journal_mod.OpenOptions {
    const backend: change_journal_mod.StorageBackend = backend_override orelse switch (primary_backend_kind) {
        .mem, .lsm_memory => .lsm_memory,
        .lmdb, .lsm => .lsm,
    };
    return .{
        .map_size = map_size,
        .no_sync = no_sync,
        .backend = backend,
        .storage = storage_override orelse if (backend == .lsm and primary_backend_kind == .lsm) primary_lsm_storage else null,
    };
}

pub fn openCoreResourcesFromPrimaryStore(
    alloc: Allocator,
    path: []const u8,
    map_size: usize,
    no_sync: bool,
    primary_backend_kind: PrimaryBackendKind,
    primary_lsm_storage: ?lsm_backend_mod.Storage,
    change_journal_backend: ?change_journal_mod.StorageBackend,
    change_journal_storage: ?lsm_backend_mod.Storage,
    index_backends: IndexBackendOptions,
    opened_primary: OpenedPrimaryStore,
    configured_identity_namespace: ?doc_identity.Namespace,
    persist_identity_namespace_if_missing: bool,
    identity_namespace_mismatch_policy: doc_identity.NamespaceMismatchPolicy,
) !OpenedCoreResources {
    var owned_path: ?[]u8 = null;
    var owned_applied_sequence_checkpoint_path: ?[]u8 = null;
    var owned_store: ?*docstore_mod.DocStore = null;
    var owned_primary_store_owner = opened_primary.owner;
    var owned_change_journal: ?*change_journal_mod.Journal = null;
    var owned_shard_manager: ?*shard_mod.ShardManager = null;
    var owned_index_manager: ?*index_manager_mod.IndexManager = null;
    var owned_apply_mutex: ?*apply_rw_lock_mod.ApplyRwLock = null;
    var owned_log_mutex: ?*std.atomic.Mutex = null;
    errdefer {
        if (owned_log_mutex) |ptr| alloc.destroy(ptr);
        if (owned_apply_mutex) |ptr| alloc.destroy(ptr);
        if (owned_index_manager) |ptr| alloc.destroy(ptr);
        if (owned_shard_manager) |ptr| alloc.destroy(ptr);
        if (owned_change_journal) |ptr| alloc.destroy(ptr);
        if (owned_store) |ptr| {
            ptr.close();
            alloc.destroy(ptr);
        }
        owned_primary_store_owner.close(alloc);
        if (owned_applied_sequence_checkpoint_path) |buf| alloc.free(buf);
        if (owned_path) |buf| alloc.free(buf);
    }

    const store = try alloc.create(docstore_mod.DocStore);
    store.* = opened_primary.store;
    owned_store = store;

    const change_journal = try alloc.create(change_journal_mod.Journal);
    owned_change_journal = change_journal;
    const shard_manager = try alloc.create(shard_mod.ShardManager);
    owned_shard_manager = shard_manager;
    const index_manager = try alloc.create(index_manager_mod.IndexManager);
    owned_index_manager = index_manager;
    const apply_mutex = try alloc.create(apply_rw_lock_mod.ApplyRwLock);
    apply_mutex.* = .{};
    owned_apply_mutex = apply_mutex;
    const log_mutex = try alloc.create(std.atomic.Mutex);
    log_mutex.* = .unlocked;
    owned_log_mutex = log_mutex;
    const path_copy = try alloc.dupe(u8, path);
    owned_path = path_copy;
    const applied_sequence_checkpoint_path = switch (primary_backend_kind) {
        .lmdb, .lsm => try apply_state.checkpointPathAlloc(alloc, path),
        .mem, .lsm_memory => null,
    };
    owned_applied_sequence_checkpoint_path = applied_sequence_checkpoint_path;

    const change_journal_path = try std.fmt.allocPrint(alloc, "{s}/change_journal", .{path});
    defer alloc.free(change_journal_path);
    const change_journal_path_z = try alloc.dupeZ(u8, change_journal_path);
    defer alloc.free(change_journal_path_z);
    change_journal.* = try change_journal_mod.Journal.open(
        change_journal_path_z,
        changeJournalOpenOptionsForPrimaryKind(
            map_size,
            no_sync,
            primary_backend_kind,
            primary_lsm_storage,
            change_journal_backend,
            change_journal_storage,
        ),
    );
    errdefer {
        change_journal.close();
    }
    const persisted_range = try range_state_mod.loadRange(alloc, store);
    defer range_state_mod.freeRange(alloc, persisted_range);
    shard_manager.* = try shard_mod.ShardManager.init(alloc, store, persisted_range);
    const identity_namespace = try doc_identity.loadOrInitNamespaceWithPolicy(
        store,
        configured_identity_namespace,
        persist_identity_namespace_if_missing,
        identity_namespace_mismatch_policy,
    );
    const artifact_cleanup_maybe = try loadArtifactCleanupMaybe(alloc, store);

    index_manager.* = try index_manager_mod.IndexManager.initWithOptions(
        alloc,
        path,
        index_backends,
    );
    index_manager.setAppliedSequenceCheckpointPath(applied_sequence_checkpoint_path);
    index_manager.updateRange(shard_manager.getByteRange());

    const schema = try schema_mod.loadSchema(store, alloc);

    owned_path = null;
    owned_applied_sequence_checkpoint_path = null;
    owned_store = null;
    owned_primary_store_owner = .none;
    owned_change_journal = null;
    owned_shard_manager = null;
    owned_index_manager = null;
    owned_apply_mutex = null;
    owned_log_mutex = null;

    return .{
        .path = path_copy,
        .applied_sequence_checkpoint_path = applied_sequence_checkpoint_path,
        .store = store,
        .primary_store_owner = opened_primary.owner,
        .change_journal = change_journal,
        .shard_manager = shard_manager,
        .index_manager = index_manager,
        .apply_mutex = apply_mutex,
        .log_mutex = log_mutex,
        .schema = schema,
        .identity_namespace = identity_namespace,
        .artifact_cleanup_maybe = artifact_cleanup_maybe,
    };
}

fn loadArtifactCleanupMaybe(alloc: Allocator, store: *docstore_mod.DocStore) !bool {
    const marker = store.get(alloc, internal_keys.artifact_presence_key[0..]) catch |err| switch (err) {
        error.NotFound => return try hasAnyUserNamespaceKey(store),
        else => return err,
    };
    alloc.free(marker);
    return true;
}

fn hasAnyUserNamespaceKey(store: *docstore_mod.DocStore) !bool {
    const State = struct {
        found: bool = false,

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            _ = key;
            _ = value;
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            state.found = true;
            return .stop;
        }
    };

    const lower = [_]u8{internal_keys.user_namespace};
    const upper = [_]u8{internal_keys.user_namespace + 1};
    var state = State{};
    try store.scanWithContext(lower[0..], upper[0..], .{}, &state, State.scanEntry);
    return state.found;
}

pub fn clearAllKeysFromStore(alloc: Allocator, store: *docstore_mod.DocStore) !void {
    const keys = try store.scanRange(alloc, "", "");
    defer docstore_mod.DocStore.freeResults(alloc, keys);
    if (keys.len == 0) return;

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    for (keys) |item| {
        try deletes.append(alloc, item.key);
    }
    try store.putBatch(&.{}, deletes.items);
}

pub fn importStoreSnapshot(alloc: Allocator, store: *docstore_mod.DocStore, snapshot_root: []const u8) !void {
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{snapshot_root});
    defer alloc.free(snapshot_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), snapshot_path, alloc, .limited(256 * 1024 * 1024));
    defer alloc.free(raw);

    var decoded = try lsm_table_file.decodeAlloc(alloc, raw);
    defer decoded.deinit(alloc);

    var writes = try alloc.alloc(docstore_mod.KVPair, decoded.entries.len);
    defer alloc.free(writes);
    for (decoded.entries, 0..) |entry, i| {
        writes[i] = .{
            .key = entry.key,
            .value = entry.value,
        };
    }
    try store.putBatch(writes, &.{});
}

pub fn importChangeJournalSnapshot(alloc: Allocator, store: *docstore_mod.DocStore, snapshot_root: []const u8) !void {
    return try importOpaqueLogSnapshot(alloc, store, snapshot_root, "change-journal.bin");
}

fn importOpaqueLogSnapshot(alloc: Allocator, store: *docstore_mod.DocStore, snapshot_root: []const u8, file_name: []const u8) !void {
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, file_name });
    defer alloc.free(snapshot_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const raw = std.Io.Dir.cwd().readFileAlloc(io, snapshot_path, alloc, .limited(256 * 1024 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer alloc.free(raw);

    if (raw.len < 8) return error.InvalidTableFile;
    var cursor: usize = 0;
    const entry_count = std.mem.readInt(u64, raw[cursor .. cursor + 8][0..8], .little);
    cursor += 8;

    var i: u64 = 0;
    while (i < entry_count) : (i += 1) {
        if (cursor + 16 > raw.len) return error.InvalidTableFile;
        const sequence = std.mem.readInt(u64, raw[cursor .. cursor + 8][0..8], .little);
        cursor += 8;
        const payload_len: usize = @intCast(std.mem.readInt(u64, raw[cursor .. cursor + 8][0..8], .little));
        cursor += 8;
        if (cursor + payload_len > raw.len) return error.InvalidTableFile;
        try store.appendReplayOpaque(alloc, sequence, raw[cursor .. cursor + payload_len]);
        cursor += payload_len;
    }
    if (cursor != raw.len) return error.InvalidTableFile;
}

fn writeStoreSnapshot(alloc: Allocator, store: *docstore_mod.DocStore, snapshot_root: []const u8) !u64 {
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{snapshot_root});
    defer alloc.free(snapshot_path);

    const docs = try store.scanRange(alloc, "", "");
    defer docstore_mod.DocStore.freeResults(alloc, docs);

    var entries = try alloc.alloc(lsm_table_file.Entry, docs.len);
    defer alloc.free(entries);
    for (docs, 0..) |doc, i| {
        entries[i] = .{
            .namespace_name = null,
            .key = doc.key,
            .value = doc.value,
            .tombstone = false,
        };
    }

    const encoded = try lsm_table_file.encodeAlloc(alloc, entries);
    defer alloc.free(encoded);
    try writeFileAbsolute(snapshot_path, encoded);
    return encoded.len;
}

fn threadedIo() if (builtin.os.tag == .freestanding) void else std.Io.Threaded {
    if (builtin.os.tag == .freestanding) return;
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn writeFileAbsolute(path: []const u8, data: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);

    var writer_buf: [1024]u8 = undefined;
    var writer = file.writer(io, &writer_buf);
    try writer.interface.writeAll(data);
    try writer.flush();
}
