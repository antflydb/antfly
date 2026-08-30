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
const snapshot_admission_mod = @import("snapshot_admission.zig");
const apply_state = @import("derived/apply_state.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const doc_identity = @import("doc_identity.zig");
const range_cardinality = @import("range_cardinality.zig");
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
const native_artifact_sink = @import("../native_artifact_sink.zig");
const lsm_table_file = @import("../lsm/table_file.zig");
const graph_mod = @import("../../graph/graph.zig");
const NodeAdmission = @import("../../graph/node_admission.zig").NodeAdmission;
const graph_pattern_mod = @import("../../graph/pattern.zig");
const paths_mod = @import("../../graph/paths.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");

const store_snapshot_file_name = "store.bin";
const store_snapshot_v2_magic = "AFSTKV02";
const logical_store_artifact_format = "antfly-kv-stream";
const logical_store_artifact_version: u32 = 2;
pub const logical_snapshot_manifest_file_name = "SNAPSHOT.json";
const logical_snapshot_manifest_format_version: u32 = 1;
const store_snapshot_batch_entries: usize = 8192;
const store_snapshot_batch_bytes: usize = 8 * 1024 * 1024;
const legacy_store_snapshot_max_bytes: usize = 256 * 1024 * 1024;
const store_snapshot_max_field_bytes: u64 = 1024 * 1024 * 1024;
pub const primary_lsm_checkpoint_directory_name = "primary-lsm";

pub const PrimaryBackendKind = db_config.PrimaryBackendKind;
pub const PrimaryBackend = db_config.PrimaryBackend;
pub const CoreOpenOptions = db_config.CoreOpenOptions;

pub const PendingWorkStats = struct {
    derived_target_sequence: u64,
    has_async_indexes: bool,
    enrichment: types.EnrichmentStats,
    resolution: types.ReplayStageStats = .{},
    promotion: types.ReplayStageStats = .{},
    text_merge: types.TextMergeStats = .{},
    repair_metadata_rebuild_pending: bool = false,
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

    pub fn nextLsmMaintenanceWakeDelayNsBestEffort(self: *const PrimaryStoreOwner) ?u64 {
        return switch (self.*) {
            .none, .mem => null,
            .lsm => |owner| owner.handle.backend.nextMaintenanceWakeDelayNsBestEffort(),
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

    pub fn snapshotLsmOpenStats(self: *const PrimaryStoreOwner) ?lsm_backend_mod.Backend.OpenStats {
        return switch (self.*) {
            .none, .mem => null,
            .lsm => |owner| owner.handle.backend.snapshotOpenStats(),
        };
    }

    pub fn checkpointLsmWalAfterDurableBoundary(self: *PrimaryStoreOwner) !void {
        switch (self.*) {
            .none, .mem => {},
            .lsm => |owner| try owner.handle.backend.checkpointWalAfterDurableBoundary(),
        }
    }

    pub fn snapshotLsmNativeStorageStats(self: *const PrimaryStoreOwner) ?lsm_backend_mod.NativeStorageStats {
        return switch (self.*) {
            .none, .mem => null,
            .lsm => |owner| owner.handle.backend.snapshotNativeStorageStats(),
        };
    }

    pub fn lsmBackend(self: *PrimaryStoreOwner) ?*lsm_backend_mod.Backend {
        return switch (self.*) {
            .none, .mem => null,
            .lsm => |owner| owner.handle.backend,
        };
    }

    pub fn runLsmMaintenanceStep(self: *PrimaryStoreOwner) !bool {
        return switch (self.*) {
            .none, .mem => false,
            .lsm => |owner| try owner.handle.backend.runMaintenanceStep(),
        };
    }

    pub fn runDueLsmObsoleteReclaim(self: *PrimaryStoreOwner) !bool {
        const backend = self.lsmBackend() orelse return false;
        if (backend.snapshotMaintenanceStats().obsolete_paths_reclaimable == 0) return false;
        _ = try backend.runMaintenanceStep();
        return true;
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

pub const IndexRepairCheckpoint = struct {
    lock_key: []u8,
    path: []u8,
    storage: ?lsm_backend_mod.Storage = null,

    pub fn location(self: *const @This()) index_repair_state.Location {
        return .{
            .lock_key = self.lock_key,
            .path = self.path,
            .storage = self.storage,
        };
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.lock_key);
        alloc.free(self.path);
        self.* = undefined;
    }
};

pub const OpenedCoreResources = struct {
    path: []u8,
    root_generation: u64,
    applied_sequence_checkpoint_path: ?[]u8,
    index_repair_checkpoint: ?IndexRepairCheckpoint,
    store: *docstore_mod.DocStore,
    primary_store_owner: PrimaryStoreOwner,
    change_journal: *change_journal_mod.Journal,
    shard_manager: *shard_mod.ShardManager,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    snapshot_admission: *snapshot_admission_mod.SnapshotAdmission,
    snapshot_replay_admission: *snapshot_admission_mod.SnapshotAdmission,
    repair_replay_mutex: *std.atomic.Mutex,
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
        self.snapshot_admission.* = undefined;
        alloc.destroy(self.snapshot_admission);
        self.snapshot_replay_admission.* = undefined;
        alloc.destroy(self.snapshot_replay_admission);
        self.repair_replay_mutex.* = undefined;
        alloc.destroy(self.repair_replay_mutex);
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
        if (self.index_repair_checkpoint) |*checkpoint| checkpoint.deinit(alloc);
        alloc.free(self.path);
        self.* = undefined;
    }
};

pub const AsyncResources = struct {
    store: *docstore_mod.DocStore,
    applied_sequence_checkpoint_path: ?[]const u8,
    index_repair_checkpoint: ?index_repair_state.Location,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    snapshot_replay_admission: *snapshot_admission_mod.SnapshotAdmission,
    repair_replay_mutex: *std.atomic.Mutex,
};

pub const BatchExecutionResources = struct {
    store: *docstore_mod.DocStore,
    applied_sequence_checkpoint_path: ?[]const u8,
    index_repair_checkpoint: ?index_repair_state.Location,
    shard_manager: *shard_mod.ShardManager,
    change_journal: *change_journal_mod.Journal,
    replay_source: replay_source_mod.Source,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    snapshot_admission: *snapshot_admission_mod.SnapshotAdmission,
    snapshot_replay_admission: *snapshot_admission_mod.SnapshotAdmission,
    repair_replay_mutex: *std.atomic.Mutex,
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
    root_generation: u64,
    applied_sequence_checkpoint_path: ?[]u8,
    index_repair_checkpoint: ?IndexRepairCheckpoint,
    store: *docstore_mod.DocStore,
    primary_store_owner: PrimaryStoreOwner,
    change_journal: *change_journal_mod.Journal,
    shard_manager: *shard_mod.ShardManager,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    snapshot_admission: *snapshot_admission_mod.SnapshotAdmission,
    snapshot_replay_admission: *snapshot_admission_mod.SnapshotAdmission,
    repair_replay_mutex: *std.atomic.Mutex,
    log_mutex: *std.atomic.Mutex,
    schema: ?schema_mod.TableSchema,
    identity_namespace: doc_identity.Namespace,
    artifact_cleanup_maybe: std.atomic.Value(bool),

    pub fn fromOpened(alloc: Allocator, opened: OpenedCoreResources) DBCore {
        return .{
            .alloc = alloc,
            .path = opened.path,
            .root_generation = opened.root_generation,
            .applied_sequence_checkpoint_path = opened.applied_sequence_checkpoint_path,
            .index_repair_checkpoint = opened.index_repair_checkpoint,
            .store = opened.store,
            .primary_store_owner = opened.primary_store_owner,
            .change_journal = opened.change_journal,
            .shard_manager = opened.shard_manager,
            .index_manager = opened.index_manager,
            .apply_mutex = opened.apply_mutex,
            .snapshot_admission = opened.snapshot_admission,
            .snapshot_replay_admission = opened.snapshot_replay_admission,
            .repair_replay_mutex = opened.repair_replay_mutex,
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
        self.snapshot_admission.* = undefined;
        self.alloc.destroy(self.snapshot_admission);
        self.snapshot_replay_admission.* = undefined;
        self.alloc.destroy(self.snapshot_replay_admission);
        self.repair_replay_mutex.* = undefined;
        self.alloc.destroy(self.repair_replay_mutex);
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
        if (self.index_repair_checkpoint) |*checkpoint| checkpoint.deinit(self.alloc);
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
            .index_repair_checkpoint = if (self.index_repair_checkpoint) |*checkpoint| checkpoint.location() else null,
            .index_manager = self.index_manager,
            .apply_mutex = self.apply_mutex,
            .snapshot_replay_admission = self.snapshot_replay_admission,
            .repair_replay_mutex = self.repair_replay_mutex,
        };
    }

    pub fn batchExecutionResources(self: *DBCore) BatchExecutionResources {
        return .{
            .store = self.store,
            .applied_sequence_checkpoint_path = self.applied_sequence_checkpoint_path,
            .index_repair_checkpoint = if (self.index_repair_checkpoint) |*checkpoint| checkpoint.location() else null,
            .shard_manager = self.shard_manager,
            .change_journal = self.change_journal,
            .replay_source = self.replaySource(),
            .index_manager = self.index_manager,
            .apply_mutex = self.apply_mutex,
            .snapshot_admission = self.snapshot_admission,
            .snapshot_replay_admission = self.snapshot_replay_admission,
            .repair_replay_mutex = self.repair_replay_mutex,
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
        const start = try self.alloc.dupe(u8, byte_range.start);
        errdefer self.alloc.free(start);
        const end = try self.alloc.dupe(u8, byte_range.end);
        errdefer self.alloc.free(end);
        try range_state_mod.saveRange(self.store, .{ .start = start, .end = end });
        self.adoptRangeInMemoryOwned(start, end);
    }

    pub fn adoptRangeInMemoryOwned(self: *DBCore, start: []u8, end: []u8) void {
        self.shard_manager.replaceByteRangeOwned(start, end);
        self.refreshIndexRange();
    }

    pub fn adoptPersistedRangeOwned(self: *DBCore, start: []u8, end: []u8) void {
        self.shard_manager.adoptOwnedByteRange(start, end);
        self.refreshIndexRange();
    }

    pub fn replaceRangeInMemoryOwned(self: *DBCore, start: []u8, end: []u8) void {
        self.shard_manager.replaceByteRangeOwned(start, end);
        self.refreshIndexRange();
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

    pub fn loadSplitBootstrapMarker(self: *DBCore, alloc: Allocator) !?range_state_mod.SplitBootstrapMarker {
        return try range_state_mod.loadSplitBootstrapMarker(alloc, self.store);
    }

    pub fn saveSplitBootstrapMarker(self: *DBCore, marker: range_state_mod.SplitBootstrapMarker) !void {
        try range_state_mod.saveSplitBootstrapMarker(self.store, marker);
    }

    pub fn clearSplitBootstrapMarker(self: *DBCore) !void {
        try range_state_mod.clearSplitBootstrapMarker(self.store);
    }

    pub fn addIndex(
        self: *DBCore,
        cfg: types.IndexConfig,
        admission: ?index_manager_mod.IndexManager.AtomicCatalogMutation,
    ) !u64 {
        try self.index_manager.addWithAtomicMutation(self.store, cfg, admission);
        const applied = if (try self.index_manager.requiresEnrichmentReplay(cfg.name))
            0
        else
            self.nextDerivedSequence();
        return applied;
    }

    pub fn addManagedIndex(
        self: *DBCore,
        cfg: types.IndexConfig,
        admission: ?index_manager_mod.IndexManager.AtomicCatalogMutation,
    ) !u64 {
        try self.index_manager.addManaged(self.store, cfg, admission);
        // Managed admission reconstructs the pre-admission corpus from its
        // stable source snapshot (and, for generated indexes, durable seed
        // records appended after this fence). Replaying history before the
        // fence is both redundant and unsafe: a later index would otherwise
        // consume obsolete generated records belonging to earlier catalog
        // generations. The admission marker and repair-unavailable gate keep
        // service fail-closed if materialization is interrupted.
        return self.nextDerivedSequence();
    }

    pub fn addEnrichment(self: *DBCore, cfg: types.EnrichmentConfig) !void {
        try self.index_manager.addEnrichment(self.store, cfg);
    }

    pub fn addResolver(self: *DBCore, cfg: index_manager_mod.ResolverConfig) !void {
        try cfg.validate();
        try self.index_manager.addResolver(self.store, cfg);
    }

    /// Add or replace a resolver; tells the caller whether existing extraction
    /// artifacts need replay-driven re-resolution.
    pub fn upsertResolver(self: *DBCore, cfg: index_manager_mod.ResolverConfig) !index_manager_mod.IndexManager.ResolverUpsertResult {
        try cfg.validate();
        return try self.index_manager.upsertResolver(self.store, cfg);
    }

    pub fn removeResolver(self: *DBCore, name: []const u8) !bool {
        return try self.index_manager.removeResolver(self.store, name);
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

    pub fn listResolvers(self: *DBCore, alloc: Allocator) ![]index_manager_mod.ResolverConfig {
        return try self.index_manager.listResolvers(alloc);
    }

    pub fn deleteIndex(self: *DBCore, name: []const u8) !bool {
        return try self.index_manager.remove(self.store, name);
    }

    pub fn deleteManagedIndex(self: *DBCore, name: []const u8, admission_key: []const u8) !bool {
        return try self.index_manager.removeManaged(self.store, name, admission_key);
    }

    pub fn deleteEnrichment(self: *DBCore, kind: types.EnrichmentKind, name: []const u8) !bool {
        return try self.index_manager.removeEnrichment(self.store, kind, name);
    }

    pub fn upsertEnrichment(self: *DBCore, cfg: types.EnrichmentConfig) !index_manager_mod.IndexManager.EnrichmentUpsertResult {
        return try self.index_manager.upsertEnrichment(self.store, cfg);
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

    pub fn textIndexSupportsUnitGrouping(self: *DBCore, name: ?[]const u8) bool {
        return self.index_manager.textIndexSupportsUnitGrouping(name);
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
        if (self.index_manager.denseProjectionCheckpointMetadata(index_name)) |dense_checkpoint| {
            if (dense_checkpoint.config_hash != 0) return dense_checkpoint.applied_sequence;
        }
        return apply_state.loadAppliedSequenceWithCheckpoint(
            alloc,
            self.index_manager.checkpointIo(),
            self.store,
            self.applied_sequence_checkpoint_path,
            index_name,
        ) catch |err| switch (err) {
            error.InvalidDerivedApplyState => 0,
            else => return err,
        };
    }

    pub fn loadProjectionCheckpoint(self: *DBCore, alloc: Allocator, index_name: []const u8) !apply_state.ProjectionCheckpoint {
        if (self.index_manager.denseProjectionCheckpointMetadata(index_name)) |dense_checkpoint| {
            if (dense_checkpoint.config_hash != 0) return dense_checkpoint;
        }
        return apply_state.loadProjectionCheckpointWithSidecar(
            alloc,
            self.index_manager.checkpointIo(),
            self.store,
            self.applied_sequence_checkpoint_path,
            index_name,
        ) catch |err| switch (err) {
            error.InvalidDerivedApplyState => .{
                .status = .repair_required,
                .config_hash = if (self.index_manager.get(index_name)) |cfg| types.indexConfigHash(cfg.*) else 0,
            },
            else => return err,
        };
    }

    pub fn indexRequiresEnrichmentReplay(self: *DBCore, index_name: []const u8) !bool {
        return try self.index_manager.requiresEnrichmentReplay(index_name);
    }

    pub fn saveAppliedSequence(self: *DBCore, index_name: []const u8, sequence: u64) !void {
        const cfg = self.index_manager.get(index_name);
        const config_hash = if (cfg) |value|
            types.indexConfigHash(value.*)
        else
            0;
        if (self.index_manager.denseProjectionCheckpointMetadata(index_name)) |checkpoint| {
            try self.index_manager.saveDenseProjectionCheckpointMetadata(index_name, .{
                .applied_sequence = sequence,
                .status = checkpoint.status,
                .generation = checkpoint.generation,
                .config_hash = if (config_hash != 0) config_hash else checkpoint.config_hash,
            });
            try self.index_manager.checkpointLsmWalForManagedIndex(.{
                .name = index_name,
                .kind = .dense_vector,
            });
        } else if (cfg) |value| {
            try self.index_manager.checkpointLsmWalForManagedIndex(.{
                .name = index_name,
                .kind = value.kind,
            });
        }
        try apply_state.saveAppliedSequenceUpdateWithCheckpoint(
            self.alloc,
            self.index_manager.checkpointIo(),
            self.store,
            self.applied_sequence_checkpoint_path,
            .{
                .index_name = index_name,
                .sequence = sequence,
                .config_hash = config_hash,
            },
        );
    }

    pub fn saveProjectionCheckpoint(self: *DBCore, index_name: []const u8, checkpoint: apply_state.ProjectionCheckpoint) !void {
        var checkpoint_with_identity = checkpoint;
        if (checkpoint_with_identity.config_hash == 0) {
            if (self.index_manager.get(index_name)) |cfg| {
                checkpoint_with_identity.config_hash = types.indexConfigHash(cfg.*);
            }
        }
        if (self.index_manager.denseProjectionCheckpointMetadata(index_name) != null) {
            try self.index_manager.saveDenseProjectionCheckpointMetadata(index_name, checkpoint_with_identity);
            try self.index_manager.checkpointLsmWalForManagedIndex(.{
                .name = index_name,
                .kind = .dense_vector,
            });
        }
        try apply_state.saveProjectionCheckpointWithSidecar(
            self.alloc,
            self.index_manager.checkpointIo(),
            self.store,
            self.applied_sequence_checkpoint_path,
            index_name,
            checkpoint_with_identity,
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

    /// Pins the backend-neutral primary image used by portable snapshots.
    /// Keep this logical even for an LSM-backed source: portable backups are
    /// intentionally restorable into any supported primary backend.
    pub fn pinPortableSnapshot(self: *DBCore) !PinnedStoreSnapshot {
        return .{ .logical = .{ .txn = try self.store.beginReadTxn() } };
    }

    /// Pins the fastest self-contained primary image supported by the active
    /// backend. Backends without a physical checkpoint fall back to the same
    /// bounded streaming image used by portable snapshots.
    pub fn pinNativeSnapshot(self: *DBCore) !PinnedStoreSnapshot {
        switch (self.primary_store_owner) {
            .lsm => |owner| {
                const checkpoint = owner.handle.backend.pinNativeCheckpoint() catch |err| switch (err) {
                    error.Unsupported => return .{ .logical = .{ .txn = try self.store.beginReadTxn() } },
                    else => return err,
                };
                return .{ .lsm = checkpoint };
            },
            .none, .mem => return try self.pinPortableSnapshot(),
        }
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

    pub fn scanStoreRangeWithContext(
        self: *DBCore,
        lower: []const u8,
        upper: []const u8,
        options: docstore_mod.DocStore.ScanOptions,
        ctx: ?*anyopaque,
        callback: docstore_mod.DocStore.ScanWithContextCallback,
    ) !void {
        return try self.store.scanWithContext(lower, upper, options, ctx, callback);
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
        const changed = try schema_mod.saveSchema(self.store, self.alloc, table_schema);
        // Refresh even when the durable value is unchanged. An index may have
        // been provisioned between the original schema commit and this
        // idempotent retry, and empty generations still need the mapping.
        if (!changed) {
            try self.index_manager.refreshEmptyTextIndexSchemas(self.store);
            return;
        }
        const next_schema = try schema_mod.loadSchema(self.store, self.alloc);
        errdefer if (next_schema) |schema| schema_mod.freeSchema(self.alloc, schema);
        try self.index_manager.refreshEmptyTextIndexSchemas(self.store);
        if (self.schema) |existing| schema_mod.freeSchema(self.alloc, existing);
        self.schema = next_schema;
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
        node_admission: ?NodeAdmission,
    ) !?paths_mod.Path {
        const entry = self.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
        return try paths_mod.findShortestPath(alloc, &entry.index, source, target, .{
            .weight_mode = weight_mode,
            .edge_types = edge_types,
            .direction = direction,
            .max_depth = max_depth,
            .min_weight = min_weight,
            .max_weight = max_weight,
            .node_admission = node_admission,
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
        node_admission: ?NodeAdmission,
    ) ![]paths_mod.Path {
        const entry = self.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
        return try paths_mod.findKShortestPaths(alloc, &entry.index, source, target, k, .{
            .weight_mode = weight_mode,
            .edge_types = edge_types,
            .direction = direction,
            .max_depth = max_depth,
            .min_weight = min_weight,
            .max_weight = max_weight,
            .node_admission = node_admission,
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
        return try self.beginTransactionWithParticipantsCreatedAt(txn_id, timestamp_ns, timestamp_ns, participants);
    }

    pub fn beginTransactionWithParticipantsCreatedAt(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        timestamp_ns: u64,
        created_at_ns: u64,
        participants: []const []const u8,
    ) !transactions_mod.TxnId {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.initTransactionWithParticipantsCreatedAt(txn_id, timestamp_ns, created_at_ns, participants);
        return txn_id;
    }

    pub fn beginTransactionWithParticipantsCreatedAtAndRole(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        timestamp_ns: u64,
        created_at_ns: u64,
        participants: []const []const u8,
        coordinator: bool,
    ) !transactions_mod.TxnId {
        return try self.beginTransactionWithParticipantsCreatedAtRoleAndRetention(
            txn_id,
            timestamp_ns,
            created_at_ns,
            participants,
            coordinator,
            false,
        );
    }

    pub fn beginTransactionWithParticipantsCreatedAtRoleAndRetention(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        timestamp_ns: u64,
        created_at_ns: u64,
        participants: []const []const u8,
        coordinator: bool,
        retain_terminal: bool,
    ) !transactions_mod.TxnId {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(
            txn_id,
            timestamp_ns,
            created_at_ns,
            participants,
            coordinator,
            retain_terminal,
        );
        return txn_id;
    }

    pub fn beginTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        timestamp_ns: u64,
        created_at_ns: u64,
        participants: []const []const u8,
        coordinator: bool,
        retain_terminal: bool,
        extra_batch: transactions_mod.MutationExtraBatch,
    ) !transactions_mod.TxnId {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch(
            txn_id,
            timestamp_ns,
            created_at_ns,
            participants,
            coordinator,
            retain_terminal,
            extra_batch,
        );
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

    pub fn writeIntentsExtraBatch(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        intents: []const transactions_mod.WriteIntent,
        predicates: []const transactions_mod.VersionPredicate,
        extra_batch: transactions_mod.MutationExtraBatch,
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.writeIntentsExtraBatch(txn_id, intents, predicates, extra_batch);
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

    pub fn checkOrdinaryWriteConflicts(
        self: *DBCore,
        writes: []const types.BatchWrite,
        deletes: []const []const u8,
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        const keys = try self.alloc.alloc([]const u8, writes.len + deletes.len);
        defer self.alloc.free(keys);
        for (writes, 0..) |write, i| keys[i] = write.key;
        for (deletes, 0..) |key, i| keys[writes.len + i] = key;
        try manager.checkOrdinaryWriteConflicts(keys);
    }

    pub fn resolveTransactionIntents(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        status: transactions_mod.TxnStatus,
        commit_version: u64,
    ) !void {
        _ = try self.resolveTransactionIntentsWithExtraBatch(txn_id, status, commit_version, .{});
    }

    pub fn resolveTransactionIntentsWithExtraBatch(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        status: transactions_mod.TxnStatus,
        commit_version: u64,
        extra_batch: transactions_mod.ResolutionExtraBatch,
    ) !transactions_mod.ResolutionOutcome {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.resolveIntentsWithExtraBatch(txn_id, status, commit_version, extra_batch);
    }

    pub fn collectTransactionIntentBatch(self: *DBCore, alloc: Allocator, txn_id: transactions_mod.TxnId) !transactions_mod.IntentBatch {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.collectIntentBatch(alloc, txn_id);
    }

    pub fn transactionHasIntents(self: *DBCore, txn_id: transactions_mod.TxnId) !bool {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.hasIntents(txn_id);
    }

    pub fn validateTransactionIntentSnapshot(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        expected_revision: u64,
    ) !transactions_mod.IntentSnapshotValidation {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.validateIntentSnapshot(txn_id, expected_revision);
    }

    pub fn loadTransactionHAOutbox(
        self: *DBCore,
        alloc: Allocator,
        txn_id: transactions_mod.TxnId,
    ) !transactions_mod.HAOutbox {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.loadHAOutbox(alloc, txn_id);
    }

    pub fn transactionHasHAOutbox(self: *DBCore, txn_id: transactions_mod.TxnId) !bool {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.hasHAOutbox(txn_id);
    }

    pub fn clearTransactionHAOutbox(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        kind: transactions_mod.HAOutboxKind,
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.clearHAOutbox(txn_id, kind);
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

    pub fn transactionDefersCoordinatorAcknowledgement(self: *DBCore, txn_id: transactions_mod.TxnId) !bool {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.defersCoordinatorAcknowledgement(txn_id);
    }

    pub fn transactionRetainsCoordinatorAcknowledgement(self: *DBCore, txn_id: transactions_mod.TxnId) !bool {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.retainsCoordinatorAcknowledgement(txn_id);
    }

    pub fn markTransactionParticipantResolved(self: *DBCore, txn_id: transactions_mod.TxnId, participant: []const u8) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.markParticipantResolved(txn_id, participant);
    }

    pub fn markTransactionParticipantResolvedExtraBatch(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        participant: []const u8,
        extra_batch: transactions_mod.MutationExtraBatch,
    ) !void {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        try manager.markParticipantResolvedExtraBatch(txn_id, participant, extra_batch);
    }

    pub fn cleanupTransactionMetadataIfEligible(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) !bool {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.cleanupTransactionMetadataIfEligible(txn_id, cutoff_timestamp, retained_cutoff_timestamp);
    }

    pub fn cleanupTransactionMetadataIfEligibleExtraBatch(
        self: *DBCore,
        txn_id: transactions_mod.TxnId,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
        extra_batch: transactions_mod.MutationExtraBatch,
    ) !bool {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.cleanupTransactionMetadataIfEligibleExtraBatch(
            txn_id,
            cutoff_timestamp,
            retained_cutoff_timestamp,
            extra_batch,
        );
    }

    pub fn getTransactionParticipants(self: *DBCore, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.getParticipants(alloc, txn_id);
    }

    pub fn listTransactions(self: *DBCore, alloc: Allocator) ![]transactions_mod.TxnSummary {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.listTransactions(alloc);
    }

    pub fn hasTopologySensitiveTransactions(self: *DBCore) !bool {
        var manager = try self.initTxnManager();
        defer manager.deinit();
        return try manager.hasTopologySensitiveTransactions();
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
    var identity_visibility_deletes = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (identity_visibility_deletes.items) |key| alloc.free(key);
        identity_visibility_deletes.deinit(alloc);
    }
    const identity_live_before = (try doc_identity.fastStatsFromStore(identity_ctx.store)).live_ordinals;
    try doc_identity.appendBatchIdentityMetadataForNamespaceWithVisibilityDeletesAlloc(
        alloc,
        identity_ctx.store,
        identity_ctx.identity_namespace,
        identity_ctx.store.lastReplaySequence(0),
        &identity_writes,
        &identity_visibility_deletes,
        identity_upserts.items,
        identity_deletes.items,
    );
    if (try doc_identity.visibilitySummaryFromWrites(identity_writes.items)) |summary| {
        const byte_range = try range_state_mod.loadRange(alloc, identity_ctx.store);
        defer {
            alloc.free(@constCast(byte_range.start));
            alloc.free(@constCast(byte_range.end));
        }
        try range_cardinality.appendIdentityTransitionAlloc(
            alloc,
            identity_ctx.store,
            byte_range,
            identity_live_before,
            summary.live_ordinals,
            &identity_writes,
        );
    }
    if (identity_writes.items.len == 0 and identity_visibility_deletes.items.len == 0) return .{};
    const owned_writes = try identity_writes.toOwnedSlice(alloc);
    errdefer {
        for (owned_writes) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        if (owned_writes.len > 0) alloc.free(owned_writes);
    }
    const owned_deletes = try identity_visibility_deletes.toOwnedSlice(alloc);
    errdefer {
        for (owned_deletes) |key| {
            alloc.free(key);
        }
        if (owned_deletes.len > 0) alloc.free(owned_deletes);
    }
    return .{
        .writes = owned_writes,
        .deletes = owned_deletes,
    };
}

fn cleanupTransactionRecoveryIdentityExtraBatch(ctx: ?*anyopaque, batch: transactions_mod.ResolutionExtraBatch) void {
    const identity_ctx: *TransactionRecoveryIdentityContext = @ptrCast(@alignCast(ctx.?));
    const alloc = identity_ctx.alloc;
    for (batch.writes) |item| {
        alloc.free(@constCast(item.key));
        alloc.free(@constCast(item.value));
    }
    for (batch.deletes) |key| {
        alloc.free(@constCast(key));
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
    read_only: bool,
) change_journal_mod.OpenOptions {
    const backend: change_journal_mod.StorageBackend = backend_override orelse switch (primary_backend_kind) {
        .mem, .lsm_memory => .lsm_memory,
        .lmdb, .lsm => .lsm,
    };
    return .{
        .map_size = map_size,
        .no_sync = no_sync,
        .backend = backend,
        .read_only = read_only,
        .storage = storage_override orelse if (backend == .lsm and primary_backend_kind == .lsm) primary_lsm_storage else null,
    };
}

pub fn openCoreResourcesFromPrimaryStore(
    alloc: Allocator,
    path: []const u8,
    index_base_path: []const u8,
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
    external_derived_checkpoints: bool,
    index_repair_checkpoint_storage: ?lsm_backend_mod.Storage,
    root_generation: u64,
    read_only: bool,
) !OpenedCoreResources {
    var owned_path: ?[]u8 = null;
    var owned_applied_sequence_checkpoint_path: ?[]u8 = null;
    var owned_index_repair_checkpoint: ?IndexRepairCheckpoint = null;
    var owned_store: ?*docstore_mod.DocStore = null;
    var owned_primary_store_owner = opened_primary.owner;
    var owned_change_journal: ?*change_journal_mod.Journal = null;
    var owned_shard_manager: ?*shard_mod.ShardManager = null;
    var owned_index_manager: ?*index_manager_mod.IndexManager = null;
    var owned_apply_mutex: ?*apply_rw_lock_mod.ApplyRwLock = null;
    var owned_snapshot_admission: ?*snapshot_admission_mod.SnapshotAdmission = null;
    var owned_snapshot_replay_admission: ?*snapshot_admission_mod.SnapshotAdmission = null;
    var owned_repair_replay_mutex: ?*std.atomic.Mutex = null;
    var owned_log_mutex: ?*std.atomic.Mutex = null;
    errdefer {
        if (owned_log_mutex) |ptr| alloc.destroy(ptr);
        if (owned_repair_replay_mutex) |ptr| alloc.destroy(ptr);
        if (owned_apply_mutex) |ptr| alloc.destroy(ptr);
        if (owned_snapshot_admission) |ptr| alloc.destroy(ptr);
        if (owned_snapshot_replay_admission) |ptr| alloc.destroy(ptr);
        if (owned_index_manager) |ptr| alloc.destroy(ptr);
        if (owned_shard_manager) |ptr| alloc.destroy(ptr);
        if (owned_change_journal) |ptr| alloc.destroy(ptr);
        if (owned_store) |ptr| {
            ptr.close();
            alloc.destroy(ptr);
        }
        owned_primary_store_owner.close(alloc);
        if (owned_applied_sequence_checkpoint_path) |buf| alloc.free(buf);
        if (owned_index_repair_checkpoint) |*checkpoint| checkpoint.deinit(alloc);
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
    const snapshot_admission = try alloc.create(snapshot_admission_mod.SnapshotAdmission);
    snapshot_admission.* = .{};
    owned_snapshot_admission = snapshot_admission;
    const snapshot_replay_admission = try alloc.create(snapshot_admission_mod.SnapshotAdmission);
    snapshot_replay_admission.* = .{};
    owned_snapshot_replay_admission = snapshot_replay_admission;
    const repair_replay_mutex = try alloc.create(std.atomic.Mutex);
    repair_replay_mutex.* = .unlocked;
    owned_repair_replay_mutex = repair_replay_mutex;
    const log_mutex = try alloc.create(std.atomic.Mutex);
    log_mutex.* = .unlocked;
    owned_log_mutex = log_mutex;
    const path_copy = try alloc.dupe(u8, path);
    owned_path = path_copy;
    const applied_sequence_checkpoint_path = if (external_derived_checkpoints) switch (primary_backend_kind) {
        .lmdb, .lsm => try apply_state.checkpointPathAlloc(alloc, path),
        .mem, .lsm_memory => null,
    } else null;
    owned_applied_sequence_checkpoint_path = applied_sequence_checkpoint_path;
    const index_repair_checkpoint = if (index_repair_checkpoint_storage) |storage| checkpoint_blk: {
        const checkpoint_path = try index_repair_state.checkpointPathAlloc(alloc, index_base_path);
        errdefer alloc.free(checkpoint_path);
        const lock_key = try storage.rootIdentityAlloc(alloc, checkpoint_path);
        break :checkpoint_blk IndexRepairCheckpoint{
            .lock_key = lock_key,
            .path = checkpoint_path,
            .storage = storage,
        };
    } else switch (primary_backend_kind) {
        .lmdb, .lsm => checkpoint_blk: {
            const checkpoint_path = try index_repair_state.checkpointPathAlloc(alloc, path);
            errdefer alloc.free(checkpoint_path);
            break :checkpoint_blk IndexRepairCheckpoint{
                .lock_key = try alloc.dupe(u8, checkpoint_path),
                .path = checkpoint_path,
            };
        },
        .mem, .lsm_memory => null,
    };
    owned_index_repair_checkpoint = index_repair_checkpoint;

    const change_journal_path = try std.fmt.allocPrint(alloc, "{s}/change_journal", .{path});
    defer alloc.free(change_journal_path);
    const change_journal_path_z = try alloc.dupeSentinel(u8, change_journal_path, 0);
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
            read_only,
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
        index_base_path,
        index_backends,
    );
    index_manager.setAppliedSequenceCheckpointPath(applied_sequence_checkpoint_path);
    index_manager.updateRange(shard_manager.getByteRange());

    const schema = try schema_mod.loadSchema(store, alloc);

    owned_path = null;
    owned_applied_sequence_checkpoint_path = null;
    owned_index_repair_checkpoint = null;
    owned_store = null;
    owned_primary_store_owner = .none;
    owned_change_journal = null;
    owned_shard_manager = null;
    owned_index_manager = null;
    owned_apply_mutex = null;
    owned_snapshot_admission = null;
    owned_snapshot_replay_admission = null;
    owned_repair_replay_mutex = null;
    owned_log_mutex = null;

    return .{
        .path = path_copy,
        .root_generation = root_generation,
        .applied_sequence_checkpoint_path = applied_sequence_checkpoint_path,
        .index_repair_checkpoint = index_repair_checkpoint,
        .store = store,
        .primary_store_owner = opened_primary.owner,
        .change_journal = change_journal,
        .shard_manager = shard_manager,
        .index_manager = index_manager,
        .apply_mutex = apply_mutex,
        .snapshot_admission = snapshot_admission,
        .snapshot_replay_admission = snapshot_replay_admission,
        .repair_replay_mutex = repair_replay_mutex,
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
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = try importStoreSnapshotWithIo(alloc, io_impl.io(), store, snapshot_root, .none);
}

/// Returns true for the streaming v2 format. Its store image already contains
/// the replay namespace, so callers must not apply the legacy sidecar again.
pub fn importStoreSnapshotWithIo(
    alloc: Allocator,
    io: std.Io,
    store: *docstore_mod.DocStore,
    snapshot_root: []const u8,
    cancellation: types.CancellationToken,
) !bool {
    try validateLogicalSnapshotManifestIfPresent(alloc, io, snapshot_root);
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, store_snapshot_file_name });
    defer alloc.free(snapshot_path);
    if (try storeSnapshotHasV2Magic(io, snapshot_path)) {
        try importStreamingStoreSnapshot(alloc, io, store, snapshot_path, cancellation);
        return true;
    }

    const raw = try std.Io.Dir.cwd().readFileAlloc(io, snapshot_path, alloc, .limited(legacy_store_snapshot_max_bytes));
    defer alloc.free(raw);

    var decoded = try lsm_table_file.decodeAlloc(alloc, raw);
    defer decoded.deinit(alloc);

    const batch_size = 8192;
    var offset: usize = 0;
    while (offset < decoded.entries.len) {
        try cancellation.check();
        const end = @min(offset + batch_size, decoded.entries.len);
        const writes = try alloc.alloc(docstore_mod.KVPair, end - offset);
        defer alloc.free(writes);
        for (decoded.entries[offset..end], 0..) |entry, i| {
            writes[i] = .{
                .key = entry.key,
                .value = entry.value,
            };
        }
        try store.putBatch(writes, &.{});
        offset = end;
    }

    try store.sync(true);
    return false;
}

const LogicalSnapshotManifest = struct {
    format_version: u32 = logical_snapshot_manifest_format_version,
    primary_artifact_format: []const u8 = logical_store_artifact_format,
    primary_artifact_version: u32 = logical_store_artifact_version,
    replay_embedded: bool = true,
};

/// Publishes the format identity for logical snapshots which travel without a
/// native-generation manifest (HA seeds, split/bootstrap snapshots, and the C
/// API). v0.2.0 snapshots have no descriptor and remain readable through the
/// legacy store.bin/change-journal.bin path.
pub fn writeLogicalSnapshotManifest(alloc: Allocator, io: std.Io, snapshot_root: []const u8) !u64 {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, logical_snapshot_manifest_file_name });
    defer alloc.free(path);
    const encoded = try std.json.Stringify.valueAlloc(alloc, LogicalSnapshotManifest{}, .{});
    defer alloc.free(encoded);
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    var writer_buffer: [1024]u8 = undefined;
    var writer = file.writer(io, &writer_buffer);
    try writer.interface.writeAll(encoded);
    try writer.end();
    try file.sync(io);
    try fs_paths.syncDirPortable(io, snapshot_root);
    return @intCast(encoded.len);
}

fn validateLogicalSnapshotManifestIfPresent(alloc: Allocator, io: std.Io, snapshot_root: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, logical_snapshot_manifest_file_name });
    defer alloc.free(path);
    const encoded = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(4096)) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer alloc.free(encoded);
    var parsed = std.json.parseFromSlice(LogicalSnapshotManifest, alloc, encoded, .{
        .ignore_unknown_fields = false,
    }) catch return error.InvalidTableFile;
    defer parsed.deinit();
    if (parsed.value.format_version != logical_snapshot_manifest_format_version or
        !std.mem.eql(u8, parsed.value.primary_artifact_format, logical_store_artifact_format) or
        parsed.value.primary_artifact_version != logical_store_artifact_version or
        !parsed.value.replay_embedded)
    {
        return error.UnsupportedBackupFormat;
    }
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

const LogicalPinnedStoreSnapshot = struct {
    txn: docstore_mod.DocStore.Txn,

    fn deinit(self: *LogicalPinnedStoreSnapshot) void {
        self.txn.abort();
        self.* = undefined;
    }

    /// Streams the immutable read transaction with bounded memory. The read
    /// snapshot is acquired under the DB revision fence, but materialization
    /// deliberately runs after that fence is released.
    fn materialize(
        self: *LogicalPinnedStoreSnapshot,
        alloc: Allocator,
        io: std.Io,
        snapshot_root: []const u8,
        cancellation: types.CancellationToken,
    ) !u64 {
        return try self.materializeWithSink(alloc, io, snapshot_root, cancellation, null);
    }

    fn materializeWithSink(
        self: *LogicalPinnedStoreSnapshot,
        alloc: Allocator,
        io: std.Io,
        snapshot_root: []const u8,
        cancellation: types.CancellationToken,
        sink: ?native_artifact_sink.Sink,
    ) !u64 {
        try cancellation.check();
        const snapshot_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, store_snapshot_file_name });
        defer alloc.free(snapshot_path);
        var file = try fs_paths.createFilePortable(io, snapshot_path, .{ .truncate = true });
        defer file.close(io);
        var writer_buffer: [256 * 1024]u8 = undefined;
        var writer = file.writer(io, &writer_buffer);
        try writer.interface.writeAll(store_snapshot_v2_magic);
        var hasher = native_artifact_sink.Sha256.init(.{});
        hasher.update(store_snapshot_v2_magic);
        var total: u64 = store_snapshot_v2_magic.len;

        var cursor = try self.txn.openCursor();
        defer cursor.close();
        var entry = try cursor.first();
        while (entry) |kv| : (entry = try cursor.next()) {
            try cancellation.check();
            var lengths: [16]u8 = undefined;
            std.mem.writeInt(u64, lengths[0..8], @intCast(kv.key.len), .little);
            std.mem.writeInt(u64, lengths[8..16], @intCast(kv.value.len), .little);
            try writer.interface.writeAll(&lengths);
            try writer.interface.writeAll(kv.key);
            try writer.interface.writeAll(kv.value);
            hasher.update(&lengths);
            hasher.update(kv.key);
            hasher.update(kv.value);
            total = std.math.add(u64, total, 16 + @as(u64, @intCast(kv.key.len)) + @as(u64, @intCast(kv.value.len))) catch
                return error.SnapshotTooLarge;
        }
        try cancellation.check();
        try writer.end();
        try file.sync(io);
        if (sink) |active| {
            var digest: [native_artifact_sink.Sha256.digest_length]u8 = undefined;
            hasher.final(&digest);
            try active.record(snapshot_path, total, digest);
        }
        try fs_paths.syncDirPortable(io, snapshot_root);
        return total;
    }
};

pub const PinnedStoreSnapshot = union(enum) {
    logical: LogicalPinnedStoreSnapshot,
    lsm: lsm_backend_mod.Backend.NativeCheckpoint,

    pub fn deinit(self: *PinnedStoreSnapshot) void {
        switch (self.*) {
            .logical => |*snapshot| snapshot.deinit(),
            .lsm => |*snapshot| snapshot.deinit(),
        }
        self.* = undefined;
    }

    pub fn materialize(
        self: *PinnedStoreSnapshot,
        alloc: Allocator,
        io: std.Io,
        snapshot_root: []const u8,
        cancellation: types.CancellationToken,
    ) !u64 {
        return try self.materializeWithSink(alloc, io, snapshot_root, cancellation, null);
    }

    pub fn materializeWithSink(
        self: *PinnedStoreSnapshot,
        alloc: Allocator,
        io: std.Io,
        snapshot_root: []const u8,
        cancellation: types.CancellationToken,
        sink: ?native_artifact_sink.Sink,
    ) !u64 {
        return switch (self.*) {
            .logical => |*snapshot| try snapshot.materializeWithSink(alloc, io, snapshot_root, cancellation, sink),
            .lsm => |*snapshot| blk: {
                const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_root, primary_lsm_checkpoint_directory_name });
                defer alloc.free(destination);
                break :blk try snapshot.materializeWithSink(io, destination, cancellation, sink);
            },
        };
    }

    pub fn artifactFormat(self: *const PinnedStoreSnapshot) []const u8 {
        return switch (self.*) {
            .logical => logical_store_artifact_format,
            .lsm => "antfly-lsm-checkpoint",
        };
    }

    pub fn artifactVersion(self: *const PinnedStoreSnapshot) u32 {
        return switch (self.*) {
            .logical => logical_store_artifact_version,
            .lsm => 1,
        };
    }
};

fn importStreamingStoreSnapshot(
    alloc: Allocator,
    io: std.Io,
    store: *docstore_mod.DocStore,
    path: []const u8,
    cancellation: types.CancellationToken,
) !void {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size < store_snapshot_v2_magic.len) return error.InvalidTableFile;
    var magic: [store_snapshot_v2_magic.len]u8 = undefined;
    if (try file.readPositionalAll(io, &magic, 0) != magic.len or
        !std.mem.eql(u8, &magic, store_snapshot_v2_magic))
    {
        return error.InvalidTableFile;
    }

    var batch = std.ArrayListUnmanaged(docstore_mod.OwnedKVPair).empty;
    defer {
        freeStoreSnapshotBatch(alloc, batch.items);
        batch.deinit(alloc);
    }
    var batch_bytes: usize = 0;
    var offset: u64 = store_snapshot_v2_magic.len;
    while (offset < stat.size) {
        try cancellation.check();
        if (stat.size - offset < 16) return error.InvalidTableFile;
        var lengths: [16]u8 = undefined;
        if (try file.readPositionalAll(io, &lengths, offset) != lengths.len)
            return error.InvalidTableFile;
        offset += lengths.len;
        const key_len_u64 = std.mem.readInt(u64, lengths[0..8], .little);
        const value_len_u64 = std.mem.readInt(u64, lengths[8..16], .little);
        if (key_len_u64 > store_snapshot_max_field_bytes or value_len_u64 > store_snapshot_max_field_bytes)
            return error.InvalidTableFile;
        const record_len = std.math.add(u64, key_len_u64, value_len_u64) catch
            return error.InvalidTableFile;
        if (record_len > stat.size - offset) return error.InvalidTableFile;
        const key_len = std.math.cast(usize, key_len_u64) orelse return error.InvalidTableFile;
        const value_len = std.math.cast(usize, value_len_u64) orelse return error.InvalidTableFile;
        const key = try alloc.alloc(u8, key_len);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (try file.readPositionalAll(io, key, offset) != key.len) return error.InvalidTableFile;
        offset += key_len_u64;
        const value = try alloc.alloc(u8, value_len);
        var value_owned = true;
        errdefer if (value_owned) alloc.free(value);
        if (try file.readPositionalAll(io, value, offset) != value.len) return error.InvalidTableFile;
        offset += value_len_u64;
        try batch.append(alloc, .{ .key = key, .value = value });
        key_owned = false;
        value_owned = false;
        const record_bytes = std.math.add(usize, key_len, value_len) catch
            return error.InvalidTableFile;
        batch_bytes = std.math.add(usize, batch_bytes, record_bytes) catch
            return error.InvalidTableFile;
        if (batch.items.len >= store_snapshot_batch_entries or batch_bytes >= store_snapshot_batch_bytes) {
            try flushStoreSnapshotBatch(alloc, store, batch.items);
            freeStoreSnapshotBatch(alloc, batch.items);
            batch.clearRetainingCapacity();
            batch_bytes = 0;
        }
    }
    if (batch.items.len > 0) {
        try flushStoreSnapshotBatch(alloc, store, batch.items);
        freeStoreSnapshotBatch(alloc, batch.items);
        batch.clearRetainingCapacity();
    }
    try store.sync(true);
}

fn flushStoreSnapshotBatch(alloc: Allocator, store: *docstore_mod.DocStore, entries: []const docstore_mod.OwnedKVPair) !void {
    const writes = try alloc.alloc(docstore_mod.KVPair, entries.len);
    defer alloc.free(writes);
    for (entries, 0..) |entry, index| writes[index] = .{ .key = entry.key, .value = entry.value };
    try store.putBatch(writes, &.{});
}

fn freeStoreSnapshotBatch(alloc: Allocator, entries: []const docstore_mod.OwnedKVPair) void {
    for (entries) |entry| {
        alloc.free(entry.key);
        alloc.free(entry.value);
    }
}

fn storeSnapshotHasV2Magic(io: std.Io, path: []const u8) !bool {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size < store_snapshot_v2_magic.len) return false;
    var magic: [store_snapshot_v2_magic.len]u8 = undefined;
    if (try file.readPositionalAll(io, &magic, 0) != magic.len) return false;
    return std.mem.eql(u8, &magic, store_snapshot_v2_magic);
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
