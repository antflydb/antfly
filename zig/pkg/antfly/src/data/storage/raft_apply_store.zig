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
const raft_engine = @import("raft_engine");
const platform = @import("antfly_platform");
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const background_runtime = @import("../../storage/background_runtime.zig");
const docstore = @import("../../storage/docstore.zig");
const generation_lifecycle = @import("../../storage/db/generation_lifecycle.zig");
const range_state = @import("../../storage/db/range_state.zig");
const lsm_backend = @import("../../storage/lsm_backend.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const raft_storage_mod = @import("../../raft/storage/mod.zig");
const wal_replica_state_mod = @import("../../raft/storage/wal_replica_state.zig");
const shard_mod = @import("../../storage/shard.zig");
const raft_state_machine = @import("../../raft/state_machine/mod.zig");
const shard_state_store = @import("shard_state_store.zig");
const data_raft_batch = @import("../raft_batch.zig");
const batch_shard_count: usize = 64;
const default_cached_group_stores_per_shard: usize = 4;
const group_store_owner_accounting_bytes: u64 = 64 * 1024;
var snapshot_spool_nonce = std.atomic.Value(u64).init(1);
var test_block_snapshot_staging = std.atomic.Value(bool).init(false);
var test_snapshot_staging_started = std.atomic.Value(bool).init(false);

pub const AppliedDataBatch = struct {
    commit_index: u64,
    entry_count: usize,
    normal_entry_count: usize,
    admin_entry_count: usize,
    last_entry_term: u64,
    last_entry_index: u64,
};

pub const AppliedNormalEntry = struct {
    index: u64,
    data: []const u8,
};

pub const AppliedDataKV = shard_state_store.AppliedDataKV;
pub const AppliedDataRange = shard_state_store.AppliedDataRange;
pub const AppliedSplitState = shard_state_store.AppliedSplitState;
pub const SplitHandoff = shard_state_store.SplitHandoff;
pub const SplitHandoffMetadata = shard_state_store.SplitHandoffMetadata;
pub const GroupStatePage = shard_state_store.GroupStatePage;

pub const SplitControlObservation = struct {
    state: ?AppliedSplitState = null,
    terminal: ?shard_state_store.AppliedSplitTerminal = null,
    acknowledgement: ?shard_state_store.SplitAcknowledgement = null,
    delta_sequence: u64 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.state) |state| shard_state_store.freeSplitState(alloc, state);
        if (self.terminal) |terminal| shard_state_store.freeSplitTerminal(alloc, terminal);
        self.* = undefined;
    }
};

pub const RaftApplyStoreConfig = struct {
    root_dir: []const u8,
    no_sync: bool = false,
    /// Writable stores inherit the LSM backend's process and native-path
    /// exclusive writer lease. Read-only inspection may coexist with the owner.
    read_only: bool = false,
    backend_runtime: ?*background_runtime.BackendRuntime = null,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    max_cached_group_stores_per_shard: usize = default_cached_group_stores_per_shard,
};

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    path: []u8,
    groups_root: []u8,
    no_sync: bool,
    read_only: bool,
    backend_runtime: ?*background_runtime.BackendRuntime,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    max_cached_group_stores_per_shard: usize,
    group_store_access_clock: std.atomic.Value(u64) = .init(1),
    // The root backend is an ownership sentinel: its native writer lease keeps
    // one process-wide data apply owner while physical state lives per group.
    backend: lsm_backend.BackendHandle,
    shutting_down: std.atomic.Value(bool) = .init(false),
    placement_transition_mutex: std.atomic.Mutex = .unlocked,
    batch_shards: [batch_shard_count]BatchShard = [_]BatchShard{.{}} ** batch_shard_count,

    const OwnedBatch = AppliedDataBatch;
    const BatchShard = struct {
        mutex: std.Io.Mutex = .init,
        state_changed: std.Io.Condition = .init,
        batches: std.AutoHashMapUnmanaged(u64, OwnedBatch) = .empty,
        stores: std.AutoHashMapUnmanaged(u64, *GroupStore) = .empty,
        snapshot_readers: std.AutoHashMapUnmanaged(u64, usize) = .empty,
        generation_preparations: std.AutoHashMapUnmanaged(u64, void) = .empty,
        active_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,
        placement_filter_enabled: bool = false,
    };

    const GroupStore = struct {
        alloc: std.mem.Allocator,
        path: []u8,
        backend: lsm_backend.BackendHandle,
        store: docstore.DocStore,
        owner_reservation: ?resource_manager_mod.Reservation = null,
        last_access: u64,

        fn open(
            alloc: std.mem.Allocator,
            path: []const u8,
            no_sync: bool,
            read_only: bool,
            resource_manager: ?*resource_manager_mod.ResourceManager,
            last_access: u64,
        ) !*GroupStore {
            var owner_reservation = if (resource_manager) |manager|
                try manager.reserve(.lsm_in_memory_state, group_store_owner_accounting_bytes)
            else
                null;
            errdefer if (owner_reservation) |*reservation| reservation.release();
            const owned = try alloc.create(GroupStore);
            errdefer alloc.destroy(owned);
            const owned_path = try alloc.dupe(u8, path);
            errdefer alloc.free(owned_path);
            var backend = try lsm_backend.BackendHandle.open(alloc, path, .{
                .backend = .{
                    .durability = if (no_sync) .none else .full,
                    .read_only = read_only,
                    .create_if_missing = !read_only,
                },
                .flush_threshold = 1,
                .resource_manager = resource_manager,
            });
            errdefer backend.close();
            var runtime_store = try backend.backend.runtimeStore(alloc, .{ .name = "data-apply-group" });
            errdefer runtime_store.deinit();
            owned.* = .{
                .alloc = alloc,
                .path = owned_path,
                .backend = backend,
                .store = try docstore.DocStore.openRuntime(alloc, runtime_store),
                .owner_reservation = owner_reservation,
                .last_access = last_access,
            };
            owner_reservation = null;
            return owned;
        }

        fn close(self: *GroupStore) void {
            const alloc = self.alloc;
            self.store.close();
            self.backend.close();
            if (self.owner_reservation) |*reservation| reservation.release();
            alloc.free(self.path);
            alloc.destroy(self);
        }
    };

    pub fn init(alloc: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        var io_impl = threaded_io_limits.initService(alloc);
        errdefer io_impl.deinit();

        const root_dir = try alloc.dupe(u8, cfg.root_dir);
        errdefer alloc.free(root_dir);

        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        const path = try std.fmt.allocPrint(alloc, "{s}/data-apply-store", .{root_dir});
        errdefer alloc.free(path);
        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), path);
        const groups_root = try std.fmt.allocPrint(alloc, "{s}/data-apply-groups", .{root_dir});
        errdefer alloc.free(groups_root);
        if (!cfg.read_only) try fs_paths.createDirPathPortable(io_impl.io(), groups_root);

        var backend = try lsm_backend.BackendHandle.open(alloc, path, .{
            .backend = .{
                .durability = if (cfg.no_sync) .none else .full,
                .read_only = cfg.read_only,
                .create_if_missing = !cfg.read_only,
            },
            .flush_threshold = 1,
        });
        errdefer backend.close();

        if (!cfg.read_only) {
            const spool_dir = try std.fmt.allocPrint(alloc, "{s}/snapshot-spool", .{root_dir});
            defer alloc.free(spool_dir);
            try std.Io.Dir.cwd().deleteTree(io_impl.io(), spool_dir);
            try fs_paths.createDirPathPortable(io_impl.io(), spool_dir);
        }

        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .root_dir = root_dir,
            .path = path,
            .groups_root = groups_root,
            .no_sync = cfg.no_sync,
            .read_only = cfg.read_only,
            .backend_runtime = cfg.backend_runtime,
            .resource_manager = cfg.resource_manager,
            .max_cached_group_stores_per_shard = @max(@as(usize, 1), cfg.max_cached_group_stores_per_shard),
            .backend = backend,
        };
    }

    /// Attaches the node-wide resource policy before the Raft host starts
    /// serving. Existing group owners would have been opened without complete
    /// accounting, so changing managers after admission is intentionally
    /// rejected.
    pub fn attachResourceManager(self: *RaftApplyStore, manager: *resource_manager_mod.ResourceManager) !void {
        const io = self.io_impl.io();
        for (&self.batch_shards) |*shard| {
            shard.mutex.lockUncancelable(io);
            if (shard.stores.count() != 0) {
                shard.mutex.unlock(io);
                return error.ApplyStoreAlreadyServing;
            }
            shard.mutex.unlock(io);
        }
        self.resource_manager = manager;
    }

    pub const ActiveGroupTransition = struct {
        store: *RaftApplyStore,
        previous: [batch_shard_count]std.AutoHashMapUnmanaged(u64, void),
        next_exact: [batch_shard_count]std.AutoHashMapUnmanaged(u64, void),
        retire_candidates: [batch_shard_count]std.AutoHashMapUnmanaged(u64, void),
        active: bool = true,

        /// Commits exact placement only after the Raft host has reconciled.
        pub fn commit(self: *@This()) void {
            std.debug.assert(self.active);
            const io = self.store.io_impl.io();
            for (&self.store.batch_shards, 0..) |*shard, shard_index| {
                shard.mutex.lockUncancelable(io);
                const admitted_union = shard.active_groups;
                shard.active_groups = self.next_exact[shard_index];
                self.next_exact[shard_index] = admitted_union;
                var candidates = self.retire_candidates[shard_index].keyIterator();
                while (candidates.next()) |group_id| {
                    self.store.closeRetiredGroupIfDrainedLocked(shard, group_id.*);
                }
                shard.mutex.unlock(io);
            }
            self.finish();
        }

        /// Reconciliation can have externally visible partial side effects.
        /// Keep old and new groups admitted so neither side is invalidated;
        /// the next successful metadata sync narrows admission exactly.
        pub fn abort(self: *@This()) void {
            if (!self.active) return;
            self.finish();
        }

        pub fn deinit(self: *@This()) void {
            self.abort();
        }

        fn finish(self: *@This()) void {
            for (&self.previous) |*groups| groups.deinit(self.store.alloc);
            for (&self.next_exact) |*groups| groups.deinit(self.store.alloc);
            for (&self.retire_candidates) |*groups| groups.deinit(self.store.alloc);
            self.active = false;
            self.store.placement_transition_mutex.unlock();
        }
    };

    /// Prepares exact placement and publishes conservative old-or-new
    /// admission before fallible host reconciliation starts. No existing
    /// owner is retired until commit().
    pub fn beginActiveGroupTransition(self: *RaftApplyStore, group_ids: []const u64) !ActiveGroupTransition {
        platform.sync.lockYielding(&self.placement_transition_mutex);
        errdefer self.placement_transition_mutex.unlock();

        var next_exact = [_]std.AutoHashMapUnmanaged(u64, void){.empty} ** batch_shard_count;
        errdefer for (&next_exact) |*groups| groups.deinit(self.alloc);
        var admitted_union = [_]std.AutoHashMapUnmanaged(u64, void){.empty} ** batch_shard_count;
        errdefer for (&admitted_union) |*groups| groups.deinit(self.alloc);
        for (group_ids) |group_id| {
            const shard_index: usize = @intCast(group_id % batch_shard_count);
            try next_exact[shard_index].put(self.alloc, group_id, {});
            try admitted_union[shard_index].put(self.alloc, group_id, {});
        }

        const io = self.io_impl.io();
        for (&self.batch_shards, 0..) |*shard, shard_index| {
            shard.mutex.lockUncancelable(io);
            var previous = shard.active_groups.keyIterator();
            while (previous.next()) |group_id| {
                admitted_union[shard_index].put(self.alloc, group_id.*, {}) catch |err| {
                    shard.mutex.unlock(io);
                    return err;
                };
            }
            shard.mutex.unlock(io);
        }

        var previous = [_]std.AutoHashMapUnmanaged(u64, void){.empty} ** batch_shard_count;
        errdefer for (&previous) |*groups| groups.deinit(self.alloc);
        for (&self.batch_shards, 0..) |*shard, shard_index| {
            shard.mutex.lockUncancelable(io);
            previous[shard_index] = shard.active_groups;
            shard.active_groups = admitted_union[shard_index];
            admitted_union[shard_index] = .empty;
            shard.placement_filter_enabled = true;
            shard.mutex.unlock(io);
        }

        // Once conservative admission is published, no new group outside the
        // union can enter a shard. Build a deduplicated retirement set while
        // holding only that shard's mutex. An allocation failure leaves the
        // safe union admitted, exactly like an aborted host reconciliation.
        var retire_candidates = [_]std.AutoHashMapUnmanaged(u64, void){.empty} ** batch_shard_count;
        errdefer for (&retire_candidates) |*groups| groups.deinit(self.alloc);
        for (&self.batch_shards, 0..) |*shard, shard_index| {
            shard.mutex.lockUncancelable(io);
            const upper_bound = std.math.add(
                usize,
                @intCast(shard.active_groups.count()),
                std.math.add(usize, @intCast(shard.stores.count()), @intCast(shard.batches.count())) catch {
                    shard.mutex.unlock(io);
                    return error.OutOfMemory;
                },
            ) catch {
                shard.mutex.unlock(io);
                return error.OutOfMemory;
            };
            const candidate_capacity = std.math.cast(@TypeOf(shard.active_groups.count()), upper_bound) orelse {
                shard.mutex.unlock(io);
                return error.OutOfMemory;
            };
            retire_candidates[shard_index].ensureTotalCapacity(self.alloc, candidate_capacity) catch |err| {
                shard.mutex.unlock(io);
                return err;
            };
            var active = shard.active_groups.keyIterator();
            while (active.next()) |group_id| {
                if (!next_exact[shard_index].contains(group_id.*)) {
                    retire_candidates[shard_index].putAssumeCapacity(group_id.*, {});
                }
            }
            var stores = shard.stores.keyIterator();
            while (stores.next()) |group_id| {
                if (!next_exact[shard_index].contains(group_id.*)) {
                    retire_candidates[shard_index].putAssumeCapacity(group_id.*, {});
                }
            }
            var batches = shard.batches.keyIterator();
            while (batches.next()) |group_id| {
                if (!next_exact[shard_index].contains(group_id.*)) {
                    retire_candidates[shard_index].putAssumeCapacity(group_id.*, {});
                }
            }
            shard.mutex.unlock(io);
        }
        return .{
            .store = self,
            .previous = previous,
            .next_exact = next_exact,
            .retire_candidates = retire_candidates,
        };
    }

    /// Convenience for initialization and tests without an external host
    /// reconciliation phase.
    pub fn retainActiveGroups(self: *RaftApplyStore, group_ids: []const u64) !void {
        var transition = try self.beginActiveGroupTransition(group_ids);
        transition.commit();
    }

    fn cachedGroupStoreCount(self: *RaftApplyStore) usize {
        const io = self.io_impl.io();
        var count: usize = 0;
        for (&self.batch_shards) |*shard| {
            shard.mutex.lockUncancelable(io);
            count += shard.stores.count();
            shard.mutex.unlock(io);
        }
        return count;
    }

    pub fn deinit(self: *RaftApplyStore) void {
        self.shutting_down.store(true, .release);
        const io = self.io_impl.io();
        for (&self.batch_shards) |*shard| {
            shard.mutex.lockUncancelable(io);
            shard.state_changed.broadcast(io);
            while (shard.snapshot_readers.count() != 0 or shard.generation_preparations.count() != 0) {
                shard.state_changed.waitUncancelable(io, &shard.mutex);
            }
            var stores = shard.stores.valueIterator();
            while (stores.next()) |store| store.*.close();
            shard.stores.deinit(self.alloc);
            shard.snapshot_readers.deinit(self.alloc);
            shard.generation_preparations.deinit(self.alloc);
            shard.active_groups.deinit(self.alloc);
            shard.batches.deinit(self.alloc);
            shard.mutex.unlock(io);
        }
        self.backend.close();
        self.alloc.free(self.groups_root);
        self.alloc.free(self.path);
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    fn groupPathAlloc(self: *RaftApplyStore, group_id: u64) ![]u8 {
        return try std.fmt.allocPrint(self.alloc, "{s}/group-{d}", .{ self.groups_root, group_id });
    }

    fn pathExists(self: *RaftApplyStore, path: []const u8) bool {
        _ = std.Io.Dir.cwd().statFile(self.io_impl.io(), path, .{}) catch return false;
        return true;
    }

    fn nextGroupStoreAccess(self: *RaftApplyStore) u64 {
        return self.group_store_access_clock.fetchAdd(1, .monotonic);
    }

    fn groupStorePinnedLocked(shard: *BatchShard, group_id: u64) bool {
        return (shard.snapshot_readers.get(group_id) orelse 0) != 0 or
            shard.generation_preparations.contains(group_id);
    }

    fn closeRetiredGroupIfDrainedLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) void {
        _ = self;
        if (!shard.placement_filter_enabled or shard.active_groups.contains(group_id) or groupStorePinnedLocked(shard, group_id)) return;
        if (shard.stores.fetchRemove(group_id)) |removed| removed.value.close();
        _ = shard.batches.remove(group_id);
    }

    fn evictOldestGroupStoreLocked(self: *RaftApplyStore, shard: *BatchShard, exclude_group_id: ?u64) bool {
        _ = self;
        var oldest_group_id: ?u64 = null;
        var oldest_access: u64 = std.math.maxInt(u64);
        var it = shard.stores.iterator();
        while (it.next()) |entry| {
            const group_id = entry.key_ptr.*;
            if (exclude_group_id != null and exclude_group_id.? == group_id) continue;
            if (groupStorePinnedLocked(shard, group_id)) continue;
            if (entry.value_ptr.*.last_access >= oldest_access) continue;
            oldest_group_id = group_id;
            oldest_access = entry.value_ptr.*.last_access;
        }
        const group_id = oldest_group_id orelse return false;
        const removed = shard.stores.fetchRemove(group_id) orelse unreachable;
        removed.value.close();
        return true;
    }

    fn trimGroupStoresForPressureLocked(self: *RaftApplyStore, shard: *BatchShard, exclude_group_id: ?u64) void {
        const pressure = if (self.resource_manager) |manager|
            manager.sliceStats(.lsm_in_memory_state).pressure
        else
            resource_manager_mod.Pressure.normal;
        const target = switch (pressure) {
            .normal => self.max_cached_group_stores_per_shard,
            .soft => @max(@as(usize, 1), self.max_cached_group_stores_per_shard / 2),
            .hard => 1,
        };
        while (shard.stores.count() > target) {
            if (!self.evictOldestGroupStoreLocked(shard, exclude_group_id)) break;
        }
    }

    fn groupStoreLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64, create: bool) !?*GroupStore {
        if (shard.stores.get(group_id)) |store| {
            store.last_access = self.nextGroupStoreAccess();
            self.trimGroupStoresForPressureLocked(shard, group_id);
            return store;
        }
        const path = try self.groupPathAlloc(group_id);
        defer self.alloc.free(path);
        if (!create and !self.pathExists(path)) return null;
        if (self.read_only and !self.pathExists(path)) return null;
        try shard.stores.ensureUnusedCapacity(self.alloc, 1);
        while (true) {
            while (shard.stores.count() >= self.max_cached_group_stores_per_shard) {
                if (!self.evictOldestGroupStoreLocked(shard, null)) return error.ApplyStoreOwnerCachePinned;
            }
            const store = GroupStore.open(
                self.alloc,
                path,
                self.no_sync,
                self.read_only,
                self.resource_manager,
                self.nextGroupStoreAccess(),
            ) catch |err| switch (err) {
                error.ResourceBudgetExceeded => {
                    if (self.evictOldestGroupStoreLocked(shard, null)) continue;
                    return err;
                },
                else => return err,
            };
            errdefer store.close();
            shard.stores.putAssumeCapacity(group_id, store);
            self.trimGroupStoresForPressureLocked(shard, group_id);
            return store;
        }
    }

    fn writableGroupStoreLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) !*GroupStore {
        if (self.read_only) return error.ReadOnly;
        return (try self.groupStoreLocked(shard, group_id, true)).?;
    }

    fn readableGroupStoreLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) !?*GroupStore {
        return try self.groupStoreLocked(shard, group_id, !self.read_only);
    }

    fn releaseSnapshotReader(self: *RaftApplyStore, group_id: u64) void {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const readers = shard.snapshot_readers.getPtr(group_id) orelse unreachable;
        std.debug.assert(readers.* > 0);
        readers.* -= 1;
        if (readers.* == 0) {
            _ = shard.snapshot_readers.remove(group_id);
            self.closeRetiredGroupIfDrainedLocked(shard, group_id);
            shard.state_changed.broadcast(io);
        }
    }

    fn waitForSnapshotReadersLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) void {
        const io = self.io_impl.io();
        while ((shard.snapshot_readers.get(group_id) orelse 0) != 0) {
            shard.state_changed.waitUncancelable(io, &shard.mutex);
        }
    }

    fn waitForGenerationPreparationLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) !void {
        const io = self.io_impl.io();
        while (shard.generation_preparations.contains(group_id)) {
            if (self.shutting_down.load(.acquire)) return error.ApplyStoreShuttingDown;
            shard.state_changed.waitUncancelable(io, &shard.mutex);
        }
        if (self.shutting_down.load(.acquire)) return error.ApplyStoreShuttingDown;
        if (shard.placement_filter_enabled and !shard.active_groups.contains(group_id)) return error.ApplyStoreGroupRetired;
    }

    fn requireTransitionReadyLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) !void {
        if (self.shutting_down.load(.acquire)) return error.ApplyStoreShuttingDown;
        if (shard.generation_preparations.contains(group_id)) return error.SplitSourceProjectionNotReady;
        if (shard.placement_filter_enabled and !shard.active_groups.contains(group_id)) return error.ApplyStoreGroupRetired;
    }

    fn beginGenerationPreparation(self: *RaftApplyStore, group_id: u64) !void {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        try shard.generation_preparations.ensureUnusedCapacity(self.alloc, 1);
        if (shard.batches.getPtr(group_id) == null) try shard.batches.ensureUnusedCapacity(self.alloc, 1);
        shard.generation_preparations.putAssumeCapacity(group_id, {});
    }

    fn beginSnapshotGenerationPreparation(
        self: *RaftApplyStore,
        group_id: u64,
        snapshot_index: u64,
    ) !bool {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        if (try self.ensureLoaded(shard, group_id)) |current| {
            if (current.commit_index > snapshot_index or current.last_entry_index > snapshot_index)
                return false;
        }
        try shard.generation_preparations.ensureUnusedCapacity(self.alloc, 1);
        if (shard.batches.getPtr(group_id) == null) try shard.batches.ensureUnusedCapacity(self.alloc, 1);
        shard.generation_preparations.putAssumeCapacity(group_id, {});
        return true;
    }

    fn finishGenerationPreparationLocked(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) void {
        std.debug.assert(shard.generation_preparations.remove(group_id));
        self.closeRetiredGroupIfDrainedLocked(shard, group_id);
        shard.state_changed.broadcast(self.io_impl.io());
    }

    fn cancelGenerationPreparation(self: *RaftApplyStore, group_id: u64) void {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        self.finishGenerationPreparationLocked(shard, group_id);
    }

    pub fn snapshotBuilder(self: *RaftApplyStore) raft_state_machine.SnapshotBuilder {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_snapshot = buildSnapshot,
                .prepare_snapshot = prepareSnapshot,
                .install_snapshot = installSnapshotFromRaft,
                .apply_batch = applyBatch,
            },
        };
    }

    pub fn latestBatch(self: *RaftApplyStore, group_id: u64) !?AppliedDataBatch {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const batch = (try self.ensureLoaded(shard, group_id)) orelse return null;
        return batch.*;
    }

    /// Returns a transition watermark without waiting behind snapshot
    /// installation. Transition RPCs are retried by the metadata driver and
    /// must not consume an HTTP worker while a replacement generation stages.
    pub fn latestBatchForTransition(self: *RaftApplyStore, group_id: u64) !?AppliedDataBatch {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.requireTransitionReadyLocked(shard, group_id);
        const batch = (try self.ensureLoaded(shard, group_id)) orelse return null;
        return batch.*;
    }

    /// Request-path protocol observation must never wait behind a generation
    /// import. The caller cannot safely treat a busy or failed lookup as an
    /// absent marker, because the marker is an irreversible log-format floor.
    pub fn raftBatchProtocolVersionForRequest(self: *RaftApplyStore, group_id: u64) !u16 {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.requireTransitionReadyLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return 0;
        return try shard_state_store.currentRaftBatchProtocolVersion(&group_store.store, self.alloc, group_id);
    }

    pub fn appliedNormalEntries(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]AppliedNormalEntry {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return try alloc.alloc(AppliedNormalEntry, 0);
        var prefix_buf: [128]u8 = undefined;
        const prefix = try normalEntryPrefixForGroup(&prefix_buf, group_id);
        const kvs = try group_store.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        var entries = try alloc.alloc(AppliedNormalEntry, kvs.len);
        errdefer {
            for (entries[0..kvs.len]) |entry| alloc.free(entry.data);
            alloc.free(entries);
        }
        for (kvs, 0..) |kv, i| {
            entries[i] = .{
                .index = try parseNormalEntryIndex(kv.key, prefix.len),
                .data = try alloc.dupe(u8, kv.value),
            };
        }
        return entries;
    }

    pub fn groupState(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]AppliedDataKV {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return try alloc.alloc(AppliedDataKV, 0);
        return try shard_state_store.groupState(&group_store.store, alloc, group_id);
    }

    pub fn installSnapshot(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        commit_index: u64,
        encoded: []const u8,
    ) !void {
        const snapshot = try shard_state_store.GroupStateSnapshotStream.init(encoded);
        const empty_batch = try raft_state_machine.encodeCommittedEntries(alloc, &.{});
        defer alloc.free(empty_batch);
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const value = try alloc.alloc(u8, @sizeOf(u64) + empty_batch.len);
        defer alloc.free(value);
        std.mem.writeInt(u64, value[0..8], commit_index, .little);
        @memcpy(value[8..], empty_batch);
        const coverage_start = std.math.add(u64, commit_index, 1) catch return error.InvalidDataApplyBatch;
        var coverage_key_buf: [176]u8 = undefined;
        const coverage_key = try entryIdentityCoverageKeyForGroup(&coverage_key_buf, group_id);
        var coverage_value: [8]u8 = undefined;
        std.mem.writeInt(u64, &coverage_value, coverage_start, .little);

        if (!try self.beginSnapshotGenerationPreparation(group_id, commit_index)) return;
        var preparation_active = true;
        defer if (preparation_active) self.cancelGenerationPreparation(group_id);
        if (builtin.is_test and test_block_snapshot_staging.load(.acquire)) {
            test_snapshot_staging_started.store(true, .release);
            while (test_block_snapshot_staging.load(.acquire)) platform.time.yieldBriefly();
        }

        const path = try self.groupPathAlloc(group_id);
        defer self.alloc.free(path);
        var preparation = try generation_lifecycle.beginProcessPreparationWithRuntime(path, self.backend_runtime);
        defer preparation.deinit();
        var staged = try preparation.beginStaging();
        defer staged.deinit();
        {
            const candidate = try GroupStore.open(
                self.alloc,
                staged.path(),
                self.no_sync,
                false,
                self.resource_manager,
                self.nextGroupStoreAccess(),
            );
            defer candidate.close();
            try shard_state_store.installSnapshotStreamIntoEmptyStore(
                &candidate.store,
                alloc,
                group_id,
                snapshot,
                &.{
                    .{ .key = key, .value = value },
                    .{ .key = coverage_key, .value = &coverage_value },
                },
            );
        }
        try staged.seal();

        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        self.waitForSnapshotReadersLocked(shard, group_id);
        // Staging deliberately releases the shard mutex. Another group on the
        // same shard may consume the earlier reservation, so re-establish the
        // capacity contract while exclusively holding the map immediately
        // before the irreversible generation publication.
        if (shard.batches.getPtr(group_id) == null) try shard.batches.ensureUnusedCapacity(self.alloc, 1);
        if (shard.stores.fetchRemove(group_id)) |removed| removed.value.close();
        var transition = try preparation.promote();
        defer transition.deinit();
        const publication_outcome = try staged.publish();

        const summary = AppliedDataBatch{
            .commit_index = commit_index,
            .entry_count = 0,
            .normal_entry_count = 0,
            .admin_entry_count = 0,
            .last_entry_term = 0,
            .last_entry_index = commit_index,
        };
        if (shard.batches.getPtr(group_id)) |existing| {
            existing.* = summary;
        } else {
            shard.batches.putAssumeCapacity(group_id, summary);
        }
        self.finishGenerationPreparationLocked(shard, group_id);
        preparation_active = false;
        if (publication_outcome == .durability_uncertain) return error.GenerationDurabilityUncertain;
    }

    /// Seeds a group that predates the data-Raft apply projection. Index zero is
    /// reserved for this synthetic baseline so later real Raft entries retain
    /// their original indexes. The per-group apply shard lock makes the
    /// existence check atomic with normal apply. Snapshot data and its baseline
    /// watermark are committed in one DocStore batch for crash-safe retry.
    pub fn seedGroupSnapshotIfAbsent(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        root_incarnation: u128,
        byte_range: AppliedDataRange,
        entries: []const AppliedDataKV,
    ) !bool {
        const encoded = try raft_state_machine.encodeCommittedEntries(alloc, &.{.{
            .term = 0,
            .index = 0,
            .entry_type = .normal,
            .data = @constCast("snapshot_seed"),
        }});
        defer alloc.free(encoded);

        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        if ((try self.ensureLoaded(shard, group_id)) != null) return false;
        try shard.batches.ensureUnusedCapacity(self.alloc, 1);
        const group_store = try self.writableGroupStoreLocked(shard, group_id);

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const value = try alloc.alloc(u8, @sizeOf(u64) + encoded.len);
        defer alloc.free(value);
        std.mem.writeInt(u64, value[0..8], 0, .little);
        @memcpy(value[8..], encoded);
        if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
        const marker_key = try projectionRootIncarnationKeyAlloc(alloc, group_id);
        defer alloc.free(marker_key);
        var marker_value: [16]u8 = undefined;
        std.mem.writeInt(u128, &marker_value, root_incarnation, .little);
        try shard_state_store.replaceGroupSnapshotWithMetadata(
            &group_store.store,
            alloc,
            group_id,
            byte_range,
            entries,
            &.{
                .{ .key = key, .value = value },
                .{ .key = marker_key, .value = &marker_value },
            },
        );

        var summary = try summarizeEntries(alloc, encoded);
        summary.commit_index = 0;
        shard.batches.putAssumeCapacity(group_id, summary);
        return true;
    }

    pub fn seedGroupSnapshotFromAuthoritativeStoreIfAbsent(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        root_incarnation: u128,
        byte_range: AppliedDataRange,
        source: *docstore.DocStore,
        max_page_entries: usize,
        max_page_bytes: usize,
    ) !bool {
        if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
        const encoded = try raft_state_machine.encodeCommittedEntries(alloc, &.{.{
            .term = 0,
            .index = 0,
            .entry_type = .normal,
            .data = @constCast("snapshot_seed"),
        }});
        defer alloc.free(encoded);
        var key_buf: [128]u8 = undefined;
        const batch_key = try keyForGroup(&key_buf, group_id);
        const batch_value = try alloc.alloc(u8, @sizeOf(u64) + encoded.len);
        defer alloc.free(batch_value);
        std.mem.writeInt(u64, batch_value[0..8], 0, .little);
        @memcpy(batch_value[8..], encoded);
        const marker_key = try projectionRootIncarnationKeyAlloc(alloc, group_id);
        defer alloc.free(marker_key);
        var marker_value: [16]u8 = undefined;
        std.mem.writeInt(u128, &marker_value, root_incarnation, .little);

        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        const group_store = prepare: {
            shard.mutex.lockUncancelable(io);
            defer shard.mutex.unlock(io);
            try self.waitForGenerationPreparationLocked(shard, group_id);
            if ((try self.ensureLoaded(shard, group_id)) != null) return false;
            try shard.generation_preparations.ensureUnusedCapacity(self.alloc, 1);
            try shard.batches.ensureUnusedCapacity(self.alloc, 1);
            const store = try self.writableGroupStoreLocked(shard, group_id);
            shard.generation_preparations.putAssumeCapacity(group_id, {});
            break :prepare store;
        };

        var preparation_active = true;
        defer if (preparation_active) self.cancelGenerationPreparation(group_id);
        try shard_state_store.reconcileAuthoritativeGroupDocumentsPaged(
            &group_store.store,
            source,
            alloc,
            group_id,
            byte_range,
            &.{
                .{ .key = batch_key, .value = batch_value },
                .{ .key = marker_key, .value = &marker_value },
            },
            max_page_entries,
            max_page_bytes,
        );

        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        if ((try self.ensureLoaded(shard, group_id)) == null)
            return error.SplitSourceProjectionNotReady;
        var summary = try summarizeEntries(alloc, encoded);
        summary.commit_index = 0;
        if (shard.batches.getPtr(group_id)) |existing| {
            existing.* = summary;
        } else {
            shard.batches.putAssumeCapacity(group_id, summary);
        }
        self.finishGenerationPreparationLocked(shard, group_id);
        preparation_active = false;
        return true;
    }

    /// Reconciles the split projection from the authoritative document DB only
    /// while Raft remains at the caller's observed watermark. This is the
    /// generation-handoff counterpart to `seedGroupSnapshotIfAbsent`: replica
    /// replacement may inherit a document root while its new Raft generation
    /// starts with only a bootstrap entry. Normal-entry history and the apply
    /// watermark are preserved. Reconciliation replaces only projected
    /// document keys, so active split controls and delta history remain
    /// durable while a relocated generation imports its inherited root.
    pub fn reconcileGroupSnapshotAtRootIncarnation(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected: AppliedDataBatch,
        root_incarnation: u128,
        byte_range: AppliedDataRange,
        entries: []const AppliedDataKV,
    ) !bool {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        return try self.reconcileGroupSnapshotAtWatermarkLocked(
            shard,
            alloc,
            group_id,
            expected,
            byte_range,
            entries,
            root_incarnation,
        );
    }

    /// Reconciles an authoritative document root with bounded memory while
    /// excluding Raft apply only for this group. Page commits are additive and
    /// retry-safe; the root marker is the final commit, so readers never mistake
    /// an interrupted import for a complete projection.
    pub fn reconcileGroupSnapshotFromAuthoritativeStoreAtRootIncarnation(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected: AppliedDataBatch,
        root_incarnation: u128,
        byte_range: AppliedDataRange,
        source: *docstore.DocStore,
        max_page_entries: usize,
        max_page_bytes: usize,
    ) !bool {
        if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        const group_store = prepare: {
            shard.mutex.lockUncancelable(io);
            defer shard.mutex.unlock(io);
            try self.waitForGenerationPreparationLocked(shard, group_id);
            const current = (try self.ensureLoaded(shard, group_id)) orelse
                return false;
            if (!batchIdentityEqual(current.*, expected)) return false;
            try shard.generation_preparations.ensureUnusedCapacity(self.alloc, 1);
            const store = try self.writableGroupStoreLocked(shard, group_id);
            shard.generation_preparations.putAssumeCapacity(group_id, {});
            break :prepare store;
        };

        var preparation_active = true;
        defer if (preparation_active) self.cancelGenerationPreparation(group_id);
        const marker_key = try projectionRootIncarnationKeyAlloc(alloc, group_id);
        defer alloc.free(marker_key);
        var marker_value: [16]u8 = undefined;
        std.mem.writeInt(u128, &marker_value, root_incarnation, .little);
        try shard_state_store.reconcileAuthoritativeGroupDocumentsPaged(
            &group_store.store,
            source,
            alloc,
            group_id,
            byte_range,
            &.{.{ .key = marker_key, .value = &marker_value }},
            max_page_entries,
            max_page_bytes,
        );

        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const published = (try self.ensureLoaded(shard, group_id)) orelse
            return error.SplitSourceProjectionNotReady;
        if (!batchIdentityEqual(published.*, expected))
            return error.SplitSourceProjectionNotReady;
        self.finishGenerationPreparationLocked(shard, group_id);
        preparation_active = false;
        return true;
    }

    /// Publishes an authoritative document projection and derives its split
    /// handoff under the same apply-store lock and Raft watermark. A caller can
    /// therefore never acknowledge a handoff from a different projection
    /// generation than the one it just validated.
    pub fn reconcileAndCaptureSplitHandoffAtRootIncarnation(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected: AppliedDataBatch,
        root_incarnation: u128,
        byte_range: AppliedDataRange,
        entries: []const AppliedDataKV,
    ) !?SplitHandoff {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        if (!try self.reconcileGroupSnapshotAtWatermarkLocked(
            shard,
            alloc,
            group_id,
            expected,
            byte_range,
            entries,
            root_incarnation,
        )) return null;
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse
            return error.AppliedDataRangeNotFound;
        return try shard_state_store.captureSplitHandoff(&group_store.store, alloc, group_id);
    }

    /// Fast path for a projection already reconciled against this DB root.
    /// The marker is durable and root-scoped; the exact Raft watermark check
    /// prevents publication after a concurrent apply.
    pub fn captureVerifiedSplitHandoffAtRootIncarnation(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected: AppliedDataBatch,
        root_incarnation: u128,
    ) !?SplitHandoff {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.requireTransitionReadyLocked(shard, group_id);
        const current = (try self.ensureLoaded(shard, group_id)) orelse return null;
        if (!batchIdentityEqual(current.*, expected)) return null;
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return null;
        if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
        const marker_key = try projectionRootIncarnationKeyAlloc(alloc, group_id);
        defer alloc.free(marker_key);
        const marker = group_store.store.get(alloc, marker_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(marker);
        if (marker.len != 16 or std.mem.readInt(u128, marker[0..16], .little) != root_incarnation) return null;
        return try shard_state_store.captureSplitHandoff(&group_store.store, alloc, group_id);
    }

    pub fn captureVerifiedSplitHandoffMetadataAtRootIncarnation(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected: AppliedDataBatch,
        root_incarnation: u128,
    ) !?SplitHandoffMetadata {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.requireTransitionReadyLocked(shard, group_id);
        const current = (try self.ensureLoaded(shard, group_id)) orelse return null;
        if (!batchIdentityEqual(current.*, expected)) return null;
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return null;
        if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
        const marker_key = try projectionRootIncarnationKeyAlloc(alloc, group_id);
        defer alloc.free(marker_key);
        const marker = group_store.store.get(alloc, marker_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(marker);
        if (marker.len != 16 or std.mem.readInt(u128, marker[0..16], .little) != root_incarnation) return null;
        return try shard_state_store.captureSplitHandoffMetadata(&group_store.store, alloc, group_id);
    }

    fn reconcileGroupSnapshotAtWatermarkLocked(
        self: *RaftApplyStore,
        shard: *BatchShard,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected: AppliedDataBatch,
        byte_range: AppliedDataRange,
        entries: []const AppliedDataKV,
        root_incarnation: u128,
    ) !bool {
        try self.requireTransitionReadyLocked(shard, group_id);
        const current = (try self.ensureLoaded(shard, group_id)) orelse return false;
        if (!batchIdentityEqual(current.*, expected)) return false;
        const group_store = try self.writableGroupStoreLocked(shard, group_id);
        const active_split = try shard_state_store.currentSplitState(&group_store.store, alloc, group_id);
        defer if (active_split) |state| shard_state_store.freeSplitState(alloc, state);
        const current_range = try shard_state_store.currentRange(&group_store.store, alloc, group_id);
        defer range_state.freeRange(alloc, current_range);
        // Split start narrows the projected serving range while deliberately
        // retaining right-hand documents for handoff. Import documents from
        // the authoritative DB without widening that serving range again.
        const replacement_range = if (active_split != null) current_range else byte_range;
        // At this exact applied watermark, the projection already contains
        // every committed put/delete. A DB generation that omits one of those
        // keys is stale or incomplete; replacing from it would erase the split
        // handoff while preserving the Raft watermark, making the loss look
        // durable. Values may differ because the inherited DB can be ahead,
        // and additional keys are safe to import.
        const comparison = try shard_state_store.compareGroupSnapshot(&group_store.store, alloc, group_id, byte_range, entries);
        if (!comparison.contains_projected_keys)
            return error.SplitSourceProjectionNotReady;
        if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
        const marker_key = try projectionRootIncarnationKeyAlloc(alloc, group_id);
        defer alloc.free(marker_key);
        var marker_value: [16]u8 = undefined;
        std.mem.writeInt(u128, &marker_value, root_incarnation, .little);
        const metadata_writes: []const docstore.KVPair = &.{.{ .key = marker_key, .value = &marker_value }};
        if (std.mem.eql(u8, current_range.start, replacement_range.start) and
            std.mem.eql(u8, current_range.end, replacement_range.end) and comparison.equal)
        {
            for (metadata_writes) |write| try group_store.store.put(write.key, write.value);
            return true;
        }
        try shard_state_store.replaceGroupSnapshotWithMetadata(
            &group_store.store,
            alloc,
            group_id,
            replacement_range,
            entries,
            metadata_writes,
        );
        return true;
    }

    pub fn currentRange(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !AppliedDataRange {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return .{ .start = "", .end = "" };
        return try shard_state_store.currentRange(&group_store.store, alloc, group_id);
    }

    pub fn currentSplitState(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?AppliedSplitState {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return null;
        return try shard_state_store.currentSplitState(&group_store.store, alloc, group_id);
    }

    /// Reads all source split control fields under one apply-store shard lock.
    /// Unlike the general state accessors, observation fails fast while a new
    /// generation is staging so control-plane polling cannot wedge serving
    /// threads behind snapshot I/O.
    pub fn observeSplitControl(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !SplitControlObservation {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.requireTransitionReadyLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return .{};

        var observation = SplitControlObservation{};
        errdefer observation.deinit(alloc);
        observation.state = try shard_state_store.currentSplitState(&group_store.store, alloc, group_id);
        observation.terminal = try shard_state_store.currentSplitTerminal(&group_store.store, alloc, group_id);
        observation.acknowledgement = try shard_state_store.currentSplitAcknowledgement(&group_store.store, alloc, group_id);
        observation.delta_sequence = try shard_state_store.currentSplitDeltaSequence(&group_store.store, alloc, group_id);
        return observation;
    }

    pub fn currentSplitDeltaSequence(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !u64 {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return 0;
        return try shard_state_store.currentSplitDeltaSequence(&group_store.store, alloc, group_id);
    }

    pub fn currentSplitAcknowledgement(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?shard_state_store.SplitAcknowledgement {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return null;
        return try shard_state_store.currentSplitAcknowledgement(&group_store.store, alloc, group_id);
    }

    pub fn currentSplitTerminal(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?shard_state_store.AppliedSplitTerminal {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return null;
        return try shard_state_store.currentSplitTerminal(&group_store.store, alloc, group_id);
    }

    pub fn captureSplitHandoff(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !SplitHandoff {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return error.AppliedDataRangeNotFound;
        return try shard_state_store.captureSplitHandoff(&group_store.store, alloc, group_id);
    }

    pub fn listSplitDeltasAfter(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, after_seq: u64) ![]shard_state_store.SplitDelta {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return try alloc.alloc(shard_state_store.SplitDelta, 0);
        return try shard_state_store.listDeltasAfter(&group_store.store, alloc, group_id, after_seq);
    }

    pub fn listSplitDeltasPage(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        after_seq: u64,
        through_seq: u64,
        max_entries: usize,
        max_bytes: usize,
    ) ![]shard_state_store.SplitDelta {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse
            return try alloc.alloc(shard_state_store.SplitDelta, 0);
        return try shard_state_store.listDeltasPage(
            &group_store.store,
            alloc,
            group_id,
            after_seq,
            through_seq,
            max_entries,
            max_bytes,
        );
    }

    pub fn groupStatePageInRange(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        byte_range: AppliedDataRange,
        after_key: ?[]const u8,
        max_entries: usize,
        max_bytes: usize,
    ) !GroupStatePage {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse
            return .{ .entries = try alloc.alloc(AppliedDataKV, 0), .exhausted = true };
        return try shard_state_store.groupStatePageInRange(
            &group_store.store,
            alloc,
            group_id,
            byte_range,
            after_key,
            max_entries,
            max_bytes,
        );
    }

    pub fn applySplitHandoff(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, handoff: SplitHandoff) !void {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = try self.writableGroupStoreLocked(shard, group_id);
        try shard_state_store.applyHandoff(&group_store.store, alloc, group_id, handoff);
    }

    pub fn applySplitDeltas(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, deltas: []const shard_state_store.SplitDelta) !void {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = try self.writableGroupStoreLocked(shard, group_id);
        try shard_state_store.applyDeltas(&group_store.store, alloc, group_id, deltas);
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.readableGroupStoreLocked(shard, group_id)) orelse return error.AppliedDataRangeNotFound;
        return try shard_state_store.buildSnapshot(&group_store.store, alloc, group_id);
    }

    const PreparedSnapshot = struct {
        owner: *RaftApplyStore,
        txn: docstore.DocStore.Txn,
        group_id: u64,
        cancelled: std.atomic.Value(bool) = .init(false),

        fn source(self: *@This()) raft_engine.runtime.storage_iface.SnapshotSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .materialize = materialize,
                    .cancel = cancel,
                    .deinit = PreparedSnapshot.deinit,
                },
            };
        }

        fn materialize(ptr: *anyopaque, alloc: std.mem.Allocator) !raft_engine.runtime.storage_iface.SnapshotMaterialization {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.cancelled.load(.acquire)) return error.SnapshotBuildCancelled;
            const io = self.owner.io_impl.io();
            const spool_dir = try std.fmt.allocPrint(alloc, "{s}/snapshot-spool", .{self.owner.root_dir});
            defer alloc.free(spool_dir);
            try fs_paths.createDirPathPortable(io, spool_dir);
            const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}-{d}.snap", .{
                spool_dir,
                self.group_id,
                snapshot_spool_nonce.fetchAdd(1, .monotonic),
            });
            defer alloc.free(path);
            errdefer std.Io.Dir.cwd().deleteFile(io, path) catch {};

            var size: u64 = 0;
            {
                var file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
                defer file.close(io);
                var buffer: [64 * 1024]u8 = undefined;
                var writer = file.writer(io, &buffer);
                try shard_state_store.writeSnapshotTxn(&self.txn, alloc, self.group_id, &writer.interface, &self.cancelled);
                try writer.end();
                try file.sync(io);
                size = (try file.stat(io)).size;
            }
            return .{ .artifact = try raft_storage_mod.file_snapshot_artifact.FileSnapshotArtifact.create(
                alloc,
                io,
                path,
                size,
            ) };
        }

        fn cancel(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cancelled.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.txn.abort();
            self.owner.releaseSnapshotReader(self.group_id);
            std.heap.page_allocator.destroy(self);
        }
    };

    fn prepareSnapshot(ptr: *anyopaque, group_id: u64, applied_index: u64) !?raft_engine.runtime.storage_iface.SnapshotSource {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        const group_store = (try self.groupStoreLocked(shard, group_id, false)) orelse return null;
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const value = group_store.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(value);
        if (value.len < @sizeOf(u64)) return error.InvalidDataApplyBatch;
        if (std.mem.readInt(u64, value[0..8], .little) != applied_index) return error.AppliedSnapshotIndexMismatch;
        try pruneEntryIdentitiesThroughApplied(&group_store.store, self.alloc, group_id, applied_index);

        var txn = try group_store.store.beginReadTxn();
        errdefer txn.abort();

        const readers = try shard.snapshot_readers.getOrPut(self.alloc, group_id);
        if (!readers.found_existing) readers.value_ptr.* = 0;
        readers.value_ptr.* = try std.math.add(usize, readers.value_ptr.*, 1);
        const prepared = std.heap.page_allocator.create(PreparedSnapshot) catch |err| {
            readers.value_ptr.* -= 1;
            if (readers.value_ptr.* == 0) _ = shard.snapshot_readers.remove(group_id);
            return err;
        };
        prepared.* = .{
            .owner = self,
            .txn = txn,
            .group_id = group_id,
        };
        return prepared.source();
    }

    fn installSnapshotFromRaft(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        commit_index: u64,
        encoded: []const u8,
    ) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try self.installSnapshot(alloc, group_id, commit_index, encoded);
    }

    fn applyBatch(ptr: *anyopaque, batch: raft_state_machine.ApplyBatch) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try self.writeBatch(batch.group_id, batch.commit_index, batch.entries_bytes);
    }

    fn writeBatch(self: *RaftApplyStore, group_id: u64, commit_index: u64, entries_bytes: []const u8) !void {
        const io = self.io_impl.io();
        const shard = self.batchShard(group_id);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        try self.waitForGenerationPreparationLocked(shard, group_id);
        try self.writeBatchLocked(shard, group_id, commit_index, entries_bytes);
    }

    fn writeBatchLocked(
        self: *RaftApplyStore,
        shard: *BatchShard,
        group_id: u64,
        commit_index: u64,
        entries_bytes: []const u8,
    ) !void {
        const decoded_entries = raft_state_machine.decodeCommittedEntries(self.alloc, entries_bytes) catch |err| switch (err) {
            error.InvalidCommittedEntriesEncoding => return error.ConflictingDataApplyBatch,
            else => return err,
        };
        defer self.alloc.free(decoded_entries);
        if (decoded_entries.len == 0 or
            !entriesAreContiguous(decoded_entries) or
            decoded_entries[decoded_entries.len - 1].index != commit_index)
        {
            return error.ConflictingDataApplyBatch;
        }

        const existing_batch = try self.ensureLoaded(shard, group_id);
        if (existing_batch) |existing| {
            // The durable state can be ahead when another state machine's applied watermark lags.
            if (commit_index <= existing.commit_index) {
                try self.verifyPersistedBatch(group_id, commit_index, entries_bytes, decoded_entries);
                return;
            }
        } else {
            // Reserve before the durable write so cache publication cannot fail
            // after storage has advanced.
            try shard.batches.ensureUnusedCapacity(self.alloc, 1);
        }
        const group_store = try self.writableGroupStoreLocked(shard, group_id);
        const identity_coverage_start = try ensureEntryIdentityCoverageStart(
            &group_store.store,
            self.alloc,
            group_id,
            if (existing_batch) |existing| existing.last_entry_index else 0,
        );
        if (existing_batch) |existing| {
            // The effects for this prefix are already durable. Verify their
            // exact Raft identities before accepting and applying the suffix;
            // filtering first would silently bless a conflicting recovery
            // window and move the durable watermark beyond it.
            for (decoded_entries) |entry| {
                if (entry.index > existing.last_entry_index) break;
                if (!try persistedEntryIdentityMatches(
                    &group_store.store,
                    self.alloc,
                    group_id,
                    identity_coverage_start,
                    entry,
                )) return error.ConflictingDataApplyBatch;
            }
        }
        // A failed sibling state machine can leave this store ahead of Raft's
        // shared applied watermark. Raft then legitimately presents an
        // overlapping committed prefix. Apply effects only for entries newer
        // than this store's own durable watermark.
        const metadata = try describeDecodedEntries(
            self.alloc,
            decoded_entries,
            if (existing_batch) |existing| existing.last_entry_index else 0,
        );
        defer freeEntryMetadata(self.alloc, metadata);
        var writes = std.ArrayListUnmanaged(docstore.OwnedKVPair).empty;
        defer shard_state_store.freeOwnedWrites(self.alloc, &writes);
        var deletes = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (deletes.items) |key_to_delete| self.alloc.free(key_to_delete);
            deletes.deinit(self.alloc);
        }
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        {
            const owned_key = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(owned_key);
            const value = try self.alloc.alloc(u8, @sizeOf(u64) + entries_bytes.len);
            errdefer self.alloc.free(value);
            std.mem.writeInt(u64, value[0..8], commit_index, .little);
            @memcpy(value[8..], entries_bytes);
            try writes.append(self.alloc, .{ .key = owned_key, .value = value });
        }

        for (metadata.normal_entries) |entry| {
            var normal_key_buf: [160]u8 = undefined;
            const normal_key = try normalEntryKeyForGroup(&normal_key_buf, group_id, entry.index);
            const owned_key = try self.alloc.dupe(u8, normal_key);
            errdefer self.alloc.free(owned_key);
            const owned_value = try self.alloc.dupe(u8, entry.data);
            errdefer self.alloc.free(owned_value);
            try writes.append(self.alloc, .{ .key = owned_key, .value = owned_value });
        }
        for (metadata.entry_identities) |entry| {
            var identity_key_buf: [176]u8 = undefined;
            const identity_key = try entryIdentityKeyForGroup(&identity_key_buf, group_id, entry.index);
            const owned_key = try self.alloc.dupe(u8, identity_key);
            errdefer self.alloc.free(owned_key);
            const owned_value = try self.alloc.alloc(u8, @sizeOf(u64) + 1 + entry.data.len);
            errdefer self.alloc.free(owned_value);
            std.mem.writeInt(u64, owned_value[0..8], entry.term, .little);
            owned_value[8] = @intFromEnum(entry.entry_type);
            @memcpy(owned_value[9..], entry.data);
            try writes.append(self.alloc, .{ .key = owned_key, .value = owned_value });
        }
        try shard_state_store.appendOperationEffects(&group_store.store, self.alloc, group_id, metadata.operations, &writes, &deletes);
        try shard_state_store.putOwnedBatch(&group_store.store, self.alloc, writes.items, deletes.items);

        const summary = AppliedDataBatch{
            .commit_index = commit_index,
            .entry_count = metadata.entry_count,
            .normal_entry_count = metadata.normal_entry_count,
            .admin_entry_count = metadata.admin_entry_count,
            .last_entry_term = metadata.last_entry_term,
            .last_entry_index = metadata.last_entry_index,
        };
        if (shard.batches.getPtr(group_id)) |existing| {
            existing.* = summary;
            return;
        }
        shard.batches.putAssumeCapacity(group_id, summary);
    }

    fn verifyPersistedBatch(
        self: *RaftApplyStore,
        group_id: u64,
        commit_index: u64,
        entries_bytes: []const u8,
        replay_entries: []const raft_state_machine.DecodedCommittedEntry,
    ) !void {
        const shard = self.batchShard(group_id);
        const group_store = (try self.groupStoreLocked(shard, group_id, false)) orelse return error.InvalidDataApplyBatch;
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = group_store.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return error.InvalidDataApplyBatch,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidDataApplyBatch;
        const persisted_commit_index = std.mem.readInt(u64, encoded[0..8], .little);
        if (persisted_commit_index < commit_index) {
            return error.ConflictingDataApplyBatch;
        }
        const coverage_start = try ensureEntryIdentityCoverageStart(
            &group_store.store,
            self.alloc,
            group_id,
            persisted_commit_index,
        );
        const persisted_bytes = encoded[8..];
        if (persisted_commit_index == commit_index and std.mem.eql(u8, persisted_bytes, entries_bytes)) return;

        const persisted_entries = raft_state_machine.decodeCommittedEntries(self.alloc, persisted_bytes) catch |err| switch (err) {
            error.InvalidCommittedEntriesEncoding => return error.ConflictingDataApplyBatch,
            else => return err,
        };
        defer self.alloc.free(persisted_entries);
        if (persisted_entries.len > 0 and
            (!entriesAreContiguous(persisted_entries) or
                persisted_entries[persisted_entries.len - 1].index != persisted_commit_index))
        {
            return error.ConflictingDataApplyBatch;
        }

        var persisted_pos: usize = 0;
        for (replay_entries) |replay| {
            while (persisted_pos < persisted_entries.len and persisted_entries[persisted_pos].index < replay.index) {
                persisted_pos += 1;
            }
            if (persisted_pos < persisted_entries.len and persisted_entries[persisted_pos].index == replay.index) {
                if (!committedEntriesEqual(persisted_entries[persisted_pos], replay)) return error.ConflictingDataApplyBatch;
                continue;
            }
            if (!try persistedEntryIdentityMatches(&group_store.store, self.alloc, group_id, coverage_start, replay)) {
                return error.ConflictingDataApplyBatch;
            }
        }
    }

    fn persistedEntryIdentityMatches(
        store: *docstore.DocStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        coverage_start: u64,
        replay: raft_state_machine.DecodedCommittedEntry,
    ) !bool {
        var identity_key_buf: [176]u8 = undefined;
        const identity_key = try entryIdentityKeyForGroup(&identity_key_buf, group_id, replay.index);
        const identity = store.get(alloc, identity_key) catch |err| switch (err) {
            error.NotFound => {
                if (replay.index >= coverage_start) return false;
                if (replay.entry_type == .normal) {
                    const data_match = try persistedNormalEntryDataMatch(store, alloc, group_id, replay);
                    if (data_match == .match) return true;
                    if (data_match == .mismatch) return false;
                }
                return true;
            },
            else => return err,
        };
        defer alloc.free(identity);
        const header_len = @sizeOf(u64) + 1;
        if (identity.len < header_len or
            std.mem.readInt(u64, identity[0..8], .little) != replay.term or
            identity[8] != @intFromEnum(replay.entry_type))
        {
            return false;
        }
        if (replay.entry_type != .normal or identity.len > header_len) {
            return std.mem.eql(u8, identity[header_len..], replay.data);
        }
        return try persistedNormalEntryDataMatch(store, alloc, group_id, replay) == .match;
    }

    const PersistedNormalEntryDataMatch = enum { match, mismatch, missing };

    fn persistedNormalEntryDataMatch(
        store: *docstore.DocStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        replay: raft_state_machine.DecodedCommittedEntry,
    ) !PersistedNormalEntryDataMatch {
        if (replay.entry_type != .normal) return .mismatch;
        var normal_key_buf: [160]u8 = undefined;
        const normal_key = try normalEntryKeyForGroup(&normal_key_buf, group_id, replay.index);
        const normal_data = store.get(alloc, normal_key) catch |err| switch (err) {
            error.NotFound => return .missing,
            else => return err,
        };
        defer alloc.free(normal_data);
        return if (std.mem.eql(u8, normal_data, replay.data)) .match else .mismatch;
    }

    fn entriesAreContiguous(entries: []const raft_state_machine.DecodedCommittedEntry) bool {
        if (entries.len == 0) return true;
        for (entries[1..], 1..) |entry, i| {
            const previous_index = entries[i - 1].index;
            if (entry.index <= previous_index or entry.index - previous_index != 1) return false;
        }
        return true;
    }

    fn committedEntriesEqual(
        left: raft_state_machine.DecodedCommittedEntry,
        right: raft_state_machine.DecodedCommittedEntry,
    ) bool {
        return left.term == right.term and
            left.index == right.index and
            left.entry_type == right.entry_type and
            std.mem.eql(u8, left.data, right.data);
    }

    fn batchShard(self: *RaftApplyStore, group_id: u64) *BatchShard {
        return &self.batch_shards[@as(usize, @intCast(group_id % batch_shard_count))];
    }

    fn ensureLoaded(self: *RaftApplyStore, shard: *BatchShard, group_id: u64) !?*OwnedBatch {
        if (shard.batches.getPtr(group_id)) |batch| return batch;

        const group_store = (try self.groupStoreLocked(shard, group_id, false)) orelse return null;
        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = group_store.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidDataApplyBatch;

        const commit_index = std.mem.readInt(u64, encoded[0..8], .little);
        var summary = try summarizeEntries(self.alloc, encoded[8..]);
        summary.commit_index = commit_index;
        if (summary.last_entry_index == 0 and commit_index > 0) summary.last_entry_index = commit_index;
        try shard.batches.put(self.alloc, group_id, summary);
        return shard.batches.getPtr(group_id);
    }

    fn summarizeEntries(alloc: std.mem.Allocator, entries_bytes: []const u8) !AppliedDataBatch {
        const decoded = try raft_state_machine.decodeCommittedEntries(alloc, entries_bytes);
        defer alloc.free(decoded);
        var normal_entry_count: usize = 0;
        var admin_entry_count: usize = 0;
        for (decoded) |entry| switch (entry.entry_type) {
            .normal => normal_entry_count += 1,
            .conf_change, .conf_change_v2 => admin_entry_count += 1,
        };
        return .{
            .commit_index = 0,
            .entry_count = decoded.len,
            .normal_entry_count = normal_entry_count,
            .admin_entry_count = admin_entry_count,
            .last_entry_term = if (decoded.len > 0) decoded[decoded.len - 1].term else 0,
            .last_entry_index = if (decoded.len > 0) decoded[decoded.len - 1].index else 0,
        };
    }

    const EntryMetadata = struct {
        entry_count: usize,
        normal_entry_count: usize,
        admin_entry_count: usize,
        last_entry_term: u64,
        last_entry_index: u64,
        normal_entries: []AppliedNormalEntry,
        entry_identities: []AppliedEntryIdentity,
        operations: []DataOperation,
    };

    const AppliedEntryIdentity = struct {
        term: u64,
        index: u64,
        entry_type: raft_engine.core.types.EntryType,
        data: []const u8,
    };

    const DataOperation = shard_state_store.DataOperation;

    fn freeDataOperation(alloc: std.mem.Allocator, operation: DataOperation) void {
        switch (operation) {
            .put => |put| {
                alloc.free(put.key);
                alloc.free(put.value);
            },
            .delete => |key| alloc.free(key),
            .set_range => |range| {
                alloc.free(range.start);
                alloc.free(range.end);
            },
            .prepare_split, .start_split, .finalize_split, .rollback_split => |transition| alloc.free(transition.split_key),
            .acknowledge_split, .set_raft_batch_protocol, .flush_split_delta => {},
        }
    }

    fn freeDataOperations(alloc: std.mem.Allocator, operations: []DataOperation) void {
        for (operations) |operation| freeDataOperation(alloc, operation);
        alloc.free(operations);
    }

    fn freeEntryMetadata(alloc: std.mem.Allocator, metadata: EntryMetadata) void {
        for (metadata.normal_entries) |entry| alloc.free(entry.data);
        alloc.free(metadata.normal_entries);
        alloc.free(metadata.entry_identities);
        freeDataOperations(alloc, metadata.operations);
    }

    fn describeEntries(alloc: std.mem.Allocator, entries_bytes: []const u8, after_index: u64) !EntryMetadata {
        const decoded = try raft_state_machine.decodeCommittedEntries(alloc, entries_bytes);
        defer alloc.free(decoded);
        return try describeDecodedEntries(alloc, decoded, after_index);
    }

    fn describeDecodedEntries(
        alloc: std.mem.Allocator,
        decoded: []const raft_state_machine.DecodedCommittedEntry,
        after_index: u64,
    ) !EntryMetadata {
        var normal_entry_count: usize = 0;
        var admin_entry_count: usize = 0;
        var normal_entries = std.ArrayListUnmanaged(AppliedNormalEntry).empty;
        var entry_identities = std.ArrayListUnmanaged(AppliedEntryIdentity).empty;
        var operations = std.ArrayListUnmanaged(DataOperation).empty;
        errdefer {
            for (normal_entries.items) |entry| alloc.free(entry.data);
            normal_entries.deinit(alloc);
        }
        errdefer {
            entry_identities.deinit(alloc);
        }
        errdefer {
            for (operations.items) |op| switch (op) {
                .put => |put| {
                    alloc.free(put.key);
                    alloc.free(put.value);
                },
                .delete => |key_to_delete| alloc.free(key_to_delete),
                .set_range => |range| {
                    alloc.free(range.start);
                    alloc.free(range.end);
                },
                .prepare_split, .start_split, .finalize_split, .rollback_split => |transition| alloc.free(transition.split_key),
                .acknowledge_split, .set_raft_batch_protocol, .flush_split_delta => {},
            };
            operations.deinit(alloc);
        }
        for (decoded) |entry| {
            if (entry.index > after_index) {
                try entry_identities.append(alloc, .{
                    .term = entry.term,
                    .index = entry.index,
                    .entry_type = entry.entry_type,
                    .data = if (entry.entry_type == .normal) &.{} else entry.data,
                });
            }
            switch (entry.entry_type) {
                .normal => {
                    normal_entry_count += 1;
                    if (entry.index <= after_index) continue;
                    {
                        const data = try alloc.dupe(u8, entry.data);
                        errdefer alloc.free(data);
                        try normal_entries.append(alloc, .{ .index = entry.index, .data = data });
                    }
                    try appendDataOperations(alloc, entry.index, entry.data, &operations);
                },
                .conf_change, .conf_change_v2 => admin_entry_count += 1,
            }
        }
        const owned_normal_entries = try normal_entries.toOwnedSlice(alloc);
        errdefer {
            for (owned_normal_entries) |entry| alloc.free(entry.data);
            alloc.free(owned_normal_entries);
        }
        const owned_entry_identities = try entry_identities.toOwnedSlice(alloc);
        errdefer alloc.free(owned_entry_identities);
        const owned_operations = try operations.toOwnedSlice(alloc);
        errdefer freeDataOperations(alloc, owned_operations);
        const last = if (decoded.len == 0) null else decoded[decoded.len - 1];
        return .{
            .entry_count = decoded.len,
            .normal_entry_count = normal_entry_count,
            .admin_entry_count = admin_entry_count,
            .last_entry_term = if (last) |entry| entry.term else 0,
            .last_entry_index = if (last) |entry| entry.index else 0,
            .normal_entries = owned_normal_entries,
            .entry_identities = owned_entry_identities,
            .operations = owned_operations,
        };
    }

    fn keyForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_raft_apply:{d}", .{group_id});
    }

    fn projectionRootIncarnationKeyAlloc(alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        return try std.fmt.allocPrint(alloc, "\x00\x00__metadata__:data_projection_root_incarnation:{d}", .{group_id});
    }

    fn batchIdentityEqual(left: AppliedDataBatch, right: AppliedDataBatch) bool {
        return left.commit_index == right.commit_index and
            left.last_entry_index == right.last_entry_index and
            left.last_entry_term == right.last_entry_term;
    }

    fn normalEntryPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_raft_normal:{d}:", .{group_id});
    }

    fn normalEntryKeyForGroup(buf: []u8, group_id: u64, index: u64) ![]const u8 {
        const prefix = try normalEntryPrefixForGroup(buf[0 .. buf.len - 8], group_id);
        const suffix: *[8]u8 = @ptrCast(buf[prefix.len .. prefix.len + 8]);
        std.mem.writeInt(u64, suffix, index, .big);
        return buf[0 .. prefix.len + 8];
    }

    fn entryIdentityPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_raft_entry_identity:{d}:", .{group_id});
    }

    fn entryIdentityKeyForGroup(buf: []u8, group_id: u64, index: u64) ![]const u8 {
        const prefix = try entryIdentityPrefixForGroup(buf[0 .. buf.len - 8], group_id);
        const suffix: *[8]u8 = @ptrCast(buf[prefix.len .. prefix.len + 8]);
        std.mem.writeInt(u64, suffix, index, .big);
        return buf[0 .. prefix.len + 8];
    }

    fn entryIdentityCoverageKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:data_raft_entry_identity_coverage:{d}", .{group_id});
    }

    fn parseEntryIdentityIndex(key: []const u8, prefix_len: usize) !u64 {
        if (key.len != prefix_len + 8) return error.InvalidAppliedEntryIdentityKey;
        return std.mem.readInt(u64, key[prefix_len..][0..8], .big);
    }

    fn loadEntryIdentityCoverageStart(
        store: *docstore.DocStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !?u64 {
        var key_buf: [176]u8 = undefined;
        const key = try entryIdentityCoverageKeyForGroup(&key_buf, group_id);
        const value = store.get(alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(value);
        if (value.len != @sizeOf(u64)) return error.InvalidDataApplyBatch;
        return std.mem.readInt(u64, value[0..8], .little);
    }

    fn ensureEntryIdentityCoverageStart(
        store: *docstore.DocStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        legacy_through_index: u64,
    ) !u64 {
        if (try loadEntryIdentityCoverageStart(store, alloc, group_id)) |coverage_start| return coverage_start;
        const coverage_start = std.math.add(u64, legacy_through_index, 1) catch return error.InvalidDataApplyBatch;
        var key_buf: [176]u8 = undefined;
        const key = try entryIdentityCoverageKeyForGroup(&key_buf, group_id);
        var value: [8]u8 = undefined;
        std.mem.writeInt(u64, &value, coverage_start, .little);
        try store.put(key, &value);
        return coverage_start;
    }

    fn pruneEntryIdentitiesThroughApplied(
        store: *docstore.DocStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        applied_index: u64,
    ) !void {
        const next_coverage_start = std.math.add(u64, applied_index, 1) catch return error.InvalidDataApplyBatch;
        const current_coverage_start = try loadEntryIdentityCoverageStart(store, alloc, group_id);
        const coverage_start = @max(current_coverage_start orelse 1, next_coverage_start);
        var prefix_buf: [176]u8 = undefined;
        const prefix = try entryIdentityPrefixForGroup(&prefix_buf, group_id);
        const entries = try store.scanPrefix(alloc, prefix);
        defer {
            for (entries) |entry| {
                alloc.free(entry.key);
                alloc.free(entry.value);
            }
            alloc.free(entries);
        }
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(alloc);
        for (entries) |entry| {
            if (try parseEntryIdentityIndex(entry.key, prefix.len) <= applied_index) {
                try deletes.append(alloc, entry.key);
            }
        }
        if (deletes.items.len == 0 and current_coverage_start != null and current_coverage_start.? == coverage_start) return;
        var coverage_key_buf: [176]u8 = undefined;
        const coverage_key = try entryIdentityCoverageKeyForGroup(&coverage_key_buf, group_id);
        var coverage_value: [8]u8 = undefined;
        std.mem.writeInt(u64, &coverage_value, coverage_start, .little);
        try store.putBatch(&.{.{ .key = coverage_key, .value = &coverage_value }}, deletes.items);
    }

    fn parseNormalEntryIndex(key: []const u8, prefix_len: usize) !u64 {
        if (key.len != prefix_len + 8) return error.InvalidAppliedNormalEntryKey;
        return std.mem.readInt(u64, key[prefix_len..][0..8], .big);
    }

    fn parseSplitTransition(alloc: std.mem.Allocator, payload: []const u8) !shard_state_store.SplitTransition {
        const transition_sep = std.mem.indexOfScalar(u8, payload, ':') orelse return error.InvalidAppliedDataRange;
        const attempt_start = transition_sep + 1;
        const attempt_rel_sep = std.mem.indexOfScalar(u8, payload[attempt_start..], ':') orelse
            return error.InvalidAppliedDataRange;
        const attempt_sep = attempt_start + attempt_rel_sep;
        const destination_start = attempt_sep + 1;
        const destination_rel_sep = std.mem.indexOfScalar(u8, payload[destination_start..], ':') orelse
            return error.InvalidAppliedDataRange;
        const destination_sep = destination_start + destination_rel_sep;
        const transition_id = try std.fmt.parseInt(u64, payload[0..transition_sep], 10);
        const attempt_epoch = try std.fmt.parseInt(u64, payload[attempt_start..attempt_sep], 10);
        const destination_group_id = try std.fmt.parseInt(u64, payload[destination_start..destination_sep], 10);
        const split_key = payload[destination_sep + 1 ..];
        if (transition_id == 0 or attempt_epoch == 0 or destination_group_id == 0 or split_key.len == 0)
            return error.InvalidAppliedDataRange;
        return .{
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .new_shard_id = destination_group_id,
            .split_key = try alloc.dupe(u8, split_key),
        };
    }

    fn parseDataOperation(alloc: std.mem.Allocator, data: []const u8) !?DataOperation {
        if (std.mem.startsWith(u8, data, "range:")) {
            const payload = data["range:".len..];
            if (std.mem.indexOfScalar(u8, payload, ':')) |first_sep| {
                if (first_sep > 0) {
                    const namespace = payload[0 .. first_sep + 1];
                    if (std.mem.indexOfPos(u8, payload, namespace.len, namespace)) |repeat_pos| {
                        const sep = repeat_pos - 1;
                        const start = try alloc.dupe(u8, payload[0..sep]);
                        errdefer alloc.free(start);
                        const end = try alloc.dupe(u8, payload[repeat_pos..]);
                        return .{ .set_range = .{
                            .start = start,
                            .end = end,
                        } };
                    }
                }
            }
            if (std.mem.indexOfScalar(u8, payload, ':')) |sep| {
                const start = try alloc.dupe(u8, payload[0..sep]);
                errdefer alloc.free(start);
                const end = try alloc.dupe(u8, payload[sep + 1 ..]);
                return .{ .set_range = .{
                    .start = start,
                    .end = end,
                } };
            }
            return error.InvalidAppliedDataRange;
        }
        if (std.mem.startsWith(u8, data, "split_prepare:")) {
            return .{ .prepare_split = try parseSplitTransition(alloc, data["split_prepare:".len..]) };
        }
        if (std.mem.startsWith(u8, data, "split_start:")) {
            return .{ .start_split = try parseSplitTransition(alloc, data["split_start:".len..]) };
        }
        if (std.mem.startsWith(u8, data, "split_finalize:") or std.mem.startsWith(u8, data, "finalize_split:")) {
            const prefix_len = if (std.mem.startsWith(u8, data, "split_finalize:")) "split_finalize:".len else "finalize_split:".len;
            return .{ .finalize_split = try parseSplitTransition(alloc, data[prefix_len..]) };
        }
        if (std.mem.startsWith(u8, data, "split_rollback:") or std.mem.startsWith(u8, data, "rollback_split:")) {
            const prefix_len = if (std.mem.startsWith(u8, data, "split_rollback:")) "split_rollback:".len else "rollback_split:".len;
            return .{ .rollback_split = try parseSplitTransition(alloc, data[prefix_len..]) };
        }
        if (std.mem.startsWith(u8, data, "put:")) {
            const payload = data["put:".len..];
            if (std.mem.indexOfScalar(u8, payload, '=')) |sep| {
                const key = try alloc.dupe(u8, payload[0..sep]);
                errdefer alloc.free(key);
                const value = try alloc.dupe(u8, payload[sep + 1 ..]);
                return .{ .put = .{
                    .key = key,
                    .value = value,
                } };
            }
            const key = try alloc.dupe(u8, payload);
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, "");
            return .{ .put = .{
                .key = key,
                .value = value,
            } };
        }
        if (std.mem.startsWith(u8, data, "del:")) {
            return .{ .delete = try alloc.dupe(u8, data["del:".len..]) };
        }
        return null;
    }

    fn appendDataOperations(
        alloc: std.mem.Allocator,
        raft_index: u64,
        data: []const u8,
        operations: *std.ArrayListUnmanaged(DataOperation),
    ) !void {
        const operation_start = operations.items.len;
        if (!data_raft_batch.looksLikeEnvelope(data)) {
            if (try parseDataOperation(alloc, data)) |operation| {
                operations.append(alloc, operation) catch |err| {
                    freeDataOperation(alloc, operation);
                    return err;
                };
            }
            if (operations.items.len != operation_start) try operations.append(alloc, .{ .flush_split_delta = raft_index });
            return;
        }

        var decoded = try data_raft_batch.decode(alloc, data);
        defer decoded.deinit(alloc);
        if (decoded.protocol_barrier_version) |version| {
            try operations.append(alloc, .{ .set_raft_batch_protocol = version });
            return;
        }
        for (decoded.batch.req.writes) |write| {
            const key = try alloc.dupe(u8, write.key);
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, write.value);
            errdefer alloc.free(value);
            try operations.append(alloc, .{ .put = .{ .key = key, .value = value } });
        }
        for (decoded.batch.req.deletes) |key| {
            const owned_key = try alloc.dupe(u8, key);
            errdefer alloc.free(owned_key);
            try operations.append(alloc, .{ .delete = owned_key });
        }
        if (decoded.batch.req.split_transition) |transition| switch (transition.kind) {
            .prepare => {
                const split_key = try alloc.dupe(u8, transition.split_key);
                errdefer alloc.free(split_key);
                try operations.append(alloc, .{ .prepare_split = .{
                    .transition_id = transition.transition_id,
                    .attempt_epoch = transition.attempt_epoch,
                    .new_shard_id = transition.destination_group_id,
                    .split_key = split_key,
                } });
            },
            .start => {
                const split_key = try alloc.dupe(u8, transition.split_key);
                errdefer alloc.free(split_key);
                try operations.append(alloc, .{ .start_split = .{
                    .transition_id = transition.transition_id,
                    .attempt_epoch = transition.attempt_epoch,
                    .new_shard_id = transition.destination_group_id,
                    .split_key = split_key,
                } });
            },
            .finalize, .rollback => {
                const split_key = try alloc.dupe(u8, transition.split_key);
                errdefer alloc.free(split_key);
                const operation: shard_state_store.DataOperation = switch (transition.kind) {
                    .finalize => .{ .finalize_split = .{
                        .transition_id = transition.transition_id,
                        .attempt_epoch = transition.attempt_epoch,
                        .new_shard_id = transition.destination_group_id,
                        .split_key = split_key,
                    } },
                    .rollback => .{ .rollback_split = .{
                        .transition_id = transition.transition_id,
                        .attempt_epoch = transition.attempt_epoch,
                        .new_shard_id = transition.destination_group_id,
                        .split_key = split_key,
                    } },
                    else => unreachable,
                };
                try operations.append(alloc, operation);
            },
        };
        if (decoded.batch.req.split_checkpoint) |checkpoint| {
            if (checkpoint.kind == .source_ack) {
                try operations.append(alloc, .{ .acknowledge_split = .{
                    .transition_id = checkpoint.transition_id,
                    .attempt_epoch = checkpoint.attempt_epoch,
                    .destination_group_id = checkpoint.destination_group_id,
                    .delta_sequence = checkpoint.delta_sequence,
                } });
            }
        }
        if (operations.items.len != operation_start) try operations.append(alloc, .{ .flush_split_delta = raft_index });
    }
};

test "data raft apply store persists batches across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const encoded = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 3, .index = 14, .entry_type = .normal, .data = @constCast("put:a=") },
            .{ .term = 3, .index = 15, .entry_type = .normal, .data = @constCast("put:b=") },
        });
        defer std.testing.allocator.free(encoded);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 31,
            .commit_index = 15,
            .entries_bytes = encoded,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const batch = (try store.latestBatch(31)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 15), batch.commit_index);
        try std.testing.expectEqual(@as(usize, 2), batch.entry_count);
        try std.testing.expectEqual(@as(usize, 2), batch.normal_entry_count);
        try std.testing.expectEqual(@as(usize, 0), batch.admin_entry_count);
        try std.testing.expectEqual(@as(u64, 3), batch.last_entry_term);
        try std.testing.expectEqual(@as(u64, 15), batch.last_entry_index);
        const normal_entries = try store.appliedNormalEntries(std.testing.allocator, 31);
        defer {
            for (normal_entries) |entry| std.testing.allocator.free(entry.data);
            std.testing.allocator.free(normal_entries);
        }
        try std.testing.expectEqual(@as(usize, 2), normal_entries.len);
        try std.testing.expectEqual(@as(u64, 14), normal_entries[0].index);
        try std.testing.expectEqualStrings("put:b=", normal_entries[1].data);
        const group_state = try store.groupState(std.testing.allocator, 31);
        defer {
            for (group_state) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
            std.testing.allocator.free(group_state);
        }
        try std.testing.expectEqual(@as(usize, 2), group_state.len);
        try std.testing.expectEqualStrings("a", group_state[0].key);
        try std.testing.expectEqualStrings("", group_state[0].value);
        try std.testing.expectEqualStrings("b", group_state[1].key);
        try std.testing.expectEqualStrings("", group_state[1].value);
    }
}

test "data raft protocol barrier persists and transfers in snapshots" {
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/protocol-source", .{source_tmp.sub_path});
    defer std.testing.allocator.free(source_root);

    var source = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer source.deinit();
    const barrier = try data_raft_batch.encodeProtocolBarrier(
        std.testing.allocator,
        "docs",
        data_raft_batch.timestamp_protocol_version,
    );
    defer std.testing.allocator.free(barrier);
    const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = barrier,
    }});
    defer std.testing.allocator.free(entries);
    try source.snapshotBuilder().applyBatch(.{
        .group_id = 45,
        .commit_index = 1,
        .entries_bytes = entries,
    });
    try std.testing.expectEqual(
        data_raft_batch.timestamp_protocol_version,
        try source.raftBatchProtocolVersionForRequest(45),
    );

    const snapshot = try source.snapshotBuilder().buildSnapshot(std.testing.allocator, 45);
    defer std.testing.allocator.free(snapshot);
    var target_tmp = std.testing.tmpDir(.{});
    defer target_tmp.cleanup();
    const target_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/protocol-target", .{target_tmp.sub_path});
    defer std.testing.allocator.free(target_root);
    var target = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = target_root });
    defer target.deinit();
    try std.testing.expect(try target.snapshotBuilder().installSnapshot(
        std.testing.allocator,
        45,
        1,
        snapshot,
    ));
    try std.testing.expectEqual(
        data_raft_batch.timestamp_protocol_version,
        try target.raftBatchProtocolVersionForRequest(45),
    );
}

test "data raft protocol request observation never waits for generation preparation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/protocol-busy", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const group_id: u64 = 46;
    const shard = store.batchShard(group_id);
    {
        shard.mutex.lockUncancelable(store.io_impl.io());
        defer shard.mutex.unlock(store.io_impl.io());
        try shard.generation_preparations.put(std.testing.allocator, group_id, {});
    }
    defer store.cancelGenerationPreparation(group_id);

    try std.testing.expectError(
        error.SplitSourceProjectionNotReady,
        store.raftBatchProtocolVersionForRequest(group_id),
    );
}

test "data raft apply store accepts equivalent restart replay with different batch boundaries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-replay", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const first = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("put:a=1") },
        });
        defer std.testing.allocator.free(first);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 1,
            .entries_bytes = first,
        });

        const second = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:b=2") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(second);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 3,
            .entries_bytes = second,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const replay = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("put:a=1") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:b=2") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(replay);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 3,
            .entries_bytes = replay,
        });

        const suffix = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(suffix);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 3,
            .entries_bytes = suffix,
        });

        const conflicting_overlap = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:b=wrong") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(conflicting_overlap);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 3,
            .entries_bytes = conflicting_overlap,
        }));

        const conflicting = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:c=wrong") },
        });
        defer std.testing.allocator.free(conflicting);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 3,
            .entries_bytes = conflicting,
        }));

        const conflicting_term = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(conflicting_term);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 3,
            .entries_bytes = conflicting_term,
        }));

        const conflicting_type = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .conf_change, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(conflicting_type);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 32,
            .commit_index = 3,
            .entries_bytes = conflicting_type,
        }));

        const batch = (try store.latestBatch(32)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 3), batch.commit_index);
        try std.testing.expectEqual(@as(usize, 2), batch.entry_count);
        try std.testing.expectEqual(@as(u64, 1), batch.last_entry_term);
        try std.testing.expectEqual(@as(u64, 3), batch.last_entry_index);
        const normal_entries = try store.appliedNormalEntries(std.testing.allocator, 32);
        defer {
            for (normal_entries) |entry| std.testing.allocator.free(entry.data);
            std.testing.allocator.free(normal_entries);
        }
        try std.testing.expectEqual(@as(usize, 3), normal_entries.len);

        const group_state = try store.groupState(std.testing.allocator, 32);
        defer {
            for (group_state) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
            std.testing.allocator.free(group_state);
        }
        try std.testing.expectEqual(@as(usize, 3), group_state.len);
        try std.testing.expectEqualStrings("1", group_state[0].value);
        try std.testing.expectEqualStrings("2", group_state[1].value);
        try std.testing.expectEqualStrings("3", group_state[2].value);
    }
}

test "data raft apply store rejects conflicting and malformed advancing overlap" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-forward-overlap", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const initial = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("put:a=1") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:b=2") },
    });
    defer std.testing.allocator.free(initial);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 36,
        .commit_index = 2,
        .entries_bytes = initial,
    });

    const conflicting_overlap = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = @constCast("put:b=wrong") },
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("put:c=3") },
    });
    defer std.testing.allocator.free(conflicting_overlap);
    try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 36,
        .commit_index = 3,
        .entries_bytes = conflicting_overlap,
    }));

    const discontinuous_overlap = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:b=2") },
        .{ .term = 2, .index = 4, .entry_type = .normal, .data = @constCast("put:d=4") },
    });
    defer std.testing.allocator.free(discontinuous_overlap);
    try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 36,
        .commit_index = 4,
        .entries_bytes = discontinuous_overlap,
    }));

    const wrong_commit = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 2,
        .index = 3,
        .entry_type = .normal,
        .data = @constCast("put:c=3"),
    }});
    defer std.testing.allocator.free(wrong_commit);
    try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 36,
        .commit_index = 4,
        .entries_bytes = wrong_commit,
    }));

    const valid_overlap = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:b=2") },
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("put:c=3") },
    });
    defer std.testing.allocator.free(valid_overlap);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 36,
        .commit_index = 3,
        .entries_bytes = valid_overlap,
    });

    const batch = (try store.latestBatch(36)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 3), batch.commit_index);
    const group_state = try store.groupState(std.testing.allocator, 36);
    defer {
        for (group_state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(group_state);
    }
    try std.testing.expectEqual(@as(usize, 3), group_state.len);
    try std.testing.expectEqualStrings("1", group_state[0].value);
    try std.testing.expectEqualStrings("2", group_state[1].value);
    try std.testing.expectEqualStrings("3", group_state[2].value);
}

test "data raft apply store accepts restart replay split below the durable watermark" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-replay-split", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("put:a=1") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:b=2") },
        });
        defer std.testing.allocator.free(entries);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 33,
            .commit_index = 2,
            .entries_bytes = entries,
        });
        const later = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 1,
            .index = 3,
            .entry_type = .normal,
            .data = @constCast("put:c=3"),
        }});
        defer std.testing.allocator.free(later);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 33,
            .commit_index = 3,
            .entries_bytes = later,
        });

        const shard = store.batchShard(33);
        shard.mutex.lockUncancelable(store.io_impl.io());
        defer shard.mutex.unlock(store.io_impl.io());
        const group_store = (try store.groupStoreLocked(shard, 33, false)) orelse return error.MissingGroupStore;
        var identity_key_buf: [176]u8 = undefined;
        const identity_key = try RaftApplyStore.entryIdentityKeyForGroup(&identity_key_buf, 33, 1);
        const identity = try group_store.store.get(std.testing.allocator, identity_key);
        defer std.testing.allocator.free(identity);
        try std.testing.expectEqual(@as(usize, @sizeOf(u64) + 1), identity.len);
        try std.testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, identity[0..8], .little));
        try std.testing.expectEqual(@intFromEnum(raft_engine.core.types.EntryType.normal), identity[8]);
        var normal_key_buf: [160]u8 = undefined;
        const normal_key = try RaftApplyStore.normalEntryKeyForGroup(&normal_key_buf, 33, 1);
        const normal_data = try group_store.store.get(std.testing.allocator, normal_key);
        defer std.testing.allocator.free(normal_data);
        try std.testing.expectEqualStrings("put:a=1", normal_data);

        const legacy_payload = "put:b=2";
        var legacy_identity: [@sizeOf(u64) + 1 + legacy_payload.len]u8 = undefined;
        std.mem.writeInt(u64, legacy_identity[0..8], 1, .little);
        legacy_identity[8] = @intFromEnum(raft_engine.core.types.EntryType.normal);
        @memcpy(legacy_identity[9..], legacy_payload);
        const legacy_identity_key = try RaftApplyStore.entryIdentityKeyForGroup(&identity_key_buf, 33, 2);
        try group_store.store.put(legacy_identity_key, &legacy_identity);
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const first = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 1,
            .index = 1,
            .entry_type = .normal,
            .data = @constCast("put:a=1"),
        }});
        defer std.testing.allocator.free(first);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 33,
            .commit_index = 1,
            .entries_bytes = first,
        });

        const conflicting_data = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 1,
            .index = 1,
            .entry_type = .normal,
            .data = @constCast("put:a=wrong"),
        }});
        defer std.testing.allocator.free(conflicting_data);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 33,
            .commit_index = 1,
            .entries_bytes = conflicting_data,
        }));

        const conflicting_term = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 2,
            .index = 1,
            .entry_type = .normal,
            .data = @constCast("put:a=1"),
        }});
        defer std.testing.allocator.free(conflicting_term);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 33,
            .commit_index = 1,
            .entries_bytes = conflicting_term,
        }));

        const conflicting_type = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 1,
            .index = 1,
            .entry_type = .conf_change,
            .data = @constCast("put:a=1"),
        }});
        defer std.testing.allocator.free(conflicting_type);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 33,
            .commit_index = 1,
            .entries_bytes = conflicting_type,
        }));

        const second = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 1,
            .index = 2,
            .entry_type = .normal,
            .data = @constCast("put:b=2"),
        }});
        defer std.testing.allocator.free(second);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 33,
            .commit_index = 2,
            .entries_bytes = second,
        });

        const state = try store.groupState(std.testing.allocator, 33);
        defer shard_state_store.freeGroupStateEntries(std.testing.allocator, state);
        try std.testing.expectEqual(@as(usize, 3), state.len);
        try std.testing.expectEqualStrings("1", state[0].value);
        try std.testing.expectEqualStrings("2", state[1].value);
        try std.testing.expectEqualStrings("3", state[2].value);
    }
}

test "data raft apply store accepts restart replay split below the durable watermark during identity upgrade" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-replay-upgrade", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const legacy = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 2,
            .index = 1,
            .entry_type = .conf_change,
            .data = @constCast("legacy-admin"),
        }});
        defer std.testing.allocator.free(legacy);
        try store.snapshotBuilder().applyBatch(.{ .group_id = 34, .commit_index = 1, .entries_bytes = legacy });
        const later = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 2,
            .index = 2,
            .entry_type = .normal,
            .data = @constCast("put:upgrade=1"),
        }});
        defer std.testing.allocator.free(later);
        try store.snapshotBuilder().applyBatch(.{ .group_id = 34, .commit_index = 2, .entries_bytes = later });

        const shard = store.batchShard(34);
        shard.mutex.lockUncancelable(store.io_impl.io());
        defer shard.mutex.unlock(store.io_impl.io());
        const group_store = (try store.groupStoreLocked(shard, 34, false)) orelse return error.MissingGroupStore;
        var identity_key_buf: [176]u8 = undefined;
        const identity_key = try RaftApplyStore.entryIdentityKeyForGroup(&identity_key_buf, 34, 1);
        try group_store.store.delete(identity_key);
        var coverage_key_buf: [176]u8 = undefined;
        const coverage_key = try RaftApplyStore.entryIdentityCoverageKeyForGroup(&coverage_key_buf, 34);
        try group_store.store.delete(coverage_key);
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const legacy = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 2,
            .index = 1,
            .entry_type = .conf_change,
            .data = @constCast("legacy-admin"),
        }});
        defer std.testing.allocator.free(legacy);
        try store.snapshotBuilder().applyBatch(.{ .group_id = 34, .commit_index = 1, .entries_bytes = legacy });

        const shard = store.batchShard(34);
        shard.mutex.lockUncancelable(store.io_impl.io());
        {
            defer shard.mutex.unlock(store.io_impl.io());
            const group_store = (try store.groupStoreLocked(shard, 34, false)) orelse return error.MissingGroupStore;
            var coverage_key_buf: [176]u8 = undefined;
            const coverage_key = try RaftApplyStore.entryIdentityCoverageKeyForGroup(&coverage_key_buf, 34);
            const coverage = try group_store.store.get(std.testing.allocator, coverage_key);
            defer std.testing.allocator.free(coverage);
            try std.testing.expectEqual(@as(usize, @sizeOf(u64)), coverage.len);
            try std.testing.expectEqual(@as(u64, 3), std.mem.readInt(u64, coverage[0..8], .little));
        }

        const covered = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 3,
            .index = 3,
            .entry_type = .conf_change_v2,
            .data = @constCast("covered-admin"),
        }});
        defer std.testing.allocator.free(covered);
        try store.snapshotBuilder().applyBatch(.{ .group_id = 34, .commit_index = 3, .entries_bytes = covered });
        const suffix = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 3,
            .index = 4,
            .entry_type = .normal,
            .data = @constCast("put:covered=1"),
        }});
        defer std.testing.allocator.free(suffix);
        try store.snapshotBuilder().applyBatch(.{ .group_id = 34, .commit_index = 4, .entries_bytes = suffix });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const conflicting = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 3,
            .index = 3,
            .entry_type = .conf_change_v2,
            .data = @constCast("wrong-covered-admin"),
        }});
        defer std.testing.allocator.free(conflicting);
        try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
            .group_id = 34,
            .commit_index = 3,
            .entries_bytes = conflicting,
        }));
    }
}

test "data raft apply store accepts restart replay split below the durable watermark after identity pruning" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-replay-prune", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const prefix = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 4, .index = 1, .entry_type = .conf_change, .data = @constCast("admin-1") },
        .{ .term = 4, .index = 2, .entry_type = .conf_change_v2, .data = @constCast("admin-2") },
    });
    defer std.testing.allocator.free(prefix);
    try store.snapshotBuilder().applyBatch(.{ .group_id = 35, .commit_index = 2, .entries_bytes = prefix });
    const third = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 4,
        .index = 3,
        .entry_type = .normal,
        .data = @constCast("put:pruned=1"),
    }});
    defer std.testing.allocator.free(third);
    try store.snapshotBuilder().applyBatch(.{ .group_id = 35, .commit_index = 3, .entries_bytes = third });

    try std.testing.expectError(error.AppliedSnapshotIndexMismatch, store.snapshotBuilder().prepareSnapshot(35, 2));
    {
        const shard = store.batchShard(35);
        shard.mutex.lockUncancelable(store.io_impl.io());
        defer shard.mutex.unlock(store.io_impl.io());
        const group_store = (try store.groupStoreLocked(shard, 35, false)) orelse return error.MissingGroupStore;
        var identity_key_buf: [176]u8 = undefined;
        const identity_key = try RaftApplyStore.entryIdentityKeyForGroup(&identity_key_buf, 35, 1);
        const retained = try group_store.store.get(std.testing.allocator, identity_key);
        defer std.testing.allocator.free(retained);
    }

    const snapshot = (try store.snapshotBuilder().prepareSnapshot(35, 3)) orelse return error.MissingDataSnapshotSource;
    snapshot.deinit();
    {
        const shard = store.batchShard(35);
        shard.mutex.lockUncancelable(store.io_impl.io());
        defer shard.mutex.unlock(store.io_impl.io());
        const group_store = (try store.groupStoreLocked(shard, 35, false)) orelse return error.MissingGroupStore;
        var identity_key_buf: [176]u8 = undefined;
        for (1..4) |index| {
            const identity_key = try RaftApplyStore.entryIdentityKeyForGroup(&identity_key_buf, 35, index);
            try std.testing.expectError(error.NotFound, group_store.store.get(std.testing.allocator, identity_key));
        }
        var coverage_key_buf: [176]u8 = undefined;
        const coverage_key = try RaftApplyStore.entryIdentityCoverageKeyForGroup(&coverage_key_buf, 35);
        const coverage = try group_store.store.get(std.testing.allocator, coverage_key);
        defer std.testing.allocator.free(coverage);
        try std.testing.expectEqual(@as(u64, 4), std.mem.readInt(u64, coverage[0..8], .little));
    }

    const missing_below_coverage = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 99,
        .index = 1,
        .entry_type = .conf_change,
        .data = @constCast("unverifiable-after-prune"),
    }});
    defer std.testing.allocator.free(missing_below_coverage);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 35,
        .commit_index = 1,
        .entries_bytes = missing_below_coverage,
    });

    const covered = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 5,
        .index = 4,
        .entry_type = .conf_change,
        .data = @constCast("covered-after-prune"),
    }});
    defer std.testing.allocator.free(covered);
    try store.snapshotBuilder().applyBatch(.{ .group_id = 35, .commit_index = 4, .entries_bytes = covered });
    const suffix = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 5,
        .index = 5,
        .entry_type = .normal,
        .data = @constCast("put:after-prune=1"),
    }});
    defer std.testing.allocator.free(suffix);
    try store.snapshotBuilder().applyBatch(.{ .group_id = 35, .commit_index = 5, .entries_bytes = suffix });

    const covered_mismatch = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 5,
        .index = 4,
        .entry_type = .conf_change,
        .data = @constCast("wrong-covered-after-prune"),
    }});
    defer std.testing.allocator.free(covered_mismatch);
    try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 35,
        .commit_index = 4,
        .entries_bytes = covered_mismatch,
    }));

    {
        const shard = store.batchShard(35);
        shard.mutex.lockUncancelable(store.io_impl.io());
        defer shard.mutex.unlock(store.io_impl.io());
        const group_store = (try store.groupStoreLocked(shard, 35, false)) orelse return error.MissingGroupStore;
        var identity_key_buf: [176]u8 = undefined;
        const identity_key = try RaftApplyStore.entryIdentityKeyForGroup(&identity_key_buf, 35, 4);
        try group_store.store.delete(identity_key);
    }
    try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 35,
        .commit_index = 4,
        .entries_bytes = covered,
    }));
}

test "data raft apply store accepts equivalent restart replay with different batch boundaries under allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const encoded = try raft_state_machine.encodeCommittedEntries(alloc, &.{
                .{ .term = 6, .index = 1, .entry_type = .normal, .data = @constCast("put:a=one") },
                .{ .term = 6, .index = 2, .entry_type = .conf_change, .data = @constCast("admin") },
                .{ .term = 6, .index = 3, .entry_type = .normal, .data = @constCast("put:b=two") },
            });
            defer alloc.free(encoded);
            const metadata = try RaftApplyStore.describeEntries(alloc, encoded, 0);
            defer {
                for (metadata.normal_entries) |entry| alloc.free(entry.data);
                alloc.free(metadata.normal_entries);
                alloc.free(metadata.entry_identities);
                for (metadata.operations) |operation| switch (operation) {
                    .put => |put| {
                        alloc.free(put.key);
                        alloc.free(put.value);
                    },
                    .delete => |key| alloc.free(key),
                    .set_range => |range| {
                        alloc.free(range.start);
                        alloc.free(range.end);
                    },
                    .prepare_split, .start_split, .finalize_split, .rollback_split => |transition| alloc.free(transition.split_key),
                    .acknowledge_split, .set_raft_batch_protocol, .flush_split_delta => {},
                };
                alloc.free(metadata.operations);
            }
            try std.testing.expectEqual(@as(usize, 3), metadata.entry_identities.len);
            try std.testing.expectEqual(@as(usize, 2), metadata.normal_entries.len);
            try std.testing.expectEqual(@as(usize, 4), metadata.operations.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "data raft apply store admits one writable owner per root" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-single-writer", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var owner = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer owner.deinit();
    try std.testing.expectError(
        error.LsmRootWriterAlreadyOpen,
        RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root }),
    );
}

test "data raft apply store separates normal and admin entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-mixed", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const encoded = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 8, .index = 40, .entry_type = .normal, .data = @constCast("put:x=1") },
        .{ .term = 8, .index = 41, .entry_type = .conf_change, .data = @constCast("admin-1") },
        .{ .term = 8, .index = 42, .entry_type = .conf_change_v2, .data = @constCast("admin-2") },
        .{ .term = 8, .index = 43, .entry_type = .normal, .data = @constCast("put:y=2") },
    });
    defer std.testing.allocator.free(encoded);

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 44,
        .commit_index = 43,
        .entries_bytes = encoded,
    });

    const batch = (try store.latestBatch(44)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(usize, 4), batch.entry_count);
    try std.testing.expectEqual(@as(usize, 2), batch.normal_entry_count);
    try std.testing.expectEqual(@as(usize, 2), batch.admin_entry_count);
    const normal_entries = try store.appliedNormalEntries(std.testing.allocator, 44);
    defer {
        for (normal_entries) |entry| std.testing.allocator.free(entry.data);
        std.testing.allocator.free(normal_entries);
    }
    try std.testing.expectEqual(@as(usize, 2), normal_entries.len);
    try std.testing.expectEqual(@as(u64, 40), normal_entries[0].index);
    try std.testing.expectEqual(@as(u64, 43), normal_entries[1].index);
    try std.testing.expectEqualStrings("put:y=2", normal_entries[1].data);

    const group_state = try store.groupState(std.testing.allocator, 44);
    defer {
        for (group_state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(group_state);
    }
    try std.testing.expectEqual(@as(usize, 2), group_state.len);
    try std.testing.expectEqualStrings("x", group_state[0].key);
    try std.testing.expectEqualStrings("1", group_state[0].value);
    try std.testing.expectEqualStrings("y", group_state[1].key);
    try std.testing.expectEqualStrings("2", group_state[1].value);

    const snapshot = try store.snapshotBuilder().buildSnapshot(std.testing.allocator, 44);
    defer std.testing.allocator.free(snapshot);
    try std.testing.expect(snapshot.len > 4);
}

test "data raft apply store applies delete operations into group state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-delete", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const first = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("put:k=1") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:z=9") },
    });
    defer std.testing.allocator.free(first);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 2,
        .entries_bytes = first,
    });
    const first_snapshot = (try store.latestBatch(77)) orelse return error.MissingDataBatch;

    const second = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("del:k") },
    });
    defer std.testing.allocator.free(second);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 3,
        .entries_bytes = second,
    });
    try std.testing.expectEqual(@as(u64, 2), first_snapshot.commit_index);
    const second_snapshot = (try store.latestBatch(77)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 3), second_snapshot.commit_index);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 3,
        .entries_bytes = second,
    });
    const conflicting = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = @constCast("put:z=10") },
    });
    defer std.testing.allocator.free(conflicting);
    try std.testing.expectError(error.ConflictingDataApplyBatch, store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 3,
        .entries_bytes = conflicting,
    }));
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 77,
        .commit_index = 2,
        .entries_bytes = first,
    });

    const group_state = try store.groupState(std.testing.allocator, 77);
    defer {
        for (group_state) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(group_state);
    }
    try std.testing.expectEqual(@as(usize, 1), group_state.len);
    try std.testing.expectEqualStrings("z", group_state[0].key);
    try std.testing.expectEqualStrings("9", group_state[0].value);
}

test "data raft apply store prepared snapshot retains its MVCC view across later writes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-prepared-snapshot", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    const first = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = @constCast("put:k=old"),
    }});
    defer std.testing.allocator.free(first);
    try store.snapshotBuilder().applyBatch(.{ .group_id = 78, .commit_index = 1, .entries_bytes = first });
    const source = (try store.snapshotBuilder().prepareSnapshot(78, 1)) orelse return error.MissingDataSnapshotSource;

    const second = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:k=new") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:z=later") },
    });
    defer std.testing.allocator.free(second);
    try store.snapshotBuilder().applyBatch(.{ .group_id = 78, .commit_index = 3, .entries_bytes = second });

    const Worker = struct {
        source: raft_engine.runtime.storage_iface.SnapshotSource,
        snapshot: ?raft_engine.runtime.storage_iface.SnapshotMaterialization = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            defer self.source.deinit();
            self.snapshot = self.source.materialize(std.heap.page_allocator) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var worker = Worker{ .source = source };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    thread.join();
    if (worker.failure) |err| return err;
    var materialized = worker.snapshot orelse return error.MissingDataSnapshot;
    defer materialized.deinit(std.heap.page_allocator);
    const artifact_bytes = switch (materialized) {
        .bytes => null,
        .artifact => |artifact| try artifact.readAll(std.heap.page_allocator),
    };
    defer if (artifact_bytes) |bytes| std.heap.page_allocator.free(bytes);
    const encoded = switch (materialized) {
        .bytes => |bytes| bytes,
        .artifact => artifact_bytes.?,
    };
    var snapshot = try shard_state_store.decodeGroupStateSnapshotAlloc(std.testing.allocator, encoded);
    defer snapshot.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), snapshot.entries.len);
    try std.testing.expectEqualStrings("k", snapshot.entries[0].key);
    try std.testing.expectEqualStrings("old", snapshot.entries[0].value);
}

test "data raft apply store orders independent groups through separate shards" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-concurrent-groups", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.heap.page_allocator, .{ .root_dir = root });
    defer store.deinit();
    try std.testing.expect(store.batchShard(1) != store.batchShard(2));
    try std.testing.expect(store.batchShard(1) == store.batchShard(1 + batch_shard_count));

    const Worker = struct {
        store: *RaftApplyStore,
        group_id: u64,
        result: ?anyerror = null,

        fn run(self: *@This()) void {
            const entry = raft_state_machine.encodeCommittedEntries(std.heap.page_allocator, &.{.{
                .term = 1,
                .index = 1,
                .entry_type = .normal,
                .data = @constCast("put:k=1"),
            }}) catch |err| {
                self.result = err;
                return;
            };
            defer std.heap.page_allocator.free(entry);
            self.store.snapshotBuilder().applyBatch(.{
                .group_id = self.group_id,
                .commit_index = 1,
                .entries_bytes = entry,
            }) catch |err| {
                self.result = err;
            };
        }
    };

    var first = Worker{ .store = &store, .group_id = 1 };
    var second = Worker{ .store = &store, .group_id = 2 };
    const first_thread = try std.Thread.spawn(.{}, Worker.run, .{&first});
    const second_thread = try std.Thread.spawn(.{}, Worker.run, .{&second});
    first_thread.join();
    second_thread.join();
    if (first.result) |err| return err;
    if (second.result) |err| return err;
    try std.testing.expectEqual(@as(u64, 1), (try store.latestBatch(1)).?.commit_index);
    try std.testing.expectEqual(@as(u64, 1), (try store.latestBatch(2)).?.commit_index);
}

test "data raft apply store persists and enforces group range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-range", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const set_range = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:b:d") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:c=3") },
        });
        defer std.testing.allocator.free(set_range);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 88,
            .commit_index = 2,
            .entries_bytes = set_range,
        });

        const byte_range = try store.currentRange(std.testing.allocator, 88);
        defer {
            if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
            if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
        }
        try std.testing.expectEqualStrings("b", byte_range.start);
        try std.testing.expectEqualStrings("d", byte_range.end);

        const out_of_range = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:a=1") },
        });
        defer std.testing.allocator.free(out_of_range);
        try std.testing.expectError(error.KeyOutOfRange, store.snapshotBuilder().applyBatch(.{
            .group_id = 88,
            .commit_index = 3,
            .entries_bytes = out_of_range,
        }));
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const byte_range = try store.currentRange(std.testing.allocator, 88);
        defer {
            if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
            if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
        }
        try std.testing.expectEqualStrings("b", byte_range.start);
        try std.testing.expectEqualStrings("d", byte_range.end);

        const group_state = try store.groupState(std.testing.allocator, 88);
        defer {
            for (group_state) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
            std.testing.allocator.free(group_state);
        }
        try std.testing.expectEqual(@as(usize, 1), group_state.len);
        try std.testing.expectEqualStrings("c", group_state[0].key);
        try std.testing.expectEqualStrings("3", group_state[0].value);
    }
}

test "data raft apply store parses empty-start colon range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-empty-start-range", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const set_range = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range::doc:024") },
    });
    defer std.testing.allocator.free(set_range);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 89,
        .commit_index = 1,
        .entries_bytes = set_range,
    });

    const byte_range = try store.currentRange(std.testing.allocator, 89);
    defer {
        if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
        if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
    }
    try std.testing.expectEqualStrings("", byte_range.start);
    try std.testing.expectEqualStrings("doc:024", byte_range.end);
}

test "data raft apply store captures split handoff and replays destination deltas" {
    const setup_commit_index: u64 = 6;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    var src = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
    defer src.deinit();
    var dst = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dst.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b=left-0") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t=right-0") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:90:1:90:doc:m") },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("split_start:90:1:90:doc:m") },
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = @constCast("put:doc:u=right-1") },
    });
    defer std.testing.allocator.free(setup);
    try src.snapshotBuilder().applyBatch(.{
        .group_id = 91,
        .commit_index = setup_commit_index,
        .entries_bytes = setup,
    });

    const handoff = try src.captureSplitHandoff(std.testing.allocator, 91);
    defer shard_state_store.freeHandoff(std.testing.allocator, handoff);
    try std.testing.expectEqual(setup_commit_index, handoff.base_delta_sequence);
    try dst.applySplitHandoff(std.testing.allocator, 92, handoff);

    const catchup_batch = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = @constCast("put:doc:c=left-1") },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = @constCast("put:doc:x=right-2") },
        .{ .term = 1, .index = 9, .entry_type = .normal, .data = @constCast("del:doc:t") },
    });
    defer std.testing.allocator.free(catchup_batch);
    try src.snapshotBuilder().applyBatch(.{
        .group_id = 91,
        .commit_index = 9,
        .entries_bytes = catchup_batch,
    });

    const deltas = try src.listSplitDeltasAfter(std.testing.allocator, 91, handoff.base_delta_sequence);
    defer shard_mod.freeDeltas(std.testing.allocator, deltas);
    // Split replication preserves the Raft-entry watermark, including source-
    // side writes that the destination range filter discards during apply.
    try std.testing.expectEqual(@as(usize, 3), deltas.len);
    try std.testing.expectEqual(@as(u64, 7), deltas[0].sequence);
    try std.testing.expectEqual(@as(u64, 8), deltas[1].sequence);
    try std.testing.expectEqual(@as(u64, 9), deltas[2].sequence);
    try dst.applySplitDeltas(std.testing.allocator, 92, deltas);

    const byte_range = try dst.currentRange(std.testing.allocator, 92);
    defer {
        if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
        if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
    }
    try std.testing.expectEqualStrings("doc:m", byte_range.start);
    try std.testing.expectEqualStrings("doc:z", byte_range.end);

    const state = try dst.groupState(std.testing.allocator, 92);
    defer shard_state_store.freeGroupStateEntries(std.testing.allocator, state);
    try std.testing.expectEqual(@as(usize, 2), state.len);
    try std.testing.expectEqualStrings("doc:u", state[0].key);
    try std.testing.expectEqualStrings("right-1", state[0].value);
    try std.testing.expectEqualStrings("doc:x", state[1].key);
    try std.testing.expectEqualStrings("right-2", state[1].value);
}

test "data raft apply store parses colon-delimited range keys correctly" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-range-colons", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const encoded = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:m={\"v\":1}") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_prepare:192:1:192:doc:n") },
    });
    defer std.testing.allocator.free(encoded);

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 191,
        .commit_index = 3,
        .entries_bytes = encoded,
    });

    const byte_range = try store.currentRange(std.testing.allocator, 191);
    defer {
        if (byte_range.start.len > 0) std.testing.allocator.free(@constCast(byte_range.start));
        if (byte_range.end.len > 0) std.testing.allocator.free(@constCast(byte_range.end));
    }
    try std.testing.expectEqualStrings("doc:a", byte_range.start);
    try std.testing.expectEqualStrings("doc:z", byte_range.end);

    const split_state = (try store.currentSplitState(std.testing.allocator, 191)) orelse return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, split_state);
    try std.testing.expectEqualStrings("doc:n", split_state.split_key);
}

test "data raft apply store persists split destination acknowledgements" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-ack", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("split_prepare:201:1:202:doc:m") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_start:201:1:202:doc:m") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("put:doc:x={}") },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("put:doc:y={}") },
    });
    defer std.testing.allocator.free(setup);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 201,
        .commit_index = 5,
        .entries_bytes = setup,
    });

    const batch = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .source_ack,
            .transition_id = 201,
            .attempt_epoch = 1,
            .source_group_id = 201,
            .destination_group_id = 202,
            .delta_sequence = 1,
        },
    });
    defer std.testing.allocator.free(batch);
    const stale_batch = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .source_ack,
            .transition_id = 201,
            .attempt_epoch = 1,
            .source_group_id = 201,
            .destination_group_id = 202,
            .delta_sequence = 0,
        },
    });
    defer std.testing.allocator.free(stale_batch);
    const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 6, .entry_type = .normal, .data = batch },
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = stale_batch },
    });
    defer std.testing.allocator.free(entries);

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 201,
        .commit_index = 7,
        .entries_bytes = entries,
    });
    const acknowledgement = (try store.currentSplitAcknowledgement(std.testing.allocator, 201)) orelse
        return error.MissingSplitAcknowledgement;
    try std.testing.expectEqual(@as(u64, 201), acknowledgement.transition_id);
    try std.testing.expectEqual(@as(u64, 202), acknowledgement.destination_group_id);
    try std.testing.expectEqual(@as(u64, 1), acknowledgement.delta_sequence);
}

test "data raft apply store skips persisted split commands in overlapping replay" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-overlap", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .prepare,
            .transition_id = 211,
            .attempt_epoch = 1,
            .destination_group_id = 212,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(prepare);
    const start = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .start,
            .transition_id = 211,
            .attempt_epoch = 1,
            .destination_group_id = 212,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(start);

    const initial = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = prepare },
    });
    defer std.testing.allocator.free(initial);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 211,
        .commit_index = 2,
        .entries_bytes = initial,
    });

    const overlapping = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = prepare },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = start },
    });
    defer std.testing.allocator.free(overlapping);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 211,
        .commit_index = 3,
        .entries_bytes = overlapping,
    });

    const split_state = (try store.currentSplitState(std.testing.allocator, 211)) orelse
        return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, split_state);
    try std.testing.expectEqual(shard_mod.SplitPhase.splitting, split_state.phase);
    try std.testing.expectEqual(@as(u64, 212), split_state.new_shard_id);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);
}

test "data raft apply store recovers committed split start after projection generation gap" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-generation-gap", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const initial = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={}") },
    });
    defer std.testing.allocator.free(initial);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 231,
        .commit_index = 2,
        .entries_bytes = initial,
    });

    const start = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .start,
            .transition_id = 231,
            .attempt_epoch = 1,
            .destination_group_id = 232,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(start);
    const suffix = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 4,
        .entry_type = .normal,
        .data = start,
    }});
    defer std.testing.allocator.free(suffix);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 231,
        .commit_index = 4,
        .entries_bytes = suffix,
    });

    const state = (try store.currentSplitState(std.testing.allocator, 231)) orelse
        return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, state);
    try std.testing.expectEqual(shard_mod.SplitPhase.splitting, state.phase);
    try std.testing.expectEqual(@as(u64, 231), state.transition_id);
    try std.testing.expectEqual(@as(u64, 232), state.new_shard_id);
    try std.testing.expectEqualStrings("doc:m", state.split_key);
    try std.testing.expectEqualStrings("doc:z", state.original_range_end);
    const byte_range = try store.currentRange(std.testing.allocator, 231);
    defer range_state.freeRange(std.testing.allocator, byte_range);
    try std.testing.expectEqualStrings("doc:a", byte_range.start);
    try std.testing.expectEqualStrings("doc:m", byte_range.end);
    const batch = (try store.latestBatch(231)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 4), batch.last_entry_index);
}

test "data raft split cursors are stable across apply batching and acknowledge same-batch writes" {
    var combined_tmp = std.testing.tmpDir(.{});
    defer combined_tmp.cleanup();
    var partitioned_tmp = std.testing.tmpDir(.{});
    defer partitioned_tmp.cleanup();
    const combined_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/combined", .{combined_tmp.sub_path});
    defer std.testing.allocator.free(combined_root);
    const partitioned_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/partitioned", .{partitioned_tmp.sub_path});
    defer std.testing.allocator.free(partitioned_root);

    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .prepare,
            .transition_id = 401,
            .attempt_epoch = 1,
            .destination_group_id = 402,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(prepare);
    const start = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .start,
            .transition_id = 401,
            .attempt_epoch = 1,
            .destination_group_id = 402,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(start);
    const acknowledgement = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .source_ack,
            .transition_id = 401,
            .attempt_epoch = 1,
            .source_group_id = 401,
            .destination_group_id = 402,
            .delta_sequence = 4,
        },
    });
    defer std.testing.allocator.free(acknowledgement);

    const entries = [_]raft_engine.core.Entry{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = prepare },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = start },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("put:doc:t=right") },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = acknowledgement },
    };

    var combined = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = combined_root });
    defer combined.deinit();
    const combined_batch = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &entries);
    defer std.testing.allocator.free(combined_batch);
    try combined.snapshotBuilder().applyBatch(.{ .group_id = 401, .commit_index = 5, .entries_bytes = combined_batch });

    var partitioned = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = partitioned_root });
    defer partitioned.deinit();
    const prefix = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, entries[0..3]);
    defer std.testing.allocator.free(prefix);
    try partitioned.snapshotBuilder().applyBatch(.{ .group_id = 401, .commit_index = 3, .entries_bytes = prefix });
    const write_batch = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, entries[3..4]);
    defer std.testing.allocator.free(write_batch);
    try partitioned.snapshotBuilder().applyBatch(.{ .group_id = 401, .commit_index = 4, .entries_bytes = write_batch });
    const acknowledgement_batch = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, entries[4..5]);
    defer std.testing.allocator.free(acknowledgement_batch);
    try partitioned.snapshotBuilder().applyBatch(.{ .group_id = 401, .commit_index = 5, .entries_bytes = acknowledgement_batch });

    for ([_]*RaftApplyStore{ &combined, &partitioned }) |store| {
        try std.testing.expectEqual(@as(u64, 4), try store.currentSplitDeltaSequence(std.testing.allocator, 401));
        const ack = (try store.currentSplitAcknowledgement(std.testing.allocator, 401)) orelse
            return error.MissingSplitAcknowledgement;
        try std.testing.expectEqual(@as(u64, 4), ack.delta_sequence);
        const deltas = try store.listSplitDeltasAfter(std.testing.allocator, 401, 0);
        defer shard_mod.freeDeltas(std.testing.allocator, deltas);
        try std.testing.expectEqual(@as(usize, 1), deltas.len);
        try std.testing.expectEqual(@as(u64, 4), deltas[0].sequence);
    }
}

test "data raft apply store recovers exact split replay after injected projection corruption" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-split-watermark-lag", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .prepare,
            .transition_id = 221,
            .attempt_epoch = 1,
            .destination_group_id = 222,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(prepare);
    const range_entry = raft_engine.core.Entry{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = @constCast("range:doc:a:doc:z"),
    };
    const prepare_entry = raft_engine.core.Entry{
        .term = 1,
        .index = 2,
        .entry_type = .normal,
        .data = prepare,
    };

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const applied = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{ range_entry, prepare_entry });
        defer std.testing.allocator.free(applied);
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 221,
            .commit_index = 2,
            .entries_bytes = applied,
        });

        // Fault-inject a marker regression that the atomic putBatch path cannot
        // produce. Split operations remain exactly idempotent as a final line
        // of defense if storage is externally corrupted or restored unevenly.
        const lagging = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{range_entry});
        defer std.testing.allocator.free(lagging);
        var key_buf: [128]u8 = undefined;
        const key = try RaftApplyStore.keyForGroup(&key_buf, 221);
        const value = try std.testing.allocator.alloc(u8, @sizeOf(u64) + lagging.len);
        defer std.testing.allocator.free(value);
        std.mem.writeInt(u64, value[0..8], 1, .little);
        @memcpy(value[8..], lagging);
        const io = store.io_impl.io();
        const shard = store.batchShard(221);
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const group_store = try store.writableGroupStoreLocked(shard, 221);
        try group_store.store.put(key, value);
    }

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    const replay = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{prepare_entry});
    defer std.testing.allocator.free(replay);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 2,
        .entries_bytes = replay,
    });

    const batch = (try store.latestBatch(221)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 2), batch.commit_index);
    try std.testing.expectEqual(@as(u64, 2), batch.last_entry_index);
    const split_state = (try store.currentSplitState(std.testing.allocator, 221)) orelse
        return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, split_state);
    try std.testing.expectEqual(shard_mod.SplitPhase.prepare, split_state.phase);
    try std.testing.expectEqualStrings("doc:m", split_state.split_key);
}

test "data raft apply store rejects mismatched terminal split identity" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-terminal-identity", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{ .kind = .prepare, .transition_id = 221, .attempt_epoch = 1, .destination_group_id = 222, .split_key = "doc:m" },
    });
    defer std.testing.allocator.free(prepare);
    const start = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{ .kind = .start, .transition_id = 221, .attempt_epoch = 1, .destination_group_id = 222, .split_key = "doc:m" },
    });
    defer std.testing.allocator.free(start);
    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = prepare },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = start },
    });
    defer std.testing.allocator.free(setup);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 3,
        .entries_bytes = setup,
    });

    const wrong_rollback = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{ .kind = .rollback, .transition_id = 221, .attempt_epoch = 1, .destination_group_id = 223, .split_key = "doc:m" },
    });
    defer std.testing.allocator.free(wrong_rollback);
    const wrong_rollback_entry = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = wrong_rollback },
    });
    defer std.testing.allocator.free(wrong_rollback_entry);
    try std.testing.expectError(error.ConflictingSplitTransition, store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 4,
        .entries_bytes = wrong_rollback_entry,
    }));

    const wrong_ack = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .source_ack,
            .transition_id = 221,
            .attempt_epoch = 1,
            .source_group_id = 221,
            .destination_group_id = 223,
            .delta_sequence = 0,
        },
    });
    defer std.testing.allocator.free(wrong_ack);
    const wrong_ack_entry = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = wrong_ack },
    });
    defer std.testing.allocator.free(wrong_ack_entry);
    try std.testing.expectError(error.ConflictingSplitTransition, store.snapshotBuilder().applyBatch(.{
        .group_id = 221,
        .commit_index = 4,
        .entries_bytes = wrong_ack_entry,
    }));

    const state = (try store.currentSplitState(std.testing.allocator, 221)) orelse return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, state);
    try std.testing.expectEqual(shard_mod.SplitPhase.splitting, state.phase);
    try std.testing.expectEqual(@as(u64, 222), state.new_shard_id);
}

test "data raft apply store seeds pre-raft snapshots once at reserved index zero" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-seed", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        try std.testing.expect(try store.seedGroupSnapshotIfAbsent(
            std.testing.allocator,
            311,
            1,
            .{ .start = "doc:a", .end = "doc:z" },
            &.{.{ .key = "doc:a", .value = "{\"v\":1}" }},
        ));
        const baseline = (try store.latestBatch(311)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 0), baseline.commit_index);
        try std.testing.expectEqual(@as(u64, 0), baseline.last_entry_index);

        try std.testing.expect(!try store.seedGroupSnapshotIfAbsent(
            std.testing.allocator,
            311,
            1,
            .{ .start = "doc:m", .end = "" },
            &.{.{ .key = "doc:replacement", .value = "{}" }},
        ));
    }

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    const restored_baseline = (try store.latestBatch(311)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 0), restored_baseline.commit_index);
    try std.testing.expectEqual(@as(u64, 0), restored_baseline.last_entry_index);

    const real = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = @constCast("put:doc:b={\"v\":2}"),
    }});
    defer std.testing.allocator.free(real);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 311,
        .commit_index = 1,
        .entries_bytes = real,
    });

    const state = try store.groupState(std.testing.allocator, 311);
    defer shard_state_store.freeGroupStateEntries(std.testing.allocator, state);
    try std.testing.expectEqual(@as(usize, 2), state.len);
    try std.testing.expectEqualStrings("doc:a", state[0].key);
    try std.testing.expectEqualStrings("doc:b", state[1].key);
}

test "data raft apply store reconciles an inherited document root at an exact watermark" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-reconcile", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const bootstrap = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = @constCast(""),
    }});
    defer std.testing.allocator.free(bootstrap);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 312,
        .commit_index = 1,
        .entries_bytes = bootstrap,
    });
    const bootstrap_watermark = (try store.latestBatch(312)) orelse return error.MissingDataBatch;
    try std.testing.expectError(error.DurableRootIncarnationUnavailable, store.reconcileGroupSnapshotAtRootIncarnation(
        std.testing.allocator,
        312,
        bootstrap_watermark,
        0,
        .{ .start = "doc:a", .end = "doc:z" },
        &.{},
    ));
    try std.testing.expect(try store.reconcileGroupSnapshotAtRootIncarnation(
        std.testing.allocator,
        312,
        bootstrap_watermark,
        (@as(u128, 1) << 96) | 1,
        .{ .start = "doc:a", .end = "doc:z" },
        &.{
            .{ .key = "doc:a", .value = "{\"v\":1}" },
            .{ .key = "doc:t", .value = "{\"v\":2}" },
        },
    ));

    const reconciled = try store.groupState(std.testing.allocator, 312);
    defer shard_state_store.freeGroupStateEntries(std.testing.allocator, reconciled);
    try std.testing.expectEqual(@as(usize, 2), reconciled.len);
    try std.testing.expectEqualStrings("doc:t", reconciled[1].key);
    try std.testing.expectEqual(bootstrap_watermark, (try store.latestBatch(312)).?);

    const advanced = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 2,
        .entry_type = .normal,
        .data = @constCast("put:doc:u={\"v\":3}"),
    }});
    defer std.testing.allocator.free(advanced);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 312,
        .commit_index = 2,
        .entries_bytes = advanced,
    });
    try std.testing.expect(!try store.reconcileGroupSnapshotAtRootIncarnation(
        std.testing.allocator,
        312,
        bootstrap_watermark,
        (@as(u128, 1) << 96) | 1,
        .{ .start = "doc:a", .end = "doc:z" },
        &.{.{ .key = "doc:stale", .value = "{}" }},
    ));
    const after_stale_attempt = try store.groupState(std.testing.allocator, 312);
    defer shard_state_store.freeGroupStateEntries(std.testing.allocator, after_stale_attempt);
    try std.testing.expectEqual(@as(usize, 3), after_stale_attempt.len);
    try std.testing.expectEqualStrings("doc:u", after_stale_attempt[2].key);
}

test "data raft apply store reconciles inherited documents while preserving active split control" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-active-split-reconcile", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range::") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("split_prepare:314:1:315:doc:024") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_start:314:1:315:doc:024") },
    });
    defer std.testing.allocator.free(setup);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 314,
        .commit_index = 3,
        .entries_bytes = setup,
    });
    const watermark = (try store.latestBatch(314)) orelse return error.MissingDataBatch;

    const handoff = (try store.reconcileAndCaptureSplitHandoffAtRootIncarnation(
        std.testing.allocator,
        314,
        watermark,
        (@as(u128, 17) << 96) | 17,
        .{ .start = "", .end = "" },
        &.{
            .{ .key = "doc:000", .value = "left" },
            .{ .key = "doc:024", .value = "boundary" },
            .{ .key = "doc:047", .value = "right" },
        },
    )) orelse return error.TestExpectedEqual;
    defer shard_state_store.freeHandoff(std.testing.allocator, handoff);

    const state = (try store.currentSplitState(std.testing.allocator, 314)) orelse
        return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, state);
    try std.testing.expectEqual(shard_mod.SplitPhase.splitting, state.phase);
    try std.testing.expectEqualStrings("doc:024", state.split_key);
    try std.testing.expectEqual(watermark, (try store.latestBatch(314)).?);
    const source_range = try store.currentRange(std.testing.allocator, 314);
    defer range_state.freeRange(std.testing.allocator, source_range);
    try std.testing.expectEqualStrings("", source_range.start);
    try std.testing.expectEqualStrings("doc:024", source_range.end);

    try std.testing.expectEqual(@as(usize, 2), handoff.entries.len);
    try std.testing.expectEqualStrings("doc:024", handoff.entries[0].key);
    try std.testing.expectEqualStrings("doc:047", handoff.entries[1].key);

    try std.testing.expect((try store.captureVerifiedSplitHandoffAtRootIncarnation(
        std.testing.allocator,
        314,
        watermark,
        (@as(u128, 18) << 96) | 18,
    )) == null);
    const verified = (try store.captureVerifiedSplitHandoffAtRootIncarnation(
        std.testing.allocator,
        314,
        watermark,
        (@as(u128, 17) << 96) | 17,
    )) orelse return error.TestExpectedEqual;
    defer shard_state_store.freeHandoff(std.testing.allocator, verified);
    try std.testing.expectEqual(@as(usize, 2), verified.entries.len);
}

test "data raft apply store rejects a regressing source generation during active split" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-active-split-regression", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range::") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:000=left") },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:024=boundary") },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:316:1:317:doc:024") },
        .{ .term = 1, .index = 5, .entry_type = .normal, .data = @constCast("split_start:316:1:317:doc:024") },
    });
    defer std.testing.allocator.free(setup);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 316,
        .commit_index = 5,
        .entries_bytes = setup,
    });
    const watermark = (try store.latestBatch(316)) orelse return error.MissingDataBatch;

    try std.testing.expectError(error.SplitSourceProjectionNotReady, store.reconcileGroupSnapshotAtRootIncarnation(
        std.testing.allocator,
        316,
        watermark,
        1,
        .{ .start = "", .end = "" },
        &.{.{ .key = "doc:000", .value = "newer-left" }},
    ));

    const handoff = try store.captureSplitHandoff(std.testing.allocator, 316);
    defer shard_state_store.freeHandoff(std.testing.allocator, handoff);
    try std.testing.expectEqual(@as(usize, 1), handoff.entries.len);
    try std.testing.expectEqualStrings("doc:024", handoff.entries[0].key);
    try std.testing.expectEqualStrings("boundary", handoff.entries[0].value);
}

test "data raft apply store transition reads fail fast during generation staging" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-transition-ready", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const bootstrap = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = @constCast(""),
    }});
    defer std.testing.allocator.free(bootstrap);
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 313,
        .commit_index = 1,
        .entries_bytes = bootstrap,
    });
    const watermark = (try store.latestBatch(313)) orelse return error.MissingDataBatch;

    try store.beginGenerationPreparation(313);
    var preparation_active = true;
    defer if (preparation_active) store.cancelGenerationPreparation(313);
    try std.testing.expectError(error.SplitSourceProjectionNotReady, store.latestBatchForTransition(313));
    try std.testing.expectError(error.SplitSourceProjectionNotReady, store.observeSplitControl(std.testing.allocator, 313));
    try std.testing.expectError(error.SplitSourceProjectionNotReady, store.reconcileGroupSnapshotAtRootIncarnation(
        std.testing.allocator,
        313,
        watermark,
        1,
        .{ .start = "doc:a", .end = "doc:z" },
        &.{},
    ));
    store.cancelGenerationPreparation(313);
    preparation_active = false;

    var observation = try store.observeSplitControl(std.testing.allocator, 313);
    defer observation.deinit(std.testing.allocator);
    try std.testing.expect(observation.state == null);
    try std.testing.expectEqual(@as(u64, 0), observation.delta_sequence);
}

test "data raft apply store installs snapshot watermark atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshot-watermark-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const target_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshot-watermark-target", .{tmp.sub_path});
    defer std.testing.allocator.free(target_root);

    var source = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer source.deinit();
    const entry_count = 513;
    const entries = try std.testing.allocator.alloc(AppliedDataKV, entry_count);
    defer std.testing.allocator.free(entries);
    var initialized: usize = 0;
    defer for (entries[0..initialized]) |entry| std.testing.allocator.free(entry.key);
    for (entries, 0..) |*entry, i| {
        entry.* = .{
            .key = try std.fmt.allocPrint(std.testing.allocator, "doc:{d:0>4}", .{i}),
            .value = "{}",
        };
        initialized += 1;
    }
    try std.testing.expect(try source.seedGroupSnapshotIfAbsent(
        std.testing.allocator,
        301,
        1,
        .{ .start = "doc:0000", .end = "doc:9999" },
        entries,
    ));
    const snapshot = try source.snapshotBuilder().buildSnapshot(std.testing.allocator, 301);
    defer std.testing.allocator.free(snapshot);

    {
        var target = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = target_root });
        defer target.deinit();
        try std.testing.expect(try target.seedGroupSnapshotIfAbsent(
            std.testing.allocator,
            302,
            1,
            .{ .start = "other:", .end = "other;" },
            &.{.{ .key = "other:a", .value = "preserved" }},
        ));
        try target.installSnapshot(std.testing.allocator, 301, 50, snapshot);
        const batch = (try target.latestBatch(301)) orelse return error.MissingAppliedBatch;
        try std.testing.expectEqual(@as(u64, 50), batch.commit_index);
        try std.testing.expectEqual(@as(u64, 50), batch.last_entry_index);
        const installed = try target.groupState(std.testing.allocator, 301);
        defer shard_state_store.freeGroupStateEntries(std.testing.allocator, installed);
        try std.testing.expectEqual(@as(usize, entry_count), installed.len);
        const preserved = try target.groupState(std.testing.allocator, 302);
        defer shard_state_store.freeGroupStateEntries(std.testing.allocator, preserved);
        try std.testing.expectEqual(@as(usize, 1), preserved.len);
        try std.testing.expectEqualStrings("preserved", preserved[0].value);
    }

    {
        var target = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = target_root });
        defer target.deinit();
        const restored = (try target.latestBatch(301)) orelse return error.MissingAppliedBatch;
        try std.testing.expectEqual(@as(u64, 50), restored.commit_index);
        try std.testing.expectEqual(@as(u64, 50), restored.last_entry_index);

        const replay_at_snapshot = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 7,
            .index = 50,
            .entry_type = .normal,
            .data = @constCast("put:doc:0513=ignored"),
        }});
        defer std.testing.allocator.free(replay_at_snapshot);
        try target.snapshotBuilder().applyBatch(.{
            .group_id = 301,
            .commit_index = 50,
            .entries_bytes = replay_at_snapshot,
        });
        const unchanged = try target.groupState(std.testing.allocator, 301);
        defer shard_state_store.freeGroupStateEntries(std.testing.allocator, unchanged);
        try std.testing.expectEqual(@as(usize, entry_count), unchanged.len);

        const suffix = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
            .term = 7,
            .index = 51,
            .entry_type = .normal,
            .data = @constCast("put:doc:0513=two"),
        }});
        defer std.testing.allocator.free(suffix);
        try target.snapshotBuilder().applyBatch(.{
            .group_id = 301,
            .commit_index = 51,
            .entries_bytes = suffix,
        });
        const advanced = (try target.latestBatch(301)) orelse return error.MissingAppliedBatch;
        try std.testing.expectEqual(@as(u64, 51), advanced.commit_index);
        try std.testing.expectEqual(@as(u64, 51), advanced.last_entry_index);
    }
}

test "data raft apply store refuses stale snapshot projection regression" {
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var target_tmp = std.testing.tmpDir(.{});
    defer target_tmp.cleanup();
    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/stale-snapshot-source", .{source_tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const target_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/stale-snapshot-target", .{target_tmp.sub_path});
    defer std.testing.allocator.free(target_root);

    const initial = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={}") },
    });
    defer std.testing.allocator.free(initial);
    var source = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer source.deinit();
    try source.snapshotBuilder().applyBatch(.{ .group_id = 241, .commit_index = 2, .entries_bytes = initial });
    const stale_snapshot = try source.snapshotBuilder().buildSnapshot(std.testing.allocator, 241);
    defer std.testing.allocator.free(stale_snapshot);

    var target = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = target_root });
    defer target.deinit();
    try target.snapshotBuilder().applyBatch(.{ .group_id = 241, .commit_index = 2, .entries_bytes = initial });
    const prepare = try data_raft_batch.encode(std.testing.allocator, "docs", .{
        .split_transition = .{
            .kind = .prepare,
            .transition_id = 241,
            .attempt_epoch = 1,
            .destination_group_id = 242,
            .split_key = "doc:m",
        },
    });
    defer std.testing.allocator.free(prepare);
    const prepare_entry = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{.{
        .term = 1,
        .index = 3,
        .entry_type = .normal,
        .data = prepare,
    }});
    defer std.testing.allocator.free(prepare_entry);
    try target.snapshotBuilder().applyBatch(.{ .group_id = 241, .commit_index = 3, .entries_bytes = prepare_entry });

    try target.installSnapshot(std.testing.allocator, 241, 2, stale_snapshot);
    const batch = (try target.latestBatch(241)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 3), batch.commit_index);
    try std.testing.expectEqual(@as(u64, 3), batch.last_entry_index);
    const state = (try target.currentSplitState(std.testing.allocator, 241)) orelse
        return error.MissingSplitState;
    defer shard_state_store.freeSplitState(std.testing.allocator, state);
    try std.testing.expectEqual(shard_mod.SplitPhase.prepare, state.phase);
    try std.testing.expectEqualStrings("doc:m", state.split_key);
}

test "data raft snapshot staging blocks only the target group" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshot-concurrency-source", .{tmp.sub_path});
    defer std.testing.allocator.free(source_root);
    const target_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshot-concurrency-target", .{tmp.sub_path});
    defer std.testing.allocator.free(target_root);

    var source = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = source_root });
    defer source.deinit();
    try std.testing.expect(try source.seedGroupSnapshotIfAbsent(
        std.testing.allocator,
        1,
        1,
        .{ .start = "doc:a", .end = "doc:z" },
        &.{.{ .key = "doc:a", .value = "snapshot" }},
    ));
    const snapshot = try source.snapshotBuilder().buildSnapshot(std.testing.allocator, 1);
    defer std.testing.allocator.free(snapshot);

    var target = try RaftApplyStore.init(std.heap.smp_allocator, .{ .root_dir = target_root });
    defer target.deinit();
    try std.testing.expect(try target.seedGroupSnapshotIfAbsent(
        std.heap.smp_allocator,
        1 + batch_shard_count,
        1,
        .{ .start = "other:a", .end = "other:z" },
        &.{.{ .key = "other:a", .value = "available" }},
    ));

    const InstallContext = struct {
        store: *RaftApplyStore,
        snapshot: []const u8,
        failure: ?anyerror = null,

        fn run(ctx: *@This()) void {
            ctx.store.installSnapshot(std.heap.smp_allocator, 1, 10, ctx.snapshot) catch |err| {
                ctx.failure = err;
            };
        }
    };
    const ReadContext = struct {
        store: *RaftApplyStore,
        completed: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,

        fn run(ctx: *@This()) void {
            const batch = ctx.store.latestBatch(1 + batch_shard_count) catch |err| {
                ctx.failure = err;
                ctx.completed.store(true, .release);
                return;
            };
            if (batch == null) ctx.failure = error.MissingDataBatch;
            ctx.completed.store(true, .release);
        }
    };

    test_snapshot_staging_started.store(false, .release);
    test_block_snapshot_staging.store(true, .release);
    defer test_block_snapshot_staging.store(false, .release);
    var install_ctx = InstallContext{ .store = &target, .snapshot = snapshot };
    var install_thread = try std.Thread.spawn(.{}, InstallContext.run, .{&install_ctx});
    var install_joined = false;
    defer if (!install_joined) {
        test_block_snapshot_staging.store(false, .release);
        install_thread.join();
    };

    var attempts: usize = 0;
    while (!test_snapshot_staging_started.load(.acquire) and attempts < 100_000) : (attempts += 1) platform.time.yieldBriefly();
    try std.testing.expect(test_snapshot_staging_started.load(.acquire));

    // Consume and repeatedly grow the colliding shard map while installation
    // has released its mutex. Publication must re-reserve under the lock; a
    // reservation made before staging is not a valid putAssumeCapacity lease.
    for (2..40) |ordinal| {
        try std.testing.expect(try target.seedGroupSnapshotIfAbsent(
            std.heap.smp_allocator,
            @intCast(1 + ordinal * batch_shard_count),
            1,
            .{ .start = "other:a", .end = "other:z" },
            &.{.{ .key = "other:a", .value = "capacity" }},
        ));
    }

    var read_ctx = ReadContext{ .store = &target };
    var read_thread = try std.Thread.spawn(.{}, ReadContext.run, .{&read_ctx});
    var read_joined = false;
    defer if (!read_joined) read_thread.join();
    attempts = 0;
    while (!read_ctx.completed.load(.acquire) and attempts < 100_000) : (attempts += 1) platform.time.yieldBriefly();
    const colliding_group_completed_during_staging = read_ctx.completed.load(.acquire);

    test_block_snapshot_staging.store(false, .release);
    read_thread.join();
    read_joined = true;
    install_thread.join();
    install_joined = true;
    if (read_ctx.failure) |err| return err;
    if (install_ctx.failure) |err| return err;
    try std.testing.expect(colliding_group_completed_during_staging);
}

test "data raft apply store bounds and retires resource-managed group owners" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/bounded-group-owners", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var manager = resource_manager_mod.ResourceManager.init(.{});
    var store = try RaftApplyStore.init(std.testing.allocator, .{
        .root_dir = root,
        .resource_manager = &manager,
        .max_cached_group_stores_per_shard = 1,
    });
    defer store.deinit();

    try std.testing.expect(try store.seedGroupSnapshotIfAbsent(
        std.testing.allocator,
        1,
        1,
        .{ .start = "a", .end = "m" },
        &.{.{ .key = "a", .value = "one" }},
    ));
    try std.testing.expect(manager.sliceStats(.lsm_in_memory_state).used_bytes >= group_store_owner_accounting_bytes);
    try std.testing.expect(try store.seedGroupSnapshotIfAbsent(
        std.testing.allocator,
        1 + batch_shard_count,
        1,
        .{ .start = "m", .end = "z" },
        &.{.{ .key = "m", .value = "two" }},
    ));
    try std.testing.expectEqual(@as(usize, 1), store.cachedGroupStoreCount());

    const restored = try store.groupState(std.testing.allocator, 1);
    defer shard_state_store.freeGroupStateEntries(std.testing.allocator, restored);
    try std.testing.expectEqual(@as(usize, 1), restored.len);
    try std.testing.expectEqualStrings("one", restored[0].value);
    try std.testing.expectEqual(@as(usize, 1), store.cachedGroupStoreCount());

    const prepared = (try store.snapshotBuilder().prepareSnapshot(1, 0)) orelse return error.MissingDataSnapshotSource;
    try store.retainActiveGroups(&.{});
    try std.testing.expectEqual(@as(usize, 1), store.cachedGroupStoreCount());
    try std.testing.expectError(error.ApplyStoreGroupRetired, store.latestBatch(1));
    prepared.deinit();
    try std.testing.expectEqual(@as(usize, 0), store.cachedGroupStoreCount());
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_in_memory_state).used_bytes);

    try store.retainActiveGroups(&.{1});
    const reassigned = (try store.latestBatch(1)) orelse return error.MissingDataBatch;
    try std.testing.expectEqual(@as(u64, 0), reassigned.commit_index);
    try std.testing.expectEqual(@as(usize, 1), store.cachedGroupStoreCount());
}

test "data raft apply placement transition retains union on abort and retires without allocation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/placement-transition", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var store = try RaftApplyStore.init(failing.allocator(), .{ .root_dir = root });
    defer store.deinit();
    try std.testing.expect(try store.seedGroupSnapshotIfAbsent(
        std.testing.allocator,
        1,
        1,
        .{ .start = "a", .end = "z" },
        &.{.{ .key = "a", .value = "one" }},
    ));
    try store.retainActiveGroups(&.{1});

    var aborted = try store.beginActiveGroupTransition(&.{1 + batch_shard_count});
    aborted.abort();
    const shard = store.batchShard(1);
    shard.mutex.lockUncancelable(store.io_impl.io());
    try std.testing.expect(shard.active_groups.contains(1));
    try std.testing.expect(shard.active_groups.contains(1 + batch_shard_count));
    shard.mutex.unlock(store.io_impl.io());

    var committed = try store.beginActiveGroupTransition(&.{1 + batch_shard_count});
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    committed.commit();
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);

    shard.mutex.lockUncancelable(store.io_impl.io());
    try std.testing.expect(!shard.active_groups.contains(1));
    try std.testing.expect(shard.active_groups.contains(1 + batch_shard_count));
    try std.testing.expect(!shard.stores.contains(1));
    shard.mutex.unlock(store.io_impl.io());
}

test "data raft apply placement transition retires high cardinality summaries in one pass" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/placement-transition-scale", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var store = try RaftApplyStore.init(failing.allocator(), .{ .root_dir = root });
    defer store.deinit();

    var group_ids: [2048]u64 = undefined;
    for (&group_ids, 1..) |*group_id, value| {
        group_id.* = value;
        const shard = store.batchShard(group_id.*);
        shard.mutex.lockUncancelable(store.io_impl.io());
        shard.batches.put(store.alloc, group_id.*, .{
            .commit_index = 0,
            .entry_count = 0,
            .normal_entry_count = 0,
            .admin_entry_count = 0,
            .last_entry_term = 0,
            .last_entry_index = 0,
        }) catch |err| {
            shard.mutex.unlock(store.io_impl.io());
            return err;
        };
        shard.mutex.unlock(store.io_impl.io());
    }
    try store.retainActiveGroups(&group_ids);

    var transition = try store.beginActiveGroupTransition(&.{});
    var candidate_count: usize = 0;
    for (&transition.retire_candidates) |*candidates| candidate_count += @intCast(candidates.count());
    try std.testing.expectEqual(group_ids.len, candidate_count);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    transition.commit();
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);

    for (&store.batch_shards) |*shard| {
        shard.mutex.lockUncancelable(store.io_impl.io());
        const remaining = shard.batches.count();
        shard.mutex.unlock(store.io_impl.io());
        try std.testing.expectEqual(@as(usize, 0), remaining);
    }
}

test "data apply store replay is idempotent when applied watermark lags WAL state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-apply-replay-idempotent", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try raft_storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 201, 1);
    defer layout.deinit(std.testing.allocator);

    const first_data = try std.testing.allocator.dupe(u8, "put:doc:a=1");
    defer std.testing.allocator.free(first_data);
    const second_data = try std.testing.allocator.dupe(u8, "put:doc:b=2");
    defer std.testing.allocator.free(second_data);

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();

        const entries = try std.testing.allocator.dupe(raft_engine.core.Entry, &[_]raft_engine.core.Entry{
            .{ .term = 4, .index = 1, .entry_type = .normal, .data = first_data },
            .{ .term = 4, .index = 2, .entry_type = .normal, .data = second_data },
        });
        defer std.testing.allocator.free(entries);

        try wal_state.groupStorage().persistReady(201, .{
            .hard_state = .{ .current_term = 4, .voted_for = 1, .commit_index = 2 },
            .entries = entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.DataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };

        try sm.stateMachine().applyReady(201, null, &.{.{
            .term = 4,
            .index = 1,
            .entry_type = .normal,
            .data = @constCast("put:doc:a=1"),
        }}, &.{});
        try sm.stateMachine().applyReady(201, null, &.{.{
            .term = 4,
            .index = 2,
            .entry_type = .normal,
            .data = @constCast("put:doc:b=2"),
        }}, &.{});
    }

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();
        try std.testing.expectEqual(@as(u64, 0), wal_state.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 201,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = wal_state.appliedIndex(),
        }, wal_state.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);

        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.DataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(201, rd.snapshot, rd.committed_entries, &.{});

        const batch = (try store.latestBatch(201)) orelse return error.MissingDataBatch;
        try std.testing.expectEqual(@as(u64, 2), batch.commit_index);
        try std.testing.expectEqual(@as(usize, 1), batch.entry_count);
        try std.testing.expectEqual(@as(u64, 2), batch.last_entry_index);

        const state = try store.groupState(std.testing.allocator, 201);
        defer shard_state_store.freeGroupStateEntries(std.testing.allocator, state);
        try std.testing.expectEqual(@as(usize, 2), state.len);
        try std.testing.expectEqualStrings("doc:a", state[0].key);
        try std.testing.expectEqualStrings("1", state[0].value);
        try std.testing.expectEqualStrings("doc:b", state[1].key);
        try std.testing.expectEqualStrings("2", state[1].value);

        const normal_entries = try store.appliedNormalEntries(std.testing.allocator, 201);
        defer {
            for (normal_entries) |entry| std.testing.allocator.free(entry.data);
            std.testing.allocator.free(normal_entries);
        }
        try std.testing.expectEqual(@as(usize, 2), normal_entries.len);
        try std.testing.expectEqual(@as(u64, 1), normal_entries[0].index);
        try std.testing.expectEqual(@as(u64, 2), normal_entries[1].index);
    }
}
