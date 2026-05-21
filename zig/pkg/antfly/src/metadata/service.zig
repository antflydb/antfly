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

const builtin = @import("builtin");
const std = @import("std");
const fs_paths = @import("../common/fs_paths.zig");
const common_secrets = @import("../common/secrets.zig");
const metadata_mod = @import("mod.zig");
const metadata_api = @import("api.zig");
const raft_engine = @import("raft_engine");
const metadata_control_loop = @import("control_loop.zig");
const metadata_reconcile_lease = @import("reconcile_lease.zig");
const metadata_reconciler = @import("reconciler.zig");
const metadata_replication_backfill = @import("replication_backfill.zig");
const metadata_state = @import("state.zig");
const metadata_table_provisioner = @import("table_provisioner.zig");
const metadata_store_observer = @import("store_observer.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_table_workflow = @import("table_workflow.zig");
const metadata_storage = @import("storage/mod.zig");
const platform_clock = @import("../platform/clock.zig");
const platform_time = @import("../platform/time.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const transition_state = @import("transition_state.zig");
const raft_catalog = @import("../raft/catalog.zig");
const raft_host = @import("../raft/host.zig");
const raft_managed_host = @import("../raft/managed_host.zig");
const raft_service = @import("../raft/service.zig");
const http_common = @import("../raft/transport/http_common.zig");
const api_table_catalog = @import("../api/table_catalog.zig");
const api_table_router = @import("../api/table_router.zig");
const api_table_writes = @import("../api/table_writes.zig");
const db_mod = @import("../storage/db/mod.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const backfill_state_mod = @import("../storage/db/backfill_state.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const foreign_mod = @import("../foreign/mod.zig");

const cdc_replication_round_interval_ms: u64 = 1_000;

const LifecycleSignal = struct {
    alloc: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    wake_epoch: std.atomic.Value(u32) = .init(0),
    epoch: u64 = 0,
    table_epochs: std.StringHashMapUnmanaged(u64) = .empty,

    const Snapshot = struct {
        global_epoch: u64,
        table_name: ?[]const u8 = null,
        table_epoch: u64 = 0,
    };

    fn init(alloc: std.mem.Allocator) LifecycleSignal {
        return .{ .alloc = alloc };
    }

    fn current(self: *const LifecycleSignal) u32 {
        const mutable: *LifecycleSignal = @constCast(self);
        mutable.lock();
        defer mutable.unlock();
        return @intCast(mutable.epoch);
    }

    fn currentEpoch(self: *const LifecycleSignal) u64 {
        const mutable: *LifecycleSignal = @constCast(self);
        mutable.lock();
        defer mutable.unlock();
        return mutable.epoch;
    }

    fn snapshot(self: *LifecycleSignal, table_name: ?[]const u8) Snapshot {
        self.lock();
        defer self.unlock();
        return .{
            .global_epoch = self.epoch,
            .table_name = table_name,
            .table_epoch = if (table_name) |name| self.table_epochs.get(name) orelse 0 else 0,
        };
    }

    fn notify(self: *LifecycleSignal, table_name: ?[]const u8) void {
        self.lock();
        self.epoch +%= 1;
        if (table_name) |name| {
            const owned_name = self.table_epochs.getKey(name) orelse blk: {
                const duped = self.alloc.dupe(u8, name) catch {
                    self.unlockAndWake();
                    return;
                };
                self.table_epochs.put(self.alloc, duped, 0) catch {
                    self.alloc.free(duped);
                    self.unlockAndWake();
                    return;
                };
                break :blk self.table_epochs.getKey(name).?;
            };
            const current_epoch = self.table_epochs.get(owned_name) orelse 0;
            self.table_epochs.putAssumeCapacity(owned_name, current_epoch +% 1);
        }
        self.unlockAndWake();
    }

    fn wait(self: *LifecycleSignal, observed: Snapshot, timeout_ns: u64) void {
        const start_ns = platform_time.monotonicNs();
        while (true) {
            self.lock();
            if (self.changedLocked(observed)) {
                self.unlock();
                return;
            }
            const wake_epoch = self.wake_epoch.load(.acquire);
            self.unlock();

            const elapsed_ns = platform_time.monotonicNs() -| start_ns;
            if (elapsed_ns >= timeout_ns) return;
            const remaining_ns = timeout_ns - elapsed_ns;
            std.Io.futexWaitTimeout(
                std.Options.debug_io,
                u32,
                &self.wake_epoch.raw,
                wake_epoch,
                .{ .duration = .{
                    .clock = .awake,
                    .raw = .fromNanoseconds(@intCast(remaining_ns)),
                } },
            ) catch return;
        }
    }

    fn changedLocked(self: *const LifecycleSignal, observed: Snapshot) bool {
        if (self.epoch != observed.global_epoch) return true;
        if (observed.table_name) |name| {
            return (self.table_epochs.get(name) orelse 0) != observed.table_epoch;
        }
        return false;
    }

    fn deinit(self: *LifecycleSignal) void {
        var it = self.table_epochs.iterator();
        while (it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.table_epochs.deinit(self.alloc);
        self.* = undefined;
    }

    fn lock(self: *LifecycleSignal) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
    }

    fn unlock(self: *LifecycleSignal) void {
        self.mutex.unlock(std.Options.debug_io);
    }

    fn unlockAndWake(self: *LifecycleSignal) void {
        self.unlock();
        _ = self.wake_epoch.fetchAdd(1, .release);
        std.Io.futexWake(std.Options.debug_io, u32, &self.wake_epoch.raw, std.math.maxInt(u32));
    }
};

pub const LifecycleReconcileHook = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        run: *const fn (ptr: *anyopaque) anyerror!void,
    };

    pub fn run(self: LifecycleReconcileHook) !void {
        try self.vtable.run(self.ptr);
    }
};

pub const LocalReplicaRootReconcileHook = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        run: *const fn (ptr: *anyopaque) anyerror!void,
    };

    pub fn run(self: LocalReplicaRootReconcileHook) !void {
        try self.vtable.run(self.ptr);
    }
};

pub const LocalReplicaRootReconcilePermitHook = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        should_reconcile: *const fn (ptr: *anyopaque) bool,
    };

    pub fn shouldReconcile(self: LocalReplicaRootReconcilePermitHook) bool {
        return self.vtable.should_reconcile(self.ptr);
    }
};

pub const MetadataServiceConfig = struct {
    raft: raft_service.ManagedServiceConfig = .{},
    reconcile_lease: metadata_reconcile_lease.Config = .{},
    observe_local_replica_root: bool = true,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    metadata_orchestration_urls: []const MetadataOrchestrationUrl = &.{},
    secret_store: ?*common_secrets.FileStore = null,
};

pub const MetadataOrchestrationUrl = struct {
    node_id: u64,
    url: []const u8,
};

fn cloneMetadataOrchestrationUrls(
    alloc: std.mem.Allocator,
    urls: []const MetadataOrchestrationUrl,
) ![]MetadataOrchestrationUrl {
    if (urls.len == 0) return &.{};
    const out = try alloc.alloc(MetadataOrchestrationUrl, urls.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |entry| alloc.free(entry.url);
        alloc.free(out);
    }
    for (urls, 0..) |entry, index| {
        out[index] = .{
            .node_id = entry.node_id,
            .url = try alloc.dupe(u8, entry.url),
        };
        initialized += 1;
    }
    return out;
}

fn freeMetadataOrchestrationUrls(alloc: std.mem.Allocator, urls: []MetadataOrchestrationUrl) void {
    for (urls) |entry| alloc.free(entry.url);
    if (urls.len > 0) alloc.free(urls);
}

pub const MetadataServiceDeps = struct {
    host: raft_managed_host.ManagedHostDeps = .{},
    raft: raft_service.ManagedServiceDeps = .{},
};

pub const MetadataHttpServiceDeps = struct {
    http: raft_managed_host.ManagedHttpHostDeps = .{},
    raft: raft_service.ManagedServiceDeps = .{},
};

pub const MetadataStatus = metadata_api.MetadataStatus;
// Backfill marker discovery does not need sub-second polling when the system is
// otherwise idle. Keep active-marker refreshes fast, but back off empty-root
// probes so they do not add filesystem churn on the read hot path.
const store_status_backfill_probe_interval_ticks: usize = 200;
const store_status_backfill_rescan_interval_ms: u64 = std.time.ms_per_s;
const store_status_backfill_empty_rescan_interval_ms: u64 = 30 * std.time.ms_per_s;
const local_placement_refresh_interval_ms: u64 = 5 * std.time.ms_per_s;
const local_transition_refresh_interval_ms: u64 = 5 * std.time.ms_per_s;
// These scan projected tables/ranges and local replica files. Epoch/group-id
// changes bypass the interval, so steady-state polling can be less aggressive
// without delaying structural metadata changes.
const local_schema_progress_refresh_interval_ms: u64 = 30 * std.time.ms_per_s;
const local_table_provisioning_refresh_interval_ms: u64 = 30 * std.time.ms_per_s;
const reconcile_lease_probe_interval_ms: u64 = 250;
const metadata_status_cache_refresh_interval_ms: u64 = 5 * std.time.ms_per_s;

const ReconcileLeaseProjectionCache = struct {
    epoch: ?u64 = null,
    record: ?metadata_reconcile_lease.ReconcileLeaseRecord = null,
    next_refresh_at_ms: u64 = 0,
};

pub const LocalGroupStatusProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        collect: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            replica_root_dir: []const u8,
            tables: []const metadata_table_manager.TableRecord,
            ranges: []const metadata_table_manager.RangeRecord,
            stores: []const metadata_table_manager.StoreRecord,
            merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
            split_transitions: []const transition_state.SplitTransitionRecord,
            merge_transitions: []const transition_state.MergeTransitionRecord,
            split_observations: []const transition_state.SplitObservationRecord,
            merge_observations: []const transition_state.MergeObservationRecord,
        ) anyerror![]metadata_table_manager.GroupStatusReport,
    };

    pub fn collect(
        self: LocalGroupStatusProvider,
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        stores: []const metadata_table_manager.StoreRecord,
        merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
        split_transitions: []const transition_state.SplitTransitionRecord,
        merge_transitions: []const transition_state.MergeTransitionRecord,
        split_observations: []const transition_state.SplitObservationRecord,
        merge_observations: []const transition_state.MergeObservationRecord,
    ) ![]metadata_table_manager.GroupStatusReport {
        return try self.vtable.collect(
            self.ptr,
            alloc,
            replica_root_dir,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        );
    }
};

fn nowMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

fn groupIdsFingerprint(group_ids: []const u64) u64 {
    return std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(group_ids));
}

const LocalProjectionInputs = struct {
    group_ids: []u64,
    group_ids_fingerprint: u64,
    tables: []metadata_table_manager.TableRecord,
    ranges: []metadata_table_manager.RangeRecord,
    stores: []metadata_table_manager.StoreRecord,
    schema_progresses: []metadata_table_manager.SchemaProgressRecord,
    restore_progresses: []metadata_table_manager.RestoreProgressRecord,
};

const ProjectedCoreSnapshot = struct {
    tables: []metadata_table_manager.TableRecord = &.{},
    ranges: []metadata_table_manager.RangeRecord = &.{},
    stores: []metadata_table_manager.StoreRecord = &.{},
    placement_intents: []raft_reconciler.PlacementIntent = &.{},
    shuffle_join_leases: []metadata_table_manager.ShuffleJoinLeaseRecord = &.{},
    schema_progresses: []metadata_table_manager.SchemaProgressRecord = &.{},
    restore_progresses: []metadata_table_manager.RestoreProgressRecord = &.{},
    replication_source_statuses: []metadata_table_manager.ReplicationSourceStatusRecord = &.{},
    split_transitions: []transition_state.SplitTransitionRecord = &.{},
    merge_transitions: []transition_state.MergeTransitionRecord = &.{},

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.tables) |record| metadata_table_manager.freeTable(alloc, record);
        if (self.tables.len > 0) alloc.free(self.tables);
        for (self.ranges) |record| metadata_table_manager.freeRange(alloc, record);
        if (self.ranges.len > 0) alloc.free(self.ranges);
        for (self.stores) |record| metadata_table_manager.freeStore(alloc, record);
        if (self.stores.len > 0) alloc.free(self.stores);
        for (self.placement_intents) |intent| alloc.free(intent.peer_node_ids);
        if (self.placement_intents.len > 0) alloc.free(self.placement_intents);
        if (self.shuffle_join_leases.len > 0) alloc.free(self.shuffle_join_leases);
        if (self.schema_progresses.len > 0) alloc.free(self.schema_progresses);
        for (self.restore_progresses) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
        if (self.restore_progresses.len > 0) alloc.free(self.restore_progresses);
        for (self.replication_source_statuses) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
        if (self.replication_source_statuses.len > 0) alloc.free(self.replication_source_statuses);
        for (self.split_transitions) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
        if (self.split_transitions.len > 0) alloc.free(self.split_transitions);
        for (self.merge_transitions) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
        if (self.merge_transitions.len > 0) alloc.free(self.merge_transitions);
        self.* = undefined;
    }
};

const ProjectedCoreSnapshotCache = struct {
    projection_epoch: u64 = 0,
    placement_epoch: u64 = 0,
    transition_epoch: u64 = 0,
    snapshot: ?ProjectedCoreSnapshot = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.snapshot) |*snapshot| snapshot.deinit(alloc);
        self.* = .{};
    }
};

const LocalPlacementInputs = struct {
    placement_intents: []raft_reconciler.PlacementIntent,
};

const LocalTransitionInputs = struct {
    split_transitions: []transition_state.SplitTransitionRecord,
    merge_transitions: []transition_state.MergeTransitionRecord,
};

fn captureLocalProjectionInputs(self: *MetadataHttpService) !LocalProjectionInputs {
    const group_ids = try self.raft.host.http_host.host.listGroupIds(self.alloc);
    errdefer self.alloc.free(group_ids);
    self.lockRuntime();
    defer self.unlockRuntime();
    const snapshot = try self.projectedCoreSnapshotLocked();
    const tables = try cloneProjectedTablesOwned(self.alloc, snapshot.tables);
    errdefer self.freeProjectedTables(self.alloc, tables);
    const ranges = try cloneProjectedRangesOwned(self.alloc, snapshot.ranges);
    errdefer self.freeProjectedRanges(self.alloc, ranges);
    const stores = try cloneProjectedStoresOwned(self.alloc, snapshot.stores);
    errdefer self.freeProjectedStores(self.alloc, stores);
    const schema_progresses = try cloneProjectedSchemaProgressOwned(self.alloc, snapshot.schema_progresses);
    errdefer self.freeProjectedSchemaProgress(self.alloc, schema_progresses);
    const restore_progresses = try cloneProjectedRestoreProgressesOwned(self.alloc, snapshot.restore_progresses);
    errdefer self.freeProjectedRestoreProgress(self.alloc, restore_progresses);
    return .{
        .group_ids = group_ids,
        .group_ids_fingerprint = groupIdsFingerprint(group_ids),
        .tables = tables,
        .ranges = ranges,
        .stores = stores,
        .schema_progresses = schema_progresses,
        .restore_progresses = restore_progresses,
    };
}

fn freeLocalProjectionInputs(self: *MetadataHttpService, inputs: *LocalProjectionInputs) void {
    self.alloc.free(inputs.group_ids);
    self.freeProjectedTables(self.alloc, inputs.tables);
    self.freeProjectedRanges(self.alloc, inputs.ranges);
    self.freeProjectedStores(self.alloc, inputs.stores);
    self.freeProjectedSchemaProgress(self.alloc, inputs.schema_progresses);
    self.freeProjectedRestoreProgress(self.alloc, inputs.restore_progresses);
    inputs.* = undefined;
}

fn cloneProjectedTablesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.TableRecord,
) ![]metadata_table_manager.TableRecord {
    const out = try alloc.alloc(metadata_table_manager.TableRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneTable(alloc, record);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedRangesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.RangeRecord,
) ![]metadata_table_manager.RangeRecord {
    const out = try alloc.alloc(metadata_table_manager.RangeRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneRange(alloc, record);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedStoresOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.StoreRecord,
) ![]metadata_table_manager.StoreRecord {
    const out = try alloc.alloc(metadata_table_manager.StoreRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeStore(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneStore(alloc, record);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedShuffleJoinLeasesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.ShuffleJoinLeaseRecord,
) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
    const out = try alloc.alloc(metadata_table_manager.ShuffleJoinLeaseRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try metadata_table_manager.cloneShuffleJoinLease(alloc, record);
    return out;
}

fn cloneProjectedSchemaProgressOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.SchemaProgressRecord,
) ![]metadata_table_manager.SchemaProgressRecord {
    const out = try alloc.alloc(metadata_table_manager.SchemaProgressRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = record;
    return out;
}

fn cloneProjectedPlacementIntentsOwned(
    alloc: std.mem.Allocator,
    intents: []const raft_reconciler.PlacementIntent,
) ![]raft_reconciler.PlacementIntent {
    const out = try alloc.alloc(raft_reconciler.PlacementIntent, intents.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |intent| {
            raft_reconciler.freeIntentOwned(alloc, intent);
        }
        alloc.free(out);
    }
    for (intents, 0..) |intent, i| {
        out[i] = try raft_reconciler.cloneIntentOwned(alloc, intent);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedSplitTransitionsOwned(
    alloc: std.mem.Allocator,
    records: []const transition_state.SplitTransitionRecord,
) ![]transition_state.SplitTransitionRecord {
    const out = try alloc.alloc(transition_state.SplitTransitionRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .source_group_id = record.source_group_id,
            .destination_group_id = record.destination_group_id,
            .phase = record.phase,
        };
        errdefer metadata_table_manager.freeSplitTransitionRecord(alloc, out[i]);
        out[i].split_key = if (record.split_key) |value| try alloc.dupe(u8, value) else null;
        out[i].source_range_end = if (record.source_range_end) |value| try alloc.dupe(u8, value) else null;
        out[i].rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null;
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedRestoreProgressesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.RestoreProgressRecord,
) ![]metadata_table_manager.RestoreProgressRecord {
    const out = try alloc.alloc(metadata_table_manager.RestoreProgressRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try metadata_table_manager.cloneRestoreProgress(alloc, record);
    return out;
}

fn cloneProjectedReplicationSourceStatusesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.ReplicationSourceStatusRecord,
) ![]metadata_table_manager.ReplicationSourceStatusRecord {
    const out = try alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try metadata_table_manager.cloneReplicationSourceStatus(alloc, record);
    return out;
}

fn cloneProjectedMergeTransitionsOwned(
    alloc: std.mem.Allocator,
    records: []const transition_state.MergeTransitionRecord,
) ![]transition_state.MergeTransitionRecord {
    const out = try alloc.alloc(transition_state.MergeTransitionRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .donor_group_id = record.donor_group_id,
            .receiver_group_id = record.receiver_group_id,
            .phase = record.phase,
            .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
        };
        errdefer metadata_table_manager.freeMergeTransitionRecord(alloc, out[i]);
        out[i].rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null;
        cloned = i + 1;
    }
    return out;
}

fn captureLocalPlacementInputs(self: *MetadataHttpService) !LocalPlacementInputs {
    self.lockRuntime();
    defer self.unlockRuntime();
    const snapshot = try self.projectedCoreSnapshotLocked();
    return .{
        .placement_intents = try cloneProjectedPlacementIntentsOwned(self.alloc, snapshot.placement_intents),
    };
}

fn freeLocalPlacementInputs(self: *MetadataHttpService, inputs: *LocalPlacementInputs) void {
    self.freeProjectedPlacementIntents(self.alloc, inputs.placement_intents);
    inputs.* = undefined;
}

fn captureLocalTransitionInputs(self: *MetadataHttpService) !LocalTransitionInputs {
    self.lockRuntime();
    defer self.unlockRuntime();
    const snapshot = try self.projectedCoreSnapshotLocked();
    return .{
        .split_transitions = try cloneProjectedSplitTransitionsOwned(self.alloc, snapshot.split_transitions),
        .merge_transitions = try cloneProjectedMergeTransitionsOwned(self.alloc, snapshot.merge_transitions),
    };
}

fn freeLocalTransitionInputs(self: *MetadataHttpService, inputs: *LocalTransitionInputs) void {
    self.freeProjectedSplitTransitions(self.alloc, inputs.split_transitions);
    self.freeProjectedMergeTransitions(self.alloc, inputs.merge_transitions);
    inputs.* = undefined;
}

fn shouldRefreshLocalProjection(
    last_epoch: ?u64,
    last_group_ids_fingerprint: ?u64,
    last_refresh_at_ms: u64,
    current_epoch: u64,
    current_group_ids_fingerprint: u64,
    refresh_interval_ms: u64,
) bool {
    if (last_epoch != current_epoch) return true;
    if (last_group_ids_fingerprint != current_group_ids_fingerprint) return true;
    if (last_refresh_at_ms == 0) return true;
    return nowMs() -| last_refresh_at_ms >= refresh_interval_ms;
}

fn shouldRefreshLocalEpoch(
    last_epoch: ?u64,
    last_refresh_at_ms: u64,
    current_epoch: u64,
    refresh_interval_ms: u64,
) bool {
    if (last_epoch != current_epoch) return true;
    if (last_refresh_at_ms == 0) return true;
    return nowMs() -| last_refresh_at_ms >= refresh_interval_ms;
}

fn reconcileLeaseCacheNextRefreshAtMs(
    state: *const metadata_reconcile_lease.State,
    is_local_leader: bool,
    record: ?metadata_reconcile_lease.ReconcileLeaseRecord,
    now_ms: u64,
) u64 {
    const fallback_refresh_at_ms = now_ms + reconcile_lease_probe_interval_ms;
    const current = record orelse return fallback_refresh_at_ms;
    if (is_local_leader and current.owner_node_id == state.local_node_id) {
        const renew_margin_ms = if (state.config.lease_ttl_ms > 1_000)
            state.config.lease_ttl_ms / 2
        else
            state.config.lease_ttl_ms;
        const renew_at_ms = current.expires_at_ms -| renew_margin_ms;
        return if (renew_at_ms > now_ms) renew_at_ms else now_ms;
    }
    return if (current.expires_at_ms > now_ms)
        @min(current.expires_at_ms, fallback_refresh_at_ms)
    else
        now_ms;
}

pub const MetadataService = struct {
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    replica_root_dir: ?[]const u8,
    observe_local_replica_root: bool,
    store_status_ticks: usize,
    projection_epoch: std.atomic.Value(u64) = .init(1),
    placement_epoch: std.atomic.Value(u64) = .init(1),
    reconcile_lease_epoch: std.atomic.Value(u64) = .init(1),
    transition_epoch: std.atomic.Value(u64) = .init(1),
    local_placement_epoch: ?u64,
    last_local_placement_refresh_at_ms: u64,
    local_transition_epoch: ?u64,
    last_local_transition_refresh_at_ms: u64,
    local_table_provisioning_fingerprint: ?u64,
    local_table_provisioning_epoch: ?u64,
    local_table_provisioning_group_ids_fingerprint: ?u64,
    last_local_table_provisioning_refresh_at_ms: u64,
    local_schema_progress_epoch: ?u64,
    local_schema_progress_group_ids_fingerprint: ?u64,
    last_local_schema_progress_refresh_at_ms: u64,
    cdc_runtime_mutex: std.atomic.Mutex = .unlocked,
    reconcile_lease: metadata_reconcile_lease.State,
    lifecycle_signal: LifecycleSignal,
    lifecycle_reconcile_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    lifecycle_reconcile_hook: ?LifecycleReconcileHook = null,
    local_replica_root_reconcile_hook: ?LocalReplicaRootReconcileHook = null,
    local_replica_root_reconcile_permit_hook: ?LocalReplicaRootReconcilePermitHook = null,
    lifecycle_listener_registered: bool = false,
    local_group_status_provider: ?LocalGroupStatusProvider = null,
    local_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    routed_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    reconcile_lease_projection_cache: ReconcileLeaseProjectionCache = .{},
    store_status_backfill_probe_ticks: usize = 0,
    store_status_backfill_marker_cache: StoreStatusBackfillMarkerCache = .{},
    cdc_backfill_registry: foreign_mod.Registry = .{},
    cdc_next_round_at_ms: u64 = 0,
    secret_store: ?*common_secrets.FileStore = null,
    backend_runtime_mutex: std.atomic.Mutex = .unlocked,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    owned_backend_runtime: ?backend_runtime_mod.BackendRuntimeHandle = null,
    raft: raft_service.ManagedHostService,

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: raft_managed_host.ManagedHostConfig,
        deps: MetadataServiceDeps,
        cfg: MetadataServiceConfig,
    ) !MetadataService {
        const metadata_group_id = host_cfg.host.metadata_group_id orelse return error.MissingMetadataGroupId;
        var service = MetadataService{
            .alloc = alloc,
            .metadata_group_id = metadata_group_id,
            .replica_root_dir = host_cfg.host.replica_root_dir,
            .observe_local_replica_root = cfg.observe_local_replica_root,
            .store_status_ticks = 0,
            .local_placement_epoch = null,
            .last_local_placement_refresh_at_ms = 0,
            .local_transition_epoch = null,
            .last_local_transition_refresh_at_ms = 0,
            .local_table_provisioning_fingerprint = null,
            .local_table_provisioning_epoch = null,
            .local_table_provisioning_group_ids_fingerprint = null,
            .last_local_table_provisioning_refresh_at_ms = 0,
            .local_schema_progress_epoch = null,
            .local_schema_progress_group_ids_fingerprint = null,
            .last_local_schema_progress_refresh_at_ms = 0,
            .reconcile_lease = metadata_reconcile_lease.State.init(host_cfg.host.local_node_id, cfg.reconcile_lease),
            .lifecycle_signal = LifecycleSignal.init(alloc),
            .backend_runtime = cfg.backend_runtime,
            .secret_store = cfg.secret_store,
            .raft = try raft_service.ManagedHostService.init(alloc, host_cfg, deps.host, cfg.raft, deps.raft),
        };
        errdefer service.deinit();
        try foreign_mod.registerDefaultPostgresExecutor(alloc, &service.cdc_backfill_registry);
        return service;
    }

    pub fn deinit(self: *MetadataService) void {
        self.store_status_backfill_marker_cache.deinit(self.alloc);
        self.cdc_backfill_registry.deinit(self.alloc);
        self.lifecycle_signal.deinit();
        self.raft.deinit();
        if (self.replica_root_dir) |replica_root_dir| {
            api_table_writes.closeHostedManagedDbCacheForRoot(replica_root_dir);
        }
        if (self.owned_backend_runtime) |*runtime| runtime.deinit();
        self.owned_backend_runtime = null;
        self.backend_runtime = null;
        self.* = undefined;
    }

    pub fn ensureBackendRuntime(self: *MetadataService) !*backend_runtime_mod.BackendRuntime {
        while (!self.backend_runtime_mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
        defer self.backend_runtime_mutex.unlock();
        if (self.backend_runtime == null) {
            self.owned_backend_runtime = try backend_runtime_mod.BackendRuntimeHandle.init(self.alloc, .{});
            self.backend_runtime = self.owned_backend_runtime.?.ptr();
        }
        if (self.backend_runtime) |runtime| return runtime;
        unreachable;
    }

    pub fn lifecycleSignalCurrent(self: *const MetadataService) u32 {
        return self.lifecycle_signal.current();
    }

    pub fn captureLifecycleSignal(self: *MetadataService, table_name: ?[]const u8) LifecycleSignal.Snapshot {
        return self.lifecycle_signal.snapshot(table_name);
    }

    pub fn waitForLifecycleSignal(self: *MetadataService, observed: LifecycleSignal.Snapshot, timeout_ns: u64) void {
        self.lifecycle_signal.wait(observed, timeout_ns);
    }

    pub fn setLifecycleReconcileHook(self: *MetadataService, hook: ?LifecycleReconcileHook) void {
        self.lifecycle_reconcile_hook = hook;
        self.lifecycle_reconcile_requested.store(true, .release);
    }

    pub fn setLocalGroupStatusProvider(self: *MetadataService, provider: ?LocalGroupStatusProvider) void {
        self.local_group_status_provider = provider;
    }

    pub fn setLocalShardDbAdapter(self: *MetadataService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.local_shard_db_adapter = adapter;
    }

    pub fn setRoutedShardDbAdapter(self: *MetadataService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.routed_shard_db_adapter = adapter;
    }

    pub fn setLocalReplicaRootReconcileHook(self: *MetadataService, hook: ?LocalReplicaRootReconcileHook) void {
        self.local_replica_root_reconcile_hook = hook;
    }

    pub fn setLocalReplicaRootReconcilePermitHook(self: *MetadataService, hook: ?LocalReplicaRootReconcilePermitHook) void {
        self.local_replica_root_reconcile_permit_hook = hook;
    }

    fn ensureLifecycleListenerRegistered(self: *MetadataService) !void {
        if (self.lifecycle_listener_registered) return;
        const store = self.projectedStore() orelse return;
        try store.addProjectionListener(.{
            .ptr = self,
            .vtable = &.{
                .on_projection_signal = metadataServiceProjectionSignal,
            },
        });
        try store.addCommittedKeyListener(.{
            .ptr = self,
            .vtable = &.{
                .matches_key = metadataServiceLifecycleKeyMatches,
                .on_committed_key = metadataServiceCommittedKeySignal,
            },
        });
        self.lifecycle_listener_registered = true;
    }

    fn metadataServiceProjectionSignal(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.ProjectionSignal) void {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        switch (signal.kind) {
            .table, .range, .shuffle_join_lease => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .placement_intent => _ = self.placement_epoch.fetchAdd(1, .monotonic),
            .reconcile_lease => _ = self.reconcile_lease_epoch.fetchAdd(1, .monotonic),
            .split_transition, .merge_transition => _ = self.transition_epoch.fetchAdd(1, .monotonic),
            else => {},
        }
        self.lifecycle_signal.notify(signal.table_name);
    }

    fn metadataServiceLifecycleKeyMatches(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.CommittedKeySignal) bool {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        return lifecycleKeyMatchesMetadataNamespace(self.metadata_group_id, signal);
    }

    fn metadataServiceCommittedKeySignal(ptr: *anyopaque, _: metadata_storage.raft_apply_store.CommittedKeySignal) void {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        self.lifecycle_reconcile_requested.store(true, .release);
        self.lifecycle_signal.notify(null);
    }

    pub fn medianKeyLookup(self: *MetadataService) ?metadata_reconciler.MedianKeyLookup {
        if (self.routed_shard_db_adapter == null and self.local_shard_db_adapter == null and self.replica_root_dir == null) return null;
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
            },
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        if (self.routed_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        if (self.local_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        const replica_root_dir = self.replica_root_dir orelse return error.UnsupportedOperation;
        var fallback = metadata_mod.FallbackLocalShardDbAdapter{
            .replica_root_dir = replica_root_dir,
            .backend_runtime = try self.ensureBackendRuntime(),
        };
        return try fallback.adapter().fetchMedianKey(alloc, group_id);
    }

    pub fn ensureMetadataReplica(self: *MetadataService, record: raft_catalog.ReplicaRecord) !raft_engine.runtime.EnsureReplicaResult {
        if (record.group_id != self.metadata_group_id) return error.InvalidMetadataGroupId;
        return try self.raft.host.host.ensureReplica(record);
    }

    pub fn campaignMetadataGroup(self: *MetadataService) !void {
        try self.raft.host.host.campaignGroup(self.metadata_group_id);
    }

    pub fn proposeTransitionCommand(self: *MetadataService, command: metadata_storage.TransitionCommand) !void {
        try metadata_storage.validateTransitionCommandDataGroupIds(command);
        const encoded = try metadata_storage.encodeTransitionCommand(self.alloc, command);
        defer self.alloc.free(encoded);
        try self.raft.host.host.propose(self.metadata_group_id, encoded);
        self.lifecycle_signal.notify(null);
    }

    pub fn upsertNode(self: *MetadataService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_node = record });
    }

    pub fn registerNode(self: *MetadataService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .register_node = record });
    }

    pub fn requestNodeShutdown(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .request_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn cancelNodeShutdown(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .cancel_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn finalizeNodeShutdown(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .finalize_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn removeNode(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_node = .{ .node_id = node_id } });
    }

    pub fn upsertStore(self: *MetadataService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_store = record });
    }

    pub fn registerStore(self: *MetadataService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .register_store = record });
    }

    pub fn reportStoreStatus(self: *MetadataService, report: metadata_table_manager.StoreStatusReport) !void {
        _ = try self.reportStoreStatuses(&.{report});
    }

    pub fn reportStoreStatuses(self: *MetadataService, reports: []const metadata_table_manager.StoreStatusReport) !usize {
        const projected = try self.listProjectedStores(self.alloc);
        defer self.freeProjectedStores(self.alloc, projected);

        var changed_indices = std.ArrayListUnmanaged(usize).empty;
        defer changed_indices.deinit(self.alloc);
        for (reports) |report| {
            const index = metadata_store_observer.findStoreIndex(projected, report.store_id) orelse return error.UnknownStore;
            if (!metadata_store_observer.observationChangesRecord(projected[index], report)) continue;
            try changed_indices.append(self.alloc, index);
        }

        const applied = try metadata_store_observer.applyObservationsOwned(self.alloc, projected, reports);
        for (changed_indices.items) |index| try self.upsertStore(projected[index]);
        return applied;
    }

    pub fn removeStore(self: *MetadataService, store_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_store = .{ .store_id = store_id } });
    }

    pub fn upsertSplitTransition(self: *MetadataService, record: transition_state.SplitTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_split_transition = record });
    }

    pub fn upsertReplicaIntent(self: *MetadataService, intent: raft_reconciler.PlacementIntent) !void {
        try self.proposeTransitionCommand(.{ .upsert_replica_intent = intent });
    }

    pub fn removeReplicaIntent(self: *MetadataService, group_id: u64, local_node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_replica_intent = .{
            .group_id = group_id,
            .local_node_id = local_node_id,
        } });
    }

    pub fn upsertTable(self: *MetadataService, record: metadata_table_manager.TableRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_table = record });
    }

    pub fn removeTable(self: *MetadataService, table_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_table = .{ .table_id = table_id } });
    }

    pub fn upsertSchemaProgress(self: *MetadataService, record: metadata_table_manager.SchemaProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_schema_progress = record });
    }

    pub fn removeSchemaProgress(self: *MetadataService, table_id: u64, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_schema_progress = .{
            .table_id = table_id,
            .node_id = node_id,
        } });
    }

    pub fn upsertRestoreProgress(self: *MetadataService, record: metadata_table_manager.RestoreProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_restore_progress = record });
    }

    pub fn removeRestoreProgress(self: *MetadataService, table_id: u64, node_id: u64, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_restore_progress = .{
            .table_id = table_id,
            .node_id = node_id,
            .group_id = group_id,
        } });
    }

    pub fn upsertReplicationSourceStatus(self: *MetadataService, record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_replication_source_status = record });
    }

    pub fn removeReplicationSourceStatus(self: *MetadataService, table_id: u64, source_ordinal: u32) !void {
        try self.proposeTransitionCommand(.{ .remove_replication_source_status = .{
            .table_id = table_id,
            .source_ordinal = source_ordinal,
        } });
    }

    pub fn upsertRange(self: *MetadataService, record: metadata_table_manager.RangeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_range = record });
    }

    pub fn removeRange(self: *MetadataService, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_range = .{ .group_id = group_id } });
    }

    pub fn removeSplitTransition(self: *MetadataService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_split_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertMergeTransition(self: *MetadataService, record: transition_state.MergeTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_merge_transition = record });
    }

    pub fn removeMergeTransition(self: *MetadataService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_merge_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertReconcileLease(self: *MetadataService, record: metadata_reconcile_lease.ReconcileLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_reconcile_lease = record });
    }

    pub fn removeReconcileLease(self: *MetadataService) !void {
        try self.proposeTransitionCommand(.{ .remove_reconcile_lease = .{} });
    }

    pub fn upsertShuffleJoinLease(self: *MetadataService, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_shuffle_join_lease = record });
    }

    pub fn removeShuffleJoinLease(self: *MetadataService, job_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_shuffle_join_lease = .{ .job_id = job_id } });
    }

    pub fn requestReallocation(self: *MetadataService, requested_at_ms: u64) !void {
        try self.proposeTransitionCommand(.{ .upsert_reallocation_request = .{
            .requested_at_ms = requested_at_ms,
        } });
    }

    pub fn clearReallocationRequest(self: *MetadataService) !void {
        try self.proposeTransitionCommand(.{ .remove_reallocation_request = .{} });
    }

    pub fn runRound(self: *MetadataService) !void {
        try self.ensureLifecycleListenerRegistered();
        defer self.lifecycle_signal.notify(null);
        try self.raft.runRound();
        if (!self.observe_local_replica_root) return;
        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForRound();
        if ((self.store_status_ticks >= 40 or backfill_markers.len > 0) and shouldRefreshLocalStoreStatus(self, backfill_markers)) {
            self.store_status_ticks = 0;
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, true) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
        }
        self.refreshLocalSchemaProgress() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        if (!try runReplicationBackfillIfLeaseHeld(self)) return;
        try self.refreshLocalPlacementIntents();
        try self.refreshLocalTransitions();
        _ = self.refreshLocalTableProvisioning() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        try self.completeRestoreIntentsIfReady();
        try self.runLifecycleReconcileHookIfRequested();
    }

    pub fn runLifecycleRound(self: *MetadataService) !void {
        try self.ensureLifecycleListenerRegistered();
        defer self.lifecycle_signal.notify(null);
        try self.raft.runRound();
        if (!self.observe_local_replica_root) return;

        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForLifecycleRound();
        if (shouldRefreshLocalStoreStatusForLifecycleRound(self, backfill_markers)) {
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, false) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
        }
        self.refreshLocalSchemaProgress() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        if (!try runReplicationBackfillIfLeaseHeld(self)) return;
        try self.refreshLocalPlacementIntents();
        try self.refreshLocalTransitions();
        _ = self.refreshLocalTableProvisioning() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        try self.completeRestoreIntentsIfReady();
        try self.runLifecycleReconcileHookIfRequested();
    }

    pub fn waitForTableLifecycle(self: *MetadataService, table_name: []const u8, expected: TableLifecycleExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableLifecycleConvergence(self, table_name, expected);
    }

    pub fn waitForTableProjection(self: *MetadataService, table_name: []const u8, expected: TableProjectionExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableProjectionConvergence(self, table_name, expected);
    }

    pub fn reconcileOnceIfLeaseHeld(self: *MetadataService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcileOnce(self);
    }

    pub fn reconcilePreparedIfLeaseHeld(self: *MetadataService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcilePrepared(self);
    }

    pub fn reconcileOnceEnsuringLease(self: *MetadataService, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            if (try self.reconcileOnceIfLeaseHeld(loop)) |summary| return summary;
            try self.runRound();
        }
        return error.ReconcileLeaseNotHeld;
    }

    pub fn applyReconciliationPlan(self: *MetadataService, plan: *const metadata_reconciler.ReconciliationPlan) !void {
        for (plan.placement_upserts) |intent| try self.upsertReplicaIntent(intent);
        for (plan.table_upserts) |record| try self.upsertTable(record);
        for (plan.range_upserts) |record| try self.upsertRange(record);
        for (plan.split_upserts) |record| try self.upsertSplitTransition(record);
        for (plan.merge_upserts) |record| try self.upsertMergeTransition(record);
        for (plan.placement_removals) |record| try self.removeReplicaIntent(record.group_id, record.local_node_id);
        for (plan.table_removals) |table_id| try self.removeTable(table_id);
        for (plan.range_removals) |group_id| try self.removeRange(group_id);
        for (plan.split_removals) |transition_id| try self.removeSplitTransition(transition_id);
        for (plan.merge_removals) |transition_id| try self.removeMergeTransition(transition_id);
        if (plan.clear_reallocation_request) try self.clearReallocationRequest();
    }

    pub fn observeSplitTransition(self: *MetadataService, transition_id: u64) !?transition_state.SplitObservation {
        return try self.raft.observeSplitTransition(transition_id);
    }

    pub fn observeMergeTransition(self: *MetadataService, transition_id: u64) !?transition_state.MergeObservation {
        return try self.raft.observeMergeTransition(transition_id);
    }

    pub fn syncPending(self: *MetadataService) !raft_managed_host.ManagedSyncResult {
        return try self.raft.syncPending();
    }

    pub fn metrics(self: *MetadataService) raft_service.ManagedServiceMetrics {
        return self.raft.metrics;
    }

    pub fn head(self: *MetadataService) metadata_api.MetadataHead {
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_epoch = projectedProvisioningFingerprint(self.alloc, self) catch self.lifecycle_signal.currentEpoch(),
        };
    }

    pub fn status(self: *MetadataService) !MetadataStatus {
        var current_status = try snapshotStatus(self.alloc, self.metadata_group_id, self, self.metrics());
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        return current_status;
    }

    pub fn adminSnapshot(self: *MetadataService) !metadata_api.AdminSnapshot {
        return try metadata_api.captureSnapshot(self.alloc, self);
    }

    pub fn freeAdminSnapshot(self: *MetadataService, snapshot: *metadata_api.AdminSnapshot) void {
        metadata_api.freeSnapshot(self.alloc, self, snapshot);
    }

    pub fn projectedStore(self: *MetadataService) ?*metadata_storage.RaftApplyStore {
        return self.raft.host.owned_metadata_store;
    }

    pub fn getProjectedReconcileLease(self: *MetadataService) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReconcileLease(self.metadata_group_id);
    }

    pub fn getProjectedShuffleJoinLease(self: *MetadataService, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getShuffleJoinLease(self.metadata_group_id, job_id);
    }

    pub fn getProjectedReallocationRequest(self: *MetadataService) !?metadata_mod.ReallocationRequestRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReallocationRequest(self.metadata_group_id);
    }

    pub fn reconcileLeaseStats(self: *MetadataService) metadata_reconcile_lease.Stats {
        return self.reconcile_lease.stats();
    }

    pub fn listProjectedTables(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listTables(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedTables(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeTables(alloc, records);
    }

    pub fn listProjectedSchemaProgress(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.SchemaProgressRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listSchemaProgress(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedSchemaProgress(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.SchemaProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSchemaProgress(alloc, records);
    }

    pub fn listProjectedRestoreProgress(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listRestoreProgress(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedRestoreProgress(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRestoreProgress(alloc, records);
    }

    pub fn listProjectedReplicationSourceStatuses(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listReplicationSourceStatuses(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedReplicationSourceStatuses(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeReplicationSourceStatuses(alloc, records);
    }

    pub fn listProjectedShuffleJoinLeases(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listShuffleJoinLeases(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedShuffleJoinLeases(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.ShuffleJoinLeaseRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeShuffleJoinLeases(alloc, records);
    }

    pub fn listLocalBootstrapStatuses(self: *MetadataService, alloc: std.mem.Allocator) ![]raft_host.BootstrapStatus {
        return try self.raft.host.host.listBootstrapStatuses(alloc);
    }

    pub fn freeLocalBootstrapStatuses(self: *MetadataService, alloc: std.mem.Allocator, statuses: []raft_host.BootstrapStatus) void {
        self.raft.host.host.freeBootstrapStatuses(alloc, statuses);
    }

    pub fn listProjectedRanges(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listRanges(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedRanges(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRanges(alloc, records);
    }

    pub fn listProjectedPlacementIntents(self: *MetadataService, alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listPlacementIntents(alloc, self.metadata_group_id);
    }

    pub fn listProjectedNodes(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.NodeRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listNodes(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedNodes(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.NodeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeNodes(alloc, records);
    }

    pub fn listProjectedStores(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listStores(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedStores(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeStores(alloc, records);
    }

    pub fn freeProjectedPlacementIntents(self: *MetadataService, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        const store = self.projectedStore() orelse return;
        store.freePlacementIntents(alloc, intents);
    }

    pub fn listProjectedSplitTransitions(self: *MetadataService, alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listSplitTransitions(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedSplitTransitions(self: *MetadataService, alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSplitTransitions(alloc, records);
    }

    pub fn listProjectedMergeTransitions(self: *MetadataService, alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listMergeTransitions(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedMergeTransitions(self: *MetadataService, alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeMergeTransitions(alloc, records);
    }

    fn refreshLocalPlacementIntents(self: *MetadataService) !void {
        const current_epoch = self.placement_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_placement_epoch,
            self.last_local_placement_refresh_at_ms,
            current_epoch,
            local_placement_refresh_interval_ms,
        )) return;

        const projected = try self.listProjectedPlacementIntents(self.alloc);
        defer self.freeProjectedPlacementIntents(self.alloc, projected);

        var local = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        defer {
            for (local.items) |intent| if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
            local.deinit(self.alloc);
        }

        for (projected) |intent| {
            if (intent.record.local_node_id != self.raft.host.host.cfg.local_node_id) continue;
            try local.append(self.alloc, .{
                .record = intent.record,
                .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
            });
        }

        if (!containsLocalIntent(local.items, self.metadata_group_id)) {
            if (self.raft.host.host.raftStatus(self.metadata_group_id)) |raft_status| {
                try local.append(self.alloc, .{
                    .record = .{
                        .group_id = self.metadata_group_id,
                        .replica_id = self.raft.host.host.cfg.local_node_id,
                        .local_node_id = self.raft.host.host.cfg.local_node_id,
                        .bootstrap_mode = .persisted,
                    },
                    .peer_node_ids = try allocPeerNodeIdsExcludingSelf(
                        self.alloc,
                        raft_status.conf_state.voters,
                        self.raft.host.host.cfg.local_node_id,
                    ),
                });
            }
        }

        try self.raft.host.replacePlacementIntents(local.items);
        _ = try self.raft.host.reconcileOnce();
        self.local_placement_epoch = current_epoch;
        self.last_local_placement_refresh_at_ms = nowMs();
    }

    fn refreshLocalTransitions(self: *MetadataService) !void {
        const transition_svc = if (self.raft.transition_svc) |*svc| svc else return;
        const current_epoch = self.transition_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_transition_epoch,
            self.last_local_transition_refresh_at_ms,
            current_epoch,
            local_transition_refresh_interval_ms,
        )) return;

        const split_records = try self.listProjectedSplitTransitions(self.alloc);
        defer self.freeProjectedSplitTransitions(self.alloc, split_records);
        const merge_records = try self.listProjectedMergeTransitions(self.alloc);
        defer self.freeProjectedMergeTransitions(self.alloc, merge_records);

        var split_index: usize = 0;
        while (split_index < transition_svc.pending_split.items.len) {
            const transition_id = transition_svc.pending_split.items[split_index].transition_id;
            if (findProjectedSplit(split_records, transition_id) == null) {
                _ = transition_svc.removeSplit(transition_id);
                continue;
            }
            split_index += 1;
        }
        for (split_records) |record| {
            if (findQueuedSplit(transition_svc.pending_split.items, record.transition_id) == null and
                !transition_svc.hasCompletedSplit(record.transition_id))
            {
                try transition_svc.submitSplit(record);
            }
        }

        var merge_index: usize = 0;
        while (merge_index < transition_svc.pending_merge.items.len) {
            const transition_id = transition_svc.pending_merge.items[merge_index].transition_id;
            if (findProjectedMerge(merge_records, transition_id) == null) {
                _ = transition_svc.removeMerge(transition_id);
                continue;
            }
            merge_index += 1;
        }
        for (merge_records) |record| {
            if (findQueuedMerge(transition_svc.pending_merge.items, record.transition_id) == null and
                !transition_svc.hasCompletedMerge(record.transition_id))
            {
                try transition_svc.submitMerge(record);
            }
        }

        self.raft.metrics.queued_split_transitions = transition_svc.metrics.queued_split_transitions;
        self.raft.metrics.queued_merge_transitions = transition_svc.metrics.queued_merge_transitions;
        self.local_transition_epoch = current_epoch;
        self.last_local_transition_refresh_at_ms = nowMs();
    }

    fn refreshLocalTableProvisioning(self: *MetadataService) !metadata_table_provisioner.ProvisionSummary {
        const replica_root_dir = self.replica_root_dir orelse return .{};
        const current_epoch = self.projection_epoch.load(.monotonic);
        const group_ids = try self.raft.host.host.listGroupIds(self.alloc);
        defer self.alloc.free(group_ids);
        const group_ids_fingerprint = groupIdsFingerprint(group_ids);
        if (!shouldRefreshLocalProjection(
            self.local_table_provisioning_epoch,
            self.local_table_provisioning_group_ids_fingerprint,
            self.last_local_table_provisioning_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_table_provisioning_refresh_interval_ms,
        )) return .{};

        const tables = try self.listProjectedTables(self.alloc);
        defer self.freeProjectedTables(self.alloc, tables);
        const ranges = try self.listProjectedRanges(self.alloc);
        defer self.freeProjectedRanges(self.alloc, ranges);
        const fingerprint = metadata_table_provisioner.provisioningFingerprint(
            self.metadata_group_id,
            group_ids,
            tables,
            ranges,
        );
        self.local_table_provisioning_epoch = current_epoch;
        self.local_table_provisioning_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_table_provisioning_refresh_at_ms = nowMs();
        if (self.local_table_provisioning_fingerprint == fingerprint) {
            try self.refreshLocalRestoreProgress(group_ids, tables, ranges);
            return .{};
        }
        if (self.local_replica_root_reconcile_permit_hook) |hook| {
            if (!hook.shouldReconcile()) return .{};
        }
        const summary = try metadata_table_provisioner.reconcileReplicaRootWithOptions(
            self.alloc,
            replica_root_dir,
            self.metadata_group_id,
            group_ids,
            tables,
            ranges,
            .{
                .backend_runtime = try self.ensureBackendRuntime(),
            },
        );
        if (self.local_replica_root_reconcile_hook) |hook| try hook.run();
        self.local_table_provisioning_fingerprint = fingerprint;
        try self.refreshLocalRestoreProgress(group_ids, tables, ranges);
        self.local_schema_progress_epoch = null;
        self.local_schema_progress_group_ids_fingerprint = null;
        self.last_local_schema_progress_refresh_at_ms = 0;
        try self.refreshLocalSchemaProgress();
        return summary;
    }

    fn refreshLocalRestoreProgress(
        self: *MetadataService,
        group_ids: []const u64,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        const local_progress = try metadata_table_provisioner.collectLocalRestoreProgress(
            self.alloc,
            replica_root_dir,
            self.metadata_group_id,
            local_node_id,
            group_ids,
            tables,
            ranges,
        );
        defer {
            for (local_progress) |record| metadata_table_manager.freeRestoreProgress(self.alloc, record);
            self.alloc.free(local_progress);
        }
        const projected_progress = try self.listProjectedRestoreProgress(self.alloc);
        defer self.freeProjectedRestoreProgress(self.alloc, projected_progress);
        try syncLocalRestoreProgress(self, local_node_id, local_progress, projected_progress);
    }

    fn refreshLocalSchemaProgress(self: *MetadataService) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        const current_epoch = self.projection_epoch.load(.monotonic);
        const group_ids = try self.raft.host.host.listGroupIds(self.alloc);
        defer self.alloc.free(group_ids);
        const group_ids_fingerprint = groupIdsFingerprint(group_ids);
        if (!shouldRefreshLocalProjection(
            self.local_schema_progress_epoch,
            self.local_schema_progress_group_ids_fingerprint,
            self.last_local_schema_progress_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_schema_progress_refresh_interval_ms,
        )) return;

        const tables = try self.listProjectedTables(self.alloc);
        defer self.freeProjectedTables(self.alloc, tables);
        const ranges = try self.listProjectedRanges(self.alloc);
        defer self.freeProjectedRanges(self.alloc, ranges);
        const backend_runtime = try self.ensureBackendRuntime();
        var fallback_shard_db = metadata_mod.FallbackLocalShardDbAdapter{
            .replica_root_dir = replica_root_dir,
            .backend_runtime = backend_runtime,
        };
        const shard_db = self.local_shard_db_adapter orelse fallback_shard_db.adapter();
        const local_progress = try metadata_table_provisioner.collectLocalSchemaProgressWithOptions(
            self.alloc,
            replica_root_dir,
            self.metadata_group_id,
            local_node_id,
            group_ids,
            tables,
            ranges,
            .{
                .backend_runtime = backend_runtime,
                .shard_db_adapter = shard_db,
            },
        );
        defer self.alloc.free(local_progress);
        const projected_progress = try self.listProjectedSchemaProgress(self.alloc);
        defer self.freeProjectedSchemaProgress(self.alloc, projected_progress);
        try syncLocalSchemaProgress(self, local_node_id, local_progress, projected_progress);
        self.local_schema_progress_epoch = current_epoch;
        self.local_schema_progress_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_schema_progress_refresh_at_ms = nowMs();
    }

    fn refreshLocalStoreStatus(self: *MetadataService) !void {
        try self.refreshLocalStoreStatusWithBackfillMarkers(null, true);
    }

    fn refreshLocalStoreStatusWithBackfillMarkers(
        self: *MetadataService,
        backfill_markers: ?[]const StoreStatusBackfillMarker,
        use_provider: bool,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        try syncLocalStoreStatus(self, local_node_id, replica_root_dir, backfill_markers, use_provider);
    }

    fn refreshStoreStatusBackfillMarkersForRound(self: *MetadataService) ![]const StoreStatusBackfillMarker {
        self.store_status_ticks += 1;
        self.store_status_backfill_probe_ticks += 1;
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            self.store_status_ticks,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn refreshStoreStatusBackfillMarkersForLifecycleRound(self: *MetadataService) ![]const StoreStatusBackfillMarker {
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        if (self.store_status_backfill_marker_cache.markers.len == 0 and self.store_status_backfill_marker_cache.scanned_at_ms == 0) {
            try refreshStoreStatusBackfillMarkerCacheNow(
                self.alloc,
                replica_root_dir,
                &self.store_status_backfill_probe_ticks,
                &self.store_status_backfill_marker_cache,
            );
            return self.store_status_backfill_marker_cache.markers;
        }

        self.store_status_backfill_probe_ticks += 1;
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            0,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn completeRestoreIntentsIfReady(self: *MetadataService) !void {
        try completeRestoreIntentsForService(self, null, null, null, null);
    }

    fn ensureReconcileLease(self: *MetadataService) !bool {
        const now_ms = self.reconcile_lease.nowMs();
        const is_local_leader = self.raft.host.host.isLocalLeader(self.metadata_group_id);
        const projected = self.getCachedProjectedReconcileLease(now_ms, is_local_leader) catch |err| switch (err) {
            error.MissingMetadataStore => null,
            else => return err,
        };
        const has_lease = self.reconcile_lease.observe(is_local_leader, projected, now_ms);
        if (self.reconcile_lease.shouldRenew(is_local_leader, projected, now_ms)) {
            self.upsertReconcileLease(self.reconcile_lease.desiredRecord(now_ms)) catch |err| {
                self.reconcile_lease.noteAcquireFailure();
                return err;
            };
        }
        return has_lease;
    }

    fn getCachedProjectedReconcileLease(
        self: *MetadataService,
        now_ms: u64,
        is_local_leader: bool,
    ) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        const current_epoch = self.reconcile_lease_epoch.load(.monotonic);
        if (self.reconcile_lease_projection_cache.epoch == current_epoch and
            now_ms < self.reconcile_lease_projection_cache.next_refresh_at_ms)
        {
            return self.reconcile_lease_projection_cache.record;
        }
        const projected = try self.getProjectedReconcileLease();
        self.reconcile_lease_projection_cache = .{
            .epoch = current_epoch,
            .record = projected,
            .next_refresh_at_ms = reconcileLeaseCacheNextRefreshAtMs(&self.reconcile_lease, is_local_leader, projected, now_ms),
        };
        return projected;
    }

    fn statusProjectedReconcileLease(self: *MetadataService, now_ms: u64) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        return try self.getCachedProjectedReconcileLease(now_ms, self.raft.host.host.isLocalLeader(self.metadata_group_id));
    }

    fn runLifecycleReconcileHookIfRequested(self: *MetadataService) !void {
        const hook = self.lifecycle_reconcile_hook orelse return;
        if (!self.lifecycle_reconcile_requested.swap(false, .acq_rel)) return;
        try hook.run();
    }

    fn runReplicationBackfillRound(self: *MetadataService) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        if (!self.raft.host.host.isLocalLeader(self.metadata_group_id)) return;
        while (!self.cdc_runtime_mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
        defer self.cdc_runtime_mutex.unlock();
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        if (now_ms < self.cdc_next_round_at_ms) return;
        self.cdc_next_round_at_ms = now_ms + cdc_replication_round_interval_ms;

        var write_source = api_table_writes.ProvisionedTableWriteSource.init(
            replica_root_dir,
            api_table_catalog.CatalogSource.fromMetadataService(self),
        );
        write_source.backend_runtime = try self.ensureBackendRuntime();
        _ = write_source.withSecretStore(self.secret_store);
        var coordinator = metadata_replication_backfill.SnapshotBackfillCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source.source(),
                .secret_store = self.secret_store,
            },
        };
        const summary = coordinator.runRound(self) catch |err| switch (err) {
            error.UnknownReplicationSource,
            error.UnsupportedReplicationSource,
            error.UnsupportedReplicationStreaming,
            error.UnsupportedReplicationTransform,
            error.UnsupportedReplicationRoute,
            error.ReplicationExactCutoverRequired,
            error.InvalidReplicationSourceConfig,
            error.InvalidReplicationSourceRow,
            error.ForeignAuthFailed,
            error.ForeignConnectionFailed,
            error.ForeignQueryFailed,
            error.ForeignReplicationSlotMissing,
            error.ForeignTableNotFound,
            error.FileNotFound,
            error.InvalidQueryRequest,
            error.WriterLocked,
            error.LmdbUnexpected,
            error.Corrupted,
            error.UnknownColumn,
            => {
                std.log.warn("metadata cdc snapshot round skipped: {s}", .{@errorName(err)});
                return;
            },
            else => return err,
        };
        if (summary.sources_considered > 0) {
            std.log.info(
                "metadata cdc snapshot round tables={d} sources={d} started={d} resumed={d} completed={d}",
                .{
                    summary.tables_considered,
                    summary.sources_considered,
                    summary.sources_started,
                    summary.sources_resumed,
                    summary.sources_completed,
                },
            );
        }
        var streaming = metadata_replication_backfill.StreamingReplicationCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source.source(),
                .secret_store = self.secret_store,
            },
        };
        const stream_summary = streaming.runRound(self) catch |err| switch (err) {
            error.UnknownReplicationSource,
            error.UnsupportedReplicationSource,
            error.UnsupportedReplicationStreaming,
            error.UnsupportedReplicationTransform,
            error.UnsupportedReplicationRoute,
            error.ReplicationExactCutoverRequired,
            error.InvalidReplicationSourceConfig,
            error.InvalidReplicationSourceRow,
            error.ForeignAuthFailed,
            error.ForeignConnectionFailed,
            error.ForeignQueryFailed,
            error.ForeignReplicationSlotMissing,
            error.ForeignTableNotFound,
            error.FileNotFound,
            error.InvalidQueryRequest,
            error.WriterLocked,
            error.LmdbUnexpected,
            error.Corrupted,
            error.UnknownColumn,
            => {
                std.log.warn("metadata cdc streaming round skipped: {s}", .{@errorName(err)});
                return;
            },
            else => return err,
        };
        if (stream_summary.sources_considered > 0) {
            std.log.info(
                "metadata cdc streaming round tables={d} sources={d} started={d} resumed={d} skipped={d} polled={d} changes={d}",
                .{
                    stream_summary.tables_considered,
                    stream_summary.sources_considered,
                    stream_summary.sources_started,
                    stream_summary.sources_resumed,
                    stream_summary.sources_skipped_pending_snapshot,
                    stream_summary.sources_polled,
                    stream_summary.changes_applied,
                },
            );
        }
    }
};

pub const MetadataHttpService = struct {
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    replica_root_dir: ?[]const u8,
    observe_local_replica_root: bool,
    store_status_ticks: usize,
    projection_epoch: std.atomic.Value(u64) = .init(1),
    placement_epoch: std.atomic.Value(u64) = .init(1),
    reconcile_lease_epoch: std.atomic.Value(u64) = .init(1),
    transition_epoch: std.atomic.Value(u64) = .init(1),
    local_placement_epoch: ?u64,
    last_local_placement_refresh_at_ms: u64,
    local_transition_epoch: ?u64,
    last_local_transition_refresh_at_ms: u64,
    local_table_provisioning_fingerprint: ?u64,
    local_table_provisioning_epoch: ?u64,
    local_table_provisioning_group_ids_fingerprint: ?u64,
    last_local_table_provisioning_refresh_at_ms: u64,
    local_schema_progress_epoch: ?u64,
    local_schema_progress_group_ids_fingerprint: ?u64,
    last_local_schema_progress_refresh_at_ms: u64,
    cdc_runtime_mutex: std.atomic.Mutex = .unlocked,
    reconcile_lease: metadata_reconcile_lease.State,
    runtime_mutex: std.atomic.Mutex = .unlocked,
    lifecycle_signal: LifecycleSignal,
    lifecycle_reconcile_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    lifecycle_reconcile_hook: ?LifecycleReconcileHook = null,
    local_replica_root_reconcile_hook: ?LocalReplicaRootReconcileHook = null,
    local_replica_root_reconcile_permit_hook: ?LocalReplicaRootReconcilePermitHook = null,
    lifecycle_listener_registered: bool = false,
    cdc_write_source_override: ?api_table_writes.TableWriteSource = null,
    local_group_status_provider: ?LocalGroupStatusProvider = null,
    local_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    routed_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    reconcile_lease_projection_cache: ReconcileLeaseProjectionCache = .{},
    projected_core_snapshot_cache: ProjectedCoreSnapshotCache = .{},
    metadata_status_cache_mutex: std.atomic.Mutex = .unlocked,
    metadata_status_cache_valid: bool = false,
    metadata_status_cache: MetadataStatus = .{ .metadata_group_id = 0, .metrics = .{} },
    metadata_status_cache_next_refresh_at_ms: u64 = 0,
    metadata_status_cache_projection_epoch: u64 = 0,
    metadata_status_cache_placement_epoch: u64 = 0,
    metadata_status_cache_transition_epoch: u64 = 0,
    store_status_backfill_probe_ticks: usize = 0,
    store_status_backfill_marker_cache: StoreStatusBackfillMarkerCache = .{},
    cdc_backfill_registry: foreign_mod.Registry = .{},
    cdc_next_round_at_ms: u64 = 0,
    secret_store: ?*common_secrets.FileStore = null,
    backend_runtime_mutex: std.atomic.Mutex = .unlocked,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    owned_backend_runtime: ?backend_runtime_mod.BackendRuntimeHandle = null,
    metadata_orchestration_urls: []MetadataOrchestrationUrl = &.{},
    raft: raft_service.ManagedHttpHostService,

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: raft_managed_host.ManagedHttpHostConfig,
        deps: MetadataHttpServiceDeps,
        cfg: MetadataServiceConfig,
    ) !MetadataHttpService {
        const metadata_group_id = host_cfg.http.host.metadata_group_id orelse return error.MissingMetadataGroupId;
        var service = MetadataHttpService{
            .alloc = alloc,
            .metadata_group_id = metadata_group_id,
            .replica_root_dir = host_cfg.http.host.replica_root_dir,
            .observe_local_replica_root = cfg.observe_local_replica_root,
            .store_status_ticks = 0,
            .local_placement_epoch = null,
            .last_local_placement_refresh_at_ms = 0,
            .local_transition_epoch = null,
            .last_local_transition_refresh_at_ms = 0,
            .local_table_provisioning_fingerprint = null,
            .local_table_provisioning_epoch = null,
            .local_table_provisioning_group_ids_fingerprint = null,
            .last_local_table_provisioning_refresh_at_ms = 0,
            .local_schema_progress_epoch = null,
            .local_schema_progress_group_ids_fingerprint = null,
            .last_local_schema_progress_refresh_at_ms = 0,
            .reconcile_lease = metadata_reconcile_lease.State.init(host_cfg.http.host.local_node_id, cfg.reconcile_lease),
            .lifecycle_signal = LifecycleSignal.init(alloc),
            .backend_runtime = cfg.backend_runtime,
            .secret_store = cfg.secret_store,
            .metadata_orchestration_urls = try cloneMetadataOrchestrationUrls(alloc, cfg.metadata_orchestration_urls),
            .raft = try raft_service.ManagedHttpHostService.init(alloc, host_cfg, deps.http, cfg.raft, deps.raft),
        };
        errdefer service.deinit();
        try foreign_mod.registerDefaultPostgresExecutor(alloc, &service.cdc_backfill_registry);
        return service;
    }

    pub fn deinit(self: *MetadataHttpService) void {
        self.projected_core_snapshot_cache.deinit(self.alloc);
        self.store_status_backfill_marker_cache.deinit(self.alloc);
        self.cdc_backfill_registry.deinit(self.alloc);
        self.lifecycle_signal.deinit();
        freeMetadataOrchestrationUrls(self.alloc, self.metadata_orchestration_urls);
        self.raft.deinit();
        if (self.replica_root_dir) |replica_root_dir| {
            api_table_writes.closeHostedManagedDbCacheForRoot(replica_root_dir);
        }
        if (self.owned_backend_runtime) |*runtime| runtime.deinit();
        self.owned_backend_runtime = null;
        self.backend_runtime = null;
        self.* = undefined;
    }

    pub fn ensureBackendRuntime(self: *MetadataHttpService) !*backend_runtime_mod.BackendRuntime {
        while (!self.backend_runtime_mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
        defer self.backend_runtime_mutex.unlock();
        if (self.backend_runtime == null) {
            self.owned_backend_runtime = try backend_runtime_mod.BackendRuntimeHandle.init(self.alloc, .{});
            self.backend_runtime = self.owned_backend_runtime.?.ptr();
        }
        if (self.backend_runtime) |runtime| return runtime;
        unreachable;
    }

    pub fn lifecycleSignalCurrent(self: *const MetadataHttpService) u32 {
        return self.lifecycle_signal.current();
    }

    pub fn captureLifecycleSignal(self: *MetadataHttpService, table_name: ?[]const u8) LifecycleSignal.Snapshot {
        return self.lifecycle_signal.snapshot(table_name);
    }

    pub fn waitForLifecycleSignal(self: *MetadataHttpService, observed: LifecycleSignal.Snapshot, timeout_ns: u64) void {
        self.lifecycle_signal.wait(observed, timeout_ns);
    }

    pub fn setLifecycleReconcileHook(self: *MetadataHttpService, hook: ?LifecycleReconcileHook) void {
        self.lifecycle_reconcile_hook = hook;
        self.lifecycle_reconcile_requested.store(true, .release);
    }

    pub fn setCdcWriteSource(self: *MetadataHttpService, source: ?api_table_writes.TableWriteSource) void {
        self.cdc_write_source_override = source;
    }

    pub fn setLocalGroupStatusProvider(self: *MetadataHttpService, provider: ?LocalGroupStatusProvider) void {
        self.local_group_status_provider = provider;
    }

    pub fn setLocalShardDbAdapter(self: *MetadataHttpService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.local_shard_db_adapter = adapter;
    }

    pub fn setRoutedShardDbAdapter(self: *MetadataHttpService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.routed_shard_db_adapter = adapter;
    }

    pub fn setLocalReplicaRootReconcileHook(self: *MetadataHttpService, hook: ?LocalReplicaRootReconcileHook) void {
        self.local_replica_root_reconcile_hook = hook;
    }

    pub fn setLocalReplicaRootReconcilePermitHook(self: *MetadataHttpService, hook: ?LocalReplicaRootReconcilePermitHook) void {
        self.local_replica_root_reconcile_permit_hook = hook;
    }

    fn ensureLifecycleListenerRegistered(self: *MetadataHttpService) !void {
        if (self.lifecycle_listener_registered) return;
        const store = self.projectedStore() orelse return;
        try store.addProjectionListener(.{
            .ptr = self,
            .vtable = &.{
                .on_projection_signal = metadataHttpServiceProjectionSignal,
            },
        });
        try store.addCommittedKeyListener(.{
            .ptr = self,
            .vtable = &.{
                .matches_key = metadataHttpServiceLifecycleKeyMatches,
                .on_committed_key = metadataHttpServiceCommittedKeySignal,
            },
        });
        self.lifecycle_listener_registered = true;
    }

    fn metadataHttpServiceProjectionSignal(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.ProjectionSignal) void {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        switch (signal.kind) {
            .table, .range, .store, .shuffle_join_lease => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .schema_progress => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .restore_progress, .replication_source_status => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .placement_intent => _ = self.placement_epoch.fetchAdd(1, .monotonic),
            .reconcile_lease => _ = self.reconcile_lease_epoch.fetchAdd(1, .monotonic),
            .split_transition, .merge_transition => _ = self.transition_epoch.fetchAdd(1, .monotonic),
        }
        self.lifecycle_signal.notify(signal.table_name);
    }

    fn metadataHttpServiceLifecycleKeyMatches(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.CommittedKeySignal) bool {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        return lifecycleKeyMatchesMetadataNamespace(self.metadata_group_id, signal);
    }

    fn metadataHttpServiceCommittedKeySignal(ptr: *anyopaque, _: metadata_storage.raft_apply_store.CommittedKeySignal) void {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        self.lifecycle_reconcile_requested.store(true, .release);
        self.lifecycle_signal.notify(null);
    }

    pub fn medianKeyLookup(self: *MetadataHttpService) ?metadata_reconciler.MedianKeyLookup {
        if (self.routed_shard_db_adapter == null and self.local_shard_db_adapter == null and self.replica_root_dir == null) return null;
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
            },
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        if (self.routed_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        if (self.local_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        const replica_root_dir = self.replica_root_dir orelse return error.UnsupportedOperation;
        var fallback = metadata_mod.FallbackLocalShardDbAdapter{
            .replica_root_dir = replica_root_dir,
            .backend_runtime = try self.ensureBackendRuntime(),
        };
        return try fallback.adapter().fetchMedianKey(alloc, group_id);
    }

    pub fn start(self: *MetadataHttpService) !void {
        try self.raft.start();
    }

    pub fn stop(self: *MetadataHttpService) void {
        self.raft.stop();
    }

    pub fn baseUri(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]u8 {
        return try self.raft.baseUri(alloc);
    }

    pub fn ensureMetadataReplica(self: *MetadataHttpService, record: raft_catalog.ReplicaRecord) !raft_engine.runtime.EnsureReplicaResult {
        if (record.group_id != self.metadata_group_id) return error.InvalidMetadataGroupId;
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.host.http_host.ensureReplica(record);
    }

    pub fn campaignMetadataGroup(self: *MetadataHttpService) !void {
        self.lockRuntime();
        defer self.unlockRuntime();
        try self.raft.host.http_host.campaignGroup(self.metadata_group_id);
    }

    pub fn forwardMetadataLeaderRequest(
        self: *MetadataHttpService,
        alloc: std.mem.Allocator,
        req: http_common.HttpRequest,
    ) !?http_common.HttpResponse {
        self.lockRuntime();
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        const leader_id = self.raft.host.http_host.leaderId(self.metadata_group_id);
        self.unlockRuntime();
        const target_node_id = leader_id orelse return null;
        if (target_node_id == local_node_id) return null;
        const base_uri = self.metadataOrchestrationUrlForNode(target_node_id) orelse return null;
        const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_uri, req.uri });
        defer alloc.free(uri);
        return try self.raft.host.http_host.request_executor.execute(alloc, .{
            .method = req.method,
            .uri = uri,
            .headers = req.headers,
            .source_node_id = local_node_id,
            .authorization = req.authorization,
            .content_type = req.content_type,
            .body = req.body,
        });
    }

    fn metadataOrchestrationUrlForNode(self: *const MetadataHttpService, node_id: u64) ?[]const u8 {
        for (self.metadata_orchestration_urls) |entry| {
            if (entry.node_id == node_id) return entry.url;
        }
        return null;
    }

    pub fn proposeTransitionCommand(self: *MetadataHttpService, command: metadata_storage.TransitionCommand) !void {
        self.lockRuntime();
        defer self.unlockRuntime();
        try metadata_storage.validateTransitionCommandDataGroupIds(command);
        const encoded = try metadata_storage.encodeTransitionCommand(self.alloc, command);
        defer self.alloc.free(encoded);
        try self.raft.host.http_host.propose(self.metadata_group_id, encoded);
        self.lifecycle_signal.notify(null);
    }

    pub fn upsertNode(self: *MetadataHttpService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_node = record });
    }

    pub fn registerNode(self: *MetadataHttpService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .register_node = record });
    }

    pub fn requestNodeShutdown(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .request_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn cancelNodeShutdown(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .cancel_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn finalizeNodeShutdown(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .finalize_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn removeNode(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_node = .{ .node_id = node_id } });
    }

    pub fn upsertStore(self: *MetadataHttpService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_store = record });
    }

    pub fn registerStore(self: *MetadataHttpService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .register_store = record });
    }

    pub fn reportStoreStatus(self: *MetadataHttpService, report: metadata_table_manager.StoreStatusReport) !void {
        _ = try self.reportStoreStatuses(&.{report});
    }

    pub fn reportStoreStatuses(self: *MetadataHttpService, reports: []const metadata_table_manager.StoreStatusReport) !usize {
        self.lockRuntime();
        var runtime_locked = true;
        errdefer if (runtime_locked) self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        const projected = try cloneProjectedStoresOwned(self.alloc, snapshot.stores);
        self.unlockRuntime();
        runtime_locked = false;
        defer self.freeProjectedStores(self.alloc, projected);

        return try reportStoreStatusesWithProjected(self, projected, reports);
    }

    pub fn removeStore(self: *MetadataHttpService, store_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_store = .{ .store_id = store_id } });
    }

    pub fn upsertSplitTransition(self: *MetadataHttpService, record: transition_state.SplitTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_split_transition = record });
    }

    pub fn upsertReplicaIntent(self: *MetadataHttpService, intent: raft_reconciler.PlacementIntent) !void {
        try self.proposeTransitionCommand(.{ .upsert_replica_intent = intent });
    }

    pub fn removeReplicaIntent(self: *MetadataHttpService, group_id: u64, local_node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_replica_intent = .{
            .group_id = group_id,
            .local_node_id = local_node_id,
        } });
    }

    pub fn upsertTable(self: *MetadataHttpService, record: metadata_table_manager.TableRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_table = record });
    }

    pub fn removeTable(self: *MetadataHttpService, table_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_table = .{ .table_id = table_id } });
    }

    pub fn upsertSchemaProgress(self: *MetadataHttpService, record: metadata_table_manager.SchemaProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_schema_progress = record });
    }

    pub fn removeSchemaProgress(self: *MetadataHttpService, table_id: u64, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_schema_progress = .{
            .table_id = table_id,
            .node_id = node_id,
        } });
    }

    pub fn upsertRestoreProgress(self: *MetadataHttpService, record: metadata_table_manager.RestoreProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_restore_progress = record });
    }

    pub fn removeRestoreProgress(self: *MetadataHttpService, table_id: u64, node_id: u64, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_restore_progress = .{
            .table_id = table_id,
            .node_id = node_id,
            .group_id = group_id,
        } });
    }

    pub fn upsertReplicationSourceStatus(self: *MetadataHttpService, record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_replication_source_status = record });
    }

    pub fn removeReplicationSourceStatus(self: *MetadataHttpService, table_id: u64, source_ordinal: u32) !void {
        try self.proposeTransitionCommand(.{ .remove_replication_source_status = .{
            .table_id = table_id,
            .source_ordinal = source_ordinal,
        } });
    }

    pub fn upsertRange(self: *MetadataHttpService, record: metadata_table_manager.RangeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_range = record });
    }

    pub fn removeRange(self: *MetadataHttpService, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_range = .{ .group_id = group_id } });
    }

    pub fn removeSplitTransition(self: *MetadataHttpService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_split_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertMergeTransition(self: *MetadataHttpService, record: transition_state.MergeTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_merge_transition = record });
    }

    pub fn removeMergeTransition(self: *MetadataHttpService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_merge_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertReconcileLease(self: *MetadataHttpService, record: metadata_reconcile_lease.ReconcileLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_reconcile_lease = record });
    }

    pub fn removeReconcileLease(self: *MetadataHttpService) !void {
        try self.proposeTransitionCommand(.{ .remove_reconcile_lease = .{} });
    }

    pub fn upsertShuffleJoinLease(self: *MetadataHttpService, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_shuffle_join_lease = record });
    }

    pub fn removeShuffleJoinLease(self: *MetadataHttpService, job_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_shuffle_join_lease = .{ .job_id = job_id } });
    }

    pub fn requestReallocation(self: *MetadataHttpService, requested_at_ms: u64) !void {
        try self.proposeTransitionCommand(.{ .upsert_reallocation_request = .{
            .requested_at_ms = requested_at_ms,
        } });
    }

    pub fn clearReallocationRequest(self: *MetadataHttpService) !void {
        try self.proposeTransitionCommand(.{ .remove_reallocation_request = .{} });
    }

    pub fn runRound(self: *MetadataHttpService) !void {
        try self.ensureLifecycleListenerRegistered();
        defer self.refreshMetadataStatusCacheIfDue();
        defer self.lifecycle_signal.notify(null);
        self.lockRuntime();
        {
            defer self.unlockRuntime();
            if (self.raft.pending_updates.items.len > 0) {
                _ = try self.raft.syncPendingRaftOnly();
            } else {
                try self.raft.runRaftRoundOnly();
            }
        }
        if (!self.observe_local_replica_root) return;

        var local_projection_inputs = try captureLocalProjectionInputs(self);
        defer freeLocalProjectionInputs(self, &local_projection_inputs);

        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForRound();
        if ((self.store_status_ticks >= 40 or backfill_markers.len > 0) and shouldRefreshLocalStoreStatus(self, backfill_markers)) {
            self.store_status_ticks = 0;
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, true) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
        }
        self.refreshLocalSchemaProgress(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return;
        var local_placement_inputs = try captureLocalPlacementInputs(self);
        defer freeLocalPlacementInputs(self, &local_placement_inputs);
        var local_transition_inputs = try captureLocalTransitionInputs(self);
        defer freeLocalTransitionInputs(self, &local_transition_inputs);
        try self.refreshLocalPlacementIntents(&local_placement_inputs);
        try self.refreshLocalTransitions(&local_transition_inputs);
        _ = self.refreshLocalTableProvisioning(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        try self.completeRestoreIntentsIfReady(&local_projection_inputs, &local_placement_inputs);
        try self.runReplicationBackfillRound();
        try self.runLifecycleReconcileHookIfRequested();
        _ = try self.raft.stepTransitions();
    }

    pub fn runLifecycleRound(self: *MetadataHttpService) !void {
        try self.ensureLifecycleListenerRegistered();
        defer self.refreshMetadataStatusCacheIfDue();
        defer self.lifecycle_signal.notify(null);
        self.lockRuntime();
        {
            defer self.unlockRuntime();
            if (self.raft.pending_updates.items.len > 0) {
                _ = try self.raft.syncPendingRaftOnly();
            } else {
                try self.raft.runRaftRoundOnly();
            }
        }
        if (!self.observe_local_replica_root) return;

        var local_projection_inputs = try captureLocalProjectionInputs(self);
        defer freeLocalProjectionInputs(self, &local_projection_inputs);

        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForLifecycleRound();
        if (shouldRefreshLocalStoreStatusForLifecycleRound(self, backfill_markers)) {
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, false) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
        }
        self.refreshLocalSchemaProgress(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return;
        var local_placement_inputs = try captureLocalPlacementInputs(self);
        defer freeLocalPlacementInputs(self, &local_placement_inputs);
        var local_transition_inputs = try captureLocalTransitionInputs(self);
        defer freeLocalTransitionInputs(self, &local_transition_inputs);
        try self.refreshLocalPlacementIntents(&local_placement_inputs);
        try self.refreshLocalTransitions(&local_transition_inputs);
        _ = self.refreshLocalTableProvisioning(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        try self.completeRestoreIntentsIfReady(&local_projection_inputs, &local_placement_inputs);
        try self.runReplicationBackfillRound();
        try self.runLifecycleReconcileHookIfRequested();
        _ = try self.raft.stepTransitions();
    }

    pub fn runCdcRound(self: *MetadataHttpService) !void {
        if (!self.observe_local_replica_root) return;
        _ = try runReplicationBackfillIfLeaseHeld(self);
    }

    pub fn waitForTableLifecycle(self: *MetadataHttpService, table_name: []const u8, expected: TableLifecycleExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableLifecycleConvergence(self, table_name, expected);
    }

    pub fn waitForTableProjection(self: *MetadataHttpService, table_name: []const u8, expected: TableProjectionExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableProjectionConvergence(self, table_name, expected);
    }

    pub fn reconcileOnceIfLeaseHeld(self: *MetadataHttpService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcileOnce(self);
    }

    pub fn reconcilePreparedIfLeaseHeld(self: *MetadataHttpService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcilePrepared(self);
    }

    pub fn reconcileOnceEnsuringLease(self: *MetadataHttpService, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            if (try self.reconcileOnceIfLeaseHeld(loop)) |summary| return summary;
            try self.runRound();
        }
        return error.ReconcileLeaseNotHeld;
    }

    pub fn applyReconciliationPlan(self: *MetadataHttpService, plan: *const metadata_reconciler.ReconciliationPlan) !void {
        for (plan.placement_upserts) |intent| try self.upsertReplicaIntent(intent);
        for (plan.table_upserts) |record| try self.upsertTable(record);
        for (plan.range_upserts) |record| try self.upsertRange(record);
        for (plan.split_upserts) |record| try self.upsertSplitTransition(record);
        for (plan.merge_upserts) |record| try self.upsertMergeTransition(record);
        for (plan.placement_removals) |record| try self.removeReplicaIntent(record.group_id, record.local_node_id);
        for (plan.table_removals) |table_id| try self.removeTable(table_id);
        for (plan.range_removals) |group_id| try self.removeRange(group_id);
        for (plan.split_removals) |transition_id| try self.removeSplitTransition(transition_id);
        for (plan.merge_removals) |transition_id| try self.removeMergeTransition(transition_id);
        if (plan.clear_reallocation_request) try self.clearReallocationRequest();
    }

    pub fn observeSplitTransition(self: *MetadataHttpService, transition_id: u64) !?transition_state.SplitObservation {
        return try self.raft.observeSplitTransition(transition_id);
    }

    pub fn observeMergeTransition(self: *MetadataHttpService, transition_id: u64) !?transition_state.MergeObservation {
        return try self.raft.observeMergeTransition(transition_id);
    }

    pub fn syncPending(self: *MetadataHttpService) !raft_managed_host.ManagedSyncResult {
        return try self.raft.syncPendingRaftOnly();
    }

    pub fn metrics(self: *MetadataHttpService) raft_service.ManagedServiceMetrics {
        return self.raft.metrics;
    }

    pub fn head(self: *MetadataHttpService) metadata_api.MetadataHead {
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_epoch = projectedProvisioningFingerprint(self.alloc, self) catch self.lifecycle_signal.currentEpoch(),
        };
    }

    fn fallbackStatus(self: *MetadataHttpService) MetadataStatus {
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_epoch = self.lifecycle_signal.currentEpoch(),
            .metrics = self.metrics(),
        };
    }

    fn loadMetadataStatusCache(self: *MetadataHttpService) ?MetadataStatus {
        self.lockMetadataStatusCache();
        defer self.metadata_status_cache_mutex.unlock();
        if (!self.metadata_status_cache_valid) return null;
        if (self.metadata_status_cache_projection_epoch != self.projection_epoch.load(.monotonic)) return null;
        if (self.metadata_status_cache_placement_epoch != self.placement_epoch.load(.monotonic)) return null;
        if (self.metadata_status_cache_transition_epoch != self.transition_epoch.load(.monotonic)) return null;
        return self.metadata_status_cache;
    }

    fn storeMetadataStatusCache(self: *MetadataHttpService, next_status: MetadataStatus, next_refresh_at_ms: u64) void {
        const projection_epoch = self.projection_epoch.load(.monotonic);
        const placement_epoch = self.placement_epoch.load(.monotonic);
        const transition_epoch = self.transition_epoch.load(.monotonic);
        self.lockMetadataStatusCache();
        defer self.metadata_status_cache_mutex.unlock();
        self.metadata_status_cache = next_status;
        self.metadata_status_cache_valid = true;
        self.metadata_status_cache_next_refresh_at_ms = next_refresh_at_ms;
        self.metadata_status_cache_projection_epoch = projection_epoch;
        self.metadata_status_cache_placement_epoch = placement_epoch;
        self.metadata_status_cache_transition_epoch = transition_epoch;
    }

    fn lockMetadataStatusCache(self: *MetadataHttpService) void {
        while (!self.metadata_status_cache_mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
    }

    fn refreshMetadataStatusCacheIfDue(self: *MetadataHttpService) void {
        const now_ms = nowMs();
        self.lockMetadataStatusCache();
        const due = now_ms >= self.metadata_status_cache_next_refresh_at_ms;
        if (due) self.metadata_status_cache_next_refresh_at_ms = now_ms + metadata_status_cache_refresh_interval_ms;
        self.metadata_status_cache_mutex.unlock();
        if (!due) return;

        var current_status = snapshotStatus(self.alloc, self.metadata_group_id, self, self.metrics()) catch |err| {
            std.log.warn("metadata status cache refresh failed err={s}", .{@errorName(err)});
            return;
        };
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        self.storeMetadataStatusCache(current_status, now_ms + metadata_status_cache_refresh_interval_ms);
    }

    pub fn status(self: *MetadataHttpService) !MetadataStatus {
        const now_ms = nowMs();
        var current_status = snapshotStatus(self.alloc, self.metadata_group_id, self, self.metrics()) catch |err| blk: {
            std.log.warn("metadata status refresh failed err={s}", .{@errorName(err)});
            break :blk self.loadMetadataStatusCache() orelse self.fallbackStatus();
        };
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        current_status.metrics = self.metrics();
        self.storeMetadataStatusCache(current_status, now_ms + metadata_status_cache_refresh_interval_ms);
        return current_status;
    }

    pub fn adminSnapshot(self: *MetadataHttpService) !metadata_api.AdminSnapshot {
        var snapshot: metadata_api.AdminSnapshot = .{
            .status = try self.status(),
            .tables = &.{},
            .ranges = &.{},
            .stores = &.{},
            .placement_intents = &.{},
            .shuffle_join_leases = &.{},
            .local_bootstrap_statuses = &.{},
            .restore_progresses = &.{},
            .replication_source_statuses = &.{},
            .replication_source_action_hints = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .split_observations = &.{},
            .merge_observations = &.{},
            .merged_group_statuses = &.{},
        };
        errdefer self.freeAdminSnapshot(&snapshot);

        self.lockRuntime();
        errdefer self.unlockRuntime();
        const core = try self.projectedCoreSnapshotLocked();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        snapshot.tables = try cloneProjectedTablesOwned(self.alloc, core.tables);
        snapshot.ranges = try cloneProjectedRangesOwned(self.alloc, core.ranges);
        snapshot.nodes = try store.listNodes(self.alloc, self.metadata_group_id);
        snapshot.stores = try cloneProjectedStoresOwned(self.alloc, core.stores);
        snapshot.placement_intents = try cloneProjectedPlacementIntentsOwned(self.alloc, core.placement_intents);
        snapshot.shuffle_join_leases = try cloneProjectedShuffleJoinLeasesOwned(self.alloc, core.shuffle_join_leases);
        snapshot.restore_progresses = try cloneProjectedRestoreProgressesOwned(self.alloc, core.restore_progresses);
        snapshot.replication_source_statuses = try cloneProjectedReplicationSourceStatusesOwned(self.alloc, core.replication_source_statuses);
        snapshot.split_transitions = try cloneProjectedSplitTransitionsOwned(self.alloc, core.split_transitions);
        snapshot.merge_transitions = try cloneProjectedMergeTransitionsOwned(self.alloc, core.merge_transitions);
        self.unlockRuntime();

        snapshot.local_bootstrap_statuses = try self.listLocalBootstrapStatuses(self.alloc);
        snapshot.replication_source_action_hints = try metadata_api.deriveReplicationSourceActionHints(
            self.alloc,
            snapshot.tables,
            snapshot.replication_source_statuses,
        );
        snapshot.merged_group_statuses = try metadata_state.mergeHealthyGroupStatuses(
            self.alloc,
            snapshot.tables,
            snapshot.ranges,
            snapshot.placement_intents,
            snapshot.restore_progresses,
            snapshot.stores,
            snapshot.split_transitions,
            snapshot.merge_transitions,
            &.{},
            &.{},
        );
        return snapshot;
    }

    pub fn freeAdminSnapshot(self: *MetadataHttpService, snapshot: *metadata_api.AdminSnapshot) void {
        metadata_api.freeSnapshot(self.alloc, self, snapshot);
    }

    pub fn projectedStore(self: *MetadataHttpService) ?*metadata_storage.RaftApplyStore {
        return self.raft.host.owned_metadata_store;
    }

    pub fn getProjectedReconcileLease(self: *MetadataHttpService) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReconcileLease(self.metadata_group_id);
    }

    pub fn getProjectedShuffleJoinLease(self: *MetadataHttpService, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getShuffleJoinLease(self.metadata_group_id, job_id);
    }

    pub fn getProjectedReallocationRequest(self: *MetadataHttpService) !?metadata_mod.ReallocationRequestRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReallocationRequest(self.metadata_group_id);
    }

    pub fn reconcileLeaseStats(self: *MetadataHttpService) metadata_reconcile_lease.Stats {
        return self.reconcile_lease.stats();
    }

    fn captureProjectedCoreSnapshotLocked(self: *MetadataHttpService) !ProjectedCoreSnapshot {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        var snapshot: ProjectedCoreSnapshot = .{};
        errdefer snapshot.deinit(self.alloc);
        snapshot.tables = try store.listTables(self.alloc, self.metadata_group_id);
        snapshot.ranges = try store.listRanges(self.alloc, self.metadata_group_id);
        snapshot.stores = try store.listStores(self.alloc, self.metadata_group_id);
        snapshot.placement_intents = try store.listPlacementIntents(self.alloc, self.metadata_group_id);
        snapshot.shuffle_join_leases = try store.listShuffleJoinLeases(self.alloc, self.metadata_group_id);
        snapshot.schema_progresses = try store.listSchemaProgress(self.alloc, self.metadata_group_id);
        snapshot.restore_progresses = try store.listRestoreProgress(self.alloc, self.metadata_group_id);
        snapshot.replication_source_statuses = try store.listReplicationSourceStatuses(self.alloc, self.metadata_group_id);
        snapshot.split_transitions = try store.listSplitTransitions(self.alloc, self.metadata_group_id);
        snapshot.merge_transitions = try store.listMergeTransitions(self.alloc, self.metadata_group_id);
        return snapshot;
    }

    fn projectedCoreSnapshotLocked(self: *MetadataHttpService) !*const ProjectedCoreSnapshot {
        try self.ensureLifecycleListenerRegistered();
        const projection_epoch = self.projection_epoch.load(.monotonic);
        const placement_epoch = self.placement_epoch.load(.monotonic);
        const transition_epoch = self.transition_epoch.load(.monotonic);
        if (self.projected_core_snapshot_cache.snapshot == null or
            self.projected_core_snapshot_cache.projection_epoch != projection_epoch or
            self.projected_core_snapshot_cache.placement_epoch != placement_epoch or
            self.projected_core_snapshot_cache.transition_epoch != transition_epoch)
        {
            var fresh = try self.captureProjectedCoreSnapshotLocked();
            errdefer fresh.deinit(self.alloc);
            if (self.projected_core_snapshot_cache.snapshot) |*snapshot| snapshot.deinit(self.alloc);
            self.projected_core_snapshot_cache = .{
                .projection_epoch = projection_epoch,
                .placement_epoch = placement_epoch,
                .transition_epoch = transition_epoch,
                .snapshot = fresh,
            };
        }
        return &(self.projected_core_snapshot_cache.snapshot orelse unreachable);
    }

    pub fn listProjectedTables(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedTablesOwned(alloc, snapshot.tables);
    }

    pub fn freeProjectedTables(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeTables(alloc, records);
    }

    pub fn listProjectedSchemaProgress(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.SchemaProgressRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedSchemaProgressOwned(alloc, snapshot.schema_progresses);
    }

    pub fn freeProjectedSchemaProgress(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.SchemaProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSchemaProgress(alloc, records);
    }

    pub fn listProjectedRestoreProgress(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedRestoreProgressesOwned(alloc, snapshot.restore_progresses);
    }

    pub fn freeProjectedRestoreProgress(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRestoreProgress(alloc, records);
    }

    pub fn listProjectedReplicationSourceStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedReplicationSourceStatusesOwned(alloc, snapshot.replication_source_statuses);
    }

    pub fn freeProjectedReplicationSourceStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeReplicationSourceStatuses(alloc, records);
    }

    pub fn listProjectedShuffleJoinLeases(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedShuffleJoinLeasesOwned(alloc, snapshot.shuffle_join_leases);
    }

    pub fn freeProjectedShuffleJoinLeases(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.ShuffleJoinLeaseRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeShuffleJoinLeases(alloc, records);
    }

    pub fn listLocalBootstrapStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]raft_host.BootstrapStatus {
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.host.http_host.host.listBootstrapStatuses(alloc);
    }

    pub fn freeLocalBootstrapStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator, statuses: []raft_host.BootstrapStatus) void {
        self.raft.host.http_host.host.freeBootstrapStatuses(alloc, statuses);
    }

    pub fn listProjectedRanges(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedRangesOwned(alloc, snapshot.ranges);
    }

    pub fn freeProjectedRanges(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRanges(alloc, records);
    }

    pub fn listProjectedPlacementIntents(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedPlacementIntentsOwned(alloc, snapshot.placement_intents);
    }

    pub fn listProjectedNodes(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.NodeRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listNodes(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedNodes(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.NodeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeNodes(alloc, records);
    }

    pub fn listProjectedStores(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedStoresOwned(alloc, snapshot.stores);
    }

    pub fn freeProjectedStores(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeStores(alloc, records);
    }

    pub fn freeProjectedPlacementIntents(self: *MetadataHttpService, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        const store = self.projectedStore() orelse return;
        store.freePlacementIntents(alloc, intents);
    }

    pub fn listProjectedSplitTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedSplitTransitionsOwned(alloc, snapshot.split_transitions);
    }

    pub fn freeProjectedSplitTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSplitTransitions(alloc, records);
    }

    pub fn listProjectedMergeTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedMergeTransitionsOwned(alloc, snapshot.merge_transitions);
    }

    pub fn freeProjectedMergeTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeMergeTransitions(alloc, records);
    }

    fn lockRuntime(self: *MetadataHttpService) void {
        while (!self.runtime_mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
    }

    fn unlockRuntime(self: *MetadataHttpService) void {
        self.runtime_mutex.unlock();
    }

    fn refreshLocalPlacementIntents(self: *MetadataHttpService, round_inputs: ?*const LocalPlacementInputs) !void {
        const current_epoch = self.placement_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_placement_epoch,
            self.last_local_placement_refresh_at_ms,
            current_epoch,
            local_placement_refresh_interval_ms,
        )) return;

        var owned_inputs: ?LocalPlacementInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalPlacementInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalPlacementInputs(self);
            break :blk &owned_inputs.?;
        };

        var local = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        defer {
            for (local.items) |intent| if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
            local.deinit(self.alloc);
        }

        for (inputs.placement_intents) |intent| {
            if (intent.record.local_node_id != self.raft.host.http_host.host.cfg.local_node_id) continue;
            try local.append(self.alloc, .{
                .record = intent.record,
                .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
            });
        }

        if (!containsLocalIntent(local.items, self.metadata_group_id)) {
            self.lockRuntime();
            const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id);
            self.unlockRuntime();
            if (raft_status) |value| {
                try local.append(self.alloc, .{
                    .record = .{
                        .group_id = self.metadata_group_id,
                        .replica_id = self.raft.host.http_host.host.cfg.local_node_id,
                        .local_node_id = self.raft.host.http_host.host.cfg.local_node_id,
                        .bootstrap_mode = .persisted,
                    },
                    .peer_node_ids = try allocPeerNodeIdsExcludingSelf(
                        self.alloc,
                        value.conf_state.voters,
                        self.raft.host.http_host.host.cfg.local_node_id,
                    ),
                });
            }
        }

        // `replacePlacementIntents()` mutates the shared metadata view that also backs the
        // managed host's placement provider. Hold the runtime lock across the mutation and
        // immediate reconcile so concurrent admin requests cannot observe torn placement state.
        self.lockRuntime();
        defer self.unlockRuntime();
        try self.raft.host.replacePlacementIntents(local.items);
        _ = try self.raft.host.reconcileOnce();
        self.local_placement_epoch = current_epoch;
        self.last_local_placement_refresh_at_ms = nowMs();
    }

    fn refreshLocalTransitions(self: *MetadataHttpService, round_inputs: ?*const LocalTransitionInputs) !void {
        const transition_svc = if (self.raft.transition_svc) |*svc| svc else return;
        const current_epoch = self.transition_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_transition_epoch,
            self.last_local_transition_refresh_at_ms,
            current_epoch,
            local_transition_refresh_interval_ms,
        )) return;

        var owned_inputs: ?LocalTransitionInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalTransitionInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalTransitionInputs(self);
            break :blk &owned_inputs.?;
        };
        const split_records = inputs.split_transitions;
        const merge_records = inputs.merge_transitions;

        var split_index: usize = 0;
        while (split_index < transition_svc.pending_split.items.len) {
            const transition_id = transition_svc.pending_split.items[split_index].transition_id;
            if (findProjectedSplit(split_records, transition_id) == null) {
                _ = transition_svc.removeSplit(transition_id);
                continue;
            }
            split_index += 1;
        }
        for (split_records) |record| {
            if (findQueuedSplit(transition_svc.pending_split.items, record.transition_id) == null and
                !transition_svc.hasCompletedSplit(record.transition_id))
            {
                try transition_svc.submitSplit(record);
            }
        }

        var merge_index: usize = 0;
        while (merge_index < transition_svc.pending_merge.items.len) {
            const transition_id = transition_svc.pending_merge.items[merge_index].transition_id;
            if (findProjectedMerge(merge_records, transition_id) == null) {
                _ = transition_svc.removeMerge(transition_id);
                continue;
            }
            merge_index += 1;
        }
        for (merge_records) |record| {
            if (findQueuedMerge(transition_svc.pending_merge.items, record.transition_id) == null and
                !transition_svc.hasCompletedMerge(record.transition_id))
            {
                try transition_svc.submitMerge(record);
            }
        }

        self.raft.metrics.queued_split_transitions = transition_svc.metrics.queued_split_transitions;
        self.raft.metrics.queued_merge_transitions = transition_svc.metrics.queued_merge_transitions;
        self.local_transition_epoch = current_epoch;
        self.last_local_transition_refresh_at_ms = nowMs();
    }

    fn refreshLocalTableProvisioning(self: *MetadataHttpService, round_inputs: ?*const LocalProjectionInputs) !metadata_table_provisioner.ProvisionSummary {
        const replica_root_dir = self.replica_root_dir orelse return .{};
        const current_epoch = self.projection_epoch.load(.monotonic);
        var owned_inputs: ?LocalProjectionInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalProjectionInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalProjectionInputs(self);
            break :blk &owned_inputs.?;
        };
        const group_ids = inputs.group_ids;
        const group_ids_fingerprint = inputs.group_ids_fingerprint;
        if (!shouldRefreshLocalProjection(
            self.local_table_provisioning_epoch,
            self.local_table_provisioning_group_ids_fingerprint,
            self.last_local_table_provisioning_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_table_provisioning_refresh_interval_ms,
        )) return .{};

        const fingerprint = metadata_table_provisioner.provisioningFingerprint(
            self.metadata_group_id,
            group_ids,
            inputs.tables,
            inputs.ranges,
        );
        self.local_table_provisioning_epoch = current_epoch;
        self.local_table_provisioning_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_table_provisioning_refresh_at_ms = nowMs();
        if (self.local_table_provisioning_fingerprint == fingerprint) {
            try self.refreshLocalRestoreProgress(group_ids, inputs.tables, inputs.ranges, inputs.restore_progresses);
            return .{};
        }
        if (self.local_replica_root_reconcile_permit_hook) |hook| {
            if (!hook.shouldReconcile()) return .{};
        }
        const summary = try metadata_table_provisioner.reconcileReplicaRootWithOptions(
            self.alloc,
            replica_root_dir,
            self.metadata_group_id,
            group_ids,
            inputs.tables,
            inputs.ranges,
            .{
                .backend_runtime = try self.ensureBackendRuntime(),
            },
        );
        if (self.local_replica_root_reconcile_hook) |hook| try hook.run();
        self.local_table_provisioning_fingerprint = fingerprint;
        try self.refreshLocalRestoreProgress(group_ids, inputs.tables, inputs.ranges, inputs.restore_progresses);
        self.local_schema_progress_epoch = null;
        self.local_schema_progress_group_ids_fingerprint = null;
        self.last_local_schema_progress_refresh_at_ms = 0;
        try self.refreshLocalSchemaProgress(inputs);
        return summary;
    }

    fn refreshLocalRestoreProgress(
        self: *MetadataHttpService,
        group_ids: []const u64,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        projected_progress: []const metadata_table_manager.RestoreProgressRecord,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        const local_progress = try metadata_table_provisioner.collectLocalRestoreProgress(
            self.alloc,
            replica_root_dir,
            self.metadata_group_id,
            local_node_id,
            group_ids,
            tables,
            ranges,
        );
        defer {
            for (local_progress) |record| metadata_table_manager.freeRestoreProgress(self.alloc, record);
            self.alloc.free(local_progress);
        }
        try syncLocalRestoreProgress(self, local_node_id, local_progress, projected_progress);
    }

    fn refreshLocalSchemaProgress(self: *MetadataHttpService, round_inputs: ?*const LocalProjectionInputs) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        const current_epoch = self.projection_epoch.load(.monotonic);
        var owned_inputs: ?LocalProjectionInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalProjectionInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalProjectionInputs(self);
            break :blk &owned_inputs.?;
        };
        const group_ids = inputs.group_ids;
        const group_ids_fingerprint = inputs.group_ids_fingerprint;
        if (!shouldRefreshLocalProjection(
            self.local_schema_progress_epoch,
            self.local_schema_progress_group_ids_fingerprint,
            self.last_local_schema_progress_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_schema_progress_refresh_interval_ms,
        )) return;

        var local_progress = try metadata_table_provisioner.collectLocalSchemaProgressFromRuntime(
            self.alloc,
            local_node_id,
            inputs.tables,
            inputs.ranges,
            inputs.stores,
        );
        defer self.alloc.free(local_progress);
        if (local_progress.len == 0) {
            self.alloc.free(local_progress);
            const backend_runtime = try self.ensureBackendRuntime();
            var fallback_shard_db = metadata_mod.FallbackLocalShardDbAdapter{
                .replica_root_dir = replica_root_dir,
                .backend_runtime = backend_runtime,
            };
            const shard_db = self.local_shard_db_adapter orelse fallback_shard_db.adapter();
            local_progress = metadata_table_provisioner.collectLocalSchemaProgressWithOptions(
                self.alloc,
                replica_root_dir,
                self.metadata_group_id,
                local_node_id,
                group_ids,
                inputs.tables,
                inputs.ranges,
                .{
                    .backend_runtime = backend_runtime,
                    .shard_db_adapter = shard_db,
                },
            ) catch |err| switch (err) {
                error.WriterLocked => try self.alloc.alloc(metadata_table_manager.SchemaProgressRecord, 0),
                else => return err,
            };
        }
        try syncLocalSchemaProgress(self, local_node_id, local_progress, inputs.schema_progresses);
        self.local_schema_progress_epoch = current_epoch;
        self.local_schema_progress_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_schema_progress_refresh_at_ms = nowMs();
    }

    fn refreshLocalStoreStatus(self: *MetadataHttpService) !void {
        try self.refreshLocalStoreStatusWithBackfillMarkers(null, true);
    }

    fn refreshLocalStoreStatusWithBackfillMarkers(
        self: *MetadataHttpService,
        backfill_markers: ?[]const StoreStatusBackfillMarker,
        use_provider: bool,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        try syncLocalStoreStatus(self, local_node_id, replica_root_dir, backfill_markers, use_provider);
    }

    fn refreshStoreStatusBackfillMarkersForRound(self: *MetadataHttpService) ![]const StoreStatusBackfillMarker {
        self.store_status_ticks += 1;
        self.store_status_backfill_probe_ticks += 1;
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            self.store_status_ticks,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn refreshStoreStatusBackfillMarkersForLifecycleRound(self: *MetadataHttpService) ![]const StoreStatusBackfillMarker {
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        if (self.store_status_backfill_marker_cache.markers.len == 0 and self.store_status_backfill_marker_cache.scanned_at_ms == 0) {
            try refreshStoreStatusBackfillMarkerCacheNow(
                self.alloc,
                replica_root_dir,
                &self.store_status_backfill_probe_ticks,
                &self.store_status_backfill_marker_cache,
            );
            return self.store_status_backfill_marker_cache.markers;
        }

        self.store_status_backfill_probe_ticks += 1;
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            0,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn completeRestoreIntentsIfReady(
        self: *MetadataHttpService,
        round_projection_inputs: ?*const LocalProjectionInputs,
        round_placement_inputs: ?*const LocalPlacementInputs,
    ) !void {
        try completeRestoreIntentsForService(
            self,
            if (round_projection_inputs) |inputs| inputs.tables else null,
            if (round_projection_inputs) |inputs| inputs.ranges else null,
            if (round_placement_inputs) |inputs| inputs.placement_intents else null,
            if (round_projection_inputs) |inputs| inputs.restore_progresses else null,
        );
    }

    fn ensureReconcileLease(self: *MetadataHttpService) !bool {
        const now_ms = self.reconcile_lease.nowMs();
        const is_local_leader = self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id);
        const projected = self.getCachedProjectedReconcileLease(now_ms, is_local_leader) catch |err| switch (err) {
            error.MissingMetadataStore => null,
            else => return err,
        };
        const has_lease = self.reconcile_lease.observe(is_local_leader, projected, now_ms);
        if (self.reconcile_lease.shouldRenew(is_local_leader, projected, now_ms)) {
            self.upsertReconcileLease(self.reconcile_lease.desiredRecord(now_ms)) catch |err| {
                self.reconcile_lease.noteAcquireFailure();
                return err;
            };
        }
        return has_lease;
    }

    fn getCachedProjectedReconcileLease(
        self: *MetadataHttpService,
        now_ms: u64,
        is_local_leader: bool,
    ) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        const current_epoch = self.reconcile_lease_epoch.load(.monotonic);
        if (self.reconcile_lease_projection_cache.epoch == current_epoch and
            now_ms < self.reconcile_lease_projection_cache.next_refresh_at_ms)
        {
            return self.reconcile_lease_projection_cache.record;
        }
        const projected = try self.getProjectedReconcileLease();
        self.reconcile_lease_projection_cache = .{
            .epoch = current_epoch,
            .record = projected,
            .next_refresh_at_ms = reconcileLeaseCacheNextRefreshAtMs(&self.reconcile_lease, is_local_leader, projected, now_ms),
        };
        return projected;
    }

    fn statusProjectedReconcileLease(self: *MetadataHttpService, now_ms: u64) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        return try self.getCachedProjectedReconcileLease(now_ms, self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id));
    }

    fn runLifecycleReconcileHookIfRequested(self: *MetadataHttpService) !void {
        const hook = self.lifecycle_reconcile_hook orelse return;
        if (!self.lifecycle_reconcile_requested.swap(false, .acq_rel)) return;
        try hook.run();
    }

    fn runReplicationBackfillRound(self: *MetadataHttpService) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        if (!self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id)) return;
        while (!self.cdc_runtime_mutex.tryLock()) {
            std.Thread.yield() catch {};
        }
        defer self.cdc_runtime_mutex.unlock();
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        if (now_ms < self.cdc_next_round_at_ms) return;
        self.cdc_next_round_at_ms = now_ms + cdc_replication_round_interval_ms;

        const catalog = api_table_catalog.CatalogSource.fromMetadataHttpService(self);
        var cdc_group_router = api_table_router.CatalogBackedGroupRouter.init(
            catalog,
            // CDC is metadata-owned but data-applied; force the routed API path even
            // when metadata and data live in the same swarm process.
            0,
        );
        var hosted_write_source = api_table_writes.HostedProvisionedTableWriteSource.init(
            replica_root_dir,
            catalog,
            cdc_group_router.router(),
            self.raft.host.http_host.request_executor,
        );
        _ = hosted_write_source.withBackendRuntime(try self.ensureBackendRuntime());
        _ = hosted_write_source.withSecretStore(self.secret_store);
        const write_source = self.cdc_write_source_override orelse hosted_write_source.source();
        var coordinator = metadata_replication_backfill.SnapshotBackfillCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source,
                .secret_store = self.secret_store,
            },
        };
        const summary = coordinator.runRound(self) catch |err| switch (err) {
            error.UnknownReplicationSource,
            error.UnsupportedReplicationSource,
            error.UnsupportedReplicationStreaming,
            error.UnsupportedReplicationTransform,
            error.UnsupportedReplicationRoute,
            error.ReplicationExactCutoverRequired,
            error.InvalidReplicationSourceConfig,
            error.InvalidReplicationSourceRow,
            error.ForeignAuthFailed,
            error.ForeignConnectionFailed,
            error.ForeignQueryFailed,
            error.ForeignReplicationSlotMissing,
            error.ForeignTableNotFound,
            error.FileNotFound,
            error.InvalidQueryRequest,
            error.WriterLocked,
            error.LmdbUnexpected,
            error.Corrupted,
            error.UnknownColumn,
            => {
                if (comptime builtin.is_test) {
                    std.debug.print("metadata http cdc snapshot round skipped: {s}\n", .{@errorName(err)});
                }
                std.log.warn("metadata http cdc snapshot round skipped: {s}", .{@errorName(err)});
                return;
            },
            else => return err,
        };
        if (summary.sources_considered > 0) {
            std.log.info(
                "metadata http cdc snapshot round tables={d} sources={d} started={d} resumed={d} completed={d}",
                .{
                    summary.tables_considered,
                    summary.sources_considered,
                    summary.sources_started,
                    summary.sources_resumed,
                    summary.sources_completed,
                },
            );
        }
        var streaming = metadata_replication_backfill.StreamingReplicationCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source,
                .secret_store = self.secret_store,
            },
        };
        const stream_summary = streaming.runRound(self) catch |err| switch (err) {
            error.UnknownReplicationSource,
            error.UnsupportedReplicationSource,
            error.UnsupportedReplicationStreaming,
            error.UnsupportedReplicationTransform,
            error.UnsupportedReplicationRoute,
            error.ReplicationExactCutoverRequired,
            error.InvalidReplicationSourceConfig,
            error.InvalidReplicationSourceRow,
            error.ForeignAuthFailed,
            error.ForeignConnectionFailed,
            error.ForeignQueryFailed,
            error.ForeignReplicationSlotMissing,
            error.ForeignTableNotFound,
            error.FileNotFound,
            error.InvalidQueryRequest,
            error.WriterLocked,
            error.LmdbUnexpected,
            error.Corrupted,
            error.UnknownColumn,
            => {
                std.log.warn("metadata http cdc streaming round skipped: {s}", .{@errorName(err)});
                return;
            },
            else => return err,
        };
        if (stream_summary.sources_considered > 0) {
            std.log.info(
                "metadata http cdc streaming round tables={d} sources={d} started={d} resumed={d} skipped={d} polled={d} changes={d}",
                .{
                    stream_summary.tables_considered,
                    stream_summary.sources_considered,
                    stream_summary.sources_started,
                    stream_summary.sources_resumed,
                    stream_summary.sources_skipped_pending_snapshot,
                    stream_summary.sources_polled,
                    stream_summary.changes_applied,
                },
            );
        }
    }
};

fn syncLocalSchemaProgress(
    service: anytype,
    local_node_id: u64,
    local_progress: []const metadata_table_manager.SchemaProgressRecord,
    projected_progress: []const metadata_table_manager.SchemaProgressRecord,
) !void {
    for (local_progress) |record| {
        const existing = findSchemaProgress(projected_progress, record.table_id, record.node_id);
        if (existing != null and existing.?.schema_version == record.schema_version) continue;
        try service.upsertSchemaProgress(record);
    }

    for (projected_progress) |record| {
        if (record.node_id != local_node_id) continue;
        if (findSchemaProgress(local_progress, record.table_id, record.node_id) != null) continue;
        try service.removeSchemaProgress(record.table_id, record.node_id);
    }
}

fn runReplicationBackfillIfLeaseHeld(service: anytype) !bool {
    const has_reconcile_lease = try service.ensureReconcileLease();
    if (!has_reconcile_lease) return false;
    try service.runReplicationBackfillRound();
    return true;
}

fn syncLocalRestoreProgress(
    service: anytype,
    local_node_id: u64,
    local_progress: []const metadata_table_manager.RestoreProgressRecord,
    projected_progress: []const metadata_table_manager.RestoreProgressRecord,
) !void {
    for (local_progress) |record| {
        const existing = findRestoreProgress(projected_progress, record.table_id, record.node_id, record.group_id);
        if (existing != null and restoreProgressEquivalent(existing.?, record)) continue;
        try service.upsertRestoreProgress(record);
    }

    for (projected_progress) |record| {
        if (record.node_id != local_node_id) continue;
        const local = findRestoreProgress(local_progress, record.table_id, record.node_id, record.group_id);
        if (local != null and restoreProgressEquivalent(local.?, record)) continue;
        try service.removeRestoreProgress(record.table_id, record.node_id, record.group_id);
    }
}

fn restoreProgressEquivalent(a: metadata_table_manager.RestoreProgressRecord, b: metadata_table_manager.RestoreProgressRecord) bool {
    return std.mem.eql(u8, a.backup_id, b.backup_id) and
        std.mem.eql(u8, a.snapshot_path, b.snapshot_path) and
        a.primary_restored == b.primary_restored and
        a.runtime_repair_complete == b.runtime_repair_complete and
        std.mem.eql(u8, a.phase, b.phase) and
        std.mem.eql(u8, a.last_error, b.last_error);
}

fn cloneSplitRuntimeObservationsForMerge(
    alloc: std.mem.Allocator,
    records: []const transition_state.SplitObservationRecord,
) ![]metadata_reconciler.SplitRuntimeObservation {
    const out = try alloc.alloc(metadata_reconciler.SplitRuntimeObservation, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .observation = record.observation,
        };
    }
    return out;
}

fn cloneMergeRuntimeObservationsForMerge(
    alloc: std.mem.Allocator,
    records: []const transition_state.MergeObservationRecord,
) ![]metadata_reconciler.MergeRuntimeObservation {
    const out = try alloc.alloc(metadata_reconciler.MergeRuntimeObservation, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .observation = record.observation,
        };
    }
    return out;
}

fn completeRestoreIntentsForService(
    service: anytype,
    provided_tables: ?[]const metadata_table_manager.TableRecord,
    provided_ranges: ?[]const metadata_table_manager.RangeRecord,
    provided_placements: ?[]const raft_reconciler.PlacementIntent,
    provided_progress: ?[]const metadata_table_manager.RestoreProgressRecord,
) !void {
    const tables = if (provided_tables) |tables| tables else try service.listProjectedTables(service.alloc);
    defer if (provided_tables == null) service.freeProjectedTables(service.alloc, @constCast(tables));
    const ranges = if (provided_ranges) |ranges| ranges else try service.listProjectedRanges(service.alloc);
    defer if (provided_ranges == null) service.freeProjectedRanges(service.alloc, @constCast(ranges));
    const placements = if (provided_placements) |placements| placements else try service.listProjectedPlacementIntents(service.alloc);
    defer if (provided_placements == null) service.freeProjectedPlacementIntents(service.alloc, @constCast(placements));
    const progress = if (provided_progress) |progress| progress else try service.listProjectedRestoreProgress(service.alloc);
    defer if (provided_progress == null) service.freeProjectedRestoreProgress(service.alloc, @constCast(progress));

    for (ranges) |range| {
        const table = findProjectedTableById(tables, range.table_id) orelse continue;
        const restore_backup_id = restoreBackupIdForRange(range, table) orelse continue;
        if (!rangeRestoreIntentComplete(table.table_id, range.group_id, restore_backup_id, placements, progress)) continue;
        if (range.restore_backup_id.len == 0 and range.restore_location.len == 0) continue;

        var cleared = try metadata_table_manager.cloneRange(service.alloc, range);
        errdefer metadata_table_manager.freeRange(service.alloc, cleared);
        service.alloc.free(cleared.restore_backup_id);
        service.alloc.free(cleared.restore_location);
        service.alloc.free(cleared.restore_snapshot_path);
        cleared.restore_backup_id = try service.alloc.dupe(u8, "");
        cleared.restore_location = try service.alloc.dupe(u8, "");
        cleared.restore_snapshot_path = try service.alloc.dupe(u8, "");
        try service.upsertRange(cleared);
        metadata_table_manager.freeRange(service.alloc, cleared);
    }

    for (tables) |table| {
        if (table.restore_backup_id.len == 0) continue;
        if (!restoreIntentComplete(table, ranges, placements, progress)) continue;

        var cleared = try metadata_table_manager.cloneTable(service.alloc, table);
        errdefer metadata_table_manager.freeTable(service.alloc, cleared);
        service.alloc.free(cleared.restore_backup_id);
        service.alloc.free(cleared.restore_location);
        cleared.restore_backup_id = try service.alloc.dupe(u8, "");
        cleared.restore_location = try service.alloc.dupe(u8, "");
        try service.upsertTable(cleared);
        metadata_table_manager.freeTable(service.alloc, cleared);
    }
}

fn restoreIntentComplete(
    table: metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    placements: []const raft_reconciler.PlacementIntent,
    progress: []const metadata_table_manager.RestoreProgressRecord,
) bool {
    var found_any_range = false;
    for (ranges) |range| {
        if (range.table_id != table.table_id) continue;
        const restore_backup_id = restoreBackupIdForRange(range, table) orelse continue;
        found_any_range = true;
        if (!rangeRestoreIntentComplete(table.table_id, range.group_id, restore_backup_id, placements, progress)) return false;
    }
    return found_any_range;
}

fn rangeRestoreIntentComplete(
    table_id: u64,
    group_id: u64,
    restore_backup_id: []const u8,
    placements: []const raft_reconciler.PlacementIntent,
    progress: []const metadata_table_manager.RestoreProgressRecord,
) bool {
    var found_any_placement = false;
    for (placements) |intent| {
        if (intent.record.group_id != group_id) continue;
        found_any_placement = true;
        const restored = findRestoreProgress(progress, table_id, intent.record.local_node_id, group_id) orelse return false;
        if (!std.mem.eql(u8, restored.backup_id, restore_backup_id)) return false;
        if (!restored.primary_restored or !restored.runtime_repair_complete) return false;
    }
    return found_any_placement;
}

fn restoreBackupIdForRange(
    range: metadata_table_manager.RangeRecord,
    table: metadata_table_manager.TableRecord,
) ?[]const u8 {
    if (range.restore_backup_id.len > 0) return range.restore_backup_id;
    if (table.restore_backup_id.len > 0) return table.restore_backup_id;
    return null;
}

fn collectProjectedSplitObservations(
    alloc: std.mem.Allocator,
    service: anytype,
    split_transitions: []const transition_state.SplitTransitionRecord,
) ![]transition_state.SplitObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.SplitObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (split_transitions) |record| {
        const observation = (try service.observeSplitTransition(record.transition_id)) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn collectProjectedMergeObservations(
    alloc: std.mem.Allocator,
    service: anytype,
    merge_transitions: []const transition_state.MergeTransitionRecord,
) ![]transition_state.MergeObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.MergeObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (merge_transitions) |record| {
        const observation = (try service.observeMergeTransition(record.transition_id)) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn findSchemaProgress(
    records: []const metadata_table_manager.SchemaProgressRecord,
    table_id: u64,
    node_id: u64,
) ?metadata_table_manager.SchemaProgressRecord {
    for (records) |record| {
        if (record.table_id == table_id and record.node_id == node_id) return record;
    }
    return null;
}

fn findRestoreProgress(
    records: []const metadata_table_manager.RestoreProgressRecord,
    table_id: u64,
    node_id: u64,
    group_id: u64,
) ?metadata_table_manager.RestoreProgressRecord {
    for (records) |record| {
        if (record.table_id == table_id and record.node_id == node_id and record.group_id == group_id) return record;
    }
    return null;
}

fn syncLocalStoreStatus(
    service: anytype,
    local_node_id: u64,
    replica_root_dir: []const u8,
    scanned_backfill_markers: ?[]const StoreStatusBackfillMarker,
    use_provider: bool,
) !void {
    var admin_snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&admin_snapshot);
    var owned_backfill_markers: ?[]const StoreStatusBackfillMarker = null;
    const backfill_markers = scanned_backfill_markers orelse blk: {
        owned_backfill_markers = try collectStoreStatusBackfillMarkers(service.alloc, replica_root_dir);
        break :blk owned_backfill_markers.?;
    };
    defer if (owned_backfill_markers) |markers| freeStoreStatusBackfillMarkers(service.alloc, markers);
    const stores = admin_snapshot.stores;
    const placements = admin_snapshot.placement_intents;
    const tables = admin_snapshot.tables;
    const ranges = admin_snapshot.ranges;
    const split_transitions = admin_snapshot.split_transitions;
    const merge_transitions = admin_snapshot.merge_transitions;
    const split_observations = admin_snapshot.split_observations;
    const merge_observations = admin_snapshot.merge_observations;

    var local_stores = std.ArrayListUnmanaged(metadata_table_manager.StoreRecord).empty;
    defer local_stores.deinit(service.alloc);
    for (stores) |store| {
        if (store.node_id != local_node_id) continue;
        try local_stores.append(service.alloc, store);
    }

    if (local_stores.items.len == 0) return;
    const group_statuses = try collectLocalGroupStatusReportsWithProvider(
        service,
        service.alloc,
        replica_root_dir,
        tables,
        ranges,
        stores,
        admin_snapshot.merged_group_statuses,
        split_transitions,
        merge_transitions,
        split_observations,
        merge_observations,
        use_provider,
    );
    defer metadata_table_manager.freeGroupStatuses(service.alloc, group_statuses);
    if (local_stores.items.len == 1) {
        const report = try collectLocalStoreStatusReport(
            service,
            service.alloc,
            replica_root_dir,
            local_stores.items[0],
            ranges,
            group_statuses,
            backfill_markers,
        );
        defer freeOwnedStoreStatusReport(service.alloc, report);
        try service.reportStoreStatus(report);
        try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
        return;
    }

    const reports = try collectExplicitLocalStoreStatusReports(
        service,
        service.alloc,
        replica_root_dir,
        local_stores.items,
        ranges,
        group_statuses,
        backfill_markers,
    );
    defer freeOwnedStoreStatusReports(service.alloc, reports);
    if (reports.len > 0) {
        _ = try reportStoreStatusesWithProjected(service, stores, reports);
        try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
        return;
    }

    const shared_reports = try collectSharedRootLocalStoreStatusReports(
        service,
        service.alloc,
        replica_root_dir,
        local_node_id,
        local_stores.items,
        placements,
        tables,
        ranges,
        group_statuses,
        backfill_markers,
    );
    defer freeOwnedStoreStatusReports(service.alloc, shared_reports);
    if (shared_reports.len == 0) {
        try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
        return;
    }
    _ = try reportStoreStatusesWithProjected(service, stores, shared_reports);
    try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
}

fn reportStoreStatusesWithProjected(
    service: anytype,
    projected: []metadata_table_manager.StoreRecord,
    reports: []const metadata_table_manager.StoreStatusReport,
) !usize {
    var changed_indices = std.ArrayListUnmanaged(usize).empty;
    defer changed_indices.deinit(service.alloc);
    for (reports) |report| {
        const index = metadata_store_observer.findStoreIndex(projected, report.store_id) orelse return error.UnknownStore;
        if (!metadata_store_observer.observationChangesRecord(projected[index], report)) continue;
        try changed_indices.append(service.alloc, index);
    }

    const applied = try metadata_store_observer.applyObservationsOwned(service.alloc, projected, reports);
    for (changed_indices.items) |index| try service.upsertStore(projected[index]);
    return applied;
}

fn collectExplicitLocalStoreStatusReports(
    service: anytype,
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    stores: []const metadata_table_manager.StoreRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
    backfill_markers: []const StoreStatusBackfillMarker,
) ![]metadata_table_manager.StoreStatusReport {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var reports = std.ArrayListUnmanaged(metadata_table_manager.StoreStatusReport).empty;
    errdefer reports.deinit(alloc);

    for (stores) |store| {
        const store_root = try std.fmt.allocPrint(alloc, "{s}/store-{d}", .{ replica_root_dir, store.store_id });
        defer alloc.free(store_root);

        var dir = openDirPath(io_impl.io(), store_root, false) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => {
                reports.deinit(alloc);
                return try alloc.alloc(metadata_table_manager.StoreStatusReport, 0);
            },
            else => return err,
        };
        dir.close(io_impl.io());

        try reports.append(alloc, try collectLocalStoreStatusReport(
            service,
            alloc,
            store_root,
            store,
            ranges,
            group_statuses,
            backfill_markers,
        ));
    }

    return try reports.toOwnedSlice(alloc);
}

fn collectLocalStoreStatusReport(
    service: anytype,
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    store: metadata_table_manager.StoreRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
    backfill_markers: []const StoreStatusBackfillMarker,
) !metadata_table_manager.StoreStatusReport {
    _ = service;
    _ = replica_root_dir;
    var report: metadata_table_manager.StoreStatusReport = .{
        .store_id = store.store_id,
        .live = store.live,
        .health_class = store.health_class,
        .capacity_bytes = store.capacity_bytes,
        .available_bytes = store.available_bytes,
        .lease_pressure = store.lease_pressure,
        .read_load = store.read_load,
        .write_load = store.write_load,
        .active_backfills = 0,
        .backfill_progress_millis = 1000,
        .group_statuses = &.{},
        .runtime_statuses = &.{},
    };

    var progress_sum: f64 = 0.0;
    for (backfill_markers) |marker| {
        if (!storeStatusBackfillMarkerMatchesStore(marker, store.store_id, true)) continue;
        try accumulateStoreStatusBackfillProgress(
            alloc,
            ranges,
            marker,
            &report.active_backfills,
            &progress_sum,
        );
    }

    if (report.active_backfills > 0) {
        const avg_progress = progress_sum / @as(f64, @floatFromInt(report.active_backfills));
        const millis = std.math.clamp(avg_progress * 1000.0, 0.0, 1000.0);
        report.backfill_progress_millis = @intFromFloat(millis);
    }
    report.group_statuses = try metadata_table_manager.cloneGroupStatuses(alloc, group_statuses);
    return report;
}

fn collectSharedRootLocalStoreStatusReports(
    service: anytype,
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    local_node_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
    backfill_markers: []const StoreStatusBackfillMarker,
) ![]metadata_table_manager.StoreStatusReport {
    _ = service;
    var reports = try alloc.alloc(metadata_table_manager.StoreStatusReport, stores.len);
    errdefer alloc.free(reports);
    for (stores, 0..) |store, i| {
        reports[i] = .{
            .store_id = store.store_id,
            .live = store.live,
            .health_class = store.health_class,
            .capacity_bytes = store.capacity_bytes,
            .available_bytes = store.available_bytes,
            .lease_pressure = store.lease_pressure,
            .read_load = store.read_load,
            .write_load = store.write_load,
            .active_backfills = 0,
            .backfill_progress_millis = 1000,
            .group_statuses = &.{},
            .runtime_statuses = &.{},
        };
    }

    var progress_sum = try alloc.alloc(f64, stores.len);
    defer alloc.free(progress_sum);
    @memset(progress_sum, 0.0);

    for (backfill_markers) |marker| {
        if (marker.store_id != null) continue;
        const range = findRangeByGroupId(ranges, marker.group_id) orelse continue;
        const store_id = try resolveSharedRootStoreAffinity(alloc, replica_root_dir, local_node_id, marker.group_id, stores, placements, tables, range);
        const report_index = findStoreStatusReportIndex(reports, store_id) orelse continue;
        try accumulateStoreStatusBackfillProgress(
            alloc,
            ranges,
            marker,
            &reports[report_index].active_backfills,
            &progress_sum[report_index],
        );
    }

    for (reports, 0..) |*report, i| {
        if (report.active_backfills == 0) continue;
        const avg_progress = progress_sum[i] / @as(f64, @floatFromInt(report.active_backfills));
        const millis = std.math.clamp(avg_progress * 1000.0, 0.0, 1000.0);
        report.backfill_progress_millis = @intFromFloat(millis);
    }
    for (reports) |*report| {
        report.group_statuses = try metadata_table_manager.cloneGroupStatuses(alloc, group_statuses);
    }
    return reports;
}

fn shouldRefreshLocalStoreStatus(service: anytype, backfill_markers: []const StoreStatusBackfillMarker) bool {
    if (@field(service, "local_group_status_provider") != null and backfill_markers.len == 0) return false;
    return true;
}

fn shouldRefreshLocalStoreStatusForLifecycleRound(service: anytype, backfill_markers: []const StoreStatusBackfillMarker) bool {
    if (backfill_markers.len > 0) return true;
    if (@field(service, "local_group_status_provider") != null) return true;
    return shouldRefreshLocalStoreStatus(service, backfill_markers);
}

fn collectLocalGroupStatusReportsWithProvider(
    service: anytype,
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    stores: []const metadata_table_manager.StoreRecord,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const transition_state.SplitObservationRecord,
    merge_observations: []const transition_state.MergeObservationRecord,
    use_provider: bool,
) ![]metadata_table_manager.GroupStatusReport {
    if (use_provider) {
        if (@field(service, "local_group_status_provider")) |provider| {
            return try provider.collect(
                alloc,
                replica_root_dir,
                tables,
                ranges,
                stores,
                merged_group_statuses,
                split_transitions,
                merge_transitions,
                split_observations,
                merge_observations,
            );
        }
    }
    return try collectLocalGroupStatusReports(
        service,
        alloc,
        replica_root_dir,
        tables,
        ranges,
        stores,
        merged_group_statuses,
        split_transitions,
        merge_transitions,
        split_observations,
        merge_observations,
    );
}

fn collectLocalGroupStatusReports(
    service: anytype,
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    stores: []const metadata_table_manager.StoreRecord,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const transition_state.SplitObservationRecord,
    merge_observations: []const transition_state.MergeObservationRecord,
) ![]metadata_table_manager.GroupStatusReport {
    var reports = std.ArrayListUnmanaged(metadata_table_manager.GroupStatusReport).empty;
    errdefer {
        for (reports.items) |record| metadata_table_manager.freeGroupStatus(alloc, record);
        reports.deinit(alloc);
    }

    for (ranges) |range| {
        _ = findTableById(tables, range.table_id) orelse continue;
        const db_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ replica_root_dir, range.group_id });
        defer alloc.free(db_path);

        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        _ = statFilePath(io_impl.io(), db_path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };

        const group_status = try collectLocalGroupStatusReport(
            service,
            alloc,
            db_path,
            replica_root_dir,
            range.group_id,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        );
        errdefer metadata_table_manager.freeGroupStatus(alloc, group_status);
        try reports.append(alloc, group_status);
    }

    return try reports.toOwnedSlice(alloc);
}

fn collectLocalGroupStatusReport(
    service: anytype,
    alloc: std.mem.Allocator,
    db_path: []const u8,
    replica_root_dir: ?[]const u8,
    group_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const transition_state.SplitObservationRecord,
    merge_observations: []const transition_state.MergeObservationRecord,
) !metadata_table_manager.GroupStatusReport {
    _ = stores;
    _ = merged_group_statuses;
    var db = try db_mod.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
        .text_merge = .{ .enabled = false },
    });
    defer db.close();

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    const now_realtime_ms = platform_clock.Clock.real().nowRealtimeMs();
    const created_at_millis = (try db.getGroupCreatedAtMillis(alloc, group_id)) orelse now_realtime_ms;
    const readiness = if (replica_root_dir) |root_dir|
        try transition_state.readinessForLocalGroup(
            alloc,
            root_dir,
            group_id,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        )
    else
        transition_state.readinessForGroup(group_id, split_transitions, merge_transitions);
    const membership = serviceGroupMembership(service, group_id);

    return .{
        .group_id = group_id,
        .doc_count = stats.doc_count,
        .disk_bytes = try directoryUsageBytes(alloc, db_path),
        .empty = stats.doc_count == 0,
        .created_at_millis = created_at_millis,
        .updated_at_millis = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)),
        .local_leader = serviceGroupLocalLeader(service, group_id),
        .local_voter = membership.local_voter,
        .voter_count = membership.voter_count,
        .joint_consensus = membership.joint_consensus,
        .transition_pending = readiness.transition_pending,
        .replay_required = readiness.replay_required,
        .replay_caught_up = readiness.replay_caught_up,
        .cutover_ready = readiness.cutover_ready,
        .reads_ready_after_cutover = readiness.reads_ready_after_cutover,
    };
}

fn serviceGroupLocalLeader(service: anytype, group_id: u64) bool {
    const Service = @TypeOf(service);
    if (Service == *MetadataService) return service.raft.host.host.isLocalLeader(group_id);
    if (Service == *MetadataHttpService) return service.raft.host.http_host.host.isLocalLeader(group_id);
    return false;
}

const ServiceGroupRaftObservation = struct {
    local_node_id: u64 = 0,
    role: []const u8 = "absent",
    leader_id: ?u64 = null,
    term: u64 = 0,
    commit_index: u64 = 0,
    local_voter: bool = false,
    voter_count: usize = 0,
    election_elapsed: u32 = 0,
    randomized_election_timeout: u32 = 0,
    votes_granted: usize = 0,
    votes_rejected: usize = 0,
    votes_unknown: usize = 0,
    inbound_message_enqueues: usize = 0,
    inbound_message_drains: usize = 0,
    pending_inbound_messages: usize = 0,
    transport_sent_frames: usize = 0,
    transport_send_failures: usize = 0,
    transport_retries_scheduled: usize = 0,
    transport_retries_exhausted: usize = 0,
    transport_retried_successes: usize = 0,
    transport_peer_refreshes: usize = 0,
    transport_peer_routes: usize = 0,
    transport_served_groups: usize = 0,
    transport_pending_retries: usize = 0,
};

fn serviceGroupRaftObservation(service: anytype, group_id: u64) ServiceGroupRaftObservation {
    const Service = @TypeOf(service);
    if (Service == *MetadataService) {
        const raft_status = service.raft.host.host.raftStatus(group_id) orelse return .{};
        var observation = raftObservationFromStatus(raft_status, service.raft.host.host.cfg.local_node_id);
        const host_metrics = service.raft.host.host.metricsSnapshot();
        observation.inbound_message_enqueues = host_metrics.inbound_message_enqueues;
        observation.inbound_message_drains = host_metrics.inbound_message_drains;
        observation.pending_inbound_messages = host_metrics.pending_inbound_messages;
        return observation;
    }
    if (Service == *MetadataHttpService) {
        const raft_status = service.raft.host.http_host.host.raftStatus(group_id) orelse return .{};
        var observation = raftObservationFromStatus(raft_status, service.raft.host.http_host.host.cfg.local_node_id);
        const host_metrics = service.raft.host.http_host.host.metricsSnapshot();
        observation.inbound_message_enqueues = host_metrics.inbound_message_enqueues;
        observation.inbound_message_drains = host_metrics.inbound_message_drains;
        observation.pending_inbound_messages = host_metrics.pending_inbound_messages;
        const transport_metrics = service.raft.host.http_host.transport_stack.transport_host.metricsSnapshot();
        observation.transport_sent_frames = transport_metrics.sent_frames;
        observation.transport_send_failures = transport_metrics.send_failures;
        observation.transport_retries_scheduled = transport_metrics.retries_scheduled;
        observation.transport_retries_exhausted = transport_metrics.retries_exhausted;
        observation.transport_retried_successes = transport_metrics.retried_successes;
        observation.transport_peer_refreshes = transport_metrics.peer_refreshes;
        observation.transport_peer_routes = service.raft.host.http_host.transport_stack.transport_host.peer_routes.count();
        observation.transport_served_groups = service.raft.host.http_host.transport_stack.transport_host.served_groups.count();
        observation.transport_pending_retries = service.raft.host.http_host.transport_stack.transport_host.pendingRetryCount();
        return observation;
    }
    return .{};
}

fn raftObservationFromStatus(
    raft_status: raft_engine.core.Status,
    local_node_id: u64,
) ServiceGroupRaftObservation {
    var local_voter = false;
    for (raft_status.conf_state.voters) |node_id| {
        if (node_id == local_node_id) {
            local_voter = true;
            break;
        }
    }
    return .{
        .local_node_id = local_node_id,
        .role = raftRoleName(raft_status.soft.role),
        .leader_id = raft_status.soft.leader_id,
        .term = raft_status.hard.current_term,
        .commit_index = raft_status.hard.commit_index,
        .local_voter = local_voter,
        .voter_count = raft_status.conf_state.voters.len,
        .election_elapsed = raft_status.election_elapsed,
        .randomized_election_timeout = raft_status.randomized_election_timeout,
        .votes_granted = raft_status.votes_granted,
        .votes_rejected = raft_status.votes_rejected,
        .votes_unknown = raft_status.votes_unknown,
    };
}

fn raftRoleName(role: raft_engine.core.types.StateRole) []const u8 {
    return switch (role) {
        .follower => "follower",
        .pre_candidate => "pre_candidate",
        .candidate => "candidate",
        .leader => "leader",
    };
}

fn serviceGroupMembership(service: anytype, group_id: u64) struct { local_voter: bool = false, voter_count: u16 = 0, joint_consensus: bool = false } {
    const Service = @TypeOf(service);
    if (Service == *MetadataService) {
        const raft_status = service.raft.host.host.raftStatus(group_id) orelse return .{};
        var local_voter = false;
        for (raft_status.conf_state.voters) |node_id| {
            if (node_id == service.raft.host.host.cfg.local_node_id) {
                local_voter = true;
                break;
            }
        }
        return .{
            .local_voter = local_voter,
            .voter_count = @intCast(raft_status.conf_state.voters.len),
            .joint_consensus = raft_status.conf_state.voters_outgoing.len > 0,
        };
    }
    if (Service == *MetadataHttpService) {
        const raft_status = service.raft.host.http_host.host.raftStatus(group_id) orelse return .{};
        var local_voter = false;
        for (raft_status.conf_state.voters) |node_id| {
            if (node_id == service.raft.host.http_host.host.cfg.local_node_id) {
                local_voter = true;
                break;
            }
        }
        return .{
            .local_voter = local_voter,
            .voter_count = @intCast(raft_status.conf_state.voters.len),
            .joint_consensus = raft_status.conf_state.voters_outgoing.len > 0,
        };
    }
    return .{};
}

fn directoryUsageBytes(alloc: std.mem.Allocator, path: []const u8) !u64 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var dir = openDirPath(io_impl.io(), path, true) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close(io_impl.io());

    var total: u64 = 0;
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        total += stat.size;
    }
    return total;
}

fn statFilePath(io: anytype, path: []const u8) !std.Io.Dir.Stat {
    if (std.fs.path.isAbsolute(path)) {
        var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
        defer file.close(io);
        return try file.stat(io);
    }
    return try std.Io.Dir.cwd().statFile(io, path, .{});
}

fn openDirPath(io: anytype, path: []const u8, iterate: bool) !std.Io.Dir {
    const opts: std.Io.Dir.OpenOptions = .{ .iterate = iterate };
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, opts)
    else
        try std.Io.Dir.cwd().openDir(io, path, opts);
}

const StoreStatusBackfillMarker = struct {
    store_id: ?u64,
    group_id: u64,
    path: []const u8,
    resume_key: ?[]const u8 = null,
};

const StoreStatusBackfillMarkerCache = struct {
    markers: []StoreStatusBackfillMarker = &.{},
    scanned_at_ms: u64 = 0,
    rescan_requested: bool = false,

    fn deinit(self: *StoreStatusBackfillMarkerCache, alloc: std.mem.Allocator) void {
        freeStoreStatusBackfillMarkers(alloc, self.markers);
        self.* = .{};
    }

    fn replace(self: *StoreStatusBackfillMarkerCache, alloc: std.mem.Allocator, markers: []StoreStatusBackfillMarker, scanned_at_ms: u64) void {
        self.deinit(alloc);
        self.markers = markers;
        self.scanned_at_ms = scanned_at_ms;
        self.rescan_requested = false;
    }
};

fn maybeRefreshStoreStatusBackfillMarkerCache(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    store_status_ticks: usize,
    probe_ticks: *usize,
    cache: *StoreStatusBackfillMarkerCache,
) !void {
    const now_ms = monotonicMs();
    const should_rescan = if (cache.markers.len > 0)
        cache.rescan_requested or now_ms -| cache.scanned_at_ms >= store_status_backfill_rescan_interval_ms
    else
        cache.scanned_at_ms == 0 or
            cache.rescan_requested or
            ((store_status_ticks >= 40 or probe_ticks.* >= store_status_backfill_probe_interval_ticks) and
                now_ms -| cache.scanned_at_ms >= store_status_backfill_empty_rescan_interval_ms);
    if (!should_rescan) return;

    const markers = try collectStoreStatusBackfillMarkers(alloc, replica_root_dir);
    const markers_missing_resume_keys = storeStatusBackfillMarkersHaveMissingResumeKeys(markers);
    cache.replace(alloc, markers, now_ms);
    cache.rescan_requested = markers_missing_resume_keys;
    probe_ticks.* = 0;
}

fn refreshStoreStatusBackfillMarkerCacheNow(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    probe_ticks: *usize,
    cache: *StoreStatusBackfillMarkerCache,
) !void {
    const markers = try collectStoreStatusBackfillMarkers(alloc, replica_root_dir);
    const markers_missing_resume_keys = storeStatusBackfillMarkersHaveMissingResumeKeys(markers);
    cache.replace(alloc, markers, monotonicMs());
    cache.rescan_requested = markers_missing_resume_keys;
    probe_ticks.* = 0;
}

fn monotonicMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

fn scanStoreStatusBackfillMarkers(alloc: std.mem.Allocator, replica_root_dir: []const u8) ![]StoreStatusBackfillMarker {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var root_dir = std.Io.Dir.cwd().openDir(io_impl.io(), replica_root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return &.{},
        else => return err,
    };
    defer root_dir.close(io_impl.io());

    var markers = std.ArrayListUnmanaged(StoreStatusBackfillMarker).empty;
    errdefer {
        for (markers.items) |marker| {
            alloc.free(marker.path);
            if (marker.resume_key) |resume_key| alloc.free(resume_key);
        }
        markers.deinit(alloc);
    }

    var walker = try root_dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, "/rebuild.state")) continue;
        const parsed = parseStoreStatusBackfillMarkerPath(entry.path) orelse continue;
        try markers.append(alloc, .{
            .store_id = parsed.store_id,
            .group_id = parsed.group_id,
            .path = try alloc.dupe(u8, entry.path),
        });
    }
    if (markers.items.len == 0) {
        markers.deinit(alloc);
        return &.{};
    }
    return try markers.toOwnedSlice(alloc);
}

fn loadStoreStatusBackfillMarkerResumeKeys(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    markers: []StoreStatusBackfillMarker,
) !void {
    _ = try refreshStoreStatusBackfillMarkerResumeKeys(alloc, replica_root_dir, markers);
}

fn refreshStoreStatusBackfillMarkerResumeKeys(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    markers: []StoreStatusBackfillMarker,
) !bool {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var any_missing = false;

    for (markers) |*marker| {
        if (marker.resume_key) |resume_key| {
            alloc.free(resume_key);
            marker.resume_key = null;
        }
        const state_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ replica_root_dir, marker.path });
        defer alloc.free(state_path);
        marker.resume_key = std.Io.Dir.cwd().readFileAlloc(io_impl.io(), state_path, alloc, .limited(64 * 1024)) catch |err| switch (err) {
            error.FileNotFound => blk: {
                any_missing = true;
                break :blk null;
            },
            else => return err,
        };
    }
    return any_missing;
}

fn collectStoreStatusBackfillMarkers(alloc: std.mem.Allocator, replica_root_dir: []const u8) ![]StoreStatusBackfillMarker {
    const markers = try scanStoreStatusBackfillMarkers(alloc, replica_root_dir);
    errdefer freeStoreStatusBackfillMarkers(alloc, markers);
    try loadStoreStatusBackfillMarkerResumeKeys(alloc, replica_root_dir, markers);
    return markers;
}

fn storeStatusBackfillMarkersHaveMissingResumeKeys(markers: []const StoreStatusBackfillMarker) bool {
    for (markers) |marker| {
        if (marker.resume_key == null) return true;
    }
    return false;
}

fn backfillMarkerStateFileExists(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    marker: StoreStatusBackfillMarker,
) !bool {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const state_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ replica_root_dir, marker.path });
    defer alloc.free(state_path);
    std.Io.Dir.cwd().access(io_impl.io(), state_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn maybeRequestStoreStatusBackfillMarkerRescan(
    service: anytype,
    replica_root_dir: []const u8,
    scanned_backfill_markers: ?[]const StoreStatusBackfillMarker,
    active_backfill_markers: []const StoreStatusBackfillMarker,
) !void {
    if (scanned_backfill_markers == null) return;
    if (active_backfill_markers.len == 0) return;
    if (service.store_status_backfill_marker_cache.rescan_requested) return;

    for (active_backfill_markers) |marker| {
        if (marker.resume_key == null) {
            service.store_status_backfill_marker_cache.rescan_requested = true;
            return;
        }
        if (!try backfillMarkerStateFileExists(service.alloc, replica_root_dir, marker)) {
            service.store_status_backfill_marker_cache.rescan_requested = true;
            return;
        }
    }
}

fn freeStoreStatusBackfillMarkers(alloc: std.mem.Allocator, markers: []const StoreStatusBackfillMarker) void {
    for (markers) |marker| {
        alloc.free(marker.path);
        if (marker.resume_key) |resume_key| alloc.free(resume_key);
    }
    if (markers.len > 0) alloc.free(markers);
}

fn parseStoreStatusBackfillMarkerPath(path: []const u8) ?struct { store_id: ?u64, group_id: u64 } {
    const first_slash = std.mem.indexOfScalar(u8, path, '/') orelse return null;
    const first_segment = path[0..first_slash];
    if (parsePrefixedStoreStatusId(first_segment, "group-")) |group_id| {
        return .{ .store_id = null, .group_id = group_id };
    }
    const store_id = parsePrefixedStoreStatusId(first_segment, "store-") orelse return null;
    const rest = path[first_slash + 1 ..];
    const second_slash = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    const second_segment = rest[0..second_slash];
    const group_id = parsePrefixedStoreStatusId(second_segment, "group-") orelse return null;
    return .{ .store_id = store_id, .group_id = group_id };
}

fn parsePrefixedStoreStatusId(segment: []const u8, prefix: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, segment, prefix)) return null;
    return std.fmt.parseInt(u64, segment[prefix.len..], 10) catch null;
}

fn storeStatusBackfillMarkerMatchesStore(marker: StoreStatusBackfillMarker, store_id: u64, include_unassigned: bool) bool {
    if (marker.store_id) |assigned_store_id| return assigned_store_id == store_id;
    return include_unassigned;
}

fn accumulateStoreStatusBackfillProgress(
    alloc: std.mem.Allocator,
    ranges: []const metadata_table_manager.RangeRecord,
    marker: StoreStatusBackfillMarker,
    active_backfills: *u32,
    progress_sum: *f64,
) !void {
    const range = findRangeByGroupId(ranges, marker.group_id) orelse return;
    active_backfills.* += 1;
    const resume_key = marker.resume_key orelse return;
    const range_start = try internal_keys.documentRangeLowerAlloc(alloc, range.start_key);
    defer alloc.free(range_start);
    const range_end = if (range.end_key) |key| try internal_keys.documentRangeUpperAlloc(alloc, key) else null;
    defer if (range_end) |key| alloc.free(key);
    progress_sum.* += backfill_state_mod.estimateProgressForKey(
        range_start,
        if (range_end) |key| key else "",
        resume_key,
    );
}

fn freeOwnedStoreStatusReport(alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) void {
    metadata_table_manager.freeGroupStatuses(alloc, report.group_statuses);
    metadata_table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
}

fn freeOwnedStoreStatusReports(alloc: std.mem.Allocator, reports: []const metadata_table_manager.StoreStatusReport) void {
    for (reports) |report| freeOwnedStoreStatusReport(alloc, report);
    if (reports.len > 0) alloc.free(reports);
}

fn resolveSharedRootStoreAffinity(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    local_node_id: u64,
    group_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    tables: []const metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
) !u64 {
    if (findPlacementIntentStoreId(placements, group_id, local_node_id, stores)) |store_id| {
        try writeStoreAffinityFile(alloc, replica_root_dir, group_id, store_id);
        return store_id;
    }
    const existing = try readStoreAffinityFile(alloc, replica_root_dir, group_id);
    if (existing) |store_id| {
        if (findProjectedStore(stores, store_id) != null) return store_id;
    }

    const assigned = try assignSharedRootStoreAffinity(alloc, stores, tables, range);
    try writeStoreAffinityFile(alloc, replica_root_dir, group_id, assigned);
    return assigned;
}

fn readStoreAffinityFile(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
) !?u64 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/store-affinity", .{ replica_root_dir, group_id });
    defer alloc.free(path);
    const contents = std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(128)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(contents);
    const trimmed = std.mem.trim(u8, contents, " \t\r\n");
    if (trimmed.len == 0) return null;
    return std.fmt.parseInt(u64, trimmed, 10) catch null;
}

fn writeStoreAffinityFile(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    store_id: u64,
) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const dir_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ replica_root_dir, group_id });
    defer alloc.free(dir_path);
    try fs_paths.createDirPathPortable(io_impl.io(), dir_path);
    const path = try std.fmt.allocPrint(alloc, "{s}/store-affinity", .{dir_path});
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().createFile(io_impl.io(), path, .{ .truncate = true });
    defer file.close(io_impl.io());
    var buf: [64]u8 = undefined;
    var writer = file.writer(io_impl.io(), &buf);
    try writer.interface.print("{d}\n", .{store_id});
    try writer.end();
}

fn assignSharedRootStoreAffinity(
    alloc: std.mem.Allocator,
    stores: []const metadata_table_manager.StoreRecord,
    tables: []const metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
) !u64 {
    var candidates = std.ArrayListUnmanaged(metadata_table_manager.StoreRecord).empty;
    defer candidates.deinit(alloc);

    const table = findTableById(tables, range.table_id);
    if (table) |owned_table| {
        for (stores) |store| {
            if (std.mem.eql(u8, store.role, owned_table.placement_role)) {
                try candidates.append(alloc, store);
            }
        }
    }
    if (candidates.items.len == 0) {
        for (stores) |store| try candidates.append(alloc, store);
    }

    std.mem.sort(metadata_table_manager.StoreRecord, candidates.items, {}, struct {
        fn lessThan(_: void, a: metadata_table_manager.StoreRecord, b: metadata_table_manager.StoreRecord) bool {
            return a.store_id < b.store_id;
        }
    }.lessThan);

    const index = @as(usize, @intCast(range.group_id % candidates.items.len));
    return candidates.items[index].store_id;
}

fn findStoreStatusReportIndex(reports: []const metadata_table_manager.StoreStatusReport, store_id: u64) ?usize {
    for (reports, 0..) |report, i| {
        if (report.store_id == store_id) return i;
    }
    return null;
}

fn findPlacementIntentStoreId(
    placements: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    local_node_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
) ?u64 {
    for (placements) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (intent.record.local_node_id != local_node_id) continue;
        if (intent.store_id == 0) return null;
        if (findProjectedStore(stores, intent.store_id) != null) return intent.store_id;
        return null;
    }
    return null;
}

fn findTableById(
    tables: []const metadata_table_manager.TableRecord,
    table_id: u64,
) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn parseStoreStatusGroupId(path: []const u8) ?u64 {
    const slash = std.mem.indexOfScalar(u8, path, '/') orelse return null;
    const group_dir = path[0..slash];
    if (!std.mem.startsWith(u8, group_dir, "group-")) return null;
    return std.fmt.parseInt(u64, group_dir["group-".len..], 10) catch null;
}

fn findRangeByGroupId(
    ranges: []const metadata_table_manager.RangeRecord,
    group_id: u64,
) ?metadata_table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.group_id == group_id) return range;
    }
    return null;
}

fn containsLocalIntent(intents: []const raft_reconciler.PlacementIntent, group_id: u64) bool {
    for (intents) |intent| {
        if (intent.record.group_id == group_id) return true;
    }
    return false;
}

fn allocPeerNodeIdsExcludingSelf(alloc: std.mem.Allocator, voters: []const u64, local_node_id: u64) ![]u64 {
    var count: usize = 0;
    for (voters) |node_id| {
        if (node_id == local_node_id) continue;
        count += 1;
    }
    if (count == 0) return &.{};

    const peer_node_ids = try alloc.alloc(u64, count);
    var index: usize = 0;
    errdefer alloc.free(peer_node_ids);
    for (voters) |node_id| {
        if (node_id == local_node_id) continue;
        peer_node_ids[index] = node_id;
        index += 1;
    }
    return peer_node_ids;
}

pub const TableLifecycleExpectation = enum {
    present,
    absent,
};

pub const TableProjectionExpectation = struct {
    schema_json: ?[]const u8 = null,
    indexes_json: ?[]const u8 = null,
};

fn lifecycleKeyMatchesMetadataNamespace(metadata_group_id: u64, signal: metadata_storage.raft_apply_store.CommittedKeySignal) bool {
    if (signal.metadata_group_id != metadata_group_id) return false;

    var node_prefix_buf: [96]u8 = undefined;
    const node_prefix = metadata_storage.raft_apply_store.nodePrefixForGroup(&node_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, node_prefix)) return true;

    var table_prefix_buf: [96]u8 = undefined;
    const table_prefix = metadata_storage.raft_apply_store.tablePrefixForGroup(&table_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, table_prefix)) return true;

    var range_prefix_buf: [96]u8 = undefined;
    const range_prefix = metadata_storage.raft_apply_store.rangePrefixForGroup(&range_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, range_prefix)) return true;

    var store_prefix_buf: [96]u8 = undefined;
    const store_prefix = metadata_storage.raft_apply_store.storePrefixForGroup(&store_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, store_prefix)) return true;

    var placement_prefix_buf: [96]u8 = undefined;
    const placement_prefix = metadata_storage.raft_apply_store.placementPrefixForGroup(&placement_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, placement_prefix)) return true;

    var schema_progress_prefix_buf: [128]u8 = undefined;
    const schema_progress_prefix = metadata_storage.raft_apply_store.schemaProgressPrefixForGroup(&schema_progress_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, schema_progress_prefix)) return true;

    var restore_progress_prefix_buf: [128]u8 = undefined;
    const restore_progress_prefix = metadata_storage.raft_apply_store.restoreProgressPrefixForGroup(&restore_progress_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, restore_progress_prefix)) return true;

    var split_transition_prefix_buf: [128]u8 = undefined;
    const split_transition_prefix = metadata_storage.raft_apply_store.splitTransitionPrefixForGroup(&split_transition_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, split_transition_prefix)) return true;

    var merge_transition_prefix_buf: [128]u8 = undefined;
    const merge_transition_prefix = metadata_storage.raft_apply_store.mergeTransitionPrefixForGroup(&merge_transition_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, merge_transition_prefix)) return true;

    var reconcile_lease_key_buf: [96]u8 = undefined;
    const reconcile_lease_key = metadata_storage.raft_apply_store.reconcileLeaseKeyForGroup(&reconcile_lease_key_buf, metadata_group_id) catch return false;
    if (std.mem.eql(u8, signal.key, reconcile_lease_key)) return true;

    var reallocation_request_key_buf: [96]u8 = undefined;
    const reallocation_request_key = metadata_storage.raft_apply_store.reallocationRequestKeyForGroup(&reallocation_request_key_buf, metadata_group_id) catch return false;
    return std.mem.eql(u8, signal.key, reallocation_request_key);
}

fn waitForTableLifecycleConvergence(
    service: anytype,
    table_name: []const u8,
    expected: TableLifecycleExpectation,
) !void {
    const timeout_ns = 30 * std.time.ns_per_s;
    const poll_interval_ns = 10 * std.time.ns_per_ms;
    const start_ns = platform_time.monotonicNs();

    while (true) {
        var snapshot = try service.adminSnapshot();
        defer service.freeAdminSnapshot(&snapshot);

        if (tableLifecycleMatches(&snapshot, table_name, expected)) return;
        if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
        const remaining_ns = timeout_ns -| (platform_time.monotonicNs() -| start_ns);
        try awaitLifecycleProgress(service, table_name, @min(remaining_ns, poll_interval_ns));
    }
}

fn waitForTableProjectionConvergence(
    service: anytype,
    table_name: []const u8,
    expected: TableProjectionExpectation,
) !void {
    const timeout_ns = 30 * std.time.ns_per_s;
    const poll_interval_ns = 10 * std.time.ns_per_ms;
    const start_ns = platform_time.monotonicNs();

    while (true) {
        var snapshot = try service.adminSnapshot();
        defer service.freeAdminSnapshot(&snapshot);

        if (tableProjectionMatches(&snapshot, table_name, expected)) return;
        if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
        const remaining_ns = timeout_ns -| (platform_time.monotonicNs() -| start_ns);
        try awaitLifecycleProgress(service, table_name, @min(remaining_ns, poll_interval_ns));
    }
}

fn awaitLifecycleProgress(service: anytype, table_name: ?[]const u8, wait_ns: u64) !void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };

    if (@hasDecl(ServiceDeclType, "captureLifecycleSignal") and @hasDecl(ServiceDeclType, "waitForLifecycleSignal")) {
        const observed = service.captureLifecycleSignal(table_name);
        service.waitForLifecycleSignal(observed, wait_ns);
        const current = service.captureLifecycleSignal(table_name);
        if (current.global_epoch != observed.global_epoch or current.table_epoch != observed.table_epoch) return;
    }

    try service.runLifecycleRound();
    if (!(@hasDecl(ServiceDeclType, "captureLifecycleSignal") and @hasDecl(ServiceDeclType, "waitForLifecycleSignal"))) {
        platform_clock.Clock.real().sleepMs(@max(@as(u64, 1), @divFloor(wait_ns + std.time.ns_per_ms - 1, std.time.ns_per_ms)));
    }
}

fn tableLifecycleMatches(
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    expected: TableLifecycleExpectation,
) bool {
    const table = findProjectedTableByName(snapshot.tables, table_name);
    return switch (expected) {
        .present => {
            const record = table orelse return false;
            return tableRangesReady(snapshot, record.table_id);
        },
        .absent => table == null,
    };
}

fn tableProjectionMatches(
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    expected: TableProjectionExpectation,
) bool {
    const record = findProjectedTableByName(snapshot.tables, table_name) orelse return false;
    if (expected.schema_json) |schema_json| {
        const matches = jsonDocumentsEqual(std.heap.page_allocator, record.schema_json, schema_json) catch return false;
        if (!matches) return false;
    }
    if (expected.indexes_json) |indexes_json| {
        const matches = jsonDocumentsEqual(std.heap.page_allocator, record.indexes_json, indexes_json) catch return false;
        if (!matches) return false;
    }
    return true;
}

fn jsonDocumentsEqual(
    alloc: std.mem.Allocator,
    lhs_json: []const u8,
    rhs_json: []const u8,
) !bool {
    if (std.mem.eql(u8, lhs_json, rhs_json)) return true;

    var lhs = try std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{});
    defer lhs.deinit();
    var rhs = try std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{});
    defer rhs.deinit();
    return jsonValuesEqual(lhs.value, rhs.value);
}

fn jsonValuesEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;

    return switch (lhs) {
        .null => true,
        .bool => |lhs_bool| lhs_bool == rhs.bool,
        .integer => |lhs_int| lhs_int == rhs.integer,
        .float => |lhs_float| lhs_float == rhs.float,
        .number_string => |lhs_number| std.mem.eql(u8, lhs_number, rhs.number_string),
        .string => |lhs_string| std.mem.eql(u8, lhs_string, rhs.string),
        .array => |lhs_array| blk: {
            if (lhs_array.items.len != rhs.array.items.len) break :blk false;
            for (lhs_array.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValuesEqual(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |lhs_object| blk: {
            if (lhs_object.count() != rhs.object.count()) break :blk false;
            var it = lhs_object.iterator();
            while (it.next()) |entry| {
                const rhs_value = rhs.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValuesEqual(entry.value_ptr.*, rhs_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn findProjectedTableByName(
    tables: []const metadata_table_manager.TableRecord,
    table_name: []const u8,
) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn findProjectedTableById(
    tables: []const metadata_table_manager.TableRecord,
    table_id: u64,
) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn tableRangesReady(snapshot: *const metadata_api.AdminSnapshot, table_id: u64) bool {
    var range_count: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        range_count += 1;
        if (!groupReadyForTableLifecycle(snapshot, range.group_id)) return false;
    }
    return range_count > 0;
}

fn groupReadyForTableLifecycle(snapshot: *const metadata_api.AdminSnapshot, group_id: u64) bool {
    const status = findMergedGroupStatus(snapshot.merged_group_statuses, group_id) orelse return false;
    if (status.updated_at_millis == 0) return false;
    if (status.joint_consensus) return false;
    if (status.transition_pending) return false;
    if (status.replay_required and !status.replay_caught_up) return false;
    if (status.restore_pending) return false;
    return groupHasExpectedHealthyPlacement(snapshot, group_id, status);
}

fn findMergedGroupStatus(
    statuses: []const metadata_reconciler.MergedGroupStatus,
    group_id: u64,
) ?metadata_reconciler.MergedGroupStatus {
    for (statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn groupHasExpectedHealthyPlacement(
    snapshot: *const metadata_api.AdminSnapshot,
    group_id: u64,
    status: metadata_reconciler.MergedGroupStatus,
) bool {
    const expected = countPlacementIntentsForGroup(snapshot.placement_intents, group_id);
    if (status.voter_count_known) {
        if (expected > 0 and status.voter_count != expected) return false;
        return status.healthy_voter_reports >= status.voter_count;
    }
    if (expected == 0) return true;
    return countHealthyStoresReportingGroup(snapshot.stores, group_id) >= expected;
}

fn countPlacementIntentsForGroup(
    intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) u16 {
    var count: u16 = 0;
    for (intents) |intent| {
        if (intent.record.group_id == group_id) count +|= 1;
    }
    return count;
}

fn countHealthyStoresReportingGroup(
    stores: []const metadata_table_manager.StoreRecord,
    group_id: u64,
) usize {
    var count: usize = 0;
    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        for (store.group_statuses) |group_status| {
            if (group_status.group_id != group_id) continue;
            count += 1;
            break;
        }
    }
    return count;
}

fn runServiceRounds(svc: *MetadataService, count: usize) !void {
    var rounds: usize = 0;
    while (rounds < count) : (rounds += 1) try svc.runRound();
}

fn logServiceWaitTimeout(svc: *MetadataService, label: []const u8, group_id: u64, desired: raft_host.HostedReplicaStatus, rounds: usize) !void {
    const metadata_status = try svc.status();
    std.log.warn(
        "metadata test wait timed out label={s} group_id={} desired={} actual={} rounds={} local_leader={} lease_held={} lease_owner={}",
        .{
            label,
            group_id,
            desired,
            svc.raft.host.host.status(group_id),
            rounds,
            svc.raft.host.host.isLocalLeader(svc.metadata_group_id),
            metadata_status.reconcile_lease_held_by_local,
            metadata_status.reconcile_lease_owner_node_id,
        },
    );
}

fn runServiceRoundsUntilHostedStatus(
    svc: *MetadataService,
    group_id: u64,
    desired: raft_host.HostedReplicaStatus,
    max_rounds: usize,
    label: []const u8,
) !void {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        if (svc.raft.host.host.status(group_id) == desired) return;
        try svc.runRound();
    }
    try logServiceWaitTimeout(svc, label, group_id, desired, rounds);
    return error.TestExpectedEqual;
}

fn findProjectedSplit(records: []const transition_state.SplitTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findProjectedMerge(records: []const transition_state.MergeTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findProjectedStore(records: []const metadata_table_manager.StoreRecord, store_id: u64) ?metadata_table_manager.StoreRecord {
    for (records) |record| {
        if (record.store_id == store_id) return record;
    }
    return null;
}

fn findQueuedSplit(records: []const transition_state.SplitTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findQueuedMerge(records: []const transition_state.MergeTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn projectedProvisioningFingerprint(alloc: std.mem.Allocator, service: anytype) !u64 {
    var hasher = std.hash.Wyhash.init(0x8f6b4f5a2c13d9e1);

    const tables = try service.listProjectedTables(alloc);
    defer service.freeProjectedTables(alloc, tables);
    for (tables) |table| {
        hasher.update(std.mem.asBytes(&table.table_id));
        hasher.update(table.name);
        hasher.update(table.schema_json);
        hasher.update(table.read_schema_json);
        hasher.update(table.indexes_json);
        hasher.update(table.replication_sources_json);
        hasher.update(table.placement_role);
        hasher.update(table.restore_backup_id);
        hasher.update(table.restore_location);
        hasher.update(std.mem.asBytes(&table.desired_replica_count));
        hasher.update(std.mem.asBytes(&table.min_ranges));
    }

    const ranges = try service.listProjectedRanges(alloc);
    defer service.freeProjectedRanges(alloc, ranges);
    for (ranges) |range| {
        hasher.update(std.mem.asBytes(&range.group_id));
        hasher.update(std.mem.asBytes(&range.table_id));
        hasher.update(range.start_key);
        if (range.end_key) |end_key| {
            hasher.update(&.{1});
            hasher.update(end_key);
        } else {
            hasher.update(&.{0});
        }
        hasher.update(range.restore_backup_id);
        hasher.update(range.restore_location);
        hasher.update(range.restore_snapshot_path);
    }

    const placements = try service.listProjectedPlacementIntents(alloc);
    defer service.freeProjectedPlacementIntents(alloc, placements);
    for (placements) |intent| {
        hasher.update(std.mem.asBytes(&intent.record.group_id));
        hasher.update(std.mem.asBytes(&intent.record.replica_id));
        hasher.update(std.mem.asBytes(&intent.record.local_node_id));
        hasher.update(std.mem.asBytes(&@intFromEnum(intent.record.bootstrap_mode)));
        hasher.update(std.mem.asBytes(&intent.record.metadata_version));
        hasher.update(std.mem.asBytes(&intent.store_id));
        hasher.update(std.mem.asBytes(&@as(u64, intent.peer_node_ids.len)));
        for (intent.peer_node_ids) |peer_node_id| hasher.update(std.mem.asBytes(&peer_node_id));
        if (intent.record.snapshot_bootstrap) |snapshot| {
            hasher.update(&.{1});
            hasher.update(std.mem.asBytes(&snapshot.from_node_id));
            hasher.update(std.mem.asBytes(&snapshot.term));
            hasher.update(snapshot.snapshot_id);
            hasher.update(snapshot.uri);
        } else {
            hasher.update(&.{0});
        }
        if (intent.record.backup_restore_bootstrap) |restore| {
            hasher.update(&.{1});
            hasher.update(restore.backup_id);
            hasher.update(restore.location);
            hasher.update(restore.snapshot_path);
        } else {
            hasher.update(&.{0});
        }
    }

    return hasher.final();
}

pub fn snapshotStatus(
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    service: anytype,
    metrics: raft_service.ManagedServiceMetrics,
) !MetadataStatus {
    const SourceType = @TypeOf(service);
    const SourceDeclType = switch (@typeInfo(SourceType)) {
        .pointer => |pointer| pointer.child,
        else => SourceType,
    };
    const PlanningSummary = struct {
        placement_upserts: usize,
        placement_removals: usize,
        repair_placement_groups: usize,
        rebalance_placement_groups: usize,
    };
    const now_ms = realtimeNowMillis();
    const lease_stats = if (@hasDecl(SourceDeclType, "reconcileLeaseStats"))
        service.reconcileLeaseStats()
    else
        metadata_reconcile_lease.Stats{};
    const projected_reconcile_lease = if (@hasDecl(SourceDeclType, "statusProjectedReconcileLease"))
        service.statusProjectedReconcileLease(now_ms) catch null
    else if (@hasDecl(SourceDeclType, "getProjectedReconcileLease"))
        service.getProjectedReconcileLease() catch null
    else
        null;
    const projected_tables = try service.listProjectedTables(alloc);
    defer service.freeProjectedTables(alloc, projected_tables);
    const projected_ranges = try service.listProjectedRanges(alloc);
    defer service.freeProjectedRanges(alloc, projected_ranges);
    const projected_stores = try service.listProjectedStores(alloc);
    defer service.freeProjectedStores(alloc, projected_stores);
    const projected_placement_intents = try service.listProjectedPlacementIntents(alloc);
    defer service.freeProjectedPlacementIntents(alloc, projected_placement_intents);
    const projected_shuffle_join_leases = if (@hasDecl(SourceDeclType, "listProjectedShuffleJoinLeases"))
        try service.listProjectedShuffleJoinLeases(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedShuffleJoinLeases") and projected_shuffle_join_leases.len > 0) {
        service.freeProjectedShuffleJoinLeases(alloc, projected_shuffle_join_leases);
    };
    const projected_restore_progress = if (@hasDecl(SourceDeclType, "listProjectedRestoreProgress"))
        try service.listProjectedRestoreProgress(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedRestoreProgress") and projected_restore_progress.len > 0) {
        service.freeProjectedRestoreProgress(alloc, projected_restore_progress);
    };
    const projected_replication_source_statuses = if (@hasDecl(SourceDeclType, "listProjectedReplicationSourceStatuses"))
        try service.listProjectedReplicationSourceStatuses(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedReplicationSourceStatuses") and projected_replication_source_statuses.len > 0) {
        service.freeProjectedReplicationSourceStatuses(alloc, projected_replication_source_statuses);
    };
    const projected_split_transitions = try service.listProjectedSplitTransitions(alloc);
    defer service.freeProjectedSplitTransitions(alloc, projected_split_transitions);
    const projected_merge_transitions = try service.listProjectedMergeTransitions(alloc);
    defer service.freeProjectedMergeTransitions(alloc, projected_merge_transitions);
    const metadata_raft = serviceGroupRaftObservation(service, metadata_group_id);

    var preferred_stores: usize = 0;
    var constrained_stores: usize = 0;
    var overloaded_stores: usize = 0;
    var excluded_stores: usize = 0;
    var backfill_stores: usize = 0;
    var active_backfills: usize = 0;
    var projected_tables_with_replication_sources: usize = 0;
    var projected_replication_sources: usize = 0;
    var projected_replication_source_statuses_exact_cutover: usize = 0;
    var projected_replication_source_statuses_non_exact_cutover: usize = 0;
    var projected_replication_source_statuses_reseed_recommended: usize = 0;
    var projected_replication_source_statuses_exported_snapshot: usize = 0;
    var projected_replication_source_statuses_slot_first: usize = 0;
    var projected_replication_source_statuses_slot_resumed: usize = 0;
    var projected_replication_source_statuses_snapshot: usize = 0;
    var projected_replication_source_statuses_cutover_prepared: usize = 0;
    var projected_replication_source_statuses_streaming: usize = 0;
    var projected_replication_source_statuses_failed: usize = 0;
    var projected_replication_source_statuses_with_last_error: usize = 0;
    var projected_replication_source_statuses_slot_missing_failed: usize = 0;
    var projected_replication_source_statuses_retryable_failed: usize = 0;
    var projected_replication_source_statuses_terminal_failed: usize = 0;
    var projected_replication_source_statuses_with_consecutive_failures: usize = 0;
    var projected_replication_source_statuses_with_success_timestamp: usize = 0;
    var projected_replication_source_statuses_with_change_timestamp: usize = 0;
    var projected_replication_source_consecutive_failures_total: u64 = 0;
    var projected_replication_source_consecutive_failures_max: u64 = 0;
    var projected_replication_source_lag_records_total: u64 = 0;
    var projected_replication_source_lag_records_max: u64 = 0;
    var projected_replication_source_lag_millis_total: u64 = 0;
    var projected_replication_source_lag_millis_max: u64 = 0;
    var projected_replication_source_observed_lag_millis_total: u64 = 0;
    var projected_replication_source_observed_lag_millis_max: u64 = 0;
    var projected_replication_source_statuses_with_source_commit_timestamp: usize = 0;
    var projected_replication_source_last_success_at_ms_max: u64 = 0;
    var projected_replication_source_last_source_commit_at_ms_max: u64 = 0;
    var projected_replication_source_last_change_applied_at_ms_max: u64 = 0;
    var projected_snapshot_bootstrap_intents: usize = 0;
    var projected_backup_restore_bootstrap_intents: usize = 0;
    for (projected_tables) |table| {
        const source_count = countReplicationSourcesJson(alloc, table.replication_sources_json) catch 0;
        if (source_count > 0) {
            projected_tables_with_replication_sources += 1;
            projected_replication_sources += source_count;
        }
    }
    for (projected_stores) |record| {
        if (record.active_backfills > 0) backfill_stores += 1;
        active_backfills += record.active_backfills;
        switch (metadata_store_observer.classifyStore(record).tag) {
            .preferred => preferred_stores += 1,
            .constrained => constrained_stores += 1,
            .overloaded => overloaded_stores += 1,
            .excluded => excluded_stores += 1,
        }
    }
    for (projected_placement_intents) |intent| {
        if (intent.record.snapshot_bootstrap != null) projected_snapshot_bootstrap_intents += 1;
        if (intent.record.backup_restore_bootstrap != null) projected_backup_restore_bootstrap_intents += 1;
    }
    for (projected_replication_source_statuses) |status| {
        if (std.mem.eql(u8, status.cutover_mode, "exported_snapshot")) {
            projected_replication_source_statuses_exact_cutover += 1;
            projected_replication_source_statuses_exported_snapshot += 1;
        } else if (std.mem.eql(u8, status.cutover_mode, "slot_first")) {
            projected_replication_source_statuses_non_exact_cutover += 1;
            projected_replication_source_statuses_slot_first += 1;
        } else if (std.mem.eql(u8, status.cutover_mode, "slot_resumed")) {
            projected_replication_source_statuses_non_exact_cutover += 1;
            projected_replication_source_statuses_slot_resumed += 1;
        }
        if (std.mem.eql(u8, status.source_kind, "postgres") and (std.mem.eql(u8, status.cutover_mode, "slot_resumed") or std.mem.eql(u8, status.last_error, "ReplicationExactCutoverRequired"))) {
            projected_replication_source_statuses_reseed_recommended += 1;
        }
        if (std.mem.eql(u8, status.phase, "snapshot")) {
            projected_replication_source_statuses_snapshot += 1;
        } else if (std.mem.eql(u8, status.phase, "cutover_prepared")) {
            projected_replication_source_statuses_cutover_prepared += 1;
        } else if (std.mem.eql(u8, status.phase, "streaming")) {
            projected_replication_source_statuses_streaming += 1;
        } else if (std.mem.eql(u8, status.phase, "failed") or std.mem.endsWith(u8, status.phase, "_failed")) {
            projected_replication_source_statuses_failed += 1;
            if (std.mem.eql(u8, status.failure_class, "terminal")) {
                projected_replication_source_statuses_terminal_failed += 1;
            } else {
                projected_replication_source_statuses_retryable_failed += 1;
            }
        }
        if (status.last_error.len > 0) {
            projected_replication_source_statuses_with_last_error += 1;
            if (std.mem.eql(u8, status.last_error, "ForeignReplicationSlotMissing")) {
                projected_replication_source_statuses_slot_missing_failed += 1;
            }
        }
        if (status.consecutive_failures > 0) projected_replication_source_statuses_with_consecutive_failures += 1;
        if (status.last_success_at_ms > 0) projected_replication_source_statuses_with_success_timestamp += 1;
        if (status.last_change_applied_at_ms > 0) projected_replication_source_statuses_with_change_timestamp += 1;
        projected_replication_source_consecutive_failures_total +%= status.consecutive_failures;
        projected_replication_source_consecutive_failures_max = @max(
            projected_replication_source_consecutive_failures_max,
            status.consecutive_failures,
        );
        projected_replication_source_lag_records_total +%= status.lag_records;
        projected_replication_source_lag_records_max = @max(projected_replication_source_lag_records_max, status.lag_records);
        projected_replication_source_lag_millis_total +%= status.lag_millis;
        projected_replication_source_lag_millis_max = @max(projected_replication_source_lag_millis_max, status.lag_millis);
        const observed_lag_millis: u64 = if (status.last_source_commit_at_ms > 0 and now_ms > status.last_source_commit_at_ms)
            @max(status.lag_millis, now_ms - status.last_source_commit_at_ms)
        else
            status.lag_millis;
        projected_replication_source_observed_lag_millis_total +%= observed_lag_millis;
        projected_replication_source_observed_lag_millis_max = @max(projected_replication_source_observed_lag_millis_max, observed_lag_millis);
        if (status.last_source_commit_at_ms > 0) projected_replication_source_statuses_with_source_commit_timestamp += 1;
        projected_replication_source_last_success_at_ms_max = @max(
            projected_replication_source_last_success_at_ms_max,
            status.last_success_at_ms,
        );
        projected_replication_source_last_source_commit_at_ms_max = @max(
            projected_replication_source_last_source_commit_at_ms_max,
            status.last_source_commit_at_ms,
        );
        projected_replication_source_last_change_applied_at_ms_max = @max(
            projected_replication_source_last_change_applied_at_ms_max,
            status.last_change_applied_at_ms,
        );
    }

    var status_state = metadata_state.MetadataState.init(alloc);
    defer status_state.deinit();
    try status_state.syncProjected(service);
    try status_state.seedDesiredFromProjected();
    var current = try status_state.captureCurrent(service);
    defer current.deinit(alloc);
    var reconciler = metadata_reconciler.Reconciler.init(alloc);
    defer reconciler.deinit();
    var plan = try reconciler.computePlan(
        status_state.tableManager(),
        status_state.placementCandidates(),
        status_state.placementCandidateInfo(),
        current.current,
    );
    defer plan.deinit(alloc);
    const planning: PlanningSummary = .{
        .placement_upserts = plan.placement_upserts.len,
        .placement_removals = plan.placement_removals.len,
        .repair_placement_groups = plan.repair_placement_groups,
        .rebalance_placement_groups = plan.rebalance_placement_groups,
    };
    var doc_identity_lifecycle_unknown: usize = 0;
    var doc_identity_lifecycle_preserving: usize = 0;
    var doc_identity_lifecycle_reassigning: usize = 0;
    var doc_identity_lifecycle_rebuild_required: usize = 0;
    var doc_identity_lifecycle_ready: usize = 0;
    for (current.current.merged_group_statuses) |merged_status| {
        const lifecycle = metadata_reconciler.deriveDocIdentityLifecycle(merged_status);
        if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_unknown)) {
            doc_identity_lifecycle_unknown += 1;
        } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_preserving)) {
            doc_identity_lifecycle_preserving += 1;
        } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_reassigning)) {
            doc_identity_lifecycle_reassigning += 1;
        } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_rebuild_required)) {
            doc_identity_lifecycle_rebuild_required += 1;
        } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_ready)) {
            doc_identity_lifecycle_ready += 1;
        }
    }

    return .{
        .metadata_group_id = metadata_group_id,
        .metadata_raft_local_node_id = metadata_raft.local_node_id,
        .metadata_raft_role = metadata_raft.role,
        .metadata_raft_leader_id = metadata_raft.leader_id,
        .metadata_raft_term = metadata_raft.term,
        .metadata_raft_commit_index = metadata_raft.commit_index,
        .metadata_raft_local_voter = metadata_raft.local_voter,
        .metadata_raft_voter_count = metadata_raft.voter_count,
        .metadata_raft_election_elapsed = metadata_raft.election_elapsed,
        .metadata_raft_randomized_election_timeout = metadata_raft.randomized_election_timeout,
        .metadata_raft_votes_granted = metadata_raft.votes_granted,
        .metadata_raft_votes_rejected = metadata_raft.votes_rejected,
        .metadata_raft_votes_unknown = metadata_raft.votes_unknown,
        .metadata_raft_inbound_message_enqueues = metadata_raft.inbound_message_enqueues,
        .metadata_raft_inbound_message_drains = metadata_raft.inbound_message_drains,
        .metadata_raft_pending_inbound_messages = metadata_raft.pending_inbound_messages,
        .metadata_raft_transport_sent_frames = metadata_raft.transport_sent_frames,
        .metadata_raft_transport_send_failures = metadata_raft.transport_send_failures,
        .metadata_raft_transport_retries_scheduled = metadata_raft.transport_retries_scheduled,
        .metadata_raft_transport_retries_exhausted = metadata_raft.transport_retries_exhausted,
        .metadata_raft_transport_retried_successes = metadata_raft.transport_retried_successes,
        .metadata_raft_transport_peer_refreshes = metadata_raft.transport_peer_refreshes,
        .metadata_raft_transport_peer_routes = metadata_raft.transport_peer_routes,
        .metadata_raft_transport_served_groups = metadata_raft.transport_served_groups,
        .metadata_raft_transport_pending_retries = metadata_raft.transport_pending_retries,
        .metrics = metrics,
        .reconcile_lease_enabled = lease_stats.enabled,
        .reconcile_lease_owner_node_id = if (projected_reconcile_lease) |record| record.owner_node_id else lease_stats.owner_node_id,
        .reconcile_lease_expires_at_ms = if (projected_reconcile_lease) |record| record.expires_at_ms else lease_stats.expires_at_ms,
        .reconcile_lease_held_by_local = lease_stats.held_by_local,
        .reconcile_lease_acquisition_count = lease_stats.acquisition_count,
        .reconcile_lease_acquire_failures = lease_stats.acquire_failures,
        .reconcile_lease_lost_leases = lease_stats.lost_leases,
        .reconcile_lease_last_acquired_ms = lease_stats.last_acquired_ms,
        .projected_tables = projected_tables.len,
        .projected_tables_with_replication_sources = projected_tables_with_replication_sources,
        .projected_replication_sources = projected_replication_sources,
        .projected_replication_source_statuses = projected_replication_source_statuses.len,
        .projected_replication_source_statuses_exact_cutover = projected_replication_source_statuses_exact_cutover,
        .projected_replication_source_statuses_non_exact_cutover = projected_replication_source_statuses_non_exact_cutover,
        .projected_replication_source_statuses_reseed_recommended = projected_replication_source_statuses_reseed_recommended,
        .projected_replication_source_statuses_exported_snapshot = projected_replication_source_statuses_exported_snapshot,
        .projected_replication_source_statuses_slot_first = projected_replication_source_statuses_slot_first,
        .projected_replication_source_statuses_slot_resumed = projected_replication_source_statuses_slot_resumed,
        .projected_replication_source_statuses_snapshot = projected_replication_source_statuses_snapshot,
        .projected_replication_source_statuses_cutover_prepared = projected_replication_source_statuses_cutover_prepared,
        .projected_replication_source_statuses_streaming = projected_replication_source_statuses_streaming,
        .projected_replication_source_statuses_failed = projected_replication_source_statuses_failed,
        .projected_replication_source_statuses_with_last_error = projected_replication_source_statuses_with_last_error,
        .projected_replication_source_statuses_slot_missing_failed = projected_replication_source_statuses_slot_missing_failed,
        .projected_replication_source_statuses_retryable_failed = projected_replication_source_statuses_retryable_failed,
        .projected_replication_source_statuses_terminal_failed = projected_replication_source_statuses_terminal_failed,
        .projected_replication_source_statuses_with_consecutive_failures = projected_replication_source_statuses_with_consecutive_failures,
        .projected_replication_source_statuses_with_success_timestamp = projected_replication_source_statuses_with_success_timestamp,
        .projected_replication_source_statuses_with_change_timestamp = projected_replication_source_statuses_with_change_timestamp,
        .projected_replication_source_consecutive_failures_total = projected_replication_source_consecutive_failures_total,
        .projected_replication_source_consecutive_failures_max = projected_replication_source_consecutive_failures_max,
        .projected_replication_source_lag_records_total = projected_replication_source_lag_records_total,
        .projected_replication_source_lag_records_max = projected_replication_source_lag_records_max,
        .projected_replication_source_lag_millis_total = projected_replication_source_lag_millis_total,
        .projected_replication_source_lag_millis_max = projected_replication_source_lag_millis_max,
        .projected_replication_source_observed_lag_millis_total = projected_replication_source_observed_lag_millis_total,
        .projected_replication_source_observed_lag_millis_max = projected_replication_source_observed_lag_millis_max,
        .projected_replication_source_statuses_with_source_commit_timestamp = projected_replication_source_statuses_with_source_commit_timestamp,
        .projected_replication_source_last_success_at_ms_max = projected_replication_source_last_success_at_ms_max,
        .projected_replication_source_last_source_commit_at_ms_max = projected_replication_source_last_source_commit_at_ms_max,
        .projected_replication_source_last_change_applied_at_ms_max = projected_replication_source_last_change_applied_at_ms_max,
        .projected_ranges = projected_ranges.len,
        .projected_stores = projected_stores.len,
        .projected_placement_intents = projected_placement_intents.len,
        .projected_snapshot_bootstrap_intents = projected_snapshot_bootstrap_intents,
        .projected_backup_restore_bootstrap_intents = projected_backup_restore_bootstrap_intents,
        .projected_shuffle_join_leases = projected_shuffle_join_leases.len,
        .projected_restore_progress = projected_restore_progress.len,
        .projected_split_transitions = projected_split_transitions.len,
        .projected_merge_transitions = projected_merge_transitions.len,
        .projected_doc_identity_lifecycle_unknown = doc_identity_lifecycle_unknown,
        .projected_doc_identity_lifecycle_preserving = doc_identity_lifecycle_preserving,
        .projected_doc_identity_lifecycle_reassigning = doc_identity_lifecycle_reassigning,
        .projected_doc_identity_lifecycle_rebuild_required = doc_identity_lifecycle_rebuild_required,
        .projected_doc_identity_lifecycle_ready = doc_identity_lifecycle_ready,
        .preferred_stores = preferred_stores,
        .constrained_stores = constrained_stores,
        .overloaded_stores = overloaded_stores,
        .excluded_stores = excluded_stores,
        .backfill_stores = backfill_stores,
        .active_backfills = active_backfills,
        .placement_upserts = planning.placement_upserts,
        .placement_removals = planning.placement_removals,
        .repair_placement_groups = planning.repair_placement_groups,
        .rebalance_placement_groups = planning.rebalance_placement_groups,
    };
}

fn realtimeNowMillis() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => return 0,
    }
    const sec: u64 = @intCast(@max(ts.sec, 0));
    const nsec: u64 = @intCast(@max(ts.nsec, 0));
    return sec * std.time.ms_per_s + @divTrunc(nsec, std.time.ns_per_ms);
}

fn countReplicationSourcesJson(alloc: std.mem.Allocator, replication_sources_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed.deinit();
    return switch (parsed.value) {
        .array => |array| array.items.len,
        else => 0,
    };
}

test "metadata service proposes split transitions into the metadata group" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-service-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-service-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1900,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1900,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertSplitTransition(.{
        .transition_id = 4001,
        .source_group_id = 2001,
        .destination_group_id = 2002,
        .phase = .prepare,
        .split_key = "doc:m",
    });

    try runServiceRounds(&svc, 8);

    const projected = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u64, 4001), projected[0].transition_id);
    try std.testing.expectEqualStrings("doc:m", projected[0].split_key.?);
}

test "metadata service requires a configured metadata group id" {
    try std.testing.expectError(error.MissingMetadataGroupId, MetadataService.init(
        std.testing.allocator,
        .{ .host = .{ .local_node_id = 1 } },
        .{},
        .{},
    ));
}

test "metadata service status reflects reconcile lease ownership" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lease-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lease-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1915,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{
        .reconcile_lease = .{ .lease_ttl_ms = 2_000 },
    });
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1915,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });

    try svc.runRound();
    const before_leadership = try svc.status();
    try std.testing.expect(before_leadership.reconcile_lease_enabled);
    try std.testing.expect(!before_leadership.reconcile_lease_held_by_local);
    try std.testing.expectEqual(@as(u64, 0), before_leadership.reconcile_lease_owner_node_id);

    try svc.campaignMetadataGroup();
    try svc.runRound();
    try svc.runRound();

    const held_status = try svc.status();
    try std.testing.expect(held_status.reconcile_lease_enabled);
    try std.testing.expect(held_status.reconcile_lease_held_by_local);
    try std.testing.expectEqual(@as(u64, 1), held_status.reconcile_lease_owner_node_id);
    try std.testing.expect(held_status.reconcile_lease_expires_at_ms > 0);
    try std.testing.expectEqual(@as(u64, 1), held_status.reconcile_lease_acquisition_count);

    const projected_lease_opt = try svc.getProjectedReconcileLease();
    try std.testing.expect(projected_lease_opt != null);
    const projected_lease = projected_lease_opt.?;
    try std.testing.expectEqual(@as(u64, 1), projected_lease.owner_node_id);
    try std.testing.expect(projected_lease.expires_at_ms >= held_status.reconcile_lease_expires_at_ms);
}

test "metadata snapshot status prefers cached reconcile lease accessor when available" {
    const FakeService = struct {
        cached_calls: usize = 0,
        direct_calls: usize = 0,

        pub fn reconcileLeaseStats(_: *@This()) metadata_reconcile_lease.Stats {
            return .{ .enabled = true };
        }

        pub fn statusProjectedReconcileLease(self: *@This(), _: u64) !?metadata_reconcile_lease.ReconcileLeaseRecord {
            self.cached_calls += 1;
            return .{
                .owner_node_id = 9,
                .expires_at_ms = 1234,
            };
        }

        pub fn getProjectedReconcileLease(self: *@This()) !?metadata_reconcile_lease.ReconcileLeaseRecord {
            self.direct_calls += 1;
            return .{
                .owner_node_id = 77,
                .expires_at_ms = 9999,
            };
        }

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            return try alloc.alloc(metadata_table_manager.TableRecord, 0);
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            return try alloc.alloc(metadata_table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedStores(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
            return try alloc.alloc(metadata_table_manager.StoreRecord, 0);
        }

        pub fn freeProjectedStores(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedShuffleJoinLeases(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
            return try alloc.alloc(metadata_table_manager.ShuffleJoinLeaseRecord, 0);
        }

        pub fn freeProjectedShuffleJoinLeases(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.ShuffleJoinLeaseRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedRestoreProgress(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
            return try alloc.alloc(metadata_table_manager.RestoreProgressRecord, 0);
        }

        pub fn freeProjectedRestoreProgress(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedReplicationSourceStatuses(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
            return try alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, 0);
        }

        pub fn freeProjectedReplicationSourceStatuses(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            return try alloc.alloc(transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            return try alloc.alloc(transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?transition_state.SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?transition_state.MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(_: *@This(), _: *const metadata_reconciler.ReconciliationPlan) !void {}
    };

    var service = FakeService{};
    const status = try snapshotStatus(std.testing.allocator, 77, &service, .{});
    try std.testing.expectEqual(@as(usize, 1), service.cached_calls);
    try std.testing.expectEqual(@as(usize, 0), service.direct_calls);
    try std.testing.expectEqual(@as(u64, 9), status.reconcile_lease_owner_node_id);
    try std.testing.expectEqual(@as(u64, 1234), status.reconcile_lease_expires_at_ms);
}

test "metadata service can apply reconciliation plan proposals" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-plan-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-plan-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1910,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1910,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var manager = metadata_table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 2101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 2102,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try manager.requestSplit(.{
        .transition_id = 9101,
        .table_id = 10,
        .source_group_id = 2101,
        .destination_group_id = 2103,
        .split_key = "doc:h",
    });

    var reconciler = metadata_reconciler.Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{});
    defer plan.deinit(std.testing.allocator);

    try svc.applyReconciliationPlan(&plan);
    try runServiceRounds(&svc, 8);

    const split_records = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, split_records);
    try std.testing.expectEqual(@as(usize, 1), split_records.len);
    try std.testing.expectEqual(@as(u64, 9101), split_records[0].transition_id);
}

test "metadata control loop can drive the real metadata service" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-loop-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-loop-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1920,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1920,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertTable(.{ .table_id = 20, .name = "docs" });
    try svc.upsertRange(.{ .group_id = 2201, .table_id = 20, .start_key = "doc:a", .end_key = "doc:m" });
    try svc.upsertRange(.{ .group_id = 2202, .table_id = 20, .start_key = "doc:m", .end_key = "doc:z" });
    try runServiceRounds(&svc, 8);

    var loop = metadata_control_loop.MetadataControlLoop.init(std.testing.allocator);
    defer loop.deinit();
    try loop.stateRef().syncProjected(&svc);
    try loop.stateRef().seedDesiredFromProjected();
    try loop.stateRef().tableManager().requestSplit(.{
        .transition_id = 9201,
        .table_id = 20,
        .source_group_id = 2201,
        .destination_group_id = 2203,
        .split_key = "doc:h",
    });

    const summary = try svc.reconcileOnceEnsuringLease(&loop);
    try std.testing.expectEqual(@as(usize, 1), summary.split_upserts);

    try runServiceRounds(&svc, 8);

    const split_records = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, split_records);
    try std.testing.expectEqual(@as(usize, 1), split_records.len);
    try std.testing.expectEqual(@as(u64, 9201), split_records[0].transition_id);
}

test "metadata service projects committed table and range topology" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1930,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1930,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertTable(.{
        .table_id = 77,
        .name = "docs",
        .description = "docs table",
        .schema_json = "{\"kind\":\"demo\"}",
        .indexes_json = "{\"default\":{}}",
        .replication_sources_json = "[\"seed\"]",
        .desired_replica_count = 5,
        .min_ranges = 2,
    });
    try svc.upsertRange(.{ .group_id = 7701, .table_id = 77, .start_key = "doc:a", .end_key = "doc:z" });

    try runServiceRounds(&svc, 8);

    const tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, tables);
    const ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 77), tables[0].table_id);
    try std.testing.expectEqualStrings("docs", tables[0].name);
    try std.testing.expectEqualStrings("docs table", tables[0].description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", tables[0].schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", tables[0].indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", tables[0].replication_sources_json);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 7701), ranges[0].group_id);
}

test "table workflow can drive real metadata service topology and split setup" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-workflow-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-workflow-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1940,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1940,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();

    const create_summary = try workflow.createTable(&svc, .{
        .table_id = 88,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 8801,
        .table_id = 88,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expectEqual(@as(usize, 1), create_summary.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), create_summary.range_upserts);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, tables);
    const ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);

    try workflow.bootstrapDesiredFromCommitted(&svc);
    const split_summary = try workflow.requestSplit(&svc, .{
        .transition_id = 9401,
        .table_id = 88,
        .source_group_id = 8801,
        .destination_group_id = 8802,
        .split_key = "doc:m",
    });
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const splits = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 9401), splits[0].transition_id);
}

test "metadata service projects committed placement intents into local hosted replicas" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1950,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1950,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 1951,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .empty,
        },
        .peer_node_ids = &.{ 1, 2, 3 },
    });

    try runServiceRoundsUntilHostedStatus(&svc, 1951, .active, 8, "placement-intent activation");

    const projected = try svc.listProjectedPlacementIntents(std.testing.allocator);
    defer svc.freeProjectedPlacementIntents(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u64, 1951), projected[0].record.group_id);

    try svc.removeReplicaIntent(1951, 1);
    try runServiceRoundsUntilHostedStatus(&svc, 1951, .absent, 8, "placement-intent removal");
}

test "table workflow can drive placement intents through the real metadata control loop" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-control-loop-placement-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-control-loop-placement-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1960,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1960,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try workflow.setPlacementCandidates(&.{ 1, 2, 3 });

    const create_summary = try workflow.createTable(&svc, .{
        .table_id = 99,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 9901,
        .table_id = 99,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expectEqual(@as(usize, 1), create_summary.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), create_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 3), create_summary.placement_upserts);

    try runServiceRoundsUntilHostedStatus(&svc, 9901, .active, 12, "table-workflow placement activation");

    const intents = try svc.listProjectedPlacementIntents(std.testing.allocator);
    defer svc.freeProjectedPlacementIntents(std.testing.allocator, intents);
    try std.testing.expectEqual(@as(usize, 3), intents.len);
}

test "metadata service reports store status without losing placement attributes" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1970,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1970,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{
        .store_id = 11,
        .node_id = 1,
        .role = "data",
        .health_class = "healthy",
        .failure_domain = "rack-a",
        .live = true,
        .drain_requested = true,
        .capacity_bytes = 1024,
        .available_bytes = 800,
    });
    try svc.runRound();

    try svc.reportStoreStatus(.{
        .store_id = 11,
        .live = false,
        .health_class = "degraded",
        .capacity_bytes = 2048,
        .available_bytes = 0,
        .lease_pressure = 92,
        .read_load = 210,
        .write_load = 130,
        .active_backfills = 2,
        .backfill_progress_millis = 375,
    });
    try svc.runRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u64, 11), projected[0].store_id);
    try std.testing.expectEqual(@as(u64, 1), projected[0].node_id);
    try std.testing.expect(std.mem.eql(u8, projected[0].role, "data"));
    try std.testing.expect(std.mem.eql(u8, projected[0].failure_domain, "rack-a"));
    try std.testing.expect(std.mem.eql(u8, projected[0].health_class, "degraded"));
    try std.testing.expectEqual(false, projected[0].live);
    try std.testing.expect(projected[0].drain_requested);
    try std.testing.expectEqual(@as(u64, 2048), projected[0].capacity_bytes);
    try std.testing.expectEqual(@as(u64, 0), projected[0].available_bytes);
    try std.testing.expectEqual(@as(u32, 92), projected[0].lease_pressure);
    try std.testing.expectEqual(@as(u32, 210), projected[0].read_load);
    try std.testing.expectEqual(@as(u32, 130), projected[0].write_load);
    try std.testing.expectEqual(@as(u32, 2), projected[0].active_backfills);
    try std.testing.expectEqual(@as(u16, 375), projected[0].backfill_progress_millis);

    const status = try svc.status();
    try std.testing.expectEqual(@as(usize, 1), status.backfill_stores);
    try std.testing.expectEqual(@as(usize, 2), status.active_backfills);
}

test "metadata service batches store status reports" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-batch-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-batch-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1971,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1971,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 21, .node_id = 1, .role = "data", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 22, .node_id = 2, .role = "data", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try svc.runRound();

    try std.testing.expectEqual(@as(usize, 2), try svc.reportStoreStatuses(&.{
        .{ .store_id = 21, .live = false, .health_class = "degraded", .capacity_bytes = 1024, .available_bytes = 0, .lease_pressure = 95, .read_load = 140, .write_load = 110, .active_backfills = 1, .backfill_progress_millis = 250 },
        .{ .store_id = 22, .live = true, .health_class = "healthy", .capacity_bytes = 2048, .available_bytes = 1200, .lease_pressure = 8, .read_load = 18, .write_load = 12, .active_backfills = 0, .backfill_progress_millis = 1000 },
    }));
    try svc.runRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 2), projected.len);
    const first = metadata_store_observer.findStoreIndex(projected, 21).?;
    const second = metadata_store_observer.findStoreIndex(projected, 22).?;
    try std.testing.expect(std.mem.eql(u8, projected[first].failure_domain, "rack-a"));
    try std.testing.expectEqual(false, projected[first].live);
    try std.testing.expect(std.mem.eql(u8, projected[first].health_class, "degraded"));
    try std.testing.expectEqual(@as(u64, 0), projected[first].available_bytes);
    try std.testing.expectEqual(@as(u32, 95), projected[first].lease_pressure);
    try std.testing.expectEqual(@as(u32, 140), projected[first].read_load);
    try std.testing.expectEqual(@as(u32, 110), projected[first].write_load);
    try std.testing.expectEqual(@as(u32, 1), projected[first].active_backfills);
    try std.testing.expectEqual(@as(u16, 250), projected[first].backfill_progress_millis);
    try std.testing.expect(std.mem.eql(u8, projected[second].failure_domain, "rack-b"));
    try std.testing.expectEqual(true, projected[second].live);
    try std.testing.expect(std.mem.eql(u8, projected[second].health_class, "healthy"));
    try std.testing.expectEqual(@as(u64, 2048), projected[second].capacity_bytes);
    try std.testing.expectEqual(@as(u64, 1200), projected[second].available_bytes);
    try std.testing.expectEqual(@as(u32, 8), projected[second].lease_pressure);
    try std.testing.expectEqual(@as(u32, 18), projected[second].read_load);
    try std.testing.expectEqual(@as(u32, 12), projected[second].write_load);
    try std.testing.expectEqual(@as(u32, 0), projected[second].active_backfills);
    try std.testing.expectEqual(@as(u16, 1000), projected[second].backfill_progress_millis);

    const status = try svc.status();
    try std.testing.expectEqual(@as(usize, 1), status.backfill_stores);
    try std.testing.expectEqual(@as(usize, 1), status.active_backfills);
}

test "metadata service persists and clears reallocation requests" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reallocation-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reallocation-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1972,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1972,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.requestReallocation(77_000);
    try svc.runRound();
    const requested = try svc.getProjectedReallocationRequest();
    try std.testing.expect(requested != null);
    try std.testing.expectEqual(@as(u64, 77_000), requested.?.requested_at_ms);

    var plan = metadata_reconciler.ReconciliationPlan.empty();
    plan.clear_reallocation_request = true;
    try svc.applyReconciliationPlan(&plan);
    try svc.runRound();
    try std.testing.expect((try svc.getProjectedReallocationRequest()) == null);
}

test "metadata service auto-reports local store backfill status during runRound" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1974,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1974,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 51, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertTable(.{ .table_id = 88, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 8801, .table_id = 88, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-8801/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u32, 1), projected[0].active_backfills);
    try std.testing.expect(projected[0].backfill_progress_millis > 0);
}

test "metadata service reports automatic store status across shared multi-store roots" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-multi-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-multi-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1975,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1975,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 61, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 62, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 });
    try svc.upsertTable(.{ .table_id = 89, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 8901, .table_id = 89, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-8901/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 2), projected.len);
    const first = metadata_store_observer.findStoreIndex(projected, 61).?;
    const second = metadata_store_observer.findStoreIndex(projected, 62).?;
    const total = projected[first].active_backfills + projected[second].active_backfills;
    try std.testing.expectEqual(@as(u32, 1), total);
    try std.testing.expect(projected[first].backfill_progress_millis > 0 or projected[second].backfill_progress_millis > 0);

    const affinity_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-8901/store-affinity", .{replica_root});
    defer std.testing.allocator.free(affinity_path);
    const affinity_contents = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), affinity_path, std.testing.allocator, .limited(128));
    defer std.testing.allocator.free(affinity_contents);
    const affinity_store_id = try std.fmt.parseInt(u64, std.mem.trim(u8, affinity_contents, " \t\r\n"), 10);
    try std.testing.expect(affinity_store_id == 61 or affinity_store_id == 62);
}

test "metadata service reports automatic store status across explicit multi-store roots" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-explicit-roots", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-explicit-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1976,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1976,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 71, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 72, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 });
    try svc.upsertTable(.{ .table_id = 90, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 9001, .table_id = 90, .start_key = "doc:a", .end_key = "doc:m" });
    try svc.upsertRange(.{ .group_id = 9002, .table_id = 90, .start_key = "doc:m", .end_key = "doc:z" });
    try svc.runRound();

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const left_db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/store-71/group-9001/table-db", .{replica_root});
    defer std.testing.allocator.free(left_db_path);
    const right_db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/store-72/group-9002/table-db", .{replica_root});
    defer std.testing.allocator.free(right_db_path);
    try fs_paths.createDirPathPortable(io_impl.io(), left_db_path);
    try fs_paths.createDirPathPortable(io_impl.io(), right_db_path);

    {
        var left_db = try db_mod.DB.open(std.testing.allocator, left_db_path, .{});
        defer left_db.close();
        try left_db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }
    {
        var right_db = try db_mod.DB.open(std.testing.allocator, right_db_path, .{});
        defer right_db.close();
        try right_db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const left_state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{left_db_path});
    defer std.testing.allocator.free(left_state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), left_state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:g");
        try writer.end();
    }

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 2), projected.len);
    const first = metadata_store_observer.findStoreIndex(projected, 71).?;
    const second = metadata_store_observer.findStoreIndex(projected, 72).?;
    try std.testing.expectEqual(@as(u32, 1), projected[first].active_backfills);
    try std.testing.expect(projected[first].backfill_progress_millis > 0);
    try std.testing.expectEqual(@as(u32, 0), projected[second].active_backfills);
}

test "metadata service prefers placement-role-compatible store affinity in shared roots" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-role-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-role-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1977,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1977,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 81, .node_id = 1, .role = "hot", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 82, .node_id = 1, .role = "cold", .live = true, .capacity_bytes = 1024, .available_bytes = 880 });
    try svc.upsertTable(.{ .table_id = 91, .name = "docs", .placement_role = "cold", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 9101, .table_id = 91, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9101/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    const hot_index = metadata_store_observer.findStoreIndex(projected, 81).?;
    const cold_index = metadata_store_observer.findStoreIndex(projected, 82).?;
    try std.testing.expectEqual(@as(u32, 0), projected[hot_index].active_backfills);
    try std.testing.expectEqual(@as(u32, 1), projected[cold_index].active_backfills);
}

test "metadata service shared-root reports survive transient rebuild marker removal" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-transient-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9301/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    const markers = try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root);
    defer freeStoreStatusBackfillMarkers(std.testing.allocator, markers);
    try std.testing.expectEqual(@as(usize, 1), markers.len);

    try std.Io.Dir.cwd().deleteFile(io_impl.io(), state_path);

    const stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 93, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 },
        .{ .store_id = 94, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 980 },
    };
    const placements = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 9301,
                .replica_id = 1,
                .local_node_id = 1,
                .bootstrap_mode = .persisted,
            },
            .store_id = 94,
            .peer_node_ids = &.{1},
        },
    };
    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 93, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 9301, .table_id = 93, .start_key = "doc:a", .end_key = "doc:z" },
    };

    const projected = try collectSharedRootLocalStoreStatusReports(
        .{},
        std.testing.allocator,
        replica_root,
        1,
        stores[0..],
        placements[0..],
        tables[0..],
        ranges[0..],
        &.{},
        markers,
    );
    defer freeOwnedStoreStatusReports(std.testing.allocator, projected);
    const first_index = findStoreStatusReportIndex(projected, 93).?;
    const second_index = findStoreStatusReportIndex(projected, 94).?;
    try std.testing.expectEqual(@as(u32, 0), projected[first_index].active_backfills);
    try std.testing.expectEqual(@as(u32, 1), projected[second_index].active_backfills);
}

test "metadata service lifecycle round uses cached backfill markers" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-store-status-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-store-status-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1978,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1978,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 101, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertTable(.{ .table_id = 101, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 10101, .table_id = 101, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-10101/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    svc.store_status_backfill_marker_cache.replace(
        std.testing.allocator,
        try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root),
        monotonicMs(),
    );
    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_marker_cache.markers.len);
    svc.store_status_ticks = 39;
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), state_path);

    try svc.runLifecycleRound();
    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_marker_cache.markers.len);
    try std.testing.expect(svc.store_status_backfill_marker_cache.rescan_requested);
    try std.testing.expectEqual(@as(usize, 39), svc.store_status_ticks);
}

test "metadata service lifecycle round discovers backfill markers immediately" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-discovery-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-discovery-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1979,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1979,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 102, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertTable(.{ .table_id = 102, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 10201, .table_id = 102, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-10201/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();

    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_marker_cache.markers.len);
    try svc.runLifecycleRound();
    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u32, 1), projected[0].active_backfills);
}

test "metadata service lifecycle round backs off empty backfill probes after initial scan" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-empty-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-empty-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1980,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1980,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    const first_scanned_at_ms = svc.store_status_backfill_marker_cache.scanned_at_ms;
    try std.testing.expect(first_scanned_at_ms > 0);

    try svc.runLifecycleRound();
    try std.testing.expectEqual(first_scanned_at_ms, svc.store_status_backfill_marker_cache.scanned_at_ms);
    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_probe_ticks);

    try svc.runLifecycleRound();
    try std.testing.expectEqual(first_scanned_at_ms, svc.store_status_backfill_marker_cache.scanned_at_ms);
    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    try std.testing.expectEqual(@as(usize, 2), svc.store_status_backfill_probe_ticks);
}

test "metadata service cached backfill markers rescan immediately after disappearance" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-store-status-rescan-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9401/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    var cache = StoreStatusBackfillMarkerCache{};
    defer cache.deinit(std.testing.allocator);
    cache.replace(
        std.testing.allocator,
        try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root),
        monotonicMs(),
    );
    try std.testing.expectEqual(@as(usize, 1), cache.markers.len);
    try std.testing.expect(!cache.rescan_requested);

    try std.Io.Dir.cwd().deleteFile(io_impl.io(), state_path);

    const FakeService = struct {
        alloc: std.mem.Allocator,
        store_status_backfill_marker_cache: StoreStatusBackfillMarkerCache,
    };
    var service = FakeService{
        .alloc = std.testing.allocator,
        .store_status_backfill_marker_cache = cache,
    };
    cache = .{};
    defer service.store_status_backfill_marker_cache.deinit(std.testing.allocator);

    for (service.store_status_backfill_marker_cache.markers) |marker| {
        try std.testing.expect(!try backfillMarkerStateFileExists(std.testing.allocator, replica_root, marker));
    }
    try maybeRequestStoreStatusBackfillMarkerRescan(
        &service,
        replica_root,
        service.store_status_backfill_marker_cache.markers,
        service.store_status_backfill_marker_cache.markers,
    );
    try std.testing.expect(service.store_status_backfill_marker_cache.rescan_requested);
    var probe_ticks: usize = 0;
    try maybeRefreshStoreStatusBackfillMarkerCache(
        std.testing.allocator,
        replica_root,
        0,
        &probe_ticks,
        &service.store_status_backfill_marker_cache,
    );
    try std.testing.expectEqual(@as(usize, 0), service.store_status_backfill_marker_cache.markers.len);
}

test "metadata service does not rescan empty backfill markers before idle interval" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-empty-backfill-marker-cache", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), replica_root);

    var cache = StoreStatusBackfillMarkerCache{};
    defer cache.deinit(std.testing.allocator);
    cache.scanned_at_ms = monotonicMs();
    const scanned_at_ms = cache.scanned_at_ms;

    var probe_ticks: usize = store_status_backfill_probe_interval_ticks;
    try maybeRefreshStoreStatusBackfillMarkerCache(
        std.testing.allocator,
        replica_root,
        40,
        &probe_ticks,
        &cache,
    );

    try std.testing.expectEqual(@as(usize, 0), cache.markers.len);
    try std.testing.expectEqual(scanned_at_ms, cache.scanned_at_ms);
    try std.testing.expectEqual(store_status_backfill_probe_interval_ticks, probe_ticks);
}

test "metadata service prefers planned store affinity in shared roots" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-planned-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9201/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    {
        var file = try std.Io.Dir.cwd().createFile(io_impl.io(), state_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [128]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll("doc:m");
        try writer.end();
    }

    const stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 91, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 },
        .{ .store_id = 92, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 980 },
    };
    const placements = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 9201,
                .replica_id = 1,
                .local_node_id = 1,
                .bootstrap_mode = .persisted,
            },
            .store_id = 92,
            .peer_node_ids = &.{1},
        },
    };
    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 92, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 9201, .table_id = 92, .start_key = "doc:a", .end_key = "doc:z" },
    };
    const markers = try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root);
    defer freeStoreStatusBackfillMarkers(std.testing.allocator, markers);

    const projected = try collectSharedRootLocalStoreStatusReports(
        .{},
        std.testing.allocator,
        replica_root,
        1,
        stores[0..],
        placements[0..],
        tables[0..],
        ranges[0..],
        &.{},
        markers,
    );
    defer freeOwnedStoreStatusReports(std.testing.allocator, projected);
    const first_index = findStoreStatusReportIndex(projected, 91).?;
    const second_index = findStoreStatusReportIndex(projected, 92).?;
    try std.testing.expectEqual(@as(u32, 0), projected[first_index].active_backfills);
    try std.testing.expectEqual(@as(u32, 1), projected[second_index].active_backfills);

    const affinity_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9201/store-affinity", .{replica_root});
    defer std.testing.allocator.free(affinity_path);
    const affinity_contents = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), affinity_path, std.testing.allocator, .limited(128));
    defer std.testing.allocator.free(affinity_contents);
    const affinity_store_id = try std.fmt.parseInt(u64, std.mem.trim(u8, affinity_contents, " \t\r\n"), 10);
    try std.testing.expectEqual(@as(u64, 92), affinity_store_id);
}

test "metadata service status reports repair and rebalance counts" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-status-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-status-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1972,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1972,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 31, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 32, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try svc.runRound();

    try svc.upsertTable(.{
        .table_id = 88,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]",
        .desired_replica_count = 3,
        .min_ranges = 1,
    });
    try svc.upsertRange(.{
        .group_id = 8801,
        .table_id = 88,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try svc.upsertReplicationSourceStatus(.{
        .table_id = 88,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .phase = "snapshot",
        .checkpoint = "lsn:0/16B6A50",
        .cutover_mode = "slot_first",
        .lag_records = 12,
        .updated_at_ms = 500,
    });
    try svc.runRound();

    const repair_status = try svc.status();
    try std.testing.expectEqual(@as(u64, 1972), repair_status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_tables);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_tables_with_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_exact_cutover);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses_non_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_reseed_recommended);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_exported_snapshot);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses_slot_first);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_slot_resumed);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses_snapshot);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_cutover_prepared);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_streaming);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_last_error);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_slot_missing_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_retryable_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_terminal_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_consecutive_failures);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_success_timestamp);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_change_timestamp);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_consecutive_failures_total);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_consecutive_failures_max);
    try std.testing.expectEqual(@as(u64, 12), repair_status.projected_replication_source_lag_records_total);
    try std.testing.expectEqual(@as(u64, 12), repair_status.projected_replication_source_lag_records_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_lag_millis_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_observed_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_observed_lag_millis_max);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_source_commit_timestamp);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_last_success_at_ms_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_last_source_commit_at_ms_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_last_change_applied_at_ms_max);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_ranges);
    try std.testing.expectEqual(@as(usize, 2), repair_status.projected_stores);
    try std.testing.expectEqual(@as(usize, 0), repair_status.rebalance_placement_groups);
    try std.testing.expectEqual(@as(usize, 1), repair_status.repair_placement_groups);

    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 8801,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .fetch_snapshot,
            .snapshot_bootstrap = .{
                .from_node_id = 41,
                .term = 7,
                .snapshot_id = "snap-8801",
                .uri = "http://node-41/snapshots/snap-8801",
            },
        },
        .peer_node_ids = &.{2},
    });
    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 8801,
            .replica_id = 2,
            .local_node_id = 2,
            .bootstrap_mode = .fetch_snapshot,
            .backup_restore_bootstrap = .{
                .backup_id = "backup-8801",
                .location = "file:///tmp/backups",
                .snapshot_path = "backup-8801/groups/8801",
            },
        },
        .peer_node_ids = &.{1},
    });
    try svc.runRound();

    try svc.reportStoreStatus(.{
        .store_id = 31,
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 900,
        .lease_pressure = 98,
        .read_load = 220,
        .write_load = 160,
    });
    try svc.upsertStore(.{ .store_id = 33, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try svc.upsertReplicationSourceStatus(.{
        .table_id = 88,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .phase = "streaming_failed",
        .checkpoint = "lsn:0/16B6A90",
        .cutover_mode = "slot_first",
        .last_error = "network timeout",
        .failure_class = "retryable",
        .lag_records = 7,
        .lag_millis = 45,
        .consecutive_failures = 3,
        .last_source_commit_at_ms = 555,
        .last_success_at_ms = 600,
        .last_change_applied_at_ms = 650,
        .updated_at_ms = 700,
    });
    try svc.runRound();

    const rebalance_status = try svc.status();
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.overloaded_stores);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.rebalance_placement_groups);
    try std.testing.expectEqual(@as(usize, 3), rebalance_status.placement_upserts);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.placement_removals);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_snapshot_bootstrap_intents);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_backup_restore_bootstrap_intents);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_tables_with_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_exact_cutover);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_non_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_reseed_recommended);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_exported_snapshot);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_slot_first);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_slot_resumed);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_snapshot);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_cutover_prepared);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_streaming);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_failed);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_last_error);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_slot_missing_failed);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_retryable_failed);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_terminal_failed);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_consecutive_failures);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_success_timestamp);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_change_timestamp);
    try std.testing.expectEqual(@as(u64, 3), rebalance_status.projected_replication_source_consecutive_failures_total);
    try std.testing.expectEqual(@as(u64, 3), rebalance_status.projected_replication_source_consecutive_failures_max);
    try std.testing.expectEqual(@as(u64, 7), rebalance_status.projected_replication_source_lag_records_total);
    try std.testing.expectEqual(@as(u64, 7), rebalance_status.projected_replication_source_lag_records_max);
    try std.testing.expectEqual(@as(u64, 45), rebalance_status.projected_replication_source_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 45), rebalance_status.projected_replication_source_lag_millis_max);
    try std.testing.expect(rebalance_status.projected_replication_source_observed_lag_millis_total >= rebalance_status.projected_replication_source_lag_millis_total);
    try std.testing.expect(rebalance_status.projected_replication_source_observed_lag_millis_max >= rebalance_status.projected_replication_source_lag_millis_max);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_source_commit_timestamp);
    try std.testing.expectEqual(@as(u64, 600), rebalance_status.projected_replication_source_last_success_at_ms_max);
    try std.testing.expectEqual(@as(u64, 555), rebalance_status.projected_replication_source_last_source_commit_at_ms_max);
    try std.testing.expectEqual(@as(u64, 650), rebalance_status.projected_replication_source_last_change_applied_at_ms_max);
}

test "metadata service admin snapshot captures projected topology and status" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-admin-snapshot-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-admin-snapshot-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1973,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1973,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 41, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 42, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try svc.upsertTable(.{ .table_id = 89, .name = "docs", .desired_replica_count = 3, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 8901, .table_id = 89, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.upsertReplicationSourceStatus(.{
        .table_id = 89,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .cutover_mode = "exported_snapshot",
        .slot_name = "antfly_postgres_users_docs",
        .publication_name = "antfly_pub_postgres_users_docs",
        .phase = "streaming",
        .checkpoint = "lsn:0/16B6B10",
        .snapshot_offset = 2,
        .prepared_checkpoint = "lsn:0/16B6A50",
        .stream_checkpoint = "lsn:0/16B6B10",
        .lag_records = 3,
        .updated_at_ms = 777,
    });
    try svc.runRound();

    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);

    try std.testing.expectEqual(@as(u64, 1973), snapshot.status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.ranges.len);
    try std.testing.expectEqual(@as(usize, 2), snapshot.stores.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.placement_intents.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.replication_source_statuses.len);
    try std.testing.expectEqualStrings("postgres", snapshot.replication_source_statuses[0].source_kind);
    try std.testing.expectEqualStrings("users", snapshot.replication_source_statuses[0].external_table);
    try std.testing.expectEqualStrings("exported_snapshot", snapshot.replication_source_statuses[0].cutover_mode);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", snapshot.replication_source_statuses[0].slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", snapshot.replication_source_statuses[0].publication_name);
    try std.testing.expectEqual(@as(u64, 2), snapshot.replication_source_statuses[0].snapshot_offset);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", snapshot.replication_source_statuses[0].prepared_checkpoint);
    try std.testing.expectEqualStrings("lsn:0/16B6B10", snapshot.replication_source_statuses[0].stream_checkpoint);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.repair_placement_groups);
}

test "metadata service committed metadata changes request lifecycle reconcile hook" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const HookCapture = struct {
        calls: usize = 0,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lifecycle-hook-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lifecycle-hook-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2048,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2048,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();

    var capture = HookCapture{};
    svc.setLifecycleReconcileHook(.{
        .ptr = &capture,
        .vtable = &.{
            .run = HookCapture.run,
        },
    });

    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);

    capture.calls = 0;
    try svc.upsertTable(.{ .table_id = 99, .name = "docs" });
    try svc.upsertRange(.{ .group_id = 9901, .table_id = 99, .start_key = "", .end_key = null });
    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);

    capture.calls = 0;
    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 9901,
            .local_node_id = 1,
            .replica_id = 1,
        },
        .store_id = 0,
        .peer_node_ids = &.{},
    });
    try svc.requestReallocation(1);
    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);
}

test "metadata service clears restore intent once all placement replicas report restore progress" {
    const FakeService = struct {
        alloc: std.mem.Allocator,
        table: metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        placements: []const raft_reconciler.PlacementIntent,
        progress: []const metadata_table_manager.RestoreProgressRecord,
        upserted_table: ?metadata_table_manager.TableRecord = null,
        upserted_range: ?metadata_table_manager.RangeRecord = null,

        fn deinit(self: *@This()) void {
            if (self.upserted_table) |record| metadata_table_manager.freeTable(self.alloc, record);
            if (self.upserted_range) |record| metadata_table_manager.freeRange(self.alloc, record);
        }

        fn listProjectedTables(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = try metadata_table_manager.cloneTable(alloc, self.table);
            return out;
        }

        fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| metadata_table_manager.freeTable(alloc, record);
            alloc.free(records);
        }

        fn listProjectedRanges(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, self.ranges.len);
            for (self.ranges, 0..) |record, i| out[i] = try metadata_table_manager.cloneRange(alloc, record);
            return out;
        }

        fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
            for (records) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(records);
        }

        fn listProjectedPlacementIntents(self: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const out = try alloc.alloc(raft_reconciler.PlacementIntent, self.placements.len);
            for (self.placements, 0..) |intent, i| {
                out[i] = .{
                    .record = intent.record,
                    .store_id = intent.store_id,
                    .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.peer_node_ids),
                };
            }
            return out;
        }

        fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, records: []raft_reconciler.PlacementIntent) void {
            for (records) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            alloc.free(records);
        }

        fn listProjectedRestoreProgress(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
            const out = try alloc.alloc(metadata_table_manager.RestoreProgressRecord, self.progress.len);
            for (self.progress, 0..) |record, i| out[i] = try metadata_table_manager.cloneRestoreProgress(alloc, record);
            return out;
        }

        fn freeProjectedRestoreProgress(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
            for (records) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
            alloc.free(records);
        }

        fn upsertTable(self: *@This(), record: metadata_table_manager.TableRecord) !void {
            if (self.upserted_table) |existing| metadata_table_manager.freeTable(self.alloc, existing);
            self.upserted_table = try metadata_table_manager.cloneTable(self.alloc, record);
        }

        fn upsertRange(self: *@This(), record: metadata_table_manager.RangeRecord) !void {
            if (self.upserted_range) |existing| metadata_table_manager.freeRange(self.alloc, existing);
            self.upserted_range = try metadata_table_manager.cloneRange(self.alloc, record);
        }
    };

    var service = FakeService{
        .alloc = std.testing.allocator,
        .table = .{
            .table_id = 7,
            .name = "docs",
            .restore_backup_id = "snap1",
            .restore_location = "file:///tmp/backups",
        },
        .ranges = &.{
            .{
                .group_id = 7001,
                .table_id = 7,
                .start_key = "",
                .end_key = null,
                .restore_backup_id = "snap1",
                .restore_location = "file:///tmp/backups",
                .restore_snapshot_path = "snap/groups/7001",
            },
        },
        .placements = &.{
            .{ .record = .{ .group_id = 7001, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
            .{ .record = .{ .group_id = 7001, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
        },
        .progress = &.{
            .{ .table_id = 7, .node_id = 1, .group_id = 7001, .backup_id = "snap1", .snapshot_path = "snap/groups/7001", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
            .{ .table_id = 7, .node_id = 2, .group_id = 7001, .backup_id = "snap1", .snapshot_path = "snap/groups/7001", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
        },
    };
    defer service.deinit();

    try completeRestoreIntentsForService(&service, null, null, null, null);
    try std.testing.expect(service.upserted_range != null);
    try std.testing.expectEqualStrings("", service.upserted_range.?.restore_backup_id);
    try std.testing.expectEqualStrings("", service.upserted_range.?.restore_location);
    try std.testing.expectEqualStrings("", service.upserted_range.?.restore_snapshot_path);
    try std.testing.expect(service.upserted_table != null);
    try std.testing.expectEqualStrings("", service.upserted_table.?.restore_backup_id);
    try std.testing.expectEqualStrings("", service.upserted_table.?.restore_location);
}

test "metadata service keeps restore intent until runtime repair completes" {
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 7001, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 7001, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
    };
    const progress = [_]metadata_table_manager.RestoreProgressRecord{
        .{ .table_id = 7, .node_id = 1, .group_id = 7001, .backup_id = "snap1", .snapshot_path = "snap/groups/7001", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
        .{ .table_id = 7, .node_id = 2, .group_id = 7001, .backup_id = "snap1", .snapshot_path = "snap/groups/7001", .primary_restored = true, .runtime_repair_complete = false, .phase = "runtime_repair" },
    };

    try std.testing.expect(!rangeRestoreIntentComplete(7, 7001, "snap1", &placements, &progress));
}

test "metadata http service projected tables cache invalidates without prior runRound registration" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataHttpService.init(std.testing.allocator, .{
        .http = .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 2900,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
            .transport = .{
                .snapshot = .{ .root_dir = snapshot_root },
            },
        },
    }, .{
        .http = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory.iface(),
                },
            },
        },
    }, .{
        .observe_local_replica_root = false,
    });
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2900,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();

    try std.testing.expectEqual(false, svc.lifecycle_listener_registered);

    const before = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, before);
    try std.testing.expectEqual(@as(usize, 0), before.len);
    try std.testing.expectEqual(true, svc.lifecycle_listener_registered);
    try std.testing.expectEqual(svc.projection_epoch.load(.monotonic), svc.projected_core_snapshot_cache.projection_epoch);

    const epoch_before = svc.projection_epoch.load(.monotonic);
    MetadataHttpService.metadataHttpServiceProjectionSignal(&svc, .{
        .kind = .table,
        .metadata_group_id = 2900,
        .table_name = "docs",
        .table_id = 77,
    });
    const epoch_after_signal = svc.projection_epoch.load(.monotonic);
    try std.testing.expect(epoch_after_signal > epoch_before);
    try std.testing.expect(svc.projected_core_snapshot_cache.projection_epoch < epoch_after_signal);

    const after = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
    try std.testing.expectEqual(epoch_after_signal, svc.projected_core_snapshot_cache.projection_epoch);
}

test "metadata http projected clone helpers clean up on allocation failure" {
    const Runner = struct {
        fn freePlacementIntents(alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            for (intents) |intent| {
                if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            }
            alloc.free(intents);
        }

        fn run(alloc: std.mem.Allocator) !void {
            const tables = [_]metadata_table_manager.TableRecord{
                .{
                    .table_id = 1,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "{\"kind\":\"demo\"}",
                    .indexes_json = "{\"default\":{}}",
                    .replication_sources_json = "[\"seed\"]",
                },
            };
            const ranges = [_]metadata_table_manager.RangeRecord{
                .{
                    .group_id = 11,
                    .table_id = 1,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                },
            };
            const stores = [_]metadata_table_manager.StoreRecord{
                .{
                    .store_id = 4,
                    .node_id = 1,
                    .role = "data",
                    .health_class = "healthy",
                    .failure_domain = "rack-a",
                    .live = true,
                    .capacity_bytes = 1024,
                    .available_bytes = 512,
                },
            };
            const placements = [_]raft_reconciler.PlacementIntent{
                .{
                    .record = .{
                        .group_id = 11,
                        .replica_id = 1,
                        .local_node_id = 1,
                        .bootstrap_mode = .persisted,
                    },
                    .store_id = 4,
                    .peer_node_ids = &.{ 2, 3 },
                },
            };
            const split_transitions = [_]transition_state.SplitTransitionRecord{
                .{
                    .transition_id = 91,
                    .source_group_id = 11,
                    .destination_group_id = 12,
                    .phase = .prepare,
                    .split_key = "doc:m",
                    .source_range_end = "doc:z",
                    .rollback_reason = "none",
                },
            };
            const merge_transitions = [_]transition_state.MergeTransitionRecord{
                .{
                    .transition_id = 92,
                    .donor_group_id = 12,
                    .receiver_group_id = 11,
                    .phase = .prepare,
                    .rollback_reason = "none",
                },
            };

            const cloned_tables = try cloneProjectedTablesOwned(alloc, &tables);
            defer {
                for (cloned_tables) |record| metadata_table_manager.freeTable(alloc, record);
                alloc.free(cloned_tables);
            }

            const cloned_ranges = try cloneProjectedRangesOwned(alloc, &ranges);
            defer {
                for (cloned_ranges) |record| metadata_table_manager.freeRange(alloc, record);
                alloc.free(cloned_ranges);
            }

            const cloned_stores = try cloneProjectedStoresOwned(alloc, &stores);
            defer {
                for (cloned_stores) |record| metadata_table_manager.freeStore(alloc, record);
                alloc.free(cloned_stores);
            }

            const cloned_placements = try cloneProjectedPlacementIntentsOwned(alloc, &placements);
            defer freePlacementIntents(alloc, cloned_placements);

            const cloned_splits = try cloneProjectedSplitTransitionsOwned(alloc, &split_transitions);
            defer {
                for (cloned_splits) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
                alloc.free(cloned_splits);
            }

            const cloned_merges = try cloneProjectedMergeTransitionsOwned(alloc, &merge_transitions);
            defer {
                for (cloned_merges) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
                alloc.free(cloned_merges);
            }
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "metadata service local replica root reconcile permit hook defers reconcile work" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const HookCapture = struct {
        calls: usize = 0,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };

    const PermitCapture = struct {
        allow: bool = false,

        fn shouldReconcile(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.allow;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reconcile-permit-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reconcile-permit-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 4096,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 4096,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();

    try svc.upsertTable(.{ .table_id = 99, .name = "docs" });
    try svc.upsertRange(.{ .group_id = 9901, .table_id = 99, .start_key = "", .end_key = null });
    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 9901,
            .local_node_id = 1,
            .replica_id = 1,
        },
        .store_id = 0,
        .peer_node_ids = &.{},
    });

    var capture = HookCapture{};
    var permit = PermitCapture{ .allow = false };
    svc.setLocalReplicaRootReconcileHook(.{
        .ptr = &capture,
        .vtable = &.{ .run = HookCapture.run },
    });
    svc.setLocalReplicaRootReconcilePermitHook(.{
        .ptr = &permit,
        .vtable = &.{ .should_reconcile = PermitCapture.shouldReconcile },
    });

    try runServiceRounds(&svc, 8);
    try std.testing.expectEqual(@as(usize, 0), capture.calls);

    permit.allow = true;
    svc.local_table_provisioning_epoch = null;
    svc.local_table_provisioning_group_ids_fingerprint = null;
    svc.last_local_table_provisioning_refresh_at_ms = 0;
    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);
}
