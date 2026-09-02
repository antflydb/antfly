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

//! Storage-free facade for the compiled metadata-Raft apply/projection owner.
//! Each projection crosses as one owned control-plane JSON envelope; backend
//! transactions, cursors, and individual records remain inside the kernel.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const contract = @import("../metadata/storage/raft_apply_contract.zig");
const metadata = @import("../metadata/domain.zig");
const metadata_incarnation = @import("../metadata/incarnation.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const raft_state_machine = @import("../raft/state_machine/mod.zig");
const extension_domain = @import("../extensions/mod.zig");

pub const RaftApplyStoreConfig = struct {
    root_dir: []const u8,
    map_size: usize = 16 * 1024 * 1024,
    no_sync: bool = false,
    read_only: bool = false,
    flush_threshold: usize = 64,
    context: ?*anyopaque = null,
};

pub const AppliedMetadataBatch = contract.AppliedMetadataBatch;
pub const TableTransitionFence = contract.TableTransitionFence;
pub const TableRestoreAdmission = contract.TableRestoreAdmission;
pub const TableDropProjection = contract.TableDropProjection;
pub const ProjectionSignalKind = contract.ProjectionSignalKind;
pub const ProjectionSignal = contract.ProjectionSignal;
pub const ProjectionListener = contract.ProjectionListener;
pub const CommittedKeySignal = contract.CommittedKeySignal;
pub const CommittedKeyListener = contract.CommittedKeyListener;

pub const MaintenanceStats = struct {
    mutable_bytes: u64 = 0,
    immutable_bytes: u64 = 0,
    total_run_bytes: u64 = 0,
    wal_retained_bytes: u64 = 0,
    wal_retained_segments: u64 = 0,
    active_readers: u64 = 0,
    obsolete_paths: u64 = 0,
    obsolete_paths_pinned_by_readers: u64 = 0,
    obsolete_paths_pinned_by_versions: u64 = 0,
    bulk_ingest_current_scan_clone_active_bytes: u64 = 0,
};

pub const CatalogProjectionSnapshot = contract.CatalogProjectionSnapshot;
pub const CatalogCursor = contract.CatalogCursor;

const ListenerRegistration = struct {
    projection: ?ProjectionListener = null,
    committed_key: ?CommittedKeyListener = null,
};

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    handle: ?*anyopaque,
    listeners: std.ArrayListUnmanaged(*ListenerRegistration) = .empty,
    latest_batches: std.AutoHashMapUnmanaged(u64, AppliedMetadataBatch) = .empty,
    latest_batches_mutex: std.Io.Mutex = .init,

    pub const RestoreJobRow = struct {
        key: []u8,
        value: []u8,
    };

    pub fn init(alloc: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        _ = cfg.map_size;
        _ = cfg.flush_threshold;
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_metadata_apply_store_open(&.{
            .no_sync = @intFromBool(cfg.no_sync),
            .read_only = @intFromBool(cfg.read_only),
            .context = cfg.context,
            .root_dir = .fromSlice(cfg.root_dir),
        }, &handle));
        return .{
            .alloc = alloc,
            .handle = handle orelse return error.StorageKernelFailure,
        };
    }

    pub fn deinit(self: *RaftApplyStore) void {
        abi.antfly_metadata_apply_store_close(self.handle);
        for (self.listeners.items) |listener| self.alloc.destroy(listener);
        self.listeners.deinit(self.alloc);
        var batches = self.latest_batches.valueIterator();
        while (batches.next()) |batch| self.alloc.free(@constCast(batch.entries_bytes));
        self.latest_batches.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn snapshotBuilder(self: *RaftApplyStore) raft_state_machine.SnapshotBuilder {
        return .{ .ptr = self, .vtable = &.{
            .build_snapshot = buildSnapshot,
            .prepare_snapshot = prepareSnapshot,
            .install_snapshot = installSnapshot,
            .apply_batch = applyBatch,
        } };
    }

    fn applyBatch(ptr: *anyopaque, batch: raft_state_machine.ApplyBatch) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try statusToError(abi.antfly_metadata_apply_store_apply_batch(self.handle, &.{
            .group_id = batch.group_id,
            .commit_index = batch.commit_index,
            .entries = .fromSlice(batch.entries_bytes),
        }));
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_build_snapshot(self.handle, &.{
            .group_id = group_id,
        }, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return try alloc.dupe(u8, response.slice());
    }

    fn installSnapshot(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        commit_index: u64,
        snapshot: []const u8,
    ) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try statusToError(abi.antfly_metadata_apply_store_install_snapshot(self.handle, &.{
            .group_id = group_id,
            .commit_index = commit_index,
            .snapshot = .fromSlice(snapshot),
        }));
    }

    const PreparedSnapshot = struct {
        handle: ?*anyopaque,

        fn source(self: *PreparedSnapshot) @import("raft_engine").runtime.storage_iface.SnapshotSource {
            return .{ .ptr = self, .vtable = &.{
                .materialize = materialize,
                .cancel = cancel,
                .deinit = destroy,
            } };
        }

        fn materialize(ptr: *anyopaque, alloc: std.mem.Allocator) !@import("raft_engine").runtime.storage_iface.SnapshotMaterialization {
            const self: *PreparedSnapshot = @ptrCast(@alignCast(ptr));
            var response: abi.OwnedBytes = .{};
            try statusToError(abi.antfly_metadata_apply_prepared_snapshot_materialize(self.handle, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            return .{ .bytes = try alloc.dupe(u8, response.slice()) };
        }

        fn cancel(ptr: *anyopaque) void {
            const self: *PreparedSnapshot = @ptrCast(@alignCast(ptr));
            std.debug.assert(abi.antfly_metadata_apply_prepared_snapshot_cancel(self.handle) == .ok);
        }

        fn destroy(ptr: *anyopaque) void {
            const self: *PreparedSnapshot = @ptrCast(@alignCast(ptr));
            abi.antfly_metadata_apply_prepared_snapshot_destroy(self.handle);
            std.heap.page_allocator.destroy(self);
        }
    };

    fn prepareSnapshot(ptr: *anyopaque, group_id: u64, applied_index: u64) !?@import("raft_engine").runtime.storage_iface.SnapshotSource {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_metadata_apply_store_prepare_snapshot(self.handle, &.{
            .group_id = group_id,
            .applied_index = applied_index,
        }, &handle));
        const value = handle orelse return null;
        const prepared = try std.heap.page_allocator.create(PreparedSnapshot);
        prepared.* = .{ .handle = value };
        return prepared.source();
    }

    fn projection(self: *RaftApplyStore, comptime T: type, request: abi.MetadataProjectionRequest) !T {
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_projection(self.handle, &request, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return std.json.parseFromSliceLeaky(T, self.alloc, response.slice(), .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.StorageKernelFailure,
        };
    }

    fn projectionWithAllocator(self: *RaftApplyStore, comptime T: type, alloc: std.mem.Allocator, request: abi.MetadataProjectionRequest) !T {
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_projection(self.handle, &request, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return std.json.parseFromSliceLeaky(T, alloc, response.slice(), .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.StorageKernelFailure,
        };
    }

    pub fn latestBatch(self: *RaftApplyStore, group_id: u64) !?AppliedMetadataBatch {
        const decoded = (try self.projectionWithAllocator(
            ?AppliedMetadataBatch,
            self.alloc,
            .{ .kind = .latest_batch, .group_id = group_id },
        )) orelse return null;
        var decoded_owned = true;
        defer if (decoded_owned) self.alloc.free(@constCast(decoded.entries_bytes));

        self.latest_batches_mutex.lockUncancelable(std.Options.debug_io);
        defer self.latest_batches_mutex.unlock(std.Options.debug_io);
        const entry = try self.latest_batches.getOrPut(self.alloc, group_id);
        if (entry.found_existing) {
            if (entry.value_ptr.commit_index == decoded.commit_index and
                std.mem.eql(u8, entry.value_ptr.entries_bytes, decoded.entries_bytes))
            {
                return entry.value_ptr.*;
            }
            self.alloc.free(@constCast(entry.value_ptr.entries_bytes));
        }
        entry.value_ptr.* = decoded;
        decoded_owned = false;
        return entry.value_ptr.*;
    }

    pub fn getMetadataIncarnation(self: *RaftApplyStore, group_id: u64) !?metadata_incarnation.MetadataClusterIncarnation {
        return try self.projection(?metadata_incarnation.MetadataClusterIncarnation, .{ .kind = .metadata_incarnation, .group_id = group_id });
    }

    pub fn getRuntimeStatusProtocolActivationVersion(self: *RaftApplyStore, group_id: u64) !u16 {
        return try self.projection(u16, .{ .kind = .runtime_status_protocol_activation_version, .group_id = group_id });
    }

    pub fn listPlacementVersionFences(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) ![]metadata_reconciler.PlacementVersionFence {
        return try self.projectionWithAllocator(
            []metadata_reconciler.PlacementVersionFence,
            alloc,
            .{ .kind = .placement_version_fences, .group_id = group_id },
        );
    }

    pub fn listSplitTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SplitTransitionRecord {
        return try self.projectionWithAllocator([]metadata.SplitTransitionRecord, alloc, .{ .kind = .split_transitions, .group_id = group_id });
    }

    pub fn listPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]raft_reconciler.PlacementIntent {
        return try self.projectionWithAllocator([]raft_reconciler.PlacementIntent, alloc, .{ .kind = .placement_intents, .group_id = group_id });
    }

    pub fn listLocalPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, node_id: u64) ![]raft_reconciler.PlacementIntent {
        return try self.projectionWithAllocator([]raft_reconciler.PlacementIntent, alloc, .{ .kind = .local_placement_intents, .group_id = group_id, .arg0 = node_id });
    }

    pub fn freePlacementIntents(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []raft_reconciler.PlacementIntent) void {
        for (values) |value| raft_reconciler.freeIntentOwned(alloc, value);
        alloc.free(values);
    }

    pub fn listNodes(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.NodeRecord {
        return try self.projectionWithAllocator([]metadata.NodeRecord, alloc, .{ .kind = .nodes, .group_id = group_id });
    }

    pub fn freeNodes(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.NodeRecord) void {
        for (values) |value| metadata_table_manager.freeNode(alloc, value);
        alloc.free(values);
    }

    pub fn listStores(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.StoreRecord {
        return try self.projectionWithAllocator([]metadata.StoreRecord, alloc, .{ .kind = .stores, .group_id = group_id });
    }

    pub fn freeStores(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.StoreRecord) void {
        for (values) |value| metadata_table_manager.freeStore(alloc, value);
        alloc.free(values);
    }

    pub fn listMergeTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.MergeTransitionRecord {
        return try self.projectionWithAllocator([]metadata.MergeTransitionRecord, alloc, .{ .kind = .merge_transitions, .group_id = group_id });
    }

    pub fn listTables(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.TableRecord {
        return try self.projectionWithAllocator([]metadata.TableRecord, alloc, .{ .kind = .tables, .group_id = group_id });
    }

    pub fn captureCatalogProjection(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        deadline_ns: ?u64,
    ) !CatalogProjectionSnapshot {
        return self.projectionWithAllocator(CatalogProjectionSnapshot, alloc, .{
            .kind = .catalog_projection,
            .group_id = group_id,
            .arg0 = deadline_ns orelse 0,
        }) catch |err| switch (err) {
            error.Timeout => error.CatalogRoutingSnapshotTimeout,
            else => err,
        };
    }

    pub fn captureCatalogCursor(self: *RaftApplyStore, group_id: u64) !CatalogCursor {
        return try self.projection(CatalogCursor, .{
            .kind = .catalog_cursor,
            .group_id = group_id,
        });
    }

    pub fn freeTables(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.TableRecord) void {
        for (values) |value| metadata_table_manager.freeTable(alloc, value);
        alloc.free(values);
    }

    pub fn getTable(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, table_id: u64) !?metadata.TableRecord {
        return try self.projectionWithAllocator(?metadata.TableRecord, alloc, .{ .kind = .table, .group_id = group_id, .arg0 = table_id });
    }

    pub fn getRange(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, range_group_id: u64) !?metadata.RangeRecord {
        return try self.projectionWithAllocator(?metadata.RangeRecord, alloc, .{
            .kind = .range,
            .group_id = group_id,
            .arg0 = range_group_id,
        });
    }

    pub fn captureTableDropProjection(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?TableDropProjection {
        return self.projectionWithAllocator(?TableDropProjection, alloc, .{
            .kind = .table_drop_projection,
            .group_id = group_id,
            .key = .fromSlice(table_name),
        }) catch |err| switch (err) {
            error.InvalidArgument => error.InvalidDerivedCatalogIndex,
            else => err,
        };
    }

    pub fn captureTableCreateGeneration(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_id: u64,
    ) !u64 {
        _ = alloc;
        return self.projection(u64, .{
            .kind = .table_create_generation,
            .group_id = group_id,
            .arg0 = table_id,
        }) catch |err| switch (err) {
            error.InvalidArgument => error.InvalidDerivedCatalogIndex,
            else => err,
        };
    }

    pub fn captureTableRestoreAdmission(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected_table: metadata.TableRecord,
    ) !TableRestoreAdmission {
        const request_json = try std.json.Stringify.valueAlloc(alloc, expected_table, .{});
        defer alloc.free(request_json);
        return self.projection(TableRestoreAdmission, .{
            .kind = .table_restore_admission,
            .group_id = group_id,
            .key = .fromSlice(request_json),
        }) catch |err| switch (err) {
            error.InvalidArgument => error.InvalidDerivedCatalogIndex,
            else => err,
        };
    }

    pub fn verifyTableCreateProjectionExact(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        expected_table: metadata.TableRecord,
        expected_ranges: []const metadata.RangeRecord,
    ) !void {
        const request_json = try std.json.Stringify.valueAlloc(alloc, .{
            .table = expected_table,
            .ranges = expected_ranges,
        }, .{});
        defer alloc.free(request_json);
        _ = self.projection(bool, .{
            .kind = .verify_table_create_projection,
            .group_id = group_id,
            .key = .fromSlice(request_json),
        }) catch |err| switch (err) {
            error.InvalidArgument => return error.InvalidDerivedCatalogIndex,
            else => return err,
        };
    }

    pub fn getTableTransitionFence(self: *RaftApplyStore, group_id: u64, table_id: u64) !TableTransitionFence {
        return try self.projection(TableTransitionFence, .{ .kind = .table_transition_fence, .group_id = group_id, .arg0 = table_id });
    }

    pub fn listSchemaProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SchemaProgressRecord {
        return try self.projectionWithAllocator([]metadata.SchemaProgressRecord, alloc, .{ .kind = .schema_progress, .group_id = group_id });
    }

    pub fn freeSchemaProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.SchemaProgressRecord) void {
        alloc.free(values);
    }

    pub fn listRestoreProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RestoreProgressRecord {
        return try self.projectionWithAllocator([]metadata.RestoreProgressRecord, alloc, .{ .kind = .restore_progress, .group_id = group_id });
    }

    pub fn listActiveRestoreRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RangeRecord {
        return self.projectionWithAllocator(
            []metadata.RangeRecord,
            alloc,
            .{ .kind = .active_restore_ranges, .group_id = group_id },
        ) catch |err| switch (err) {
            error.InvalidArgument => error.InvalidDerivedCatalogIndex,
            else => err,
        };
    }

    pub fn ensureDerivedCatalogIndexes(self: *RaftApplyStore, group_id: u64) !void {
        _ = try self.projection(bool, .{ .kind = .ensure_derived_catalog_indexes, .group_id = group_id });
    }

    pub fn rebuildDerivedCatalogIndexes(self: *RaftApplyStore, group_id: u64) !void {
        _ = try self.projection(bool, .{ .kind = .rebuild_derived_catalog_indexes, .group_id = group_id });
    }

    pub fn freeRestoreProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.RestoreProgressRecord) void {
        for (values) |value| metadata_table_manager.freeRestoreProgress(alloc, value);
        alloc.free(values);
    }

    pub fn listReplicationSourceStatuses(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ReplicationSourceStatusRecord {
        return try self.projectionWithAllocator([]metadata.ReplicationSourceStatusRecord, alloc, .{ .kind = .replication_source_statuses, .group_id = group_id });
    }

    pub fn freeReplicationSourceStatuses(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.ReplicationSourceStatusRecord) void {
        for (values) |value| metadata_table_manager.freeReplicationSourceStatus(alloc, value);
        alloc.free(values);
    }

    pub fn getReplicationSourceStatus(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, table_id: u64, ordinal: u32) !?metadata.ReplicationSourceStatusRecord {
        return try self.projectionWithAllocator(?metadata.ReplicationSourceStatusRecord, alloc, .{ .kind = .replication_source_status, .group_id = group_id, .arg0 = table_id, .arg1 = ordinal });
    }

    pub fn listExtensionPackages(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.PackageManifest {
        return try self.projectionWithAllocator([]extension_domain.PackageManifest, alloc, .{ .kind = .extension_packages, .group_id = group_id });
    }

    pub fn freeExtensionPackages(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []extension_domain.PackageManifest) void {
        for (values) |*value| value.deinitOwned(alloc);
        alloc.free(values);
    }

    pub fn listInstalledExtensions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.InstalledExtension {
        return try self.projectionWithAllocator([]extension_domain.InstalledExtension, alloc, .{ .kind = .installed_extensions, .group_id = group_id });
    }

    pub fn freeInstalledExtensions(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []extension_domain.InstalledExtension) void {
        for (values) |*value| value.deinitOwned(alloc);
        alloc.free(values);
    }

    pub fn listExtensionMembers(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.ExtensionMember {
        return try self.projectionWithAllocator([]extension_domain.ExtensionMember, alloc, .{ .kind = .extension_members, .group_id = group_id });
    }

    pub fn freeExtensionMembers(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []extension_domain.ExtensionMember) void {
        for (values) |*value| value.deinitOwned(alloc);
        alloc.free(values);
    }

    pub fn listExtensionDependencies(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]extension_domain.ExtensionDependency {
        return try self.projectionWithAllocator([]extension_domain.ExtensionDependency, alloc, .{ .kind = .extension_dependencies, .group_id = group_id });
    }

    pub fn extensionLifecycleDeltaApplied(
        self: *RaftApplyStore,
        alloc: std.mem.Allocator,
        group_id: u64,
        delta: anytype,
    ) !bool {
        const request_json = try std.json.Stringify.valueAlloc(alloc, delta, .{});
        defer alloc.free(request_json);
        return try self.projection(bool, .{
            .kind = .extension_lifecycle_delta_applied,
            .group_id = group_id,
            .key = .fromSlice(request_json),
        });
    }

    pub fn freeExtensionDependencies(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []extension_domain.ExtensionDependency) void {
        for (values) |*value| value.deinitOwned(alloc);
        alloc.free(values);
    }

    pub fn listShuffleJoinLeases(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ShuffleJoinLeaseRecord {
        return try self.projectionWithAllocator([]metadata.ShuffleJoinLeaseRecord, alloc, .{ .kind = .shuffle_join_leases, .group_id = group_id });
    }

    pub fn freeShuffleJoinLeases(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.ShuffleJoinLeaseRecord) void {
        alloc.free(values);
    }

    pub fn listRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RangeRecord {
        return try self.projectionWithAllocator([]metadata.RangeRecord, alloc, .{ .kind = .ranges, .group_id = group_id });
    }

    pub fn freeRanges(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.RangeRecord) void {
        for (values) |value| metadata_table_manager.freeRange(alloc, value);
        alloc.free(values);
    }

    pub fn freeSplitTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.SplitTransitionRecord) void {
        for (values) |value| metadata_table_manager.freeSplitTransitionRecord(alloc, value);
        alloc.free(values);
    }

    pub fn freeMergeTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, values: []metadata.MergeTransitionRecord) void {
        for (values) |value| metadata_table_manager.freeMergeTransitionRecord(alloc, value);
        alloc.free(values);
    }

    pub fn getReconcileLease(self: *RaftApplyStore, group_id: u64) !?metadata.ReconcileLeaseRecord {
        return try self.projection(?metadata.ReconcileLeaseRecord, .{ .kind = .reconcile_lease, .group_id = group_id });
    }

    pub fn getReallocationRequest(self: *RaftApplyStore, group_id: u64) !?metadata.ReallocationRequestRecord {
        return try self.projection(?metadata.ReallocationRequestRecord, .{ .kind = .reallocation_request, .group_id = group_id });
    }

    pub fn getShuffleJoinLease(self: *RaftApplyStore, group_id: u64, job_id: u64) !?metadata.ShuffleJoinLeaseRecord {
        return try self.projection(?metadata.ShuffleJoinLeaseRecord, .{ .kind = .shuffle_join_lease, .group_id = group_id, .arg0 = job_id });
    }

    pub fn listRestoreJobRows(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]RestoreJobRow {
        return try self.projectionWithAllocator([]RestoreJobRow, alloc, .{ .kind = .restore_job_rows, .group_id = group_id });
    }

    pub fn getRestoreJobValue(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, key: []const u8) !?[]u8 {
        return try self.projectionWithAllocator(?[]u8, alloc, .{ .kind = .restore_job_value, .group_id = group_id, .key = .fromSlice(key) });
    }

    pub fn freeRestoreJobRows(_: *RaftApplyStore, alloc: std.mem.Allocator, rows: []RestoreJobRow) void {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }

    pub fn snapshotMaintenanceStats(self: *const RaftApplyStore) MaintenanceStats {
        return @constCast(self).projection(MaintenanceStats, .{ .kind = .maintenance_stats }) catch .{};
    }

    fn projectionCallback(context: ?*anyopaque, signal: *const abi.MetadataProjectionSignal) callconv(.c) void {
        const registration: *ListenerRegistration = @ptrCast(@alignCast(context orelse return));
        const listener = registration.projection orelse return;
        listener.onProjectionSignal(.{
            .kind = switch (signal.kind) {
                .metadata_incarnation => .metadata_incarnation,
                .table => .table,
                .range => .range,
                .store => .store,
                .placement_intent => .placement_intent,
                .reconcile_lease => .reconcile_lease,
                .shuffle_join_lease => .shuffle_join_lease,
                .split_transition => .split_transition,
                .merge_transition => .merge_transition,
                .schema_progress => .schema_progress,
                .restore_progress => .restore_progress,
                .restore_job => .restore_job,
                .replication_source_status => .replication_source_status,
            },
            .metadata_group_id = signal.metadata_group_id,
            .table_name = if (signal.table_name.len == 0) null else signal.table_name.slice(),
            .table_id = signal.table_id,
            .group_id = signal.group_id,
            .store_id = signal.store_id,
            .node_id = signal.node_id,
        });
    }

    fn beforeProjectionCommitCallback(context: ?*anyopaque) callconv(.c) void {
        const registration: *ListenerRegistration = @ptrCast(@alignCast(context orelse return));
        const listener = registration.projection orelse return;
        listener.beginCommitBarrier();
    }

    fn afterProjectionCommitCallback(context: ?*anyopaque) callconv(.c) void {
        const registration: *ListenerRegistration = @ptrCast(@alignCast(context orelse return));
        const listener = registration.projection orelse return;
        listener.endCommitBarrier();
    }

    fn committedKeyCallback(context: ?*anyopaque, group_id: u64, key: abi.BorrowedBytes) callconv(.c) void {
        const registration: *ListenerRegistration = @ptrCast(@alignCast(context orelse return));
        const listener = registration.committed_key orelse return;
        listener.onCommittedKey(.{ .metadata_group_id = group_id, .key = key.slice() });
    }

    fn addListeners(self: *RaftApplyStore, projection_listener: ?ProjectionListener, committed_listener: ?CommittedKeyListener) !void {
        if (projection_listener) |listener| try listener.validate();
        try self.listeners.ensureUnusedCapacity(self.alloc, 1);
        const registration = try self.alloc.create(ListenerRegistration);
        errdefer self.alloc.destroy(registration);
        registration.* = .{ .projection = projection_listener, .committed_key = committed_listener };
        const commit_barrier_kind = if (projection_listener) |listener| listener.commit_barrier_kind else null;
        try statusToError(abi.antfly_metadata_apply_store_add_listeners(self.handle, &.{
            .context = registration,
            .projection_fn = if (projection_listener != null) projectionCallback else null,
            .committed_key_fn = if (committed_listener != null) committedKeyCallback else null,
            .commit_barrier_kind = if (commit_barrier_kind) |kind| switch (kind) {
                .metadata_incarnation => .metadata_incarnation,
                .table => .table,
                .range => .range,
                .store => .store,
                .placement_intent => .placement_intent,
                .reconcile_lease => .reconcile_lease,
                .shuffle_join_lease => .shuffle_join_lease,
                .split_transition => .split_transition,
                .merge_transition => .merge_transition,
                .schema_progress => .schema_progress,
                .restore_progress => .restore_progress,
                .restore_job => .restore_job,
                .replication_source_status => .replication_source_status,
            } else .table,
            .has_commit_barrier_kind = @intFromBool(commit_barrier_kind != null),
            .before_projection_commit_fn = if (commit_barrier_kind != null) beforeProjectionCommitCallback else null,
            .after_projection_commit_fn = if (commit_barrier_kind != null) afterProjectionCommitCallback else null,
        }));
        self.listeners.appendAssumeCapacity(registration);
    }

    pub fn addProjectionListener(self: *RaftApplyStore, listener: ProjectionListener) !void {
        try self.addListeners(listener, null);
    }

    pub fn addCommittedKeyListener(self: *RaftApplyStore, listener: CommittedKeyListener) !void {
        try self.addListeners(null, listener);
    }

    pub fn addLifecycleListeners(self: *RaftApplyStore, projection_listener: ProjectionListener, committed_listener: CommittedKeyListener) !void {
        try self.addListeners(projection_listener, committed_listener);
    }
};

fn statusToError(status: abi.Status) !void {
    return error_identity.statusToError(status);
}
