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

//! Storage-free facade for the compiled metadata-Raft apply/projection owner.
//! Each projection crosses as one owned control-plane JSON envelope; backend
//! transactions, cursors, and individual records remain inside the kernel.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const contract = @import("../metadata/storage/raft_apply_contract.zig");
const metadata = @import("../metadata/domain.zig");
const metadata_incarnation = @import("antfly_local_sources").metadata_incarnation;
const metadata_table_manager = @import("../metadata/table_manager.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const raft_state_machine = @import("../raft/state_machine/mod.zig");
const extension_domain = @import("../extensions/mod.zig");
const restore_staging = @import("../metadata/restore_staging.zig");
const physical_metadata = @import("../metadata/storage/raft_apply_store.zig");
const sql_settings = @import("antfly_local_sources").system_catalog_settings;
const sql_policies = @import("antfly_local_sources").system_catalog_policies;
const fk_generation_publication = @import("../metadata/fk_generation_publication.zig");
const fk_initial_retirement_wire = @import("../metadata/fk_initial_retirement_wire.zig");

pub const RaftApplyStoreConfig = struct {
    borrowed_store: ?*@import("antfly_local_sources").storage_backend_erased.Store = null,
    root_dir: []const u8,
    map_size: usize = 16 * 1024 * 1024,
    no_sync: bool = false,
    read_only: bool = false,
    flush_threshold: usize = 64,
    context: ?*anyopaque = null,
};

pub const AppliedMetadataCheckpoint = contract.AppliedMetadataCheckpoint;
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

pub const LifecycleListenerRegistration = @import("../metadata/storage/raft_apply_contract.zig").LifecycleListenerRegistration;

const ListenerRegistration = struct {
    id: u64 = 0,
    projection: ?ProjectionListener = null,
    committed_key: ?CommittedKeyListener = null,
};

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    handle: ?*anyopaque,
    listeners: std.ArrayListUnmanaged(*ListenerRegistration) = .empty,
    listeners_mutex: std.Io.Mutex = .init,
    hot_standby_adapter: ?*@import("metadata_hot_standby_adapter.zig").Adapter = null,

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
            .system_store = if (cfg.borrowed_store) |store| try @import("kernel_system_store_client.zig").nativeHandle(store) else null,
        }, &handle));
        return .{
            .alloc = alloc,
            .handle = handle orelse return error.StorageKernelFailure,
        };
    }

    pub fn deinit(self: *RaftApplyStore) void {
        abi.antfly_metadata_apply_store_close(self.handle);
        if (self.hot_standby_adapter) |adapter| {
            adapter.io_impl.deinit();
            self.alloc.destroy(adapter);
        }
        for (self.listeners.items) |listener| self.alloc.destroy(listener);
        self.listeners.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn snapshotBuilder(self: *RaftApplyStore) raft_state_machine.SnapshotBuilder {
        return .{ .ptr = self, .vtable = &.{
            .build_snapshot = buildSnapshot,
            .prepare_snapshot = prepareSnapshot,
            .install_snapshot = installSnapshot,
            .apply_batch = applyBatch,
            .is_apply_retryable = isApplyRetryable,
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

    fn isApplyRetryable(_: *anyopaque, _: u64, err: anyerror) bool {
        return err == error.CatalogPublicationProofPending;
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

    fn parsedProjection(self: *RaftApplyStore, comptime T: type, alloc: std.mem.Allocator, request: abi.MetadataProjectionRequest) !?std.json.Parsed(T) {
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_projection(self.handle, &request, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (std.mem.eql(u8, response.slice(), "null")) return null;
        return try std.json.parseFromSlice(T, alloc, response.slice(), .{ .allocate = .alloc_always });
    }

    pub fn bindHotStandby(self: *RaftApplyStore, gate: ?@import("antfly_local_sources").storage_db_replication_contract.WriteGate, mirror: ?@import("antfly_local_sources").storage_db_replication_contract.AsyncEffectMirror) !void {
        const Adapter = @import("metadata_hot_standby_adapter.zig").Adapter;
        const next = try self.alloc.create(Adapter);
        errdefer self.alloc.destroy(next);
        next.* = .{ .alloc = self.alloc, .io_impl = std.Io.Threaded.init(self.alloc, .{}), .gate = gate, .mirror = mirror };
        errdefer next.io_impl.deinit();
        const port = next.asPort();
        try statusToError(abi.antfly_metadata_apply_store_bind_ha(self.handle, &.{ .port = &port }));
        if (self.hot_standby_adapter) |old| {
            old.io_impl.deinit();
            self.alloc.destroy(old);
        }
        self.hot_standby_adapter = next;
    }
    pub fn flushHotStandbyOutbox(self: *RaftApplyStore) !void {
        _ = try self.projection(bool, .{ .kind = .flush_ha_outbox });
    }
    pub fn applyHotStandbyRecord(self: *RaftApplyStore, record: @import("antfly_local_sources").storage_db_replication_record.RecordView) !void {
        const bytes = try @import("antfly_local_sources").storage_db_replication_record.encodeAlloc(self.alloc, record);
        defer self.alloc.free(bytes);
        _ = try self.projection(bool, .{ .kind = .apply_ha_record, .key = .fromSlice(bytes) });
    }
    pub fn exportHotStandbyCheckpoint(self: *RaftApplyStore, io: std.Io, path: []const u8) !@import("hot_standby/metadata_effects.zig").CheckpointArtifact {
        _ = io; // IO is performed by the owning native metadata runtime.
        return self.projection(@import("hot_standby/metadata_effects.zig").CheckpointArtifact, .{ .kind = .export_ha_checkpoint, .key = .fromSlice(path) });
    }
    pub fn importHotStandbyCheckpoint(self: *RaftApplyStore, io: std.Io, path: []const u8, size_bytes: u64) !void {
        _ = io;
        _ = try self.projection(bool, .{ .kind = .import_ha_checkpoint, .key = .fromSlice(path), .arg0 = size_bytes });
    }
    pub fn migrateStandaloneRestoreJobs(self: *RaftApplyStore, rows: []const RestoreJobRow) !void {
        var encoded: std.Io.Writer.Allocating = .init(self.alloc);
        defer encoded.deinit();
        var stream: std.json.Stringify = .{ .writer = &encoded.writer, .options = .{} };
        try @import("antfly_local_sources").storage_db_relational_integrity_json.write(rows, &stream);
        _ = try self.projection(bool, .{ .kind = .migrate_standalone_restore_jobs, .key = .fromSlice(encoded.written()) });
    }

    pub fn getBackupCohort(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, job_id: u64) !?[]u8 {
        return self.projectionWithAllocator(?[]u8, alloc, .{ .kind = .backup_cohort, .group_id = group_id, .arg0 = job_id });
    }
    pub fn getLakeIndexLifecycle(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, table_id: u64) ![]u8 {
        return self.projectionWithAllocator([]u8, alloc, .{ .kind = .lake_index_lifecycle, .group_id = group_id, .arg0 = table_id });
    }
    pub fn lakeIndexLifecycleWork(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, after: ?u64) ![]u8 {
        return self.projectionWithAllocator([]u8, alloc, .{ .kind = .lake_index_lifecycle_work, .group_id = group_id, .arg0 = after orelse 0, .arg1 = @intFromBool(after != null) });
    }
    pub fn getBackupCohortProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, job_id: u64) !?[]u8 {
        return self.projectionWithAllocator(?[]u8, alloc, .{ .kind = .backup_cohort_progress, .group_id = group_id, .arg0 = job_id });
    }
    pub fn listBackupCohorts(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, after: ?[]const u8, limit: usize) ![]@import("antfly_local_sources").storage_docstore.OwnedKVPair {
        return self.projectionWithAllocator([]@import("antfly_local_sources").storage_docstore.OwnedKVPair, alloc, .{ .kind = .backup_cohorts, .group_id = group_id, .arg0 = limit, .arg1 = @intFromBool(after != null), .key = .fromSlice(after orelse "") });
    }
    pub fn loadRestoreStaging(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, id: restore_staging.Id) !?std.json.Parsed(restore_staging.Job) {
        return self.parsedProjection(restore_staging.Job, alloc, .{ .kind = .restore_staging_job, .group_id = group_id, .key = .fromSlice(&id) });
    }
    pub fn loadRestoreStagingForOwner(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, owner_group: u64) !?std.json.Parsed(restore_staging.Job) {
        return self.parsedProjection(restore_staging.Job, alloc, .{ .kind = .restore_staging_owner_job, .group_id = group_id, .arg0 = owner_group });
    }
    pub fn loadRestoreStagingProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, id: restore_staging.Id) !?restore_staging.Progress {
        return self.projectionWithAllocator(?restore_staging.Progress, alloc, .{ .kind = .restore_staging_progress, .group_id = group_id, .key = .fromSlice(&id) });
    }
    pub fn restoreStagingAuthorityAllowed(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, id: restore_staging.Id, node_id: u64, owner_group: ?u64) !bool {
        if (owner_group == 0) return false;
        return self.projectionWithAllocator(bool, alloc, .{ .kind = .restore_staging_authority_allowed, .group_id = group_id, .key = .fromSlice(&id), .arg0 = node_id, .arg1 = owner_group orelse 0 });
    }
    pub fn loadRestoreStagingReceipt(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, id: restore_staging.Id, state: restore_staging.State, owner_group: u64) !?[]u8 {
        return self.projectionWithAllocator(?[]u8, alloc, .{ .kind = .restore_staging_receipt, .group_id = group_id, .key = .fromSlice(&id), .arg0 = @backingInt(state), .arg1 = owner_group });
    }
    pub fn captureProvisioningCatalog(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !restore_staging.ProvisioningProjection {
        return self.projectionWithAllocator(restore_staging.ProvisioningProjection, alloc, .{ .kind = .provisioning_catalog, .group_id = group_id });
    }
    pub fn initialGroupReservation(self: *RaftApplyStore, metadata_group_id: u64, range_group_id: u64) !?fk_generation_publication.InitialGroupReservation {
        return self.projection(?fk_generation_publication.InitialGroupReservation, .{
            .kind = .fk_initial_group_reservation,
            .group_id = metadata_group_id,
            .arg0 = range_group_id,
        });
    }
    pub fn getRelationalTopologyProtocolActivationVersion(self: *RaftApplyStore, group_id: u64) !u16 {
        return self.projection(u16, .{ .kind = .relational_topology_protocol_activation_version, .group_id = group_id });
    }
    pub fn getSchemaReaderFloor(self: *RaftApplyStore, group_id: u64) !metadata_table_manager.SchemaReaderFloor {
        return self.catalogProjection(metadata_table_manager.SchemaReaderFloor, self.alloc, group_id, .schema_reader_floor);
    }
    pub fn resolveTableCreateIdentity(self: *RaftApplyStore, group_id: u64, initial: u64) !u64 {
        return self.projection(u64, .{ .kind = .resolve_table_create_identity, .group_id = group_id, .arg0 = initial });
    }
    pub fn applyStandaloneCommand(self: *RaftApplyStore, group_id: u64, command: physical_metadata.TransitionCommand) !void {
        const encoded = try physical_metadata.encodeTransitionCommand(self.alloc, command);
        defer self.alloc.free(encoded);
        _ = try self.projection(bool, .{ .kind = .standalone_command, .group_id = group_id, .key = .fromSlice(encoded) });
    }
    pub fn loadStandaloneCatalog(self: *RaftApplyStore, alloc: std.mem.Allocator) !?[]u8 {
        return self.projectionWithAllocator(?[]u8, alloc, .{ .kind = .standalone_catalog });
    }
    pub fn loadStandaloneCatalogSnapshot(self: *RaftApplyStore, alloc: std.mem.Allocator) !?[]u8 {
        return self.projectionWithAllocator(?[]u8, alloc, .{ .kind = .standalone_catalog, .arg0 = 1 });
    }
    pub fn exportStandalonePortableCatalog(self: *RaftApplyStore, alloc: std.mem.Allocator) ![]u8 {
        return self.catalogProjection([]u8, alloc, @import("antfly_local_sources").common_group_ids.main_metadata_group_id, .{ .catalog_export_standalone_portable = {} });
    }
    pub fn standaloneRevision(self: *RaftApplyStore) !u64 {
        return self.projection(u64, .{ .kind = .standalone_revision });
    }
    pub const StandaloneRevisionState = contract.StandaloneRevisionState;
    pub fn standaloneRevisionState(self: *RaftApplyStore) !StandaloneRevisionState {
        return self.catalogProjection(StandaloneRevisionState, self.alloc, 0, .standalone_revision_state);
    }
    pub fn replaceStandaloneCatalog(self: *RaftApplyStore, group_id: u64, expected_revision: u64, tables: []const metadata.TableRecord, ranges: []const metadata.RangeRecord, auxiliary_json: []const u8) !void {
        return self.updateStandaloneCatalog(group_id, expected_revision, .{ .replace = true, .tables = tables, .ranges = ranges, .auxiliary_json = auxiliary_json });
    }

    pub fn updateStandaloneCatalog(self: *RaftApplyStore, group_id: u64, expected_revision: u64, update: contract.StandaloneCatalogUpdate) !void {
        var encoded: std.Io.Writer.Allocating = .init(self.alloc);
        defer encoded.deinit();
        var stream: std.json.Stringify = .{ .writer = &encoded.writer, .options = .{} };
        try @import("antfly_local_sources").storage_db_relational_integrity_json.write(update, &stream);
        _ = try self.projection(bool, .{ .kind = .replace_standalone_catalog, .group_id = group_id, .arg0 = expected_revision, .key = .fromSlice(encoded.written()) });
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

    pub fn readControlStores(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, groups: []const u64) ![]metadata.StoreRecord {
        return self.catalogProjection([]metadata.StoreRecord, alloc, group_id, .{ .read_control_stores = groups });
    }
    pub fn readStore(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, store_id: u64, reports: bool) !?metadata.StoreRecord {
        return self.catalogProjection(?metadata.StoreRecord, alloc, group_id, .{ .read_store = .{ .store_id = store_id, .reports = reports } });
    }
    pub fn readStoreGroupFacts(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, store_id: u64) !?metadata.StoreRecord {
        return self.catalogProjection(?metadata.StoreRecord, alloc, group_id, .{ .read_store_group_facts = store_id });
    }
    pub fn readStoreReportTargetsWithRuntime(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, update: store_report_update.Update, include_runtime: bool) !?metadata.StoreRecord {
        var ids: std.ArrayListUnmanaged(u64) = .empty;
        defer ids.deinit(alloc);
        if (update.base != null) {
            for (update.report.group_statuses) |item| try ids.append(alloc, item.group_id);
            for (update.report.runtime_statuses) |item| try ids.append(alloc, item.group_id);
            try ids.appendSlice(alloc, update.removed_groups);
        }
        return self.catalogProjection(?metadata.StoreRecord, alloc, group_id, .{ .read_store_report_targets = .{ .store_id = update.report.store_id, .group_ids = ids.items, .full = update.base == null, .include_runtime = include_runtime } });
    }
    pub fn systemCatalogRead(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: system_catalog.Read) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .catalog_read = request });
    }
    pub fn exportSystemCatalog(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .catalog_export = {} });
    }
    pub fn captureHotStandbyCatalogCheckpoint(self: *RaftApplyStore, alloc: std.mem.Allocator, io: std.Io, group_id: u64, path: []const u8) !@import("../system_catalog/projection.zig").CatalogCheckpoint {
        _ = io; // The native owner performs filesystem IO on its runtime.
        return self.catalogProjection(@import("../system_catalog/projection.zig").CatalogCheckpoint, alloc, group_id, .{ .catalog_checkpoint = path });
    }
    pub fn projectHotStandbyCheckpointCatalog(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, checkpoint: @import("hot_standby/metadata_effects.zig").CheckpointArtifact) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .catalog_checkpoint_projection = checkpoint });
    }
    pub fn sqlSnapshotCohort(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, database: []const u8) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .sql_snapshot_cohort = database });
    }
    pub fn listSystemCatalogTables(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: system_catalog.TableList) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .catalog_list_tables = request });
    }
    pub fn systemCatalogMeta(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !system_catalog.Meta {
        return self.catalogProjection(system_catalog.Meta, alloc, group_id, .{ .catalog_meta = {} });
    }
    pub fn systemCatalogAdmission(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: system_catalog.Mutation) !contract.CatalogAdmission {
        return self.catalogProjection(contract.CatalogAdmission, alloc, group_id, .{ .catalog_admission = request });
    }
    pub fn prepareSystemCatalogResult(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, command: contract.SystemCatalogCommand) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .catalog_prepare = command });
    }
    pub fn prepareColumnSequenceHandoffs(self: *RaftApplyStore, alloc: std.mem.Allocator, group: u64, request: @import("../metadata/column_sequence_handoff.zig").Request) ![]u8 {
        return self.catalogProjection([]u8, alloc, group, .{ .catalog_column_sequence_handoffs = request });
    }
    pub fn bindTableColumnSequences(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, command: contract.SystemCatalogCommand) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .catalog_bind_column_sequences = command });
    }
    pub fn sqlSequenceAdmissionHead(self: *RaftApplyStore, group_id: u64) !u64 {
        return self.catalogProjection(u64, self.alloc, group_id, .{ .sql_sequence_admission_head = {} });
    }
    pub fn getSqlColumnIdentities(self: *RaftApplyStore, alloc: std.mem.Allocator, group: u64, guard: @import("antfly_local_sources").system_catalog_domain.RelationMutationGuard) !std.json.Parsed(@import("antfly_local_sources").system_catalog_column_identities.State) {
        const bytes = try self.catalogProjection([]u8, alloc, group, .{ .sql_column_identities = guard });
        defer alloc.free(bytes);
        return @import("antfly_local_sources").system_catalog_column_identities.State.decode(alloc, bytes);
    }
    pub fn sqlSequenceSnapshot(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../system_catalog/sequence_catalog.zig").SnapshotRequest, context: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate();
        const deadline = (@import("antfly_local_sources").api_routing_budget.RoutingBudget{}).deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        const Callback = struct {
            fn cancelled(ptr: ?*anyopaque) callconv(.c) u8 {
                const token: *const @import("antfly_cancellation").CancellationToken = @ptrCast(@alignCast(ptr.?));
                return @intFromBool(token.isCancelled());
            }
        };
        const bytes = try std.json.Stringify.valueAlloc(alloc, contract.CatalogProjectionRequest{ .sql_sequence_snapshot = request }, .{});
        defer alloc.free(bytes);
        const result = try self.projectionWithAllocator([]u8, alloc, .{ .kind = .system_catalog, .group_id = group_id, .key = .fromSlice(bytes), .control = .{
            .execution_deadline_ns = deadline orelse 0,
            .has_execution_deadline = @intFromBool(deadline != null),
            .cancellation_ctx = @ptrCast(@constCast(&context.cancellation)),
            .cancellation_fn = Callback.cancelled,
        } });
        errdefer alloc.free(result);
        try context.ensureActive();
        return result;
    }
    pub fn sqlSequencePreflight(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../system_catalog/sequence_catalog.zig").Mutation) !@import("../system_catalog/sequence_catalog.zig").Outcome {
        return self.catalogProjection(@import("../system_catalog/sequence_catalog.zig").Outcome, alloc, group_id, .{ .sql_sequence_preflight = request });
    }
    pub fn sqlSequenceOutcome(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../system_catalog/sequence_catalog.zig").Mutation) !@import("../system_catalog/sequence_catalog.zig").Outcome {
        return self.catalogProjection(@import("../system_catalog/sequence_catalog.zig").Outcome, alloc, group_id, .{ .sql_sequence_outcome = request });
    }
    pub fn sqlSequenceCounterPreflight(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../system_catalog/sequence_catalog.zig").CounterRequest) !@import("../system_catalog/sequence_catalog.zig").CounterAdmission {
        return self.catalogProjection(@import("../system_catalog/sequence_catalog.zig").CounterAdmission, alloc, group_id, .{ .sql_sequence_counter_preflight = request });
    }
    pub fn sqlSequenceCounterOutcome(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../system_catalog/sequence_catalog.zig").CounterRequest) !?@import("../system_catalog/sequence_catalog.zig").CounterOutcome {
        return self.catalogProjection(?@import("../system_catalog/sequence_catalog.zig").CounterOutcome, alloc, group_id, .{ .sql_sequence_counter_outcome = request });
    }
    pub fn resolveSystemCatalogTable(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, target: system_catalog.Target) !?metadata.TableRecord {
        return self.catalogProjection(?metadata.TableRecord, alloc, group_id, .{ .catalog_resolve_table = target });
    }
    pub fn resolveSystemCatalogIdentity(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, target: system_catalog.Target) !?system_catalog.ResolvedTable {
        return self.catalogProjection(?system_catalog.ResolvedTable, alloc, group_id, .{ .catalog_resolve_identity = target });
    }
    pub fn resolveSystemCatalogIdentities(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: system_catalog.ResolveMany) !system_catalog.ResolvedMany {
        return self.resolveSystemCatalogIdentitiesWithContext(alloc, group_id, request, .{});
    }

    pub fn resolveSystemCatalogIdentitiesWithContext(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: system_catalog.ResolveMany, context: @import("antfly_local_sources").api_operation.RequestContext) !system_catalog.ResolvedMany {
        try context.ensureActive();
        const deadline = (@import("antfly_local_sources").api_routing_budget.RoutingBudget{}).deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        const Callback = struct {
            fn cancelled(ptr: ?*anyopaque) callconv(.c) u8 {
                const token: *const @import("antfly_cancellation").CancellationToken = @ptrCast(@alignCast(ptr.?));
                return @intFromBool(token.isCancelled());
            }
        };
        const bytes = try std.json.Stringify.valueAlloc(alloc, contract.CatalogProjectionRequest{ .catalog_resolve_many = request }, .{});
        defer alloc.free(bytes);
        const result = try self.projectionWithAllocator(system_catalog.ResolvedMany, alloc, .{
            .kind = .system_catalog,
            .group_id = group_id,
            .key = .fromSlice(bytes),
            .control = .{
                .execution_deadline_ns = deadline orelse 0,
                .has_execution_deadline = @intFromBool(deadline != null),
                .cancellation_ctx = @ptrCast(@constCast(&context.cancellation)),
                .cancellation_fn = Callback.cancelled,
            },
        });
        errdefer result.deinit(alloc);
        try context.ensureActive();
        try result.validateRelations(request);
        return result;
    }
    pub fn tableWriteValidation(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, name: []const u8) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .catalog_write_validation = name });
    }
    pub fn writeValidationRevision(self: *RaftApplyStore, group_id: u64) !u64 {
        return self.catalogProjection(u64, self.alloc, group_id, .catalog_write_validation_revision);
    }
    pub fn queryTableDefinition(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, name: []const u8) !?system_catalog.QueryDefinition {
        return self.catalogProjection(?system_catalog.QueryDefinition, alloc, group_id, .{ .catalog_query_definition = name });
    }
    pub fn topologyActivation(self: *RaftApplyStore, group_id: u64) !?topology_protocol.Activation {
        return self.catalogProjection(?topology_protocol.Activation, self.alloc, group_id, .{ .topology_activation = {} });
    }
    pub fn relationSourceTrackingActive(self: *RaftApplyStore, group_id: u64) !bool {
        return self.catalogProjection(bool, self.alloc, group_id, .relation_source_tracking);
    }
    pub fn relationReconciliationWork(self: *RaftApplyStore, group_id: u64) !@import("antfly_local_sources").system_catalog_relation_reconciliation.Work {
        return self.catalogProjection(@import("antfly_local_sources").system_catalog_relation_reconciliation.Work, self.alloc, group_id, .relation_reconciliation_work);
    }

    pub fn stepRelationPublicationProof(self: *RaftApplyStore, state: @import("antfly_local_sources").system_catalog_relation_reconciliation.State, now_ns: u64) !?contract.RelationPublicationEvidence {
        const encoded = try state.encode();
        var result: abi.MetadataRelationPublicationResult = .{};
        try statusToError(abi.antfly_metadata_apply_store_relation_publication(self.handle, &.{
            .group_id = state.group_id,
            .now_ns = now_ns,
            .expected_state = .fromSlice(&encoded),
        }, &result));
        if (result.ready > 1 or result.has_root > 1 or !std.mem.allEqual(u8, &result._reserved, 0)) return error.StorageKernelFailure;
        if (result.ready == 0) {
            if (result.has_root != 0 or result.applied_index != 0 or !std.mem.allEqual(u8, &result.root, 0)) return error.StorageKernelFailure;
            return null;
        }
        const root = if (result.has_root == 1) try @import("antfly_local_sources").system_catalog_relation_reconciliation.Generation.decode(&result.root) else null;
        if (root) |value| {
            if (value.group_id != state.group_id) return error.StorageKernelFailure;
        } else if (!std.mem.allEqual(u8, &result.root, 0)) return error.StorageKernelFailure;
        return .{ .state = state, .applied_index = result.applied_index, .root = root };
    }
    pub fn cancelRelationPublicationProof(self: *RaftApplyStore, group: u64) !void {
        var result: abi.MetadataRelationPublicationResult = .{};
        try statusToError(abi.antfly_metadata_apply_store_relation_publication(self.handle, &.{ .operation = .cancel, .group_id = group }, &result));
    }
    pub fn closeRelationPublicationProof(self: *RaftApplyStore, group: u64) void {
        var result: abi.MetadataRelationPublicationResult = .{};
        // Fixed-layout teardown performs no serialization or allocation.
        const status = abi.antfly_metadata_apply_store_relation_publication(self.handle, &.{ .operation = .close, .group_id = group }, &result);
        std.debug.assert(status == .ok);
    }
    pub fn expireRelationPublicationProofs(self: *RaftApplyStore, now_ns: u64) !void {
        var result: abi.MetadataRelationPublicationResult = .{};
        try statusToError(abi.antfly_metadata_apply_store_relation_publication(self.handle, &.{ .operation = .expire, .now_ns = now_ns }, &result));
    }
    pub fn reportBaselineProgress(self: *RaftApplyStore, group_id: u64, request: @import("../metadata/store_report_baseline.zig").Request) !@import("../metadata/store_report_baseline.zig").Progress {
        return self.reportBaselineProgressForKey(group_id, try request.progressQuery());
    }
    pub fn reportBaselineProgressForKey(self: *RaftApplyStore, group_id: u64, request: @import("../metadata/store_report_baseline.zig").ProgressQuery) !@import("../metadata/store_report_baseline.zig").Progress {
        return self.catalogProjection(@import("../metadata/store_report_baseline.zig").Progress, self.alloc, group_id, .{ .report_baseline_progress = request });
    }
    pub fn admitBaselineFragment(self: *RaftApplyStore, group_id: u64, request: @import("../metadata/store_report_baseline.zig").Request) !u16 {
        return self.catalogProjection(u16, self.alloc, group_id, .{ .report_baseline_fragment_admission = request });
    }
    pub fn reportCursor(self: *RaftApplyStore, group_id: u64, store_id: u64) !?store_report_update.Cursor {
        return self.catalogProjection(?store_report_update.Cursor, self.alloc, group_id, .{ .report_cursor = store_id });
    }
    pub fn readStoreReportTargets(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, update: store_report_update.Update) !?metadata.StoreRecord {
        return self.readStoreReportTargetsWithRuntime(alloc, group_id, update, true);
    }
    pub fn validateSystemCatalog(self: *RaftApplyStore, group_id: u64, command: contract.SystemCatalogCommand) !void {
        const result = try self.prepareSystemCatalogResult(self.alloc, group_id, command);
        self.alloc.free(result);
    }
    pub fn systemCatalogSnapshot(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !system_catalog.OwnedState {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const value = try self.catalogProjection(struct { meta: system_catalog.Meta, value: system_catalog.State }, arena.allocator(), group_id, .{ .catalog_snapshot = {} });
        return .{ .arena = arena, .meta = value.meta, .value = value.value };
    }
    pub fn sqlSettingSnapshotJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, scope: sql_settings.Scope) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .sql_setting_snapshot = scope });
    }
    pub const SqlSettingMutationCut = struct {
        arena: std.heap.ArenaAllocator,
        meta: system_catalog.Meta,
        records: []const sql_settings.Record,
        database_exists: bool,
        database_id: ?u64,
        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
            self.* = undefined;
        }
    };
    pub fn sqlSettingMutationCut(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, database: ?[]const u8) !SqlSettingMutationCut {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const value = try self.catalogProjection(struct { meta: system_catalog.Meta, records: []const sql_settings.Record, database_exists: bool, database_id: ?u64 }, arena.allocator(), group_id, .{ .sql_setting_mutation_cut = database });
        return .{ .arena = arena, .meta = value.meta, .records = value.records, .database_exists = value.database_exists, .database_id = value.database_id };
    }
    pub fn sqlPolicySnapshotJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, table_id: u64, principal: []const u8, database: []const u8, roles: []const []const u8) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .sql_policy_snapshot = .{ .table_id = table_id, .principal = principal, .database = database, .roles = roles } });
    }
    pub fn sqlPolicyInstallSnapshotJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: sql_policies.InstallRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .sql_policy_install_snapshot = request });
    }
    pub fn sqlPolicyPublicationStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, table_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .sql_policy_publication_status = table_id });
    }
    pub fn sqlPolicyPublicationWorkJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, after_table_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .sql_policy_publication_work = after_table_id });
    }
    pub fn sqlPolicyBeginCommandJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: sql_policies.BeginRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .sql_policy_begin_command = request });
    }
    pub fn sqlPolicyDefinitionCommandJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, command: sql_policies.Command) ![]u8 {
        var output: std.Io.Writer.Allocating = .init(alloc);
        defer output.deinit();
        var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
        try @import("antfly_local_sources").storage_db_relational_integrity_json.write(contract.CatalogProjectionRequest{ .sql_policy_definition_command = command }, &stream);
        return self.projectionWithAllocator([]u8, alloc, .{ .kind = .system_catalog, .group_id = group_id, .key = .fromSlice(output.written()) });
    }
    pub fn requirePolicyIndexMutationAllowed(self: *RaftApplyStore, group_id: u64, table_id: u64) !void {
        const value = try self.catalogProjection(bool, self.alloc, group_id, .{ .require_policy_index_mutation_allowed = table_id });
        if (!value) return error.RowPolicyUnsupported;
    }
    pub fn requirePolicyTopologyMutationAllowed(self: *RaftApplyStore, group_id: u64, table_id: u64) !void {
        const value = try self.catalogProjection(bool, self.alloc, group_id, .{ .require_policy_topology_mutation_allowed = table_id });
        if (!value) return error.RowPolicyUnsupported;
    }
    pub fn fkGenerationPublicationStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, child_table_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_generation_publication_status = child_table_id });
    }
    pub fn fkGenerationPublicationWorkJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, after_child_table_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_generation_publication_work = after_child_table_id });
    }
    pub fn fkGenerationPublicationDecisionJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_generation_publication.DecisionRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_generation_publication_decision = request });
    }
    pub fn fkGenerationPublicationSourceDecisionJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_generation_publication.SourceDecisionRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_generation_publication_source_decision = request });
    }
    pub fn relationalIndexStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_publication.zig").StatusRequest) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_status = request });
    }
    pub fn relationalIndexPrepareCutJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_publication.zig").PrepareRequest) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_prepare = request });
    }
    pub fn standaloneNativeOwnerBinding(self: *RaftApplyStore, group_id: u64) !?@import("../metadata/standalone_native_owner.zig").Binding {
        return self.catalogProjection(?@import("../metadata/standalone_native_owner.zig").Binding, self.alloc, group_id, .standalone_native_owner_binding);
    }
    pub fn nativeReporterRestartProgress(self: *RaftApplyStore, group_id: u64, store_id: u64) !?@import("../metadata/standalone_native_owner.zig").RestartProgress {
        return self.catalogProjection(?@import("../metadata/standalone_native_owner.zig").RestartProgress, self.alloc, group_id, .{ .standalone_native_reporter_progress = store_id });
    }
    pub fn observeStandaloneNativeReporter(self: *RaftApplyStore, group_id: u64, request: @import("../metadata/standalone_native_owner.zig").Registration) !@import("../metadata/standalone_native_owner.zig").RestartObservation {
        try request.validate();
        return self.catalogProjection(@import("../metadata/standalone_native_owner.zig").RestartObservation, self.alloc, group_id, .{ .standalone_native_reporter_observation = request });
    }
    pub fn relationalIndexAdmissionStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_publication.zig").AdmissionRequest) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_admission_status = request });
    }
    pub fn validateRelationalIndexBegin(self: *RaftApplyStore, group_id: u64, request: @import("../metadata/relational_index_publication.zig").BeginRequest, prepared: *const @import("../metadata/relational_index_publication.zig").Prepared) !void {
        _ = try request.observation(prepared);
        if (!try self.catalogProjection(bool, self.alloc, group_id, .{ .relational_index_begin_preflight = request })) return error.InvalidGenerationPublication;
    }
    pub fn validateRelationalIndexPublicationDecision(self: *RaftApplyStore, group_id: u64, request: @import("../metadata/relational_index_publication.zig").DecidePublish) !void {
        try request.validate();
        if (!try self.catalogProjection(bool, self.alloc, group_id, .{ .relational_index_publish_preflight = request })) return error.InvalidGenerationPublication;
    }
    pub fn relationalIndexPublicationDecisionStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_publication.zig").DecidePublish) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_publish_status = request });
    }
    pub fn validateRelationalIndexCancellation(self: *RaftApplyStore, group_id: u64, request: @import("../metadata/relational_index_publication.zig").CancelDecision) !void {
        try request.validate();
        if (!try self.catalogProjection(bool, self.alloc, group_id, .{ .relational_index_cancel_preflight = request })) return error.InvalidGenerationPublication;
    }
    pub fn relationalIndexCancellationStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_publication.zig").CancelDecision) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_cancel_status = request });
    }

    pub fn schemaValidationPreflightJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, command: []const u8) ![]u8 {
        if (command.len == 0 or command.len > 64 * 1024 * 1024) return error.InvalidGenerationPublication;
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_projection(self.handle, &.{ .kind = .schema_validation_preflight, .group_id = group_id, .key = .fromSlice(command) }, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (response.len == 0 or response.len > 16 * 1024) return error.StorageKernelFailure;
        return alloc.dupe(u8, response.slice());
    }

    pub fn schemaValidationDiscover(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/schema_validation_read.zig").DiscoveryRequest) ![]u8 {
        try request.validate();
        if (request.group != group_id) return error.InvalidGenerationPublication;
        return self.rawCatalogProjection(alloc, group_id, .{ .schema_validation_discover = request }, 1024);
    }

    pub fn schemaValidationWork(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/schema_validation_work.zig").Query) ![]u8 {
        try request.validate();
        if (request.group != group_id) return error.InvalidGenerationPublication;
        return self.rawCatalogProjection(alloc, group_id, .{ .schema_validation_work = request }, @import("../metadata/schema_validation_work.zig").max_wire_bytes);
    }

    pub fn schemaValidationPrepareCut(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/schema_validation_admission.zig").Request) ![]u8 {
        try request.validate();
        return self.rawCatalogProjection(alloc, group_id, .{ .schema_validation_prepare = request }, @import("../metadata/schema_validation_admission.zig").max_wire_bytes);
    }

    pub fn schemaValidationOwnerRead(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/schema_validation_read.zig").OwnerReadRequest) ![]u8 {
        try request.validate();
        if (request.lookup.group != group_id) return error.InvalidGenerationPublication;
        return self.rawCatalogProjection(alloc, group_id, .{ .schema_validation_owner_read = request }, request.responseLimit());
    }
    fn rawCatalogProjection(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: contract.CatalogProjectionRequest, limit: usize) ![]u8 {
        const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(bytes);
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_projection(self.handle, &.{ .kind = .system_catalog, .group_id = group_id, .key = .fromSlice(bytes) }, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (response.len > limit) return error.StorageKernelFailure;
        return alloc.dupe(u8, response.slice());
    }
    pub fn schemaValidationOwnerControlJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, lookup: @import("../metadata/schema_validation_read.zig").Lookup, ordinal: u16, action: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Action) ![]u8 {
        return self.schemaValidationOwnerRead(alloc, group_id, .{ .lookup = lookup, .ordinal = ordinal, .action = action });
    }
    pub fn schemaValidationOwnerInputBytes(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, lookup: @import("../metadata/schema_validation_read.zig").Lookup, ordinal: u16) ![]u8 {
        return self.schemaValidationOwnerRead(alloc, group_id, .{ .lookup = lookup, .ordinal = ordinal, .projection = .inputs });
    }

    pub fn schemaValidationParentJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, lookup: @import("../metadata/schema_validation_read.zig").Lookup, image: @import("../metadata/schema_validation_read.zig").ImageIdentity) ![]u8 {
        return self.schemaValidationOwnerRead(alloc, group_id, .{ .lookup = lookup, .ordinal = 0, .projection = .parent, .source_image = image });
    }

    pub fn schemaValidationRecoveryInputBytes(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, lookup: @import("../metadata/schema_validation_read.zig").Lookup, ordinal: u16, image: @import("../metadata/schema_validation_read.zig").ImageIdentity) ![]u8 {
        return self.schemaValidationOwnerRead(alloc, group_id, .{ .lookup = lookup, .ordinal = ordinal, .projection = .recovery_inputs, .source_image = image });
    }

    pub fn schemaValidationStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/schema_validation_admission.zig").StatusRequest) ![]u8 {
        try request.validate();
        if (request.lookup.group != group_id) return error.InvalidGenerationPublication;
        return self.rawCatalogProjection(alloc, group_id, .{ .schema_validation_status = request }, @import("../metadata/schema_validation_admission.zig").Status.max_wire_bytes);
    }

    pub fn schemaValidationReceiptJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/schema_validation_receipt.zig").Request) ![]u8 {
        try request.validate();
        if (request.lookup.group != group_id) return error.InvalidGenerationPublication;
        return self.rawCatalogProjection(alloc, group_id, .{ .schema_validation_receipt_read = request }, 16 * 1024);
    }

    pub fn relationalIndexOwnerDecisionJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_publication.zig").OwnerRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_owner_decision = request });
    }
    pub fn relationalIndexOwnerAckStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_publication.zig").Ack) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_owner_ack_status = request });
    }
    pub fn validateRelationalIndexOwnerAck(self: *RaftApplyStore, group_id: u64, bytes: []const u8) !void {
        const publication = @import("../metadata/relational_index_publication.zig");
        if (bytes.len == 0 or bytes.len > publication.max_ack_bytes) return error.InvalidGenerationPublication;
        const valid = try self.catalogProjection(bool, self.alloc, group_id, .{ .relational_index_owner_ack_preflight = bytes });
        if (!valid) return error.InvalidIndexOwnerReceipt;
    }
    pub fn relationalIndexWorkJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_work.zig").Query) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_work = request });
    }
    pub fn relationalIndexSourceAcceptanceStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("../metadata/relational_index_delivery_journal.zig").Query) ![]u8 {
        try request.validate();
        return self.catalogProjection([]u8, alloc, group_id, .{ .relational_index_source_accept_status = request });
    }
    pub fn validateRelationalIndexSourceAcceptance(self: *RaftApplyStore, group_id: u64, bytes: []const u8) !void {
        const journal = @import("../metadata/relational_index_delivery_journal.zig");
        if (bytes.len == 0 or bytes.len > journal.max_command_bytes) return error.InvalidGenerationPublication;
        if (!try self.catalogProjection(bool, self.alloc, group_id, .{ .relational_index_source_accept_preflight = bytes })) return error.InvalidPrivateIndexClaimReceipt;
    }
    pub fn validateRelationalIndexSourceCompletion(self: *RaftApplyStore, group_id: u64, bytes: []const u8) !void {
        if (bytes.len == 0 or bytes.len > @import("../metadata/relational_index_delivery_journal.zig").max_completion_bytes) return error.InvalidGenerationPublication;
        if (!try self.catalogProjection(bool, self.alloc, group_id, .{ .relational_index_source_complete_preflight = bytes })) return error.InvalidPrivateIndexClaimReceipt;
    }
    pub fn fkInitialCreatePrepareJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_generation_publication.InitialCreatePrepareRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_initial_create_prepare = request });
    }
    pub fn fkInitialCreatePrepareJsonWithContext(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_generation_publication.InitialCreatePrepareRequest, context: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
        const normalized = try context.platformDeadline();
        const bytes = try self.catalogProjection([]u8, alloc, group_id, .{ .fk_initial_create_prepare_with_deadline = .{ .request = request, .deadline_ns = normalized.deadline_ns } });
        errdefer alloc.free(bytes);
        try context.ensureActive();
        return bytes;
    }
    pub fn prepareConstraintNamesJsonWithContext(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("antfly_local_sources").system_catalog_domain.ConstraintNamePreparation, context: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
        const normalized = try context.platformDeadline();
        const bytes = try self.catalogProjection([]u8, alloc, group_id, .{ .constraint_name_prepare = .{ .request = request, .deadline_ns = normalized.deadline_ns } });
        errdefer alloc.free(bytes);
        try context.ensureActive();
        return bytes;
    }
    pub fn checkConstraintNamesWithContext(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: @import("antfly_local_sources").system_catalog_domain.ConstraintNameCheck, context: @import("antfly_local_sources").api_operation.RequestContext) !void {
        const normalized = try context.platformDeadline();
        const ready = try self.catalogProjection(bool, alloc, group_id, .{ .constraint_name_check = .{ .request = request, .deadline_ns = normalized.deadline_ns } });
        try context.ensureActive();
        if (!ready) return error.InvalidCatalogRecord;
    }
    pub fn preflightFkGenerationNameAdmission(self: *RaftApplyStore, group_id: u64, plan: fk_generation_publication.Plan) !void {
        if (plan.constraint_name_guard == null) return;
        const ready = try self.catalogProjection(bool, self.alloc, group_id, .{ .fk_generation_name_admission = plan });
        if (!ready) return error.InvalidCatalogRecord;
    }
    pub fn preflightFkInitialCreateCommand(self: *RaftApplyStore, group_id: u64, bytes: []const u8) !void {
        const result = try self.projection(contract.InitialFkPreflight, .{
            .kind = .fk_initial_create_preflight,
            .group_id = group_id,
            .key = .fromSlice(bytes),
        });
        return switch (result) {
            .ready => {},
            .generation_changed => error.GenerationPublicationChanged,
            .catalog_exists => error.CatalogAlreadyExists,
            .table_transition_active => error.TableTransitionActive,
        };
    }
    pub fn fkInitialChildDecisionJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_generation_publication.InitialChildDecisionRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_initial_child_decision = request });
    }
    pub fn fkInitialCreateStatusJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, child_table_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_initial_create_status = child_table_id });
    }
    pub fn fkGenerationTableLockedJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, table_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_generation_table_locked = table_id });
    }
    pub fn fkInitialCreateWorkJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, after_child_table_id: u64) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_initial_create_work = after_child_table_id });
    }
    pub fn fkInitialRetirementTicketPageJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_initial_retirement_wire.PageRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_initial_retirement_page = request });
    }
    pub fn storeRootControlJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_initial_retirement_wire.Control) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .store_root_control = request });
    }
    pub fn validateStoreRootEnrollmentProtocol(self: *RaftApplyStore, group_id: u64) !void {
        const valid = try self.catalogProjection(bool, self.alloc, group_id, .validate_store_root_enrollment_protocol);
        if (!valid) return error.TableTopologyProtocolUpgradeRequired;
    }
    pub fn fkInitialParentDecisionJson(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, request: fk_generation_publication.DecisionRequest) ![]u8 {
        return self.catalogProjection([]u8, alloc, group_id, .{ .fk_initial_parent_decision = request });
    }
    fn catalogProjection(self: *RaftApplyStore, comptime T: type, alloc: std.mem.Allocator, group_id: u64, request: contract.CatalogProjectionRequest) !T {
        const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(bytes);
        return self.projectionWithAllocator(T, alloc, .{ .kind = .system_catalog, .group_id = group_id, .key = .fromSlice(bytes) });
    }

    pub fn latestCheckpoint(self: *RaftApplyStore, group_id: u64) !?AppliedMetadataCheckpoint {
        return self.projection(?AppliedMetadataCheckpoint, .{ .kind = .latest_checkpoint, .group_id = group_id });
    }

    pub fn getMetadataIncarnation(self: *RaftApplyStore, group_id: u64) !?metadata_incarnation.MetadataClusterIncarnation {
        return try self.projection(?metadata_incarnation.MetadataClusterIncarnation, .{ .kind = .metadata_incarnation, .group_id = group_id });
    }

    pub fn getDenseNativeStorageProtocolActivationVersion(self: *RaftApplyStore, group_id: u64) !u16 {
        return try self.projection(u16, .{ .kind = .dense_native_storage_protocol_activation_version, .group_id = group_id });
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

    pub fn getMergeTransition(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, transition_id: u64) !?metadata.MergeTransitionRecord {
        return self.projectionWithAllocator(?metadata.MergeTransitionRecord, alloc, .{ .kind = .merge_transition, .group_id = group_id, .arg0 = transition_id });
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

    pub fn resolverCatalogRead(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, input: @import("../metadata/resolver_catalog_read.zig").Request) ![]u8 {
        const bytes = try std.json.Stringify.valueAlloc(alloc, input, .{});
        defer alloc.free(bytes);
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_projection(self.handle, &.{ .kind = .resolver_catalog_read, .group_id = group_id, .key = .fromSlice(bytes) }, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        return alloc.dupe(u8, response.slice());
    }
    pub fn resolverProjectionValid(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, table: metadata.TableRecord) !bool {
        const bytes = try std.json.Stringify.valueAlloc(alloc, table, .{});
        defer alloc.free(bytes);
        return self.projectionWithAllocator(bool, alloc, .{ .kind = .resolver_projection_valid, .group_id = group_id, .key = .fromSlice(bytes) });
    }
    pub fn pendingResolverNamespaceTable(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?metadata.TableRecord {
        return self.projectionWithAllocator(?metadata.TableRecord, alloc, .{ .kind = .resolver_namespace_work, .group_id = group_id });
    }
    pub fn pendingResolverNamespaceDrop(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) !?u64 {
        return self.projectionWithAllocator(?u64, alloc, .{ .kind = .resolver_namespace_work, .group_id = group_id, .arg0 = 2 });
    }
    pub fn pendingResolverNamespaceSources(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.TableRecord {
        return self.projectionWithAllocator([]metadata.TableRecord, alloc, .{ .kind = .resolver_namespace_work, .group_id = group_id, .arg0 = 1 });
    }
    pub fn resolverNamespaceReplacementRejected(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, term: u64, index: u64, expected: metadata.TableRecord, replacement: metadata.TableRecord) !bool {
        const bytes = try std.json.Stringify.valueAlloc(alloc, .{ .current = expected, .replacement = replacement, .term = term, .index = index }, .{});
        defer alloc.free(bytes);
        return self.projectionWithAllocator(bool, alloc, .{ .kind = .resolver_namespace_replacement_matches, .group_id = group_id, .arg0 = 1, .key = .fromSlice(bytes) });
    }
    pub fn resolverNamespaceReplacementMatches(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, current: metadata.TableRecord, replacement: metadata.TableRecord) !bool {
        const bytes = try std.json.Stringify.valueAlloc(alloc, .{ .current = current, .replacement = replacement }, .{});
        defer alloc.free(bytes);
        return self.projectionWithAllocator(bool, alloc, .{ .kind = .resolver_namespace_replacement_matches, .group_id = group_id, .key = .fromSlice(bytes) });
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

    pub fn getSecretCollection(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64, scope: []const u8) !?[]u8 {
        var response: abi.OwnedBytes = .{};
        try statusToError(abi.antfly_metadata_apply_store_projection(self.handle, &.{ .kind = .secret_collection, .group_id = group_id, .key = .fromSlice(scope) }, &response));
        defer abi.antfly_storage_owner_buffer_destroy(&response);
        if (response.len == 0) return null;
        return try alloc.dupe(u8, response.slice());
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
            .store_reports_changed = signal.store_reports_changed != 0,
            .store_runtime_changed = signal.store_runtime_changed != 0,
            .store_group_ids = if (signal.has_store_group_ids != 0) (if (signal.store_group_ids) |ids| ids[0..signal.store_group_ids_len] else &.{}) else null,
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

    fn addListeners(self: *RaftApplyStore, projection_listener: ?ProjectionListener, committed_listener: ?CommittedKeyListener) !LifecycleListenerRegistration {
        if (projection_listener) |listener| try listener.validate();
        self.listeners_mutex.lockUncancelable(std.Options.debug_io);
        defer self.listeners_mutex.unlock(std.Options.debug_io);
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
        }, &registration.id));
        self.listeners.appendAssumeCapacity(registration);
        return .{ .id = registration.id };
    }

    pub fn addProjectionListener(self: *RaftApplyStore, listener: ProjectionListener) !void {
        _ = try self.addListeners(listener, null);
    }

    pub fn addCommittedKeyListener(self: *RaftApplyStore, listener: CommittedKeyListener) !void {
        _ = try self.addListeners(null, listener);
    }

    pub fn addLifecycleListeners(self: *RaftApplyStore, projection_listener: ProjectionListener, committed_listener: CommittedKeyListener) !LifecycleListenerRegistration {
        return try self.addListeners(projection_listener, committed_listener);
    }
    pub fn removeLifecycleListeners(self: *RaftApplyStore, token: LifecycleListenerRegistration) bool {
        self.listeners_mutex.lockUncancelable(std.Options.debug_io);
        defer self.listeners_mutex.unlock(std.Options.debug_io);
        for (self.listeners.items, 0..) |registration, index| {
            if (registration.id != token.id) continue;
            if (abi.antfly_metadata_apply_store_remove_listeners(self.handle, token.id) == 0) return false;
            _ = self.listeners.orderedRemove(index);
            self.alloc.destroy(registration);
            return true;
        }
        return false;
    }
};

fn statusToError(status: abi.Status) !void {
    return error_identity.statusToError(status);
}

const system_catalog = @import("antfly_local_sources").system_catalog_domain;
const store_report_update = @import("../metadata/store_report_update.zig");
const topology_protocol = @import("../metadata/topology_protocol.zig");
