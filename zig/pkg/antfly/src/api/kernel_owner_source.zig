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

//! Provisioned group-local read/write adapters for the compiled storage owner.
//! Distributed sources retain routing, admission, consistency, aggregation,
//! and lifecycle; this source owns only coarse physical operations.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const abi = @import("kernel_owner_abi");
const client = @import("../storage/kernel_owner_client.zig");
const data_apply_client = @import("../storage/data_raft_apply_client.zig");
const descriptor_contract = @import("../storage/kernel_owner_descriptor.zig");
const backend_types = @import("../storage/backend_types.zig");
const db_types = @import("../storage/db/types.zig");
const document_artifact_child_range = @import("../storage/db/document_artifact_child_range.zig");
const ha_replication_record = @import("../storage/ha/replication_record.zig");
const runtime_preflight = @import("../storage/db/runtime_preflight.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const backup_contract = @import("backup_contract.zig");
const distributed_graph = @import("distributed_graph.zig");
const query_response = @import("query_response.zig");
const runtime_status = @import("runtime_status.zig");
const read_gate = @import("../raft/read_gate.zig");
const feature_reads = @import("../raft/feature_reads.zig");
const table_catalog = @import("table_catalog.zig");
const table_read_source = @import("table_read_source.zig");
const table_reads = @import("table_reads.zig");
const storage_snapshot_source = @import("storage_snapshot_source.zig");
const storage_maintenance_source = @import("storage_maintenance_source.zig");
const table_write_source = @import("table_write_source.zig");
const table_writes = @import("table_writes.zig");

pub const ProvisionedKernelOwnerSource = struct {
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: read_gate.ReadableLeaseRequester,
    group_visible_root_generation: ?table_reads.GroupVisibleRootGenerationSource = null,
    transaction_recovery_source: ?*table_writes.ProvisionedTableWriteSource = null,
    context: client.Context = .{},
    owns_context: bool = true,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    owner_cache_hits: std.atomic.Value(u64) = .init(0),
    owner_cache_misses: std.atomic.Value(u64) = .init(0),

    const Identity = descriptor_contract.Identity;

    pub const CacheStats = struct {
        hit_count: u64 = 0,
        miss_count: u64 = 0,
    };

    pub const LoadedDescriptor = struct {
        path: []u8,
        schema_json: []u8,
        indexes_json: []u8,
        generation: u64,
        identity: descriptor_contract.Identity,

        pub fn view(self: *const LoadedDescriptor) descriptor_contract.Descriptor {
            return .{
                .lsm_root_generation = self.generation,
                .identity = self.identity,
                .schema_json = self.schema_json,
                .indexes_json = self.indexes_json,
            };
        }

        pub fn deinit(self: *LoadedDescriptor, alloc: std.mem.Allocator) void {
            alloc.free(self.path);
            alloc.free(self.schema_json);
            alloc.free(self.indexes_json);
            self.* = undefined;
        }
    };

    const Entry = struct {
        group_id: u64,
        table_name: []u8,
        generation: u64,
        identity: Identity,
        owner: client.Owner,
        active_users: usize = 0,
        retired: bool = false,
        bulk_ingest_active: std.atomic.Value(bool) = .init(false),
    };

    const Lease = struct {
        source: *ProvisionedKernelOwnerSource,
        entry: *Entry,
        active: bool = true,

        fn owner(self: *Lease) *client.Owner {
            return &self.entry.owner;
        }

        fn deinit(self: *Lease) void {
            if (!self.active) return;
            self.source.release(self.entry);
            self.active = false;
        }
    };

    pub fn init(
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        requester: read_gate.ReadableLeaseRequester,
    ) ProvisionedKernelOwnerSource {
        return .{
            .alloc = alloc,
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .requester = requester,
        };
    }

    pub fn withGroupVisibleRootGeneration(
        self: *ProvisionedKernelOwnerSource,
        source: ?table_reads.GroupVisibleRootGenerationSource,
    ) *ProvisionedKernelOwnerSource {
        self.group_visible_root_generation = source;
        return self;
    }

    pub fn withRequester(
        self: *ProvisionedKernelOwnerSource,
        requester: read_gate.ReadableLeaseRequester,
    ) *ProvisionedKernelOwnerSource {
        self.requester = requester;
        return self;
    }

    pub fn withTransactionRecoverySource(
        self: *ProvisionedKernelOwnerSource,
        source: ?*table_writes.ProvisionedTableWriteSource,
    ) *ProvisionedKernelOwnerSource {
        self.transaction_recovery_source = source;
        return self;
    }

    pub fn withStorageContextHandle(
        self: *ProvisionedKernelOwnerSource,
        handle: ?*anyopaque,
    ) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        std.debug.assert(self.context.handle == null);
        self.context.handle = handle;
        self.owns_context = false;
        return self;
    }

    /// Call only after every attached read/write source has drained. Owner
    /// closure is deliberately centralized here so one live DB serves both
    /// operation families for its full group lifecycle.
    pub fn deinit(self: *ProvisionedKernelOwnerSource) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            std.debug.assert(entry.active_users == 0);
            entry.owner.deinit();
            self.alloc.free(entry.table_name);
            self.alloc.destroy(entry);
        }
        self.entries.deinit(self.alloc);
        self.entries = .empty;
        if (self.owns_context) self.context.deinit();
    }

    pub fn readSource(self: *ProvisionedKernelOwnerSource) table_read_source.TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = unsupportedTopLevelLookup,
                .scan = unsupportedTopLevelScan,
                .query = unsupportedTopLevelQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .document_artifact_manifest_group_local = documentArtifactManifestGroupLocal,
                .document_artifact_manifests_group_local = documentArtifactManifestsGroupLocal,
            },
        };
    }

    pub fn writeSource(self: *ProvisionedKernelOwnerSource) table_write_source.TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch = unsupportedTopLevelBatch,
                .batch_group_local = batchGroupLocal,
                .backup_table_group_local = backupTableGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .txn_acknowledge_group_local = txnAcknowledgeGroupLocal,
                .begin_bulk_ingest_group_local = beginBulkIngestGroupLocal,
                .finish_bulk_ingest_group_local = finishBulkIngestGroupLocal,
                .abort_bulk_ingest_group_local = abortBulkIngestGroupLocal,
                .corrupt_embedding_artifact_group_local = corruptEmbeddingArtifactGroupLocal,
                .reprocess_document_artifact_group_local = reprocessDocumentArtifactGroupLocal,
                .reprocess_document_artifact_range_group_local = reprocessDocumentArtifactRangeGroupLocal,
                .list_artifact_repair_issues_group_local = listArtifactRepairIssuesGroupLocal,
                .repair_artifact_issues_group_local = repairArtifactIssuesGroupLocal,
                .repair_artifact_issues_group_local_controlled = repairArtifactIssuesGroupLocalControlled,
                .update_document_artifact_child_range_placement_group_local = updateDocumentArtifactChildRangePlacementGroupLocal,
                .apply_document_artifact_child_range_batch_group_local = applyDocumentArtifactChildRangeBatchGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
                .reconcile_table_group_local = reconcileTableGroupLocal,
                .retire_table_group_local = retireTableGroupLocal,
            },
        };
    }

    pub fn snapshotSource(self: *ProvisionedKernelOwnerSource) storage_snapshot_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .retire_group_for_publication = retireGroupForPublication,
                .prepare = prepareSnapshot,
                .prepare_restore = prepareRestore,
                .reconcile_restore = reconcileRestore,
                .repair_published_restore = repairPublishedRestore,
                .promote = promoteSnapshot,
                .publish_prepared = publishPreparedSnapshot,
                .commit = commitSnapshot,
                .rollback = rollbackSnapshot,
                .destroy = destroySnapshot,
            },
        };
    }

    pub fn maintenanceSource(self: *ProvisionedKernelOwnerSource) storage_maintenance_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .run_lsm_round = runLsmMaintenanceRound,
                .run_dense_posting_round = runDensePostingMaintenanceRound,
                .snapshot = maintenanceSnapshot,
            },
        };
    }

    /// Process-owner reuse replaces the legacy read/write cache split. Report
    /// one shared acquisition counter to both compatibility metric names until
    /// those public metrics are renamed around the owner model.
    pub fn cacheStats(self: *const ProvisionedKernelOwnerSource) CacheStats {
        return .{
            .hit_count = self.owner_cache_hits.load(.monotonic),
            .miss_count = self.owner_cache_misses.load(.monotonic),
        };
    }

    pub fn contextMetrics(self: *ProvisionedKernelOwnerSource) !abi.ContextMetricsResult {
        return try self.context.metrics();
    }

    pub fn storageContextHandle(self: *ProvisionedKernelOwnerSource) !?*anyopaque {
        try self.context.ensure();
        return self.context.handle;
    }

    /// Run one bounded projection reconciliation while borrowing the same
    /// resident physical owner used by table reads and writes.
    pub fn reconcileDataRaftProjection(
        self: *ProvisionedKernelOwnerSource,
        apply_store: *data_apply_client.RaftApplyStore,
        work_alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        expected: ?data_apply_client.AppliedDataBatch,
        capture_handoff: bool,
        max_page_entries: usize,
        max_page_bytes: usize,
    ) !data_apply_client.RaftApplyStore.ReconcileResult {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try apply_store.reconcileAuthoritativeOwner(
            work_alloc,
            lease.owner().handle,
            group_id,
            expected,
            capture_handoff,
            max_page_entries,
            max_page_bytes,
        );
    }

    /// Borrow both resident group owners for one complete local split/merge
    /// phase. Acquisition is globally ordered so inverse group pairs cannot
    /// deadlock, while argument order remains source/destination or
    /// donor/receiver at the compiled ABI.
    pub fn runLocalTransition(
        self: *ProvisionedKernelOwnerSource,
        apply_store: ?*data_apply_client.RaftApplyStore,
        primary_group_id: u64,
        secondary_group_id: u64,
        table_name: []const u8,
        request: client.LocalTransitionRequest,
    ) !client.LocalTransitionResult {
        if (primary_group_id == secondary_group_id or
            request.primary_group_id != primary_group_id or
            request.secondary_group_id != secondary_group_id or
            !std.mem.eql(u8, request.table_name.slice(), table_name))
        {
            return error.InvalidTransitionRequest;
        }

        var primary_lease: ?Lease = null;
        defer if (primary_lease) |*lease| lease.deinit();
        var secondary_lease: ?Lease = null;
        defer if (secondary_lease) |*lease| lease.deinit();
        const primary_path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            primary_group_id,
        });
        defer self.alloc.free(primary_path);
        const secondary_path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            secondary_group_id,
        });
        defer self.alloc.free(secondary_path);
        const primary_descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = self.visibleRootGeneration(primary_group_id),
            .identity = .{
                .table_id = request.table_id,
                .shard_id = request.source_identity_shard_id,
                .range_id = request.source_identity_range_id,
            },
            .schema_json = request.schema_json.slice(),
            .indexes_json = request.indexes_json.slice(),
        };
        const secondary_descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = self.visibleRootGeneration(secondary_group_id),
            .identity = .{
                .table_id = request.table_id,
                .shard_id = request.target_identity_shard_id,
                .range_id = request.target_identity_range_id,
            },
            .schema_json = request.schema_json.slice(),
            .indexes_json = request.indexes_json.slice(),
        };
        if (primary_group_id < secondary_group_id) {
            primary_lease = try self.acquireDescriptor(
                primary_group_id,
                table_name,
                primary_path,
                primary_descriptor,
            );
            secondary_lease = try self.acquireDescriptor(
                secondary_group_id,
                table_name,
                secondary_path,
                secondary_descriptor,
            );
        } else {
            secondary_lease = try self.acquireDescriptor(
                secondary_group_id,
                table_name,
                secondary_path,
                secondary_descriptor,
            );
            primary_lease = try self.acquireDescriptor(
                primary_group_id,
                table_name,
                primary_path,
                primary_descriptor,
            );
        }
        return try primary_lease.?.owner().localTransition(
            secondary_lease.?.owner(),
            if (apply_store) |store| store.handle else null,
            request,
        );
    }

    pub fn retireAll(self: *ProvisionedKernelOwnerSource) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var retired_count: usize = 0;
        var i = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.entries.items[i];
            retired_count += 1;
            entry.retired = true;
            if (entry.active_users == 0) self.destroyEntryAtIndexLocked(i);
        }
        return retired_count;
    }

    /// Retire every resident generation for a table. Active calls retain their
    /// old owner until their lease drains; the next call then opens the catalog
    /// descriptor that won the outer structural transition.
    pub fn retireTable(self: *ProvisionedKernelOwnerSource, table_name: []const u8) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var retired_count: usize = 0;
        var i = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.entries.items[i];
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            retired_count += 1;
            entry.retired = true;
            if (entry.active_users == 0) self.destroyEntryAtIndexLocked(i);
        }
        return retired_count;
    }

    fn retireGroupForPublication(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        lock(&self.mutex);
        defer self.mutex.unlock();
        var i = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.entries.items[i];
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.active_users != 0) return error.StorageBusy;
            entry.retired = true;
            self.destroyEntryAtIndexLocked(i);
        }
    }

    fn retireTableGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        try retireGroupForPublication(ptr, group_id, table_name);
        return {};
    }

    fn prepareSnapshot(
        ptr: *anyopaque,
        request: storage_snapshot_source.PrepareRequest,
    ) !*anyopaque {
        _ = ptr;
        const snapshot = try client.Snapshot.prepare(.{
            .path = .fromSlice(request.path),
            .table_name = .fromSlice(request.table_name),
            .group_id = request.group_id,
            .lsm_root_generation = request.lsm_root_generation,
            .identity_table_id = request.identity.table_id,
            .identity_shard_id = request.identity.shard_id,
            .identity_range_id = request.identity.range_id,
            .schema_json = .fromSlice(request.schema_json),
            .indexes_json = .fromSlice(request.indexes_json),
            .encoded_snapshot = .fromSlice(request.encoded_snapshot),
        });
        return snapshot.handle orelse error.StorageKernelFailure;
    }

    const EncodedRestoreRequest = struct {
        alloc: std.mem.Allocator,
        manifest_json: []u8,
        request: abi.RestorePrepareRequest,

        fn deinit(self: *EncodedRestoreRequest) void {
            self.alloc.free(self.manifest_json);
            self.* = undefined;
        }
    };

    fn encodeRestoreRequest(
        self: *ProvisionedKernelOwnerSource,
        request: storage_snapshot_source.RestoreRequest,
    ) !EncodedRestoreRequest {
        const manifest_json = try std.json.Stringify.valueAlloc(self.alloc, request.manifest.*, .{
            .emit_null_optional_fields = false,
        });
        return .{
            .alloc = self.alloc,
            .manifest_json = manifest_json,
            .request = .{
                .path = .fromSlice(request.path),
                .table_name = .fromSlice(request.table_name),
                .group_id = request.group_id,
                .lsm_root_generation = request.lsm_root_generation,
                .has_identity_namespace = @intFromBool(request.identity != null),
                .identity_table_id = if (request.identity) |identity| identity.table_id else 0,
                .identity_shard_id = if (request.identity) |identity| identity.shard_id else 0,
                .identity_range_id = if (request.identity) |identity| identity.range_id else 0,
                .backup_root = .fromSlice(request.backup_root),
                .backup_id = .fromSlice(request.manifest.backup_id),
                .artifact_backup_id = .fromSlice(request.artifact_backup_id),
                .source_identity = .fromSlice(request.source_identity),
                .snapshot_path = .fromSlice(request.shard.snapshot_path),
                .expected_artifact_size_bytes = request.shard.artifact_size_bytes,
                .expected_artifact_sha256 = .fromSlice(request.shard.artifact_sha256),
                .manifest_json = .fromSlice(manifest_json),
            },
        };
    }

    fn prepareRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !storage_snapshot_source.RestorePreparation {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        return switch (try client.Snapshot.prepareRestore(encoded.request)) {
            .prepared => |snapshot| .{ .prepared = .{
                .source = self.snapshotSource(),
                .handle = snapshot.handle orelse return error.StorageKernelFailure,
            } },
            .already_imported => .already_imported,
        };
    }

    fn reconcileRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        try client.Snapshot.reconcileRestore(encoded.request);
    }

    fn repairPublishedRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        var lease = try self.acquire(request.group_id, request.table_name);
        defer lease.deinit();
        try lease.owner().repairRestore(&encoded.request);
    }

    fn promoteSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.promote();
    }

    fn publishPreparedSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !bool {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        return try snapshot.publishPrepared();
    }

    fn commitSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.commit();
    }

    fn rollbackSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.rollback();
    }

    fn destroySnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        snapshot.deinit();
    }

    /// Apply one already-committed local Raft batch without consulting the
    /// catalog from the apply thread. The descriptor is part of the replicated
    /// envelope, so every replica opens the same generation and identity.
    pub fn applyPreparedReplicatedBatchGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        req: db_types.BatchRequest,
    ) !void {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            group_id,
        });
        defer alloc.free(path);
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor);
        defer lease.deinit();
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
    }

    pub fn waitForCurrentSyncGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        sync_level: db_types.SyncLevel,
    ) !void {
        switch (sync_level) {
            .propose, .write => return,
            .full_text, .enrichments, .full_index => {},
        }
        const owner_sync_level: abi.SyncLevel = switch (sync_level) {
            .propose => .propose,
            .write => .write,
            .full_text => .full_text,
            .enrichments => .enrichments,
            .full_index => .full_index,
        };
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().waitForSync(table_name, owner_sync_level);
    }

    pub fn applyHAReplicationRecordGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        record: ha_replication_record.RecordView,
    ) !void {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().applyHAReplicationRecord(table_name, .{
            .record_kind = @intFromEnum(record.kind),
            .payload_codec = @intFromEnum(record.payload_codec),
            .flags = record.flags,
            .cluster_id = record.cluster_id,
            .shard_id = record.shard_id,
            .table_id = record.table_id,
            .timeline_id = record.timeline_id,
            .epoch = record.epoch,
            .lsn = record.lsn,
            .previous_lsn = record.previous_lsn,
            .commit_timestamp_ns = record.commit_timestamp_ns,
            .payload = record.payload,
        });
    }

    const BackupShardWire = struct {
        group_id: u64,
        start_key: []const u8,
        end_key: ?[]const u8 = null,
        snapshot_path: []const u8,
        artifact_size_bytes: u64 = 0,
        artifact_sha256: []const u8 = "",
    };

    fn backupTableGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        plan: backup_contract.TableBackupPlan,
    ) !?[]backup_contract.ShardSnapshot {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().backupJson(
            table_name,
            plan.backup_root,
            plan.backup_id,
            switch (plan.format) {
                .native => .native,
                .portable => .portable,
            },
        );
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(
            []BackupShardWire,
            alloc,
            response.bytes(),
            .{},
        );
        defer parsed.deinit();
        if (parsed.value.len != 1 or parsed.value[0].group_id != group_id)
            return error.StorageKernelFailure;
        const shards = try alloc.alloc(backup_contract.ShardSnapshot, parsed.value.len);
        var initialized: usize = 0;
        errdefer {
            for (shards[0..initialized]) |shard| shard.deinit(alloc);
            alloc.free(shards);
        }
        for (parsed.value, 0..) |shard, i| {
            const start_key = try alloc.dupe(u8, shard.start_key);
            errdefer alloc.free(start_key);
            const end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null;
            errdefer if (end_key) |value| alloc.free(value);
            const snapshot_path = try alloc.dupe(u8, shard.snapshot_path);
            errdefer alloc.free(snapshot_path);
            const artifact_sha256 = if (shard.artifact_sha256.len > 0)
                try alloc.dupe(u8, shard.artifact_sha256)
            else
                "";
            errdefer if (artifact_sha256.len > 0) alloc.free(@constCast(artifact_sha256));
            shards[i] = .{
                .group_id = shard.group_id,
                .start_key = start_key,
                .end_key = end_key,
                .snapshot_path = snapshot_path,
                .artifact_size_bytes = shard.artifact_size_bytes,
                .artifact_sha256 = artifact_sha256,
            };
            initialized += 1;
        }
        return shards;
    }

    pub fn ownerCountForTest(self: *ProvisionedKernelOwnerSource) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.entries.items.len;
    }

    /// Pre-open the same resident owner used by reads and writes. Warmup must
    /// not create a second status-only DB in the distributed compilation unit;
    /// acquiring and releasing the owner performs descriptor validation
    /// without opening a second DB in distributed code.
    pub fn warmTableGroup(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        var lease = try self.acquire(group_id, table_name);
        lease.deinit();
        // Preserve the existing warmup contract: validate that the physical
        // root can be opened, but do not pin a writer before startup catch-up
        // and structural reconciliation have run. The first actual read or
        // write will install the long-lived owner.
        try retireGroupForPublication(self, group_id, table_name);
    }

    /// Apply the latest catalog schema/index contract to the already-resident
    /// physical group owner, or open that owner with the contract when absent.
    /// Routing and catalog selection remain in distributed control.
    pub fn reconcileTableGroup(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !abi.ReconcileResult {
        return try self.reconcileTableGroupStep(group_id, table_name, null, false);
    }

    /// Advance at most one durable index-repair intent in addition to the
    /// desired-state pass. Node scheduling decides when to request that work.
    pub fn reconcileTableGroupStep(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !abi.ReconcileResult {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        var lease = try self.acquireDescriptor(
            group_id,
            table_name,
            descriptor.path,
            descriptor.view(),
        );
        defer lease.deinit();
        return try lease.owner().reconcile(
            table_name,
            descriptor.schema_json,
            descriptor.indexes_json,
            target_index_name,
            advance_index_repair,
        );
    }

    fn reconcileTableGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?table_write_source.LocalStructuralReconcileResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const result = try self.reconcileTableGroupStep(
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
        );
        return .{
            .state = switch (result.state) {
                .complete => .complete,
                .repair_pending => .repair_pending,
                .busy => .busy,
                .degraded => .degraded,
            },
            .indexes_added = result.indexes_added,
            .indexes_removed = result.indexes_removed,
            .indexes_pending = result.indexes_pending,
            .repair_discovered = result.repair_discovered,
            .repair_attempted = result.repair_attempted,
            .repair_repaired = result.repair_repaired,
            .repair_remaining = result.repair_remaining,
            .repair_terminal = result.repair_terminal,
            .repair_busy = result.repair_busy,
            .repair_disk_waits = result.repair_disk_waits,
            .next_retry_at_ms = result.next_retry_at_ms,
        };
    }

    fn visibleRootGeneration(self: *const ProvisionedKernelOwnerSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |source|
            source.visibleRootGenerationForGroup(group_id)
        else
            table_reads.backend_current_root_generation;
    }

    fn lock(mutex: *std.atomic.Mutex) void {
        platform_sync.lockYielding(mutex);
    }

    fn destroyEntryAtIndexLocked(self: *ProvisionedKernelOwnerSource, index: usize) void {
        const entry = self.entries.orderedRemove(index);
        std.debug.assert(entry.active_users == 0);
        entry.owner.deinit();
        self.alloc.free(entry.table_name);
        self.alloc.destroy(entry);
    }

    fn release(self: *ProvisionedKernelOwnerSource, entry: *Entry) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(entry.active_users > 0);
        entry.active_users -= 1;
        if (!entry.retired or entry.active_users != 0) return;
        for (self.entries.items, 0..) |candidate, index| {
            if (candidate != entry) continue;
            self.destroyEntryAtIndexLocked(index);
            return;
        }
        unreachable;
    }

    fn snapshotMaintenanceLeases(
        self: *ProvisionedKernelOwnerSource,
        best_effort: bool,
    ) !?[]Lease {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();

        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.retired or entry.bulk_ingest_active.load(.acquire)) continue;
            count += 1;
        }
        const leases = try self.alloc.alloc(Lease, count);
        var initialized: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.retired or entry.bulk_ingest_active.load(.acquire)) continue;
            entry.active_users += 1;
            leases[initialized] = .{ .source = self, .entry = entry };
            initialized += 1;
        }
        std.debug.assert(initialized == count);
        return leases;
    }

    fn releaseMaintenanceLeases(self: *ProvisionedKernelOwnerSource, leases: []Lease) void {
        for (leases) |*lease| lease.deinit();
        self.alloc.free(leases);
    }

    fn runLsmMaintenanceRound(
        ptr: *anyopaque,
        best_effort: bool,
    ) !storage_maintenance_source.RoundResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const maybe_leases = try self.snapshotMaintenanceLeases(best_effort);
        const leases = maybe_leases orelse return .{};
        defer self.releaseMaintenanceLeases(leases);

        var selected_index: ?usize = null;
        var selected_score: u64 = 0;
        var selected_due = false;
        for (leases, 0..) |*lease, index| {
            const status = lease.owner().maintenance(
                lease.entry.table_name,
                if (best_effort) .inspect_best_effort else .inspect,
            ) catch |err| {
                if (best_effort) continue;
                return err;
            };
            const due = status.has_next_wake_delay != 0 and status.next_wake_delay_ns == 0;
            if (!due and status.maintenance_score == 0) continue;
            if (selected_index == null or
                (due and !selected_due) or
                (due == selected_due and status.maintenance_score > selected_score))
            {
                selected_index = index;
                selected_score = status.maintenance_score;
                selected_due = due;
            }
        }
        const index = selected_index orelse return .{};
        const lease = &leases[index];
        const result = try lease.owner().maintenance(
            lease.entry.table_name,
            if (best_effort) .lsm_step_best_effort else .lsm_step,
        );
        return .{
            .progressed = result.progressed != 0,
            .group_id = lease.entry.group_id,
        };
    }

    fn runDensePostingMaintenanceRound(ptr: *anyopaque) !usize {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const maybe_leases = try self.snapshotMaintenanceLeases(true);
        const leases = maybe_leases orelse return 0;
        defer self.releaseMaintenanceLeases(leases);

        var total_steps: usize = 0;
        for (leases) |*lease| {
            const result = lease.owner().maintenance(
                lease.entry.table_name,
                .dense_posting_idle,
            ) catch |err| {
                std.log.warn("storage owner dense posting maintenance failed table={s} group_id={d} err={s}", .{
                    lease.entry.table_name,
                    lease.entry.group_id,
                    @errorName(err),
                });
                continue;
            };
            total_steps = std.math.add(usize, total_steps, @intCast(result.dense_steps)) catch
                std.math.maxInt(usize);
        }
        return total_steps;
    }

    fn maintenanceSnapshot(
        ptr: *anyopaque,
        best_effort: bool,
    ) !storage_maintenance_source.Snapshot {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const maybe_leases = try self.snapshotMaintenanceLeases(best_effort);
        const leases = maybe_leases orelse return .{};
        defer self.releaseMaintenanceLeases(leases);

        var result = storage_maintenance_source.Snapshot{ .owner_count = leases.len };
        for (leases) |*lease| {
            const status = lease.owner().maintenance(
                lease.entry.table_name,
                if (best_effort) .inspect_best_effort else .inspect,
            ) catch continue;
            result.maintenance_score = @max(result.maintenance_score, status.maintenance_score);
            if (status.has_next_wake_delay != 0) {
                result.next_wake_delay_ns = if (result.next_wake_delay_ns) |current|
                    @min(current, status.next_wake_delay_ns)
                else
                    status.next_wake_delay_ns;
            }
        }
        return result;
    }

    pub fn loadDescriptor(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !LoadedDescriptor {
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);

        var table_record: ?metadata_table_manager.TableRecord = null;
        for (snapshot.tables) |table| {
            if (!std.mem.eql(u8, table.name, table_name)) continue;
            table_record = table;
            break;
        }
        const table = table_record orelse return error.TableNotFound;
        for (snapshot.ranges) |range| {
            if (range.table_id != table.table_id or range.group_id != group_id) continue;
            const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
            errdefer alloc.free(path);
            const schema_json = try alloc.dupe(u8, table.schema_json);
            errdefer alloc.free(schema_json);
            return .{
                .path = path,
                .schema_json = schema_json,
                .indexes_json = try alloc.dupe(u8, table.indexes_json),
                .generation = self.visibleRootGeneration(group_id),
                .identity = .{
                    .table_id = table.table_id,
                    .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
                    .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
                },
            };
        }
        return error.TableNotFound;
    }

    fn acquire(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !Lease {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);

        return try self.acquireDescriptor(group_id, table_name, descriptor.path, descriptor.view());
    }

    fn transactionRecoveryStatus(err: anyerror) abi.Status {
        return switch (err) {
            error.TxnNotFound => .transaction_not_found,
            error.Timeout => .timeout,
            error.Canceled, error.Cancelled => .cancelled,
            error.WouldBlock, error.StorageBusy, error.ResourceBudgetExceeded => .busy,
            else => .internal,
        };
    }

    fn transactionRecoveryResolve(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        participant: abi.BorrowedBytes,
        status: abi.TxnStatus,
        commit_version: u64,
    ) callconv(.c) abi.Status {
        const source: *table_writes.ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        source.storageOwnerResolveRecoveryParticipant(
            txn_id.bytes,
            participant.slice(),
            switch (status) {
                .pending => return .invalid_argument,
                .committed => .committed,
                .aborted => .aborted,
            },
            commit_version,
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryOwns(
        ptr: ?*anyopaque,
        owner_participant: abi.BorrowedBytes,
    ) callconv(.c) u8 {
        const source: *table_writes.ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr orelse return 0));
        return @intFromBool(source.storageOwnerOwnsTransactionRecovery(owner_participant.slice()));
    }

    fn transactionRecoveryAcknowledge(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        owner_participant: abi.BorrowedBytes,
        participant: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const source: *table_writes.ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        source.storageOwnerAcknowledgeRecoveryParticipant(
            txn_id.bytes,
            owner_participant.slice(),
            participant.slice(),
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryCleanup(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        owner_participant: abi.BorrowedBytes,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) callconv(.c) abi.Status {
        const source: *table_writes.ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        source.storageOwnerCleanupRecoveryTransaction(
            txn_id.bytes,
            owner_participant.slice(),
            cutoff_timestamp,
            retained_cutoff_timestamp,
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryConfig(self: *ProvisionedKernelOwnerSource) abi.TransactionRecoveryConfig {
        const source = self.transaction_recovery_source orelse return .{};
        const options = source.storageOwnerTransactionRecoveryOptions();
        if (!options.enabled) return .{};
        return .{
            .enabled = 1,
            .lease_owned = @intFromBool(options.lease_owned),
            .replicated_metadata = @intFromBool(options.replicated_metadata),
            .interval_ms = options.interval_ms,
            .cutoff_ns = options.cutoff_ns,
            .callback_ctx = source,
            .owner_id = .fromSlice(options.owner_id),
            .resolve_participant_fn = transactionRecoveryResolve,
            .owns_recovery_fn = if (options.replicated_metadata) transactionRecoveryOwns else null,
            .acknowledge_participant_fn = if (options.replicated_metadata) transactionRecoveryAcknowledge else null,
            .cleanup_transaction_fn = if (options.replicated_metadata) transactionRecoveryCleanup else null,
        };
    }

    fn acquireDescriptor(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
    ) !Lease {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var stale_index: ?usize = null;
        for (self.entries.items, 0..) |entry, index| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.retired or entry.generation != descriptor.lsm_root_generation or !entry.identity.eql(descriptor.identity)) {
                entry.retired = true;
                if (entry.active_users == 0) {
                    stale_index = index;
                    break;
                }
                return error.StorageKernelOwnerTransitionRequired;
            }
            entry.active_users += 1;
            _ = self.owner_cache_hits.fetchAdd(1, .monotonic);
            return .{ .source = self, .entry = entry };
        }
        if (stale_index) |index| self.destroyEntryAtIndexLocked(index);

        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(entry);
        try self.context.ensure();
        var owner = try client.Owner.open(.{
            .context = self.context.handle,
            .path = abi.BorrowedBytes.fromSlice(path),
            .table_name = abi.BorrowedBytes.fromSlice(table_name),
            .group_id = group_id,
            .lsm_root_generation = descriptor.lsm_root_generation,
            .has_identity_namespace = 1,
            .identity_table_id = descriptor.identity.table_id,
            .identity_shard_id = descriptor.identity.shard_id,
            .identity_range_id = descriptor.identity.range_id,
            .schema_json = .fromSlice(descriptor.schema_json),
            .indexes_json = .fromSlice(descriptor.indexes_json),
            .transaction_recovery = self.transactionRecoveryConfig(),
        });
        errdefer owner.deinit();
        entry.* = .{
            .group_id = group_id,
            .table_name = owned_table_name,
            .generation = descriptor.lsm_root_generation,
            .identity = descriptor.identity,
            .owner = owner,
            .active_users = 1,
        };
        self.entries.appendAssumeCapacity(entry);
        _ = self.owner_cache_misses.fetchAdd(1, .monotonic);
        return .{ .source = self, .entry = entry };
    }

    fn prepareQueryRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.requester);
        reads.prepareSearchWithConsistency(group_id, req, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareSearchWithConsistency(group_id, req, .stale),
            else => return err,
        };
    }

    fn prepareLookupRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.requester);
        reads.prepareLookupWithConsistency(group_id, key, opts, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareLookupWithConsistency(group_id, key, opts, .stale),
            else => return err,
        };
    }

    fn prepareScanRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.requester);
        reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, .stale),
            else => return err,
        };
    }

    fn prepareGraphExpandRead(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        for (req.frontier) |item| {
            const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
            defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
            try self.prepareQueryRead(group_id, search_req, consistency);
        }
    }

    fn executeQuery(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !client.Response {
        try self.prepareQueryRead(group_id, req, consistency);
        const request_json = try table_reads.encodeStorageKernelQueryRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try lease.owner().queryJson(table_name, request_json);
    }

    fn unsupportedTopLevelLookup(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: db_types.LookupOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelScan(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: []const u8,
        _: db_types.ScanOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelQuery(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.SearchRequest,
        _: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelBatch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.BatchRequest,
    ) !?void {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareLookupRead(group_id, key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = lease.owner().lookupJson(table_name, request_json) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return .{
            .json = try alloc.dupe(u8, response.bytes()),
            .version = response.version(),
        };
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareScanRead(group_id, from_key, to_key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelScanRequest(alloc, from_key, to_key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().scanNdjson(table_name, request_json);
        defer response.deinit();
        return .{ .ndjson = try alloc.dupe(u8, response.bytes()) };
    }

    fn documentArtifactManifestGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifest {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareLookupRead(group_id, doc_key, .{}, consistency);
        const request_json = try table_reads.encodeStorageKernelDocumentArtifactManifestRequest(
            alloc,
            doc_key,
            artifact_name,
        );
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = lease.owner().documentArtifactManifestJson(
            table_name,
            request_json,
        ) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return try table_reads.parseStorageKernelDocumentArtifactManifestResponse(
            alloc,
            response.bytes(),
        );
    }

    fn documentArtifactManifestsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifestList {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareLookupRead(group_id, doc_key, .{}, consistency);
        const request_json = try table_reads.encodeStorageKernelDocumentArtifactManifestsRequest(
            alloc,
            doc_key,
        );
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().documentArtifactManifestsJson(
            table_name,
            request_json,
        );
        defer response.deinit();
        return try table_reads.parseStorageKernelDocumentArtifactManifestsResponse(
            alloc,
            response.bytes(),
        );
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.prepareQueryRead(group_id, req, consistency);
        const request_json = try table_reads.encodeStorageKernelPreflightRequest(alloc, req, max_work);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().preflightJson(table_name, request_json);
        defer response.deinit();
        var summary = try table_reads.parseStorageKernelPreflightSummary(alloc, response.bytes());
        table_reads.annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().batchJson(table_name, request_json);
        defer response.deinit();
        return {};
    }

    fn executeArtifactOperation(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        operation: abi.ArtifactOperation,
        request_json: []const u8,
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
        defer_durable_index_repair_execution: bool,
    ) !client.Response {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try lease.owner().artifactOperationJson(
            table_name,
            operation,
            request_json,
            cancellation_ctx,
            cancellation_fn,
            defer_durable_index_repair_execution,
        );
    }

    fn corruptEmbeddingArtifactGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelEmbeddingCorruptionRequest(
            alloc,
            doc_key,
            index_name,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .corrupt_embedding,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        if (!try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes())) return error.NotFound;
        return {};
    }

    fn reprocessDocumentArtifactGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactDocumentRequest(
            alloc,
            doc_key,
            artifact_name,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .reprocess_document,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes());
    }

    fn reprocessDocumentArtifactRangeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
        request: db_types.DocumentArtifactTableReprocessRequest,
    ) !?db_types.DocumentArtifactTableReprocessResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactRangeRequest(
            alloc,
            artifact_name,
            request,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .reprocess_document_range,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelDocumentArtifactTableReprocessResult(
            alloc,
            response.bytes(),
        );
    }

    fn listArtifactRepairIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairListRequest,
    ) !?db_types.ArtifactRepairListResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactRepairListRequest(alloc, request);
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .list_repair_issues,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelArtifactRepairListResult(alloc, response.bytes());
    }

    const ArtifactRepairCancellation = struct {
        check: db_types.RepairCancelCheck,

        fn requested(ctx: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return 0));
            return @intFromBool(self.check.requested());
        }
    };

    fn validateArtifactRepairControls(options: db_types.ArtifactRepairRunOptions) !void {
        const defaults = db_types.ArtifactRepairRunOptions{};
        if (options.yield_check != null or
            options.activation_check != null or
            options.capacity_source != null or
            options.capacity_check != null or
            options.owner_epoch != defaults.owner_epoch or
            options.max_activation_gap_sequences != defaults.max_activation_gap_sequences or
            options.max_convergence_rounds != defaults.max_convergence_rounds or
            options.max_activation_pause_ms != defaults.max_activation_pause_ms or
            options.estimated_candidate_bytes != defaults.estimated_candidate_bytes or
            options.planned_disk_bytes != defaults.planned_disk_bytes or
            options.capacity_domain_id != defaults.capacity_domain_id or
            !std.meta.eql(options.capacity_observation, defaults.capacity_observation) or
            options.executing_durable_index_repair != defaults.executing_durable_index_repair)
        {
            return error.UnsupportedStorageKernelRepairControls;
        }
    }

    fn repairArtifactIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairRunRequest,
    ) !?db_types.ArtifactRepairResult {
        return try repairArtifactIssuesGroupLocalControlled(
            ptr,
            alloc,
            group_id,
            table_name,
            request,
            .{},
        );
    }

    fn repairArtifactIssuesGroupLocalControlled(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairRunRequest,
        options: db_types.ArtifactRepairRunOptions,
    ) !?db_types.ArtifactRepairResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try validateArtifactRepairControls(options);
        if (options.cancelled()) return error.Canceled;
        const request_json = try table_writes.encodeStorageKernelArtifactRepairRequest(alloc, request);
        defer alloc.free(request_json);
        var cancellation: ?ArtifactRepairCancellation = if (options.cancel_check) |check|
            .{ .check = check }
        else
            null;
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .repair_issues,
            request_json,
            if (cancellation) |*value| value else null,
            if (cancellation != null) ArtifactRepairCancellation.requested else null,
            options.defer_durable_index_repair_execution,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelArtifactRepairResult(alloc, response.bytes());
    }

    fn updateDocumentArtifactChildRangePlacementGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactPlacementRequest(
            alloc,
            doc_key,
            artifact_name,
            update,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .update_child_range_placement,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes());
    }

    fn applyDocumentArtifactChildRangeBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        batch: document_artifact_child_range.ApplyBatch,
    ) !?u64 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactChildRangeBatchRequest(
            alloc,
            doc_key,
            artifact_name,
            batch,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .apply_child_range_batch,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelSequenceResponse(alloc, response.bytes());
    }

    fn applyTransactionGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !void {
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        retain_terminal: bool,
        participants: []const []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .transaction = .{ .begin = .{
                .txn_id = txn_id,
                .begin_timestamp = begin_timestamp,
                .created_at_ns = platform_time.realtimeNs(),
                .topology_epoch = topology_epoch,
                .retain_terminal = retain_terminal,
                .participants = participants,
            } },
        });
        return {};
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        topology_epoch: u64,
        req: db_types.TransactionIntentRequest,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        comptime std.debug.assert(@sizeOf(db_types.TransactionWrite) == @sizeOf(db_types.BatchWrite));
        comptime std.debug.assert(@alignOf(db_types.TransactionWrite) == @alignOf(db_types.BatchWrite));
        const writes: []const db_types.BatchWrite = @ptrCast(req.writes);
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .writes = writes,
            .deletes = req.deletes,
            .transforms = req.transforms,
            .predicates = req.predicates,
            .transaction = .{ .prepare = .{
                .txn_id = txn_id,
                .topology_epoch = topology_epoch,
            } },
        });
        return {};
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        status: db_types.TxnStatus,
        commit_version: u64,
        _: u64,
        sync_level: db_types.SyncLevel,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .sync_level = sync_level,
            .transaction = .{ .resolve = .{
                .txn_id = txn_id,
                .status = status,
                .commit_version = commit_version,
            } },
        });
        return {};
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
    ) !?db_types.TxnStatus {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return switch (try lease.owner().transactionStatus(table_name, txn_id)) {
            .pending => .pending,
            .committed => .committed,
            .aborted => .aborted,
        };
    }

    fn txnAcknowledgeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        participant: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .transaction = .{ .acknowledge = .{
                .txn_id = txn_id,
                .participant = participant,
            } },
        });
        return {};
    }

    fn beginBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().beginBulkIngest(table_name);
        lease.entry.bulk_ingest_active.store(true, .release);
        return {};
    }

    const BulkFinishCallbacks = struct {
        options: backend_types.BulkIngestFinishOptions,

        fn progress(
            ctx: ?*anyopaque,
            value: *const abi.BulkProgress,
        ) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            const callback = self.options.progress_fn orelse return;
            callback(self.options.progress_ctx.?, .{
                .phase = switch (value.phase) {
                    .begin => .begin,
                    .split => .split,
                    .publish => .publish,
                    .complete => .complete,
                },
                .publish_window = value.publish_window,
                .split_steps = value.split_steps,
                .deferred_leaf_splits = value.deferred_leaf_splits,
                .elapsed_ns = value.elapsed_ns,
            });
        }

        fn admission(ctx: ?*anyopaque) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.options.checkAdmission() catch |err| return switch (err) {
                error.WouldBlock, error.StorageBusy, error.ResourceBudgetExceeded => .busy,
                error.Timeout => .timeout,
                error.Canceled, error.Cancelled => .cancelled,
                error.HAReadOnlyStandby => .read_only,
                else => .internal,
            };
            return .ok;
        }
    };

    fn finishBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var callbacks = BulkFinishCallbacks{ .options = options };
        const request = abi.BulkFinishRequest{
            .compact = @intFromBool(options.compact),
            .flush = @intFromBool(options.flush),
            .has_max_deferred_l0_runs = @intFromBool(options.max_deferred_l0_runs != null),
            .has_max_foreground_compaction_input_bytes = @intFromBool(options.max_foreground_compaction_input_bytes != null),
            .has_max_foreground_compaction_ns = @intFromBool(options.max_foreground_compaction_ns != null),
            .has_max_deferred_hbc_leaf_splits_per_publish = @intFromBool(options.max_deferred_hbc_leaf_splits_per_publish != null),
            .has_max_deferred_hbc_leaf_split_members_per_publish = @intFromBool(options.max_deferred_hbc_leaf_split_members_per_publish != null),
            .has_bulk_rebuild_hbc_leaf_min_members = @intFromBool(options.bulk_rebuild_hbc_leaf_min_members != null),
            .table_name = .fromSlice(table_name),
            .max_deferred_l0_runs = @intCast(options.max_deferred_l0_runs orelse 0),
            .max_foreground_compaction_steps = @intCast(options.max_foreground_compaction_steps),
            .max_foreground_compaction_input_bytes = options.max_foreground_compaction_input_bytes orelse 0,
            .max_foreground_compaction_ns = options.max_foreground_compaction_ns orelse 0,
            .max_deferred_hbc_leaf_splits_per_publish = @intCast(options.max_deferred_hbc_leaf_splits_per_publish orelse 0),
            .max_deferred_hbc_leaf_split_members_per_publish = @intCast(options.max_deferred_hbc_leaf_split_members_per_publish orelse 0),
            .bulk_rebuild_hbc_leaf_min_members = @intCast(options.bulk_rebuild_hbc_leaf_min_members orelse 0),
            .callback_ctx = &callbacks,
            .progress_fn = if (options.progress_fn != null and options.progress_ctx != null)
                BulkFinishCallbacks.progress
            else
                null,
            .admission_fn = if (options.admission_fn != null and options.admission_ctx != null)
                BulkFinishCallbacks.admission
            else
                null,
        };
        try lease.owner().finishBulkIngest(&request);
        lease.entry.bulk_ingest_active.store(false, .release);
        return {};
    }

    fn abortBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = self.acquire(group_id, table_name) catch |err| {
            std.log.warn("storage owner bulk abort acquire failed table={s} group_id={d} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return;
        };
        defer lease.deinit();
        lease.owner().abortBulkIngest(table_name) catch |err| {
            std.log.warn("storage owner bulk abort failed table={s} group_id={d} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return;
        };
        lease.entry.bulk_ingest_active.store(false, .release);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, group_ids.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(items);
        }
        for (group_ids) |group_id| {
            var lease = try self.acquire(group_id, table_name);
            defer lease.deinit();
            var response = try lease.owner().runtimeStatusJson(table_name);
            defer response.deinit();
            var parsed = try std.json.parseFromSlice(
                runtime_status.LocalTableRuntimeStatus,
                alloc,
                response.bytes(),
                .{},
            );
            defer parsed.deinit();
            items[initialized] = try parsed.value.clone(alloc);
            items[initialized].group_id = group_id;
            items[initialized].metadata = .{
                .updated_at_ns = platform_time.monotonicNs(),
                .source = .live_writer_publish,
                .freshness = .fresh,
                .lsm_root_generation = lease.entry.generation,
            };
            initialized += 1;
        }
        return .{ .items = items };
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().textStatsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().algebraicPartialsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        try self.prepareGraphExpandRead(alloc, group_id, controlled, consistency);
        const request_json = try distributed_graph.encodeGraphExpandRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().graphExpandJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (req.cancellation) |flag| @ptrCast(flag) else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphExpandResponse(alloc, response.bytes());
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        try self.prepareQueryRead(group_id, table_reads.graphHydrateSearchRequest(controlled), consistency);
        const request_json = try distributed_graph.encodeGraphHydrateRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().graphHydrateJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (req.cancellation) |flag| @ptrCast(flag) else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphHydrateResponse(alloc, response.bytes());
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        try self.prepareLookupRead(group_id, req.key, .{}, consistency);
        const request_json = try distributed_graph.encodeGraphEdgesRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().graphEdgesJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (req.cancellation) |flag| @ptrCast(flag) else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphEdgesResponse(alloc, response.bytes());
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.SearchResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency);
        defer response.deinit();
        return try table_reads.parseStorageKernelSearchResult(alloc, response.bytes());
    }
};
