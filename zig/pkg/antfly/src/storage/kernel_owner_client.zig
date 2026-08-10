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

//! Typed consumer for the compiled storage owner. It intentionally imports
//! only the ABI contract, never DB, LSM, indexes, or storage implementation.

const std = @import("std");
const abi = @import("kernel_owner_abi");

pub const Context = struct {
    handle: ?*anyopaque = null,

    pub fn ensure(self: *Context) !void {
        if (self.handle != null) return;
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_context_create(&.{}, &handle));
        self.handle = handle orelse return error.StorageKernelFailure;
    }

    pub fn deinit(self: *Context) void {
        const status = abi.antfly_storage_context_destroy(self.handle);
        std.debug.assert(status == .ok);
        self.* = .{};
    }
};

pub const Response = struct {
    buffer: abi.OwnedBytes = .{},

    pub fn bytes(self: Response) []const u8 {
        return self.buffer.slice();
    }

    pub fn deinit(self: *Response) void {
        abi.antfly_storage_owner_buffer_destroy(&self.buffer);
        self.* = undefined;
    }
};

pub const VersionedResponse = struct {
    response: abi.VersionedOwnedBytes = .{},

    pub fn bytes(self: VersionedResponse) []const u8 {
        return self.response.buffer.slice();
    }

    pub fn version(self: VersionedResponse) u64 {
        return self.response.version;
    }

    pub fn deinit(self: *VersionedResponse) void {
        abi.antfly_storage_owner_buffer_destroy(&self.response.buffer);
        self.* = undefined;
    }
};

pub const Owner = struct {
    handle: ?*anyopaque,

    pub fn open(request: abi.OpenRequest) !Owner {
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_owner_open(&request, &handle));
        return .{ .handle = handle orelse return error.StorageKernelFailure };
    }

    pub fn deinit(self: *Owner) void {
        abi.antfly_storage_owner_close(self.handle);
        self.* = undefined;
    }

    pub fn configure(
        self: *Owner,
        table_name: []const u8,
        schema_json: []const u8,
        indexes_json: []const u8,
    ) !void {
        try statusToError(abi.antfly_storage_owner_configure(self.handle, &.{
            .table_name = .fromSlice(table_name),
            .schema_json = .fromSlice(schema_json),
            .indexes_json = .fromSlice(indexes_json),
        }));
    }

    pub fn reconcile(
        self: *Owner,
        table_name: []const u8,
        schema_json: []const u8,
        indexes_json: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !abi.ReconcileResult {
        var result: abi.ReconcileResult = .{};
        try statusToError(abi.antfly_storage_owner_reconcile(self.handle, &.{
            .advance_index_repair = @intFromBool(advance_index_repair),
            .table_name = .fromSlice(table_name),
            .schema_json = .fromSlice(schema_json),
            .indexes_json = .fromSlice(indexes_json),
            .target_index_name = .fromSlice(target_index_name orelse ""),
        }, &result));
        if (result.version != abi.abi_version) return error.InvalidAbi;
        return result;
    }

    pub fn beginBulkIngest(self: *Owner, table_name: []const u8) !void {
        try statusToError(abi.antfly_storage_owner_bulk_begin(self.handle, &.{
            .table_name = .fromSlice(table_name),
        }));
    }

    pub fn finishBulkIngest(self: *Owner, request: *const abi.BulkFinishRequest) !void {
        try statusToError(abi.antfly_storage_owner_bulk_finish(self.handle, request));
    }

    pub fn abortBulkIngest(self: *Owner, table_name: []const u8) !void {
        try statusToError(abi.antfly_storage_owner_bulk_abort(self.handle, &.{
            .table_name = .fromSlice(table_name),
        }));
    }

    pub fn batchJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_batch_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn replicatedBatchJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_replicated_batch_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn transactionStatus(
        self: *Owner,
        table_name: []const u8,
        txn_id: [16]u8,
    ) !abi.TxnStatus {
        var result: abi.TransactionStatusResult = .{};
        try statusToError(abi.antfly_storage_owner_transaction_status(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .txn_id = .{ .bytes = txn_id },
            },
            &result,
        ));
        if (result.version != abi.abi_version) return error.InvalidAbi;
        return result.status;
    }

    pub fn waitForSync(self: *Owner, table_name: []const u8, sync_level: abi.SyncLevel) !void {
        try statusToError(abi.antfly_storage_owner_wait_for_sync(
            self.handle,
            &.{
                .sync_level = @intFromEnum(sync_level),
                .table_name = .fromSlice(table_name),
            },
        ));
    }

    pub fn applyHAReplicationRecord(
        self: *Owner,
        table_name: []const u8,
        record: HAReplicationRecord,
    ) !void {
        try statusToError(abi.antfly_storage_owner_apply_ha_replication_record(
            self.handle,
            &.{
                .flags = record.flags,
                .table_name = .fromSlice(table_name),
                .record_kind = record.record_kind,
                .payload_codec = record.payload_codec,
                .cluster_id = record.cluster_id,
                .shard_id = record.shard_id,
                .table_id = record.table_id,
                .timeline_id = record.timeline_id,
                .epoch = record.epoch,
                .lsn = record.lsn,
                .previous_lsn = record.previous_lsn,
                .commit_timestamp_ns = record.commit_timestamp_ns,
                .payload = .fromSlice(record.payload),
            },
        ));
    }

    pub fn backupJson(
        self: *Owner,
        table_name: []const u8,
        backup_root: []const u8,
        backup_id: []const u8,
        format: abi.BackupFormat,
    ) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_backup_json(
            self.handle,
            &.{
                .format = @intFromEnum(format),
                .table_name = .fromSlice(table_name),
                .backup_root = .fromSlice(backup_root),
                .backup_id = .fromSlice(backup_id),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn repairRestore(self: *Owner, request: *const abi.RestorePrepareRequest) !void {
        try statusToError(abi.antfly_storage_owner_restore_repair(self.handle, request));
    }

    pub fn queryJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_query_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn lookupJson(self: *Owner, table_name: []const u8, request_json: []const u8) !VersionedResponse {
        var response: VersionedResponse = .{};
        try statusToError(abi.antfly_storage_owner_lookup_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.response,
        ));
        return response;
    }

    pub fn scanNdjson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_scan_ndjson(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn preflightJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_preflight_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn textStatsJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_text_stats_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn algebraicPartialsJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_algebraic_partials_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn graphExpandJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        execution_deadline_ns: ?u64,
        cancellation_flag: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        try statusToError(abi.antfly_storage_owner_graph_expand_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn graphHydrateJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        execution_deadline_ns: ?u64,
        cancellation_flag: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        try statusToError(abi.antfly_storage_owner_graph_hydrate_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn graphEdgesJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        execution_deadline_ns: ?u64,
        cancellation_flag: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        try statusToError(abi.antfly_storage_owner_graph_edges_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn documentArtifactManifestJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
    ) !Response {
        var response: Response = .{};
        const request = operationRequest(table_name, request_json);
        try statusToError(abi.antfly_storage_owner_document_artifact_manifest_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn documentArtifactManifestsJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
    ) !Response {
        var response: Response = .{};
        const request = operationRequest(table_name, request_json);
        try statusToError(abi.antfly_storage_owner_document_artifact_manifests_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn artifactOperationJson(
        self: *Owner,
        table_name: []const u8,
        operation: abi.ArtifactOperation,
        request_json: []const u8,
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
        defer_durable_index_repair_execution: bool,
    ) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_artifact_operation_json(
            self.handle,
            &.{
                .operation = @intFromEnum(operation),
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
                .cancellation_ctx = cancellation_ctx,
                .cancellation_fn = cancellation_fn,
                .defer_durable_index_repair_execution = @intFromBool(defer_durable_index_repair_execution),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn runtimeStatusJson(self: *Owner, table_name: []const u8) !Response {
        var response: Response = .{};
        const request = operationRequest(table_name, "");
        try statusToError(abi.antfly_storage_owner_runtime_status_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn maintenance(
        self: *Owner,
        table_name: []const u8,
        action: abi.MaintenanceAction,
    ) !abi.MaintenanceResult {
        var result: abi.MaintenanceResult = .{};
        try statusToError(abi.antfly_storage_owner_maintenance(
            self.handle,
            &.{
                .action = @intFromEnum(action),
                .table_name = .fromSlice(table_name),
            },
            &result,
        ));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return result;
    }
};

pub const HAReplicationRecord = struct {
    record_kind: u16,
    payload_codec: u16,
    flags: u32 = 0,
    cluster_id: u64,
    shard_id: u64 = 0,
    table_id: u64 = 0,
    timeline_id: u64,
    epoch: u64,
    lsn: u64,
    previous_lsn: u64,
    commit_timestamp_ns: i64 = 0,
    payload: []const u8 = &.{},
};

pub const Snapshot = struct {
    handle: ?*anyopaque,

    pub fn prepare(request: abi.SnapshotPrepareRequest) !Snapshot {
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_snapshot_prepare(&request, &handle));
        return .{ .handle = handle orelse return error.StorageKernelFailure };
    }

    pub fn prepareRestore(request: abi.RestorePrepareRequest) !RestorePreparation {
        var result: abi.RestorePrepareResult = .{};
        try statusToError(abi.antfly_storage_restore_prepare(&request, &result));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return switch (result.state) {
            .prepared => .{ .prepared = .{
                .handle = result.snapshot orelse return error.StorageKernelFailure,
            } },
            .already_imported => blk: {
                if (result.snapshot != null) return error.StorageKernelFailure;
                break :blk .already_imported;
            },
        };
    }

    pub fn reconcileRestore(request: abi.RestorePrepareRequest) !void {
        try statusToError(abi.antfly_storage_restore_reconcile(&request));
    }

    pub fn promote(self: *Snapshot) !void {
        try statusToError(abi.antfly_storage_snapshot_promote(self.handle));
    }

    pub fn publishPrepared(self: *Snapshot) !bool {
        var result: abi.SnapshotPublishResult = .{};
        try statusToError(abi.antfly_storage_snapshot_publish_prepared(self.handle, &result));
        return result.durability_uncertain != 0;
    }

    pub fn commit(self: *Snapshot) !void {
        try statusToError(abi.antfly_storage_snapshot_commit(self.handle));
    }

    pub fn rollback(self: *Snapshot) !void {
        try statusToError(abi.antfly_storage_snapshot_rollback(self.handle));
    }

    pub fn deinit(self: *Snapshot) void {
        abi.antfly_storage_snapshot_destroy(self.handle);
        self.* = undefined;
    }
};

pub const RestorePreparation = union(enum) {
    prepared: Snapshot,
    already_imported,
};

fn operationRequest(
    table_name: []const u8,
    request_json: []const u8,
) abi.JsonOperationRequest {
    return .{
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
    };
}

fn controlledRequest(
    table_name: []const u8,
    request_json: []const u8,
    execution_deadline_ns: ?u64,
    cancellation_flag: ?*const anyopaque,
) abi.ControlledJsonOperationRequest {
    return .{
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .execution_deadline_ns = execution_deadline_ns orelse 0,
        .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
        .cancellation_flag = cancellation_flag,
    };
}

pub fn statusToError(status: abi.Status) !void {
    return switch (status) {
        .ok => {},
        .invalid_abi => error.InvalidAbiVersion,
        .invalid_argument => error.InvalidArgument,
        .not_found => error.NotFound,
        .busy => error.StorageBusy,
        .version_conflict => error.VersionConflict,
        .intent_conflict => error.IntentConflict,
        .transaction_not_found => error.TxnNotFound,
        .read_only => error.ReadOnly,
        .out_of_memory => error.OutOfMemory,
        .corrupted => error.Corrupted,
        .identity_namespace_mismatch => error.DocIdentityNamespaceMismatch,
        .invalid_query => error.InvalidQueryRequest,
        .unsupported_query => error.UnsupportedQueryRequest,
        .index_not_found => error.IndexNotFound,
        .identity_read_generation_changed => error.IdentityReadGenerationChanged,
        .timeout => error.Timeout,
        .cancelled => error.Cancelled,
        .restore_identity_mismatch => error.RestoreIdentityMismatch,
        .invalid_backup => error.InvalidBackupRequest,
        .backup_integrity => error.BackupArtifactIntegrityMismatch,
        .unsupported_backup_migration => error.UnsupportedBackupMigrationState,
        .restore_identity_namespace_mismatch => error.IdentityNamespaceMismatch,
        .internal => error.StorageKernelFailure,
    };
}
