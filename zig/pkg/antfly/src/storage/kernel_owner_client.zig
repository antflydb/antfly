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
const error_identity = @import("kernel_error_identity");
const maintenance = @import("maintenance.zig");
const system_store_client = @import("kernel_system_store_client.zig");

pub const LocalTransitionAction = abi.LocalTransitionAction;
pub const LocalTransitionPhase = abi.LocalTransitionPhase;
pub const LocalTransitionResultKind = abi.LocalTransitionResultKind;
pub const LocalTransitionRequest = abi.LocalTransitionRequest;
pub const LocalTransitionResult = abi.LocalTransitionResult;
pub const AggregationHit = abi.AggregationHit;
pub const AggregationRequest = abi.AggregationRequest;
pub const singleNamespaceStore = system_store_client.singleNamespaceStore;

pub const Context = struct {
    handle: ?*anyopaque = null,

    pub fn ensure(self: *Context) !void {
        return try self.ensureWith(.{});
    }

    pub fn ensureWith(self: *Context, request: abi.ContextRequest) !void {
        if (self.handle != null) return;
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_context_create(&request, &handle));
        self.handle = handle orelse return error.StorageKernelFailure;
    }

    pub fn deinit(self: *Context) void {
        const status = abi.antfly_storage_context_destroy(self.handle);
        std.debug.assert(status == .ok);
        self.* = .{};
    }

    pub fn metrics(self: *Context) !abi.ContextMetricsResult {
        try self.ensure();
        var result: abi.ContextMetricsResult = .{};
        try statusToError(abi.antfly_storage_context_metrics(self.handle, &result));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return result;
    }

    pub fn systemStore(
        self: *Context,
        allocator: std.mem.Allocator,
        namespace: []const u8,
    ) !@import("backend_erased.zig").Store {
        try self.ensure();
        return try system_store_client.open(allocator, self.handle, namespace);
    }

    pub fn liteAdoptionProbe(self: *Context) !abi.LiteAdoptionProbeResult {
        var result: abi.LiteAdoptionProbeResult = .{};
        try statusToError(abi.antfly_storage_context_lite_adoption_probe(self.handle, &result));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return result;
    }

    pub fn liteAdoptAndVerify(self: *Context, request: abi.LiteAdoptionRequest) !void {
        try statusToError(abi.antfly_storage_context_lite_adopt_and_verify(self.handle, &request));
    }

    pub fn liteMarkStandalone(self: *Context) !void {
        try statusToError(abi.antfly_storage_context_lite_mark_standalone(self.handle));
    }

    pub fn maintenanceSource(self: *Context) maintenance.Source {
        return .{ .ptr = self, .vtable = &.{
            .status = contextMaintenanceStatus,
            .run = contextMaintenanceRun,
        } };
    }

    fn contextMaintenanceStatus(ptr: *anyopaque) maintenance.Status {
        const self: *Context = @ptrCast(@alignCast(ptr));
        var result: abi.ContextMaintenanceStatus = .{};
        statusToError(abi.antfly_storage_context_maintenance_status(self.handle, &result)) catch
            return .{ .engine = "kernel", .maintenance = .{} };
        return .{
            .engine = result.engine.slice(),
            .format = if (result.format.len == 0) null else result.format.slice(),
            .fsync = if (result.has_fsync != 0) result.fsync != 0 else null,
            .maintenance = .{
                .check = result.check != 0,
                .compact = result.compact != 0,
                .vacuum = result.vacuum != 0,
                .online = result.online != 0,
                .asynchronous = result.asynchronous != 0,
            },
        };
    }

    fn contextMaintenanceRun(
        ptr: *anyopaque,
        operation: maintenance.Operation,
        cancel: *const maintenance.CancelToken,
    ) !maintenance.Result {
        const self: *Context = @ptrCast(@alignCast(ptr));
        var result: abi.ContextMaintenanceResult = .{};
        try statusToError(abi.antfly_storage_context_maintenance_run(self.handle, &.{
            .operation = switch (operation) {
                .check => .check,
                .compact => .compact,
                .vacuum => .vacuum,
            },
            .cancel_token = cancel,
        }, &result));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return .{
            .valid = if (result.has_valid != 0) result.valid != 0 else null,
            .issue = if (result.issue.len == 0) null else result.issue.slice(),
            .file_size = if (result.present_mask & (1 << 0) != 0) result.file_size else null,
            .valid_prefix_size = if (result.present_mask & (1 << 1) != 0) result.valid_prefix_size else null,
            .reclaimable_bytes = if (result.present_mask & (1 << 2) != 0) result.reclaimable_bytes else null,
            .before_size = if (result.present_mask & (1 << 3) != 0) result.before_size else null,
            .after_size = if (result.present_mask & (1 << 4) != 0) result.after_size else null,
            .reclaimed_bytes = if (result.present_mask & (1 << 5) != 0) result.reclaimed_bytes else null,
            .live_file_count = if (result.present_mask & (1 << 6) != 0) result.live_file_count else null,
            .live_bytes = if (result.present_mask & (1 << 7) != 0) result.live_bytes else null,
        };
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

fn haSeedRequest(operation: abi.HASeedOperation, request_json: []const u8) abi.HASeedJsonRequest {
    return .{
        .operation = operation,
        .request_json = .fromSlice(request_json),
    };
}

pub fn haSeedActivate(request_json: []const u8) !Response {
    var response: Response = .{};
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_storage_ha_seed_activate_json(
        &haSeedRequest(.activate, request_json),
        &response.buffer,
        &failure,
    );
    try acceptStorageOwnerFailure(status, failure, "HA seed activation");
    return response;
}

pub fn haSeedValidateActivatedGeneration(request_json: []const u8) !u64 {
    var result: abi.HASeedValidationResult = .{};
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_storage_ha_seed_validate_json(
        &haSeedRequest(.validate_activated_generation, request_json),
        &result,
        &failure,
    );
    try acceptStorageOwnerFailure(status, failure, "HA seed startup validation");
    if (result.version != abi.abi_version or result._reserved0 != 0)
        return error.InvalidAbiVersion;
    return result.checkpoint_lsn;
}

pub fn haSeedPruneActivatedGenerations(request_json: []const u8) !Response {
    var response: Response = .{};
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_storage_ha_seed_prune_json(
        &haSeedRequest(.prune_activated_generations, request_json),
        &response.buffer,
        &failure,
    );
    try acceptStorageOwnerFailure(status, failure, "HA seed generation prune");
    return response;
}

pub fn aggregate(request: AggregationRequest) !Response {
    var response: Response = .{};
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_storage_aggregate_json(&request, &response.buffer, &failure);
    try acceptStorageOwnerFailure(status, failure, "storage-owner aggregation");
    return response;
}

pub const Owner = struct {
    handle: ?*anyopaque,

    pub fn open(request: abi.OpenRequest) !Owner {
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_owner_open(&request, &handle));
        return .{ .handle = handle orelse return error.StorageKernelFailure };
    }

    /// Runs one complete local split/merge phase while both opaque owners are
    /// borrowed by the caller. `apply_store` is optional; a null handle asks
    /// the provider to open the phase-local projection store itself.
    pub fn localTransition(
        self: *Owner,
        secondary: *Owner,
        apply_store: ?*anyopaque,
        request: abi.LocalTransitionRequest,
    ) !abi.LocalTransitionResult {
        var result: abi.LocalTransitionResult = .{};
        try statusToError(abi.antfly_storage_owner_local_transition(
            self.handle,
            secondary.handle,
            apply_store,
            &request,
            &result,
        ));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return result;
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

    pub fn preflightWriteAdmission(self: *Owner, table_name: []const u8) !void {
        try statusToError(abi.antfly_storage_owner_preflight_write_admission(self.handle, &.{
            .table_name = .fromSlice(table_name),
        }));
    }

    pub fn findMedianKey(self: *Owner, table_name: []const u8) !?Response {
        var response: Response = .{};
        const status = abi.antfly_storage_owner_find_median_key(self.handle, &.{
            .table_name = .fromSlice(table_name),
        }, &response.buffer);
        if (status == .not_found) return null;
        try statusToError(status);
        return response;
    }

    pub fn beginBulkIngest(self: *Owner, table_name: []const u8) !void {
        try statusToError(abi.antfly_storage_owner_bulk_begin(self.handle, &.{
            .table_name = .fromSlice(table_name),
        }));
    }

    pub fn finishBulkIngest(self: *Owner, request: *const abi.BulkFinishRequest) !void {
        try statusToError(self.finishBulkIngestStatus(request));
    }

    /// Raw status is exposed only for a synchronous consumer adapter that owns
    /// callback error state. It must inspect that state before translating the
    /// provider status so the unwind sentinel cannot replace the exact
    /// callback-originated error.
    pub fn finishBulkIngestStatus(self: *Owner, request: *const abi.BulkFinishRequest) abi.Status {
        return abi.antfly_storage_owner_bulk_finish(self.handle, request);
    }

    pub fn abortBulkIngest(self: *Owner, table_name: []const u8) !void {
        try statusToError(abi.antfly_storage_owner_bulk_abort(self.handle, &.{
            .table_name = .fromSlice(table_name),
        }));
    }

    pub fn batchJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        return try self.batchJsonWithDocumentChildRangeDispatcher(
            table_name,
            request_json,
            null,
            null,
        );
    }

    pub fn batchJsonWithDocumentChildRangeDispatcher(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        dispatch_ctx: ?*anyopaque,
        dispatch_fn: ?abi.DocumentChildRangeDispatchFn,
    ) !Response {
        if ((dispatch_ctx == null) != (dispatch_fn == null)) return error.InvalidArgument;
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_batch_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
                .document_child_range_dispatch_ctx = dispatch_ctx,
                .document_child_range_dispatch_fn = dispatch_fn,
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
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_storage_owner_query_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
            &failure,
        );
        error_identity.validateFailureEnvelope(status, &failure, abi.abi_version) catch |err| {
            std.log.err(
                "storage-owner query returned inconsistent failure identity status={s} identity_status={s} identity_boundary={s} identity_version={d} operation={d}",
                .{ @tagName(status), @tagName(failure.status), @tagName(failure.boundary), failure.boundary_version, failure.operation },
            );
            return err;
        };
        if (status != .ok and failure.boundary != .storage_owner and failure.boundary != .local_query) {
            std.log.err("storage-owner query returned failure from unexpected boundary={s}", .{@tagName(failure.boundary)});
            return error.InvalidBoundaryFailureIdentity;
        }
        statusToError(status) catch |err| {
            if (status == .internal) {
                std.log.err("storage-owner query failed provider_error={s} hash={x} operation={d}", .{
                    failure.errorName(),
                    failure.error_name_hash,
                    failure.operation,
                });
            }
            return err;
        };
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
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_storage_owner_preflight_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner preflight");
        return response;
    }

    pub fn textStatsJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_storage_owner_text_stats_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner text stats");
        return response;
    }

    pub fn algebraicPartialsJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_storage_owner_algebraic_partials_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner algebraic partials");
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
        var failure: abi.FailureIdentity = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        const status = abi.antfly_storage_owner_graph_expand_json(
            self.handle,
            &request,
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner graph expand");
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
        var failure: abi.FailureIdentity = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        const status = abi.antfly_storage_owner_graph_hydrate_json(
            self.handle,
            &request,
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner graph hydrate");
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
        var failure: abi.FailureIdentity = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        const status = abi.antfly_storage_owner_graph_edges_json(
            self.handle,
            &request,
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner graph edges");
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

    pub fn restoreStateJson(self: *Owner, table_name: []const u8) !?Response {
        var response: Response = .{};
        const request = operationRequest(table_name, "");
        const status = abi.antfly_storage_owner_restore_state_json(
            self.handle,
            &request,
            &response.buffer,
        );
        if (status == .not_found) return null;
        try statusToError(status);
        return response;
    }

    pub fn textMemoryJson(self: *Owner, table_name: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_text_memory_json(
            self.handle,
            &.{ .table_name = .fromSlice(table_name) },
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

    pub fn applyRestoreBootstrap(request: abi.RestoreBootstrapRequest) !void {
        try statusToError(abi.antfly_storage_restore_apply_bootstrap(&request));
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

fn acceptStorageOwnerFailure(
    status: abi.Status,
    failure: abi.FailureIdentity,
    label: []const u8,
) !void {
    error_identity.validateFailureEnvelope(status, &failure, abi.abi_version) catch |err| {
        std.log.err(
            "{s} returned inconsistent failure identity status={s} identity_status={s} boundary={s} version={d} operation={d} error={s} hash={x}",
            .{
                label,
                @tagName(status),
                @tagName(failure.status),
                @tagName(failure.boundary),
                failure.boundary_version,
                failure.operation,
                failure.boundedErrorName(),
                failure.error_name_hash,
            },
        );
        return err;
    };
    if (status != .ok and failure.boundary != .storage_owner and failure.boundary != .local_query) {
        std.log.err("{s} returned failure from unexpected boundary={s}", .{ label, @tagName(failure.boundary) });
        return error.InvalidBoundaryFailureIdentity;
    }
    statusToError(status) catch |err| {
        if (status == .internal and failure.error_name_len != 0) {
            std.log.err("{s} failed boundary={s} operation={d} provider_error={s} hash={x}", .{
                label,
                @tagName(failure.boundary),
                failure.operation,
                failure.errorName(),
                failure.error_name_hash,
            });
        }
        return err;
    };
}

pub fn statusToError(status: abi.Status) !void {
    return error_identity.statusToError(status);
}

test "storage kernel aggregation statuses preserve error identity" {
    try std.testing.expectError(error.InvalidAggregation, statusToError(.invalid_aggregation));
    try std.testing.expectError(error.UnsupportedAggregation, statusToError(.unsupported_aggregation));
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, statusToError(.query_candidate_budget_exceeded));
    try std.testing.expectError(error.InvalidIndexConfig, statusToError(.invalid_index_config));
    try std.testing.expectError(error.AlgebraicPlannerScanTooLarge, statusToError(.algebraic_planner_scan_too_large));
    try std.testing.expectError(error.AlgebraicResultBucketLimit, statusToError(.algebraic_result_bucket_limit));
    try std.testing.expectError(error.InvalidAlgebraicTensorExpr, statusToError(.invalid_algebraic_tensor_expr));
    try std.testing.expectError(error.InvalidAlgebraicTensorRow, statusToError(.invalid_algebraic_tensor_row));
}
