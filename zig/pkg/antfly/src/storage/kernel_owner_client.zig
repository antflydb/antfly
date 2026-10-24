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

//! Typed consumer for the compiled storage owner. It intentionally imports
//! only the ABI contract, never DB, LSM, indexes, or storage implementation.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const maintenance = @import("antfly_local_sources").storage_maintenance;
const system_store_client = @import("kernel_system_store_client.zig");

pub const LocalTransitionAction = abi.LocalTransitionAction;
pub const LocalTransitionPhase = abi.LocalTransitionPhase;
pub const LocalTransitionResultKind = abi.LocalTransitionResultKind;
pub const LocalTransitionRequest = abi.LocalTransitionRequest;
pub const LocalTransitionResult = abi.LocalTransitionResult;
pub const ContextResourceBudgetStats = abi.ContextResourceBudgetStats;
pub const ContextMetricsResult = abi.ContextMetricsResult;
pub const AggregationHit = abi.AggregationHit;
pub const AggregationRequest = abi.AggregationRequest;
pub const singleNamespaceStore = system_store_client.singleNamespaceStore;

pub const Context = struct {
    handle: ?*anyopaque = null,

    pub fn memoryParent(self: *const Context) !abi.MemoryParent {
        var result: abi.MemoryParent = undefined;
        if (!abi.antfly_storage_context_memory_parent(self.handle, &result)) return error.StorageContextUnavailable;
        return result;
    }

    pub fn reclaimBackupPinJson(self: *Context, request: abi.BackupPinReclaimRequest) !Response {
        try self.ensure();
        var response: Response = .{};
        try statusToError(abi.antfly_storage_backup_pin_reclaim_json(self.handle, &request, &response.buffer));
        return response;
    }

    pub fn ensure(self: *Context) !void {
        return try self.ensureWith(.{});
    }

    pub fn ensureWith(self: *Context, request: abi.ContextRequest) !void {
        if (self.handle != null) return;
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_context_create(&request, &handle));
        self.handle = handle orelse return error.StorageKernelFailure;
    }

    /// The node runtime outlives this context and every attached owner.
    pub fn ensureWithBackendRuntime(self: *Context, runtime: *@import("antfly_local_sources").storage_background_runtime.BackendRuntime, options: @import("kernel_runtime_services.zig").RuntimeRequest.Options) !void {
        if (self.handle != null) return;
        var request = try @import("kernel_runtime_services.zig").RuntimeRequest.init(runtime, options);
        try self.ensureWithRuntime(request.request());
    }

    pub fn ensureWithRuntime(self: *Context, request: @import("kernel_runtime_services.zig").Request) !void {
        if (self.handle != null) return;
        var handle: ?*anyopaque = null;
        try statusToError(@import("kernel_runtime_services.zig").antfly_storage_context_create_with_runtime(&request, &handle));
        self.handle = handle orelse return error.StorageKernelFailure;
    }

    /// Joined destruction. A busy physical owner keeps its exact handle for
    /// retry; an idle owner performs the existing executor/cache joins.
    pub fn tryDeinit(self: *Context) !void {
        try statusToError(abi.antfly_storage_context_destroy(self.handle));
        self.* = .{};
    }
    pub fn deinit(self: *Context) void {
        self.tryDeinit() catch @panic("storage context still retained");
    }

    pub fn attachInferenceProvider(self: *Context, inference_handle: *anyopaque) !void {
        try self.ensure();
        try statusToError(abi.antfly_storage_context_attach_inference_provider(
            self.handle,
            inference_handle,
        ));
    }

    pub fn configureSecrets(self: *Context, store: ?*anyopaque) !void {
        try self.ensure();
        try statusToError(abi.antfly_storage_context_configure_secrets(self.handle, store));
    }
    pub fn configureNativeQueries(self: *Context, setup: []const u8) !void {
        try self.ensure();
        try statusToError(abi.antfly_storage_context_configure_native_queries(self.handle, .fromSlice(setup)));
    }

    /// Keep the remote-content publisher live across the compiled owner boundary.
    pub fn configureRemoteContent(self: *Context, config: ?*const @import("antfly_scraping").RemoteContentConfig) !void {
        const remote = config orelse return self.configureRemoteContentSecurity("");
        const Adapter = struct {
            fn acquire(raw: *anyopaque, out: *abi.OwnedBytes) callconv(.c) abi.Status {
                const facade: *const @import("antfly_scraping").RemoteContentConfig = @ptrCast(@alignCast(raw));
                const bytes = @import("antfly_local_sources").common_config.remoteContentSecurityJsonAlloc(std.heap.smp_allocator, facade) catch |err| return switch (err) {
                    error.OutOfMemory => .out_of_memory,
                    error.Timeout => .timeout,
                    error.Canceled => .cancelled,
                    else => .resource_temporarily_unavailable,
                };
                out.* = .{ .ptr = bytes.ptr, .len = bytes.len };
                return .ok;
            }
            fn release(_: *anyopaque, bytes: abi.OwnedBytes) callconv(.c) void {
                std.heap.smp_allocator.free(bytes.slice());
            }
        };
        const services = @import("kernel_runtime_services.zig");
        const source = services.RemoteContentSource{ .context = @ptrCast(@constCast(remote)), .acquire = Adapter.acquire, .release = Adapter.release };
        try self.ensure();
        try statusToError(services.antfly_storage_context_configure_remote_content_source(self.handle, &source));
    }

    pub fn configureRemoteContentSecurity(self: *Context, security_json: []const u8) !void {
        try self.ensure();
        try statusToError(abi.antfly_storage_context_configure_remote_content_security(
            self.handle,
            .fromSlice(security_json),
        ));
    }

    pub fn reserveExternal(self: *Context, bytes: usize) bool {
        return abi.antfly_storage_context_reserve_external(self.handle, bytes);
    }

    pub fn releaseExternal(self: *Context, bytes: usize) void {
        abi.antfly_storage_context_release_external(self.handle, bytes);
    }

    pub fn metrics(self: *Context) !abi.ContextMetricsResult {
        try self.ensure();
        var result: abi.ContextMetricsResult = .{};
        try statusToError(abi.antfly_storage_context_metrics(self.handle, &result));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return result;
    }

    pub fn invalidateCaches(self: *Context) !void {
        try self.ensure();
        try statusToError(abi.antfly_storage_context_invalidate_caches(self.handle));
    }

    pub fn systemStore(
        self: *Context,
        allocator: std.mem.Allocator,
        namespace: []const u8,
    ) !@import("antfly_local_sources").storage_backend_erased.Store {
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

pub const QueryResponse = struct {
    response: abi.QueryOwnedResponse = .{},

    pub fn bytes(self: QueryResponse) []const u8 {
        return self.response.buffer.slice();
    }

    pub fn identityReadGeneration(self: QueryResponse) ?u64 {
        return if (self.response.has_identity_read_generation != 0)
            self.response.identity_read_generation
        else
            null;
    }

    pub fn deinit(self: *QueryResponse) void {
        abi.antfly_storage_owner_buffer_destroy(&self.response.buffer);
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

    pub fn expectedContentDigest(self: VersionedResponse) ?[32]u8 {
        return if (self.response.has_expected_content_digest != 0) self.response.expected_content_digest else null;
    }

    pub fn deinit(self: *VersionedResponse) void {
        abi.antfly_storage_owner_buffer_destroy(&self.response.buffer);
        self.* = undefined;
    }
};

fn hotStandbySeedRequest(operation: abi.HASeedOperation, request_json: []const u8) abi.HASeedJsonRequest {
    return .{
        .operation = operation,
        .request_json = .fromSlice(request_json),
    };
}

pub fn hotStandbySeedActivate(request_json: []const u8) !Response {
    var response: Response = .{};
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_storage_hot_standby_seed_activate_json(
        &hotStandbySeedRequest(.activate, request_json),
        &response.buffer,
        &failure,
    );
    try acceptStorageOwnerFailure(status, failure, "HA seed activation");
    return response;
}

pub fn hotStandbySeedValidateActivatedGeneration(request_json: []const u8) !u64 {
    var result: abi.HASeedValidationResult = .{};
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_storage_hot_standby_seed_validate_json(
        &hotStandbySeedRequest(.validate_activated_generation, request_json),
        &result,
        &failure,
    );
    try acceptStorageOwnerFailure(status, failure, "HA seed startup validation");
    if (result.version != abi.abi_version or result._reserved0 != 0)
        return error.InvalidAbiVersion;
    return result.checkpoint_lsn;
}

pub fn hotStandbySeedPruneActivatedGenerations(request_json: []const u8) !Response {
    var response: Response = .{};
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_storage_hot_standby_seed_prune_json(
        &hotStandbySeedRequest(.prune_activated_generations, request_json),
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

    /// Caller owns the Raft completion fence until capture returns. Ownership
    /// transfers only when PreparedSnapshot.attachNative succeeds.
    pub fn captureNativeRaftSnapshot(self: *Owner, group_id: u64, through_index: u64) !NativeSnapshotCapture {
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_owner_snapshot_capture(self.handle, group_id, through_index, &handle));
        return .{ .handle = handle orelse return error.StorageKernelFailure };
    }

    pub fn restoreControlJson(self: *Owner, request: abi.RestoreOwnerControlRequest) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_restore_control_json(self.handle, &request, &response.buffer));
        return response;
    }

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

    pub fn drainClose(self: *Owner, request: abi.CloseDrainRequest) !void {
        try statusToError(abi.antfly_storage_owner_drain_close(self.handle, &request));
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
        _ = try self.preflightWriteAdmissionJson(table_name, "");
    }

    pub fn preflightWriteAdmissionJson(self: *Owner, table_name: []const u8, request_json: []const u8) !bool {
        var has_groups: u8 = 0;
        try statusToError(abi.antfly_storage_owner_preflight_write_admission(self.handle, &.{
            .table_name = .fromSlice(table_name),
            .request_json = .fromSlice(request_json),
        }, &has_groups));
        return has_groups != 0;
    }

    /// Reconstruction uses a shared generation lease. Configuration is a
    /// separate exclusive operation and cannot change under this invocation.
    pub fn repairIndex(self: *Owner, table_name: []const u8, target_index_name: ?[]const u8, controls: abi.RepairControls) !abi.ReconcileResult {
        var result: abi.ReconcileResult = .{};
        try statusToError(abi.antfly_storage_owner_reconcile(self.handle, &.{
            .advance_index_repair = 1,
            .repair_only = 1,
            .table_name = .fromSlice(table_name),
            .target_index_name = .fromSlice(target_index_name orelse ""),
            .repair_controls = controls,
        }, &result));
        if (result.version != abi.abi_version) return error.InvalidAbi;
        return result;
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
        var response: Response = .{};
        try statusToError(self.batchJsonWithCallbacksStatus(
            table_name,
            request_json,
            dispatch_ctx,
            dispatch_fn,
            null,
            null,
            &response,
        ));
        return response;
    }

    /// Raw status is exposed for callback consumers that retain an exact local
    /// error relay across the compiled provider boundary.
    pub fn batchJsonWithCallbacksStatus(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        dispatch_ctx: ?*anyopaque,
        dispatch_fn: ?abi.DocumentChildRangeDispatchFn,
        committed_effects_ctx: ?*anyopaque,
        committed_effects_fn: ?abi.CommittedBatchEffectsFn,
        response: *Response,
    ) abi.Status {
        return self.batchJsonWithCallbacksAndAdmissionStatus(table_name, request_json, dispatch_ctx, dispatch_fn, committed_effects_ctx, committed_effects_fn, null, response);
    }

    pub fn batchJsonWithCallbacksAndAdmissionStatus(self: *Owner, table_name: []const u8, request_json: []const u8, dispatch_ctx: ?*anyopaque, dispatch_fn: ?abi.DocumentChildRangeDispatchFn, committed_effects_ctx: ?*anyopaque, committed_effects_fn: ?abi.CommittedBatchEffectsFn, admission: ?*const @import("antfly_local_sources").common_read_admission.Port, response: *Response) abi.Status {
        return self.batchJsonWithCallbacksAndAdmissionContextStatus(table_name, request_json, dispatch_ctx, dispatch_fn, committed_effects_ctx, committed_effects_fn, admission, response);
    }

    pub fn batchJsonWithCallbacksAndAdmissionContextStatus(self: *Owner, table_name: []const u8, request_json: []const u8, dispatch_ctx: ?*anyopaque, dispatch_fn: ?abi.DocumentChildRangeDispatchFn, committed_effects_ctx: ?*anyopaque, committed_effects_fn: ?abi.CommittedBatchEffectsFn, admission: ?*const anyopaque, response: *Response) abi.Status {
        response.* = .{};
        if ((dispatch_ctx == null) != (dispatch_fn == null)) return .invalid_argument;
        if ((committed_effects_ctx == null) != (committed_effects_fn == null)) return .invalid_argument;
        const request: abi.BatchJsonOperationRequest = .{
            .table_name = .fromSlice(table_name),
            .request_json = .fromSlice(request_json),
            .document_child_range_dispatch_ctx = dispatch_ctx,
            .document_child_range_dispatch_fn = dispatch_fn,
            .committed_batch_effects_ctx = committed_effects_ctx,
            .committed_batch_effects_fn = committed_effects_fn,
        };
        if (admission) |port| return abi.antfly_storage_owner_batch_json_with_admission(self.handle, &request, port, &response.buffer);
        return abi.antfly_storage_owner_batch_json(self.handle, &request, &response.buffer);
    }

    pub fn transactionBeginWithAdmission(self: *Owner, _: std.mem.Allocator, table: []const u8, txn_id: [16]u8, timestamp: u64, created_at: u64, participants: []const []const u8, retain_terminal: bool, scope: ?[32]u8, plan: ?[16]u8, port: *const @import("antfly_local_sources").common_read_admission.Port) !void {
        try port.checkpoint();
        if ((scope == null) != (plan == null)) return error.RestoreStagingScopeChanged;
        const arrays = @import("antfly_local_sources").api_transaction_begin_admission;
        const participant_payload: arrays.Payload = .{ .participants = participants };
        const participant_port = arrays.Port.init(&participant_payload);
        var trace: @import("antfly_local_sources").common_read_admission.RejectionTrace = .{};
        const original = trace.bind(port.*).?;
        const status = abi.antfly_storage_owner_transaction_begin_with_admission(self.handle, &.{ .table_name = .fromSlice(table), .txn_id = .{ .bytes = txn_id }, .begin_timestamp = timestamp, .created_at_ns = created_at, .participants = &participant_port, .participants_len = participants.len, .retain_terminal = @intFromBool(retain_terminal), .has_scope = @intFromBool(scope != null), .scope = scope orelse @splat(0), .plan_id = plan orelse @splat(0), .port = &original });
        if (trace.failure) |cause| return cause;
        try statusToError(status);
    }

    pub fn transactionPrepareWithAdmission(self: *Owner, table: []const u8, txn_id: [16]u8, intents: @import("antfly_local_sources").storage_db_types.TransactionIntentRequest, port: *const @import("antfly_local_sources").common_read_admission.Port) !void {
        try port.checkpoint();
        const requests = @import("antfly_local_sources").api_transaction_prepare_admission;
        const payload: requests.Payload = .{ .request = intents };
        const borrowed = requests.Port.init(&payload);
        var trace: @import("antfly_local_sources").common_read_admission.RejectionTrace = .{};
        const original = trace.bind(port.*).?;
        const status = abi.antfly_storage_owner_transaction_prepare_with_admission(self.handle, &.{ .table_name = .fromSlice(table), .txn_id = .{ .bytes = txn_id }, .intents = &borrowed, .port = &original });
        if (trace.failure) |cause| return cause;
        try statusToError(status);
    }

    pub fn transactionAcknowledgeWithAdmission(self: *Owner, table: []const u8, txn_id: [16]u8, participants: []const []const u8, port: *const @import("antfly_local_sources").common_read_admission.Port) !void {
        try port.checkpoint();
        const arrays = @import("antfly_local_sources").api_transaction_begin_admission;
        const payload: arrays.Payload = .{ .participants = participants };
        const borrowed = arrays.Port.init(&payload);
        var trace: @import("antfly_local_sources").common_read_admission.RejectionTrace = .{};
        const original = trace.bind(port.*).?;
        const status = abi.antfly_storage_owner_transaction_acknowledge_with_admission(self.handle, &.{ .table_name = .fromSlice(table), .txn_id = .{ .bytes = txn_id }, .participants = &borrowed, .port = &original });
        if (trace.failure) |cause| return cause;
        try statusToError(status);
    }

    pub fn transactionResolveWithAdmission(self: *Owner, table: []const u8, txn_id: [16]u8, status: abi.TxnStatus, commit_version: u64, sync_level: abi.SyncLevel, port: *const @import("antfly_local_sources").common_read_admission.Port) !void {
        try port.checkpoint();
        var trace: @import("antfly_local_sources").common_read_admission.RejectionTrace = .{};
        const original = trace.bind(port.*).?;
        const result = abi.antfly_storage_owner_transaction_resolve_with_admission(self.handle, &.{ .table_name = .fromSlice(table), .txn_id = .{ .bytes = txn_id }, .status = @backingInt(status), .commit_version = commit_version, .sync_level = @backingInt(sync_level), .port = &original });
        if (trace.failure) |cause| return cause;
        try statusToError(result);
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

    pub fn replicatedBatchAtRaftEntryJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        raft_term: u64,
        raft_index: u64,
    ) !Response {
        if (raft_term == 0 or raft_index == 0) return error.InvalidArgument;
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_replicated_batch_at_raft_entry_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
                .raft_term = raft_term,
                .raft_index = raft_index,
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn nativeFkGenerationControlJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_native_fk_generation_control_json(self.handle, &.{
            .table_name = .fromSlice(table_name),
            .request_json = .fromSlice(request_json),
        }, &response.buffer));
        return response;
    }

    pub fn nativeInitialChildControlJson(self: *Owner, table_name: []const u8, request_json: []const u8, operation_index: u64) !Response {
        if (operation_index < 1 or operation_index > 3) return error.InvalidArgument;
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_native_initial_child_control_json(self.handle, &.{
            .table_name = .fromSlice(table_name),
            .request_json = .fromSlice(request_json),
            .operation_term = 1,
            .operation_index = operation_index,
        }, &response.buffer));
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

    pub fn transactionPreparationStatus(self: *Owner, table_name: []const u8, txn_id: [16]u8, port: *const @import("antfly_local_sources").common_read_admission.Port) !abi.TransactionPreparationStatusResult {
        try port.validate();
        // The status-only C ABI cannot enumerate arbitrary original controls.
        // Retain rejection at the caller edge before translating owner status.
        var trace: @import("antfly_local_sources").common_read_admission.RejectionTrace = .{};
        const original = trace.bind(port.*).?;
        var result: abi.TransactionPreparationStatusResult = .{};
        const status = abi.antfly_storage_owner_transaction_preparation_status(self.handle, &.{ .table_name = .fromSlice(table_name), .txn_id = .{ .bytes = txn_id }, .port = &original }, &result);
        if (trace.failure) |cause| return cause;
        try statusToError(status);
        if (std.enums.fromInt(abi.TxnStatus, @backingInt(result.status)) == null or result.version != abi.abi_version or result.present > 1 or result.has_binding > 1 or !std.mem.allEqual(u8, &result._reserved, 0)) return error.InvalidAbi;
        return result;
    }

    pub fn waitForSync(self: *Owner, table_name: []const u8, sync_level: abi.SyncLevel) !void {
        return try self.waitForSyncWithCancellation(table_name, sync_level, .none);
    }

    pub fn waitForSyncWithCancellation(self: *Owner, table_name: []const u8, sync_level: abi.SyncLevel, cancellation: @import("antfly_cancellation").CancellationToken) !void {
        return self.waitForSyncTargetWithCancellation(table_name, sync_level, null, cancellation);
    }

    pub fn waitForSyncTargetWithCancellation(self: *Owner, table_name: []const u8, sync_level: abi.SyncLevel, target: ?*abi.SyncTarget, cancellation: @import("antfly_cancellation").CancellationToken) !void {
        const Callback = struct {
            fn cancelled(ptr: ?*anyopaque) callconv(.c) u8 {
                const token: *const @import("antfly_cancellation").CancellationToken = @ptrCast(@alignCast(ptr.?));
                return @intFromBool(token.isCancelled());
            }
        };
        try statusToError(abi.antfly_storage_owner_wait_for_sync(
            self.handle,
            &.{
                .target = target,
                .sync_level = @backingInt(sync_level),
                .table_name = .fromSlice(table_name),
                .cancellation_ctx = @ptrCast(@constCast(&cancellation)),
                .cancellation_fn = Callback.cancelled,
            },
        ));
    }

    pub fn applyHotStandbyReplicationRecord(
        self: *Owner,
        table_name: []const u8,
        record: HotStandbyReplicationRecord,
    ) !void {
        try statusToError(abi.antfly_storage_owner_apply_hot_standby_replication_record(
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
        return self.backupWithControl(.{ .format = @backingInt(format), .table_name = .fromSlice(table_name), .backup_root = .fromSlice(backup_root), .backup_id = .fromSlice(backup_id) });
    }

    pub fn backupWithControl(self: *Owner, request: abi.BackupRequest) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_backup_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn backupPinControlJson(self: *Owner, request: abi.ControlledJsonOperationRequest) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_backup_pin_control_json(self.handle, &request, &response.buffer));
        return response;
    }

    pub fn prepareSourcePinPublicationJson(self: *Owner, request: abi.ControlledJsonOperationRequest) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_source_pin_publication_json(self.handle, &request, &response.buffer));
        return response;
    }

    pub fn sourceArtifactJson(self: *Owner, request: abi.ControlledJsonOperationRequest) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_source_artifact_json(self.handle, &request, &response.buffer));
        return response;
    }

    pub fn onlineMergeIoJson(self: *Owner, request: abi.ControlledJsonOperationRequest) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_online_merge_io_json(self.handle, &request, &response.buffer));
        return response;
    }

    pub fn repairRestore(self: *Owner, request: *const abi.RestorePrepareRequest) !void {
        try statusToError(abi.antfly_storage_owner_restore_repair(self.handle, request));
    }

    pub const QueryOptions = struct {
        execution_deadline_ns: ?u64 = null,
        cancellation_ctx: ?*anyopaque = null,
        cancellation_fn: ?abi.CancellationCheckFn = null,
        execution_runnable: ?*const anyopaque = null,
        execution: abi.LocalQueryExecutionOptions = .{},
    };

    pub fn queryJson(self: *Owner, table_name: []const u8, request_json: []const u8) !QueryResponse {
        return self.queryJsonWithOptions(table_name, request_json, .{});
    }

    pub fn queryJsonWithOptions(self: *Owner, table_name: []const u8, request_json: []const u8, options: QueryOptions) !QueryResponse {
        var response: QueryResponse = .{};
        var failure: abi.FailureIdentity = .{};
        var control = controlledRequest(table_name, request_json, options.execution_deadline_ns, options.cancellation_ctx, options.cancellation_fn);
        control.execution_runnable = options.execution_runnable;
        const status = abi.antfly_storage_owner_query_json(
            self.handle,
            &.{
                .control = control,
                .execution_options = options.execution,
            },
            &response.response,
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
            std.log.debug("storage-owner query rejected provider_error={s} boundary={s} operation={d}", .{ failure.errorName(), @tagName(failure.boundary), failure.operation });
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

    pub fn scanStream(self: *Owner, table_name: []const u8, request_json: []const u8, sink: @import("antfly_local_sources").runtime_scan_sink.ScanStreamSink) !void {
        return self.scanStreamWithOptions(table_name, request_json, sink, .{});
    }

    pub fn scanStreamWithOptions(self: *Owner, table_name: []const u8, request_json: []const u8, sink: @import("antfly_local_sources").runtime_scan_sink.ScanStreamSink, options: QueryOptions) !void {
        const Bridge = struct {
            sink: @import("antfly_local_sources").runtime_scan_sink.ScanStreamSink,
            failure: ?anyerror = null,
            fn start(ptr: ?*anyopaque) callconv(.c) u8 {
                const bridge: *@This() = @ptrCast(@alignCast(ptr.?));
                bridge.sink.start() catch |err| {
                    bridge.failure = err;
                    return 0;
                };
                return 1;
            }
            fn write(ptr: ?*anyopaque, bytes: abi.BorrowedBytes) callconv(.c) u8 {
                const bridge: *@This() = @ptrCast(@alignCast(ptr.?));
                bridge.sink.write(bytes.slice()) catch |err| {
                    bridge.failure = err;
                    return 0;
                };
                return 1;
            }
        };
        var bridge = Bridge{ .sink = sink };
        var failure: abi.FailureIdentity = .{};
        const request = controlledRequest(table_name, request_json, options.execution_deadline_ns, options.cancellation_ctx, options.cancellation_fn);
        const status = abi.antfly_storage_owner_scan_stream(self.handle, &request, &.{ .context = &bridge, .start = Bridge.start, .write = Bridge.write }, &failure);
        if (bridge.failure) |err| return err;
        try acceptStorageOwnerFailure(status, failure, "storage-owner scan");
    }

    pub fn scanNdjson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        return self.scanNdjsonWithOptions(table_name, request_json, .{});
    }

    pub fn scanNdjsonWithOptions(self: *Owner, table_name: []const u8, request_json: []const u8, options: QueryOptions) !Response {
        var response: Response = .{};
        const request = controlledRequest(table_name, request_json, options.execution_deadline_ns, options.cancellation_ctx, options.cancellation_fn);
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_storage_owner_scan_ndjson(
            self.handle,
            &request,
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner scan");
        return response;
    }

    pub fn graphMetricMaintenanceJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_storage_owner_graph_metric_maintenance_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner graph metric maintenance");
        return response;
    }

    pub fn resolverOperationJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_storage_owner_resolver_operation_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
            &failure,
        );
        try acceptStorageOwnerFailure(status, failure, "storage-owner resolver operation");
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

    pub fn textStatsJsonWithOptions(self: *Owner, table_name: []const u8, request_json: []const u8, options: QueryOptions) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        var request = controlledRequest(table_name, request_json, options.execution_deadline_ns, options.cancellation_ctx, options.cancellation_fn);
        request.execution_runnable = options.execution_runnable;
        const status = abi.antfly_storage_owner_text_stats_controlled_json(self.handle, &request, &response.buffer, &failure);
        try acceptStorageOwnerFailure(status, failure, "storage-owner controlled text stats");
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
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
        execution_runnable: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        var request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_ctx, cancellation_fn);
        request.execution_runnable = execution_runnable;
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
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
        execution_runnable: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        var request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_ctx, cancellation_fn);
        request.execution_runnable = execution_runnable;
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
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
        execution_runnable: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        var failure: abi.FailureIdentity = .{};
        var request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_ctx, cancellation_fn);
        request.execution_runnable = execution_runnable;
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

    pub fn vectorMigrationJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        const request = operationRequest(table_name, request_json);
        try statusToError(abi.antfly_storage_owner_vector_migration_json(self.handle, &request, &response.buffer));
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
                .operation = @backingInt(operation),
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

    pub fn observedDynamicFieldCapabilitySetsJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        execution_deadline_ns: ?u64,
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
    ) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_observed_dynamic_field_capability_sets_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
                .execution_deadline_ns = execution_deadline_ns orelse 0,
                .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
                .cancellation_ctx = cancellation_ctx,
                .cancellation_fn = cancellation_fn,
            },
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
                .action = @backingInt(action),
                .table_name = .fromSlice(table_name),
            },
            &result,
        ));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        return result;
    }

    pub fn captureHotStandbySeedSnapshot(self: *Owner, table_name: []const u8, token: []const u8, destination: []const u8) !void {
        var result: abi.MaintenanceResult = .{};
        try statusToError(abi.antfly_storage_owner_maintenance(self.handle, &.{
            .action = @backingInt(abi.MaintenanceAction.capture_ha_seed_snapshot),
            .table_name = .fromSlice(table_name),
            .snapshot_token = .fromSlice(token),
            .destination_root = .fromSlice(destination),
        }, &result));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
    }

    pub fn prepareHotStandbySeedSnapshot(
        self: *Owner,
        table_name: []const u8,
        deadline_ns: u64,
    ) !void {
        return self.prepareHotStandbySeedSnapshotWithLifetime(table_name, deadline_ns, null);
    }
    pub fn prepareHotStandbySeedSnapshotWithLifetime(
        self: *Owner,
        table_name: []const u8,
        deadline_ns: u64,
        lifetime: ?*const @import("antfly_runtime_abi").runnable_abi.Reentry,
    ) !void {
        if (lifetime) |original| try original.checkpoint();
        var result: abi.MaintenanceResult = .{};
        try statusToError(abi.antfly_storage_owner_maintenance(
            self.handle,
            &.{
                .action = @backingInt(abi.MaintenanceAction.prepare_ha_seed_snapshot),
                .table_name = .fromSlice(table_name),
                .deadline_ns = deadline_ns,
                .lifetime_check = lifetime,
            },
            &result,
        ));
        if (result.version != abi.abi_version) return error.InvalidAbiVersion;
        if (lifetime) |original| try original.checkpoint();
    }
};

pub const HotStandbyReplicationRecord = struct {
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

pub const NativeSnapshotCapture = struct {
    handle: ?*anyopaque,
    pub fn bindLease(self: *NativeSnapshotCapture, ctx: ?*anyopaque, release: *const fn (?*anyopaque) callconv(.c) void) !void {
        try statusToError(abi.antfly_storage_snapshot_capture_bind_lease(self.handle, ctx, release));
    }
    pub fn deinit(self: *NativeSnapshotCapture) void {
        abi.antfly_storage_snapshot_capture_destroy(self.handle);
        self.handle = null;
    }
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
    cancellation_ctx: ?*anyopaque,
    cancellation_fn: ?abi.CancellationCheckFn,
) abi.ControlledJsonOperationRequest {
    return .{
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .execution_deadline_ns = execution_deadline_ns orelse 0,
        .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
        .cancellation_ctx = cancellation_ctx,
        .cancellation_fn = cancellation_fn,
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
