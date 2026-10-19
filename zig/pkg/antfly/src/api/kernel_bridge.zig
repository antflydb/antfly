// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Internal compiled boundary for the public API/storage server. Production
//! runtime units own only opaque handles; test builds retain the direct server
//! type for focused coverage.

const builtin = @import("builtin");
const std = @import("std");
const runtime_http_bridge = @import("antfly_runtime_abi").http_bridge;
const abi = @import("kernel_abi.zig");
const server_mod = @import("http_server.zig");
const handler_mod = @import("httpx_handler.zig");
const table_reads = @import("antfly_local_sources").api_table_read_source;
const table_writes = @import("antfly_local_sources").api_table_write_source;
const restore_jobs = @import("restore_jobs.zig");
const managed_embedder = @import("antfly_local_sources").inference_managed_embedder;
const backend_erased = @import("antfly_local_sources").storage_backend_erased;
const hot_standby_http_operation = @import("../storage/hot_standby/http_operation.zig");
const httpx = @import("httpx");
const internal_routes = @import("../internal/routes.zig");

const CreateContext = abi.CreateContext;
const CallContext = abi.CallContext;
const HandlerCreateContext = abi.HandlerCreateContext;
// Ordinary unit tests retain the native implementation. A dedicated linked
// consumer opts into the same external table symbol used by production.
const direct_codegen = builtin.is_test and !(if (@hasDecl(@import("antfly_source_root"), "antfly_test_use_compiled_api_kernel")) @import("antfly_source_root").antfly_test_use_compiled_api_kernel else false);

const BoundaryAllocator = struct {
    allocator: std.mem.Allocator,
    abi_allocator: abi.memory_abi.Allocator,
};

extern fn antfly_api_kernel_get_function_table() callconv(.c) *const abi.FunctionTable;

/// Project the original lifetime into the executor clock. Capture the target
/// clock first so conversion cannot add observation time to the remaining
/// budget. Identical clock authorities keep the exact original timestamp.
fn httpDeadline(context: *const httpx.Context) abi.HttpDeadline {
    const deadline = context.application_deadline_ns orelse return .{};
    if (context.application_deadline_io) |origin| {
        if (origin.userdata == context.io.userdata and origin.vtable == context.io.vtable)
            return .{ .present = 1, .timestamp_ns = deadline };
    }
    const target_now = @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, context.io).nanoseconds)));
    const origin_now = if (context.application_deadline_io) |origin|
        @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, origin).nanoseconds)))
    else
        @import("antfly_platform").time.monotonicNs();
    return .{ .present = 1, .timestamp_ns = target_now +| (deadline -| origin_now) };
}

fn callError(status: abi.Status) !void {
    if (status.isOk()) return;
    return abi.errorFromStatus(status);
}

fn validateFunctionTable() !*const abi.FunctionTable {
    const table = if (direct_codegen) @import("kernel_exports.zig").getFunctionTable() else antfly_api_kernel_get_function_table();
    if (!abi.validFunctionTable(
        table,
        abi.Capability.core | abi.Capability.inference_admission_stats,
    ))
        return error.UnsupportedVersion;
    return table;
}

pub const ApiHttpServer = if (direct_codegen) server_mod.ApiHttpServer else OpaqueApiHttpServer;
pub const HttpxHandler = if (direct_codegen) handler_mod.AntflyApiHandler else OpaqueHttpxHandler;

/// Node lifetime owns this handle independently of each borrowing API. Closing
/// precedes consumer joins; destruction follows all API/inference/background joins.
pub const NodeRunnableOwner = struct {
    handle: *anyopaque,
    functions: *const abi.FunctionTable,
    boundary_allocator: *BoundaryAllocator,

    pub fn init(alloc: std.mem.Allocator, policy: server_mod.RunnableScheduler.Config) !NodeRunnableOwner {
        const functions = if (direct_codegen) @import("kernel_exports.zig").getFunctionTable() else try validateFunctionTable();
        if (!abi.validFunctionTable(functions, abi.Capability.node_runnable_owner)) return error.UnsupportedVersion;
        const boundary_allocator = try alloc.create(BoundaryAllocator);
        errdefer alloc.destroy(boundary_allocator);
        boundary_allocator.allocator = alloc;
        boundary_allocator.abi_allocator = .fromStd(&boundary_allocator.allocator);
        var handle: ?*anyopaque = null;
        try callError(functions.runnable_owner_create(&.{
            .abi_version = abi.abi_version,
            .owner_alloc = &boundary_allocator.abi_allocator,
            .policy = &policy,
            .policy_contract = .of(server_mod.RunnableScheduler.Config),
            .out_handle = &handle,
        }));
        return .{ .handle = handle orelse return error.ApiKernelOperationFailed, .functions = functions, .boundary_allocator = boundary_allocator };
    }

    /// Borrowed port: consumers retain before storing it and release after joins.
    pub fn executionPort(self: *const NodeRunnableOwner) !abi.runnable_abi.Owner {
        if (!abi.validFunctionTable(self.functions, abi.Capability.node_runnable_execution)) return error.UnsupportedVersion;
        var port: abi.runnable_abi.Owner = undefined;
        try callError(self.functions.runnable_owner_execution_port(&.{
            .abi_version = abi.abi_version,
            .handle = self.handle,
            .output = &port,
            .output_contract = .of(abi.runnable_abi.Owner),
        }));
        if (!port.valid()) return error.UnsupportedVersion;
        return port;
    }

    pub fn close(self: *const NodeRunnableOwner) !void {
        try callError(self.functions.runnable_owner_close(&.{ .abi_version = abi.abi_version, .handle = self.handle }));
    }

    pub fn deinit(self: *NodeRunnableOwner) !void {
        try callError(self.functions.runnable_owner_destroy(&.{ .abi_version = abi.abi_version, .handle = self.handle }));
        const alloc = self.boundary_allocator.allocator;
        alloc.destroy(self.boundary_allocator);
        self.* = undefined;
    }
};

test "runnable scheduler opaque node owner retains its allocator after refused destruction" {
    var owner = try NodeRunnableOwner.init(std.testing.allocator, .{ .slots = 2 });
    const port = try owner.executionPort();
    try std.testing.expectEqual(owner.handle, port.context);
    try std.testing.expect(port.retain(port.context).isOk());
    try std.testing.expectError(error.InvalidRunnableTransition, owner.deinit());
    port.release(port.context);
    // Exercise the second retain through the opaque capability too: this
    // test must remain valid when its producer is a separate runtime image.
    try std.testing.expect(port.retain(port.context).isOk());
    try std.testing.expectError(error.InvalidRunnableTransition, owner.deinit());
    port.release(port.context);
    try owner.close();
    try owner.deinit();
}

const OpaqueApiHttpServer = struct {
    opaque_handle: *anyopaque,
    functions: *const abi.FunctionTable,
    boundary_allocator: *BoundaryAllocator,
    alloc: std.mem.Allocator,
    cfg: server_mod.ApiHttpServerConfig,

    pub const RequestStats = server_mod.ApiHttpServer.RequestStats;
    pub const AdmissionStats = server_mod.RequestAdmission.Stats;

    pub fn initWithConfig(
        owner_alloc: std.mem.Allocator,
        cfg: server_mod.ApiHttpServerConfig,
        source: server_mod.StatusSource,
        read_source: ?table_reads.TableReadSource,
        write_source: ?table_writes.TableWriteSource,
    ) !OpaqueApiHttpServer {
        return createOpaqueServer(owner_alloc, cfg, source, read_source, write_source, true);
    }

    pub fn initWithProcessRequestAllocator(
        owner_alloc: std.mem.Allocator,
        cfg: server_mod.ApiHttpServerConfig,
        source: server_mod.StatusSource,
        read_source: ?table_reads.TableReadSource,
        write_source: ?table_writes.TableWriteSource,
    ) OpaqueApiHttpServer {
        return createOpaqueServer(owner_alloc, cfg, source, read_source, write_source, false) catch
            @panic("API kernel allocation failed");
    }

    pub fn initWithProcessRequestAllocatorFallible(
        owner_alloc: std.mem.Allocator,
        cfg: server_mod.ApiHttpServerConfig,
        source: server_mod.StatusSource,
        read_source: ?table_reads.TableReadSource,
        write_source: ?table_writes.TableWriteSource,
    ) !OpaqueApiHttpServer {
        return createOpaqueServer(owner_alloc, cfg, source, read_source, write_source, false);
    }

    pub fn deinit(self: *OpaqueApiHttpServer) void {
        const boundary_allocator = self.boundary_allocator;
        const allocator = boundary_allocator.allocator;
        self.functions.destroy(self.opaque_handle);
        allocator.destroy(boundary_allocator);
        self.* = undefined;
    }

    pub fn requestStats(self: *OpaqueApiHttpServer) RequestStats {
        var out: RequestStats = undefined;
        callInfallible(void, RequestStats, self.functions.request_stats, self.opaque_handle, null, &out);
        return out;
    }

    pub fn configurePublicHttpTransport(self: *const OpaqueApiHttpServer, config: httpx.ServerConfig) httpx.ServerConfig {
        var out: httpx.ServerConfig = undefined;
        var value = config;
        callInfallible(httpx.ServerConfig, httpx.ServerConfig, self.functions.configure_http_transport, self.opaque_handle, &value, &out);
        return out;
    }

    pub fn queryAdmissionStats(self: *const OpaqueApiHttpServer) AdmissionStats {
        var out: AdmissionStats = undefined;
        callInfallible(void, AdmissionStats, self.functions.query_admission_stats, self.opaque_handle, null, &out);
        return out;
    }

    pub fn runnableSchedulingStats(self: *const OpaqueApiHttpServer) abi.RunnableStats {
        return self.runnableSchedulingStatsChecked() catch @panic("invalid API scheduler telemetry");
    }

    pub fn runnableSchedulingStatsChecked(self: *const OpaqueApiHttpServer) !abi.RunnableStats {
        if (!abi.validFunctionTable(self.functions, abi.Capability.core)) return error.UnsupportedVersion;
        var out: abi.RunnableStats = undefined;
        try callFallible(void, abi.RunnableStats, self.functions.runnable_scheduling_stats, self.opaque_handle, null, &out);
        try out.service.validate();
        return out;
    }

    pub fn writeAdmissionStats(self: *const OpaqueApiHttpServer) AdmissionStats {
        var out: AdmissionStats = undefined;
        callInfallible(void, AdmissionStats, self.functions.write_admission_stats, self.opaque_handle, null, &out);
        return out;
    }

    pub fn inferenceAdmissionStats(self: *const OpaqueApiHttpServer) AdmissionStats {
        var out: AdmissionStats = undefined;
        callInfallible(void, AdmissionStats, self.functions.inference_admission_stats, self.opaque_handle, null, &out);
        return out;
    }

    pub fn setAntflyProvider(self: *OpaqueApiHttpServer, provider: ?managed_embedder.AntflyProvider) void {
        var input = provider;
        callInfallible(?managed_embedder.AntflyProvider, void, self.functions.set_provider, self.opaque_handle, &input, null);
    }

    pub fn setHotStandbyInternalExecutor(self: *OpaqueApiHttpServer, executor_value: ?hot_standby_http_operation.Executor) void {
        var input = executor_value;
        callInfallible(?hot_standby_http_operation.Executor, void, self.functions.set_ha_executor, self.opaque_handle, &input, null);
    }

    pub fn attachRestoreJobRuntimeStore(self: *OpaqueApiHttpServer, store: *backend_erased.Store) !void {
        try callFallible(backend_erased.Store, void, self.functions.attach_runtime_restore_store, self.opaque_handle, store, null);
    }

    pub fn attachReplicatedRestoreJobStore(self: *OpaqueApiHttpServer, persistence: restore_jobs.ReplicatedPersistence) !void {
        var input = persistence;
        try callError(self.functions.attach_replicated_restore_store(abi.abi_version, self.opaque_handle, &input));
    }

    pub fn bootstrapRemoteAttempts(self: *OpaqueApiHttpServer, config: @import("remote_attempt_owner.zig").Bootstrap) !void {
        if (!abi.validFunctionTable(self.functions, abi.Capability.remote_attempt_owner)) return error.UnsupportedVersion;
        var value = config;
        try callFallible(@import("remote_attempt_owner.zig").Bootstrap, void, self.functions.bootstrap_remote_attempts, self.opaque_handle, &value, null);
    }
    pub fn refreshRemoteAttemptPeers(self: *OpaqueApiHttpServer) !void {
        if (!abi.validFunctionTable(self.functions, abi.Capability.remote_peer_refresh)) return error.UnsupportedVersion;
        try callFallible(void, void, self.functions.refresh_remote_attempt_peers, self.opaque_handle, null, null);
    }
    /// The API kernel owns its listener. Inspect only checked scalar output,
    /// including when port zero requested an ephemeral bind during startup.
    pub fn pgwireBoundPort(self: *OpaqueApiHttpServer) !?u16 {
        if (!abi.validFunctionTable(self.functions, abi.Capability.pgwire_bound_port)) return error.UnsupportedVersion;
        const function = self.functions.pgwire_bound_port orelse return error.UnsupportedVersion;
        var port: u16 = 0;
        try callFallible(void, u16, function, self.opaque_handle, null, &port);
        return if (port == 0) null else port;
    }

    pub fn remoteAttemptOwnerStats(self: *OpaqueApiHttpServer) !@import("remote_attempt_owner.zig").Stats {
        if (!abi.validFunctionTable(self.functions, abi.Capability.remote_attempt_owner)) return error.UnsupportedVersion;
        var value: @import("remote_attempt_owner.zig").Stats = undefined;
        try callFallible(void, @import("remote_attempt_owner.zig").Stats, self.functions.remote_attempt_owner_stats, self.opaque_handle, null, &value);
        return value;
    }

    pub fn resumeRestoreJobsOnce(self: *OpaqueApiHttpServer) !void {
        try callFallible(void, void, self.functions.resume_restore_jobs, self.opaque_handle, null, null);
    }

    pub fn pollRestoreJobsOnce(self: *OpaqueApiHttpServer) !void {
        try callFallible(void, void, self.functions.poll_restore_jobs, self.opaque_handle, null, null);
    }

    pub fn prepareRestoreLeadership(self: *OpaqueApiHttpServer, term: u64) !void {
        var input = term;
        try callFallible(u64, void, self.functions.prepare_restore_leadership, self.opaque_handle, &input, null);
    }

    pub fn scheduleSessionMaintenance(self: *OpaqueApiHttpServer) !void {
        try callFallible(void, void, self.functions.schedule_session_maintenance, self.opaque_handle, null, null);
    }

    pub fn storageMaintenanceExclusiveActive(self: *const OpaqueApiHttpServer) bool {
        var out = false;
        callInfallible(void, bool, self.functions.storage_maintenance_active, self.opaque_handle, null, &out);
        return out;
    }

    /// Opaque servers bind the source from inside `create`, after the heap-owned
    /// kernel server reaches its stable address. Keep the direct and opaque
    /// call sites uniform without exposing a kernel-owned cache pointer.
    pub fn bindIncomingGraphRoutes(self: *OpaqueApiHttpServer, source: table_reads.TableReadSource) void {
        _ = self;
        _ = source;
    }

    pub fn checkReady(self: *OpaqueApiHttpServer) !void {
        try callFallible(void, void, self.functions.check_ready, self.opaque_handle, null, null);
    }

    pub fn authorizeInferenceRequest(
        self: *OpaqueApiHttpServer,
        request: server_mod.AuthenticatedRequest,
        permission: abi.InferencePermission,
    ) !abi.AuthorizationDecision {
        var decision: abi.AuthorizationDecision = undefined;
        try callError(self.functions.authorize_inference(&.{
            .abi_version = abi.abi_version,
            .handle = self.opaque_handle,
            .authorization = abi.OptionalBytes.init(request.authorization),
            .trusted_principal = abi.OptionalBytes.init(request.trusted_principal),
            .permission = permission,
            .out_decision = &decision,
        }));
        return decision;
    }
};

fn createOpaqueServer(
    owner_alloc: std.mem.Allocator,
    cfg: server_mod.ApiHttpServerConfig,
    source: server_mod.StatusSource,
    read_source: ?table_reads.TableReadSource,
    write_source: ?table_writes.TableWriteSource,
    fallible: bool,
) !OpaqueApiHttpServer {
    const functions = try validateFunctionTable();
    var cfg_copy = cfg;
    const api_io = if (cfg.imported_runtime_io) |views| views.api else if (cfg.backend_runtime) |runtime| runtime.apiIo() else null;
    const api_network_io = if (cfg.imported_runtime_io) |views| views.api_network else if (cfg.backend_runtime) |runtime| runtime.apiNetworkIo() else null;
    const api_filesystem_io = if (cfg.imported_runtime_io) |views| views.api_filesystem else if (cfg.backend_runtime) |runtime| runtime.apiFilesystemIo() else null;
    const durable_io = if (cfg.imported_runtime_io) |views| views.durable else if (cfg.backend_runtime) |runtime| runtime.io() else null;
    const api_borrow: ?abi.native_abi.IoBorrow = if (api_io) |io| .init(&io) else null;
    const api_network_borrow: ?abi.native_abi.IoBorrow = if (api_network_io) |io| .init(&io) else null;
    const api_filesystem_borrow: ?abi.native_abi.IoBorrow = if (api_filesystem_io) |io| .init(&io) else null;
    const durable_borrow: ?abi.native_abi.IoBorrow = if (durable_io) |io| .init(&io) else null;
    const sql_execution_io = if (cfg.imported_runtime_io) |views| views.sql_execution else if (cfg.backend_runtime) |runtime| runtime.io() else null;
    const sql_control_io = if (cfg.imported_runtime_io) |views| views.sql_control else if (cfg.backend_runtime) |runtime| runtime.controlIo() else null;
    const sql_execution_borrow: ?abi.native_abi.IoBorrow = if (sql_execution_io) |io| .init(&io) else null;
    const sql_control_borrow: ?abi.native_abi.IoBorrow = if (sql_control_io) |io| .init(&io) else null;
    const sql_worker_factory = if (cfg.imported_runtime_io) |views| views.sql_workers else if (cfg.backend_runtime) |runtime| @import("sql_worker_lease.zig").factory(runtime) else null;
    const runtime_io: abi.RuntimeIoBorrows = .{
        .api = if (api_borrow) |*borrow| borrow else null,
        .api_network = if (api_network_borrow) |*borrow| borrow else null,
        .api_filesystem = if (api_filesystem_borrow) |*borrow| borrow else null,
        .durable = if (durable_borrow) |*borrow| borrow else null,
        .sql_execution = if (sql_execution_borrow) |*borrow| borrow else null,
        .sql_control = if (sql_control_borrow) |*borrow| borrow else null,
        .sql_workers = if (sql_worker_factory) |*producer| producer else null,
    };
    var source_copy = source;
    var reads_copy = read_source;
    var writes_copy = write_source;
    var handle: ?*anyopaque = null;
    var request_alloc_abi: ?*const abi.memory_abi.Allocator = null;
    const boundary_allocator = try owner_alloc.create(BoundaryAllocator);
    errdefer owner_alloc.destroy(boundary_allocator);
    boundary_allocator.allocator = owner_alloc;
    boundary_allocator.abi_allocator = .fromStd(&boundary_allocator.allocator);
    const status = functions.create(&.{
        .abi_version = abi.abi_version,
        .owner_alloc = &boundary_allocator.abi_allocator,
        .cfg = &cfg_copy,
        .cfg_contract = .of(server_mod.ApiHttpServerConfig),
        .runtime_io = if (cfg.backend_runtime != null or cfg.imported_runtime_io != null) &runtime_io else null,
        .source = &source_copy,
        .source_contract = .of(server_mod.StatusSource),
        .table_reads = &reads_copy,
        .table_reads_contract = .of(?table_reads.TableReadSource),
        .table_writes = &writes_copy,
        .table_writes_contract = .of(?table_writes.TableWriteSource),
        .flags = if (fallible) CreateContext.fallible_init else 0,
        .out_handle = &handle,
        .out_request_alloc = &request_alloc_abi,
    });
    try callError(status);
    const owned_handle = handle orelse return error.ApiKernelOperationFailed;
    const owned_request_alloc = request_alloc_abi orelse {
        functions.destroy(owned_handle);
        return error.ApiKernelOperationFailed;
    };
    return .{
        .opaque_handle = owned_handle,
        .functions = functions,
        .boundary_allocator = boundary_allocator,
        .alloc = owned_request_alloc.asStd(),
        .cfg = cfg,
    };
}

fn callFallible(
    comptime Input: type,
    comptime Output: type,
    function: *const fn (*const CallContext) callconv(.c) abi.Status,
    handle: *anyopaque,
    input: ?*const Input,
    output: ?*Output,
) !void {
    try callError(function(&.{
        .abi_version = abi.abi_version,
        .handle = handle,
        .input = input,
        .input_contract = .of(Input),
        .output = output,
        .output_contract = .of(Output),
    }));
}

fn callInfallible(
    comptime Input: type,
    comptime Output: type,
    function: *const fn (*const CallContext) callconv(.c) abi.Status,
    handle: *anyopaque,
    input: ?*const Input,
    output: ?*Output,
) void {
    callFallible(Input, Output, function, handle, input, output) catch @panic("infallible API kernel call failed");
}

pub const HandlerStats = abi.HandlerStats;

const OpaqueHttpxHandler = struct {
    const RouteSelection = enum {
        all,
        all_without_probes,
        generated_with_probes,
    };

    handle: *anyopaque,
    functions: *const abi.FunctionTable,
    alloc: ?std.mem.Allocator = null,
    runtime_routes: std.ArrayListUnmanaged(*RuntimeRoute) = .empty,

    pub fn initRuntime(self: *OpaqueHttpxHandler, alloc: std.mem.Allocator) !void {
        try callFallible(void, void, self.functions.handler_init, self.handle, null, null);
        self.alloc = alloc;
    }

    pub fn stats(self: *const OpaqueHttpxHandler) HandlerStats {
        var out: HandlerStats = undefined;
        callInfallible(void, HandlerStats, self.functions.handler_stats, self.handle, null, &out);
        return out;
    }

    pub fn registerRoutes(self: *OpaqueHttpxHandler, server: *httpx.Server) !void {
        return self.registerRoutesWithOptions(server, .all);
    }

    pub fn registerRoutesWithoutProbes(self: *OpaqueHttpxHandler, server: *httpx.Server) !void {
        return self.registerRoutesWithOptions(server, .all_without_probes);
    }

    pub fn registerGeneratedRoutesWithProbes(self: *OpaqueHttpxHandler, server: *httpx.Server) !void {
        return self.registerRoutesWithOptions(server, .generated_with_probes);
    }

    fn installHostInternalServiceAuth(self: *OpaqueHttpxHandler, server: *httpx.Server) !void {
        if (!abi.validFunctionTable(self.functions, abi.Capability.internal_service_ingress))
            return error.UnsupportedVersion;
        try server.use(httpx.Middleware.bind(
            "antfly-host-internal-service-auth",
            self,
            enforceHostInternalServiceAuth,
        ));
    }

    fn enforceHostInternalServiceAuth(
        self: *OpaqueHttpxHandler,
        context: *httpx.Context,
        next: *httpx.Next,
    ) !httpx.Response {
        if (!requiresHostInternalServicePrincipal(context.request.uri.path)) return next.call(context);

        const source_headers = context.request.headers.iterator();
        const headers = try context.allocator.alloc(abi.HeaderView, source_headers.len);
        defer context.allocator.free(headers);
        for (source_headers, 0..) |header, i| {
            headers[i] = .{ .name = abi.Bytes.init(header.name), .value = abi.Bytes.init(header.value) };
        }
        const request_view: abi.HttpRequestView = .{
            // Internal-service authorization is method independent. Preserve
            // supported methods and use GET as a transport placeholder for an
            // unsupported method so authentication still precedes the 405.
            .method = switch (context.request.method) {
                .GET => .get,
                .POST => .post,
                .PUT => .put,
                .DELETE => .delete,
                else => .get,
            },
            .path = abi.Bytes.init(context.request.uri.path),
            .query = abi.OptionalBytes.init(context.request.uri.query),
            .headers_ptr = if (headers.len == 0) null else headers.ptr,
            .headers_len = headers.len,
        };
        var response_handle: ?*anyopaque = null;
        var response_view: abi.HttpResponseView = undefined;
        var legacy_accepted: u8 = 0;
        const borrowed_io = context.io;
        try callError(self.functions.handler_authorize_internal_service(&.{
            .abi_version = abi.abi_version,
            .handler_handle = self.handle,
            .request = &request_view,
            .executor = .init(&borrowed_io),
            .deadline = httpDeadline(context),
            .out_response_handle = &response_handle,
            .out_response = &response_view,
            .out_legacy_accepted = &legacy_accepted,
        }));
        if (response_handle) |owned_handle|
            return copyKernelResponse(context, self.functions, owned_handle, response_view);

        var response = try next.call(context);
        errdefer response.deinit();
        if (legacy_accepted != 0)
            try response.headers.set("X-Antfly-Internal-Auth", "legacy-migration");
        return response;
    }

    fn registerRoutesWithOptions(self: *OpaqueHttpxHandler, server: *httpx.Server, selection: RouteSelection) !void {
        if (!abi.validFunctionTable(self.functions, abi.Capability.route_manifest)) return error.UnsupportedVersion;
        if (self.runtime_routes.items.len != 0) return error.RoutesAlreadyRegistered;
        const alloc = self.alloc orelse return error.ApiKernelNotInitialized;
        var entries_ptr: ?[*]const abi.RouteManifestEntry = null;
        var entries_len: usize = 0;
        try callError(self.functions.handler_route_manifest(&.{
            .abi_version = abi.abi_version,
            .handler_handle = self.handle,
            .out_entries = &entries_ptr,
            .out_len = &entries_len,
        }));
        const entries = if (entries_ptr) |ptr| ptr[0..entries_len] else &.{};
        for (entries) |entry| {
            const path = entry.path.slice();
            const is_probe = std.mem.eql(u8, path, "/healthz") or std.mem.eql(u8, path, "/readyz");
            const is_generated = std.mem.eql(u8, path, "/db/v1") or
                std.mem.startsWith(u8, path, "/db/v1/") or
                std.mem.eql(u8, path, "/auth/v1") or
                std.mem.startsWith(u8, path, "/auth/v1/");
            switch (selection) {
                .all => {},
                .all_without_probes => if (is_probe) continue,
                .generated_with_probes => if (!is_generated and !is_probe) continue,
            }
            const route = try alloc.create(RuntimeRoute);
            errdefer alloc.destroy(route);
            route.* = .{
                .functions = self.functions,
                .kernel_route_handle = entry.route_handle,
                .request_body = entry.request_body,
                .streaming_response = entry.streaming_response != 0,
            };
            try self.runtime_routes.append(alloc, route);
            errdefer _ = self.runtime_routes.pop();
            const method: httpx.Method = switch (entry.method) {
                .get => .GET,
                .post => .POST,
                .put => .PUT,
                .delete => .DELETE,
                .patch => .PATCH,
            };
            if (method == .POST) {
                if (@import("http_routes.zig").postBodyLimit(path)) |limit| {
                    try server.routeWithDataAndBodyLimit(method, path, runtimeApiHttpHandler, route, limit);
                    continue;
                }
            }
            try server.routeWithData(method, path, runtimeApiHttpHandler, route);
        }
    }

    pub fn deinit(self: *OpaqueHttpxHandler) void {
        if (self.alloc) |alloc| {
            for (self.runtime_routes.items) |route| alloc.destroy(route);
            self.runtime_routes.deinit(alloc);
        }
        self.functions.handler_destroy(self.handle);
        self.* = undefined;
    }
};

fn requiresHostInternalServicePrincipal(path: []const u8) bool {
    const in_internal_namespace = std.mem.eql(u8, path, internal_routes.base) or
        std.mem.startsWith(u8, path, internal_routes.base ++ "/");
    const hot_standby_exempt = std.mem.eql(u8, path, internal_routes.standby) or
        std.mem.startsWith(u8, path, internal_routes.standby ++ "/") or
        std.mem.eql(u8, path, internal_routes.legacy_standby) or
        std.mem.startsWith(u8, path, internal_routes.legacy_standby ++ "/");
    return in_internal_namespace and !hot_standby_exempt;
}

const RuntimeRoute = struct {
    functions: *const abi.FunctionTable,
    kernel_route_handle: *anyopaque,
    request_body: abi.RequestBodyMode,
    streaming_response: bool,
};

fn runtimeApiHttpHandler(context: *httpx.Context) anyerror!httpx.Response {
    const route: *const RuntimeRoute = @ptrCast(@alignCast(context.route_data orelse return error.ApiKernelUnavailable));
    const source_headers = context.request.headers.iterator();
    const headers = try context.allocator.alloc(abi.HeaderView, source_headers.len);
    defer context.allocator.free(headers);
    for (source_headers, 0..) |header, i| {
        headers[i] = .{ .name = abi.Bytes.init(header.name), .value = abi.Bytes.init(header.value) };
    }
    const params = try context.allocator.alloc(abi.RouteParamView, context.params.len);
    defer context.allocator.free(params);
    for (context.params, 0..) |param, i| {
        params[i] = .{ .name = abi.Bytes.init(param.name), .value = abi.Bytes.init(param.value) };
    }

    var response_handle: ?*anyopaque = null;
    var response_view: abi.HttpResponseView = undefined;
    const request_view: abi.HttpRequestView = .{
        .method = switch (context.request.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            .DELETE => .delete,
            .PATCH => .patch,
            else => return error.MethodNotAllowed,
        },
        .path = abi.Bytes.init(context.request.uri.path),
        .query = abi.OptionalBytes.init(context.request.uri.query),
        .headers_ptr = if (headers.len == 0) null else headers.ptr,
        .headers_len = headers.len,
        .params_ptr = if (params.len == 0) null else params.ptr,
        .params_len = params.len,
        .body = abi.OptionalBytes.init(context.request.body),
        .authorization = abi.OptionalBytes.init(context.request.headers.get("Authorization")),
        .content_type = abi.OptionalBytes.init(context.request.headers.get("Content-Type")),
    };
    var transport = runtime_http_bridge.Outbound{ .context = context };
    const borrowed_io = context.io;
    const body_source = if (route.request_body == .buffered) transport.bodySource() else abi.RequestBodySource{};
    try callError(route.functions.handler_handle_http(&.{
        .abi_version = abi.abi_version,
        .route_handle = route.kernel_route_handle,
        .request = &request_view,
        .cancellation = transport.cancellation(),
        .body_source = body_source,
        .stream = if (route.streaming_response) transport.stream() else .{},
        .executor = .init(&borrowed_io),
        .deadline = httpDeadline(context),
        .out_response_handle = &response_handle,
        .out_response = &response_view,
    }));
    return copyKernelResponse(
        context,
        route.functions,
        response_handle orelse return error.RuntimeBoundaryFailure,
        response_view,
    );
}

fn copyKernelResponse(
    context: *httpx.Context,
    functions: *const abi.FunctionTable,
    owned_response_handle: *anyopaque,
    response_view: abi.HttpResponseView,
) !httpx.Response {
    defer functions.handler_destroy_http_response(owned_response_handle);
    var response = httpx.Response.init(context.allocator, response_view.status);
    errdefer response.deinit();
    if (response_view.content_type.slice()) |content_type|
        try response.headers.set("Content-Type", content_type);
    const response_headers = if (response_view.headers_ptr) |ptr| ptr[0..response_view.headers_len] else &.{};
    for (response_headers) |header| {
        if (response_view.content_type.slice() != null and
            std.ascii.eqlIgnoreCase(header.name.slice(), "Content-Type")) continue;
        try response.headers.append(header.name.slice(), header.value.slice());
    }
    const body = try context.allocator.dupe(u8, response_view.body.slice());
    response.body = body;
    response.body_owned = true;
    return response;
}

/// Installs kernel-owned authentication for host-registered `/internal/v1`
/// routes. Test builds use the direct registrar, which already installs the
/// same middleware; production opaque builds cross the ABI explicitly.
pub fn installHostInternalServiceAuth(handler: *HttpxHandler, server: *httpx.Server) !void {
    if (comptime direct_codegen) return;
    try handler.installHostInternalServiceAuth(server);
}

pub fn createHandler(server: *ApiHttpServer) !HttpxHandler {
    if (comptime direct_codegen) return .{ .api_server = server };
    return createOpaqueHandler(server);
}

fn createOpaqueHandler(server: *OpaqueApiHttpServer) !OpaqueHttpxHandler {
    var handle: ?*anyopaque = null;
    const status = server.functions.handler_create(&.{
        .abi_version = abi.abi_version,
        .api_server_handle = server.opaque_handle,
        .out_handle = &handle,
    });
    try callError(status);
    return .{
        .handle = handle orelse return error.ApiKernelOperationFailed,
        .functions = server.functions,
    };
}

pub fn handlerStats(handler: *const HttpxHandler) HandlerStats {
    if (comptime direct_codegen) {
        const query = handler.api_server.queryAdmissionStats();
        const write = handler.api_server.writeAdmissionStats();
        const inference = handler.api_server.inferenceAdmissionStats();
        const query_body = handler.query_body_admission.stats();
        const memory = handler.api_server.workloadMemoryStats();
        return .{
            .query_capacity = query.capacity,
            .query_in_flight = query.in_flight,
            .query_peak_in_flight = query.peak_in_flight,
            .query_rejected_total = query.rejected_total,
            .write_capacity = write.capacity,
            .write_in_flight = write.in_flight,
            .write_peak_in_flight = write.peak_in_flight,
            .write_rejected_total = write.rejected_total,
            .inference_capacity = inference.capacity,
            .inference_in_flight = inference.in_flight,
            .inference_peak_in_flight = inference.peak_in_flight,
            .inference_rejected_total = inference.rejected_total,
            .query_body_capacity = query_body.capacity,
            .query_body_in_flight = query_body.in_flight,
            .query_body_peak_in_flight = query_body.peak_in_flight,
            .query_body_rejected_total = query_body.rejected_total,
            .query_configured_capacity = query.configured_capacity,
            .query_queued = query.queued,
            .query_queued_bytes = query.queued_bytes,
            .query_waited_total = query.waited_total,
            .query_wait_ns_total = query.wait_ns_total,
            .query_wait_completed_total = query.wait_completed_total,
            .query_wait_buckets = query.wait_buckets,
            .query_expired_total = query.expired_total,
            .query_cancelled_total = query.cancelled_total,
            .query_queue_capacity = query.waiting.max_queued_requests,
            .query_queue_bytes = query.waiting.max_queued_bytes,
            .query_wait_ms = query.waiting.max_wait_ms,
            .query_policy_version = query.policy_version,
            .query_policy_retired_total = query.policy_retired_total,
            .query_retiring_waiters = query.retiring_waiters,
            .query_draining = query.draining,
            .query_suspended = query.suspended,
            .query_quiescent = query.quiescent,
            .query_sql_waiting_enabled = query.sql_waiting_enabled orelse false,
            .runnable = handler.api_server.runnableSchedulingStats(),
            .read_memory = handler.api_server.readMemoryStats(),
            .memory = .{ memoryAbi(memory.foreground), memoryAbi(memory.controlWithCleanup()), memoryAbi(memory.retained) },
        };
    }
    return handler.stats();
}

pub fn deinitHandler(handler: *HttpxHandler) void {
    if (comptime direct_codegen) handler.deinitRuntime() else handler.deinit();
}

fn memoryAbi(stats: anytype) abi.WorkloadMemoryStats {
    if (stats) |value| return .{ .enabled = true, .limit_bytes = value.limit_bytes, .used_bytes = value.used_bytes, .peak_bytes = value.peak_bytes, .rejected_total = value.rejected_total };
    return .{ .enabled = false, .limit_bytes = 0, .used_bytes = 0, .peak_bytes = 0, .rejected_total = 0 };
}

pub fn setAntflyProvider(server: *ApiHttpServer, provider: ?managed_embedder.AntflyProvider) void {
    if (comptime direct_codegen)
        server.antfly_provider = provider
    else
        server.setAntflyProvider(provider);
}

test "opaque host middleware protects direct internal routes across the kernel ABI" {
    const FakeKernel = struct {
        var calls: usize = 0;
        var reject = true;
        var legacy = false;

        fn authorize(context: *const abi.InternalServiceAuthContext) callconv(.c) abi.Status {
            calls += 1;
            if (!std.mem.eql(u8, context.request.path.slice(), "/internal/v1/tables/docs"))
                return abi.statusFromError(error.TestUnexpectedResult);
            context.out_legacy_accepted.* = @intFromBool(legacy);
            if (reject) {
                context.out_response_handle.* = @ptrFromInt(1);
                context.out_response.* = .{
                    .status = 401,
                    .body = abi.Bytes.init("unauthorized"),
                };
            } else {
                context.out_response_handle.* = null;
            }
            return .ok;
        }

        fn destroy(_: *anyopaque) callconv(.c) void {}

        fn next(_: *httpx.Next, ctx: *httpx.Context) anyerror!httpx.Response {
            return ctx.status(204).text("");
        }
    };

    var functions: abi.FunctionTable = undefined;
    functions.handler_authorize_internal_service = FakeKernel.authorize;
    functions.handler_destroy_http_response = FakeKernel.destroy;
    var handler = OpaqueHttpxHandler{
        .handle = @ptrFromInt(1),
        .functions = &functions,
    };
    var next_handler = httpx.Next{ ._call = FakeKernel.next };

    var internal_request = try httpx.Request.init(std.testing.allocator, .POST, "/internal/v1/tables/docs");
    defer internal_request.deinit();
    var internal_context = httpx.Context.init(std.testing.allocator, std.testing.io, &internal_request);
    defer internal_context.deinit();

    FakeKernel.calls = 0;
    FakeKernel.reject = true;
    FakeKernel.legacy = false;
    var rejected = try handler.enforceHostInternalServiceAuth(&internal_context, &next_handler);
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 401), rejected.status.code);
    try std.testing.expectEqual(@as(usize, 1), FakeKernel.calls);

    FakeKernel.reject = false;
    FakeKernel.legacy = true;
    var admitted = try handler.enforceHostInternalServiceAuth(&internal_context, &next_handler);
    defer admitted.deinit();
    try std.testing.expectEqual(@as(u16, 204), admitted.status.code);
    try std.testing.expectEqualStrings("legacy-migration", admitted.headers.get("X-Antfly-Internal-Auth").?);

    var public_request = try httpx.Request.init(std.testing.allocator, .GET, "/db/v1/tables");
    defer public_request.deinit();
    var public_context = httpx.Context.init(std.testing.allocator, std.testing.io, &public_request);
    defer public_context.deinit();
    var public_response = try handler.enforceHostInternalServiceAuth(&public_context, &next_handler);
    defer public_response.deinit();
    try std.testing.expectEqual(@as(u16, 204), public_response.status.code);
    try std.testing.expectEqual(@as(usize, 2), FakeKernel.calls);
}

test "linked transport projects the universal request cancellation callback" {
    const FakeKernel = struct {
        var saw_cancellation = false;

        fn handle(context: *const abi.HttpHandleContext) callconv(.c) abi.Status {
            saw_cancellation = context.cancellation.requested();
            context.out_response_handle.* = @ptrFromInt(1);
            context.out_response.* = .{
                .status = 200,
                .body = abi.Bytes.init("ok"),
            };
            return .ok;
        }

        fn destroy(_: *anyopaque) callconv(.c) void {}
    };

    var functions: abi.FunctionTable = undefined;
    functions.handler_handle_http = FakeKernel.handle;
    functions.handler_destroy_http_response = FakeKernel.destroy;
    var route = RuntimeRoute{
        .functions = &functions,
        .kernel_route_handle = @ptrFromInt(1),
        .request_body = .none,
        .streaming_response = false,
    };
    var request = try httpx.Request.init(std.testing.allocator, .GET, "http://127.0.0.1/healthz");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var cancellation = std.atomic.Value(bool).init(true);
    context.cancellation = &cancellation;
    context.h1_has_buffered_input = true;
    context.route_data = &route;

    var response = try runtimeApiHttpHandler(&context);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    try std.testing.expectEqualStrings("ok", response.body.?);
    try std.testing.expect(FakeKernel.saw_cancellation);
}

test "linked transport admits a streaming body before the kernel pulls it" {
    const BodyState = struct {
        calls: usize = 0,

        fn readAll(raw: ?*anyopaque) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.TestUnexpectedResult));
            self.calls += 1;
            return "deferred body";
        }
    };
    const FakeKernel = struct {
        var observed_state: ?*BodyState = null;
        var body_calls_before_dispatch: usize = 0;
        var saw_streaming_source = false;
        var saw_body = false;

        fn handle(context: *const abi.HttpHandleContext) callconv(.c) abi.Status {
            body_calls_before_dispatch = observed_state.?.calls;
            saw_streaming_source = context.body_source.streaming != 0;
            var body: abi.OptionalBytes = .{};
            const callback = context.body_source.read_all orelse return abi.statusFromError(error.InvalidArgument);
            if (callback(context.body_source.context, &body) != .ok) return abi.statusFromError(error.BodyReadFailed);
            saw_body = if (body.slice()) |bytes| std.mem.eql(u8, bytes, "deferred body") else false;
            context.out_response_handle.* = @ptrFromInt(1);
            context.out_response.* = .{ .status = 200, .body = abi.Bytes.init("ok") };
            return .ok;
        }

        fn destroy(_: *anyopaque) callconv(.c) void {}
    };

    var functions: abi.FunctionTable = undefined;
    functions.handler_handle_http = FakeKernel.handle;
    functions.handler_destroy_http_response = FakeKernel.destroy;
    var route = RuntimeRoute{
        .functions = &functions,
        .kernel_route_handle = @ptrFromInt(1),
        .request_body = .buffered,
        .streaming_response = false,
    };
    var request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/query");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var body_state = BodyState{};
    FakeKernel.observed_state = &body_state;
    defer FakeKernel.observed_state = null;
    context.body_delegate = .{
        .ptr = &body_state,
        .read_all = BodyState.readAll,
        .streaming = true,
    };
    context.route_data = &route;

    var response = try runtimeApiHttpHandler(&context);
    defer response.deinit();
    try std.testing.expectEqual(@as(usize, 0), FakeKernel.body_calls_before_dispatch);
    try std.testing.expect(FakeKernel.saw_streaming_source);
    try std.testing.expect(FakeKernel.saw_body);
    try std.testing.expectEqual(@as(usize, 1), body_state.calls);
}

test "API kernel create host bridge supplies SQL worker capabilities and retires startup reservation" {
    const background = @import("antfly_local_sources").storage_background_runtime;
    var runtime = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 32 } });
    defer runtime.deinit();
    var token: u8 = 0;
    const Stub = struct {
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.UnsupportedOperation;
        }
    };
    const source: server_mod.StatusSource = .{ .ptr = &token, .vtable = &.{ .status = Stub.status } };
    var server = try OpaqueApiHttpServer.initWithConfig(std.testing.allocator, .{ .backend_runtime = runtime.ptr(), .runnable_policy = .{ .slots = 2, .max_operations = 8 } }, source, null, null);
    var live = true;
    defer if (live) server.deinit();
    try std.testing.expectEqual(@as(?u16, null), try server.pgwireBoundPort());
    // Force the real opaque producer/receiver setup even in test builds, where
    // the ordinary ApiHttpServer alias otherwise chooses direct codegen.
    try std.testing.expectEqual(@as(usize, 5), runtime.ptr().laneStats().reserved_workers);
    server.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
}

test "remote attempt owner bootstrap crosses opaque API and never recreates missing state" {
    const alloc = std.testing.allocator;
    const owner_mod = @import("remote_attempt_owner.zig");
    const metadata = @import("../metadata/api.zig");
    const background = @import("antfly_local_sources").storage_background_runtime;
    const signing_mod = @import("antfly_local_sources").storage_db_root_signing_identity;
    const realm = "0123456789abcdef0123456789abcdef".*;
    const Source = struct {
        nodes: [1]@import("../metadata/table_manager.zig").NodeRecord = .{.{ .node_id = 9 }},
        stores: [1]@import("../metadata/table_manager.zig").StoreRecord,
        captures: usize = 0,
        frees: usize = 0,
        mutex: std.atomic.Mutex = .unlocked,
        entry: ?@import("../metadata/remote_attempt_initialization.zig").Entry = null,
        decoder_ready: bool = false,
        block_activation: bool = false,
        lose_activation_response: bool = false,
        activations: usize = 0,
        fn lock(self: *@This()) void {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        }
        fn refresh(server: anytype) !void {
            const until = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
            while (true) {
                server.refreshRemoteAttemptPeers() catch |err| {
                    // The live opaque consumer shares protected C1. Production
                    // refresh is fail-fast; retry only bounded capacity here.
                    if (err != error.SchedulerFull and err != error.SqlWriteCapacityUnavailable and err != error.Unavailable) return err;
                    if (@import("antfly_platform").time.monotonicNs() >= until) return err;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                    continue;
                };
                return;
            }
        }
        fn setProfile(self: *@This(), store_id: u64, reporter: u64, uri: []const u8) void {
            self.lock();
            defer self.mutex.unlock();
            self.stores[0].api_url = uri;
            self.stores[0].store_id = store_id;
            self.stores[0].reporter_incarnation = reporter;
        }
        fn catalog(raw: *anyopaque, allocator: std.mem.Allocator, request: @import("antfly_local_sources").api_operation.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try request.ensureActive();
            try std.testing.expect(!request.setting_admin);
            if (input != .remote_attempt_initialization) return error.TestUnexpectedResult;
            const wire = @import("../metadata/remote_attempt_initialization.zig");
            const command = input.remote_attempt_initialization;
            try command.validate();
            const registered = self.stores[0];
            if (!std.mem.eql(u8, &command.identity.metadata_incarnation, &realm) or
                command.identity.node_id != registered.node_id or command.identity.store_id != registered.store_id or
                command.identity.root_incarnation != registered.replica_root_incarnation or
                command.reporter_incarnation != registered.reporter_incarnation or
                !std.mem.eql(u8, &command.identity.public_key, &registered.replica_root_public_key)) return error.StoreRootEnrollmentChanged;
            if (command.action == .status) return std.json.Stringify.valueAlloc(allocator, self.entry orelse return error.CatalogNotFound, .{});
            if (!self.decoder_ready) return error.TableTopologyProtocolUpgradeRequired;
            if (command.action == .activate and self.block_activation) return error.MetadataMutationOutcomeUnknown;
            self.entry = try wire.transition(self.entry, command);
            if (command.action == .activate) {
                self.activations += 1;
                if (self.lose_activation_response) return error.MetadataMutationOutcomeUnknown;
            }
            return std.json.Stringify.valueAlloc(allocator, self.entry.?, .{});
        }
        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_incarnation = realm, .metrics = .{} };
        }
        fn snapshot(raw: *anyopaque, request: @import("antfly_local_sources").api_operation.RequestContext) !?metadata.AdminSnapshot {
            try request.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lock();
            defer self.mutex.unlock();
            const nodes = try std.testing.allocator.dupe(@TypeOf(self.nodes[0]), &self.nodes);
            errdefer std.testing.allocator.free(nodes);
            const stores = try std.testing.allocator.dupe(@TypeOf(self.stores[0]), &self.stores);
            self.captures += 1;
            return .{ .status = try status(raw), .nodes = nodes, .stores = stores, .tables = &.{}, .ranges = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(raw: *anyopaque, snapshot_: *metadata.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.testing.allocator.free(snapshot_.nodes);
            std.testing.allocator.free(snapshot_.stores);
            self.lock();
            defer self.mutex.unlock();
            self.frees += 1;
        }
    };
    inline for (.{ false, true }) |use_opaque| {
        const Server = if (use_opaque) OpaqueApiHttpServer else server_mod.ApiHttpServer;
        var runtime = try background.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 16 } });
        defer runtime.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root_path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
        defer alloc.free(root_path);
        const signing = try signing_mod.loadOrCreate(alloc, std.testing.io, root_path);
        var source = Source{ .stores = .{.{ .store_id = 13, .node_id = 9, .replica_root_incarnation = signing.root_incarnation, .replica_root_public_key = signing.public_key, .reporter_incarnation = 17 }} };
        const status_source: server_mod.StatusSource = .{ .ptr = &source, .vtable = &.{ .status = Source.status, .linearizable_snapshot = Source.snapshot, .free_admin_snapshot = Source.free, .system_catalog = Source.catalog } };
        const api_config: server_mod.ApiHttpServerConfig = .{ .request_memory_bytes = 16 << 20, .retained_memory_bytes = 16 << 20, .backend_runtime = runtime.ptr(), .internal_service_secret = "0123456789abcdef0123456789abcdef", .internal_service_issuer = "cluster", .runnable_policy = .{ .slots = 2, .max_bytes = @import("antfly_local_sources").common_config.default_runnable_policy.max_bytes, .control_bytes = @import("antfly_local_sources").common_config.default_runnable_policy.control_bytes, .read_isolation = false } };
        const local = @import("../common/remote_membership.zig").Local{ .metadata_incarnation = realm, .node_id = 9, .store_id = 13, .root_incarnation = signing.root_incarnation, .reporter_incarnation = 17 };
        const endpoint = try alloc.dupe(u8, "http://127.0.0.1:1");
        defer alloc.free(endpoint);
        const targets = [_]owner_mod.Target{.{ .local = local, .base_uri = endpoint, .max_attempts = 2, .max_bytes = 8192 }};
        const config: owner_mod.Bootstrap = .{ .root_path = root_path, .local = local, .targets = &targets, .max_outbound_attempts = 4, .max_outbound_bytes = 8192 };
        {
            var small_config = api_config;
            small_config.runnable_policy.?.control_bytes = 64 << 10;
            var small_api = try Server.initWithConfig(alloc, small_config, status_source, null, null);
            defer small_api.deinit();
            try std.testing.expectError(if (use_opaque) error.Unavailable else error.CleanupCapacityUnavailable, small_api.bootstrapRemoteAttempts(config));
            try std.testing.expectEqual(@as(usize, 0), source.captures);
            try std.testing.expect(source.entry == null);
            try std.testing.expect(!(try small_api.remoteAttemptOwnerStats()).initialized);
        }
        var api = try Server.initWithConfig(alloc, api_config, status_source, null, null);
        var live = true;
        defer if (live) api.deinit();
        try std.testing.expect(!(try api.remoteAttemptOwnerStats()).initialized);
        try std.testing.expectError(if (use_opaque) error.UnsupportedVersion else error.TableTopologyProtocolUpgradeRequired, api.bootstrapRemoteAttempts(config));
        try std.testing.expect(!(try api.remoteAttemptOwnerStats()).initialized);
        try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, "remote-fences.v1", .{}));
        source.decoder_ready = true;
        source.block_activation = true;
        try std.testing.expectError(if (use_opaque) error.Unavailable else error.MetadataMutationOutcomeUnknown, api.bootstrapRemoteAttempts(config));
        try std.testing.expect(!(try api.remoteAttemptOwnerStats()).initialized);
        try std.testing.expectEqual(@import("../metadata/remote_attempt_initialization.zig").Phase.prepared, source.entry.?.phase);
        source.block_activation = false;
        source.lose_activation_response = true;
        try api.bootstrapRemoteAttempts(config);
        const first = try api.remoteAttemptOwnerStats();
        try std.testing.expect(first.initialized and first.recovery_complete);
        try std.testing.expectEqual(@as(u64, 2), first.generation);
        try std.testing.expectEqual(@as(usize, 1), first.startup_fences_remaining);
        try std.testing.expectEqual(@as(usize, 0), first.outbound_bytes);
        try std.testing.expectError(error.InvalidArgument, api.bootstrapRemoteAttempts(config));
        endpoint[7] = '9';
        if (!use_opaque) {
            const state = api.owned_remote_attempts.load(.acquire).?;
            try std.testing.expectEqualStrings("http://127.0.0.1:1", api.owned_remote_attempts.load(.acquire).?.targets[0].base_uri);
            try std.testing.expect(!api.owned_remote_attempts.load(.acquire).?.targets[0].owned_reads_v1);
            try std.testing.expect(api.read_retirement_runtime != null and api.remote_reconciliation != null);
            // Every node-owned control allocation remains charged until its
            // owner retires, including the preallocated admission gate.
            const expected_control_bytes = state.reservedBytes() + api.read_retirement_runtime.?.reservedBytes() +
                api.remote_reconciliation.?.reservedBytes() +
                state.capabilities.?.reservedBytes() +
                state.endpoint_view.reservedBytes() +
                state.endpoint_source.?.reservedBytes() +
                state.control_admission.reservedBytes();
            try std.testing.expectEqual(expected_control_bytes, api.runnable_scheduler.?.stats().control_retained_bytes);
            try std.testing.expect(!api.owned_remote_attempts.load(.acquire).?.targets[0].owned_retained_reads_v1);
            // Exercise the owner's actual protected transport, not a supplied
            // executor. Recovery follows the durable unused-journal proof.
            api.stopRemoteReconciliation();
            // Explicit trusted rollout on a fully bootstrapped host. Exercise
            // production readiness advertisement and automatic signed discovery
            // over the real protected listener and node-owned transport.
            try std.testing.expect(!api.cfg.remote_read_capabilities.group_query and !api.cfg.remote_read_capabilities.retained_read);
            api.cfg.remote_read_capabilities = .{ .group_query = true, .retained_read = true };
            var listener = try @import("http_test_runtime.zig").Runtime.startRemoteAttemptControlOwned(alloc, &api);
            var listener_live = true;
            defer if (listener_live) listener.deinit();
            const base_uri = try listener.baseUri(alloc);
            defer alloc.free(base_uri);
            const replacement = try state.allocator.dupe(u8, base_uri);
            state.allocator.free(state.targets[0].base_uri);
            state.targets[0].base_uri = replacement;
            {
                const peers = state.endpoint_source.?;
                var current = try peers.current();
                defer current.release();
                const profiles = [_]owner_mod.Target{.{ .local = state.targets[0].authority.local, .base_uri = replacement, .max_attempts = state.destinations[0].max_attempts, .max_bytes = state.destinations[0].max_bytes }};
                _ = try peers.acceptSnapshot(current.revision, &profiles, &.{state.targets[0].authority}, &.{true});
            }
            try api.startRemoteReconciliation(state.client(api.alloc, .{ .secret = api_config.internal_service_secret.?, .issuer = api_config.internal_service_issuer.? }), .{ .interval_ms = 5, .pass_duration_ns = 500 * std.time.ns_per_ms, .rpc_timeout_ns = 250 * std.time.ns_per_ms });
            // Both the outbound round and real receiver share the same single
            // protected CPU slot. Actual socket/file waits must lend it.
            const until = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
            while (true) {
                const observed = try state.capabilities.?.snapshot(0, state.targets[0].authority);
                if (state.ledger.reconciliationAction(0) == null and observed.group_query and observed.retained_read) break;
                if (@import("antfly_platform").time.monotonicNs() >= until) return error.TestOwnedTransportDiscoveryTimeout;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            try std.testing.expectEqual(@as(usize, 0), (try api.remoteAttemptOwnerStats()).startup_fences_remaining);
            try std.testing.expect(api.remote_reconciliation.?.snapshot().discovery_passes > 0);
            const owned_query = api.remoteQueryClient(api.alloc) orelse return error.TestUnexpectedResult;
            const owned_retained = api.remoteRetainedReadClient() orelse return error.TestUnexpectedResult;
            try std.testing.expect(try owned_query.supportsRead(0, false));
            try std.testing.expect(try owned_retained.supportsRead(0, true));
            try std.testing.expect(!state.targets[0].owned_reads_v1 and !state.targets[0].owned_retained_reads_v1);
            api.stopRemoteReconciliation();
            listener.deinit();
            listener_live = false;
        }
        // Refresh obtains its own authoritative URI and preserves durable
        // destination indices. Exercise the real native and opaque entry points.
        source.setProfile(13, 18, "http://127.0.0.1:2");
        try Source.refresh(&api);
        if (!use_opaque) {
            const state = api.owned_remote_attempts.load(.acquire).?;
            var current = try state.endpoint_source.?.current();
            defer current.release();
            try std.testing.expectEqualStrings("http://127.0.0.1:2", current.view().targets[0].base_uri);
            try std.testing.expectEqual(@as(u64, 18), current.view().targets[0].authority.local.reporter_incarnation);
            try std.testing.expectEqual(@as(u64, 17), state.targets[0].authority.local.reporter_incarnation);
            try std.testing.expect(state.capabilities.?.everEnabled(false));
            const families = try current.view().capabilities.?.snapshot(0, current.view().targets[0].authority);
            try std.testing.expect(!families.group_query and !families.retained_read);
        }
        // A missing enrolled root closes new data eligibility; it does not
        // remove the durable destination or retire any attempt.
        source.setProfile(99, 18, "http://127.0.0.1:2");
        try Source.refresh(&api);
        if (!use_opaque) {
            const state = api.owned_remote_attempts.load(.acquire).?;
            var current = try state.endpoint_source.?.current();
            defer current.release();
            try std.testing.expect(!current.view().targets[0].available);
            try std.testing.expectEqual(@as(usize, 1), state.destinations.len);
        }
        source.setProfile(13, 17, "http://127.0.0.1:2");
        try Source.refresh(&api);
        if (!use_opaque) {
            const state = api.owned_remote_attempts.load(.acquire).?;
            const peers = state.endpoint_source.?;
            const binding = try peers.bind(std.math.maxInt(u128));
            defer binding.release();
            const held = peers.scheduler.stats().control_retained_bytes;
            var occupied = try peers.scheduler.reserveControlBaseline(peers.scheduler.config.control_bytes / 4 - held);
            defer occupied.release();
            source.setProfile(13, 17, "http://127.0.0.1:3");
            try std.testing.expectError(error.CleanupCapacityUnavailable, Source.refresh(&api));
            {
                var cleanup = try peers.controlFor(binding.operation, 0);
                defer cleanup.release();
                try std.testing.expectEqualStrings("http://127.0.0.1:3", cleanup.endpoint().base_uri);
                try std.testing.expectEqual(@as(u64, 17), cleanup.endpoint().authority.local.reporter_incarnation);
                var current = try peers.current();
                defer current.release();
                try std.testing.expectEqualStrings("http://127.0.0.1:2", current.view().targets[0].base_uri);
            }
            try std.testing.expectEqual(peers.scheduler.config.control_bytes / 4, peers.scheduler.stats().control_retained_bytes);
            occupied.release();
            try Source.refresh(&api);
            var current = try peers.current();
            defer current.release();
            try std.testing.expectEqualStrings("http://127.0.0.1:3", current.view().targets[0].base_uri);
        }
        if (use_opaque) {
            // Verify the real manifest bridge installs control limits on the
            // host router as well as accepting their kernel registration.
            var handler = try createOpaqueHandler(&api);
            defer handler.deinit();
            try handler.initRuntime(alloc);
            var host = httpx.Server.initWithConfig(alloc, std.testing.io, .{});
            defer host.deinit();
            try handler.registerRoutes(&host);
            var params: [16]httpx.RouteParam = undefined;
            try std.testing.expect(host.router.find(.POST, "/internal/v1/groups/7/tables/docs/owned-query/11/17", &params) != null);
            try std.testing.expect(host.router.find(.POST, "/internal/v1/groups/7/tables/docs/owned-retained-read/11/17", &params) != null);
            try std.testing.expectEqual(@as(?usize, 32), host.router.bodySizeLimit(.POST, internal_routes.remote_attempt_capabilities));
            inline for (.{ internal_routes.remote_attempt_status, internal_routes.remote_attempt_cancel, internal_routes.remote_attempt_fence }) |path| {
                try std.testing.expectEqual(@as(?usize, 2), host.router.bodySizeLimit(.POST, path));
            }
        }
        api.deinit();
        live = false;
        endpoint[7] = '1';
        try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
        api = try Server.initWithConfig(alloc, api_config, status_source, null, null);
        live = true;
        try api.bootstrapRemoteAttempts(config);
        const reopened = try api.remoteAttemptOwnerStats();
        try std.testing.expectEqual(@as(u64, 3), reopened.generation);
        try std.testing.expectEqual(@as(usize, 1), reopened.startup_fences_remaining);
        try std.testing.expect(reopened.recovery_complete);
        api.deinit();
        live = false;
        // Once any work was published, a later process must obtain actual
        // external-work recovery. Mere restart or successful fencing is no proof.
        var journal = try @import("../common/remote_fence_store.zig").Store.open(alloc, std.testing.io, tmp.dir, signing.root_incarnation, owner_mod.node_coordinator_capacity);
        try journal.markPublicationStarted();
        journal.deinit();
        api = try Server.initWithConfig(alloc, api_config, status_source, null, null);
        live = true;
        {
            // Five ordinary control workers and the first exclusive consumer
            // fit; the second exclusive worker must fail without publication.
            var pressure = try runtime.ptr().acquireWorkers(.{ .capacity = 10 });
            defer pressure.release();
            try std.testing.expectError(if (use_opaque) error.Unavailable else error.WorkerCapacityExceeded, api.bootstrapRemoteAttempts(config));
            try std.testing.expect(!(try api.remoteAttemptOwnerStats()).initialized);
            if (!use_opaque) {
                try std.testing.expect(api.read_retirement_runtime == null and api.remote_reconciliation == null);
                try std.testing.expectEqual(@as(u64, 0), api.runnable_scheduler.?.stats().retained_bytes);
                try std.testing.expect(!api.read_retirement_closing);
            }
        }
        try api.bootstrapRemoteAttempts(config);
        try std.testing.expect(!(try api.remoteAttemptOwnerStats()).recovery_complete);
        if (!use_opaque) {
            // Required owned reads must stay on the rejecting owned path while
            // recovery is incomplete, rather than selecting legacy dispatch.
            api.cfg.remote_read_capabilities = .{ .group_query = true, .retained_read = true };
            const query_client = api.remoteQueryClient(alloc) orelse return error.TestUnexpectedResult;
            const retained_client = api.remoteRetainedReadClient() orelse return error.TestUnexpectedResult;
            try std.testing.expect(!query_client.reads_ready and !retained_client.reads_ready);
            try std.testing.expect(!try query_client.supportsRead(0, false));
            try std.testing.expect(!try retained_client.supportsRead(0, true));
        }
        api.deinit();
        live = false;
        if (!use_opaque) {
            const Failures = struct {
                fn run(failing: std.mem.Allocator, cfg: owner_mod.Bootstrap, key: signing_mod.State, initialization_id: [32]u8) !void {
                    const Initializer = struct {
                        fn resolve(raw: *anyopaque) !@import("../common/remote_fence_store.zig").InitializationGrant {
                            const id: *[32]u8 = @ptrCast(@alignCast(raw));
                            return .{ .id = id.*, .phase = .active };
                        }
                        fn activate(raw: *anyopaque, id: [32]u8) !void {
                            const expected: *[32]u8 = @ptrCast(@alignCast(raw));
                            try std.testing.expectEqualDeep(expected.*, id);
                        }
                    };
                    var id = initialization_id;
                    const authority: @import("../common/remote_attempt_reply.zig").Authority = .{ .local = cfg.local, .public_key = key.public_key };
                    var node = try server_mod.RunnableScheduler.NodeOwner.init(.{ .slots = 2, .max_bytes = @import("antfly_local_sources").common_config.default_runnable_policy.max_bytes, .control_bytes = @import("antfly_local_sources").common_config.default_runnable_policy.control_bytes });
                    defer node.deinit();
                    const state = try owner_mod.State.create(failing, failing, std.testing.io, cfg, key, authority, &.{authority}, .{ .authority = .{ .context = &id, .resolve = Initializer.resolve }, .activate = Initializer.activate, .scheduler = &node.scheduler });
                    defer state.destroy();
                }
            };
            try std.testing.checkAllAllocationFailures(alloc, Failures.run, .{ config, signing, source.entry.?.initialization_id });
        }
        try tmp.dir.deleteFile(std.testing.io, "remote-fences.v1");
        api = try Server.initWithConfig(alloc, api_config, status_source, null, null);
        live = true;
        try std.testing.expectError(if (use_opaque) error.FileNotFound else error.RemoteFenceStoreMissing, api.bootstrapRemoteAttempts(config));
        try std.testing.expect(!(try api.remoteAttemptOwnerStats()).initialized);
        // A metadata enrollment mismatch must fail before first-use creation,
        // including when a host sends the descriptor through the opaque ABI.
        source.stores[0].replica_root_public_key = (try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(8))).public_key.toBytes();
        try std.testing.expectError(if (use_opaque) error.Forbidden else error.InvalidRemoteMembership, api.bootstrapRemoteAttempts(config));
        try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, "remote-fences.v1", .{}));
        try std.testing.expectEqual(source.captures, source.frees);
    }
}

test "opaque API pgwire port checks tail before callback and preserves refusal" {
    const Fixture = struct {
        calls: usize = 0,
        port: u16 = 0,
        refuse: bool = false,
        fn read(context: *const CallContext) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(context.handle));
            self.calls += 1;
            if (context.abi_version != abi.abi_version or context.struct_size != @sizeOf(CallContext) or
                !context.input_contract.matches(abi.native_abi.TypeContract.of(void)) or
                !context.output_contract.matches(abi.native_abi.TypeContract.of(u16))) return abi.statusFromError(error.UnsupportedVersion);
            if (self.refuse) return abi.statusFromError(error.Canceled);
            const port: *u16 = @ptrCast(@alignCast(context.output orelse return abi.statusFromError(error.InvalidArgument)));
            port.* = self.port;
            return .ok;
        }
    };
    var fixture: Fixture = .{};
    var table: abi.FunctionTable = undefined;
    table.abi_version = abi.abi_version;
    table.capabilities = abi.Capability.remote_peer_refresh;
    table.struct_size = abi.requiredFunctionTableSize(abi.Capability.remote_peer_refresh).?;
    var server: OpaqueApiHttpServer = undefined;
    server.opaque_handle = &fixture;
    server.functions = &table;
    try std.testing.expectError(error.UnsupportedVersion, server.pgwireBoundPort());
    table.capabilities |= abi.Capability.pgwire_bound_port;
    try std.testing.expectError(error.UnsupportedVersion, server.pgwireBoundPort());
    table.struct_size = abi.requiredFunctionTableSize(abi.Capability.pgwire_bound_port).?;
    table.pgwire_bound_port = null;
    try std.testing.expectError(error.UnsupportedVersion, server.pgwireBoundPort());
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    table.pgwire_bound_port = Fixture.read;
    try std.testing.expectEqual(@as(?u16, null), try server.pgwireBoundPort());
    fixture.port = 54321;
    try std.testing.expectEqual(@as(?u16, 54321), try server.pgwireBoundPort());
    fixture.refuse = true;
    try std.testing.expectError(error.Canceled, server.pgwireBoundPort());
    try std.testing.expectEqual(@as(usize, 3), fixture.calls);
}

test "API kernel HTTP original deadline capture preserves and projects clock authority" {
    const a = std.testing.allocator;
    var target = try @import("vopr").vopr_io.VoprIo.init(.{});
    defer target.deinit();
    var origin = try @import("vopr").vopr_io.VoprIo.init(.{});
    defer origin.deinit();
    origin.monotonic_ns = 100 * std.time.ns_per_ms;
    var request = try httpx.Request.init(a, .GET, "/deadline-probe");
    defer request.deinit();
    var ctx = httpx.Context.init(a, target.io(), &request);
    defer ctx.deinit();
    try std.testing.expectEqual(@as(u8, 0), httpDeadline(&ctx).present);
    ctx.application_deadline_io = origin.io();
    ctx.application_deadline_ns = 350 * std.time.ns_per_ms;
    try std.testing.expectEqual(@as(u64, 250 * std.time.ns_per_ms), httpDeadline(&ctx).timestamp_ns);
    ctx.application_deadline_ns = 50 * std.time.ns_per_ms;
    try std.testing.expectEqual(@as(u8, 1), httpDeadline(&ctx).present);
    try std.testing.expectEqual(@as(u64, 0), httpDeadline(&ctx).timestamp_ns);
    ctx.application_deadline_io = target.io();
    for ([_]u64{ 0, 1, 250 * std.time.ns_per_ms }) |deadline| {
        ctx.application_deadline_ns = deadline;
        try std.testing.expectEqual(deadline, httpDeadline(&ctx).timestamp_ns);
        try std.testing.expectEqual(@as(u8, 1), httpDeadline(&ctx).present);
    }
    ctx.application_deadline_io = null;
    ctx.application_deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
    const native = httpDeadline(&ctx);
    try std.testing.expectEqual(@as(u8, 1), native.present);
    try std.testing.expect(native.timestamp_ns > 0 and native.timestamp_ns <= std.time.ns_per_s);
}

test "runnable metrics service opaque bridge checks owner prefix and preserves versioned telemetry" {
    const Fixture = struct {
        calls: usize = 0,
        service: @import("antfly_runtime_abi").RunnableServiceStats = .{ .available = true, .quantum_ns = 1_000_000 },
        fn read(context: *const CallContext) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(context.handle));
            self.calls += 1;
            if (!abi.validContext(CallContext, context.abi_version, context.struct_size) or !context.output_contract.matches(.of(abi.RunnableStats))) return abi.statusFromError(error.UnsupportedVersion);
            const out: *abi.RunnableStats = @ptrCast(@alignCast(context.output.?));
            out.* = .{ .enabled = true, .service = self.service };
            return .ok;
        }
    };
    var fixture: Fixture = .{};
    fixture.service.observed_slot_ns[0] = 5_000_000;
    fixture.service.debt_slot_ns[0] = 4_000_000;
    fixture.service.estimated_slot_ns[0] = 1_000_000;
    fixture.service.unobserved_estimate_ns[2] = 999;
    fixture.service.refunded_slot_ns[1] = 123;
    fixture.service.debt_paid_slot_ns[0] = 60;
    fixture.service.clipped_adjustments[3] = 1;
    fixture.service.rebate_slot_ns[1] = 500;
    var table: abi.FunctionTable = undefined;
    table.abi_version = 49;
    table.struct_size = @sizeOf(abi.FunctionTable);
    table.capabilities = abi.Capability.core;
    table.runnable_scheduling_stats = Fixture.read;
    var server: OpaqueApiHttpServer = undefined;
    server.opaque_handle = &fixture;
    server.functions = &table;
    try std.testing.expectError(error.UnsupportedVersion, server.runnableSchedulingStatsChecked());
    table.abi_version = abi.abi_version;
    table.struct_size = abi.requiredFunctionTableSize(abi.Capability.core).? - 1;
    try std.testing.expectError(error.UnsupportedVersion, server.runnableSchedulingStatsChecked());
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    table.struct_size += 1;
    const snapshot = try server.runnableSchedulingStatsChecked();
    try std.testing.expectEqualDeep(fixture.service, snapshot.service);
    try std.testing.expectEqual(@as(u64, 5_000_000), snapshot.service.observed_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 4_000_000), snapshot.service.debt_slot_ns[0]);
    fixture.service.available = false;
    try std.testing.expect(!(try server.runnableSchedulingStatsChecked()).service.available);
    fixture.service.version = 0;
    try std.testing.expectError(error.UnsupportedVersion, server.runnableSchedulingStatsChecked());
}
