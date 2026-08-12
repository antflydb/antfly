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

//! Internal compiled boundary for the public API/storage server. Production
//! runtime units own only opaque handles; test builds retain the direct server
//! type for focused coverage.

const builtin = @import("builtin");
const std = @import("std");
const runtime_http_bridge = @import("../runtime_http_bridge.zig");
const abi = @import("kernel_abi.zig");
const server_mod = @import("http_server.zig");
const handler_mod = @import("httpx_handler.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_writes.zig");
const restore_jobs = @import("restore_jobs.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const ha_http_operation = @import("../storage/ha/http_operation.zig");
const httpx = @import("httpx");
const http_common = @import("../common/http/http_common.zig");
const PeerObserver = @import("../common/http/peer_disconnect_observer.zig").Observer;

const CreateContext = abi.CreateContext;
const CallContext = abi.CallContext;
const HandlerCreateContext = abi.HandlerCreateContext;
const direct_codegen = builtin.is_test;

const BoundaryAllocator = struct {
    allocator: std.mem.Allocator,
    abi_allocator: abi.memory_abi.Allocator,
};

extern fn antfly_api_kernel_get_function_table() callconv(.c) *const abi.FunctionTable;

fn callError(status: abi.Status) !void {
    if (status.isOk()) return;
    return abi.errorFromStatus(status);
}

fn validateFunctionTable() !*const abi.FunctionTable {
    const table = antfly_api_kernel_get_function_table();
    if (!abi.validFunctionTable(table, abi.Capability.core))
        return error.UnsupportedVersion;
    return table;
}

pub const ApiHttpServer = if (direct_codegen) server_mod.ApiHttpServer else OpaqueApiHttpServer;
pub const HttpxHandler = if (direct_codegen) handler_mod.AntflyApiHandler else OpaqueHttpxHandler;

const OpaqueApiHttpServer = struct {
    opaque_handle: *anyopaque,
    functions: *const abi.FunctionTable,
    boundary_allocator: *BoundaryAllocator,
    alloc: std.mem.Allocator,
    cfg: server_mod.ApiHttpServerConfig,

    pub const RequestStats = server_mod.ApiHttpServer.RequestStats;

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

    pub fn setAntflyProvider(self: *OpaqueApiHttpServer, provider: ?managed_embedder.AntflyProvider) void {
        var input = provider;
        callInfallible(?managed_embedder.AntflyProvider, void, self.functions.set_provider, self.opaque_handle, &input, null);
    }

    pub fn setHAInternalExecutor(self: *OpaqueApiHttpServer, executor_value: ?ha_http_operation.Executor) void {
        var input = executor_value;
        callInfallible(?ha_http_operation.Executor, void, self.functions.set_ha_executor, self.opaque_handle, &input, null);
    }

    pub fn attachRestoreJobRuntimeStore(self: *OpaqueApiHttpServer, store: *backend_erased.Store) !void {
        try callFallible(backend_erased.Store, void, self.functions.attach_runtime_restore_store, self.opaque_handle, store, null);
    }

    pub fn attachReplicatedRestoreJobStore(self: *OpaqueApiHttpServer, persistence: restore_jobs.ReplicatedPersistence) !void {
        var input = persistence;
        try callError(self.functions.attach_replicated_restore_store(abi.abi_version, self.opaque_handle, &input));
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
    peer_observer: ?PeerObserver = null,

    pub fn initRuntime(self: *OpaqueHttpxHandler, alloc: std.mem.Allocator) !void {
        try callFallible(void, void, self.functions.handler_init, self.handle, null, null);
        self.alloc = alloc;
        // The opaque runtime side owns the real H1 socket. Observe it here and
        // project cancellation through the ABI instead of exposing a handle to
        // the independently generated kernel archive.
        self.peer_observer = PeerObserver.init(alloc, 4096);
        try self.peer_observer.?.start();
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
                .owner = self,
                .functions = self.functions,
                .kernel_route_handle = entry.route_handle,
                .request_body = entry.request_body,
                .streaming_response = entry.streaming_response != 0,
            };
            try self.runtime_routes.append(alloc, route);
            errdefer _ = self.runtime_routes.pop();
            try server.routeWithData(switch (entry.method) {
                .get => .GET,
                .post => .POST,
                .put => .PUT,
                .delete => .DELETE,
            }, path, runtimeApiHttpHandler, route);
        }
    }

    pub fn deinit(self: *OpaqueHttpxHandler) void {
        if (self.alloc) |alloc| {
            for (self.runtime_routes.items) |route| alloc.destroy(route);
            self.runtime_routes.deinit(alloc);
        }
        if (self.peer_observer) |*observer| observer.deinit();
        self.functions.handler_destroy(self.handle);
        self.* = undefined;
    }
};

const RuntimeRoute = struct {
    owner: *OpaqueHttpxHandler,
    functions: *const abi.FunctionTable,
    kernel_route_handle: *anyopaque,
    request_body: abi.RequestBodyMode,
    streaming_response: bool,
};

fn runtimeApiHttpHandler(context: *httpx.Context) anyerror!httpx.Response {
    const route: *const RuntimeRoute = @ptrCast(@alignCast(context.route_data orelse return error.ApiKernelUnavailable));
    if (route.request_body == .buffered) _ = try context.body();
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
            else => return error.MethodNotAllowed,
        },
        .path = abi.Bytes.init(context.request.uri.path),
        .query = abi.OptionalBytes.init(context.request.uri.query),
        .headers_ptr = if (headers.len == 0) null else headers.ptr,
        .headers_len = headers.len,
        .params_ptr = if (params.len == 0) null else params.ptr,
        .params_len = params.len,
        .body = abi.Bytes.init(context.request.body orelse ""),
        .authorization = abi.OptionalBytes.init(context.request.headers.get("Authorization")),
        .content_type = abi.OptionalBytes.init(context.request.headers.get("Content-Type")),
    };
    var transport = runtime_http_bridge.Outbound{ .context = context };
    var cancellation: http_common.RequestCancellation = .{ .borrowed = context.cancellation };
    var cancellation_registration: PeerObserver.Registration = .{};
    if (context.h1_sock) |socket| {
        if (context.h1_has_buffered_input)
            return context.status(503).text("request cancellation unavailable for pipelined request");
        const observer = &(route.owner.peer_observer orelse return error.ObserverUnavailable);
        cancellation_registration = try observer.register(socket.handle, &cancellation, null, null);
    }
    defer cancellation_registration.deinit();
    const cancellation_view: abi.CancellationView = .{
        .context = &cancellation,
        .is_cancelled = struct {
            fn call(raw: ?*const anyopaque) callconv(.c) u8 {
                const value: *const http_common.RequestCancellation = @ptrCast(@alignCast(raw orelse return 0));
                return @intFromBool(value.isCancelled());
            }
        }.call,
    };
    try callError(route.functions.handler_handle_http(&.{
        .abi_version = abi.abi_version,
        .route_handle = route.kernel_route_handle,
        .request = &request_view,
        .cancellation = cancellation_view,
        .stream = if (route.streaming_response) transport.stream() else .{},
        .out_response_handle = &response_handle,
        .out_response = &response_view,
    }));
    const owned_response_handle = response_handle orelse return error.RuntimeBoundaryFailure;
    defer route.functions.handler_destroy_http_response(owned_response_handle);

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

pub fn createHandler(server: *ApiHttpServer) !HttpxHandler {
    if (comptime direct_codegen) return .{ .api_server = server };
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
        const query = handler.query_admission.stats();
        const query_body = handler.query_body_admission.stats();
        const runtime = handler.runtimeStats();
        return .{
            .query_capacity = query.capacity,
            .query_in_flight = query.in_flight,
            .query_peak_in_flight = query.peak_in_flight,
            .query_rejected_total = query.rejected_total,
            .query_body_capacity = query_body.capacity,
            .query_body_in_flight = query_body.in_flight,
            .query_body_peak_in_flight = query_body.peak_in_flight,
            .query_body_rejected_total = query_body.rejected_total,
            .cancellation_watcher_start_failures_total = runtime.cancellation_watcher_start_failures_total,
            .peer_disconnect_cancellations_total = runtime.peer_disconnect_cancellations_total,
            .peer_observer_failures_total = runtime.peer_observer_failures_total,
            .active_peer_observers = runtime.active_peer_observers,
        };
    }
    return handler.stats();
}

pub fn deinitHandler(handler: *HttpxHandler) void {
    if (comptime direct_codegen) handler.deinitRuntime() else handler.deinit();
}

pub fn setAntflyProvider(server: *ApiHttpServer, provider: ?managed_embedder.AntflyProvider) void {
    if (comptime direct_codegen)
        server.antfly_provider = provider
    else
        server.setAntflyProvider(provider);
}
