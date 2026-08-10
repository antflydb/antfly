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
//! runtimes own only opaque handles; test builds retain the direct server type.

const std = @import("std");
const linked_runtime_options = @import("linked_runtime_options");
const abi = @import("kernel_abi.zig");
const server_mod = @import("http_server.zig");
const handler_mod = @import("httpx_handler.zig");
const table_read_source = @import("table_read_source.zig");
const table_write_source = @import("table_write_source.zig");
const restore_jobs = @import("restore_jobs.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const http_common = @import("../raft/transport/http_common.zig");
const httpx = @import("httpx");

const ErrorInt = abi.ErrorInt;
const CreateContext = abi.CreateContext;
const CallContext = abi.CallContext;
const HandlerCreateContext = abi.HandlerCreateContext;
const ok: c_int = 0;

extern fn antfly_api_kernel_create(context: *const CreateContext) callconv(.c) c_int;
extern fn antfly_api_kernel_destroy(handle: *anyopaque) callconv(.c) void;
extern fn antfly_api_kernel_request_stats(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_set_provider(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_set_ha_executor(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_executor(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_streaming_executor(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_attach_runtime_restore_store(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_attach_replicated_restore_store(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_resume_restore_jobs(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_poll_restore_jobs(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_prepare_restore_leadership(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_schedule_session_maintenance(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_storage_maintenance_active(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_handle(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_handle_internal(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_handler_create(context: *const HandlerCreateContext) callconv(.c) c_int;
extern fn antfly_api_kernel_handler_init(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_handler_stats(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_handler_register_routes(context: *const CallContext) callconv(.c) c_int;
extern fn antfly_api_kernel_handler_destroy(handle: *anyopaque) callconv(.c) void;

fn callError(status: c_int, error_code: ErrorInt) !void {
    if (status == ok) return;
    if (error_code != 0) return @errorFromInt(error_code);
    return error.ApiKernelOperationFailed;
}

pub const ApiHttpServer = if (linked_runtime_options.enabled) OpaqueApiHttpServer else server_mod.ApiHttpServer;
pub const HttpxHandler = if (linked_runtime_options.enabled) OpaqueHttpxHandler else handler_mod.AntflyApiHandler;

const OpaqueApiHttpServer = struct {
    opaque_handle: *anyopaque,
    alloc: std.mem.Allocator,
    cfg: server_mod.ApiHttpServerConfig,

    pub const RequestStats = server_mod.ApiHttpServer.RequestStats;

    pub fn initWithConfig(
        owner_alloc: std.mem.Allocator,
        cfg: server_mod.ApiHttpServerConfig,
        source: server_mod.StatusSource,
        read_source: ?table_read_source.TableReadSource,
        write_source: ?table_write_source.TableWriteSource,
    ) !OpaqueApiHttpServer {
        return createOpaqueServer(owner_alloc, cfg, source, read_source, write_source, true);
    }

    pub fn initWithProcessRequestAllocator(
        owner_alloc: std.mem.Allocator,
        cfg: server_mod.ApiHttpServerConfig,
        source: server_mod.StatusSource,
        read_source: ?table_read_source.TableReadSource,
        write_source: ?table_write_source.TableWriteSource,
    ) OpaqueApiHttpServer {
        return createOpaqueServer(owner_alloc, cfg, source, read_source, write_source, false) catch
            @panic("API kernel allocation failed");
    }

    pub fn deinit(self: *OpaqueApiHttpServer) void {
        antfly_api_kernel_destroy(self.opaque_handle);
        self.* = undefined;
    }

    pub fn requestStats(self: *OpaqueApiHttpServer) RequestStats {
        var out: RequestStats = undefined;
        callInfallible(antfly_api_kernel_request_stats, self.opaque_handle, null, &out);
        return out;
    }

    pub fn setAntflyProvider(self: *OpaqueApiHttpServer, provider: ?managed_embedder.AntflyProvider) void {
        var input = provider;
        callInfallible(antfly_api_kernel_set_provider, self.opaque_handle, &input, null);
    }

    pub fn setHAInternalExecutor(self: *OpaqueApiHttpServer, executor_value: ?http_common.RequestExecutor) void {
        var input = executor_value;
        callInfallible(antfly_api_kernel_set_ha_executor, self.opaque_handle, &input, null);
    }

    pub fn executor(self: *OpaqueApiHttpServer) http_common.RequestExecutor {
        var out: http_common.RequestExecutor = undefined;
        callInfallible(antfly_api_kernel_executor, self.opaque_handle, null, &out);
        return out;
    }

    pub fn streamingExecutor(self: *OpaqueApiHttpServer) http_common.StreamingRequestExecutor {
        var out: http_common.StreamingRequestExecutor = undefined;
        callInfallible(antfly_api_kernel_streaming_executor, self.opaque_handle, null, &out);
        return out;
    }

    pub fn attachRestoreJobRuntimeStore(self: *OpaqueApiHttpServer, store: *backend_erased.Store) !void {
        try callFallible(antfly_api_kernel_attach_runtime_restore_store, self.opaque_handle, store, null);
    }

    pub fn attachReplicatedRestoreJobStore(self: *OpaqueApiHttpServer, persistence: restore_jobs.ReplicatedPersistence) !void {
        var input = persistence;
        try callFallible(antfly_api_kernel_attach_replicated_restore_store, self.opaque_handle, &input, null);
    }

    pub fn resumeRestoreJobsOnce(self: *OpaqueApiHttpServer) !void {
        try callFallible(antfly_api_kernel_resume_restore_jobs, self.opaque_handle, null, null);
    }

    pub fn pollRestoreJobsOnce(self: *OpaqueApiHttpServer) !void {
        try callFallible(antfly_api_kernel_poll_restore_jobs, self.opaque_handle, null, null);
    }

    pub fn prepareRestoreLeadership(self: *OpaqueApiHttpServer, term: u64) !void {
        var input = term;
        try callFallible(antfly_api_kernel_prepare_restore_leadership, self.opaque_handle, &input, null);
    }

    pub fn scheduleSessionMaintenance(self: *OpaqueApiHttpServer) !void {
        try callFallible(antfly_api_kernel_schedule_session_maintenance, self.opaque_handle, null, null);
    }

    pub fn storageMaintenanceExclusiveActive(self: *const OpaqueApiHttpServer) bool {
        var out = false;
        callInfallible(antfly_api_kernel_storage_maintenance_active, self.opaque_handle, null, &out);
        return out;
    }

    pub fn handle(self: *OpaqueApiHttpServer, req: http_common.HttpRequest) !http_common.HttpResponse {
        var input = req;
        var out: http_common.HttpResponse = undefined;
        try callFallible(antfly_api_kernel_handle, self.opaque_handle, &input, &out);
        return out;
    }

    pub fn handleInternalRoute(self: *OpaqueApiHttpServer, req: http_common.HttpRequest) !?http_common.HttpResponse {
        var input = req;
        var out: ?http_common.HttpResponse = null;
        try callFallible(antfly_api_kernel_handle_internal, self.opaque_handle, &input, &out);
        return out;
    }
};

fn createOpaqueServer(
    owner_alloc: std.mem.Allocator,
    cfg: server_mod.ApiHttpServerConfig,
    source: server_mod.StatusSource,
    read_source: ?table_read_source.TableReadSource,
    write_source: ?table_write_source.TableWriteSource,
    fallible: bool,
) !OpaqueApiHttpServer {
    var alloc_copy = owner_alloc;
    var cfg_copy = cfg;
    var source_copy = source;
    var reads_copy = read_source;
    var writes_copy = write_source;
    var handle: ?*anyopaque = null;
    var request_alloc: std.mem.Allocator = undefined;
    var error_code: ErrorInt = 0;
    const status = antfly_api_kernel_create(&.{
        .owner_alloc = &alloc_copy,
        .cfg = &cfg_copy,
        .source = &source_copy,
        .table_reads = &reads_copy,
        .table_writes = &writes_copy,
        .fallible = fallible,
        .out_handle = &handle,
        .out_request_alloc = &request_alloc,
        .error_code = &error_code,
    });
    try callError(status, error_code);
    return .{ .opaque_handle = handle orelse return error.ApiKernelOperationFailed, .alloc = request_alloc, .cfg = cfg };
}

fn callFallible(
    comptime function: fn (*const CallContext) callconv(.c) c_int,
    handle: *anyopaque,
    input: ?*const anyopaque,
    output: ?*anyopaque,
) !void {
    var error_code: ErrorInt = 0;
    try callError(function(&.{ .handle = handle, .input = input, .output = output, .error_code = &error_code }), error_code);
}

fn callInfallible(
    comptime function: fn (*const CallContext) callconv(.c) c_int,
    handle: *anyopaque,
    input: ?*const anyopaque,
    output: ?*anyopaque,
) void {
    callFallible(function, handle, input, output) catch @panic("infallible API kernel call failed");
}

pub const HandlerStats = abi.HandlerStats;

const OpaqueHttpxHandler = struct {
    handle: *anyopaque,

    pub fn initRuntime(self: *OpaqueHttpxHandler, alloc: std.mem.Allocator) !void {
        var input = alloc;
        try callFallible(antfly_api_kernel_handler_init, self.handle, &input, null);
    }

    pub fn stats(self: *const OpaqueHttpxHandler) HandlerStats {
        var out: HandlerStats = undefined;
        callInfallible(antfly_api_kernel_handler_stats, self.handle, null, &out);
        return out;
    }

    pub fn registerRoutes(self: *OpaqueHttpxHandler, server: *httpx.Server) !void {
        try callFallible(antfly_api_kernel_handler_register_routes, self.handle, server, null);
    }

    pub fn deinit(self: *OpaqueHttpxHandler) void {
        antfly_api_kernel_handler_destroy(self.handle);
        self.* = undefined;
    }
};

pub fn createHandler(server: *ApiHttpServer) !HttpxHandler {
    if (comptime !linked_runtime_options.enabled) return .{ .api_server = server };
    var handle: ?*anyopaque = null;
    var error_code: ErrorInt = 0;
    const status = antfly_api_kernel_handler_create(&.{
        .api_server_handle = server.opaque_handle,
        .out_handle = &handle,
        .error_code = &error_code,
    });
    try callError(status, error_code);
    return .{ .handle = handle orelse return error.ApiKernelOperationFailed };
}

pub fn handlerStats(handler: *const HttpxHandler) HandlerStats {
    if (comptime !linked_runtime_options.enabled) {
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
    if (comptime linked_runtime_options.enabled) handler.deinit() else handler.deinitRuntime();
}

pub fn setAntflyProvider(server: *ApiHttpServer, provider: ?managed_embedder.AntflyProvider) void {
    if (comptime linked_runtime_options.enabled) {
        server.setAntflyProvider(provider);
    } else {
        server.antfly_provider = provider;
    }
}
