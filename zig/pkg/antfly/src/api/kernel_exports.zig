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

const std = @import("std");
const abi = @import("kernel_abi.zig");
const server_mod = @import("http_server.zig");
const handler_mod = @import("httpx_handler.zig");
const table_reads = @import("table_read_source.zig");
const table_writes = @import("table_write_source.zig");
const restore_jobs = @import("restore_jobs.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const http_common = @import("../raft/transport/http_common.zig");
const httpx = @import("httpx");
const platform_sync = @import("antfly_platform").sync;

pub const CreateContext = abi.CreateContext;
pub const CallContext = abi.CallContext;
pub const HandlerCreateContext = abi.HandlerCreateContext;

const ServerState = struct {
    owner_alloc: std.mem.Allocator,
    server: server_mod.ApiHttpServer,
    request_alloc_abi: abi.memory_abi.Allocator,
};

const HandlerState = struct {
    alloc: std.mem.Allocator,
    handler: handler_mod.AntflyApiHandler,
    io_impl: ?std.Io.Threaded = null,
    routes: std.ArrayListUnmanaged(*RouteState) = .empty,
    route_manifest: std.ArrayListUnmanaged(abi.RouteManifestEntry) = .empty,
    route_manifest_mutex: std.atomic.Mutex = .unlocked,
    route_manifest_ready: bool = false,
};

const RouteState = struct {
    owner: *HandlerState,
    handler: httpx.Handler,
};

const HttpResponseState = struct {
    alloc: std.mem.Allocator,
    response: httpx.Response,
    header_views: []abi.HeaderView,
};

fn fail(err: anyerror) abi.Status {
    return abi.statusFromError(err);
}

fn validateVersion(version: u32) ?abi.Status {
    if (version == abi.abi_version) return null;
    return fail(error.UnsupportedVersion);
}

fn validateContext(comptime T: type, version: u32, struct_size: u32) ?abi.Status {
    if (!abi.validContext(T, version, struct_size)) return fail(error.UnsupportedVersion);
    return null;
}

fn validateNativeValue(
    comptime T: type,
    pointer: ?*const anyopaque,
    contract: abi.native_abi.TypeContract,
) ?abi.Status {
    if (T == void) {
        if (pointer != null or !contract.matches(.of(void)))
            return fail(error.InvalidArgument);
        return null;
    }
    if (pointer == null or !contract.matches(.of(T)))
        return fail(error.InvalidArgument);
    return null;
}

fn validateNativeOutput(
    comptime T: type,
    pointer: ?*anyopaque,
    contract: abi.native_abi.TypeContract,
) ?abi.Status {
    return validateNativeValue(T, pointer, contract);
}

fn validateCall(comptime Input: type, comptime Output: type, context: *const CallContext) ?abi.Status {
    if (validateContext(CallContext, context.abi_version, context.struct_size)) |failure| return failure;
    if (validateNativeValue(Input, context.input, context.input_contract)) |failure| return failure;
    if (validateNativeOutput(Output, context.output, context.output_contract)) |failure| return failure;
    return null;
}

fn serverState(context: *const CallContext) *ServerState {
    return @ptrCast(@alignCast(context.handle));
}

fn input(comptime T: type, context: *const CallContext) *const T {
    return @ptrCast(@alignCast(context.input orelse @panic("missing API kernel input")));
}

fn output(comptime T: type, context: *const CallContext) *T {
    return @ptrCast(@alignCast(context.output orelse @panic("missing API kernel output")));
}

pub fn create(context: *const CreateContext) callconv(.c) abi.Status {
    if (validateContext(CreateContext, context.abi_version, context.struct_size)) |failure| return failure;
    if (context.flags & ~CreateContext.fallible_init != 0)
        return fail(error.UnsupportedVersion);
    if (!context.owner_alloc.valid())
        return fail(error.UnsupportedVersion);
    if (!context.cfg_contract.matches(.of(server_mod.ApiHttpServerConfig)) or
        !context.source_contract.matches(.of(server_mod.StatusSource)) or
        !context.table_reads_contract.matches(.of(?table_reads.TableReadSource)) or
        !context.table_writes_contract.matches(.of(?table_writes.TableWriteSource)))
        return fail(error.InvalidArgument);
    const cfg: *const server_mod.ApiHttpServerConfig = @ptrCast(@alignCast(context.cfg));
    const source: *const server_mod.StatusSource = @ptrCast(@alignCast(context.source));
    const reads: *const ?table_reads.TableReadSource = @ptrCast(@alignCast(context.table_reads));
    const writes: *const ?table_writes.TableWriteSource = @ptrCast(@alignCast(context.table_writes));
    const owner_alloc = context.owner_alloc.asStd();
    const state = owner_alloc.create(ServerState) catch |err| {
        std.log.err("API kernel create failed allocating state: error.{s}", .{@errorName(err)});
        return fail(err);
    };
    errdefer owner_alloc.destroy(state);

    state.* = .{
        .owner_alloc = owner_alloc,
        .server = if (context.flags & CreateContext.fallible_init != 0)
            server_mod.ApiHttpServer.initWithConfig(owner_alloc, cfg.*, source.*, reads.*, writes.*) catch |err| {
                std.log.err("API kernel create failed initializing server: error.{s}", .{@errorName(err)});
                return fail(err);
            }
        else
            server_mod.ApiHttpServer.initWithProcessRequestAllocator(owner_alloc, cfg.*, source.*, reads.*, writes.*),
        .request_alloc_abi = undefined,
    };
    state.request_alloc_abi = .fromStd(&state.server.alloc);
    context.out_handle.* = state;
    context.out_request_alloc.* = &state.request_alloc_abi;
    return .ok;
}

pub fn destroy(opaque_handle: *anyopaque) callconv(.c) void {
    const state: *ServerState = @ptrCast(@alignCast(opaque_handle));
    const owner_alloc = state.owner_alloc;
    state.server.deinit();
    owner_alloc.destroy(state);
}

pub fn requestStats(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, server_mod.ApiHttpServer.RequestStats, context)) |failure| return failure;
    output(server_mod.ApiHttpServer.RequestStats, context).* = serverState(context).server.requestStats();
    return .ok;
}

pub fn setProvider(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(?managed_embedder.AntflyProvider, void, context)) |failure| return failure;
    serverState(context).server.antfly_provider = input(?managed_embedder.AntflyProvider, context).*;
    return .ok;
}

pub fn setHAExecutor(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(?http_common.RequestExecutor, void, context)) |failure| return failure;
    serverState(context).server.setHAInternalExecutor(input(?http_common.RequestExecutor, context).*);
    return .ok;
}

pub fn attachRuntimeRestoreStore(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(backend_erased.Store, void, context)) |failure| return failure;
    serverState(context).server.attachRestoreJobRuntimeStore(@constCast(input(backend_erased.Store, context))) catch |err|
        return fail(err);
    return .ok;
}

pub fn attachReplicatedRestoreStore(
    abi_version: u32,
    server_handle: *anyopaque,
    persistence_opaque: *const anyopaque,
) callconv(.c) abi.Status {
    if (validateVersion(abi_version)) |failure| return failure;
    const persistence: *const restore_jobs.ReplicatedPersistence = @ptrCast(@alignCast(persistence_opaque));
    if (persistence.version != restore_jobs.ReplicatedPersistence.abi_version)
        return fail(error.UnsupportedVersion);
    const state: *ServerState = @ptrCast(@alignCast(server_handle));
    state.server.attachReplicatedRestoreJobStore(persistence.*) catch |err|
        return fail(err);
    return .ok;
}

pub fn resumeRestoreJobs(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    serverState(context).server.resumeRestoreJobsOnce() catch |err| return fail(err);
    return .ok;
}

pub fn pollRestoreJobs(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    serverState(context).server.pollRestoreJobsOnce() catch |err| return fail(err);
    return .ok;
}

pub fn prepareRestoreLeadership(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(u64, void, context)) |failure| return failure;
    serverState(context).server.prepareRestoreLeadership(input(u64, context).*) catch |err| return fail(err);
    return .ok;
}

pub fn scheduleSessionMaintenance(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    serverState(context).server.scheduleSessionMaintenance() catch |err| return fail(err);
    return .ok;
}

pub fn storageMaintenanceActive(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, bool, context)) |failure| return failure;
    output(bool, context).* = serverState(context).server.storageMaintenanceExclusiveActive();
    return .ok;
}

pub fn checkReady(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    serverState(context).server.checkReady() catch |err| return fail(err);
    return .ok;
}

pub fn authorizeInference(context: *const abi.AuthorizeInferenceContext) callconv(.c) abi.Status {
    if (validateContext(abi.AuthorizeInferenceContext, context.abi_version, context.struct_size)) |failure| return failure;
    const state: *ServerState = @ptrCast(@alignCast(context.handle));
    context.out_decision.* = state.server.authorizeInferenceRequest(.{
        .authorization = context.authorization.slice(),
        .trusted_principal = context.trusted_principal.slice(),
    }, context.permission) catch |err| return fail(err);
    return .ok;
}

pub fn handlerCreate(context: *const HandlerCreateContext) callconv(.c) abi.Status {
    if (validateContext(HandlerCreateContext, context.abi_version, context.struct_size)) |failure| return failure;
    const api_state: *ServerState = @ptrCast(@alignCast(context.api_server_handle));
    const state = api_state.owner_alloc.create(HandlerState) catch |err| return fail(err);
    state.* = .{
        .alloc = api_state.owner_alloc,
        .handler = .{ .api_server = &api_state.server },
    };
    context.out_handle.* = state;
    return .ok;
}

fn handlerState(context: *const CallContext) *HandlerState {
    return @ptrCast(@alignCast(context.handle));
}

pub fn handlerInit(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    const state = handlerState(context);
    state.handler.initRuntime(state.alloc) catch |err|
        return fail(err);
    state.io_impl = std.Io.Threaded.init(state.alloc, .{});
    return .ok;
}

pub fn handlerStats(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, abi.HandlerStats, context)) |failure| return failure;
    const handler = &handlerState(context).handler;
    const query = handler.query_admission.stats();
    const query_body = handler.query_body_admission.stats();
    const runtime = handler.runtimeStats();
    output(abi.HandlerStats, context).* = .{
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
    return .ok;
}

pub fn handlerRouteManifest(context: *const abi.RouteManifestContext) callconv(.c) abi.Status {
    if (validateContext(abi.RouteManifestContext, context.abi_version, context.struct_size)) |failure| return failure;
    const state: *HandlerState = @ptrCast(@alignCast(context.handler_handle));
    platform_sync.lockYielding(&state.route_manifest_mutex);
    defer state.route_manifest_mutex.unlock();
    if (!state.route_manifest_ready) {
        const routes_start = state.routes.items.len;
        const manifest_start = state.route_manifest.items.len;
        const handler = &state.handler;
        var public_manifest = ManifestServer("/db/v1"){ .owner = state };
        var root_manifest = ManifestServer(""){ .owner = state };
        handler.registerRouteSets(&public_manifest, &root_manifest, true, true) catch |err| {
            for (state.routes.items[routes_start..]) |route| state.alloc.destroy(route);
            state.routes.shrinkRetainingCapacity(routes_start);
            state.route_manifest.shrinkRetainingCapacity(manifest_start);
            return fail(err);
        };
        state.route_manifest_ready = true;
    }
    context.out_entries.* = if (state.route_manifest.items.len == 0) null else state.route_manifest.items.ptr;
    context.out_len.* = state.route_manifest.items.len;
    return .ok;
}

pub fn handlerHandleHttp(context: *const abi.HttpHandleContext) callconv(.c) abi.Status {
    if (validateContext(abi.HttpHandleContext, context.abi_version, context.struct_size)) |failure| return failure;
    const route: *RouteState = @ptrCast(@alignCast(context.route_handle));
    const state = route.owner;
    const request = context.request;
    const alloc = state.alloc;
    const input_headers = if (request.headers_ptr) |ptr| ptr[0..request.headers_len] else &.{};

    const query = request.query.slice();
    const target = if (query) |value|
        std.fmt.allocPrint(alloc, "{s}?{s}", .{ request.path.slice(), value }) catch |err| return fail(err)
    else
        alloc.dupe(u8, request.path.slice()) catch |err| return fail(err);
    defer alloc.free(target);

    var http_request = httpx.Request.init(alloc, switch (request.method) {
        .get => .GET,
        .post => .POST,
        .put => .PUT,
        .delete => .DELETE,
    }, target) catch |err| return fail(err);
    defer http_request.deinit();
    for (input_headers) |header|
        http_request.headers.append(header.name.slice(), header.value.slice()) catch |err| return fail(err);
    http_request.body = request.body.slice();

    const input_params = if (request.params_ptr) |ptr| ptr[0..request.params_len] else &.{};
    const params = alloc.alloc(httpx.RouteParam, input_params.len) catch |err| return fail(err);
    defer alloc.free(params);
    for (input_params, 0..) |param, i| {
        params[i] = .{ .name = param.name.slice(), .value = param.value.slice() };
    }

    const io_impl = &(state.io_impl orelse return fail(error.ApiKernelNotInitialized));
    var http_context = httpx.Context.init(alloc, io_impl.io(), &http_request);
    defer http_context.deinit();
    http_context.params = params;
    var response = route.handler.invoke(&http_context) catch |err| return fail(err);
    errdefer response.deinit();

    const response_state = alloc.create(HttpResponseState) catch |err| return fail(err);
    errdefer alloc.destroy(response_state);
    const response_headers = response.headers.iterator();
    const header_views = alloc.alloc(abi.HeaderView, response_headers.len) catch |err| return fail(err);
    errdefer alloc.free(header_views);
    for (response_headers, 0..) |header, i| {
        header_views[i] = .{
            .name = abi.Bytes.init(header.name),
            .value = abi.Bytes.init(header.value),
        };
    }
    response_state.* = .{
        .alloc = alloc,
        .response = response,
        .header_views = header_views,
    };
    context.out_response_handle.* = response_state;
    context.out_response.* = .{
        .status = response.status.code,
        .content_type = abi.OptionalBytes.init(response.contentType()),
        .headers_ptr = if (header_views.len == 0) null else header_views.ptr,
        .headers_len = header_views.len,
        .body = abi.Bytes.init(response.body orelse ""),
    };
    return .ok;
}

pub fn handlerDestroyHttpResponse(response_handle: *anyopaque) callconv(.c) void {
    const state: *HttpResponseState = @ptrCast(@alignCast(response_handle));
    const alloc = state.alloc;
    state.response.deinit();
    alloc.free(state.header_views);
    alloc.destroy(state);
}

pub fn handlerDestroy(opaque_handle: *anyopaque) callconv(.c) void {
    const state: *HandlerState = @ptrCast(@alignCast(opaque_handle));
    const alloc = state.alloc;
    for (state.routes.items) |route| alloc.destroy(route);
    state.routes.deinit(alloc);
    state.route_manifest.deinit(alloc);
    state.handler.deinitRuntime();
    if (state.io_impl) |*io_impl| io_impl.deinit();
    alloc.destroy(state);
}

const function_table: abi.FunctionTable = .{
    .abi_version = abi.abi_version,
    .struct_size = @sizeOf(abi.FunctionTable),
    .capabilities = abi.Capability.core | abi.Capability.route_manifest,
    .create = &create,
    .destroy = &destroy,
    .request_stats = &requestStats,
    .set_provider = &setProvider,
    .set_ha_executor = &setHAExecutor,
    .attach_runtime_restore_store = &attachRuntimeRestoreStore,
    .attach_replicated_restore_store = &attachReplicatedRestoreStore,
    .resume_restore_jobs = &resumeRestoreJobs,
    .poll_restore_jobs = &pollRestoreJobs,
    .prepare_restore_leadership = &prepareRestoreLeadership,
    .schedule_session_maintenance = &scheduleSessionMaintenance,
    .storage_maintenance_active = &storageMaintenanceActive,
    .check_ready = &checkReady,
    .authorize_inference = &authorizeInference,
    .handler_create = &handlerCreate,
    .handler_init = &handlerInit,
    .handler_stats = &handlerStats,
    .handler_route_manifest = &handlerRouteManifest,
    .handler_handle_http = &handlerHandleHttp,
    .handler_destroy_http_response = &handlerDestroyHttpResponse,
    .handler_destroy = &handlerDestroy,
};

pub fn getFunctionTable() callconv(.c) *const abi.FunctionTable {
    return &function_table;
}

fn ManifestServer(comptime prefix: []const u8) type {
    return struct {
        owner: *HandlerState,

        fn register(self: *const @This(), method: abi.HttpMethod, comptime path: []const u8, handler: httpx.Handler) !void {
            const route = try self.owner.alloc.create(RouteState);
            errdefer self.owner.alloc.destroy(route);
            route.* = .{ .owner = self.owner, .handler = handler };
            try self.owner.routes.append(self.owner.alloc, route);
            errdefer _ = self.owner.routes.pop();
            try self.owner.route_manifest.append(self.owner.alloc, .{
                .route_handle = route,
                .method = method,
                .path = abi.Bytes.init(prefix ++ path),
            });
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.get, path, handler);
        }

        pub fn post(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.post, path, handler);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.put, path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.delete, path, handler);
        }
    };
}
