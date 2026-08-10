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
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_writes.zig");
const restore_jobs = @import("restore_jobs.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const http_common = @import("../raft/transport/http_common.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");
const httpx = @import("httpx");

const ErrorInt = abi.ErrorInt;
pub const CreateContext = abi.CreateContext;
pub const CallContext = abi.CallContext;
pub const HandlerCreateContext = abi.HandlerCreateContext;

const ServerState = struct {
    owner_alloc: std.mem.Allocator,
    server: server_mod.ApiHttpServer,
};

const HandlerState = struct {
    alloc: std.mem.Allocator,
    handler: handler_mod.AntflyApiHandler,
};

fn fail(error_code: *ErrorInt, err: anyerror) c_int {
    error_code.* = @intFromError(err);
    return 1;
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

pub fn create(context: *const CreateContext) callconv(.c) c_int {
    const owner_alloc_ptr: *const std.mem.Allocator = @ptrCast(@alignCast(context.owner_alloc));
    const cfg: *const server_mod.ApiHttpServerConfig = @ptrCast(@alignCast(context.cfg));
    const source: *const server_mod.StatusSource = @ptrCast(@alignCast(context.source));
    const reads: *const ?table_reads.TableReadSource = @ptrCast(@alignCast(context.table_reads));
    const writes: *const ?table_writes.TableWriteSource = @ptrCast(@alignCast(context.table_writes));
    const owner_alloc = owner_alloc_ptr.*;
    const state = owner_alloc.create(ServerState) catch |err| return fail(context.error_code, err);
    errdefer owner_alloc.destroy(state);

    state.* = .{
        .owner_alloc = owner_alloc,
        .server = if (context.fallible)
            server_mod.ApiHttpServer.initWithConfig(owner_alloc, cfg.*, source.*, reads.*, writes.*) catch |err|
                return fail(context.error_code, err)
        else
            server_mod.ApiHttpServer.initWithProcessRequestAllocator(owner_alloc, cfg.*, source.*, reads.*, writes.*),
    };
    context.out_handle.* = state;
    const out_alloc: *std.mem.Allocator = @ptrCast(@alignCast(context.out_request_alloc));
    out_alloc.* = state.server.alloc;
    return 0;
}

pub fn destroy(opaque_handle: *anyopaque) callconv(.c) void {
    const state: *ServerState = @ptrCast(@alignCast(opaque_handle));
    const owner_alloc = state.owner_alloc;
    state.server.deinit();
    owner_alloc.destroy(state);
}

pub fn requestStats(context: *const CallContext) callconv(.c) c_int {
    output(server_mod.ApiHttpServer.RequestStats, context).* = serverState(context).server.requestStats();
    return 0;
}

pub fn setProvider(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.antfly_provider = input(?managed_embedder.AntflyProvider, context).*;
    return 0;
}

pub fn setHAExecutor(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.setHAInternalExecutor(input(?http_common.RequestExecutor, context).*);
    return 0;
}

pub fn executor(context: *const CallContext) callconv(.c) c_int {
    output(http_common.RequestExecutor, context).* = serverState(context).server.executor();
    return 0;
}

pub fn streamingExecutor(context: *const CallContext) callconv(.c) c_int {
    output(http_common.StreamingRequestExecutor, context).* = serverState(context).server.streamingExecutor();
    return 0;
}

pub fn attachRuntimeRestoreStore(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.attachRestoreJobRuntimeStore(@constCast(input(backend_erased.Store, context))) catch |err|
        return fail(context.error_code, err);
    return 0;
}

pub fn attachReplicatedRestoreStore(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.attachReplicatedRestoreJobStore(input(restore_jobs.ReplicatedPersistence, context).*) catch |err|
        return fail(context.error_code, err);
    return 0;
}

pub fn resumeRestoreJobs(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.resumeRestoreJobsOnce() catch |err| return fail(context.error_code, err);
    return 0;
}

pub fn pollRestoreJobs(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.pollRestoreJobsOnce() catch |err| return fail(context.error_code, err);
    return 0;
}

pub fn prepareRestoreLeadership(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.prepareRestoreLeadership(input(u64, context).*) catch |err| return fail(context.error_code, err);
    return 0;
}

pub fn scheduleSessionMaintenance(context: *const CallContext) callconv(.c) c_int {
    serverState(context).server.scheduleSessionMaintenance() catch |err| return fail(context.error_code, err);
    return 0;
}

pub fn storageMaintenanceActive(context: *const CallContext) callconv(.c) c_int {
    output(bool, context).* = serverState(context).server.storageMaintenanceExclusiveActive();
    return 0;
}

pub fn handle(context: *const CallContext) callconv(.c) c_int {
    output(http_common.HttpResponse, context).* = serverState(context).server.handle(input(http_common.HttpRequest, context).*) catch |err|
        return fail(context.error_code, err);
    return 0;
}

pub fn handleInternal(context: *const CallContext) callconv(.c) c_int {
    output(?http_common.HttpResponse, context).* = serverState(context).server.handleInternalRoute(input(http_common.HttpRequest, context).*) catch |err|
        return fail(context.error_code, err);
    return 0;
}

pub fn handlerCreate(context: *const HandlerCreateContext) callconv(.c) c_int {
    const api_state: *ServerState = @ptrCast(@alignCast(context.api_server_handle));
    const state = api_state.owner_alloc.create(HandlerState) catch |err| return fail(context.error_code, err);
    state.* = .{
        .alloc = api_state.owner_alloc,
        .handler = .{ .api_server = &api_state.server },
    };
    context.out_handle.* = state;
    return 0;
}

fn handlerState(context: *const CallContext) *HandlerState {
    return @ptrCast(@alignCast(context.handle));
}

pub fn handlerInit(context: *const CallContext) callconv(.c) c_int {
    handlerState(context).handler.initRuntime(input(std.mem.Allocator, context).*) catch |err|
        return fail(context.error_code, err);
    return 0;
}

pub fn handlerStats(context: *const CallContext) callconv(.c) c_int {
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
    return 0;
}

pub fn handlerRegisterRoutes(context: *const CallContext) callconv(.c) c_int {
    const server: *httpx.Server = @constCast(input(httpx.Server, context));
    const handler = &handlerState(context).handler;
    const public_router = metadata_openapi.server.ServerRouter(handler_mod.AntflyApiHandler).init(handler);
    var public_prefixed = PrefixedServer("/db/v1", httpx.Server){ .inner = server };
    public_router.register(&public_prefixed) catch |err| return fail(context.error_code, err);
    const usermgr_router = usermgr_openapi.server.ServerRouter(handler_mod.AntflyApiHandler).init(handler);
    usermgr_router.register(server) catch |err| return fail(context.error_code, err);
    return 0;
}

pub fn handlerDestroy(opaque_handle: *anyopaque) callconv(.c) void {
    const state: *HandlerState = @ptrCast(@alignCast(opaque_handle));
    const alloc = state.alloc;
    state.handler.deinitRuntime();
    alloc.destroy(state);
}

fn PrefixedServer(comptime prefix: []const u8, comptime ServerType: type) type {
    return struct {
        inner: *ServerType,

        pub fn get(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.get(prefix ++ path, handler);
        }

        pub fn post(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.post(prefix ++ path, handler);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.put(prefix ++ path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.inner.delete(prefix ++ path, handler);
        }
    };
}
