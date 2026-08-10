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
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");
const httpx = @import("httpx");

extern fn antfly_distributed_httpx_register(context: *const abi.RouteContext) callconv(.c) abi.Status;

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

fn fail(err: anyerror) abi.Status {
    return abi.statusFromError(err);
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
    const owner_alloc_ptr: *const std.mem.Allocator = @ptrCast(@alignCast(context.owner_alloc));
    const cfg: *const server_mod.ApiHttpServerConfig = @ptrCast(@alignCast(context.cfg));
    const source: *const server_mod.StatusSource = @ptrCast(@alignCast(context.source));
    const reads: *const ?table_reads.TableReadSource = @ptrCast(@alignCast(context.table_reads));
    const writes: *const ?table_writes.TableWriteSource = @ptrCast(@alignCast(context.table_writes));
    const owner_alloc = owner_alloc_ptr.*;
    const state = owner_alloc.create(ServerState) catch |err| return fail(err);
    errdefer owner_alloc.destroy(state);

    state.* = .{
        .owner_alloc = owner_alloc,
        .server = if (context.fallible)
            server_mod.ApiHttpServer.initWithConfig(owner_alloc, cfg.*, source.*, reads.*, writes.*) catch |err|
                return fail(err)
        else
            server_mod.ApiHttpServer.initWithProcessRequestAllocator(owner_alloc, cfg.*, source.*, reads.*, writes.*),
    };
    context.out_handle.* = state;
    const out_alloc: *std.mem.Allocator = @ptrCast(@alignCast(context.out_request_alloc));
    out_alloc.* = state.server.alloc;
    return .ok;
}

pub fn destroy(opaque_handle: *anyopaque) callconv(.c) void {
    const state: *ServerState = @ptrCast(@alignCast(opaque_handle));
    const owner_alloc = state.owner_alloc;
    state.server.deinit();
    owner_alloc.destroy(state);
}

pub fn requestStats(context: *const CallContext) callconv(.c) abi.Status {
    output(server_mod.ApiHttpServer.RequestStats, context).* = serverState(context).server.requestStats();
    return .ok;
}

pub fn setProvider(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.antfly_provider = input(?managed_embedder.AntflyProvider, context).*;
    return .ok;
}

pub fn setHAExecutor(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.setHAInternalExecutor(input(?http_common.RequestExecutor, context).*);
    return .ok;
}

pub fn executor(context: *const CallContext) callconv(.c) abi.Status {
    output(http_common.RequestExecutor, context).* = serverState(context).server.executor();
    return .ok;
}

pub fn streamingExecutor(context: *const CallContext) callconv(.c) abi.Status {
    output(http_common.StreamingRequestExecutor, context).* = serverState(context).server.streamingExecutor();
    return .ok;
}

pub fn attachRuntimeRestoreStore(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.attachRestoreJobRuntimeStore(@constCast(input(backend_erased.Store, context))) catch |err|
        return fail(err);
    return .ok;
}

pub fn attachReplicatedRestoreStore(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.attachReplicatedRestoreJobStore(input(restore_jobs.ReplicatedPersistence, context).*) catch |err|
        return fail(err);
    return .ok;
}

pub fn resumeRestoreJobs(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.resumeRestoreJobsOnce() catch |err| return fail(err);
    return .ok;
}

pub fn pollRestoreJobs(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.pollRestoreJobsOnce() catch |err| return fail(err);
    return .ok;
}

pub fn prepareRestoreLeadership(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.prepareRestoreLeadership(input(u64, context).*) catch |err| return fail(err);
    return .ok;
}

pub fn scheduleSessionMaintenance(context: *const CallContext) callconv(.c) abi.Status {
    serverState(context).server.scheduleSessionMaintenance() catch |err| return fail(err);
    return .ok;
}

pub fn storageMaintenanceActive(context: *const CallContext) callconv(.c) abi.Status {
    output(bool, context).* = serverState(context).server.storageMaintenanceExclusiveActive();
    return .ok;
}

pub fn handle(context: *const CallContext) callconv(.c) abi.Status {
    output(http_common.HttpResponse, context).* = serverState(context).server.handle(input(http_common.HttpRequest, context).*) catch |err|
        return fail(err);
    return .ok;
}

pub fn handleInternal(context: *const CallContext) callconv(.c) abi.Status {
    output(?http_common.HttpResponse, context).* = serverState(context).server.handleInternalRoute(input(http_common.HttpRequest, context).*) catch |err|
        return fail(err);
    return .ok;
}

pub fn handlerCreate(context: *const HandlerCreateContext) callconv(.c) abi.Status {
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
    handlerState(context).handler.initRuntime(input(std.mem.Allocator, context).*) catch |err|
        return fail(err);
    return .ok;
}

pub fn handlerStats(context: *const CallContext) callconv(.c) abi.Status {
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

pub fn handlerRegisterRoutes(context: *const CallContext) callconv(.c) abi.Status {
    const server: *httpx.Server = @constCast(input(httpx.Server, context));
    const handler = &handlerState(context).handler;
    const public_router = metadata_openapi.server.ServerRouter(handler_mod.AntflyApiHandler).init(handler);
    var public_prefixed = BoundaryServer("/db/v1"){ .inner = server };
    public_router.register(&public_prefixed) catch |err| return fail(err);
    const usermgr_router = usermgr_openapi.server.ServerRouter(handler_mod.AntflyApiHandler).init(handler);
    var usermgr_boundary = BoundaryServer(""){ .inner = server };
    usermgr_router.register(&usermgr_boundary) catch |err| return fail(err);
    return .ok;
}

pub fn handlerDestroy(opaque_handle: *anyopaque) callconv(.c) void {
    const state: *HandlerState = @ptrCast(@alignCast(opaque_handle));
    const alloc = state.alloc;
    state.handler.deinitRuntime();
    alloc.destroy(state);
}

fn stableHandler(comptime handler: httpx.Handler) httpx.Handler {
    return struct {
        fn call(context: *httpx.Context) anyerror!httpx.Response {
            return handler(context) catch |err| {
                // Interpret and log the error while still in the API owner's
                // codegen unit. The distributed server sees only a response.
                std.log.err("API route failed err={s}", .{@errorName(err)});
                return httpx.Response.init(context.allocator, 500);
            };
        }
    }.call;
}

fn BoundaryServer(comptime prefix: []const u8) type {
    return struct {
        inner: *httpx.Server,

        fn register(self: *const @This(), method: abi.HttpMethod, comptime path: []const u8, comptime handler: httpx.Handler) !void {
            const full_path = prefix ++ path;
            const status = antfly_distributed_httpx_register(&.{
                .server = self.inner,
                .method = method,
                .path_ptr = full_path.ptr,
                .path_len = full_path.len,
                .handler = @ptrCast(stableHandler(handler)),
            });
            if (!status.isOk()) return abi.errorFromStatus(status);
        }

        pub fn get(self: *const @This(), comptime path: []const u8, comptime handler: httpx.Handler) !void {
            try self.register(.get, path, handler);
        }

        pub fn post(self: *const @This(), comptime path: []const u8, comptime handler: httpx.Handler) !void {
            try self.register(.post, path, handler);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, comptime handler: httpx.Handler) !void {
            try self.register(.put, path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, comptime handler: httpx.Handler) !void {
            try self.register(.delete, path, handler);
        }
    };
}
