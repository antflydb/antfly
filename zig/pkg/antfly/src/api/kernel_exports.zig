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

const std = @import("std");
const ant_json = @import("antfly-json");
const abi = @import("kernel_abi.zig");
const server_mod = @import("http_server.zig");
const handler_mod = @import("httpx_handler.zig");
const distributed_txn_contract = @import("antfly_local_sources").api_distributed_txn_contract;
const table_reads = @import("antfly_local_sources").api_table_read_source;
const table_writes = @import("antfly_local_sources").api_table_write_source;
const restore_jobs = @import("restore_jobs.zig");
const managed_embedder = @import("antfly_local_sources").inference_managed_embedder;
const backend_erased = @import("antfly_local_sources").storage_backend_erased;
const hot_standby_http_operation = @import("../storage/hot_standby/http_operation.zig");
const httpx = @import("httpx");
const platform_sync = @import("antfly_platform").sync;
const runtime_http_bridge = @import("antfly_runtime_abi").http_bridge;
const metadata_api = @import("../metadata/api.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");
const metadata_server_openapi = @import("antfly_metadata_server_openapi");
const usermgr_server_openapi = @import("antfly_usermgr_server_openapi");
const runtime_io_abi = @import("antfly_runtime_abi").io_abi;

pub const CreateContext = abi.CreateContext;
pub const CallContext = abi.CallContext;
pub const HandlerCreateContext = abi.HandlerCreateContext;

const ServerState = struct {
    owner_alloc: std.mem.Allocator,
    server: server_mod.ApiHttpServer,
    request_alloc_abi: abi.memory_abi.Allocator,
    runtime_io: RuntimeIoReceivers = .{},
};

const RunnableOwnerState = @import("runnable_owner_execution.zig").State;

const RuntimeIoReceivers = struct {
    api: ?runtime_io_abi.Receiver = null,
    api_network: ?runtime_io_abi.Receiver = null,
    api_filesystem: ?runtime_io_abi.Receiver = null,
    durable: ?runtime_io_abi.Receiver = null,
    sql_execution: ?runtime_io_abi.Receiver = null,
    sql_control: ?runtime_io_abi.Receiver = null,
    sql_workers: ?abi.SqlWorkerFactory = null,

    fn init(self: *@This(), borrows: *const abi.RuntimeIoBorrows) !void {
        if (!abi.validContext(abi.RuntimeIoBorrows, borrows.version, borrows.struct_size))
            return error.UnsupportedVersion;
        inline for (.{ "api", "api_network", "api_filesystem", "durable", "sql_execution", "sql_control" }) |field| {
            @field(self, field) = if (@field(borrows, field)) |borrow| try borrow.receive() else null;
        }
        self.sql_workers = if (borrows.sql_workers) |producer| blk: {
            if (!abi.validContext(abi.SqlWorkerFactory, producer.version, producer.struct_size) or producer.shared_capacity < 10)
                return error.UnsupportedVersion;
            break :blk producer.*;
        } else null;
    }

    fn views(self: *@This()) server_mod.RuntimeIoViews {
        var result: server_mod.RuntimeIoViews = .{};
        inline for (.{ "api", "api_network", "api_filesystem", "durable", "sql_execution", "sql_control" }) |field| {
            @field(result, field) = if (@field(self, field)) |*receiver| receiver.io() else null;
        }
        result.sql_workers = self.sql_workers;
        return result;
    }
};

const HandlerState = struct {
    alloc: std.mem.Allocator,
    handler: handler_mod.AntflyApiHandler,
    routes: std.ArrayListUnmanaged(*RouteState) = .empty,
    route_manifest: std.ArrayListUnmanaged(abi.RouteManifestEntry) = .empty,
    route_validator: httpx.Router,
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

pub fn runnableOwnerCreate(context: *const abi.RunnableOwnerCreateContext) callconv(.c) abi.Status {
    if (validateContext(abi.RunnableOwnerCreateContext, context.abi_version, context.struct_size)) |failure| return failure;
    if (!context.owner_alloc.valid()) return fail(error.UnsupportedVersion);
    if (!context.policy_contract.matches(.of(server_mod.RunnableScheduler.Config))) return fail(error.InvalidArgument);
    const policy: *const server_mod.RunnableScheduler.Config = @ptrCast(@alignCast(context.policy));
    const owner = server_mod.RunnableScheduler.NodeOwner.init(policy.*) catch return fail(error.InvalidArgument);
    const alloc = context.owner_alloc.asStd();
    const state = alloc.create(RunnableOwnerState) catch |err| return fail(err);
    state.* = .{ .owner_alloc = alloc, .owner = owner, .native_now_ns = @import("antfly_platform").time.monotonicNs };
    context.out_handle.* = &state.owner;
    return .ok;
}

pub fn runnableOwnerExecutionPort(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, abi.runnable_abi.Owner, context)) |failure| return failure;
    const owner: *server_mod.RunnableScheduler.NodeOwner = @ptrCast(@alignCast(context.handle));
    const state: *RunnableOwnerState = @fieldParentPtr("owner", owner);
    output(abi.runnable_abi.Owner, context).* = state.port();
    return .ok;
}

pub fn runnableOwnerClose(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    const owner: *server_mod.RunnableScheduler.NodeOwner = @ptrCast(@alignCast(context.handle));
    owner.close();
    return .ok;
}

pub fn runnableOwnerDestroy(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    const owner: *server_mod.RunnableScheduler.NodeOwner = @ptrCast(@alignCast(context.handle));
    const state: *RunnableOwnerState = @fieldParentPtr("owner", owner);
    const alloc = state.owner_alloc;
    owner.tryDeinit() catch |err| return fail(err);
    alloc.destroy(state);
    return .ok;
}

test "runnable scheduler API kernel node owner rejects unsafe creation and destruction" {
    const allocator = std.testing.allocator;
    const allocator_abi = abi.memory_abi.Allocator.fromStd(&allocator);
    const policy: server_mod.RunnableScheduler.Config = .{ .slots = 2 };
    var handle: ?*anyopaque = null;
    var context: abi.RunnableOwnerCreateContext = .{
        .abi_version = abi.abi_version - 1,
        .owner_alloc = &allocator_abi,
        .policy = @ptrFromInt(8),
        .policy_contract = .of(void),
        .out_handle = &handle,
    };
    try std.testing.expectEqual(error.UnsupportedVersion, abi.errorFromStatus(runnableOwnerCreate(&context)));
    try std.testing.expect(handle == null);
    context.abi_version = abi.abi_version;
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(runnableOwnerCreate(&context)));
    try std.testing.expect(handle == null);
    context.policy = &policy;
    context.policy_contract = .of(server_mod.RunnableScheduler.Config);
    const invalid_policy: server_mod.RunnableScheduler.Config = .{ .slots = 1 };
    context.policy = &invalid_policy;
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(runnableOwnerCreate(&context)));
    try std.testing.expect(handle == null);
    context.policy = &policy;
    try std.testing.expect(runnableOwnerCreate(&context).isOk());
    const owner: *server_mod.RunnableScheduler.NodeOwner = @ptrCast(@alignCast(handle.?));
    const call: CallContext = .{ .abi_version = abi.abi_version, .handle = handle.? };
    // Reject the wrong output contract before touching an invalid output pointer.
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(runnableOwnerExecutionPort(&.{
        .abi_version = abi.abi_version,
        .handle = handle.?,
        .output = @ptrFromInt(8),
        .output_contract = .of(u64),
    })));
    var port: abi.runnable_abi.Owner = undefined;
    try std.testing.expect(runnableOwnerExecutionPort(&.{
        .abi_version = abi.abi_version,
        .handle = handle.?,
        .output = &port,
        .output_contract = .of(abi.runnable_abi.Owner),
    }).isOk());
    try std.testing.expect(port.valid());
    try std.testing.expectEqual(handle.?, port.context);
    try std.testing.expect(port.retain(port.context).isOk());
    try std.testing.expectEqual(error.InvalidRunnableTransition, abi.errorFromStatus(runnableOwnerDestroy(&call)));
    port.release(port.context);
    try owner.retain();
    try std.testing.expectEqual(error.InvalidRunnableTransition, abi.errorFromStatus(runnableOwnerDestroy(&call)));
    try std.testing.expect(!owner.closed);
    owner.release();
    var execution: server_mod.RunnableScheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner.scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    try std.testing.expectEqual(error.InvalidRunnableTransition, abi.errorFromStatus(runnableOwnerDestroy(&call)));
    try std.testing.expect(runnableOwnerClose(&call).isOk());
    try std.testing.expect(runnableOwnerClose(&call).isOk());
    try std.testing.expectError(error.SchedulerClosed, owner.retain());
    execution.finish();
    try std.testing.expect(runnableOwnerDestroy(&call).isOk());
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
    if (cfg.backend_runtime != null and context.runtime_io == null)
        return fail(error.InvalidArgument);
    const source: *const server_mod.StatusSource = @ptrCast(@alignCast(context.source));
    const reads: *const ?table_reads.TableReadSource = @ptrCast(@alignCast(context.table_reads));
    const writes: *const ?table_writes.TableWriteSource = @ptrCast(@alignCast(context.table_writes));
    const owner_alloc = context.owner_alloc.asStd();
    const state = owner_alloc.create(ServerState) catch |err| {
        std.log.err("API kernel create failed allocating state: error.{s}", .{@errorName(err)});
        return fail(err);
    };
    var published = false;
    defer if (!published) owner_alloc.destroy(state);

    state.owner_alloc = owner_alloc;
    state.runtime_io = .{};
    var imported_cfg = cfg.*;
    imported_cfg.imported_runtime_io = null;
    if (context.runtime_io) |borrows| {
        state.runtime_io.init(borrows) catch |err| return fail(err);
        imported_cfg.imported_runtime_io = state.runtime_io.views();
    }
    state.server = if (context.flags & CreateContext.fallible_init != 0)
        server_mod.ApiHttpServer.initWithConfig(owner_alloc, imported_cfg, source.*, reads.*, writes.*) catch |err| {
            std.log.err("API kernel create failed initializing server: error.{s}", .{@errorName(err)});
            return fail(err);
        }
    else
        server_mod.ApiHttpServer.initWithProcessRequestAllocator(owner_alloc, imported_cfg, source.*, reads.*, writes.*);
    state.server.startResourceCleanup() catch |err| {
        std.log.err("API memory cleanup startup failed: error.{s}", .{@errorName(err)});
        state.server.deinit();
        return fail(err);
    };
    state.server.startPgwire() catch |err| {
        std.log.err("API pgwire listener startup failed: error.{s}", .{@errorName(err)});
        state.server.deinit();
        return fail(err);
    };
    if (reads.*) |read_source| read_source.bindIncomingGraphRoutes(@ptrCast(&state.server.incoming_graph_routes));
    state.request_alloc_abi = .fromStd(&state.server.alloc);
    context.out_handle.* = state;
    context.out_request_alloc.* = &state.request_alloc_abi;
    published = true;
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

pub fn queryAdmissionStats(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, server_mod.RequestAdmission.Stats, context)) |failure| return failure;
    output(server_mod.RequestAdmission.Stats, context).* = serverState(context).server.queryAdmissionStats();
    return .ok;
}

pub fn writeAdmissionStats(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, server_mod.RequestAdmission.Stats, context)) |failure| return failure;
    output(server_mod.RequestAdmission.Stats, context).* = serverState(context).server.writeAdmissionStats();
    return .ok;
}

pub fn runnableSchedulingStats(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, abi.RunnableStats, context)) |failure| return failure;
    output(abi.RunnableStats, context).* = serverState(context).server.runnableSchedulingStats();
    return .ok;
}

pub fn inferenceAdmissionStats(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, server_mod.RequestAdmission.Stats, context)) |failure| return failure;
    output(server_mod.RequestAdmission.Stats, context).* = serverState(context).server.inferenceAdmissionStats();
    return .ok;
}

pub fn setProvider(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(?managed_embedder.AntflyProvider, void, context)) |failure| return failure;
    serverState(context).server.antfly_provider = input(?managed_embedder.AntflyProvider, context).*;
    return .ok;
}

pub fn setHotStandbyExecutor(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(?hot_standby_http_operation.Executor, void, context)) |failure| return failure;
    serverState(context).server.setHotStandbyInternalExecutor(input(?hot_standby_http_operation.Executor, context).*);
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
    if (validateVersion(abi_version)) |failure| {
        std.log.warn("restore job attachment API ABI mismatch: caller={d} owner={d}", .{ abi_version, abi.abi_version });
        return failure;
    }
    const persistence: *const restore_jobs.ReplicatedPersistence = @ptrCast(@alignCast(persistence_opaque));
    if (persistence.version != restore_jobs.ReplicatedPersistence.abi_version) {
        std.log.warn("restore job persistence ABI mismatch: provider={d} consumer={d}", .{ persistence.version, restore_jobs.ReplicatedPersistence.abi_version });
        return fail(error.UnsupportedVersion);
    }
    const state: *ServerState = @ptrCast(@alignCast(server_handle));
    state.server.attachReplicatedRestoreJobStore(persistence.*) catch |err| {
        const status = fail(err);
        std.log.warn("restore job persistence load failed after ABI validation: version={d} code={d} detail={d}", .{ persistence.version, status.code, status.detail });
        return status;
    };
    return .ok;
}

pub fn bootstrapRemoteAttempts(context: *const CallContext) callconv(.c) abi.Status {
    const Bootstrap = @import("remote_attempt_owner.zig").Bootstrap;
    if (validateCall(Bootstrap, void, context)) |failure| return failure;
    serverState(context).server.bootstrapRemoteAttempts(input(Bootstrap, context).*) catch |err| return fail(switch (err) {
        error.OutOfMemory, error.InvalidArgument, error.FileNotFound, error.UnsupportedVersion, error.Forbidden, error.Unavailable => err,
        error.InvalidRemoteMembership, error.RemoteMembershipChanged, error.RemoteWorkerIdentityChanged, error.InvalidRemoteReplySignature => error.Forbidden,
        error.TableTopologyProtocolUpgradeRequired => error.UnsupportedVersion,
        error.InvalidStoreRootEnrollment, error.StoreRootEnrollmentChanged, error.RemoteInitializationChanged => error.Forbidden,
        error.RemoteFenceStoreMissing => error.FileNotFound,
        error.RemoteFenceStoreBusy, error.RemoteFenceStoreAlreadyExists => error.ResourceTemporarilyUnavailable,
        else => error.Unavailable,
    });
    return .ok;
}

pub fn refreshRemoteAttemptPeers(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, void, context)) |failure| return failure;
    serverState(context).server.refreshRemoteAttemptPeers() catch |err| return fail(switch (err) {
        error.InvalidArgument, error.OutOfMemory, error.Unavailable, error.UnsupportedVersion => err,
        error.InvalidRemoteMembership, error.RemoteMembershipChanged, error.RemoteWorkerIdentityChanged => error.Forbidden,
        else => error.Unavailable,
    });
    return .ok;
}

pub fn pgwireBoundPort(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, u16, context)) |failure| return failure;
    const server = &serverState(context).server;
    output(u16, context).* = if (server.pgwire_listener) |listener| listener.server.address().getPort() else 0;
    return .ok;
}

pub fn remoteAttemptOwnerStats(context: *const CallContext) callconv(.c) abi.Status {
    const Stats = @import("remote_attempt_owner.zig").Stats;
    if (validateCall(void, Stats, context)) |failure| return failure;
    output(Stats, context).* = serverState(context).server.remoteAttemptOwnerStats() catch |err| return fail(err);
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

pub fn configureHttpTransport(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(httpx.ServerConfig, httpx.ServerConfig, context)) |failure| return failure;
    output(httpx.ServerConfig, context).* = serverState(context).server.configurePublicHttpTransport(input(httpx.ServerConfig, context).*);
    return .ok;
}

pub fn handlerCreate(context: *const HandlerCreateContext) callconv(.c) abi.Status {
    if (validateContext(HandlerCreateContext, context.abi_version, context.struct_size)) |failure| return failure;
    const api_state: *ServerState = @ptrCast(@alignCast(context.api_server_handle));
    const state = api_state.owner_alloc.create(HandlerState) catch |err| return fail(err);
    state.* = .{
        .alloc = api_state.owner_alloc,
        .handler = .{ .api_server = &api_state.server },
        .route_validator = httpx.Router.init(api_state.owner_alloc),
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
    return .ok;
}

pub fn handlerStats(context: *const CallContext) callconv(.c) abi.Status {
    if (validateCall(void, abi.HandlerStats, context)) |failure| return failure;
    const handler = &handlerState(context).handler;
    const query = handler.api_server.queryAdmissionStats();
    const write = handler.api_server.writeAdmissionStats();
    const inference = handler.api_server.inferenceAdmissionStats();
    const query_body = handler.query_body_admission.stats();
    const memory = handler.api_server.workloadMemoryStats();
    output(abi.HandlerStats, context).* = .{
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
    return .ok;
}

fn memoryAbi(stats: ?@import("../common/allocation_budget.zig").Shared.Stats) abi.WorkloadMemoryStats {
    if (stats) |value| return .{ .enabled = true, .limit_bytes = value.limit_bytes, .used_bytes = value.used_bytes, .peak_bytes = value.peak_bytes, .rejected_total = value.rejected_total };
    return .{ .enabled = false, .limit_bytes = 0, .used_bytes = 0, .peak_bytes = 0, .rejected_total = 0 };
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
            state.route_validator.deinit();
            state.route_validator = httpx.Router.init(state.alloc);
            return fail(err);
        };
        state.route_manifest_ready = true;
    }
    context.out_entries.* = if (state.route_manifest.items.len == 0) null else state.route_manifest.items.ptr;
    context.out_len.* = state.route_manifest.items.len;
    return .ok;
}

fn exportHttpResponse(
    alloc: std.mem.Allocator,
    response: httpx.Response,
    out_handle: *?*anyopaque,
    out_view: *abi.HttpResponseView,
) !void {
    const response_state = try alloc.create(HttpResponseState);
    errdefer alloc.destroy(response_state);
    const response_headers = response.headers.iterator();
    const header_views = try alloc.alloc(abi.HeaderView, response_headers.len);
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
    out_handle.* = response_state;
    out_view.* = .{
        .status = response.status.code,
        .content_type = abi.OptionalBytes.init(response.contentType()),
        .headers_ptr = if (header_views.len == 0) null else header_views.ptr,
        .headers_len = header_views.len,
        .body = abi.Bytes.init(response.body orelse ""),
    };
}

pub fn handlerAuthorizeInternalService(context: *const abi.InternalServiceAuthContext) callconv(.c) abi.Status {
    if (validateContext(abi.InternalServiceAuthContext, context.abi_version, context.struct_size)) |failure| return failure;
    if (context.deadline.present > 1) return fail(error.InvalidArgument);
    var executor = context.executor.receive() catch |err| return fail(err);
    const io = executor.io();
    const state: *HandlerState = @ptrCast(@alignCast(context.handler_handle));
    const request = context.request;
    const alloc = state.alloc;
    context.out_response_handle.* = null;
    context.out_legacy_accepted.* = 0;

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
        .patch => .PATCH,
    }, target) catch |err| return fail(err);
    defer http_request.deinit();
    const input_headers = if (request.headers_ptr) |ptr| ptr[0..request.headers_len] else &.{};
    for (input_headers) |header|
        http_request.headers.append(header.name.slice(), header.value.slice()) catch |err| return fail(err);

    var http_context = httpx.Context.init(alloc, io, &http_request);
    if (context.deadline.present != 0) {
        http_context.application_deadline_ns = context.deadline.timestamp_ns;
        http_context.application_deadline_io = io;
    }
    defer http_context.deinit();
    var legacy_accepted = false;
    if (state.handler.authorizeHostInternalServiceRoute(&http_context, &legacy_accepted) catch |err| return fail(err)) |response| {
        var owned_response = response;
        errdefer owned_response.deinit();
        exportHttpResponse(alloc, owned_response, context.out_response_handle, context.out_response) catch |err| return fail(err);
    } else if (legacy_accepted) {
        context.out_legacy_accepted.* = 1;
    }
    return .ok;
}

pub fn handlerHandleHttp(context: *const abi.HttpHandleContext) callconv(.c) abi.Status {
    if (validateContext(abi.HttpHandleContext, context.abi_version, context.struct_size)) |failure| return failure;
    if (context.deadline.present > 1) return fail(error.InvalidArgument);
    var executor = context.executor.receive() catch |err| return fail(err);
    const io = executor.io();
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
        .patch => .PATCH,
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

    var http_context = httpx.Context.init(alloc, io, &http_request);
    if (context.deadline.present != 0) {
        http_context.application_deadline_ns = context.deadline.timestamp_ns;
        http_context.application_deadline_io = io;
    }
    defer http_context.deinit();
    http_context.params = params;
    runtime_http_bridge.installInbound(&http_context, &context.cancellation, &context.body_source, &context.stream);
    var response = state.handler.dispatchLinkedRoute(&http_context, route.handler) catch |err| return fail(err);
    errdefer response.deinit();

    exportHttpResponse(alloc, response, context.out_response_handle, context.out_response) catch |err| return fail(err);
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
    state.route_validator.deinit();
    state.handler.deinitRuntime();
    alloc.destroy(state);
}

const function_table: abi.FunctionTable = .{
    .abi_version = abi.abi_version,
    .struct_size = @sizeOf(abi.FunctionTable),
    .capabilities = abi.Capability.core |
        abi.Capability.route_manifest |
        abi.Capability.inference_admission_stats |
        abi.Capability.internal_service_ingress |
        abi.Capability.node_runnable_owner |
        abi.Capability.node_runnable_execution |
        abi.Capability.remote_attempt_owner |
        abi.Capability.remote_peer_refresh |
        abi.Capability.pgwire_bound_port,
    .create = &create,
    .destroy = &destroy,
    .request_stats = &requestStats,
    .configure_http_transport = &configureHttpTransport,
    .query_admission_stats = &queryAdmissionStats,
    .runnable_scheduling_stats = &runnableSchedulingStats,
    .write_admission_stats = &writeAdmissionStats,
    .set_provider = &setProvider,
    .set_ha_executor = &setHotStandbyExecutor,
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
    .inference_admission_stats = &inferenceAdmissionStats,
    .handler_authorize_internal_service = &handlerAuthorizeInternalService,
    .runnable_owner_create = &runnableOwnerCreate,
    .runnable_owner_close = &runnableOwnerClose,
    .runnable_owner_destroy = &runnableOwnerDestroy,
    .runnable_owner_execution_port = &runnableOwnerExecutionPort,
    .bootstrap_remote_attempts = &bootstrapRemoteAttempts,
    .remote_attempt_owner_stats = &remoteAttemptOwnerStats,
    .refresh_remote_attempt_peers = &refreshRemoteAttemptPeers,
    .pgwire_bound_port = &pgwireBoundPort,
};

pub fn getFunctionTable() callconv(.c) *const abi.FunctionTable {
    return &function_table;
}

fn ManifestServer(comptime prefix: []const u8) type {
    return struct {
        owner: *HandlerState,

        fn register(self: *const @This(), method: abi.HttpMethod, comptime path: []const u8, handler: httpx.Handler) !void {
            try self.registerWithMetadata(method, path, handler, routeMetadata(method, path));
        }

        fn registerWithMetadata(self: *const @This(), method: abi.HttpMethod, comptime path: []const u8, handler: httpx.Handler, metadata: RouteMetadata) !void {
            self.owner.route_validator.add(switch (method) {
                .get => .GET,
                .post => .POST,
                .put => .PUT,
                .delete => .DELETE,
                .patch => .PATCH,
            }, prefix ++ path, handler) catch |err| {
                std.log.err("API kernel route manifest rejected method={s} path={s} err={}", .{
                    @tagName(method),
                    prefix ++ path,
                    err,
                });
                return err;
            };
            const route = try self.owner.alloc.create(RouteState);
            errdefer self.owner.alloc.destroy(route);
            route.* = .{
                .owner = self.owner,
                .handler = handler,
            };
            try self.owner.routes.append(self.owner.alloc, route);
            errdefer _ = self.owner.routes.pop();
            try self.owner.route_manifest.append(self.owner.alloc, .{
                .route_handle = route,
                .method = method,
                .path = abi.Bytes.init(prefix ++ path),
                .request_body = metadata.request_body,
                .streaming_response = @intFromBool(metadata.streaming_response),
            });
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.get, path, handler);
        }

        pub fn post(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.post, path, handler);
        }

        pub fn postWithBodyLimit(self: *const @This(), comptime path: []const u8, limit: usize, handler: httpx.Handler) !void {
            // The host applies this canonical policy when consuming the opaque
            // manifest. Reject an undeclared limit instead of dropping it.
            const expected = @import("http_routes.zig").postBodyLimit(prefix ++ path) orelse return error.InvalidArgument;
            if (limit != expected) return error.InvalidArgument;
            try self.register(.post, path, handler);
        }

        pub fn postResponseStreaming(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.registerWithMetadata(.post, path, handler, .{
                .request_body = .buffered,
                .streaming_response = true,
            });
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.put, path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.delete, path, handler);
        }

        pub fn patch(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            try self.register(.patch, path, handler);
        }
    };
}

const RouteMetadata = struct {
    request_body: abi.RequestBodyMode,
    streaming_response: bool,
};

fn routeMetadata(method: abi.HttpMethod, path: []const u8) RouteMetadata {
    const method_name = switch (method) {
        .get => "GET",
        .post => "POST",
        .put => "PUT",
        .delete => "DELETE",
        .patch => "PATCH",
    };
    inline for (.{ metadata_server_openapi.server.routes, usermgr_server_openapi.server.routes }) |routes| {
        for (routes) |route| {
            if (std.mem.eql(u8, route.method, method_name) and metadataPathMatches(route.path, path)) {
                return .{
                    .request_body = switch (route.request_body) {
                        .none => .none,
                        .buffered => .buffered,
                    },
                    .streaming_response = route.streaming_response,
                };
            }
        }
    }
    // Contextual routes predate generated metadata. Conservatively buffer
    // mutating methods; body-less operations remain correct and future manual
    // handlers cannot accidentally observe a partial H2 body.
    return .{
        .request_body = if (method == .get) .none else .buffered,
        .streaming_response = false,
    };
}

fn metadataPathMatches(metadata_path: []const u8, registered_path: []const u8) bool {
    var metadata_index: usize = 0;
    var registered_index: usize = 0;
    while (metadata_index < metadata_path.len and registered_index < registered_path.len) {
        if (metadata_path[metadata_index] == '{') {
            if (registered_path[registered_index] != ':') return false;
            const close = std.mem.indexOfScalarPos(u8, metadata_path, metadata_index + 1, '}') orelse return false;
            const name = metadata_path[metadata_index + 1 .. close];
            if (!std.mem.startsWith(u8, registered_path[registered_index + 1 ..], name)) return false;
            metadata_index = close + 1;
            registered_index += name.len + 1;
            continue;
        }
        if (metadata_path[metadata_index] != registered_path[registered_index]) return false;
        metadata_index += 1;
        registered_index += 1;
    }
    return metadata_index == metadata_path.len and registered_index == registered_path.len;
}

const KernelIngressTestStatus = struct {
    fn source(self: *@This()) server_mod.StatusSource {
        return .{ .ptr = self, .vtable = &.{ .status = status } };
    }

    fn status(_: *anyopaque) !metadata_api.MetadataStatus {
        return .{
            .metadata_group_id = 1,
            .metrics = .{},
            .projected_stores = 1,
        };
    }
};

const KernelIngressTestRoutes = struct {
    var mutation_calls: usize = 0;

    fn mutation(ctx: *httpx.Context) !httpx.Response {
        mutation_calls += 1;
        return ctx.text("unexpected mutation execution");
    }

    fn metadataNotLeader(_: *httpx.Context) !httpx.Response {
        return error.NotLeader;
    }
};

fn responseHeader(response: abi.HttpResponseView, name: []const u8) ?[]const u8 {
    const headers = if (response.headers_ptr) |ptr| ptr[0..response.headers_len] else return null;
    for (headers) |header| {
        if (std.ascii.eqlIgnoreCase(header.name.slice(), name)) return header.value.slice();
    }
    return null;
}

test "linked API route manifest preserves internal scan response streaming" {
    const alloc = std.testing.allocator;
    var status_source = KernelIngressTestStatus{};
    const api_server = try alloc.create(server_mod.ApiHttpServer);
    defer alloc.destroy(api_server);
    api_server.* = server_mod.ApiHttpServer.init(alloc, .{}, status_source.source(), null, null);
    defer api_server.deinit();
    var state = HandlerState{
        .alloc = alloc,
        .handler = .{ .api_server = api_server },
        .route_validator = httpx.Router.init(alloc),
    };
    defer {
        for (state.routes.items) |route| alloc.destroy(route);
        state.routes.deinit(alloc);
        state.route_manifest.deinit(alloc);
        state.route_validator.deinit();
    }
    var entries: ?[*]const abi.RouteManifestEntry = null;
    var len: usize = 0;
    const status = handlerRouteManifest(&.{
        .abi_version = abi.abi_version,
        .handler_handle = &state,
        .out_entries = &entries,
        .out_len = &len,
    });
    try std.testing.expect(status.isOk());
    const routes = @import("http_routes.zig").Routes;
    const table_prefix = routes.internal_groups_prefix ++ ":group_id/tables/:table_name";
    var found_scan = false;
    var found_query = false;
    var public_queries: usize = 0;
    for (entries.?[0..len]) |entry| {
        if (entry.method != .post) continue;
        inline for (.{ "/db/v1/query", "/db/v1/tables/:tableName/query", "/db/v1/databases/:databaseName/namespaces/:namespaceName/tables/:tableName/query" }) |path| {
            if (std.mem.eql(u8, entry.path.slice(), path)) {
                try std.testing.expectEqual(abi.RequestBodyMode.buffered, entry.request_body);
                try std.testing.expectEqual(@as(u8, 1), entry.streaming_response);
                public_queries += 1;
            }
        }
        if (std.mem.eql(u8, entry.path.slice(), table_prefix ++ routes.documents_suffix)) {
            try std.testing.expectEqual(abi.RequestBodyMode.buffered, entry.request_body);
            try std.testing.expectEqual(@as(u8, 1), entry.streaming_response);
            found_scan = true;
        }
        if (std.mem.eql(u8, entry.path.slice(), table_prefix ++ routes.query_suffix)) {
            try std.testing.expectEqual(abi.RequestBodyMode.buffered, entry.request_body);
            try std.testing.expectEqual(@as(u8, 0), entry.streaming_response);
            found_query = true;
        }
    }
    try std.testing.expect(found_scan and found_query);
    try std.testing.expectEqual(@as(usize, 3), public_queries);
}

test "linked API dispatch preserves kernel-owned ingress policy" {
    const alloc = std.testing.allocator;
    var status_source = KernelIngressTestStatus{};
    const api_server = try alloc.create(server_mod.ApiHttpServer);
    defer alloc.destroy(api_server);
    api_server.* = server_mod.ApiHttpServer.init(
        alloc,
        .{
            .hot_standby_failover_safe_mutations_only = true,
            .internal_service_secret = "kernel-ingress-test-internal-secret-v1",
            .internal_service_issuer = "kernel-ingress-test",
        },
        status_source.source(),
        null,
        null,
    );
    defer api_server.deinit();

    var state = HandlerState{
        .alloc = alloc,
        .handler = .{ .api_server = api_server },
        .route_validator = httpx.Router.init(alloc),
    };
    defer state.route_validator.deinit();
    const test_io = std.testing.io;

    const internal_request = abi.HttpRequestView{
        .method = .post,
        .path = abi.Bytes.init("/internal/v1/tables/docs"),
    };
    var internal_response_handle: ?*anyopaque = null;
    var internal_response: abi.HttpResponseView = undefined;
    var legacy_accepted: u8 = 0;
    const internal_status = handlerAuthorizeInternalService(&.{
        .abi_version = abi.abi_version,
        .handler_handle = &state,
        .request = &internal_request,
        .executor = .init(&test_io),
        .out_response_handle = &internal_response_handle,
        .out_response = &internal_response,
        .out_legacy_accepted = &legacy_accepted,
    });
    try std.testing.expect(internal_status.isOk());
    try std.testing.expectEqual(@as(u16, 401), internal_response.status);
    try std.testing.expectEqual(@as(u8, 0), legacy_accepted);
    handlerDestroyHttpResponse(internal_response_handle.?);

    api_server.cfg.internal_service_accept_legacy_unauthenticated = true;
    internal_response_handle = null;
    const migration_status = handlerAuthorizeInternalService(&.{
        .abi_version = abi.abi_version,
        .handler_handle = &state,
        .request = &internal_request,
        .executor = .init(&test_io),
        .out_response_handle = &internal_response_handle,
        .out_response = &internal_response,
        .out_legacy_accepted = &legacy_accepted,
    });
    try std.testing.expect(migration_status.isOk());
    try std.testing.expect(internal_response_handle == null);
    try std.testing.expectEqual(@as(u8, 1), legacy_accepted);
    api_server.cfg.internal_service_accept_legacy_unauthenticated = false;

    KernelIngressTestRoutes.mutation_calls = 0;
    var mutation_route = RouteState{
        .owner = &state,
        .handler = httpx.Handler.from(KernelIngressTestRoutes.mutation),
    };
    const mutation_request = abi.HttpRequestView{
        .method = .post,
        .path = abi.Bytes.init("/db/v1/tables/docs"),
        .body = abi.OptionalBytes.init("{}"),
    };
    var mutation_response_handle: ?*anyopaque = null;
    var mutation_response: abi.HttpResponseView = undefined;
    const mutation_status = handlerHandleHttp(&.{
        .abi_version = abi.abi_version,
        .route_handle = &mutation_route,
        .request = &mutation_request,
        .executor = .init(&test_io),
        .out_response_handle = &mutation_response_handle,
        .out_response = &mutation_response,
    });
    try std.testing.expect(mutation_status.isOk());
    defer handlerDestroyHttpResponse(mutation_response_handle.?);
    try std.testing.expectEqual(@as(u16, 503), mutation_response.status);
    try std.testing.expectEqual(@as(usize, 0), KernelIngressTestRoutes.mutation_calls);
    try std.testing.expect(std.mem.indexOf(u8, mutation_response.body.slice(), "ha_mutation_not_replicated") != null);

    // Linked dispatch starts the transaction deadline before policy work, but
    // malformed metadata must not let an unauthenticated caller distinguish
    // an internal route. Validation runs only after service authentication.
    api_server.cfg.hot_standby_failover_safe_mutations_only = false;
    const invalid_deadline_headers = [_]abi.HeaderView{.{
        .name = abi.Bytes.init(distributed_txn_contract.pre_decision_remaining_ms_header),
        .value = abi.Bytes.init("5001"),
    }};
    var invalid_deadline_route = RouteState{
        .owner = &state,
        .handler = httpx.Handler.from(KernelIngressTestRoutes.mutation),
    };
    const invalid_deadline_request = abi.HttpRequestView{
        .method = .post,
        .path = abi.Bytes.init("/internal/v1/groups/7/tables/docs/txn-begin"),
        .headers_ptr = invalid_deadline_headers[0..].ptr,
        .headers_len = invalid_deadline_headers.len,
        .body = abi.OptionalBytes.init("{}"),
    };
    var invalid_deadline_response_handle: ?*anyopaque = null;
    var invalid_deadline_response: abi.HttpResponseView = undefined;
    const unauthenticated_deadline_status = handlerHandleHttp(&.{
        .abi_version = abi.abi_version,
        .route_handle = &invalid_deadline_route,
        .request = &invalid_deadline_request,
        .executor = .init(&test_io),
        .out_response_handle = &invalid_deadline_response_handle,
        .out_response = &invalid_deadline_response,
    });
    try std.testing.expect(unauthenticated_deadline_status.isOk());
    try std.testing.expectEqual(@as(u16, 401), invalid_deadline_response.status);
    handlerDestroyHttpResponse(invalid_deadline_response_handle.?);

    api_server.cfg.internal_service_accept_legacy_unauthenticated = true;
    invalid_deadline_response_handle = null;
    const authenticated_deadline_status = handlerHandleHttp(&.{
        .abi_version = abi.abi_version,
        .route_handle = &invalid_deadline_route,
        .request = &invalid_deadline_request,
        .executor = .init(&test_io),
        .out_response_handle = &invalid_deadline_response_handle,
        .out_response = &invalid_deadline_response,
    });
    try std.testing.expect(authenticated_deadline_status.isOk());
    try std.testing.expectEqual(@as(u16, 400), invalid_deadline_response.status);
    try std.testing.expectEqual(@as(usize, 0), KernelIngressTestRoutes.mutation_calls);
    handlerDestroyHttpResponse(invalid_deadline_response_handle.?);
    api_server.cfg.internal_service_accept_legacy_unauthenticated = false;

    var retry_route = RouteState{
        .owner = &state,
        .handler = httpx.Handler.from(KernelIngressTestRoutes.metadataNotLeader),
    };
    const retry_request = abi.HttpRequestView{
        .method = .get,
        .path = abi.Bytes.init("/db/v1/tables/docs"),
        .body = abi.OptionalBytes.init(""),
    };
    var retry_response_handle: ?*anyopaque = null;
    var retry_response: abi.HttpResponseView = undefined;
    const retry_status = handlerHandleHttp(&.{
        .abi_version = abi.abi_version,
        .route_handle = &retry_route,
        .request = &retry_request,
        .executor = .init(&test_io),
        .out_response_handle = &retry_response_handle,
        .out_response = &retry_response,
    });
    try std.testing.expect(retry_status.isOk());
    defer handlerDestroyHttpResponse(retry_response_handle.?);
    try std.testing.expectEqual(@as(u16, 503), retry_response.status);
    try std.testing.expectEqualStrings("1", responseHeader(retry_response, "Retry-After").?);
    try std.testing.expectEqualStrings("true", responseHeader(retry_response, "X-Antfly-Metadata-Not-Leader").?);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"code\":\"metadata_leader_unavailable\",\"error\":\"metadata leader unavailable\",\"message\":\"metadata leader unavailable\",\"retryable\":true,\"retry_after_ms\":1000}",
        retry_response.body.slice(),
    );
    try std.testing.expectEqual(@as(u64, 6), api_server.requestStats().request_count);
}

test "API kernel create rejects raw owner runtime without transferred capabilities" {
    const background = @import("antfly_local_sources").storage_background_runtime;
    var runtime = try background.BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .borrowed_io = .{ .general = std.testing.io },
    });
    defer runtime.deinit();
    const cfg: server_mod.ApiHttpServerConfig = .{ .backend_runtime = runtime.ptr() };
    var token: u8 = 0;
    const Stub = struct {
        fn status(_: *anyopaque) anyerror!metadata_api.MetadataStatus {
            return error.UnsupportedOperation;
        }
    };
    const source: server_mod.StatusSource = .{ .ptr = &token, .vtable = &.{ .status = Stub.status } };
    const reads: ?table_reads.TableReadSource = null;
    const writes: ?table_writes.TableWriteSource = null;
    var allocator = std.testing.allocator;
    const owner_alloc = abi.memory_abi.Allocator.fromStd(&allocator);
    var handle: ?*anyopaque = null;
    var request_alloc: ?*const abi.memory_abi.Allocator = null;
    var context: CreateContext = .{
        .abi_version = abi.abi_version,
        .owner_alloc = &owner_alloc,
        .cfg = &cfg,
        .cfg_contract = .of(server_mod.ApiHttpServerConfig),
        .source = &source,
        .source_contract = .of(server_mod.StatusSource),
        .table_reads = &reads,
        .table_reads_contract = .of(?table_reads.TableReadSource),
        .table_writes = &writes,
        .table_writes_contract = .of(?table_writes.TableWriteSource),
        .out_handle = &handle,
        .out_request_alloc = &request_alloc,
    };
    const result = create(&context);
    defer if (handle) |created| destroy(created);
    std.debug.print("API_RUNTIME_IO_RAW_REJECT expects InvalidArgument\n", .{});
    try std.testing.expect(!result.isOk());
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(result));
    try std.testing.expect(handle == null);
    try std.testing.expect(request_alloc == null);
}

test "API kernel failed fallible create releases unpublished state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(std.testing.io, "not-a-directory", .{});
    file.close(std.testing.io);
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/not-a-directory/store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    // Listener startup can fail after server initialization too. Both paths
    // must release the unpublished API owner and leave outputs untouched.
    for ([_]server_mod.ApiHttpServerConfig{
        .{ .session_store_path = path },
        .{ .pgwire = .{ .enabled = true } },
    }) |cfg| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var accounting = std.testing.FailingAllocator.init(arena.allocator(), .{});
        var allocator = accounting.allocator();
        const owner_alloc = abi.memory_abi.Allocator.fromStd(&allocator);
        var token: u8 = 0;
        const Stub = struct {
            fn status(_: *anyopaque) anyerror!metadata_api.MetadataStatus {
                return error.UnsupportedOperation;
            }
        };
        const source: server_mod.StatusSource = .{ .ptr = &token, .vtable = &.{ .status = Stub.status } };
        const reads: ?table_reads.TableReadSource = null;
        const writes: ?table_writes.TableWriteSource = null;
        var handle: ?*anyopaque = null;
        var request_alloc: ?*const abi.memory_abi.Allocator = null;
        @import("antfly_test_error_logs").expectErrorLogs(1);
        const result = create(&.{
            .abi_version = abi.abi_version,
            .owner_alloc = &owner_alloc,
            .cfg = &cfg,
            .cfg_contract = .of(server_mod.ApiHttpServerConfig),
            .source = &source,
            .source_contract = .of(server_mod.StatusSource),
            .table_reads = &reads,
            .table_reads_contract = .of(?table_reads.TableReadSource),
            .table_writes = &writes,
            .table_writes_contract = .of(?table_writes.TableWriteSource),
            .flags = CreateContext.fallible_init,
            .out_handle = &handle,
            .out_request_alloc = &request_alloc,
        });
        defer if (handle) |created| destroy(created);
        try std.testing.expect(!result.isOk());
        try std.testing.expect(handle == null);
        try std.testing.expect(request_alloc == null);
        try std.testing.expect(accounting.allocated_bytes > 0);
        std.debug.print("API_CREATE_CLEANUP expects all owner bytes freed\n", .{});
        try std.testing.expectEqual(accounting.allocated_bytes, accounting.freed_bytes);
    }
}

test "API kernel runtime I/O receivers validate capability layout and domains" {
    var receivers: RuntimeIoReceivers = .{};
    var borrow = abi.native_abi.IoBorrow.init(&std.testing.io);
    var borrows: abi.RuntimeIoBorrows = .{
        .api = &borrow,
        .api_network = &borrow,
        .api_filesystem = &borrow,
        .durable = &borrow,
        .sql_execution = &borrow,
        .sql_control = &borrow,
    };
    try receivers.init(&borrows);
    const views = receivers.views();
    inline for (.{ "api", "api_network", "api_filesystem", "durable", "sql_execution", "sql_control" }) |field| {
        const io = @field(views, field).?;
        try std.testing.expect(io.userdata == std.testing.io.userdata);
        try std.testing.expect(io.vtable == std.testing.io.vtable);
    }
    borrows.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, receivers.init(&borrows));
    borrows.version = abi.abi_version;
    borrows.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, receivers.init(&borrows));
    borrows.struct_size = @sizeOf(abi.RuntimeIoBorrows);
    borrow.contract.version +%= 1;
    try std.testing.expectError(error.InvalidArgument, receivers.init(&borrows));
}

test "API kernel runtime I/O receiver keeps imported unavailable views null" {
    var receivers: RuntimeIoReceivers = .{};
    try receivers.init(&.{});
    const views = receivers.views();
    inline for (.{ "api", "api_network", "api_filesystem", "durable", "sql_execution", "sql_control" }) |field| {
        try std.testing.expect(@field(views, field) == null);
    }
}

test "API kernel create enforces owner I/O capabilities and preserves their lifetime" {
    const background = @import("antfly_local_sources").storage_background_runtime;
    var runtime = try background.BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .borrowed_io = .{ .general = std.testing.io },
    });
    defer runtime.deinit();
    const cfg: server_mod.ApiHttpServerConfig = .{ .backend_runtime = runtime.ptr() };
    var token: u8 = 0;
    const Stub = struct {
        fn status(_: *anyopaque) anyerror!metadata_api.MetadataStatus {
            return error.UnsupportedOperation;
        }
    };
    const source: server_mod.StatusSource = .{ .ptr = &token, .vtable = &.{ .status = Stub.status } };
    const reads: ?table_reads.TableReadSource = null;
    const writes: ?table_writes.TableWriteSource = null;
    var allocator = std.testing.allocator;
    const owner_alloc = abi.memory_abi.Allocator.fromStd(&allocator);
    var handle: ?*anyopaque = null;
    var request_alloc: ?*const abi.memory_abi.Allocator = null;
    var context: CreateContext = .{
        .abi_version = abi.abi_version,
        .owner_alloc = &owner_alloc,
        .cfg = &cfg,
        .cfg_contract = .of(server_mod.ApiHttpServerConfig),
        .source = &source,
        .source_contract = .of(server_mod.StatusSource),
        .table_reads = &reads,
        .table_reads_contract = .of(?table_reads.TableReadSource),
        .table_writes = &writes,
        .table_writes_contract = .of(?table_writes.TableWriteSource),
        .out_handle = &handle,
        .out_request_alloc = &request_alloc,
    };
    const result = create(&context);
    defer if (handle) |created| destroy(created);
    try std.testing.expect(!result.isOk());
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(result));
    try std.testing.expect(handle == null);
    try std.testing.expect(request_alloc == null);
    var borrow = abi.native_abi.IoBorrow.init(&std.testing.io);
    const borrows: abi.RuntimeIoBorrows = .{
        .api = &borrow,
        .api_network = &borrow,
        .api_filesystem = &borrow,
        .durable = &borrow,
        .sql_execution = &borrow,
        .sql_control = &borrow,
    };
    context.runtime_io = &borrows;
    borrow.contract.version +%= 1;
    const malformed = create(&context);
    try std.testing.expect(!malformed.isOk());
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(malformed));
    try std.testing.expect(handle == null);
    borrow = abi.native_abi.IoBorrow.init(&std.testing.io);
    const Dispatch = @FieldType(abi.native_abi.IoBorrow, "dispatch");
    const Parameters = @typeInfo(@typeInfo(Dispatch).pointer.child).@"fn".param_types;
    const Forward = struct {
        var owner_dispatch: Dispatch = undefined;

        fn dispatch(
            owner: Parameters[0].?,
            operation: Parameters[1].?,
            arguments: Parameters[2].?,
            out_value: Parameters[3].?,
            reader: Parameters[4].?,
            error_names: Parameters[5].?,
        ) callconv(.c) void {
            owner_dispatch(owner, operation, arguments, out_value, reader, error_names);
        }
    };
    Forward.owner_dispatch = borrow.dispatch;
    borrow.dispatch = &Forward.dispatch;
    const Probe = struct {
        fn run(io: std.Io) !void {
            try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().statFile(io, "/antfly-api-owner-borrow-missing", .{}));
        }
    };
    for ([_]u32{ 0, CreateContext.fallible_init }) |flags| {
        context.flags = flags;
        try std.testing.expect(create(&context).isOk());
        const state: *ServerState = @ptrCast(@alignCast(handle.?));
        try std.testing.expect(state.server.cfg.imported_runtime_io != null);
        const views = state.server.cfg.imported_runtime_io.?;
        inline for (.{ "api", "api_network", "api_filesystem", "durable", "sql_execution", "sql_control" }) |field| {
            const view = @field(views, field).?;
            try std.testing.expect(view.userdata == @as(?*anyopaque, &@field(state.runtime_io, field).?));
            try std.testing.expect(view.vtable != std.testing.io.vtable);
        }
        const imported = state.server.sharedApiFilesystemIo().?;
        var future = try state.server.sharedApiIo().?.concurrent(Probe.run, .{imported});
        try future.await(state.server.sharedApiIo().?);
        destroy(handle.?);
        handle = null;
        try Probe.run(std.testing.io);
    }
}

test "API kernel create keeps SQL lanes owner-produced through saturation and shutdown" {
    const background = @import("antfly_local_sources").storage_background_runtime;
    const worker = @import("sql_worker_lease.zig");
    var runtime = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 32 } });
    defer runtime.deinit();
    const cfg: server_mod.ApiHttpServerConfig = .{
        .backend_runtime = runtime.ptr(),
        .runnable_policy = .{ .slots = 2, .max_operations = 8 },
        .query_max_concurrent_requests = 4,
        .write_max_concurrent_requests = 1,
        .query_waiting = .{ .max_wait_ms = 100, .max_queued_requests = 2, .max_queued_bytes = 1024 * 1024 },
    };
    var token: u8 = 0;
    const Stub = struct {
        fn status(_: *anyopaque) anyerror!metadata_api.MetadataStatus {
            return error.UnsupportedOperation;
        }
        fn blocked(io: std.Io, gate: *std.Io.Event) void {
            gate.wait(io) catch {};
        }
        fn probe(io: std.Io) anyerror!void {
            try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().statFile(io, "/antfly-kernel-sql-lane-missing", .{}));
        }
    };
    const source: server_mod.StatusSource = .{ .ptr = &token, .vtable = &.{ .status = Stub.status } };
    const reads: ?table_reads.TableReadSource = null;
    const writes: ?table_writes.TableWriteSource = null;
    var alloc = std.testing.allocator;
    const owner_alloc = abi.memory_abi.Allocator.fromStd(&alloc);
    var handle: ?*anyopaque = null;
    var request_alloc: ?*const abi.memory_abi.Allocator = null;
    var general = abi.native_abi.IoBorrow.init(&std.testing.io);
    const producer = worker.factory(runtime.ptr()).?;
    const borrows: abi.RuntimeIoBorrows = .{ .api = &general, .api_network = &general, .api_filesystem = &general, .durable = &general, .sql_execution = &general, .sql_control = &general, .sql_workers = &producer };
    try std.testing.expect(create(&.{
        .abi_version = abi.abi_version,
        .owner_alloc = &owner_alloc,
        .cfg = &cfg,
        .cfg_contract = .of(server_mod.ApiHttpServerConfig),
        .runtime_io = &borrows,
        .source = &source,
        .source_contract = .of(server_mod.StatusSource),
        .table_reads = &reads,
        .table_reads_contract = .of(?table_reads.TableReadSource),
        .table_writes = &writes,
        .table_writes_contract = .of(?table_writes.TableWriteSource),
        .flags = CreateContext.fallible_init,
        .out_handle = &handle,
        .out_request_alloc = &request_alloc,
    }).isOk());
    defer if (handle) |active| destroy(active);
    const state: *ServerState = @ptrCast(@alignCast(handle.?));
    // Explicit imported SQL capabilities must take precedence over raw runtime
    // state. Restore the pointer before unrelated shutdown paths use it.
    state.server.cfg.backend_runtime = @ptrFromInt(0x1000);
    defer if (handle != null) {
        state.server.cfg.backend_runtime = runtime.ptr();
    };
    const control = try state.server.sqlControlIo();
    const foreground = try state.server.sqlExecutionIo();
    const cleanup_io = try state.server.sqlCleanupIo();
    try std.testing.expect(state.server.sqlQueryWaitingEnabled());
    const reaper = try state.server.sqlCleanupOwner();
    state.server.cfg.backend_runtime = runtime.ptr();
    try std.testing.expect(reaper == state.server.sql_cleanup_runtime.?);
    try std.testing.expect(state.server.sql_workers.?.native == null);
    try std.testing.expect(state.server.sql_control_workers.?.native == null);
    try std.testing.expect(state.server.sql_cleanup_workers.?.native == null);
    try std.testing.expect(state.server.sql_cleanup_reaper_workers.?.native == null);
    try std.testing.expect(foreground.userdata != cleanup_io.userdata and foreground.userdata != control.userdata);
    try std.testing.expectEqual(@as(usize, 26), runtime.ptr().laneStats().reserved_workers);
    var foreground_gate: std.Io.Event = .unset;
    var cleanup_gate: std.Io.Event = .unset;
    var foreground_jobs: std.Io.Group = .init;
    var foreground_open = true;
    defer if (foreground_open) {
        foreground_gate.set(std.testing.io);
        foreground_jobs.await(foreground) catch {};
    };
    var cleanup_jobs: std.Io.Group = .init;
    var cleanup_open = true;
    defer if (cleanup_open) {
        cleanup_gate.set(std.testing.io);
        cleanup_jobs.await(cleanup_io) catch {};
    };
    for (0..state.server.sql_workers.?.capacity) |_| try foreground_jobs.concurrent(foreground, Stub.blocked, .{ foreground, &foreground_gate });
    for (0..state.server.sql_cleanup_workers.?.capacity) |_| try cleanup_jobs.concurrent(cleanup_io, Stub.blocked, .{ cleanup_io, &cleanup_gate });
    try std.testing.expectError(error.ConcurrencyUnavailable, foreground.concurrent(Stub.probe, .{foreground}));
    try std.testing.expectError(error.ConcurrencyUnavailable, cleanup_io.concurrent(Stub.probe, .{cleanup_io}));
    var control_job = try control.concurrent(Stub.probe, .{control});
    try control_job.await(control);
    foreground_gate.set(std.testing.io);
    foreground_jobs.await(foreground) catch {};
    foreground_open = false;
    cleanup_gate.set(std.testing.io);
    cleanup_jobs.await(cleanup_io) catch {};
    cleanup_open = false;
    destroy(handle.?);
    handle = null;
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
}

test "API kernel HTTP original deadline reaches reconstructed request context" {
    const alloc = std.testing.allocator;
    var status_source: KernelIngressTestStatus = .{};
    const server = try alloc.create(server_mod.ApiHttpServer);
    defer alloc.destroy(server);
    server.* = server_mod.ApiHttpServer.init(alloc, .{}, status_source.source(), null, null);
    defer server.deinit();
    var state: HandlerState = .{ .alloc = alloc, .handler = .{ .api_server = server }, .route_validator = httpx.Router.init(alloc) };
    defer state.route_validator.deinit();
    const Probe = struct {
        expected: ?u64,
        calls: usize = 0,
        fn handle(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            try std.testing.expectEqual(self.expected, ctx.application_deadline_ns);
            if (self.expected != null) {
                try std.testing.expect(ctx.application_deadline_io != null);
                try std.testing.expect(ctx.application_deadline_io.?.userdata == ctx.io.userdata);
                try std.testing.expect(ctx.application_deadline_io.?.vtable == ctx.io.vtable);
            }
            self.calls += 1;
            return httpx.Response.init(ctx.allocator, 200);
        }
    };
    const request: abi.HttpRequestView = .{ .method = .get, .path = abi.Bytes.init("/deadline-probe") };
    const io = std.testing.io;
    for ([_]?u64{ null, 0, 1, 500 * std.time.ns_per_ms }) |deadline| {
        var probe: Probe = .{ .expected = deadline };
        var route: RouteState = .{ .owner = &state, .handler = httpx.Handler.bind(&probe, Probe.handle) };
        var response_handle: ?*anyopaque = null;
        var response: abi.HttpResponseView = undefined;
        const result = handlerHandleHttp(&.{ .abi_version = abi.abi_version, .route_handle = &route, .request = &request, .executor = .init(&io), .out_response_handle = &response_handle, .out_response = &response, .deadline = .{ .present = @intFromBool(deadline != null), .timestamp_ns = deadline orelse 0 } });
        if (response_handle) |handle| {
            defer handlerDestroyHttpResponse(handle);
            try std.testing.expect(result.isOk());
            try std.testing.expectEqual(@as(u16, 200), response.status);
        } else try std.testing.expect(result.isOk());
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
    }
}

test "API kernel HTTP original deadline validates presence before invoking opaque owners" {
    const io = std.testing.io;
    var response_handle: ?*anyopaque = null;
    var response: abi.HttpResponseView = undefined;
    var legacy: u8 = 0;
    const request: abi.HttpRequestView = .{ .method = .get, .path = abi.Bytes.init("/") };
    const opaque_owner: *anyopaque = @ptrFromInt(8);
    const route_result = handlerHandleHttp(&.{ .abi_version = abi.abi_version, .route_handle = opaque_owner, .request = &request, .executor = .init(&io), .out_response_handle = &response_handle, .out_response = &response, .deadline = .{ .present = 2 } });
    const auth_result = handlerAuthorizeInternalService(&.{ .abi_version = abi.abi_version, .handler_handle = opaque_owner, .request = &request, .executor = .init(&io), .out_response_handle = &response_handle, .out_response = &response, .out_legacy_accepted = &legacy, .deadline = .{ .present = 2 } });
    try std.testing.expect(abi.errorFromStatus(route_result) == error.InvalidArgument);
    try std.testing.expect(abi.errorFromStatus(auth_result) == error.InvalidArgument);
    try std.testing.expect(response_handle == null);
}

test "runnable metrics service API export rejects older and truncated outputs before touching a poison owner" {
    const Legacy = extern struct { bytes: [@offsetOf(abi.RunnableStats, "service")]u8 };
    var legacy: Legacy = .{ .bytes = @splat(0xa5) };
    var context: CallContext = .{ .abi_version = 49, .handle = @ptrFromInt(1), .output = &legacy, .output_contract = .of(Legacy) };
    try std.testing.expectEqual(error.UnsupportedVersion, abi.errorFromStatus(runnableSchedulingStats(&context)));
    context.abi_version = abi.abi_version;
    try std.testing.expectEqual(error.InvalidArgument, abi.errorFromStatus(runnableSchedulingStats(&context)));
    context.output_contract = .of(abi.RunnableStats);
    context.struct_size -= 1;
    try std.testing.expectEqual(error.UnsupportedVersion, abi.errorFromStatus(runnableSchedulingStats(&context)));
    for (legacy.bytes) |byte| try std.testing.expectEqual(@as(u8, 0xa5), byte);
}

test "runnable metrics service API export projects the actual server owner counters" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var state: ServerState = undefined;
    state.server.runnable_owner = null;
    state.server.runnable_scheduler = try server_mod.RunnableScheduler.Scheduler.init(.{ .slots = 2, .quantum_ms = 1 });
    const scheduler = &state.server.runnable_scheduler.?;
    var execution: server_mod.RunnableScheduler.Execution = .{};
    defer execution.finish();
    try execution.start(scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    Clock.tick = 5 * std.time.ns_per_ms;
    execution.finish();
    var out: abi.RunnableStats = undefined;
    const context: CallContext = .{ .abi_version = abi.abi_version, .handle = &state, .output = &out, .output_contract = .of(abi.RunnableStats) };
    try std.testing.expect(runnableSchedulingStats(&context).isOk());
    try std.testing.expect(out.enabled and out.service.available);
    try std.testing.expectEqual(@as(u64, 5_000_000), out.service.observed_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 4_000_000), out.service.debt_slot_ns[0]);
    try std.testing.expectEqual(@as(u64, 0), out.operations);
}
