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
const httpx = @import("httpx");
const platform_sync = @import("antfly_platform").sync;
const metadata_mod = @import("domain.zig");
const metadata_authority = @import("authority.zig");
const service = @import("service.zig");
const transition_state = @import("transition_state.zig");
const metadata_storage = @import("storage/mod.zig");
const metadata_http_server = @import("http_server.zig");
const public_api_http_server = @import("../api/http_server.zig");
const public_api_kernel = @import("../api/kernel_bridge.zig");
const api_table_catalog = @import("../api/table_catalog.zig");
const api_table_reads = @import("../api/table_reads.zig");
const api_table_router = @import("../api/table_router.zig");
const api_table_writes = @import("../api/table_writes.zig");
const restore_jobs = @import("../api/restore_jobs.zig");
const raft = @import("../raft/mod.zig");
const raft_host = @import("../raft/host.zig");
const raft_managed_host = @import("../raft/managed_host.zig");
const raft_hosted_shard_ops = @import("../raft/hosted_shard_ops.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const raft_shard_ops = @import("../raft/shard_ops.zig");
const raft_transport = @import("../raft/transport/mod.zig");
const runtime_lifecycle = @import("../common/runtime_lifecycle.zig");

pub const MetadataServerConfig = struct {
    http: raft_managed_host.ManagedHttpHostConfig,
    service: service.MetadataServiceConfig = .{},
    admin_listener: ?raft_transport.StdHttpListenerConfig = null,
    api_server_cfg: public_api_http_server.ApiHttpServerConfig = .{},
    reconciler_config: metadata_mod.Reconciler.Config = .{},
};

pub const MetadataServerDeps = struct {
    http: service.MetadataHttpServiceDeps = .{},
};

pub const MetadataServer = struct {
    alloc: std.mem.Allocator,
    svc: *service.MetadataHttpService,
    control_loop: metadata_mod.MetadataControlLoop,
    transition_ops_registration: ?raft_shard_ops.OwnedShardOperationAdapter.Registration = null,
    owned_hosted_shard_ops: ?*raft_hosted_shard_ops.HostedShardOperationAdapter = null,
    owned_hosted_shard_db: ?*raft_hosted_shard_ops.HostedShardDbAdapter = null,
    owned_admin_http_server: ?*metadata_http_server.MetadataHttpServer = null,
    owned_public_read_source: ?*api_table_reads.HostedProvisionedTableReadSource = null,
    owned_public_write_source: ?*api_table_writes.HostedProvisionedTableWriteSource = null,
    owned_public_http_server: ?*public_api_kernel.ApiHttpServer = null,
    owned_admin_mux: ?*MetadataAdminMux = null,
    owned_admin_listener: ?*MetadataAdminHttpRuntime = null,
    restore_supervisor_owner_id: u64 = 0,
    restore_supervisor_stop: std.atomic.Value(bool) = .init(false),

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: MetadataServerConfig,
        deps: MetadataServerDeps,
    ) !MetadataServer {
        const svc = try alloc.create(service.MetadataHttpService);
        errdefer alloc.destroy(svc);
        svc.* = try service.MetadataHttpService.init(alloc, cfg.http, deps.http, cfg.service);
        errdefer svc.deinit();

        var owned_hosted_shard_ops: ?*raft_hosted_shard_ops.HostedShardOperationAdapter = null;
        errdefer if (owned_hosted_shard_ops) |ops| alloc.destroy(ops);
        var owned_hosted_shard_db: ?*raft_hosted_shard_ops.HostedShardDbAdapter = null;
        errdefer if (owned_hosted_shard_db) |adapter| alloc.destroy(adapter);
        var transition_ops_registration: ?raft_shard_ops.OwnedShardOperationAdapter.Registration = null;
        errdefer if (transition_ops_registration) |*registration| registration.deinit();

        if (deps.http.raft.transition_ops == null) {
            const local_ops = metadataLocalShardOperationAdapter(svc);
            const hosted_ops = try alloc.create(raft_hosted_shard_ops.HostedShardOperationAdapter);
            hosted_ops.* = raft_hosted_shard_ops.HostedShardOperationAdapter.initWithRouters(
                alloc,
                api_table_catalog.CatalogSource.fromMetadataHttpService(svc),
                metadataStoreGroupRouter(svc),
                metadataDataBearingStoreGroupRouter(svc),
                svc.raft.host.http_host.request_executor,
                .{
                    .ptr = svc,
                    .readiness = metadataGroupTransitionReadiness,
                },
                local_ops,
            );
            transition_ops_registration = try svc.raft.replaceTransitionOps(hosted_ops.adapter());
            owned_hosted_shard_ops = hosted_ops;
        }
        {
            const local_db = metadataLocalShardDbAdapter(svc);
            const hosted_db = try alloc.create(raft_hosted_shard_ops.HostedShardDbAdapter);
            hosted_db.* = raft_hosted_shard_ops.HostedShardDbAdapter.init(
                alloc,
                api_table_catalog.CatalogSource.fromMetadataHttpService(svc),
                metadataDataBearingStoreGroupRouter(svc),
                svc.raft.host.http_host.request_executor,
                local_db,
            );
            svc.setRoutedShardDbAdapter(hosted_db.adapter());
            owned_hosted_shard_db = hosted_db;
        }

        var owned_admin_http_server: ?*metadata_http_server.MetadataHttpServer = null;
        errdefer if (owned_admin_http_server) |admin_http_server| alloc.destroy(admin_http_server);
        var owned_public_read_source: ?*api_table_reads.HostedProvisionedTableReadSource = null;
        errdefer if (owned_public_read_source) |read_source| alloc.destroy(read_source);
        var owned_public_write_source: ?*api_table_writes.HostedProvisionedTableWriteSource = null;
        errdefer if (owned_public_write_source) |write_source| alloc.destroy(write_source);
        var owned_public_http_server: ?*public_api_kernel.ApiHttpServer = null;
        errdefer if (owned_public_http_server) |public_http_server| {
            public_http_server.deinit();
            alloc.destroy(public_http_server);
        };
        var owned_admin_mux: ?*MetadataAdminMux = null;
        errdefer if (owned_admin_mux) |mux| alloc.destroy(mux);
        var owned_admin_listener: ?*MetadataAdminHttpRuntime = null;
        errdefer if (owned_admin_listener) |listener| {
            listener.deinit();
        };

        if (cfg.admin_listener) |listener_cfg| {
            // The request allocator must share identity with the process
            // allocator used by DB internals (c_allocator when libc is
            // linked): request-scoped results adopt buffers allocated by
            // DB-owned components (index metadata, table record clones), so a
            // distinct allocator identity here turns those adoptions into
            // cross-allocator frees that corrupt the heap.
            const admin_http_server = try alloc.create(metadata_http_server.MetadataHttpServer);
            admin_http_server.* = metadata_http_server.MetadataHttpServer.init(
                alloc,
                .{},
                metadata_http_server.AdminSource.fromMetadataHttpService(svc),
            );
            owned_admin_http_server = admin_http_server;

            const catalog = api_table_catalog.CatalogSource.fromMetadataHttpService(svc);
            const data_router = metadataDataBearingStoreGroupRouter(svc);
            const replica_root_dir = svc.replica_root_dir orelse "";

            const public_read_source = try alloc.create(api_table_reads.HostedProvisionedTableReadSource);
            public_read_source.* = api_table_reads.HostedProvisionedTableReadSource.init(
                replica_root_dir,
                catalog,
                raft.read_gate.noopReadableLeaseRequester(),
                data_router,
                svc.raft.host.http_host.request_executor,
            );
            owned_public_read_source = public_read_source;

            const public_write_source = try alloc.create(api_table_writes.HostedProvisionedTableWriteSource);
            public_write_source.* = api_table_writes.HostedProvisionedTableWriteSource.init(
                replica_root_dir,
                catalog,
                data_router,
                svc.raft.host.http_host.request_executor,
            );
            const backend_runtime = try svc.ensureBackendRuntime();
            _ = public_write_source.withBackendRuntime(backend_runtime);
            _ = public_write_source.withInferenceAPIURL(if (cfg.api_server_cfg.node_config) |node_config| node_config.inference.api_url else null);
            _ = public_write_source.withSecretStore(cfg.api_server_cfg.secret_store);
            _ = public_write_source.withRemoteContent(cfg.api_server_cfg.remote_content);
            owned_public_write_source = public_write_source;

            var api_server_cfg = cfg.api_server_cfg;
            api_server_cfg.shard_ops = if (owned_hosted_shard_ops) |ops| ops.adapter() else null;
            api_server_cfg.shard_db_adapter = owned_hosted_shard_db.?.adapter();
            api_server_cfg.backend_runtime = backend_runtime;
            api_server_cfg.restore_execution_guard = .{
                .ptr = svc,
                .is_current = metadataRestoreLeadershipIsCurrent,
            };

            const public_http_server = try alloc.create(public_api_kernel.ApiHttpServer);
            public_http_server.* = public_api_kernel.ApiHttpServer.initWithProcessRequestAllocator(
                alloc,
                api_server_cfg,
                public_api_http_server.StatusSource.fromMetadataHttpService(svc),
                public_read_source.source(),
                public_write_source.source(),
            );
            try public_http_server.attachReplicatedRestoreJobStore(metadataRestoreJobPersistence(svc));
            owned_public_http_server = public_http_server;

            const mux = try alloc.create(MetadataAdminMux);
            mux.* = .{
                .admin = admin_http_server,
                .public_api = public_http_server,
                .svc = svc,
            };
            owned_admin_mux = mux;

            owned_admin_listener = try MetadataAdminHttpRuntime.init(
                alloc,
                try svc.ensureBackendRuntime(),
                listener_cfg,
                mux,
                public_http_server,
            );
        }

        return .{
            .alloc = alloc,
            .svc = svc,
            .control_loop = metadata_mod.MetadataControlLoop.initWithConfig(alloc, cfg.reconciler_config),
            .transition_ops_registration = transition_ops_registration,
            .owned_hosted_shard_ops = owned_hosted_shard_ops,
            .owned_hosted_shard_db = owned_hosted_shard_db,
            .owned_admin_http_server = owned_admin_http_server,
            .owned_public_read_source = owned_public_read_source,
            .owned_public_write_source = owned_public_write_source,
            .owned_public_http_server = owned_public_http_server,
            .owned_admin_mux = owned_admin_mux,
            .owned_admin_listener = owned_admin_listener,
        };
    }

    fn metadataGroupTransitionReadiness(
        ptr: *anyopaque,
        group_id: u64,
    ) !transition_state.StablePlacementReadiness {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return try svc.groupTransitionReadiness(group_id);
    }

    pub fn deinit(self: *MetadataServer) void {
        self.deinitWithDeadline(runtime_lifecycle.ShutdownDeadline.afterMilliseconds(30_000));
    }

    pub fn deinitWithDeadline(self: *MetadataServer, deadline: runtime_lifecycle.ShutdownDeadline) void {
        self.stopRestoreSupervisor();
        if (self.transition_ops_registration) |*registration| registration.deinit();
        self.transition_ops_registration = null;
        if (self.owned_admin_listener) |listener| {
            listener.deinitWithDeadline(deadline);
        }
        if (self.owned_admin_mux) |mux| {
            self.alloc.destroy(mux);
        }
        if (self.owned_public_http_server) |public_http_server| {
            public_http_server.deinit();
            self.alloc.destroy(public_http_server);
        }
        if (self.owned_public_write_source) |write_source| {
            self.alloc.destroy(write_source);
        }
        if (self.owned_public_read_source) |read_source| {
            self.alloc.destroy(read_source);
        }
        if (self.owned_admin_http_server) |admin_http_server| {
            self.alloc.destroy(admin_http_server);
        }
        self.control_loop.deinit();
        self.svc.deinit();
        self.alloc.destroy(self.svc);
        if (self.owned_hosted_shard_db) |hosted_db| self.alloc.destroy(hosted_db);
        if (self.owned_hosted_shard_ops) |hosted_ops| self.alloc.destroy(hosted_ops);
        self.* = undefined;
    }

    pub fn start(self: *MetadataServer) !void {
        self.svc.setLifecycleReconcileHook(self.lifecycleReconcileHook());
        self.svc.start() catch |err| {
            std.log.err("metadata server start failed step=service_start err={}", .{err});
            return err;
        };
        errdefer self.svc.stop();
        if (self.owned_admin_listener) |listener| {
            listener.start() catch |err| {
                std.log.err("metadata server start failed step=admin_listener_start err={}", .{err});
                return err;
            };
            errdefer listener.stop();
        }
        if (self.owned_admin_mux != null) {
            const runtime = try self.svc.ensureBackendRuntime();
            if (runtime.threaded_jobs != null and runtime.io() != null) {
                self.restore_supervisor_stop.store(false, .release);
                self.restore_supervisor_owner_id = try runtime.allocOwnerId();
                errdefer self.restore_supervisor_owner_id = 0;
                try runtime.durable_jobs.submit(.{
                    .owner_id = self.restore_supervisor_owner_id,
                    .class = .maintenance,
                    .ptr = self,
                    .run = restoreSupervisorRun,
                    .deinit = restoreSupervisorDeinit,
                });
            }
        }
    }

    pub fn stop(self: *MetadataServer) void {
        self.stopWithDeadline(runtime_lifecycle.ShutdownDeadline.afterMilliseconds(30_000));
    }

    pub fn stopWithDeadline(self: *MetadataServer, deadline: runtime_lifecycle.ShutdownDeadline) void {
        if (self.owned_admin_listener) |listener| listener.stopWithDeadline(deadline);
        self.stopRestoreSupervisor();
        self.svc.stop();
    }

    pub fn adminListenerFailure(self: *const MetadataServer) ?anyerror {
        return if (self.owned_admin_listener) |listener| listener.listener_task.runtimeFailure() else null;
    }

    pub fn adminListenerHealthy(self: *const MetadataServer) bool {
        const listener = self.owned_admin_listener orelse return false;
        return public_api_kernel.handlerStats(&listener.handler).peer_observer_failures_total == 0;
    }

    fn restoreSupervisorRun(ptr: *anyopaque) !void {
        const self: *MetadataServer = @ptrCast(@alignCast(ptr));
        const io = (try self.svc.ensureBackendRuntime()).io() orelse return error.AsyncRestoreUnavailable;
        var last_unexpected_error: ?anyerror = null;
        while (!self.restore_supervisor_stop.load(.acquire)) {
            if (self.owned_admin_mux) |mux| {
                if (mux.ensureRestoreLeadershipIfLocalLeader()) |local_leader| {
                    if (local_leader) {
                        if (mux.public_api.pollRestoreJobsOnce()) |_| {
                            last_unexpected_error = null;
                        } else |err| {
                            if (last_unexpected_error == null or last_unexpected_error.? != err)
                                std.log.err("metadata restore dispatch retry failed err={s}", .{@errorName(err)});
                            last_unexpected_error = err;
                        }
                    } else {
                        last_unexpected_error = null;
                    }
                } else |err| {
                    if (metadata_authority.isRetryableError(err)) {
                        last_unexpected_error = null;
                    } else {
                        if (last_unexpected_error == null or last_unexpected_error.? != err)
                            std.log.err("metadata restore supervisor failed err={s}", .{@errorName(err)});
                        last_unexpected_error = err;
                    }
                }
            }
            io.sleep(std.Io.Duration.fromMilliseconds(250), .awake) catch return;
        }
    }

    fn restoreSupervisorDeinit(_: *anyopaque) void {}

    fn stopRestoreSupervisor(self: *MetadataServer) void {
        self.restore_supervisor_stop.store(true, .release);
        if (self.restore_supervisor_owner_id != 0) {
            if (self.svc.backend_runtime) |runtime| runtime.durable_jobs.closeOwner(self.restore_supervisor_owner_id);
        }
        self.restore_supervisor_owner_id = 0;
    }

    pub fn baseUri(self: *MetadataServer, alloc: std.mem.Allocator) ![]u8 {
        return try self.svc.baseUri(alloc);
    }

    pub fn adminBaseUri(self: *MetadataServer, alloc: std.mem.Allocator) ![]u8 {
        const listener = self.owned_admin_listener orelse return error.MissingAdminListener;
        return try listener.baseUri(alloc);
    }

    pub fn campaignMetadataGroup(self: *MetadataServer) !void {
        try self.svc.campaignMetadataGroup();
    }

    pub fn runRound(self: *MetadataServer) !void {
        try self.svc.runRound();
    }

    pub fn runRaftRoundOnly(self: *MetadataServer) !void {
        try self.svc.runRaftRoundOnly();
    }

    pub fn runControlRoundOnly(self: *MetadataServer) !void {
        try self.svc.runControlRoundOnly();
    }

    pub fn runCdcRound(self: *MetadataServer) !void {
        try self.svc.runCdcRound();
    }

    pub fn setCdcWriteSource(self: *MetadataServer, source: api_table_writes.TableWriteSource) void {
        self.svc.setCdcWriteSource(source);
    }

    pub fn setLocalReplicaRootReconcileHook(self: *MetadataServer, hook: ?service.LocalReplicaRootReconcileHook) void {
        self.svc.setLocalReplicaRootReconcileHook(hook);
    }

    pub fn setLocalReplicaRootReconcilePermitHook(self: *MetadataServer, hook: ?service.LocalReplicaRootReconcilePermitHook) void {
        self.svc.setLocalReplicaRootReconcilePermitHook(hook);
    }

    pub fn proposeTransitionCommand(self: *MetadataServer, command: metadata_storage.TransitionCommand) !void {
        try self.svc.proposeTransitionCommand(command);
    }

    pub fn upsertSplitTransition(self: *MetadataServer, record: transition_state.SplitTransitionRecord) !void {
        try self.svc.upsertSplitTransition(record);
    }

    pub fn upsertMergeTransition(self: *MetadataServer, record: transition_state.MergeTransitionRecord) !void {
        try self.svc.upsertMergeTransition(record);
    }

    pub fn status(self: *MetadataServer) !service.MetadataStatus {
        return try self.svc.status();
    }

    pub fn adminSnapshot(self: *MetadataServer) !@import("api.zig").AdminSnapshot {
        return try self.svc.adminSnapshot();
    }

    pub fn validatePublication(self: *MetadataServer, contract: @import("api.zig").CatalogPublicationContract) !bool {
        return try self.svc.validatePublication(contract);
    }

    pub fn validateTablePublication(self: *MetadataServer, contract: @import("api.zig").CatalogTablePublicationContract) !bool {
        return try self.svc.validateTablePublication(contract);
    }

    pub fn freeAdminSnapshot(self: *MetadataServer, snapshot: *@import("api.zig").AdminSnapshot) void {
        self.svc.freeAdminSnapshot(snapshot);
    }

    fn lifecycleReconcileHook(self: *MetadataServer) service.LifecycleReconcileHook {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = runLifecycleReconcile,
            },
        };
    }

    fn runLifecycleReconcile(ptr: *anyopaque) !void {
        const self: *MetadataServer = @ptrCast(@alignCast(ptr));
        try self.control_loop.stateRef().syncProjected(self.svc);
        try self.control_loop.stateRef().seedDesiredFromProjected();
        _ = try self.svc.reconcilePreparedIfLeaseHeld(&self.control_loop);
    }
};

const MetadataAdminHttpRuntime = struct {
    alloc: std.mem.Allocator,
    cfg: raft_transport.StdHttpListenerConfig,
    mux: *MetadataAdminMux,
    handler: public_api_kernel.HttpxHandler,
    server: httpx.Server,
    listener_task: httpx.ListenerTask,
    api_lane_lease: @import("../storage/background_runtime.zig").BackendRuntime.ApiLaneLease,

    fn init(
        alloc: std.mem.Allocator,
        backend_runtime: *@import("../storage/background_runtime.zig").BackendRuntime,
        cfg: raft_transport.StdHttpListenerConfig,
        mux: *MetadataAdminMux,
        public_api: *public_api_kernel.ApiHttpServer,
    ) !*MetadataAdminHttpRuntime {
        var api_lane_lease = try backend_runtime.acquireApiLane();
        errdefer api_lane_lease.release();
        const runtime = try alloc.create(MetadataAdminHttpRuntime);
        errdefer alloc.destroy(runtime);
        const max_connections: u32 = if (cfg.max_connection_threads == 0) 256 else cfg.max_connection_threads;
        const max_active_requests: u32 = if (cfg.max_active_requests == 0) @min(max_connections, 32) else cfg.max_active_requests;
        runtime.* = .{
            .alloc = alloc,
            .cfg = cfg,
            .mux = mux,
            .handler = try public_api_kernel.createHandler(public_api),
            .server = httpx.Server.initWithConfig(alloc, api_lane_lease.io(), .{
                .host = cfg.bind_host,
                .port = cfg.bind_port,
                .max_body_size = cfg.max_request_bytes,
                .request_body_buffer_budget_bytes = cfg.max_request_bytes * @as(usize, max_active_requests),
                .max_h1_inflight_bodies = max_active_requests,
                .max_connections = max_connections,
                .accept_error_backoff_initial_ms = cfg.accept_error_backoff_initial_ms,
                .accept_error_backoff_max_ms = cfg.accept_error_backoff_max_ms,
                .reuse_address = cfg.reuse_address,
            }),
            .listener_task = undefined,
            .api_lane_lease = api_lane_lease,
        };
        errdefer public_api_kernel.deinitHandler(&runtime.handler);
        errdefer runtime.server.deinit();
        // The direct handler's observer receives `self`; start it only after
        // the handler is resident at its final heap address.
        try runtime.handler.initRuntime(alloc);
        runtime.listener_task = httpx.ListenerTask.init(&runtime.server);
        try runtime.server.use(httpx.Middleware.bind("metadata-authority", runtime, authorityMiddleware));
        try runtime.handler.registerGeneratedRoutesWithProbes(&runtime.server);
        try runtime.mux.admin.registerRoutes(&runtime.server);
        return runtime;
    }

    fn start(self: *MetadataAdminHttpRuntime) !void {
        try self.listener_task.start();
    }

    fn stop(self: *MetadataAdminHttpRuntime) void {
        self.stopWithDeadline(runtime_lifecycle.ShutdownDeadline.afterMilliseconds(30_000));
    }

    fn stopWithDeadline(self: *MetadataAdminHttpRuntime, deadline: runtime_lifecycle.ShutdownDeadline) void {
        self.listener_task.shutdown(deadline.remainingMilliseconds());
        self.listener_task.join() catch |err| {
            std.log.err("metadata admin listener failed during shutdown err={s}", .{@errorName(err)});
        };
    }

    fn deinit(self: *MetadataAdminHttpRuntime) void {
        self.deinitWithDeadline(runtime_lifecycle.ShutdownDeadline.afterMilliseconds(30_000));
    }

    fn deinitWithDeadline(self: *MetadataAdminHttpRuntime, deadline: runtime_lifecycle.ShutdownDeadline) void {
        const alloc = self.alloc;
        self.stopWithDeadline(deadline);
        self.server.deinit();
        public_api_kernel.deinitHandler(&self.handler);
        self.api_lane_lease.release();
        alloc.destroy(self);
    }

    fn baseUri(self: *MetadataAdminHttpRuntime, alloc: std.mem.Allocator) ![]u8 {
        const address = self.server.boundAddress() orelse return error.MissingAdminListener;
        return try std.fmt.allocPrint(alloc, "http://{f}", .{address});
    }

    fn authorityMiddleware(
        self: *MetadataAdminHttpRuntime,
        ctx: *httpx.Context,
        next: *httpx.Next,
    ) anyerror!httpx.Response {
        if (!MetadataAdminMux.isRestoreApiRequest(ctx.request.uri.raw)) return next.call(ctx);
        const local_leader = self.mux.ensureRestoreLeadershipIfLocalLeader() catch |err| {
            if (!metadata_authority.isRetryableError(err)) return err;
            return self.metadataNotLeader(ctx);
        };
        const is_collection_get = ctx.request.method == .GET and
            MetadataAdminMux.isRestoreJobCollectionRequest(ctx.request.uri.raw);
        if (!local_leader and (ctx.request.method != .GET or is_collection_get))
            return self.metadataNotLeader(ctx);
        return next.call(ctx);
    }

    fn metadataNotLeader(_: *MetadataAdminHttpRuntime, ctx: *httpx.Context) !httpx.Response {
        _ = ctx.status(503);
        try ctx.setHeader("content-type", "application/json");
        try ctx.setHeader("Retry-After", "1");
        try ctx.setHeader(
            raft_transport.http_common.metadata_not_leader_header,
            raft_transport.http_common.metadata_not_leader_value,
        );
        _ = ctx.response.body("{\"error\":\"metadata leader unavailable\"}");
        return ctx.response.build();
    }
};

const MetadataAdminMux = struct {
    admin: *metadata_http_server.MetadataHttpServer,
    public_api: *public_api_kernel.ApiHttpServer,
    svc: *service.MetadataHttpService,
    restore_leadership_mutex: std.atomic.Mutex = .unlocked,
    restore_leadership_term: u64 = 0,

    fn ensureRestoreLeadershipIfLocalLeader(self: *MetadataAdminMux) !bool {
        const term = self.svc.localMetadataLeadershipTerm() orelse {
            platform_sync.lockYielding(&self.restore_leadership_mutex);
            self.restore_leadership_term = 0;
            self.restore_leadership_mutex.unlock();
            return false;
        };
        platform_sync.lockYielding(&self.restore_leadership_mutex);
        defer self.restore_leadership_mutex.unlock();
        if (self.restore_leadership_term == term) return true;
        try self.svc.ensureLinearizableRead();
        try self.public_api.prepareRestoreLeadership(term);
        self.restore_leadership_term = term;
        return true;
    }

    fn isRestoreApiRequest(uri: []const u8) bool {
        const path = if (std.mem.indexOfScalar(u8, uri, '?')) |query| uri[0..query] else uri;
        if (std.mem.eql(u8, path, "/db/v1/restore") or
            std.mem.eql(u8, path, "/db/v1/restore/jobs") or
            std.mem.startsWith(u8, path, "/db/v1/restore/jobs/")) return true;
        return std.mem.startsWith(u8, path, "/db/v1/tables/") and std.mem.endsWith(u8, path, "/restore");
    }

    fn isRestoreJobCollectionRequest(uri: []const u8) bool {
        const path = if (std.mem.indexOfScalar(u8, uri, '?')) |query| uri[0..query] else uri;
        return std.mem.eql(u8, path, "/db/v1/restore/jobs");
    }
};

fn metadataRestoreJobPersistence(svc: *service.MetadataHttpService) restore_jobs.ReplicatedPersistence {
    return restore_jobs.ReplicatedPersistence.fromLocal(svc, .{
        .load = metadataRestoreJobLoad,
        .get = metadataRestoreJobGet,
        .put = metadataRestoreJobPut,
        .delete = metadataRestoreJobDelete,
        .delete_many = metadataRestoreJobDeleteMany,
    });
}

fn metadataRestoreJobGet(ptr: *anyopaque, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const local_leader = svc.localMetadataLeadershipTerm() != null;
    if (local_leader) try svc.ensureLinearizableRead();
    const store = svc.projectedStore() orelse {
        if (!local_leader) return error.NotLeader;
        return error.MissingMetadataStore;
    };
    const value = try store.getRestoreJobValue(alloc, svc.metadata_group_id, key);
    // A follower cannot distinguish "not committed here yet" from "does not
    // exist". Fail retryably until the record is visible instead of leaking a
    // load-balancer-dependent 404 for a durable job accepted by the leader.
    if (value == null and !local_leader) return error.NotLeader;
    return value;
}

fn metadataRestoreJobLoad(ptr: *anyopaque, alloc: std.mem.Allocator) ![]restore_jobs.ReplicatedPersistence.OwnedRow {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const rows = try store.listRestoreJobRows(alloc, svc.metadata_group_id);
    defer store.freeRestoreJobRows(alloc, rows);
    const out = try alloc.alloc(restore_jobs.ReplicatedPersistence.OwnedRow, rows.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(out);
    }
    for (rows, 0..) |row, i| {
        out[i] = .{ .key = try alloc.dupe(u8, row.key), .value = try alloc.dupe(u8, row.value) };
        initialized += 1;
    }
    return out;
}

fn metadataRestoreJobPut(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    try svc.proposeTransitionCommand(.{ .upsert_restore_job = .{ .key = key, .value = value } });
    // `propose` only admits the command to the local Raft node. A successful
    // job mutation must not become visible to the HTTP caller until a read
    // barrier proves that this proposal is committed and applied locally.
    try svc.ensureLinearizableRead();
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const committed = (try store.getRestoreJobValue(svc.alloc, svc.metadata_group_id, key)) orelse
        return error.RestoreJobCommitNotApplied;
    defer svc.alloc.free(committed);
    if (!std.mem.eql(u8, committed, value)) return error.RestoreJobCommitNotApplied;
}

fn metadataRestoreJobDelete(ptr: *anyopaque, key: []const u8) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    try svc.proposeTransitionCommand(.{ .remove_restore_job = .{ .key = key } });
    try svc.ensureLinearizableRead();
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    if (try store.getRestoreJobValue(svc.alloc, svc.metadata_group_id, key)) |committed| {
        svc.alloc.free(committed);
        return error.RestoreJobCommitNotApplied;
    }
}

fn metadataRestoreJobDeleteMany(ptr: *anyopaque, keys: []const []const u8) !void {
    if (keys.len == 0) return;
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    try svc.proposeTransitionCommand(.{ .remove_restore_jobs = .{ .keys = keys } });
    try svc.ensureLinearizableRead();
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    for (keys) |key| {
        if (try store.getRestoreJobValue(svc.alloc, svc.metadata_group_id, key)) |committed| {
            svc.alloc.free(committed);
            return error.RestoreJobCommitNotApplied;
        }
    }
}

fn metadataRestoreLeadershipIsCurrent(ptr: *anyopaque, leadership_term: u64) bool {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return svc.localMetadataLeadershipTerm() == leadership_term;
}

fn metadataStoreGroupRouter(svc: *service.MetadataHttpService) api_table_router.HostedGroupRouter {
    return .{
        .ptr = svc,
        .vtable = &.{
            .local_node_id = metadataStoreRouterLocalNodeId,
            .local_status = metadataStoreRouterLocalStatus,
            .group_node_ids = metadataStoreRouterGroupNodeIds,
            .node_status = metadataStoreRouterNodeStatus,
            .node_base_uri = metadataStoreRouterNodeBaseUri,
            .node_base_uri_for_group = metadataStoreRouterNodeBaseUriForGroup,
        },
    };
}

fn metadataDataBearingStoreGroupRouter(svc: *service.MetadataHttpService) api_table_router.HostedGroupRouter {
    return .{
        .ptr = svc,
        .vtable = &.{
            .local_node_id = metadataStoreRouterLocalNodeId,
            .local_status = metadataStoreRouterLocalStatus,
            .group_leader_node_id = metadataDataBearingStoreRouterGroupLeaderNodeId,
            .group_node_ids = metadataDataBearingStoreRouterGroupNodeIds,
            .node_status = metadataDataBearingStoreRouterNodeStatus,
            .node_base_uri = metadataStoreRouterNodeBaseUri,
            .node_base_uri_for_group = metadataStoreRouterNodeBaseUriForGroup,
        },
    };
}

fn metadataStoreRouterLocalNodeId(ptr: *anyopaque) u64 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return svc.raft.host.http_host.host.cfg.local_node_id;
}

fn metadataStoreRouterLocalStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
    return .absent;
}

fn metadataStoreRouterNodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    var snapshot = loadMetadataRoutingSnapshot(svc, svc.alloc) catch return .absent;
    defer snapshot.deinit(svc, svc.alloc);
    const store = storeForNode(snapshot.stores, node_id) orelse return .absent;
    if (!store.live or !std.mem.eql(u8, store.health_class, "healthy")) return .absent;
    if (!nodeHasGroupPlacement(snapshot.placements, group_id, node_id)) return .absent;
    return .active;
}

fn metadataDataBearingStoreRouterNodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    var snapshot = loadMetadataRoutingSnapshot(svc, svc.alloc) catch return .absent;
    defer snapshot.deinit(svc, svc.alloc);
    const store = storeForNode(snapshot.stores, node_id) orelse return .absent;
    if (!store.live or !std.mem.eql(u8, store.health_class, "healthy")) return .absent;
    if (!nodeHasReadableGroupPlacement(snapshot.placements, group_id, node_id)) return .absent;
    if (!storeHasGroupData(store, group_id)) return .absent;
    return .active;
}

fn metadataDataBearingStoreRouterGroupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    var snapshot = loadMetadataRoutingSnapshot(svc, svc.alloc) catch return null;
    defer snapshot.deinit(svc, svc.alloc);
    const candidate = bestDataBearingStoreCandidate(snapshot.stores, snapshot.placements, group_id) orelse return null;
    if (!candidate.local_leader) return null;
    return candidate.node_id;
}

fn metadataDataBearingStoreRouterGroupNodeIds(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u64 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    var snapshot = try loadMetadataRoutingSnapshot(svc, svc.alloc);
    defer snapshot.deinit(svc, svc.alloc);

    var candidates = std.ArrayListUnmanaged(DataBearingStoreCandidate).empty;
    defer candidates.deinit(alloc);
    for (snapshot.stores) |store| {
        const candidate = dataBearingStoreCandidate(store, snapshot.placements, group_id) orelse continue;
        try candidates.append(alloc, candidate);
    }
    std.mem.sort(DataBearingStoreCandidate, candidates.items, {}, struct {
        fn lessThan(_: void, a: DataBearingStoreCandidate, b: DataBearingStoreCandidate) bool {
            return dataBearingStoreCandidateLessThan(a, b);
        }
    }.lessThan);

    const out = try alloc.alloc(u64, candidates.items.len);
    errdefer alloc.free(out);
    for (candidates.items, 0..) |candidate, i| out[i] = candidate.node_id;
    return out;
}

fn metadataStoreRouterGroupNodeIds(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u64 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const placements = try svc.listProjectedPlacementIntents(svc.alloc);
    defer svc.freeProjectedPlacementIntents(svc.alloc, placements);
    var node_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer node_ids.deinit(alloc);
    for (placements) |intent| {
        if (intent.record.group_id != group_id) continue;
        try node_ids.append(alloc, intent.record.local_node_id);
    }
    return try node_ids.toOwnedSlice(alloc);
}

fn metadataStoreRouterNodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const stores = try svc.listProjectedStores(svc.alloc);
    defer svc.freeProjectedStores(svc.alloc, stores);
    const store = storeForNode(stores, node_id) orelse return null;
    if (store.api_url.len == 0) return null;
    return try alloc.dupe(u8, store.api_url);
}

fn metadataStoreRouterNodeBaseUriForGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) !?[]u8 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    var snapshot = try loadMetadataRoutingSnapshot(svc, svc.alloc);
    defer snapshot.deinit(svc, svc.alloc);
    if (!nodeHasReadableGroupPlacement(snapshot.placements, group_id, node_id)) return null;
    const store = storeForNode(snapshot.stores, node_id) orelse return null;
    if (store.api_url.len == 0) return null;
    return try alloc.dupe(u8, store.api_url);
}

const MetadataRoutingSnapshot = struct {
    stores: []metadata_mod.StoreRecord,
    placements: []raft_reconciler.PlacementIntent,

    fn deinit(self: *MetadataRoutingSnapshot, svc: *service.MetadataHttpService, alloc: std.mem.Allocator) void {
        svc.freeProjectedPlacementIntents(alloc, self.placements);
        svc.freeProjectedStores(alloc, self.stores);
        self.* = undefined;
    }
};

fn loadMetadataRoutingSnapshot(svc: *service.MetadataHttpService, alloc: std.mem.Allocator) !MetadataRoutingSnapshot {
    var snapshot = MetadataRoutingSnapshot{
        .stores = &.{},
        .placements = &.{},
    };
    errdefer snapshot.deinit(svc, alloc);
    snapshot.stores = try svc.listProjectedStores(alloc);
    snapshot.placements = try svc.listProjectedPlacementIntents(alloc);
    return snapshot;
}

fn storeForNode(stores: []const metadata_mod.StoreRecord, node_id: u64) ?metadata_mod.StoreRecord {
    for (stores) |store| {
        if (store.node_id == node_id) return store;
    }
    return null;
}

fn storeHasGroupData(store: metadata_mod.StoreRecord, group_id: u64) bool {
    for (store.group_statuses) |status| {
        if (status.group_id != group_id) continue;
        if (status.local_leader) return true;
        if (!status.empty or status.doc_count > 0 or status.disk_bytes > 1024) return true;
    }
    for (store.runtime_statuses) |status| {
        if (status.group_id != group_id) continue;
        if (status.doc_count > 0 or status.disk_bytes > 1024) return true;
    }
    return false;
}

const DataBearingStoreCandidate = struct {
    node_id: u64,
    store_id: u64,
    local_leader: bool = false,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    updated_at_millis: u64 = 0,
};

fn bestDataBearingStoreCandidate(
    stores: []const metadata_mod.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) ?DataBearingStoreCandidate {
    var best: ?DataBearingStoreCandidate = null;
    for (stores) |store| {
        const candidate = dataBearingStoreCandidate(store, placements, group_id) orelse continue;
        if (best == null or dataBearingStoreCandidateLessThan(candidate, best.?)) {
            best = candidate;
        }
    }
    return best;
}

fn dataBearingStoreCandidate(
    store: metadata_mod.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) ?DataBearingStoreCandidate {
    if (!store.live or !std.mem.eql(u8, store.health_class, "healthy")) return null;
    if (!nodeHasReadableGroupPlacement(placements, group_id, store.node_id)) return null;

    var candidate = DataBearingStoreCandidate{
        .node_id = store.node_id,
        .store_id = store.store_id,
    };
    var has_data = false;
    for (store.group_statuses) |status| {
        if (status.group_id != group_id) continue;
        candidate.local_leader = candidate.local_leader or status.local_leader;
        if (!status.empty or status.doc_count > 0 or status.disk_bytes > 1024) has_data = true;
        if (status.doc_count > candidate.doc_count) candidate.doc_count = status.doc_count;
        if (status.disk_bytes > candidate.disk_bytes) candidate.disk_bytes = status.disk_bytes;
        if (status.updated_at_millis > candidate.updated_at_millis) candidate.updated_at_millis = status.updated_at_millis;
    }
    for (store.runtime_statuses) |status| {
        if (status.group_id != group_id) continue;
        if (status.doc_count > 0 or status.disk_bytes > 1024) has_data = true;
        if (status.doc_count > candidate.doc_count) candidate.doc_count = status.doc_count;
        if (status.disk_bytes > candidate.disk_bytes) candidate.disk_bytes = status.disk_bytes;
        const updated_at_millis = @divTrunc(status.updated_at_ns, std.time.ns_per_ms);
        if (updated_at_millis > candidate.updated_at_millis) candidate.updated_at_millis = updated_at_millis;
    }
    return if (has_data or candidate.local_leader) candidate else null;
}

fn dataBearingStoreCandidateLessThan(a: DataBearingStoreCandidate, b: DataBearingStoreCandidate) bool {
    if (a.local_leader != b.local_leader) return a.local_leader;
    if (a.doc_count != b.doc_count) return a.doc_count > b.doc_count;
    if (a.disk_bytes != b.disk_bytes) return a.disk_bytes > b.disk_bytes;
    if (a.updated_at_millis != b.updated_at_millis) return a.updated_at_millis > b.updated_at_millis;
    return a.store_id < b.store_id;
}

fn nodeHasGroupPlacement(placements: []const raft_reconciler.PlacementIntent, group_id: u64, node_id: u64) bool {
    for (placements) |intent| {
        if (intent.record.group_id == group_id and intent.record.local_node_id == node_id) return true;
    }
    return false;
}

fn nodeHasReadableGroupPlacement(placements: []const raft_reconciler.PlacementIntent, group_id: u64, node_id: u64) bool {
    for (placements) |intent| {
        if (intent.record.group_id != group_id or intent.record.local_node_id != node_id) continue;
        return raft_reconciler.placementReadableWithPeers(placements, intent);
    }
    return false;
}

fn metadataLocalShardOperationAdapter(svc: *service.MetadataHttpService) ?raft_shard_ops.ShardOperationAdapter {
    if (svc.raft.local_transition_runtime == null) return null;
    return .{
        .ptr = svc,
        .vtable = &.{
            .observe_split = observeSplit,
            .observe_merge = observeMerge,
            .prepare_split_source = prepareSplitSource,
            .start_split_source = startSplitSource,
            .bootstrap_split_destination = bootstrapSplitDestination,
            .catch_up_split_destination = catchUpSplitDestination,
            .finalize_split_source = finalizeSplitSource,
            .rollback_split = rollbackSplit,
            .accept_merge_receiver = acceptMergeReceiver,
            .catch_up_merge_receiver = catchUpMergeReceiver,
            .finalize_merge = finalizeMerge,
            .rollback_merge = rollbackMerge,
        },
    };
}

fn metadataLocalShardDbAdapter(svc: *service.MetadataHttpService) metadata_mod.ShardDbAdapter {
    return .{
        .ptr = svc,
        .vtable = &.{
            .fetch_median_key = fetchMedianKey,
            .schema_index_ready = schemaIndexReady,
        },
    };
}

fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    if (svc.local_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
    const replica_root_dir = svc.replica_root_dir orelse return error.UnsupportedOperation;
    var shard_db = metadata_mod.FallbackLocalShardDbAdapter{
        .replica_root_dir = replica_root_dir,
        .backend_runtime = try svc.ensureBackendRuntime(),
    };
    return try shard_db.adapter().fetchMedianKey(alloc, group_id);
}

fn schemaIndexReady(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    schema_version: u32,
    read_schema_version: u32,
) !bool {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    if (svc.local_shard_db_adapter) |adapter| {
        return try adapter.schemaIndexReady(alloc, table_name, group_id, schema_version, read_schema_version);
    }
    const replica_root_dir = svc.replica_root_dir orelse return error.UnsupportedOperation;
    var shard_db = metadata_mod.FallbackLocalShardDbAdapter{
        .replica_root_dir = replica_root_dir,
        .backend_runtime = try svc.ensureBackendRuntime(),
    };
    return try shard_db.adapter().schemaIndexReady(alloc, table_name, group_id, schema_version, read_schema_version);
}

fn observeSplit(ptr: *anyopaque, _: u64, record: transition_state.SplitTransitionRecord) !transition_state.SplitObservation {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    return try runtime.shardOperationAdapter().observeSplit(record);
}

fn observeMerge(ptr: *anyopaque, _: u64, record: transition_state.MergeTransitionRecord) !transition_state.MergeObservation {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    return try runtime.shardOperationAdapter().observeMerge(record);
}

fn prepareSplitSource(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "prepare_split_source")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .prepare_split_source = op });
}

fn startSplitSource(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "start_split_source")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .start_split_source = op });
}

fn bootstrapSplitDestination(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "bootstrap_split_destination")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .bootstrap_split_destination = op });
}

fn catchUpSplitDestination(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "catch_up_split_destination")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .catch_up_split_destination = op });
}

fn finalizeSplitSource(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "finalize_split_source")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .finalize_split_source = op });
}

fn rollbackSplit(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "rollback_split")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .rollback_split = op });
}

fn acceptMergeReceiver(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "accept_merge_receiver")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .accept_merge_receiver = op });
}

fn catchUpMergeReceiver(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "catch_up_merge_receiver")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .catch_up_merge_receiver = op });
}

fn finalizeMerge(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "finalize_merge")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .finalize_merge = op });
}

fn rollbackMerge(ptr: *anyopaque, _: u64, op: @FieldType(metadata_mod.TransitionAction, "rollback_merge")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .rollback_merge = op });
}

test "metadata server module compiles" {
    _ = MetadataServerConfig;
    _ = MetadataServerDeps;
    _ = MetadataServer;
}

test "metadata server wires hosted shard adapters by default" {
    const raft_engine = @import("raft_engine");

    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var server = try MetadataServer.init(std.testing.allocator, .{
        .http = .{
            .http = .{
                .host = .{
                    .local_node_id = 1,
                    .metadata_group_id = 1991,
                },
                .transport = .{
                    .snapshot = .{ .root_dir = ".zig-cache/tmp/metadata-server-hosted-shard-ops" },
                },
            },
        },
    }, .{
        .http = .{
            .http = .{
                .http = .{
                    .host = .{
                        .descriptor_factory = factory.iface(),
                    },
                },
            },
        },
    });
    defer server.deinit();

    try std.testing.expect(server.owned_hosted_shard_ops != null);
    try std.testing.expect(server.owned_hosted_shard_db != null);
    try std.testing.expect(server.svc.routed_shard_db_adapter != null);
    try std.testing.expect(server.svc.raft.transition_svc != null);
}

test "metadata server can expose admin listener endpoints" {
    const raft_engine = @import("raft_engine");
    const metadata_http_client = @import("http_client.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");

    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.heap.page_allocator, ".zig-cache/tmp/{s}/metadata-server-root", .{tmp.sub_path});
    defer std.heap.page_allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.heap.page_allocator, ".zig-cache/tmp/{s}/metadata-server-catalog.txt", .{tmp.sub_path});
    defer std.heap.page_allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.heap.page_allocator, ".zig-cache/tmp/{s}/metadata-server-snapshots", .{tmp.sub_path});
    defer std.heap.page_allocator.free(snapshot_root);

    var store = raft_engine.core.MemoryStorage.init(std.heap.page_allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.heap.page_allocator, .store = &store };

    var server = try MetadataServer.init(std.heap.page_allocator, .{
        .http = .{
            .http = .{
                .host = .{
                    .local_node_id = 1,
                    .metadata_group_id = 1990,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                },
                .transport = .{
                    .snapshot = .{ .root_dir = snapshot_root },
                },
            },
        },
        .admin_listener = .{},
    }, .{
        .http = .{
            .http = .{
                .http = .{
                    .host = .{
                        .descriptor_factory = factory.iface(),
                    },
                },
            },
        },
    });
    defer server.deinit();
    try server.start();

    _ = try server.svc.ensureMetadataReplica(.{
        .group_id = 1990,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try server.svc.campaignMetadataGroup();
    try server.runRound();
    try server.svc.upsertTable(.{ .table_id = 77, .name = "docs" });

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try server.runRound();

    const admin_base_uri = try server.adminBaseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(admin_base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, executor.executor());

    const healthz_uri = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/healthz", .{admin_base_uri});
    defer std.heap.page_allocator.free(healthz_uri);
    var healthz = try executor.executor().execute(std.heap.page_allocator, .{ .method = .GET, .uri = healthz_uri });
    defer healthz.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), healthz.status);

    const readyz_uri = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/readyz", .{admin_base_uri});
    defer std.heap.page_allocator.free(readyz_uri);
    var readyz = try executor.executor().execute(std.heap.page_allocator, .{ .method = .GET, .uri = readyz_uri });
    defer readyz.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), readyz.status);

    const mcp_uri = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/mcp/v1", .{admin_base_uri});
    defer std.heap.page_allocator.free(mcp_uri);
    var mcp = try executor.executor().execute(std.heap.page_allocator, .{ .method = .GET, .uri = mcp_uri });
    defer mcp.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), mcp.status);

    const status = try client.fetchStatus(admin_base_uri);
    try std.testing.expectEqual(@as(u64, 1990), status.metadata_group_id);

    var snapshot = try client.fetchSnapshot(admin_base_uri);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), snapshot.value.tables.len);
    try std.testing.expectEqualStrings("docs", snapshot.value.tables[0].name);
}

test "metadata admin maps retryable authority loss to service unavailable" {
    const FakeSource = struct {
        fn iface(self: *@This()) metadata_http_server.AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .trigger_reallocate = triggerReallocate,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_mod.MetadataStatus {
            return .{ .metadata_group_id = 77, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_mod.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 77, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_mod.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn triggerReallocate(_: *anyopaque) !void {
            return error.NotLeader;
        }
    };

    var source = FakeSource{};
    var admin = metadata_http_server.MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var server_io = std.Io.Threaded.init(std.testing.allocator, .{});
    defer server_io.deinit();
    var http_server = httpx.Server.initWithConfig(std.testing.allocator, server_io.io(), .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer http_server.deinit();
    try admin.registerRoutes(&http_server);
    var listener_task = httpx.ListenerTask.init(&http_server);
    try listener_task.start();
    defer {
        listener_task.shutdown(30_000);
        listener_task.join() catch unreachable;
    }

    const address = http_server.boundAddress() orelse return error.MissingAdminListener;
    const uri = try std.fmt.allocPrint(std.testing.allocator, "http://127.0.0.1:{d}/internal/v1/reallocate", .{address.ip4.port});
    defer std.testing.allocator.free(uri);
    var executor = raft_transport.StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var response = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = uri,
    });
    defer response.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), response.status);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "metadata authority unavailable") != null);
}

test "metadata public api server carries auth and restore configuration" {
    const raft_engine = @import("raft_engine");

    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-mux-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-mux-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-mux-snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var server = try MetadataServer.init(std.testing.allocator, .{
        .http = .{
            .http = .{
                .host = .{
                    .local_node_id = 1,
                    .metadata_group_id = 1992,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                },
                .transport = .{
                    .snapshot = .{ .root_dir = snapshot_root },
                },
            },
        },
        .admin_listener = .{},
        .api_server_cfg = .{
            .auth_enabled = true,
            .trusted_principal_secret = "shared-secret",
        },
    }, .{
        .http = .{
            .http = .{
                .http = .{
                    .host = .{
                        .descriptor_factory = factory.iface(),
                    },
                },
            },
        },
    });
    defer server.deinit();

    try std.testing.expect(server.owned_public_http_server != null);
    try std.testing.expect(server.owned_admin_mux != null);
    try std.testing.expect(server.owned_public_http_server.?.cfg.auth_enabled);
    try std.testing.expectEqualStrings("shared-secret", server.owned_public_http_server.?.cfg.trusted_principal_secret.?);

    try std.testing.expectError(
        error.NotLeader,
        metadataRestoreJobGet(server.svc, std.testing.allocator, "\x00\x00__api_restore_jobs__:0000000000000001"),
    );
}
