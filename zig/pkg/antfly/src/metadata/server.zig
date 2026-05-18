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
const metadata_mod = @import("mod.zig");
const service = @import("service.zig");
const transition_state = @import("transition_state.zig");
const metadata_storage = @import("storage/mod.zig");
const metadata_http_server = @import("http_server.zig");
const api_table_catalog = @import("../api/table_catalog.zig");
const api_table_router = @import("../api/table_router.zig");
const api_table_writes = @import("../api/table_writes.zig");
const raft_host = @import("../raft/host.zig");
const raft_managed_host = @import("../raft/managed_host.zig");
const raft_hosted_shard_ops = @import("../raft/hosted_shard_ops.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const raft_shard_ops = @import("../raft/shard_ops.zig");
const raft_transport = @import("../raft/transport/mod.zig");

pub const MetadataServerConfig = struct {
    http: raft_managed_host.ManagedHttpHostConfig,
    service: service.MetadataServiceConfig = .{},
    admin_listener: ?raft_transport.StdHttpListenerConfig = null,
    reconciler_config: metadata_mod.Reconciler.Config = .{},
};

pub const MetadataServerDeps = struct {
    http: service.MetadataHttpServiceDeps = .{},
};

pub const MetadataServer = struct {
    alloc: std.mem.Allocator,
    svc: *service.MetadataHttpService,
    control_loop: metadata_mod.MetadataControlLoop,
    owned_hosted_shard_ops: ?*raft_hosted_shard_ops.HostedShardOperationAdapter = null,
    owned_hosted_shard_db: ?*raft_hosted_shard_ops.HostedShardDbAdapter = null,
    owned_admin_http_server: ?*metadata_http_server.MetadataHttpServer = null,
    owned_admin_listener: ?*raft_transport.StdHttpListener = null,

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

        if (deps.http.raft.transition_ops == null) {
            const local_ops = metadataLocalShardOperationAdapter(svc);
            const hosted_ops = try alloc.create(raft_hosted_shard_ops.HostedShardOperationAdapter);
            hosted_ops.* = raft_hosted_shard_ops.HostedShardOperationAdapter.initWithRouters(
                alloc,
                api_table_catalog.CatalogSource.fromMetadataHttpService(svc),
                metadataStoreGroupRouter(svc),
                metadataDataBearingStoreGroupRouter(svc),
                svc.raft.host.http_host.request_executor,
                local_ops,
            );
            try svc.raft.replaceTransitionOps(hosted_ops.adapter());
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
        var owned_admin_listener: ?*raft_transport.StdHttpListener = null;
        errdefer if (owned_admin_listener) |listener| {
            listener.deinit();
            alloc.destroy(listener);
        };

        if (cfg.admin_listener) |listener_cfg| {
            const admin_http_server = try alloc.create(metadata_http_server.MetadataHttpServer);
            admin_http_server.* = metadata_http_server.MetadataHttpServer.init(
                alloc,
                .{},
                metadata_http_server.AdminSource.fromMetadataHttpService(svc),
            );
            owned_admin_http_server = admin_http_server;

            const listener = try alloc.create(raft_transport.StdHttpListener);
            listener.* = raft_transport.StdHttpListener.init(alloc, listener_cfg, admin_http_server.executor());
            owned_admin_listener = listener;
        }

        return .{
            .alloc = alloc,
            .svc = svc,
            .control_loop = metadata_mod.MetadataControlLoop.initWithConfig(alloc, cfg.reconciler_config),
            .owned_hosted_shard_ops = owned_hosted_shard_ops,
            .owned_hosted_shard_db = owned_hosted_shard_db,
            .owned_admin_http_server = owned_admin_http_server,
            .owned_admin_listener = owned_admin_listener,
        };
    }

    pub fn deinit(self: *MetadataServer) void {
        if (self.owned_admin_listener) |listener| {
            listener.deinit();
            self.alloc.destroy(listener);
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
        }
    }

    pub fn stop(self: *MetadataServer) void {
        if (self.owned_admin_listener) |listener| listener.stop();
        self.svc.stop();
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
    if (!nodeHasGroupPlacement(snapshot.placements, group_id, node_id)) return .absent;
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
    if (!nodeHasGroupPlacement(snapshot.placements, group_id, node_id)) return null;
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
    if (!nodeHasGroupPlacement(placements, group_id, store.node_id)) return null;

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

fn observeSplit(ptr: *anyopaque, record: transition_state.SplitTransitionRecord) !transition_state.SplitObservation {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    return try runtime.shardOperationAdapter().observeSplit(record);
}

fn observeMerge(ptr: *anyopaque, record: transition_state.MergeTransitionRecord) !transition_state.MergeObservation {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    return try runtime.shardOperationAdapter().observeMerge(record);
}

fn prepareSplitSource(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "prepare_split_source")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .prepare_split_source = op });
}

fn startSplitSource(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "start_split_source")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .start_split_source = op });
}

fn bootstrapSplitDestination(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "bootstrap_split_destination")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .bootstrap_split_destination = op });
}

fn catchUpSplitDestination(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "catch_up_split_destination")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .catch_up_split_destination = op });
}

fn finalizeSplitSource(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "finalize_split_source")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .finalize_split_source = op });
}

fn rollbackSplit(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "rollback_split")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .rollback_split = op });
}

fn acceptMergeReceiver(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "accept_merge_receiver")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .accept_merge_receiver = op });
}

fn catchUpMergeReceiver(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "catch_up_merge_receiver")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .catch_up_merge_receiver = op });
}

fn finalizeMerge(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "finalize_merge")) !void {
    const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    const runtime = svc.raft.local_transition_runtime orelse return error.UnsupportedOperation;
    try runtime.shardOperationAdapter().execute(.{ .finalize_merge = op });
}

fn rollbackMerge(ptr: *anyopaque, op: @FieldType(metadata_mod.TransitionAction, "rollback_merge")) !void {
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

    const status = try client.fetchStatus(admin_base_uri);
    try std.testing.expectEqual(@as(u64, 1990), status.metadata_group_id);

    var snapshot = try client.fetchSnapshot(admin_base_uri);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), snapshot.value.tables.len);
    try std.testing.expectEqualStrings("docs", snapshot.value.tables[0].name);
}
