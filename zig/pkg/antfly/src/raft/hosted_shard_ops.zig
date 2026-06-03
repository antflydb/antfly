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
const api_http_client = @import("../api/http_client.zig");
const api_table_catalog = @import("../api/table_catalog.zig");
const api_table_router = @import("../api/table_router.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const http_common = @import("transport/http_common.zig");
const shard_ops = @import("shard_ops.zig");

const PrepareSplitSource = @FieldType(metadata_mod.TransitionAction, "prepare_split_source");
const StartSplitSource = @FieldType(metadata_mod.TransitionAction, "start_split_source");
const BootstrapSplitDestination = @FieldType(metadata_mod.TransitionAction, "bootstrap_split_destination");
const CatchUpSplitDestination = @FieldType(metadata_mod.TransitionAction, "catch_up_split_destination");
const FinalizeSplitSource = @FieldType(metadata_mod.TransitionAction, "finalize_split_source");
const RollbackSplit = @FieldType(metadata_mod.TransitionAction, "rollback_split");
const AcceptMergeReceiver = @FieldType(metadata_mod.TransitionAction, "accept_merge_receiver");
const CatchUpMergeReceiver = @FieldType(metadata_mod.TransitionAction, "catch_up_merge_receiver");
const FinalizeMerge = @FieldType(metadata_mod.TransitionAction, "finalize_merge");
const RollbackMerge = @FieldType(metadata_mod.TransitionAction, "rollback_merge");

pub const HostedShardOperationAdapter = struct {
    alloc: std.mem.Allocator,
    catalog: api_table_catalog.CatalogSource,
    router: api_table_router.HostedGroupRouter,
    data_router: api_table_router.HostedGroupRouter,
    executor: http_common.RequestExecutor,
    local_ops: ?shard_ops.ShardOperationAdapter = null,

    pub fn init(
        alloc: std.mem.Allocator,
        catalog: api_table_catalog.CatalogSource,
        router: api_table_router.HostedGroupRouter,
        executor: http_common.RequestExecutor,
        local_ops: ?shard_ops.ShardOperationAdapter,
    ) HostedShardOperationAdapter {
        return initWithRouters(alloc, catalog, router, router, executor, local_ops);
    }

    pub fn initWithRouters(
        alloc: std.mem.Allocator,
        catalog: api_table_catalog.CatalogSource,
        placement_router: api_table_router.HostedGroupRouter,
        data_router: api_table_router.HostedGroupRouter,
        executor: http_common.RequestExecutor,
        local_ops: ?shard_ops.ShardOperationAdapter,
    ) HostedShardOperationAdapter {
        return .{
            .alloc = alloc,
            .catalog = catalog,
            .router = placement_router,
            .data_router = data_router,
            .executor = executor,
            .local_ops = local_ops,
        };
    }

    pub fn adapter(self: *HostedShardOperationAdapter) shard_ops.ShardOperationAdapter {
        return .{
            .ptr = self,
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

    fn observeSplit(ptr: *anyopaque, record: metadata_transition_state.SplitTransitionRecord) !metadata_transition_state.SplitObservation {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        return try self.observeSplitRouted(record);
    }

    fn observeMerge(ptr: *anyopaque, record: metadata_transition_state.MergeTransitionRecord) !metadata_transition_state.MergeObservation {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        return try self.observeMergeRouted(record);
    }

    fn prepareSplitSource(ptr: *anyopaque, op: PrepareSplitSource) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.source_group_id, .{ .prepare_split_source = op });
    }

    fn startSplitSource(ptr: *anyopaque, op: StartSplitSource) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.source_group_id, .{ .start_split_source = op });
    }

    fn bootstrapSplitDestination(ptr: *anyopaque, op: BootstrapSplitDestination) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.source_group_id, .{ .bootstrap_split_destination = op });
    }

    fn catchUpSplitDestination(ptr: *anyopaque, op: CatchUpSplitDestination) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.source_group_id, .{ .catch_up_split_destination = op });
    }

    fn finalizeSplitSource(ptr: *anyopaque, op: FinalizeSplitSource) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.source_group_id, .{ .finalize_split_source = op });
    }

    fn rollbackSplit(ptr: *anyopaque, op: RollbackSplit) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.source_group_id, .{ .rollback_split = op });
    }

    fn acceptMergeReceiver(ptr: *anyopaque, op: AcceptMergeReceiver) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.receiver_group_id, .{ .accept_merge_receiver = op });
    }

    fn catchUpMergeReceiver(ptr: *anyopaque, op: CatchUpMergeReceiver) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.receiver_group_id, .{ .catch_up_merge_receiver = op });
    }

    fn finalizeMerge(ptr: *anyopaque, op: FinalizeMerge) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.receiver_group_id, .{ .finalize_merge = op });
    }

    fn rollbackMerge(ptr: *anyopaque, op: RollbackMerge) !void {
        const self: *HostedShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.executeRouted(self.data_router, op.receiver_group_id, .{ .rollback_merge = op });
    }

    fn observeSplitRouted(self: *HostedShardOperationAdapter, record: metadata_transition_state.SplitTransitionRecord) !metadata_transition_state.SplitObservation {
        var route = (try api_table_router.resolveGroupRoute(self.alloc, self.catalog, self.data_router, record.source_group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(self.alloc);
        return switch (route) {
            .local => {
                const local_ops = self.local_ops orelse return error.UnsupportedOperation;
                var observation = try local_ops.observeSplit(record);
                observation.source_local_leader = true;
                return observation;
            },
            .remote => |remote| {
                var client = api_http_client.ApiHttpClient.init(self.alloc, self.executor);
                return try client.fetchGroupShardObserveSplit(remote.base_uri, record.source_group_id, record);
            },
        };
    }

    fn observeMergeRouted(self: *HostedShardOperationAdapter, record: metadata_transition_state.MergeTransitionRecord) !metadata_transition_state.MergeObservation {
        var route = (try api_table_router.resolveGroupRoute(self.alloc, self.catalog, self.data_router, record.receiver_group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(self.alloc);
        return switch (route) {
            .local => {
                const local_ops = self.local_ops orelse return error.UnsupportedOperation;
                var observation = try local_ops.observeMerge(record);
                observation.receiver_local_leader = true;
                return observation;
            },
            .remote => |remote| {
                var client = api_http_client.ApiHttpClient.init(self.alloc, self.executor);
                return try client.fetchGroupShardObserveMerge(remote.base_uri, record.receiver_group_id, record);
            },
        };
    }

    fn executeRouted(self: *HostedShardOperationAdapter, router: api_table_router.HostedGroupRouter, group_id: u64, action: metadata_mod.TransitionAction) !void {
        var route = (try api_table_router.resolveGroupRoute(self.alloc, self.catalog, router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(self.alloc);
        switch (route) {
            .local => {
                const local_ops = self.local_ops orelse return error.UnsupportedOperation;
                try local_ops.execute(action);
            },
            .remote => |remote| {
                var client = api_http_client.ApiHttpClient.init(self.alloc, self.executor);
                _ = try client.fetchGroupShardExecute(remote.base_uri, group_id, action);
            },
        }
    }
};

pub const HostedShardDbAdapter = struct {
    alloc: std.mem.Allocator,
    catalog: api_table_catalog.CatalogSource,
    router: api_table_router.HostedGroupRouter,
    executor: http_common.RequestExecutor,
    local_db: ?metadata_mod.ShardDbAdapter = null,

    pub fn init(
        alloc: std.mem.Allocator,
        catalog: api_table_catalog.CatalogSource,
        router: api_table_router.HostedGroupRouter,
        executor: http_common.RequestExecutor,
        local_db: ?metadata_mod.ShardDbAdapter,
    ) HostedShardDbAdapter {
        return .{
            .alloc = alloc,
            .catalog = catalog,
            .router = router,
            .executor = executor,
            .local_db = local_db,
        };
    }

    pub fn adapter(self: *HostedShardDbAdapter) metadata_mod.ShardDbAdapter {
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
                .schema_index_ready = schemaIndexReady,
            },
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const self: *HostedShardDbAdapter = @ptrCast(@alignCast(ptr));
        var tried_node_ids = std.ArrayListUnmanaged(u64).empty;
        defer tried_node_ids.deinit(self.alloc);

        if (try self.fetchMedianKeyFromResolvedRoute(alloc, group_id, .prefer_leader, &tried_node_ids)) |median_key| {
            return median_key;
        }

        const maybe_node_ids = try self.router.groupNodeIds(self.alloc, group_id);
        const node_ids = maybe_node_ids orelse blk: {
            var snapshot = try self.catalog.adminSnapshot();
            defer self.catalog.freeAdminSnapshot(&snapshot);
            const placements = try metadata_mod.admin.listGroupPlacement(self.alloc, &snapshot, group_id);
            defer metadata_mod.admin.freePlacementRefs(self.alloc, placements);
            var nodes = try std.ArrayListUnmanaged(u64).initCapacity(self.alloc, placements.len);
            errdefer nodes.deinit(self.alloc);
            for (placements) |intent| {
                try nodes.append(self.alloc, intent.record.local_node_id);
            }
            break :blk try nodes.toOwnedSlice(self.alloc);
        };
        defer self.alloc.free(node_ids);

        const local_node_id = self.router.localNodeId();
        if (self.router.localStatus(group_id) == .active and !containsNodeId(tried_node_ids.items, local_node_id)) {
            try tried_node_ids.append(self.alloc, local_node_id);
            if (self.local_db) |local_db| {
                if (try local_db.fetchMedianKey(alloc, group_id)) |median_key| return median_key;
            }
        }

        var saw_candidate = tried_node_ids.items.len > 0;
        for (node_ids) |node_id| {
            if (containsNodeId(tried_node_ids.items, node_id)) continue;
            if (node_id == local_node_id) continue;
            if (self.router.nodeStatus(node_id, group_id)) |status| {
                if (status != .active) continue;
            }
            const base_uri = (try self.router.nodeBaseUriForGroup(self.alloc, group_id, node_id)) orelse continue;
            defer self.alloc.free(base_uri);
            saw_candidate = true;
            var client = api_http_client.ApiHttpClient.init(alloc, self.executor);
            if (try client.fetchGroupDbMedianKey(base_uri, group_id)) |median_key| return median_key;
        }

        return if (saw_candidate) null else error.UnknownGroup;
    }

    fn fetchMedianKeyFromResolvedRoute(
        self: *HostedShardDbAdapter,
        alloc: std.mem.Allocator,
        group_id: u64,
        policy: api_table_router.RoutePolicy,
        tried_node_ids: *std.ArrayListUnmanaged(u64),
    ) !?[]u8 {
        var route = (try api_table_router.resolveGroupRoute(self.alloc, self.catalog, self.router, group_id, policy)) orelse return null;
        defer route.deinit(self.alloc);
        return switch (route) {
            .local => {
                try tried_node_ids.append(self.alloc, self.router.localNodeId());
                const local_db = self.local_db orelse return null;
                return try local_db.fetchMedianKey(alloc, group_id);
            },
            .remote => |remote| {
                try tried_node_ids.append(self.alloc, remote.node_id);
                var client = api_http_client.ApiHttpClient.init(alloc, self.executor);
                return try client.fetchGroupDbMedianKey(remote.base_uri, group_id);
            },
        };
    }

    fn containsNodeId(items: []const u64, node_id: u64) bool {
        for (items) |item| {
            if (item == node_id) return true;
        }
        return false;
    }

    fn schemaIndexReady(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        schema_version: u32,
        read_schema_version: u32,
    ) !bool {
        const self: *HostedShardDbAdapter = @ptrCast(@alignCast(ptr));
        var route = (try api_table_router.resolveGroupRoute(self.alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(self.alloc);
        return switch (route) {
            .local => {
                const local_db = self.local_db orelse return error.UnsupportedOperation;
                return try local_db.schemaIndexReady(alloc, table_name, group_id, schema_version, read_schema_version);
            },
            .remote => return error.UnsupportedOperation,
        };
    }
};

test "hosted shard operation adapter uses local shard ops when preferred leader is local" {
    const metadata_api = @import("../metadata/api.zig");
    const metadata_table_manager = @import("../metadata/table_manager.zig");
    const raft_reconciler = @import("reconciler.zig");

    const FakeCatalog = struct {
        fn iface() api_table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{
                    .{ .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 1 } },
                    .{ .record = .{ .group_id = 77, .replica_id = 2, .local_node_id = 2 } },
                })[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface(self: *@This()) api_table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) @import("host.zig").HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) @import("host.zig").HostedReplicaStatus {
            return if (node_id == 1 or node_id == 2) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const FakeShardOps = struct {
        execute_called: bool = false,

        fn adapter(self: *@This()) shard_ops.ShardOperationAdapter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_split = observeSplit,
                    .observe_merge = observeMerge,
                    .prepare_split_source = prepareSplitSource,
                    .start_split_source = noopStartSplitSource,
                    .bootstrap_split_destination = noopBootstrapSplitDestination,
                    .catch_up_split_destination = noopCatchUpSplitDestination,
                    .finalize_split_source = noopFinalizeSplitSource,
                    .rollback_split = noopRollbackSplit,
                    .accept_merge_receiver = noopAcceptMergeReceiver,
                    .catch_up_merge_receiver = noopCatchUpMergeReceiver,
                    .finalize_merge = noopFinalizeMerge,
                    .rollback_merge = noopRollbackMerge,
                },
            };
        }

        fn observeSplit(_: *anyopaque, _: metadata_transition_state.SplitTransitionRecord) !metadata_transition_state.SplitObservation {
            return .{
                .status = .{
                    .phase = .cutover_ready,
                    .source_split_phase = .finalizing,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 3,
                    .dest_delta_sequence = 3,
                },
            };
        }

        fn observeMerge(_: *anyopaque, record: metadata_transition_state.MergeTransitionRecord) !metadata_transition_state.MergeObservation {
            const status = @import("../data/mod.zig").MergeTransitionStatus{
                .phase = .replay_deltas,
                .donor_group_id = record.donor_group_id,
                .receiver_group_id = record.receiver_group_id,
                .receiver_accepts_donor_range = true,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
            return .{ .donor = status, .receiver = status };
        }

        fn prepareSplitSource(ptr: *anyopaque, _: PrepareSplitSource) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.execute_called = true;
        }
        fn noopStartSplitSource(_: *anyopaque, _: StartSplitSource) !void {}
        fn noopBootstrapSplitDestination(_: *anyopaque, _: BootstrapSplitDestination) !void {}
        fn noopCatchUpSplitDestination(_: *anyopaque, _: CatchUpSplitDestination) !void {}
        fn noopFinalizeSplitSource(_: *anyopaque, _: FinalizeSplitSource) !void {}
        fn noopRollbackSplit(_: *anyopaque, _: RollbackSplit) !void {}
        fn noopAcceptMergeReceiver(_: *anyopaque, _: AcceptMergeReceiver) !void {}
        fn noopCatchUpMergeReceiver(_: *anyopaque, _: CatchUpMergeReceiver) !void {}
        fn noopFinalizeMerge(_: *anyopaque, _: FinalizeMerge) !void {}
        fn noopRollbackMerge(_: *anyopaque, _: RollbackMerge) !void {}
    };

    var fake_ops = FakeShardOps{};
    var router = FakeRouter{};
    var hosted = HostedShardOperationAdapter.init(
        std.testing.allocator,
        FakeCatalog.iface(),
        router.iface(),
        undefined,
        fake_ops.adapter(),
    );

    const observation = try hosted.adapter().observeSplit(.{
        .transition_id = 1,
        .source_group_id = 77,
        .destination_group_id = 78,
    });
    try std.testing.expect(observation.source_local_leader);
    try std.testing.expectEqual(@as(u64, 3), observation.status.source_delta_sequence);

    try hosted.adapter().execute(.{
        .prepare_split_source = .{
            .transition_id = 1,
            .source_group_id = 77,
            .destination_group_id = 78,
            .split_key = "doc:m",
        },
    });
    try std.testing.expect(fake_ops.execute_called);
}

test "hosted shard db adapter routes median key to remote leader" {
    const api_http_server = @import("../api/http_server.zig");
    const metadata_api = @import("../metadata/api.zig");
    const metadata_table_manager = @import("../metadata/table_manager.zig");
    const raft_reconciler = @import("reconciler.zig");
    const std_http_executor = @import("transport/std_http_executor.zig");
    const std_http_listener = @import("transport/std_http_listener.zig");

    const FakeCatalog = struct {
        fn iface() api_table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{
                    .{ .record = .{ .group_id = 88, .replica_id = 1, .local_node_id = 1 } },
                    .{ .record = .{ .group_id = 88, .replica_id = 2, .local_node_id = 2 } },
                })[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeStatus = struct {
        fn iface() api_http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 1,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    const FakeRemoteShardDb = struct {
        fn adapter() metadata_mod.ShardDbAdapter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .fetch_median_key = fetchMedianKey,
                    .schema_index_ready = schemaIndexReady,
                },
            };
        }

        fn fetchMedianKey(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
            return switch (group_id) {
                88 => try alloc.dupe(u8, "doc:m"),
                else => error.UnknownGroup,
            };
        }

        fn schemaIndexReady(_: *anyopaque, _: std.mem.Allocator, _: []const u8, group_id: u64, _: u32, _: u32) !bool {
            return switch (group_id) {
                88 => true,
                else => error.UnknownGroup,
            };
        }
    };

    var server = api_http_server.ApiHttpServer.init(std.heap.page_allocator, .{
        .shard_db_adapter = FakeRemoteShardDb.adapter(),
    }, FakeStatus.iface(), null, null);
    defer server.deinit();
    var listener = std_http_listener.StdHttpListener.init(std.heap.page_allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const remote_base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(remote_base_uri);

    const FakeRouter = struct {
        remote_base_uri: []const u8,

        fn iface(self: *@This()) api_table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) @import("host.zig").HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 2;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) @import("host.zig").HostedReplicaStatus {
            return if (node_id == 1 or node_id == 2) .active else .absent;
        }

        fn nodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id != 2) return null;
            return try alloc.dupe(u8, self.remote_base_uri);
        }
    };

    var router = FakeRouter{ .remote_base_uri = remote_base_uri };
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var hosted = HostedShardDbAdapter.init(
        std.testing.allocator,
        FakeCatalog.iface(),
        router.iface(),
        executor.executor(),
        null,
    );

    const median_key = (try hosted.adapter().fetchMedianKey(std.testing.allocator, 88)).?;
    defer std.testing.allocator.free(median_key);
    try std.testing.expectEqualStrings("doc:m", median_key);
    try std.testing.expectError(error.UnsupportedOperation, hosted.adapter().schemaIndexReady(std.testing.allocator, "docs", 88, 2, 1));
}
