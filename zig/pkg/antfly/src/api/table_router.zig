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
const metadata_admin = @import("../metadata/admin.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_host = @import("../raft/host.zig");
const raft_managed_host = @import("../raft/managed_host.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const table_catalog = @import("table_catalog.zig");

pub const HostedGroupRouter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        local_node_id: *const fn (ptr: *anyopaque) u64,
        local_status: *const fn (ptr: *anyopaque, group_id: u64) raft_host.HostedReplicaStatus,
        group_leader_node_id: ?*const fn (ptr: *anyopaque, group_id: u64) ?u64 = null,
        group_node_ids: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) anyerror![]u64 = null,
        node_status: ?*const fn (ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus = null,
        node_base_uri: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) anyerror!?[]u8,
        node_base_uri_for_group: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) anyerror!?[]u8 = null,
    };

    pub fn localNodeId(self: HostedGroupRouter) u64 {
        return self.vtable.local_node_id(self.ptr);
    }

    pub fn localStatus(self: HostedGroupRouter, group_id: u64) raft_host.HostedReplicaStatus {
        return self.vtable.local_status(self.ptr, group_id);
    }

    pub fn groupLeaderNodeId(self: HostedGroupRouter, group_id: u64) ?u64 {
        const fn_ptr = self.vtable.group_leader_node_id orelse return null;
        return fn_ptr(self.ptr, group_id);
    }

    pub fn groupNodeIds(self: HostedGroupRouter, alloc: std.mem.Allocator, group_id: u64) !?[]u64 {
        const fn_ptr = self.vtable.group_node_ids orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id);
    }

    pub fn nodeStatus(self: HostedGroupRouter, node_id: u64, group_id: u64) ?raft_host.HostedReplicaStatus {
        const fn_ptr = self.vtable.node_status orelse return null;
        return fn_ptr(self.ptr, node_id, group_id);
    }

    pub fn nodeBaseUri(self: HostedGroupRouter, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
        return try self.vtable.node_base_uri(self.ptr, alloc, node_id);
    }

    pub fn nodeBaseUriForGroup(self: HostedGroupRouter, alloc: std.mem.Allocator, group_id: u64, node_id: u64) !?[]u8 {
        if (self.vtable.node_base_uri_for_group) |fn_ptr| return try fn_ptr(self.ptr, alloc, group_id, node_id);
        return try self.nodeBaseUri(alloc, node_id);
    }

    pub fn fromManagedHttpHost(host: *raft_managed_host.ManagedHttpHost) HostedGroupRouter {
        return .{
            .ptr = host,
            .vtable = &.{
                .local_node_id = managedHttpHostLocalNodeId,
                .local_status = managedHttpHostLocalStatus,
                .group_leader_node_id = managedHttpHostGroupLeaderNodeId,
                .node_base_uri = managedHttpHostNodeBaseUri,
                .node_base_uri_for_group = managedHttpHostNodeBaseUriForGroup,
            },
        };
    }
};

pub const GroupRoute = union(enum) {
    local,
    remote: struct {
        node_id: u64,
        base_uri: []u8,
    },

    pub fn deinit(self: *GroupRoute, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .local => {},
            .remote => |remote| alloc.free(remote.base_uri),
        }
        self.* = undefined;
    }
};

pub const CatalogBackedGroupRouter = struct {
    catalog: table_catalog.CatalogSource,
    local_node_id: u64 = 0,

    pub fn init(catalog: table_catalog.CatalogSource, local_node_id: u64) CatalogBackedGroupRouter {
        return .{
            .catalog = catalog,
            .local_node_id = local_node_id,
        };
    }

    pub fn router(self: *CatalogBackedGroupRouter) HostedGroupRouter {
        return .{
            .ptr = self,
            .vtable = &.{
                .local_node_id = catalogBackedRouterLocalNodeId,
                .local_status = catalogBackedRouterLocalStatus,
                .group_leader_node_id = catalogBackedRouterGroupLeaderNodeId,
                .group_node_ids = catalogBackedRouterGroupNodeIds,
                .node_status = catalogBackedRouterNodeStatus,
                .node_base_uri = catalogBackedRouterNodeBaseUri,
                .node_base_uri_for_group = catalogBackedRouterNodeBaseUriForGroup,
            },
        };
    }
};

pub const RoutePolicy = enum {
    any_active,
    prefer_leader,
};

pub fn resolveGroupRoute(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    router: HostedGroupRouter,
    group_id: u64,
    policy: RoutePolicy,
) !?GroupRoute {
    const local_node_id = router.localNodeId();
    const local_status = router.localStatus(group_id);
    if (policy == .prefer_leader) {
        if (router.groupLeaderNodeId(group_id)) |leader_node_id| {
            if (leader_node_id == local_node_id and local_status == .active) return .local;
            if (leader_node_id != local_node_id) {
                const leader_active = if (router.nodeStatus(leader_node_id, group_id)) |leader_status|
                    leader_status == .active
                else
                    true;
                if (leader_active) {
                    if (try router.nodeBaseUriForGroup(alloc, group_id, leader_node_id)) |base_uri| {
                        return .{ .remote = .{ .node_id = leader_node_id, .base_uri = base_uri } };
                    }
                }
            }
        }
    }

    if (policy != .prefer_leader and local_status == .active) return .local;

    const maybe_router_node_ids = try router.groupNodeIds(alloc, group_id);
    const placement_node_ids = maybe_router_node_ids orelse blk: {
        var snapshot = try catalog.adminSnapshot();
        defer catalog.freeAdminSnapshot(&snapshot);
        const placements = try metadata_admin.listGroupPlacement(alloc, &snapshot, group_id);
        defer metadata_admin.freePlacementRefs(alloc, placements);
        var nodes = try std.ArrayListUnmanaged(u64).initCapacity(alloc, placements.len);
        errdefer nodes.deinit(alloc);
        for (placements) |intent| {
            try nodes.append(alloc, intent.record.local_node_id);
        }
        break :blk try nodes.toOwnedSlice(alloc);
    };
    defer alloc.free(placement_node_ids);

    for (placement_node_ids) |node_id| {
        if (node_id == local_node_id) continue;
        if (router.nodeStatus(node_id, group_id)) |status| {
            if (status != .active) continue;
        }
        if (try router.nodeBaseUriForGroup(alloc, group_id, node_id)) |base_uri| {
            return .{ .remote = .{ .node_id = node_id, .base_uri = base_uri } };
        }
    }

    if (local_status == .active) return .local;
    return null;
}

fn managedHttpHostLocalNodeId(ptr: *anyopaque) u64 {
    const host: *raft_managed_host.ManagedHttpHost = @ptrCast(@alignCast(ptr));
    return host.http_host.host.cfg.local_node_id;
}

fn managedHttpHostLocalStatus(ptr: *anyopaque, group_id: u64) raft_host.HostedReplicaStatus {
    const host: *raft_managed_host.ManagedHttpHost = @ptrCast(@alignCast(ptr));
    return host.http_host.host.status(group_id);
}

fn managedHttpHostGroupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
    const host: *raft_managed_host.ManagedHttpHost = @ptrCast(@alignCast(ptr));
    return host.http_host.host.leaderId(group_id);
}

fn managedHttpHostNodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
    const host: *raft_managed_host.ManagedHttpHost = @ptrCast(@alignCast(ptr));
    const groups = try host.http_host.host.listGroupIds(alloc);
    defer alloc.free(groups);
    for (groups) |group_id| {
        if (try managedHttpHostNodeBaseUriForGroup(ptr, alloc, group_id, node_id)) |base_uri| return base_uri;
    }
    return null;
}

fn managedHttpHostNodeBaseUriForGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) !?[]u8 {
    const host: *raft_managed_host.ManagedHttpHost = @ptrCast(@alignCast(ptr));
    const endpoints = host.view.peerResolver().resolveGroupPeer(alloc, group_id, node_id) catch |err| switch (err) {
        error.UnknownPeer => return null,
        else => return err,
    };
    defer {
        for (endpoints) |endpoint| {
            alloc.free(endpoint.address);
            alloc.free(endpoint.metadata);
        }
        alloc.free(endpoints);
    }

    for (endpoints) |endpoint| {
        switch (endpoint.protocol) {
            .http, .https, .http2, .http3 => return try alloc.dupe(u8, endpoint.address),
            .quic => {},
        }
    }
    return null;
}

fn catalogBackedRouterLocalNodeId(ptr: *anyopaque) u64 {
    const router: *CatalogBackedGroupRouter = @ptrCast(@alignCast(ptr));
    return router.local_node_id;
}

fn catalogBackedRouterLocalStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
    return .absent;
}

fn catalogBackedRouterGroupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
    const router: *CatalogBackedGroupRouter = @ptrCast(@alignCast(ptr));
    var snapshot = router.catalog.adminSnapshot() catch return null;
    defer router.catalog.freeAdminSnapshot(&snapshot);
    const leader_store_id = groupLeaderStoreId(snapshot.merged_group_statuses, group_id) orelse return null;
    const store = storeForStoreId(snapshot.stores, leader_store_id) orelse return null;
    if (!storeUsableForRemoteWrites(store)) return null;
    if (!nodeHasGroupPlacement(snapshot.placement_intents, group_id, store.node_id)) return null;
    return store.node_id;
}

fn catalogBackedRouterGroupNodeIds(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u64 {
    const router: *CatalogBackedGroupRouter = @ptrCast(@alignCast(ptr));
    var snapshot = try router.catalog.adminSnapshot();
    defer router.catalog.freeAdminSnapshot(&snapshot);

    var node_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer node_ids.deinit(alloc);
    for (snapshot.placement_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (containsNodeId(node_ids.items, intent.record.local_node_id)) continue;
        try node_ids.append(alloc, intent.record.local_node_id);
    }
    return try node_ids.toOwnedSlice(alloc);
}

fn catalogBackedRouterNodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
    const router: *CatalogBackedGroupRouter = @ptrCast(@alignCast(ptr));
    var snapshot = router.catalog.adminSnapshot() catch return .absent;
    defer router.catalog.freeAdminSnapshot(&snapshot);
    const store = storeForNode(snapshot.stores, node_id) orelse return .absent;
    if (!storeUsableForRemoteWrites(store)) return .absent;
    if (!nodeHasGroupPlacement(snapshot.placement_intents, group_id, node_id)) return .absent;
    return .active;
}

fn catalogBackedRouterNodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
    const router: *CatalogBackedGroupRouter = @ptrCast(@alignCast(ptr));
    var snapshot = try router.catalog.adminSnapshot();
    defer router.catalog.freeAdminSnapshot(&snapshot);
    const store = storeForNode(snapshot.stores, node_id) orelse return null;
    if (!storeUsableForRemoteWrites(store)) return null;
    return try alloc.dupe(u8, store.api_url);
}

fn catalogBackedRouterNodeBaseUriForGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) !?[]u8 {
    const router: *CatalogBackedGroupRouter = @ptrCast(@alignCast(ptr));
    var snapshot = try router.catalog.adminSnapshot();
    defer router.catalog.freeAdminSnapshot(&snapshot);
    if (!nodeHasGroupPlacement(snapshot.placement_intents, group_id, node_id)) return null;
    const store = storeForNode(snapshot.stores, node_id) orelse return null;
    if (!storeUsableForRemoteWrites(store)) return null;
    return try alloc.dupe(u8, store.api_url);
}

fn groupLeaderStoreId(statuses: []const metadata_reconciler.MergedGroupStatus, group_id: u64) ?u64 {
    for (statuses) |status| {
        if (status.group_id != group_id) continue;
        if (!status.leader_known or status.leader_store_id == 0) return null;
        return status.leader_store_id;
    }
    return null;
}

fn storeForStoreId(stores: []const metadata_table_manager.StoreRecord, store_id: u64) ?metadata_table_manager.StoreRecord {
    for (stores) |store| {
        if (store.store_id == store_id) return store;
    }
    return null;
}

fn storeForNode(stores: []const metadata_table_manager.StoreRecord, node_id: u64) ?metadata_table_manager.StoreRecord {
    for (stores) |store| {
        if (store.node_id == node_id) return store;
    }
    return null;
}

fn storeUsableForRemoteWrites(store: metadata_table_manager.StoreRecord) bool {
    return store.live and
        std.mem.eql(u8, store.health_class, "healthy") and
        store.api_url.len > 0;
}

fn nodeHasGroupPlacement(placements: []const raft_reconciler.PlacementIntent, group_id: u64, node_id: u64) bool {
    for (placements) |intent| {
        if (intent.record.group_id == group_id and intent.record.local_node_id == node_id) return true;
    }
    return false;
}

fn containsNodeId(node_ids: []const u64, node_id: u64) bool {
    for (node_ids) |existing| {
        if (existing == node_id) return true;
    }
    return false;
}

test "resolve group route allows stale reads on local active replica" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
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
        fn iface() HostedGroupRouter {
            return .{
                .ptr = undefined,
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

        fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 2;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_host.HostedReplicaStatus {
            return if (node_id == 1 or node_id == 2) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            return try std.fmt.allocPrint(alloc, "http://node-{d}", .{node_id});
        }
    };

    var route = (try resolveGroupRoute(std.testing.allocator, FakeCatalog.iface(), FakeRouter.iface(), 77, .any_active)).?;
    defer route.deinit(std.testing.allocator);
    try std.testing.expect(route == .local);
}

test "resolve group route prefers leader for strong reads and writes" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
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
        fn iface() HostedGroupRouter {
            return .{
                .ptr = undefined,
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

        fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 2;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_host.HostedReplicaStatus {
            return if (node_id == 1 or node_id == 2) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            return try std.fmt.allocPrint(alloc, "http://node-{d}", .{node_id});
        }
    };

    var route = (try resolveGroupRoute(std.testing.allocator, FakeCatalog.iface(), FakeRouter.iface(), 77, .prefer_leader)).?;
    defer route.deinit(std.testing.allocator);
    switch (route) {
        .local => return error.TestExpectedRemoteLeaderRoute,
        .remote => |remote| try std.testing.expectEqual(@as(u64, 2), remote.node_id),
    }
}

test "catalog backed router routes metadata-owned writes to placement leader api url" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
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
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{
                    .{ .store_id = 10, .node_id = 1, .api_url = "http://node-1", .role = "data", .health_class = "healthy", .live = true },
                    .{ .store_id = 20, .node_id = 2, .api_url = "http://node-2", .role = "data", .health_class = "healthy", .live = true },
                })[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{
                    .{ .store_id = 10, .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 1 } },
                    .{ .store_id = 20, .record = .{ .group_id = 77, .replica_id = 2, .local_node_id = 2 } },
                })[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast((&[_]metadata_reconciler.MergedGroupStatus{
                    .{ .group_id = 77, .leader_known = true, .leader_store_id = 20 },
                })[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog_router = CatalogBackedGroupRouter.init(FakeCatalog.iface(), 0);
    var route = (try resolveGroupRoute(std.testing.allocator, FakeCatalog.iface(), catalog_router.router(), 77, .prefer_leader)).?;
    defer route.deinit(std.testing.allocator);
    switch (route) {
        .local => return error.TestExpectedRemoteLeaderRoute,
        .remote => |remote| {
            try std.testing.expectEqual(@as(u64, 2), remote.node_id);
            try std.testing.expectEqualStrings("http://node-2", remote.base_uri);
        },
    }
}
