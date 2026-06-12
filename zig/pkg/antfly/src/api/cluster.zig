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
const common_secrets = @import("../common/secrets.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const table_manager = @import("../metadata/table_manager.zig");

pub const ClusterHealth = enum {
    healthy,
    degraded,
    @"error",
};

pub const ClusterStatus = struct {
    health: ClusterHealth,
    message: ?[]u8 = null,
    auth_enabled: bool = false,
    swarm_mode: bool = false,
    secret_store: ?SecretStoreStatus = null,

    pub fn deinit(self: *ClusterStatus, alloc: std.mem.Allocator) void {
        if (self.message) |message| alloc.free(message);
        self.* = undefined;
    }
};

pub const ClusterTopology = struct {
    health: ClusterHealth,
    message: ?[]u8 = null,
    auth_enabled: bool = false,
    swarm_mode: bool = false,
    secret_store: ?SecretStoreStatus = null,
    data: ClusterDataStatus = .{},

    pub fn deinit(self: *ClusterTopology, alloc: std.mem.Allocator) void {
        if (self.message) |message| alloc.free(message);
        self.data.deinit(alloc);
        self.* = undefined;
    }
};

pub const SecretStoreStatus = struct {
    stale: bool = false,
};

pub const ClusterDataStatus = struct {
    nodes: []const DataNodeStatus = &.{},
    ranges: []const DataRangeStatus = &.{},
    replicas: []const DataReplicaStatus = &.{},
    groups: []const DataGroupStatus = &.{},

    pub fn deinit(self: *ClusterDataStatus, alloc: std.mem.Allocator) void {
        if (self.nodes.len > 0) alloc.free(@constCast(self.nodes));
        if (self.ranges.len > 0) alloc.free(@constCast(self.ranges));
        if (self.replicas.len > 0) alloc.free(@constCast(self.replicas));
        if (self.groups.len > 0) alloc.free(@constCast(self.groups));
        self.* = undefined;
    }
};

pub const DataNodeStatus = struct {
    data_id: u64,
    node_id: u64,
    api_url: []const u8 = "",
    raft_url: []const u8 = "",
    role: []const u8 = "data",
    state: []const u8 = "healthy",
    health_class: []const u8 = "healthy",
    failure_domain: []const u8 = "",
    live: bool = true,
    drain_requested: bool = false,
    capacity_bytes: u64 = 0,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    active_backfills: u32 = 0,
};

pub const DataRangeStatus = struct {
    group_id: u64,
    range_id: u64,
    table_id: u64,
    table_name: []const u8 = "",
    start_key: []const u8 = "",
    end_key: ?[]const u8 = null,
    doc_identity_shard_id: u64 = 0,
    doc_identity_range_id: u64 = 0,
    state: []const u8 = "unknown",
    leader_data_id: ?u64 = null,
    voter_count: u16 = 0,
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    empty: bool = true,
};

pub const DataReplicaStatus = struct {
    group_id: u64,
    data_id: u64,
    node_id: u64,
    replica_id: u64,
    peer_node_ids: []const u64 = &.{},
};

pub const DataGroupStatus = struct {
    group_id: u64,
    leader_known: bool = false,
    leader_data_id: ?u64 = null,
    voter_count_known: bool = false,
    voter_count: u16 = 0,
    healthy_voter_reports: u16 = 0,
    joint_consensus: bool = false,
    transition_pending: bool = false,
    replay_required: bool = false,
    replay_caught_up: bool = false,
    cutover_ready: bool = false,
    reads_ready_after_cutover: bool = false,
    doc_identity_lifecycle: []const u8 = "unknown",
    doc_count: u64 = 0,
    disk_bytes: u64 = 0,
    empty: bool = true,
};

pub fn fromMetadataStatus(alloc: std.mem.Allocator, status: metadata_api.MetadataStatus) !ClusterStatus {
    if (status.projected_stores == 0 and (status.projected_ranges > 0 or status.projected_tables > 0 or status.projected_placement_intents > 0)) {
        return .{
            .health = .@"error",
            .message = try std.fmt.allocPrint(alloc, "metadata tracks {d} tables and {d} ranges but no data nodes", .{
                status.projected_tables,
                status.projected_ranges,
            }),
        };
    }
    if (status.repair_placement_groups > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} placement groups require repair", .{status.repair_placement_groups}),
        };
    }
    if (status.projected_doc_identity_lifecycle_rebuild_required > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} ranges require document identity rebuild", .{status.projected_doc_identity_lifecycle_rebuild_required}),
        };
    }
    if (status.excluded_stores > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} data nodes are excluded from placement", .{status.excluded_stores}),
        };
    }
    if (status.overloaded_stores > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} data nodes are overloaded", .{status.overloaded_stores}),
        };
    }
    return .{
        .health = .healthy,
        .message = if (status.projected_doc_identity_lifecycle_reassigning > 0)
            try std.fmt.allocPrint(alloc, "{d} ranges are reassigning document identity", .{status.projected_doc_identity_lifecycle_reassigning})
        else if (status.rebalance_placement_groups > 0)
            try std.fmt.allocPrint(alloc, "{d} placement groups are rebalancing", .{status.rebalance_placement_groups})
        else
            null,
    };
}

pub fn topologyFromStatusAndSnapshot(
    alloc: std.mem.Allocator,
    status: ClusterStatus,
    snapshot: *const metadata_api.AdminSnapshot,
) !ClusterTopology {
    var topology = try topologyFromStatus(alloc, status);
    errdefer topology.deinit(alloc);
    topology.data = try dataFromSnapshot(alloc, snapshot);
    return topology;
}

pub fn topologyFromStatus(alloc: std.mem.Allocator, status: ClusterStatus) !ClusterTopology {
    return .{
        .health = status.health,
        .message = if (status.message) |message| try alloc.dupe(u8, message) else null,
        .auth_enabled = status.auth_enabled,
        .swarm_mode = status.swarm_mode,
        .secret_store = status.secret_store,
        .data = .{},
    };
}

pub fn dataFromSnapshot(alloc: std.mem.Allocator, snapshot: *const metadata_api.AdminSnapshot) !ClusterDataStatus {
    const nodes = try alloc.alloc(DataNodeStatus, snapshot.stores.len);
    errdefer alloc.free(nodes);
    for (snapshot.stores, 0..) |store, i| {
        nodes[i] = .{
            .data_id = store.store_id,
            .node_id = store.node_id,
            .api_url = store.api_url,
            .raft_url = store.raft_url,
            .role = store.role,
            .state = if (store.live) store.health_class else "unhealthy",
            .health_class = store.health_class,
            .failure_domain = store.failure_domain,
            .live = store.live,
            .drain_requested = store.drain_requested,
            .capacity_bytes = store.capacity_bytes,
            .available_bytes = store.available_bytes,
            .lease_pressure = store.lease_pressure,
            .read_load = store.read_load,
            .write_load = store.write_load,
            .active_backfills = store.active_backfills,
        };
    }

    const ranges = try alloc.alloc(DataRangeStatus, snapshot.ranges.len);
    errdefer alloc.free(ranges);
    for (snapshot.ranges, 0..) |range, i| {
        const group = findGroupStatus(snapshot.merged_group_statuses, range.group_id);
        ranges[i] = .{
            .group_id = range.group_id,
            .range_id = range.range_id,
            .table_id = range.table_id,
            .table_name = tableName(snapshot.tables, range.table_id),
            .start_key = range.start_key,
            .end_key = range.end_key,
            .doc_identity_shard_id = range.doc_identity_shard_id,
            .doc_identity_range_id = range.doc_identity_range_id,
            .state = if (group) |status_value| rangeState(status_value) else "unknown",
            .leader_data_id = if (group) |status_value| if (status_value.leader_known) status_value.leader_store_id else null else null,
            .voter_count = if (group) |status_value| status_value.voter_count else 0,
            .doc_count = if (group) |status_value| status_value.doc_count else 0,
            .disk_bytes = if (group) |status_value| status_value.disk_bytes else 0,
            .empty = if (group) |status_value| status_value.empty else true,
        };
    }

    const replicas = try alloc.alloc(DataReplicaStatus, snapshot.placement_intents.len);
    errdefer alloc.free(replicas);
    for (snapshot.placement_intents, 0..) |intent, i| {
        replicas[i] = .{
            .group_id = intent.record.group_id,
            .data_id = intent.store_id,
            .node_id = intent.record.local_node_id,
            .replica_id = intent.record.replica_id,
            .peer_node_ids = intent.peer_node_ids,
        };
    }

    const groups = try alloc.alloc(DataGroupStatus, snapshot.merged_group_statuses.len);
    errdefer alloc.free(groups);
    for (snapshot.merged_group_statuses, 0..) |group, i| {
        groups[i] = .{
            .group_id = group.group_id,
            .leader_known = group.leader_known,
            .leader_data_id = if (group.leader_known) group.leader_store_id else null,
            .voter_count_known = group.voter_count_known,
            .voter_count = group.voter_count,
            .healthy_voter_reports = group.healthy_voter_reports,
            .joint_consensus = group.joint_consensus,
            .transition_pending = group.transition_pending,
            .replay_required = group.replay_required,
            .replay_caught_up = group.replay_caught_up,
            .cutover_ready = group.cutover_ready,
            .reads_ready_after_cutover = group.reads_ready_after_cutover,
            .doc_identity_lifecycle = group.doc_identity_lifecycle,
            .doc_count = group.doc_count,
            .disk_bytes = group.disk_bytes,
            .empty = group.empty,
        };
    }

    return .{
        .nodes = nodes,
        .ranges = ranges,
        .replicas = replicas,
        .groups = groups,
    };
}

pub fn applySecretStoreHealth(status: *ClusterStatus, health: common_secrets.ReloadHealth) void {
    status.secret_store = .{
        .stale = health.stale_snapshot,
    };
}

fn tableName(tables: []const table_manager.TableRecord, table_id: u64) []const u8 {
    for (tables) |table| {
        if (table.table_id == table_id) return table.name;
    }
    return "";
}

fn findGroupStatus(
    groups: []const metadata_reconciler.MergedGroupStatus,
    group_id: u64,
) ?metadata_reconciler.MergedGroupStatus {
    for (groups) |group| {
        if (group.group_id == group_id) return group;
    }
    return null;
}

fn rangeState(group: metadata_reconciler.MergedGroupStatus) []const u8 {
    if (group.transition_pending) return "transitioning";
    if (group.replay_required and !group.replay_caught_up) return "replaying";
    if (group.leader_known and group.healthy_voter_reports > 0) return "healthy";
    return "unknown";
}

test "cluster status derives degraded and error states from metadata status" {
    var error_status = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_tables = 1,
        .projected_ranges = 1,
        .projected_stores = 0,
        .projected_placement_intents = 1,
    });
    defer error_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.@"error", error_status.health);

    var degraded = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .repair_placement_groups = 2,
    });
    defer degraded.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.degraded, degraded.health);

    var rebuild_required = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .projected_doc_identity_lifecycle_rebuild_required = 1,
    });
    defer rebuild_required.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.degraded, rebuild_required.health);

    var reassigning = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .projected_doc_identity_lifecycle_reassigning = 1,
    });
    defer reassigning.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.healthy, reassigning.health);

    var healthy = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .rebalance_placement_groups = 1,
    });
    defer healthy.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.healthy, healthy.health);
}

test "cluster status carries non-secret secret store health" {
    var status = ClusterStatus{ .health = .healthy };
    applySecretStoreHealth(&status, .{
        .generation = 7,
        .entry_count = 3,
        .last_reload_failed = true,
        .stale_snapshot = true,
        .reload_successes = 2,
        .reload_failures = 1,
        .last_success_ns = 123,
        .last_failure_ns = 456,
    });
    const secret_store = status.secret_store orelse return error.TestUnexpectedResult;
    try std.testing.expect(secret_store.stale);
}
