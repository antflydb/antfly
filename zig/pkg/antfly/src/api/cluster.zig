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
const common_config = @import("../common/config.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const raft_reconciler = @import("../raft/reconciler.zig");

pub const ClusterHealth = enum {
    healthy,
    degraded,
    @"error",
};

pub const ClusterStatus = struct {
    health: ClusterHealth,
    message: ?[]u8 = null,
    auth_enabled: bool = false,
    deployment_mode: common_config.DeploymentMode = .distributed,
    index_capabilities: IndexRuntimeCapabilities = .{},
    secret_store: ?SecretStoreStatus = null,
    runtime_config: ?RuntimeConfigStatus = null,
    storage: ?metadata_openapi.StorageRuntimeStatus = null,

    pub fn deinit(self: *ClusterStatus, alloc: std.mem.Allocator) void {
        if (self.message) |message| alloc.free(message);
        if (self.secret_store) |*secret_store| secret_store.deinit(alloc);
        if (self.runtime_config) |*runtime_config| runtime_config.deinit(alloc);
        self.* = undefined;
    }
};

pub const ClusterTopology = struct {
    health: ClusterHealth,
    message: ?[]u8 = null,
    auth_enabled: bool = false,
    deployment_mode: common_config.DeploymentMode = .distributed,
    index_capabilities: IndexRuntimeCapabilities = .{},
    secret_store: ?SecretStoreStatus = null,
    runtime_config: ?RuntimeConfigStatus = null,
    storage: ?metadata_openapi.StorageRuntimeStatus = null,
    data: ClusterDataStatus = .{},

    pub fn deinit(self: *ClusterTopology, alloc: std.mem.Allocator) void {
        if (self.message) |message| alloc.free(message);
        if (self.secret_store) |*secret_store| secret_store.deinit(alloc);
        if (self.runtime_config) |*runtime_config| runtime_config.deinit(alloc);
        self.data.deinit(alloc);
        self.* = undefined;
    }
};

pub const IndexRuntimeCapabilities = struct {
    artifact_sources: bool = true,
    artifact_sources_state: ArtifactSourcesCapabilityState = .available,
};

pub const ArtifactSourcesCapabilityState = enum {
    available,
    upgrade_pending,
    unsupported,
};

pub const SecretStoreStatus = struct {
    generation: u64 = 0,
    /// Protocol capability, independent of whether the currently loaded file
    /// carries a control-plane publication generation.
    supports_source_generation: bool = true,
    source_generation: ?[]u8 = null,
    last_reload_failed: bool = false,
    stale: bool = false,
    reload_successes: u64 = 0,
    reload_failures: u64 = 0,

    pub fn deinit(self: *SecretStoreStatus, alloc: std.mem.Allocator) void {
        if (self.source_generation) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const RuntimeConfigStatus = struct {
    generation: u64 = 0,
    hash: []u8,
    last_reload_failed: bool = false,
    stale: bool = false,
    reload_successes: u64 = 0,
    reload_failures: u64 = 0,

    pub fn deinit(self: *RuntimeConfigStatus, alloc: std.mem.Allocator) void {
        alloc.free(self.hash);
        self.* = undefined;
    }
};

pub const ClusterDataStatus = struct {
    nodes: []const DataNodeStatus = &.{},
    ranges: []const DataRangeStatus = &.{},
    replicas: []const DataReplicaStatus = &.{},
    groups: []const DataGroupStatus = &.{},

    pub fn deinit(self: *ClusterDataStatus, alloc: std.mem.Allocator) void {
        if (self.nodes.len > 0) {
            for (@constCast(self.nodes)) |*node| node.deinit(alloc);
            alloc.free(@constCast(self.nodes));
        }
        if (self.ranges.len > 0) {
            for (@constCast(self.ranges)) |*range| range.deinit(alloc);
            alloc.free(@constCast(self.ranges));
        }
        if (self.replicas.len > 0) {
            for (@constCast(self.replicas)) |*replica| replica.deinit(alloc);
            alloc.free(@constCast(self.replicas));
        }
        if (self.groups.len > 0) {
            for (@constCast(self.groups)) |*group| group.deinit(alloc);
            alloc.free(@constCast(self.groups));
        }
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

    fn deinit(self: *DataNodeStatus, alloc: std.mem.Allocator) void {
        alloc.free(self.api_url);
        alloc.free(self.raft_url);
        alloc.free(self.role);
        alloc.free(self.state);
        alloc.free(self.health_class);
        alloc.free(self.failure_domain);
        self.* = undefined;
    }
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

    fn deinit(self: *DataRangeStatus, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.start_key);
        if (self.end_key) |end_key| alloc.free(end_key);
        alloc.free(self.state);
        self.* = undefined;
    }
};

pub const DataReplicaStatus = struct {
    group_id: u64,
    data_id: u64,
    node_id: u64,
    replica_id: u64,
    peer_node_ids: []const u64 = &.{},

    fn deinit(self: *DataReplicaStatus, alloc: std.mem.Allocator) void {
        alloc.free(self.peer_node_ids);
        self.* = undefined;
    }
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

    fn deinit(self: *DataGroupStatus, alloc: std.mem.Allocator) void {
        alloc.free(self.doc_identity_lifecycle);
        self.* = undefined;
    }
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
    const message = if (status.message) |value| try alloc.dupe(u8, value) else null;
    errdefer if (message) |value| alloc.free(value);
    var runtime_config: ?RuntimeConfigStatus = if (status.runtime_config) |value| .{
        .generation = value.generation,
        .hash = try alloc.dupe(u8, value.hash),
        .last_reload_failed = value.last_reload_failed,
        .stale = value.stale,
        .reload_successes = value.reload_successes,
        .reload_failures = value.reload_failures,
    } else null;
    errdefer if (runtime_config) |*value| value.deinit(alloc);
    var secret_store: ?SecretStoreStatus = if (status.secret_store) |value| .{
        .generation = value.generation,
        .supports_source_generation = value.supports_source_generation,
        .source_generation = if (value.source_generation) |generation| try alloc.dupe(u8, generation) else null,
        .last_reload_failed = value.last_reload_failed,
        .stale = value.stale,
        .reload_successes = value.reload_successes,
        .reload_failures = value.reload_failures,
    } else null;
    errdefer if (secret_store) |*value| value.deinit(alloc);
    return .{
        .health = status.health,
        .message = message,
        .auth_enabled = status.auth_enabled,
        .deployment_mode = status.deployment_mode,
        .index_capabilities = status.index_capabilities,
        .secret_store = secret_store,
        .runtime_config = runtime_config,
        .storage = status.storage,
        .data = .{},
    };
}

pub fn dataFromSnapshot(alloc: std.mem.Allocator, snapshot: *const metadata_api.AdminSnapshot) !ClusterDataStatus {
    var data: ClusterDataStatus = .{};
    errdefer data.deinit(alloc);
    data.nodes = try cloneNodeStatuses(alloc, snapshot.stores);
    data.ranges = try cloneRangeStatuses(
        alloc,
        snapshot.tables,
        snapshot.ranges,
        snapshot.merged_group_statuses,
    );
    data.replicas = try cloneReplicaStatuses(alloc, snapshot.placement_intents);
    data.groups = try cloneGroupStatuses(alloc, snapshot.merged_group_statuses);
    return data;
}

fn cloneNodeStatuses(alloc: std.mem.Allocator, stores: []const table_manager.StoreRecord) ![]DataNodeStatus {
    const nodes = try alloc.alloc(DataNodeStatus, stores.len);
    var initialized: usize = 0;
    errdefer {
        for (nodes[0..initialized]) |*node| node.deinit(alloc);
        alloc.free(nodes);
    }
    for (stores, 0..) |store, i| {
        const api_url = try alloc.dupe(u8, store.api_url);
        errdefer alloc.free(api_url);
        const raft_url = try alloc.dupe(u8, store.raft_url);
        errdefer alloc.free(raft_url);
        const role = try alloc.dupe(u8, store.role);
        errdefer alloc.free(role);
        const state = try alloc.dupe(u8, if (store.live) store.health_class else "unhealthy");
        errdefer alloc.free(state);
        const health_class = try alloc.dupe(u8, store.health_class);
        errdefer alloc.free(health_class);
        const failure_domain = try alloc.dupe(u8, store.failure_domain);
        nodes[i] = .{
            .data_id = store.store_id,
            .node_id = store.node_id,
            .api_url = api_url,
            .raft_url = raft_url,
            .role = role,
            .state = state,
            .health_class = health_class,
            .failure_domain = failure_domain,
            .live = store.live,
            .drain_requested = store.drain_requested,
            .capacity_bytes = store.capacity_bytes,
            .available_bytes = store.available_bytes,
            .lease_pressure = store.lease_pressure,
            .read_load = store.read_load,
            .write_load = store.write_load,
            .active_backfills = store.active_backfills,
        };
        initialized += 1;
    }
    return nodes;
}

fn cloneRangeStatuses(
    alloc: std.mem.Allocator,
    tables: []const table_manager.TableRecord,
    source_ranges: []const table_manager.RangeRecord,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
) ![]DataRangeStatus {
    const ranges = try alloc.alloc(DataRangeStatus, source_ranges.len);
    var initialized: usize = 0;
    errdefer {
        for (ranges[0..initialized]) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }
    for (source_ranges, 0..) |range, i| {
        const group = findGroupStatus(merged_group_statuses, range.group_id);
        const table_name = try alloc.dupe(u8, tableName(tables, range.table_id));
        errdefer alloc.free(table_name);
        const start_key = try alloc.dupe(u8, range.start_key);
        errdefer alloc.free(start_key);
        const end_key = if (range.end_key) |value| try alloc.dupe(u8, value) else null;
        errdefer if (end_key) |value| alloc.free(value);
        const state = try alloc.dupe(u8, if (group) |status_value| rangeState(status_value) else "unknown");
        ranges[i] = .{
            .group_id = range.group_id,
            .range_id = range.range_id,
            .table_id = range.table_id,
            .table_name = table_name,
            .start_key = start_key,
            .end_key = end_key,
            .doc_identity_shard_id = range.doc_identity_shard_id,
            .doc_identity_range_id = range.doc_identity_range_id,
            .state = state,
            .leader_data_id = if (group) |status_value| if (status_value.leader_known) status_value.leader_store_id else null else null,
            .voter_count = if (group) |status_value| status_value.voter_count else 0,
            .doc_count = if (group) |status_value| status_value.doc_count else 0,
            .disk_bytes = if (group) |status_value| status_value.disk_bytes else 0,
            .empty = if (group) |status_value| status_value.empty else true,
        };
        initialized += 1;
    }
    return ranges;
}

fn cloneReplicaStatuses(
    alloc: std.mem.Allocator,
    placement_intents: []const raft_reconciler.PlacementIntent,
) ![]DataReplicaStatus {
    const replicas = try alloc.alloc(DataReplicaStatus, placement_intents.len);
    var initialized: usize = 0;
    errdefer {
        for (replicas[0..initialized]) |*replica| replica.deinit(alloc);
        alloc.free(replicas);
    }
    for (placement_intents, 0..) |intent, i| {
        const peer_node_ids = try alloc.dupe(u64, intent.peer_node_ids);
        replicas[i] = .{
            .group_id = intent.record.group_id,
            .data_id = intent.store_id,
            .node_id = intent.record.local_node_id,
            .replica_id = intent.record.replica_id,
            .peer_node_ids = peer_node_ids,
        };
        initialized += 1;
    }
    return replicas;
}

fn cloneGroupStatuses(
    alloc: std.mem.Allocator,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
) ![]DataGroupStatus {
    const groups = try alloc.alloc(DataGroupStatus, merged_group_statuses.len);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |*group| group.deinit(alloc);
        alloc.free(groups);
    }
    for (merged_group_statuses, 0..) |group, i| {
        const doc_identity_lifecycle = try alloc.dupe(u8, group.doc_identity_lifecycle);
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
            .doc_identity_lifecycle = doc_identity_lifecycle,
            .doc_count = group.doc_count,
            .disk_bytes = group.disk_bytes,
            .empty = group.empty,
        };
        initialized += 1;
    }
    return groups;
}

pub fn applySecretStoreHealth(alloc: std.mem.Allocator, status: *ClusterStatus, health: common_secrets.ReloadHealth) !void {
    if (status.secret_store) |*previous| previous.deinit(alloc);
    status.secret_store = .{
        .generation = health.generation,
        .supports_source_generation = health.supports_source_generation,
        .source_generation = if (health.source_generation) |generation|
            try std.fmt.allocPrint(alloc, "{x}", .{generation})
        else
            null,
        .last_reload_failed = health.last_reload_failed,
        .stale = health.stale_snapshot,
        .reload_successes = health.reload_successes,
        .reload_failures = health.reload_failures,
    };
}

pub fn applyRuntimeConfigHealth(
    alloc: std.mem.Allocator,
    status: *ClusterStatus,
    health: @import("antfly_scraping").RemoteContentConfig.RuntimeHealth,
) !void {
    if (status.runtime_config) |*previous| previous.deinit(alloc);
    status.runtime_config = .{
        .generation = health.generation,
        .hash = try std.fmt.allocPrint(alloc, "{x}", .{health.hash}),
        .last_reload_failed = health.last_reload_failed,
        .stale = health.stale_snapshot,
        .reload_successes = health.reload_successes,
        .reload_failures = health.reload_failures,
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
    const alloc = std.testing.allocator;
    var status = ClusterStatus{ .health = .healthy };
    defer status.deinit(alloc);
    var source_generation = @as([32]u8, @splat(0));
    source_generation[0] = 0xab;
    try applySecretStoreHealth(alloc, &status, .{
        .generation = 7,
        .content_hash = @as([32]u8, @splat(0)),
        .supports_source_generation = true,
        .source_generation = source_generation,
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
    try std.testing.expectEqual(@as(u64, 7), secret_store.generation);
    try std.testing.expect(secret_store.supports_source_generation);
    try std.testing.expectEqualStrings("ab00000000000000000000000000000000000000000000000000000000000000", secret_store.source_generation.?);
    try std.testing.expect(secret_store.last_reload_failed);
}

test "generationless secret store status serializes a null source generation" {
    const status: ClusterTopology = .{
        .health = .healthy,
        .secret_store = .{
            .supports_source_generation = true,
            .source_generation = null,
        },
    };
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, status, .{});
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"supports_source_generation\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"source_generation\":null") != null);
}

test "cluster topology preserves deployment index capabilities" {
    const alloc = std.testing.allocator;
    var status = ClusterStatus{
        .health = .healthy,
        .deployment_mode = .serverless,
        .index_capabilities = .{ .artifact_sources = false, .artifact_sources_state = .unsupported },
    };
    defer status.deinit(alloc);
    var topology = try topologyFromStatus(alloc, status);
    defer topology.deinit(alloc);

    try std.testing.expect(!topology.index_capabilities.artifact_sources);
    try std.testing.expectEqual(ArtifactSourcesCapabilityState.unsupported, topology.index_capabilities.artifact_sources_state);
    const encoded = try std.json.Stringify.valueAlloc(alloc, topology, .{});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"artifact_sources\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"artifact_sources_state\":\"unsupported\"") != null);
}

test "secret store status preserves unsupported source generation capability" {
    const alloc = std.testing.allocator;
    var status = ClusterStatus{ .health = .healthy };
    defer status.deinit(alloc);
    try applySecretStoreHealth(alloc, &status, .{
        .generation = 2,
        .content_hash = @as([32]u8, @splat(0)),
        .supports_source_generation = false,
        .entry_count = 2,
        .last_reload_failed = false,
        .stale_snapshot = false,
        .reload_successes = 1,
        .reload_failures = 0,
        .last_success_ns = 1,
        .last_failure_ns = 0,
    });
    const secret_store = status.secret_store orelse return error.TestUnexpectedResult;
    try std.testing.expect(!secret_store.supports_source_generation);
    try std.testing.expect(secret_store.source_generation == null);
}

test "cluster status carries non-secret runtime config generation and hash" {
    const alloc = std.testing.allocator;
    var status = ClusterStatus{ .health = .healthy };
    defer status.deinit(alloc);
    var hash = @as([32]u8, @splat(0));
    hash[0..8].* = .{ 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef };
    try applyRuntimeConfigHealth(alloc, &status, .{
        .generation = 4,
        .hash = hash,
        .last_reload_failed = true,
        .stale_snapshot = true,
        .reload_successes = 4,
        .reload_failures = 1,
    });
    const runtime_config = status.runtime_config orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 4), runtime_config.generation);
    try std.testing.expectEqualStrings("0123456789abcdef000000000000000000000000000000000000000000000000", runtime_config.hash);
    try std.testing.expect(runtime_config.last_reload_failed);
    try std.testing.expect(runtime_config.stale);
}

test "cluster topology owns snapshot data through serialization" {
    const alloc = std.testing.allocator;
    var source_arena = std.heap.ArenaAllocator.init(alloc);
    var source_arena_live = true;
    defer if (source_arena_live) source_arena.deinit();
    const source = source_arena.allocator();

    const tables = try source.alloc(table_manager.TableRecord, 1);
    tables[0] = .{
        .table_id = 7,
        .name = try source.dupe(u8, "docs"),
    };
    const ranges = try source.alloc(table_manager.RangeRecord, 1);
    ranges[0] = .{
        .group_id = 11,
        .range_id = 12,
        .table_id = 7,
        .start_key = try source.dupe(u8, "doc:a"),
        .end_key = try source.dupe(u8, "doc:z"),
    };
    const stores = try source.alloc(table_manager.StoreRecord, 1);
    stores[0] = .{
        .store_id = 3,
        .node_id = 4,
        .api_url = try source.dupe(u8, "http://127.0.0.1:8080"),
        .raft_url = try source.dupe(u8, "http://127.0.0.1:9021"),
        .role = try source.dupe(u8, "data"),
        .health_class = try source.dupe(u8, "healthy"),
        .failure_domain = try source.dupe(u8, "rack-a"),
    };
    const placement_intents = try source.alloc(raft_reconciler.PlacementIntent, 1);
    placement_intents[0] = .{
        .record = .{ .group_id = 11, .replica_id = 5, .local_node_id = 4 },
        .store_id = 3,
        .peer_node_ids = try source.dupe(u64, &.{ 4, 8 }),
    };
    const merged_group_statuses = try source.alloc(metadata_reconciler.MergedGroupStatus, 1);
    merged_group_statuses[0] = .{
        .group_id = 11,
        .leader_known = true,
        .leader_store_id = 3,
        .voter_count_known = true,
        .voter_count = 2,
        .healthy_voter_reports = 2,
        .doc_identity_lifecycle = try source.dupe(u8, "ready"),
    };
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables,
        .ranges = ranges,
        .stores = stores,
        .placement_intents = placement_intents,
        .split_transitions = &.{},
        .merge_transitions = &.{},
        .merged_group_statuses = merged_group_statuses,
    };

    var topology = try topologyFromStatusAndSnapshot(
        alloc,
        .{ .health = .healthy },
        &snapshot,
    );
    defer topology.deinit(alloc);
    try std.testing.expect(topology.data.nodes[0].api_url.ptr != stores[0].api_url.ptr);
    try std.testing.expect(topology.data.ranges[0].table_name.ptr != tables[0].name.ptr);
    try std.testing.expect(topology.data.ranges[0].start_key.ptr != ranges[0].start_key.ptr);
    try std.testing.expect(topology.data.replicas[0].peer_node_ids.ptr != placement_intents[0].peer_node_ids.ptr);
    try std.testing.expect(topology.data.groups[0].doc_identity_lifecycle.ptr != merged_group_statuses[0].doc_identity_lifecycle.ptr);

    source_arena.deinit();
    source_arena_live = false;

    try std.testing.expectEqualStrings("http://127.0.0.1:8080", topology.data.nodes[0].api_url);
    try std.testing.expectEqualStrings("http://127.0.0.1:9021", topology.data.nodes[0].raft_url);
    try std.testing.expectEqualStrings("data", topology.data.nodes[0].role);
    try std.testing.expectEqualStrings("healthy", topology.data.nodes[0].state);
    try std.testing.expectEqualStrings("healthy", topology.data.nodes[0].health_class);
    try std.testing.expectEqualStrings("rack-a", topology.data.nodes[0].failure_domain);
    try std.testing.expectEqualStrings("docs", topology.data.ranges[0].table_name);
    try std.testing.expectEqualStrings("doc:a", topology.data.ranges[0].start_key);
    try std.testing.expectEqualStrings("doc:z", topology.data.ranges[0].end_key.?);
    try std.testing.expectEqualStrings("healthy", topology.data.ranges[0].state);
    try std.testing.expectEqualSlices(u64, &.{ 4, 8 }, topology.data.replicas[0].peer_node_ids);
    try std.testing.expectEqualStrings("ready", topology.data.groups[0].doc_identity_lifecycle);

    const encoded = try std.json.Stringify.valueAlloc(alloc, topology, .{});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"api_url\":\"http://127.0.0.1:8080\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"role\":\"data\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"state\":\"healthy\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"health_class\":\"healthy\"") != null);

    const AllocationRunner = struct {
        fn run(failing_alloc: std.mem.Allocator) !void {
            var source_tables = [_]table_manager.TableRecord{.{ .table_id = 7, .name = "docs" }};
            var source_ranges = [_]table_manager.RangeRecord{.{
                .group_id = 11,
                .range_id = 12,
                .table_id = 7,
                .start_key = "doc:a",
                .end_key = "doc:z",
            }};
            var source_stores = [_]table_manager.StoreRecord{.{
                .store_id = 3,
                .node_id = 4,
                .api_url = "http://127.0.0.1:8080",
                .raft_url = "http://127.0.0.1:9021",
                .role = "data",
                .health_class = "healthy",
                .failure_domain = "rack-a",
            }};
            var source_placements = [_]raft_reconciler.PlacementIntent{.{
                .record = .{ .group_id = 11, .replica_id = 5, .local_node_id = 4 },
                .store_id = 3,
                .peer_node_ids = &.{ 4, 8 },
            }};
            var source_groups = [_]metadata_reconciler.MergedGroupStatus{.{
                .group_id = 11,
                .doc_identity_lifecycle = "ready",
            }};
            const source_snapshot: metadata_api.AdminSnapshot = .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &source_tables,
                .ranges = &source_ranges,
                .stores = &source_stores,
                .placement_intents = &source_placements,
                .split_transitions = &.{},
                .merge_transitions = &.{},
                .merged_group_statuses = &source_groups,
            };
            var owned = try topologyFromStatusAndSnapshot(
                failing_alloc,
                .{ .health = .healthy },
                &source_snapshot,
            );
            defer owned.deinit(failing_alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{});
}
