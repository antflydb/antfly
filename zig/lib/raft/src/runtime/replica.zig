// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const core = @import("../core/mod.zig");
const group_mod = @import("group.zig");
const snapshot_transport_iface = @import("snapshot_transport_iface.zig");

pub const ReplicaRaftConfig = struct {
    peers: []const core.types.NodeId,
    election_tick: u32 = 10,
    heartbeat_tick: u32 = 1,
    random_seed: ?u64 = null,
    applied: core.types.Index = 0,
    max_size_per_msg: usize = std.math.maxInt(usize),
    max_committed_size_per_ready: usize = 0,
    max_inflight_msgs: u32 = 256,
    max_inflight_bytes: usize = 0,
    max_uncommitted_entries_size: usize = std.math.maxInt(usize),
    async_storage_writes: bool = false,
    check_quorum: bool = true,
    pre_vote: bool = true,
    step_down_on_removal: bool = false,
    disable_proposal_forwarding: bool = false,
    disable_conf_change_validation: bool = false,
    read_only_option: core.types.ReadOnlyOption = .safe,
    trace_logger: ?core.TraceLogger = null,

    pub fn clone(self: ReplicaRaftConfig, alloc: std.mem.Allocator) !ReplicaRaftConfig {
        var cloned = self;
        cloned.peers = try alloc.dupe(core.types.NodeId, self.peers);
        return cloned;
    }

    pub fn deinit(self: *ReplicaRaftConfig, alloc: std.mem.Allocator) void {
        if (self.peers.len > 0) alloc.free(self.peers);
        self.* = undefined;
    }

    pub fn fromConfig(alloc: std.mem.Allocator, cfg: core.Config) !ReplicaRaftConfig {
        return .{
            .peers = try alloc.dupe(core.types.NodeId, cfg.peers),
            .election_tick = cfg.election_tick,
            .heartbeat_tick = cfg.heartbeat_tick,
            .random_seed = cfg.random_seed,
            .applied = cfg.applied,
            .max_size_per_msg = cfg.max_size_per_msg,
            .max_committed_size_per_ready = cfg.max_committed_size_per_ready,
            .max_inflight_msgs = cfg.max_inflight_msgs,
            .max_inflight_bytes = cfg.max_inflight_bytes,
            .max_uncommitted_entries_size = cfg.max_uncommitted_entries_size,
            .async_storage_writes = cfg.async_storage_writes,
            .check_quorum = cfg.check_quorum,
            .pre_vote = cfg.pre_vote,
            .step_down_on_removal = cfg.step_down_on_removal,
            .disable_proposal_forwarding = cfg.disable_proposal_forwarding,
            .disable_conf_change_validation = cfg.disable_conf_change_validation,
            .read_only_option = cfg.read_only_option,
            .trace_logger = cfg.trace_logger,
        };
    }

    pub fn toConfig(self: ReplicaRaftConfig, group_id: core.types.GroupId, local_node_id: core.types.NodeId) core.Config {
        return .{
            .id = local_node_id,
            .group_id = group_id,
            .peers = self.peers,
            .election_tick = self.election_tick,
            .heartbeat_tick = self.heartbeat_tick,
            .random_seed = self.random_seed,
            .applied = self.applied,
            .max_size_per_msg = self.max_size_per_msg,
            .max_committed_size_per_ready = self.max_committed_size_per_ready,
            .max_inflight_msgs = self.max_inflight_msgs,
            .max_inflight_bytes = self.max_inflight_bytes,
            .max_uncommitted_entries_size = self.max_uncommitted_entries_size,
            .async_storage_writes = self.async_storage_writes,
            .check_quorum = self.check_quorum,
            .pre_vote = self.pre_vote,
            .step_down_on_removal = self.step_down_on_removal,
            .disable_proposal_forwarding = self.disable_proposal_forwarding,
            .disable_conf_change_validation = self.disable_conf_change_validation,
            .read_only_option = self.read_only_option,
            .trace_logger = self.trace_logger,
        };
    }
};

pub const ReplicaBootstrap = union(enum) {
    empty,
    persisted,
    fetch_snapshot: SnapshotBootstrap,
};

pub const SnapshotBootstrap = struct {
    from: core.types.NodeId,
    term: core.types.Term = 0,
    locator: snapshot_transport_iface.SnapshotLocator,
    fetch_immediately: bool = true,

    pub fn clone(self: SnapshotBootstrap, alloc: std.mem.Allocator) !SnapshotBootstrap {
        return .{
            .from = self.from,
            .term = self.term,
            .locator = .{
                .snapshot_id = try alloc.dupe(u8, self.locator.snapshot_id),
                .uri = try alloc.dupe(u8, self.locator.uri),
            },
            .fetch_immediately = self.fetch_immediately,
        };
    }

    pub fn deinit(self: *SnapshotBootstrap, alloc: std.mem.Allocator) void {
        if (self.locator.snapshot_id.len > 0) alloc.free(self.locator.snapshot_id);
        if (self.locator.uri.len > 0) alloc.free(self.locator.uri);
        self.* = undefined;
    }
};

pub const ReplicaDescriptor = struct {
    group: group_mod.GroupConfig,
    bootstrap: ReplicaBootstrap = .persisted,
};

pub const ReplicaRecord = struct {
    group_id: core.types.GroupId,
    local_node_id: core.types.NodeId,
    raft: ReplicaRaftConfig,
    bootstrap: ReplicaBootstrap = .persisted,

    pub fn deinit(self: *ReplicaRecord, alloc: std.mem.Allocator) void {
        self.raft.deinit(alloc);
        switch (self.bootstrap) {
            .empty, .persisted => {},
            .fetch_snapshot => |*snapshot| snapshot.deinit(alloc),
        }
        self.* = undefined;
    }

    pub fn clone(self: ReplicaRecord, alloc: std.mem.Allocator) !ReplicaRecord {
        return .{
            .group_id = self.group_id,
            .local_node_id = self.local_node_id,
            .raft = try self.raft.clone(alloc),
            .bootstrap = switch (self.bootstrap) {
                .empty => .empty,
                .persisted => .persisted,
                .fetch_snapshot => |snapshot| .{ .fetch_snapshot = try snapshot.clone(alloc) },
            },
        };
    }

    pub fn fromDescriptor(alloc: std.mem.Allocator, desc: ReplicaDescriptor) !ReplicaRecord {
        return .{
            .group_id = desc.group.group_id,
            .local_node_id = desc.group.local_node_id,
            .raft = try ReplicaRaftConfig.fromConfig(alloc, desc.group.raft_config),
            .bootstrap = switch (desc.bootstrap) {
                .empty => .empty,
                .persisted => .persisted,
                .fetch_snapshot => |snapshot| .{ .fetch_snapshot = try snapshot.clone(alloc) },
            },
        };
    }
};

pub const EnsureReplicaResult = struct {
    created: bool = false,
    resumed: bool = false,
    fetched_snapshot: bool = false,
};

test "replica descriptor compiles" {
    _ = ReplicaRaftConfig;
    _ = ReplicaBootstrap;
    _ = SnapshotBootstrap;
    _ = ReplicaDescriptor;
    _ = ReplicaRecord;
    _ = EnsureReplicaResult;
}
