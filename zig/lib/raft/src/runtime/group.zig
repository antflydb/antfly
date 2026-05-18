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

pub const GroupConfig = struct {
    group_id: core.types.GroupId,
    local_node_id: core.types.NodeId,
    raft_config: core.Config,
    storage: core.Storage,
};

pub const Group = struct {
    alloc: std.mem.Allocator,
    cfg: GroupConfig,
    raw_node: core.RawNode,

    pub fn init(alloc: std.mem.Allocator, cfg: GroupConfig) !Group {
        if (cfg.group_id == 0) return error.InvalidGroupId;
        if (cfg.local_node_id == 0) return error.InvalidLocalNodeId;
        if (cfg.raft_config.group_id != cfg.group_id) return error.GroupIdMismatch;
        if (cfg.raft_config.id != cfg.local_node_id) return error.LocalNodeIdMismatch;

        const owned_peers = try alloc.dupe(core.types.NodeId, cfg.raft_config.peers);
        errdefer alloc.free(owned_peers);

        var owned_cfg = cfg;
        owned_cfg.raft_config.peers = owned_peers;

        return .{
            .alloc = alloc,
            .cfg = owned_cfg,
            .raw_node = try core.RawNode.init(alloc, cfg.raft_config, cfg.storage),
        };
    }

    pub fn deinit(self: *Group) void {
        self.raw_node.deinit();
        if (self.cfg.raft_config.peers.len > 0) self.alloc.free(self.cfg.raft_config.peers);
        self.* = undefined;
    }

    pub fn id(self: *const Group) core.types.GroupId {
        return self.cfg.group_id;
    }

    pub fn localNodeId(self: *const Group) core.types.NodeId {
        return self.cfg.local_node_id;
    }

    pub fn asyncStorageWrites(self: *const Group) bool {
        return self.cfg.raft_config.async_storage_writes;
    }

    pub fn tick(self: *Group) void {
        self.raw_node.tick();
    }

    pub fn step(self: *Group, msg: core.Message) !void {
        return try self.raw_node.step(msg);
    }

    pub fn campaign(self: *Group) !void {
        return try self.raw_node.campaign();
    }

    pub fn transferLeader(self: *Group, transferee: core.types.NodeId) !void {
        return try self.raw_node.transferLeader(transferee);
    }

    pub fn forgetLeader(self: *Group) !void {
        return try self.raw_node.forgetLeader();
    }

    pub fn propose(self: *Group, data: []const u8) !void {
        return try self.raw_node.propose(data);
    }

    pub fn readIndex(self: *Group, request_ctx: []const u8) !void {
        return try self.raw_node.readIndex(request_ctx);
    }

    pub fn proposeConfChange(self: *Group, conf_change: core.ConfChange) !void {
        return try self.raw_node.proposeConfChange(conf_change);
    }

    pub fn proposeConfChangeV2(self: *Group, conf_change: core.ConfChangeV2) !void {
        return try self.raw_node.proposeConfChangeV2(conf_change);
    }

    pub fn hasReady(self: *const Group) bool {
        return self.raw_node.hasReady();
    }

    pub fn ready(self: *Group) core.Ready {
        return self.raw_node.ready();
    }

    pub fn advance(self: *Group, rd: core.Ready) void {
        self.raw_node.advance(rd);
    }

    pub fn status(self: *const Group) core.Status {
        return self.raw_node.status();
    }

    pub fn compactAppliedLogTo(self: *Group, index: core.types.Index) !void {
        try self.raw_node.compactAppliedLogTo(index);
    }
};

test "group wraps a real raw node" {
    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var peers = [_]core.types.NodeId{1};
    var group = try Group.init(std.testing.allocator, .{
        .group_id = 7,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 7,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = storage.storage(),
    });
    defer group.deinit();

    try group.campaign();
    try std.testing.expect(group.hasReady());
    const ready = group.ready();
    try std.testing.expect(!ready.isEmpty());
}
