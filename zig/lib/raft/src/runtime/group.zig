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
    const max_tracked_proposal_receipts: usize = 4096;

    const ProposalReceiptKey = struct {
        term: core.types.Term,
        index: core.types.Index,
    };

    const ProposalReceiptProof = struct {
        observed_term: ?core.types.Term = null,
        waiters: usize = 0,
    };

    alloc: std.mem.Allocator,
    cfg: GroupConfig,
    raw_node: core.RawNode,
    tracked_proposal_receipts: std.AutoHashMapUnmanaged(ProposalReceiptKey, ProposalReceiptProof) = .empty,

    pub fn validateConfig(cfg: GroupConfig) !void {
        if (cfg.group_id == 0) return error.InvalidGroupId;
        if (cfg.local_node_id == 0) return error.InvalidLocalNodeId;
        if (cfg.raft_config.group_id != cfg.group_id) return error.GroupIdMismatch;
        if (cfg.raft_config.id != cfg.local_node_id) return error.LocalNodeIdMismatch;
        try cfg.raft_config.validate();
    }

    pub fn init(alloc: std.mem.Allocator, cfg: GroupConfig) !Group {
        try validateConfig(cfg);

        var owned_cfg = cfg;
        owned_cfg.raft_config = cfg.raft_config.withNormalizedDefaults();
        const owned_peers = try alloc.dupe(core.types.NodeId, owned_cfg.raft_config.peers);
        errdefer alloc.free(owned_peers);
        owned_cfg.raft_config.peers = owned_peers;

        return .{
            .alloc = alloc,
            .cfg = owned_cfg,
            .raw_node = try core.RawNode.init(alloc, owned_cfg.raft_config, cfg.storage),
        };
    }

    pub fn deinit(self: *Group) void {
        self.tracked_proposal_receipts.deinit(self.alloc);
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

    /// Catalog-backed `ensureReplica` is idempotent only for the descriptor
    /// that originally admitted this live group. Membership evolution remains
    /// a Raft operation; it must not be smuggled in by overwriting the catalog.
    /// `applied` is deliberately excluded because durable providers refresh it
    /// from storage while the live group's bootstrap value remains unchanged.
    pub fn admissionConfigEql(self: *const Group, cfg: GroupConfig) bool {
        if (self.cfg.group_id != cfg.group_id or self.cfg.local_node_id != cfg.local_node_id)
            return false;
        const lhs = self.cfg.raft_config;
        const rhs = cfg.raft_config.withNormalizedDefaults();
        if (lhs.id != rhs.id or lhs.group_id != rhs.group_id or
            lhs.election_tick != rhs.election_tick or lhs.heartbeat_tick != rhs.heartbeat_tick or
            lhs.random_seed != rhs.random_seed or
            lhs.max_size_per_msg != rhs.max_size_per_msg or
            lhs.max_committed_size_per_ready != rhs.max_committed_size_per_ready or
            lhs.max_inflight_msgs != rhs.max_inflight_msgs or
            lhs.max_inflight_bytes != rhs.max_inflight_bytes or
            lhs.max_uncommitted_entries_size != rhs.max_uncommitted_entries_size or
            lhs.async_storage_writes != rhs.async_storage_writes or
            lhs.check_quorum != rhs.check_quorum or lhs.pre_vote != rhs.pre_vote or
            lhs.step_down_on_removal != rhs.step_down_on_removal or
            lhs.disable_proposal_forwarding != rhs.disable_proposal_forwarding or
            lhs.disable_conf_change_validation != rhs.disable_conf_change_validation or
            lhs.read_only_option != rhs.read_only_option or lhs.peers.len != rhs.peers.len)
            return false;
        for (lhs.peers) |peer| {
            if (std.mem.indexOfScalar(core.types.NodeId, rhs.peers, peer) == null) return false;
        }
        return true;
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

    pub fn reportSnapshotFailure(
        self: *Group,
        to: core.types.NodeId,
        leader_term: core.types.Term,
        snapshot_index: core.types.Index,
        snapshot_term: core.types.Term,
        attempt_generation: u64,
    ) bool {
        return self.raw_node.reportSnapshotFailure(to, leader_term, snapshot_index, snapshot_term, attempt_generation);
    }

    pub fn reportSnapshotDelivered(
        self: *Group,
        to: core.types.NodeId,
        leader_term: core.types.Term,
        snapshot_index: core.types.Index,
        snapshot_term: core.types.Term,
        attempt_generation: u64,
    ) bool {
        return self.raw_node.reportSnapshotDelivered(to, leader_term, snapshot_index, snapshot_term, attempt_generation);
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
        var accepted_index: ?core.types.Index = null;
        return try self.proposeWithReceipt(data, &accepted_index);
    }

    pub fn proposeWithReceipt(self: *Group, data: []const u8, accepted_index: *?core.types.Index) !void {
        return try self.raw_node.proposeWithReceipt(data, accepted_index);
    }

    pub fn proposeBatchWithReceipt(
        self: *Group,
        payloads: []const []const u8,
        accepted_first_index: *?core.types.Index,
        accepted_last_index: *?core.types.Index,
    ) !void {
        return try self.raw_node.proposeBatchWithReceipt(
            payloads,
            accepted_first_index,
            accepted_last_index,
        );
    }

    /// Reserves the bounded bookkeeping needed before Raft accepts a proposal.
    /// Callers must do this before proposing so an allocation failure can never
    /// strand an accepted receipt without its compaction proof.
    pub fn prepareProposalReceiptTracking(self: *Group) !void {
        if (self.tracked_proposal_receipts.count() >= max_tracked_proposal_receipts) {
            return error.ProposalReceiptCapacityExhausted;
        }
        try self.tracked_proposal_receipts.ensureUnusedCapacity(self.alloc, 1);
    }

    /// Records an accepted receipt using capacity reserved before proposal.
    pub fn trackProposalReceipt(self: *Group, term: core.types.Term, index: core.types.Index) void {
        std.debug.assert(term != 0 and index != 0);
        self.tracked_proposal_receipts.putAssumeCapacity(.{ .term = term, .index = index }, .{});
    }

    pub fn acquireProposalReceipt(self: *Group, term: core.types.Term, index: core.types.Index) bool {
        const proof = self.tracked_proposal_receipts.getPtr(.{ .term = term, .index = index }) orelse return false;
        proof.waiters +|= 1;
        return true;
    }

    pub fn releaseProposalReceipt(self: *Group, term: core.types.Term, index: core.types.Index) void {
        const key: ProposalReceiptKey = .{ .term = term, .index = index };
        const proof = self.tracked_proposal_receipts.getPtr(key) orelse return;
        std.debug.assert(proof.waiters > 0);
        proof.waiters -= 1;
        if (proof.waiters == 0) _ = self.tracked_proposal_receipts.remove(key);
    }

    /// The caller must first establish that `index` is applied. While the log
    /// entry is live this captures its actual term; after local compaction it
    /// returns the proof captured by compactAppliedLogTo.
    pub fn termAtTrackedProposalReceipt(self: *Group, term: core.types.Term, index: core.types.Index) !core.types.Term {
        const key: ProposalReceiptKey = .{ .term = term, .index = index };
        if (self.tracked_proposal_receipts.getPtr(key)) |proof| {
            if (proof.observed_term) |observed| return observed;
            const observed = try self.termAt(index);
            proof.observed_term = observed;
            return observed;
        }
        return try self.termAt(index);
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

    pub fn applyCommittedConfChanges(self: *Group, entries: []const core.Entry) !bool {
        var changed = false;
        for (entries) |entry| {
            switch (entry.entry_type) {
                .conf_change => {
                    const conf_change = try core.ConfChange.decode(entry.data);
                    _ = try self.raw_node.applyConfChange(conf_change);
                    changed = true;
                },
                .conf_change_v2 => {
                    var conf_change = try core.ConfChangeV2.decode(entry.data, self.alloc);
                    defer conf_change.deinit(self.alloc);
                    _ = try self.raw_node.applyConfChangeV2(conf_change);
                    changed = true;
                },
                .normal => {},
            }
        }
        return changed;
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
        var receipts = self.tracked_proposal_receipts.iterator();
        while (receipts.next()) |receipt| {
            if (receipt.key_ptr.index > index or receipt.value_ptr.observed_term != null) continue;
            // A replacement at the same position is just as important to
            // retain as a match: waiters must return superseded, never success.
            receipt.value_ptr.observed_term = self.raw_node.termAt(receipt.key_ptr.index) catch null;
        }
        try self.raw_node.compactAppliedLogTo(index);
    }

    pub fn termAt(self: *Group, index: core.types.Index) !core.types.Term {
        return try self.raw_node.termAt(index);
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
