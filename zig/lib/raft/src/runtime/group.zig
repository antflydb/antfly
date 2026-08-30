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

/// Stable identity of one locally hosted replica. Desired membership is
/// deliberately absent: voter, learner, and transport topology evolve while
/// this identity remains installed.
pub const ReplicaIdentity = struct {
    group_id: core.types.GroupId,
    local_node_id: core.types.NodeId,
};

/// Restart-scoped Raft behavior captured when a live group is admitted.
/// Bootstrap position, membership, and diagnostic hooks are intentionally not
/// part of this value: they are either durable observed state or independently
/// mutable control-plane state.
pub const ReplicaRuntimePolicy = struct {
    election_tick: u32,
    heartbeat_tick: u32,
    random_seed: ?u64,
    max_size_per_msg: usize,
    max_committed_size_per_ready: usize,
    max_inflight_msgs: u32,
    max_inflight_bytes: usize,
    max_uncommitted_entries_size: usize,
    async_storage_writes: bool,
    check_quorum: bool,
    pre_vote: bool,
    step_down_on_removal: bool,
    disable_proposal_forwarding: bool,
    disable_conf_change_validation: bool,
    read_only_option: core.types.ReadOnlyOption,

    pub fn fromConfig(cfg: core.Config) ReplicaRuntimePolicy {
        const normalized = cfg.withNormalizedDefaults();
        return .{
            .election_tick = normalized.election_tick,
            .heartbeat_tick = normalized.heartbeat_tick,
            .random_seed = normalized.random_seed,
            .max_size_per_msg = normalized.max_size_per_msg,
            .max_committed_size_per_ready = normalized.max_committed_size_per_ready,
            .max_inflight_msgs = normalized.max_inflight_msgs,
            .max_inflight_bytes = normalized.max_inflight_bytes,
            .max_uncommitted_entries_size = normalized.max_uncommitted_entries_size,
            .async_storage_writes = normalized.async_storage_writes,
            .check_quorum = normalized.check_quorum,
            .pre_vote = normalized.pre_vote,
            .step_down_on_removal = normalized.step_down_on_removal,
            .disable_proposal_forwarding = normalized.disable_proposal_forwarding,
            .disable_conf_change_validation = normalized.disable_conf_change_validation,
            .read_only_option = normalized.read_only_option,
        };
    }

    pub fn firstConflict(
        self: ReplicaRuntimePolicy,
        desired: ReplicaRuntimePolicy,
    ) ?ReplicaRuntimePolicyField {
        inline for (std.meta.fields(ReplicaRuntimePolicy)) |field| {
            if (!std.meta.eql(@field(self, field.name), @field(desired, field.name)))
                return @field(ReplicaRuntimePolicyField, field.name);
        }
        return null;
    }
};

pub const ReplicaRuntimePolicyField = std.meta.FieldEnum(ReplicaRuntimePolicy);

pub const ReplicaAdmissionConflict = union(enum) {
    local_node_id: struct {
        installed: core.types.NodeId,
        desired: core.types.NodeId,
    },
    runtime_policy: ReplicaRuntimePolicyField,

    pub fn fieldName(self: ReplicaAdmissionConflict) []const u8 {
        return switch (self) {
            .local_node_id => "local_node_id",
            .runtime_policy => |field| @tagName(field),
        };
    }
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

    pub fn identity(self: *const Group) ReplicaIdentity {
        return .{ .group_id = self.cfg.group_id, .local_node_id = self.cfg.local_node_id };
    }

    pub fn runtimePolicy(self: *const Group) ReplicaRuntimePolicy {
        return .fromConfig(self.cfg.raft_config);
    }

    /// Admission idempotence covers stable identity and restart-scoped policy
    /// only. `raft_config.peers` is a bootstrap/transport reachability input;
    /// live membership is authoritative in ConfState and changes exclusively
    /// through committed ConfChange entries.
    pub fn admissionConflict(self: *const Group, cfg: GroupConfig) ?ReplicaAdmissionConflict {
        if (self.cfg.local_node_id != cfg.local_node_id) return .{ .local_node_id = .{
            .installed = self.cfg.local_node_id,
            .desired = cfg.local_node_id,
        } };
        if (self.runtimePolicy().firstConflict(.fromConfig(cfg.raft_config))) |field|
            return .{ .runtime_policy = field };
        return null;
    }

    pub fn admissionConfigEql(self: *const Group, cfg: GroupConfig) bool {
        return self.admissionConflict(cfg) == null;
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
