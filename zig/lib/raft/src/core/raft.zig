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
const types = @import("types.zig");
const message = @import("message.zig");
const logger_mod = @import("logger.zig");
const random_mod = @import("random.zig");
const storage_mod = @import("storage.zig");
const log_mod = @import("log.zig");
const ready_mod = @import("ready.zig");

const transfer_campaign_context = "campaign_transfer";

const Inflight = struct {
    index: types.Index,
    bytes: usize,
};

const VoteState = enum {
    unknown,
    granted,
    rejected,
};

const PendingRead = struct {
    index: types.Index,
    requester: types.NodeId,
    context: []u8,
    acks: []bool,

    fn deinit(self: *PendingRead, alloc: std.mem.Allocator) void {
        if (self.context.len > 0) alloc.free(self.context);
        if (self.acks.len > 0) alloc.free(self.acks);
        self.* = undefined;
    }
};

pub const Config = struct {
    id: types.NodeId,
    group_id: types.GroupId,
    peers: []const types.NodeId,
    election_tick: u32 = 10,
    heartbeat_tick: u32 = 1,
    random_seed: ?u64 = null,
    random_source: ?random_mod.RandomSource = null,
    applied: types.Index = 0,
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
    read_only_option: types.ReadOnlyOption = .safe,
    logger: ?logger_mod.Logger = null,
    trace_logger: ?logger_mod.TraceLogger = null,
};

pub const Raft = struct {
    alloc: std.mem.Allocator,
    cfg: Config,
    storage: storage_mod.Storage,
    logger: logger_mod.Logger,
    trace_logger: ?logger_mod.TraceLogger,
    peers: []types.NodeId,
    log: log_mod.RaftLog,
    soft_state: types.SoftState = .{},
    hard_state: types.HardState = .{},
    prev_soft_state: types.SoftState = .{},
    prev_hard_state: types.HardState = .{},
    election_elapsed: u32 = 0,
    randomized_election_timeout: u32 = 0,
    heartbeat_elapsed: u32 = 0,
    random_source: ?random_mod.RandomSource = null,
    seeded_random: ?random_mod.SplitMix64 = null,
    lead_transferee: ?types.NodeId = null,
    pending_conf_index: types.Index = 0,
    votes: []VoteState,
    progress: []types.Progress,
    inflights: []std.ArrayListUnmanaged(Inflight),
    conf_state: types.ConfState,
    pending_snapshot: ?types.Snapshot = null,
    snapshot_in_progress: bool = false,
    uncommitted_size: usize = 0,
    pending_reads: std.ArrayListUnmanaged(PendingRead) = .empty,
    read_states: std.ArrayListUnmanaged(types.ReadState) = .empty,
    messages: std.ArrayListUnmanaged(message.Message) = .empty,

    pub fn init(alloc: std.mem.Allocator, cfg: Config, storage: storage_mod.Storage) !Raft {
        var normalized_cfg = cfg;
        if (normalized_cfg.id == 0) return error.InvalidNodeId;
        if (message.isLocalStorageThread(normalized_cfg.id)) return error.InvalidLocalNodeId;
        if (normalized_cfg.heartbeat_tick == 0) return error.InvalidHeartbeatTick;
        if (normalized_cfg.election_tick <= normalized_cfg.heartbeat_tick) return error.InvalidElectionTick;
        if (normalized_cfg.max_committed_size_per_ready == 0) {
            normalized_cfg.max_committed_size_per_ready = normalized_cfg.max_size_per_msg;
        }
        if (normalized_cfg.max_uncommitted_entries_size == 0) {
            normalized_cfg.max_uncommitted_entries_size = std.math.maxInt(usize);
        }
        if (normalized_cfg.max_inflight_bytes == 0) {
            normalized_cfg.max_inflight_bytes = std.math.maxInt(usize);
        }
        if (normalized_cfg.logger == null) {
            normalized_cfg.logger = logger_mod.defaultLogger();
        }

        if (normalized_cfg.peers.len == 0) return error.EmptyPeerSet;
        if (normalized_cfg.max_inflight_msgs == 0) return error.InvalidMaxInflightMsgs;
        if (normalized_cfg.max_inflight_bytes < normalized_cfg.max_size_per_msg) {
            return error.InvalidMaxInflightBytes;
        }
        if (normalized_cfg.read_only_option == .lease_based and !normalized_cfg.check_quorum) {
            return error.LeaseBasedReadRequiresCheckQuorum;
        }

        const peers = try alloc.dupe(types.NodeId, normalized_cfg.peers);
        errdefer alloc.free(peers);
        if (peerIndex(peers, normalized_cfg.id) == null) return error.LocalNodeNotInPeerSet;

        const votes = try alloc.alloc(VoteState, peers.len);
        errdefer alloc.free(votes);
        @memset(votes, .unknown);

        const progress = try alloc.alloc(types.Progress, peers.len);
        errdefer alloc.free(progress);
        @memset(progress, .{});

        const inflights = try alloc.alloc(std.ArrayListUnmanaged(Inflight), peers.len);
        errdefer alloc.free(inflights);
        for (inflights) |*queue| queue.* = .empty;

        var raft_log = try log_mod.RaftLog.init(alloc, storage);
        errdefer raft_log.deinit();

        var initial_state = try storage.initialState(alloc);
        defer initial_state.deinit(alloc);
        const hard_state = initial_state.hard_state;
        var initial_snapshot = try storage.snapshot(alloc);
        defer initial_snapshot.deinit(alloc);
        var conf_state = if (initial_state.conf_state.voters.len > 0 or
            initial_state.conf_state.voters_outgoing.len > 0 or
            initial_state.conf_state.learners.len > 0 or
            initial_state.conf_state.learners_next.len > 0 or
            initial_state.conf_state.auto_leave)
            try initial_state.conf_state.clone(alloc)
        else if (initial_snapshot.metadata.conf_state.voters.len > 0 or
            initial_snapshot.metadata.conf_state.voters_outgoing.len > 0 or
            initial_snapshot.metadata.conf_state.learners.len > 0 or
            initial_snapshot.metadata.conf_state.learners_next.len > 0 or
            initial_snapshot.metadata.conf_state.auto_leave)
            try initial_snapshot.metadata.conf_state.clone(alloc)
        else
            try (types.ConfState{ .voters = peers }).clone(alloc);
        errdefer conf_state.deinit(alloc);

        if (hard_state.commit_index > initial_snapshot.metadata.index) {
            const committed_entries = try storage.entries(
                alloc,
                initial_snapshot.metadata.index + 1,
                hard_state.commit_index + 1,
                0,
            );
            defer types.freeEntries(alloc, committed_entries);

            for (committed_entries) |entry| {
                const next_conf_state = switch (entry.entry_type) {
                    .conf_change => blk: {
                        const conf_change = try types.ConfChange.decode(entry.data);
                        break :blk try replayConfChange(alloc, conf_state, .{
                            .change_type = conf_change.change_type,
                            .node_id = conf_change.node_id,
                        });
                    },
                    .conf_change_v2 => blk: {
                        var conf_change = try types.ConfChangeV2.decode(entry.data, alloc);
                        defer conf_change.deinit(alloc);
                        break :blk try replayConfChangeV2(alloc, conf_state, conf_change);
                    },
                    else => null,
                };
                if (next_conf_state) |next| {
                    conf_state.deinit(alloc);
                    conf_state = next;
                }
            }
        }

        if (normalized_cfg.applied > raft_log.applied and normalized_cfg.applied > hard_state.commit_index) {
            return error.InvalidApplied;
        }

        var self = Raft{
            .alloc = alloc,
            .cfg = normalized_cfg,
            .storage = storage,
            .logger = normalized_cfg.logger.?,
            .trace_logger = null,
            .peers = peers,
            .log = raft_log,
            .hard_state = hard_state,
            .prev_hard_state = hard_state,
            .votes = votes,
            .progress = progress,
            .inflights = inflights,
            .conf_state = conf_state,
            .randomized_election_timeout = normalized_cfg.election_tick,
            .random_source = normalized_cfg.random_source,
            .seeded_random = if (normalized_cfg.random_source == null and normalized_cfg.random_seed != null)
                random_mod.SplitMix64.init(normalized_cfg.random_seed.?)
            else
                null,
        };
        self.becomeFollower(hard_state.current_term, null);
        self.prev_soft_state = self.soft_state;
        self.log.commitTo(hard_state.commit_index);
        if (normalized_cfg.applied > self.log.applied) {
            self.log.appliedTo(normalized_cfg.applied);
        }
        self.trace_logger = normalized_cfg.trace_logger;
        self.trace(.init_state, null);
        return self;
    }

    pub fn deinit(self: *Raft) void {
        for (self.messages.items) |*msg| msg.deinit(self.alloc);
        self.messages.deinit(self.alloc);
        for (self.read_states.items) |*read_state| read_state.deinit(self.alloc);
        self.read_states.deinit(self.alloc);
        if (self.pending_snapshot) |*snapshot| snapshot.deinit(self.alloc);
        for (self.pending_reads.items) |*pending_read| pending_read.deinit(self.alloc);
        self.pending_reads.deinit(self.alloc);
        self.conf_state.deinit(self.alloc);
        for (self.inflights) |*queue| queue.deinit(self.alloc);
        self.alloc.free(self.inflights);
        self.alloc.free(self.progress);
        self.alloc.free(self.votes);
        self.alloc.free(self.peers);
        self.log.deinit();
        self.* = undefined;
    }

    pub fn tick(self: *Raft) void {
        switch (self.soft_state.role) {
            .leader => {
                self.election_elapsed += 1;
                if (self.election_elapsed >= self.cfg.election_tick) {
                    self.election_elapsed = 0;
                    if (self.cfg.check_quorum and !self.quorumRecentlyActive()) {
                        self.becomeFollower(self.hard_state.current_term, null);
                        self.clearRecentActive();
                        return;
                    }
                    self.clearRecentActive();
                    if (self.lead_transferee != null) {
                        self.abortLeaderTransfer();
                    }
                }
                self.heartbeat_elapsed += 1;
                if (self.heartbeat_elapsed >= self.cfg.heartbeat_tick) {
                    self.heartbeat_elapsed = 0;
                    self.bcastHeartbeat() catch unreachable;
                }
            },
            else => {
                self.election_elapsed += 1;
                if (self.election_elapsed >= self.randomized_election_timeout) {
                    self.election_elapsed = 0;
                    if (self.isPromotableConsideringCommitted()) {
                        self.startCampaign() catch unreachable;
                    }
                }
            },
        }
    }

    pub fn campaign(self: *Raft) !void {
        if (!self.isPromotableConsideringCommitted()) return error.NotPromotable;
        try self.startCampaignWithContext(&.{});
    }

    pub fn transferLeader(self: *Raft, transferee: types.NodeId) !void {
        try self.step(.{
            .msg_type = .transfer_leader,
            .from = transferee,
            .to = self.cfg.id,
        });
    }

    pub fn step(self: *Raft, msg: message.Message) !void {
        if (msg.to != 0 and msg.to != self.cfg.id and !message.isLocalStorageThread(msg.to)) return;
        self.trace(.receive_message, &msg);
        if (self.shouldIgnoreCampaignMessage(msg)) return;
        const higher_term_pre_vote_reject =
            msg.msg_type == .pre_vote_response and msg.reject and msg.term > self.hard_state.current_term;
        if ((msg.msg_type != .pre_vote and msg.msg_type != .pre_vote_response and msg.term > self.hard_state.current_term) or
            higher_term_pre_vote_reject)
        {
            self.becomeFollower(msg.term, null);
            self.hard_state.voted_for = null;
        }
        if (self.soft_state.role == .leader) {
            self.markRecentActive(msg.from);
        }

        switch (msg.msg_type) {
            .propose => try self.handleProposal(msg),
            .pre_vote => try self.handlePreVote(msg),
            .pre_vote_response => try self.handlePreVoteResponse(msg),
            .request_vote => try self.handleRequestVote(msg),
            .request_vote_response => try self.handleRequestVoteResponse(msg),
            .append_entries => try self.handleAppendEntries(msg),
            .append_entries_response => try self.handleAppendEntriesResponse(msg),
            .heartbeat => try self.handleHeartbeat(msg),
            .heartbeat_response => try self.handleHeartbeatResponse(msg),
            .snapshot => try self.handleSnapshot(msg),
            .snapshot_response => try self.handleSnapshotResponse(msg),
            .transfer_leader => try self.handleTransferLeader(msg),
            .forget_leader => self.handleForgetLeader(),
            .timeout_now => try self.handleTimeoutNow(msg),
            .read_index => try self.handleReadIndexRequest(msg),
            .read_index_response => try self.handleReadIndexResponse(msg),
            .storage_append, .storage_apply => return error.CannotStepLocalStorageMessage,
            .storage_append_response => self.handleStorageAppendResponse(msg),
            .storage_apply_response => self.handleStorageApplyResponse(msg),
        }
    }

    fn shouldIgnoreCampaignMessage(self: *const Raft, msg: message.Message) bool {
        if (!self.cfg.check_quorum) return false;
        if (msg.msg_type != .request_vote and msg.msg_type != .pre_vote) return false;
        if (msg.term <= self.hard_state.current_term) return false;
        if (std.mem.eql(u8, msg.context, transfer_campaign_context)) return false;
        if (self.soft_state.leader_id == null) return false;
        return self.election_elapsed < self.cfg.election_tick;
    }

    fn handleProposal(self: *Raft, msg: message.Message) !void {
        if (self.soft_state.role != .leader) {
            if (self.cfg.disable_proposal_forwarding) return;
            const leader = self.soft_state.leader_id orelse return;
            if (leader == msg.from) return;
            try self.forwardProposal(msg.from, leader, msg.entries);
            return;
        }
        if (self.lead_transferee != null) return;
        if (msg.entries.len == 0) return;

        for (msg.entries) |entry| {
            switch (entry.entry_type) {
                .normal => {},
                .conf_change => {
                    _ = try types.ConfChange.decode(entry.data);
                    try self.validateConfChangeProposal(.auto, 1);
                },
                .conf_change_v2 => {
                    var conf_change = try types.ConfChangeV2.decode(entry.data, self.alloc);
                    defer conf_change.deinit(self.alloc);
                    try self.validateConfChangeProposal(conf_change.transition, conf_change.changes.len);
                },
            }
        }
        if (!self.increaseUncommittedSizeEntries(msg.entries)) return;
        for (msg.entries) |entry| {
            const appended = try self.appendLocalEntryOfTypeUnchecked(entry.entry_type, entry.data);
            switch (entry.entry_type) {
                .normal => {},
                .conf_change, .conf_change_v2 => self.pending_conf_index = appended,
            }
            self.trace(.replicate, null);
        }
        _ = self.maybeCommit();
        try self.bcastAppend();
    }

    pub fn propose(self: *Raft, data: []const u8) !void {
        if (self.soft_state.role != .leader) {
            if (self.cfg.disable_proposal_forwarding) return error.ProposalDropped;
            const leader = self.soft_state.leader_id orelse return error.NotLeader;
            var forwarded = types.Entry{
                .term = 0,
                .index = 0,
                .entry_type = .normal,
                .data = try self.alloc.dupe(u8, data),
            };
            defer forwarded.deinit(self.alloc);
            try self.forwardProposal(self.cfg.id, leader, &.{forwarded});
            return;
        }
        if (self.lead_transferee != null) return error.LeaderTransferInProgress;
        _ = try self.appendLocalEntryOfType(.normal, data);
        self.trace(.replicate, null);
        _ = self.maybeCommit();
        try self.bcastAppend();
    }

    pub fn readIndex(self: *Raft, rctx: []const u8) !void {
        if (self.soft_state.role != .leader) {
            const leader = self.soft_state.leader_id orelse return error.NotLeader;
            try self.send(.{
                .msg_type = .read_index,
                .from = self.cfg.id,
                .to = leader,
                .context = try self.alloc.dupe(u8, rctx),
            });
            return;
        }
        try self.startReadIndex(self.cfg.id, rctx);
    }

    fn startReadIndex(self: *Raft, requester: types.NodeId, rctx: []const u8) !void {
        if (!containsNode(self.conf_state.voters, self.cfg.id)) return error.NotPromotable;
        if (self.conf_state.voters.len == 1 and self.conf_state.voters[0] == self.cfg.id and self.conf_state.voters_outgoing.len == 0) {
            if (requester == self.cfg.id) {
                try self.enqueueReadState(self.log.committed, rctx);
            } else {
                try self.send(.{
                    .msg_type = .read_index_response,
                    .from = self.cfg.id,
                    .to = requester,
                    .log_index = self.log.committed,
                    .context = try self.alloc.dupe(u8, rctx),
                });
            }
            return;
        }
        if (self.cfg.read_only_option == .lease_based) {
            if (requester == self.cfg.id) {
                try self.enqueueReadState(self.log.committed, rctx);
            } else {
                try self.send(.{
                    .msg_type = .read_index_response,
                    .from = self.cfg.id,
                    .to = requester,
                    .log_index = self.log.committed,
                    .context = try self.alloc.dupe(u8, rctx),
                });
            }
            return;
        }

        var pending = PendingRead{
            .index = self.log.committed,
            .requester = requester,
            .context = try self.alloc.dupe(u8, rctx),
            .acks = try self.alloc.alloc(bool, self.peers.len),
        };
        errdefer pending.deinit(self.alloc);
        @memset(pending.acks, false);
        if (peerIndex(self.peers, self.cfg.id)) |self_idx| pending.acks[self_idx] = true;

        try self.pending_reads.append(self.alloc, pending);
        try self.bcastHeartbeatWithContext(rctx);
    }

    pub fn proposeConfChange(self: *Raft, conf_change: types.ConfChange) !void {
        if (self.soft_state.role != .leader) {
            if (self.cfg.disable_proposal_forwarding) return error.ProposalDropped;
            const leader = self.soft_state.leader_id orelse return error.NotLeader;
            const encoded = try conf_change.encode(self.alloc);
            defer self.alloc.free(encoded);

            var forwarded = types.Entry{
                .term = 0,
                .index = 0,
                .entry_type = .conf_change,
                .data = try self.alloc.dupe(u8, encoded),
            };
            defer forwarded.deinit(self.alloc);
            try self.forwardProposal(self.cfg.id, leader, &.{forwarded});
            return;
        }
        if (self.lead_transferee != null) return error.LeaderTransferInProgress;
        try self.validateConfChangeProposal(.auto, 1);
        const encoded = try conf_change.encode(self.alloc);
        defer self.alloc.free(encoded);

        self.pending_conf_index = try self.appendLocalEntryOfType(.conf_change, encoded);
        self.trace(.replicate, null);
        _ = self.maybeCommit();
        try self.bcastAppend();
    }

    pub fn proposeConfChangeV2(self: *Raft, conf_change: types.ConfChangeV2) !void {
        if (self.soft_state.role != .leader) {
            if (self.cfg.disable_proposal_forwarding) return error.ProposalDropped;
            const leader = self.soft_state.leader_id orelse return error.NotLeader;
            const encoded = try conf_change.encode(self.alloc);
            defer self.alloc.free(encoded);

            var forwarded = types.Entry{
                .term = 0,
                .index = 0,
                .entry_type = .conf_change_v2,
                .data = try self.alloc.dupe(u8, encoded),
            };
            defer forwarded.deinit(self.alloc);
            try self.forwardProposal(self.cfg.id, leader, &.{forwarded});
            return;
        }
        if (self.lead_transferee != null) return error.LeaderTransferInProgress;
        try self.validateConfChangeProposal(conf_change.transition, conf_change.changes.len);
        const encoded = try conf_change.encode(self.alloc);
        defer self.alloc.free(encoded);

        self.pending_conf_index = try self.appendLocalEntryOfType(.conf_change_v2, encoded);
        self.trace(.replicate, null);
        _ = self.maybeCommit();
        try self.bcastAppend();
    }

    pub fn hasReady(self: *const Raft) bool {
        if (!types.SoftState.eql(self.soft_state, self.prev_soft_state)) return true;
        if (!types.HardState.eql(self.hard_state, self.prev_hard_state)) return true;
        if (self.log.unstableEntries().len > 0) return true;
        if (self.pending_snapshot != null) return true;
        if (self.log.nextCommittedEntries().len > 0) return true;
        if (self.read_states.items.len > 0) return true;
        return self.messages.items.len > 0;
    }

    pub fn ready(self: *Raft) ready_mod.Ready {
        const rd: ready_mod.Ready = .{
            .soft_state = if (!types.SoftState.eql(self.soft_state, self.prev_soft_state)) self.soft_state else null,
            .hard_state = if (!types.HardState.eql(self.hard_state, self.prev_hard_state)) self.hard_state else null,
            .snapshot = if (self.pending_snapshot) |snapshot| snapshot else null,
            .entries = self.log.unstableEntries(),
            .committed_entries = self.log.nextCommittedEntriesMax(self.cfg.max_committed_size_per_ready),
            .read_states = self.read_states.items,
            .messages = self.messages.items,
        };
        self.trace(.ready, null);
        return rd;
    }

    pub fn advance(self: *Raft, rd: ready_mod.Ready) void {
        if (self.cfg.async_storage_writes) @panic("advance must not be used when async_storage_writes is enabled");
        if (rd.soft_state != null) self.prev_soft_state = self.soft_state;
        if (rd.hard_state != null) self.prev_hard_state = self.hard_state;
        if (rd.snapshot != null and self.pending_snapshot != null) {
            if (self.pending_snapshot) |*snapshot| snapshot.deinit(self.alloc);
            self.pending_snapshot = null;
        }
        if (rd.entries.len > 0) self.log.stableTo(rd.entries[rd.entries.len - 1].index);
        if (rd.committed_entries.len > 0) {
            self.log.appliedTo(rd.committed_entries[rd.committed_entries.len - 1].index);
            self.reduceUncommittedSizeEntries(rd.committed_entries);
        }
        if (rd.read_states.len > 0) {
            for (self.read_states.items[0..rd.read_states.len]) |*read_state| read_state.deinit(self.alloc);
            std.mem.copyForwards(
                types.ReadState,
                self.read_states.items[0 .. self.read_states.items.len - rd.read_states.len],
                self.read_states.items[rd.read_states.len..],
            );
            self.read_states.shrinkRetainingCapacity(self.read_states.items.len - rd.read_states.len);
        }
        for (self.messages.items[0..rd.messages.len]) |*msg| msg.deinit(self.alloc);
        std.mem.copyForwards(
            message.Message,
            self.messages.items[0 .. self.messages.items.len - rd.messages.len],
            self.messages.items[rd.messages.len..],
        );
        self.messages.shrinkRetainingCapacity(self.messages.items.len - rd.messages.len);
    }

    pub fn status(self: *const Raft) types.Status {
        var votes_granted: usize = 0;
        var votes_rejected: usize = 0;
        var votes_unknown: usize = 0;
        for (self.votes) |vote| switch (vote) {
            .granted => votes_granted += 1,
            .rejected => votes_rejected += 1,
            .unknown => votes_unknown += 1,
        };
        return .{
            .id = self.cfg.id,
            .group_id = self.cfg.group_id,
            .soft = self.soft_state,
            .hard = self.hard_state,
            .conf_state = self.conf_state,
            .last_index = self.log.lastIndex(),
            .applied_index = self.log.applied,
            .election_elapsed = self.election_elapsed,
            .randomized_election_timeout = self.randomized_election_timeout,
            .votes_granted = votes_granted,
            .votes_rejected = votes_rejected,
            .votes_unknown = votes_unknown,
        };
    }

    pub fn noteReady(self: *const Raft) void {
        self.trace(.ready, null);
    }

    pub fn compactAppliedLogTo(self: *Raft, index: types.Index) !void {
        try self.log.compactTo(index);
    }

    pub fn applyConfChange(self: *Raft, conf_change: types.ConfChange) !types.ConfState {
        const next = (try replayConfChange(self.alloc, self.conf_state, .{
            .change_type = conf_change.change_type,
            .node_id = conf_change.node_id,
        })) orelse return self.conf_state;
        try self.applyRuntimeConfState(next);
        self.maybeStepDownOnRemoval();
        return self.conf_state;
    }

    pub fn applyConfChangeV2(self: *Raft, conf_change: types.ConfChangeV2) !types.ConfState {
        const should_append_auto_leave =
            conf_change.transition == .joint_implicit and self.soft_state.role == .leader;
        const next = (try replayConfChangeV2(self.alloc, self.conf_state, conf_change)) orelse return self.conf_state;
        try self.applyRuntimeConfState(next);
        if (should_append_auto_leave) {
            try self.appendAutoLeaveJointEntry();
        }
        self.maybeStepDownOnRemoval();
        return self.conf_state;
    }

    fn startCampaign(self: *Raft) !void {
        try self.startCampaignWithContext(&.{});
    }

    fn startCampaignWithContext(self: *Raft, context: []const u8) !void {
        if (self.cfg.pre_vote) {
            try self.startPreCampaignWithContext(context);
            return;
        }
        try self.startElectionWithContext(context);
    }

    fn startPreCampaign(self: *Raft) !void {
        try self.startPreCampaignWithContext(&.{});
    }

    fn startPreCampaignWithContext(self: *Raft, context: []const u8) !void {
        if (!self.isPromotableConsideringCommitted()) return error.NotPromotable;
        self.becomePreCandidate();
        try self.recordVote(self.cfg.id, true);
        if (self.conf_state.voters.len == 1 and
            self.conf_state.voters[0] == self.cfg.id and
            self.conf_state.voters_outgoing.len == 0)
        {
            try self.startElectionWithContext(context);
            return;
        }

        const next_term = self.hard_state.current_term + 1;
        for (self.peers) |peer| {
            if (peer == self.cfg.id or !self.isVotingMember(peer)) continue;
            try self.send(.{
                .msg_type = .pre_vote,
                .from = self.cfg.id,
                .to = peer,
                .term = next_term,
                .log_index = self.log.lastIndex(),
                .log_term = self.log.term(self.log.lastIndex()) orelse 0,
                .context = try self.alloc.dupe(u8, context),
            });
        }
    }

    fn handleStorageAppendResponse(self: *Raft, msg: message.Message) void {
        if (msg.term != 0 and msg.term != self.hard_state.current_term) return;
        if (msg.log_index > 0) self.log.stableTo(msg.log_index);
        if (msg.snapshot != null) {
            if (self.pending_snapshot) |*snapshot| snapshot.deinit(self.alloc);
            self.pending_snapshot = null;
            self.snapshot_in_progress = false;
        }
    }

    fn handleStorageApplyResponse(self: *Raft, msg: message.Message) void {
        if (msg.entries.len == 0) return;
        self.log.appliedTo(msg.entries[msg.entries.len - 1].index);
        self.reduceUncommittedSizeEntries(msg.entries);
    }

    fn startElection(self: *Raft) !void {
        try self.startElectionWithContext(&.{});
    }

    fn startElectionWithContext(self: *Raft, context: []const u8) !void {
        if (!self.isPromotableConsideringCommitted()) return error.NotPromotable;
        self.becomeCandidate();
        try self.recordVote(self.cfg.id, true);
        if (self.conf_state.voters.len == 1 and
            self.conf_state.voters[0] == self.cfg.id and
            self.conf_state.voters_outgoing.len == 0)
        {
            try self.becomeLeader();
            return;
        }

        for (self.peers) |peer| {
            if (peer == self.cfg.id or !self.isVotingMember(peer)) continue;
            try self.send(.{
                .msg_type = .request_vote,
                .from = self.cfg.id,
                .to = peer,
                .term = self.hard_state.current_term,
                .log_index = self.log.lastIndex(),
                .log_term = self.log.term(self.log.lastIndex()) orelse 0,
                .context = try self.alloc.dupe(u8, context),
            });
        }
    }

    fn becomeFollower(self: *Raft, term: types.Term, leader: ?types.NodeId) void {
        self.soft_state = .{ .leader_id = leader, .role = .follower };
        self.hard_state.current_term = term;
        self.election_elapsed = 0;
        self.resetRandomizedElectionTimeout();
        self.heartbeat_elapsed = 0;
        self.lead_transferee = null;
        self.pending_conf_index = 0;
        self.uncommitted_size = 0;
        self.clearPendingReads();
        self.clearAllInflights();
        @memset(self.votes, .unknown);
        self.trace(.become_follower, null);
    }

    fn becomePreCandidate(self: *Raft) void {
        self.soft_state = .{ .leader_id = null, .role = .pre_candidate };
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.lead_transferee = null;
        self.pending_conf_index = 0;
        self.uncommitted_size = 0;
        self.clearPendingReads();
        self.clearAllInflights();
        @memset(self.votes, .unknown);
        self.trace(.become_pre_candidate, null);
    }

    fn becomeCandidate(self: *Raft) void {
        self.soft_state = .{ .leader_id = null, .role = .candidate };
        self.hard_state.current_term += 1;
        self.hard_state.voted_for = self.cfg.id;
        self.election_elapsed = 0;
        self.resetRandomizedElectionTimeout();
        self.heartbeat_elapsed = 0;
        self.lead_transferee = null;
        self.pending_conf_index = 0;
        self.uncommitted_size = 0;
        self.clearPendingReads();
        self.clearAllInflights();
        @memset(self.votes, .unknown);
        self.trace(.become_candidate, null);
    }

    fn becomeLeader(self: *Raft) !void {
        self.soft_state = .{ .leader_id = self.cfg.id, .role = .leader };
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.resetRandomizedElectionTimeout();
        self.lead_transferee = null;
        self.pending_conf_index = self.log.lastIndex();
        self.uncommitted_size = 0;
        self.clearAllInflights();
        const last_index = self.log.lastIndex();
        for (self.progress, 0..) |*progress, i| {
            if (!self.isReplicationTarget(self.peers[i])) {
                progress.* = .{};
            } else if (self.peers[i] == self.cfg.id) {
                progress.* = .{
                    .match_index = last_index,
                    .next_index = last_index + 1,
                    .state = .replicate,
                    .probe_sent = false,
                    .recent_active = true,
                };
            } else {
                progress.* = .{
                    .match_index = 0,
                    .next_index = last_index + 1,
                    .state = .probe,
                    .probe_sent = false,
                    .recent_active = false,
                };
            }
        }

        self.trace(.become_leader, null);
        _ = try self.appendLocalEntry(&.{});
        _ = self.maybeCommit();
        try self.bcastAppend();
    }

    fn handlePreVote(self: *Raft, msg: message.Message) !void {
        if (msg.term < self.hard_state.current_term + 1) {
            try self.sendPreVoteResponse(msg.from, self.hard_state.current_term, false);
            return;
        }

        const up_to_date = isUpToDate(self.log.lastIndex(), self.log.term(self.log.lastIndex()) orelse 0, msg.log_index, msg.log_term);
        try self.sendPreVoteResponse(msg.from, if (up_to_date) msg.term else self.hard_state.current_term, up_to_date);
    }

    fn handlePreVoteResponse(self: *Raft, msg: message.Message) !void {
        if (self.soft_state.role != .pre_candidate) return;

        try self.recordVote(msg.from, !msg.reject);
        switch (self.tallyVotes()) {
            .won => try self.startElection(),
            .lost => self.becomeFollower(self.hard_state.current_term, null),
            .pending => {},
        }
    }

    fn handleRequestVote(self: *Raft, msg: message.Message) !void {
        if (msg.term < self.hard_state.current_term) {
            try self.sendVoteResponse(msg.from, false);
            return;
        }

        if (msg.term > self.hard_state.current_term) {
            self.becomeFollower(msg.term, null);
            self.hard_state.voted_for = null;
        }

        const can_vote = self.hard_state.voted_for == null or self.hard_state.voted_for.? == msg.from;
        const up_to_date = isUpToDate(self.log.lastIndex(), self.log.term(self.log.lastIndex()) orelse 0, msg.log_index, msg.log_term);
        const granted = can_vote and up_to_date;
        if (granted) {
            self.hard_state.voted_for = msg.from;
            self.election_elapsed = 0;
        }
        try self.sendVoteResponse(msg.from, granted);
    }

    fn handleRequestVoteResponse(self: *Raft, msg: message.Message) !void {
        if (self.soft_state.role != .candidate) return;
        if (msg.term != self.hard_state.current_term) return;

        try self.recordVote(msg.from, !msg.reject);
        switch (self.tallyVotes()) {
            .won => try self.becomeLeader(),
            .lost => self.becomeFollower(self.hard_state.current_term, null),
            .pending => {},
        }
    }

    fn handleAppendEntries(self: *Raft, msg: message.Message) !void {
        if (msg.term < self.hard_state.current_term) {
            try self.sendAppendResponse(msg.from, true, self.log.lastIndex());
            return;
        }

        self.becomeFollower(msg.term, msg.from);

        const maybe_last = try self.log.maybeAppend(msg.log_index, msg.log_term, msg.commit_index, msg.entries);
        if (maybe_last) |last_new_index| {
            self.hard_state.commit_index = self.log.committed;
            try self.sendAppendResponse(msg.from, false, last_new_index);
        } else {
            try self.sendAppendResponse(msg.from, true, self.log.lastIndex());
        }
    }

    fn handleAppendEntriesResponse(self: *Raft, msg: message.Message) !void {
        if (self.soft_state.role != .leader) return;
        const idx = peerIndex(self.peers, msg.from) orelse return;

        if (msg.reject) {
            self.progress[idx].state = .probe;
            self.progress[idx].probe_sent = false;
            self.clearInflights(idx);
            const next_index = if (msg.reject_hint > 0) msg.reject_hint else self.progress[idx].next_index -| 1;
            self.progress[idx].next_index = @max(@as(types.Index, 1), next_index);
            try self.sendAppend(msg.from);
            return;
        }

        const was_probe = self.progress[idx].state == .probe;
        self.progress[idx].state = .replicate;
        self.freeInflightsTo(idx, msg.log_index);
        self.progress[idx].match_index = msg.log_index;
        self.progress[idx].next_index = msg.log_index + 1;
        self.progress[idx].probe_sent = false;
        if (self.maybeCommit()) {
            try self.bcastAppend();
        } else if (was_probe) {
            try self.sendAppend(msg.from);
        }
        if (self.lead_transferee == msg.from and self.progress[idx].match_index == self.log.lastIndex()) {
            try self.sendTimeoutNow(msg.from);
        }
    }

    fn handleHeartbeat(self: *Raft, msg: message.Message) !void {
        if (msg.term < self.hard_state.current_term) {
            try self.send(.{
                .msg_type = .heartbeat_response,
                .from = self.cfg.id,
                .to = msg.from,
                .term = self.hard_state.current_term,
                .reject = true,
            });
            return;
        }

        self.becomeFollower(msg.term, msg.from);
        self.log.commitTo(@min(msg.commit_index, self.log.lastIndex()));
        self.hard_state.commit_index = self.log.committed;
        try self.send(.{
            .msg_type = .heartbeat_response,
            .from = self.cfg.id,
            .to = msg.from,
            .term = self.hard_state.current_term,
            .commit_index = self.log.committed,
            .context = try self.alloc.dupe(u8, msg.context),
        });
    }

    fn handleHeartbeatResponse(self: *Raft, msg: message.Message) !void {
        if (self.soft_state.role != .leader) return;
        if (peerIndex(self.peers, msg.from)) |idx| {
            self.progress[idx].probe_sent = false;
            if (self.progress[idx].state == .probe and self.isReplicationTarget(msg.from)) {
                try self.sendAppend(msg.from);
            }
        }
        if (msg.context.len > 0) {
            try self.handleReadAck(msg.from, msg.context);
        }
    }

    fn handleSnapshot(self: *Raft, msg: message.Message) !void {
        if (msg.term < self.hard_state.current_term) {
            try self.send(.{
                .msg_type = .snapshot_response,
                .from = self.cfg.id,
                .to = msg.from,
                .term = self.hard_state.current_term,
                .reject = true,
            });
            return;
        }

        const snapshot = msg.snapshot orelse return error.MissingSnapshot;
        self.becomeFollower(msg.term, msg.from);
        if (snapshot.metadata.index <= self.log.committed or !confStateContainsTarget(snapshot.metadata.conf_state, self.cfg.id)) {
            if (snapshot.metadata.index <= self.log.committed) {
                self.logf(.warning, "ignoring stale snapshot index={} committed={}", .{ snapshot.metadata.index, self.log.committed });
            } else {
                self.logf(.warning, "ignoring snapshot index={} missing local node {}", .{ snapshot.metadata.index, self.cfg.id });
            }
            try self.send(.{
                .msg_type = .snapshot_response,
                .from = self.cfg.id,
                .to = msg.from,
                .term = self.hard_state.current_term,
                .log_index = self.log.committed,
            });
            return;
        }
        if (self.log.matchTerm(snapshot.metadata.index, snapshot.metadata.term)) {
            self.logf(.info, "fast-forwarding commit to matching snapshot index={} term={}", .{ snapshot.metadata.index, snapshot.metadata.term });
            self.log.commitTo(snapshot.metadata.index);
            self.hard_state.commit_index = self.log.committed;
            try self.send(.{
                .msg_type = .snapshot_response,
                .from = self.cfg.id,
                .to = msg.from,
                .term = self.hard_state.current_term,
                .log_index = self.log.committed,
            });
            return;
        }
        self.log.restore(snapshot);
        self.hard_state.commit_index = snapshot.metadata.index;
        try self.replaceConfState(try snapshot.metadata.conf_state.clone(self.alloc));

        if (self.pending_snapshot) |*pending| pending.deinit(self.alloc);
        self.pending_snapshot = try snapshot.clone(self.alloc);

        try self.send(.{
            .msg_type = .snapshot_response,
            .from = self.cfg.id,
            .to = msg.from,
            .term = self.hard_state.current_term,
            .log_index = snapshot.metadata.index,
        });
    }

    fn handleSnapshotResponse(self: *Raft, msg: message.Message) !void {
        if (self.soft_state.role != .leader) return;
        const idx = peerIndex(self.peers, msg.from) orelse return;

        self.progress[idx].probe_sent = false;
        self.clearInflights(idx);
        if (msg.reject) return;

        self.progress[idx].state = .probe;
        self.progress[idx].match_index = msg.log_index;
        self.progress[idx].next_index = msg.log_index + 1;
        try self.sendAppend(msg.from);
    }

    fn handleTransferLeader(self: *Raft, msg: message.Message) !void {
        switch (self.soft_state.role) {
            .leader => {
                if (!containsNode(self.conf_state.voters, msg.from)) return;
                if (msg.from == self.cfg.id) return;

                if (self.lead_transferee) |current| {
                    if (current == msg.from) return;
                    self.abortLeaderTransfer();
                }

                self.lead_transferee = msg.from;
                self.election_elapsed = 0;

                const idx = peerIndex(self.peers, msg.from) orelse return;
                if (self.progress[idx].match_index == self.log.lastIndex()) {
                    try self.sendTimeoutNow(msg.from);
                } else {
                    try self.sendAppend(msg.from);
                }
            },
            .follower, .candidate, .pre_candidate => {
                const leader = self.soft_state.leader_id orelse return;
                try self.send(.{
                    .msg_type = .transfer_leader,
                    .from = msg.from,
                    .to = leader,
                    .term = self.hard_state.current_term,
                });
            },
        }
    }

    fn handleForgetLeader(self: *Raft) void {
        if (self.cfg.read_only_option == .lease_based) return;
        if (self.soft_state.role != .follower) return;
        self.soft_state.leader_id = null;
    }

    fn handleTimeoutNow(self: *Raft, msg: message.Message) !void {
        _ = msg;
        if (self.soft_state.role != .follower) return;
        if (!self.isPromotableConsideringCommitted()) return;
        try self.startElectionWithContext(transfer_campaign_context);
    }

    fn handleReadIndexRequest(self: *Raft, msg: message.Message) !void {
        if (self.soft_state.role != .leader) {
            const leader = self.soft_state.leader_id orelse return;
            if (leader == msg.from) return;
            try self.send(.{
                .msg_type = .read_index,
                .from = msg.from,
                .to = leader,
                .context = try self.alloc.dupe(u8, msg.context),
            });
            return;
        }
        try self.startReadIndex(msg.from, msg.context);
    }

    fn handleReadIndexResponse(self: *Raft, msg: message.Message) !void {
        if (msg.to != self.cfg.id) return;
        try self.enqueueReadState(msg.log_index, msg.context);
    }

    fn sendVoteResponse(self: *Raft, to: types.NodeId, granted: bool) !void {
        try self.send(.{
            .msg_type = .request_vote_response,
            .from = self.cfg.id,
            .to = to,
            .term = self.hard_state.current_term,
            .reject = !granted,
        });
    }

    fn sendPreVoteResponse(self: *Raft, to: types.NodeId, term: types.Term, granted: bool) !void {
        try self.send(.{
            .msg_type = .pre_vote_response,
            .from = self.cfg.id,
            .to = to,
            .term = term,
            .reject = !granted,
        });
    }

    fn sendAppendResponse(self: *Raft, to: types.NodeId, reject: bool, log_index: types.Index) !void {
        try self.send(.{
            .msg_type = .append_entries_response,
            .from = self.cfg.id,
            .to = to,
            .term = self.hard_state.current_term,
            .log_index = log_index,
            .reject = reject,
            .reject_hint = if (reject) self.log.lastIndex() else 0,
        });
    }

    fn sendTimeoutNow(self: *Raft, to: types.NodeId) !void {
        try self.send(.{
            .msg_type = .timeout_now,
            .from = self.cfg.id,
            .to = to,
            .term = self.hard_state.current_term,
        });
    }

    fn resetRandomizedElectionTimeout(self: *Raft) void {
        self.randomized_election_timeout = self.cfg.election_tick + self.nextElectionJitter();
    }

    fn nextElectionJitter(self: *Raft) u32 {
        if (self.random_source) |random_source| {
            return random_source.nextBelow(self.cfg.election_tick);
        }
        if (self.seeded_random) |*seeded_random| {
            return seeded_random.nextBelow(self.cfg.election_tick);
        }
        return 0;
    }

    fn bcastHeartbeat(self: *Raft) !void {
        try self.bcastHeartbeatWithContext(&.{});
    }

    fn bcastHeartbeatWithContext(self: *Raft, context: []const u8) !void {
        for (self.peers, 0..) |peer, i| {
            if (peer == self.cfg.id or !self.isReplicationTarget(peer)) continue;
            try self.send(.{
                .msg_type = .heartbeat,
                .from = self.cfg.id,
                .to = peer,
                .term = self.hard_state.current_term,
                .commit_index = @min(self.progress[i].match_index, self.log.committed),
                .context = try self.alloc.dupe(u8, context),
            });
        }
    }

    fn bcastAppend(self: *Raft) !void {
        for (self.peers) |peer| {
            if (peer == self.cfg.id or !self.isReplicationTarget(peer)) continue;
            try self.sendAppend(peer);
        }
    }

    fn sendAppend(self: *Raft, to: types.NodeId) !void {
        if (!self.isReplicationTarget(to)) return;
        const idx = peerIndex(self.peers, to) orelse return;
        const next_index = self.progress[idx].next_index;
        if (next_index < self.log.firstIndex()) {
            self.progress[idx].probe_sent = false;
            try self.sendSnapshot(to);
            return;
        }
        const prev_index = next_index -| 1;
        const prev_term = self.log.term(prev_index) orelse 0;
        const msg_size_limit = if (self.cfg.max_size_per_msg == 0) @as(usize, 1) else self.cfg.max_size_per_msg;
        const entries = self.log.entriesFromMax(next_index, msg_size_limit);
        const inflight_bytes = types.entriesApproxEncodedSize(entries);
        if (to != self.cfg.id and
            entries.len > 0 and
            self.progress[idx].state == .replicate and
            (self.inflights[idx].items.len >= self.cfg.max_inflight_msgs or self.inflightBytesFull(idx)))
        {
            return;
        }
        if (to != self.cfg.id and
            entries.len > 0 and
            self.progress[idx].state == .probe and
            self.progress[idx].probe_sent)
        {
            return;
        }
        const sent_last_index = if (entries.len > 0) entries[entries.len - 1].index else prev_index;

        try self.send(.{
            .msg_type = .append_entries,
            .from = self.cfg.id,
            .to = to,
            .term = self.hard_state.current_term,
            .log_index = prev_index,
            .log_term = prev_term,
            .commit_index = self.log.committed,
            .entries = try types.cloneEntries(self.alloc, entries),
        });

        if (entries.len > 0) {
            if (self.progress[idx].state == .replicate) {
                self.progress[idx].next_index = sent_last_index + 1;
                self.inflights[idx].append(self.alloc, .{
                    .index = sent_last_index,
                    .bytes = inflight_bytes,
                }) catch unreachable;
            } else {
                self.progress[idx].probe_sent = true;
            }
        }
    }

    fn sendSnapshot(self: *Raft, to: types.NodeId) !void {
        if (!self.isReplicationTarget(to)) return;
        const idx = peerIndex(self.peers, to) orelse return;
        if (self.progress[idx].probe_sent) return;
        if (!self.progress[idx].recent_active) return;

        var snapshot = try self.storage.snapshot(self.alloc);
        errdefer snapshot.deinit(self.alloc);

        try self.send(.{
            .msg_type = .snapshot,
            .from = self.cfg.id,
            .to = to,
            .term = self.hard_state.current_term,
            .log_index = snapshot.metadata.index,
            .log_term = snapshot.metadata.term,
            .snapshot = snapshot,
        });

        self.progress[idx].probe_sent = true;
    }

    fn maybeCommit(self: *Raft) bool {
        const match_indexes = tryVoterMatchIndexes(self.alloc, self.peers, self.progress, self.conf_state.voters) catch return false;
        defer self.alloc.free(match_indexes);
        std.mem.sort(types.Index, match_indexes, {}, comptime std.sort.asc(types.Index));
        const incoming_idx = match_indexes[match_indexes.len - types.quorum(match_indexes.len)];
        var quorum_idx = incoming_idx;
        if (self.conf_state.voters_outgoing.len > 0) {
            const outgoing_match_indexes = tryVoterMatchIndexes(self.alloc, self.peers, self.progress, self.conf_state.voters_outgoing) catch return false;
            defer self.alloc.free(outgoing_match_indexes);
            std.mem.sort(types.Index, outgoing_match_indexes, {}, comptime std.sort.asc(types.Index));
            const outgoing_idx = outgoing_match_indexes[outgoing_match_indexes.len - types.quorum(outgoing_match_indexes.len)];
            quorum_idx = @min(incoming_idx, outgoing_idx);
        }
        const prev_committed = self.log.committed;
        self.log.commitTo(quorum_idx);
        self.hard_state.commit_index = self.log.committed;
        if (self.log.committed != prev_committed) self.trace(.commit, null);
        return self.log.committed != prev_committed;
    }

    fn appendLocalEntry(self: *Raft, data: []const u8) !types.Index {
        return try self.appendLocalEntryOfType(.normal, data);
    }

    fn appendLocalEntryOfType(self: *Raft, entry_type: types.EntryType, data: []const u8) !types.Index {
        if (!self.increaseUncommittedSizeEntry(entry_type, data)) return error.ProposalDropped;
        return try self.appendLocalEntryOfTypeUnchecked(entry_type, data);
    }

    fn appendLocalEntryOfTypeUnchecked(self: *Raft, entry_type: types.EntryType, data: []const u8) !types.Index {
        const next_index = self.log.lastIndex() + 1;
        const entry = types.Entry{
            .term = self.hard_state.current_term,
            .index = next_index,
            .entry_type = entry_type,
            .data = try self.alloc.dupe(u8, data),
        };
        defer {
            var owned = entry;
            owned.deinit(self.alloc);
        }

        _ = try self.log.appendEntries(&.{entry});
        const self_idx = peerIndex(self.peers, self.cfg.id).?;
        self.progress[self_idx].match_index = self.log.lastIndex();
        self.progress[self_idx].next_index = self.log.lastIndex() + 1;
        return self.log.lastIndex();
    }

    fn increaseUncommittedSizeEntry(self: *Raft, entry_type: types.EntryType, data: []const u8) bool {
        const size = types.payloadSizeForEntry(entry_type, data);
        if (self.uncommitted_size > 0 and size > 0 and self.uncommitted_size + size > self.cfg.max_uncommitted_entries_size) {
            return false;
        }
        self.uncommitted_size += size;
        return true;
    }

    fn increaseUncommittedSizeEntries(self: *Raft, entries: []const types.Entry) bool {
        const size = types.entriesPayloadSize(entries);
        if (self.uncommitted_size > 0 and size > 0 and self.uncommitted_size + size > self.cfg.max_uncommitted_entries_size) {
            return false;
        }
        self.uncommitted_size += size;
        return true;
    }

    fn reduceUncommittedSizeEntries(self: *Raft, entries: []const types.Entry) void {
        const size = types.entriesPayloadSize(entries);
        if (size >= self.uncommitted_size) {
            self.uncommitted_size = 0;
            return;
        }
        self.uncommitted_size -= size;
    }

    fn send(self: *Raft, msg: message.Message) !void {
        self.trace(.send_message, &msg);
        try self.messages.append(self.alloc, msg);
    }

    fn logf(self: *const Raft, level: logger_mod.LogLevel, comptime fmt: []const u8, args: anytype) void {
        const rendered = std.fmt.allocPrint(self.alloc, fmt, args) catch return;
        defer self.alloc.free(rendered);
        self.logger.log(level, rendered);
    }

    fn trace(self: *const Raft, event_type: logger_mod.TraceEventType, msg: ?*const message.Message) void {
        const trace_logger = self.trace_logger orelse return;
        const event = logger_mod.TraceEvent{
            .event_type = event_type,
            .node_id = self.cfg.id,
            .leader_id = self.soft_state.leader_id,
            .role = self.soft_state.role,
            .term = self.hard_state.current_term,
            .vote = self.hard_state.voted_for,
            .commit_index = self.hard_state.commit_index,
            .applied_index = self.log.applied,
            .last_index = self.log.lastIndex(),
            .voters = self.conf_state.voters,
            .voters_outgoing = self.conf_state.voters_outgoing,
            .learners = self.conf_state.learners,
            .learners_next = self.conf_state.learners_next,
            .auto_leave = self.conf_state.auto_leave,
            .message = msg,
        };
        trace_logger.traceEvent(&event);
    }

    fn forwardProposal(
        self: *Raft,
        requester: types.NodeId,
        leader: types.NodeId,
        entries: []const types.Entry,
    ) !void {
        try self.send(.{
            .msg_type = .propose,
            .from = requester,
            .to = leader,
            .entries = try types.cloneEntries(self.alloc, entries),
        });
    }

    fn recordVote(self: *Raft, from: types.NodeId, granted: bool) !void {
        const idx = peerIndex(self.peers, from) orelse return error.UnknownPeer;
        self.votes[idx] = if (granted) .granted else .rejected;
    }

    fn tallyVotes(self: *const Raft) types.VoteResult {
        const incoming = tallyVotesForSet(self.peers, self.votes, self.conf_state.voters);
        if (self.conf_state.voters_outgoing.len == 0) {
            if (incoming.granted >= types.quorum(self.conf_state.voters.len)) return .won;
            if (incoming.rejected >= types.quorum(self.conf_state.voters.len)) return .lost;
            return .pending;
        }

        const outgoing = tallyVotesForSet(self.peers, self.votes, self.conf_state.voters_outgoing);
        if (incoming.granted >= types.quorum(self.conf_state.voters.len) and
            outgoing.granted >= types.quorum(self.conf_state.voters_outgoing.len))
        {
            return .won;
        }
        if (incoming.rejected >= types.quorum(self.conf_state.voters.len) or
            outgoing.rejected >= types.quorum(self.conf_state.voters_outgoing.len))
        {
            return .lost;
        }
        return .pending;
    }

    fn appendAutoLeaveJointEntry(self: *Raft) !void {
        if (self.conf_state.voters_outgoing.len == 0 or !self.conf_state.auto_leave) return;
        const leave = types.ConfChangeV2{};
        const encoded = try leave.encode(self.alloc);
        defer self.alloc.free(encoded);

        self.pending_conf_index = try self.appendLocalEntryOfType(.conf_change_v2, encoded);
        _ = self.maybeCommit();
        try self.bcastAppend();
    }

    fn validateConfChangeProposal(
        self: *const Raft,
        transition: types.ConfChangeTransition,
        change_count: usize,
    ) !void {
        if (self.cfg.disable_conf_change_validation) return;
        if (self.pending_conf_index > self.log.applied) return error.PendingConfChange;

        const already_joint = self.conf_state.voters_outgoing.len > 0;
        const wants_leave_joint = change_count == 0;
        const wants_enter_joint = transition != .auto or change_count > 1;

        if (already_joint and !wants_leave_joint) return error.MustLeaveJointFirst;
        if (!already_joint and wants_leave_joint) return error.NotInJointState;
        if (wants_enter_joint and self.conf_state.voters.len == 0) return error.ZeroVoterJointConfig;
    }

    fn replaceConfState(self: *Raft, next: types.ConfState) !void {
        self.conf_state.deinit(self.alloc);
        self.conf_state = next;
    }

    fn applyRuntimeConfState(self: *Raft, next: types.ConfState) !void {
        var added_targets = std.ArrayListUnmanaged(types.NodeId).empty;
        defer added_targets.deinit(self.alloc);

        const next_targets = [_][]const types.NodeId{ next.voters, next.voters_outgoing, next.learners, next.learners_next };
        for (next_targets) |targets| {
            for (targets) |node_id| {
                if (!confStateContainsTarget(self.conf_state, node_id)) {
                    try added_targets.append(self.alloc, node_id);
                }
                try self.ensureReplicationPeer(node_id);
            }
        }

        var removed_targets = std.ArrayListUnmanaged(types.NodeId).empty;
        defer removed_targets.deinit(self.alloc);
        for (self.peers) |node_id| {
            if (!confStateContainsTarget(self.conf_state, node_id)) continue;
            if (confStateContainsTarget(next, node_id)) continue;
            try removed_targets.append(self.alloc, node_id);
        }
        for (removed_targets.items) |node_id| {
            try self.removeReplicationPeer(node_id);
        }

        self.normalizePeerOrder();
        std.mem.sort(types.NodeId, added_targets.items, {}, comptime std.sort.asc(types.NodeId));
        try self.replaceConfState(next);
        if (self.lead_transferee) |transferee| {
            if (!containsNode(self.conf_state.voters, transferee)) {
                self.abortLeaderTransfer();
            }
        }
        if (self.soft_state.role == .leader) {
            for (added_targets.items) |node_id| {
                if (node_id == self.cfg.id) continue;
                try self.sendAppend(node_id);
            }
        }
    }

    fn maybeStepDownOnRemoval(self: *Raft) void {
        if (!self.cfg.step_down_on_removal or self.soft_state.role != .leader) return;
        if (containsNode(self.conf_state.learners, self.cfg.id) or
            (!containsNode(self.conf_state.voters, self.cfg.id) and
                !containsNode(self.conf_state.voters_outgoing, self.cfg.id) and
                !containsNode(self.conf_state.learners_next, self.cfg.id)))
        {
            self.becomeFollower(self.hard_state.current_term, null);
        }
    }

    fn enqueueReadState(self: *Raft, index: types.Index, context: []const u8) !void {
        try self.read_states.append(self.alloc, .{
            .index = index,
            .request_ctx = try self.alloc.dupe(u8, context),
        });
    }

    fn handleReadAck(self: *Raft, from: types.NodeId, context: []const u8) !void {
        for (self.pending_reads.items, 0..) |*pending_read, i| {
            if (!std.mem.eql(u8, pending_read.context, context)) continue;
            if (peerIndex(self.peers, from)) |peer_idx| pending_read.acks[peer_idx] = true;
            if (!self.readAckedQuorum(pending_read.acks)) return;

            const release_count = i + 1;
            for (self.pending_reads.items[0..release_count]) |pending| {
                if (pending.requester == self.cfg.id) {
                    try self.enqueueReadState(pending.index, pending.context);
                } else {
                    try self.send(.{
                        .msg_type = .read_index_response,
                        .from = self.cfg.id,
                        .to = pending.requester,
                        .log_index = pending.index,
                        .context = try self.alloc.dupe(u8, pending.context),
                    });
                }
            }
            for (self.pending_reads.items[0..release_count]) |*pending| pending.deinit(self.alloc);
            std.mem.copyForwards(
                PendingRead,
                self.pending_reads.items[0 .. self.pending_reads.items.len - release_count],
                self.pending_reads.items[release_count..],
            );
            self.pending_reads.shrinkRetainingCapacity(self.pending_reads.items.len - release_count);
            return;
        }
    }

    fn readAckedQuorum(self: *const Raft, acks: []const bool) bool {
        if (!ackedVoterQuorum(self.peers, acks, self.conf_state.voters)) return false;
        if (self.conf_state.voters_outgoing.len == 0) return true;
        return ackedVoterQuorum(self.peers, acks, self.conf_state.voters_outgoing);
    }

    fn ensureReplicationPeer(self: *Raft, node_id: types.NodeId) !void {
        if (peerIndex(self.peers, node_id)) |existing_idx| {
            if (!self.isReplicationTarget(node_id)) {
                self.votes[existing_idx] = .unknown;
                self.progress[existing_idx] = .{
                    .match_index = 0,
                    .next_index = @max(@as(types.Index, 1), self.log.lastIndex()),
                    .state = .probe,
                    .probe_sent = false,
                };
            }
            return;
        }

        const old_peers = self.peers;
        const old_votes = self.votes;
        const old_progress = self.progress;

        const new_len = old_peers.len + 1;
        const new_peers = try self.alloc.alloc(types.NodeId, new_len);
        errdefer self.alloc.free(new_peers);
        @memcpy(new_peers[0..old_peers.len], old_peers);
        new_peers[old_peers.len] = node_id;

        const new_votes = try self.alloc.alloc(VoteState, new_len);
        errdefer self.alloc.free(new_votes);
        @memcpy(new_votes[0..old_votes.len], old_votes);
        new_votes[old_peers.len] = .unknown;

        const new_progress = try self.alloc.alloc(types.Progress, new_len);
        errdefer self.alloc.free(new_progress);
        @memcpy(new_progress[0..old_progress.len], old_progress);
        new_progress[old_peers.len] = .{
            .match_index = 0,
            .next_index = @max(@as(types.Index, 1), self.log.lastIndex()),
            .state = .probe,
            .probe_sent = false,
        };

        self.peers = new_peers;
        self.votes = new_votes;
        self.progress = new_progress;
        self.alloc.free(old_peers);
        self.alloc.free(old_votes);
        self.alloc.free(old_progress);
    }

    fn removeReplicationPeer(self: *Raft, node_id: types.NodeId) !void {
        const remove_idx = peerIndex(self.peers, node_id) orelse return;
        if (self.peers.len == 1) return;

        const old_peers = self.peers;
        const old_votes = self.votes;
        const old_progress = self.progress;

        const new_len = old_peers.len - 1;
        const new_peers = try self.alloc.alloc(types.NodeId, new_len);
        errdefer self.alloc.free(new_peers);
        const new_votes = try self.alloc.alloc(VoteState, new_len);
        errdefer self.alloc.free(new_votes);
        const new_progress = try self.alloc.alloc(types.Progress, new_len);
        errdefer self.alloc.free(new_progress);

        var next: usize = 0;
        for (old_peers, 0..) |peer, i| {
            if (i == remove_idx) continue;
            new_peers[next] = peer;
            new_votes[next] = old_votes[i];
            new_progress[next] = old_progress[i];
            next += 1;
        }

        self.peers = new_peers;
        self.votes = new_votes;
        self.progress = new_progress;
        self.alloc.free(old_peers);
        self.alloc.free(old_votes);
        self.alloc.free(old_progress);
    }

    fn isPromotable(self: *const Raft) bool {
        return containsNode(self.conf_state.voters, self.cfg.id) or containsNode(self.conf_state.voters_outgoing, self.cfg.id);
    }

    fn isPromotableConsideringCommitted(self: *Raft) bool {
        if (self.pending_snapshot != null) return false;

        var effective_conf_state = self.conf_state.clone(self.alloc) catch return self.isPromotable();
        defer effective_conf_state.deinit(self.alloc);

        for (self.log.nextCommittedEntries()) |entry| {
            const next_conf_state = switch (entry.entry_type) {
                .conf_change => blk: {
                    const conf_change = types.ConfChange.decode(entry.data) catch return self.isPromotable();
                    break :blk replayConfChange(self.alloc, effective_conf_state, .{
                        .change_type = conf_change.change_type,
                        .node_id = conf_change.node_id,
                    }) catch return self.isPromotable();
                },
                .conf_change_v2 => blk: {
                    var conf_change = types.ConfChangeV2.decode(entry.data, self.alloc) catch return self.isPromotable();
                    defer conf_change.deinit(self.alloc);
                    break :blk replayConfChangeV2(self.alloc, effective_conf_state, conf_change) catch return self.isPromotable();
                },
                else => null,
            };
            if (next_conf_state) |next| {
                effective_conf_state.deinit(self.alloc);
                effective_conf_state = next;
            }
        }

        return containsNode(effective_conf_state.voters, self.cfg.id) or
            containsNode(effective_conf_state.voters_outgoing, self.cfg.id);
    }

    fn isVotingMember(self: *const Raft, node_id: types.NodeId) bool {
        return containsNode(self.conf_state.voters, node_id) or containsNode(self.conf_state.voters_outgoing, node_id);
    }

    fn isReplicationTarget(self: *const Raft, node_id: types.NodeId) bool {
        return containsNode(self.conf_state.voters, node_id) or
            containsNode(self.conf_state.voters_outgoing, node_id) or
            containsNode(self.conf_state.learners, node_id) or
            containsNode(self.conf_state.learners_next, node_id);
    }

    fn abortLeaderTransfer(self: *Raft) void {
        self.lead_transferee = null;
        self.election_elapsed = 0;
    }

    fn markRecentActive(self: *Raft, node_id: types.NodeId) void {
        const idx = peerIndex(self.peers, node_id) orelse return;
        self.progress[idx].recent_active = true;
    }

    fn clearRecentActive(self: *Raft) void {
        for (self.progress, 0..) |*progress, i| {
            progress.recent_active = self.peers[i] == self.cfg.id;
        }
    }

    fn quorumRecentlyActive(self: *const Raft) bool {
        return self.quorumActiveForSet(self.conf_state.voters) and
            (self.conf_state.voters_outgoing.len == 0 or self.quorumActiveForSet(self.conf_state.voters_outgoing));
    }

    fn quorumActiveForSet(self: *const Raft, voters: []const types.NodeId) bool {
        if (voters.len == 0) return true;
        var active_count: usize = 0;
        for (voters) |node_id| {
            if (node_id == self.cfg.id) {
                active_count += 1;
                continue;
            }
            const idx = peerIndex(self.peers, node_id) orelse continue;
            if (self.progress[idx].recent_active) active_count += 1;
        }
        return active_count >= types.quorum(voters.len);
    }

    fn clearPendingReads(self: *Raft) void {
        for (self.pending_reads.items) |*pending_read| pending_read.deinit(self.alloc);
        self.pending_reads.clearRetainingCapacity();
    }

    fn clearAllInflights(self: *Raft) void {
        for (self.inflights) |*queue| queue.clearRetainingCapacity();
    }

    fn clearInflights(self: *Raft, idx: usize) void {
        self.inflights[idx].clearRetainingCapacity();
    }

    fn inflightBytesFull(self: *const Raft, idx: usize) bool {
        var bytes: usize = 0;
        for (self.inflights[idx].items) |flight| bytes += flight.bytes;
        return bytes >= self.cfg.max_inflight_bytes;
    }

    fn freeInflightsTo(self: *Raft, idx: usize, acknowledged: types.Index) void {
        while (self.inflights[idx].items.len > 0 and self.inflights[idx].items[0].index <= acknowledged) {
            _ = self.inflights[idx].orderedRemove(0);
        }
    }

    fn normalizePeerOrder(self: *Raft) void {
        var i: usize = 0;
        while (i < self.peers.len) : (i += 1) {
            var j: usize = i + 1;
            while (j < self.peers.len) : (j += 1) {
                if (self.peers[j] >= self.peers[i]) continue;
                std.mem.swap(types.NodeId, &self.peers[i], &self.peers[j]);
                std.mem.swap(VoteState, &self.votes[i], &self.votes[j]);
                std.mem.swap(types.Progress, &self.progress[i], &self.progress[j]);
            }
        }
    }
};

fn peerIndex(peers: []const types.NodeId, id: types.NodeId) ?usize {
    for (peers, 0..) |peer, i| {
        if (peer == id) return i;
    }
    return null;
}

fn isUpToDate(last_index: types.Index, last_term: types.Term, candidate_index: types.Index, candidate_term: types.Term) bool {
    if (candidate_term != last_term) return candidate_term > last_term;
    return candidate_index >= last_index;
}

fn tryVoterMatchIndexes(
    alloc: std.mem.Allocator,
    peers: []const types.NodeId,
    progress: []const types.Progress,
    voters: []const types.NodeId,
) ![]types.Index {
    const out = try alloc.alloc(types.Index, voters.len);
    var next: usize = 0;
    for (voters) |voter| {
        const idx = peerIndex(peers, voter) orelse return error.UnknownPeer;
        out[next] = progress[idx].match_index;
        next += 1;
    }
    return out;
}

fn ackedVoterQuorum(peers: []const types.NodeId, acks: []const bool, voters: []const types.NodeId) bool {
    var granted: usize = 0;
    for (voters) |voter| {
        const idx = peerIndex(peers, voter) orelse return false;
        if (idx >= acks.len) return false;
        if (acks[idx]) granted += 1;
    }
    return granted >= types.quorum(voters.len);
}

fn tallyVotesForSet(peers: []const types.NodeId, votes: []const VoteState, voters: []const types.NodeId) struct {
    granted: usize,
    rejected: usize,
} {
    var granted: usize = 0;
    var rejected: usize = 0;
    for (voters) |voter| {
        const idx = peerIndex(peers, voter) orelse continue;
        switch (votes[idx]) {
            .granted => granted += 1,
            .rejected => rejected += 1,
            .unknown => {},
        }
    }
    return .{ .granted = granted, .rejected = rejected };
}

fn containsNode(nodes: []const types.NodeId, node_id: types.NodeId) bool {
    for (nodes) |node| {
        if (node == node_id) return true;
    }
    return false;
}

fn appendUniqueNode(alloc: std.mem.Allocator, nodes: []const types.NodeId, node_id: types.NodeId) ![]types.NodeId {
    if (containsNode(nodes, node_id)) return try alloc.dupe(types.NodeId, nodes);
    const out = try alloc.alloc(types.NodeId, nodes.len + 1);
    @memcpy(out[0..nodes.len], nodes);
    out[nodes.len] = node_id;
    return out;
}

fn removeNode(alloc: std.mem.Allocator, nodes: []const types.NodeId, node_id: types.NodeId) ![]types.NodeId {
    var count: usize = 0;
    for (nodes) |node| {
        if (node != node_id) count += 1;
    }

    const out = try alloc.alloc(types.NodeId, count);
    var next: usize = 0;
    for (nodes) |node| {
        if (node == node_id) continue;
        out[next] = node;
        next += 1;
    }
    return out;
}

fn unionNodeSlices(alloc: std.mem.Allocator, a: []const types.NodeId, b: []const types.NodeId) ![]types.NodeId {
    var out = try alloc.dupe(types.NodeId, a);
    errdefer alloc.free(out);

    for (b) |node_id| {
        if (containsNode(out, node_id)) continue;
        const expanded = try alloc.alloc(types.NodeId, out.len + 1);
        @memcpy(expanded[0..out.len], out);
        expanded[out.len] = node_id;
        alloc.free(out);
        out = expanded;
    }
    return out;
}

fn replayConfChange(alloc: std.mem.Allocator, conf_state: types.ConfState, change: types.ConfChangeSingle) !?types.ConfState {
    var next: types.ConfState = switch (change.change_type) {
        .add_node => .{
            .voters = try appendUniqueNode(alloc, conf_state.voters, change.node_id),
            .voters_outgoing = try removeNode(alloc, conf_state.voters_outgoing, change.node_id),
            .learners = try removeNode(alloc, conf_state.learners, change.node_id),
            .learners_next = try removeNode(alloc, conf_state.learners_next, change.node_id),
            .auto_leave = conf_state.auto_leave,
        },
        .remove_node => .{
            .voters = try removeNode(alloc, conf_state.voters, change.node_id),
            .voters_outgoing = try removeNode(alloc, conf_state.voters_outgoing, change.node_id),
            .learners = try removeNode(alloc, conf_state.learners, change.node_id),
            .learners_next = try removeNode(alloc, conf_state.learners_next, change.node_id),
            .auto_leave = conf_state.auto_leave,
        },
        .add_learner_node => .{
            .voters = try removeNode(alloc, conf_state.voters, change.node_id),
            .voters_outgoing = try removeNode(alloc, conf_state.voters_outgoing, change.node_id),
            .learners = try appendUniqueNode(alloc, conf_state.learners, change.node_id),
            .learners_next = try removeNode(alloc, conf_state.learners_next, change.node_id),
            .auto_leave = conf_state.auto_leave,
        },
    };
    normalizeConfState(&next);
    return next;
}

fn replayConfChangeV2(alloc: std.mem.Allocator, conf_state: types.ConfState, conf_change: types.ConfChangeV2) !?types.ConfState {
    if (conf_change.changes.len == 0) {
        if (conf_state.voters_outgoing.len == 0) return null;
        var next: types.ConfState = .{
            .voters = try alloc.dupe(types.NodeId, conf_state.voters),
            .voters_outgoing = &.{},
            .learners = try unionNodeSlices(alloc, conf_state.learners, conf_state.learners_next),
            .learners_next = &.{},
            .auto_leave = false,
        };
        normalizeConfState(&next);
        return next;
    }

    if (conf_change.transition == .auto and conf_change.changes.len == 1) {
        return try replayConfChange(alloc, conf_state, conf_change.changes[0]);
    }

    if (conf_state.voters_outgoing.len > 0) {
        var base_conf_state = types.ConfState{
            .voters = try alloc.dupe(types.NodeId, conf_state.voters_outgoing),
            .learners = try alloc.dupe(types.NodeId, conf_state.learners),
        };
        defer base_conf_state.deinit(alloc);
        if (try replayProducesCurrentJointState(alloc, base_conf_state, conf_state, conf_change)) return null;

        var learners_and_next = try unionNodeSlices(alloc, conf_state.learners, conf_state.learners_next);
        defer alloc.free(learners_and_next);
        var base_with_staged_learners = types.ConfState{
            .voters = try alloc.dupe(types.NodeId, conf_state.voters_outgoing),
            .learners = learners_and_next,
            .learners_next = &.{},
            .auto_leave = false,
        };
        learners_and_next = &.{};
        defer base_with_staged_learners.deinit(alloc);
        if (try replayProducesCurrentJointState(alloc, base_with_staged_learners, conf_state, conf_change)) return null;

        var current_stable = types.ConfState{
            .voters = try alloc.dupe(types.NodeId, conf_state.voters),
            .learners = try unionNodeSlices(alloc, conf_state.learners, conf_state.learners_next),
        };
        defer current_stable.deinit(alloc);
        if (try replayProducesCurrentJointState(alloc, current_stable, conf_state, conf_change)) return null;

        return error.UnsupportedJointConsensusPath;
    }

    var next = types.ConfState{
        .voters = try alloc.dupe(types.NodeId, conf_state.voters),
        .voters_outgoing = try alloc.dupe(types.NodeId, conf_state.voters),
        .learners = try alloc.dupe(types.NodeId, conf_state.learners),
        .learners_next = try alloc.dupe(types.NodeId, conf_state.learners_next),
        .auto_leave = conf_change.transition == .joint_implicit,
    };
    errdefer next.deinit(alloc);

    for (conf_change.changes) |change| {
        switch (change.change_type) {
            .add_node => {
                const next_voters = try appendUniqueNode(alloc, next.voters, change.node_id);
                alloc.free(next.voters);
                next.voters = next_voters;

                const next_learners = try removeNode(alloc, next.learners, change.node_id);
                alloc.free(next.learners);
                next.learners = next_learners;

                const next_learners_next = try removeNode(alloc, next.learners_next, change.node_id);
                alloc.free(next.learners_next);
                next.learners_next = next_learners_next;
            },
            .remove_node => {
                const next_voters = try removeNode(alloc, next.voters, change.node_id);
                alloc.free(next.voters);
                next.voters = next_voters;

                const next_learners = try removeNode(alloc, next.learners, change.node_id);
                alloc.free(next.learners);
                next.learners = next_learners;

                const next_learners_next = try removeNode(alloc, next.learners_next, change.node_id);
                alloc.free(next.learners_next);
                next.learners_next = next_learners_next;
            },
            .add_learner_node => {
                const next_voters = try removeNode(alloc, next.voters, change.node_id);
                alloc.free(next.voters);
                next.voters = next_voters;

                if (containsNode(next.voters_outgoing, change.node_id)) {
                    const next_learners_next = try appendUniqueNode(alloc, next.learners_next, change.node_id);
                    alloc.free(next.learners_next);
                    next.learners_next = next_learners_next;

                    const next_learners = try removeNode(alloc, next.learners, change.node_id);
                    alloc.free(next.learners);
                    next.learners = next_learners;
                } else {
                    const next_learners = try appendUniqueNode(alloc, next.learners, change.node_id);
                    alloc.free(next.learners);
                    next.learners = next_learners;

                    const next_learners_next = try removeNode(alloc, next.learners_next, change.node_id);
                    alloc.free(next.learners_next);
                    next.learners_next = next_learners_next;
                }
            },
        }
    }

    if (next.voters.len == 0) return error.UnsupportedJointConsensusPath;
    normalizeConfState(&next);
    return next;
}

fn normalizeConfState(conf_state: *types.ConfState) void {
    std.mem.sort(types.NodeId, conf_state.voters, {}, comptime std.sort.asc(types.NodeId));
    std.mem.sort(types.NodeId, conf_state.voters_outgoing, {}, comptime std.sort.asc(types.NodeId));
    std.mem.sort(types.NodeId, conf_state.learners, {}, comptime std.sort.asc(types.NodeId));
    std.mem.sort(types.NodeId, conf_state.learners_next, {}, comptime std.sort.asc(types.NodeId));
}

fn confStateContainsTarget(conf_state: types.ConfState, node_id: types.NodeId) bool {
    return containsNode(conf_state.voters, node_id) or
        containsNode(conf_state.voters_outgoing, node_id) or
        containsNode(conf_state.learners, node_id) or
        containsNode(conf_state.learners_next, node_id);
}

fn confStateEq(a: types.ConfState, b: types.ConfState) bool {
    return std.mem.eql(types.NodeId, a.voters, b.voters) and
        std.mem.eql(types.NodeId, a.voters_outgoing, b.voters_outgoing) and
        std.mem.eql(types.NodeId, a.learners, b.learners) and
        std.mem.eql(types.NodeId, a.learners_next, b.learners_next) and
        a.auto_leave == b.auto_leave;
}

fn replayProducesCurrentJointState(
    alloc: std.mem.Allocator,
    base_conf_state: types.ConfState,
    current_conf_state: types.ConfState,
    conf_change: types.ConfChangeV2,
) std.mem.Allocator.Error!bool {
    const replayed = replayConfChangeV2(alloc, base_conf_state, conf_change) catch |err| switch (err) {
        error.UnsupportedJointConsensusPath => return false,
        else => |other| return other,
    };
    if (replayed) |replayed_next| {
        var next = replayed_next;
        defer next.deinit(alloc);
        return confStateEq(next, current_conf_state);
    }
    return false;
}
