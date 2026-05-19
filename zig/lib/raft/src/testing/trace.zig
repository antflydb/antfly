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
const message_mod = @import("../core/message.zig");
const Cluster = @import("cluster.zig").Cluster;

pub const ActionTag = enum {
    tick,
    set_randomized_election_timeout,
    campaign,
    campaign_settle,
    transfer_leader,
    forget_leader,
    propose,
    propose_dropped,
    read_index,
    read_index_not_leader,
    propose_conf_change,
    propose_conf_change_v2,
    leave_joint,
    collect_ready,
    drain_committed,
    restart_node,
    restart_node_with_applied,
    restart_node_with_pre_vote,
    compact_node,
    reject_snapshot,
    abort_snapshot,
    deliver_one,
    deliver_all,
    block_link,
    unblock_link,
    clear_blocks,
};

pub const Action = union(ActionTag) {
    pub const ConfChangeAction = struct {
        change_type: core.types.ConfChangeType,
        target_node_id: core.types.NodeId,
    };

    tick: struct {
        node_id: core.types.NodeId,
        count: usize,
    },
    set_randomized_election_timeout: struct {
        node_id: core.types.NodeId,
        timeout: u32,
    },
    campaign: struct {
        node_id: core.types.NodeId,
    },
    campaign_settle: struct {
        node_id: core.types.NodeId,
    },
    transfer_leader: struct {
        node_id: core.types.NodeId,
        target_node_id: core.types.NodeId,
    },
    forget_leader: struct {
        node_id: core.types.NodeId,
    },
    propose: struct {
        node_id: core.types.NodeId,
        data: []const u8,
    },
    propose_dropped: struct {
        node_id: core.types.NodeId,
        data: []const u8,
    },
    read_index: struct {
        node_id: core.types.NodeId,
        request_ctx: []const u8,
    },
    read_index_not_leader: struct {
        node_id: core.types.NodeId,
        request_ctx: []const u8,
    },
    propose_conf_change: struct {
        node_id: core.types.NodeId,
        change_type: core.types.ConfChangeType,
        target_node_id: core.types.NodeId,
    },
    propose_conf_change_v2: struct {
        node_id: core.types.NodeId,
        transition: core.types.ConfChangeTransition,
        changes: []ConfChangeAction,
    },
    leave_joint: struct {
        node_id: core.types.NodeId,
    },
    collect_ready: struct {
        node_id: core.types.NodeId,
    },
    drain_committed: struct {
        node_id: core.types.NodeId,
    },
    restart_node: struct {
        node_id: core.types.NodeId,
    },
    restart_node_with_applied: struct {
        node_id: core.types.NodeId,
        applied: core.types.Index,
    },
    restart_node_with_pre_vote: struct {
        node_id: core.types.NodeId,
        pre_vote: bool,
    },
    compact_node: struct {
        node_id: core.types.NodeId,
        compact_index: core.types.Index,
    },
    reject_snapshot: struct {
        from: core.types.NodeId,
        to: core.types.NodeId,
    },
    abort_snapshot: struct {
        from: core.types.NodeId,
        to: core.types.NodeId,
        log_index: core.types.Index,
    },
    deliver_one: void,
    deliver_all: void,
    block_link: struct {
        from: core.types.NodeId,
        to: core.types.NodeId,
    },
    unblock_link: struct {
        from: core.types.NodeId,
        to: core.types.NodeId,
    },
    clear_blocks: void,

    pub fn clone(self: Action, alloc: std.mem.Allocator) !Action {
        return switch (self) {
            .tick => |tick| .{ .tick = tick },
            .set_randomized_election_timeout => |timeout| .{ .set_randomized_election_timeout = timeout },
            .campaign => |campaign| .{ .campaign = campaign },
            .campaign_settle => |campaign| .{ .campaign_settle = campaign },
            .transfer_leader => |transfer| .{ .transfer_leader = transfer },
            .forget_leader => |forget| .{ .forget_leader = forget },
            .propose => |propose| .{
                .propose = .{
                    .node_id = propose.node_id,
                    .data = try alloc.dupe(u8, propose.data),
                },
            },
            .propose_dropped => |propose| .{
                .propose_dropped = .{
                    .node_id = propose.node_id,
                    .data = try alloc.dupe(u8, propose.data),
                },
            },
            .read_index => |read_index| .{
                .read_index = .{
                    .node_id = read_index.node_id,
                    .request_ctx = try alloc.dupe(u8, read_index.request_ctx),
                },
            },
            .read_index_not_leader => |read_index| .{
                .read_index_not_leader = .{
                    .node_id = read_index.node_id,
                    .request_ctx = try alloc.dupe(u8, read_index.request_ctx),
                },
            },
            .propose_conf_change => |conf_change| .{ .propose_conf_change = conf_change },
            .propose_conf_change_v2 => |conf_change| .{
                .propose_conf_change_v2 = .{
                    .node_id = conf_change.node_id,
                    .transition = conf_change.transition,
                    .changes = try alloc.dupe(ConfChangeAction, conf_change.changes),
                },
            },
            .leave_joint => |leave_joint| .{ .leave_joint = leave_joint },
            .collect_ready => |collect_ready| .{ .collect_ready = collect_ready },
            .drain_committed => |drain| .{ .drain_committed = drain },
            .restart_node => |restart| .{ .restart_node = restart },
            .restart_node_with_applied => |restart| .{ .restart_node_with_applied = restart },
            .restart_node_with_pre_vote => |restart| .{ .restart_node_with_pre_vote = restart },
            .compact_node => |compact| .{ .compact_node = compact },
            .reject_snapshot => |reject_snapshot| .{ .reject_snapshot = reject_snapshot },
            .abort_snapshot => |abort_snapshot| .{ .abort_snapshot = abort_snapshot },
            .deliver_one => .{ .deliver_one = {} },
            .deliver_all => .{ .deliver_all = {} },
            .block_link => |block| .{ .block_link = block },
            .unblock_link => |unblock| .{ .unblock_link = unblock },
            .clear_blocks => .{ .clear_blocks = {} },
        };
    }

    pub fn deinit(self: *Action, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .propose => |propose| if (propose.data.len > 0) alloc.free(propose.data),
            .propose_dropped => |propose| if (propose.data.len > 0) alloc.free(propose.data),
            .read_index => |read_index| if (read_index.request_ctx.len > 0) alloc.free(read_index.request_ctx),
            .read_index_not_leader => |read_index| if (read_index.request_ctx.len > 0) alloc.free(read_index.request_ctx),
            .propose_conf_change_v2 => |conf_change| if (conf_change.changes.len > 0) alloc.free(conf_change.changes),
            else => {},
        }
        self.* = undefined;
    }

    pub fn format(self: Action, writer: *std.Io.Writer) !void {
        switch (self) {
            .tick => |tick| try writer.print("tick node={} count={}", .{ tick.node_id, tick.count }),
            .set_randomized_election_timeout => |timeout| try writer.print("set_randomized_election_timeout node={} timeout={}", .{ timeout.node_id, timeout.timeout }),
            .campaign => |campaign| try writer.print("campaign node={}", .{campaign.node_id}),
            .campaign_settle => |campaign| try writer.print("campaign_settle node={}", .{campaign.node_id}),
            .transfer_leader => |transfer| try writer.print("transfer_leader node={} target={}", .{ transfer.node_id, transfer.target_node_id }),
            .forget_leader => |forget| try writer.print("forget_leader node={}", .{forget.node_id}),
            .propose => |propose| try writer.print("propose node={} data=\"{s}\"", .{ propose.node_id, propose.data }),
            .propose_dropped => |propose| try writer.print("propose_dropped node={} data=\"{s}\"", .{ propose.node_id, propose.data }),
            .read_index => |read_index| try writer.print("read_index node={} ctx=\"{s}\"", .{ read_index.node_id, read_index.request_ctx }),
            .read_index_not_leader => |read_index| try writer.print("read_index_not_leader node={} ctx=\"{s}\"", .{ read_index.node_id, read_index.request_ctx }),
            .propose_conf_change => |conf_change| try writer.print(
                "propose_conf_change node={} type={s} target={}",
                .{ conf_change.node_id, @tagName(conf_change.change_type), conf_change.target_node_id },
            ),
            .propose_conf_change_v2 => |conf_change| {
                try writer.print(
                    "propose_conf_change_v2 node={} transition={s} changes=",
                    .{ conf_change.node_id, @tagName(conf_change.transition) },
                );
                try writer.writeByte('[');
                for (conf_change.changes, 0..) |change, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try writer.print("{s}:{}", .{ @tagName(change.change_type), change.target_node_id });
                }
                try writer.writeByte(']');
            },
            .leave_joint => |leave_joint| try writer.print("leave_joint node={}", .{leave_joint.node_id}),
            .collect_ready => |collect_ready| try writer.print("collect_ready node={}", .{collect_ready.node_id}),
            .drain_committed => |drain| try writer.print("drain_committed node={}", .{drain.node_id}),
            .restart_node => |restart| try writer.print("restart node={}", .{restart.node_id}),
            .restart_node_with_applied => |restart| try writer.print("restart node={} applied={}", .{ restart.node_id, restart.applied }),
            .restart_node_with_pre_vote => |restart| try writer.print("restart node={} pre_vote={}", .{ restart.node_id, restart.pre_vote }),
            .compact_node => |compact| try writer.print("compact node={} index={}", .{ compact.node_id, compact.compact_index }),
            .reject_snapshot => |reject_snapshot| try writer.print("reject_snapshot from={} to={}", .{ reject_snapshot.from, reject_snapshot.to }),
            .abort_snapshot => |abort_snapshot| try writer.print("abort_snapshot from={} to={} index={}", .{ abort_snapshot.from, abort_snapshot.to, abort_snapshot.log_index }),
            .deliver_one => try writer.writeAll("deliver_one"),
            .deliver_all => try writer.writeAll("deliver_all"),
            .block_link => |block| try writer.print("block from={} to={}", .{ block.from, block.to }),
            .unblock_link => |unblock| try writer.print("unblock from={} to={}", .{ unblock.from, unblock.to }),
            .clear_blocks => try writer.writeAll("clear_blocks"),
        }
    }
};

pub const NodeSnapshot = struct {
    node_id: core.types.NodeId,
    role: core.types.StateRole,
    leader_id: ?core.types.NodeId,
    term: core.types.Term,
    voted_for: ?core.types.NodeId,
    commit_index: core.types.Index,
};

pub const MessageSummary = struct {
    msg_type: core.message.MessageType,
    from: core.types.NodeId,
    to: core.types.NodeId,
    term: core.types.Term,
    log_index: core.types.Index,
    log_term: core.types.Term,
    commit_index: core.types.Index,
    reject: bool,
    reject_hint: core.types.Index,
    entries_len: usize,
    first_entry_index: core.types.Index,
    last_entry_index: core.types.Index,
};

pub const CommittedSummary = struct {
    node_id: core.types.NodeId,
    count: usize,
    first_index: core.types.Index,
    last_index: core.types.Index,
};

pub const ReadStateSummary = struct {
    node_id: core.types.NodeId,
    index: core.types.Index,
    request_ctx: []u8,

    pub fn deinit(self: *ReadStateSummary, alloc: std.mem.Allocator) void {
        if (self.request_ctx.len > 0) alloc.free(self.request_ctx);
        self.* = undefined;
    }
};

pub const ConfStateSummary = struct {
    node_id: core.types.NodeId,
    voters: []core.types.NodeId,
    voters_outgoing: []core.types.NodeId,
    learners: []core.types.NodeId,
    learners_next: []core.types.NodeId,
    auto_leave: bool,

    pub fn deinit(self: *ConfStateSummary, alloc: std.mem.Allocator) void {
        if (self.voters.len > 0) alloc.free(self.voters);
        if (self.voters_outgoing.len > 0) alloc.free(self.voters_outgoing);
        if (self.learners.len > 0) alloc.free(self.learners);
        if (self.learners_next.len > 0) alloc.free(self.learners_next);
        self.* = undefined;
    }
};

pub const Step = struct {
    action: Action,
    nodes: []NodeSnapshot,
    messages: []MessageSummary,
    committed: []CommittedSummary,
    read_states: []ReadStateSummary,
    conf_states: []ConfStateSummary,

    pub fn deinit(self: *Step, alloc: std.mem.Allocator) void {
        self.action.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        if (self.messages.len > 0) alloc.free(self.messages);
        if (self.committed.len > 0) alloc.free(self.committed);
        for (self.read_states) |*read_state| read_state.deinit(alloc);
        if (self.read_states.len > 0) alloc.free(self.read_states);
        for (self.conf_states) |*conf_state| conf_state.deinit(alloc);
        if (self.conf_states.len > 0) alloc.free(self.conf_states);
        self.* = undefined;
    }
};

pub const TraceRecorder = struct {
    alloc: std.mem.Allocator,
    cluster: Cluster,
    steps: std.ArrayListUnmanaged(Step) = .empty,

    pub fn init(alloc: std.mem.Allocator, peer_ids: []const core.types.NodeId) !TraceRecorder {
        return .{
            .alloc = alloc,
            .cluster = try Cluster.init(alloc, peer_ids),
        };
    }

    pub fn initWithOptions(alloc: std.mem.Allocator, peer_ids: []const core.types.NodeId, options: Cluster.Options) !TraceRecorder {
        return .{
            .alloc = alloc,
            .cluster = try Cluster.initWithOptions(alloc, peer_ids, options),
        };
    }

    pub fn deinit(self: *TraceRecorder) void {
        for (self.steps.items) |*step| step.deinit(self.alloc);
        self.steps.deinit(self.alloc);
        self.cluster.deinit();
        self.* = undefined;
    }

    pub fn clusterPtr(self: *TraceRecorder) *Cluster {
        return &self.cluster;
    }

    pub fn stepsSlice(self: *const TraceRecorder) []const Step {
        return self.steps.items;
    }

    pub fn tick(self: *TraceRecorder, node_id: core.types.NodeId, count: usize) !void {
        try self.cluster.tick(node_id, count);
        try self.capture(.{ .tick = .{ .node_id = node_id, .count = count } });
    }

    pub fn setRandomizedElectionTimeout(self: *TraceRecorder, node_id: core.types.NodeId, timeout: u32) !void {
        self.cluster.setRandomizedElectionTimeout(node_id, timeout);
        try self.capture(.{ .set_randomized_election_timeout = .{
            .node_id = node_id,
            .timeout = timeout,
        } });
    }

    pub fn applyAction(self: *TraceRecorder, action: Action) !void {
        switch (action) {
            .tick => |tick_action| try self.tick(tick_action.node_id, tick_action.count),
            .set_randomized_election_timeout => |timeout_action| try self.setRandomizedElectionTimeout(timeout_action.node_id, timeout_action.timeout),
            .campaign => |campaign_action| try self.campaign(campaign_action.node_id),
            .campaign_settle => |campaign_action| try self.campaignSettle(campaign_action.node_id),
            .transfer_leader => |transfer_action| try self.transferLeader(transfer_action.node_id, transfer_action.target_node_id),
            .forget_leader => |forget_action| try self.forgetLeader(forget_action.node_id),
            .propose => |propose_action| try self.propose(propose_action.node_id, propose_action.data),
            .propose_dropped => |propose_action| try self.proposeDropped(propose_action.node_id, propose_action.data),
            .read_index => |read_index_action| try self.readIndex(read_index_action.node_id, read_index_action.request_ctx),
            .read_index_not_leader => |read_index_action| try self.readIndexNotLeader(read_index_action.node_id, read_index_action.request_ctx),
            .propose_conf_change => |conf_change_action| try self.proposeConfChange(conf_change_action.node_id, .{
                .change_type = conf_change_action.change_type,
                .node_id = conf_change_action.target_node_id,
            }),
            .propose_conf_change_v2 => |conf_change_action| {
                const changes = try self.alloc.alloc(core.types.ConfChangeSingle, conf_change_action.changes.len);
                defer self.alloc.free(changes);
                for (conf_change_action.changes, 0..) |change, i| {
                    changes[i] = .{
                        .change_type = change.change_type,
                        .node_id = change.target_node_id,
                    };
                }
                try self.proposeConfChangeV2(conf_change_action.node_id, .{
                    .transition = conf_change_action.transition,
                    .changes = changes,
                });
            },
            .leave_joint => |leave_joint_action| try self.leaveJoint(leave_joint_action.node_id),
            .collect_ready => |collect_ready_action| try self.collectReady(collect_ready_action.node_id),
            .drain_committed => |drain_action| try self.drainCommitted(drain_action.node_id),
            .restart_node => |restart_action| try self.restart(restart_action.node_id),
            .restart_node_with_applied => |restart_action| try self.restartWithApplied(restart_action.node_id, restart_action.applied),
            .restart_node_with_pre_vote => |restart_action| try self.restartWithPreVote(restart_action.node_id, restart_action.pre_vote),
            .compact_node => |compact_action| try self.compact(compact_action.node_id, compact_action.compact_index),
            .reject_snapshot => |reject_snapshot_action| try self.rejectSnapshot(reject_snapshot_action.from, reject_snapshot_action.to),
            .abort_snapshot => |abort_snapshot_action| try self.abortSnapshot(abort_snapshot_action.from, abort_snapshot_action.to, abort_snapshot_action.log_index),
            .deliver_one => try self.deliverOne(),
            .deliver_all => try self.deliverAll(),
            .block_link => |block_action| try self.block(block_action.from, block_action.to),
            .unblock_link => |unblock_action| try self.unblock(unblock_action.from, unblock_action.to),
            .clear_blocks => try self.clearBlocks(),
        }
    }

    pub fn campaign(self: *TraceRecorder, node_id: core.types.NodeId) !void {
        try self.cluster.campaign(node_id);
        try self.capture(.{ .campaign = .{ .node_id = node_id } });
    }

    pub fn campaignSettle(self: *TraceRecorder, node_id: core.types.NodeId) !void {
        try self.cluster.node(node_id).campaign();
        try self.cluster.collectReady(node_id);
        while (self.cluster.node(node_id).hasReady()) {
            try self.cluster.collectReady(node_id);
        }
        try self.capture(.{ .campaign_settle = .{ .node_id = node_id } });
    }

    pub fn transferLeader(self: *TraceRecorder, node_id: core.types.NodeId, target_node_id: core.types.NodeId) !void {
        try self.cluster.transferLeader(node_id, target_node_id);
        try self.capture(.{ .transfer_leader = .{
            .node_id = node_id,
            .target_node_id = target_node_id,
        } });
    }

    pub fn forgetLeader(self: *TraceRecorder, node_id: core.types.NodeId) !void {
        try self.cluster.forgetLeader(node_id);
        try self.capture(.{ .forget_leader = .{
            .node_id = node_id,
        } });
    }

    pub fn propose(self: *TraceRecorder, node_id: core.types.NodeId, data: []const u8) !void {
        try self.cluster.propose(node_id, data);
        try self.capture(.{
            .propose = .{
                .node_id = node_id,
                .data = data,
            },
        });
    }

    pub fn proposeDropped(self: *TraceRecorder, node_id: core.types.NodeId, data: []const u8) !void {
        self.cluster.propose(node_id, data) catch |err| switch (err) {
            error.ProposalDropped => {},
            else => return err,
        };
        try self.capture(.{
            .propose_dropped = .{
                .node_id = node_id,
                .data = data,
            },
        });
    }

    pub fn readIndex(self: *TraceRecorder, node_id: core.types.NodeId, request_ctx: []const u8) !void {
        try self.cluster.readIndex(node_id, request_ctx);
        try self.capture(.{
            .read_index = .{
                .node_id = node_id,
                .request_ctx = request_ctx,
            },
        });
    }

    pub fn readIndexNotLeader(self: *TraceRecorder, node_id: core.types.NodeId, request_ctx: []const u8) !void {
        self.cluster.readIndex(node_id, request_ctx) catch |err| switch (err) {
            error.NotLeader => {},
            else => return err,
        };
        try self.capture(.{
            .read_index_not_leader = .{
                .node_id = node_id,
                .request_ctx = request_ctx,
            },
        });
    }

    pub fn proposeConfChange(self: *TraceRecorder, node_id: core.types.NodeId, conf_change: core.types.ConfChange) !void {
        try self.cluster.node(node_id).proposeConfChange(conf_change);
        try self.cluster.collectReady(node_id);
        try self.capture(.{ .propose_conf_change = .{
            .node_id = node_id,
            .change_type = conf_change.change_type,
            .target_node_id = conf_change.node_id,
        } });
    }

    pub fn proposeConfChangeV2(
        self: *TraceRecorder,
        node_id: core.types.NodeId,
        conf_change: core.types.ConfChangeV2,
    ) !void {
        var initial_conf_state = try self.cluster.node(node_id).status().conf_state.clone(self.alloc);
        defer initial_conf_state.deinit(self.alloc);

        try self.cluster.node(node_id).proposeConfChangeV2(conf_change);
        while (true) {
            try self.cluster.collectReady(node_id);
            const current_conf_state = self.cluster.node(node_id).status().conf_state;
            const changed = !confStateEql(current_conf_state, initial_conf_state);
            const settled_implicit = conf_change.transition == .joint_implicit and
                changed and
                current_conf_state.voters_outgoing.len == 0 and
                !self.cluster.node(node_id).hasReady();
            if ((conf_change.transition != .joint_implicit and changed) or
                settled_implicit or
                !self.cluster.node(node_id).hasReady())
            {
                break;
            }
        }
        const action_changes = try cloneActionConfChanges(self.alloc, conf_change.changes);
        defer if (action_changes.len > 0) self.alloc.free(action_changes);

        try self.capture(.{ .propose_conf_change_v2 = .{
            .node_id = node_id,
            .transition = conf_change.transition,
            .changes = action_changes,
        } });
    }

    pub fn collectReady(self: *TraceRecorder, node_id: core.types.NodeId) !void {
        try self.cluster.collectReady(node_id);
        try self.capture(.{ .collect_ready = .{ .node_id = node_id } });
    }

    pub fn drainCommitted(self: *TraceRecorder, node_id: core.types.NodeId) !void {
        const drained = try self.cluster.collectCommitted(node_id);
        defer core.types.freeEntries(self.alloc, drained);
        try self.capture(.{ .drain_committed = .{ .node_id = node_id } });
    }

    pub fn leaveJoint(self: *TraceRecorder, node_id: core.types.NodeId) !void {
        var initial_conf_state = try self.cluster.node(node_id).status().conf_state.clone(self.alloc);
        defer initial_conf_state.deinit(self.alloc);

        try self.cluster.node(node_id).proposeConfChangeV2(.{});
        while (true) {
            try self.cluster.collectReady(node_id);
            const current_conf_state = self.cluster.node(node_id).status().conf_state;
            if (!confStateEql(current_conf_state, initial_conf_state) or !self.cluster.node(node_id).hasReady()) {
                break;
            }
        }
        try self.capture(.{ .leave_joint = .{ .node_id = node_id } });
    }

    pub fn restart(self: *TraceRecorder, node_id: core.types.NodeId) !void {
        try self.cluster.restart(node_id);
        try self.capture(.{ .restart_node = .{ .node_id = node_id } });
    }

    pub fn restartWithApplied(self: *TraceRecorder, node_id: core.types.NodeId, applied: core.types.Index) !void {
        try self.cluster.restartWithApplied(node_id, applied);
        try self.capture(.{ .restart_node_with_applied = .{
            .node_id = node_id,
            .applied = applied,
        } });
    }

    pub fn restartWithPreVote(self: *TraceRecorder, node_id: core.types.NodeId, pre_vote: bool) !void {
        self.cluster.setNodePreVote(node_id, pre_vote);
        try self.cluster.restart(node_id);
        try self.capture(.{ .restart_node_with_pre_vote = .{
            .node_id = node_id,
            .pre_vote = pre_vote,
        } });
    }

    pub fn compact(self: *TraceRecorder, node_id: core.types.NodeId, compact_index: core.types.Index) !void {
        try self.cluster.compact(node_id, compact_index);
        try self.capture(.{ .compact_node = .{
            .node_id = node_id,
            .compact_index = compact_index,
        } });
    }

    pub fn rejectSnapshot(self: *TraceRecorder, from: core.types.NodeId, to: core.types.NodeId) !void {
        try self.cluster.rejectSnapshot(from, to);
        try self.capture(.{ .reject_snapshot = .{ .from = from, .to = to } });
    }

    pub fn abortSnapshot(self: *TraceRecorder, from: core.types.NodeId, to: core.types.NodeId, log_index: core.types.Index) !void {
        try self.cluster.abortSnapshot(from, to, log_index);
        try self.capture(.{ .abort_snapshot = .{
            .from = from,
            .to = to,
            .log_index = log_index,
        } });
    }

    pub fn deliverOne(self: *TraceRecorder) !void {
        try self.cluster.deliverNext();
        try self.capture(.{ .deliver_one = {} });
    }

    pub fn deliverAll(self: *TraceRecorder) !void {
        try self.cluster.deliverAll();
        try self.capture(.{ .deliver_all = {} });
    }

    pub fn block(self: *TraceRecorder, from: core.types.NodeId, to: core.types.NodeId) !void {
        try self.cluster.block(from, to);
        try self.capture(.{ .block_link = .{ .from = from, .to = to } });
    }

    pub fn unblock(self: *TraceRecorder, from: core.types.NodeId, to: core.types.NodeId) !void {
        self.cluster.unblock(from, to);
        try self.capture(.{ .unblock_link = .{ .from = from, .to = to } });
    }

    pub fn clearBlocks(self: *TraceRecorder) !void {
        self.cluster.clearBlocks();
        try self.capture(.{ .clear_blocks = {} });
    }

    pub fn render(self: *const TraceRecorder, alloc: std.mem.Allocator) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(alloc);
        errdefer out.deinit();

        for (self.steps.items, 0..) |step, idx| {
            try out.writer.print("step {}: ", .{idx});
            try step.action.format(&out.writer);
            try out.writer.writeByte('\n');

            for (step.nodes) |node| {
                try out.writer.print(
                    "  node {} role={s} leader={any} term={} vote={any} commit={}\n",
                    .{
                        node.node_id,
                        @tagName(node.role),
                        node.leader_id,
                        node.term,
                        node.voted_for,
                        node.commit_index,
                    },
                );
            }

            for (step.messages) |msg| {
                try out.writer.print(
                    "  msg {s} {}->{} term={} prev={} prev_term={} commit={} reject={} hint={} entries={} first={} last={}\n",
                    .{
                        @tagName(msg.msg_type),
                        msg.from,
                        msg.to,
                        msg.term,
                        msg.log_index,
                        msg.log_term,
                        msg.commit_index,
                        msg.reject,
                        msg.reject_hint,
                        msg.entries_len,
                        msg.first_entry_index,
                        msg.last_entry_index,
                    },
                );
            }

            for (step.committed) |commit| {
                try out.writer.print(
                    "  committed node={} count={} first={} last={}\n",
                    .{ commit.node_id, commit.count, commit.first_index, commit.last_index },
                );
            }

            for (step.read_states) |read_state| {
                try out.writer.print(
                    "  read_state node={} index={} ctx=\"{s}\"\n",
                    .{ read_state.node_id, read_state.index, read_state.request_ctx },
                );
            }

            for (step.conf_states) |conf_state| {
                try out.writer.print(
                    "  conf_state node={} voters={any} outgoing={any} learners={any} learners_next={any} auto_leave={}\n",
                    .{
                        conf_state.node_id,
                        conf_state.voters,
                        conf_state.voters_outgoing,
                        conf_state.learners,
                        conf_state.learners_next,
                        conf_state.auto_leave,
                    },
                );
            }
        }

        return try out.toOwnedSlice();
    }

    pub fn writeJson(self: *const TraceRecorder, writer: *std.Io.Writer) !void {
        var js: std.json.Stringify = .{
            .writer = writer,
            .options = .{ .whitespace = .indent_2 },
        };

        try js.beginObject();
        try js.objectField("version");
        try js.write(@as(u32, 1));
        try js.objectField("peers");
        try js.write(self.cluster.peerIds());
        try js.objectField("config");
        try js.beginObject();
        try js.objectField("election_tick");
        try js.write(self.cluster.election_tick);
        try js.objectField("heartbeat_tick");
        try js.write(self.cluster.heartbeat_tick);
        if (self.cluster.max_size_per_msg != std.math.maxInt(usize)) {
            try js.objectField("max_size_per_msg");
            try js.write(self.cluster.max_size_per_msg);
        }
        if (self.cluster.max_committed_size_per_ready != 0) {
            try js.objectField("max_committed_size_per_ready");
            try js.write(self.cluster.max_committed_size_per_ready);
        }
        if (self.cluster.max_inflight_msgs != 256) {
            try js.objectField("max_inflight_msgs");
            try js.write(self.cluster.max_inflight_msgs);
        }
        if (self.cluster.max_inflight_bytes != 0) {
            try js.objectField("max_inflight_bytes");
            try js.write(self.cluster.max_inflight_bytes);
        }
        if (self.cluster.max_uncommitted_entries_size != std.math.maxInt(usize)) {
            try js.objectField("max_uncommitted_entries_size");
            try js.write(self.cluster.max_uncommitted_entries_size);
        }
        if (self.cluster.async_storage_writes) {
            try js.objectField("async_storage_writes");
            try js.write(true);
        }
        try js.objectField("check_quorum");
        try js.write(self.cluster.check_quorum);
        try js.objectField("pre_vote");
        try js.write(self.cluster.pre_vote);
        try js.objectField("step_down_on_removal");
        try js.write(self.cluster.step_down_on_removal);
        if (self.cluster.random_seed) |seed| {
            try js.objectField("random_seed");
            try js.write(seed);
        }
        if (self.cluster.disable_proposal_forwarding) {
            try js.objectField("disable_proposal_forwarding");
            try js.write(true);
        }
        if (self.cluster.disable_conf_change_validation) {
            try js.objectField("disable_conf_change_validation");
            try js.write(true);
        }
        if (self.cluster.read_only_option == .lease_based) {
            try js.objectField("read_only_option");
            try js.write("lease_based");
        }
        try js.objectField("initial_conf_state");
        try js.beginObject();
        const initial_conf_state = self.cluster.initialConfState();
        try js.objectField("voters");
        try js.write(initial_conf_state.voters);
        try js.objectField("voters_outgoing");
        try js.write(initial_conf_state.voters_outgoing);
        try js.objectField("learners");
        try js.write(initial_conf_state.learners);
        try js.objectField("learners_next");
        try js.write(initial_conf_state.learners_next);
        try js.objectField("auto_leave");
        try js.write(initial_conf_state.auto_leave);
        try js.endObject();
        try js.endObject();
        try js.objectField("steps");
        try js.beginArray();
        for (self.steps.items) |step| {
            try js.beginObject();
            try js.objectField("action");
            try writeActionJson(step.action, &js);
            try js.objectField("nodes");
            try js.beginArray();
            for (step.nodes) |node| try writeNodeSnapshotJson(node, &js);
            try js.endArray();
            try js.objectField("messages");
            try js.beginArray();
            for (step.messages) |msg| try writeMessageSummaryJson(msg, &js);
            try js.endArray();
            try js.objectField("committed");
            try js.beginArray();
            for (step.committed) |commit| try writeCommittedSummaryJson(commit, &js);
            try js.endArray();
            try js.objectField("read_states");
            try js.beginArray();
            for (step.read_states) |read_state| try writeReadStateSummaryJson(read_state, &js);
            try js.endArray();
            try js.objectField("conf_states");
            try js.beginArray();
            for (step.conf_states) |conf_state| try writeConfStateSummaryJson(conf_state, &js);
            try js.endArray();
            try js.endObject();
        }
        try js.endArray();
        try js.endObject();
    }

    pub fn toJson(self: *const TraceRecorder, alloc: std.mem.Allocator) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(alloc);
        errdefer out.deinit();
        try self.writeJson(&out.writer);
        return try out.toOwnedSlice();
    }

    fn capture(self: *TraceRecorder, action: Action) !void {
        const owned_action = try action.clone(self.alloc);
        errdefer {
            var to_free = owned_action;
            to_free.deinit(self.alloc);
        }

        const nodes = try self.snapshotNodes();
        errdefer self.alloc.free(nodes);

        const messages = try self.snapshotMessages();
        errdefer self.alloc.free(messages);

        const committed = try self.snapshotCommitted();
        errdefer self.alloc.free(committed);

        const read_states = try self.snapshotReadStates();
        errdefer {
            for (read_states) |*read_state| read_state.deinit(self.alloc);
            self.alloc.free(read_states);
        }

        const conf_states = try self.snapshotConfStates();
        errdefer {
            for (conf_states) |*conf_state| conf_state.deinit(self.alloc);
            self.alloc.free(conf_states);
        }

        try self.steps.append(self.alloc, .{
            .action = owned_action,
            .nodes = nodes,
            .messages = messages,
            .committed = committed,
            .read_states = read_states,
            .conf_states = conf_states,
        });
    }

    fn snapshotNodes(self: *const TraceRecorder) ![]NodeSnapshot {
        const peer_ids = self.cluster.peerIds();
        const out = try self.alloc.alloc(NodeSnapshot, peer_ids.len);
        for (peer_ids, 0..) |id, i| {
            const status = self.cluster.nodes[i].status();
            out[i] = .{
                .node_id = id,
                .role = status.soft.role,
                .leader_id = status.soft.leader_id,
                .term = status.hard.current_term,
                .voted_for = status.hard.voted_for,
                .commit_index = status.hard.commit_index,
            };
        }
        return out;
    }

    fn snapshotMessages(self: *const TraceRecorder) ![]MessageSummary {
        const pending = self.cluster.pendingMessageSlice();
        const out = try self.alloc.alloc(MessageSummary, pending.len);
        for (pending, 0..) |msg, i| {
            const first_index = if (msg.entries.len > 0)
                msg.entries[0].index
            else if (msg.snapshot) |snapshot|
                snapshot.metadata.index
            else
                0;
            const last_index = if (msg.entries.len > 0)
                msg.entries[msg.entries.len - 1].index
            else if (msg.snapshot) |snapshot|
                snapshot.metadata.index
            else
                0;
            out[i] = .{
                .msg_type = msg.msg_type,
                .from = msg.from,
                .to = msg.to,
                .term = msg.term,
                .log_index = if (msg.snapshot) |snapshot| snapshot.metadata.index else msg.log_index,
                .log_term = if (msg.snapshot) |snapshot| snapshot.metadata.term else msg.log_term,
                .commit_index = msg.commit_index,
                .reject = msg.reject,
                .reject_hint = msg.reject_hint,
                .entries_len = msg.entries.len,
                .first_entry_index = first_index,
                .last_entry_index = last_index,
            };
        }
        return out;
    }

    fn snapshotCommitted(self: *const TraceRecorder) ![]CommittedSummary {
        const peer_ids = self.cluster.peerIds();
        var count: usize = 0;
        for (peer_ids) |id| {
            if (self.cluster.queuedCommittedSlice(id).len > 0) count += 1;
        }

        const out = try self.alloc.alloc(CommittedSummary, count);
        var next: usize = 0;
        for (peer_ids) |id| {
            const entries = self.cluster.queuedCommittedSlice(id);
            if (entries.len == 0) continue;
            out[next] = .{
                .node_id = id,
                .count = entries.len,
                .first_index = entries[0].index,
                .last_index = entries[entries.len - 1].index,
            };
            next += 1;
        }
        return out;
    }

    fn snapshotReadStates(self: *const TraceRecorder) ![]ReadStateSummary {
        const peer_ids = self.cluster.peerIds();
        var count: usize = 0;
        for (peer_ids) |id| {
            count += self.cluster.queuedReadStateSlice(id).len;
        }

        const out = try self.alloc.alloc(ReadStateSummary, count);
        var next: usize = 0;
        errdefer {
            for (out[0..next]) |*read_state| read_state.deinit(self.alloc);
            self.alloc.free(out);
        }

        for (peer_ids) |id| {
            for (self.cluster.queuedReadStateSlice(id)) |read_state| {
                out[next] = .{
                    .node_id = id,
                    .index = read_state.index,
                    .request_ctx = try self.alloc.dupe(u8, read_state.request_ctx),
                };
                next += 1;
            }
        }
        return out;
    }

    fn snapshotConfStates(self: *const TraceRecorder) ![]ConfStateSummary {
        const peer_ids = self.cluster.peerIds();
        const out = try self.alloc.alloc(ConfStateSummary, peer_ids.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*conf_state| conf_state.deinit(self.alloc);
            self.alloc.free(out);
        }

        for (peer_ids, 0..) |id, i| {
            const status = self.cluster.nodes[i].status();
            out[i] = .{
                .node_id = id,
                .voters = try self.alloc.dupe(core.types.NodeId, status.conf_state.voters),
                .voters_outgoing = try self.alloc.dupe(core.types.NodeId, status.conf_state.voters_outgoing),
                .learners = try self.alloc.dupe(core.types.NodeId, status.conf_state.learners),
                .learners_next = try self.alloc.dupe(core.types.NodeId, status.conf_state.learners_next),
                .auto_leave = status.conf_state.auto_leave,
            };
            initialized += 1;
        }
        return out;
    }
};

pub fn recordCanonicalLeaderProposal(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.init(alloc, &.{ 1, 2, 3 });
    errdefer trace.deinit();

    try trace.tick(1, 3);
    try trace.deliverAll();
    try trace.propose(1, "hello");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCampaignProposal(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "hello");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialPreVoteCampaignProposal(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "hello");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialPreVoteCheckQuorumLeaseProtection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.campaign(3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransfer(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.transferLeader(1, 2);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransferSlowFollower(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialLeaderTransferSlowFollowerWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumLeaderTransferSlowFollower(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialLeaderTransferSlowFollowerWithCheckQuorum(alloc, true);
}

fn recordDifferentialLeaderTransferSlowFollowerWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.propose(1, "stale");
    try trace.deliverAll();
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.transferLeader(1, 3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransferTimeout(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialLeaderTransferTimeoutWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumLeaderTransferTimeout(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialLeaderTransferTimeoutWithCheckQuorum(alloc, true);
}

fn recordDifferentialLeaderTransferTimeoutWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.transferLeader(1, 3);
    try trace.deliverAll();
    try trace.tick(1, 3);
    try trace.propose(1, "after-timeout");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransferFromFollower(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.transferLeader(2, 3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransferReplacePending(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialLeaderTransferReplacePendingWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumLeaderTransferReplacePending(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialLeaderTransferReplacePendingWithCheckQuorum(alloc, true);
}

fn recordDifferentialLeaderTransferReplacePendingWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.transferLeader(1, 3);
    try trace.transferLeader(1, 2);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCheckQuorumTransferJointReconfig(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialCheckQuorumTransferJointReconfigWithAsync(alloc, false);
}

pub fn recordDifferentialAsyncCheckQuorumTransferJointReconfig(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialCheckQuorumTransferJointReconfigWithAsync(alloc, true);
}

fn recordDifferentialCheckQuorumTransferJointReconfigWithAsync(alloc: std.mem.Allocator, async_storage_writes: bool) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .async_storage_writes = async_storage_writes,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 4);
    try trace.block(4, 1);
    try trace.block(2, 4);
    try trace.block(4, 2);
    try trace.block(3, 4);
    try trace.block(4, 3);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.transferLeader(1, 2);
    try trace.deliverAll();
    try trace.propose(2, "during-transfer-joint");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransferSamePendingTimeout(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.transferLeader(1, 3);
    try trace.tick(1, 1);
    try trace.transferLeader(1, 3);
    try trace.tick(1, 2);
    try trace.propose(1, "after-same-timeout");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransferHigherTermAbort(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.transferLeader(1, 3);
    try trace.campaign(2);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLeaderTransferLearnerTarget(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_learner_node,
            .node_id = 4,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .auto,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.transferLeader(1, 4);
    try trace.deliverAll();
    try trace.propose(1, "after-learner-transfer");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndex(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "read-one");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialFollowerReadIndex(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(2, "follower-read");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialFollowerProposal(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(2, "follower-proposal");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialFollowerConfChange(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_learner_node,
        .node_id = 4,
    }});
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(2, .{
        .transition = .auto,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.propose(1, "after-forwarded-conf-change");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialDisableProposalForwardingLeaderProposal(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .disable_proposal_forwarding = true,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "leader-proposal-forwarding-disabled");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialForgetLeader(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.forgetLeader(2);

    return trace;
}

pub fn recordDifferentialForgetLeaderLeaseBased(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.forgetLeader(2);

    return trace;
}

pub fn recordDifferentialMaxInflightGating(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .max_inflight_msgs = 1,
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "first");
    try trace.deliverOne();
    try trace.deliverOne();
    try trace.propose(1, "second");

    return trace;
}

pub fn recordDifferentialAsyncStorageWrites(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .async_storage_writes = true,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "async-storage");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialMaxSizePerMsgBatching(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .max_size_per_msg = 1,
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.propose(1, "a");
    try trace.deliverAll();
    try trace.propose(1, "b");
    try trace.deliverAll();
    try trace.unblock(1, 2);
    try trace.unblock(2, 1);
    try trace.propose(1, "c");

    return trace;
}

pub fn recordDifferentialMaxUncommittedEntriesSize(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .max_uncommitted_entries_size = 1,
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.propose(1, "ab");
    try trace.deliverAll();
    try trace.proposeDropped(1, "c");
    try trace.unblock(1, 2);
    try trace.unblock(2, 1);
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexLeaseBased(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(2, "lease-read");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexLeaseBasedJointConfig(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.readIndex(1, "lease-joint-read");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexMultiPending(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexMultiPendingWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumReadIndexMultiPending(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexMultiPendingWithCheckQuorum(alloc, true);
}

fn recordDifferentialReadIndexMultiPendingWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "ctx-one");
    try trace.readIndex(1, "ctx-two");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexJointConfig(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.readIndex(1, "joint-read");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexDuringLeaderTransfer(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexDuringLeaderTransferWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumReadIndexDuringLeaderTransfer(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexDuringLeaderTransferWithCheckQuorum(alloc, true);
}

fn recordDifferentialReadIndexDuringLeaderTransferWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.transferLeader(1, 2);
    try trace.readIndex(1, "during-transfer");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexLeaseBasedDuringLeaderTransfer(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.transferLeader(1, 2);
    try trace.readIndex(1, "lease-during-transfer");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexLeaseBasedRestart(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "lease-before-restart");
    try trace.deliverAll();
    try trace.restart(1);
    try trace.restart(2);
    try trace.restart(3);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "lease-after-restart");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexLeaseBasedReelection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.tick(1, 8);
    try trace.deliverAll();
    try trace.tick(2, 8);
    try trace.tick(3, 8);
    try trace.deliverAll();
    try trace.campaign(2);
    try trace.deliverAll();
    const reelected_leader = blk: {
        if (trace.cluster.node(2).status().soft.role == .leader) break :blk @as(core.types.NodeId, 2);
        if (trace.cluster.node(3).status().soft.role == .leader) break :blk @as(core.types.NodeId, 3);
        return error.NotLeader;
    };
    try trace.readIndex(reelected_leader, "lease-after-reelection");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexLeaseBasedAutomaticReelection(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexLeaseBasedAutomaticReelectionWithPreVote(alloc, false);
}

pub fn recordDifferentialPreVoteReadIndexLeaseBasedAutomaticReelection(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexLeaseBasedAutomaticReelectionWithPreVote(alloc, true);
}

fn recordDifferentialReadIndexLeaseBasedAutomaticReelectionWithPreVote(alloc: std.mem.Allocator, pre_vote: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = pre_vote,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.setRandomizedElectionTimeout(2, 6);
    try trace.setRandomizedElectionTimeout(3, 9);
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.readIndex(1, "lease-before-expiry");
    try trace.tick(1, 6);
    try trace.tick(2, 6);
    try trace.tick(3, 5);
    try trace.deliverAll();
    try trace.readIndex(2, "lease-after-auto-reelection");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexLeaseBasedExpiryAndReelection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.setRandomizedElectionTimeout(2, 6);
    try trace.setRandomizedElectionTimeout(3, 9);
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.readIndex(1, "lease-before-expiry");
    try trace.tick(1, 6);
    try trace.deliverAll();
    try trace.readIndexNotLeader(1, "lease-after-expiry");
    try trace.tick(2, 6);
    try trace.tick(3, 5);
    try trace.deliverAll();
    try trace.readIndex(2, "lease-after-replacement");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncReadIndexLeaseBased(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "lease-async-leader");
    try trace.deliverAll();
    try trace.readIndex(2, "lease-async-follower");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncReadIndexLeaseBasedJointConfig(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.readIndex(1, "lease-async-joint");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncReadIndexLeaseBasedTransfer(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.transferLeader(1, 2);
    try trace.readIndex(1, "lease-async-during-transfer");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncReadIndexLeaseBasedRestart(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "lease-async-before-restart");
    try trace.deliverAll();
    try trace.restart(1);
    try trace.restart(2);
    try trace.restart(3);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "lease-async-after-restart");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncReadIndexLeaseBasedReelection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.setRandomizedElectionTimeout(2, 100);
    try trace.setRandomizedElectionTimeout(3, 100);
    try trace.tick(1, 8);
    try trace.deliverAll();
    try trace.tick(2, 3);
    try trace.tick(3, 3);
    try trace.deliverAll();
    try trace.campaign(2);
    try trace.deliverAll();
    const reelected_leader = blk: {
        if (trace.cluster.node(2).status().soft.role == .leader) break :blk @as(core.types.NodeId, 2);
        if (trace.cluster.node(3).status().soft.role == .leader) break :blk @as(core.types.NodeId, 3);
        return error.NotLeader;
    };
    try trace.readIndex(reelected_leader, "lease-async-after-reelection");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncReadIndexLeaseBasedExpiryAndReelection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.setRandomizedElectionTimeout(2, 6);
    try trace.setRandomizedElectionTimeout(3, 9);
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.readIndex(1, "lease-async-before-expiry");
    try trace.tick(1, 6);
    try trace.deliverAll();
    try trace.readIndexNotLeader(1, "lease-async-after-expiry");
    try trace.tick(2, 6);
    try trace.tick(3, 5);
    try trace.deliverAll();
    try trace.readIndex(2, "lease-async-after-replacement");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncPreVoteReadIndexLeaseBasedAutomaticReelection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.setRandomizedElectionTimeout(2, 6);
    try trace.setRandomizedElectionTimeout(3, 9);
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.readIndex(1, "lease-async-before-expiry");
    try trace.tick(1, 6);
    try trace.tick(2, 6);
    try trace.tick(3, 5);
    try trace.deliverAll();
    try trace.readIndex(2, "lease-async-after-auto-reelection");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialAsyncPreVoteReadIndexLeaseBasedExpiryAndReelection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.setRandomizedElectionTimeout(2, 6);
    try trace.setRandomizedElectionTimeout(3, 9);
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.readIndex(1, "lease-async-before-expiry");
    try trace.tick(1, 6);
    try trace.deliverAll();
    try trace.readIndexNotLeader(1, "lease-async-after-expiry");
    try trace.tick(2, 6);
    try trace.tick(3, 5);
    try trace.deliverAll();
    try trace.readIndex(2, "lease-async-after-replacement");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexRestartClearsPending(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexRestartClearsPendingWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumReadIndexRestartClearsPending(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialReadIndexRestartClearsPendingWithCheckQuorum(alloc, true);
}

fn recordDifferentialReadIndexRestartClearsPendingWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.readIndex(1, "stale-before-restart");
    try trace.deliverAll();
    try trace.restart(1);
    if (check_quorum) {
        try trace.restart(2);
        try trace.restart(3);
    }
    try trace.unblock(1, 2);
    try trace.unblock(2, 1);
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    if (check_quorum) {
        try trace.campaign(1);
    } else {
        try trace.campaign(1);
    }
    try trace.deliverAll();
    try trace.readIndex(1, "fresh-after-restart");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialReadIndexReelectionClearsPending(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.readIndex(1, "stale-before-reelection");
    try trace.deliverAll();
    try trace.unblock(2, 3);
    try trace.unblock(3, 2);
    try trace.campaign(2);
    try trace.deliverAll();
    try trace.unblock(1, 2);
    try trace.unblock(2, 1);
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(2, 1);
    try trace.deliverAll();
    try trace.block(2, 1);
    try trace.block(1, 2);
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.readIndex(1, "fresh-after-reelection");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialLearnerCatchup(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{1};
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_learner_node,
        .node_id = 2,
    }});
    defer alloc.free(changes);

    try trace.campaignSettle(1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialJointAutoLeave(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{1}, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_learner_node,
        .node_id = 2,
    }});
    defer alloc.free(changes);

    try trace.campaignSettle(1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });

    return trace;
}

pub fn recordDifferentialJointExplicitLeave(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialJointExplicitLeaveWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumJointExplicitLeave(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialJointExplicitLeaveWithCheckQuorum(alloc, true);
}

pub fn recordDifferentialCheckQuorumJointIncomingVoterReplacement(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_node,
        .node_id = 3,
    }});
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(1, 3);
    try trace.tick(1, 6);
    try trace.deliverAll();
    try trace.tick(2, 2);
    try trace.tick(3, 2);
    try trace.unblock(1, 2);
    try trace.unblock(1, 3);
    try trace.campaign(3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialPreVoteCheckQuorumJointIncomingVoterReplacement(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_node,
        .node_id = 3,
    }});
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(1, 3);
    try trace.tick(1, 6);
    try trace.deliverAll();
    try trace.tick(2, 2);
    try trace.tick(3, 2);
    try trace.unblock(1, 2);
    try trace.unblock(1, 3);
    try trace.campaign(3);
    try trace.deliverAll();

    return trace;
}

fn recordDifferentialJointExplicitLeaveWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{1}, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_learner_node,
        .node_id = 2,
    }});
    defer alloc.free(changes);

    try trace.campaignSettle(1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.leaveJoint(1);

    return trace;
}

pub fn recordDifferentialJointMultiAddExplicit(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{1};
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer alloc.free(changes);

    try trace.campaignSettle(1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialRestartJointReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartJointReplayWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumRestartJointReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartJointReplayWithCheckQuorum(alloc, true);
}

fn recordDifferentialRestartJointReplayWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{1};
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer alloc.free(changes);

    try trace.campaignSettle(1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.restart(1);

    return trace;
}

pub fn recordDifferentialRestartJointMixedReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartJointMixedReplayWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumRestartJointMixedReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartJointMixedReplayWithCheckQuorum(alloc, true);
}

fn recordDifferentialRestartJointMixedReplayWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 5,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.restart(1);

    return trace;
}

pub fn recordDifferentialRestartJointPartialReplay(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.restart(1);
    try trace.unblock(1, 2);
    try trace.unblock(2, 1);
    try trace.campaign(1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialRestartJointReaddRemovedReplay(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const first_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer alloc.free(first_changes);

    const second_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer alloc.free(second_changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = first_changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.deliverAll();
    try trace.clearBlocks();
    try trace.compact(1, 3);
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = second_changes,
    });
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialRestartJointDemotePromoteReplay(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const demote_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer alloc.free(demote_changes);

    const promote_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .add_node,
            .node_id = 3,
        },
    });
    defer alloc.free(promote_changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = demote_changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.compact(1, 3);
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .auto,
        .changes = promote_changes,
    });
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialRestartJointSnapshotReplay(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 5,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.compact(1, 2);
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.propose(1, "after-restart-joint-snapshot");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCheckQuorumRestartJointSnapshotProgress(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 5 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.compact(1, 2);
    try trace.restart(1);
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(3, 2);
    try trace.block(2, 3);
    try trace.block(4, 2);
    try trace.block(2, 4);
    try trace.block(5, 2);
    try trace.block(2, 5);
    try trace.leaveJoint(3);
    try trace.deliverAll();
    try trace.propose(3, "after-check-quorum-joint-restart");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialRestartJointLeaderChurnReplay(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.restart(1);
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(1, 4);
    try trace.block(4, 1);
    try trace.campaign(2);
    try trace.deliverAll();
    try trace.leaveJoint(2);
    try trace.deliverAll();
    try trace.propose(2, "after-restart-joint-churn");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialPreVoteCheckQuorumRestartJointSnapshotProgress(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 5 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.compact(1, 2);
    try trace.restart(1);
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(3, 2);
    try trace.block(2, 3);
    try trace.block(4, 2);
    try trace.block(2, 4);
    try trace.block(5, 2);
    try trace.block(2, 5);
    try trace.leaveJoint(3);
    try trace.deliverAll();
    try trace.propose(3, "after-pre-vote-check-quorum-joint-restart");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialJointReplaceVoter(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.deliverAll();
    try trace.clearBlocks();

    return trace;
}

pub fn recordDifferentialJointDemoteVoter(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialJointMixedChange(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialJointMixedChangeWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumJointMixedChange(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialJointMixedChangeWithCheckQuorum(alloc, true);
}

pub fn recordDifferentialCheckQuorumJointDemotedOutgoingElection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(1, 3);
    try trace.tick(1, 6);
    try trace.deliverAll();
    try trace.tick(2, 2);
    try trace.tick(3, 2);
    try trace.unblock(1, 2);
    try trace.unblock(1, 3);
    try trace.campaign(3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialPreVoteCheckQuorumJointDemotedOutgoingElection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = true,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 3,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(1, 3);
    try trace.tick(1, 6);
    try trace.deliverAll();
    try trace.tick(2, 2);
    try trace.tick(3, 2);
    try trace.unblock(1, 2);
    try trace.unblock(1, 3);
    try trace.campaign(3);
    try trace.deliverAll();

    return trace;
}

fn recordDifferentialJointMixedChangeWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 5,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.propose(1, "after-mixed");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCheckQuorumPartitionJointChurn(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.propose(1, "after-partition-joint");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialJointChainedMixedChange(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5, 6 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const first_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 5,
        },
    });
    defer alloc.free(first_changes);

    const second_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 3,
        },
        .{
            .change_type = .add_node,
            .node_id = 6,
        },
        .{
            .change_type = .add_learner_node,
            .node_id = 2,
        },
    });
    defer alloc.free(second_changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = first_changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = second_changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.deliverAll();
    try trace.propose(1, "after-chained-mixed");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialJointIdempotentMixed(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{1};
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 9 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 1 },
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 9 },
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
    });
    defer alloc.free(changes);

    try trace.campaignSettle(1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialJointLearnersNextChurn(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const demote_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
    });
    defer alloc.free(demote_changes);

    const promote_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 4 },
    });
    defer alloc.free(promote_changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = demote_changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = promote_changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.propose(1, "after-learners-next-churn");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialStepDownOnRemoval(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .step_down_on_removal = true,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 1 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_implicit,
        .changes = changes,
    });
    try trace.collectReady(1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialRestartLearnerReaddReplay(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const add_learner = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .add_learner_node,
        .node_id = 3,
    }});
    defer alloc.free(add_learner);

    const remove_learner = try alloc.dupe(core.types.ConfChangeSingle, &.{.{
        .change_type = .remove_node,
        .node_id = 3,
    }});
    defer alloc.free(remove_learner);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .auto,
        .changes = add_learner,
    });
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .auto,
        .changes = remove_learner,
    });
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.deliverAll();
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.unblock(2, 3);
    try trace.unblock(3, 2);
    try trace.proposeConfChangeV2(1, .{
        .transition = .auto,
        .changes = add_learner,
    });
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialSnapshotReaddRemovedVoter(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const first_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer alloc.free(first_changes);

    const second_changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .add_node, .node_id = 3 },
    });
    defer alloc.free(second_changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = first_changes,
    });
    try trace.deliverAll();
    try trace.leaveJoint(1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.deliverAll();
    try trace.compact(1, 3);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = second_changes,
    });
    try trace.deliverAll();
    try trace.block(1, 4);
    try trace.block(4, 1);
    try trace.block(2, 4);
    try trace.block(4, 2);
    try trace.leaveJoint(1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialSnapshotJointTransport(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{
            .change_type = .remove_node,
            .node_id = 2,
        },
        .{
            .change_type = .add_node,
            .node_id = 4,
        },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 4);
    try trace.block(4, 1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.compact(1, 2);
    try trace.unblock(1, 4);
    try trace.unblock(4, 1);
    try trace.leaveJoint(1);
    try trace.deliverAll();
    try trace.propose(1, "after-joint-snapshot");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialFollowerCatchup(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.propose(1, "one");
    try trace.deliverAll();
    try trace.unblock(1, 3);
    try trace.propose(1, "two");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialSplitVoteRetry(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    const left = [_]core.types.NodeId{ 1, 2 };
    const right = [_]core.types.NodeId{ 3, 4 };
    for (left) |from| {
        for (right) |to| {
            try trace.block(from, to);
            try trace.block(to, from);
        }
    }

    try trace.campaign(1);
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.clearBlocks();
    try trace.campaign(1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialConflictBacktrack(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();

    const left = [_]core.types.NodeId{ 1, 2 };
    const right = [_]core.types.NodeId{ 3, 4, 5 };
    for (left) |from| {
        for (right) |to| {
            try trace.block(from, to);
            try trace.block(to, from);
        }
    }

    try trace.propose(1, "stale");
    try trace.deliverAll();

    try trace.campaign(3);
    try trace.deliverAll();

    try trace.clearBlocks();
    try trace.propose(3, "fresh");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialRestartReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartReplayWithCheckQuorum(alloc, false);
}

pub fn recordDifferentialCheckQuorumRestartReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartReplayWithCheckQuorum(alloc, true);
}

pub fn recordDifferentialRestartAppliedReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartAppliedReplayWithOptions(alloc, .{
        .check_quorum = false,
        .async_storage_writes = false,
    });
}

pub fn recordDifferentialCheckQuorumRestartAppliedReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartAppliedReplayWithOptions(alloc, .{
        .check_quorum = true,
        .async_storage_writes = false,
    });
}

pub fn recordDifferentialAsyncRestartAppliedReplay(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordDifferentialRestartAppliedReplayWithOptions(alloc, .{
        .check_quorum = false,
        .async_storage_writes = true,
    });
}

fn recordDifferentialRestartReplayWithCheckQuorum(alloc: std.mem.Allocator, check_quorum: bool) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = check_quorum,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "one");
    try trace.deliverAll();
    try trace.restart(2);

    return trace;
}

const RestartAppliedOptions = struct {
    check_quorum: bool,
    async_storage_writes: bool,
};

fn recordDifferentialRestartAppliedReplayWithOptions(
    alloc: std.mem.Allocator,
    options: RestartAppliedOptions,
) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = options.check_quorum,
        .pre_vote = false,
        .async_storage_writes = options.async_storage_writes,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "one");
    try trace.deliverAll();
    try trace.propose(1, "two");
    try trace.deliverAll();
    try trace.drainCommitted(2);
    try trace.restartWithApplied(2, 1);

    return trace;
}

pub fn recordDifferentialRejectBackoff(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4, 5 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    const minority = [_]core.types.NodeId{ 1, 2 };
    const majority = [_]core.types.NodeId{ 3, 4, 5 };

    try trace.campaign(1);
    try trace.deliverAll();

    for (minority) |from| {
        for (majority) |to| {
            try trace.block(from, to);
            try trace.block(to, from);
        }
    }

    try trace.propose(1, "stale-one");
    try trace.deliverAll();
    try trace.propose(1, "stale-two");
    try trace.deliverAll();

    try trace.campaign(3);
    try trace.deliverAll();

    try trace.clearBlocks();
    try trace.propose(3, "fresh");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialSnapshotCompactionReplay(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.propose(1, "one");
    try trace.deliverAll();
    try trace.compact(2, 2);
    try trace.restart(2);

    return trace;
}

pub fn recordDifferentialSnapshotTransport(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();

    try trace.block(1, 3);
    try trace.block(3, 1);

    try trace.propose(1, "one");
    try trace.deliverAll();

    try trace.compact(1, 2);
    try trace.restart(1);

    try trace.campaign(1);
    try trace.deliverAll();

    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(1, 1);
    try trace.deliverAll();
    try trace.propose(1, "two");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialSnapshotSuccess(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();

    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.propose(1, "one");
    try trace.deliverAll();

    try trace.compact(1, 2);
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();

    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(1, 1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialSnapshotFailure(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();

    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.propose(1, "one");
    try trace.deliverAll();

    try trace.compact(1, 2);
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();

    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(1, 1);
    try trace.rejectSnapshot(1, 3);

    return trace;
}

pub fn recordDifferentialSnapshotAbort(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();

    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.propose(1, "one");
    try trace.deliverAll();

    try trace.compact(1, 2);
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();

    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(1, 1);
    try trace.abortSnapshot(1, 3, 2);

    return trace;
}

pub fn recordDifferentialSnapshotRetry(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();

    try trace.block(1, 3);
    try trace.block(3, 1);

    try trace.propose(1, "one");
    try trace.deliverAll();

    try trace.compact(1, 2);
    try trace.restart(1);
    try trace.campaign(1);
    try trace.deliverAll();

    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(1, 1);
    try trace.rejectSnapshot(1, 3);
    try trace.deliverAll();
    try trace.tick(1, 1);
    try trace.deliverAll();
    try trace.propose(1, "two");
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialMembershipChange(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();

    try trace.proposeConfChange(1, .{
        .change_type = .remove_node,
        .node_id = 3,
    });
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.deliverAll();

    try trace.propose(1, "after-remove");
    try trace.deliverAll();

    return trace;
}

pub const SeededTraceOptions = struct {
    pub const Profile = enum {
        stable,
        stress,
    };

    seed: u64,
    steps: usize,
    check_quorum: bool = true,
    pre_vote: bool = true,
    async_storage_writes: bool = false,
    read_only_option: core.ReadOnlyOption = .safe,
    profile: Profile = .stable,
};

pub fn recordSeededDifferentialTrace(alloc: std.mem.Allocator, options: SeededTraceOptions) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = options.check_quorum,
        .pre_vote = options.pre_vote,
        .async_storage_writes = options.async_storage_writes,
        .read_only_option = options.read_only_option,
        .random_seed = options.seed,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    var prng = std.Random.DefaultPrng.init(options.seed);
    const random = prng.random();

    try trace.campaignSettle(1);
    try trace.deliverAll();

    var emitted_steps: usize = 0;
    var attempts: usize = 0;
    while (emitted_steps < options.steps) {
        attempts += 1;
        if (attempts > options.steps * 32) return error.SeededTraceExhausted;

        var action = try chooseSeededAction(alloc, &trace, random, emitted_steps, options.profile);
        defer action.deinit(alloc);
        trace.applyAction(action) catch |err| switch (err) {
            error.NotLeader,
            error.NotPromotable,
            error.LeaderTransferInProgress,
            error.PendingConfChange,
            error.MustLeaveJointFirst,
            error.UnsupportedJointConsensusPath,
            => continue,
            else => return err,
        };
        emitted_steps += 1;
    }

    return trace;
}

pub fn recordDifferentialCheckQuorumLeaderStepDown(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.tick(1, 8);
    try trace.deliverAll();
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.unblock(1, 2);
    try trace.unblock(2, 1);
    try trace.campaign(2);
    try trace.deliverAll();
    try trace.unblock(2, 3);
    try trace.unblock(3, 2);
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCheckQuorumFollowerLeaseProtection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .election_tick = 5,
        .heartbeat_tick = 1,
        .check_quorum = true,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.tick(2, 4);
    try trace.campaign(1);
    try trace.deliverAll();
    try trace.campaign(3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCheckQuorumHigherTermDisruption(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(1, 1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCheckQuorumRepeatedHigherTermDisruption(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.tick(1, 1);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialCheckQuorumSnapshotTransfer(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.propose(1, "snapshot-transfer-one");
    try trace.deliverAll();
    try trace.propose(1, "snapshot-transfer-two");
    try trace.deliverAll();
    try trace.compact(1, 2);
    try trace.unblock(1, 3);
    try trace.transferLeader(1, 3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialStressJointRestartChurn(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = true,
        .random_seed = 1,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 1 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(1, 4);
    try trace.block(4, 1);
    try trace.tick(1, 8);
    try trace.deliverAll();
    try trace.restart(1);
    try trace.tick(2, 8);
    try trace.deliverAll();
    try trace.tick(3, 8);
    try trace.deliverAll();
    try trace.tick(4, 8);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialStressSeededRestartConfigChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 5,
        .steps = 18,
        .check_quorum = true,
        .pre_vote = true,
        .profile = .stress,
    });
}

pub fn recordDifferentialStressSeededTransferChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 10,
        .steps = 22,
        .check_quorum = true,
        .pre_vote = true,
        .profile = .stress,
    });
}

pub fn recordDifferentialStressSeededLeaseTransferChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 10,
        .steps = 22,
        .check_quorum = true,
        .pre_vote = true,
        .read_only_option = .lease_based,
        .profile = .stress,
    });
}

pub fn recordDifferentialStressSeededSnapshotRestoreChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 1,
        .steps = 28,
        .check_quorum = true,
        .pre_vote = true,
        .profile = .stress,
    });
}

pub fn recordDifferentialStressSeededAsyncChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 0,
        .steps = 24,
        .check_quorum = true,
        .pre_vote = true,
        .async_storage_writes = true,
        .profile = .stress,
    });
}

pub fn recordDifferentialStressSeededAsyncLeaseChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 0,
        .steps = 24,
        .check_quorum = true,
        .pre_vote = true,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
        .profile = .stress,
    });
}

pub fn recordDifferentialStressSeededAsyncRestartSnapshotChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 1,
        .steps = 28,
        .check_quorum = true,
        .pre_vote = true,
        .async_storage_writes = true,
        .profile = .stress,
    });
}

pub fn recordDifferentialStressSeededAsyncLeaseRestartSnapshotChurn(alloc: std.mem.Allocator) !TraceRecorder {
    return try recordSeededDifferentialTrace(alloc, .{
        .seed = 1,
        .steps = 28,
        .check_quorum = true,
        .pre_vote = true,
        .async_storage_writes = true,
        .read_only_option = .lease_based,
        .profile = .stress,
    });
}

pub fn recordDifferentialSnapshotJointReelectionOverlap(alloc: std.mem.Allocator) !TraceRecorder {
    var initial_voters = [_]core.types.NodeId{ 1, 2, 3 };
    const initial_conf_state = core.types.ConfState{
        .voters = initial_voters[0..],
    };

    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3, 4 }, .{
        .check_quorum = true,
        .pre_vote = false,
        .initial_conf_state = initial_conf_state,
    });
    errdefer trace.deinit();

    const changes = try alloc.dupe(core.types.ConfChangeSingle, &.{
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 4 },
    });
    defer alloc.free(changes);

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 4);
    try trace.block(4, 1);
    try trace.proposeConfChangeV2(1, .{
        .transition = .joint_explicit,
        .changes = changes,
    });
    try trace.deliverAll();
    try trace.compact(1, 2);
    try trace.unblock(1, 4);
    try trace.unblock(4, 1);
    try trace.deliverAll();
    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.campaign(3);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialSnapshotLeadershipChurnRecovery(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = true,
        .pre_vote = false,
    });
    errdefer trace.deinit();

    try trace.campaign(1);
    try trace.deliverAll();
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.propose(1, "one");
    try trace.deliverAll();
    try trace.propose(1, "two");
    try trace.deliverAll();
    try trace.compact(1, 2);
    try trace.compact(2, 2);
    try trace.transferLeader(1, 2);
    try trace.deliverAll();

    return trace;
}

pub fn recordDifferentialPreVoteMigrationElection(alloc: std.mem.Allocator) !TraceRecorder {
    var trace = try TraceRecorder.initWithOptions(alloc, &.{ 1, 2, 3 }, .{
        .check_quorum = false,
        .pre_vote = true,
    });
    errdefer trace.deinit();

    try trace.restartWithPreVote(3, false);
    try trace.campaign(1);
    try trace.deliverAll();

    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.block(2, 3);
    try trace.block(3, 2);
    try trace.propose(1, "some data");
    try trace.deliverAll();
    try trace.campaign(3);
    try trace.campaign(3);
    try trace.deliverAll();

    try trace.restartWithPreVote(3, true);
    try trace.unblock(1, 3);
    try trace.unblock(3, 1);
    try trace.unblock(2, 3);
    try trace.unblock(3, 2);

    try trace.block(1, 2);
    try trace.block(2, 1);
    try trace.block(1, 3);
    try trace.block(3, 1);
    try trace.campaign(3);
    try trace.campaign(2);
    try trace.deliverAll();
    try trace.campaign(3);
    try trace.deliverAll();
    try trace.campaign(2);
    try trace.deliverAll();

    return trace;
}

fn chooseSeededAction(
    alloc: std.mem.Allocator,
    trace: *TraceRecorder,
    random: std.Random,
    step_idx: usize,
    profile: SeededTraceOptions.Profile,
) !Action {
    const cluster = trace.clusterPtr();
    if (cluster.pendingMessageSlice().len > 0 and random.uintLessThan(u32, 100) < 35) {
        return .{ .deliver_all = {} };
    }
    if (profile == .stress and
        cluster.blockedLinkCount() > 0 and
        cluster.pendingMessageSlice().len == 0 and
        random.uintLessThan(u32, 100) < 15)
    {
        return .{ .clear_blocks = {} };
    }

    const active_nodes = try activeNodeIds(alloc, cluster);
    defer if (active_nodes.len > 0) alloc.free(active_nodes);

    var attempts: usize = 0;
    while (attempts < 16) : (attempts += 1) {
        const leader = currentLeader(cluster);
        switch (switch (profile) {
            .stable => random.uintLessThan(u32, 6),
            .stress => random.uintLessThan(u32, 9),
        }) {
            0 => if (leader) |leader_id| {
                const node_id = leader_id;
                return .{ .tick = .{
                    .node_id = node_id,
                    .count = 1 + random.uintLessThan(usize, 3),
                } };
            },
            1 => if (leader) |leader_id| {
                return .{ .propose = .{
                    .node_id = leader_id,
                    .data = try std.fmt.allocPrint(alloc, "seed-{d}-op-{d}", .{ random.int(u32), step_idx }),
                } };
            },
            2 => if (leader) |leader_id| {
                return .{ .read_index = .{
                    .node_id = leader_id,
                    .request_ctx = try std.fmt.allocPrint(alloc, "seed-read-{d}-{d}", .{ random.int(u32), step_idx }),
                } };
            },
            3 => if (leader) |leader_id| {
                if (cluster.pendingMessageSlice().len > 0) continue;
                if (chooseTransferTarget(cluster, leader_id, random)) |target| {
                    return .{ .transfer_leader = .{
                        .node_id = leader_id,
                        .target_node_id = target,
                    } };
                }
            },
            4 => if (chooseSeededRestartAction(active_nodes, leader, cluster, random)) |action| {
                return action;
            },
            5 => switch (profile) {
                .stable => {
                    if (leader != null) continue;
                    if (active_nodes.len == 0) continue;
                    const node_id = active_nodes[random.uintLessThan(usize, active_nodes.len)];
                    return .{ .campaign = .{ .node_id = node_id } };
                },
                .stress => if (leader) |leader_id| {
                    if (try chooseSeededConfigAction(alloc, cluster, leader_id, random)) |action| {
                        return action;
                    }
                },
            },
            6 => if (profile == .stress) {
                if (chooseSeededPartitionAction(active_nodes, cluster, random)) |action| {
                    return action;
                }
            },
            7 => if (profile == .stress) {
                if (chooseSeededStorageAction(active_nodes, cluster, random)) |action| {
                    return action;
                }
            },
            8 => {
                if (leader != null) continue;
                if (active_nodes.len == 0) continue;
                const node_id = active_nodes[random.uintLessThan(usize, active_nodes.len)];
                return .{ .campaign = .{ .node_id = node_id } };
            },
            else => unreachable,
        }
    }

    return .{ .deliver_all = {} };
}

fn chooseSeededConfigAction(
    alloc: std.mem.Allocator,
    cluster: *Cluster,
    leader_id: core.types.NodeId,
    random: std.Random,
) !?Action {
    const conf_state = cluster.node(leader_id).status().conf_state;
    if (conf_state.voters_outgoing.len > 0) {
        return .{ .leave_joint = .{ .node_id = leader_id } };
    }

    var learners = std.ArrayListUnmanaged(core.types.NodeId).empty;
    defer learners.deinit(alloc);
    for (conf_state.learners) |node_id| {
        if (node_id == leader_id) continue;
        try learners.append(alloc, node_id);
    }
    if (learners.items.len > 0) {
        const target = learners.items[random.uintLessThan(usize, learners.items.len)];
        return .{ .propose_conf_change = .{
            .node_id = leader_id,
            .change_type = .add_node,
            .target_node_id = target,
        } };
    }

    var removable_voters = std.ArrayListUnmanaged(core.types.NodeId).empty;
    defer removable_voters.deinit(alloc);
    for (conf_state.voters) |node_id| {
        if (node_id == leader_id) continue;
        try removable_voters.append(alloc, node_id);
    }
    if (removable_voters.items.len > 0) {
        const target = removable_voters.items[random.uintLessThan(usize, removable_voters.items.len)];
        return .{ .propose_conf_change = .{
            .node_id = leader_id,
            .change_type = if (random.uintLessThan(u32, 2) == 0) .remove_node else .add_learner_node,
            .target_node_id = target,
        } };
    }

    return null;
}

fn activeNodeIds(alloc: std.mem.Allocator, cluster: *const Cluster) ![]core.types.NodeId {
    var ids = std.ArrayListUnmanaged(core.types.NodeId).empty;
    errdefer ids.deinit(alloc);
    for (cluster.peerIds()) |node_id| {
        if (!cluster.isNodeActive(node_id)) continue;
        try ids.append(alloc, node_id);
    }
    return try ids.toOwnedSlice(alloc);
}

fn chooseSeededRestartAction(
    active_nodes: []const core.types.NodeId,
    leader: ?core.types.NodeId,
    cluster: *const Cluster,
    random: std.Random,
) ?Action {
    if (active_nodes.len == 0) return null;
    if (cluster.pendingMessageSlice().len > 0) return null;
    if (cluster.blockedLinkCount() > 0) return null;

    var candidates: [8]core.types.NodeId = undefined;
    var candidates_len: usize = 0;
    for (active_nodes) |node_id| {
        if (leader != null and node_id == leader.?) continue;
        if (candidates_len >= candidates.len) break;
        candidates[candidates_len] = node_id;
        candidates_len += 1;
    }

    if (candidates_len == 0) {
        if (leader != null) return null;
        return .{ .restart_node = .{
            .node_id = active_nodes[random.uintLessThan(usize, active_nodes.len)],
        } };
    }

    return .{ .restart_node = .{
        .node_id = candidates[random.uintLessThan(usize, candidates_len)],
    } };
}

fn chooseSeededPartitionAction(
    active_nodes: []const core.types.NodeId,
    cluster: *const Cluster,
    random: std.Random,
) ?Action {
    if (active_nodes.len < 2) return null;
    if (cluster.pendingMessageSlice().len > 0) return null;

    if (cluster.blockedLinkCount() > 0 and random.uintLessThan(u32, 100) < 30) {
        return .{ .clear_blocks = {} };
    }

    const pair = randomActivePair(active_nodes, random) orelse return null;
    if (cluster.isBlocked(pair.from, pair.to)) {
        return .{ .unblock_link = .{ .from = pair.from, .to = pair.to } };
    }
    return .{ .block_link = .{ .from = pair.from, .to = pair.to } };
}

fn chooseSeededStorageAction(
    active_nodes: []const core.types.NodeId,
    cluster: *Cluster,
    random: std.Random,
) ?Action {
    if (findPendingSnapshot(cluster)) |snapshot| {
        if (random.uintLessThan(u32, 100) < 50) {
            return .{ .reject_snapshot = .{
                .from = snapshot.from,
                .to = snapshot.to,
            } };
        }
        return null;
    }

    if (cluster.pendingMessageSlice().len > 0) return null;

    var candidates: [8]struct {
        node_id: core.types.NodeId,
        compact_index: core.types.Index,
    } = undefined;
    var candidates_len: usize = 0;

    for (active_nodes) |node_id| {
        const status = cluster.node(node_id).status();
        if (status.hard.commit_index == 0) continue;
        if (candidates_len >= candidates.len) break;
        candidates[candidates_len] = .{
            .node_id = node_id,
            .compact_index = status.hard.commit_index,
        };
        candidates_len += 1;
    }

    if (candidates_len == 0) return null;
    const choice = candidates[random.uintLessThan(usize, candidates_len)];
    return .{ .compact_node = .{
        .node_id = choice.node_id,
        .compact_index = choice.compact_index,
    } };
}

fn currentLeader(cluster: *Cluster) ?core.types.NodeId {
    for (cluster.peerIds()) |node_id| {
        if (!cluster.isNodeActive(node_id)) continue;
        const status = cluster.node(node_id).status();
        if (status.soft.role == .leader) return node_id;
    }
    return null;
}

fn seedConfContains(conf_state: core.types.ConfState, node_id: core.types.NodeId) bool {
    return std.mem.indexOfScalar(core.types.NodeId, conf_state.voters, node_id) != null or
        std.mem.indexOfScalar(core.types.NodeId, conf_state.voters_outgoing, node_id) != null or
        std.mem.indexOfScalar(core.types.NodeId, conf_state.learners, node_id) != null or
        std.mem.indexOfScalar(core.types.NodeId, conf_state.learners_next, node_id) != null;
}

fn chooseTransferTarget(cluster: *Cluster, leader_id: core.types.NodeId, random: std.Random) ?core.types.NodeId {
    const status = cluster.node(leader_id).status();
    var targets: [8]core.types.NodeId = undefined;
    var targets_len: usize = 0;
    for (status.conf_state.voters) |node_id| {
        if (node_id == leader_id or !cluster.isNodeActive(node_id)) continue;
        if (targets_len >= targets.len) break;
        targets[targets_len] = node_id;
        targets_len += 1;
    }
    if (targets_len == 0) return null;
    return targets[random.uintLessThan(usize, targets_len)];
}

fn randomActivePair(active_nodes: []const core.types.NodeId, random: std.Random) ?struct { from: core.types.NodeId, to: core.types.NodeId } {
    if (active_nodes.len < 2) return null;
    const from_idx = random.uintLessThan(usize, active_nodes.len);
    var to_idx = random.uintLessThan(usize, active_nodes.len - 1);
    if (to_idx >= from_idx) to_idx += 1;
    return .{
        .from = active_nodes[from_idx],
        .to = active_nodes[to_idx],
    };
}

fn findPendingSnapshot(cluster: *const Cluster) ?message_mod.Message {
    for (cluster.pendingMessageSlice()) |msg| {
        if (msg.msg_type == .snapshot) return msg;
    }
    return null;
}

fn writeActionJson(action: Action, js: *std.json.Stringify) !void {
    try js.beginObject();
    switch (action) {
        .tick => |tick| {
            try js.objectField("kind");
            try js.write("tick");
            try js.objectField("node_id");
            try js.write(tick.node_id);
            try js.objectField("count");
            try js.write(tick.count);
        },
        .set_randomized_election_timeout => |timeout| {
            try js.objectField("kind");
            try js.write("set_randomized_election_timeout");
            try js.objectField("node_id");
            try js.write(timeout.node_id);
            try js.objectField("timeout");
            try js.write(timeout.timeout);
        },
        .campaign => |campaign| {
            try js.objectField("kind");
            try js.write("campaign");
            try js.objectField("node_id");
            try js.write(campaign.node_id);
        },
        .campaign_settle => |campaign| {
            try js.objectField("kind");
            try js.write("campaign_settle");
            try js.objectField("node_id");
            try js.write(campaign.node_id);
        },
        .transfer_leader => |transfer| {
            try js.objectField("kind");
            try js.write("transfer_leader");
            try js.objectField("node_id");
            try js.write(transfer.node_id);
            try js.objectField("target_node_id");
            try js.write(transfer.target_node_id);
        },
        .forget_leader => |forget| {
            try js.objectField("kind");
            try js.write("forget_leader");
            try js.objectField("node_id");
            try js.write(forget.node_id);
        },
        .propose => |propose| {
            try js.objectField("kind");
            try js.write("propose");
            try js.objectField("node_id");
            try js.write(propose.node_id);
            try js.objectField("data");
            try js.write(propose.data);
        },
        .propose_dropped => |propose| {
            try js.objectField("kind");
            try js.write("propose_dropped");
            try js.objectField("node_id");
            try js.write(propose.node_id);
            try js.objectField("data");
            try js.write(propose.data);
        },
        .read_index => |read_index| {
            try js.objectField("kind");
            try js.write("read_index");
            try js.objectField("node_id");
            try js.write(read_index.node_id);
            try js.objectField("request_ctx");
            try js.write(read_index.request_ctx);
        },
        .read_index_not_leader => |read_index| {
            try js.objectField("kind");
            try js.write("read_index_not_leader");
            try js.objectField("node_id");
            try js.write(read_index.node_id);
            try js.objectField("request_ctx");
            try js.write(read_index.request_ctx);
        },
        .propose_conf_change => |conf_change| {
            try js.objectField("kind");
            try js.write("propose_conf_change");
            try js.objectField("node_id");
            try js.write(conf_change.node_id);
            try js.objectField("change_type");
            try js.write(@tagName(conf_change.change_type));
            try js.objectField("target_node_id");
            try js.write(conf_change.target_node_id);
        },
        .propose_conf_change_v2 => |conf_change| {
            try js.objectField("kind");
            try js.write("propose_conf_change_v2");
            try js.objectField("node_id");
            try js.write(conf_change.node_id);
            try js.objectField("transition");
            try js.write(@tagName(conf_change.transition));
            try js.objectField("changes");
            try js.beginArray();
            for (conf_change.changes) |change| {
                try js.beginObject();
                try js.objectField("change_type");
                try js.write(@tagName(change.change_type));
                try js.objectField("target_node_id");
                try js.write(change.target_node_id);
                try js.endObject();
            }
            try js.endArray();
        },
        .leave_joint => |leave_joint| {
            try js.objectField("kind");
            try js.write("leave_joint");
            try js.objectField("node_id");
            try js.write(leave_joint.node_id);
        },
        .collect_ready => |collect_ready| {
            try js.objectField("kind");
            try js.write("collect_ready");
            try js.objectField("node_id");
            try js.write(collect_ready.node_id);
        },
        .drain_committed => |drain| {
            try js.objectField("kind");
            try js.write("drain_committed");
            try js.objectField("node_id");
            try js.write(drain.node_id);
        },
        .restart_node => |restart| {
            try js.objectField("kind");
            try js.write("restart_node");
            try js.objectField("node_id");
            try js.write(restart.node_id);
        },
        .restart_node_with_applied => |restart| {
            try js.objectField("kind");
            try js.write("restart_node_with_applied");
            try js.objectField("node_id");
            try js.write(restart.node_id);
            try js.objectField("applied");
            try js.write(restart.applied);
        },
        .restart_node_with_pre_vote => |restart| {
            try js.objectField("kind");
            try js.write("restart_node_with_pre_vote");
            try js.objectField("node_id");
            try js.write(restart.node_id);
            try js.objectField("pre_vote");
            try js.write(restart.pre_vote);
        },
        .compact_node => |compact| {
            try js.objectField("kind");
            try js.write("compact_node");
            try js.objectField("node_id");
            try js.write(compact.node_id);
            try js.objectField("compact_index");
            try js.write(compact.compact_index);
        },
        .reject_snapshot => |reject_snapshot| {
            try js.objectField("kind");
            try js.write("reject_snapshot");
            try js.objectField("from");
            try js.write(reject_snapshot.from);
            try js.objectField("to");
            try js.write(reject_snapshot.to);
        },
        .abort_snapshot => |abort_snapshot| {
            try js.objectField("kind");
            try js.write("abort_snapshot");
            try js.objectField("from");
            try js.write(abort_snapshot.from);
            try js.objectField("to");
            try js.write(abort_snapshot.to);
            try js.objectField("log_index");
            try js.write(abort_snapshot.log_index);
        },
        .deliver_one => {
            try js.objectField("kind");
            try js.write("deliver_one");
        },
        .deliver_all => {
            try js.objectField("kind");
            try js.write("deliver_all");
        },
        .block_link => |block| {
            try js.objectField("kind");
            try js.write("block_link");
            try js.objectField("from");
            try js.write(block.from);
            try js.objectField("to");
            try js.write(block.to);
        },
        .unblock_link => |unblock| {
            try js.objectField("kind");
            try js.write("unblock_link");
            try js.objectField("from");
            try js.write(unblock.from);
            try js.objectField("to");
            try js.write(unblock.to);
        },
        .clear_blocks => {
            try js.objectField("kind");
            try js.write("clear_blocks");
        },
    }
    try js.endObject();
}

fn writeNodeSnapshotJson(node: NodeSnapshot, js: *std.json.Stringify) !void {
    try js.beginObject();
    try js.objectField("node_id");
    try js.write(node.node_id);
    try js.objectField("role");
    try js.write(@tagName(node.role));
    try js.objectField("leader_id");
    try writeOptionalNodeId(node.leader_id, js);
    try js.objectField("term");
    try js.write(node.term);
    try js.objectField("voted_for");
    try writeOptionalNodeId(node.voted_for, js);
    try js.objectField("commit_index");
    try js.write(node.commit_index);
    try js.endObject();
}

fn writeMessageSummaryJson(msg: MessageSummary, js: *std.json.Stringify) !void {
    try js.beginObject();
    try js.objectField("type");
    try js.write(@tagName(msg.msg_type));
    try js.objectField("from");
    try js.write(msg.from);
    try js.objectField("to");
    try js.write(msg.to);
    try js.objectField("term");
    try js.write(msg.term);
    try js.objectField("log_index");
    try js.write(msg.log_index);
    try js.objectField("log_term");
    try js.write(msg.log_term);
    try js.objectField("commit_index");
    try js.write(msg.commit_index);
    try js.objectField("reject");
    try js.write(msg.reject);
    try js.objectField("reject_hint");
    try js.write(msg.reject_hint);
    try js.objectField("entries_len");
    try js.write(msg.entries_len);
    try js.objectField("first_entry_index");
    try js.write(msg.first_entry_index);
    try js.objectField("last_entry_index");
    try js.write(msg.last_entry_index);
    try js.endObject();
}

fn writeCommittedSummaryJson(commit: CommittedSummary, js: *std.json.Stringify) !void {
    try js.beginObject();
    try js.objectField("node_id");
    try js.write(commit.node_id);
    try js.objectField("count");
    try js.write(commit.count);
    try js.objectField("first_index");
    try js.write(commit.first_index);
    try js.objectField("last_index");
    try js.write(commit.last_index);
    try js.endObject();
}

fn writeReadStateSummaryJson(read_state: ReadStateSummary, js: *std.json.Stringify) !void {
    try js.beginObject();
    try js.objectField("node_id");
    try js.write(read_state.node_id);
    try js.objectField("index");
    try js.write(read_state.index);
    try js.objectField("request_ctx");
    try js.write(read_state.request_ctx);
    try js.endObject();
}

fn writeConfStateSummaryJson(conf_state: ConfStateSummary, js: *std.json.Stringify) !void {
    try js.beginObject();
    try js.objectField("node_id");
    try js.write(conf_state.node_id);
    try js.objectField("voters");
    try js.write(conf_state.voters);
    try js.objectField("voters_outgoing");
    try js.write(conf_state.voters_outgoing);
    try js.objectField("learners");
    try js.write(conf_state.learners);
    try js.objectField("learners_next");
    try js.write(conf_state.learners_next);
    try js.objectField("auto_leave");
    try js.write(conf_state.auto_leave);
    try js.endObject();
}

fn writeOptionalNodeId(value: ?core.types.NodeId, js: *std.json.Stringify) !void {
    if (value) |node_id| {
        try js.write(node_id);
    } else {
        try js.write(@as(?core.types.NodeId, null));
    }
}

fn confStateEql(a: core.types.ConfState, b: core.types.ConfState) bool {
    return std.mem.eql(core.types.NodeId, a.voters, b.voters) and
        std.mem.eql(core.types.NodeId, a.voters_outgoing, b.voters_outgoing) and
        std.mem.eql(core.types.NodeId, a.learners, b.learners) and
        std.mem.eql(core.types.NodeId, a.learners_next, b.learners_next) and
        a.auto_leave == b.auto_leave;
}

fn cloneActionConfChanges(alloc: std.mem.Allocator, changes: []const core.types.ConfChangeSingle) ![]Action.ConfChangeAction {
    const out = try alloc.alloc(Action.ConfChangeAction, changes.len);
    for (changes, 0..) |change, i| {
        out[i] = .{
            .change_type = change.change_type,
            .target_node_id = change.node_id,
        };
    }
    return out;
}
