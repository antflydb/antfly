// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! A replayable, per-group Raft scenario over the real Raft core.
//!
//! The adapter deliberately schedules network delivery, network loss,
//! persistence, state-machine application, node restart, snapshot compaction,
//! elections, and proposals as separate decisions. The underlying test
//! cluster owns the real `RawNode` and `MemoryStorage` instances; this module
//! owns only the deterministic application oracle and VOPR trace vocabulary.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const raft_engine = @import("raft_engine");

const Cluster = raft_engine.testing.Cluster;
const Message = raft_engine.core.Message;
const NodeId = raft_engine.core.types.NodeId;

const peers = [_]NodeId{ 1, 2, 3 };

const finish_id = vopr.id.stable("transition", "raft.group.finish_and_verify");
const tick_base = vopr.id.stable("transition", "raft.group.tick");
const campaign_base = vopr.id.stable("transition", "raft.group.campaign");
const propose_base = vopr.id.stable("transition", "raft.group.propose");
const restart_base = vopr.id.stable("transition", "raft.group.restart");
const partition_start_base = vopr.id.stable("transition", "raft.group.partition.start");
const partition_stop_base = vopr.id.stable("transition", "raft.group.partition.stop");
const deliver_base = vopr.id.stable("transition", "raft.group.message.deliver");
const drop_base = vopr.id.stable("transition", "raft.group.message.drop");
const persist_base = vopr.id.stable("transition", "raft.group.storage.persist");
const apply_completion_base = vopr.id.stable("transition", "raft.group.storage.apply_completion");
const consume_apply_base = vopr.id.stable("transition", "raft.group.application.consume");
const compact_base = vopr.id.stable("transition", "raft.group.snapshot.compact");

const one_leader_per_term_id = vopr.id.stable("property", "raft.group.one_leader_per_term");
const monotonic_progress_id = vopr.id.stable("property", "raft.group.term_commit_apply_monotonic");
const applied_prefix_id = vopr.id.stable("property", "raft.group.applied_logs_share_prefix");
const commit_bounds_id = vopr.id.stable("property", "raft.group.apply_and_commit_within_log");
const leader_reached_id = vopr.id.stable("property", "raft.group.leader_reached");
const proposal_replicated_id = vopr.id.stable("property", "raft.group.proposal_replicated_after_heal");

pub fn Scenario(comptime hostile_budget: u64) type {
    return struct {
        const Self = @This();

        pub const name: []const u8 = "raft-group";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = one_leader_per_term_id, .name = "raft.group.one_leader_per_term", .kind = .always },
            .{ .id = monotonic_progress_id, .name = "raft.group.term_commit_apply_monotonic", .kind = .always },
            .{ .id = applied_prefix_id, .name = "raft.group.applied_logs_share_prefix", .kind = .always },
            .{ .id = commit_bounds_id, .name = "raft.group.apply_and_commit_within_log", .kind = .always },
            .{ .id = leader_reached_id, .name = "raft.group.leader_reached", .kind = .reachable },
            .{ .id = proposal_replicated_id, .name = "raft.group.proposal_replicated_after_heal", .kind = .reachable },
        };

        const Applied = std.ArrayListUnmanaged(u64);

        const State = struct {
            allocator: std.mem.Allocator,
            cluster: Cluster,
            faults: vopr.fault.Controller,
            applied: [peers.len]Applied = .{ .empty, .empty, .empty },
            max_term: [peers.len]u64 = .{ 0, 0, 0 },
            max_commit: [peers.len]u64 = .{ 0, 0, 0 },
            max_applied: [peers.len]u64 = .{ 0, 0, 0 },
            compacted: [peers.len]u64 = .{ 0, 0, 0 },
            actions: u64 = 0,
            next_proposal: u64 = 1,
            monotonic: bool = true,
            finished: bool = false,
            ever_had_leader: bool = false,
            replicated_after_heal: bool = false,

            fn deinit(self: *State) void {
                for (&self.applied) |*items| items.deinit(self.allocator);
                self.faults.deinit();
                self.cluster.deinit();
            }
        };

        pub const World = struct { state: *State };

        pub fn init(allocator: std.mem.Allocator) !World {
            const state = try allocator.create(State);
            errdefer allocator.destroy(state);
            state.* = undefined;
            state.allocator = allocator;
            state.cluster = try Cluster.initWithOptions(allocator, &peers, .{
                .random_seed = 0xA17F_5A17,
                .async_storage_writes = true,
                .defer_storage_writes = true,
                .max_inflight_msgs = 2,
                .max_uncommitted_entries_size = 64,
                .check_quorum = true,
                .pre_vote = true,
            });
            errdefer state.cluster.deinit();
            state.faults = try vopr.fault.Controller.init(allocator, peers.len, .{
                .max_simultaneous_node_failures = 1,
                .max_partitioned_links = 1,
                .max_outstanding_delayed_messages = 64,
                .minimum_healthy_nodes = 2,
            });
            state.applied = .{ .empty, .empty, .empty };
            state.max_term = .{ 0, 0, 0 };
            state.max_commit = .{ 0, 0, 0 };
            state.max_applied = .{ 0, 0, 0 };
            state.compacted = .{ 0, 0, 0 };
            state.actions = 0;
            state.next_proposal = 1;
            state.monotonic = true;
            state.finished = false;
            state.ever_had_leader = false;
            state.replicated_after_heal = false;
            auditProgress(state);
            return .{ .state = state };
        }

        pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
            world.state.deinit();
            allocator.destroy(world.state);
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
            const state = world.state;
            if (state.actions >= hostile_budget) {
                try list.append(allocator, .{ .id = finish_id, .name = "raft.group.finish_and_verify", .kind = .quiescence });
                return;
            }

            for (peers, 0..) |node_id, node_index| {
                if (!state.cluster.isNodeActive(node_id)) continue;
                try list.append(allocator, nodeTransition(tick_base, "raft.group.tick", .scheduler, node_id));
                try list.append(allocator, nodeTransition(campaign_base, "raft.group.campaign", .workload, node_id));
                try list.append(allocator, nodeTransition(restart_base, "raft.group.restart", .fault, node_id));
                const status = state.cluster.node(node_id).status();
                if (status.soft.role == .leader) {
                    try list.append(allocator, nodeTransition(propose_base, "raft.group.propose", .workload, node_id));
                }
                if (status.applied_index > state.compacted[node_index]) {
                    try list.append(allocator, nodeTransition(compact_base, "raft.group.snapshot.compact", .maintenance, node_id));
                }
                if (state.cluster.queuedCommittedSlice(node_id).len > 0) {
                    try list.append(allocator, nodeTransition(consume_apply_base, "raft.group.application.consume", .scheduler, node_id));
                }

                const spec = partitionSpec(node_id);
                if (state.faults.isActive(spec.id)) {
                    try list.append(allocator, spec.stopTransition());
                } else if ((try state.faults.admission(spec)).isAllowed()) {
                    try list.append(allocator, spec.startTransition());
                }
            }

            try enumerateMessages(state.cluster.pendingMessageSlice(), list, allocator, false);
            try enumerateMessages(state.cluster.pendingStorageSlice(), list, allocator, true);
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: std.mem.Allocator,
        ) !vopr.outcome.TransitionOutcome {
            const state = world.state;
            if (selected.id == finish_id) {
                try finishAndVerify(state, events, allocator);
                auditProgress(state);
                return vopr.outcome.TransitionOutcome.targetReached("raft.group.quiescent", state.next_proposal - 1);
            }

            state.actions += 1;
            if (nodeForTransition(selected, tick_base)) |node_id| {
                try state.cluster.tick(node_id, 1);
                try events.emitNamed(allocator, .state_change, "raft.group.node_ticked", node_id);
            } else if (nodeForTransition(selected, campaign_base)) |node_id| {
                try state.cluster.campaign(node_id);
                try events.emitNamed(allocator, .state_change, "raft.group.campaign_started", node_id);
            } else if (nodeForTransition(selected, propose_base)) |node_id| {
                var payload: [8]u8 = undefined;
                std.mem.writeInt(u64, &payload, state.next_proposal, .little);
                try state.cluster.propose(node_id, &payload);
                try events.emitNamed(allocator, .client_response, "raft.group.proposal_accepted", state.next_proposal);
                state.next_proposal += 1;
            } else if (nodeForTransition(selected, restart_base)) |node_id| {
                const spec = restartSpec(node_id);
                try state.faults.start(spec, events, allocator);
                try state.cluster.restart(node_id);
                _ = try state.faults.consumeOneShot(.node, spec.resource_id, events, allocator);
                try events.emitNamed(allocator, .state_change, "raft.group.node_restarted", node_id);
            } else if (nodeForTransition(selected, compact_base)) |node_id| {
                const index = state.cluster.node(node_id).status().applied_index;
                try state.cluster.compact(node_id, index);
                state.compacted[nodeOffset(node_id)] = index;
                try events.emitNamed(allocator, .state_change, "raft.group.snapshot_compacted", index);
            } else if (nodeForTransition(selected, consume_apply_base)) |node_id| {
                try consumeCommitted(state, node_id, events, allocator);
            } else if (partitionNodeForTransition(state, selected, true)) |node_id| {
                const spec = partitionSpec(node_id);
                try state.faults.start(spec, events, allocator);
                for (peers) |peer_id| if (peer_id != node_id) {
                    try state.cluster.block(node_id, peer_id);
                    try state.cluster.block(peer_id, node_id);
                };
            } else if (partitionNodeForTransition(state, selected, false)) |node_id| {
                const spec = partitionSpec(node_id);
                for (peers) |peer_id| if (peer_id != node_id) {
                    state.cluster.unblock(node_id, peer_id);
                    state.cluster.unblock(peer_id, node_id);
                };
                try state.faults.stop(spec.id, events, allocator);
            } else if (std.mem.eql(u8, selected.name, "raft.group.message.deliver")) {
                const index: usize = @intCast(selected.parameter);
                const msg = state.cluster.pendingMessageSlice()[index];
                const digest = messageDigest(msg);
                try state.cluster.deliverAt(index);
                try events.emitNamed(allocator, .message_enqueued, "raft.group.message_delivered", digest);
            } else if (std.mem.eql(u8, selected.name, "raft.group.message.drop")) {
                const index: usize = @intCast(selected.parameter);
                const msg = state.cluster.pendingMessageSlice()[index];
                const spec = messageDropSpec(msg, selected.id);
                try state.faults.start(spec, events, allocator);
                try state.cluster.dropMessageAt(index);
                _ = try state.faults.consumeOneShot(.delayed_message, spec.resource_id, events, allocator);
                try events.emitNamed(allocator, .injected_error, "raft.group.message_dropped", messageDigest(msg));
            } else if (std.mem.eql(u8, selected.name, "raft.group.storage.persist") or
                std.mem.eql(u8, selected.name, "raft.group.storage.apply_completion"))
            {
                const index: usize = @intCast(selected.parameter);
                const msg = state.cluster.pendingStorageSlice()[index];
                const digest = messageDigest(msg);
                try state.cluster.completeStorageAt(index);
                try events.emitNamed(allocator, .state_change, if (msg.msg_type == .storage_append)
                    "raft.group.persistence_completed"
                else
                    "raft.group.apply_completed", digest);
            } else {
                return error.UnknownRaftVoprTransition;
            }

            auditProgress(state);
            return vopr.outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            const state = world.state;
            try builder.addNamed(allocator, "raft.group.actions", @intCast(state.actions));
            try builder.addNamed(allocator, "raft.group.pending_messages", @intCast(state.cluster.pendingMessages()));
            try builder.addNamed(allocator, "raft.group.pending_storage", @intCast(state.cluster.pendingStorageCompletions()));
            try builder.addNamed(allocator, "raft.group.active_faults", @intCast(state.faults.activeTotal()));
            try builder.addNamed(allocator, "raft.group.next_proposal", @intCast(state.next_proposal));
            try builder.addNamed(allocator, "raft.group.finished", @intFromBool(state.finished));
            for (peers, 0..) |node_id, index| {
                const status = state.cluster.node(node_id).status();
                try addNodeObservation(builder, allocator, "raft.group.node.term", node_id, @intCast(status.hard.current_term));
                try addNodeObservation(builder, allocator, "raft.group.node.role", node_id, @intFromEnum(status.soft.role));
                try addNodeObservation(builder, allocator, "raft.group.node.leader", node_id, @intCast(status.soft.leader_id orelse 0));
                try addNodeObservation(builder, allocator, "raft.group.node.commit", node_id, @intCast(status.hard.commit_index));
                try addNodeObservation(builder, allocator, "raft.group.node.applied", node_id, @intCast(status.applied_index));
                try addNodeObservation(builder, allocator, "raft.group.node.last_index", node_id, @intCast(status.last_index));
                try addNodeObservation(builder, allocator, "raft.group.node.app_count", node_id, @intCast(state.applied[index].items.len));
                try addNodeObservation(builder, allocator, "raft.group.node.app_digest", node_id, @bitCast(appliedDigest(state.applied[index].items)));
            }
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            const state = world.state;
            try sink.check(allocator, one_leader_per_term_id, oneLeaderPerTerm(state));
            try sink.check(allocator, monotonic_progress_id, state.monotonic);
            try sink.check(allocator, applied_prefix_id, appliedLogsSharePrefix(state));
            try sink.check(allocator, commit_bounds_id, progressWithinBounds(state));
            try sink.check(allocator, leader_reached_id, state.ever_had_leader);
            try sink.check(allocator, proposal_replicated_id, state.replicated_after_heal);
        }

        pub fn done(world: *World) bool {
            return world.state.finished;
        }
    };
}

pub const CliScenario = Scenario(32);

pub fn record(allocator: std.mem.Allocator, seed: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    return vopr.runner.run(CliScenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = 33,
        .source_revision = "raft-vopr-cli",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
}

pub fn replay(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(CliScenario, allocator, artifact);
}

fn nodeTransition(base: u64, name: []const u8, kind: vopr.transition.Kind, node_id: NodeId) vopr.transition.Transition {
    return .{
        .id = vopr.id.derive("raft.group.node-transition", base, node_id),
        .name = name,
        .kind = kind,
        .resource_id = nodeResource(node_id),
        .parameter = @intCast(node_id),
    };
}

fn nodeForTransition(selected: vopr.transition.Transition, base: u64) ?NodeId {
    const node_id: NodeId = @intCast(selected.parameter);
    if (node_id < peers[0] or node_id > peers[peers.len - 1]) return null;
    return if (selected.id == vopr.id.derive("raft.group.node-transition", base, node_id)) node_id else null;
}

fn nodeResource(node_id: NodeId) u64 {
    return vopr.id.derive("raft.group.node", vopr.id.stable("resource", "raft.group.node"), node_id);
}

fn partitionSpec(node_id: NodeId) vopr.fault.Spec {
    return .{
        .id = vopr.id.derive("raft.group.partition-fault", vopr.id.stable("fault", "raft.group.partition"), node_id),
        .name = "raft.group.partition",
        .kind = .partitioned_link,
        .lifecycle = .persistent,
        .resource_id = nodeResource(node_id),
    };
}

fn restartSpec(node_id: NodeId) vopr.fault.Spec {
    return .{
        .id = vopr.id.derive("raft.group.restart-fault", restart_base, node_id),
        .name = "raft.group.restart",
        .kind = .node,
        .lifecycle = .one_shot,
        .resource_id = nodeResource(node_id),
    };
}

fn partitionNodeForTransition(state: anytype, selected: vopr.transition.Transition, start: bool) ?NodeId {
    _ = partition_start_base;
    _ = partition_stop_base;
    for (peers) |node_id| {
        const spec = partitionSpec(node_id);
        const expected = if (start) spec.startTransition().id else spec.stopTransition().id;
        if (selected.id == expected and state.faults.isActive(spec.id) != start) return node_id;
    }
    return null;
}

fn enumerateMessages(messages: []const Message, list: *vopr.transition.List, allocator: std.mem.Allocator, storage: bool) !void {
    for (messages, 0..) |msg, index| {
        const occurrence = identicalOccurrence(messages[0..index], msg);
        const identity = vopr.id.derive("raft.group.queued-message-occurrence", messageDigest(msg), occurrence);
        if (storage) {
            const is_append = msg.msg_type == .storage_append;
            try list.append(allocator, .{
                .id = vopr.id.derive("raft.group.storage-completion", if (is_append) persist_base else apply_completion_base, identity),
                .name = if (is_append) "raft.group.storage.persist" else "raft.group.storage.apply_completion",
                .kind = .scheduler,
                .resource_id = identity,
                .parameter = @intCast(index),
            });
        } else {
            try list.append(allocator, .{
                .id = vopr.id.derive("raft.group.message-delivery", deliver_base, identity),
                .name = "raft.group.message.deliver",
                .kind = .scheduler,
                .resource_id = identity,
                .parameter = @intCast(index),
            });
            try list.append(allocator, .{
                .id = vopr.id.derive("raft.group.message-drop", drop_base, identity),
                .name = "raft.group.message.drop",
                .kind = .fault,
                .resource_id = identity,
                .parameter = @intCast(index),
            });
        }
    }
}

fn identicalOccurrence(prior: []const Message, target: Message) u64 {
    var count: u64 = 0;
    for (prior) |msg| if (messageDigest(msg) == messageDigest(target)) {
        count += 1;
    };
    return count;
}

fn messageDigest(msg: Message) u64 {
    var digest = vopr.id.derive("raft.message.endpoints", msg.from, msg.to);
    digest = vopr.id.derive("raft.message.type", digest, @intFromEnum(msg.msg_type));
    digest = vopr.id.derive("raft.message.term", digest, msg.term);
    digest = vopr.id.derive("raft.message.index", digest, msg.log_index);
    digest = vopr.id.derive("raft.message.commit", digest, msg.commit_index);
    for (msg.entries) |entry| {
        digest = vopr.id.derive("raft.message.entry", digest, entry.index);
        digest = vopr.id.derive("raft.message.entry-data", digest, vopr.id.digest(entry.data));
    }
    return digest;
}

fn messageDropSpec(msg: Message, transition_id: u64) vopr.fault.Spec {
    return .{
        .id = vopr.id.derive("raft.group.message-drop-fault", transition_id, messageDigest(msg)),
        .name = "raft.group.message.drop",
        .kind = .delayed_message,
        .lifecycle = .one_shot,
        .resource_id = messageDigest(msg),
    };
}

fn consumeCommitted(state: anytype, node_id: NodeId, events: *vopr.event.Sink, allocator: std.mem.Allocator) !void {
    const entries = try state.cluster.collectCommitted(node_id);
    defer raft_engine.core.types.freeEntries(allocator, entries);
    const app = &state.applied[nodeOffset(node_id)];
    for (entries) |entry| {
        if (entry.data.len != @sizeOf(u64) or entry.entry_type != .normal) continue;
        const value = std.mem.readInt(u64, entry.data[0..8], .little);
        try app.append(allocator, value);
        try events.emitNamed(allocator, .state_change, "raft.group.application_applied", value);
    }
}

fn finishAndVerify(state: anytype, events: *vopr.event.Sink, allocator: std.mem.Allocator) !void {
    state.faults.beginQuietSuffix();
    for (peers) |node_id| {
        const spec = partitionSpec(node_id);
        if (!state.faults.isActive(spec.id)) continue;
        try state.faults.stop(spec.id, events, allocator);
    }
    state.cluster.clearBlocks();

    var rounds: usize = 0;
    while (rounds < 512) : (rounds += 1) {
        if (state.cluster.pendingStorageCompletions() > 0) {
            try state.cluster.completeStorageAt(0);
            continue;
        }
        if (state.cluster.pendingMessages() > 0) {
            try state.cluster.deliverAt(0);
            continue;
        }
        var consumed = false;
        for (peers) |node_id| {
            if (state.cluster.queuedCommittedSlice(node_id).len == 0) continue;
            try consumeCommitted(state, node_id, events, allocator);
            consumed = true;
        }
        if (consumed) continue;
        if (!hasLeader(state)) {
            for (peers) |node_id| try state.cluster.tick(node_id, 1);
            continue;
        }
        break;
    }
    if (rounds == 512) return error.RaftVoprQuietSuffixDidNotConverge;

    // Ensure the quiet suffix contains at least one acknowledged proposal and
    // drive it through persistence, replication, and application everywhere.
    if (state.next_proposal == 1) {
        const leader = currentLeader(state) orelse return error.RaftVoprLeaderMissingAfterHeal;
        var payload: [8]u8 = undefined;
        std.mem.writeInt(u64, &payload, state.next_proposal, .little);
        try state.cluster.propose(leader, &payload);
        state.next_proposal += 1;
    }

    rounds = 0;
    while (rounds < 512) : (rounds += 1) {
        if (state.cluster.pendingStorageCompletions() > 0) {
            try state.cluster.completeStorageAt(0);
            continue;
        }
        if (state.cluster.pendingMessages() > 0) {
            try state.cluster.deliverAt(0);
            continue;
        }
        var consumed = false;
        for (peers) |node_id| {
            if (state.cluster.queuedCommittedSlice(node_id).len == 0) continue;
            try consumeCommitted(state, node_id, events, allocator);
            consumed = true;
        }
        if (!consumed) break;
    }
    if (rounds == 512) return error.RaftVoprProposalDidNotConverge;
    state.replicated_after_heal = allApplicationsEqualAndNonEmpty(state);
    state.finished = true;
    try events.emitNamed(allocator, .domain, "raft.group.quiet_suffix_complete", state.next_proposal - 1);
}

fn auditProgress(state: anytype) void {
    for (peers, 0..) |node_id, index| {
        const status = state.cluster.node(node_id).status();
        if (status.hard.current_term < state.max_term[index] or
            status.hard.commit_index < state.max_commit[index] or
            status.applied_index < state.max_applied[index]) state.monotonic = false;
        state.max_term[index] = @max(state.max_term[index], status.hard.current_term);
        state.max_commit[index] = @max(state.max_commit[index], status.hard.commit_index);
        state.max_applied[index] = @max(state.max_applied[index], status.applied_index);
        state.ever_had_leader = state.ever_had_leader or status.soft.role == .leader;
    }
}

fn hasLeader(state: anytype) bool {
    return currentLeader(state) != null;
}

fn currentLeader(state: anytype) ?NodeId {
    for (peers) |node_id| if (state.cluster.node(node_id).status().soft.role == .leader) return node_id;
    return null;
}

fn oneLeaderPerTerm(state: anytype) bool {
    for (peers, 0..) |lhs_id, lhs_index| {
        const lhs = state.cluster.node(lhs_id).status();
        if (lhs.soft.role != .leader) continue;
        for (peers[lhs_index + 1 ..]) |rhs_id| {
            const rhs = state.cluster.node(rhs_id).status();
            if (rhs.soft.role == .leader and rhs.hard.current_term == lhs.hard.current_term) return false;
        }
    }
    return true;
}

fn progressWithinBounds(state: anytype) bool {
    for (peers) |node_id| {
        const status = state.cluster.node(node_id).status();
        if (status.applied_index > status.hard.commit_index or status.hard.commit_index > status.last_index) return false;
    }
    return true;
}

fn appliedLogsSharePrefix(state: anytype) bool {
    for (state.applied, 0..) |lhs, lhs_index| {
        for (state.applied[lhs_index + 1 ..]) |rhs| {
            const shared = @min(lhs.items.len, rhs.items.len);
            if (!std.mem.eql(u64, lhs.items[0..shared], rhs.items[0..shared])) return false;
        }
    }
    return true;
}

fn allApplicationsEqualAndNonEmpty(state: anytype) bool {
    if (state.applied[0].items.len == 0) return false;
    for (state.applied[1..]) |items| if (!std.mem.eql(u64, state.applied[0].items, items.items)) return false;
    return true;
}

fn appliedDigest(values: []const u64) u64 {
    var digest = vopr.id.stable("raft.application", "empty");
    for (values) |value| digest = vopr.id.derive("raft.application.value", digest, value);
    return digest;
}

fn addNodeObservation(
    builder: *vopr.observation.Builder,
    allocator: std.mem.Allocator,
    name: []const u8,
    node_id: NodeId,
    value: i64,
) !void {
    try builder.add(allocator, .{
        .id = vopr.id.derive("raft.group.observation", vopr.id.stable("observation", name), node_id),
        .name = name,
        .value = value,
    });
}

fn nodeOffset(node_id: NodeId) usize {
    return @intCast(node_id - 1);
}

fn runRecordReplay(comptime budget: u64, seed: u64) !void {
    const RaftScenario = Scenario(budget);
    var seeded = vopr.choice.Seeded.init(seed);
    var recorded = try vopr.runner.run(RaftScenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = budget + 1,
        .source_revision = "raft-vopr-test",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
    defer recorded.deinit();
    try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
    try std.testing.expectEqual(budget + 1, recorded.summary.?.transitions);
    try std.testing.expect(recorded.events.items.len > 0);

    const encoded = try recorded.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    var parsed = try vopr.trace.parseAlloc(std.testing.allocator, encoded);
    defer parsed.deinit();
    var replayed = try vopr.replay.exact(RaftScenario, std.testing.allocator, &parsed);
    replayed.deinit();
}

test "raft VOPR schedules real per-group network storage apply restart and snapshot transitions" {
    try runRecordReplay(32, 0xA17F_AA01);
    try runRecordReplay(32, 0xA17F_AA02);
}

test "raft VOPR exact replay is stable across repeated fresh worlds" {
    const RaftScenario = Scenario(24);
    var seeded = vopr.choice.Seeded.init(0xA17F_AA03);
    var recorded = try vopr.runner.run(RaftScenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = 0xA17F_AA03,
        .transition_budget = 25,
    });
    defer recorded.deinit();
    for (0..10) |_| {
        var replayed = try vopr.replay.exact(RaftScenario, std.testing.allocator, &recorded);
        replayed.deinit();
    }
}
