// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Cross-domain Antfly protocol scenarios for deterministic VOPR campaigns.
//!
//! These models define the scheduler vocabulary and executable oracles used by
//! the production adapters. They deliberately share the normal VOPR runner,
//! trace, replay, reduction, and fixture contracts.

const std = @import("std");
const vopr = @import("vopr");
const backend_erased = @import("../storage/backend_erased.zig");
const mem_backend = @import("../storage/mem_backend.zig");
const transactions = @import("../storage/transactions.zig");
const tracing = @import("../tracing/antfly_trace_writer.zig");
const background_runtime = @import("../storage/background_runtime.zig");
const durable_job_lane = @import("../storage/vopr_durable_job_lane.zig");
const backup_manifest = @import("../storage/ha/backup_manifest.zig");

const Allocator = std.mem.Allocator;

fn transitionId(scenario_name: []const u8, action: []const u8) u64 {
    return vopr.id.derive("transition", vopr.id.stable("scenario", scenario_name), vopr.id.digest(action));
}

fn propertyId(comptime scenario_name: []const u8, comptime property_name: []const u8) u64 {
    return vopr.id.stable("property", scenario_name ++ "." ++ property_name);
}

pub const DistributedTransactionScenario = struct {
    pub const name: []const u8 = "distributed-transaction-lifecycle";
    pub const version: u32 = 2;
    const consistent_id = propertyId(name, "globally_consistent_decision");
    const fenced_id = propertyId(name, "stale_owner_fenced");
    const resolved_id = propertyId(name, "durable_intents_resolve");
    const differential_id = propertyId(name, "client_history_matches_production_state");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = consistent_id, .name = name ++ ".globally_consistent_decision", .kind = .always },
        .{ .id = fenced_id, .name = name ++ ".stale_owner_fenced", .kind = .always },
        .{ .id = resolved_id, .name = name ++ ".durable_intents_resolve", .kind = .reachable },
        .{ .id = differential_id, .name = name ++ ".client_history_matches_production_state", .kind = .always },
    };
    const begin_id = transitionId(name, "begin");
    const intent_id = transitionId(name, "write_intents");
    const prepare_a_id = transitionId(name, "prepare_a");
    const prepare_b_id = transitionId(name, "prepare_b");
    const commit_id = transitionId(name, "durable_commit");
    const abort_id = transitionId(name, "durable_abort");
    const ambiguous_id = transitionId(name, "ambiguous_response");
    const crash_id = transitionId(name, "coordinator_crash_restart");
    const lease_id = transitionId(name, "lease_expire_adopt");
    const deliver_a_id = transitionId(name, "phase_two_a");
    const deliver_b_id = transitionId(name, "phase_two_b");
    const repair_id = transitionId(name, "recovery_repair");
    const stale_owner_id = transitionId(name, "stale_owner_attempt");
    const retry_id = transitionId(name, "idempotent_phase_two_retry");

    const txn_id: transactions.TxnId = .{ 0xa1, 0x7f, 0x50, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const participant_names = [_][]const u8{ "table-a", "table-b" };

    pub const TraceContext = struct { writer: *std.Io.Writer };

    const Node = struct {
        allocator: Allocator,
        backend: mem_backend.Backend,
        store: backend_erased.Store,
        manager: transactions.TxnManager,

        fn init(allocator: Allocator, shard_id: []const u8) !*Node {
            const self = try allocator.create(Node);
            errdefer allocator.destroy(self);
            self.allocator = allocator;
            self.backend = mem_backend.Backend.init(allocator, .{});
            errdefer self.backend.close();
            self.store = try self.backend.runtimeStore(allocator, .{ .name = shard_id });
            errdefer self.store.deinit();
            self.manager = try transactions.TxnManager.init(allocator, &self.store);
            self.manager.trace_writer = null;
            self.manager.shard_id = shard_id;
            return self;
        }

        fn restart(self: *Node, allocator: Allocator, shard_id: []const u8) !void {
            var next = try transactions.TxnManager.init(allocator, &self.store);
            next.trace_writer = null;
            next.shard_id = shard_id;
            self.manager.deinit();
            self.manager = next;
        }

        fn deinit(self: *Node) void {
            self.manager.deinit();
            self.store.deinit();
            self.backend.close();
            self.allocator.destroy(self);
        }

        fn valueEquals(self: *Node, key: []const u8, expected: []const u8) !bool {
            var read = try self.store.beginRead();
            defer read.abort();
            const value = read.get(key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            return std.mem.eql(u8, value, expected);
        }
    };

    const Decision = enum { none, commit, abort };
    const State = struct {
        allocator: Allocator,
        nodes: [3]*Node,
        phase: enum { begin, intents, prepare, decide, deliver, terminal } = .begin,
        prepared: u2 = 0,
        delivered: u2 = 0,
        decision: Decision = .none,
        participant_decisions: [2]Decision = .{ .none, .none },
        lease_epoch: u32 = 1,
        stale_attempt_succeeded: bool = false,
        stale_attempted: bool = false,
        ambiguous: bool = false,
        crashed_once: bool = false,
        decision_crashed: bool = false,
        retry_attempted: bool = false,
        trace_sink: ?tracing.AntflyNdjsonTraceWriter = null,

        fn configureTracing(self: *State) void {
            const writer = if (self.trace_sink) |*sink| sink.traceWriter() else null;
            for (self.nodes) |node| node.manager.trace_writer = writer;
        }

        fn deinit(self: *State) void {
            for (self.nodes) |node| node.deinit();
        }
    };
    pub const World = struct { state: *State };

    pub fn init(allocator: Allocator) !World {
        return initWithContext(allocator, null);
    }

    pub fn initWithContext(allocator: Allocator, opaque_context: ?*anyopaque) !World {
        const coordinator = try Node.init(allocator, "coordinator");
        errdefer coordinator.deinit();
        const participant_a = try Node.init(allocator, participant_names[0]);
        errdefer participant_a.deinit();
        const participant_b = try Node.init(allocator, participant_names[1]);
        errdefer participant_b.deinit();
        const state = try allocator.create(State);
        state.* = .{
            .allocator = allocator,
            .nodes = .{ coordinator, participant_a, participant_b },
        };
        if (opaque_context) |context_ptr| {
            const context: *TraceContext = @ptrCast(@alignCast(context_ptr));
            state.trace_sink = .{ .writer = context.writer };
            state.configureTracing();
        }
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: Allocator) void {
        world.state.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        const state = world.state;
        switch (state.phase) {
            .begin => try add(list, allocator, begin_id, name ++ ".begin", .workload),
            .intents => try add(list, allocator, intent_id, name ++ ".write_intents", .workload),
            .prepare => {
                if (state.prepared & 1 == 0) try add(list, allocator, prepare_a_id, name ++ ".prepare_a", .scheduler);
                if (state.prepared & 2 == 0) try add(list, allocator, prepare_b_id, name ++ ".prepare_b", .scheduler);
                if (state.prepared != 3 and !state.crashed_once) try add(list, allocator, crash_id, name ++ ".coordinator_crash_restart", .fault);
            },
            .decide => {
                try add(list, allocator, commit_id, name ++ ".durable_commit", .workload);
                try add(list, allocator, abort_id, name ++ ".durable_abort", .workload);
            },
            .deliver => {
                if (!state.ambiguous) try add(list, allocator, ambiguous_id, name ++ ".ambiguous_response", .fault);
                if (state.lease_epoch == 1) try add(list, allocator, lease_id, name ++ ".lease_expire_adopt", .maintenance);
                if (state.lease_epoch > 1 and !state.stale_attempted) try add(list, allocator, stale_owner_id, name ++ ".stale_owner_attempt", .fault);
                if (!state.decision_crashed) try add(list, allocator, crash_id, name ++ ".decision_crash_restart", .fault);
                if (state.delivered & 1 == 0) try add(list, allocator, deliver_a_id, name ++ ".phase_two_a", .scheduler);
                if (state.delivered & 2 == 0) try add(list, allocator, deliver_b_id, name ++ ".phase_two_b", .scheduler);
                if (state.delivered != 0 and !state.retry_attempted) try add(list, allocator, retry_id, name ++ ".idempotent_phase_two_retry", .scheduler);
                if (state.delivered != 3) try add(list, allocator, repair_id, name ++ ".recovery_repair", .maintenance);
            },
            .terminal => {},
        }
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (selected.id == begin_id) {
            try state.nodes[0].manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(txn_id, 10_000, 10_000, &participant_names, true, true);
            try state.nodes[1].manager.initTransactionWithParticipantsCreatedAtAndRole(txn_id, 10_000, 10_000, &.{}, false);
            try state.nodes[2].manager.initTransactionWithParticipantsCreatedAtAndRole(txn_id, 10_000, 10_000, &.{}, false);
            state.phase = .intents;
        } else if (selected.id == intent_id) {
            state.phase = .prepare;
        } else if (selected.id == prepare_a_id) {
            try state.nodes[1].manager.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "value-a" }}, &.{});
            state.prepared |= 1;
        } else if (selected.id == prepare_b_id) {
            try state.nodes[2].manager.writeIntents(txn_id, &.{.{ .key = "doc:b", .value = "value-b" }}, &.{});
            state.prepared |= 2;
        } else if (selected.id == crash_id) {
            try state.nodes[0].restart(state.allocator, "coordinator");
            if (state.phase == .prepare) {
                const participant_index: usize = if (state.prepared & 1 != 0) 1 else 2;
                try state.nodes[participant_index].restart(state.allocator, participant_names[participant_index - 1]);
                state.crashed_once = true;
            } else {
                try state.nodes[1].restart(state.allocator, participant_names[0]);
                try state.nodes[2].restart(state.allocator, participant_names[1]);
                state.decision_crashed = true;
            }
            state.configureTracing();
        } else if (selected.id == commit_id or selected.id == abort_id) {
            state.decision = if (selected.id == commit_id) .commit else .abort;
            try state.nodes[0].manager.resolveIntents(txn_id, decisionStatus(state.decision), 10_100);
            state.phase = .deliver;
        } else if (selected.id == ambiguous_id) state.ambiguous = true else if (selected.id == lease_id) state.lease_epoch += 1 else if (selected.id == stale_owner_id) {
            state.stale_attempted = true;
            const conflicting: transactions.TxnStatus = if (state.decision == .commit) .aborted else .committed;
            state.nodes[0].manager.resolveIntents(txn_id, conflicting, 10_101) catch |err| switch (err) {
                error.DecisionConflict => {},
                else => return err,
            };
            if (try state.nodes[0].manager.getTransactionStatus(txn_id) == conflicting) state.stale_attempt_succeeded = true;
        } else if (selected.id == deliver_a_id) {
            try resolveParticipant(state, 0);
        } else if (selected.id == deliver_b_id) {
            try resolveParticipant(state, 1);
        } else if (selected.id == retry_id) {
            const participant: usize = if (state.delivered & 1 != 0) 0 else 1;
            try state.nodes[participant + 1].manager.resolveIntents(txn_id, decisionStatus(state.decision), 10_100);
            try state.nodes[0].manager.markParticipantResolved(txn_id, participant_names[participant]);
            state.retry_attempted = true;
        } else if (selected.id == repair_id) {
            try resolveParticipant(state, 0);
            try resolveParticipant(state, 1);
        } else return error.InvalidDistributedTransactionTransition;
        if (state.phase == .prepare and state.prepared == 3) state.phase = .decide;
        if (state.phase == .deliver and state.delivered == 3) state.phase = .terminal;
        try events.emitNamed(allocator, .domain, selected.name, state.lease_epoch);
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".decision", @intFromEnum(state.decision));
        try builder.addNamed(allocator, name ++ ".prepared", state.prepared);
        try builder.addNamed(allocator, name ++ ".delivered", state.delivered);
        try builder.addNamed(allocator, name ++ ".lease_epoch", state.lease_epoch);
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const state = world.state;
        const consistent = for (state.participant_decisions) |decision| {
            if (decision != .none and decision != state.decision) break false;
        } else true;
        const visible = try state.nodes[1].valueEquals("doc:a", "value-a") or try state.nodes[2].valueEquals("doc:b", "value-b");
        const visibility_valid = state.decision != .abort or !visible;
        var differential = true;
        if (state.phase != .begin) {
            const coordinator_expected: transactions.TxnStatus = if (state.decision == .none) .pending else decisionStatus(state.decision);
            differential = try state.nodes[0].manager.getTransactionStatus(txn_id) == coordinator_expected;
            for (0..2) |participant| {
                const expected: transactions.TxnStatus = if (state.participant_decisions[participant] == .none)
                    .pending
                else
                    decisionStatus(state.participant_decisions[participant]);
                differential = differential and try state.nodes[participant + 1].manager.getTransactionStatus(txn_id) == expected;
            }
        }
        try sink.check(allocator, consistent_id, consistent and visibility_valid);
        try sink.check(allocator, fenced_id, !state.stale_attempt_succeeded);
        try sink.check(allocator, resolved_id, state.phase == .terminal and !(try state.nodes[0].manager.hasTopologySensitiveTransactions()));
        try sink.check(allocator, differential_id, differential);
    }
    pub fn done(world: *World) bool {
        return world.state.phase == .terminal;
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        try sink.add("transaction", if (world.state.phase == .terminal) "resolved" else "pending");
    }

    fn decisionStatus(decision: Decision) transactions.TxnStatus {
        return switch (decision) {
            .commit => .committed,
            .abort => .aborted,
            .none => unreachable,
        };
    }

    fn resolveParticipant(state: *State, participant: usize) !void {
        const bit: u2 = @as(u2, 1) << @intCast(participant);
        if (state.delivered & bit != 0) return;
        try state.nodes[participant + 1].manager.resolveIntents(txn_id, decisionStatus(state.decision), 10_100);
        try state.nodes[0].manager.markParticipantResolved(txn_id, participant_names[participant]);
        state.delivered |= bit;
        state.participant_decisions[participant] = state.decision;
    }
};

pub const DataPlaneScenario = struct {
    pub const name: []const u8 = "dataserver-public-microsteps";
    pub const version: u32 = 2;
    const safety_id = propertyId(name, "routing_ownership_and_acknowledged_visibility");
    const topology_id = propertyId(name, "topology_has_no_gap_or_overlap");
    const agreement_id = propertyId(name, "same_applied_index_agrees");
    const visible_id = propertyId(name, "acknowledged_write_is_query_visible");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = safety_id, .name = name ++ ".routing_ownership_and_acknowledged_visibility", .kind = .always },
        .{ .id = topology_id, .name = name ++ ".topology_has_no_gap_or_overlap", .kind = .always },
        .{ .id = agreement_id, .name = name ++ ".same_applied_index_agrees", .kind = .always },
        .{ .id = visible_id, .name = name ++ ".acknowledged_write_is_query_visible", .kind = .reachable },
    };
    const admit_id = transitionId(name, "admit");
    const route_id = transitionId(name, "route");
    const drop_id = transitionId(name, "drop_next_packet");
    const duplicate_id = transitionId(name, "duplicate_next_packet");
    const persist_id = transitionId(name, "raft_persist");
    const apply_id = transitionId(name, "raft_apply");
    const acknowledge_id = transitionId(name, "acknowledge");
    const split_copy_id = transitionId(name, "split_copy");
    const handoff_id = transitionId(name, "writer_lease_handoff");
    const split_cutover_id = transitionId(name, "split_cutover");
    const read_id = transitionId(name, "point_and_query_read");

    const Stage = enum { admit, route, packet, persist, apply, acknowledge, split_copy, split_cutover, read, terminal };
    const State = struct {
        sim: vopr.vopr_io.VoprIo,
        pair: [2]std.Io.net.Socket,
        raft_log: std.Io.File,
        stage: Stage = .admit,
        drop_injected: bool = false,
        duplicate_injected: bool = false,
        request_admitted: bool = false,
        applied: bool = false,
        acknowledged: bool = false,
        query_visible: bool = false,
        owner_epoch: u64 = 1,
        retired_owner_published: bool = false,
        split_copied: bool = false,
        cutover: bool = false,
        replica_indexes: [2]u64 = .{ 0, 0 },
        replica_digests: [2]u64 = .{ 0, 0 },
    };
    pub const World = struct { state: *State };

    pub fn init(allocator: Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.sim = try .init(.{ .required = .of(&.{ .files, .sockets, .task_scheduling, .resources }) });
        errdefer state.sim.deinit();
        const io = state.sim.io();
        state.pair = try std.Io.net.Socket.createPair(io, .{});
        errdefer {
            state.pair[0].close(io);
            state.pair[1].close(io);
        }
        _ = try std.Io.Dir.cwd().createDirPath(io, "data");
        state.raft_log = try std.Io.Dir.cwd().createFile(io, "data/raft.log", .{});
        state.stage = .admit;
        state.drop_injected = false;
        state.duplicate_injected = false;
        state.request_admitted = false;
        state.applied = false;
        state.acknowledged = false;
        state.query_visible = false;
        state.owner_epoch = 1;
        state.retired_owner_published = false;
        state.split_copied = false;
        state.cutover = false;
        state.replica_indexes = .{ 0, 0 };
        state.replica_digests = .{ 0, 0 };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: Allocator) void {
        const state = world.state;
        const io = state.sim.io();
        state.raft_log.close(io);
        state.pair[0].close(io);
        state.pair[1].close(io);
        state.sim.deinit();
        allocator.destroy(state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        const state = world.state;
        if (state.stage == .packet) {
            try state.sim.scheduler().enumerateReady(list, allocator);
            return;
        }
        switch (state.stage) {
            .admit => try add(list, allocator, admit_id, name ++ ".admit", .workload),
            .route => {
                try add(list, allocator, route_id, name ++ ".route", .workload);
                if (!state.drop_injected) try add(list, allocator, drop_id, name ++ ".drop_next_packet", .fault);
                if (!state.duplicate_injected) try add(list, allocator, duplicate_id, name ++ ".duplicate_next_packet", .fault);
            },
            .persist => try add(list, allocator, persist_id, name ++ ".raft_persist", .scheduler),
            .apply => try add(list, allocator, apply_id, name ++ ".raft_apply", .scheduler),
            .acknowledge => try add(list, allocator, acknowledge_id, name ++ ".acknowledge", .workload),
            .split_copy => {
                try add(list, allocator, split_copy_id, name ++ ".split_copy", .maintenance);
                if (state.owner_epoch == 1) try add(list, allocator, handoff_id, name ++ ".writer_lease_handoff", .scheduler);
            },
            .split_cutover => try add(list, allocator, split_cutover_id, name ++ ".split_cutover", .maintenance),
            .read => try add(list, allocator, read_id, name ++ ".point_and_query_read", .workload),
            .terminal, .packet => {},
        }
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        const io = state.sim.io();
        if (selected.id == admit_id) {
            state.request_admitted = true;
            state.stage = .route;
        } else if (selected.id == drop_id) {
            state.sim.dropNextNetworkPacket();
            state.drop_injected = true;
        } else if (selected.id == duplicate_id) {
            state.sim.duplicateNextNetworkPacket();
            state.duplicate_injected = true;
        } else if (selected.id == route_id) {
            var stream = std.Io.net.Stream{ .socket = state.pair[0] };
            var buffer: [32]u8 = undefined;
            var writer = stream.writer(io, &buffer);
            try writer.interface.writeAll("put:a=1");
            try writer.interface.flush();
            state.stage = .packet;
        } else if (state.stage == .packet) {
            const dropped = std.mem.eql(u8, selected.name, "sim-io.packet_drop");
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
            if (state.sim.scheduler().quiescent()) state.stage = if (dropped) .route else .persist;
        } else if (selected.id == persist_id) {
            try state.raft_log.writeStreamingAll(io, "put:a=1");
            try state.raft_log.sync(io);
            state.replica_indexes = .{ 1, 1 };
            state.replica_digests = .{ vopr.id.digest("put:a=1"), vopr.id.digest("put:a=1") };
            state.stage = .apply;
        } else if (selected.id == apply_id) {
            var stream = std.Io.net.Stream{ .socket = state.pair[1] };
            var read_buffer: [32]u8 = undefined;
            var reader = stream.reader(io, &read_buffer);
            var payload: [7]u8 = undefined;
            try reader.interface.readSliceAll(&payload);
            state.applied = std.mem.eql(u8, &payload, "put:a=1");
            state.stage = .acknowledge;
        } else if (selected.id == acknowledge_id) {
            state.acknowledged = state.applied;
            state.stage = .split_copy;
        } else if (selected.id == handoff_id) state.owner_epoch += 1 else if (selected.id == split_copy_id) {
            var atomic = try std.Io.Dir.cwd().createFileAtomic(io, "data/split.copy", .{});
            defer atomic.deinit(io);
            try atomic.file.writeStreamingAll(io, "put:a=1");
            try atomic.file.sync(io);
            try atomic.link(io);
            state.split_copied = true;
            state.stage = .split_cutover;
        } else if (selected.id == split_cutover_id) {
            state.cutover = state.split_copied;
            state.stage = .read;
        } else if (selected.id == read_id) {
            var bytes: [7]u8 = undefined;
            const read_file = try std.Io.Dir.cwd().openFile(io, "data/raft.log", .{});
            defer read_file.close(io);
            state.query_visible = try read_file.readPositionalAll(io, &bytes, 0) == bytes.len and std.mem.eql(u8, &bytes, "put:a=1");
            state.stage = .terminal;
        } else return error.InvalidDataPlaneTransition;
        try events.emitNamed(allocator, .domain, selected.name, @intFromEnum(state.stage));
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".stage", @intCast(@intFromEnum(state.stage)));
        try builder.addNamed(allocator, name ++ ".owner_epoch", @intCast(state.owner_epoch));
        try builder.addNamed(allocator, name ++ ".acknowledged", @intFromBool(state.acknowledged));
        try builder.addNamed(allocator, name ++ ".query_visible", @intFromBool(state.query_visible));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const state = world.state;
        try sink.check(allocator, safety_id, !state.retired_owner_published and (!state.acknowledged or state.applied));
        try sink.check(allocator, topology_id, !state.cutover or state.split_copied);
        try sink.check(allocator, agreement_id, state.replica_indexes[0] != state.replica_indexes[1] or state.replica_digests[0] == state.replica_digests[1]);
        try sink.check(allocator, visible_id, state.stage == .terminal and state.acknowledged and state.query_visible);
    }
    pub fn done(world: *World) bool {
        return world.state.stage == .terminal;
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        try sink.add("data-plane", if (world.state.stage == .terminal) "visible" else "in-flight");
    }
};

pub const DerivedWorkflowScenario = struct {
    pub const name: []const u8 = "enrichment-index-repair-compaction";
    pub const version: u32 = 2;
    const safe_id = propertyId(name, "derived_state_never_exceeds_source_and_stale_generation_never_publishes");
    const debt_id = propertyId(name, "durable_workflow_debt_completes");
    const bounded_id = propertyId(name, "durable_job_budget_is_bounded");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = safe_id, .name = name ++ ".derived_state_never_exceeds_source_and_stale_generation_never_publishes", .kind = .always },
        .{ .id = debt_id, .name = name ++ ".durable_workflow_debt_completes", .kind = .reachable },
        .{ .id = bounded_id, .name = name ++ ".durable_job_budget_is_bounded", .kind = .always },
    };
    const provider_id = transitionId(name, "provider_result");
    const checkpoint_id = transitionId(name, "checkpoint_submit");
    const build_id = transitionId(name, "generation_build");
    const fence_id = transitionId(name, "leadership_fence");
    const publish_id = transitionId(name, "publish_submit");
    const repair_id = transitionId(name, "repair_submit");
    const compact_id = transitionId(name, "compact_submit");
    const cleanup_id = transitionId(name, "cleanup_submit");
    const cancel_id = transitionId(name, "cancel_pending_job");

    const Stage = enum {
        provider,
        checkpoint_ready,
        checkpoint_pending,
        build_ready,
        publish_ready,
        publish_pending,
        repair_ready,
        repair_pending,
        compact_ready,
        compact_pending,
        cleanup_ready,
        cleanup_pending,
        terminal,
    };
    const JobKind = enum { checkpoint, publish, repair, compact, cleanup };
    const JobContext = struct {
        state: *State,
        kind: JobKind,

        fn run(ptr: *anyopaque) !void {
            const self: *JobContext = @ptrCast(@alignCast(ptr));
            const state = self.state;
            switch (self.kind) {
                .checkpoint => {
                    state.checkpoint_revision = state.source_revision;
                    state.stage = .build_ready;
                },
                .publish => {
                    if (state.generation_epoch == state.leader_epoch) {
                        state.derived_revision = state.checkpoint_revision;
                        state.published_epoch = state.leader_epoch;
                    } else state.stale_publish_blocked = true;
                    state.stage = .repair_ready;
                },
                .repair => {
                    state.generation_epoch = state.leader_epoch;
                    state.derived_revision = state.source_revision;
                    state.published_epoch = state.leader_epoch;
                    state.stage = .compact_ready;
                },
                .compact => state.stage = .cleanup_ready,
                .cleanup => state.stage = .terminal,
            }
        }

        fn deinit(_: *anyopaque) void {}
    };
    const State = struct {
        runtime: vopr.sim_runtime.SimRuntime,
        lane: durable_job_lane.Lane,
        contexts: [5]JobContext,
        stage: Stage = .provider,
        owner_id: u64 = 1,
        source_revision: u64 = 0,
        checkpoint_revision: u64 = 0,
        derived_revision: u64 = 0,
        leader_epoch: u64 = 1,
        generation_epoch: u64 = 1,
        published_epoch: u64 = 0,
        stale_publish_blocked: bool = false,
        canceled: bool = false,
    };
    pub const World = struct { state: *State };

    pub fn init(allocator: Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.runtime = .init(allocator, 0);
        errdefer state.runtime.deinit();
        state.lane = .init(allocator, state.runtime.runtime().executor);
        errdefer state.lane.deinit();
        state.stage = .provider;
        state.owner_id = 1;
        state.source_revision = 0;
        state.checkpoint_revision = 0;
        state.derived_revision = 0;
        state.leader_epoch = 1;
        state.generation_epoch = 1;
        state.published_epoch = 0;
        state.stale_publish_blocked = false;
        state.canceled = false;
        inline for (std.meta.tags(JobKind), 0..) |kind, index| state.contexts[index] = .{ .state = state, .kind = kind };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: Allocator) void {
        world.state.lane.deinit();
        world.state.runtime.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        const state = world.state;
        try state.runtime.scheduler().enumerateReady(list, allocator);
        switch (state.stage) {
            .provider => try add(list, allocator, provider_id, name ++ ".provider_result", .workload),
            .checkpoint_ready => try add(list, allocator, checkpoint_id, name ++ ".checkpoint_submit", .maintenance),
            .build_ready => try add(list, allocator, build_id, name ++ ".generation_build", .maintenance),
            .publish_ready => {
                try add(list, allocator, publish_id, name ++ ".publish_submit", .maintenance);
                if (state.leader_epoch == 1) try add(list, allocator, fence_id, name ++ ".leadership_fence", .fault);
            },
            .publish_pending => if (state.leader_epoch == 1) try add(list, allocator, fence_id, name ++ ".leadership_fence", .fault),
            .repair_ready => try add(list, allocator, repair_id, name ++ ".repair_submit", .maintenance),
            .compact_ready => try add(list, allocator, compact_id, name ++ ".compact_submit", .maintenance),
            .cleanup_ready => try add(list, allocator, cleanup_id, name ++ ".cleanup_submit", .maintenance),
            .checkpoint_pending, .repair_pending, .compact_pending, .cleanup_pending => {
                if (!state.canceled and state.stage != .cleanup_pending)
                    try add(list, allocator, cancel_id, name ++ ".cancel_pending_job", .fault);
            },
            .terminal => {},
        }
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (selected.id == provider_id) {
            state.source_revision = 1;
            state.stage = .checkpoint_ready;
        } else if (selected.id == checkpoint_id) try submit(state, .checkpoint, .checkpoint_pending) else if (selected.id == build_id) {
            state.generation_epoch = state.leader_epoch;
            state.stage = .publish_ready;
        } else if (selected.id == fence_id) state.leader_epoch += 1 else if (selected.id == publish_id) try submit(state, .publish, .publish_pending) else if (selected.id == repair_id) try submit(state, .repair, .repair_pending) else if (selected.id == compact_id) try submit(state, .compact, .compact_pending) else if (selected.id == cleanup_id) try submit(state, .cleanup, .cleanup_pending) else if (selected.id == cancel_id) {
            state.lane.lane().closeOwner(state.owner_id);
            state.owner_id += 1;
            try state.lane.registerOwner(state.owner_id);
            state.canceled = true;
            state.stage = .cleanup_ready;
        } else try state.runtime.scheduler().executeReady(selected.id, events, allocator);
        try events.emitNamed(allocator, .domain, selected.name, @intFromEnum(state.stage));
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".stage", @intCast(@intFromEnum(state.stage)));
        try builder.addNamed(allocator, name ++ ".source_revision", @intCast(state.source_revision));
        try builder.addNamed(allocator, name ++ ".derived_revision", @intCast(state.derived_revision));
        try builder.addNamed(allocator, name ++ ".pending_jobs", @intCast(state.lane.stats().pending_jobs));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const state = world.state;
        const safe = state.derived_revision <= state.source_revision and
            (state.published_epoch == 0 or state.published_epoch == state.leader_epoch);
        try sink.check(allocator, safe_id, safe);
        try sink.check(allocator, bounded_id, state.lane.stats().pending_jobs <= 1);
        try sink.check(allocator, debt_id, state.stage == .terminal and state.lane.stats().pending_jobs == 0);
    }
    pub fn done(world: *World) bool {
        return world.state.stage == .terminal;
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        try sink.add("derived-workflow", if (world.state.stage == .terminal) "complete" else if (world.state.canceled) "canceling" else "pending");
    }

    fn submit(state: *State, kind: JobKind, pending: Stage) !void {
        const context = &state.contexts[@intFromEnum(kind)];
        try state.lane.lane().submit(.{
            .owner_id = state.owner_id,
            .class = if (kind == .cleanup) background_runtime.Job.Class.cleanup else .maintenance,
            .ptr = context,
            .run = JobContext.run,
            .deinit = JobContext.deinit,
        });
        state.stage = pending;
    }
};

pub const BackupRestoreScenario = struct {
    pub const name: []const u8 = "backup-restore-ha-seed-lifecycle";
    pub const version: u32 = 2;
    const atomic_id = propertyId(name, "restore_is_atomic_and_fail_closed");
    const pin_id = propertyId(name, "active_generation_remains_pinned");
    const winner_id = propertyId(name, "completion_and_cancellation_have_one_winner");
    const complete_id = propertyId(name, "restore_reaches_a_durable_terminal_state");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = atomic_id, .name = name ++ ".restore_is_atomic_and_fail_closed", .kind = .always },
        .{ .id = pin_id, .name = name ++ ".active_generation_remains_pinned", .kind = .always },
        .{ .id = winner_id, .name = name ++ ".completion_and_cancellation_have_one_winner", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".restore_reaches_a_durable_terminal_state", .kind = .reachable },
    };
    const upload_id = transitionId(name, "upload_chunk");
    const partial_id = transitionId(name, "partial_transfer");
    const duplicate_id = transitionId(name, "duplicate_request");
    const crash_id = transitionId(name, "crash_resume");
    const publish_id = transitionId(name, "manifest_publish");
    const pin_transition_id = transitionId(name, "retention_pin");
    const persist_id = transitionId(name, "restore_persist");
    const download_id = transitionId(name, "download_chunk");
    const topology_id = transitionId(name, "topology_reconstruct");
    const activate_id = transitionId(name, "activate");
    const cancel_id = transitionId(name, "cancel");
    const gc_id = transitionId(name, "generation_gc");

    const Stage = enum { upload, publish, pin, persist, download, topology, activate, gc, terminal };
    const Winner = enum { none, success, canceled };
    const State = struct {
        allocator: Allocator,
        stage: Stage = .upload,
        upload_chunks: u8 = 0,
        download_chunks: u8 = 0,
        transfer_faults: u8 = 0,
        crash_seen: bool = false,
        duplicate_seen: bool = false,
        manifest_bytes: ?[]u8 = null,
        manifest_valid: bool = false,
        pinned: bool = false,
        job_persisted: bool = false,
        topology_ready: bool = false,
        usable: bool = false,
        partial_reported_success: bool = false,
        required_generation_deleted: bool = false,
        winner: Winner = .none,

        fn deinit(self: *State) void {
            if (self.manifest_bytes) |bytes| self.allocator.free(bytes);
        }
    };
    pub const World = struct { state: *State };

    pub fn init(allocator: Allocator) !World {
        const state = try allocator.create(State);
        state.* = .{ .allocator = allocator };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: Allocator) void {
        world.state.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        const state = world.state;
        switch (state.stage) {
            .upload => {
                try add(list, allocator, upload_id, name ++ ".upload_chunk", .scheduler);
                if (state.transfer_faults < 2) try add(list, allocator, partial_id, name ++ ".partial_upload", .fault);
                if (!state.crash_seen) try add(list, allocator, crash_id, name ++ ".upload_crash_resume", .fault);
                if (!state.duplicate_seen) try add(list, allocator, duplicate_id, name ++ ".duplicate_backup_request", .workload);
            },
            .publish => try add(list, allocator, publish_id, name ++ ".manifest_publish", .maintenance),
            .pin => try add(list, allocator, pin_transition_id, name ++ ".retention_pin", .maintenance),
            .persist => try add(list, allocator, persist_id, name ++ ".restore_persist", .workload),
            .download => {
                try add(list, allocator, download_id, name ++ ".download_chunk", .scheduler);
                if (state.transfer_faults < 2) try add(list, allocator, partial_id, name ++ ".partial_download", .fault);
                if (!state.crash_seen) try add(list, allocator, crash_id, name ++ ".download_crash_resume", .fault);
            },
            .topology => try add(list, allocator, topology_id, name ++ ".topology_reconstruct", .maintenance),
            .activate => {
                try add(list, allocator, activate_id, name ++ ".activate", .maintenance);
                try add(list, allocator, cancel_id, name ++ ".cancel", .fault);
            },
            .gc => try add(list, allocator, gc_id, name ++ ".generation_gc", .maintenance),
            .terminal => {},
        }
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (selected.id == upload_id) {
            state.upload_chunks +|= 1;
            if (state.upload_chunks >= 2) state.stage = .publish;
        } else if (selected.id == partial_id) state.transfer_faults += 1 else if (selected.id == duplicate_id) state.duplicate_seen = true else if (selected.id == crash_id) state.crash_seen = true else if (selected.id == publish_id) {
            const content = "acknowledged-documents";
            const files = [_]backup_manifest.FileEntry{.{
                .path = "table/docs.sst",
                .kind = .sstable,
                .size_bytes = content.len,
                .crc32 = backup_manifest.crc32(content),
            }};
            state.manifest_bytes = try backup_manifest.encodeAlloc(state.allocator, .{
                .identity = .{ .cluster_id = 1, .shard_id = 2, .table_id = 3, .timeline_id = 4, .epoch = 5 },
                .manifest_id = "generation-1",
                .backup_lsn = 40,
                .checkpoint_lsn = 42,
                .files = &files,
            });
            const decoded = try backup_manifest.decodeAlloc(state.allocator, state.manifest_bytes.?);
            defer backup_manifest.freeDecoded(state.allocator, decoded);
            try backup_manifest.verifyFileContents(decoded, &.{.{ .path = "table/docs.sst", .bytes = content }});
            state.manifest_valid = true;
            state.stage = .pin;
        } else if (selected.id == pin_transition_id) {
            state.pinned = true;
            state.stage = .persist;
        } else if (selected.id == persist_id) {
            state.job_persisted = true;
            state.stage = .download;
        } else if (selected.id == download_id) {
            state.download_chunks +|= 1;
            if (state.download_chunks >= 2) state.stage = .topology;
        } else if (selected.id == topology_id) {
            state.topology_ready = true;
            state.stage = .activate;
        } else if (selected.id == activate_id) {
            if (!state.manifest_valid or !state.job_persisted or !state.topology_ready or state.download_chunks < 2)
                state.partial_reported_success = true
            else {
                state.winner = .success;
                state.usable = true;
            }
            state.stage = .gc;
        } else if (selected.id == cancel_id) {
            state.winner = .canceled;
            state.stage = .gc;
        } else if (selected.id == gc_id) {
            if (!state.pinned) state.required_generation_deleted = true;
            state.stage = .terminal;
        } else return error.InvalidBackupRestoreTransition;
        try events.emitNamed(allocator, .domain, selected.name, @intFromEnum(state.stage));
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".stage", @intCast(@intFromEnum(state.stage)));
        try builder.addNamed(allocator, name ++ ".winner", @intCast(@intFromEnum(state.winner)));
        try builder.addNamed(allocator, name ++ ".upload_chunks", state.upload_chunks);
        try builder.addNamed(allocator, name ++ ".download_chunks", state.download_chunks);
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const state = world.state;
        try sink.check(allocator, atomic_id, !state.partial_reported_success and (!state.usable or state.winner == .success));
        try sink.check(allocator, pin_id, !state.required_generation_deleted);
        try sink.check(allocator, winner_id, !(state.usable and state.winner == .canceled));
        try sink.check(allocator, complete_id, state.stage == .terminal and state.winner != .none);
    }
    pub fn done(world: *World) bool {
        return world.state.stage == .terminal;
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        try sink.add("backup-restore", switch (world.state.winner) {
            .none => "pending",
            .success => "activated",
            .canceled => "canceled",
        });
    }
};

pub const ClockLeaseTtlScenario = struct {
    pub const name: []const u8 = "clock-lease-retention-ttl-faults";
    pub const version: u32 = 2;
    const safe_id = propertyId(name, "no_early_takeover_or_deletion");
    const cleanup_id = propertyId(name, "cleanup_after_stabilization");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = safe_id, .name = name ++ ".no_early_takeover_or_deletion", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".cleanup_after_stabilization", .kind = .reachable },
    };
    const forward_id = transitionId(name, "realtime_forward");
    const backward_id = transitionId(name, "realtime_backward");
    const monotonic_id = transitionId(name, "monotonic_advance");
    const frequency_id = transitionId(name, "frequency_change");
    const pause_id = transitionId(name, "node_pause");
    const timer_id = transitionId(name, "timer_delivery");
    const stabilize_id = transitionId(name, "stabilize_and_cleanup");
    const State = struct {
        sim: vopr.vopr_io.VoprIo,
        clock: vopr.clock_fault.Domain,
        steps: u8 = 0,
        lease_deadline_real: i96 = 100,
        ttl_deadline_real: i96 = 120,
        takeover: bool = false,
        deleted: bool = false,
        early_action: bool = false,
        stable: bool = false,
    };
    pub const World = struct { state: *State };
    pub fn init(allocator: Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.sim = try .init(.{ .required = .of(&.{ .clock_read, .sleep }), .realtime_ns = 50 });
        errdefer state.sim.deinit();
        state.clock = try .init(&state.sim, .{ .fault_budget = 4 });
        state.steps = 0;
        state.lease_deadline_real = 100;
        state.ttl_deadline_real = 120;
        state.takeover = false;
        state.deleted = false;
        state.early_action = false;
        state.stable = false;
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: Allocator) void {
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        const state = world.state;
        if (state.stable) return;
        if (state.steps < 3) {
            if (state.clock.remainingFaultBudget() > 0) {
                try add(list, allocator, forward_id, name ++ ".realtime_forward", .fault);
                try add(list, allocator, backward_id, name ++ ".realtime_backward", .fault);
                try add(list, allocator, frequency_id, name ++ ".frequency_change", .fault);
                try add(list, allocator, pause_id, name ++ ".node_pause", .fault);
            }
            try add(list, allocator, monotonic_id, name ++ ".monotonic_advance", .scheduler);
            if (state.clock.timer_delivery_pending and !state.clock.paused)
                try add(list, allocator, timer_id, name ++ ".timer_delivery", .scheduler);
        } else try add(list, allocator, stabilize_id, name ++ ".stabilize_and_cleanup", .quiescence);
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, _: *vopr.event.Sink, _: Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (selected.id == forward_id) try state.clock.jumpRealtime(30) else if (selected.id == backward_id) try state.clock.jumpRealtime(-20) else if (selected.id == monotonic_id) try state.clock.advance(25) else if (selected.id == frequency_id) try state.clock.setRate(if (state.clock.rate_ppm == vopr.clock_fault.normal_rate_ppm) 1_500_000 else vopr.clock_fault.normal_rate_ppm) else if (selected.id == pause_id) try state.clock.setPaused(!state.clock.paused) else if (selected.id == timer_id) try state.clock.deliverTimers() else if (selected.id == stabilize_id) {
            try state.clock.stabilize();
            const now_real = std.Io.Clock.real.now(state.sim.io()).toNanoseconds();
            if (now_real < 130) try state.clock.advance(@intCast(130 - now_real));
            state.takeover = true;
            state.deleted = true;
            state.stable = true;
        } else return error.InvalidClockScenarioTransition;
        state.steps +|= 1;
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".real", @intCast(std.Io.Clock.real.now(state.sim.io()).toNanoseconds()));
        try builder.addNamed(allocator, name ++ ".monotonic", @intCast(std.Io.Clock.awake.now(state.sim.io()).toNanoseconds()));
        try builder.addNamed(allocator, name ++ ".rate_ppm", state.clock.rate_ppm);
        try builder.addNamed(allocator, name ++ ".paused", @intFromBool(state.clock.paused));
        try builder.addNamed(allocator, name ++ ".stable", @intFromBool(state.stable));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const state = world.state;
        const now = std.Io.Clock.real.now(state.sim.io()).toNanoseconds();
        if (state.takeover and now < state.lease_deadline_real) state.early_action = true;
        if (state.deleted and now < state.ttl_deadline_real) state.early_action = true;
        try sink.check(allocator, safe_id, !state.early_action);
        try sink.check(allocator, cleanup_id, state.stable and state.takeover and state.deleted);
    }
    pub fn done(world: *World) bool {
        return world.state.stable;
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        try sink.add("clocks", if (world.state.stable) "stable" else "faulted");
    }
};

fn add(list: *vopr.transition.List, allocator: Allocator, id: u64, transition_name: []const u8, kind: vopr.transition.Kind) !void {
    try list.append(allocator, .{ .id = id, .name = transition_name, .kind = kind });
}

fn record(comptime Scenario: type, allocator: Allocator, seed: u64, budget: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    return vopr.runner.run(Scenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = budget,
        .backend_ids = &backend_ids,
        .source_revision = "antfly-domain-vopr-v2",
        .target = "native",
        .optimize = @tagName(@import("builtin").mode),
    });
}

pub fn recordDistributedTransaction(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(DistributedTransactionScenario, allocator, seed, 20);
}

pub fn replayDistributedTransactionToTrace(allocator: Allocator, artifact: *const vopr.trace.Trace, writer: *std.Io.Writer) !vopr.trace.Trace {
    if (!std.mem.eql(u8, artifact.header.scenario, DistributedTransactionScenario.name)) return error.UnsupportedDomainScenario;
    var context = DistributedTransactionScenario.TraceContext{ .writer = writer };
    return vopr.replay.exactWithContext(DistributedTransactionScenario, allocator, artifact, &context);
}
pub fn recordDataPlane(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(DataPlaneScenario, allocator, seed, 16);
}
pub fn recordDerivedWorkflow(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(DerivedWorkflowScenario, allocator, seed, 16);
}
pub fn recordBackupRestore(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(BackupRestoreScenario, allocator, seed, 16);
}
pub fn recordClockLeaseTtl(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(ClockLeaseTtlScenario, allocator, seed, 8);
}

pub fn replayKnown(allocator: Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    if (std.mem.eql(u8, artifact.header.scenario, DistributedTransactionScenario.name))
        return vopr.replay.exact(DistributedTransactionScenario, allocator, artifact);
    if (std.mem.eql(u8, artifact.header.scenario, DataPlaneScenario.name))
        return vopr.replay.exact(DataPlaneScenario, allocator, artifact);
    if (std.mem.eql(u8, artifact.header.scenario, DerivedWorkflowScenario.name))
        return vopr.replay.exact(DerivedWorkflowScenario, allocator, artifact);
    if (std.mem.eql(u8, artifact.header.scenario, BackupRestoreScenario.name))
        return vopr.replay.exact(BackupRestoreScenario, allocator, artifact);
    if (std.mem.eql(u8, artifact.header.scenario, ClockLeaseTtlScenario.name))
        return vopr.replay.exact(ClockLeaseTtlScenario, allocator, artifact);
    return error.UnsupportedDomainScenario;
}

pub const Kind = enum {
    distributed_transaction,
    data_plane,
    derived_workflow,
    backup_restore,
    clock_fault,

    pub fn cliName(self: Kind) []const u8 {
        return switch (self) {
            .distributed_transaction => "distributed-transaction",
            .data_plane => "data-plane",
            .derived_workflow => "derived-workflow",
            .backup_restore => "backup-restore",
            .clock_fault => "clock-fault",
        };
    }

    pub fn scenarioName(self: Kind) []const u8 {
        return switch (self) {
            .distributed_transaction => DistributedTransactionScenario.name,
            .data_plane => DataPlaneScenario.name,
            .derived_workflow => DerivedWorkflowScenario.name,
            .backup_restore => BackupRestoreScenario.name,
            .clock_fault => ClockLeaseTtlScenario.name,
        };
    }

    pub fn transitionBudget(self: Kind) usize {
        return switch (self) {
            .distributed_transaction => 20,
            .data_plane, .derived_workflow, .backup_restore => 16,
            .clock_fault => 8,
        };
    }
};

pub fn kindFromCliName(cli_name: []const u8) ?Kind {
    inline for (std.meta.tags(Kind)) |kind| {
        if (std.mem.eql(u8, cli_name, kind.cliName())) return kind;
    }
    return null;
}

pub fn kindFromArtifact(artifact: *const vopr.trace.Trace) ?Kind {
    inline for (std.meta.tags(Kind)) |kind| {
        if (std.mem.eql(u8, artifact.header.scenario, kind.scenarioName())) return kind;
    }
    return null;
}

pub fn recordNamed(allocator: Allocator, cli_name: []const u8, seed: u64) !vopr.trace.Trace {
    return switch (kindFromCliName(cli_name) orelse return error.UnsupportedDomainScenario) {
        .distributed_transaction => recordDistributedTransaction(allocator, seed),
        .data_plane => recordDataPlane(allocator, seed),
        .derived_workflow => recordDerivedWorkflow(allocator, seed),
        .backup_restore => recordBackupRestore(allocator, seed),
        .clock_fault => recordClockLeaseTtl(allocator, seed),
    };
}

pub fn artifactMatchesCliName(artifact: *const vopr.trace.Trace, cli_name: []const u8) bool {
    const expected = kindFromCliName(cli_name) orelse return false;
    return std.mem.eql(u8, artifact.header.scenario, expected.scenarioName());
}

fn runWithChoices(
    comptime Scenario: type,
    allocator: Allocator,
    artifact: *const vopr.trace.Trace,
    source: vopr.choice.Source,
    recorder: ?*vopr.flight_recorder.Recorder,
) !vopr.trace.Trace {
    return vopr.runner.run(Scenario, allocator, source, .{
        .system = artifact.header.system,
        .seed = artifact.config.seed,
        .transition_budget = artifact.config.transition_budget,
        .resource_budget = artifact.config.resource_budget,
        .fixture_hashes = artifact.config.fixture_hashes,
        .feature_flags = artifact.config.feature_flags,
        .backend_ids = artifact.config.backend_ids,
        .scenario_parameters = artifact.config.scenario_parameters,
        .source_revision = artifact.header.source_revision,
        .target = artifact.header.target,
        .optimize = artifact.header.optimize,
        .flight_recorder = recorder,
    });
}

pub fn runKnownWithChoices(allocator: Allocator, artifact: *const vopr.trace.Trace, source: vopr.choice.Source) !vopr.trace.Trace {
    return runKnownWithChoicesAndRecorder(allocator, artifact, source, null);
}

pub fn runKnownWithChoicesAndRecorder(
    allocator: Allocator,
    artifact: *const vopr.trace.Trace,
    source: vopr.choice.Source,
    recorder: ?*vopr.flight_recorder.Recorder,
) !vopr.trace.Trace {
    return switch (kindFromArtifact(artifact) orelse return error.UnsupportedDomainScenario) {
        .distributed_transaction => runWithChoices(DistributedTransactionScenario, allocator, artifact, source, recorder),
        .data_plane => runWithChoices(DataPlaneScenario, allocator, artifact, source, recorder),
        .derived_workflow => runWithChoices(DerivedWorkflowScenario, allocator, artifact, source, recorder),
        .backup_restore => runWithChoices(BackupRestoreScenario, allocator, artifact, source, recorder),
        .clock_fault => runWithChoices(ClockLeaseTtlScenario, allocator, artifact, source, recorder),
    };
}

pub fn reduceKnown(allocator: Allocator, artifact: *const vopr.trace.Trace, target: u64, config: vopr.reducer.Config) !vopr.reducer.Result {
    return switch (kindFromArtifact(artifact) orelse return error.UnsupportedDomainScenario) {
        .distributed_transaction => vopr.reducer.reduce(DistributedTransactionScenario, allocator, artifact, target, config),
        .data_plane => vopr.reducer.reduce(DataPlaneScenario, allocator, artifact, target, config),
        .derived_workflow => vopr.reducer.reduce(DerivedWorkflowScenario, allocator, artifact, target, config),
        .backup_restore => vopr.reducer.reduce(BackupRestoreScenario, allocator, artifact, target, config),
        .clock_fault => vopr.reducer.reduce(ClockLeaseTtlScenario, allocator, artifact, target, config),
    };
}

fn expectRecordAndReplay(comptime Scenario: type, seed: u64, budget: u64) !void {
    var artifact = try record(Scenario, std.testing.allocator, seed, budget);
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    for (0..100) |_| {
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
    var collected = try vopr.debugger.collectAt(Scenario, std.testing.allocator, &artifact, artifact.choices.items.len);
    collected.deinit();
    for (0..16) |offset| {
        var generated = try record(Scenario, std.testing.allocator, seed + 1 + offset, budget);
        defer generated.deinit();
        try std.testing.expectEqual(@as(u64, 0), generated.summary.?.property_failures);
        var generated_replay = try vopr.replay.exact(Scenario, std.testing.allocator, &generated);
        generated_replay.deinit();
    }
}

test "distributed transaction lifecycle VOPR records and exact replays" {
    try expectRecordAndReplay(DistributedTransactionScenario, 0xA17F_5000, 20);
    var artifact = try recordDistributedTransaction(std.testing.allocator, 0xA17F_5000);
    defer artifact.deinit();
    var trace: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer trace.deinit();
    var replayed = try replayDistributedTransactionToTrace(std.testing.allocator, &artifact, &trace.writer);
    replayed.deinit();
    try std.testing.expect(std.mem.indexOf(u8, trace.written(), "\"name\":\"InitTransaction\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, trace.written(), "\"name\":\"WriteIntentOnShard\"") != null);
}

test "data plane microstep VOPR records and exact replays" {
    try expectRecordAndReplay(DataPlaneScenario, 0xA17F_5001, 16);
}

test "derived workflow VOPR records and exact replays" {
    try expectRecordAndReplay(DerivedWorkflowScenario, 0xA17F_5002, 16);
}

test "backup restore lifecycle VOPR records and exact replays" {
    try expectRecordAndReplay(BackupRestoreScenario, 0xA17F_5003, 16);
}

test "clock lease TTL fault VOPR records and exact replays" {
    try expectRecordAndReplay(ClockLeaseTtlScenario, 0xA17F_5004, 8);
}
