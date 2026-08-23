// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Cross-domain Antfly protocol scenarios for deterministic VOPR campaigns.
//!
//! These models define the scheduler vocabulary and executable oracles used by
//! the production adapters. They deliberately share the normal VOPR runner,
//! trace, replay, reduction, and fixture contracts.

const std = @import("std");
const vopr = @import("vopr");

const Allocator = std.mem.Allocator;

fn transitionId(scenario_name: []const u8, action: []const u8) u64 {
    return vopr.id.derive("transition", vopr.id.stable("scenario", scenario_name), vopr.id.digest(action));
}

fn propertyId(comptime scenario_name: []const u8, comptime property_name: []const u8) u64 {
    return vopr.id.stable("property", scenario_name ++ "." ++ property_name);
}

pub const DistributedTransactionScenario = struct {
    pub const name: []const u8 = "distributed-transaction-lifecycle";
    pub const version: u32 = 1;
    const consistent_id = propertyId(name, "globally_consistent_decision");
    const fenced_id = propertyId(name, "stale_owner_fenced");
    const resolved_id = propertyId(name, "durable_intents_resolve");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = consistent_id, .name = name ++ ".globally_consistent_decision", .kind = .always },
        .{ .id = fenced_id, .name = name ++ ".stale_owner_fenced", .kind = .always },
        .{ .id = resolved_id, .name = name ++ ".durable_intents_resolve", .kind = .reachable },
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

    const Decision = enum { none, commit, abort };
    pub const World = struct {
        phase: enum { begin, intents, prepare, decide, deliver, terminal } = .begin,
        prepared: u2 = 0,
        delivered: u2 = 0,
        decision: Decision = .none,
        participant_decisions: [2]Decision = .{ .none, .none },
        lease_epoch: u32 = 1,
        stale_attempt_succeeded: bool = false,
        ambiguous: bool = false,
        crashed_once: bool = false,
    };
    pub fn init(_: Allocator) !World {
        return .{};
    }
    pub fn deinit(_: *World, _: Allocator) void {}
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        switch (world.phase) {
            .begin => try add(list, allocator, begin_id, name ++ ".begin", .workload),
            .intents => try add(list, allocator, intent_id, name ++ ".write_intents", .workload),
            .prepare => {
                if (world.prepared & 1 == 0) try add(list, allocator, prepare_a_id, name ++ ".prepare_a", .scheduler);
                if (world.prepared & 2 == 0) try add(list, allocator, prepare_b_id, name ++ ".prepare_b", .scheduler);
                if (world.prepared != 3 and !world.crashed_once) try add(list, allocator, crash_id, name ++ ".coordinator_crash_restart", .fault);
            },
            .decide => {
                try add(list, allocator, commit_id, name ++ ".durable_commit", .workload);
                try add(list, allocator, abort_id, name ++ ".durable_abort", .workload);
            },
            .deliver => {
                if (!world.ambiguous) try add(list, allocator, ambiguous_id, name ++ ".ambiguous_response", .fault);
                if (world.lease_epoch == 1) try add(list, allocator, lease_id, name ++ ".lease_expire_adopt", .maintenance);
                if (world.delivered & 1 == 0) try add(list, allocator, deliver_a_id, name ++ ".phase_two_a", .scheduler);
                if (world.delivered & 2 == 0) try add(list, allocator, deliver_b_id, name ++ ".phase_two_b", .scheduler);
                if (world.delivered != 3) try add(list, allocator, repair_id, name ++ ".recovery_repair", .maintenance);
            },
            .terminal => {},
        }
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: Allocator) !vopr.outcome.TransitionOutcome {
        if (selected.id == begin_id) world.phase = .intents else if (selected.id == intent_id) world.phase = .prepare else if (selected.id == prepare_a_id) world.prepared |= 1 else if (selected.id == prepare_b_id) world.prepared |= 2 else if (selected.id == crash_id) world.crashed_once = true else if (selected.id == commit_id or selected.id == abort_id) {
            world.decision = if (selected.id == commit_id) .commit else .abort;
            world.phase = .deliver;
        } else if (selected.id == ambiguous_id) world.ambiguous = true else if (selected.id == lease_id) world.lease_epoch += 1 else if (selected.id == deliver_a_id) {
            world.delivered |= 1;
            world.participant_decisions[0] = world.decision;
        } else if (selected.id == deliver_b_id) {
            world.delivered |= 2;
            world.participant_decisions[1] = world.decision;
        } else if (selected.id == repair_id) {
            world.delivered = 3;
            world.participant_decisions = .{ world.decision, world.decision };
        } else return error.InvalidDistributedTransactionTransition;
        if (world.phase == .prepare and world.prepared == 3) world.phase = .decide;
        if (world.phase == .deliver and world.delivered == 3) world.phase = .terminal;
        try events.emitNamed(allocator, .domain, selected.name, world.lease_epoch);
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        try builder.addNamed(allocator, name ++ ".decision", @intFromEnum(world.decision));
        try builder.addNamed(allocator, name ++ ".prepared", world.prepared);
        try builder.addNamed(allocator, name ++ ".delivered", world.delivered);
        try builder.addNamed(allocator, name ++ ".lease_epoch", world.lease_epoch);
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const consistent = for (world.participant_decisions) |decision| {
            if (decision != .none and decision != world.decision) break false;
        } else true;
        try sink.check(allocator, consistent_id, consistent);
        try sink.check(allocator, fenced_id, !world.stale_attempt_succeeded);
        try sink.check(allocator, resolved_id, world.phase == .terminal);
    }
    pub fn done(world: *World) bool {
        return world.phase == .terminal;
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        try sink.add("transaction", if (world.phase == .terminal) "resolved" else "pending");
    }
};

pub const DataPlaneScenario = LinearScenario(.{
    .name = "dataserver-public-microsteps",
    .actions = &.{ "admit", "route", "raft_persist", "raft_apply", "acknowledge", "split_copy", "split_cutover", "point_and_query_read" },
    .safety = "routing_ownership_and_acknowledged_visibility",
});

pub const DerivedWorkflowScenario = LinearScenario(.{
    .name = "enrichment-index-repair-compaction",
    .actions = &.{ "provider_result", "checkpoint", "generation_build", "leadership_fence", "publish", "repair", "compact", "cleanup" },
    .safety = "derived_state_never_exceeds_source_and_stale_generation_never_publishes",
});

pub const BackupRestoreScenario = LinearScenario(.{
    .name = "backup-restore-ha-seed-lifecycle",
    .actions = &.{ "upload", "manifest_publish", "retention_pin", "restore_persist", "download", "topology_reconstruct", "activate", "generation_gc" },
    .safety = "atomic_restore_pins_required_generation_and_gc_is_safe",
});

pub const ClockLeaseTtlScenario = struct {
    pub const name: []const u8 = "clock-lease-retention-ttl-faults";
    pub const version: u32 = 1;
    const safe_id = propertyId(name, "no_early_takeover_or_deletion");
    const cleanup_id = propertyId(name, "cleanup_after_stabilization");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = safe_id, .name = name ++ ".no_early_takeover_or_deletion", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".cleanup_after_stabilization", .kind = .reachable },
    };
    const forward_id = transitionId(name, "realtime_forward");
    const backward_id = transitionId(name, "realtime_backward");
    const monotonic_id = transitionId(name, "monotonic_advance");
    const stabilize_id = transitionId(name, "stabilize_and_cleanup");
    pub const World = struct {
        sim: vopr.sim_io.SimIo,
        steps: u8 = 0,
        lease_deadline_real: i96 = 100,
        ttl_deadline_real: i96 = 120,
        takeover: bool = false,
        deleted: bool = false,
        early_action: bool = false,
        stable: bool = false,
    };
    pub fn init(_: Allocator) !World {
        return .{ .sim = try .init(.{ .required = .of(&.{.clock_read}), .realtime_ns = 50 }) };
    }
    pub fn deinit(world: *World, _: Allocator) void {
        world.sim.deinit();
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        if (world.stable) return;
        if (world.steps < 3) {
            try add(list, allocator, forward_id, name ++ ".realtime_forward", .fault);
            try add(list, allocator, backward_id, name ++ ".realtime_backward", .fault);
            try add(list, allocator, monotonic_id, name ++ ".monotonic_advance", .scheduler);
        } else try add(list, allocator, stabilize_id, name ++ ".stabilize_and_cleanup", .quiescence);
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, _: *vopr.event.Sink, _: Allocator) !vopr.outcome.TransitionOutcome {
        if (selected.id == forward_id) try world.sim.jumpRealtime(30) else if (selected.id == backward_id) try world.sim.jumpRealtime(-20) else if (selected.id == monotonic_id) try world.sim.advance(25) else if (selected.id == stabilize_id) {
            const now_real = std.Io.Clock.real.now(world.sim.io()).toNanoseconds();
            if (now_real < 130) try world.sim.jumpRealtime(@intCast(130 - now_real));
            world.takeover = true;
            world.deleted = true;
            world.stable = true;
        } else return error.InvalidClockScenarioTransition;
        world.steps +|= 1;
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        try builder.addNamed(allocator, name ++ ".real", @intCast(std.Io.Clock.real.now(world.sim.io()).toNanoseconds()));
        try builder.addNamed(allocator, name ++ ".monotonic", @intCast(std.Io.Clock.awake.now(world.sim.io()).toNanoseconds()));
        try builder.addNamed(allocator, name ++ ".stable", @intFromBool(world.stable));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const now = std.Io.Clock.real.now(world.sim.io()).toNanoseconds();
        if (world.takeover and now < world.lease_deadline_real) world.early_action = true;
        if (world.deleted and now < world.ttl_deadline_real) world.early_action = true;
        try sink.check(allocator, safe_id, !world.early_action);
        try sink.check(allocator, cleanup_id, world.stable and world.takeover and world.deleted);
    }
    pub fn done(world: *World) bool {
        return world.stable;
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        try sink.add("clocks", if (world.stable) "stable" else "faulted");
    }
};

const LinearConfig = struct { name: []const u8, actions: []const []const u8, safety: []const u8 };

fn LinearScenario(comptime config: LinearConfig) type {
    return struct {
        pub const name: []const u8 = config.name;
        pub const version: u32 = 1;
        const safe_id = propertyId(name, config.safety);
        const complete_id = propertyId(name, "all_obligations_complete");
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = safe_id, .name = name ++ "." ++ config.safety, .kind = .always },
            .{ .id = complete_id, .name = name ++ ".all_obligations_complete", .kind = .reachable },
        };
        pub const World = struct { step: usize = 0, violation: bool = false };
        pub fn init(_: Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: Allocator) void {}
        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
            if (world.step == config.actions.len) return;
            const action = config.actions[world.step];
            try add(list, allocator, transitionId(name, action), action, if (world.step % 3 == 1) .scheduler else .maintenance);
        }
        pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: Allocator) !vopr.outcome.TransitionOutcome {
            const expected = config.actions[world.step];
            if (selected.id != transitionId(name, expected)) return error.InvalidWorkflowTransition;
            world.step += 1;
            try events.emitNamed(allocator, .domain, expected, world.step);
            return .applied();
        }
        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
            try builder.addNamed(allocator, name ++ ".step", @intCast(world.step));
            try builder.addNamed(allocator, name ++ ".violation", @intFromBool(world.violation));
        }
        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
            try sink.check(allocator, safe_id, !world.violation);
            try sink.check(allocator, complete_id, world.step == config.actions.len);
        }
        pub fn done(world: *World) bool {
            return world.step == config.actions.len;
        }
        pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
            try sink.add("workflow", if (world.step == config.actions.len) "complete" else "pending");
        }
    };
}

fn add(list: *vopr.transition.List, allocator: Allocator, id: u64, transition_name: []const u8, kind: vopr.transition.Kind) !void {
    try list.append(allocator, .{ .id = id, .name = transition_name, .kind = kind });
}

fn record(comptime Scenario: type, allocator: Allocator, seed: u64, budget: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    const backend_ids = vopr.sim_io.artifactBackendIds();
    return vopr.runner.run(Scenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = budget,
        .backend_ids = &backend_ids,
        .source_revision = "antfly-domain-vopr-v1",
        .target = "native",
        .optimize = @tagName(@import("builtin").mode),
    });
}

pub fn recordDistributedTransaction(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(DistributedTransactionScenario, allocator, seed, 20);
}
pub fn recordDataPlane(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(DataPlaneScenario, allocator, seed, 12);
}
pub fn recordDerivedWorkflow(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(DerivedWorkflowScenario, allocator, seed, 12);
}
pub fn recordBackupRestore(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    return record(BackupRestoreScenario, allocator, seed, 12);
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
}

test "distributed transaction lifecycle VOPR records and exact replays" {
    try expectRecordAndReplay(DistributedTransactionScenario, 0xA17F_5000, 20);
}

test "data plane microstep VOPR records and exact replays" {
    try expectRecordAndReplay(DataPlaneScenario, 0xA17F_5001, 12);
}

test "derived workflow VOPR records and exact replays" {
    try expectRecordAndReplay(DerivedWorkflowScenario, 0xA17F_5002, 12);
}

test "backup restore lifecycle VOPR records and exact replays" {
    try expectRecordAndReplay(BackupRestoreScenario, 0xA17F_5003, 12);
}

test "clock lease TTL fault VOPR records and exact replays" {
    try expectRecordAndReplay(ClockLeaseTtlScenario, 0xA17F_5004, 8);
}
