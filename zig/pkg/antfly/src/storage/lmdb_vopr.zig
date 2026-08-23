// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Common-runner adapter for the C-LMDB versus Zig-LMDB differential and
//! modeled crash-publication simulator.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const lmdb = @import("lmdb.zig");

const DifferentialAction = lmdb.VoprDifferentialAction;
const ScheduledAction = lmdb.VoprScheduledAction;
const CommitPhase = lmdb.VoprCommitPhase;

const finish_id = vopr.id.stable("transition", "storage.lmdb.finish");
const direct_base = vopr.id.stable("transition", "storage.lmdb.direct");
const nested_commit_base = vopr.id.stable("transition", "storage.lmdb.nested_commit");
const nested_abort_base = vopr.id.stable("transition", "storage.lmdb.nested_abort");
const reader_base = vopr.id.stable("transition", "storage.lmdb.reader_then_direct");
const crash_base = vopr.id.stable("transition", "storage.lmdb.crash_publish_phase");

const differential_id = vopr.id.stable("property", "storage.lmdb.c_and_zig_snapshots_match");
const crash_outcome_id = vopr.id.stable("property", "storage.lmdb.crash_reopens_to_allowed_snapshot");
const completion_id = vopr.id.stable("property", "storage.lmdb.campaign_completed");

const DirectKind = enum(u8) {
    put_main,
    delete_main,
    put_docs,
    delete_docs,
    put_dup,
    delete_dup_value,
};

pub fn Scenario(comptime action_budget: u64) type {
    return struct {
        pub const name: []const u8 = "lmdb-differential";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = differential_id, .name = "storage.lmdb.c_and_zig_snapshots_match", .kind = .always },
            .{ .id = crash_outcome_id, .name = "storage.lmdb.crash_reopens_to_allowed_snapshot", .kind = .always },
            .{ .id = completion_id, .name = "storage.lmdb.campaign_completed", .kind = .reachable },
        };

        const State = struct {
            allocator: std.mem.Allocator,
            actions: std.ArrayListUnmanaged(ScheduledAction) = .empty,
            crash_prelude: std.ArrayListUnmanaged(DifferentialAction) = .empty,
            faults: vopr.fault.Controller,
            summary: lmdb.VoprSnapshotSummary = .{ .main_count = 0, .docs_count = 0, .dups_count = 0 },
            differential_matches: bool = true,
            crash_outcome_allowed: bool = true,
            crash_outcome: ?lmdb.VoprCrashOutcome = null,
            nested_commits: u64 = 0,
            nested_aborts: u64 = 0,
            reader_snapshots: u64 = 0,
            finished: bool = false,

            fn deinit(self: *State) void {
                self.actions.deinit(self.allocator);
                self.crash_prelude.deinit(self.allocator);
                self.faults.deinit();
            }
        };

        pub const World = struct { state: *State };

        pub fn init(allocator: std.mem.Allocator) !World {
            const state = try allocator.create(State);
            errdefer allocator.destroy(state);
            state.* = .{
                .allocator = allocator,
                .faults = try vopr.fault.Controller.init(allocator, 1, .{
                    .max_storage_faults_per_durability_epoch = 1,
                    .minimum_healthy_nodes = 1,
                }),
            };
            return .{ .state = state };
        }

        pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
            world.state.deinit();
            allocator.destroy(world.state);
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
            const state = world.state;
            if (state.actions.items.len >= action_budget or state.crash_outcome != null) {
                try list.append(allocator, .{ .id = finish_id, .name = "storage.lmdb.finish", .kind = .quiescence });
                return;
            }

            for (0..3) |key| {
                for (std.enums.values(DirectKind)) |kind| {
                    try list.append(allocator, directTransition(kind, @intCast(key)));
                }
                try list.append(allocator, keyedTransition(nested_commit_base, "storage.lmdb.nested_commit", .workload, @intCast(key)));
                try list.append(allocator, keyedTransition(nested_abort_base, "storage.lmdb.nested_abort", .workload, @intCast(key)));
                try list.append(allocator, keyedTransition(reader_base, "storage.lmdb.reader_then_direct", .workload, @intCast(key)));
            }
            for (std.enums.values(CommitPhase)) |phase| {
                const spec = crashSpec(phase);
                if ((try state.faults.admission(spec)).isAllowed()) try list.append(allocator, .{
                    .id = vopr.id.derive("storage.lmdb.crash-transition", crash_base, @intFromEnum(phase)),
                    .name = "storage.lmdb.crash_publish_phase",
                    .kind = .fault,
                    .resource_id = spec.resource_id,
                    .parameter = @intFromEnum(phase),
                });
            }
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: std.mem.Allocator,
        ) !vopr.outcome.TransitionOutcome {
            const state = world.state;
            if (selected.id == finish_id) {
                state.finished = true;
                try events.emitNamed(allocator, .domain, "storage.lmdb.campaign_complete", state.actions.items.len);
                return vopr.outcome.TransitionOutcome.targetReached("storage.lmdb.campaign_complete", state.actions.items.len);
            }

            if (std.mem.eql(u8, selected.name, "storage.lmdb.crash_publish_phase")) {
                const phase: CommitPhase = @enumFromInt(selected.parameter);
                const spec = crashSpec(phase);
                try state.faults.start(spec, events, allocator);
                const step: u16 = @intCast(state.actions.items.len);
                const crash_action = DifferentialAction{ .put_main = .{ .key_index = 7, .value_index = 9_000 + step } };
                state.crash_outcome = lmdb.replayVoprCrash(allocator, state.crash_prelude.items, crash_action, phase) catch |err| {
                    if (!isOracleMismatch(err)) return err;
                    state.crash_outcome_allowed = false;
                    _ = try state.faults.consumeOneShot(.storage, spec.resource_id, events, allocator);
                    return vopr.outcome.TransitionOutcome.propertyViolation(crash_outcome_id, "storage.lmdb.crash_snapshot_mismatch", @intFromEnum(phase));
                };
                _ = try state.faults.consumeOneShot(.storage, spec.resource_id, events, allocator);
                try events.emitNamed(allocator, .state_change, "storage.lmdb.crash_reopened", @intFromEnum(state.crash_outcome.?));
                return vopr.outcome.TransitionOutcome.applied();
            }

            const step: u16 = @intCast(state.actions.items.len);
            const action = actionForTransition(selected, step) orelse return error.UnknownLmdbVoprTransition;
            try state.actions.append(allocator, action);
            appendCrashPrelude(state, action) catch |err| {
                _ = state.actions.pop();
                return err;
            };
            state.summary = lmdb.replayVoprDifferential(allocator, state.actions.items) catch |err| {
                if (!isOracleMismatch(err)) return err;
                state.differential_matches = false;
                return vopr.outcome.TransitionOutcome.propertyViolation(differential_id, "storage.lmdb.differential_snapshot_mismatch", state.actions.items.len);
            };
            switch (action) {
                .nested_commit => state.nested_commits += 1,
                .nested_abort => state.nested_aborts += 1,
                .reader_then_direct => state.reader_snapshots += 1,
                .direct => {},
            }
            try events.emitNamed(allocator, .state_change, "storage.lmdb.action_replayed", actionDigest(action));
            return vopr.outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            const state = world.state;
            try builder.addNamed(allocator, "storage.lmdb.actions", @intCast(state.actions.items.len));
            try builder.addNamed(allocator, "storage.lmdb.main_count", @intCast(state.summary.main_count));
            try builder.addNamed(allocator, "storage.lmdb.docs_count", @intCast(state.summary.docs_count));
            try builder.addNamed(allocator, "storage.lmdb.dups_count", @intCast(state.summary.dups_count));
            try builder.addNamed(allocator, "storage.lmdb.nested_commits", @intCast(state.nested_commits));
            try builder.addNamed(allocator, "storage.lmdb.nested_aborts", @intCast(state.nested_aborts));
            try builder.addNamed(allocator, "storage.lmdb.reader_snapshots", @intCast(state.reader_snapshots));
            try builder.addNamed(allocator, "storage.lmdb.crash_phase", if (state.crash_outcome) |value| @intFromEnum(value) + 1 else 0);
            try builder.addNamed(allocator, "storage.lmdb.finished", @intFromBool(state.finished));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            const state = world.state;
            try sink.check(allocator, differential_id, state.differential_matches);
            try sink.check(allocator, crash_outcome_id, state.crash_outcome_allowed);
            try sink.check(allocator, completion_id, state.finished);
        }

        pub fn done(world: *World) bool {
            return world.state.finished;
        }
    };
}

pub const CliScenario = Scenario(12);

pub fn record(allocator: std.mem.Allocator, seed: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    return vopr.runner.run(CliScenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = 13,
        .source_revision = "lmdb-vopr-cli",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
}

pub fn replay(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(CliScenario, allocator, artifact);
}

fn keyedTransition(base: u64, name: []const u8, kind: vopr.transition.Kind, key: u8) vopr.transition.Transition {
    return .{
        .id = vopr.id.derive("storage.lmdb.keyed-transition", base, key),
        .name = name,
        .kind = kind,
        .parameter = key,
    };
}

fn directTransition(kind: DirectKind, key: u8) vopr.transition.Transition {
    const encoded = (@as(u64, @intFromEnum(kind)) << 8) | key;
    return .{
        .id = vopr.id.derive("storage.lmdb.direct-transition", direct_base, encoded),
        .name = "storage.lmdb.direct",
        .kind = .workload,
        .parameter = @intCast(encoded),
    };
}

fn actionForTransition(selected: vopr.transition.Transition, step: u16) ?ScheduledAction {
    if (std.mem.eql(u8, selected.name, "storage.lmdb.direct")) {
        const encoded: u64 = @bitCast(selected.parameter);
        const kind: DirectKind = @enumFromInt(encoded >> 8);
        const key: u8 = @truncate(encoded);
        if (selected.id != directTransition(kind, key).id) return null;
        return .{ .direct = differentialAction(kind, key, valueFor(step, key)) };
    }
    const key: u8 = @intCast(selected.parameter);
    if (selected.id == keyedTransition(nested_commit_base, "storage.lmdb.nested_commit", .workload, key).id) return .{
        .nested_commit = .{
            .parent = .{ .put_main = .{ .key_index = key, .value_index = valueFor(step, key) } },
            .child = .{ .put_docs = .{ .key_index = key, .value_index = valueFor(step + 1, key) } },
        },
    };
    if (selected.id == keyedTransition(nested_abort_base, "storage.lmdb.nested_abort", .workload, key).id) return .{
        .nested_abort = .{
            .parent = .{ .put_main = .{ .key_index = key, .value_index = valueFor(step, key) } },
            .child = .{ .put_docs = .{ .key_index = key, .value_index = valueFor(step + 1, key) } },
        },
    };
    if (selected.id == keyedTransition(reader_base, "storage.lmdb.reader_then_direct", .workload, key).id) return .{
        .reader_then_direct = .{ .put_dup = .{ .key_index = key, .value_index = valueFor(step, key) } },
    };
    return null;
}

fn differentialAction(kind: DirectKind, key: u8, value: u16) DifferentialAction {
    return switch (kind) {
        .put_main => .{ .put_main = .{ .key_index = key, .value_index = value } },
        .delete_main => .{ .delete_main = .{ .key_index = key } },
        .put_docs => .{ .put_docs = .{ .key_index = key, .value_index = value } },
        .delete_docs => .{ .delete_docs = .{ .key_index = key } },
        .put_dup => .{ .put_dup = .{ .key_index = key, .value_index = value } },
        .delete_dup_value => .{ .delete_dup_value = .{ .key_index = key, .value_index = value } },
    };
}

fn appendCrashPrelude(state: anytype, action: ScheduledAction) !void {
    switch (action) {
        .direct, .reader_then_direct => |direct| try state.crash_prelude.append(state.allocator, direct),
        .nested_commit => |nested| {
            try state.crash_prelude.append(state.allocator, nested.parent);
            try state.crash_prelude.append(state.allocator, nested.child);
        },
        .nested_abort => |nested| try state.crash_prelude.append(state.allocator, nested.parent),
    }
}

fn crashSpec(phase: CommitPhase) vopr.fault.Spec {
    const resource = vopr.id.derive("storage.lmdb.commit-publication", vopr.id.stable("resource", "storage.lmdb.commit"), @intFromEnum(phase));
    return .{
        .id = vopr.id.derive("storage.lmdb.crash-fault", vopr.id.stable("fault", "storage.lmdb.crash"), @intFromEnum(phase)),
        .name = "storage.lmdb.crash_publish_phase",
        .kind = .storage,
        .lifecycle = .one_shot,
        .resource_id = resource,
    };
}

fn valueFor(step: u16, key: u8) u16 {
    return (step *% 97 + key *% 17 + 1) % 10_000;
}

fn actionDigest(action: ScheduledAction) u64 {
    var hasher = std.hash.Wyhash.init(0xA17F_1DB0);
    std.hash.autoHash(&hasher, action);
    return hasher.final();
}

fn isOracleMismatch(err: anyerror) bool {
    return err == error.TestExpectedEqual or err == error.TestExpectedEqualStrings;
}

fn runRecordReplay(comptime budget: u64, seed: u64) !void {
    const LmdbScenario = Scenario(budget);
    var seeded = vopr.choice.Seeded.init(seed);
    var artifact = try vopr.runner.run(LmdbScenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = budget + 1,
    });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try vopr.replay.exact(LmdbScenario, std.testing.allocator, &artifact);
    replayed.deinit();
}

test "LMDB VOPR differentially replays C and Zig backends" {
    try runRecordReplay(8, 0xA17F_1DB1);
    try runRecordReplay(8, 0xA17F_1DB2);
}

test "LMDB VOPR replays every modeled crash publication phase" {
    const LmdbScenario = Scenario(4);
    const Sequence = struct {
        phase: CommitPhase,
        step: usize = 0,

        fn source(self: *@This()) vopr.choice.Source {
            return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
        }

        fn choose(ptr: *anyopaque, request: vopr.choice.Request) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const wanted = if (self.step == 0) directTransition(.put_main, 0).id else if (self.step == 1)
                vopr.id.derive("storage.lmdb.crash-transition", crash_base, @intFromEnum(self.phase))
            else
                finish_id;
            for (request.enabled) |candidate| if (candidate.id == wanted) {
                self.step += 1;
                return wanted;
            };
            return error.ExpectedLmdbTransitionNotEnabled;
        }

        fn finish(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.step != 3) return error.LmdbCrashSequenceIncomplete;
        }
    };

    for (std.enums.values(CommitPhase)) |phase| {
        var sequence = Sequence{ .phase = phase };
        var artifact = try vopr.runner.run(LmdbScenario, std.testing.allocator, sequence.source(), .{
            .system = "antfly",
            .seed = 0,
            .transition_budget = 3,
        });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(LmdbScenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
