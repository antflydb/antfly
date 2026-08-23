// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Common-runner adapter for the real PersistentIndex modeled-device world.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const persistent = @import("persistent.zig");
const fixture = @import("persistent_sim_fixture.zig");

const segment_base = vopr.id.stable("transition", "storage.persistent.index_segment");
const reopen_id = vopr.id.stable("transition", "storage.persistent.reopen");
const crash_id = vopr.id.stable("transition", "storage.persistent.crash_and_recover");

const model_id = vopr.id.stable("property", "storage.persistent.visible_state_matches_action_model");
const durability_id = vopr.id.stable("property", "storage.persistent.acknowledged_segments_survive_crash");
const recovered_id = vopr.id.stable("property", "storage.persistent.modeled_recovery_reached");

pub fn Scenario(comptime action_budget: u64) type {
    return struct {
        pub const name: []const u8 = "modeled-persistent-index";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = model_id, .name = "storage.persistent.visible_state_matches_action_model", .kind = .always },
            .{ .id = durability_id, .name = "storage.persistent.acknowledged_segments_survive_crash", .kind = .always },
            .{ .id = recovered_id, .name = "storage.persistent.modeled_recovery_reached", .kind = .reachable },
        };

        pub const World = struct {
            harness: *persistent.VoprHarness,
            decisions: u64 = 0,
        };

        pub fn init(allocator: std.mem.Allocator) !World {
            return .{ .harness = try persistent.VoprHarness.init(allocator) };
        }

        pub fn deinit(world: *World, _: std.mem.Allocator) void {
            world.harness.deinit();
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
            if (world.decisions >= action_budget) {
                try list.append(allocator, .{ .id = crash_id, .name = "storage.persistent.crash_and_recover", .kind = .fault });
                return;
            }
            inline for (std.meta.tags(fixture.SegmentSpec)) |spec| {
                try list.append(allocator, .{
                    .id = segmentId(spec),
                    .name = "storage.persistent.index_segment",
                    .kind = .workload,
                    .parameter = @intFromEnum(spec),
                });
            }
            try list.append(allocator, .{ .id = reopen_id, .name = "storage.persistent.reopen", .kind = .maintenance });
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: std.mem.Allocator,
        ) !vopr.outcome.TransitionOutcome {
            if (selected.id == crash_id) {
                try world.harness.crashAndRecover();
                try events.emitNamed(allocator, .state_change, "storage.persistent.recovered", world.decisions);
                return vopr.outcome.TransitionOutcome.targetReached("storage.persistent.recovered", world.decisions);
            }
            world.decisions += 1;
            if (selected.id == reopen_id) {
                try world.harness.apply(.reopen);
                try events.emitNamed(allocator, .state_change, "storage.persistent.reopened", world.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            const spec: fixture.SegmentSpec = @enumFromInt(@as(u2, @intCast(selected.parameter)));
            if (selected.id != segmentId(spec)) return error.UnknownPersistentVoprTransition;
            try world.harness.apply(.{ .index_segment = spec });
            try events.emitNamed(allocator, .client_response, "storage.persistent.segment_acknowledged", @intCast(selected.parameter));
            return vopr.outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            const summary = try world.harness.summary();
            try builder.addNamed(allocator, "storage.persistent.decisions", @intCast(world.decisions));
            try builder.addNamed(allocator, "storage.persistent.doc_count", summary.doc_count);
            try builder.addNamed(allocator, "storage.persistent.segment_count", @intCast(summary.segment_count));
            try builder.addNamed(allocator, "storage.persistent.alpha_hits", @intCast(summary.alpha_hits));
            try builder.addNamed(allocator, "storage.persistent.beta_hits", @intCast(summary.beta_hits));
            try builder.addNamed(allocator, "storage.persistent.gamma_hits", @intCast(summary.gamma_hits));
            try builder.addNamed(allocator, "storage.persistent.recovered", @intFromBool(world.harness.recovered));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            const matches = std.meta.eql(world.harness.expected(), try world.harness.summary());
            try sink.check(allocator, model_id, matches);
            try sink.check(allocator, durability_id, !world.harness.recovered or matches);
            try sink.check(allocator, recovered_id, world.harness.recovered);
        }

        pub fn done(world: *World) bool {
            return world.harness.recovered;
        }
    };
}

pub const CliScenario = Scenario(16);

pub fn record(allocator: std.mem.Allocator, seed: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    return vopr.runner.run(CliScenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = 17,
        .source_revision = "persistent-vopr-cli",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
}

pub fn replay(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(CliScenario, allocator, artifact);
}

fn segmentId(spec: fixture.SegmentSpec) u64 {
    return vopr.id.derive("storage.persistent.segment", segment_base, @intFromEnum(spec));
}

fn runRecordReplay(seed: u64) !void {
    var artifact = try record(std.testing.allocator, seed);
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try replay(std.testing.allocator, &artifact);
    replayed.deinit();
}

test "persistent VOPR runs real modeled index actions and exactly replays recovery" {
    try runRecordReplay(0xA17F_B601);
    try runRecordReplay(0xA17F_B602);
}

test "persistent VOPR preserves exact observations across fresh worlds" {
    var artifact = try record(std.testing.allocator, 0xA17F_B603);
    defer artifact.deinit();
    for (0..5) |_| {
        var replayed = try replay(std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
