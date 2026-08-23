// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Common-runner adapter for source/destination IndexManager split routing.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const manager = @import("db/catalog/index_manager.zig");
const fixture = @import("db/catalog/index_manager_sim_fixture.zig");

const add_base = vopr.id.stable("transition", "storage.index_manager.add_doc");
const reopen_id = vopr.id.stable("transition", "storage.index_manager.reopen");
const split_id = vopr.id.stable("transition", "storage.index_manager.split_handoff");
const crash_id = vopr.id.stable("transition", "storage.index_manager.crash_and_recover");

const model_id = vopr.id.stable("property", "storage.index_manager.projections_match_routing_model");
const durability_id = vopr.id.stable("property", "storage.index_manager.acknowledged_projections_survive_crash");
const recovered_id = vopr.id.stable("property", "storage.index_manager.modeled_recovery_reached");

pub fn Scenario(comptime action_budget: u64) type {
    return struct {
        pub const name: []const u8 = "modeled-index-manager";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = model_id, .name = "storage.index_manager.projections_match_routing_model", .kind = .always },
            .{ .id = durability_id, .name = "storage.index_manager.acknowledged_projections_survive_crash", .kind = .always },
            .{ .id = recovered_id, .name = "storage.index_manager.modeled_recovery_reached", .kind = .reachable },
        };

        pub const World = struct {
            harness: *manager.VoprHarness,
            decisions: u64 = 0,
        };

        pub fn init(allocator: std.mem.Allocator) !World {
            return .{ .harness = try manager.VoprHarness.init(allocator) };
        }

        pub fn deinit(world: *World, _: std.mem.Allocator) void {
            world.harness.deinit();
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
            if (world.decisions >= action_budget) {
                try list.append(allocator, .{ .id = crash_id, .name = "storage.index_manager.crash_and_recover", .kind = .fault });
                return;
            }
            inline for (std.meta.tags(fixture.DocSpec)) |spec| {
                try list.append(allocator, .{
                    .id = addId(spec),
                    .name = "storage.index_manager.add_doc",
                    .kind = .workload,
                    .parameter = @intFromEnum(spec),
                });
            }
            try list.append(allocator, .{ .id = reopen_id, .name = "storage.index_manager.reopen", .kind = .maintenance });
            if (!world.harness.splitActive())
                try list.append(allocator, .{ .id = split_id, .name = "storage.index_manager.split_handoff", .kind = .maintenance });
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: std.mem.Allocator,
        ) !vopr.outcome.TransitionOutcome {
            if (selected.id == crash_id) {
                try world.harness.crashAndRecover();
                try events.emitNamed(allocator, .state_change, "storage.index_manager.recovered", world.decisions);
                return vopr.outcome.TransitionOutcome.targetReached("storage.index_manager.recovered", world.decisions);
            }
            world.decisions += 1;
            if (selected.id == reopen_id) {
                try world.harness.apply(.reopen);
                try events.emitNamed(allocator, .state_change, "storage.index_manager.reopened", world.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == split_id) {
                try world.harness.apply(.split_handoff);
                try events.emitNamed(allocator, .state_change, "storage.index_manager.split_handoff_complete", world.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            const spec: fixture.DocSpec = @enumFromInt(@as(u2, @intCast(selected.parameter)));
            if (selected.id != addId(spec)) return error.UnknownIndexManagerVoprTransition;
            try world.harness.apply(.{ .add_doc = spec });
            try events.emitNamed(allocator, .client_response, "storage.index_manager.document_acknowledged", @intCast(selected.parameter));
            return vopr.outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            const summary = try world.harness.summary();
            try builder.addNamed(allocator, "storage.index_manager.decisions", @intCast(world.decisions));
            try builder.addNamed(allocator, "storage.index_manager.source_docs", summary.source_doc_count);
            try builder.addNamed(allocator, "storage.index_manager.dest_docs", summary.dest_doc_count);
            try builder.addNamed(allocator, "storage.index_manager.source_alpha", summary.source_alpha_hits);
            try builder.addNamed(allocator, "storage.index_manager.source_beta", summary.source_beta_hits);
            try builder.addNamed(allocator, "storage.index_manager.source_gamma", summary.source_gamma_hits);
            try builder.addNamed(allocator, "storage.index_manager.dest_alpha", summary.dest_alpha_hits);
            try builder.addNamed(allocator, "storage.index_manager.dest_beta", summary.dest_beta_hits);
            try builder.addNamed(allocator, "storage.index_manager.dest_gamma", summary.dest_gamma_hits);
            try builder.addNamed(allocator, "storage.index_manager.split_active", @intFromBool(world.harness.splitActive()));
            try builder.addNamed(allocator, "storage.index_manager.recovered", @intFromBool(world.harness.recovered));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            const matches = std.meta.eql(try world.harness.expected(), try world.harness.summary());
            try sink.check(allocator, model_id, matches);
            try sink.check(allocator, durability_id, !world.harness.recovered or matches);
            try sink.check(allocator, recovered_id, world.harness.recovered);
        }

        pub fn done(world: *World) bool {
            return world.harness.recovered;
        }
    };
}

pub const CliScenario = Scenario(10);

pub fn record(allocator: std.mem.Allocator, seed: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    return vopr.runner.run(CliScenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = 11,
        .source_revision = "index-manager-vopr-cli",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
}

pub fn replay(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(CliScenario, allocator, artifact);
}

fn addId(spec: fixture.DocSpec) u64 {
    return vopr.id.derive("storage.index_manager.doc", add_base, @intFromEnum(spec));
}

fn runRecordReplay(seed: u64) !void {
    var artifact = try record(std.testing.allocator, seed);
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try replay(std.testing.allocator, &artifact);
    replayed.deinit();
}

test "index manager VOPR runs split routing reopen and modeled recovery through the common runner" {
    try runRecordReplay(0xA17F_D601);
    try runRecordReplay(0xA17F_D602);
}

test "index manager VOPR repeats exact clean-world replay" {
    var artifact = try record(std.testing.allocator, 0xA17F_D603);
    defer artifact.deinit();
    for (0..5) |_| {
        var replayed = try replay(std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
