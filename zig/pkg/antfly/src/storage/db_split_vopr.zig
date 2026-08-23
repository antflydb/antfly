// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Common-runner adapter for the real two-DB full split modeled world.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const db = @import("db/db.zig");
const fixture = @import("db/db_split_sim_fixture.zig");

const add_base = vopr.id.stable("transition", "storage.db_split.add_doc");
const reopen_source_id = vopr.id.stable("transition", "storage.db_split.reopen_source");
const reopen_dest_id = vopr.id.stable("transition", "storage.db_split.reopen_dest");
const split_id = vopr.id.stable("transition", "storage.db_split.split_full");
const crash_id = vopr.id.stable("transition", "storage.db_split.crash_and_recover");

const model_id = vopr.id.stable("property", "storage.db_split.source_and_dest_match_routing_model");
const topology_id = vopr.id.stable("property", "storage.db_split.full_split_completed");
const durability_id = vopr.id.stable("property", "storage.db_split.acknowledged_documents_survive_crash");
const recovered_id = vopr.id.stable("property", "storage.db_split.modeled_recovery_reached");

pub fn Scenario(comptime action_budget: u64) type {
    return struct {
        pub const name: []const u8 = "modeled-db-split";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = model_id, .name = "storage.db_split.source_and_dest_match_routing_model", .kind = .always },
            .{ .id = topology_id, .name = "storage.db_split.full_split_completed", .kind = .reachable },
            .{ .id = durability_id, .name = "storage.db_split.acknowledged_documents_survive_crash", .kind = .always },
            .{ .id = recovered_id, .name = "storage.db_split.modeled_recovery_reached", .kind = .reachable },
        };

        pub const World = struct {
            harness: *db.VoprSplitHarness,
            decisions: u64 = 0,
        };

        pub fn init(allocator: std.mem.Allocator) !World {
            return .{ .harness = try db.VoprSplitHarness.init(allocator) };
        }

        pub fn deinit(world: *World, _: std.mem.Allocator) void {
            world.harness.deinit();
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
            if (world.decisions >= action_budget) {
                if (!world.harness.splitComplete())
                    try list.append(allocator, .{ .id = split_id, .name = "storage.db_split.split_full", .kind = .maintenance })
                else
                    try list.append(allocator, .{ .id = crash_id, .name = "storage.db_split.crash_and_recover", .kind = .fault });
                return;
            }
            inline for (std.meta.tags(fixture.DocSpec)) |spec| {
                try list.append(allocator, .{
                    .id = addId(spec),
                    .name = "storage.db_split.add_doc",
                    .kind = .workload,
                    .parameter = @intFromEnum(spec),
                });
            }
            try list.append(allocator, .{ .id = reopen_source_id, .name = "storage.db_split.reopen_source", .kind = .maintenance });
            if (world.harness.splitComplete()) {
                try list.append(allocator, .{ .id = reopen_dest_id, .name = "storage.db_split.reopen_dest", .kind = .maintenance });
            } else {
                try list.append(allocator, .{ .id = split_id, .name = "storage.db_split.split_full", .kind = .maintenance });
            }
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: std.mem.Allocator,
        ) !vopr.outcome.TransitionOutcome {
            if (selected.id == crash_id) {
                try world.harness.crashAndRecover();
                try events.emitNamed(allocator, .state_change, "storage.db_split.recovered", world.decisions);
                return vopr.outcome.TransitionOutcome.targetReached("storage.db_split.recovered", world.decisions);
            }
            world.decisions += 1;
            if (selected.id == reopen_source_id) {
                try world.harness.apply(.reopen_source);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == reopen_dest_id) {
                try world.harness.apply(.reopen_dest);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == split_id) {
                try world.harness.apply(.split_full);
                try events.emitNamed(allocator, .state_change, "storage.db_split.full_split_complete", world.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            const spec: fixture.DocSpec = @enumFromInt(@as(u2, @intCast(selected.parameter)));
            if (selected.id != addId(spec)) return error.UnknownDbSplitVoprTransition;
            try world.harness.apply(.{ .add_doc = spec });
            try events.emitNamed(allocator, .client_response, "storage.db_split.document_acknowledged", @intCast(selected.parameter));
            return vopr.outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            const summary = try world.harness.summary();
            try builder.addNamed(allocator, "storage.db_split.decisions", @intCast(world.decisions));
            try builder.addNamed(allocator, "storage.db_split.source_docs", summary.source_doc_count);
            try builder.addNamed(allocator, "storage.db_split.dest_docs", summary.dest_doc_count);
            try builder.addNamed(allocator, "storage.db_split.source_alpha", summary.source_alpha_hits);
            try builder.addNamed(allocator, "storage.db_split.source_beta", summary.source_beta_hits);
            try builder.addNamed(allocator, "storage.db_split.source_gamma", summary.source_gamma_hits);
            try builder.addNamed(allocator, "storage.db_split.dest_alpha", summary.dest_alpha_hits);
            try builder.addNamed(allocator, "storage.db_split.dest_beta", summary.dest_beta_hits);
            try builder.addNamed(allocator, "storage.db_split.dest_gamma", summary.dest_gamma_hits);
            try builder.addNamed(allocator, "storage.db_split.split_complete", @intFromBool(world.harness.splitComplete()));
            try builder.addNamed(allocator, "storage.db_split.recovered", @intFromBool(world.harness.recovered));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            const matches = std.meta.eql(try world.harness.expected(), try world.harness.summary());
            try sink.check(allocator, model_id, matches);
            try sink.check(allocator, topology_id, world.harness.splitComplete());
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
        .transition_budget = 12,
        .source_revision = "db-split-vopr-cli",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
}

pub fn replay(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(CliScenario, allocator, artifact);
}

fn addId(spec: fixture.DocSpec) u64 {
    return vopr.id.derive("storage.db_split.doc", add_base, @intFromEnum(spec));
}

fn runRecordReplay(seed: u64) !void {
    var artifact = try record(std.testing.allocator, seed);
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try replay(std.testing.allocator, &artifact);
    replayed.deinit();
}

test "DB split VOPR runs real routing split reopen and modeled recovery" {
    try runRecordReplay(0xA17F_E601);
    try runRecordReplay(0xA17F_E602);
}

test "DB split VOPR repeats exact clean-world replay" {
    var artifact = try record(std.testing.allocator, 0xA17F_E603);
    defer artifact.deinit();
    for (0..5) |_| {
        var replayed = try replay(std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
