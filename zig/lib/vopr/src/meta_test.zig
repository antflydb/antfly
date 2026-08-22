// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deliberately faulty finite state machines used to prove the simulator can
//! discover, replay, and reduce the interaction shapes required by VOPR.md.

const std = @import("std");
const choice = @import("choice.zig");
const event = @import("event.zig");
const ids = @import("id.zig");
const observation = @import("observation.zig");
const property = @import("property.zig");
const reducer = @import("reducer.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");
const trace = @import("trace.zig");
const transition = @import("transition.zig");

pub const Bug = enum {
    message_ordering,
    fault_workload_overlap,
    crash_before_sync,
    clock_jump_retry,
    two_client_interleaving,
};

fn bugPrefix(comptime bug: Bug) []const u8 {
    return switch (bug) {
        .message_ordering => "meta.message_ordering",
        .fault_workload_overlap => "meta.fault_workload_overlap",
        .crash_before_sync => "meta.crash_before_sync",
        .clock_jump_retry => "meta.clock_jump_retry",
        .two_client_interleaving => "meta.two_client_interleaving",
    };
}

pub fn Scenario(comptime bug: Bug) type {
    return struct {
        pub const name: []const u8 = bugPrefix(bug);
        pub const version: u32 = 1;
        const property_name = name ++ ".safety";
        const property_id = ids.stable("property", property_name);
        const first_a_id = ids.stable("transition", name ++ ".first.a");
        const first_b_id = ids.stable("transition", name ++ ".first.b");
        const second_a_id = ids.stable("transition", name ++ ".second.a");
        const second_b_id = ids.stable("transition", name ++ ".second.b");
        const noise_id = ids.stable("transition", name ++ ".diagnostic_noise");
        const finish_id = ids.stable("transition", name ++ ".finish");
        pub const properties = &[_]property.Declaration{.{
            .id = property_id,
            .name = property_name,
            .kind = .always,
        }};

        pub const World = struct {
            stage: u8 = 0,
            first_a: bool = false,
            second_a: bool = false,
            triggered: bool = false,
            noise: u8 = 0,
            complete: bool = false,
        };

        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }

        pub fn deinit(_: *World, _: std.mem.Allocator) void {}

        pub fn enumerate(world: *World, list: *transition.List, alloc: std.mem.Allocator) !void {
            switch (world.stage) {
                0 => {
                    try list.append(alloc, .{ .id = first_a_id, .name = name ++ ".first.a", .kind = firstKind() });
                    try list.append(alloc, .{ .id = first_b_id, .name = name ++ ".first.b", .kind = firstKind() });
                },
                1 => {
                    try list.append(alloc, .{ .id = second_a_id, .name = name ++ ".second.a", .kind = secondKind() });
                    try list.append(alloc, .{ .id = second_b_id, .name = name ++ ".second.b", .kind = secondKind() });
                },
                2 => {
                    try list.append(alloc, .{ .id = finish_id, .name = name ++ ".finish", .kind = .workload });
                    if (world.noise < 2) try list.append(alloc, .{ .id = noise_id, .name = name ++ ".diagnostic_noise", .kind = .scheduler });
                },
                else => {},
            }
        }

        pub fn execute(world: *World, selected: transition.Transition, events: *event.Sink, alloc: std.mem.Allocator) !void {
            switch (world.stage) {
                0 => {
                    world.first_a = selected.id == first_a_id;
                    if (!world.first_a and selected.id != first_b_id) return error.InvalidMetaFirstChoice;
                    world.stage = 1;
                },
                1 => {
                    world.second_a = selected.id == second_a_id;
                    if (!world.second_a and selected.id != second_b_id) return error.InvalidMetaSecondChoice;
                    world.triggered = targetCombination(world.first_a, world.second_a);
                    world.stage = 2;
                },
                2 => {
                    if (selected.id == noise_id) {
                        world.noise += 1;
                    } else if (selected.id == finish_id) {
                        world.complete = true;
                        world.stage = 3;
                    } else return error.InvalidMetaTerminalChoice;
                },
                else => return error.MetaScenarioAlreadyComplete,
            }
            try events.emitNamed(alloc, if (world.triggered) .injected_error else .state_change, name ++ ".step", selected.id);
        }

        pub fn observe(world: *World, builder: *observation.Builder, alloc: std.mem.Allocator) !void {
            try builder.addNamed(alloc, name ++ ".stage", world.stage);
            try builder.addNamed(alloc, name ++ ".first_a", @intFromBool(world.first_a));
            try builder.addNamed(alloc, name ++ ".second_a", @intFromBool(world.second_a));
            try builder.addNamed(alloc, name ++ ".triggered", @intFromBool(world.triggered));
            try builder.addNamed(alloc, name ++ ".noise", world.noise);
        }

        pub fn evaluate(world: *World, sink: *property.Sink, alloc: std.mem.Allocator) !void {
            try sink.check(alloc, property_id, !world.triggered);
        }

        pub fn done(world: *World) bool {
            return world.complete;
        }

        fn firstKind() transition.Kind {
            return switch (bug) {
                .fault_workload_overlap, .crash_before_sync => .fault,
                .clock_jump_retry => .scheduler,
                .message_ordering, .two_client_interleaving => .workload,
            };
        }

        fn secondKind() transition.Kind {
            return switch (bug) {
                .fault_workload_overlap, .crash_before_sync, .clock_jump_retry => .fault,
                .message_ordering, .two_client_interleaving => .workload,
            };
        }

        fn targetCombination(first_a: bool, second_a: bool) bool {
            return switch (bug) {
                // m2 before m1; client 2 before client 1.
                .message_ordering, .two_client_interleaving => !first_a and second_a,
                // fault A and B overlap the operation.
                .fault_workload_overlap => first_a and second_a,
                // write/schedule followed by crash/jump (the B alternative).
                .crash_before_sync, .clock_jump_retry => first_a and !second_a,
            };
        }
    };
}

fn discover(comptime bug: Bug, alloc: std.mem.Allocator) !trace.Trace {
    const S = Scenario(bug);
    var enumerating = choice.Enumerating.init(alloc);
    defer enumerating.deinit();
    while (true) {
        try enumerating.beginHistory();
        var artifact = try runner.run(S, alloc, enumerating.source(), .{
            .system = "vopr-meta-test",
            .transition_budget = 5,
        });
        if (artifact.failures.items.len > 0) return artifact;
        artifact.deinit();
        if (!try enumerating.advance()) return error.MetaBugNotDiscovered;
    }
}

fn targetScript(comptime bug: Bug) [5]ids.StableId {
    const S = Scenario(bug);
    return switch (bug) {
        .message_ordering, .two_client_interleaving => .{ S.first_b_id, S.second_a_id, S.noise_id, S.noise_id, S.finish_id },
        .fault_workload_overlap => .{ S.first_a_id, S.second_a_id, S.noise_id, S.noise_id, S.finish_id },
        .crash_before_sync, .clock_jump_retry => .{ S.first_a_id, S.second_b_id, S.noise_id, S.noise_id, S.finish_id },
    };
}

fn proveMetaBug(comptime bug: Bug) !void {
    const alloc = std.testing.allocator;
    const S = Scenario(bug);
    var discovered = try discover(bug, alloc);
    defer discovered.deinit();
    try std.testing.expectEqual(@as(usize, 1), discovered.failures.items.len);
    var discovered_replay = try replay.exact(S, alloc, &discovered);
    discovered_replay.deinit();

    const selections = targetScript(bug);
    var scripted = choice.Scripted{ .selections = &selections };
    var verbose = try runner.run(S, alloc, scripted.source(), .{
        .system = "vopr-meta-test",
        .transition_budget = selections.len,
    });
    defer verbose.deinit();
    const fingerprint = verbose.failures.items[0].fingerprint;
    var reduced = try reducer.reduce(S, alloc, &verbose, fingerprint, .{ .max_attempts = 64 });
    defer reduced.deinit();
    try std.testing.expect(reduced.report.reduced_transitions < reduced.report.original_transitions);
    try std.testing.expectEqual(fingerprint, reduced.artifact.failures.items[0].fingerprint);
    var reduced_replay = try replay.exact(S, alloc, &reduced.artifact);
    reduced_replay.deinit();
}

test "systematic meta-suite discovers replays and reduces required bug shapes" {
    inline for (std.meta.tags(Bug)) |bug| try proveMetaBug(bug);
}
