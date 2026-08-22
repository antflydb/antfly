// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const observation = @import("observation.zig");
const property = @import("property.zig");
const transition = @import("transition.zig");

pub const ToyScenario = struct {
    pub const name: []const u8 = "phase0-toy-counter";
    pub const version: u32 = 1;
    pub const bounded_id = ids.stable("property", "toy.counter_bounded");
    pub const positive_id = ids.stable("property", "toy.counter_positive_reached");
    pub const increment_id = ids.stable("transition", "toy.increment");
    pub const decrement_id = ids.stable("transition", "toy.decrement");
    pub const properties = &[_]property.Declaration{
        .{ .id = bounded_id, .name = "toy.counter_bounded", .kind = .always },
        .{ .id = positive_id, .name = "toy.counter_positive_reached", .kind = .reachable },
    };

    pub const World = struct { counter: i64 = 0, steps: u8 = 0 };

    pub fn init(_: std.mem.Allocator) !World {
        return .{};
    }

    pub fn deinit(_: *World, _: std.mem.Allocator) void {}

    pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
        try list.append(allocator, .{ .id = increment_id, .name = "toy.increment", .kind = .workload, .parameter = 1 });
        if (world.counter > 0) try list.append(allocator, .{ .id = decrement_id, .name = "toy.decrement", .kind = .workload, .parameter = -1 });
    }

    pub fn execute(world: *World, selected: transition.Transition, events: *event.Sink, allocator: std.mem.Allocator) !void {
        if (selected.id != increment_id and selected.id != decrement_id) return error.UnknownToyTransition;
        world.counter += selected.parameter;
        world.steps += 1;
        try events.emitNamed(allocator, .state_change, "toy.counter_changed", @bitCast(world.counter));
    }

    pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
        // Add in reverse lexical order to prove canonicalization is owned by the engine.
        try builder.addNamed(allocator, "toy.steps", world.steps);
        try builder.addNamed(allocator, "toy.counter", world.counter);
    }

    pub fn evaluate(world: *World, sink: *property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, bounded_id, world.counter >= -4 and world.counter <= 4);
        try sink.check(allocator, positive_id, world.counter > 0);
    }

    pub fn done(world: *World) bool {
        return world.steps == 4;
    }
};
