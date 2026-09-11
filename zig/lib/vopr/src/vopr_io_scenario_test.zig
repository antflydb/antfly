// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! End-to-end replay proof for scheduler-visible std.Io fibers.

const std = @import("std");
const choice = @import("choice.zig");
const event = @import("event.zig");
const observation = @import("observation.zig");
const outcome = @import("outcome.zig");
const property = @import("property.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");
const vopr_io = @import("vopr_io.zig");
const transition = @import("transition.zig");

const Scenario = struct {
    pub const name: []const u8 = "vopr-io-fiber-replay";
    pub const version: u32 = 1;
    pub const properties = &[_]property.Declaration{};

    const Shared = struct {
        io: std.Io,
        completed: bool = false,
        sum: u32 = 0,

        fn child(self: *@This(), delay_ns: i64, value: u32) u32 {
            std.Io.sleep(self.io, .fromNanoseconds(delay_ns), .awake) catch unreachable;
            self.sum += value;
            return value;
        }

        fn root(self: *@This()) void {
            var slow = self.io.async(child, .{ self, 7, 10 });
            var fast = self.io.async(child, .{ self, 3, 20 });
            std.debug.assert(slow.await(self.io) == 10);
            std.debug.assert(fast.await(self.io) == 20);
            self.completed = true;
        }
    };

    pub const World = struct {
        sim: *vopr_io.VoprIo,
        shared: *Shared,
    };

    pub fn init(allocator: std.mem.Allocator) !World {
        const sim = try allocator.create(vopr_io.VoprIo);
        errdefer allocator.destroy(sim);
        sim.* = try vopr_io.VoprIo.init(.{
            .required = .of(&.{ .task_scheduling, .sleep, .clock_read }),
            .seed = 0x51_4d_49_4f,
        });
        errdefer sim.deinit();
        const shared = try allocator.create(Shared);
        errdefer allocator.destroy(shared);
        shared.* = .{ .io = sim.io() };
        _ = shared.io.async(Shared.root, .{shared});
        return .{ .sim = sim, .shared = shared };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.sim.deinit();
        allocator.destroy(world.shared);
        allocator.destroy(world.sim);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
        try world.sim.scheduler().enumerateReady(list, allocator);
    }

    pub fn execute(world: *World, selected: transition.Transition, events: *event.Sink, allocator: std.mem.Allocator) !outcome.TransitionOutcome {
        try world.sim.scheduler().executeReady(selected.id, events, allocator);
        return .applied();
    }

    pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, "vopr-io.completed", @intFromBool(world.shared.completed));
        try builder.addNamed(allocator, "vopr-io.sum", world.shared.sum);
        try builder.addNamed(allocator, "vopr-io.active_tasks", @intCast(world.sim.tasks.activeTaskCount()));
        try builder.addNamed(
            allocator,
            "vopr-io.monotonic_ns",
            @intCast(std.Io.Clock.awake.now(world.shared.io).toNanoseconds()),
        );
    }

    pub fn evaluate(_: *World, _: *property.Sink, _: std.mem.Allocator) !void {}

    pub fn done(world: *World) bool {
        return world.shared.completed and world.sim.scheduler().quiescent();
    }
};

test "VoprIo fiber histories exact replay through the VOPR runner" {
    const backend_ids = vopr_io.artifactBackendIds();
    var seeded = choice.Seeded.init(0x51_4d_49_4f);
    var recorded = try runner.run(Scenario, std.testing.allocator, seeded.source(), .{
        .system = "vopr-test",
        .seed = 0x51_4d_49_4f,
        .transition_budget = 32,
        .backend_ids = &backend_ids,
        .source_revision = "vopr-io-task-kernel-v1",
        .target = @tagName(@import("builtin").target.cpu.arch),
        .optimize = @tagName(@import("builtin").mode),
    });
    defer recorded.deinit();
    try vopr_io.validateArtifactBackendIds(recorded.config.backend_ids);
    try std.testing.expect(recorded.choices.items.len >= 6);
    for (0..100) |_| {
        var replayed = try replay.exact(Scenario, std.testing.allocator, &recorded);
        replayed.deinit();
    }
}
