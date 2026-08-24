// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deterministic activation, nesting, and cleanup of the production media registries.

const std = @import("std");
const vopr = @import("vopr");
const audio_runtime = @import("../common/audio_runtime.zig");
const config_mod = @import("../common/config.zig");
const provider_registry = @import("../common/provider_registry.zig");
const transcribing = @import("antfly_transcribing");
const readers = @import("antfly_readers");
const synthesizing = @import("antfly_synthesizing");

pub const Scenario = struct {
    pub const name = "media-runtime";
    pub const version: u32 = 1;
    const restored_id = vopr.id.stable(name, "globals-restored");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = restored_id, .name = name ++ ".globals-restored", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };
    const Mode = enum { empty, configured, nested };
    const mode_ids = [_]vopr.id.StableId{ vopr.id.stable(name, "empty"), vopr.id.stable(name, "configured"), vopr.id.stable(name, "nested") };
    const mode_names = [_][]const u8{ name ++ ".empty", name ++ ".configured", name ++ ".nested" };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        cfg: config_mod.Config,
        original_stt: ?*const transcribing.Runtime,
        original_tts: ?*const synthesizing.Runtime,
        sound: bool = true,
        complete: bool = false,

        fn run(self: *@This(), mode: Mode) !void {
            switch (mode) {
                .empty => {
                    var active = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), null);
                    active.deinit();
                },
                .configured => {
                    var active = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), &self.cfg);
                    self.sound = transcribing.getActiveRuntime() != null and synthesizing.getActiveRuntime() != null;
                    active.deinit();
                },
                .nested => {
                    var outer = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), &self.cfg);
                    const outer_stt = transcribing.getActiveRuntime();
                    var inner = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), &self.cfg);
                    self.sound = transcribing.getActiveRuntime() != outer_stt;
                    inner.deinit();
                    self.sound = self.sound and transcribing.getActiveRuntime() == outer_stt;
                    outer.deinit();
                },
            }
            self.sound = self.sound and transcribing.getActiveRuntime() == self.original_stt and synthesizing.getActiveRuntime() == self.original_tts;
            self.complete = true;
        }
    };
    pub const World = struct { state: *State };
    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        var cfg = config_mod.Config{
            .registry = provider_registry.Registry.init(allocator),
            .transcribers = transcribing.Registry.init(allocator),
            .readers = readers.Registry.init(allocator),
            .text_to_speech = synthesizing.Registry.init(allocator),
        };
        errdefer cfg.deinit();
        var stt = transcribing.Config{ .provider = .antfly, .api_url = try allocator.dupe(u8, "http://vopr.invalid"), .model = try allocator.dupe(u8, "vopr-stt") };
        defer transcribing.deinitConfig(allocator, &stt);
        try cfg.transcribers.registerConfig("vopr-stt", stt);
        var tts = synthesizing.Config{ .provider = .openai, .api_key = try allocator.dupe(u8, "vopr-key"), .model = try allocator.dupe(u8, "vopr-tts"), .voice = try allocator.dupe(u8, "alloy") };
        defer synthesizing.deinitConfig(allocator, &tts);
        try cfg.text_to_speech.registerConfig("vopr-tts", tts);
        state.* = .{
            .allocator = allocator,
            .sim = try vopr.vopr_io.VoprIo.init(.{ .seed = 0x4d45_4449_41, .required = .of(&.{ .clock_read, .sockets }) }),
            .cfg = cfg,
            .original_stt = transcribing.getActiveRuntime(),
            .original_tts = synthesizing.getActiveRuntime(),
        };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.cfg.deinit();
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{ .id = id, .name = mode_name, .kind = if (mode == .nested) .fault else .workload });
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            try world.state.run(mode);
            found = true;
        };
        if (!found) return error.InvalidMediaRuntimeTransition;
        try events.emitNamed(allocator, .domain, selected.name, 1);
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
        try builder.addNamed(allocator, name ++ ".sound", @intFromBool(world.state.sound));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, restored_id, world.state.sound);
        try sink.check(allocator, complete_id, world.state.complete);
    }
    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        return .{ .progress_expected = true, .progress_units = @intFromBool(world.state.complete), .active_tasks = world.state.sim.tasks.activeTaskCount(), .cleanup_complete = world.state.complete };
    }
    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "media runtime VOPR exact replays activation nesting and cleanup" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var artifact = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{ .system = "antfly", .transition_budget = 1, .backend_ids = &backend_ids });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
