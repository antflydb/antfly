// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Embedded and native Lite ownership histories on caller-owned `std.Io`.

const std = @import("std");
const vopr = @import("vopr");
const embedded = @import("embedded_db_surface");
const embedded_support = @import("embedded_support");

const EmbeddedRuntime = embedded_support.background_runtime.BackendRuntime;
const NativeFile = embedded_support.lite.native.NativeFile;

pub const Scenario = struct {
    pub const name: []const u8 = "embedded-lite-lifecycle";
    pub const version: u32 = 1;

    const sound_id = vopr.id.stable(name, "ownership-and-durability-sound");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".ownership-and-durability-sound", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum {
        native_crash_reopen,
        embedded_overlapping_open_close,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "native-crash-reopen"),
        vopr.id.stable(name, "embedded-overlapping-open-close"),
    };
    const mode_names = [_][]const u8{
        name ++ ".native-crash-reopen",
        name ++ ".embedded-overlapping-open-close",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,

        fn runNativeCrashReopen(self: *@This()) !void {
            var file = try NativeFile.createWithIo(self.allocator, self.sim.io(), "native.aflite", .{ .exclusive = true });
            try std.testing.expect(file.usesBorrowedIo());
            try file.putDocument("doc:native", "durable");
            file.close();
            try self.sim.crashFileSystem();

            var reopened = try NativeFile.openWithIo(self.allocator, self.sim.io(), "native.aflite", .{ .read_only = true });
            defer reopened.close();
            const value = (try reopened.getDocumentAlloc(self.allocator, "doc:native")) orelse {
                self.sound = false;
                return;
            };
            defer self.allocator.free(value);
            self.sound = std.mem.eql(u8, value, "durable") and reopened.usesBorrowedIo();
            self.progress += 3;
        }

        fn runEmbeddedOpenClose(self: *@This()) !void {
            var runtime = try EmbeddedRuntime.init(self.allocator, .{
                .backend = .manual,
                .borrowed_io = .{ .general = self.sim.io() },
            });
            defer runtime.deinit();
            const writer_opts = embedded.OpenOptions{
                .backend_runtime = &runtime,
                .lite_io = self.sim.io(),
            };
            var writer = try embedded.DB.createLiteHosted(self.allocator, "embedded.aflite", writer_opts);
            var writer_live = true;
            defer if (writer_live) writer.close();
            try writer.batch(.{
                .writes = &.{.{ .key = "doc:embedded", .value = "{\"title\":\"pinned\"}" }},
                .sync_level = .write,
            });

            var reader = try embedded.DB.openLiteHosted(self.allocator, "embedded.aflite", .{
                .open_mode = .query_readonly,
                .backend_runtime = &runtime,
                .lite_io = self.sim.io(),
            });
            writer.close();
            writer_live = false;

            var held = (try reader.lookup(self.allocator, "doc:embedded", .{})) orelse {
                reader.close();
                self.sound = false;
                return;
            };
            held.deinit(self.allocator);
            reader.close();
            try self.sim.crashFileSystem();

            var reopened = try embedded.DB.openLiteHosted(self.allocator, "embedded.aflite", .{
                .open_mode = .query_readonly,
                .backend_runtime = &runtime,
                .lite_io = self.sim.io(),
            });
            defer reopened.close();
            var durable = (try reopened.lookup(self.allocator, "doc:embedded", .{})) orelse {
                self.sound = false;
                return;
            };
            defer durable.deinit(self.allocator);
            self.sound = std.mem.indexOf(u8, durable.json, "pinned") != null;
            self.progress += 5;
        }

        fn run(self: *@This(), mode: Mode) !void {
            switch (mode) {
                .native_crash_reopen => try self.runNativeCrashReopen(),
                .embedded_overlapping_open_close => try self.runEmbeddedOpenClose(),
            }
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{
            .allocator = allocator,
            .sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0xe8be_dded,
                .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy, .clock_read }),
            }),
        };
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| {
            try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = if (mode == .native_crash_reopen) .fault else .workload,
            });
        }
    }

    pub fn execute(
        world: *World,
        selected: vopr.transition.Transition,
        events: *vopr.event.Sink,
        allocator: std.mem.Allocator,
    ) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            try world.state.run(mode);
            found = true;
        };
        if (!found) return error.InvalidEmbeddedLiteLifecycleTransition;
        try events.emitNamed(allocator, .domain, selected.name, world.state.progress);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".progress", @intCast(world.state.progress));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, sound_id, world.state.sound);
        try sink.check(allocator, complete_id, world.state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        return .{
            .progress_expected = true,
            .progress_units = world.state.progress,
            .active_tasks = world.state.sim.tasks.activeTaskCount(),
            .cleanup_complete = world.state.complete,
        };
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "embedded and Lite lifecycle exact replay" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var artifact = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
        });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}

fn physicalDigest(io: std.Io, path: []const u8) !u64 {
    var file = try NativeFile.createWithIo(std.testing.allocator, io, path, .{ .exclusive = true });
    defer file.close();
    try file.putDocument("doc:differential", "same-logical-value");
    const value = (try file.getDocumentAlloc(std.testing.allocator, "doc:differential")) orelse
        return error.MissingDifferentialValue;
    defer std.testing.allocator.free(value);
    var hash = std.hash.Wyhash.init(0);
    hash.update(value);
    hash.update(std.mem.asBytes(&file.activeCheckpoint().commit_sequence));
    return hash.final();
}

test "Lite native and VoprIo produce the same logical checkpoint" {
    // vopr-audit: allow(host_filesystem) this half is the explicit physical-backend differential oracle
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const physical_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/physical.aflite", .{tmp.sub_path});
    defer std.testing.allocator.free(physical_path);
    // vopr-audit: allow(host_filesystem) std.testing.io is confined to the physical differential half
    const physical = try physicalDigest(std.testing.io, physical_path);

    var sim = try vopr.vopr_io.VoprIo.init(.{
        .seed = 0xd1ff_e2,
        .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy, .clock_read }),
    });
    defer sim.deinit();
    const modeled = try physicalDigest(sim.io(), "modeled.aflite");
    try std.testing.expectEqual(physical, modeled);
}
