// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Startup format admission and replica provisioning on borrowed `VoprIo`.

const std = @import("std");
const vopr = @import("vopr");
const data_format = @import("../common/data_format.zig");
const fs_paths = @import("../common/fs_paths.zig");
const provisioner = @import("../metadata/table_provisioner.zig");
const background_runtime = @import("../storage/background_runtime.zig");

pub const Scenario = struct {
    pub const name: []const u8 = "provisioning-startup";
    pub const version: u32 = 1;
    const sound_id = vopr.id.stable(name, "startup-fails-closed-and-recovers");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".startup-fails-closed-and-recovers", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };
    const Mode = enum { fresh_provision, idempotent_restart, partial_marker, legacy_rejected, write_fault_retry, crash_restart };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "fresh-provision"),
        vopr.id.stable(name, "idempotent-restart"),
        vopr.id.stable(name, "partial-marker"),
        vopr.id.stable(name, "legacy-rejected"),
        vopr.id.stable(name, "write-fault-retry"),
        vopr.id.stable(name, "crash-restart"),
    };
    const mode_names = [_][]const u8{
        name ++ ".fresh-provision",
        name ++ ".idempotent-restart",
        name ++ ".partial-marker",
        name ++ ".legacy-rejected",
        name ++ ".write-fault-retry",
        name ++ ".crash-restart",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,

        fn admit(self: *@This()) !void {
            try data_format.ensureCompatible(self.allocator, self.sim.io(), "/startup");
            self.progress += 1;
        }

        fn provision(self: *@This()) !provisioner.ProvisionSummary {
            var runtime = try background_runtime.BackendRuntime.init(self.allocator, .{
                .backend = .manual,
                .borrowed_io = .{ .general = self.sim.io() },
            });
            defer runtime.deinit();
            const summary = try provisioner.reconcileReplicaRootWithOptions(
                self.allocator,
                "/startup/data/replicas",
                100,
                &.{ 100, 2001 },
                &.{.{ .table_id = 7, .name = "docs", .indexes_json = "{}" }},
                &.{.{ .group_id = 2001, .table_id = 7, .start_key = "", .end_key = null }},
                .{ .io = self.sim.io(), .backend_runtime = &runtime },
            );
            self.progress += 1;
            return summary;
        }

        fn run(self: *@This(), mode: Mode) !void {
            switch (mode) {
                .fresh_provision => {
                    try self.admit();
                    const summary = try self.provision();
                    self.sound = summary.groups_considered == 1 and summary.dbs_opened == 1;
                },
                .idempotent_restart => {
                    try self.admit();
                    const first = try self.provision();
                    try self.admit();
                    const second = try self.provision();
                    self.sound = first.dbs_opened == 1 and second.dbs_opened == 1 and second.indexes_added == 0;
                },
                .partial_marker => {
                    try fs_paths.createDirPathPortable(self.sim.io(), "/startup");
                    try std.Io.Dir.cwd().writeFile(self.sim.io(), .{ .sub_path = "/startup/" ++ data_format.marker_file_name, .data = "{" });
                    data_format.ensureCompatible(self.allocator, self.sim.io(), "/startup") catch {
                        try std.Io.Dir.cwd().deleteFile(self.sim.io(), "/startup/" ++ data_format.marker_file_name);
                        try self.admit();
                        self.complete = true;
                        return;
                    };
                    self.sound = false;
                },
                .legacy_rejected => {
                    try fs_paths.createDirPathPortable(self.sim.io(), "/startup/store");
                    data_format.ensureCompatible(self.allocator, self.sim.io(), "/startup") catch |err| {
                        self.sound = err == data_format.Error.IncompatibleAntflyDataDir;
                        self.progress += 1;
                        self.complete = true;
                        return;
                    };
                    self.sound = false;
                },
                .write_fault_retry => {
                    self.sim.failNextFileWrite();
                    data_format.ensureCompatible(self.allocator, self.sim.io(), "/startup") catch {
                        try self.admit();
                        self.complete = true;
                        return;
                    };
                    self.sound = false;
                },
                .crash_restart => {
                    try self.admit();
                    try self.sim.crashFileSystem();
                    try self.admit();
                },
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
            .sim = try vopr.vopr_io.VoprIo.init(.{ .seed = 0x570a_7a, .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy, .clock_read }) }),
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
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{ .id = id, .name = mode_name, .kind = switch (mode) {
            .fresh_provision, .idempotent_restart => .workload,
            else => .fault,
        } });
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            try world.state.run(mode);
            found = true;
        };
        if (!found) return error.InvalidProvisioningStartupTransition;
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
        return .{ .progress_expected = true, .progress_units = world.state.progress, .active_tasks = world.state.sim.tasks.activeTaskCount(), .cleanup_complete = world.state.complete };
    }
    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "provisioning startup VOPR exact replays admission provisioning and recovery" {
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
