// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable generation publication, rollback, reconciliation, locking, and
//! cleanup through the production lifecycle API on borrowed `VoprIo`.

const std = @import("std");
const vopr = @import("vopr");
const lifecycle = @import("../storage/db/generation_lifecycle.zig");

pub const Scenario = struct {
    pub const name: []const u8 = "generation-lifecycle";
    pub const version: u32 = 1;

    const sound_id = vopr.id.stable(name, "publication-remains-recoverable");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".publication-remains-recoverable", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum {
        fresh_publish,
        prepared_rollback,
        rename_failure_retry,
        uncertain_sync_reconcile,
        prepared_crash_reconcile,
        reader_writer_locking,
        canonical_alias_readers,
        stale_stage_cleanup,
        corrupt_marker_repair,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "fresh-publish"),
        vopr.id.stable(name, "prepared-rollback"),
        vopr.id.stable(name, "rename-failure-retry"),
        vopr.id.stable(name, "uncertain-sync-reconcile"),
        vopr.id.stable(name, "prepared-crash-reconcile"),
        vopr.id.stable(name, "reader-writer-locking"),
        vopr.id.stable(name, "canonical-alias-readers"),
        vopr.id.stable(name, "stale-stage-cleanup"),
        vopr.id.stable(name, "corrupt-marker-repair"),
    };
    const mode_names = [_][]const u8{
        name ++ ".fresh-publish",
        name ++ ".prepared-rollback",
        name ++ ".rename-failure-retry",
        name ++ ".uncertain-sync-reconcile",
        name ++ ".prepared-crash-reconcile",
        name ++ ".reader-writer-locking",
        name ++ ".canonical-alias-readers",
        name ++ ".stale-stage-cleanup",
        name ++ ".corrupt-marker-repair",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,

        fn exists(self: *@This(), path: []const u8) bool {
            _ = std.Io.Dir.cwd().statFile(self.sim.io(), path, .{}) catch return false;
            return true;
        }

        fn writePayload(self: *@This(), root: []const u8, value: []const u8) !void {
            const path = try std.fs.path.join(self.allocator, &.{ root, "payload" });
            defer self.allocator.free(path);
            try std.Io.Dir.cwd().writeFile(self.sim.io(), .{ .sub_path = path, .data = value });
        }

        fn payloadEquals(self: *@This(), root: []const u8, expected: []const u8) !bool {
            const path = try std.fs.path.join(self.allocator, &.{ root, "payload" });
            defer self.allocator.free(path);
            const bytes = try std.Io.Dir.cwd().readFileAlloc(self.sim.io(), path, self.allocator, .limited(1024));
            defer self.allocator.free(bytes);
            return std.mem.eql(u8, bytes, expected);
        }

        fn publishFresh(self: *@This(), path: []const u8, payload: []const u8) !lifecycle.PublicationOutcome {
            var transition = try lifecycle.beginProcessExclusiveWithIo(path, self.sim.io());
            defer transition.deinit();
            var staged = try transition.beginStaging();
            defer staged.deinit();
            try self.writePayload(staged.path(), payload);
            const outcome = try staged.publish();
            self.progress += 1;
            return outcome;
        }

        fn verifyPublished(self: *@This(), path: []const u8, expected: []const u8) !bool {
            var lease = (try lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, path, self.sim.io())) orelse
                return false;
            defer lease.deinit();
            self.progress += 1;
            return self.payloadEquals(path, expected);
        }

        fn run(self: *@This(), mode: Mode) !void {
            const path = try std.fmt.allocPrint(self.allocator, "/generation/{s}", .{@tagName(mode)});
            defer self.allocator.free(path);
            switch (mode) {
                .fresh_publish => {
                    self.sound = try self.publishFresh(path, "fresh") == .durable and
                        try self.verifyPublished(path, "fresh");
                },
                .prepared_rollback => {
                    var transition = try lifecycle.beginProcessExclusiveWithIo(path, self.sim.io());
                    defer transition.deinit();
                    var staged = try transition.beginStaging();
                    defer staged.deinit();
                    try self.writePayload(staged.path(), "candidate");
                    _ = try staged.publishPrepared();
                    try staged.rollbackPublication();
                    self.sound = !self.exists(path);
                    self.progress += 1;
                },
                .rename_failure_retry => {
                    var transition = try lifecycle.beginProcessExclusiveWithIo(path, self.sim.io());
                    defer transition.deinit();
                    var staged = try transition.beginStaging();
                    defer staged.deinit();
                    try self.writePayload(staged.path(), "retry");
                    self.sim.failNextFileRename();
                    _ = staged.publish() catch |err| {
                        if (err != error.HardwareFailure) return err;
                        const outcome = try staged.publish();
                        self.sound = outcome == .durable and try self.payloadEquals(path, "retry");
                        self.progress += 1;
                        self.complete = true;
                        return;
                    };
                    self.sound = false;
                },
                .uncertain_sync_reconcile => {
                    lifecycle.failNextPublishedParentSyncForTest();
                    const outcome = try self.publishFresh(path, "uncertain");
                    self.sound = outcome == .durability_uncertain and try self.verifyPublished(path, "uncertain");
                },
                .prepared_crash_reconcile => {
                    var transition = try lifecycle.beginProcessExclusiveWithIo(path, self.sim.io());
                    var staged = try transition.beginStaging();
                    try self.writePayload(staged.path(), "uncommitted");
                    _ = try staged.publishPrepared();
                    const retained = try self.allocator.dupe(u8, staged.staging_path);
                    defer self.allocator.free(retained);
                    staged.abandonForCrashForTest();
                    transition.deinit();
                    try self.sim.crashFileSystem();
                    var lease = (try lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, path, self.sim.io())) orelse
                        return error.GenerationReadLeaseMissing;
                    lease.deinit();
                    self.sound = !self.exists(path) and !self.exists(retained);
                    self.progress += 1;
                },
                .reader_writer_locking => {
                    _ = try self.publishFresh(path, "visible");
                    var first = (try lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, path, self.sim.io())).?;
                    defer first.deinit();
                    var second = (try lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, path, self.sim.io())).?;
                    defer second.deinit();
                    if (lifecycle.beginProcessExclusiveWithIo(path, self.sim.io())) |unexpected| {
                        var transition = unexpected;
                        transition.deinit();
                        self.sound = false;
                    } else |err| {
                        self.sound = err == error.GenerationTransitionActive;
                    }
                    self.progress += 1;
                },
                .canonical_alias_readers => {
                    _ = try self.publishFresh(path, "alias");
                    const alias = try std.fmt.allocPrint(self.allocator, "/generation/../generation/{s}", .{@tagName(mode)});
                    defer self.allocator.free(alias);
                    var first = (try lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, path, self.sim.io())).?;
                    defer first.deinit();
                    var second = (try lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, alias, self.sim.io())).?;
                    defer second.deinit();
                    self.sound = try self.payloadEquals(alias, "alias");
                    self.progress += 1;
                },
                .stale_stage_cleanup => {
                    _ = try self.publishFresh(path, "live");
                    const stale = try std.fmt.allocPrint(self.allocator, "{s}.restore-stage-a-b", .{path});
                    defer self.allocator.free(stale);
                    try std.Io.Dir.cwd().createDirPath(self.sim.io(), stale);
                    try self.writePayload(stale, "stale");
                    self.sound = try self.verifyPublished(path, "live") and !self.exists(stale);
                },
                .corrupt_marker_repair => {
                    try std.Io.Dir.cwd().createDirPath(self.sim.io(), path);
                    const marker = try std.fs.path.join(self.allocator, &.{ path, lifecycle.publication_marker_name });
                    defer self.allocator.free(marker);
                    try std.Io.Dir.cwd().writeFile(self.sim.io(), .{ .sub_path = marker, .data = "{" });
                    _ = lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, path, self.sim.io()) catch |err| {
                        if (err != error.InvalidGenerationPublicationMarker) return err;
                        try std.Io.Dir.cwd().deleteFile(self.sim.io(), marker);
                        var lease = (try lifecycle.acquirePublishedGenerationReadWithIo(self.allocator, path, self.sim.io())).?;
                        lease.deinit();
                        self.progress += 1;
                        self.complete = true;
                        return;
                    };
                    self.sound = false;
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
            .sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x6e65_7261_7469_6f6e,
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
                .kind = switch (mode) {
                    .fresh_publish, .reader_writer_locking, .canonical_alias_readers => .workload,
                    else => .fault,
                },
            });
        }
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| {
            if (selected.id == id) {
                try world.state.run(mode);
                found = true;
            }
        }
        if (!found) return error.InvalidGenerationLifecycleTransition;
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

test "generation lifecycle VOPR exact replays publication recovery and locking" {
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
