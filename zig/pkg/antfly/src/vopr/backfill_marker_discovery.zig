// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Metadata backfill-marker discovery and cache throttling through the
//! production scanner on borrowed `VoprIo`.

const std = @import("std");
const vopr = @import("vopr");
const metadata_service = @import("../metadata/service.zig");
const backfill_state = @import("../storage/db/backfill_state.zig");

pub const Scenario = struct {
    pub const name = "backfill-marker-discovery";
    pub const version: u32 = 1;

    const sound_id = vopr.id.stable(name, "cache-reflects-durable-marker-state");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".cache-reflects-durable-marker-state", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum {
        absent,
        legacy,
        valid_owned,
        corrupt,
        appearance_throttled,
        disappearance_rescan,
        ownership_mismatch,
        read_fault_retry,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "absent"),
        vopr.id.stable(name, "legacy"),
        vopr.id.stable(name, "valid-owned"),
        vopr.id.stable(name, "corrupt"),
        vopr.id.stable(name, "appearance-throttled"),
        vopr.id.stable(name, "disappearance-rescan"),
        vopr.id.stable(name, "ownership-mismatch"),
        vopr.id.stable(name, "read-fault-retry"),
    };
    const mode_names = [_][]const u8{
        name ++ ".absent",
        name ++ ".legacy",
        name ++ ".valid-owned",
        name ++ ".corrupt",
        name ++ ".appearance-throttled",
        name ++ ".disappearance-rescan",
        name ++ ".ownership-mismatch",
        name ++ ".read-fault-retry",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,

        fn indexRoot(self: *@This(), mode: Mode) ![]u8 {
            return std.fmt.allocPrint(
                self.allocator,
                "/replicas/{s}/store-7/group-42/table-db/indexes/search_idx",
                .{@tagName(mode)},
            );
        }

        fn replicaRoot(self: *@This(), mode: Mode) ![]u8 {
            return std.fmt.allocPrint(self.allocator, "/replicas/{s}", .{@tagName(mode)});
        }

        fn createOwned(self: *@This(), mode: Mode, generation: u64) ![]u8 {
            const root = try self.indexRoot(mode);
            defer self.allocator.free(root);
            try std.Io.Dir.cwd().createDirPath(self.sim.io(), root);
            const state = backfill_state.RebuildState.initOwned(root, null, generation);
            try state.updateWithIo(self.sim.io(), "doc:m");
            return state.pathAlloc(self.allocator);
        }

        fn refreshNow(
            self: *@This(),
            mode: Mode,
            cache: *metadata_service.StoreStatusBackfillMarkerCache,
            probe_ticks: *usize,
        ) !void {
            const root = try self.replicaRoot(mode);
            defer self.allocator.free(root);
            try metadata_service.refreshStoreStatusBackfillMarkerCacheNowWithIo(
                self.allocator,
                self.sim.io(),
                root,
                probe_ticks,
                cache,
            );
            self.progress += 1;
        }

        fn markerIs(cache: *const metadata_service.StoreStatusBackfillMarkerCache, comptime tag: std.meta.Tag(metadata_service.StoreStatusBackfillMarker.State)) bool {
            return cache.markers.len == 1 and std.meta.activeTag(cache.markers[0].state) == tag;
        }

        fn run(self: *@This(), mode: Mode) !void {
            var cache: metadata_service.StoreStatusBackfillMarkerCache = .{};
            defer cache.deinit(self.allocator);
            var probe_ticks: usize = 0;

            switch (mode) {
                .absent => {
                    try self.refreshNow(mode, &cache, &probe_ticks);
                    self.sound = cache.markers.len == 0 and cache.scanned_at_ms != 0;
                },
                .legacy => {
                    const root = try self.indexRoot(mode);
                    defer self.allocator.free(root);
                    try std.Io.Dir.cwd().createDirPath(self.sim.io(), root);
                    const state = backfill_state.RebuildState.init(root);
                    const path = try state.pathAlloc(self.allocator);
                    defer self.allocator.free(path);
                    try std.Io.Dir.cwd().writeFile(self.sim.io(), .{ .sub_path = path, .data = "doc:m" });
                    try self.refreshNow(mode, &cache, &probe_ticks);
                    self.sound = markerIs(&cache, .legacy);
                },
                .valid_owned => {
                    const path = try self.createOwned(mode, 95);
                    defer self.allocator.free(path);
                    try self.refreshNow(mode, &cache, &probe_ticks);
                    self.sound = markerIs(&cache, .valid) and
                        cache.markers[0].owner_generation == 95 and
                        std.mem.eql(u8, cache.markers[0].state.valid, "doc:m");
                },
                .corrupt => {
                    const path = try self.createOwned(mode, 95);
                    defer self.allocator.free(path);
                    const encoded = try std.Io.Dir.cwd().readFileAlloc(self.sim.io(), path, self.allocator, .limited(128 * 1024));
                    defer self.allocator.free(encoded);
                    encoded[encoded.len - 1] ^= 0xff;
                    try std.Io.Dir.cwd().writeFile(self.sim.io(), .{ .sub_path = path, .data = encoded });
                    try self.refreshNow(mode, &cache, &probe_ticks);
                    self.sound = markerIs(&cache, .corrupt);
                },
                .appearance_throttled => {
                    try self.refreshNow(mode, &cache, &probe_ticks);
                    const state_path = try self.createOwned(mode, 95);
                    defer self.allocator.free(state_path);
                    const root = try self.replicaRoot(mode);
                    defer self.allocator.free(root);
                    probe_ticks = 1;
                    try metadata_service.maybeRefreshStoreStatusBackfillMarkerCacheWithIo(
                        self.allocator,
                        self.sim.io(),
                        root,
                        1,
                        &probe_ticks,
                        &cache,
                    );
                    self.sound = cache.markers.len == 0;
                    try self.sim.advance(31 * std.time.ns_per_s);
                    try metadata_service.maybeRefreshStoreStatusBackfillMarkerCacheWithIo(
                        self.allocator,
                        self.sim.io(),
                        root,
                        40,
                        &probe_ticks,
                        &cache,
                    );
                    self.sound = self.sound and markerIs(&cache, .valid);
                    self.progress += 1;
                },
                .disappearance_rescan => {
                    const path = try self.createOwned(mode, 95);
                    defer self.allocator.free(path);
                    try self.refreshNow(mode, &cache, &probe_ticks);
                    try std.Io.Dir.cwd().deleteFile(self.sim.io(), path);
                    const root = try self.replicaRoot(mode);
                    defer self.allocator.free(root);
                    self.sound = try metadata_service.storeStatusBackfillMarkersChangedWithIo(
                        self.allocator,
                        self.sim.io(),
                        root,
                        cache.markers,
                    );
                    cache.rescan_requested = true;
                    try metadata_service.maybeRefreshStoreStatusBackfillMarkerCacheWithIo(
                        self.allocator,
                        self.sim.io(),
                        root,
                        0,
                        &probe_ticks,
                        &cache,
                    );
                    self.sound = self.sound and cache.markers.len == 0;
                    self.progress += 1;
                },
                .ownership_mismatch => {
                    const old_path = try self.createOwned(mode, 95);
                    defer self.allocator.free(old_path);
                    const root = try self.indexRoot(mode);
                    defer self.allocator.free(root);
                    const mismatched_path = try std.fmt.allocPrint(
                        self.allocator,
                        "{s}/rebuild.state.g-{x:0>16}",
                        .{ root, @as(u64, 96) },
                    );
                    defer self.allocator.free(mismatched_path);
                    try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), mismatched_path, self.sim.io());
                    try self.refreshNow(mode, &cache, &probe_ticks);
                    self.sound = markerIs(&cache, .legacy) and cache.markers[0].owner_generation == 96;
                },
                .read_fault_retry => {
                    const path = try self.createOwned(mode, 95);
                    defer self.allocator.free(path);
                    self.sim.failNextFileRead();
                    self.refreshNow(mode, &cache, &probe_ticks) catch |err| {
                        if (err != error.InputOutput) return err;
                        try self.refreshNow(mode, &cache, &probe_ticks);
                        self.sound = markerIs(&cache, .valid);
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
                .seed = 0xbac1_f111,
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
                    .absent, .valid_owned, .appearance_throttled => .workload,
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
        if (!found) return error.InvalidBackfillMarkerDiscoveryTransition;
        try events.emitNamed(allocator, .domain, selected.name, world.state.progress);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".progress", world.state.progress);
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

test "backfill marker discovery VOPR exact replays scans rechecks and throttling" {
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
