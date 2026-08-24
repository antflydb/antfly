// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Persistent Parquet/object-range cache histories on borrowed `VoprIo`.

const std = @import("std");
const vopr = @import("vopr");
const lake = @import("../serverless/query/lake_parquet_rowgroup.zig");

pub const Scenario = struct {
    pub const name = "parquet-cache";
    pub const version: u32 = 1;
    const sound_id = vopr.id.stable(name, "persistence-sound");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".persistence-sound", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };
    const Mode = enum { write_read, duplicate_write, write_fault, read_fault, crash_reopen };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "write-read"),
        vopr.id.stable(name, "duplicate-write"),
        vopr.id.stable(name, "write-fault"),
        vopr.id.stable(name, "read-fault"),
        vopr.id.stable(name, "crash-reopen"),
    };
    const mode_names = [_][]const u8{
        name ++ ".write-read",
        name ++ ".duplicate-write",
        name ++ ".write-fault",
        name ++ ".read-fault",
        name ++ ".crash-reopen",
    };
    const cache_key = "lake-range:v1:bucket/object:version=v1:offset=0:len=7";
    const payload = "parquet";

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        faults: vopr.fault.Controller,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,

        fn open(self: *@This(), durability: lake.PersistentObjectRangeCacheDurability) !lake.PersistentObjectRangeCache {
            return lake.PersistentObjectRangeCache.initWithPolicy(self.sim.io(), "/parquet-cache", .{
                .max_total_bytes = 4096,
                .max_entries = 8,
                .max_write_queue_bytes = 4096,
                .max_write_queue_entries = 8,
                .durability = durability,
            });
        }

        fn write(self: *@This(), cache: *lake.PersistentObjectRangeCache) void {
            _ = cache.enqueueWrite(cache_key, payload);
            cache.flush();
            self.progress += 1;
        }

        fn readMatches(self: *@This(), cache: *lake.PersistentObjectRangeCache) !bool {
            const bytes = (try cache.readAlloc(self.allocator, cache_key, payload.len)) orelse return false;
            defer self.allocator.free(bytes);
            self.progress += 1;
            return std.mem.eql(u8, bytes, payload);
        }

        fn run(self: *@This(), mode: Mode, events: *vopr.event.Sink) !void {
            var cache = try self.open(if (mode == .crash_reopen) .durable else .cache_only);
            var cache_live = true;
            defer if (cache_live) cache.deinit();
            switch (mode) {
                .write_read => {
                    self.write(&cache);
                    self.sound = try self.readMatches(&cache);
                },
                .duplicate_write => {
                    const first = cache.enqueueWrite(cache_key, payload);
                    const second = cache.enqueueWrite(cache_key, payload);
                    cache.flush();
                    self.progress += 1;
                    self.sound = first == .enqueued and (second == .coalesced or second == .enqueued) and try self.readMatches(&cache);
                },
                .write_fault => {
                    var spec = vopr.fault.Spec.named(.storage, .one_shot, name ++ ".fail-next-write");
                    spec.resource_id = vopr.id.stable(name, "filesystem");
                    try self.faults.start(spec, events, self.allocator);
                    var adapter = vopr.fault_vopr_io.Adapter.init(self.allocator, &self.sim, &.{.{ .fault_id = spec.id, .effect = .fail_next_file_write }});
                    defer adapter.deinit();
                    try adapter.reconcile(&self.faults);
                    self.write(&cache);
                    const stats = cache.statsSnapshot();
                    self.sound = !(try self.readMatches(&cache)) and stats.write_errors > 0;
                    try self.faults.stop(spec.id, events, self.allocator);
                    try adapter.reconcile(&self.faults);
                },
                .read_fault => {
                    self.write(&cache);
                    self.sim.failNextFileRead();
                    self.sound = !(try self.readMatches(&cache));
                },
                .crash_reopen => {
                    self.write(&cache);
                    cache.deinit();
                    cache_live = false;
                    try self.sim.crashFileSystem();
                    var reopened = try self.open(.durable);
                    defer reopened.deinit();
                    self.sound = try self.readMatches(&reopened);
                },
            }
            self.complete = true;
        }
    };
    pub const World = struct { state: *State };
    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        var sim = try vopr.vopr_io.VoprIo.init(.{ .seed = 0xca_c4e, .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy }) });
        errdefer sim.deinit();
        state.* = .{
            .allocator = allocator,
            .sim = sim,
            .faults = try vopr.fault.Controller.initWithAlgebra(allocator, 1, .{ .minimum_healthy_nodes = 1 }, .{ .rules = &.{.{ .left = .storage_corruption, .right = .resource_exhaustion, .relationship = .left_precedes }} }),
        };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.faults.deinit();
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{ .id = id, .name = mode_name, .kind = if (mode == .write_read or mode == .duplicate_write) .workload else .fault });
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, _: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            try world.state.run(mode, events);
            found = true;
        };
        if (!found) return error.InvalidParquetCacheTransition;
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
        return .{ .progress_expected = true, .progress_units = world.state.progress, .active_tasks = world.state.sim.tasks.activeTaskCount(), .cleanup_complete = world.state.complete };
    }
    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "persistent Parquet cache VOPR exact replays write faults and crash recovery" {
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
