// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Persistent Parquet/object-range cache histories on borrowed `VoprIo`.

const std = @import("std");
const vopr = @import("vopr");
const lake = @import("../serverless/query/lake_parquet_rowgroup.zig");
const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "parquet-cache";
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
        fixture_allocator: FixtureAllocator,
        sim: vopr.vopr_io.VoprIo,
        faults: vopr.fault.Controller,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,
        operation: ?std.Io.Future(void) = null,
        operation_error: ?anyerror = null,
        fault_adapter: ?vopr.fault_vopr_io.Adapter = null,
        active_fault_id: ?vopr.id.StableId = null,

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

        fn run(self: *@This(), mode: Mode) !void {
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
                    self.write(&cache);
                    const stats = cache.statsSnapshot();
                    self.sound = !(try self.readMatches(&cache)) and stats.write_errors > 0;
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
        }

        fn runAsync(self: *@This(), mode: Mode) void {
            self.run(mode) catch |err| {
                self.operation_error = err;
                self.complete = true;
                return;
            };
            self.complete = true;
        }
    };
    pub const World = struct { state: *State };
    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.fixture_allocator = .init;
        errdefer _ = state.fixture_allocator.deinit();
        const fixture_alloc = state.fixture_allocator.allocator();
        state.allocator = fixture_alloc;
        state.sim = try vopr.vopr_io.VoprIo.init(.{ .seed = 0xca_c4e, .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy }) });
        errdefer state.sim.deinit();
        state.faults = try vopr.fault.Controller.initWithAlgebra(fixture_alloc, 1, .{ .minimum_healthy_nodes = 1 }, .{ .rules = &.{.{ .left = .storage_corruption, .right = .resource_exhaustion, .relationship = .left_precedes }} });
        state.sound = true;
        state.complete = false;
        state.progress = 0;
        state.operation = null;
        state.operation_error = null;
        state.fault_adapter = null;
        state.active_fault_id = null;
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        if (world.state.fault_adapter) |*adapter| adapter.deinit();
        world.state.faults.deinit();
        world.state.sim.deinit();
        std.debug.assert(world.state.fixture_allocator.deinit() == .ok);
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.operation == null) {
            if (world.state.complete) return;
            inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{ .id = id, .name = mode_name, .kind = if (mode == .write_read or mode == .duplicate_write) .workload else .fault });
            return;
        }
        try world.state.sim.scheduler().enumerateReady(list, allocator);
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.operation == null and !state.complete) {
            var found = false;
            inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
                if (mode == .write_fault) {
                    var spec = vopr.fault.Spec.named(.storage, .one_shot, name ++ ".fail-next-write");
                    spec.resource_id = vopr.id.stable(name, "filesystem");
                    try state.faults.start(spec, events, allocator);
                    state.active_fault_id = spec.id;
                    state.fault_adapter = vopr.fault_vopr_io.Adapter.init(state.allocator, &state.sim, &.{.{ .fault_id = spec.id, .effect = .fail_next_file_write }});
                    try state.fault_adapter.?.reconcile(&state.faults);
                }
                state.operation = try state.sim.io().concurrent(State.runAsync, .{ state, mode });
                found = true;
            };
            if (!found) return error.InvalidParquetCacheTransition;
            return .applied();
        }

        try state.sim.scheduler().executeReady(selected.id, events, allocator);
        if (state.complete) {
            state.operation = null;
            if (state.active_fault_id) |fault_id| {
                try state.faults.stop(fault_id, events, allocator);
                try state.fault_adapter.?.reconcile(&state.faults);
                state.fault_adapter.?.deinit();
                state.fault_adapter = null;
                state.active_fault_id = null;
            }
            if (state.operation_error) |err| return err;
            try state.sim.ensureNoCapabilityViolation();
        }
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
        return world.state.complete and world.state.operation == null and world.state.sim.scheduler().quiescent();
    }
};

test "persistent Parquet cache VOPR exact replays write faults and crash recovery" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, mode_ordinal| {
        var choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0xca_c4e ^ mode_id);
        var artifact = vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{ .system = "antfly", .transition_budget = 256, .backend_ids = &backend_ids }) catch |err| {
            std.debug.print("parquet cache mode={s} failed: {s}\n", .{ Scenario.mode_names[mode_ordinal], @errorName(err) });
            return err;
        };
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
