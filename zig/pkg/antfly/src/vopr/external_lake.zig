// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deterministic external-lake range planning and cache histories.

const std = @import("std");
const vopr = @import("vopr");
const lake = @import("../serverless/query/lake_parquet_rowgroup.zig");
const range_io = @import("../serverless/query/lake_range_io.zig");

pub const Scenario = struct {
    pub const name = "external-lake";
    pub const version: u32 = 1;
    const sound_id = vopr.id.stable(name, "range-and-cache-sound");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".range-and-cache-sound", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum { cached_read, version_isolation, short_response, timeout, admission_limit };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "cached-read"),
        vopr.id.stable(name, "version-isolation"),
        vopr.id.stable(name, "short-response"),
        vopr.id.stable(name, "timeout"),
        vopr.id.stable(name, "admission-limit"),
    };
    const mode_names = [_][]const u8{
        name ++ ".cached-read",
        name ++ ".version-isolation",
        name ++ ".short-response",
        name ++ ".timeout",
        name ++ ".admission-limit",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        cache: lake.ObjectRangeCache = .{},
        calls: u64 = 0,
        mode: Mode = .cached_read,
        sound: bool = true,
        complete: bool = false,

        fn reader(self: *@This()) lake.ObjectRangeReader {
            return .{ .ctx = self, .read_range_alloc = readRange };
        }

        fn readRange(raw: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.mode == .timeout) return error.Timeout;
            const actual_len = if (self.mode == .short_response) len - 1 else len;
            const bytes = try alloc.alloc(u8, actual_len);
            for (bytes, 0..) |*byte, index| byte.* = @intCast((offset + index) & 0xff);
            return bytes;
        }

        fn read(self: *@This(), object_version: []const u8) ![]u8 {
            const object = range_io.ObjectRef{
                .bucket = "bucket",
                .key = "table/data.parquet",
                .byte_len = 64,
                .version = .{ .version_id = object_version },
            };
            const planned = try range_io.planParquetFooterRead(object, 8);
            return self.cache.readAlloc(self.allocator, self.reader(), planned);
        }

        fn run(self: *@This()) !void {
            switch (self.mode) {
                .cached_read => {
                    const first = try self.read("v1");
                    defer self.allocator.free(first);
                    const second = try self.read("v1");
                    defer self.allocator.free(second);
                    self.sound = self.calls == 1 and std.mem.eql(u8, first, second);
                },
                .version_isolation => {
                    const first = try self.read("v1");
                    defer self.allocator.free(first);
                    const second = try self.read("v2");
                    defer self.allocator.free(second);
                    self.sound = self.calls == 2;
                },
                .short_response => {
                    _ = self.read("short") catch |err| {
                        self.sound = err == error.InvalidLakeRangeRead;
                        self.complete = true;
                        return;
                    };
                    self.sound = false;
                },
                .timeout => {
                    _ = self.read("timeout") catch |err| {
                        self.sound = err == error.Timeout;
                        self.complete = true;
                        return;
                    };
                    self.sound = false;
                },
                .admission_limit => {
                    self.cache.policy.setFetchLimit(4);
                    _ = self.read("limited") catch |err| {
                        self.sound = err == error.LakeRangeReadTooLarge and self.calls == 0;
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
            .sim = try vopr.vopr_io.VoprIo.init(.{ .seed = 0xe17e_4a4e, .required = .of(&.{.clock_read}) }),
        };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.cache.deinit(allocator);
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
            .id = id,
            .name = mode_name,
            .kind = if (mode == .cached_read or mode == .version_isolation) .workload else .fault,
        });
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            world.state.mode = mode;
            found = true;
        };
        if (!found) return error.InvalidExternalLakeTransition;
        try world.state.run();
        try events.emitNamed(allocator, .domain, selected.name, world.state.calls);
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".calls", world.state.calls);
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, sound_id, world.state.sound);
        try sink.check(allocator, complete_id, world.state.complete);
    }
    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        return .{ .progress_expected = true, .progress_units = @intFromBool(world.state.complete), .active_tasks = world.state.sim.tasks.activeTaskCount(), .cleanup_complete = world.state.complete };
    }
    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "external lake VOPR exact replays planning cache and response faults" {
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
