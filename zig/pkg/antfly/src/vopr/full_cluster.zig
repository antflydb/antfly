// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deployment-shaped exact-replay composition. A metadata quorum, two hosted
//! data replicas, three public API nodes, two client tasks, and a production
//! serverless worker share one VoprIo scheduler and virtual clock.

const std = @import("std");
const vopr = @import("vopr");
const metadata_sim = @import("../metadata/sim_harness.zig");
const serverless_workflow = @import("serverless_workflow.zig");
const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "full-cluster";
    pub const version: u32 = 1;

    const acknowledged_id = vopr.id.stable(name, "acknowledged-data-visible");
    const quorum_id = vopr.id.stable(name, "metadata-quorum-recovers");
    const routing_id = vopr.id.stable(name, "non-host-routing-sound");
    const isolation_id = vopr.id.stable(name, "multi-table-isolation");
    const publication_id = vopr.id.stable(name, "serverless-publication-visible");
    const shared_io_id = vopr.id.stable(name, "one-shared-vopr-io");
    const cleanup_id = vopr.id.stable(name, "cluster-resources-cleaned");
    const complete_id = vopr.id.stable(name, "history-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = acknowledged_id, .name = name ++ ".acknowledged-data-visible", .kind = .always },
        .{ .id = quorum_id, .name = name ++ ".metadata-quorum-recovers", .kind = .always },
        .{ .id = routing_id, .name = name ++ ".non-host-routing-sound", .kind = .always },
        .{ .id = isolation_id, .name = name ++ ".multi-table-isolation", .kind = .always },
        .{ .id = publication_id, .name = name ++ ".serverless-publication-visible", .kind = .always },
        .{ .id = shared_io_id, .name = name ++ ".one-shared-vopr-io", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".cluster-resources-cleaned", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".history-completes", .kind = .reachable },
    };

    const Mode = metadata_sim.VoprPublicClusterFixture.FaultMode;
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "clean"),
        vopr.id.stable(name, "metadata-partition"),
        vopr.id.stable(name, "node-restart"),
        vopr.id.stable(name, "partial-http-write"),
    };
    const mode_names = [_][]const u8{
        name ++ ".clean",
        name ++ ".metadata-partition",
        name ++ ".node-restart",
        name ++ ".partial-http-write",
    };

    const State = struct {
        owner_alloc: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        sim: vopr.vopr_io.VoprIo,
        public_cluster: *metadata_sim.VoprPublicClusterFixture,
        serverless: *serverless_workflow.Scenario.Fixture,
        mode: ?Mode = null,
        serverless_done: bool = false,
        serverless_sound: bool = false,
        shared_io_sound: bool = false,
        complete: bool = false,

        fn init(alloc: std.mem.Allocator) !*State {
            const self = try alloc.create(State);
            errdefer alloc.destroy(self);
            self.owner_alloc = alloc;
            self.fixture_allocator = .init;
            errdefer _ = self.fixture_allocator.deinit();
            const fixture_alloc = self.fixture_allocator.allocator();
            self.sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x4655_4c4c,
                .tasks = .{ .stack_size = 8 * 1024 * 1024 },
                .network = .{ .max_sockets = 96, .stream_capacity = 256 * 1024 },
                .files = .{ .capacity_bytes = 64 * 1024 * 1024 },
                .instrumentation = .{ .enabled = false, .map_digest = 0x4655_4c4c },
            });
            errdefer self.sim.deinit();
            self.public_cluster = try metadata_sim.VoprPublicClusterFixture.init(fixture_alloc, &self.sim);
            errdefer self.public_cluster.deinit();
            self.serverless = try serverless_workflow.Scenario.Fixture.initWithVoprIo(fixture_alloc, &self.sim);
            errdefer self.serverless.deinit();
            self.mode = null;
            self.serverless_done = false;
            self.serverless_sound = false;
            self.shared_io_sound = self.serverless.sim == &self.sim and
                self.public_cluster.sim == &self.sim;
            self.complete = false;
            return self;
        }

        fn deinit(self: *State) void {
            self.serverless.deinit();
            self.public_cluster.deinit();
            self.sim.deinit();
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            self.owner_alloc.destroy(self);
        }

        fn start(self: *State, mode: Mode) !void {
            self.mode = mode;
            try self.public_cluster.start(mode);
            _ = self.sim.io().async(runServerless, .{self});
            _ = self.sim.io().async(waitForCompletion, .{self});
        }

        fn runServerless(self: *State) void {
            self.serverless.runClean() catch {
                self.serverless_done = true;
                return;
            };
            self.serverless_sound = self.serverless.cleanWorkflowVisible();
            self.serverless_done = true;
        }

        fn waitForCompletion(self: *State) void {
            while (!self.public_cluster.complete or !self.serverless_done) {
                self.sim.io().sleep(.fromNanoseconds(1), .awake) catch return;
            }
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        return .{ .state = try State.init(allocator) };
    }

    pub fn deinit(world: *World, _: std.mem.Allocator) void {
        world.state.deinit();
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        const state = world.state;
        if (state.mode == null) {
            inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = if (mode == .clean) .workload else .fault,
            });
            return;
        }
        if (!state.sim.scheduler().quiescent()) try state.sim.scheduler().enumerateReady(list, allocator);
    }

    pub fn execute(
        world: *World,
        selected: vopr.transition.Transition,
        events: *vopr.event.Sink,
        allocator: std.mem.Allocator,
    ) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.mode == null) {
            var found = false;
            inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
                try state.start(mode);
                found = true;
            };
            if (!found) return error.InvalidFullClusterMode;
        } else {
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
        }
        const snapshot = state.public_cluster.healthSnapshot();
        try events.emitNamed(allocator, .domain, selected.name, @intFromBool(snapshot.requests_ok));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const cluster = state.public_cluster.healthSnapshot();
        const resources = state.sim.resourceSnapshot();
        try builder.addNamed(allocator, name ++ ".mode", if (state.mode) |mode| @as(i64, @intFromEnum(mode)) + 1 else 0);
        try builder.addNamed(allocator, name ++ ".data-hosts", @intCast(cluster.hosts));
        try builder.addNamed(allocator, name ++ ".public-requests-ok", @intFromBool(cluster.requests_ok));
        try builder.addNamed(allocator, name ++ ".write-ok", @intFromBool(state.public_cluster.write_sound));
        try builder.addNamed(allocator, name ++ ".read-ok", @intFromBool(state.public_cluster.read_sound));
        try builder.addNamed(allocator, name ++ ".tenant-write-ok", @intFromBool(state.public_cluster.tenant_write_sound));
        try builder.addNamed(allocator, name ++ ".tenant-read-ok", @intFromBool(state.public_cluster.tenant_read_sound));
        try builder.addNamed(allocator, name ++ ".table-isolation-ok", @intFromBool(state.public_cluster.table_isolation_sound));
        try builder.addNamed(allocator, name ++ ".request-errors", @intCast(state.public_cluster.request_errors));
        try builder.addNamed(allocator, name ++ ".last-request-error", @intCast(state.public_cluster.last_request_error_code));
        try builder.addNamed(allocator, name ++ ".serverless-visible", @intFromBool(state.serverless_sound));
        try builder.addNamed(allocator, name ++ ".open-sockets", @intCast(resources.open_sockets));
        try builder.addNamed(allocator, name ++ ".active-tasks", @intCast(resources.active_tasks));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const cluster = state.public_cluster.healthSnapshot();
        const resources = state.sim.resourceSnapshot();
        try sink.check(allocator, acknowledged_id, !state.complete or cluster.requests_ok);
        try sink.check(allocator, quorum_id, !state.complete or cluster.topology_ok);
        try sink.check(allocator, routing_id, !state.complete or cluster.hosts == 2);
        try sink.check(allocator, isolation_id, !state.complete or state.public_cluster.table_isolation_sound);
        try sink.check(allocator, publication_id, !state.complete or state.serverless_sound);
        try sink.check(allocator, shared_io_id, state.shared_io_sound);
        try sink.check(allocator, cleanup_id, !state.complete or
            (cluster.cleanup_ok and resources.open_sockets == 0));
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        const cluster = state.public_cluster.healthSnapshot();
        return state.sim.healthSnapshot(.{
            .progress_expected = state.mode != null,
            .progress_units = @as(u64, @intFromBool(state.public_cluster.write_finished)) +
                @as(u64, @intFromBool(state.public_cluster.read_finished)) +
                @as(u64, @intFromBool(state.public_cluster.tenant_write_finished)) +
                @as(u64, @intFromBool(state.public_cluster.tenant_read_finished)) +
                @as(u64, @intFromBool(state.serverless_done)),
            .recovery_expected = state.mode != null and state.mode.? != .clean,
            .recovery_complete = state.complete and cluster.topology_ok,
            .consistency_valid = cluster.requests_ok and state.serverless_sound,
            .cleanup_complete = state.complete and cluster.cleanup_ok,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "full cluster VOPR exact replays metadata data serverless HTTP clients and recovery" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, mode_ordinal| {
        var choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0x4655_4c4c + mode_ordinal);
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{
            .system = "antfly",
            .transition_budget = 20_000,
            .resource_budget = 96,
            .backend_ids = &backend_ids,
            .source_revision = "full-cluster-vopr-v1",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        if (recorded.summary.?.property_failures != 0) {
            for (recorded.failures.items) |failure| std.debug.print(
                "full cluster mode={s} failure={s} class={s}\n",
                .{ Scenario.mode_names[mode_ordinal], failure.identity, @tagName(failure.class) },
            );
            if (recorded.observations.items.len > 0) for (recorded.observations.items[recorded.observations.items.len - 1].features) |feature| {
                std.debug.print("  {s}={d}\n", .{ feature.name, feature.value });
                if (std.mem.eql(u8, feature.name, Scenario.name ++ ".last-request-error") and feature.value != 0) {
                    const request_error: anyerror = @errorFromInt(@as(u16, @intCast(feature.value)));
                    std.debug.print("  request-error-name={s}\n", .{@errorName(request_error)});
                }
            };
        }
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
        replayed.deinit();
    }
}
