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
    pub const version: u32 = 2;

    const acknowledged_id = vopr.id.stable(name, "acknowledged-data-visible");
    const quorum_id = vopr.id.stable(name, "metadata-quorum-recovers");
    const routing_id = vopr.id.stable(name, "non-host-routing-sound");
    const isolation_id = vopr.id.stable(name, "multi-table-isolation");
    const publication_id = vopr.id.stable(name, "serverless-publication-visible");
    const shared_io_id = vopr.id.stable(name, "one-shared-vopr-io");
    const raft_wire_id = vopr.id.stable(name, "raft-crosses-vopr-http-wire");
    const node_resources_id = vopr.id.stable(name, "node-resource-owners-distinct");
    const cleanup_id = vopr.id.stable(name, "cluster-resources-cleaned");
    const complete_id = vopr.id.stable(name, "history-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = acknowledged_id, .name = name ++ ".acknowledged-data-visible", .kind = .always },
        .{ .id = quorum_id, .name = name ++ ".metadata-quorum-recovers", .kind = .always },
        .{ .id = routing_id, .name = name ++ ".non-host-routing-sound", .kind = .always },
        .{ .id = isolation_id, .name = name ++ ".multi-table-isolation", .kind = .always },
        .{ .id = publication_id, .name = name ++ ".serverless-publication-visible", .kind = .always },
        .{ .id = shared_io_id, .name = name ++ ".one-shared-vopr-io", .kind = .always },
        .{ .id = raft_wire_id, .name = name ++ ".raft-crosses-vopr-http-wire", .kind = .always },
        .{ .id = node_resources_id, .name = name ++ ".node-resource-owners-distinct", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".cluster-resources-cleaned", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".history-completes", .kind = .reachable },
    };

    const PublicFault = metadata_sim.VoprPublicClusterFixture.FaultMode;
    const Mode = enum {
        clean,
        metadata_partition,
        node_restart,
        partial_http_write,
        serverless_stale_generation,

        fn publicFault(self: Mode) PublicFault {
            return switch (self) {
                .clean, .serverless_stale_generation => .clean,
                .metadata_partition => .metadata_partition,
                .node_restart => .node_restart,
                .partial_http_write => .partial_http_write,
            };
        }

        fn serverlessMode(self: Mode) serverless_workflow.Scenario.Mode {
            return if (self == .serverless_stale_generation)
                .stale_enrichment_generation
            else
                .clean;
        }
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "clean"),
        vopr.id.stable(name, "metadata-partition"),
        vopr.id.stable(name, "node-restart"),
        vopr.id.stable(name, "partial-http-write"),
        vopr.id.stable(name, "serverless-stale-generation"),
    };
    const mode_names = [_][]const u8{
        name ++ ".clean",
        name ++ ".metadata-partition",
        name ++ ".node-restart",
        name ++ ".partial-http-write",
        name ++ ".serverless-stale-generation",
    };

    const State = struct {
        owner_alloc: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        sim: vopr.vopr_io.VoprIo,
        public_cluster: ?*metadata_sim.VoprPublicClusterFixture = null,
        serverless: *serverless_workflow.Scenario.Fixture,
        mode: ?Mode = null,
        initialization_done: bool = false,
        initialization_failed: bool = false,
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
                .tasks = .{ .stack_size = 4 * 1024 * 1024 },
                .network = .{ .max_sockets = 96, .stream_capacity = 256 * 1024 },
                .files = .{ .capacity_bytes = 64 * 1024 * 1024 },
                .instrumentation = .{ .enabled = false, .map_digest = 0x4655_4c4c },
            });
            errdefer self.sim.deinit();
            self.serverless = try serverless_workflow.Scenario.Fixture.initWithVoprIo(fixture_alloc, &self.sim);
            errdefer self.serverless.deinit();
            self.mode = null;
            self.public_cluster = null;
            self.initialization_done = false;
            self.initialization_failed = false;
            self.serverless_done = false;
            self.serverless_sound = false;
            self.shared_io_sound = false;
            self.complete = false;
            _ = self.sim.io().async(initializeAndRun, .{self});
            return self;
        }

        fn deinit(self: *State) void {
            self.serverless.deinit();
            if (self.public_cluster) |fixture| fixture.deinit();
            self.sim.deinit();
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            self.owner_alloc.destroy(self);
        }

        fn start(self: *State, mode: Mode) !void {
            self.mode = mode;
        }

        fn initializeAndRun(self: *State) void {
            const mode = self.mode orelse {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            const fixture = metadata_sim.VoprPublicClusterFixture.init(
                self.fixture_allocator.allocator(),
                &self.sim,
            ) catch {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            self.public_cluster = fixture;
            self.shared_io_sound = self.serverless.sim == &self.sim and fixture.sim == &self.sim;
            fixture.start(mode.publicFault()) catch {
                self.initialization_failed = true;
                self.initialization_done = true;
                self.complete = true;
                return;
            };
            _ = self.sim.io().async(runServerless, .{self});
            _ = self.sim.io().async(waitForCompletion, .{self});
            self.initialization_done = true;
        }

        fn runServerless(self: *State) void {
            const serverless_mode = self.mode.?.serverlessMode();
            self.serverless.runMode(serverless_mode) catch {
                self.serverless_done = true;
                return;
            };
            self.serverless_sound = self.serverless.workflowVisibleForMode(serverless_mode);
            self.serverless_done = true;
        }

        fn waitForCompletion(self: *State) void {
            while (!self.public_cluster.?.complete or !self.serverless_done) {
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
        const requests_ok = if (state.public_cluster) |fixture| fixture.healthSnapshot().requests_ok else false;
        try events.emitNamed(allocator, .domain, selected.name, @intFromBool(requests_ok));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const fixture = state.public_cluster;
        const cluster = if (fixture) |public_cluster| public_cluster.healthSnapshot() else null;
        const resources = state.sim.resourceSnapshot();
        try builder.addNamed(allocator, name ++ ".mode", if (state.mode) |mode| @as(i64, @intFromEnum(mode)) + 1 else 0);
        try builder.addNamed(allocator, name ++ ".data-hosts", if (cluster) |snapshot| @intCast(snapshot.hosts) else 0);
        try builder.addNamed(allocator, name ++ ".public-requests-ok", @intFromBool(if (cluster) |snapshot| snapshot.requests_ok else false));
        try builder.addNamed(allocator, name ++ ".write-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.write_sound else false));
        try builder.addNamed(allocator, name ++ ".read-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.read_sound else false));
        try builder.addNamed(allocator, name ++ ".tenant-write-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_write_sound else false));
        try builder.addNamed(allocator, name ++ ".tenant-read-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_read_sound else false));
        try builder.addNamed(allocator, name ++ ".table-isolation-ok", @intFromBool(if (fixture) |public_cluster| public_cluster.table_isolation_sound else false));
        try builder.addNamed(allocator, name ++ ".request-errors", if (fixture) |public_cluster| @intCast(public_cluster.request_errors) else 0);
        try builder.addNamed(allocator, name ++ ".last-request-error", if (fixture) |public_cluster| @intCast(public_cluster.last_request_error_code) else 0);
        try builder.addNamed(allocator, name ++ ".serverless-visible", @intFromBool(state.serverless_sound));
        try builder.addNamed(allocator, name ++ ".raft-wire-requests", if (cluster) |snapshot| @intCast(snapshot.raft_wire_requests) else 0);
        try builder.addNamed(allocator, name ++ ".node-resource-managers", if (cluster) |snapshot| @intCast(snapshot.node_resource_managers) else 0);
        try builder.addNamed(allocator, name ++ ".initialization-failed", @intFromBool(state.initialization_failed));
        try builder.addNamed(allocator, name ++ ".open-sockets", @intCast(resources.open_sockets));
        try builder.addNamed(allocator, name ++ ".active-tasks", @intCast(resources.active_tasks));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const fixture = state.public_cluster;
        const cluster = if (fixture) |public_cluster| public_cluster.healthSnapshot() else null;
        const resources = state.sim.resourceSnapshot();
        try sink.check(allocator, acknowledged_id, !state.complete or (cluster != null and cluster.?.requests_ok));
        try sink.check(allocator, quorum_id, !state.complete or (cluster != null and cluster.?.topology_ok));
        try sink.check(allocator, routing_id, !state.complete or (cluster != null and cluster.?.hosts == 2));
        try sink.check(allocator, isolation_id, !state.complete or (fixture != null and fixture.?.table_isolation_sound));
        try sink.check(allocator, publication_id, !state.complete or state.serverless_sound);
        try sink.check(allocator, shared_io_id, !state.initialization_done or state.shared_io_sound);
        try sink.check(allocator, raft_wire_id, !state.complete or (cluster != null and cluster.?.raft_wire_requests > 0));
        try sink.check(allocator, node_resources_id, !state.initialization_done or (cluster != null and cluster.?.node_resource_managers == 3));
        try sink.check(allocator, cleanup_id, !state.complete or
            (cluster != null and cluster.?.cleanup_ok and resources.open_sockets == 0));
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        const fixture = state.public_cluster;
        const cluster = if (fixture) |public_cluster| public_cluster.healthSnapshot() else null;
        return state.sim.healthSnapshot(.{
            .progress_expected = state.mode != null,
            .progress_units = @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.write_finished else false)) +
                @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.read_finished else false)) +
                @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_write_finished else false)) +
                @as(u64, @intFromBool(if (fixture) |public_cluster| public_cluster.tenant_read_finished else false)) +
                @as(u64, @intFromBool(state.serverless_done)),
            .recovery_expected = state.mode != null and state.mode.? != .clean,
            .recovery_complete = state.complete and cluster != null and cluster.?.topology_ok,
            .consistency_valid = !state.initialization_failed and (cluster == null or cluster.?.requests_ok) and (!state.serverless_done or state.serverless_sound),
            .cleanup_complete = state.complete and cluster != null and cluster.?.cleanup_ok,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "full cluster VOPR exact replays metadata data serverless HTTP clients and recovery" {
    // Stackful fibers make host unwinding both expensive and unsafe. Preserve
    // leak and ownership checking while keeping stack capture disabled, as the
    // production-sized fixtures below already do.
    var history_allocator: FixtureAllocator = .init;
    defer std.debug.assert(history_allocator.deinit() == .ok);
    const history_alloc = history_allocator.allocator();
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, mode_ordinal| {
        var choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0x4655_4c4c + mode_ordinal);
        var recorded = try vopr.runner.run(Scenario, history_alloc, choices.source(), .{
            .system = "antfly",
            .transition_budget = 20_000,
            .resource_budget = 96,
            .backend_ids = &backend_ids,
            .source_revision = "full-cluster-vopr-v2",
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
        var replayed = try vopr.replay.exact(Scenario, history_alloc, &recorded);
        replayed.deinit();
    }
}
