// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production graph maintenance under replayable operation ordering and time.
//! The real in-memory LSM implements graph storage. Native process death and
//! filesystem durability remain integration-test responsibilities.
const std = @import("std");
const vopr = @import("vopr");
const graph_mod = @import("../graph/graph.zig");
const Graph = graph_mod.GraphIndex;
const clock_mod = @import("antfly_platform").clock;

const families = [_]graph_mod.GraphMetricKind{ .degree, .pagerank, .eigenvector, .hits_authority };
const safety = vopr.id.stable("property", "index-maintenance.fenced-publication");
const complete = vopr.id.stable("property", "index-maintenance.complete");
const Step = enum { configure, baseline, start, prepare, claim, blocked, expire, reclaim, stale, drain, pause, resume_work, mutate, fail, cleanup, restart, exhaust, finish };
fn id(step: Step) u64 {
    return vopr.id.stable("transition", @tagName(step));
}

pub const Scenario = struct {
    pub const name: []const u8 = "index-maintenance";
    pub const version: u32 = 1;
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = safety, .name = name ++ ".fenced-publication", .kind = .always },
        .{ .id = complete, .name = name ++ ".complete", .kind = .reachable },
    };
    pub const State = struct {
        clock: clock_mod.ManualClock = .{},
        graph: ?Graph = null,
        configs: [2]graph_mod.GraphMetricConfig = undefined,
        step: Step = .configure,
        family: u8 = 0,
        phase: Graph.GraphMetricBuildPhase = .idle,
        iteration: u32 = 0,
        job: u64 = 0,
        page: u64 = 0,
        attempt: u64 = 0,
        expires: u64 = 0,
        total: u64 = 0,
        published: u64 = 0,
        prior: u64 = 0,
        progress: u64 = 0,
        prepare_steps: u16 = 0,
        fault_checks: u8 = 0,
        exhausted_attempts: u8 = 0,
        valid: bool = true,
    };
    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        state.* = .{};
        state.clock.setRealtimeNs(1_000 * std.time.ns_per_ms);
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        if (world.state.graph) |*graph| graph.close();
        allocator.destroy(world.state);
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        const state = world.state;
        if (state.step == .finish) return;
        if (state.step == .configure) {
            inline for (families) |family| try list.append(allocator, .{
                .id = vopr.id.stable("transition", "index-maintenance." ++ @tagName(family)),
                .name = "index-maintenance." ++ @tagName(family),
                .kind = .workload,
            });
        } else if (state.step == .prepare) {
            // Explore where the owner disappears, including later iterative
            // phases. The cap forces a fault before an unbounded history.
            try list.append(allocator, .{ .id = id(.claim), .name = "index-maintenance.claim-and-lose-worker", .kind = .fault });
            if (state.prepare_steps < 6) try list.append(allocator, .{ .id = id(.prepare), .name = "index-maintenance.advance-before-loss", .kind = .maintenance });
        } else {
            try list.append(allocator, .{ .id = id(state.step), .name = @tagName(state.step), .kind = .maintenance });
        }
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, _: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.step == .configure) {
            inline for (families, 0..) |family, i| {
                if (selected.id == vopr.id.stable("transition", "index-maintenance." ++ @tagName(family))) state.family = i;
            }
            state.configs = .{
                .{ .name = "metric", .kind = families[state.family], .refresh = .manual, .max_iterations = 3 },
                .{ .name = "hub", .kind = .hits_hub, .refresh = .manual, .max_iterations = 3 },
            };
            state.graph = try Graph.openWithPrivateStores(allocator, "vopr-forward", "vopr-reverse", "graph", .{
                .reverse_backend = .lsm_memory,
                .clock = state.clock.clock(),
                .metric_configs = state.configs[0..if (state.family == 3) @as(usize, 2) else 1],
            });
            try state.graph.?.addEdge("a", "b", "link", 1, 0, 0, "");
            try state.graph.?.addEdge("b", "c", "link", 1, 0, 0, "");
            state.step = .baseline;
        } else {
            const graph = &state.graph.?;
            switch (state.step) {
                .baseline => {
                    var status = try graph.runGraphMetricPlannedDrain("metric", graph.edge_generation, .{ .worker_ids = &.{"healthy"}, .max_steps = 512 });
                    defer status.deinit(allocator);
                    state.valid = state.valid and status.state == .fresh;
                    state.prior = status.published_generation;
                    try graph.addEdge("c", "a", "link", 1, 0, 0, "");
                    state.step = .start;
                },
                .start => {
                    var status = try graph.ensureGraphMetricPlannedBuild("metric", graph.edge_generation);
                    defer status.deinit(allocator);
                    state.step = .prepare;
                },
                .prepare => {
                    if (selected.id == id(.claim)) {
                        state.step = .claim;
                    } else {
                        _ = try graph.runGraphMetricPlannedWorkerPageStepForMetric("metric", "healthy");
                        _ = try graph.runGraphMetricPlannedCoordinatorStepForMetric("metric");
                        state.prepare_steps += 1;
                    }
                },
                .claim => {
                    var status = try graph.graphMetricStatus("metric");
                    defer status.deinit(allocator);
                    state.job = status.build_job_id;
                    state.phase = status.phase;
                    state.iteration = status.build_iteration;
                    if (try graph.claimNextGraphMetricBuildPageAt("metric", state.job, state.phase, state.iteration, "lost", state.clock.clock().nowRealtimeMs())) |page| {
                        state.page = page.page_id;
                        state.attempt = page.attempt;
                        state.expires = page.lease_expires_at_ms;
                        state.total = page.total_units;
                        _ = try graph.updateGraphMetricBuildPageProgressForAttempt("metric", state.job, state.phase, state.iteration, state.page, "lost", state.attempt, "interrupted", 0, state.total);
                        state.step = .blocked;
                    } else {
                        _ = try graph.runGraphMetricPlannedCoordinatorStepForMetric("metric");
                        if (status.state == .fresh) return error.FaultBoundaryNotReached;
                    }
                },
                .blocked => {
                    const early = try graph.claimNextGraphMetricBuildPageAt("metric", state.job, state.phase, state.iteration, "replacement", state.expires - 1);
                    state.valid = state.valid and early == null;
                    state.fault_checks += 1;
                    state.step = .expire;
                },
                .expire => {
                    state.clock.setRealtimeNs((state.expires + 1) * std.time.ns_per_ms);
                    state.step = .reclaim;
                },
                .reclaim => {
                    const page = try graph.claimNextGraphMetricBuildPageAt("metric", state.job, state.phase, state.iteration, "replacement", state.clock.clock().nowRealtimeMs()) orelse return error.ReclaimFailed;
                    state.valid = state.valid and page.page_id == state.page and page.attempt == state.attempt + 1 and page.cursor.len == 0;
                    state.fault_checks += 1;
                    state.step = .stale;
                },
                .stale => {
                    const stale = graph.completeGraphMetricBuildPageForAttempt("metric", state.job, state.phase, state.iteration, state.page, "lost", state.attempt, state.total, 0);
                    if (stale) |_| {
                        state.valid = false;
                    } else |err| {
                        state.valid = state.valid and err == error.GraphMetricBuildPageNotLeased;
                    }
                    state.fault_checks += 1;
                    state.step = .drain;
                },
                .drain => {
                    var status = try graph.runGraphMetricPlannedDrain("metric", graph.edge_generation, .{ .worker_ids = &.{"replacement"}, .max_steps = 512 });
                    defer status.deinit(allocator);
                    state.valid = state.valid and status.state == .fresh and status.published_generation > state.prior;
                    state.prior = status.published_generation;
                    state.step = .pause;
                },
                .pause => {
                    var status = try graph.pauseGraphMetricMaintenance("metric");
                    defer status.deinit(allocator);
                    state.valid = state.valid and status.maintenance_paused;
                    state.step = .resume_work;
                },
                .resume_work => {
                    var status = try graph.resumeGraphMetricMaintenance("metric");
                    defer status.deinit(allocator);
                    state.valid = state.valid and !status.maintenance_paused;
                    state.step = .mutate;
                },
                .mutate => {
                    try graph.addEdge("d", "a", "link", 1, 0, 0, "");
                    var status = try graph.ensureGraphMetricPlannedBuild("metric", graph.edge_generation);
                    defer status.deinit(allocator);
                    state.step = .fail;
                },
                .fail => {
                    var status = try graph.failGraphMetricPlannedBuild("metric", error.InjectedPublicationFailure);
                    defer status.deinit(allocator);
                    state.valid = state.valid and status.state == .failed and status.published_generation == state.prior;
                    state.fault_checks += 1;
                    state.step = .cleanup;
                },
                .cleanup => {
                    if (!try graph.cleanupFailedGraphMetricBuildJobPage("metric")) state.step = .restart;
                },
                .restart => {
                    var status = try graph.ensureGraphMetricPlannedBuild("metric", graph.edge_generation);
                    defer status.deinit(allocator);
                    _ = try graph.runGraphMetricPlannedWorkerPageStepForMetric("metric", "prepare");
                    _ = try graph.runGraphMetricPlannedCoordinatorStepForMetric("metric");
                    var active = try graph.graphMetricStatus("metric");
                    defer active.deinit(allocator);
                    state.job = active.build_job_id;
                    state.phase = active.phase;
                    state.iteration = active.build_iteration;
                    state.step = .exhaust;
                },
                .exhaust => {
                    var status = try graph.graphMetricStatus("metric");
                    defer status.deinit(allocator);
                    if (try graph.claimNextGraphMetricBuildPageAt("metric", state.job, state.phase, state.iteration, "repeatedly-lost", state.clock.clock().nowRealtimeMs())) |page| {
                        state.exhausted_attempts += 1;
                        if (state.exhausted_attempts > 16) return error.UnboundedPageRetries;
                        state.clock.setRealtimeNs((page.lease_expires_at_ms + 1) * std.time.ns_per_ms);
                    } else {
                        const result = try graph.runGraphMetricPlannedCoordinatorStepForMetric("metric");
                        var failed = try graph.graphMetricStatus("metric");
                        defer failed.deinit(allocator);
                        state.valid = state.valid and result.failed_build and failed.published_generation == state.prior and
                            std.mem.indexOf(u8, failed.last_error, "GraphMetricBuildPageAttemptsExhausted") != null;
                        state.fault_checks += 1;
                        state.step = .finish;
                    }
                },
                else => unreachable,
            }
        }
        if (state.graph) |*graph| {
            var status = try graph.graphMetricStatus("metric");
            defer status.deinit(allocator);
            state.valid = state.valid and status.published_generation >= state.published;
            state.published = status.published_generation;
            if (state.family == 3) {
                var paired = try graph.graphMetricStatus("hub");
                defer paired.deinit(allocator);
                state.valid = state.valid and paired.published_generation == status.published_generation and
                    paired.computed_at_ms == status.computed_at_ms and paired.maintenance_paused == status.maintenance_paused;
            }
        }
        state.progress += 1;
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, "step", @intCast(@intFromEnum(state.step)));
        try builder.addNamed(allocator, "family", @intCast(state.family));
        try builder.addNamed(allocator, "time", @intCast(state.clock.clock().nowRealtimeMs()));
        try builder.addNamed(allocator, "fault-checks", @intCast(state.fault_checks));
        if (state.graph) |*graph| {
            var status = try graph.graphMetricStatus("metric");
            defer status.deinit(allocator);
            try builder.addNamed(allocator, "published", @intCast(status.published_generation));
            try builder.addNamed(allocator, "computed-at", @intCast(status.computed_at_ms));
            try builder.addNamed(allocator, "phase", @intCast(@intFromEnum(status.phase)));
            try builder.addNamed(allocator, "iteration", @intCast(status.build_iteration));
            try builder.addNamed(allocator, "pages", @intCast(status.build_pages.len));
            if (state.family == 3) {
                var paired = try graph.graphMetricStatus("hub");
                defer paired.deinit(allocator);
                try builder.addNamed(allocator, "paired-published", @intCast(paired.published_generation));
            }
        }
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, safety, world.state.valid);
        try sink.check(allocator, complete, world.state.step == .finish and world.state.fault_checks == 5);
    }
    pub fn done(world: *World) bool {
        return world.state.step == .finish;
    }
};

fn checkSeed(allocator: std.mem.Allocator, seed: u64) !u8 {
    _ = try checkScenarioSeed(OwnerScenario, allocator, seed);
    return checkScenarioSeed(Scenario, allocator, seed);
}

fn checkScenarioSeed(comptime Selected: type, allocator: std.mem.Allocator, seed: u64) !u8 {
    var source = vopr.choice.Seeded.init(seed);
    var artifact = try vopr.runner.run(Selected, allocator, source.source(), .{ .system = "antfly", .seed = seed, .transition_budget = 256, .source_revision = "index-maintenance-v1", .target = "native", .optimize = @tagName(@import("builtin").mode) });
    defer artifact.deinit();
    if (artifact.summary.?.property_failures != 0) {
        const rendered = try artifact.renderAlloc(allocator);
        defer allocator.free(rendered);
        std.debug.print("failed {s} seed={d}\n{s}\n", .{ Selected.name, seed, rendered });
    }
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try vopr.replay.exact(Selected, allocator, &artifact);
    defer replayed.deinit();
    if (Selected == Scenario) {
        inline for (families, 0..) |family, i| {
            if (artifact.transitions.items[0].id == vopr.id.stable("transition", "index-maintenance." ++ @tagName(family))) return @as(u8, 1) << i;
        }
        return error.MissingMetricFamily;
    }
    return 0;
}

test "index maintenance VOPR regression exact replay" {
    _ = try checkSeed(std.testing.allocator, 1);
}

test "index maintenance VOPR campaign exact replay" {
    var families_seen: u8 = 0;
    for (1..33) |seed| families_seen |= try checkSeed(std.testing.allocator, seed);
    try std.testing.expectEqual(@as(u8, 0b1111), families_seen);
}

/// Runtime-owner fencing uses the production DB/runtime and the same explicit
/// differential storage boundary as the existing DB/index VOPR scenarios.
pub const OwnerScenario = struct {
    pub const name: []const u8 = "index-maintenance-ownership";
    pub const version: u32 = 1;
    const owner_safe = vopr.id.stable("property", "index-maintenance.owner-fencing");
    const owner_done = vopr.id.stable("property", "index-maintenance.owner-complete");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = owner_safe, .name = name ++ ".fenced", .kind = .always },
        .{ .id = owner_done, .name = name ++ ".complete", .kind = .reachable },
    };
    const Fixture = @import("db_index_races.zig").Fixture;
    const Runtime = @import("../storage/db/maintenance/graph_metric_runtime.zig").GraphMetricRuntime;
    const State = struct {
        fixture: Fixture,
        clock: clock_mod.ManualClock = .{},
        owners: [2]?Runtime = .{ null, null },
        stage: u8 = 0,
        stale_closed: bool = false,
        valid: bool = true,
    };
    pub const World = struct { state: *State };
    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{ .fixture = try Fixture.init(allocator) };
        errdefer state.fixture.deinit();
        state.clock.setRealtimeNs(1000 * std.time.ns_per_ms);
        const resources = state.fixture.db.core.asyncResources();
        for (&state.owners, 0..) |*owner, i| {
            owner.* = Runtime.init(allocator, resources.store, resources.index_manager, resources.apply_mutex, state.fixture.db.backend_runtime, .{
                .enabled = true,
                .start_background_loop = false,
                .role = .coordinator,
                .runtime_id = if (i == 0) "original" else "replacement",
                .owner_id = if (i == 0) "original" else "replacement",
                .lease_owned = true,
                .lease_ttl_ms = 100,
                .clock = state.clock.clock(),
            }) catch |err| {
                for (state.owners[0..i]) |*prior| if (prior.*) |*runtime| runtime.deinit();
                return err;
            };
        }
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        for (&world.state.owners) |*owner| if (owner.*) |*runtime| runtime.deinit();
        world.state.fixture.deinit();
        allocator.destroy(world.state);
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.stage >= 6) return;
        try list.append(allocator, .{ .id = world.state.stage + 1, .name = "owner-transition", .kind = .maintenance });
        if (world.state.stage == 4 and !world.state.stale_closed) try list.append(allocator, .{ .id = 100, .name = "stale-close-before-tick", .kind = .fault });
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, _: *vopr.event.Sink, _: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (selected.id == 100) {
            state.owners[0].?.deinit();
            state.owners[0] = null;
            state.stale_closed = true;
            return .applied();
        }
        switch (state.stage) {
            0 => {
                _ = try state.owners[0].?.runOnceDetailed();
                state.valid = state.valid and state.owners[0].?.stats().has_lease;
            },
            1 => {
                _ = try state.owners[1].?.runOnceDetailed();
                state.valid = state.valid and !state.owners[1].?.stats().has_lease;
            },
            2 => state.clock.advanceMs(101),
            3 => {
                _ = try state.owners[1].?.runOnceDetailed();
                state.valid = state.valid and state.owners[1].?.stats().takeover_count == 1;
            },
            4 => {
                if (state.owners[0]) |*owner| {
                    _ = try owner.runOnceDetailed();
                    state.valid = state.valid and !owner.stats().has_lease and owner.stats().lost_leases == 1;
                    owner.deinit();
                    state.owners[0] = null;
                }
                _ = try state.owners[1].?.runOnceDetailed();
                state.valid = state.valid and state.owners[1].?.stats().has_lease and state.owners[1].?.stats().acquisition_count == 1;
            },
            5 => {
                state.owners[1].?.deinit();
                state.owners[1] = null;
                const runtime_mod = @import("../storage/db/maintenance/graph_metric_runtime.zig");
                const lease_mod = @import("../storage/db/lease.zig");
                var lease = try lease_mod.Lease.init(state.fixture.allocator, state.fixture.db.core.asyncResources().store, runtime_mod.default_coordinator_lease_key);
                defer lease.deinit();
                if (try lease.load(state.fixture.allocator)) |record_value| {
                    var record = record_value;
                    lease_mod.deinitRecord(state.fixture.allocator, &record);
                    state.valid = false;
                }
            },
            else => unreachable,
        }
        state.stage += 1;
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, "stage", world.state.stage);
        try builder.addNamed(allocator, "stale-closed", @intFromBool(world.state.stale_closed));
        try builder.addNamed(allocator, "time", @intCast(world.state.clock.clock().nowRealtimeMs()));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, owner_safe, world.state.valid);
        try sink.check(allocator, owner_done, done(world));
    }
    pub fn done(world: *World) bool {
        return world.state.stage == 6;
    }
};
