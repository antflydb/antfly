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
const Step = enum { configure, baseline, start, prepare, coordinate, claim, leased, lose, blocked, expire, reclaim, stale, drain, pause, resume_work, mutate, fail, cleanup, restart, exhaust, finish };
fn id(step: Step) u64 {
    return vopr.id.stable("transition", @tagName(step));
}

pub const Scenario = struct {
    pub const name: []const u8 = "index-maintenance";
    pub const version: u32 = 2;
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
        lost_worker: bool = false,
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
            var status = try state.graph.?.graphMetricStatus("metric");
            defer status.deinit(allocator);
            if (status.state == .fresh) {
                try list.append(allocator, .{ .id = id(.drain), .name = "index-maintenance.finished-normally", .kind = .maintenance });
            } else {
                // Acquisition may find no work (for example, coordinator-only
                // phases). Only a successful acquisition enables worker loss.
                try list.append(allocator, .{ .id = id(.claim), .name = "index-maintenance.acquire-page", .kind = .maintenance, .parameter = @as(i64, @intFromEnum(status.phase)) | (@as(i64, status.build_iteration) << 32) });
                try list.append(allocator, .{ .id = id(.prepare), .name = "index-maintenance.advance-worker", .kind = .maintenance });
                try list.append(allocator, .{ .id = id(.coordinate), .name = "index-maintenance.advance-coordinator", .kind = .maintenance });
            }
        } else if (state.step == .leased) {
            try list.append(allocator, .{ .id = id(.lose), .name = "index-maintenance.lose-leased-worker", .kind = .fault, .parameter = @intFromEnum(state.phase) });
            try list.append(allocator, .{ .id = id(.prepare), .name = "index-maintenance.complete-leased-work", .kind = .maintenance });
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
                        try acquirePage(state, allocator);
                    } else if (selected.id == id(.drain)) {
                        state.step = .drain;
                    } else if (selected.id == id(.coordinate)) {
                        _ = try graph.runGraphMetricPlannedCoordinatorStepForMetric("metric");
                    } else {
                        _ = try graph.runGraphMetricPlannedWorkerPageStepForMetric("metric", "healthy");
                        _ = try graph.runGraphMetricPlannedCoordinatorStepForMetric("metric");
                        state.prepare_steps += 1;
                    }
                },
                .leased => {
                    if (selected.id == id(.lose)) {
                        _ = try graph.updateGraphMetricBuildPageProgressForAttempt("metric", state.job, state.phase, state.iteration, state.page, "healthy", state.attempt, "interrupted", 0, state.total);
                        state.lost_worker = true;
                        state.step = .blocked;
                    } else {
                        const completed_work = try graph.runGraphMetricPlannedWorkerPageStepForMetric("metric", "healthy");
                        // Cleanup can retire its own page record on completion.
                        if (completed_work.completed_build or (completed_work.completed_page and completed_work.phase == state.phase and completed_work.page_id == state.page)) {
                            state.step = .prepare;
                        } else {
                            const page = try graph.graphMetricBuildPageSnapshotForTest("metric", state.job, state.phase, state.iteration, state.page) orelse return error.MissingLeasedPage;
                            if (page.state == .complete) state.step = .prepare;
                        }
                    }
                },
                .blocked => {
                    const early = try graph.claimNextGraphMetricBuildPageAt("metric", state.job, state.phase, state.iteration, "replacement", state.expires - 1);
                    // Another pending page may be claimable concurrently; the
                    // exclusion property concerns this worker's leased page.
                    state.valid = state.valid and (early == null or early.?.page_id != state.page);
                    const owned = try graph.graphMetricBuildPageSnapshotForTest("metric", state.job, state.phase, state.iteration, state.page) orelse return error.MissingLeasedPage;
                    state.valid = state.valid and owned.state == .leased and owned.attempt == state.attempt and
                        owned.worker_id_hash == std.hash.Wyhash.hash(0, "healthy");
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
                    const stale = graph.completeGraphMetricBuildPageForAttempt("metric", state.job, state.phase, state.iteration, state.page, "healthy", state.attempt, state.total, 0);
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
    fn acquirePage(state: *State, allocator: std.mem.Allocator) !void {
        const graph = &state.graph.?;
        var status = try graph.graphMetricStatus("metric");
        defer status.deinit(allocator);
        state.job = status.build_job_id;
        state.phase = status.phase;
        state.iteration = if (status.phase == .cleanup_old_generations) 0 else status.build_iteration;
        if (try graph.claimNextGraphMetricBuildPageAt("metric", state.job, state.phase, state.iteration, "healthy", state.clock.clock().nowRealtimeMs())) |page| {
            state.page = page.page_id;
            state.attempt = page.attempt;
            state.expires = page.lease_expires_at_ms;
            state.total = page.total_units;
            state.step = .leased;
        }
    }
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        const state = world.state;
        const logical = try std.json.Stringify.valueAlloc(sink.allocator, .{
            .step = @tagName(state.step),
            .family = @tagName(families[state.family]),
            .time_ms = state.clock.clock().nowRealtimeMs(),
            .job = state.job,
            .phase = @tagName(state.phase),
            .iteration = state.iteration,
            .page = state.page,
            .attempt = state.attempt,
            .lease_expires_ms = state.expires,
            .lost_worker = state.lost_worker,
            .fault_checks = state.fault_checks,
            .valid = state.valid,
        }, .{});
        defer sink.allocator.free(logical);
        try sink.add("maintenance", logical);
        if (state.graph) |*graph| {
            var status = try graph.graphMetricStatus("metric");
            defer status.deinit(graph.alloc);
            const encoded = try std.json.Stringify.valueAlloc(sink.allocator, status, .{});
            defer sink.allocator.free(encoded);
            try sink.add("metric", encoded);
            if (state.job != 0) {
                const page = try graph.graphMetricBuildPageSnapshotForTest("metric", state.job, state.phase, state.iteration, state.page);
                const page_json = try std.json.Stringify.valueAlloc(sink.allocator, page, .{});
                defer sink.allocator.free(page_json);
                try sink.add("selected-page", page_json);
            }

            if (state.family == 3) {
                var paired = try graph.graphMetricStatus("hub");
                defer paired.deinit(graph.alloc);
                const pair_json = try std.json.Stringify.valueAlloc(sink.allocator, paired, .{});
                defer sink.allocator.free(pair_json);
                try sink.add("paired-metric", pair_json);
            }
        }
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
        const state = world.state;
        try sink.check(allocator, safety, state.valid and (state.step != .finish or
            state.fault_checks == @as(u8, if (state.lost_worker) 5 else 2)));
        try sink.check(allocator, complete, world.state.step == .finish);
    }
    pub fn done(world: *World) bool {
        return world.state.step == .finish;
    }
};

const Coverage = struct { families: u8 = 0, fault_families: u8 = 0, phases: u16 = 0 };

fn checkSeed(allocator: std.mem.Allocator, seed: u64) !Coverage {
    _ = try checkScenarioSeed(OwnerScenario, allocator, seed);
    return checkScenarioSeed(Scenario, allocator, seed);
}

fn checkScenarioSeed(comptime Selected: type, allocator: std.mem.Allocator, seed: u64) !Coverage {
    var source = vopr.choice.Seeded.init(seed);
    var artifact = try vopr.runner.run(Selected, allocator, source.source(), .{ .system = "antfly", .seed = seed, .transition_budget = 256, .source_revision = "index-maintenance-v2", .target = "native", .optimize = @tagName(@import("builtin").mode) });
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
            if (artifact.transitions.items[0].id == vopr.id.stable("transition", "index-maintenance." ++ @tagName(family))) {
                var coverage = Coverage{ .families = @as(u8, 1) << i };
                for (artifact.transitions.items) |transition| {
                    if (transition.id == id(.lose)) {
                        coverage.fault_families = coverage.families;
                        coverage.phases |= @as(u16, 1) << @as(u4, @intCast(transition.parameter));
                    }
                }
                return coverage;
            }
        }
        return error.MissingMetricFamily;
    }
    return .{};
}

test "index maintenance VOPR regression exact replay" {
    _ = try checkSeed(std.testing.allocator, 1);
}

test "index maintenance VOPR campaign exact replay" {
    var coverage = Coverage{};
    for (1..33) |seed| {
        const seen = try checkSeed(std.testing.allocator, seed);
        coverage.families |= seen.families;
        coverage.fault_families |= seen.fault_families;
        coverage.phases |= seen.phases;
    }
    // Coverage is a campaign obligation, not a safety property of each history.
    try std.testing.expectEqual(@as(u8, 0b1111), coverage.families);
    try std.testing.expectEqual(@as(u8, 0b1111), coverage.fault_families);
    try std.testing.expect(@popCount(coverage.phases) >= 3);
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
    pub fn collect(world: *World, sink: *vopr.collector.Sink) !void {
        const state = world.state;
        const logical = try std.json.Stringify.valueAlloc(sink.allocator, .{
            .stage = state.stage,
            .stale_closed = state.stale_closed,
            .time_ms = state.clock.clock().nowRealtimeMs(),
            .valid = state.valid,
        }, .{});
        defer sink.allocator.free(logical);
        try sink.add("ownership", logical);
        for (&state.owners, 0..) |*owner, i| {
            const owner_name = if (i == 0) "original" else "replacement";
            if (owner.*) |*runtime| {
                const encoded = try std.json.Stringify.valueAlloc(sink.allocator, runtime.stats(), .{});
                defer sink.allocator.free(encoded);
                try sink.add(owner_name, encoded);
            } else try sink.add(owner_name, "closed");
        }
        const lease_mod = @import("../storage/db/lease.zig");
        var lease = try lease_mod.Lease.init(state.fixture.allocator, state.fixture.db.core.asyncResources().store, @import("../storage/db/maintenance/graph_metric_runtime.zig").default_coordinator_lease_key);
        defer lease.deinit();
        if (try lease.load(state.fixture.allocator)) |value| {
            var record = value;
            defer lease_mod.deinitRecord(state.fixture.allocator, &record);
            const encoded = try std.json.Stringify.valueAlloc(sink.allocator, record, .{});
            defer sink.allocator.free(encoded);
            try sink.add("durable-lease", encoded);
        } else try sink.add("durable-lease", "absent");
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

/// Directed boundary histories use the same enabled transitions as seeded
/// campaigns. Their policies select opportunities, never mutate the world.
const BoundaryChoices = struct {
    family: graph_mod.GraphMetricKind,
    fault_phase: ?Graph.GraphMetricBuildPhase = null,
    later_iteration: bool = false,
    advanced: usize = 0,
    lost: bool = false,
    complete_acquired: bool = false,
    acquired: bool = false,
    fn source(self: *@This()) vopr.choice.Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }
    fn choose(ptr: *anyopaque, req: vopr.choice.Request) !u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        inline for (families) |family| {
            if (self.family == family) for (req.enabled) |candidate| {
                if (candidate.id == vopr.id.stable("transition", "index-maintenance." ++ @tagName(family))) return candidate.id;
            };
        }
        if (self.fault_phase) |phase| {
            for (req.enabled) |candidate| {
                if (candidate.id == id(.claim) and (candidate.parameter & 0xffffffff) == @intFromEnum(phase) and
                    (!self.later_iteration or candidate.parameter >> 32 > 0) and !self.acquired)
                {
                    self.acquired = true;
                    return candidate.id;
                }
                if (candidate.id == id(.lose) and !self.complete_acquired) {
                    self.lost = true;
                    return candidate.id;
                }
            }
        }
        for (req.enabled) |candidate| {
            if (candidate.id == id(.prepare)) {
                self.advanced += 1;
                return candidate.id;
            }
        }
        return req.enabled[0].id;
    }
    fn finish(_: *anyopaque) !void {}
};

fn CollectingScenario(comptime Base: type) type {
    return struct {
        pub const name = Base.name;
        pub const version = Base.version;
        pub const properties = Base.properties;
        pub const World = Base.World;
        pub const deinit = Base.deinit;
        pub const enumerate = Base.enumerate;
        pub const observe = Base.observe;
        pub const evaluate = Base.evaluate;
        pub const done = Base.done;
        fn inspect(world: *World, allocator: std.mem.Allocator) !void {
            // Collector output may have a shorter lifetime than the live
            // world. Keep its allocator distinct to catch ownership mistakes.
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            var sink = vopr.collector.Sink.init(arena.allocator(), 0);
            defer sink.deinit();
            try Base.collect(world, &sink);
            try std.testing.expect(sink.records.items.len > 0);
        }
        pub fn init(allocator: std.mem.Allocator) !World {
            var world = try Base.init(allocator);
            errdefer Base.deinit(&world, allocator);
            try inspect(&world, allocator);
            return world;
        }
        pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
            const result = try Base.execute(world, selected, events, allocator);
            try inspect(world, allocator);
            return result;
        }
    };
}

fn verifyDiagnostics(comptime Selected: type, allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !void {
    const before = try artifact.renderAlloc(allocator);
    defer allocator.free(before);
    // Inspect initial, lease, fault/reclaim and terminal boundaries without
    // repeating every healthy worker step in the diagnostic matrix.
    for (0..artifact.choices.items.len + 1) |prefix| {
        if (Selected == Scenario and prefix != 0 and prefix != artifact.choices.items.len) {
            const transition = artifact.transitions.items[prefix - 1].id;
            if (transition != id(.claim) and transition != id(.lose) and transition != id(.reclaim)) continue;
        }
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var collected = vopr.collector.Sink.init(arena.allocator(), prefix);
        defer collected.deinit();
        try vopr.runner.collectAt(Selected, allocator, artifact, prefix, &collected);
        try std.testing.expect(collected.records.items.len > 0);
    }
    var replayed = try vopr.replay.exact(CollectingScenario(Selected), allocator, artifact);
    defer replayed.deinit();
    const after = try replayed.renderAlloc(allocator);
    defer allocator.free(after);
    try std.testing.expectEqualStrings(before, after);
}

test "index maintenance VOPR boundary histories permit normal completion and late worker loss" {
    const allocator = std.testing.allocator;
    for (families) |family| {
        for (0..4) |mode| {
            const inject = mode == 1 or mode == 3;
            var choices = BoundaryChoices{
                .family = family,
                .fault_phase = if (mode == 3) .cleanup_old_generations else if (mode != 0) (if (family == .degree) .reduce_ranks else .publish_generation) else null,
                .complete_acquired = mode == 2,
            };
            var artifact = try vopr.runner.run(Scenario, allocator, choices.source(), .{ .system = "antfly", .transition_budget = 512, .source_revision = "index-maintenance-v2", .target = "native", .optimize = @tagName(@import("builtin").mode) });
            defer artifact.deinit();
            if (inject != choices.lost or artifact.summary.?.property_failures != 0) {
                std.debug.print("boundary history family={s} mode={d} inject={} steps={d} advances={d}\n", .{ @tagName(family), mode, inject, artifact.transitions.items.len, choices.advanced });
            }
            try std.testing.expectEqual(inject, choices.lost);
            try std.testing.expectEqual(mode != 0, choices.acquired);
            try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
            if (mode == 0 and family == .degree) try std.testing.expectEqual(@as(usize, 6), choices.advanced);
            var replayed = try vopr.replay.exact(Scenario, allocator, &artifact);
            defer replayed.deinit();
            // Exercise both the single metric and paired HITS allocations,
            // including initial, acquired, reclaimed and terminal prefixes.
            if (mode == 1 and (family == .degree or family == .hits_authority)) try verifyDiagnostics(Scenario, allocator, &artifact);
        }
    }
    var owner = try @import("domain_vopr.zig").recordNamed(allocator, "index-ownership", 1);
    defer owner.deinit();
    try verifyDiagnostics(OwnerScenario, allocator, &owner);
}

// A test-only property regression exercises the entire debug recipe against
// the real maintenance world and collectors without changing production checks.
fn BrokenProperty(comptime Base: type) type {
    return struct {
        pub const name = Base.name;
        pub const version = Base.version;
        pub const World = Base.World;
        pub const init = Base.init;
        pub const deinit = Base.deinit;
        pub const enumerate = Base.enumerate;
        pub const execute = Base.execute;
        pub const observe = Base.observe;
        pub const collect = Base.collect;
        pub const done = Base.done;
        const diagnostic_failure = vopr.id.stable("property", "maintenance.diagnostic-fixture");
        pub const properties = &[_]vopr.property.Declaration{.{ .id = diagnostic_failure, .name = "maintenance.diagnostic-fixture", .kind = .always }};
        pub fn evaluate(world: *World, sink: *vopr.property.Sink, alloc: std.mem.Allocator) !void {
            try sink.check(alloc, diagnostic_failure, !Base.done(world));
        }
    };
}

test "index maintenance VOPR failure recipes collect both owners" {
    const alloc = std.testing.allocator;
    inline for (.{ Scenario, OwnerScenario }) |Base| {
        const Selected = BrokenProperty(Base);
        var source = vopr.choice.Seeded.init(1);
        var artifact = try vopr.runner.run(Selected, alloc, source.source(), .{ .system = "antfly", .seed = 1, .transition_budget = 512, .source_revision = "diagnostic-fixture", .target = "native", .optimize = @tagName(@import("builtin").mode) });
        defer artifact.deinit();
        try std.testing.expect(artifact.summary.?.property_failures > 0);
        var package = try vopr.debug_recipe.run(Selected, alloc, &artifact, .{ .reduction = .{ .max_attempts = 2 }, .counterfactual = .{ .max_experiments = 1, .descendants_per_alternative = 1 }, .flight = null });
        defer package.deinit();
    }
}
