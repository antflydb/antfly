// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Standalone/serverless process supervision over the production lifecycle
//! state machine and VoprIo clock. Scenario children are deliberately narrow:
//! their start/stop ownership mirrors the real role composition while the
//! readiness, cancellation, failure, deadline, and restart rules are the
//! production `RuntimeSupervisor` implementation.

const std = @import("std");
const vopr = @import("vopr");
const lifecycle = @import("../common/runtime_lifecycle.zig");

pub const Scenario = struct {
    pub const name: []const u8 = "runtime-supervision-production-lifecycle";
    pub const version: u32 = 1;

    const readiness_id = vopr.id.stable(name, "readiness-after-all-children");
    const rollback_id = vopr.id.stable(name, "partial-startup-rolls-back");
    const cancellation_id = vopr.id.stable(name, "failure-cancels-runtime");
    const shutdown_id = vopr.id.stable(name, "coordinated-shutdown-cleans-children");
    const restart_id = vopr.id.stable(name, "restart-reaches-ready");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = readiness_id, .name = name ++ ".readiness-after-all-children", .kind = .always },
        .{ .id = rollback_id, .name = name ++ ".partial-startup-rolls-back", .kind = .always },
        .{ .id = cancellation_id, .name = name ++ ".failure-cancels-runtime", .kind = .always },
        .{ .id = shutdown_id, .name = name ++ ".coordinated-shutdown-cleans-children", .kind = .always },
        .{ .id = restart_id, .name = name ++ ".restart-reaches-ready", .kind = .reachable },
    };

    const Mode = enum {
        clean,
        partial_startup_failure,
        shutdown_during_startup,
        child_service_failure,
        coordinated_shutdown,
        watchdog_expiry,
        restart,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "clean"),
        vopr.id.stable(name, "partial-startup-failure"),
        vopr.id.stable(name, "shutdown-during-startup"),
        vopr.id.stable(name, "child-service-failure"),
        vopr.id.stable(name, "coordinated-shutdown"),
        vopr.id.stable(name, "watchdog-expiry"),
        vopr.id.stable(name, "restart"),
    };

    const Child = struct {
        running: bool = false,
        starts: u8 = 0,
        stops: u8 = 0,

        fn start(self: *@This()) void {
            std.debug.assert(!self.running);
            self.running = true;
            self.starts +|= 1;
        }

        fn stop(self: *@This()) void {
            if (!self.running) return;
            self.running = false;
            self.stops +|= 1;
        }
    };

    const State = struct {
        sim: vopr.vopr_io.VoprIo,
        supervisor: lifecycle.RuntimeSupervisor,
        public_api: Child = .{},
        maintenance: Child = .{},
        mode: Mode = .clean,
        complete: bool = false,
        readiness_violation: bool = false,
        rollback_violation: bool = false,
        failure_without_cancellation: bool = false,
        ready_generations: u8 = 0,
        restart_count: u8 = 0,

        fn publishReady(self: *@This()) !void {
            if (!self.public_api.running or !self.maintenance.running)
                self.readiness_violation = true;
            try self.supervisor.publishReady();
            self.ready_generations +|= 1;
        }

        fn stopChildren(self: *@This()) void {
            // Listener admission stops before maintenance state is released.
            self.public_api.stop();
            self.maintenance.stop();
        }

        fn finishGeneration(self: *@This()) void {
            self.supervisor.requestShutdown();
            self.stopChildren();
            self.supervisor.markStopped();
        }

        fn restartHealthy(self: *@This()) !void {
            try self.supervisor.restart();
            self.restart_count +|= 1;
            self.maintenance.start();
            self.public_api.start();
            try self.publishReady();
        }

        fn run(self: *@This()) !void {
            self.maintenance.start();
            switch (self.mode) {
                .partial_startup_failure => {
                    _ = self.supervisor.fail("serverless", "public-api-start", error.AddressInUse);
                    self.maintenance.stop();
                    if (self.maintenance.running) self.rollback_violation = true;
                    self.supervisor.markStopped();
                    try self.restartHealthy();
                },
                .shutdown_during_startup => {
                    self.supervisor.requestShutdown();
                    self.maintenance.stop();
                    if (self.maintenance.running) self.rollback_violation = true;
                    self.supervisor.markStopped();
                    try self.restartHealthy();
                },
                .watchdog_expiry => {
                    const deadline = self.supervisor.startupDeadline();
                    try self.sim.advance(31 * std.time.ns_per_ms);
                    if (!deadline.expired()) return error.VoprStartupDeadlineDidNotExpire;
                    _ = self.supervisor.fail("standalone", "startup-watchdog", error.StartupTimeout);
                    self.maintenance.stop();
                    self.supervisor.markStopped();
                    try self.restartHealthy();
                },
                else => {
                    self.public_api.start();
                    try self.publishReady();
                },
            }

            if (self.mode == .child_service_failure) {
                _ = self.supervisor.fail("serverless", "maintenance", error.InputOutput);
                if (!self.supervisor.token().isCancelled()) self.failure_without_cancellation = true;
                self.stopChildren();
                self.supervisor.markStopped();
                try self.restartHealthy();
            } else if (self.mode == .restart) {
                self.finishGeneration();
                try self.restartHealthy();
            }

            if (self.supervisor.currentState() == .ready) self.finishGeneration();
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.sim = try vopr.vopr_io.VoprIo.init(.{
            .required = .of(&.{ .clock_read, .sleep }),
            .seed = 0x535550,
        });
        errdefer state.sim.deinit();
        state.supervisor = lifecycle.RuntimeSupervisor.initWithIo(state.sim.io(), 30);
        state.public_api = .{};
        state.maintenance = .{};
        state.mode = .clean;
        state.complete = false;
        state.readiness_violation = false;
        state.rollback_violation = false;
        state.failure_without_cancellation = false;
        state.ready_generations = 0;
        state.restart_count = 0;
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| {
            try list.append(allocator, .{
                .id = id,
                .name = name ++ "." ++ @tagName(mode),
                .kind = if (mode == .clean or mode == .coordinated_shutdown or mode == .restart) .workload else .fault,
            });
        }
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            world.state.mode = mode;
            found = true;
        };
        if (!found) return error.InvalidSupervisionTransition;
        try world.state.run();
        try events.emitNamed(allocator, .domain, selected.name, world.state.ready_generations);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".state", @intFromEnum(state.supervisor.currentState()));
        try builder.addNamed(allocator, name ++ ".ready-generations", state.ready_generations);
        try builder.addNamed(allocator, name ++ ".restarts", state.restart_count);
        try builder.addNamed(allocator, name ++ ".public-running", @intFromBool(state.public_api.running));
        try builder.addNamed(allocator, name ++ ".maintenance-running", @intFromBool(state.maintenance.running));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, readiness_id, !state.readiness_violation);
        try sink.check(allocator, rollback_id, !state.rollback_violation);
        try sink.check(allocator, cancellation_id, !state.failure_without_cancellation);
        try sink.check(allocator, shutdown_id, !state.complete or
            (state.supervisor.currentState() == .stopped and !state.public_api.running and !state.maintenance.running));
        const needs_restart = state.mode == .partial_startup_failure or
            state.mode == .shutdown_during_startup or
            state.mode == .child_service_failure or
            state.mode == .watchdog_expiry or
            state.mode == .restart;
        try sink.check(allocator, restart_id, state.complete and state.ready_generations >= 1 and (!needs_restart or state.restart_count >= 1));
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "standalone serverless supervision VOPR exact replays lifecycle failures" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
            .source_revision = "runtime-supervision-vopr-v1",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.property_failures);
        for (0..20) |_| {
            var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
            replayed.deinit();
        }
    }
}
