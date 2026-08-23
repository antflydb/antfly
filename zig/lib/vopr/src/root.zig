// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");

pub const choice = @import("choice.zig");
pub const command = @import("command.zig");
pub const benchmark = @import("benchmark.zig");
pub const causal = @import("causal.zig");
pub const coverage = @import("coverage.zig");
pub const corpus = @import("corpus.zig");
pub const event = @import("event.zig");
pub const explorer = @import("explorer.zig");
pub const fault = @import("fault.zig");
pub const fixture = @import("fixture.zig");
pub const id = @import("id.zig");
pub const meta_test = @import("meta_test.zig");
pub const observation = @import("observation.zig");
pub const outcome = @import("outcome.zig");
pub const property = @import("property.zig");
pub const replay = @import("replay.zig");
pub const reducer = @import("reducer.zig");
pub const runtime = @import("runtime.zig");
pub const scheduler = @import("scheduler.zig");
pub const sim_io = @import("sim_io.zig");
pub const sim_io_file = @import("sim_io_file.zig");
pub const sim_io_instrumentation = @import("sim_io_instrumentation.zig");
pub const sim_io_net = @import("sim_io_net.zig");
pub const sim_io_process = @import("sim_io_process.zig");
pub const sim_io_task = @import("sim_io_task.zig");
pub const sim_io_scenario_test = @import("sim_io_scenario_test.zig");
pub const sim_runtime = @import("sim_runtime.zig");
pub const snapshot = @import("snapshot.zig");
pub const splice = @import("splice.zig");
pub const runner = @import("runner.zig");
pub const scenario = @import("scenario.zig");
pub const trace = @import("trace.zig");
pub const transition = @import("transition.zig");
pub const time = @import("time.zig");

const ToyScenario = @import("toy_scenario.zig").ToyScenario;

const FailingScenario = struct {
    pub const name: []const u8 = "phase0-failing-toy";
    pub const version: u32 = 1;
    const failure_id = id.stable("property", "toy.injected_failure");
    const fault_id = id.stable("transition", "toy.injected_fault");
    pub const properties = &[_]property.Declaration{.{ .id = failure_id, .name = "toy.injected_failure", .kind = .always }};
    pub const World = struct { complete: bool = false };

    pub fn init(_: std.mem.Allocator) !World {
        return .{};
    }
    pub fn deinit(_: *World, _: std.mem.Allocator) void {}
    pub fn enumerate(_: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
        try list.append(allocator, .{ .id = fault_id, .name = "toy.injected_fault", .kind = .fault });
    }
    pub fn execute(world: *World, _: transition.Transition, events: *event.Sink, allocator: std.mem.Allocator) !outcome.TransitionOutcome {
        world.complete = true;
        try events.emitNamed(allocator, .injected_error, "toy.fault_injected", 1);
        return outcome.TransitionOutcome.injectedError("toy.fault_injected", 1);
    }
    pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, "toy.complete", @intFromBool(world.complete));
    }
    pub fn evaluate(_: *World, sink: *property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, failure_id, false);
    }
    pub fn done(world: *World) bool {
        return world.complete;
    }
};

fn NoTransitionScenario(comptime classification: ?scenario.NoTransition) type {
    return struct {
        pub const name: []const u8 = if (classification == null) "blocked-default" else switch (classification.?) {
            .clean_quiescence => "blocked-clean",
            .expected_blocked => "blocked-expected",
            .liveness_failure => "blocked-liveness",
            .harness_deadlock => "blocked-harness",
        };
        pub const version: u32 = 1;
        pub const properties = &[_]property.Declaration{};
        pub const World = struct {};
        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: std.mem.Allocator) void {}
        pub fn enumerate(_: *World, _: *transition.List, _: std.mem.Allocator) !void {}
        pub fn execute(_: *World, _: transition.Transition, _: *event.Sink, _: std.mem.Allocator) !outcome.TransitionOutcome {
            return error.UnreachableTransition;
        }
        pub fn observe(_: *World, _: *observation.Builder, _: std.mem.Allocator) !void {}
        pub fn evaluate(_: *World, _: *property.Sink, _: std.mem.Allocator) !void {}
        pub fn done(_: *World) bool {
            return false;
        }
        pub fn classifyNoTransition(_: *World) scenario.NoTransition {
            if (classification) |value| return value;
            unreachable;
        }
    };
}

const BudgetScenario = struct {
    pub const name: []const u8 = "budget-terminated";
    pub const version: u32 = 1;
    const step_id = id.stable("transition", "budget.step");
    pub const properties = &[_]property.Declaration{};
    pub const World = struct { steps: u64 = 0 };
    pub fn init(_: std.mem.Allocator) !World {
        return .{};
    }
    pub fn deinit(_: *World, _: std.mem.Allocator) void {}
    pub fn enumerate(_: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
        try list.append(allocator, .{ .id = step_id, .name = "budget.step", .kind = .workload });
    }
    pub fn execute(world: *World, _: transition.Transition, _: *event.Sink, _: std.mem.Allocator) !outcome.TransitionOutcome {
        world.steps += 1;
        return outcome.TransitionOutcome.applied();
    }
    pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, "budget.steps", @intCast(world.steps));
    }
    pub fn evaluate(_: *World, _: *property.Sink, _: std.mem.Allocator) !void {}
    pub fn done(_: *World) bool {
        return false;
    }
};

test "toy scenario records, parses, and exactly replays" {
    var seeded = choice.Seeded.init(0xa17f_0001);
    var recorded = try runner.run(ToyScenario, std.testing.allocator, seeded.source(), .{
        .system = "vopr-test",
        .seed = 0xa17f_0001,
        .transition_budget = 4,
        .source_revision = "phase0-test",
        .target = "test",
        .optimize = "Debug",
    });
    defer recorded.deinit();
    const encoded = try recorded.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"format\":\"vopr-trace-v1\"") != null);

    var parsed = try trace.parseAlloc(std.testing.allocator, encoded);
    defer parsed.deinit();
    for (0..100) |_| {
        var replayed = try replay.exact(ToyScenario, std.testing.allocator, &parsed);
        replayed.deinit();
    }
    try std.testing.expectEqual(@as(u64, 0), parsed.summary.?.property_failures);
}

test "exact replay rejects a scenario ABI mismatch" {
    var seeded = choice.Seeded.init(7);
    var recorded = try runner.run(ToyScenario, std.testing.allocator, seeded.source(), .{ .seed = 7, .transition_budget = 4 });
    defer recorded.deinit();
    recorded.header.scenario_version += 1;
    try std.testing.expectError(error.IncompatibleScenarioVersion, replay.exact(ToyScenario, std.testing.allocator, &recorded));
}

test "logical checkpoint resumes a mutated suffix with exact property history" {
    var seeded = choice.Seeded.init(0xc4ec_5001);
    var parent = try runner.run(ToyScenario, std.testing.allocator, seeded.source(), .{
        .system = "vopr-test",
        .seed = 0xc4ec_5001,
        .transition_budget = 4,
    });
    defer parent.deinit();
    var mutation_index: usize = 0;
    for (parent.choices.items, 0..) |record, index| {
        if (index > 0 and record.enabled_ids.len > 1) {
            mutation_index = index;
            break;
        }
    }
    const record = parent.choices.items[mutation_index];
    var replacement = record.selected_id;
    for (record.enabled_ids) |candidate| {
        if (candidate != record.selected_id) {
            replacement = candidate;
            break;
        }
    }
    try std.testing.expect(replacement != record.selected_id);

    var checkpoint = try runner.captureCheckpoint(ToyScenario, std.testing.allocator, &parent, mutation_index);
    defer checkpoint.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, ToyScenario.properties.len), checkpoint.property_statuses.len);
    var mutating = choice.Mutating.initAt(parent.choices.items, mutation_index, replacement, 0x55aa, mutation_index);
    var resumed = try runner.resumeFromCheckpoint(ToyScenario, std.testing.allocator, &parent, checkpoint, mutating.source());
    defer resumed.deinit();
    var replayed = try replay.exact(ToyScenario, std.testing.allocator, &resumed);
    replayed.deinit();
}

test "property violations are recorded with stable failure fingerprints" {
    var seeded = choice.Seeded.init(3);
    var recorded = try runner.run(FailingScenario, std.testing.allocator, seeded.source(), .{ .seed = 3, .transition_budget = 1 });
    defer recorded.deinit();
    try std.testing.expectEqual(@as(u64, 1), recorded.summary.?.property_failures);
    try std.testing.expectEqual(@as(usize, 1), recorded.failures.items.len);
    try std.testing.expectEqual(@as(usize, 1), recorded.faults.items.len);
    try std.testing.expectEqual(id.stable("failure", "toy.injected_failure"), recorded.failures.items[0].fingerprint);

    var replayed = try replay.exact(FailingScenario, std.testing.allocator, &recorded);
    replayed.deinit();
}

test "bounded histories stop cleanly at their transition budget" {
    var seeded = choice.Seeded.init(4);
    var recorded = try runner.run(BudgetScenario, std.testing.allocator, seeded.source(), .{ .seed = 4, .transition_budget = 2 });
    defer recorded.deinit();
    try std.testing.expectEqual(@as(u64, 2), recorded.summary.?.transitions);
    try std.testing.expectEqual(@as(usize, 0), recorded.failures.items.len);
    var replayed = try replay.exact(BudgetScenario, std.testing.allocator, &recorded);
    replayed.deinit();
}

test "no-transition states distinguish clean liveness and harness outcomes" {
    const Clean = NoTransitionScenario(.clean_quiescence);
    var seeded = choice.Seeded.init(5);
    var clean = try runner.run(Clean, std.testing.allocator, seeded.source(), .{ .seed = 5, .transition_budget = 1 });
    defer clean.deinit();
    try std.testing.expectEqual(@as(u64, 0), clean.summary.?.transitions);
    try std.testing.expectEqual(@as(usize, 0), clean.failures.items.len);

    const Liveness = NoTransitionScenario(.{ .liveness_failure = "test.recovery_stalled" });
    var liveness = try runner.run(Liveness, std.testing.allocator, seeded.source(), .{ .seed = 5, .transition_budget = 1 });
    defer liveness.deinit();
    try std.testing.expectEqual(@as(usize, 1), liveness.failures.items.len);
    try std.testing.expectEqual(trace.FailureClass.liveness, liveness.failures.items[0].class);

    const Harness = NoTransitionScenario(.{ .harness_deadlock = "test.scheduler_deadlock" });
    try std.testing.expectError(error.ScenarioDeadlock, runner.run(Harness, std.testing.allocator, seeded.source(), .{ .seed = 5, .transition_budget = 1 }));
}

test "record and replay paths are allocation-failure safe" {
    const AllocationRunner = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var seeded = choice.Seeded.init(19);
            var recorded = try runner.run(ToyScenario, allocator, seeded.source(), .{ .seed = 19, .transition_budget = 4 });
            defer recorded.deinit();
            const encoded = try recorded.renderAlloc(allocator);
            defer allocator.free(encoded);
            var parsed = try trace.parseAlloc(allocator, encoded);
            defer parsed.deinit();
            var replayed = try replay.exact(ToyScenario, allocator, &parsed);
            replayed.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, AllocationRunner.run, .{});
}

test "trace parser rejects incompatible format" {
    const invalid = "{\"type\":\"header\",\"format\":\"vopr-trace-v0\",\"simulator_abi\":1,\"system\":\"test\",\"scenario\":\"toy\",\"scenario_version\":1,\"source_revision\":\"x\",\"target\":\"x\",\"optimize\":\"x\"}\n" ++
        "{\"type\":\"config\",\"seed\":null,\"transition_budget\":1}\n";
    try std.testing.expectError(error.IncompatibleTrace, trace.parseAlloc(std.testing.allocator, invalid));
}

test "trace v1 schema is checked in and valid JSON" {
    const schema = @embedFile("vopr-trace-v1.schema.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, schema, .{});
    defer parsed.deinit();
    const object = parsed.value.object;
    try std.testing.expectEqualStrings("https://json-schema.org/draft/2020-12/schema", object.get("$schema").?.string);
    try std.testing.expectEqualStrings("VOPR deterministic simulation trace v1 NDJSON records", object.get("title").?.string);
}

test {
    _ = choice;
    _ = command;
    _ = benchmark;
    _ = causal;
    _ = coverage;
    _ = corpus;
    _ = event;
    _ = explorer;
    _ = fault;
    _ = fixture;
    _ = id;
    _ = observation;
    _ = outcome;
    _ = property;
    _ = replay;
    _ = reducer;
    _ = runtime;
    _ = scheduler;
    _ = sim_runtime;
    _ = snapshot;
    _ = splice;
    _ = transition;
    _ = time;
}
