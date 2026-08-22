// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");

pub const choice = @import("choice.zig");
pub const coverage = @import("coverage.zig");
pub const corpus = @import("corpus.zig");
pub const event = @import("event.zig");
pub const explorer = @import("explorer.zig");
pub const id = @import("id.zig");
pub const observation = @import("observation.zig");
pub const property = @import("property.zig");
pub const replay = @import("replay.zig");
pub const reducer = @import("reducer.zig");
pub const runtime = @import("runtime.zig");
pub const scheduler = @import("scheduler.zig");
pub const sim_runtime = @import("sim_runtime.zig");
pub const runner = @import("runner.zig");
pub const scenario = @import("scenario.zig");
pub const trace = @import("trace.zig");
pub const transition = @import("transition.zig");

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
    pub fn execute(world: *World, _: transition.Transition, events: *event.Sink, allocator: std.mem.Allocator) !void {
        world.complete = true;
        try events.emitNamed(allocator, .injected_error, "toy.fault_injected", 1);
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
    _ = coverage;
    _ = corpus;
    _ = event;
    _ = explorer;
    _ = id;
    _ = observation;
    _ = property;
    _ = replay;
    _ = reducer;
    _ = runtime;
    _ = scheduler;
    _ = sim_runtime;
    _ = transition;
}
