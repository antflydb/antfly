// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const choice = @import("choice.zig");
const runner = @import("runner.zig");
const scenario_contract = @import("scenario.zig");
const trace = @import("trace.zig");

/// Rebuild a clean world, consume the authoritative decision stream, and then
/// require the complete canonical artifact (events, observations, properties,
/// failures, and summary included) to match byte for byte.
pub fn exact(comptime Scenario: type, allocator: std.mem.Allocator, recorded: *const trace.Trace) !trace.Trace {
    comptime scenario_contract.assertContract(Scenario);
    try recorded.validate();
    if (!std.mem.eql(u8, recorded.header.scenario, Scenario.name)) return error.IncompatibleScenario;
    if (recorded.header.scenario_version != Scenario.version) return error.IncompatibleScenarioVersion;

    var source = choice.Replay{ .records = recorded.choices.items };
    var replayed = try runner.run(Scenario, allocator, source.source(), .{
        .system = recorded.header.system,
        .seed = recorded.config.seed,
        .transition_budget = recorded.config.transition_budget,
        .resource_budget = recorded.config.resource_budget,
        .fixture_hashes = recorded.config.fixture_hashes,
        .feature_flags = recorded.config.feature_flags,
        .backend_ids = recorded.config.backend_ids,
        .scenario_parameters = recorded.config.scenario_parameters,
        .source_revision = recorded.header.source_revision,
        .target = recorded.header.target,
        .optimize = recorded.header.optimize,
    });
    errdefer replayed.deinit();

    const expected = try recorded.renderAlloc(allocator);
    defer allocator.free(expected);
    const actual = try replayed.renderAlloc(allocator);
    defer allocator.free(actual);
    if (!std.mem.eql(u8, expected, actual)) return error.ReplayArtifactDiverged;
    return replayed;
}
