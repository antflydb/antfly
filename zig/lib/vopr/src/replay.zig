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
    return exactWithContext(Scenario, allocator, recorded, null);
}

/// Exact replay with a diagnostic-only scenario context. The complete VOPR
/// artifact must remain byte-identical, proving the side sink did not affect
/// simulation semantics.
pub fn exactWithContext(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    recorded: *const trace.Trace,
    scenario_context: ?*anyopaque,
) !trace.Trace {
    comptime scenario_contract.assertContract(Scenario);
    try recorded.validate();
    if (!std.mem.eql(u8, recorded.header.scenario, Scenario.name)) return error.IncompatibleScenario;
    if (recorded.header.scenario_version != Scenario.version) return error.IncompatibleScenarioVersion;

    var source = choice.Replay{
        .records = recorded.choices.items,
        .expected_transitions = recorded.transitions.items,
    };
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
        .scenario_context = scenario_context,
    });
    errdefer replayed.deinit();

    if (!try recorded.canonicalEqual(&replayed, allocator)) {
        return error.ReplayArtifactDiverged;
    }
    return replayed;
}

fn lineAt(bytes: []const u8, offset: usize) []const u8 {
    const bounded = @min(offset, bytes.len);
    const start = if (std.mem.lastIndexOfScalar(u8, bytes[0..bounded], '\n')) |newline|
        newline + 1
    else
        0;
    const end = if (std.mem.indexOfScalarPos(u8, bytes, bounded, '\n')) |newline|
        newline
    else
        bytes.len;
    return bytes[start..end];
}

test "exact replay diagnostics select the differing canonical line" {
    const artifact = "header\nchoice\nobservation\n";
    try std.testing.expectEqualStrings("header", lineAt(artifact, 2));
    try std.testing.expectEqualStrings("choice", lineAt(artifact, 9));
    try std.testing.expectEqualStrings("observation", lineAt(artifact, 18));
    try std.testing.expectEqualStrings("", lineAt(artifact, artifact.len));
}
