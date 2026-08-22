// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Promotion and explicit replay-proven fixture migration contracts.
//!
//! This module performs no filesystem writes. Product CLIs own reviewed paths
//! and overwrite policy; the reusable layer owns canonical names, exact replay
//! proof, and semantic outcome equivalence.

const std = @import("std");
const ids = @import("id.zig");
const trace = @import("trace.zig");

pub const ReplayFn = *const fn (std.mem.Allocator, *const trace.Trace) anyerror!trace.Trace;
pub const TransformFn = *const fn (std.mem.Allocator, *const trace.Trace) anyerror!trace.Trace;

pub const EquivalencePolicy = struct {
    require_same_system: bool = true,
    require_same_scenario: bool = true,
    require_final_observation: bool = true,
    require_property_history: bool = true,
    require_failure_set: bool = true,
};

pub const MigrationReport = struct {
    source_trace_digest: u64,
    migrated_trace_digest: u64,
    source_outcome_digest: u64,
    migrated_outcome_digest: u64,
    source_scenario_version: u32,
    migrated_scenario_version: u32,
};

pub const Migration = struct {
    artifact: trace.Trace,
    report: MigrationReport,

    pub fn deinit(self: *Migration) void {
        self.artifact.deinit();
        self.* = undefined;
    }
};

pub fn validateName(name: []const u8) !void {
    if (name.len == 0 or name.len > 96) return error.InvalidFixtureName;
    if (name[0] == '-' or name[name.len - 1] == '-') return error.InvalidFixtureName;
    var prior_dash = false;
    for (name) |byte| {
        const dash = byte == '-';
        if (!std.ascii.isLower(byte) and !std.ascii.isDigit(byte) and !dash) return error.InvalidFixtureName;
        if (dash and prior_dash) return error.InvalidFixtureName;
        prior_dash = dash;
    }
}

pub fn validatePromotion(artifact: *const trace.Trace, name: []const u8) !void {
    try validateName(name);
    try artifact.validate();
    if (artifact.failures.items.len == 0) return error.FailingTraceRequired;
    if (artifact.header.source_revision.len == 0 or std.mem.eql(u8, artifact.header.source_revision, "unknown"))
        return error.DiscoveryRevisionRequired;
    if (artifact.config.seed == null) return error.DiscoverySeedRequired;
}

/// Replays the source exactly, performs one explicit transformation, replays
/// the transformed artifact exactly under the target adapter, then compares
/// canonical semantic outcomes under the stated policy.
pub fn migrate(
    allocator: std.mem.Allocator,
    source: *const trace.Trace,
    source_replay: ReplayFn,
    transform: TransformFn,
    target_replay: ReplayFn,
    policy: EquivalencePolicy,
) !Migration {
    try source.validate();
    var source_proof = try source_replay(allocator, source);
    defer source_proof.deinit();
    try requireExactBytes(allocator, source, &source_proof, error.SourceFixtureReplayDiverged);

    var migrated = try transform(allocator, source);
    errdefer migrated.deinit();
    try migrated.validate();
    var target_proof = try target_replay(allocator, &migrated);
    defer target_proof.deinit();
    try requireExactBytes(allocator, &migrated, &target_proof, error.MigratedFixtureReplayDiverged);
    try requireEquivalent(source, &migrated, policy);

    const source_bytes = try source.renderAlloc(allocator);
    defer allocator.free(source_bytes);
    const migrated_bytes = try migrated.renderAlloc(allocator);
    defer allocator.free(migrated_bytes);
    return .{
        .artifact = migrated,
        .report = .{
            .source_trace_digest = ids.digest(source_bytes),
            .migrated_trace_digest = ids.digest(migrated_bytes),
            .source_outcome_digest = outcomeDigest(source, policy),
            .migrated_outcome_digest = outcomeDigest(&migrated, policy),
            .source_scenario_version = source.header.scenario_version,
            .migrated_scenario_version = migrated.header.scenario_version,
        },
    };
}

pub fn canonicalClone(allocator: std.mem.Allocator, source: *const trace.Trace) !trace.Trace {
    const bytes = try source.renderAlloc(allocator);
    defer allocator.free(bytes);
    return trace.parseAlloc(allocator, bytes);
}

fn requireExactBytes(allocator: std.mem.Allocator, expected: *const trace.Trace, actual: *const trace.Trace, divergence: anyerror) !void {
    const expected_bytes = try expected.renderAlloc(allocator);
    defer allocator.free(expected_bytes);
    const actual_bytes = try actual.renderAlloc(allocator);
    defer allocator.free(actual_bytes);
    if (!std.mem.eql(u8, expected_bytes, actual_bytes)) return divergence;
}

fn requireEquivalent(source: *const trace.Trace, migrated: *const trace.Trace, policy: EquivalencePolicy) !void {
    if (policy.require_same_system and !std.mem.eql(u8, source.header.system, migrated.header.system))
        return error.FixtureMigrationSystemChanged;
    if (policy.require_same_scenario and !std.mem.eql(u8, source.header.scenario, migrated.header.scenario))
        return error.FixtureMigrationScenarioChanged;
    if (policy.require_final_observation and source.summary.?.final_observation_digest != migrated.summary.?.final_observation_digest)
        return error.FixtureMigrationFinalObservationChanged;
    if (policy.require_property_history and propertyDigest(source) != propertyDigest(migrated))
        return error.FixtureMigrationPropertyHistoryChanged;
    if (policy.require_failure_set and failureDigest(source) != failureDigest(migrated))
        return error.FixtureMigrationFailureSetChanged;
}

fn outcomeDigest(artifact: *const trace.Trace, policy: EquivalencePolicy) u64 {
    var digest = ids.stable("fixture-outcome", "v1");
    if (policy.require_final_observation) digest = ids.derive("fixture-outcome.observation", digest, artifact.summary.?.final_observation_digest);
    if (policy.require_property_history) digest = ids.derive("fixture-outcome.properties", digest, propertyDigest(artifact));
    if (policy.require_failure_set) digest = ids.derive("fixture-outcome.failures", digest, failureDigest(artifact));
    return digest;
}

fn propertyDigest(artifact: *const trace.Trace) u64 {
    var digest = ids.stable("fixture-properties", "empty");
    for (artifact.properties.items) |record| {
        const identity = ids.derive("fixture-property.identity", record.property_id, @intFromEnum(record.kind));
        const state = ids.derive("fixture-property.state", record.index, @intFromBool(record.condition));
        digest = ids.derive("fixture-property", digest ^ identity, state);
    }
    return digest;
}

fn failureDigest(artifact: *const trace.Trace) u64 {
    var digest = ids.stable("fixture-failures", "empty");
    for (artifact.failures.items) |record| {
        const identity = ids.derive("fixture-failure.identity", record.fingerprint, @intFromEnum(record.class));
        digest = ids.derive("fixture-failure", digest ^ identity, record.property_id orelse 0);
    }
    return digest;
}

test "fixture names describe behavior rather than raw paths or seeds" {
    try validateName("split-leader-crash-before-finalize");
    try std.testing.expectError(error.InvalidFixtureName, validateName("0xA17F_0001"));
    try std.testing.expectError(error.InvalidFixtureName, validateName("../escape"));
    try std.testing.expectError(error.InvalidFixtureName, validateName("double--dash"));
}

test "migration requires exact replay and equivalent semantic outcomes" {
    const choice = @import("choice.zig");
    const replay = @import("replay.zig");
    const runner = @import("runner.zig");
    const ToyScenario = @import("toy_scenario.zig").ToyScenario;
    const Adapter = struct {
        fn replayArtifact(allocator: std.mem.Allocator, artifact: *const trace.Trace) !trace.Trace {
            return replay.exact(ToyScenario, allocator, artifact);
        }
    };

    var seeded = choice.Seeded.init(0xa17f);
    var source = try runner.run(ToyScenario, std.testing.allocator, seeded.source(), .{
        .seed = 0xa17f,
        .transition_budget = 4,
        .source_revision = "fixture-test",
    });
    defer source.deinit();
    var result = try migrate(
        std.testing.allocator,
        &source,
        Adapter.replayArtifact,
        canonicalClone,
        Adapter.replayArtifact,
        .{},
    );
    defer result.deinit();
    try std.testing.expectEqual(result.report.source_trace_digest, result.report.migrated_trace_digest);
    try std.testing.expectEqual(result.report.source_outcome_digest, result.report.migrated_outcome_digest);
}
