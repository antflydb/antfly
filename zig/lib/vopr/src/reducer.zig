// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Same-fingerprint structured trace reduction.

const std = @import("std");
const choice = @import("choice.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");
const trace = @import("trace.zig");

pub const Config = struct {
    max_attempts: u64 = 10_000,
    suffix_seed: u64 = 0x7265_6475_6365_7231,
};

pub const Report = struct {
    original_transitions: u64,
    reduced_transitions: u64,
    target_fingerprint: u64,
    attempts: u64 = 0,
    accepted: u64 = 0,
    replay_checks: u64 = 0,
};

pub const Result = struct {
    artifact: trace.Trace,
    report: Report,

    pub fn deinit(self: *Result) void {
        self.artifact.deinit();
        self.* = undefined;
    }
};

pub fn reduce(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    original: *const trace.Trace,
    target_fingerprint: u64,
    config: Config,
) !Result {
    if (config.max_attempts == 0) return error.InvalidReductionAttemptBudget;
    if (!hasFingerprint(original, target_fingerprint)) return error.TargetFingerprintMissing;

    // Prove the input itself is an exact replay before treating it as a bug.
    var replayed = try replay.exact(Scenario, allocator, original);
    replayed.deinit();
    var current = try cloneTrace(allocator, original);
    errdefer current.deinit();
    var report = Report{
        .original_transitions = original.summary.?.transitions,
        .reduced_transitions = original.summary.?.transitions,
        .target_fingerprint = target_fingerprint,
        .replay_checks = 1,
    };

    var changed = true;
    while (changed and report.attempts < config.max_attempts) {
        changed = false;
        var choice_index = current.choices.items.len;
        while (choice_index > 0 and report.attempts < config.max_attempts) {
            choice_index -= 1;
            const record = current.choices.items[choice_index];
            for (record.enabled_ids) |alternative| {
                if (alternative == record.selected_id or report.attempts == config.max_attempts) continue;
                report.attempts += 1;
                var source = choice.Mutating.init(current.choices.items, choice_index, alternative, config.suffix_seed +% report.attempts);
                var candidate = runner.run(Scenario, allocator, source.source(), runnerConfig(&current)) catch continue;
                defer candidate.deinit();
                if (!hasFingerprint(&candidate, target_fingerprint)) continue;
                if (!try strictlySimpler(allocator, &candidate, &current)) continue;

                var exact = replay.exact(Scenario, allocator, &candidate) catch continue;
                exact.deinit();
                report.replay_checks += 1;
                const replacement = try cloneTrace(allocator, &candidate);
                current.deinit();
                current = replacement;
                report.accepted += 1;
                report.reduced_transitions = current.summary.?.transitions;
                changed = true;
                break;
            }
            if (changed) break;
        }
    }

    return .{ .artifact = current, .report = report };
}

fn runnerConfig(artifact: *const trace.Trace) runner.Config {
    return .{
        .system = artifact.header.system,
        .seed = artifact.config.seed,
        .transition_budget = artifact.config.transition_budget,
        .resource_budget = artifact.config.resource_budget,
        .fixture_hashes = artifact.config.fixture_hashes,
        .feature_flags = artifact.config.feature_flags,
        .backend_ids = artifact.config.backend_ids,
        .source_revision = artifact.header.source_revision,
        .target = artifact.header.target,
        .optimize = artifact.header.optimize,
    };
}

fn hasFingerprint(artifact: *const trace.Trace, target: u64) bool {
    for (artifact.failures.items) |failure| if (failure.fingerprint == target) return true;
    return false;
}

fn cloneTrace(allocator: std.mem.Allocator, artifact: *const trace.Trace) !trace.Trace {
    const bytes = try artifact.renderAlloc(allocator);
    defer allocator.free(bytes);
    return try trace.parseAlloc(allocator, bytes);
}

fn strictlySimpler(allocator: std.mem.Allocator, candidate: *const trace.Trace, current: *const trace.Trace) !bool {
    const candidate_count = candidate.summary.?.transitions;
    const current_count = current.summary.?.transitions;
    if (candidate_count != current_count) return candidate_count < current_count;
    const candidate_bytes = try candidate.renderAlloc(allocator);
    defer allocator.free(candidate_bytes);
    const current_bytes = try current.renderAlloc(allocator);
    defer allocator.free(current_bytes);
    return std.mem.lessThan(u8, candidate_bytes, current_bytes);
}

test "reducer shortens a history while preserving its failure fingerprint" {
    const event = @import("event.zig");
    const ids = @import("id.zig");
    const observation = @import("observation.zig");
    const property = @import("property.zig");
    const transition = @import("transition.zig");
    const Scenario = struct {
        pub const name: []const u8 = "reducer-test";
        pub const version: u32 = 1;
        const failure_id = ids.stable("property", "reducer.failure");
        const loop_id = ids.stable("transition", "reducer.loop");
        const finish_id = ids.stable("transition", "reducer.finish");
        pub const properties = &[_]property.Declaration{.{ .id = failure_id, .name = "reducer.failure", .kind = .always }};
        pub const World = struct { steps: u64 = 0, complete: bool = false };
        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: std.mem.Allocator) void {}
        pub fn enumerate(_: *World, list: *transition.List, allocator_: std.mem.Allocator) !void {
            try list.append(allocator_, .{ .id = loop_id, .name = "reducer.loop", .kind = .workload });
            try list.append(allocator_, .{ .id = finish_id, .name = "reducer.finish", .kind = .workload });
        }
        pub fn execute(world: *World, selected: transition.Transition, _: *event.Sink, _: std.mem.Allocator) !void {
            world.steps += 1;
            world.complete = selected.id == finish_id;
        }
        pub fn observe(world: *World, builder: *observation.Builder, allocator_: std.mem.Allocator) !void {
            try builder.addNamed(allocator_, "reducer.steps", @intCast(world.steps));
        }
        pub fn evaluate(_: *World, sink: *property.Sink, allocator_: std.mem.Allocator) !void {
            try sink.check(allocator_, failure_id, false);
        }
        pub fn done(world: *World) bool {
            return world.complete;
        }
    };

    var scripted = choice.Scripted{ .selections = &.{ Scenario.loop_id, Scenario.loop_id, Scenario.finish_id } };
    var original = try runner.run(Scenario, std.testing.allocator, scripted.source(), .{ .transition_budget = 4 });
    defer original.deinit();
    const fingerprint = original.failures.items[0].fingerprint;
    var result = try reduce(Scenario, std.testing.allocator, &original, fingerprint, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 3), result.report.original_transitions);
    try std.testing.expectEqual(@as(u64, 1), result.report.reduced_transitions);
    try std.testing.expect(result.report.accepted > 0);
    try std.testing.expect(hasFingerprint(&result.artifact, fingerprint));
}
