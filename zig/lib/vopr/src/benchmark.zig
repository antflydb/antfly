// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic search-efficiency benchmarks. These report modeled transition
//! work rather than wall time so results remain stable across hosts and builds.

const std = @import("std");
const choice = @import("choice.zig");
const event = @import("event.zig");
const explorer = @import("explorer.zig");
const ids = @import("id.zig");
const observation = @import("observation.zig");
const outcome = @import("outcome.zig");
const property = @import("property.zig");
const replay = @import("replay.zig");
const runner = @import("runner.zig");
const ToyScenario = @import("toy_scenario.zig").ToyScenario;
const transition = @import("transition.zig");

pub const CheckpointResult = struct {
    histories: u64,
    semantic_features: usize,
    retained: u64,
    failures: u64,
    baseline_transition_work_units: u64,
    optimized_transition_work_units: u64,
    checkpoint_hits: u64,
    checkpoint_resumes: u64,
    checkpoint_transitions_avoided: u64,
    improvement_ppm: u64,

    pub fn validate(self: CheckpointResult) !void {
        if (self.checkpoint_hits == 0 or self.checkpoint_resumes == 0 or self.checkpoint_transitions_avoided == 0)
            return error.CheckpointBenchmarkDidNotExerciseOptimization;
        if (self.optimized_transition_work_units >= self.baseline_transition_work_units)
            return error.CheckpointBenchmarkDidNotImproveWorkUnits;
    }
};

pub fn checkpointResume(allocator: std.mem.Allocator, histories: u64) !CheckpointResult {
    const common = explorer.Config{
        .system = "vopr-benchmark",
        .histories = histories,
        .transition_budget = 4,
        .seed = 0xbec4_2026,
        .uniform_percent = 0,
        .splice_percent = 0,
        .checkpoint_percent = 0,
    };
    var baseline = try explorer.Campaign(ToyScenario).init(allocator, common);
    defer baseline.deinit();
    try baseline.run();
    var optimized_config = common;
    optimized_config.checkpoint_percent = 100;
    var optimized = try explorer.Campaign(ToyScenario).init(allocator, optimized_config);
    defer optimized.deinit();
    try optimized.run();

    if (baseline.report.semantic_features != optimized.report.semantic_features or
        baseline.report.retained != optimized.report.retained or
        baseline.report.failures != optimized.report.failures)
        return error.CheckpointBenchmarkChangedSearchResults;
    const saved = baseline.report.transition_work_units - optimized.report.transition_work_units;
    const result = CheckpointResult{
        .histories = histories,
        .semantic_features = optimized.report.semantic_features,
        .retained = optimized.report.retained,
        .failures = optimized.report.failures,
        .baseline_transition_work_units = baseline.report.transition_work_units,
        .optimized_transition_work_units = optimized.report.transition_work_units,
        .checkpoint_hits = optimized.report.checkpoint_hits,
        .checkpoint_resumes = optimized.report.checkpoint_resumes,
        .checkpoint_transitions_avoided = optimized.report.checkpoint_transitions_avoided,
        .improvement_ppm = saved *| 1_000_000 / baseline.report.transition_work_units,
    };
    try result.validate();
    return result;
}

/// The benchmark corpus intentionally contains a stable, reviewable defect:
/// servicing a victim after it has been runnable but bypassed twice violates
/// an always property. It is deliberately small enough that changes in search
/// quality are attributable to policy rather than fixture setup or wall time.
const StarvationBugScenario = struct {
    pub const name: []const u8 = "vopr-search-quality-starvation-bug";
    pub const version: u32 = 1;
    pub const arm_id = ids.stable("transition", "search-quality.arm");
    pub const victim_id = ids.stable("transition", "search-quality.service-victim");
    pub const competitor_id = ids.stable("transition", "search-quality.run-competitor");
    pub const safety_id = ids.stable("property", "search-quality.victim-not-starved");
    pub const failure_fingerprint = ids.stable("failure", "search-quality.victim-not-starved");
    pub const properties = &[_]property.Declaration{.{
        .id = safety_id,
        .name = "search-quality.victim-not-starved",
        .kind = .always,
    }};

    pub const World = struct {
        armed: bool = false,
        completed: bool = false,
        violated: bool = false,
        deferrals: u8 = 0,
    };

    pub fn init(_: std.mem.Allocator) !World {
        return .{};
    }

    pub fn deinit(_: *World, _: std.mem.Allocator) void {}

    pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
        if (!world.armed) {
            try list.append(allocator, .{ .id = arm_id, .name = "search-quality.arm", .kind = .workload });
            return;
        }
        try list.append(allocator, .{
            .id = victim_id,
            .name = "search-quality.service-victim",
            .kind = .workload,
            .actor_id = ids.stable("actor", "search-quality.victim"),
        });
        if (world.deferrals < 3) try list.append(allocator, .{
            .id = competitor_id,
            .name = "search-quality.run-competitor",
            .kind = .workload,
            .actor_id = ids.stable("actor", "search-quality.competitor"),
        });
    }

    pub fn execute(world: *World, selected: transition.Transition, events: *event.Sink, allocator: std.mem.Allocator) !outcome.TransitionOutcome {
        if (selected.id == arm_id) {
            world.armed = true;
            try events.emitNamed(allocator, .domain, "search-quality.victim-runnable", 1);
        } else if (selected.id == competitor_id) {
            world.deferrals += 1;
            try events.emitNamed(allocator, .domain, "search-quality.victim-deferred", world.deferrals);
        } else if (selected.id == victim_id) {
            world.violated = world.deferrals >= 2;
            world.completed = true;
            try events.emitNamed(allocator, .state_change, "search-quality.victim-serviced", world.deferrals);
        } else return error.UnknownSearchQualityTransition;
        return outcome.TransitionOutcome.applied();
    }

    pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, "search-quality.armed", @intFromBool(world.armed));
        try builder.addNamed(allocator, "search-quality.completed", @intFromBool(world.completed));
        try builder.addNamed(allocator, "search-quality.deferrals", world.deferrals);
        try builder.addNamed(allocator, "search-quality.violated", @intFromBool(world.violated));
    }

    pub fn evaluate(world: *World, sink: *property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, safety_id, !world.violated);
    }

    pub fn done(world: *World) bool {
        return world.completed;
    }

    pub fn snapshotAlloc(world: *const World, allocator: std.mem.Allocator) ![]u8 {
        const bytes = try allocator.alloc(u8, 4);
        bytes[0] = @intFromBool(world.armed);
        bytes[1] = @intFromBool(world.completed);
        bytes[2] = @intFromBool(world.violated);
        bytes[3] = world.deferrals;
        return bytes;
    }

    pub fn restoreSnapshot(world: *World, bytes: []const u8, _: std.mem.Allocator) !void {
        if (bytes.len != 4 or bytes[0] > 1 or bytes[1] > 1 or bytes[2] > 1 or bytes[3] > 3)
            return error.InvalidSearchQualitySnapshot;
        world.* = .{
            .armed = bytes[0] == 1,
            .completed = bytes[1] == 1,
            .violated = bytes[2] == 1,
            .deferrals = bytes[3],
        };
    }
};

pub const SearchStrategy = enum {
    random,
    guided,
    spliced,
    starvation,
    checkpoint_assisted,

    pub fn jsonName(self: SearchStrategy) []const u8 {
        return switch (self) {
            .random => "random",
            .guided => "guided",
            .spliced => "spliced",
            .starvation => "starvation",
            .checkpoint_assisted => "checkpoint-assisted",
        };
    }
};

pub const StrategyResult = struct {
    strategy: SearchStrategy,
    histories: u64,
    failure_histories: u64,
    discovery_probability_ppm: u64,
    transition_work_units: u64,
    transition_cost_per_failure: u64,
    first_failure_history: ?u64,
    distinct_failure_fingerprints: usize,
    exact_replay_checks: u64,
    policy_exercises: u64,
    harness_errors: u64,
    replay_divergences: u64,

    pub fn validate(self: StrategyResult) !void {
        if (self.histories == 0 or self.failure_histories == 0 or self.distinct_failure_fingerprints != 1)
            return error.SearchStrategyDidNotDiscoverInjectedBug;
        if (self.first_failure_history == null or self.discovery_probability_ppm == 0 or self.transition_work_units == 0 or self.transition_cost_per_failure == 0)
            return error.SearchStrategyMissingQualityEvidence;
        if (self.policy_exercises == 0) return error.SearchStrategyPolicyWasNotExercised;
        if (self.harness_errors != 0) return error.SearchStrategyHarnessError;
        if (self.replay_divergences != 0) return error.SearchStrategyReplayDiverged;
    }
};

pub const SearchQualityResult = struct {
    intentional_bug_fingerprint: u64,
    strategies: [5]StrategyResult,

    pub fn validate(self: SearchQualityResult) !void {
        if (self.intentional_bug_fingerprint != StarvationBugScenario.failure_fingerprint)
            return error.SearchQualityFingerprintChanged;
        var seen: [5]bool = @splat(false);
        for (self.strategies) |result| {
            try result.validate();
            const ordinal = @intFromEnum(result.strategy);
            if (seen[ordinal]) return error.DuplicateSearchStrategy;
            seen[ordinal] = true;
        }
        for (seen) |present| if (!present) return error.MissingSearchStrategy;
    }
};

/// Run the same injected failure through every supported exploration policy.
/// Probabilities are deterministic empirical rates in parts per million and
/// cost is measured only in modeled scenario transitions, never host time.
pub fn searchQuality(allocator: std.mem.Allocator, histories: u64) !SearchQualityResult {
    const result = SearchQualityResult{
        .intentional_bug_fingerprint = StarvationBugScenario.failure_fingerprint,
        .strategies = .{
            try explorerStrategy(allocator, .random, histories, .{
                .uniform_percent = 100,
                .splice_percent = 0,
                .checkpoint_percent = 0,
            }),
            try explorerStrategy(allocator, .guided, histories, .{
                .uniform_percent = 0,
                .splice_percent = 0,
                .checkpoint_percent = 0,
            }),
            try explorerStrategy(allocator, .spliced, histories, .{
                .uniform_percent = 20,
                .splice_percent = 80,
                .checkpoint_percent = 0,
            }),
            try starvationStrategy(allocator, histories),
            try explorerStrategy(allocator, .checkpoint_assisted, histories, .{
                .uniform_percent = 0,
                .splice_percent = 0,
                .checkpoint_percent = 100,
            }),
        },
    };
    try result.validate();
    return result;
}

const ExplorerPolicy = struct {
    uniform_percent: u8,
    splice_percent: u8,
    checkpoint_percent: u8,
};

fn explorerStrategy(
    allocator: std.mem.Allocator,
    strategy: SearchStrategy,
    histories: u64,
    policy_config: ExplorerPolicy,
) !StrategyResult {
    var campaign = try explorer.Campaign(StarvationBugScenario).init(allocator, .{
        .system = "vopr-search-quality-benchmark",
        .histories = histories,
        .transition_budget = 5,
        .seed = @as(u64, 0x5ea2_c420_26) +% @intFromEnum(strategy),
        .uniform_percent = policy_config.uniform_percent,
        .splice_percent = policy_config.splice_percent,
        .checkpoint_percent = policy_config.checkpoint_percent,
    });
    defer campaign.deinit();
    try campaign.run();
    const example = campaign.failure_examples.get(StarvationBugScenario.failure_fingerprint) orelse
        return error.SearchStrategyFoundWrongFailure;
    if (campaign.report.distinct_failure_fingerprints != 1 or campaign.report.failures != campaign.report.property_failure_histories)
        return error.SearchStrategyFoundWrongFailure;
    const exercises = switch (strategy) {
        .random => campaign.report.generated,
        .guided => campaign.report.mutated,
        .spliced => campaign.report.spliced,
        .checkpoint_assisted => campaign.report.checkpoint_resumes,
        .starvation => unreachable,
    };
    return strategyResult(
        strategy,
        histories,
        campaign.report.property_failure_histories,
        campaign.report.transition_work_units,
        example.first_history,
        campaign.report.distinct_failure_fingerprints,
        campaign.report.exact_replay_checks,
        exercises,
        campaign.report.harness_errors,
        campaign.report.replay_divergences,
    );
}

fn starvationStrategy(allocator: std.mem.Allocator, histories: u64) !StrategyResult {
    var failures: u64 = 0;
    var transition_work_units: u64 = 0;
    var exact_replay_checks: u64 = 0;
    var forced: u64 = 0;
    var first_failure_history: ?u64 = null;
    for (0..histories) |history| {
        var starving = choice.Starving.init(StarvationBugScenario.victim_id, 2, @as(u64, 0x57a2_0000) +% history);
        var artifact = try runner.run(StarvationBugScenario, allocator, starving.source(), .{
            .system = "vopr-search-quality-benchmark",
            .seed = @as(u64, 0x57a2_0000) +% history,
            .transition_budget = 5,
        });
        defer artifact.deinit();
        transition_work_units +|= artifact.summary.?.transitions;
        forced += @intFromBool(starving.forced);
        if (artifact.failures.items.len != 0) {
            if (artifact.failures.items.len != 1 or artifact.failures.items[0].fingerprint != StarvationBugScenario.failure_fingerprint)
                return error.SearchStrategyFoundWrongFailure;
            failures += 1;
            if (first_failure_history == null) first_failure_history = history;
        }
        var replayed = try replay.exact(StarvationBugScenario, allocator, &artifact);
        replayed.deinit();
        exact_replay_checks += 1;
    }
    return strategyResult(
        .starvation,
        histories,
        failures,
        transition_work_units,
        first_failure_history,
        @intFromBool(failures != 0),
        exact_replay_checks,
        forced,
        0,
        0,
    );
}

fn strategyResult(
    strategy: SearchStrategy,
    histories: u64,
    failures: u64,
    transition_work_units: u64,
    first_failure_history: ?u64,
    fingerprints: usize,
    exact_replay_checks: u64,
    policy_exercises: u64,
    harness_errors: u64,
    replay_divergences: u64,
) StrategyResult {
    return .{
        .strategy = strategy,
        .histories = histories,
        .failure_histories = failures,
        .discovery_probability_ppm = failures *| 1_000_000 / histories,
        .transition_work_units = transition_work_units,
        .transition_cost_per_failure = if (failures == 0) 0 else transition_work_units / failures,
        .first_failure_history = first_failure_history,
        .distinct_failure_fingerprints = fingerprints,
        .exact_replay_checks = exact_replay_checks,
        .policy_exercises = policy_exercises,
        .harness_errors = harness_errors,
        .replay_divergences = replay_divergences,
    };
}

test "checkpoint resume improves deterministic work units without changing search results" {
    const result = try checkpointResume(std.testing.allocator, 128);
    try result.validate();
}

test "search-quality benchmark discovers one injected bug with every search policy" {
    const result = try searchQuality(std.testing.allocator, 64);
    try result.validate();
    try std.testing.expectEqual(@as(u64, 1_000_000), result.strategies[@intFromEnum(SearchStrategy.starvation)].discovery_probability_ppm);
    try std.testing.expect(result.strategies[@intFromEnum(SearchStrategy.spliced)].policy_exercises > 0);
    try std.testing.expect(result.strategies[@intFromEnum(SearchStrategy.checkpoint_assisted)].policy_exercises > 0);
}
