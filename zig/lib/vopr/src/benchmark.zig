// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic search-efficiency and recurrence benchmarks. These report
//! modeled transition work rather than wall time so regressions remain stable
//! across hosts and builds.

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

pub const search_quality_format = "vopr-search-quality-v2";

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

pub const BugClass = enum { scheduling, durability, cancellation };

const BugSpec = struct {
    name: []const u8,
    class: BugClass,
    property_name: []const u8,
    arm_name: []const u8,
    secondary_arm_name: ?[]const u8 = null,
    perturb_name: []const u8,
    target_name: []const u8,
    perturb_kind: transition.Kind,
    required_perturbations: u8,
    perturb_limit: u8,
};

const starvation_bug = BugSpec{
    .name = "scheduling-starvation",
    .class = .scheduling,
    .property_name = "search-quality.victim-not-starved",
    .arm_name = "search-quality.make-victim-runnable",
    .perturb_name = "search-quality.run-competitor",
    .target_name = "search-quality.service-victim",
    .perturb_kind = .workload,
    .required_perturbations = 2,
    .perturb_limit = 3,
};

const durability_bug = BugSpec{
    .name = "durable-publication-cutover",
    .class = .durability,
    .property_name = "search-quality.publication-requires-stable-generation",
    .arm_name = "search-quality.prepare-generation",
    .secondary_arm_name = "search-quality.write-generation-bytes",
    .perturb_name = "search-quality.crash-before-directory-sync",
    .target_name = "search-quality.publish-generation",
    .perturb_kind = .fault,
    .required_perturbations = 2,
    .perturb_limit = 3,
};

const cancellation_bug = BugSpec{
    .name = "cancellation-admission-leak",
    .class = .cancellation,
    .property_name = "search-quality.cancel-releases-admission-once",
    .arm_name = "search-quality.acquire-admission",
    .perturb_name = "search-quality.callback-races-cancel",
    .target_name = "search-quality.finish-cancel",
    .perturb_kind = .workload,
    .required_perturbations = 3,
    .perturb_limit = 4,
};

/// Each fixture is intentionally small and reviewable, but represents a
/// distinct production failure class: scheduler starvation, publication after
/// unstable durability, and cancellation/admission cleanup. The target action
/// becomes unsafe only after two competing transitions, making scheduling
/// policy materially affect discovery.
fn InjectedBugScenario(comptime spec: BugSpec) type {
    return struct {
        pub const name: []const u8 = "vopr-search-quality-" ++ spec.name;
        pub const version: u32 = 1;
        pub const arm_id = ids.stable("transition", spec.arm_name);
        pub const secondary_arm_id = if (spec.secondary_arm_name) |name_value|
            ids.stable("transition", name_value)
        else
            0;
        pub const target_id = ids.stable("transition", spec.target_name);
        pub const perturb_id = ids.stable("transition", spec.perturb_name);
        pub const safety_id = ids.stable("property", spec.property_name);
        pub const failure_fingerprint = ids.stable("failure", spec.property_name);
        pub const transition_budget: u64 = 2 + spec.perturb_limit + @intFromBool(spec.secondary_arm_name != null);
        pub const starvation_budget: usize = spec.required_perturbations;
        pub const properties = &[_]property.Declaration{.{
            .id = safety_id,
            .name = spec.property_name,
            .kind = .always,
        }};

        pub const World = struct {
            armed: bool = false,
            prepared: bool = false,
            completed: bool = false,
            violated: bool = false,
            perturbations: u8 = 0,
        };

        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }

        pub fn deinit(_: *World, _: std.mem.Allocator) void {}

        pub fn enumerate(world: *World, list: *transition.List, allocator: std.mem.Allocator) !void {
            if (!world.armed) {
                if (!world.prepared) {
                    try list.append(allocator, .{ .id = arm_id, .name = spec.arm_name, .kind = .workload });
                } else if (spec.secondary_arm_name) |secondary_name| {
                    try list.append(allocator, .{ .id = secondary_arm_id, .name = secondary_name, .kind = .workload });
                } else unreachable;
                return;
            }
            try list.append(allocator, .{
                .id = target_id,
                .name = spec.target_name,
                .kind = .workload,
                .actor_id = ids.stable("actor", spec.target_name),
                .resource_id = ids.stable("resource", spec.property_name),
            });
            if (world.perturbations < spec.perturb_limit) try list.append(allocator, .{
                .id = perturb_id,
                .name = spec.perturb_name,
                .kind = spec.perturb_kind,
                .actor_id = ids.stable("actor", spec.perturb_name),
                .resource_id = ids.stable("resource", spec.property_name),
            });
        }

        pub fn execute(world: *World, selected: transition.Transition, events: *event.Sink, allocator: std.mem.Allocator) !outcome.TransitionOutcome {
            if (selected.id == arm_id) {
                world.prepared = true;
                world.armed = spec.secondary_arm_name == null;
                try events.emitDetailed(allocator, .{
                    .id = ids.stable("event", spec.arm_name),
                    .name = spec.arm_name,
                    .kind = .state_change,
                    .fields = &.{.{ .name = "bug_class", .value = @tagName(spec.class) }},
                }, "injected benchmark precondition armed");
            } else if (spec.secondary_arm_name != null and selected.id == secondary_arm_id) {
                world.armed = true;
                try events.emitDetailed(allocator, .{
                    .id = ids.stable("event", spec.secondary_arm_name.?),
                    .name = spec.secondary_arm_name.?,
                    .kind = .state_change,
                    .fields = &.{.{ .name = "bug_class", .value = @tagName(spec.class) }},
                }, "second durable setup step completed");
            } else if (selected.id == perturb_id) {
                world.perturbations += 1;
                try events.emitDetailed(allocator, .{
                    .id = ids.stable("event", spec.perturb_name),
                    .name = spec.perturb_name,
                    .kind = if (spec.perturb_kind == .fault) .injected_error else .domain,
                    .payload_digest = world.perturbations,
                    .fields = &.{.{ .name = "bug_class", .value = @tagName(spec.class) }},
                }, "injected competing transition");
            } else if (selected.id == target_id) {
                world.violated = world.perturbations >= spec.required_perturbations;
                world.completed = true;
                try events.emitDetailed(allocator, .{
                    .id = ids.stable("event", spec.target_name),
                    .name = spec.target_name,
                    .kind = .client_response,
                    .payload_digest = world.perturbations,
                    .fields = &.{.{ .name = "bug_class", .value = @tagName(spec.class) }},
                }, if (world.violated) "injected failure manifested" else "safe early completion");
            } else return error.UnknownSearchQualityTransition;
            return outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *observation.Builder, allocator: std.mem.Allocator) !void {
            try builder.addNamed(allocator, "search-quality.armed", @intFromBool(world.armed));
            try builder.addNamed(allocator, "search-quality.prepared", @intFromBool(world.prepared));
            try builder.addNamed(allocator, "search-quality.completed", @intFromBool(world.completed));
            try builder.addNamed(allocator, "search-quality.perturbations", world.perturbations);
            try builder.addNamed(allocator, "search-quality.violated", @intFromBool(world.violated));
        }

        pub fn evaluate(world: *World, sink: *property.Sink, allocator: std.mem.Allocator) !void {
            try sink.checkDetailed(allocator, safety_id, !world.violated, if (world.violated) spec.name else "");
        }

        pub fn done(world: *World) bool {
            return world.completed;
        }

        pub fn snapshotAlloc(world: *const World, allocator: std.mem.Allocator) ![]u8 {
            const bytes = try allocator.alloc(u8, 5);
            bytes[0] = @intFromBool(world.armed);
            bytes[1] = @intFromBool(world.prepared);
            bytes[2] = @intFromBool(world.completed);
            bytes[3] = @intFromBool(world.violated);
            bytes[4] = world.perturbations;
            return bytes;
        }

        pub fn restoreSnapshot(world: *World, bytes: []const u8, _: std.mem.Allocator) !void {
            if (bytes.len != 5 or bytes[0] > 1 or bytes[1] > 1 or bytes[2] > 1 or bytes[3] > 1 or bytes[4] > spec.perturb_limit)
                return error.InvalidSearchQualitySnapshot;
            world.* = .{
                .armed = bytes[0] == 1,
                .prepared = bytes[1] == 1,
                .completed = bytes[2] == 1,
                .violated = bytes[3] == 1,
                .perturbations = bytes[4],
            };
        }
    };
}

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

pub const ConfidenceInterval = struct {
    lower_ppm: u64,
    upper_ppm: u64,
    confidence_ppm: u64 = 950_000,
};

pub const StrategyResult = struct {
    strategy: SearchStrategy,
    histories: u64,
    failure_histories: u64,
    repeated_occurrences: u64,
    discovery_probability_ppm: u64,
    rarity_ppm: u64,
    confidence: ConfidenceInterval,
    transitions_executed: u64,
    transition_cost_per_occurrence: u64,
    logical_work_units: u64,
    logical_work_cost_per_occurrence: u64,
    first_failure_history: ?u64,
    first_failure_logical_work_units: ?u64,
    simplest_transitions: ?u64,
    simplest_transition_trace_digest: ?u64,
    smallest_retained_output_bytes: ?u64,
    smallest_output_trace_digest: ?u64,
    retained_output_bytes: u64,
    distinct_failure_fingerprints: usize,
    exact_replay_checks: u64,
    policy_exercises: u64,
    harness_errors: u64,
    replay_divergences: u64,

    pub fn validate(self: StrategyResult) !void {
        if (self.histories == 0 or self.failure_histories == 0 or self.repeated_occurrences < 2 or
            self.distinct_failure_fingerprints != 1) return error.SearchStrategyDidNotRepeatedlyDiscoverInjectedBug;
        if (self.first_failure_history == null or self.first_failure_logical_work_units == null or
            self.simplest_transitions == null or self.smallest_retained_output_bytes == null or
            self.discovery_probability_ppm == 0 or self.transitions_executed == 0 or
            self.transition_cost_per_occurrence == 0 or self.logical_work_units == 0 or
            self.logical_work_cost_per_occurrence == 0 or self.retained_output_bytes == 0)
            return error.SearchStrategyMissingQualityEvidence;
        if (self.confidence.lower_ppm > self.discovery_probability_ppm or
            self.confidence.upper_ppm < self.discovery_probability_ppm or
            self.confidence.upper_ppm > 1_000_000) return error.InvalidSearchConfidenceInterval;
        if (self.rarity_ppm != 1_000_000 - self.discovery_probability_ppm)
            return error.InvalidSearchRarity;
        if (self.policy_exercises == 0) return error.SearchStrategyPolicyWasNotExercised;
        if (self.exact_replay_checks == 0) return error.SearchStrategyDidNotExactReplayRetainedEvidence;
        if (self.harness_errors != 0) return error.SearchStrategyHarnessError;
        if (self.replay_divergences != 0) return error.SearchStrategyReplayDiverged;
    }
};

pub const BugResult = struct {
    name: []const u8,
    class: BugClass,
    fingerprint: ids.StableId,
    strategies: [5]StrategyResult,

    pub fn validate(self: BugResult) !void {
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

pub const SearchQualityResult = struct {
    histories_per_bug_strategy: u64,
    bugs: [3]BugResult,

    pub fn validate(self: SearchQualityResult) !void {
        if (self.histories_per_bug_strategy == 0) return error.InvalidHistoryBudget;
        var fingerprints: [3]ids.StableId = undefined;
        for (self.bugs, 0..) |bug, index| {
            try bug.validate();
            if (bug.fingerprint == 0) return error.InvalidInjectedBugFingerprint;
            for (fingerprints[0..index]) |prior| if (prior == bug.fingerprint)
                return error.DuplicateInjectedBugFingerprint;
            fingerprints[index] = bug.fingerprint;
        }
    }

    pub fn renderJsonAlloc(self: SearchQualityResult, allocator: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(allocator, .{
            .format = search_quality_format,
            .histories_per_bug_strategy = self.histories_per_bug_strategy,
            .bugs = self.bugs,
        }, .{ .whitespace = .indent_2 });
    }
};

/// Run every injected failure through every supported exploration policy.
/// Probability and Wilson 95% confidence bounds are deterministic empirical
/// rates in parts per million. Costs use modeled scenario transitions and
/// logical search work, never host time.
pub fn searchQuality(allocator: std.mem.Allocator, histories: u64) !SearchQualityResult {
    if (histories < 8) return error.SearchQualityHistoryBudgetTooSmall;
    const result = SearchQualityResult{
        .histories_per_bug_strategy = histories,
        .bugs = .{
            try benchmarkBug(InjectedBugScenario(starvation_bug), allocator, starvation_bug, histories),
            try benchmarkBug(InjectedBugScenario(durability_bug), allocator, durability_bug, histories),
            try benchmarkBug(InjectedBugScenario(cancellation_bug), allocator, cancellation_bug, histories),
        },
    };
    try result.validate();
    return result;
}

fn benchmarkBug(comptime Scenario: type, allocator: std.mem.Allocator, comptime spec: BugSpec, histories: u64) !BugResult {
    return .{
        .name = spec.name,
        .class = spec.class,
        .fingerprint = Scenario.failure_fingerprint,
        .strategies = .{
            try explorerStrategy(Scenario, allocator, .random, histories, .{
                .uniform_percent = 100,
                .splice_percent = 0,
                .checkpoint_percent = 0,
            }),
            try explorerStrategy(Scenario, allocator, .guided, histories, .{
                .uniform_percent = 0,
                .splice_percent = 0,
                .checkpoint_percent = 0,
            }),
            try explorerStrategy(Scenario, allocator, .spliced, histories, .{
                .uniform_percent = 20,
                .splice_percent = 80,
                .checkpoint_percent = 0,
            }),
            try starvationStrategy(Scenario, allocator, histories),
            try explorerStrategy(Scenario, allocator, .checkpoint_assisted, histories, .{
                .uniform_percent = 0,
                .splice_percent = 0,
                .checkpoint_percent = 100,
            }),
        },
    };
}

const ExplorerPolicy = struct {
    uniform_percent: u8,
    splice_percent: u8,
    checkpoint_percent: u8,
};

fn explorerStrategy(
    comptime Scenario: type,
    allocator: std.mem.Allocator,
    strategy: SearchStrategy,
    histories: u64,
    policy_config: ExplorerPolicy,
) !StrategyResult {
    var campaign = try explorer.Campaign(Scenario).init(allocator, .{
        .system = "vopr-search-quality-benchmark",
        .histories = histories,
        .transition_budget = Scenario.transition_budget,
        .seed = @as(u64, 0x5ea2_c420_26) +% @intFromEnum(strategy) +% Scenario.failure_fingerprint,
        .uniform_percent = policy_config.uniform_percent,
        .splice_percent = policy_config.splice_percent,
        .checkpoint_percent = policy_config.checkpoint_percent,
        .retained_flight_recordings = 0,
    });
    defer campaign.deinit();
    try campaign.run();
    const example = campaign.failure_examples.get(Scenario.failure_fingerprint) orelse
        return error.SearchStrategyFoundWrongFailure;
    if (campaign.report.distinct_failure_fingerprints != 1 or
        campaign.report.failures != campaign.report.property_failure_histories)
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
        example.occurrences,
        campaign.report.transitions_executed,
        campaign.report.transition_work_units,
        example.first_history,
        example.first_logical_work_units,
        example.smallest_transitions,
        example.smallest_trace_digest,
        example.smallest_retained_output_bytes,
        example.smallest_output_trace_digest,
        campaign.report.retained_trace_bytes,
        campaign.report.distinct_failure_fingerprints,
        campaign.report.exact_replay_checks,
        exercises,
        campaign.report.harness_errors,
        campaign.report.replay_divergences,
    );
}

fn starvationStrategy(comptime Scenario: type, allocator: std.mem.Allocator, histories: u64) !StrategyResult {
    var failures: u64 = 0;
    var transitions_executed: u64 = 0;
    var logical_work_units: u64 = 0;
    var exact_replay_checks: u64 = 0;
    var forced: u64 = 0;
    var retained_output_bytes: u64 = 0;
    var first_failure_history: ?u64 = null;
    var first_failure_logical_work_units: ?u64 = null;
    var simplest_transitions: ?u64 = null;
    var simplest_transition_trace_digest: ?u64 = null;
    var smallest_retained_output_bytes: ?u64 = null;
    var smallest_output_trace_digest: ?u64 = null;
    for (0..histories) |history| {
        var starving = choice.Starving.init(Scenario.target_id, Scenario.starvation_budget, @as(u64, 0x57a2_0000) +% history +% Scenario.failure_fingerprint);
        var artifact = try runner.run(Scenario, allocator, starving.source(), .{
            .system = "vopr-search-quality-benchmark",
            .seed = @as(u64, 0x57a2_0000) +% history,
            .transition_budget = Scenario.transition_budget,
        });
        defer artifact.deinit();
        transitions_executed +|= artifact.summary.?.transitions;
        logical_work_units +|= artifact.summary.?.transitions;
        forced += @intFromBool(starving.forced);
        if (artifact.failures.items.len != 0) {
            if (artifact.failures.items.len != 1 or artifact.failures.items[0].fingerprint != Scenario.failure_fingerprint)
                return error.SearchStrategyFoundWrongFailure;
            failures += 1;
            const bytes = try artifact.renderAlloc(allocator);
            defer allocator.free(bytes);
            const output_bytes: u64 = @intCast(bytes.len);
            const digest = ids.digest(bytes);
            retained_output_bytes +|= output_bytes;
            if (first_failure_history == null) {
                first_failure_history = history;
                first_failure_logical_work_units = logical_work_units;
            }
            if (simplest_transitions == null or artifact.summary.?.transitions < simplest_transitions.? or
                (artifact.summary.?.transitions == simplest_transitions.? and digest < simplest_transition_trace_digest.?))
            {
                simplest_transitions = artifact.summary.?.transitions;
                simplest_transition_trace_digest = digest;
            }
            if (smallest_retained_output_bytes == null or output_bytes < smallest_retained_output_bytes.? or
                (output_bytes == smallest_retained_output_bytes.? and digest < smallest_output_trace_digest.?))
            {
                smallest_retained_output_bytes = output_bytes;
                smallest_output_trace_digest = digest;
            }
        }
        var replayed = try replay.exact(Scenario, allocator, &artifact);
        replayed.deinit();
        exact_replay_checks += 1;
    }
    return strategyResult(
        .starvation,
        histories,
        failures,
        failures,
        transitions_executed,
        logical_work_units,
        first_failure_history,
        first_failure_logical_work_units,
        simplest_transitions,
        simplest_transition_trace_digest,
        smallest_retained_output_bytes,
        smallest_output_trace_digest,
        retained_output_bytes,
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
    failure_histories: u64,
    repeated_occurrences: u64,
    transitions_executed: u64,
    logical_work_units: u64,
    first_failure_history: ?u64,
    first_failure_logical_work_units: ?u64,
    simplest_transitions: ?u64,
    simplest_transition_trace_digest: ?u64,
    smallest_retained_output_bytes: ?u64,
    smallest_output_trace_digest: ?u64,
    retained_output_bytes: u64,
    fingerprints: usize,
    exact_replay_checks: u64,
    policy_exercises: u64,
    harness_errors: u64,
    replay_divergences: u64,
) StrategyResult {
    const probability = failure_histories *| 1_000_000 / histories;
    return .{
        .strategy = strategy,
        .histories = histories,
        .failure_histories = failure_histories,
        .repeated_occurrences = repeated_occurrences,
        .discovery_probability_ppm = probability,
        .rarity_ppm = 1_000_000 - probability,
        .confidence = wilson95(failure_histories, histories),
        .transitions_executed = transitions_executed,
        .transition_cost_per_occurrence = if (repeated_occurrences == 0) 0 else transitions_executed / repeated_occurrences,
        .logical_work_units = logical_work_units,
        .logical_work_cost_per_occurrence = if (repeated_occurrences == 0) 0 else logical_work_units / repeated_occurrences,
        .first_failure_history = first_failure_history,
        .first_failure_logical_work_units = first_failure_logical_work_units,
        .simplest_transitions = simplest_transitions,
        .simplest_transition_trace_digest = simplest_transition_trace_digest,
        .smallest_retained_output_bytes = smallest_retained_output_bytes,
        .smallest_output_trace_digest = smallest_output_trace_digest,
        .retained_output_bytes = retained_output_bytes,
        .distinct_failure_fingerprints = fingerprints,
        .exact_replay_checks = exact_replay_checks,
        .policy_exercises = policy_exercises,
        .harness_errors = harness_errors,
        .replay_divergences = replay_divergences,
    };
}

/// Wilson score interval with z=1.96. Floating point is used only to summarize
/// already deterministic integer counts and is rounded immediately to ppm;
/// it never guides exploration or enters replay bytes.
fn wilson95(successes: u64, trials: u64) ConfidenceInterval {
    if (trials == 0) return .{ .lower_ppm = 0, .upper_ppm = 1_000_000 };
    const n: f64 = @floatFromInt(trials);
    const p: f64 = @as(f64, @floatFromInt(successes)) / n;
    const z: f64 = 1.959963984540054;
    const z_squared = z * z;
    const denominator = 1.0 + z_squared / n;
    const center = (p + z_squared / (2.0 * n)) / denominator;
    const half = z * @sqrt((p * (1.0 - p) / n) + z_squared / (4.0 * n * n)) / denominator;
    return .{
        .lower_ppm = probabilityPpm(center - half),
        .upper_ppm = probabilityPpm(center + half),
    };
}

fn probabilityPpm(value: f64) u64 {
    const bounded = @max(0.0, @min(1.0, value));
    return @intFromFloat(@round(bounded * 1_000_000.0));
}

test "checkpoint resume improves deterministic work units without changing search results" {
    const result = try checkpointResume(std.testing.allocator, 128);
    try result.validate();
}

test "search-quality benchmark repeatedly discovers a multi-bug corpus with every policy" {
    const result = try searchQuality(std.testing.allocator, 64);
    try result.validate();
    try std.testing.expectEqual(@as(usize, 3), result.bugs.len);
    for (result.bugs) |bug| {
        const starvation = bug.strategies[@intFromEnum(SearchStrategy.starvation)];
        try std.testing.expectEqual(@as(u64, 1_000_000), starvation.discovery_probability_ppm);
        try std.testing.expectEqual(@as(u64, 64), starvation.repeated_occurrences);
        try std.testing.expect(bug.strategies[@intFromEnum(SearchStrategy.spliced)].policy_exercises > 0);
        try std.testing.expect(bug.strategies[@intFromEnum(SearchStrategy.checkpoint_assisted)].policy_exercises > 0);
    }
    const rendered = try result.renderJsonAlloc(std.testing.allocator);
    defer std.testing.allocator.free(rendered);
    try std.testing.expect(std.mem.indexOf(u8, rendered, search_quality_format) != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "smallest_retained_output_bytes") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "confidence") != null);
}
