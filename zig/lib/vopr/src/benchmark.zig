// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic search-efficiency benchmarks. These report modeled transition
//! work rather than wall time so results remain stable across hosts and builds.

const std = @import("std");
const explorer = @import("explorer.zig");
const ToyScenario = @import("toy_scenario.zig").ToyScenario;

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

test "checkpoint resume improves deterministic work units without changing search results" {
    const result = try checkpointResume(std.testing.allocator, 128);
    try result.validate();
}
