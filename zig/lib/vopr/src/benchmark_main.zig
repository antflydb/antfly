// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const vopr = @import("vopr");

pub fn main(init: std.process.Init) !void {
    const checkpoint = try vopr.benchmark.checkpointResume(init.gpa, 128);
    const search = try vopr.benchmark.searchQuality(init.gpa, 128);
    var buffer: [2048]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print(
        "{{\"benchmark\":\"checkpoint-resume\",\"histories\":{d},\"semantic_features\":{d},\"retained\":{d},\"failures\":{d},\"baseline_transition_work_units\":{d},\"optimized_transition_work_units\":{d},\"checkpoint_hits\":{d},\"checkpoint_resumes\":{d},\"checkpoint_transitions_avoided\":{d},\"improvement_ppm\":{d}}}\n",
        .{
            checkpoint.histories,
            checkpoint.semantic_features,
            checkpoint.retained,
            checkpoint.failures,
            checkpoint.baseline_transition_work_units,
            checkpoint.optimized_transition_work_units,
            checkpoint.checkpoint_hits,
            checkpoint.checkpoint_resumes,
            checkpoint.checkpoint_transitions_avoided,
            checkpoint.improvement_ppm,
        },
    );
    for (search.strategies) |result| try stdout.print(
        "{{\"benchmark\":\"search-quality\",\"intentional_bug_fingerprint\":{d},\"strategy\":\"{s}\",\"histories\":{d},\"failure_histories\":{d},\"discovery_probability_ppm\":{d},\"transition_work_units\":{d},\"transition_cost_per_failure\":{d},\"first_failure_history\":{?d},\"distinct_failure_fingerprints\":{d},\"exact_replay_checks\":{d},\"policy_exercises\":{d},\"harness_errors\":{d},\"replay_divergences\":{d}}}\n",
        .{
            search.intentional_bug_fingerprint,
            result.strategy.jsonName(),
            result.histories,
            result.failure_histories,
            result.discovery_probability_ppm,
            result.transition_work_units,
            result.transition_cost_per_failure,
            result.first_failure_history,
            result.distinct_failure_fingerprints,
            result.exact_replay_checks,
            result.policy_exercises,
            result.harness_errors,
            result.replay_divergences,
        },
    );
    try stdout.flush();
}
