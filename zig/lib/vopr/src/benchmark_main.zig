// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const vopr = @import("vopr");

pub fn main(init: std.process.Init) !void {
    const checkpoint = try vopr.benchmark.checkpointResume(init.gpa, 128);
    const search = try vopr.benchmark.searchQuality(init.gpa, 128);
    var buffer: [4096]u8 = undefined;
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
    for (search.bugs) |bug| for (bug.strategies) |result| try stdout.print(
        "{{\"format\":\"{s}\",\"benchmark\":\"search-quality\",\"bug\":\"{s}\",\"bug_class\":\"{s}\",\"fingerprint\":{d},\"strategy\":\"{s}\",\"histories\":{d},\"failure_histories\":{d},\"repeated_occurrences\":{d},\"discovery_probability_ppm\":{d},\"rarity_ppm\":{d},\"confidence_95_lower_ppm\":{d},\"confidence_95_upper_ppm\":{d},\"transitions_executed\":{d},\"transition_cost_per_occurrence\":{d},\"logical_work_units\":{d},\"logical_work_cost_per_occurrence\":{d},\"first_failure_history\":{?d},\"first_failure_logical_work_units\":{?d},\"simplest_transitions\":{?d},\"simplest_transition_trace_digest\":{?d},\"smallest_retained_output_bytes\":{?d},\"smallest_output_trace_digest\":{?d},\"retained_output_bytes\":{d},\"distinct_failure_fingerprints\":{d},\"exact_replay_checks\":{d},\"policy_exercises\":{d},\"harness_errors\":{d},\"replay_divergences\":{d}}}\n",
        .{
            vopr.benchmark.search_quality_format,
            bug.name,
            @tagName(bug.class),
            bug.fingerprint,
            result.strategy.jsonName(),
            result.histories,
            result.failure_histories,
            result.repeated_occurrences,
            result.discovery_probability_ppm,
            result.rarity_ppm,
            result.confidence.lower_ppm,
            result.confidence.upper_ppm,
            result.transitions_executed,
            result.transition_cost_per_occurrence,
            result.logical_work_units,
            result.logical_work_cost_per_occurrence,
            result.first_failure_history,
            result.first_failure_logical_work_units,
            result.simplest_transitions,
            result.simplest_transition_trace_digest,
            result.smallest_retained_output_bytes,
            result.smallest_output_trace_digest,
            result.retained_output_bytes,
            result.distinct_failure_fingerprints,
            result.exact_replay_checks,
            result.policy_exercises,
            result.harness_errors,
            result.replay_divergences,
        },
    );
    try stdout.flush();
}
