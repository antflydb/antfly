// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const vopr = @import("vopr");

pub fn main(init: std.process.Init) !void {
    const result = try vopr.benchmark.checkpointResume(init.gpa, 128);
    var buffer: [2048]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print(
        "{{\"benchmark\":\"checkpoint-resume\",\"histories\":{d},\"semantic_features\":{d},\"retained\":{d},\"failures\":{d},\"baseline_transition_work_units\":{d},\"optimized_transition_work_units\":{d},\"checkpoint_hits\":{d},\"checkpoint_resumes\":{d},\"checkpoint_transitions_avoided\":{d},\"improvement_ppm\":{d}}}\n",
        .{
            result.histories,
            result.semantic_features,
            result.retained,
            result.failures,
            result.baseline_transition_work_units,
            result.optimized_transition_work_units,
            result.checkpoint_hits,
            result.checkpoint_resumes,
            result.checkpoint_transitions_avoided,
            result.improvement_ppm,
        },
    );
    try stdout.flush();
}
