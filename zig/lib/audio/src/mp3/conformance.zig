// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

pub const AlignmentMetrics = struct {
    offset: i32,
    compared: usize,
    correlation: f32,
    mean_abs_error: f32,
};

pub fn bestAlignmentMetrics(reference: []const f32, candidate: []const f32, max_offset: usize) AlignmentMetrics {
    var best = AlignmentMetrics{
        .offset = 0,
        .compared = 0,
        .correlation = -1.0,
        .mean_abs_error = std.math.inf(f32),
    };

    var offset: isize = -@as(isize, @intCast(max_offset));
    while (offset <= @as(isize, @intCast(max_offset))) : (offset += 1) {
        const ref_start: usize = if (offset > 0) @intCast(offset) else 0;
        const candidate_start: usize = if (offset < 0) @intCast(-offset) else 0;
        const compared = @min(reference.len -| ref_start, candidate.len -| candidate_start);
        if (compared < 8000) continue;

        var dot: f64 = 0.0;
        var ref_energy: f64 = 0.0;
        var candidate_energy: f64 = 0.0;
        var abs_diff_sum: f64 = 0.0;

        for (0..compared) |i| {
            const ref_sample = reference[ref_start + i];
            const candidate_sample = candidate[candidate_start + i];
            dot += @as(f64, ref_sample) * @as(f64, candidate_sample);
            ref_energy += @as(f64, ref_sample) * @as(f64, ref_sample);
            candidate_energy += @as(f64, candidate_sample) * @as(f64, candidate_sample);
            abs_diff_sum += @abs(@as(f64, ref_sample) - @as(f64, candidate_sample));
        }

        if (ref_energy == 0 or candidate_energy == 0) continue;

        const correlation: f32 = @floatCast(dot / @sqrt(ref_energy * candidate_energy));
        const mean_abs_error: f32 = @floatCast(abs_diff_sum / @as(f64, @floatFromInt(compared)));
        if (correlation > best.correlation) {
            best = .{
                .offset = @intCast(offset),
                .compared = compared,
                .correlation = correlation,
                .mean_abs_error = mean_abs_error,
            };
        }
    }

    return best;
}
