// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const bench = @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup.RefinementBenchmark;
test "native dictionary refinement benchmark" {
    _ = try bench.dictionary(std.testing.io, 512, false);
    _ = try bench.dictionary(std.testing.io, 512, true);
    for ([_]usize{ 262144, 1048576 }) |count| for (0..3) |sample| {
        const first = try bench.dictionary(std.testing.io, count, sample % 2 != 0);
        const second = try bench.dictionary(std.testing.io, count, sample % 2 == 0);
        const baseline = if (sample % 2 == 0) first else second;
        const cached = if (sample % 2 == 0) second else first;
        try std.testing.expectEqual(@as(i64, @intCast(count)), baseline.checksum);
        try std.testing.expectEqual(baseline.checksum, cached.checksum);
        std.debug.print("native_refinement {{\"case\":\"dictionary_reuse\",\"rows\":{d},\"sample\":{d},\"redecode_ns\":{d},\"cached_ns\":{d},\"redecode_peak_bytes\":{d},\"cached_peak_bytes\":{d}}}\n", .{ count, sample, baseline.ns, cached.ns, baseline.peak, cached.peak });
    };
}
