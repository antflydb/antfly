// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const core = @import("core.zig");

// Keep filter parameters dynamic so ReleaseFast executes the optimized loops
// instead of folding the regression into a constant result.
fn filter(spec: []i32, lpc: []const i32, direction: i32, q_lpc: i32) i32 {
    var parameters = [_]i32{ @intCast(spec.len), direction, q_lpc, @intCast(lpc.len) };
    const dynamic: *volatile [4]i32 = &parameters;
    return @call(.never_inline, core.tns_ar_filter, .{ spec.ptr, dynamic.*[0], dynamic.*[1], lpc.ptr, dynamic.*[2], dynamic.*[3] });
}

const fixture_lpc = [_]i32{ 1173774336, 724049920, 59947008, -245432320, 55267328, 223242240 };

test "aacplus TNS preserves signed impulses with zero filter history in ReleaseFast" {
    // Reduced from the reverse order-six filter in the owned 48 kHz HE-AAC
    // mixed fixture. The old subtract reduction changed -370736 into +370736.
    for ([_]i32{ -1, 1 }) |direction| {
        for ([_]i32{ -5931776, 5931776 }) |impulse| {
            var spec: [20]i32 = @splat(0);
            const index: usize = if (direction == -1) 5 else 14;
            spec[index] = impulse;
            try std.testing.expectEqual(@as(i32, 4), filter(&spec, &fixture_lpc, direction, 14));
            try std.testing.expectEqual(impulse >> 4, spec[index]);
        }
    }
}

// Independent wide-integer form of the all-pole recurrence. Each Q31 product
// contributes its signed high word; truncate only after subtracting the full
// feedback sum. Q_lpc <= 16 keeps the final scale an exact left shift.
fn reference(spec: []i32, lpc: []const i32, direction: i32, q_lpc: u5) i32 {
    const up: u6 = @intCast(16 - @as(i32, q_lpc));
    const down: u6 = @intCast(std.math.log2_int(usize, lpc.len) + up);
    for (0..spec.len) |step| {
        const index = if (direction == -1) spec.len - 1 - step else step;
        var feedback: i64 = 0;
        for (0..@min(step, lpc.len)) |tap| {
            const prior = if (direction == -1) index + tap + 1 else index - tap - 1;
            feedback += (@as(i64, spec[prior]) * lpc[tap]) >> 32;
        }
        spec[index] = @truncate((@as(i64, spec[index]) >> down) - (feedback << up));
    }
    return @intCast(down);
}

test "aacplus TNS forward and reverse filters match wide integer recurrence" {
    for ([_]usize{ 1, 2, 3, 4, 5, 6, 8, 12, 16, 20 }) |order| {
        for ([_]usize{ order, order + 1, order + 7, 128 }) |length| {
            for ([_]i32{ -1, 1 }) |direction| {
                for ([_]u5{ 3, 14, 15 }) |q_lpc| {
                    var coefficients: [20]i32 = @splat(0);
                    for (coefficients[0..order], 0..) |*value, i| value.* = @divTrunc(fixture_lpc[i % fixture_lpc.len], @as(i32, @intCast(order)));
                    var actual: [128]i32 = @splat(0);
                    for (actual[0..length], 0..) |*value, i| value.* = (@as(i32, @intCast((i * 7919 + 17) % 65536)) - 32768) * 128;
                    var expected = actual;
                    const shift = reference(expected[0..length], coefficients[0..order], direction, q_lpc);
                    try std.testing.expectEqual(shift, filter(actual[0..length], coefficients[0..order], direction, q_lpc));
                    try std.testing.expectEqualSlices(i32, expected[0..length], actual[0..length]);
                }
            }
        }
    }
}
