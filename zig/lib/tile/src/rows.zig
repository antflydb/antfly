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

//! Ordered row operations. NaN, ties, masking and empty-row behavior are explicit.
const std = @import("std");

pub const Argmax = struct { index: u32 = 0, value: f32 = -std.math.inf(f32), found: bool = false };
/// First allowed NaN sticks; subsequent NaNs never win. Equal values keep the
/// lowest index, including -inf. Empty/all-suppressed rows return index zero.
pub fn argmaxSuppressed(values: []const f32, suppressed_ids: []const i32) Argmax {
    var best: Argmax = .{};
    for (values, 0..) |value, index| {
        if (best.found and !(value > best.value)) continue;
        var suppressed = false;
        for (suppressed_ids) |raw| {
            if (raw >= 0 and @as(usize, @intCast(raw)) == index) {
                suppressed = true;
                break;
            }
        }
        if (!suppressed) best = .{ .index = @intCast(index), .value = value, .found = true };
    }
    return best;
}

/// Ordered two-pass statistics with a boolean inclusion mask. NaNs propagate,
/// +inf has logsumexp +inf, an empty/all -inf row has logsumexp -inf.
pub const Statistics = struct { count: usize = 0, sum: f64 = 0, mean: f64 = 0, variance: f64 = 0, logsumexp: f64 = -std.math.inf(f64) };
pub fn statistics(values: []const f32, mask: ?[]const bool) Statistics {
    if (mask) |m| std.debug.assert(m.len == values.len);
    var result: Statistics = .{};
    var maximum: f64 = -std.math.inf(f64);
    for (values, 0..) |v, i| {
        if (mask) |m| {
            if (!m[i]) continue;
        }
        result.count += 1;
        result.sum += v;
        maximum = if (std.math.isNan(v) or std.math.isNan(maximum)) std.math.nan(f64) else @max(maximum, v);
    }
    if (result.count == 0) return result;
    const count: f64 = @floatFromInt(result.count);
    result.mean = result.sum / count;
    var exp_sum: f64 = 0;
    for (values, 0..) |v, i| {
        if (mask) |m| {
            if (!m[i]) continue;
        }
        const d = v - result.mean;
        result.variance += d * d;
        exp_sum += @exp(@as(f64, v) - maximum);
    }
    result.variance /= count;
    result.logsumexp = if (std.math.isInf(maximum)) maximum else maximum + @log(exp_sum);
    return result;
}

/// SIMD single-accumulator sum of squares. No FMA or changed reduction tree.
pub fn sumSquares(comptime lanes: comptime_int, data: []const f32) f32 {
    const V = @Vector(lanes, f32);
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + lanes <= data.len) : (i += lanes) {
        const v: V = data[i..][0..lanes].*;
        acc += v * v;
    }
    var sum = @reduce(.Add, acc);
    while (i < data.len) : (i += 1) sum += data[i] * data[i];
    return sum;
}
