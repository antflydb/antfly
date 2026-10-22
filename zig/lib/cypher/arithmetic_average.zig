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

//! Certificates for an arithmetic sequence whose ordinary online AVG recurrence
//! is exact in binary64. This never substitutes SUM/COUNT for a rounded mean.
const std = @import("std");
pub const Sequence = struct {
    start: i64,
    step: i64,
    length: u128,
    pub fn concat(a: @This(), b: @This()) ?@This() {
        if (a.length == 0) return b;
        if (b.length == 0) return a;
        const step = if (a.length == 1) (if (b.length == 1) std.math.cast(i64, @as(i128, b.start) - a.start) orelse return null else b.step) else a.step;
        if (b.length > 1 and b.step != step) return null;
        const length = std.math.add(u128, a.length, b.length) catch return null;
        const distance = std.math.mul(i128, std.math.cast(i128, a.length) orelse return null, step) catch return null;
        const next = std.math.add(i128, a.start, distance) catch return null;
        if (next != b.start) return null;
        return .{ .start = a.start, .step = step, .length = length };
    }
    pub fn reversed(self: @This()) ?@This() {
        if (self.length == 0) return self;
        const distance = std.math.mul(i128, std.math.cast(i128, self.length - 1) orelse return null, self.step) catch return null;
        const last = std.math.add(i128, self.start, distance) catch return null;
        return .{ .start = std.math.cast(i64, last) orelse return null, .step = std.math.negate(self.step) catch return null, .length = self.length };
    }
    pub fn meanAfter(self: @This(), count: u64, mean: f64) ?f64 {
        if (self.length == 0) return mean;
        const total = std.math.add(u128, count, self.length) catch return null;
        // Keep the ordinary accumulator's weight updates exactly representable.
        if (total > @as(u128, 1) << 52) return null;
        // A repeated integer converts to the same binary64 sample every time.
        // After the first sample the recurrence adds exactly zero, including
        // rounded i64 endpoints. Do not relax nonconstant progression bounds.
        if (self.step == 0) {
            const sample: f64 = @floatFromInt(self.start);
            if (count == 0 or mean == sample) return sample;
            return null;
        }
        // Integers, half-integer means, subtractions, and denominators in the
        // recurrence must all be exactly representable. The conservative
        // bounds cover the entire prefix, not just the final average.
        const first = @as(i128, self.start) - @as(i128, count) * self.step;
        const last = first + @as(i128, @intCast(total - 1)) * self.step;
        const bound = @as(i128, 1) << 50;
        if (@abs(first) > bound or @abs(last) > bound or @as(u128, @abs(@as(i128, self.step))) * total > @as(u128, 1) << 51) return null;
        if (count != 0) {
            const prior: f64 = @as(f64, @floatFromInt(2 * first + @as(i128, count - 1) * self.step)) / 2;
            if (mean != prior) return null;
        }
        return @as(f64, @floatFromInt(first + last)) / 2;
    }
};

test "arithmetic AVG certificates match every ordinary recurrence bit" {
    const average = @import("numbers.zig").nextAverage;
    for ([_]i64{ -1000000, -17, 0, 1, 1000000, (@as(i64, 1) << 50) - 1000 }) |start| {
        for ([_]i64{ -31, -2, -1, 0, 1, 2, 31 }) |step| {
            var mean: f64 = 0;
            for (0..1000) |index| {
                mean = average(mean, @floatFromInt(@as(i128, start) + @as(i128, @intCast(index)) * step), index + 1);
                const sequence: Sequence = .{ .start = start, .step = step, .length = index + 1 };
                if (sequence.meanAfter(0, 0)) |certified| try std.testing.expectEqual(@as(u64, @bitCast(mean)), @as(u64, @bitCast(certified)));
            }
        }
    }
    try std.testing.expect((Sequence{ .start = std.math.maxInt(i64), .step = -1, .length = 10 }).meanAfter(0, 0) == null);
    try std.testing.expect((Sequence{ .start = 1, .step = 1, .length = 100 }).meanAfter(10, 1.234) == null);
    try std.testing.expectEqual(@as(?f64, 100.5), (Sequence{ .start = 101, .step = 1, .length = 100 }).meanAfter(100, 50.5));
}

test "constant AVG converts wide samples once without changing recurrence bits" {
    for ([_]i64{ std.math.minInt(i64), std.math.maxInt(i64), (@as(i64, 1) << 53) + 1, 0, -7 }) |sample| {
        var mean: f64 = 0;
        for (1..1001) |count| {
            mean = @import("numbers.zig").nextAverage(mean, @floatFromInt(sample), count);
            const sequence: Sequence = .{ .start = sample, .step = 0, .length = count };
            try std.testing.expectEqual(@as(u64, @bitCast(mean)), @as(u64, @bitCast(sequence.meanAfter(0, 0).?)));
        }
    }
    try std.testing.expect((Sequence{ .start = 7, .step = 0, .length = 10 }).meanAfter(5, 6.9) == null);
    try std.testing.expect((Sequence{ .start = 7, .step = 0, .length = (@as(u128, 1) << 52) + 1 }).meanAfter(0, 0) == null);
}
