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

pub const Span = struct {
    first: usize,
    end: usize,
};

pub const Plan = struct {
    allocator: std.mem.Allocator,
    spans: []Span,
    mel_bins: usize,
    frequencies: usize,

    pub fn init(
        allocator: std.mem.Allocator,
        filters: []const f32,
        mel_bins: usize,
        frequencies: usize,
    ) !Plan {
        if (mel_bins == 0 or frequencies == 0) return error.InvalidTensorShape;
        const expected = std.math.mul(usize, mel_bins, frequencies) catch return error.InvalidTensorShape;
        if (filters.len != expected) return error.InvalidTensorShape;

        const spans = try allocator.alloc(Span, mel_bins);
        errdefer allocator.free(spans);
        for (spans, 0..) |*span, mel| {
            const row = filters[mel * frequencies ..][0..frequencies];
            var first: usize = 0;
            while (first < row.len and row[first] == 0.0) : (first += 1) {}
            if (first == row.len) {
                span.* = .{ .first = 0, .end = 0 };
                continue;
            }
            var end = row.len;
            while (end > first and row[end - 1] == 0.0) : (end -= 1) {}
            span.* = .{ .first = first, .end = end };
        }
        return .{
            .allocator = allocator,
            .spans = spans,
            .mel_bins = mel_bins,
            .frequencies = frequencies,
        };
    }

    pub fn deinit(self: *Plan) void {
        self.allocator.free(self.spans);
        self.* = undefined;
    }
};

/// Computes one row of mel energies. Finite magnitude rows skip exact-zero
/// filter prefixes and suffixes while retaining the original ascending
/// accumulation order. Non-finite rows use the dense loop so `Inf * 0` and
/// `NaN * 0` retain their IEEE behavior.
pub fn melSums(
    filters: []const f32,
    magnitudes: []const f32,
    plan: Plan,
    out: []f32,
) !void {
    const expected = std.math.mul(usize, plan.mel_bins, plan.frequencies) catch return error.InvalidTensorShape;
    if (filters.len != expected or magnitudes.len != plan.frequencies or
        out.len != plan.mel_bins or plan.spans.len != plan.mel_bins)
        return error.InvalidTensorShape;

    var finite = true;
    for (magnitudes) |magnitude| finite = finite and std.math.isFinite(magnitude);
    for (out, 0..) |*sum_out, mel| {
        const span = plan.spans[mel];
        if (span.first > span.end or span.end > plan.frequencies) return error.InvalidTensorShape;
        const filter = filters[mel * plan.frequencies ..][0..plan.frequencies];
        var sum: f32 = 0.0;
        if (finite) {
            for (span.first..span.end) |frequency| sum += magnitudes[frequency] * filter[frequency];
        } else {
            for (magnitudes, 0..) |magnitude, frequency| sum += magnitude * filter[frequency];
        }
        sum_out.* = sum;
    }
}

test "finite sparse mel sums are bitwise identical to dense accumulation" {
    const filters = [_]f32{
        0, 0,   0.25, 0.75, 0,   0,
        0, 0.5, 1.0,  0.5,  0,   0,
        0, 0,   0,    0.2,  0.8, 0,
    };
    const magnitudes = [_]f32{ 0.125, 2.0, 3.5, 0.75, 4.0, 9.0 };
    var plan = try Plan.init(std.testing.allocator, &filters, 3, 6);
    defer plan.deinit();
    var actual: [3]f32 = undefined;
    try melSums(&filters, &magnitudes, plan, &actual);
    for (actual, 0..) |value, mel| {
        var expected: f32 = 0.0;
        for (magnitudes, 0..) |magnitude, frequency|
            expected += magnitude * filters[mel * 6 + frequency];
        try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(value)));
    }
}

test "all-zero mel bands and non-finite rows preserve dense behavior" {
    const filters = [_]f32{
        0, 0, 0, 0,
        0, 1, 0, 0,
    };
    var plan = try Plan.init(std.testing.allocator, &filters, 2, 4);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 0), plan.spans[0].first);
    try std.testing.expectEqual(@as(usize, 0), plan.spans[0].end);

    const nonfinite_rows = [_][4]f32{
        .{ 1, std.math.inf(f32), 2, 3 },
        .{ 1, std.math.nan(f32), 2, 3 },
    };
    for (nonfinite_rows) |magnitudes| {
        var actual: [2]f32 = undefined;
        try melSums(&filters, &magnitudes, plan, &actual);
        for (actual, 0..) |value, mel| {
            var expected: f32 = 0.0;
            for (magnitudes, 0..) |magnitude, frequency|
                expected += magnitude * filters[mel * 4 + frequency];
            try std.testing.expectEqual(std.math.isNan(expected), std.math.isNan(value));
            if (!std.math.isNan(expected))
                try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(value)));
        }
    }
}

test "sparse mel plan rejects malformed shapes and bounds" {
    try std.testing.expectError(error.InvalidTensorShape, Plan.init(std.testing.allocator, &.{ 1, 2 }, 1, 3));
    try std.testing.expectError(error.InvalidTensorShape, Plan.init(std.testing.allocator, &.{}, 0, 1));

    const filters = [_]f32{ 0, 1, 0, 0 };
    var plan = try Plan.init(std.testing.allocator, &filters, 1, 4);
    defer plan.deinit();
    var out: [1]f32 = undefined;
    try std.testing.expectError(error.InvalidTensorShape, melSums(&filters, &.{ 1, 2, 3 }, plan, &out));
    plan.spans[0].end = 5;
    try std.testing.expectError(error.InvalidTensorShape, melSums(&filters, &.{ 1, 2, 3, 4 }, plan, &out));
}
