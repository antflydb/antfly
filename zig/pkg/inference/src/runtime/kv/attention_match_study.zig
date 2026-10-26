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

//! History-only representative study selection shared with the CUDA reference
//! at b14eea52c7783e8b19c021f6aec646595f1f1a8b. No query values or validation
//! results participate in selecting the study tokens.
const std = @import("std");

/// Build a bounded self-study sequence from the saved prefix only. Repeating
/// just its tail can calibrate exclusively on boilerplate and miss identifiers
/// earlier in the history. Prefer contiguous windows containing infrequent
/// tokens, keep their original order, and always include the recent suffix.
/// This selects calibration inputs; it never changes the authoritative history
/// or authorizes a candidate without the independent numerical validation.
pub fn studyTokens(allocator: std.mem.Allocator, source: []const i64, maximum: usize) ![]i64 {
    if (source.len < 2 or source.len > 32768 or maximum < 2 or maximum > 32768)
        return error.CompactionQueryLimit;
    for (source) |token| if (token < 0) return error.InvalidCompactionQueries;
    if (source.len <= maximum or maximum < 256)
        return allocator.dupe(i64, source[source.len - @min(source.len, maximum) ..]);
    const window = 64;
    // Keep most calibration queries near the current continuation context.
    // Older informative passages supplement that context rather than replacing
    // it with a replay dominated by early chat/tool declarations.
    const informative = @min(128, (maximum / 4 / window) * window);
    const recent = maximum - informative;
    const take = (maximum - recent) / window;
    const count = (source.len - recent) / window;
    const order = try allocator.alloc(u32, source.len);
    defer allocator.free(order);
    const frequency = try allocator.alloc(u32, source.len);
    defer allocator.free(frequency);
    for (order, 0..) |*index, i| index.* = @intCast(i);
    const ByToken = struct {
        fn less(tokens: []const i64, left: u32, right: u32) bool {
            return tokens[left] < tokens[right];
        }
    };
    std.mem.sort(u32, order, source, ByToken.less);
    var first: usize = 0;
    while (first < order.len) {
        var end = first + 1;
        while (end < order.len and source[order[end]] == source[order[first]]) : (end += 1) {}
        for (order[first..end]) |index| frequency[index] = @intCast(end - first);
        first = end;
    }
    const scores = try allocator.alloc(f64, count);
    defer allocator.free(scores);
    const selected = try allocator.alloc(bool, count);
    defer allocator.free(selected);
    @memset(selected, false);
    for (scores, 0..) |*value, i| {
        value.* = 0;
        for (frequency[i * window ..][0..window]) |n| value.* += 1.0 / @as(f64, @floatFromInt(n));
    }
    for (0..take) |_| {
        var best: ?usize = null;
        var best_distance: usize = 0;
        for (scores, 0..) |value, i| {
            if (selected[i]) continue;
            // Equal-scoring boilerplate is spread across the prefix instead
            // of filling every remaining slot with adjacent early windows.
            var distance = count - i;
            for (selected, 0..) |chosen, j| if (chosen) {
                distance = @min(distance, if (i > j) i - j else j - i);
            };
            if (best == null or value > scores[best.?] or
                (value == scores[best.?] and distance > best_distance))
            {
                best = i;
                best_distance = distance;
            }
        }
        selected[best orelse return error.InvalidCompactionQueries] = true;
    }
    const result = try allocator.alloc(i64, maximum);
    var written: usize = 0;
    for (selected, 0..) |chosen, i| if (chosen) {
        @memcpy(result[written..][0..window], source[i * window ..][0..window]);
        written += window;
    };
    @memcpy(result[written..], source[source.len - recent ..]);
    return result;
}

test "attention match bounded study includes rare earlier spans and exact recent suffix" {
    var source: [32768]i64 = @splat(7);
    for (source[630..650], 0..) |*token, i| token.* = @intCast(100 + i);
    const before = source;
    const study = try studyTokens(std.testing.allocator, &source, 512);
    defer std.testing.allocator.free(study);
    try std.testing.expectEqual(@as(usize, 512), study.len);
    try std.testing.expect(std.mem.indexOf(i64, study, source[630..650]) != null);
    try std.testing.expectEqualSlices(i64, source[source.len - 128 ..], study[study.len - 128 ..]);
    try std.testing.expectEqualSlices(i64, &before, &source);
    const again = try studyTokens(std.testing.allocator, &source, 512);
    defer std.testing.allocator.free(again);
    try std.testing.expectEqualSlices(i64, study, again);
}

test "attention match bounded study is an ordered prefix subsequence at exact budgets" {
    var source: [1024]i64 = undefined;
    for (&source, 0..) |*token, i| token.* = @intCast(i);
    for ([_]usize{ 2, 255, 256, 257, 511, 512, 513, 1023, 1024, 2048 }) |limit| {
        const study = try studyTokens(std.testing.allocator, &source, limit);
        defer std.testing.allocator.free(study);
        try std.testing.expectEqual(@min(source.len, limit), study.len);
        for (study[1..], study[0 .. study.len - 1]) |next, previous|
            try std.testing.expect(next > previous);
        try std.testing.expectEqual(source[source.len - 1], study[study.len - 1]);
        if (limit >= source.len) try std.testing.expectEqualSlices(i64, &source, study);
    }
    try std.testing.expectError(error.CompactionQueryLimit, studyTokens(std.testing.allocator, &source, 1));
    try std.testing.expectError(error.InvalidCompactionQueries, studyTokens(std.testing.allocator, &.{ 1, -1 }, 2));
}

fn studyAllocationFailure(allocator: std.mem.Allocator) !void {
    const source: [1024]i64 = @splat(1);
    const study = try studyTokens(allocator, &source, 512);
    defer allocator.free(study);
}

test "attention match bounded study unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, studyAllocationFailure, .{});
}
