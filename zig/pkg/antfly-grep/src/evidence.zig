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
const source = @import("source.zig");
pub const Region = struct { start: usize, end: usize, p: f64, label: []const u8, label_line: usize };
pub const Evidence = struct {
    file_score: ?f64 = null,
    index: source.Index,
    regions: std.ArrayList(Region) = .empty,
    line_scores: []f64,
    complete: bool = true,
};

/// Coarse source regions are sorted and disjoint. Excerpt rows advance in source
/// order, so each region is visited at most once even when rows have large gaps.
pub const RegionCursor = struct {
    regions: []const Region,
    index: usize = 0,
    previous_line: usize = 0,
    pub fn probability(self: *RegionCursor, line: usize) ?f64 {
        std.debug.assert(line >= self.previous_line);
        self.previous_line = line;
        while (self.index < self.regions.len and self.regions[self.index].end < line) self.index += 1;
        return if (self.index < self.regions.len and self.regions[self.index].start <= line) self.regions[self.index].p else null;
    }
};

test "region sweep agrees with interval lookup for sparse and dense excerpts" {
    var regions: [256]Region = undefined;
    for (&regions, 0..) |*region, i| region.* = .{ .start = i * 7 + 2, .end = i * 7 + 5, .p = @as(f64, @floatFromInt(i % 8)) / 8, .label = "", .label_line = 0 };
    for ([_]usize{ 1, 2, 17, 701 }) |step| {
        var cursor = RegionCursor{ .regions = &regions };
        var line: usize = 1;
        while (line < 4096) : (line += step) {
            var expected: ?f64 = null;
            for (regions) |region| if (region.start <= line and region.end >= line) {
                expected = region.p;
                break;
            };
            try std.testing.expectEqual(expected, cursor.probability(line));
            try std.testing.expect(cursor.index <= regions.len);
        }
    }
    var empty: RegionCursor = .{ .regions = &.{} };
    try std.testing.expectEqual(@as(?f64, null), empty.probability(1));
}

/// Union sorted context intervals, writing each mask position at most once.
pub fn contextMask(a: std.mem.Allocator, selected: []const bool, before: usize, after: usize) ![]const bool {
    if (before == 0 and after == 0) return selected;
    const shown = try a.alloc(bool, selected.len);
    @memset(shown, false);
    var end: usize = 0;
    for (selected, 0..) |yes, i| if (yes) {
        const high = i + @min(after +| 1, selected.len - i);
        const low = @max(end, i -| before);
        if (low < high) @memset(shown[low..high], true);
        end = high;
    };
    return shown;
}

test "linear context union agrees with naive windows including saturated bounds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var selected: [128]bool = undefined;
    for ([_]usize{ 1, 7, 53 }) |step| {
        for (&selected, 0..) |*yes, i| yes.* = i % step == 0;
        for ([_]usize{ 0, 1, 17, std.math.maxInt(usize) }) |before| for ([_]usize{ 0, 2, 33, std.math.maxInt(usize) }) |after| {
            const shown = try contextMask(arena.allocator(), &selected, before, after);
            for (shown, 0..) |yes, i| {
                const low = i -| after;
                const high = i + @min(before, selected.len - i - 1) + 1;
                const expected = std.mem.indexOfScalar(bool, selected[low..high], true) != null;
                try std.testing.expectEqual(expected, yes);
            }
        };
    }
}
