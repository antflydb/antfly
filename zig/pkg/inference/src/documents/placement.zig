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

//! Allocation-free ordered text placement shared by mutation and evidence.
//! Earliest/latest monotone scans establish a unique assignment without
//! rescanning each region's remaining suffix to count repeated occurrences.
const std = @import("std");
const c = @import("contract.zig");
pub const Range = struct { start: usize, end: usize };
pub const Description = struct {
    text: ?[]const u8,
    first: ?usize = null,
    last: ?usize = null,
    pub fn region(item: c.Region) Description {
        return .{ .text = item.text, .first = item.text_start, .last = item.text_end };
    }
};
fn explicit(text: []const u8, scope: Range, item: Description) ?Range {
    const start = item.first orelse return null;
    const end = item.last orelse return null;
    if (scope.end > text.len or start < scope.start or end <= start or end > scope.end) return null;
    if (item.text) |value| if (!std.mem.eql(u8, text[start..end], value)) return null;
    return .{ .start = start, .end = end };
}
pub fn forward(text: []const u8, scope: Range, cursor: usize, item: Description) ?Range {
    if (scope.start > cursor or cursor > scope.end or scope.end > text.len) return null;
    // Explicit, partial or invalid offsets never fall back to another occurrence.
    if (item.first != null or item.last != null) {
        const range = explicit(text, scope, item) orelse return null;
        return if (range.start >= cursor) range else null;
    }
    const value = item.text orelse return null;
    if (value.len == 0) return null;
    const start = std.mem.indexOfPos(u8, text[0..scope.end], cursor, value) orelse return null;
    return .{ .start = start, .end = start + value.len };
}
fn backward(text: []const u8, scope: Range, cursor: usize, item: Description) ?Range {
    if (scope.start > cursor or cursor > scope.end or scope.end > text.len) return null;
    if (item.first != null or item.last != null) {
        const range = explicit(text, scope, item) orelse return null;
        return if (range.end <= cursor) range else null;
    }
    const value = item.text orelse return null;
    if (value.len == 0) return null;
    const offset = std.mem.lastIndexOf(u8, text[scope.start..cursor], value) orelse return null;
    return .{ .start = scope.start + offset, .end = scope.start + offset + value.len };
}
pub const Check = struct {
    scope: Range,
    left: usize,
    right: usize,
    earliest: u128 = 0,
    latest: u128 = 0,
    pub fn init(scope: Range) Check {
        return .{ .scope = scope, .left = scope.start, .right = scope.end };
    }
    pub fn push(self: *Check, text: []const u8, item: Description) bool {
        const range = forward(text, self.scope, self.left, item) orelse return false;
        self.left = range.end;
        self.earliest += range.start;
        return true;
    }
    pub fn pop(self: *Check, text: []const u8, item: Description) bool {
        const range = backward(text, self.scope, self.right, item) orelse return false;
        self.right = range.start;
        self.latest += range.start;
        return true;
    }
    pub fn unique(self: Check) bool {
        // Every earliest position is <= its latest position. Equal sums imply
        // equality per region, with no hash collisions or per-region storage.
        // u128 holds usize positions summed over the bounded region count.
        return self.earliest == self.latest;
    }
};
pub fn qualified(text: []const u8, scope: Range, regions: []const c.Region) bool {
    var check = Check.init(scope);
    for (regions) |region| if (!check.push(text, Description.region(region))) return false;
    var i = regions.len;
    while (i > 0) {
        i -= 1;
        if (!check.pop(text, Description.region(regions[i]))) return false;
    }
    return check.unique();
}

test "document placement distinguishes unique sibling order from ambiguous repeated text" {
    const repeated = [_]c.Region{ .{ .id = 0, .text = "10" }, .{ .id = 1, .text = "10" } };
    try std.testing.expect(qualified("10 10", .{ .start = 0, .end = 5 }, &repeated));
    try std.testing.expect(!qualified("10 10", .{ .start = 0, .end = 5 }, repeated[0..1]));
    try std.testing.expect(!qualified("10 10 10", .{ .start = 0, .end = 8 }, &repeated));
    const anchored = [_]c.Region{.{ .id = 0, .text = "10", .text_start = 3, .text_end = 5 }};
    try std.testing.expect(qualified("10 10", .{ .start = 0, .end = 5 }, &anchored));
    for ([_]Description{
        .{ .text = "10", .first = 0, .last = 1 },
        .{ .text = "10", .first = 3 },
        .{ .text = "10", .last = 5 },
        .{ .text = "10", .first = 5, .last = 7 },
        .{ .text = "10", .first = std.math.maxInt(usize), .last = 5 },
    }) |item| try std.testing.expect(forward("10 10", .{ .start = 0, .end = 5 }, 0, item) == null);
    try std.testing.expect(forward("10 10", .{ .start = 3, .end = 5 }, 3, .{ .text = "10", .first = 0, .last = 2 }) == null);
    const unicode = [_]c.Region{.{ .id = 0, .text = "李", .text_start = 5, .text_end = 8 }};
    try std.testing.expect(qualified("李; 李", .{ .start = 0, .end = 8 }, &unicode));
}

test "document placement qualification matches exhaustive ordered assignments" {
    const Oracle = struct {
        fn count(text: []const u8, items: []const c.Region, cursor: usize) usize {
            if (items.len == 0) return 1;
            var assignments: usize = 0;
            var pos = cursor;
            while (std.mem.indexOfPos(u8, text, pos, items[0].text)) |start| {
                assignments += @This().count(text, items[1..], start + items[0].text.len);
                if (assignments > 1) return 2;
                pos = start + 1;
            }
            return assignments;
        }
    };
    const words = [_][]const u8{ "a", "b", "aa" };
    for (0..7) |length| {
        var bytes: [6]u8 = undefined;
        for (0..@as(usize, 1) << @intCast(length)) |bits| {
            for (bytes[0..length], 0..) |*byte, i| byte.* = if (bits & (@as(usize, 1) << @intCast(i)) == 0) 'a' else 'b';
            const text = bytes[0..length];
            for (0..4) |size| {
                var combinations: usize = 1;
                for (0..size) |_| combinations *= words.len;
                for (0..combinations) |combination| {
                    var regions: [3]c.Region = undefined;
                    var code = combination;
                    for (regions[0..size], 0..) |*region, i| {
                        region.* = .{ .id = i, .text = words[code % words.len] };
                        code /= words.len;
                    }
                    try std.testing.expectEqual(Oracle.count(text, regions[0..size], 0) == 1, qualified(text, .{ .start = 0, .end = text.len }, regions[0..size]));
                }
            }
        }
    }
}
