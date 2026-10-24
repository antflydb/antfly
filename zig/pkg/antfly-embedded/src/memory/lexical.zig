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

//! Shared whole-word retrieval features. Scores are retrieval signals, not
//! identity judgments. Token sets avoid repeated-word and substring inflation.
const std = @import("std");
const A = std.mem.Allocator;
pub const Set = std.StringHashMapUnmanaged(void);
const stops = " a an and are as at be been being by for from had has have he her hers him his i in into is it its me my of on or our ours she that the their theirs them they this those to was we were what when where which who will with you your ";
fn word(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c >= 128;
}
pub fn tokens(a: A, text: []const u8) !Set {
    var result: Set = .empty;
    var start: usize = 0;
    while (start < text.len) {
        if (!word(text[start])) {
            start += 1;
            continue;
        }
        var end = start + 1;
        while (end < text.len and word(text[end])) : (end += 1) {}
        const token = try a.dupe(u8, text[start..end]);
        for (token) |*c| c.* = std.ascii.toLower(c.*);
        const needle = try std.fmt.allocPrint(a, " {s} ", .{token});
        if (std.mem.indexOf(u8, stops, needle) == null) try result.put(a, token, {});
        start = end;
    }
    return result;
}
// Token keys borrow the input, which must outlive the prepared query. Hashing
// folds ASCII on the stack so scoring never needs a normalized text copy.
const Folded = struct {
    pub fn hash(_: @This(), value: []const u8) u64 {
        var state = std.hash.Wyhash.init(0);
        var buffer: [64]u8 = undefined;
        var offset: usize = 0;
        while (offset < value.len) {
            const count = @min(buffer.len, value.len - offset);
            for (value[offset..][0..count], buffer[0..count]) |c, *lower| lower.* = std.ascii.toLower(c);
            state.update(buffer[0..count]);
            offset += count;
        }
        return state.final();
    }
    pub fn eql(_: @This(), left: []const u8, right: []const u8) bool {
        return std.ascii.eqlIgnoreCase(left, right);
    }
};
const Words = std.HashMapUnmanaged([]const u8, u64, Folded, std.hash_map.default_max_load_percentage);
const TextWords = std.HashMapUnmanaged([]const u8, void, Folded, std.hash_map.default_max_load_percentage);
const Iterator = struct {
    text: []const u8,
    offset: usize = 0,
    fn next(self: *@This()) ?[]const u8 {
        while (self.offset < self.text.len) {
            if (!word(self.text[self.offset])) {
                self.offset += 1;
                continue;
            }
            const start = self.offset;
            while (self.offset < self.text.len and word(self.text[self.offset])) : (self.offset += 1) {}
            const token = self.text[start..self.offset];
            var stopwords = std.mem.tokenizeScalar(u8, stops, ' ');
            var skip = false;
            while (stopwords.next()) |stop| if (std.ascii.eqlIgnoreCase(stop, token)) {
                skip = true;
                break;
            };
            if (!skip) return token;
        }
        return null;
    }
};

/// Query vocabulary has request lifetime. Coverage uses an epoch per statement
/// to count each query token once, with no per-hit allocation. Overlap reuses
/// a separate text set whose capacity is bounded by the largest compared view.
pub const PreparedQuery = struct {
    allocator: A,
    input: []const u8,
    words: Words = .empty,
    text_words: TextWords = .empty,
    epoch: u64 = 0,
    pub fn init(a: A, input: []const u8) !PreparedQuery {
        var self: PreparedQuery = .{ .allocator = a, .input = input };
        errdefer self.deinit();
        var iterator: Iterator = .{ .text = input };
        while (iterator.next()) |token| try self.words.put(a, token, 0);
        return self;
    }
    pub fn deinit(self: *PreparedQuery) void {
        self.words.deinit(self.allocator);
        self.text_words.deinit(self.allocator);
        self.* = undefined;
    }
    fn matches(self: *PreparedQuery, text: []const u8) usize {
        self.epoch +%= 1;
        if (self.epoch == 0) {
            var values = self.words.valueIterator();
            while (values.next()) |value| value.* = 0;
            self.epoch = 1;
        }
        var count: usize = 0;
        var iterator: Iterator = .{ .text = text };
        while (iterator.next()) |token| if (self.words.getPtr(token)) |seen| {
            if (seen.* != self.epoch) {
                seen.* = self.epoch;
                count += 1;
            }
        };
        return count;
    }
    pub fn coverage(self: *PreparedQuery, text: []const u8) f64 {
        if (self.input.len == 0) return 1;
        const count = self.words.count();
        return if (count == 0) 0 else @as(f64, @floatFromInt(self.matches(text))) / @as(f64, @floatFromInt(count));
    }
    pub fn overlap(self: *PreparedQuery, text: []const u8) !f64 {
        // Keys from the previous view are retired before its input can expire.
        self.text_words.clearRetainingCapacity();
        var iterator: Iterator = .{ .text = text };
        while (iterator.next()) |token| try self.text_words.put(self.allocator, token, {});
        const count = @min(self.words.count(), self.text_words.count());
        return if (count == 0) 0 else @as(f64, @floatFromInt(self.matches(text))) / @as(f64, @floatFromInt(count));
    }
};
pub fn overlap(a: A, left: []const u8, right: []const u8) !f64 {
    var query = try PreparedQuery.init(a, left);
    defer query.deinit();
    return query.overlap(right);
}
pub fn coverage(a: A, input: []const u8, text: []const u8) !f64 {
    var query = try PreparedQuery.init(a, input);
    defer query.deinit();
    return query.coverage(text);
}
test "whole-word retrieval excludes stopwords substrings and duplicate inflation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqual(@as(f64, 0), try overlap(a, "a is the", "Antfly database is there"));
    try std.testing.expectEqual(@as(f64, 0), try overlap(a, "cat", "concatenate"));
    try std.testing.expectEqual(@as(f64, 1), try overlap(a, "GRAPH graph", "A knowledge graph."));
    try std.testing.expectEqual(@as(f64, 0.5), try coverage(a, "graph database", "Graph only"));
    try std.testing.expectEqual(@as(f64, 1), try overlap(a, "Memory_API", "memory api service"));
}

test "prepared query reuses coverage and overlap without retaining prior text vocabulary" {
    var query = try PreparedQuery.init(std.testing.allocator, "graph GRAPH database the");
    defer query.deinit();
    for (0..1024) |_| {
        try std.testing.expectEqual(@as(f64, 0.5), query.coverage("Graph graph"));
        try std.testing.expectEqual(@as(f64, 1), query.coverage("DATABASE graph database"));
        try std.testing.expectEqual(@as(f64, 1), try query.overlap("graph graph"));
        try std.testing.expectEqual(@as(f64, 0), try query.overlap("unrelated unrelated"));
        try std.testing.expectEqual(@as(f64, 0.5), try query.overlap("graph unrelated unrelated"));
    }
}
