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
pub fn overlap(a: A, left: []const u8, right: []const u8) !f64 {
    const l = try tokens(a, left);
    const r = try tokens(a, right);
    return compare(l, r, @min(l.count(), r.count()));
}
pub fn coverage(a: A, query: []const u8, text: []const u8) !f64 {
    if (query.len == 0) return 1;
    const l = try tokens(a, query);
    const r = try tokens(a, text);
    return compare(l, r, l.count());
}
fn compare(left: Set, right: Set, denominator: usize) f64 {
    if (denominator == 0) return 0;
    var matches: usize = 0;
    var it = left.keyIterator();
    while (it.next()) |token| if (right.contains(token.*)) {
        matches += 1;
    };
    return @as(f64, @floatFromInt(matches)) / @as(f64, @floatFromInt(denominator));
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
