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

//! Bounded best-first selection. IDs remain borrowed until the caller clones
//! its result; score accumulation is separate and retains its request budget.
const std = @import("std");
const Doc = @import("indexed_reader.zig").ScoredDoc;
pub const TopDocs = struct {
    items: std.ArrayList(Doc) = .empty,
    capacity: usize,
    pub fn deinit(self: *TopDocs, a: std.mem.Allocator) void {
        self.items.deinit(a);
    }
    fn better(_: void, l: Doc, r: Doc) bool {
        return @import("indexed_reader.zig").lessScoredDoc({}, l, r);
    }
    pub fn add(self: *TopDocs, a: std.mem.Allocator, value: Doc) !void {
        if (self.capacity == 0) return;
        if (self.items.items.len < self.capacity) {
            try self.items.append(a, value);
            var i = self.items.items.len - 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (!better({}, self.items.items[parent], self.items.items[i])) break;
                std.mem.swap(Doc, &self.items.items[parent], &self.items.items[i]);
                i = parent;
            }
            return;
        }
        if (!better({}, value, self.items.items[0])) return;
        self.items.items[0] = value;
        var i: usize = 0;
        while (i * 2 + 1 < self.items.items.len) {
            var child = i * 2 + 1;
            if (child + 1 < self.items.items.len and better({}, self.items.items[child], self.items.items[child + 1])) child += 1;
            if (!better({}, self.items.items[i], self.items.items[child])) break;
            std.mem.swap(Doc, &self.items.items[i], &self.items.items[child]);
            i = child;
        }
    }
    pub fn sorted(self: *TopDocs) []Doc {
        std.mem.sort(Doc, self.items.items, {}, better);
        return self.items.items;
    }
};

test "serverless bounded ranking matches exhaustive order across ties, offsets and input order" {
    const a = std.testing.allocator;
    var ids: [513][8]u8 = undefined;
    var all: [513]Doc = undefined;
    for (&all, &ids, 0..) |*doc, *buf, i| doc.* = .{ .doc_id = try std.fmt.bufPrint(buf, "{d:0>6}", .{i}), .score = @intCast((i * 37) % 29) };
    for ([_]usize{ 0, 1, 10, 37, 513, 600 }) |capacity| {
        var top = TopDocs{ .capacity = capacity };
        defer top.deinit(a);
        // Prime permutation visits every entry once in a different order.
        for (0..all.len) |i| try top.add(a, all[(i * 31) % all.len]);
        var expected = all;
        std.mem.sort(Doc, &expected, {}, TopDocs.better);
        const actual = top.sorted();
        try std.testing.expectEqual(@min(capacity, all.len), actual.len);
        for (expected[0..actual.len], actual) |l, r| {
            try std.testing.expectEqualStrings(l.doc_id, r.doc_id);
            try std.testing.expectEqual(l.score, r.score);
        }
    }
}

test "serverless bounded vector ranking preserves exact distances within rounded scores" {
    const a = std.testing.allocator;
    var top = TopDocs{ .capacity = 2 };
    defer top.deinit(a);
    try top.add(a, .{ .doc_id = "a", .score = 500, .distance = 0.4999 });
    try top.add(a, .{ .doc_id = "z", .score = 500, .distance = 0.4991 });
    try top.add(a, .{ .doc_id = "y", .score = 500, .distance = 0.4992 });
    const hits = top.sorted();
    try std.testing.expectEqualStrings("z", hits[0].doc_id);
    try std.testing.expectEqualStrings("y", hits[1].doc_id);
}
