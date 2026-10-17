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

//! Immutable bounded Aho-Corasick prefilter for unions of mandatory literals.
//! Folding ASCII adds false positives for case-sensitive expressions, preserving
//! the proof-of-absence contract while combining all needles in a single pass.
const std = @import("std");
const literal = @import("literal.zig");
const A = std.mem.Allocator;
const Budget = @import("bounded.zig").Budget;
pub const Needle = struct { bytes: []const u8, insensitive: bool = false, unicode: bool = true };
pub const Set = struct {
    a: A,
    memory: *Budget,
    nodes: std.ArrayList(Node) = .empty,
    unicode_fold: bool = false,
    const Node = struct { next: [256]u16 = @splat(0), suffix: u16 = 0, output: bool = false };
    pub const Error = A.Error || error{ PrefilterTooLarge, UnsupportedPrefilter };
    pub fn init(backing: A, needles: []const Needle, budget: usize) Error!Set {
        if (budget <= @sizeOf(Budget)) return error.PrefilterTooLarge;
        const memory = try backing.create(Budget);
        memory.* = .{ .backing = backing, .limit = budget - @sizeOf(Budget) };
        return initBounded(memory, needles) catch |err| {
            const denied = memory.denied;
            std.debug.assert(memory.live == 0);
            backing.destroy(memory);
            return if (denied) error.PrefilterTooLarge else err;
        };
    }
    fn initBounded(memory: *Budget, needles: []const Needle) Error!Set {
        const a = memory.allocator();
        var self = Set{ .a = a, .memory = memory };
        errdefer self.nodes.deinit(a);
        _ = try self.addNode();
        for (needles) |needle| {
            if (needle.bytes.len == 0) return error.UnsupportedPrefilter;
            if (needle.insensitive and needle.unicode) {
                if (!literal.isAscii(needle.bytes)) return error.UnsupportedPrefilter;
                self.unicode_fold = true;
            }
            var state: u16 = 0;
            for (needle.bytes) |ch| {
                const byte = std.ascii.toLower(ch);
                var next = self.nodes.items[state].next[byte];
                if (next == 0) {
                    next = try self.addNode();
                    self.nodes.items[state].next[byte] = next;
                }
                state = next;
            }
            self.nodes.items[state].output = true;
        }
        const queue = try a.alloc(u16, self.nodes.items.len);
        defer a.free(queue);
        var head: usize = 0;
        var tail: usize = 0;
        for (self.nodes.items[0].next) |next| if (next != 0) {
            queue[tail] = next;
            tail += 1;
        };
        while (head < tail) : (head += 1) {
            const state = queue[head];
            for (0..256) |byte| {
                const next = self.nodes.items[state].next[byte];
                const fallback = self.nodes.items[self.nodes.items[state].suffix].next[byte];
                if (next != 0) {
                    self.nodes.items[next].suffix = fallback;
                    self.nodes.items[next].output = self.nodes.items[next].output or self.nodes.items[fallback].output;
                    queue[tail] = next;
                    tail += 1;
                } else self.nodes.items[state].next[byte] = fallback;
            }
        }
        return self;
    }
    fn addNode(self: *Set) Error!u16 {
        if (self.nodes.items.len == std.math.maxInt(u16)) return error.PrefilterTooLarge;
        const index: u16 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.a, .{});
        return index;
    }
    pub fn deinit(self: *Set) void {
        self.nodes.deinit(self.a);
        std.debug.assert(self.memory.live == 0);
        self.memory.backing.destroy(self.memory);
    }
    pub fn mayMatch(self: *const Set, bytes: []const u8) bool {
        var state: u16 = 0;
        for (bytes) |byte| {
            if (self.unicode_fold and byte >= 128) return true;
            state = self.nodes.items[state].next[std.ascii.toLower(byte)];
            if (self.nodes.items[state].output) return true;
        }
        return false;
    }
};
test "combined prefilter handles failure suffixes, Unicode folding and admission bounds" {
    const a = std.testing.allocator;
    var set = try Set.init(a, &.{ .{ .bytes = "hers" }, .{ .bytes = "she" }, .{ .bytes = "his" }, .{ .bytes = "he" } }, 65536);
    defer set.deinit();
    try std.testing.expect(set.memory.peak + @sizeOf(Budget) <= 65536);
    for ([_][]const u8{ "ushers", "SHERS", "a his", "he" }) |text| try std.testing.expect(set.mayMatch(text));
    try std.testing.expect(!set.mayMatch("hissless"[3..]));
    try std.testing.expectError(error.PrefilterTooLarge, Set.init(a, &.{.{ .bytes = "a" }}, 1));
    var folded = try Set.init(a, &.{.{ .bytes = "k", .insensitive = true }}, 65536);
    defer folded.deinit();
    try std.testing.expect(folded.mayMatch("K"));
    try std.testing.expect(!folded.mayMatch("absent"));
}
