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

//! Exact multi-output byte automaton for compatible logical contains leaves.
//! Each output retains its leaf identity, so boolean membership and boosts stay
//! independent. Unsupported sizes fall back to ordinary literal matching.
const std = @import("std");
const A = std.mem.Allocator;
pub const Set = struct {
    const Node = struct { next: [256]u16 = @splat(0), suffix: u16 = 0, outputs: u64 = 0 };
    a: A,
    nodes: std.ArrayListUnmanaged(Node) = .empty,
    all: u64,
    pub fn init(a: A, needles: []const []const u8) !Set {
        if (needles.len == 0 or needles.len > 64) return error.LiteralSetTooLarge;
        var self = Set{ .a = a, .all = @as(u64, std.math.maxInt(u64)) >> @as(u6, @intCast(64 - needles.len)) };
        errdefer self.deinit();
        _ = try self.add();
        for (needles, 0..) |needle, i| {
            var state: u16 = 0;
            for (needle) |byte| {
                var next = self.nodes.items[state].next[byte];
                if (next == 0) {
                    next = try self.add();
                    self.nodes.items[state].next[byte] = next;
                }
                state = next;
            }
            self.nodes.items[state].outputs |= @as(u64, 1) << @as(u6, @intCast(i));
        }
        const queue = try a.alloc(u16, self.nodes.items.len);
        defer a.free(queue);
        var head: usize = 0;
        var tail: usize = 0;
        for (self.nodes.items[0].next) |next| if (next != 0) {
            self.nodes.items[next].outputs |= self.nodes.items[0].outputs;
            queue[tail] = next;
            tail += 1;
        };
        while (head < tail) : (head += 1) {
            const state = queue[head];
            for (0..256) |byte| {
                const next = self.nodes.items[state].next[byte];
                const fallback = self.nodes.items[self.nodes.items[state].suffix].next[byte];
                if (next == 0) {
                    self.nodes.items[state].next[byte] = fallback;
                    continue;
                }
                self.nodes.items[next].suffix = fallback;
                self.nodes.items[next].outputs |= self.nodes.items[fallback].outputs;
                queue[tail] = next;
                tail += 1;
            }
        }
        return self;
    }
    fn add(self: *Set) !u16 {
        // Dense transitions make scanning branch-light. Admission bounds actual
        // ArrayList capacity to 1 MiB (plus the bounded construction queue).
        if ((self.nodes.items.len + 1) * @sizeOf(Node) > 512 * 1024) return error.LiteralSetTooLarge;
        const id: u16 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.a, .{});
        return id;
    }
    pub fn deinit(self: *Set) void {
        self.nodes.deinit(self.a);
    }
    pub fn string(self: *const Set, text: []const u8) u64 {
        var state: u16 = 0;
        var outputs = self.nodes.items[0].outputs;
        for (text) |byte| {
            state = self.nodes.items[state].next[byte];
            outputs |= self.nodes.items[state].outputs;
            if (outputs == self.all) break;
        }
        return outputs;
    }
    pub fn document(self: *const Set, root: std.json.Value, field: []const u8) !u64 {
        var visits: usize = 16384;
        return self.at(root, field, 0, &visits);
    }
    fn at(self: *const Set, value: std.json.Value, path: []const u8, depth: usize, visits: *usize) anyerror!u64 {
        if (depth >= 128 or visits.* == 0) return error.ValueSearchLimitExceeded;
        visits.* -= 1;
        if (value == .array) {
            var found: u64 = 0;
            for (value.array.items) |item| {
                found |= try self.at(item, path, depth + 1, visits);
                if (found == self.all) break;
            }
            return found;
        }
        if (path.len == 0) return if (value == .string) self.string(value.string) else 0;
        if (value != .object) return 0;
        const dot = std.mem.indexOfScalar(u8, path, '.');
        const head = if (dot) |i| path[0..i] else path;
        const child = value.object.get(head) orelse return 0;
        return self.at(child, if (dot) |i| path[i + 1 ..] else "", depth + 1, visits);
    }
};

test "logical value fused literals preserve overlap case arrays and empty strings" {
    const a = std.testing.allocator;
    const needles = [_][]const u8{ "hers", "she", "he", "HE", "", "he" };
    var set = try Set.init(a, &needles);
    defer set.deinit();
    for ([_][]const u8{ "ushers", "", "HE", "absent", "she", "hehers" }) |text| {
        var expected: u64 = 0;
        for (needles, 0..) |needle, i| if (std.mem.indexOf(u8, text, needle) != null) {
            expected |= @as(u64, 1) << @as(u6, @intCast(i));
        };
        try std.testing.expectEqual(expected, set.string(text));
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"nested\":[{\"value\":[\"s\",\"he\",null,9]}]}", .{});
    defer parsed.deinit();
    // Matching never crosses array string boundaries (s + he != she).
    try std.testing.expectEqual(@as(u64, 52), try set.document(parsed.value, "nested.value"));
    try std.testing.expectEqual(@as(u64, 0), try set.document(parsed.value, "absent"));
    const huge = try a.alloc(u8, 2048);
    defer a.free(huge);
    @memset(huge, 'x');
    try std.testing.expectError(error.LiteralSetTooLarge, Set.init(a, &.{huge}));
}

test "logical value fused automaton agrees with independent byte matching" {
    const a = std.testing.allocator;
    var random = std.Random.DefaultPrng.init(0x7ac117);
    var storage: [32][8]u8 = undefined;
    var needles: [32][]const u8 = undefined;
    for (&storage, &needles) |*bytes, *needle| {
        random.random().bytes(bytes);
        for (bytes) |*byte| byte.* %= 5;
        needle.* = bytes[0 .. 1 + random.random().uintLessThan(usize, 8)];
    }
    var set = try Set.init(a, &needles);
    defer set.deinit();
    var text: [1024]u8 = undefined;
    for (0..100) |_| {
        random.random().bytes(&text);
        for (&text) |*byte| byte.* %= 5;
        var expected: u64 = 0;
        for (needles, 0..) |needle, i| if (std.mem.indexOf(u8, &text, needle) != null) {
            expected |= @as(u64, 1) << @as(u6, @intCast(i));
        };
        try std.testing.expectEqual(expected, set.string(&text));
    }
}
