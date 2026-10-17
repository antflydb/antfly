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

//! Bounded worst-first heap. Admission uses score, path and discovery order;
//! losers need no rendering. The caller joins producers before sorting/emitting.
const std = @import("std");
const Limits = @import("budget.zig").Limits;
pub fn Heap(comptime Candidate: type) type {
    return struct {
        const Self = @This();
        a: std.mem.Allocator,
        io: std.Io,
        limits: *Limits,
        capacity: usize,
        max_bytes: usize,
        items: std.ArrayList(*Candidate) = .empty,
        considered: usize = 0,
        mutex: std.Io.Mutex = .init,
        pub fn better(a: *const Candidate, b: *const Candidate) bool {
            return betterFields(a.score, a.path, a.ordinal, b.score, b.path, b.ordinal);
        }
        fn betterFields(score: f64, path: []const u8, ordinal: usize, other: f64, other_path: []const u8, other_ordinal: usize) bool {
            if (score != other) return score > other;
            const order = std.mem.order(u8, path, other_path);
            if (order != .eq) return order == .lt;
            return ordinal < other_ordinal;
        }
        pub fn consider(self: *Self, score: f64, path: []const u8, ordinal: usize) bool {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.considered += 1;
            if (self.items.items.len < self.capacity) return true;
            const worst = self.items.items[0];
            return betterFields(score, path, ordinal, worst.score, worst.path, worst.ordinal);
        }
        /// Consumes candidate on success, rejection, and failure.
        pub fn offer(self: *Self, candidate: *Candidate) !void {
            var retained = false;
            var victim: ?*Candidate = null;
            self.mutex.lockUncancelable(self.io);
            defer {
                self.mutex.unlock(self.io);
                if (victim) |old| old.deinit();
                if (!retained) candidate.deinit();
            }
            const full = self.items.items.len == self.capacity;
            if (full and !better(candidate, self.items.items[0])) return;
            const old_bytes = if (full) self.items.items[0].bytes() else 0;
            const size = candidate.bytes();
            const increase = size -| old_bytes;
            if (!Limits.reserve(&self.limits.pending_output, increase, self.max_bytes)) return error.PendingOutputBudgetExceeded;
            errdefer _ = self.limits.pending_output.fetchSub(increase, .monotonic);
            if (full) {
                victim = self.items.items[0];
                self.items.items[0] = candidate;
                var index: usize = 0;
                while (true) {
                    const left = index * 2 + 1;
                    if (left >= self.items.items.len) break;
                    const right = left + 1;
                    const child = if (right < self.items.items.len and better(self.items.items[left], self.items.items[right])) right else left;
                    if (!better(self.items.items[index], self.items.items[child])) break;
                    std.mem.swap(*Candidate, &self.items.items[index], &self.items.items[child]);
                    index = child;
                }
            } else {
                try self.items.append(self.a, candidate);
                var index = self.items.items.len - 1;
                while (index > 0) {
                    const parent = (index - 1) / 2;
                    if (!better(self.items.items[parent], self.items.items[index])) break;
                    std.mem.swap(*Candidate, &self.items.items[parent], &self.items.items[index]);
                    index = parent;
                }
            }
            if (old_bytes > size) _ = self.limits.pending_output.fetchSub(old_bytes - size, .monotonic);
            retained = true;
        }
        pub fn sort(self: *Self) void {
            std.mem.sort(*Candidate, self.items.items, {}, struct {
                fn less(_: void, a: *Candidate, b: *Candidate) bool {
                    return better(a, b);
                }
            }.less);
        }
        pub fn deinit(self: *Self) void {
            for (self.items.items) |candidate| {
                _ = self.limits.pending_output.fetchSub(candidate.bytes(), .monotonic);
                candidate.deinit();
            }
            self.items.deinit(self.a);
        }
    };
}

test "bounded selection preserves ties and releases evicted evidence" {
    const C = struct {
        score: f64,
        path: []const u8,
        ordinal: usize,
        size: usize,
        pub fn bytes(c: *const @This()) usize {
            return c.size;
        }
        pub fn deinit(c: *@This()) void {
            std.testing.allocator.destroy(c);
        }
        fn make(score: f64, path: []const u8, ordinal: usize, size: usize) !*@This() {
            const c = try std.testing.allocator.create(@This());
            c.* = .{ .score = score, .path = path, .ordinal = ordinal, .size = size };
            return c;
        }
    };
    var limits: Limits = .{};
    var heap = Heap(C){ .a = std.testing.allocator, .io = std.testing.io, .limits = &limits, .capacity = 2, .max_bytes = 100 };
    defer heap.deinit();
    try heap.offer(try C.make(0.7, "z", 0, 40));
    try heap.offer(try C.make(0.9, "b", 1, 40));
    try std.testing.expect(!heap.consider(0.1, "a", 0));
    try std.testing.expect(heap.consider(0.9, "a", 3));
    try heap.offer(try C.make(0.9, "a", 3, 30));
    try heap.offer(try C.make(0.9, "a", 2, 20));
    try std.testing.expectEqual(@as(usize, 50), limits.pending_output.load(.monotonic));
    try std.testing.expectError(error.PendingOutputBudgetExceeded, heap.offer(try C.make(1, "winner", 4, 90)));
    try std.testing.expectEqual(@as(usize, 50), limits.pending_output.load(.monotonic));
    heap.sort();
    try std.testing.expectEqual(@as(usize, 2), heap.items.items[0].ordinal);
    try std.testing.expectEqual(@as(usize, 3), heap.items.items[1].ordinal);
}
