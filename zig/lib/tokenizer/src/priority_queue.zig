// Copyright 2026 Antfly, Inc.
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

// Generic binary max-heap priority queue.
// Ported from go-sentencepiece/internal/priorityqueue.
//
// 1-indexed: items[0] is unused, items[1] is the root.

const std = @import("std");

pub fn PriorityQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        items: std.ArrayListUnmanaged(T),
        allocator: std.mem.Allocator,
        cmp: *const fn (T, T) std.math.Order,

        pub fn init(allocator: std.mem.Allocator, cmp: *const fn (T, T) std.math.Order) !Self {
            var items: std.ArrayListUnmanaged(T) = .empty;
            // Dummy element at index 0 (heap is 1-indexed).
            try items.append(allocator, undefined);
            return .{ .items = items, .allocator = allocator, .cmp = cmp };
        }

        pub fn deinit(self: *Self) void {
            self.items.deinit(self.allocator);
        }

        pub fn len(self: *const Self) usize {
            return self.items.items.len - 1;
        }

        pub fn insert(self: *Self, elem: T) !void {
            try self.items.append(self.allocator, elem);
            self.siftUp(self.items.items.len - 1);
        }

        pub fn popMax(self: *Self) T {
            std.debug.assert(self.items.items.len > 1);
            const max_item = self.items.items[1];
            const last = self.items.items.len - 1;
            self.items.items[1] = self.items.items[last];
            self.items.items.len = last;
            if (self.items.items.len > 1) {
                self.siftDown(1);
            }
            return max_item;
        }

        /// Remove all elements matching a predicate, then rebuild the heap.
        pub fn removeMatching(self: *Self, ctx: anytype) void {
            var write: usize = 1;
            for (1..self.items.items.len) |read| {
                if (!ctx.isDead(self.items.items[read])) {
                    self.items.items[write] = self.items.items[read];
                    write += 1;
                }
            }
            self.items.items.len = write;
            self.rebuildHeap();
        }

        fn rebuildHeap(self: *Self) void {
            var i = self.items.items.len / 2;
            while (i >= 1) : (i -= 1) {
                self.siftDown(i);
            }
        }

        fn siftUp(self: *Self, n: usize) void {
            var i = n;
            while (i > 1) {
                const parent = i / 2;
                if (self.cmp(self.items.items[parent], self.items.items[i]) != .lt) break;
                std.mem.swap(T, &self.items.items[i], &self.items.items[parent]);
                i = parent;
            }
        }

        fn siftDown(self: *Self, start: usize) void {
            var i = start;
            while (true) {
                const left = 2 * i;
                if (left >= self.items.items.len) break;

                var max_child = left;
                const right = left + 1;
                if (right < self.items.items.len and
                    self.cmp(self.items.items[right], self.items.items[left]) == .gt)
                {
                    max_child = right;
                }

                if (self.cmp(self.items.items[i], self.items.items[max_child]) != .lt) break;
                std.mem.swap(T, &self.items.items[i], &self.items.items[max_child]);
                i = max_child;
            }
        }
    };
}

fn intCmp(a: i32, b: i32) std.math.Order {
    return std.math.order(a, b);
}

test "basic operations" {
    const allocator = std.testing.allocator;
    var pq = try PriorityQueue(i32).init(allocator, intCmp);
    defer pq.deinit();

    try pq.insert(3);
    try pq.insert(1);
    try pq.insert(4);
    try pq.insert(1);
    try pq.insert(5);

    try std.testing.expectEqual(@as(usize, 5), pq.len());
    try std.testing.expectEqual(@as(i32, 5), pq.popMax());
    try std.testing.expectEqual(@as(i32, 4), pq.popMax());
    try std.testing.expectEqual(@as(i32, 3), pq.popMax());
    try std.testing.expectEqual(@as(i32, 1), pq.popMax());
    try std.testing.expectEqual(@as(i32, 1), pq.popMax());
    try std.testing.expectEqual(@as(usize, 0), pq.len());
}

test "remove matching" {
    const allocator = std.testing.allocator;
    var pq = try PriorityQueue(i32).init(allocator, intCmp);
    defer pq.deinit();

    try pq.insert(1);
    try pq.insert(2);
    try pq.insert(3);
    try pq.insert(4);
    try pq.insert(5);

    // Remove even numbers
    pq.removeMatching(struct {
        pub fn isDead(_: @This(), val: i32) bool {
            return @mod(val, 2) == 0;
        }
    }{});

    try std.testing.expectEqual(@as(usize, 3), pq.len());
    try std.testing.expectEqual(@as(i32, 5), pq.popMax());
    try std.testing.expectEqual(@as(i32, 3), pq.popMax());
    try std.testing.expectEqual(@as(i32, 1), pq.popMax());
}
