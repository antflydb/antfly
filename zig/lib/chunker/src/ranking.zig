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

//! Stable bounded top-k selection. Callers supply their own score/tie contract
//! and storage; neither model vocabulary size nor payload ownership is retained.
const std = @import("std");
pub const max_top_k = 128;

pub fn TopK(comptime T: type, comptime less: fn (T, T) bool) type {
    return struct {
        const Self = @This();
        pub const Entry = struct { value: T, order: usize };
        storage: []Entry,
        count: usize = 0,
        next_order: usize = 0,
        fn better(a: Entry, b: Entry) bool {
            return less(a.value, b.value) or (!less(b.value, a.value) and a.order < b.order);
        }
        pub fn offer(self: *Self, value: T) void {
            const candidate = Entry{ .value = value, .order = self.next_order };
            self.next_order +|= 1;
            if (self.storage.len == 0) return;
            if (self.count < self.storage.len) {
                var i = self.count;
                self.count += 1;
                self.storage[i] = candidate;
                while (i != 0) {
                    const parent = (i - 1) / 2;
                    if (!better(self.storage[parent], self.storage[i])) break;
                    std.mem.swap(Entry, &self.storage[parent], &self.storage[i]);
                    i = parent;
                }
                return;
            }
            if (!better(candidate, self.storage[0])) return;
            self.storage[0] = candidate;
            var i: usize = 0;
            while (i * 2 + 1 < self.count) {
                var child = i * 2 + 1;
                if (child + 1 < self.count and better(self.storage[child], self.storage[child + 1])) child += 1;
                if (!better(self.storage[i], self.storage[child])) break;
                std.mem.swap(Entry, &self.storage[i], &self.storage[child]);
                i = child;
            }
        }
        /// Finishes selection. Do not offer more values after sorting.
        pub fn finish(self: *Self) []const Entry {
            std.mem.sort(Entry, self.storage[0..self.count], {}, struct {
                fn compare(_: void, a: Entry, b: Entry) bool {
                    return better(a, b);
                }
            }.compare);
            return self.storage[0..self.count];
        }
    };
}

test "model aware media bounded top k preserves caller order on ties across capacities" {
    const Value = struct { score: i32, index: usize };
    const Order = struct {
        fn less(a: Value, b: Value) bool {
            return a.score > b.score;
        }
    };
    const Selector = TopK(Value, Order.less);
    var random = std.Random.DefaultPrng.init(91);
    var values: [1024]Value = undefined;
    for (&values, 0..) |*value, i| value.* = .{ .score = random.random().intRangeLessThan(i32, -8, 8), .index = i };
    var expected = values;
    std.mem.sort(Value, &expected, {}, struct {
        fn less(_: void, a: Value, b: Value) bool {
            return a.score > b.score or (a.score == b.score and a.index < b.index);
        }
    }.less);
    for (0..max_top_k + 1) |k| {
        var buffer: [max_top_k]Selector.Entry = undefined;
        var selector = Selector{ .storage = buffer[0..k] };
        for (values) |value| selector.offer(value);
        const selected = selector.finish();
        try std.testing.expectEqual(k, selected.len);
        for (selected, expected[0..k]) |actual, wanted| try std.testing.expectEqualDeep(wanted, actual.value);
    }
}
