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

//! Bounded specialization ownership shared by imported and traced programs.
//! Values retain their own immutable-program/backend leases. Callers serialize
//! lookup/build/retirement and dispose returned victims in backend-safe order.
const std = @import("std");

pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    builds: u64 = 0,
    evictions: u64 = 0,
    pub fn since(self: Stats, before: Stats) Stats {
        return .{ .hits = self.hits -% before.hits, .misses = self.misses -% before.misses, .builds = self.builds -% before.builds, .evictions = self.evictions -% before.evictions };
    }
};

pub fn Cache(comptime Key: type, comptime Value: type, comptime keyOf: fn (Value) Key, comptime equal: fn (Key, Key) bool) type {
    return struct {
        const Self = @This();
        // LRU to MRU. Values are handles; moving a handle never moves a program,
        // executable or captured storage owner. Repeated MRU hits copy nothing.
        entries: std.ArrayListUnmanaged(Value) = .empty,
        limit: usize,
        stats: Stats = .{},

        pub fn init(limit: usize) Self {
            std.debug.assert(limit > 0);
            return .{ .limit = limit };
        }
        pub fn get(self: *Self, key: Key) ?Value {
            for (self.entries.items, 0..) |value, i| {
                if (!equal(keyOf(value), key)) continue;
                self.stats.hits +%= 1;
                if (i + 1 != self.entries.items.len) {
                    const hit = self.entries.orderedRemove(i);
                    self.entries.appendAssumeCapacity(hit);
                }
                return value;
            }
            self.stats.misses +%= 1;
            return null;
        }
        /// Reserve before destroying a victim or transferring a new owner.
        /// A failed build/allocation leaves every cached executable intact.
        pub fn reserve(self: *Self, allocator: std.mem.Allocator) !void {
            if (self.entries.items.len < self.limit) try self.entries.ensureUnusedCapacity(allocator, 1);
        }
        /// Key must be absent. Returns the old owner for explicit retirement.
        pub fn insert(self: *Self, value: Value) ?Value {
            std.debug.assert(self.entries.items.len <= self.limit);
            const victim = if (self.entries.items.len == self.limit) self.entries.orderedRemove(0) else null;
            self.entries.appendAssumeCapacity(value);
            self.stats.builds +%= 1;
            if (victim != null) self.stats.evictions +%= 1;
            return victim;
        }
        pub fn clear(self: *Self) void {
            self.entries.clearRetainingCapacity();
        }
        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.entries.deinit(allocator);
        }
    };
}

test "specialization ownership preserves LRU victims and allocation failure atomicity" {
    const Entry = struct {
        key: u32,
        value: *u32,
        fn keyOf(self: @This()) u32 {
            return self.key;
        }
        fn equal(a: u32, b: u32) bool {
            return a == b;
        }
    };
    var cache = Cache(u32, Entry, Entry.keyOf, Entry.equal).init(2);
    defer cache.deinit(std.testing.allocator);
    var values = [_]u32{ 10, 20, 30 };
    for (0..2) |i| {
        try cache.reserve(std.testing.allocator);
        try std.testing.expect(cache.insert(.{ .key = @intCast(i), .value = &values[i] }) == null);
    }
    try std.testing.expect(cache.get(9) == null);
    const held = cache.get(0).?;
    // Full caches require no allocation: admission cannot fail after eviction.
    var failing = std.heap.FixedBufferAllocator.init(&.{});
    try cache.reserve(failing.allocator());
    const victim = cache.insert(.{ .key = 2, .value = &values[2] }).?;
    try std.testing.expectEqual(@as(u32, 1), victim.key);
    try std.testing.expectEqual(@as(u32, 10), held.value.*);
    try std.testing.expectEqual(@as(u64, 1), cache.stats.evictions);
    var empty = Cache(u32, Entry, Entry.keyOf, Entry.equal).init(2);
    defer empty.deinit(std.testing.allocator);
    try std.testing.expectError(error.OutOfMemory, empty.reserve(failing.allocator()));
    try std.testing.expectEqual(@as(usize, 0), empty.entries.items.len);
}
