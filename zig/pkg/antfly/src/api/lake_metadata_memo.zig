// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Bounded data-only memoization of immutable metadata. Authority, clients and
//! operation lifetimes stay with callers; hits return independently owned values.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
pub fn Memo(comptime Value: type) type {
    return struct {
        const Self = @This();
        const Entry = struct { bytes: []u8, expires: u64, used: u64 };
        mutex: std.atomic.Mutex = .unlocked,
        entries: std.AutoHashMapUnmanaged([32]u8, Entry) = .empty,
        max_entries: usize = 128,
        clock: u64 = 0,
        hits: u64 = 0,
        puts: u64 = 0,
        budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 32 * 1024 * 1024 },
        managed: ?local.storage_resource_manager.BudgetedAllocator = null,
        fn lock(self: *Self) void {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
        }
        pub fn attach(self: *Self, manager: *local.storage_resource_manager.ResourceManager) void {
            self.lock();
            defer self.mutex.unlock();
            if (self.managed != null) return;
            std.debug.assert(self.budget.live == 0);
            self.managed = local.storage_resource_manager.BudgetedAllocator.init(manager, .dense_search_working_set, @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), 1);
            self.budget.backing = self.managed.?.allocator();
        }
        pub fn deinit(self: *Self) void {
            const a = self.budget.allocator();
            var it = self.entries.valueIterator();
            while (it.next()) |entry| a.free(entry.bytes);
            self.entries.deinit(a);
            std.debug.assert(self.budget.live == 0);
            if (self.managed) |*managed| managed.deinit();
        }
        pub fn get(self: *Self, a: A, key: [32]u8, minimum_expiry: u64) !?Value {
            const bytes = copied: {
                self.lock();
                defer self.mutex.unlock();
                const entry = self.entries.getPtr(key) orelse return null;
                if (entry.expires <= minimum_expiry) return null;
                const owned = try a.dupe(u8, entry.bytes);
                self.clock +|= 1;
                entry.used = self.clock;
                self.hits +|= 1;
                break :copied owned;
            };
            defer a.free(bytes);
            return try std.json.parseFromSliceLeaky(Value, a, bytes, .{ .allocate = .alloc_always });
        }
        fn evict(self: *Self) bool {
            var oldest: ?[32]u8 = null;
            var used: u64 = std.math.maxInt(u64);
            var it = self.entries.iterator();
            while (it.next()) |entry| if (entry.value_ptr.used <= used) {
                oldest = entry.key_ptr.*;
                used = entry.value_ptr.used;
            };
            const removed = self.entries.fetchRemove(oldest orelse return false).?;
            self.budget.allocator().free(removed.value.bytes);
            return true;
        }
        pub fn put(self: *Self, key: [32]u8, value: Value, expires: u64) void {
            if (self.max_entries == 0) return;
            const bytes = std.json.Stringify.valueAlloc(std.heap.page_allocator, value, .{}) catch return;
            defer std.heap.page_allocator.free(bytes);
            self.lock();
            defer self.mutex.unlock();
            if (self.entries.get(key)) |entry| if (entry.expires >= expires) return;
            if (bytes.len > self.budget.limit / 2) return;
            while (self.entries.count() >= self.max_entries or (local.sql_memory_budget.headroom(self.budget.allocator()) orelse 0) < bytes.len + 4096) {
                if (!self.evict()) break;
            }
            const a = self.budget.allocator();
            const owned = a.dupe(u8, bytes) catch return;
            const slot = self.entries.getOrPut(a, key) catch {
                a.free(owned);
                return;
            };
            if (slot.found_existing) a.free(slot.value_ptr.bytes);
            self.clock +|= 1;
            self.puts +|= 1;
            slot.value_ptr.* = .{ .bytes = owned, .expires = expires, .used = self.clock };
        }
    };
}
test "external lake bounded WAL metadata memos own hits across eviction and enforce expiry" {
    const a = std.testing.allocator;
    const Value = struct { name: []const u8 };
    var memo: Memo(Value) = .{ .max_entries = 1, .budget = .{ .backing = a, .limit = 64 * 1024 } };
    defer memo.deinit();
    memo.put(@splat(1), .{ .name = "first" }, 100);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const hit = (try memo.get(arena.allocator(), @splat(1), 99)).?;
    try std.testing.expect(try memo.get(arena.allocator(), @splat(1), 100) == null);
    memo.put(@splat(2), .{ .name = "second" }, 101);
    try std.testing.expect(try memo.get(arena.allocator(), @splat(1), 0) == null);
    try std.testing.expectEqualStrings("first", hit.name);
    memo.put(@splat(2), .{ .name = "stale" }, 99);
    try std.testing.expectEqualStrings("second", (try memo.get(arena.allocator(), @splat(2), 100)).?.name);
}
