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
        // Fixed-size construction admission: saturation bypasses coalescing,
        // never blocks unrelated metadata or retains request capabilities.
        flights: [32]?[32]u8 = @splat(null),
        coalesced: u64 = 0,
        clock: u64 = 0,
        hits: u64 = 0,
        puts: u64 = 0,
        budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 32 * 1024 * 1024 },
        managed: ?local.storage_resource_manager.BudgetedAllocator = null,
        pub const Flight = struct {
            memo: *Self,
            slot: usize,
            pub fn deinit(self: Flight) void {
                self.memo.lock();
                defer self.memo.mutex.unlock();
                std.debug.assert(self.memo.flights[self.slot] != null);
                self.memo.flights[self.slot] = null;
            }
        };
        /// Claim construction, then recheck the memo. Failed/canceled leaders
        /// release their slot; waiters retry using their own authority and TTL.
        pub fn claim(self: *Self, key: [32]u8, io: std.Io, token: @import("antfly_cancellation").CancellationToken) !?Flight {
            while (true) {
                try token.check();
                self.lock();
                var empty: ?usize = null;
                var waiting = false;
                for (self.flights, 0..) |flight, i| {
                    if (flight) |active| {
                        if (std.mem.eql(u8, &active, &key)) waiting = true;
                    } else if (empty == null) empty = i;
                }
                if (!waiting) {
                    if (empty) |slot| {
                        self.flights[slot] = key;
                        self.mutex.unlock();
                        return .{ .memo = self, .slot = slot };
                    }
                    self.mutex.unlock();
                    return null;
                }
                self.coalesced +|= 1;
                self.mutex.unlock();
                try io.sleep(.fromMilliseconds(1), .awake);
            }
        }
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
            for (self.flights) |flight| std.debug.assert(flight == null);
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

test "external lake bounded WAL publication flights independently cancel waiters and recover failed leaders" {
    const io = std.testing.io;
    const M = Memo(u64);
    var memo: M = .{ .budget = .{ .backing = std.testing.allocator, .limit = 64 * 1024 } };
    defer memo.deinit();
    var leader: ?M.Flight = (try memo.claim(@splat(1), io, .none)).?;
    defer if (leader) |flight| flight.deinit();
    const Worker = struct {
        cancel: std.atomic.Value(bool) = .init(false),
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).cancel.load(.acquire);
        }
        fn run(self: *@This(), shared: *M) anyerror!void {
            const flight = (try shared.claim(@splat(1), std.testing.io, .{ .ptr = self, .is_cancelled_fn = canceled })).?;
            defer flight.deinit();
        }
    };
    var worker: Worker = .{};
    var waiter = try io.concurrent(Worker.run, .{ &worker, &memo });
    var joined = false;
    defer if (!joined) {
        worker.cancel.store(true, .release);
        waiter.cancel(io) catch {};
    };
    while (true) {
        memo.lock();
        const ready = memo.coalesced != 0;
        memo.mutex.unlock();
        if (ready) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    worker.cancel.store(true, .release);
    joined = true;
    try std.testing.expectError(error.Canceled, waiter.await(io));
    // Cancellation of a waiter does not discard a leader's eventual payload.
    memo.put(@splat(1), 42, 100);
    leader.?.deinit();
    leader = null;
    const recovered = (try memo.claim(@splat(1), io, .none)).?;
    defer recovered.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectEqual(@as(u64, 42), (try memo.get(arena.allocator(), @splat(1), 99)).?);
    // No successful value is needed for a released flight to be retried.
    const failed = (try memo.claim(@splat(2), io, .none)).?;
    failed.deinit();
    const retry = (try memo.claim(@splat(2), io, .none)).?;
    retry.deinit();
    var slots: [31]M.Flight = undefined;
    var claimed: usize = 0;
    defer for (slots[0..claimed]) |flight| flight.deinit();
    for (&slots, 0..) |*slot, i| {
        slot.* = (try memo.claim(@splat(@as(u8, @intCast(i + 2))), io, .none)).?;
        claimed += 1;
    }
    try std.testing.expect(try memo.claim(@splat(99), io, .none) == null);
}
