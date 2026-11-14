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

/// One bounded immutable specialization. A borrowed entry is never evicted;
/// callers retire detached values outside this metadata-only mutex.
pub fn PinnedSlot(comptime Value: type, comptime Key: type, comptime keyOf: fn (Value) Key, comptime equal: fn (Key, Key) bool, comptime retain: fn (Value) Value, comptime pinned: fn (Value) bool) type {
    return struct {
        mutex: std.atomic.Mutex = .unlocked,
        value: ?Value = null,
        stats: Stats = .{},
        /// One owned cumulative cache observation, including concurrent cold
        /// constructors. Busy is unavailable, never a zero or invocation delta.
        pub fn trySnapshot(self: *@This()) ?Stats {
            if (!self.mutex.tryLock()) return null;
            defer self.mutex.unlock();
            return self.stats;
        }
        pub fn acquire(self: *@This(), key: Key) ?Value {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (self.value) |value| if (equal(keyOf(value), key)) {
                self.stats.hits +%= 1;
                return retain(value);
            };
            self.stats.misses +%= 1;
            return null;
        }
        /// Retain the singleton only. Full semantic/provider checks run outside
        /// the mutex. Producers record an outcome only after their complete
        /// source preflight succeeds, so a source error is not a cache miss.
        pub fn acquireCandidate(self: *@This()) ?Value {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            return if (self.value) |value| retain(value) else null;
        }
        /// Cumulative lookup outcomes, including concurrent constructors. This
        /// records no ownership and calls no validation or provider callback.
        pub fn recordLookup(self: *@This(), qualified: bool) void {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (qualified) self.stats.hits +%= 1 else self.stats.misses +%= 1;
        }
        pub fn publish(self: *@This(), value: Value) ?Value {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (self.value) |old| {
                if (equal(keyOf(old), keyOf(value)) or pinned(old)) return null;
            }
            const old = self.value;
            self.value = retain(value);
            self.stats.builds +%= 1;
            if (old != null) self.stats.evictions +%= 1;
            return old;
        }
        pub fn takeUnpinned(self: *@This()) ?Value {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            const old = self.value orelse return null;
            if (pinned(old)) return null;
            self.value = null;
            self.stats.evictions +%= 1;
            return old;
        }
        pub fn take(self: *@This()) ?Value {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            const old = self.value;
            self.value = null;
            return old;
        }
    };
}

test "qualified pinned lookup validates outside slot mutex and counts semantic misses across concurrent replacement" {
    if (comptime @import("builtin").single_threaded) return error.SkipZigTest;
    const Entry = struct {
        references: std.atomic.Value(usize) = .init(1),
        deaths: *std.atomic.Value(usize),
        semantic: u32,
        fn key(_: *@This()) u32 {
            return 0;
        }
        fn equal(a: u32, b: u32) bool {
            return a == b;
        }
        fn retain(self: *@This()) *@This() {
            _ = self.references.fetchAdd(1, .monotonic);
            return self;
        }
        fn pinned(self: *@This()) bool {
            return self.references.load(.acquire) > 1;
        }
        fn release(self: *@This()) void {
            if (self.references.fetchSub(1, .acq_rel) == 1) _ = self.deaths.fetchAdd(1, .monotonic);
        }
    };
    const Slot = PinnedSlot(*Entry, u32, Entry.key, Entry.equal, Entry.retain, Entry.pinned);
    const Worker = struct {
        fn replace(slot: *Slot, replacement: *Entry) void {
            // Model/owner teardown can detach the cached root while a source
            // qualifier still retains its singleton candidate independently.
            const old = slot.take().?;
            old.release();
            if (slot.publish(replacement) != null) @panic("unexpected replacement victim");
        }
    };
    var deaths = std.atomic.Value(usize).init(0);
    var first = Entry{ .deaths = &deaths, .semantic = 7 };
    var replacement = Entry{ .deaths = &deaths, .semantic = 9 };
    var slot = Slot{};
    defer if (slot.take()) |entry| entry.release();
    var replacement_open = true;
    defer if (replacement_open) replacement.release();
    try std.testing.expect(slot.publish(&first) == null);
    first.release();
    const candidate = slot.acquireCandidate().?;
    var candidate_live = true;
    defer if (candidate_live) candidate.release();
    try std.testing.expect(slot.mutex.tryLock());
    slot.mutex.unlock();
    const thread = try std.Thread.spawn(.{}, Worker.replace, .{ &slot, &replacement });
    thread.join();

    try std.testing.expectEqual(@as(u32, 7), candidate.semantic);
    try std.testing.expectEqual(@as(usize, 0), deaths.load(.acquire));
    // Same digest is a semantic mismatch. The retained old candidate stays
    // live while the actual cache now owns a different semantic definition.
    slot.recordLookup(candidate.semantic == 9);
    const current = slot.acquireCandidate().?;
    var current_live = true;
    defer if (current_live) current.release();
    try std.testing.expect(slot.mutex.tryLock());
    slot.mutex.unlock();
    slot.recordLookup(current.semantic == 9);
    const stats = slot.trySnapshot().?;
    try std.testing.expectEqual(@as(u64, 1), stats.hits);
    try std.testing.expectEqual(@as(u64, 1), stats.misses);
    try std.testing.expectEqual(@as(u64, 2), stats.builds);
    candidate.release();
    candidate_live = false;
    try std.testing.expectEqual(@as(usize, 1), deaths.load(.acquire));
    current.release();
    current_live = false;
    slot.take().?.release();
    replacement.release();
    replacement_open = false;
    try std.testing.expectEqual(@as(usize, 2), deaths.load(.acquire));
}
