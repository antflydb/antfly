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

//! Immutable parsed metadata and decoded vectors. Leases pin buffers across
//! eviction; all payload allocations belong to the cache, never a request.
const std = @import("std");
const Budget = @import("../../sql/memory_budget.zig");
const A = std.mem.Allocator;
pub const Payload = union(enum) {
    /// Layer-owned immutable metadata. The type-domain is included in the key;
    /// values and every referenced byte must belong to this item's arena.
    extension: *anyopaque,
    snapshot: @import("lake_iceberg_snapshot.zig").SnapshotWithDeletePlan,
    prepared: *@import("lake_prepared_deletes.zig").Prepared,
    footer: @import("lake_parquet_metadata.zig").ParsedFooter,
    dictionary: @import("lake_parquet_page.zig").Dictionary,
    columns: []const @import("../../storage/rowsource/types.zig").ColumnVector,
};
pub const Item = struct {
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    payload: Payload = .{ .columns = &.{} },
    // Snapshot-local canonical row-ID ordering and totals, computed once.
    file_order: ?[]usize = null,
    file_rank: ?[]usize = null,
    file_by_id: std.StringHashMapUnmanaged(usize) = .empty,
    estimated_rows: u64 = 0,
    estimated_bytes: u64 = 0,
    refs: usize = 1,
    cached: bool = false,
    /// A decoded page pins its immutable chunk dictionary in this cache.
    dependency: ?*Item = null,
    touched: u64 = 0,
    fn destroy(self: *Item, cache: *Cache) void {
        const parent = self.dependency;
        const a = self.budget.backing;
        if (self.payload == .prepared) self.payload.prepared.destroy(self.budget.allocator());
        if (self.payload == .snapshot) self.payload.snapshot.deinit(self.budget.allocator());
        self.file_by_id.deinit(self.budget.allocator());
        if (self.file_rank) |rank| self.budget.allocator().free(rank);
        if (self.file_order) |order| self.budget.allocator().free(order);
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        a.destroy(self);
        if (parent) |item| cache.releaseLocked(item);
    }
};
pub const Lease = struct {
    cache: *Cache,
    item: *Item,
    pub fn retain(self: Lease) Lease {
        self.cache.lock();
        defer self.cache.mutex.unlock();
        self.item.refs += 1;
        return self;
    }
    pub fn release(self: Lease) void {
        self.cache.lock();
        defer self.cache.mutex.unlock();
        self.cache.releaseLocked(self.item);
    }
};
pub const Cache = struct {
    a: A,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Item) = .empty,
    max_bytes: usize = 64 * 1024 * 1024,
    max_entries: usize = 512,
    bytes: usize = 0,
    tick: u64 = 0,
    hits: u64 = 0,
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn releaseLocked(self: *Cache, item: *Item) void {
        std.debug.assert(item.refs != 0);
        item.refs -= 1;
        if (item.refs == 0 and !item.cached) item.destroy(self);
    }
    pub fn depend(self: *Cache, page: Lease, dictionary: Lease) void {
        std.debug.assert(page.cache == self and dictionary.cache == self);
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(page.item.dependency == null);
        dictionary.item.refs += 1;
        page.item.dependency = dictionary.item;
    }
    pub fn deinit(self: *Cache) void {
        // Remove leaf pages first; their release makes dictionaries evictable.
        while (self.entries.count() != 0) {
            var it = self.entries.iterator();
            const key = while (it.next()) |entry| {
                if (entry.value_ptr.*.refs == 0) break entry.key_ptr.*;
            } else unreachable; // All request leases must already be closed.
            const removed = self.entries.fetchRemove(key).?;
            removed.value.cached = false;
            removed.value.destroy(self);
        }
        self.entries.deinit(self.a);
    }
    pub fn lookup(self: *Cache, key: [32]u8) ?Lease {
        self.lock();
        defer self.mutex.unlock();
        const item = self.entries.get(key) orelse return null;
        self.tick +%= 1;
        item.touched = self.tick;
        item.refs += 1;
        self.hits += 1;
        return .{ .cache = self, .item = item };
    }
    pub fn create(self: *Cache, limit: usize) !Lease {
        const item = try self.a.create(Item);
        item.* = .{ .budget = .{ .backing = self.a, .limit = limit }, .arena = undefined };
        item.arena = std.heap.ArenaAllocator.init(item.budget.allocator());
        return .{ .cache = self, .item = item };
    }
    /// Admission is optional. A saturated cache returns an uncached lease,
    /// reclaimed immediately when the scan releases it.
    pub fn publish(self: *Cache, key: [32]u8, lease: Lease) void {
        self.lock();
        defer self.mutex.unlock();
        const item = lease.item;
        const size = item.budget.live;
        if (size > self.max_bytes or self.max_entries == 0 or self.entries.contains(key)) return;
        while (size > self.max_bytes -| self.bytes or self.entries.count() >= self.max_entries) {
            var oldest: ?[32]u8 = null;
            var tick: u64 = std.math.maxInt(u64);
            var it = self.entries.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.*.refs != 0) continue;
                if (oldest == null or entry.value_ptr.*.touched < tick) {
                    oldest = entry.key_ptr.*;
                    tick = entry.value_ptr.*.touched;
                }
            }
            const removed = self.entries.fetchRemove(oldest orelse return).?;
            self.bytes -= removed.value.budget.live;
            removed.value.cached = false;
            removed.value.destroy(self);
        }
        self.entries.put(self.a, key, item) catch return;
        self.tick +%= 1;
        item.touched = self.tick;
        item.cached = true;
        self.bytes += size;
    }
};

test "external lake decoded leases preserve pinned buffers under bounded eviction" {
    var cache: Cache = .{ .a = std.testing.allocator, .max_entries = 1 };
    defer cache.deinit();
    const first = try cache.create(8192);
    const bytes = try first.item.arena.allocator().dupe(u8, "pinned");
    first.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(1), first);
    const hit = cache.lookup(@splat(1)).?;
    const second = try cache.create(8192);
    second.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(2), second);
    try std.testing.expect(!second.item.cached);
    second.release();
    first.release();
    try std.testing.expectEqualStrings("pinned", bytes);
    hit.release();
    const replacement = try cache.create(8192);
    replacement.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(2), replacement);
    replacement.release();
    try std.testing.expect(cache.lookup(@splat(1)) == null);
}

test "external lake page dependencies pin a single dictionary across eviction" {
    var cache: Cache = .{ .a = std.testing.allocator, .max_entries = 2 };
    defer cache.deinit();
    const dictionary = try cache.create(8192);
    const a = dictionary.item.arena.allocator();
    const entries = try a.alloc([]u8, 1);
    entries[0] = try a.dupe(u8, "shared dictionary");
    dictionary.item.payload = .{ .dictionary = .{ .bytes = entries } };
    cache.publish(@splat(1), dictionary);
    const page = try cache.create(8192);
    page.item.payload = .{ .columns = &.{} };
    cache.depend(page, dictionary);
    cache.publish(@splat(2), page);
    dictionary.release();
    const pinned = cache.lookup(@splat(2)).?;
    page.release();
    const extra = try cache.create(8192);
    extra.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(3), extra);
    try std.testing.expect(!extra.item.cached);
    extra.release();
    try std.testing.expectEqualStrings("shared dictionary", pinned.item.dependency.?.payload.dictionary.bytes[0]);
    pinned.release();
    const replacement = try cache.create(8192);
    replacement.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(3), replacement);
    replacement.release();
    try std.testing.expect(cache.entries.count() <= 2);
}
