// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Immutable parsed metadata and decoded vectors. Leases pin buffers across
//! eviction; all payload allocations belong to the cache, never a request.
const std = @import("std");
const Budget = @import("../../sql/memory_budget.zig");
const A = std.mem.Allocator;
pub const Payload = union(enum) {
    footer: @import("lake_parquet_metadata.zig").ParsedFooter,
    columns: []const @import("../../storage/rowsource/types.zig").ColumnVector,
};
pub const Item = struct {
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    payload: Payload = undefined,
    refs: usize = 1,
    cached: bool = false,
    touched: u64 = 0,
    fn destroy(self: *Item) void {
        const a = self.budget.backing;
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        a.destroy(self);
    }
};
pub const Lease = struct {
    cache: *Cache,
    item: *Item,
    pub fn release(self: Lease) void {
        self.cache.lock();
        defer self.cache.mutex.unlock();
        std.debug.assert(self.item.refs != 0);
        self.item.refs -= 1;
        if (self.item.refs == 0 and !self.item.cached) self.item.destroy();
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
    pub fn deinit(self: *Cache) void {
        var it = self.entries.valueIterator();
        while (it.next()) |item| {
            std.debug.assert(item.*.refs == 0);
            item.*.destroy();
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
            removed.value.destroy();
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
