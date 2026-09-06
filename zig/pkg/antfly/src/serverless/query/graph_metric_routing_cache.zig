// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded, process-local cache of authenticated immutable routing indexes.
//! Leases keep borrowed node IDs alive. Eviction never invalidates a reader;
//! pinned entries remain charged and admission bypasses a saturated cache.
const std = @import("std");
const codec = @import("../graph_metric_segment/codec.zig");
const Allocator = std.mem.Allocator;

pub const Entry = struct {
    key: [32]u8,
    alloc: Allocator,
    footer: []u8,
    routing: codec.RoutingIndex,
    references: usize = 0,
    touched: u64 = 0,

    pub fn bytes(self: *const Entry) usize {
        return @sizeOf(Entry) + self.footer.len + self.routing.entries.len * @sizeOf(codec.RoutingEntry) +
            self.routing.ranked_entries.len * @sizeOf(codec.RankedRoutingEntry);
    }

    pub fn destroy(self: *Entry) void {
        const alloc = self.alloc;
        self.routing.deinit(alloc);
        alloc.free(self.footer);
        alloc.destroy(self);
    }
};

pub const Lease = struct {
    entry: *Entry,
    cache: ?*Cache = null,

    pub fn deinit(self: *Lease) void {
        if (self.cache) |cache| {
            cache.lock();
            defer cache.mu.unlock();
            std.debug.assert(self.entry.references > 0);
            self.entry.references -= 1;
        } else self.entry.destroy();
        self.* = undefined;
    }
};

pub const Cache = struct {
    mu: std.atomic.Mutex = .unlocked,
    entries: [64]?*Entry = @splat(null),
    retained_bytes: usize = 0,
    clock: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,

    pub fn snapshot(self: *Cache) struct { hits: u64, misses: u64, bytes: usize } {
        self.lock();
        defer self.mu.unlock();
        return .{ .hits = self.hits, .misses = self.misses, .bytes = self.retained_bytes };
    }

    fn lock(self: *Cache) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn acquire(self: *Cache, key: [32]u8) ?Lease {
        self.lock();
        defer self.mu.unlock();
        self.clock +|= 1;
        for (self.entries) |maybe_entry| {
            const entry = maybe_entry orelse continue;
            if (!std.mem.eql(u8, &entry.key, &key)) continue;
            entry.references += 1;
            entry.touched = self.clock;
            self.hits +|= 1;
            return .{ .entry = entry, .cache = self };
        }
        self.misses +|= 1;
        return null;
    }

    /// Takes ownership even on bypass. Decode and object-store I/O happen
    /// before this call, never while holding the cache mutex.
    pub fn adopt(self: *Cache, entry: *Entry, max_bytes: usize) Lease {
        self.lock();
        defer self.mu.unlock();
        self.clock +|= 1;
        for (self.entries) |maybe_existing| {
            const existing = maybe_existing orelse continue;
            if (!std.mem.eql(u8, &existing.key, &entry.key)) continue;
            existing.references += 1;
            existing.touched = self.clock;
            entry.destroy();
            return .{ .entry = existing, .cache = self };
        }
        const needed = entry.bytes();
        if (needed > max_bytes) return .{ .entry = entry };
        while (true) {
            var empty: ?usize = null;
            var victim: ?usize = null;
            for (self.entries, 0..) |maybe_existing, index| {
                const existing = maybe_existing orelse {
                    empty = index;
                    continue;
                };
                if (existing.references == 0 and (victim == null or existing.touched < self.entries[victim.?].?.touched)) victim = index;
            }
            if (empty != null and self.retained_bytes <= max_bytes - needed) {
                entry.references = 1;
                entry.touched = self.clock;
                self.entries[empty.?] = entry;
                self.retained_bytes += needed;
                return .{ .entry = entry, .cache = self };
            }
            const index = victim orelse return .{ .entry = entry };
            const old = self.entries[index].?;
            self.retained_bytes -= old.bytes();
            self.entries[index] = null;
            old.destroy();
        }
    }

    /// The owning QueryCache outlives all query sessions and their leases.
    pub fn deinit(self: *Cache) void {
        for (self.entries) |maybe_entry| if (maybe_entry) |entry| {
            std.debug.assert(entry.references == 0);
            entry.destroy();
        };
        self.* = undefined;
    }
};

fn testEntry(key: u8) !*Entry {
    const alloc = std.testing.allocator;
    const entry = try alloc.create(Entry);
    errdefer alloc.destroy(entry);
    const footer = try alloc.dupe(u8, "immutable footer");
    errdefer alloc.free(footer);
    const entries = try alloc.alloc(codec.RoutingEntry, 0);
    errdefer alloc.free(entries);
    const ranked = try alloc.alloc(codec.RankedRoutingEntry, 0);
    entry.* = .{
        .key = @splat(key),
        .alloc = alloc,
        .footer = footer,
        .routing = .{ .entries = entries, .ranked_entries = ranked, .footer_offset = 1, .top_score_count = 0 },
    };
    return entry;
}

test "serverless graph metric routing cache bounds pinned memory and deduplicates concurrent fills" {
    var cache = Cache{};
    defer cache.deinit();
    const first = try testEntry(1);
    const limit = first.bytes();
    var pinned = cache.adopt(first, limit);
    var duplicate = cache.adopt(try testEntry(1), limit);
    try std.testing.expect(pinned.entry == duplicate.entry);
    try std.testing.expectEqual(limit, cache.retained_bytes);
    duplicate.deinit();
    var bypass = cache.adopt(try testEntry(2), limit);
    try std.testing.expect(bypass.cache == null);
    try std.testing.expectEqualStrings("immutable footer", pinned.entry.footer);
    bypass.deinit();
    pinned.deinit();
    var replacement = cache.adopt(try testEntry(2), limit);
    defer replacement.deinit();
    try std.testing.expect(cache.acquire(@splat(1)) == null);
    var hit = cache.acquire(@splat(2)).?;
    defer hit.deinit();
    try std.testing.expectEqual(limit, cache.retained_bytes);
}
