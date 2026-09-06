// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded, process-local cache of authenticated immutable routing indexes.
//! Leases keep borrowed node IDs alive. Eviction never invalidates a reader;
//! pinned entries remain charged and admission bypasses a saturated cache.
const std = @import("std");
const codec = @import("../graph_metric_segment/codec.zig");
const CancellationToken = @import("../../api/operation.zig").CancellationToken;
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
    // Fixed-capacity ownership table: fetch/decode happens outside the lock.
    // Releasing a failed/canceled fill permits a live waiter to take over.
    fills: [64]?[32]u8 = @splat(null),
    wake_epoch: std.atomic.Value(u32) = .init(0),

    pub const Lookup = union(enum) { hit: Lease, fill: usize, wait: u32 };

    pub fn begin(self: *Cache, key: [32]u8) Lookup {
        self.lock();
        defer self.mu.unlock();
        if (self.acquireLocked(key)) |lease| return .{ .hit = lease };
        var empty: ?usize = null;
        for (self.fills, 0..) |fill, index| {
            if (fill) |existing| {
                if (std.mem.eql(u8, &existing, &key)) return .{ .wait = self.wake_epoch.load(.acquire) };
            } else empty = index;
        }
        const index = empty orelse return .{ .wait = self.wake_epoch.load(.acquire) };
        self.fills[index] = key;
        self.misses +|= 1;
        return .{ .fill = index };
    }

    pub fn finish(self: *Cache, index: usize, io: ?std.Io) void {
        self.lock();
        std.debug.assert(self.fills[index] != null);
        self.fills[index] = null;
        _ = self.wake_epoch.fetchAdd(1, .release);
        self.mu.unlock();
        (io orelse std.Options.debug_io).futexWake(u32, &self.wake_epoch.raw, std.math.maxInt(u32));
    }

    /// Notification-driven waiting avoids extra cold-read latency. The bounded
    /// timeout observes request-token cancellation independently of Io task
    /// cancellation. Epoch comparison prevents lost wakes; no lock spans I/O.
    pub fn awaitFill(self: *Cache, io: ?std.Io, cancellation: CancellationToken, observed_epoch: u32) !void {
        try cancellation.check();
        if (self.wake_epoch.load(.acquire) != observed_epoch) return;
        try (io orelse std.Options.debug_io).futexWaitTimeout(u32, &self.wake_epoch.raw, observed_epoch, .{
            .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake },
        });
        try cancellation.check();
    }

    pub fn snapshot(self: *Cache) struct { hits: u64, misses: u64, bytes: usize } {
        self.lock();
        defer self.mu.unlock();
        return .{ .hits = self.hits, .misses = self.misses, .bytes = self.retained_bytes };
    }

    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
    }

    pub fn acquire(self: *Cache, key: [32]u8) ?Lease {
        self.lock();
        defer self.mu.unlock();
        const lease = self.acquireLocked(key);
        if (lease == null) self.misses +|= 1;
        return lease;
    }

    fn acquireLocked(self: *Cache, key: [32]u8) ?Lease {
        self.clock +|= 1;
        for (self.entries) |maybe_entry| {
            const entry = maybe_entry orelse continue;
            if (!std.mem.eql(u8, &entry.key, &key)) continue;
            entry.references += 1;
            entry.touched = self.clock;
            self.hits +|= 1;
            return .{ .entry = entry, .cache = self };
        }
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
        for (self.fills) |fill| std.debug.assert(fill == null);
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

test "serverless graph metric routing cache coalesces misses and releases canceled fills" {
    var cache = Cache{};
    defer cache.deinit();
    const leader = cache.begin(@splat(1)).fill;
    try std.testing.expect(cache.begin(@splat(1)) == .wait);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().misses);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, cache.awaitFill(null, CancellationToken.fromAtomic(&canceled), 0));
    // Canceling a waiter does not disturb its leader. A failed leader releases
    // ownership so another live caller can independently retry.
    try std.testing.expect(cache.begin(@splat(1)) == .wait);
    const epoch = cache.begin(@splat(1)).wait;
    cache.finish(leader, null);
    try cache.awaitFill(null, .none, epoch); // Completion before waiting cannot lose a wake.
    const replacement = cache.begin(@splat(1)).fill;
    var filled = cache.adopt(try testEntry(1), 4096);
    defer filled.deinit();
    cache.finish(replacement, null);
    var shared = cache.begin(@splat(1)).hit;
    defer shared.deinit();
    try std.testing.expect(shared.entry == filled.entry);

    var fills: [64]usize = undefined;
    for (&fills, 0..) |*fill, i| fill.* = cache.begin(@splat(@intCast(i + 2))).fill;
    try std.testing.expect(cache.begin(@splat(100)) == .wait);
    for (fills) |fill| cache.finish(fill, null);
}

test "serverless graph metric routing cache wakes a concurrent waiter without duplicating a fill" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var cache = Cache{};
    defer cache.deinit();
    const leader = cache.begin(@splat(1)).fill;
    var started: std.Io.Event = .unset;
    const Waiter = struct {
        fn run(runtime: std.Io, target: *Cache, ready: *std.Io.Event) !void {
            const epoch = target.begin(@splat(1)).wait;
            ready.set(runtime);
            var observed = epoch;
            while (true) {
                try target.awaitFill(runtime, .none, observed);
                switch (target.begin(@splat(1))) {
                    .wait => |next| observed = next,
                    .hit => |hit| {
                        var lease = hit;
                        defer lease.deinit();
                        try std.testing.expectEqualStrings("immutable footer", lease.entry.footer);
                        return;
                    },
                    .fill => |unexpected| {
                        target.finish(unexpected, runtime);
                        return error.TestUnexpectedResult;
                    },
                }
            }
        }
    };
    var waiter = try io.concurrent(Waiter.run, .{ io, &cache, &started });
    defer _ = waiter.cancel(io) catch {};
    try started.wait(io);
    var filled = cache.adopt(try testEntry(1), 4096);
    defer filled.deinit();
    cache.finish(leader, io);
    try waiter.await(io);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().misses);
}
