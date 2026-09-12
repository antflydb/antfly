// Copyright 2026 Antfly, Inc.
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
    resident: bool = false,
    // Metadata gets a separate LRU so streaming decoded pages cannot evict
    // hot roots/directories ahead of unused pages. Both share the byte limit.
    class: enum(u1) { metadata, page } = .metadata,
    hash_next: ?*Entry = null,
    older: ?*Entry = null,
    newer: ?*Entry = null,

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
            cache.releaseLocked(self.entry);
        } else self.entry.destroy();
        self.* = undefined;
    }
};

pub const Cache = struct {
    mu: std.atomic.Mutex = .unlocked,
    // Intrusive buckets have no entry-count ceiling or hidden map allocations.
    // Every variable-size allocation is included in Entry.bytes().
    buckets: [1024]?*Entry = @splat(null),
    oldest: [2]?*Entry = @splat(null),
    newest: [2]?*Entry = @splat(null),
    retained_bytes: usize = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    // Fixed-capacity ownership table: fetch/decode happens outside the lock.
    // Releasing a failed/canceled fill permits a live waiter to take over.
    fills: [64]?Fill = @splat(null),
    wake_epoch: std.atomic.Value(u32) = .init(0),

    const Fill = struct {
        key: [32]u8,
        waiters: usize = 0,
        finished: bool = false,
        result: ?*Entry = null,
    };

    pub const Lookup = union(enum) { hit: Lease, fill: usize, wait: Waiter, saturated: u32 };

    /// A registered waiter pins the fill slot and its eventual result, even
    /// when the LRU cannot retain it or the producer drops its lease first.
    /// Registration happens once, not on every timed cancellation check.
    pub const Waiter = struct {
        cache: *Cache,
        index: usize,

        pub fn deinit(self: *Waiter) void {
            const cache = self.cache;
            cache.lock();
            defer cache.mu.unlock();
            const fill = &cache.fills[self.index].?;
            std.debug.assert(fill.waiters > 0);
            fill.waiters -= 1;
            cache.retireFillLocked(self.index);
            self.* = undefined;
        }

        pub fn awaitResult(self: *Waiter, io: ?std.Io, cancellation: CancellationToken) !?Lease {
            const cache = self.cache;
            while (true) {
                try cancellation.check();
                cache.lock();
                const fill = cache.fills[self.index].?;
                const epoch = cache.wake_epoch.load(.acquire);
                if (fill.finished) {
                    const result = if (fill.result) |entry| blk: {
                        entry.references += 1;
                        cache.hits +|= 1;
                        break :blk Lease{ .entry = entry, .cache = cache };
                    } else null;
                    cache.mu.unlock();
                    return result;
                }
                cache.mu.unlock();
                try cache.awaitFill(io, cancellation, epoch);
            }
        }
    };

    pub fn begin(self: *Cache, key: [32]u8) Lookup {
        self.lock();
        defer self.mu.unlock();
        if (self.acquireLocked(key)) |lease| return .{ .hit = lease };
        var empty: ?usize = null;
        for (&self.fills, 0..) |*fill, index| {
            if (fill.*) |*existing| {
                if (std.mem.eql(u8, &existing.key, &key)) {
                    if (existing.finished and existing.result == null)
                        return .{ .saturated = self.wake_epoch.load(.acquire) };
                    existing.waiters += 1;
                    return .{ .wait = .{ .cache = self, .index = index } };
                }
            } else empty = index;
        }
        const index = empty orelse return .{ .saturated = self.wake_epoch.load(.acquire) };
        self.fills[index] = .{ .key = key };
        self.misses +|= 1;
        return .{ .fill = index };
    }

    pub fn finish(self: *Cache, index: usize, io: ?std.Io) void {
        self.lock();
        std.debug.assert(self.fills[index] != null);
        self.fills[index].?.finished = true;
        self.retireFillLocked(index);
        _ = self.wake_epoch.fetchAdd(1, .release);
        self.mu.unlock();
        (io orelse std.Options.debug_io).futexWake(u32, &self.wake_epoch.raw, std.math.maxInt(u32));
    }

    fn retireFillLocked(self: *Cache, index: usize) void {
        const fill = self.fills[index].?;
        if (!fill.finished or fill.waiters != 0) return;
        if (fill.result) |entry| self.releaseLocked(entry);
        self.fills[index] = null;
        // Saturation waiters also use a bounded timeout; changing the epoch
        // ensures a newly available slot cannot suffer a lost notification.
        _ = self.wake_epoch.fetchAdd(1, .release);
    }

    fn releaseLocked(self: *Cache, entry: *Entry) void {
        std.debug.assert(entry.references > 0);
        entry.references -= 1;
        if (entry.references == 0) {
            if (entry.resident) self.append(entry) else entry.destroy();
        }
    }

    pub fn publish(self: *Cache, index: usize, entry: *Entry, max_bytes: usize) Lease {
        self.lock();
        defer self.mu.unlock();
        const lease = self.adoptLocked(entry, max_bytes);
        const fill = &self.fills[index].?;
        std.debug.assert(!fill.finished and fill.result == null);
        std.debug.assert(std.mem.eql(u8, &fill.key, &lease.entry.key));
        lease.entry.references += 1;
        fill.result = lease.entry;
        return lease;
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

    pub fn snapshot(self: *Cache) struct { hits: u64, misses: u64, bytes: usize, waiters: usize } {
        self.lock();
        defer self.mu.unlock();
        var waiters: usize = 0;
        for (self.fills) |fill| if (fill) |active| {
            waiters += active.waiters;
        };
        return .{ .hits = self.hits, .misses = self.misses, .bytes = self.retained_bytes, .waiters = waiters };
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
        if (self.find(key)) |entry| {
            if (entry.references == 0) self.unlink(entry);
            entry.references += 1;
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
        return self.adoptLocked(entry, max_bytes);
    }

    fn adoptLocked(self: *Cache, entry: *Entry, max_bytes: usize) Lease {
        if (self.find(entry.key)) |existing| {
            if (existing.references == 0) self.unlink(existing);
            existing.references += 1;
            entry.destroy();
            return .{ .entry = existing, .cache = self };
        }
        const needed = entry.bytes();
        entry.references = 1;
        if (needed > max_bytes) return .{ .entry = entry, .cache = self };
        while (self.retained_bytes > max_bytes - needed) {
            const old = self.victim(1) orelse self.victim(0) orelse return .{ .entry = entry, .cache = self };
            self.retained_bytes -= old.bytes();
            self.unlink(old);
            var link = &self.buckets[bucket(old.key)];
            while (link.*.? != old) link = &link.*.?.hash_next;
            link.* = old.hash_next;
            old.destroy();
        }
        entry.resident = true;
        const slot = &self.buckets[bucket(entry.key)];
        entry.hash_next = slot.*;
        slot.* = entry;
        self.retained_bytes += needed;
        return .{ .entry = entry, .cache = self };
    }

    fn bucket(key: [32]u8) usize {
        return std.mem.readInt(u64, key[0..8], .little) % 1024;
    }

    fn find(self: *Cache, key: [32]u8) ?*Entry {
        var next = self.buckets[bucket(key)];
        while (next) |entry| : (next = entry.hash_next) {
            if (std.mem.eql(u8, &entry.key, &key)) return entry;
        }
        return null;
    }

    fn victim(self: *Cache, class: usize) ?*Entry {
        // Only unpinned entries enter the LRU. Admission must remain O(1)
        // when a large query pins thousands of decoded pages.
        return self.oldest[class];
    }

    fn unlink(self: *Cache, entry: *Entry) void {
        const class = @intFromEnum(entry.class);
        if (entry.older) |older| older.newer = entry.newer else self.oldest[class] = entry.newer;
        if (entry.newer) |newer| newer.older = entry.older else self.newest[class] = entry.older;
    }

    fn append(self: *Cache, entry: *Entry) void {
        const class = @intFromEnum(entry.class);
        entry.older = self.newest[class];
        entry.newer = null;
        if (self.newest[class]) |newest| newest.newer = entry else self.oldest[class] = entry;
        self.newest[class] = entry;
    }

    /// The owning QueryCache outlives all query sessions and their leases.
    pub fn deinit(self: *Cache) void {
        for (self.fills) |fill| std.debug.assert(fill == null);
        for (self.buckets) |head| {
            var next = head;
            while (next) |entry| {
                next = entry.hash_next;
                std.debug.assert(entry.references == 0);
                entry.destroy();
            }
        }
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
    try std.testing.expect(!bypass.entry.resident);
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

test "serverless graph metric routing cache retains wide multi-metric working sets by bytes" {
    var cache = Cache{};
    defer cache.deinit();
    // Sixteen metrics, each with root + directory + three decoded pages.
    // The former 64-slot LRU missed all 80 entries on every sequential query.
    const limit = 1024 * 1024;
    for (0..80) |i| {
        const entry = try testEntry(@intCast(i));
        entry.class = if (i % 5 < 2) .metadata else .page;
        var lease = cache.adopt(entry, limit);
        lease.deinit();
    }
    for (0..4) |_| {
        var leases: [80]Lease = undefined;
        var count: usize = 0;
        defer for (leases[0..count]) |*lease| lease.deinit();
        for (&leases, 0..) |*lease, i| {
            lease.* = cache.acquire(@splat(@intCast(i))) orelse return error.TestUnexpectedResult;
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(u64, 320), cache.snapshot().hits);
    try std.testing.expectEqual(@as(u64, 0), cache.snapshot().misses);
    try std.testing.expect(cache.retained_bytes < limit);
}

test "serverless graph metric routing cache evicts pages before metadata and preserves pins" {
    var cache = Cache{};
    defer cache.deinit();
    const root = try testEntry(1);
    const limit = root.bytes() * 3;
    var metadata = cache.adopt(root, limit);
    metadata.deinit();
    const first = try testEntry(2);
    first.class = .page;
    var pinned = cache.adopt(first, limit);
    defer pinned.deinit();
    for (3..100) |i| {
        const entry = try testEntry(@intCast(i));
        entry.class = .page;
        var lease = cache.adopt(entry, limit);
        lease.deinit();
    }
    var hit = cache.acquire(@splat(1)) orelse return error.TestUnexpectedResult;
    defer hit.deinit();
    try std.testing.expectEqualStrings("immutable footer", pinned.entry.footer);
    try std.testing.expectEqual(limit, cache.retained_bytes);
}

test "serverless graph metric routing cache coalesces misses and releases canceled fills" {
    var cache = Cache{};
    defer cache.deinit();
    const leader = cache.begin(@splat(1)).fill;
    var canceled_waiter = cache.begin(@splat(1)).wait;
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().misses);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, canceled_waiter.awaitResult(null, CancellationToken.fromAtomic(&canceled)));
    canceled_waiter.deinit();
    // Canceling a waiter does not disturb its leader. A failed leader releases
    // ownership so another live caller can independently retry.
    var failed_waiter = cache.begin(@splat(1)).wait;
    cache.finish(leader, null);
    try std.testing.expect(try failed_waiter.awaitResult(null, .none) == null);
    failed_waiter.deinit();
    const replacement = cache.begin(@splat(1)).fill;
    var filled = cache.publish(replacement, try testEntry(1), 4096);
    defer filled.deinit();
    cache.finish(replacement, null);
    var shared = cache.begin(@splat(1)).hit;
    defer shared.deinit();
    try std.testing.expect(shared.entry == filled.entry);

    var fills: [64]usize = undefined;
    for (&fills, 0..) |*fill, i| fill.* = cache.begin(@splat(@intCast(i + 2))).fill;
    try std.testing.expect(cache.begin(@splat(100)) == .saturated);
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
            var registered = target.begin(@splat(1)).wait;
            defer registered.deinit();
            ready.set(runtime);
            var lease = (try registered.awaitResult(runtime, .none)) orelse return error.TestUnexpectedResult;
            defer lease.deinit();
            try std.testing.expectEqualStrings("immutable footer", lease.entry.footer);
        }
    };
    var waiter = try io.concurrent(Waiter.run, .{ io, &cache, &started });
    defer _ = waiter.cancel(io) catch {};
    try started.wait(io);
    var filled = cache.publish(leader, try testEntry(1), 0);
    defer filled.deinit();
    cache.finish(leader, io);
    try waiter.await(io);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().misses);
}

test "serverless graph metric routing cache shares bypass after producer release and canceled waiters" {
    var cache = Cache{};
    defer cache.deinit();
    inline for (.{ false, true }) |pinned_pressure| {
        var pinned = cache.adopt(try testEntry(2), 4096);
        const leader = cache.begin(@splat(1)).fill;
        var first = cache.begin(@splat(1)).wait;
        var canceled = cache.begin(@splat(1)).wait;
        var last = cache.begin(@splat(1)).wait;
        var producer = cache.publish(leader, try testEntry(1), if (pinned_pressure) pinned.entry.bytes() else 0);
        try std.testing.expect(!producer.entry.resident);
        cache.finish(leader, null);
        producer.deinit();
        canceled.deinit();
        var a = (try first.awaitResult(null, .none)).?;
        first.deinit();
        var b = (try last.awaitResult(null, .none)).?;
        last.deinit();
        try std.testing.expect(a.entry == b.entry);
        a.deinit();
        try std.testing.expectEqualStrings("immutable footer", b.entry.footer);
        b.deinit();
        pinned.deinit();
        try std.testing.expect(cache.acquire(@splat(1)) == null);
    }
}
