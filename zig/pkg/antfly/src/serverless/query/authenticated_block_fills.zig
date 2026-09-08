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

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../api/operation.zig").CancellationToken;

pub fn blockKey(artifact_id: []const u8, artifact_checksum: []const u8, offset: u64, len: usize, checksum: *const [32]u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(artifact_id);
    hash.update(artifact_checksum);
    hash.update(checksum);
    var extent: [16]u8 = undefined;
    std.mem.writeInt(u64, extent[0..8], offset, .little);
    std.mem.writeInt(u64, extent[8..16], len, .little);
    hash.update(&extent);
    var key: [32]u8 = undefined;
    hash.final(&key);
    return key;
}

/// Shared authenticated blocks, independent of transport grouping. Batches
/// claim all missing keys atomically and never wait while owning new fills.
pub const Cache = struct {
    const max_entries = 4096;
    const max_bytes = 64 * 1024 * 1024;
    const Entry = struct {
        key: [32]u8,
        data: []u8,
        refs: usize = 0,
        state: enum { pending, ready, failed } = .pending,
        touched: u64 = 0,
    };
    pub const Spec = struct { key: [32]u8, len: usize };
    pub const Item = struct {
        entry: *Entry,
        producer: bool,
        pub fn bytes(self: Item) []const u8 {
            return self.entry.data;
        }
        pub fn buffer(self: Item) []u8 {
            std.debug.assert(self.producer and self.entry.state == .pending);
            return self.entry.data;
        }
    };
    pub const Batch = struct {
        cache: *Cache,
        alloc: Allocator,
        items: []Item,
        pub fn publish(self: *Batch, io: ?std.Io) void {
            const cache = self.cache;
            cache.lock();
            for (self.items) |item| if (item.producer) {
                std.debug.assert(item.entry.state == .pending);
                item.entry.state = .ready;
            };
            cache.wake(io);
            cache.mu.unlock();
        }
        pub fn deinit(self: *Batch) void {
            const cache = self.cache;
            cache.lock();
            for (self.items) |item| cache.releaseLocked(item.entry, item.producer);
            cache.wake(null);
            cache.mu.unlock();
            self.alloc.free(self.items);
            self.* = undefined;
        }
    };
    pub const Waiter = struct {
        cache: *Cache,
        entry: *Entry,
        pub fn deinit(self: *Waiter) void {
            self.cache.lock();
            self.cache.waiters -= 1;
            self.cache.releaseLocked(self.entry, false);
            self.cache.wake(null);
            self.cache.mu.unlock();
            self.* = undefined;
        }
        pub fn awaitReady(self: *Waiter, io: ?std.Io, cancellation: CancellationToken) !void {
            while (true) {
                try cancellation.check();
                self.cache.lock();
                const ready = self.entry.state != .pending;
                const epoch = self.cache.epoch.load(.acquire);
                self.cache.mu.unlock();
                if (ready) return;
                try self.cache.wait(io, cancellation, epoch);
            }
        }
    };
    pub const Lookup = union(enum) { batch: Batch, wait: Waiter, saturated: u32 };
    mu: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Entry) = .empty,
    alloc: ?Allocator = null,
    retained: usize = 0,
    live_entries: usize = 0,
    waiters: usize = 0,
    clock: u64 = 0,
    epoch: std.atomic.Value(u32) = .init(0),

    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
    }
    fn wake(self: *Cache, io: ?std.Io) void {
        _ = self.epoch.fetchAdd(1, .release);
        (io orelse std.Options.debug_io).futexWake(u32, &self.epoch.raw, std.math.maxInt(u32));
    }
    pub fn wait(self: *Cache, io: ?std.Io, cancellation: CancellationToken, epoch: u32) !void {
        try cancellation.check();
        try (io orelse std.Options.debug_io).futexWaitTimeout(u32, &self.epoch.raw, epoch, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } });
        try cancellation.check();
    }
    fn destroyLocked(self: *Cache, entry: *Entry) void {
        self.retained -= entry.data.len;
        self.live_entries -= 1;
        self.alloc.?.free(entry.data);
        self.alloc.?.destroy(entry);
    }
    fn releaseLocked(self: *Cache, entry: *Entry, producer: bool) void {
        if (producer and entry.state == .pending) {
            _ = self.entries.remove(entry.key);
            entry.state = .failed;
        }
        std.debug.assert(entry.refs > 0);
        entry.refs -= 1;
        if (entry.refs == 0 and entry.state == .failed) self.destroyLocked(entry);
    }

    pub fn begin(self: *Cache, owner: Allocator, result_alloc: Allocator, specs: []const Spec) !Lookup {
        var bytes: usize = 0;
        for (specs) |spec| {
            if (spec.len == 0) return error.InvalidCacheBatch;
            bytes = std.math.add(usize, bytes, spec.len) catch return error.InvalidCacheBatch;
        }
        if (specs.len > max_entries or bytes > 8 * 1024 * 1024) return error.InvalidCacheBatch;
        const items = try result_alloc.alloc(Item, specs.len);
        var transferred = false;
        defer if (!transferred) result_alloc.free(items);
        self.lock();
        defer self.mu.unlock();
        if (self.alloc == null) self.alloc = owner;
        self.clock +%= 1;
        var needed_bytes: usize = 0;
        var needed_entries: usize = 0;
        for (specs) |spec| {
            if (self.entries.get(spec.key)) |entry| {
                if (entry.data.len != spec.len or entry.touched == self.clock) return error.InvalidCacheBatch;
                entry.touched = self.clock;
                if (entry.state == .pending) {
                    entry.refs += 1;
                    self.waiters += 1;
                    return .{ .wait = .{ .cache = self, .entry = entry } };
                }
            } else {
                needed_bytes += spec.len;
                needed_entries += 1;
            }
        }
        if (self.retained + needed_bytes > max_bytes or self.live_entries + needed_entries > max_entries) {
            // Select the whole eviction batch once; rescanning the table for
            // every victim makes a broad cold miss quadratic under this lock.
            var victims: [max_entries]*Entry = undefined;
            var victim_count: usize = 0;
            var reclaimable: usize = 0;
            var it = self.entries.valueIterator();
            while (it.next()) |ptr| {
                const entry = ptr.*;
                if (entry.refs != 0 or entry.state != .ready or entry.touched == self.clock) continue;
                victims[victim_count] = entry;
                victim_count += 1;
                reclaimable += entry.data.len;
            }
            if (self.retained - reclaimable + needed_bytes > max_bytes or self.live_entries - victim_count + needed_entries > max_entries)
                return .{ .saturated = self.epoch.load(.acquire) };
            std.mem.sort(*Entry, victims[0..victim_count], {}, struct {
                fn less(_: void, a: *Entry, b: *Entry) bool {
                    return a.touched < b.touched;
                }
            }.less);
            for (victims[0..victim_count]) |entry| {
                if (self.retained + needed_bytes <= max_bytes and self.live_entries + needed_entries <= max_entries) break;
                _ = self.entries.remove(entry.key);
                self.destroyLocked(entry);
            }
        }
        const a = self.alloc.?;
        try self.entries.ensureUnusedCapacity(a, @intCast(needed_entries));
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |item| self.releaseLocked(item.entry, item.producer);
            self.wake(null);
        }
        for (specs, items) |spec, *item| {
            if (self.entries.get(spec.key)) |entry| {
                // Duplicate new keys must not turn one batch into its own waiter.
                if (entry.state == .pending) return error.InvalidCacheBatch;
                entry.refs += 1;
                item.* = .{ .entry = entry, .producer = false };
            } else {
                const entry = try a.create(Entry);
                errdefer a.destroy(entry);
                const data = try a.alloc(u8, spec.len);
                entry.* = .{ .key = spec.key, .data = data, .refs = 1, .touched = self.clock };
                self.entries.putAssumeCapacity(spec.key, entry);
                self.retained += spec.len;
                self.live_entries += 1;
                item.* = .{ .entry = entry, .producer = true };
            }
            initialized += 1;
        }
        transferred = true;
        return .{ .batch = .{ .cache = self, .alloc = result_alloc, .items = items } };
    }
    pub fn acquire(self: *Cache, owner: Allocator, result_alloc: Allocator, specs: []const Spec, io: ?std.Io, cancellation: CancellationToken) !Batch {
        while (true) {
            try cancellation.check();
            switch (try self.begin(owner, result_alloc, specs)) {
                .batch => |batch| return batch,
                .wait => |registered| {
                    var waiter = registered;
                    defer waiter.deinit();
                    try waiter.awaitReady(io, cancellation);
                },
                .saturated => |epoch| try self.wait(io, cancellation, epoch),
            }
        }
    }
    pub fn deinit(self: *Cache) void {
        var it = self.entries.valueIterator();
        while (it.next()) |ptr| {
            std.debug.assert(ptr.*.refs == 0 and ptr.*.state == .ready);
            self.destroyLocked(ptr.*);
        }
        if (self.alloc) |a| self.entries.deinit(a);
        std.debug.assert(self.retained == 0);
        self.* = undefined;
    }

    pub fn snapshot(self: *Cache) struct { bytes: usize, entries: usize, waiters: usize } {
        self.lock();
        defer self.mu.unlock();
        return .{ .bytes = self.retained, .entries = self.live_entries, .waiters = self.waiters };
    }

    pub const Lease = struct {
        cache: *Cache,
        entry: *Entry,

        pub fn bytes(self: Lease) []const u8 {
            return self.entry.data;
        }

        pub fn deinit(self: *Lease) void {
            self.cache.lock();
            self.cache.releaseLocked(self.entry, false);
            self.cache.wake(null);
            self.cache.mu.unlock();
            self.* = undefined;
        }
    };

    pub fn leaseIfReady(self: *Cache, key: [32]u8) ?Lease {
        self.lock();
        const entry = self.entries.get(key) orelse {
            self.mu.unlock();
            return null;
        };
        if (entry.state != .ready) {
            self.mu.unlock();
            return null;
        }
        entry.refs += 1;
        self.clock +%= 1;
        entry.touched = self.clock;
        self.mu.unlock();
        return .{ .cache = self, .entry = entry };
    }

    pub fn copyIfReadyAlloc(self: *Cache, result_alloc: Allocator, key: [32]u8) !?[]u8 {
        var lease = self.leaseIfReady(key) orelse return null;
        defer lease.deinit();
        return try result_alloc.dupe(u8, lease.bytes());
    }

    /// Optional disk-hit promotion. Never wait: the caller may itself own a
    /// pending fill, and retention pressure must not prevent serving a hit.
    /// The caller authenticates bytes against this canonical key first.
    pub fn retainVerified(self: *Cache, owner: Allocator, key: [32]u8, bytes: []const u8) void {
        switch (self.begin(owner, owner, &.{.{ .key = key, .len = bytes.len }}) catch return) {
            .batch => |reserved| {
                var batch = reserved;
                defer batch.deinit();
                if (batch.items[0].producer) @memcpy(batch.items[0].buffer(), bytes);
                batch.publish(null);
            },
            .wait => |registered| {
                var waiter = registered;
                waiter.deinit();
            },
            .saturated => {},
        }
    }
};

test "serverless canonical block disk promotion bypasses pending fills and allocation pressure" {
    const alloc = std.testing.allocator;
    var cache = Cache{};
    defer cache.deinit();
    const key = [_]u8{7} ** 32;
    var pending = try cache.acquire(alloc, alloc, &.{.{ .key = key, .len = 4 }}, null, .none);
    defer pending.deinit();
    cache.retainVerified(alloc, key, "data");
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().waiters);
    try std.testing.expect(cache.leaseIfReady(key) == null);
    @memcpy(pending.items[0].buffer(), "data");
    pending.publish(null);
    var lease = cache.leaseIfReady(key).?;
    defer lease.deinit();
    try std.testing.expectEqualStrings("data", lease.bytes());

    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var pressured = Cache{};
    defer pressured.deinit();
    pressured.retainVerified(failing.allocator(), key, "data");
    try std.testing.expectEqual(@as(usize, 0), pressured.snapshot().entries);
}

test "serverless canonical block fills atomically share overlapping transport sets" {
    const alloc = std.testing.allocator;
    var cache = Cache{};
    defer cache.deinit();
    const a = Cache.Spec{ .key = @splat(1), .len = 4 };
    const b = Cache.Spec{ .key = @splat(2), .len = 4 };
    const c = Cache.Spec{ .key = @splat(3), .len = 4 };
    var first = (try cache.begin(alloc, alloc, &.{ a, b })).batch;
    var waiter = (try cache.begin(alloc, alloc, &.{ b, c })).wait;
    defer waiter.deinit();
    try std.testing.expectEqual(@as(usize, 2), cache.entries.count());
    @memcpy(first.items[0].buffer(), "aaaa");
    @memcpy(first.items[1].buffer(), "bbbb");
    first.publish(null);
    first.deinit();
    try waiter.awaitReady(null, .none);
    var second = try cache.acquire(alloc, alloc, &.{ b, c }, null, .none);
    defer second.deinit();
    try std.testing.expect(!second.items[0].producer);
    try std.testing.expect(second.items[1].producer);
    try std.testing.expectEqualStrings("bbbb", second.items[0].bytes());
    @memcpy(second.items[1].buffer(), "cccc");
    second.publish(null);
}

test "serverless canonical block fill cancellation leaves producer alive and failure permits takeover" {
    const alloc = std.testing.allocator;
    var cache = Cache{};
    defer cache.deinit();
    const spec = Cache.Spec{ .key = @splat(1), .len = 4 };
    var producer = (try cache.begin(alloc, alloc, &.{spec})).batch;
    var waiter = (try cache.begin(alloc, alloc, &.{spec})).wait;
    const Cancel = struct {
        fn canceled(_: *const anyopaque) bool {
            return true;
        }
    };
    try std.testing.expectError(error.Canceled, waiter.awaitReady(null, .{ .ptr = &cache, .is_cancelled_fn = Cancel.canceled }));
    waiter.deinit();
    try std.testing.expectEqual(@as(usize, 1), producer.items[0].entry.refs);
    waiter = (try cache.begin(alloc, alloc, &.{spec})).wait;
    producer.deinit();
    try waiter.awaitReady(null, .none);
    try std.testing.expectEqual(@as(usize, 1), cache.live_entries);
    waiter.deinit();
    try std.testing.expectEqual(@as(usize, 0), cache.live_entries);
    var replacement = try cache.acquire(alloc, alloc, &.{spec}, null, .none);
    defer replacement.deinit();
    try std.testing.expect(replacement.items[0].producer);
    @memcpy(replacement.items[0].buffer(), "good");
    replacement.publish(null);
}

test "serverless canonical block fill allocations unwind every partial claim" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var cache = Cache{};
            defer cache.deinit();
            var batch = try cache.acquire(alloc, alloc, &.{
                .{ .key = @splat(1), .len = 4 }, .{ .key = @splat(2), .len = 4 },
            }, null, .none);
            defer batch.deinit();
            for (batch.items) |item| @memset(item.buffer(), 0);
            batch.publish(null);
            const copied = (try cache.copyIfReadyAlloc(alloc, @splat(1))).?;
            defer alloc.free(copied);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "serverless canonical block fills reject duplicate keys without leaking pending claims" {
    const alloc = std.testing.allocator;
    var cache = Cache{};
    defer cache.deinit();
    const spec = Cache.Spec{ .key = @splat(1), .len = 4 };
    try std.testing.expectError(error.InvalidCacheBatch, cache.begin(alloc, alloc, &.{ spec, spec }));
    try std.testing.expectEqual(@as(usize, 0), cache.live_entries);
}

test "serverless canonical block admission bounds failed pinned entries and preserves requested hits on eviction" {
    const alloc = std.testing.allocator;
    var cache = Cache{};
    defer cache.deinit();
    const specs = try alloc.alloc(Cache.Spec, Cache.max_entries);
    defer alloc.free(specs);
    for (specs, 0..) |*spec, i| {
        spec.* = .{ .key = @splat(0), .len = 1 };
        std.mem.writeInt(u64, spec.key[0..8], i, .little);
    }
    var producer = (try cache.begin(alloc, alloc, specs)).batch;
    var waiter = (try cache.begin(alloc, alloc, specs[0..1])).wait;
    const extra = Cache.Spec{ .key = @splat(255), .len = 1 };
    try std.testing.expect((try cache.begin(alloc, alloc, &.{extra})) == .saturated);
    cache.retainVerified(alloc, extra.key, "x");
    try std.testing.expectEqual(Cache.max_entries, cache.snapshot().entries);
    try std.testing.expect(cache.leaseIfReady(extra.key) == null);
    producer.deinit();
    // The failed entry has left the map, but its waiter still pins memory.
    try std.testing.expectEqual(@as(usize, 1), cache.live_entries);
    try std.testing.expect((try cache.begin(alloc, alloc, specs)) == .saturated);
    waiter.deinit();
    producer = (try cache.begin(alloc, alloc, specs)).batch;
    for (producer.items) |item| @memset(item.buffer(), 42);
    producer.publish(null);
    producer.deinit();
    var mixed = (try cache.begin(alloc, alloc, &.{ specs[0], extra })).batch;
    defer mixed.deinit();
    try std.testing.expect(!mixed.items[0].producer and mixed.items[1].producer);
    try std.testing.expectEqual(@as(u8, 42), mixed.items[0].bytes()[0]);
    @memset(mixed.items[1].buffer(), 1);
    mixed.publish(null);
    try std.testing.expectEqual(@as(usize, Cache.max_entries), cache.live_entries);
}
