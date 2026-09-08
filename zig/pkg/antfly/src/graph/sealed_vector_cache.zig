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

//! Index-owned LRU of sealed numeric vector chunks. Only consumers past the
//! producer barrier may use this cache. Keys hash the complete persisted key
//! (metric, job, lane, iteration, chunk); separate indexes never share entries.
//! Values are owned copies, never transaction/cursor memory. A caller copies
//! a hit under the lock, so eviction cannot invalidate an active checkpoint.
const std = @import("std");
const vector = @import("vector_chunk.zig");
const Allocator = std.mem.Allocator;

/// Shared admission across indexes, including allocation overhead. Exhaustion
/// is a storage-read fallback, never a failed build. Hosts may supply a smaller
/// budget; the default process-wide pool prevents per-index multiplication.
pub const Budget = struct {
    /// Configure before sharing with caches; immutable while they are active.
    limit: usize = 64 * 1024 * 1024,
    used: std.atomic.Value(usize) = .init(0),

    fn reserve(self: *Budget, bytes: usize) bool {
        var used = self.used.load(.monotonic);
        while (bytes <= self.limit and used <= self.limit - bytes) {
            used = self.used.cmpxchgWeak(used, used + bytes, .monotonic, .monotonic) orelse return true;
        }
        return false;
    }

    fn release(self: *Budget, bytes: usize) void {
        const prior = self.used.fetchSub(bytes, .monotonic);
        std.debug.assert(prior >= bytes);
    }
};

var process_budget = Budget{};

pub const Cache = struct {
    pub const default_capacity = 4096; // 8.125 MiB of payload; allocated lazily.
    const Entry = struct {
        key: [32]u8,
        data: vector.Chunk,
        scope: [32]u8,
        hash_next: ?*Entry = null,
        older: ?*Entry = null,
        newer: ?*Entry = null,
    };

    mu: std.atomic.Mutex = .unlocked,
    // Lazily allocated fixed buckets: no unaccounted hash-table growth.
    buckets: ?[]?*Entry = null,
    count: usize = 0,
    budget: *Budget = &process_budget,
    generation: u64 = 0,
    oldest: ?*Entry = null,
    newest: ?*Entry = null,
    capacity: usize = default_capacity,
    hits: u64 = 0,
    misses: u64 = 0,

    pub fn key(persisted_key: []const u8) [32]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(persisted_key, &digest, .{});
        return digest;
    }

    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
    }

    pub fn copy(self: *Cache, id: [32]u8, out: *vector.Chunk) bool {
        self.lock();
        defer self.mu.unlock();
        const entry = self.find(id) orelse {
            self.misses +|= 1;
            return false;
        };
        self.hits +|= 1;
        self.unlink(entry);
        self.append(entry);
        out.* = entry.data;
        return true;
    }

    /// Cache admission is optional: allocation failure must not fail a build.
    /// Missing chunks are never cached. Producers can populate partial chunks
    /// before the barrier; this API must only see a sealed source lane.
    pub fn put(self: *Cache, alloc: Allocator, id: [32]u8, bytes: []const u8) void {
        self.putAt(alloc, id, bytes, @splat(0), self.ticket());
    }

    pub fn ticket(self: *Cache) u64 {
        self.lock();
        defer self.mu.unlock();
        return self.generation;
    }

    /// A retired checkpoint may still finish a read, but cannot readmit data.
    pub fn putAt(self: *Cache, alloc: Allocator, id: [32]u8, bytes: []const u8, scope: [32]u8, generation: u64) void {
        if (bytes.len != vector.encoded_len or self.capacity == 0) return;
        self.lock();
        defer self.mu.unlock();
        if (generation != self.generation or self.find(id) != null) return;
        if (self.buckets == null) {
            const size = 1024 * @sizeOf(?*Entry);
            if (!self.budget.reserve(size)) return;
            self.buckets = alloc.alloc(?*Entry, 1024) catch {
                self.budget.release(size);
                return;
            };
            @memset(self.buckets.?, null);
        }
        const entry = if (self.count == self.capacity) blk: {
            const victim = self.oldest.?;
            self.removeHash(victim);
            self.unlink(victim);
            break :blk victim;
        } else blk: {
            if (!self.budget.reserve(@sizeOf(Entry))) {
                // A full shared pool must not freeze an existing index's
                // working set. Recycle locally without growing admission.
                if (self.oldest) |victim| {
                    self.removeHash(victim);
                    self.unlink(victim);
                    break :blk victim;
                }
                self.releaseEmptyBuckets(alloc);
                return;
            }
            const created = alloc.create(Entry) catch {
                self.budget.release(@sizeOf(Entry));
                self.releaseEmptyBuckets(alloc);
                return;
            };
            self.count += 1;
            break :blk created;
        };
        entry.* = .{ .key = id, .scope = scope, .data = bytes[0..vector.encoded_len].* };
        const slot = &self.buckets.?[bucket(id)];
        entry.hash_next = slot.*;
        slot.* = entry;
        self.append(entry);
    }

    fn bucket(id: [32]u8) usize {
        return std.mem.readInt(u64, id[0..8], .little) % 1024;
    }

    fn find(self: *Cache, id: [32]u8) ?*Entry {
        var next = (self.buckets orelse return null)[bucket(id)];
        while (next) |entry| : (next = entry.hash_next) {
            if (std.mem.eql(u8, &entry.key, &id)) return entry;
        }
        return null;
    }

    fn removeHash(self: *Cache, entry: *Entry) void {
        var link = &self.buckets.?[bucket(entry.key)];
        while (link.*.? != entry) link = &link.*.?.hash_next;
        link.* = entry.hash_next;
    }

    fn releaseEmptyBuckets(self: *Cache, alloc: Allocator) void {
        if (self.count != 0) return;
        if (self.buckets) |buckets| {
            alloc.free(buckets);
            self.budget.release(buckets.len * @sizeOf(?*Entry));
            self.buckets = null;
        }
    }

    pub fn retire(self: *Cache, alloc: Allocator, scope: [32]u8) void {
        self.lock();
        defer self.mu.unlock();
        self.generation +%= 1;
        var next = self.oldest;
        while (next) |entry| {
            next = entry.newer;
            if (!std.mem.eql(u8, &entry.scope, &scope)) continue;
            self.removeHash(entry);
            self.unlink(entry);
            alloc.destroy(entry);
            self.budget.release(@sizeOf(Entry));
            self.count -= 1;
        }
        self.releaseEmptyBuckets(alloc);
    }

    fn unlink(self: *Cache, entry: *Entry) void {
        if (entry.older) |older| older.newer = entry.newer else self.oldest = entry.newer;
        if (entry.newer) |newer| newer.older = entry.older else self.newest = entry.older;
    }

    fn append(self: *Cache, entry: *Entry) void {
        entry.older = self.newest;
        entry.newer = null;
        if (self.newest) |newest| newest.newer = entry else self.oldest = entry;
        self.newest = entry;
    }

    pub fn deinit(self: *Cache, alloc: Allocator) void {
        var next = self.oldest;
        while (next) |entry| {
            next = entry.newer;
            alloc.destroy(entry);
            self.budget.release(@sizeOf(Entry));
        }
        self.count = 0;
        self.releaseEmptyBuckets(alloc);
        self.* = .{ .capacity = self.capacity, .budget = self.budget };
    }
};

test "graph metric vector chunks sealed cache owns bytes isolates epochs and evicts least recent" {
    const alloc = std.testing.allocator;
    var cache = Cache{ .capacity = 2 };
    defer cache.deinit(alloc);
    var source: vector.Chunk = @splat(0);
    try vector.put(&source, 0, 0.5);
    const a = Cache.key("metric/job1/rank/0/1");
    const b = Cache.key("metric/job1/rank/0/2");
    const c = Cache.key("metric/job1/rank/1/1");
    cache.put(alloc, a, &source);
    cache.put(alloc, b, &source);
    try vector.put(&source, 0, 0.75);
    var read: vector.Chunk = undefined;
    try std.testing.expect(cache.copy(a, &read));
    try std.testing.expectEqual(0.5, try vector.get(&read, 0, true));
    cache.put(alloc, c, &source);
    try std.testing.expect(!cache.copy(b, &read));
    try std.testing.expect(cache.copy(c, &read));
    try std.testing.expectEqual(0.75, try vector.get(&read, 0, true));
    try std.testing.expect(!cache.copy(Cache.key("metric/job2/rank/1/1"), &read));
    try std.testing.expectEqual(@as(usize, 2), cache.count);
}

test "graph metric vector chunks shared budget retires scopes and fences late admission" {
    const alloc = std.testing.allocator;
    var budget = Budget{ .limit = 2 * 1024 * @sizeOf(?*Cache.Entry) + 3 * @sizeOf(Cache.Entry) };
    var a = Cache{ .budget = &budget };
    defer a.deinit(alloc);
    var b = Cache{ .budget = &budget };
    defer b.deinit(alloc);
    const bytes: vector.Chunk = @splat(0);
    const scope = Cache.key("metric/job");
    const other = Cache.key("other/job");
    const ticket = a.ticket();
    a.putAt(alloc, Cache.key("a"), &bytes, scope, ticket);
    a.putAt(alloc, Cache.key("b"), &bytes, other, ticket);
    b.put(alloc, Cache.key("c"), &bytes);
    try std.testing.expectEqual(budget.limit, budget.used.load(.monotonic));
    a.putAt(alloc, Cache.key("d"), &bytes, scope, ticket);
    try std.testing.expectEqual(@as(usize, 2), a.count);
    try std.testing.expectEqual(budget.limit, budget.used.load(.monotonic));
    a.retire(alloc, scope);
    try std.testing.expectEqual(@as(usize, 1), a.count);
    a.putAt(alloc, Cache.key("late"), &bytes, scope, ticket);
    try std.testing.expectEqual(@as(usize, 1), a.count);
    var copy: vector.Chunk = undefined;
    try std.testing.expect(a.copy(Cache.key("b"), &copy));
    a.retire(alloc, other);
    try std.testing.expect(a.buckets == null);
    b.retire(alloc, @splat(0));
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    budget.limit = 1;
    a.put(alloc, Cache.key("no room"), &bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "graph metric vector chunks failed optional allocations release shared admission" {
    for (0..2) |fail_index| {
        var budget = Budget{};
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        var cache = Cache{ .budget = &budget };
        defer cache.deinit(failing.allocator());
        const bytes: vector.Chunk = @splat(0);
        cache.put(failing.allocator(), Cache.key("failed"), &bytes);
        try std.testing.expectEqual(@as(usize, 0), cache.count);
        try std.testing.expect(cache.buckets == null);
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}
