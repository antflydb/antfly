// Copyright 2026 Antfly, Inc.
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

//! Session-owned cache of tree-packed Laya trunk keys and values
//! (models/laya/LAYA.md, "State cache"). A trunk never attends to a branch,
//! so its per-layer keys and values depend only on its tokens and the model.
//! A later request about the same state encodes only its question branches.
//!
//! Entries are host f32, bounded by `limit_bytes`, and evicted least recently
//! used. Entries in use are pinned and never evicted. The bound defaults to
//! ANTFLY_LAYA_TRUNK_CACHE_MB (256 MiB); 0 disables caching.
const std = @import("std");
const platform = @import("antfly_platform");

pub const default_limit_mb = 256;

/// Per-layer trunk keys (after RoPE for the encoder) and values, `[T, H]`
/// each, for every encoder layer followed by every decision-head layer.
pub const Entry = struct {
    key: [32]u8,
    tokens: usize,
    hidden: usize,
    layers: usize,
    values: []f32,
    pins: usize = 0,
    last_used: u64 = 0,

    pub fn keys(self: *const Entry, layer: usize) []const f32 {
        return self.values[(2 * layer) * self.tokens * self.hidden ..][0 .. self.tokens * self.hidden];
    }
    pub fn vals(self: *const Entry, layer: usize) []const f32 {
        return self.values[(2 * layer + 1) * self.tokens * self.hidden ..][0 .. self.tokens * self.hidden];
    }
    pub fn slot(self: *Entry, layer: usize, which: enum { keys, values }) []f32 {
        const index = 2 * layer + @intFromBool(which == .values);
        return self.values[index * self.tokens * self.hidden ..][0 .. self.tokens * self.hidden];
    }
    pub fn bytes(self: *const Entry) usize {
        return self.values.len * @sizeOf(f32);
    }
};

pub const Stats = struct { hits: u64 = 0, misses: u64 = 0, entries: usize = 0, bytes: usize = 0, evictions: u64 = 0 };

pub const Cache = struct {
    allocator: std.mem.Allocator,
    limit_bytes: usize,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    clock: u64 = 0,
    stats: Stats = .{},

    pub fn init(allocator: std.mem.Allocator, limit_bytes: usize) Cache {
        return .{ .allocator = allocator, .limit_bytes = limit_bytes };
    }

    pub fn fromEnvironment(allocator: std.mem.Allocator) Cache {
        const mb = platform.env.getenvUsize("ANTFLY_LAYA_TRUNK_CACHE_MB") orelse default_limit_mb;
        return init(allocator, std.math.mul(usize, mb, 1024 * 1024) catch std.math.maxInt(usize));
    }

    pub fn deinit(self: *Cache) void {
        for (self.entries.items) |entry| self.destroy(entry);
        self.entries.deinit(self.allocator);
    }

    fn destroy(self: *Cache, entry: *Entry) void {
        self.allocator.free(entry.values);
        self.allocator.destroy(entry);
    }

    pub fn key(ids: []const i64, layers: usize, hidden: usize) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-laya-trunk/v1");
        hash.update(std.mem.asBytes(&layers));
        hash.update(std.mem.asBytes(&hidden));
        hash.update(std.mem.sliceAsBytes(ids));
        return hash.finalResult();
    }

    /// Pin and return a cached trunk, or null on a miss.
    pub fn acquire(self: *Cache, k: [32]u8) ?*Entry {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &k)) {
            entry.pins += 1;
            self.clock += 1;
            entry.last_used = self.clock;
            self.stats.hits += 1;
            return entry;
        };
        self.stats.misses += 1;
        return null;
    }

    pub fn release(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        entry.pins -= 1;
        if (entry.pins == 0 and std.mem.indexOfScalar(*Entry, self.entries.items, entry) == null) self.destroy(entry);
    }

    /// Allocate an unpublished, pinned entry for the caller to fill.
    pub fn create(self: *Cache, k: [32]u8, tokens: usize, layers: usize, hidden: usize) !*Entry {
        const count = try std.math.mul(usize, try std.math.mul(usize, 2 * layers, tokens), hidden);
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .key = k, .tokens = tokens, .hidden = hidden, .layers = layers, .values = try self.allocator.alloc(f32, count), .pins = 1 };
        return entry;
    }

    /// Publish a filled entry when it fits the budget, evicting least recently
    /// used unpinned entries. The caller keeps its pin either way and must
    /// `release` it; an unpublished entry is freed on its last release.
    pub fn publish(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (entry.bytes() > self.limit_bytes) return;
        for (self.entries.items) |existing| if (std.mem.eql(u8, &existing.key, &entry.key)) return;
        while (self.stats.bytes + entry.bytes() > self.limit_bytes) {
            var victim: ?usize = null;
            for (self.entries.items, 0..) |candidate, i| {
                if (candidate.pins != 0) continue;
                if (victim == null or candidate.last_used < self.entries.items[victim.?].last_used) victim = i;
            }
            const index = victim orelse return;
            const evicted = self.entries.swapRemove(index);
            self.stats.bytes -= evicted.bytes();
            self.stats.evictions += 1;
            self.destroy(evicted);
        }
        self.entries.append(self.allocator, entry) catch return;
        self.clock += 1;
        entry.last_used = self.clock;
        self.stats.bytes += entry.bytes();
        self.stats.entries = self.entries.items.len;
    }

    pub fn snapshot(self: *Cache) Stats {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var out = self.stats;
        out.entries = self.entries.items.len;
        return out;
    }
};

test "laya trunk cache pins, evicts least recently used, and frees unpublished entries" {
    const a = std.testing.allocator;
    var cache = Cache.init(a, 2 * 2 * 4 * 8 * @sizeOf(f32));
    defer cache.deinit();
    const k1 = Cache.key(&.{ 1, 2, 3, 4 }, 1, 8);
    const k2 = Cache.key(&.{ 1, 2, 3, 5 }, 1, 8);
    const k3 = Cache.key(&.{ 9, 9, 9, 9 }, 1, 8);
    try std.testing.expect(!std.mem.eql(u8, &k1, &k2));
    try std.testing.expect(cache.acquire(k1) == null);
    for ([_][32]u8{ k1, k2 }) |k| {
        const entry = try cache.create(k, 4, 1, 8);
        @memset(entry.values, 1);
        cache.publish(entry);
        cache.release(entry);
    }
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
    // k1 is pinned, so publishing k3 must evict k2 even though k1 is older.
    const pinned = cache.acquire(k1).?;
    const third = try cache.create(k3, 4, 1, 8);
    cache.publish(third);
    cache.release(third);
    try std.testing.expect(cache.acquire(k2) == null);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().evictions);
    cache.release(pinned);
    // An entry larger than the whole budget is never published but still freed.
    const huge = try cache.create(Cache.key(&.{7}, 3, 64), 4, 3, 64);
    cache.publish(huge);
    cache.release(huge);
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
}
