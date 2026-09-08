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

pub const Cache = struct {
    pub const default_capacity = 4096; // 8.125 MiB of payload; allocated lazily.
    const Entry = struct {
        key: [32]u8,
        data: vector.Chunk,
        older: ?*Entry = null,
        newer: ?*Entry = null,
    };

    mu: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Entry) = .empty,
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
        const entry = self.entries.get(id) orelse {
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
        if (bytes.len != vector.encoded_len or self.capacity == 0) return;
        self.lock();
        defer self.mu.unlock();
        if (self.entries.contains(id)) return;
        const entry = if (self.entries.count() == self.capacity) blk: {
            const victim = self.oldest.?;
            _ = self.entries.remove(victim.key);
            self.unlink(victim);
            break :blk victim;
        } else alloc.create(Entry) catch return;
        entry.* = .{ .key = id, .data = bytes[0..vector.encoded_len].* };
        self.entries.put(alloc, id, entry) catch {
            alloc.destroy(entry);
            return;
        };
        self.append(entry);
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
        }
        self.entries.deinit(alloc);
        self.* = .{ .capacity = self.capacity };
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
    try std.testing.expectEqual(@as(usize, 2), cache.entries.count());
}
