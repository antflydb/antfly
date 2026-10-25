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

//! Runtime-owned validated vocabularies and packed prompt matrices. Leases keep
//! immutable entries alive across concurrent scoring; busy entries are never
//! evicted. Unknown model revisions bypass retention.
const std = @import("std");
const labels = @import("vector_labels.zig");
const resources = @import("../../resource_manager.zig");
const Allocator = std.mem.Allocator;
pub const Cache = struct {
    pub const max_bytes = 32 * 1024 * 1024;
    pub const max_entries = 4096;
    const bucket_count = 1024;
    mutex: std.atomic.Mutex = .unlocked,
    buckets: [bucket_count]?*Entry = @splat(null),
    oldest: ?*Entry = null,
    newest: ?*Entry = null,
    count: usize = 0,
    bytes: usize = 0,
    manager: ?*resources.ResourceManager = null,
    reclaimer_id: u64 = 0,
    pub const Entry = struct {
        backing: Allocator,
        budget: ?resources.BudgetedAllocator = null,
        arena: std.heap.ArenaAllocator,
        config: labels.Config,
        scorer: ?labels.Scorer = null,
        key: [32]u8,
        refs: usize = 1,
        hash_next: ?*Entry = null,
        older: ?*Entry = null,
        newer: ?*Entry = null,
        header: ?resources.Reservation = null,
        bytes: usize = 0,
        retained: bool = false,
        pub fn create(a: Allocator, key: [32]u8, raw: []const u8) !*Entry {
            return createAdmitted(a, key, raw, null);
        }
        pub fn createAdmitted(a: Allocator, key: [32]u8, raw: []const u8, manager: ?*resources.ResourceManager) !*Entry {
            var header = if (manager) |value| try value.reserve(.document_extraction_working_set, @sizeOf(Entry)) else null;
            const entry = a.create(Entry) catch |err| {
                if (header) |*lease| lease.release();
                return err;
            };
            entry.* = .{ .header = header, .backing = a, .arena = undefined, .key = key, .config = undefined, .budget = if (manager) |resource_manager| resources.BudgetedAllocator.init(resource_manager, .document_extraction_working_set, a, 1) else null };
            entry.arena = .init(if (entry.budget) |*budget| budget.allocator() else a);
            errdefer entry.destroy();
            entry.initialize(raw) catch |err| return entry.admissionError(err);
            return entry;
        }
        fn initialize(self: *Entry, raw: []const u8) !void {
            const a = self.arena.allocator();
            const owned = try a.dupe(u8, raw);
            self.config = try std.json.parseFromSliceLeaky(labels.Config, a, owned, .{});
            try self.config.validate();
        }
        pub fn admissionError(self: *Entry, err: anyerror) anyerror {
            if (err == error.OutOfMemory) if (self.budget) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
            return err;
        }
        fn destroy(self: *Entry) void {
            const a = self.backing;
            self.arena.deinit();
            if (self.budget) |*budget| budget.deinit();
            if (self.header) |*lease| lease.release();
            a.destroy(self);
        }
    };
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn bucket(identity: [32]u8) usize {
        return @intCast(std.hash.Wyhash.hash(0, &identity) % bucket_count);
    }
    fn find(self: *Cache, identity: [32]u8) ?*Entry {
        var next = self.buckets[bucket(identity)];
        while (next) |entry| : (next = entry.hash_next) {
            if (std.mem.eql(u8, &identity, &entry.key)) return entry;
        }
        return null;
    }
    fn unlinkLru(self: *Cache, entry: *Entry) void {
        if (entry.older) |older| older.newer = entry.newer else self.oldest = entry.newer;
        if (entry.newer) |newer| newer.older = entry.older else self.newest = entry.older;
    }
    fn touch(self: *Cache, entry: *Entry) void {
        entry.older = self.newest;
        entry.newer = null;
        if (self.newest) |previous| previous.newer = entry else self.oldest = entry;
        self.newest = entry;
    }
    fn evictable(self: *Cache) ?*Entry {
        var next = self.oldest;
        while (next) |entry| : (next = entry.newer) if (entry.refs == 0) return entry;
        return null;
    }
    fn remove(self: *Cache, entry: *Entry) void {
        var link = &self.buckets[bucket(entry.key)];
        while (link.*) |candidate| {
            if (candidate == entry) {
                link.* = entry.hash_next;
                break;
            }
            link = &candidate.hash_next;
        }
        self.unlinkLru(entry);
        self.bytes -= entry.bytes;
        self.count -= 1;
        entry.retained = false;
        entry.destroy();
    }
    /// Register only at the lifecycle owner's stable address. A different
    /// admission domain bypasses retention rather than borrowing its budget.
    pub fn bind(self: *Cache, manager: ?*resources.ResourceManager) bool {
        self.lock();
        defer self.mutex.unlock();
        if (self.manager) |current| return manager == current;
        if (manager) |current| {
            if (self.count != 0) return false;
            self.reclaimer_id = current.registerReclaimer(.document_extraction_working_set, self, reclaim) catch return false;
            self.manager = current;
        }
        return true;
    }
    pub fn reclaim(raw: *anyopaque, target_bytes: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(raw));
        if (!self.mutex.tryLock()) return 0;
        defer self.mutex.unlock();
        var released: u64 = 0;
        while (released < target_bytes) {
            const entry = self.evictable() orelse break;
            released += if (entry.budget) |budget| budget.reservation.bytes + (if (entry.header) |header| header.bytes else 0) else entry.bytes;
            self.remove(entry);
        }
        return released;
    }
    pub fn get(self: *Cache, key: [32]u8) ?*Entry {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.find(key) orelse return null;
        self.unlinkLru(entry);
        self.touch(entry);
        entry.refs += 1;
        return entry;
    }
    /// Takes the creator's single lease. Failure to retain is a cache miss,
    /// not an inference failure; the caller still owns the entry until release.
    pub fn retain(self: *Cache, entry: *Entry) void {
        if (!self.bind(if (entry.budget) |budget| budget.reservation.manager else null)) return;
        entry.bytes = entry.arena.queryCapacity() + @sizeOf(Entry);
        if (entry.bytes > max_bytes) return;
        self.lock();
        defer self.mutex.unlock();
        if (self.find(entry.key) != null) return;
        while (self.count >= max_entries or entry.bytes > max_bytes - self.bytes) {
            const victim = self.evictable() orelse return;
            self.remove(victim);
        }
        const index = bucket(entry.key);
        entry.hash_next = self.buckets[index];
        self.buckets[index] = entry;
        self.touch(entry);
        entry.retained = true;
        self.count += 1;
        self.bytes += entry.bytes;
    }
    pub fn release(self: *Cache, entry: *Entry) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(entry.refs > 0);
        entry.refs -= 1;
        if (!entry.retained) entry.destroy();
    }
    /// Called after runtime execution lanes have joined.
    pub fn deinit(self: *Cache) void {
        // Unregistration drains callbacks without holding the cache lock.
        if (self.manager) |manager| manager.unregisterReclaimer(self.reclaimer_id);
        self.manager = null;
        self.reclaimer_id = 0;
        while (self.oldest) |entry| {
            std.debug.assert(entry.refs == 0);
            self.remove(entry);
        }
    }
};

test "model aware media vocabulary cache owns strings fences generations and preserves busy leases" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit();
    const raw = try a.dupe(u8, "{\"labels\":[{\"label\":\"speech\"}],\"top_k\":1}");
    const first = try Cache.Entry.create(a, @splat(1), raw);
    a.free(raw);
    first.scorer = try labels.Scorer.initValidated(first.arena.allocator(), first.config, &.{&.{ 1, 0 }});
    cache.retain(first);
    const hit = cache.get(@splat(1)).?;
    try std.testing.expect(first == hit);
    try std.testing.expectEqualStrings("speech", hit.scorer.?.config.labels[0].label);
    try std.testing.expect(cache.get(@splat(2)) == null);
    cache.release(hit);
    for (2..12) |generation| {
        const entry = try Cache.Entry.create(a, @splat(@intCast(generation)), "{\"labels\":[{\"label\":\"other\"}]}");
        cache.retain(entry);
        cache.release(entry);
    }
    const still_live = cache.get(@splat(1)).?;
    try std.testing.expect(first == still_live);
    cache.release(still_live);
    cache.release(first);
    try std.testing.expect(cache.bytes <= Cache.max_bytes);
}

test "model aware media vocabulary construction and retained matrices hold resource admission until eviction" {
    const a = std.testing.allocator;
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.document_extraction_working_set)] = .{ .hard_limit_bytes = 1 };
    var denied = resources.ResourceManager.init(.{ .budgets = budgets });
    defer denied.deinit(a);
    try std.testing.expectError(error.ResourceBudgetExceeded, Cache.Entry.createAdmitted(a, @splat(1), "{\"labels\":[{\"label\":\"speech\"}]}", &denied));
    try std.testing.expectEqual(@as(u64, 0), denied.sliceStats(.document_extraction_working_set).used_bytes);
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(a);
    var cache: Cache = .{};
    const entry = try Cache.Entry.createAdmitted(a, @splat(2), "{\"labels\":[{\"label\":\"speech\"}]}", &manager);
    entry.scorer = try labels.Scorer.initValidated(entry.arena.allocator(), entry.config, &.{&.{ 1, 0 }});
    cache.retain(entry);
    cache.release(entry);
    try std.testing.expect(manager.sliceStats(.document_extraction_working_set).used_bytes > 0);
    cache.deinit();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.document_extraction_working_set).used_bytes);
}

test "model aware media vocabulary cache releases idle admission under pressure and keeps scoring leases pinned" {
    const a = std.testing.allocator;
    var budgets = resources.Options.defaultBudgets();
    const limit = 64 * 1024;
    budgets[@backingInt(resources.Slice.document_extraction_working_set)] = .{ .hard_limit_bytes = limit };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    var cache: Cache = .{};
    defer cache.deinit();
    const entry = try Cache.Entry.createAdmitted(a, @splat(1), "{\"labels\":[{\"label\":\"speech\"}]}", &manager);
    cache.retain(entry);
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(&cache, limit));
    cache.release(entry);
    var required = try manager.reserve(.document_extraction_working_set, limit);
    defer required.release();
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try std.testing.expect(cache.get(@splat(1)) == null);
}

test "model aware media vocabulary LRU handles hash collisions and reclaims only idle leases" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit();
    var keys: [32][32]u8 = undefined;
    var candidate: u64 = 0;
    for (&keys) |*key| {
        while (true) : (candidate += 1) {
            key.* = @splat(0);
            std.mem.writeInt(u64, key[0..8], candidate, .little);
            if (Cache.bucket(key.*) == 0) break;
        }
        candidate += 1;
        const entry = try Cache.Entry.create(a, key.*, "{\"labels\":[{\"label\":\"speech\"}]}");
        cache.retain(entry);
        cache.release(entry);
    }
    try std.testing.expectEqual(@as(usize, 32), cache.count);
    const pinned = cache.get(keys[1]).?;
    const recent = cache.get(keys[0]).?;
    cache.release(recent);
    try std.testing.expect(Cache.reclaim(&cache, 1) > 0);
    try std.testing.expect(cache.get(keys[2]) == null);
    const hit = cache.get(keys[0]).?;
    cache.release(hit);
    _ = Cache.reclaim(&cache, Cache.max_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.count);
    try std.testing.expectEqualStrings("speech", pinned.config.labels[0].label);
    cache.release(pinned);
    _ = Cache.reclaim(&cache, Cache.max_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.count);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
}
