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
    pub const max_entries = 8;
    mutex: std.atomic.Mutex = .unlocked,
    entries: [max_entries]?*Entry = @splat(null),
    bytes: usize = 0,
    clock: u64 = 0,
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
        used: u64 = 0,
        bytes: usize = 0,
        retained: bool = false,
        pub fn create(a: Allocator, key: [32]u8, raw: []const u8) !*Entry {
            return createAdmitted(a, key, raw, null);
        }
        pub fn createAdmitted(a: Allocator, key: [32]u8, raw: []const u8, manager: ?*resources.ResourceManager) !*Entry {
            const entry = try a.create(Entry);
            entry.* = .{ .backing = a, .arena = undefined, .key = key, .config = undefined, .budget = if (manager) |resource_manager| resources.BudgetedAllocator.init(resource_manager, .document_extraction_working_set, a, 1) else null };
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
            a.destroy(self);
        }
    };
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    /// Register only at the lifecycle owner's stable address. A different
    /// admission domain bypasses retention rather than borrowing its budget.
    pub fn bind(self: *Cache, manager: ?*resources.ResourceManager) bool {
        self.lock();
        defer self.mutex.unlock();
        if (self.manager) |current| return manager == current;
        if (manager) |current| {
            for (self.entries) |slot| if (slot != null) return false;
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
            var victim: ?usize = null;
            for (self.entries, 0..) |slot, index| if (slot) |entry| {
                if (entry.refs == 0 and (victim == null or entry.used < self.entries[victim.?].?.used)) victim = index;
            };
            const index = victim orelse break;
            const entry = self.entries[index].?;
            self.entries[index] = null;
            self.bytes -= entry.bytes;
            released += if (entry.budget) |budget| budget.reservation.bytes else entry.bytes;
            entry.destroy();
        }
        return released;
    }
    pub fn get(self: *Cache, key: [32]u8) ?*Entry {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |slot| if (slot) |entry| {
            if (std.mem.eql(u8, &entry.key, &key)) {
                self.clock +%= 1;
                entry.used = self.clock;
                entry.refs += 1;
                return entry;
            }
        };
        return null;
    }
    /// Takes the creator's single lease. Failure to retain is a cache miss,
    /// not an inference failure; the caller still owns the entry until release.
    pub fn retain(self: *Cache, entry: *Entry) void {
        if (!self.bind(if (entry.budget) |budget| budget.reservation.manager else null)) return;
        entry.bytes = entry.arena.queryCapacity() + @sizeOf(Entry);
        if (entry.bytes > max_bytes) return;
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |slot| if (slot) |old| {
            if (std.mem.eql(u8, &old.key, &entry.key)) return;
        };
        while (true) {
            var empty: ?usize = null;
            var victim: ?usize = null;
            for (self.entries, 0..) |slot, i| {
                if (slot) |old| {
                    if (old.refs == 0 and (victim == null or old.used < self.entries[victim.?].?.used)) victim = i;
                } else empty = i;
            }
            if (empty != null and entry.bytes <= max_bytes - self.bytes) {
                self.clock +%= 1;
                entry.used = self.clock;
                entry.retained = true;
                self.entries[empty.?] = entry;
                self.bytes += entry.bytes;
                return;
            }
            const index = victim orelse return;
            const old = self.entries[index].?;
            self.bytes -= old.bytes;
            self.entries[index] = null;
            old.destroy();
        }
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
        for (self.entries) |slot| if (slot) |entry| {
            std.debug.assert(entry.refs == 0);
            entry.destroy();
        };
        self.entries = @splat(null);
        self.bytes = 0;
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
