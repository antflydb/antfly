// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Immutable publication maps, admitted only after fresh coverage and reader
//! authorization. Entries contain owned identities, never request contexts.
const std = @import("std");
const local = @import("antfly_local_sources");
const platform = @import("antfly_platform");
const state = @import("lake_index_native_state.zig");
const A = std.mem.Allocator;
const Map = std.StringHashMapUnmanaged([]const u8);
pub const Entry = struct {
    key: [32]u8,
    used: u64 = 0,
    references: std.atomic.Value(usize) = .init(1),
    arena: std.heap.ArenaAllocator,
    files: Map = .empty,
    private_files: Map = .empty,
    private_digests: Map = .empty,
    pub fn release(self: *Entry) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        self.arena.deinit();
        std.heap.page_allocator.destroy(self);
    }
};
pub const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Entry) = .empty,
    max_entries: usize = 64,
    tick: u64 = 0,
    budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 64 * 1024 * 1024 },
    pub fn deinit(self: *Cache) void {
        var iterator = self.entries.valueIterator();
        while (iterator.next()) |entry| {
            std.debug.assert(entry.*.references.load(.acquire) == 1);
            entry.*.release();
        }
        self.entries.deinit(std.heap.page_allocator);
        std.debug.assert(self.budget.live == 0);
    }
    fn lookup(self: *Cache, key: [32]u8) ?*Entry {
        const entry = self.entries.get(key) orelse return null;
        self.tick +|= 1;
        entry.used = self.tick;
        _ = entry.references.fetchAdd(1, .monotonic);
        return entry;
    }
    pub fn acquire(self: *Cache, key: [32]u8, provider: *@import("lake_index_row_source.zig").Provider) !*Entry {
        return self.acquireInner(key, provider) catch |err| {
            if (err != error.OutOfMemory) return err;
            self.evictIdle();
            return self.acquireInner(key, provider) catch |retry| switch (retry) {
                error.OutOfMemory => error.NativeLakeRuntimeCacheBusy,
                else => retry,
            };
        };
    }
    fn evictIdle(self: *Cache) void {
        var retired: [64]*Entry = undefined;
        var count: usize = 0;
        platform.sync.lockYielding(&self.mutex);
        var iterator = self.entries.valueIterator();
        while (iterator.next()) |entry| {
            if (entry.*.references.load(.acquire) != 1) continue;
            retired[count] = entry.*;
            count += 1;
        }
        for (retired[0..count]) |entry| _ = self.entries.remove(entry.key);
        self.mutex.unlock();
        for (retired[0..count]) |entry| entry.release();
    }
    fn acquireInner(self: *Cache, key: [32]u8, provider: *@import("lake_index_row_source.zig").Provider) !*Entry {
        try provider.context.ensureActive();
        platform.sync.lockYielding(&self.mutex);
        if (self.lookup(key)) |entry| {
            self.mutex.unlock();
            return entry;
        }
        self.mutex.unlock();
        // Build outside the cache lock: authenticated delete preparation may
        // perform remote I/O. Racing builders converge on the first admission.
        const entry = try std.heap.page_allocator.create(Entry);
        entry.* = .{ .key = key, .arena = .init(self.budget.allocator()) };
        errdefer entry.release();
        const a = entry.arena.allocator();
        for (provider.source.inventory.files) |file| {
            const owned_id = try a.dupe(u8, file.file_id);
            const public = try a.dupe(u8, &std.fmt.bytesToHex(local.storage_rowsource_identity.fileDigest(provider.source.inventory.source_id, provider.source.inventory.snapshot_id, file.file_id), .lower));
            const private = try a.dupe(u8, &std.fmt.bytesToHex(try state.identity(a, provider, file), .lower));
            try entry.files.put(a, public, owned_id);
            try entry.private_files.put(a, private, owned_id);
            try entry.private_digests.put(a, owned_id, private);
        }
        try provider.context.ensureActive();
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.lookup(key)) |existing| {
            entry.release();
            return existing;
        }
        if (self.max_entries == 0) return entry;
        if (self.entries.count() >= @min(self.max_entries, 64)) {
            var victim: ?*Entry = null;
            var iterator = self.entries.valueIterator();
            while (iterator.next()) |candidate| {
                if (candidate.*.references.load(.acquire) != 1) continue;
                if (victim == null or candidate.*.used < victim.?.used) victim = candidate.*;
            }
            if (victim) |old| {
                _ = self.entries.remove(old.key);
                old.release();
            } else return entry;
        }
        try self.entries.put(std.heap.page_allocator, key, entry);
        self.tick +|= 1;
        entry.used = self.tick;
        _ = entry.references.fetchAdd(1, .monotonic);
        return entry;
    }
};
