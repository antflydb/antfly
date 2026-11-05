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

//! Worker-owned, reference-counted prepared builds. Only completed in-memory
//! indexes are shared; probe cursors and match witnesses belong to each task.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Hash = local.sql_operators.HashJoin;
const Filter = local.sql_dynamic_filter.Filter;
pub const Cache = struct {
    a: A,
    bytes: usize,
    mutex: std.atomic.Mutex = .unlocked,
    entries: [4]?*Entry = @splat(null),
    live: usize = 0,
    peak: usize = 0,
    clock: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    rejected: [8]?[32]u8 = @splat(null),
    rejected_hand: usize = 0,
    const Entry = struct {
        key: [32]u8,
        budget: local.sql_memory_budget,
        hash: ?*Hash = null,
        filter: ?*Filter = null,
        names: std.heap.ArenaAllocator,
        used: u64,
        refs: usize = 1,
        ready: bool = false,
    };
    pub const Lease = struct {
        cache: *Cache,
        entry: *Entry,
        hit: bool,
        pub fn hash(self: Lease) *Hash {
            return self.entry.hash.?;
        }
        pub fn allocator(self: Lease) A {
            return self.entry.budget.allocator();
        }
        pub fn filter(self: Lease) ?*const Filter {
            return self.entry.filter;
        }
        pub fn createFilter(self: Lease, columns: []const local.sql_dynamic_filter.Column) !*Filter {
            std.debug.assert(!self.hit and !self.entry.ready and self.entry.filter == null);
            const a = self.entry.names.allocator();
            const owned = try a.dupe(local.sql_dynamic_filter.Column, columns);
            for (owned) |*column| column.name = try a.dupe(u8, column.name);
            self.entry.filter = try Filter.create(self.allocator(), owned, 64 * 1024);
            return self.entry.filter.?;
        }
        pub fn complete(self: *Lease) void {
            if (self.hit) return;
            self.hash().freezeShared();
            if (self.entry.filter) |value| {
                std.debug.assert(value.sealed and !value.failed);
            }
            self.cache.lock();
            defer self.cache.mutex.unlock();
            self.entry.ready = true;
        }
        pub fn release(self: *Lease) void {
            self.cache.lock();
            self.entry.refs -= 1;
            if (!self.entry.ready) {
                std.debug.assert(self.entry.refs == 0);
                // Capacity failures local to this build are repeatable. Shared
                // pressure and cancellation remain eligible for a later retry.
                if (self.entry.budget.isLocallyExhausted() or (self.entry.hash != null and self.entry.hash.?.budget.isLocallyExhausted())) {
                    self.cache.rejected[self.cache.rejected_hand] = self.entry.key;
                    self.cache.rejected_hand = (self.cache.rejected_hand + 1) % self.cache.rejected.len;
                }
                for (&self.cache.entries) |*slot| if (slot.* == self.entry) {
                    slot.* = null;
                    break;
                };
                self.cache.mutex.unlock();
                self.cache.destroy(self.entry);
            } else self.cache.mutex.unlock();
        }
    };
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn destroy(self: *Cache, entry: *Entry) void {
        if (entry.filter) |filter| filter.close();
        if (entry.hash) |hash| hash.deinit();
        entry.names.deinit();
        self.a.destroy(entry);
    }
    pub fn deinit(self: *Cache) void {
        for (&self.entries) |*slot| if (slot.*) |entry| {
            std.debug.assert(entry.refs == 0);
            slot.* = null;
            self.destroy(entry);
        };
        std.debug.assert(self.live == 0);
        self.rejected = @splat(null);
    }
    fn oldest(self: *Cache) ?usize {
        var index: ?usize = null;
        for (self.entries, 0..) |slot, i| if (slot) |entry| {
            if (entry.refs == 0 and (index == null or entry.used < self.entries[index.?].?.used)) index = i;
        };
        return index;
    }
    pub fn acquire(self: *Cache, key: [32]u8, task_bytes: usize, rows: usize) ?Lease {
        // Directory operations never perform source I/O. Brief contention
        // must not turn a completed shared build into a redundant rebuild.
        self.lock();
        for (self.rejected) |rejected| if (rejected) |digest| {
            if (std.mem.eql(u8, &digest, &key)) {
                self.mutex.unlock();
                return null;
            }
        };
        self.clock +%= 1;
        var free: ?usize = null;
        for (self.entries, 0..) |slot, index| {
            if (slot) |entry| {
                if (std.mem.eql(u8, &entry.key, &key)) {
                    if (!entry.ready or entry.budget.limit > task_bytes or entry.hash.?.buildKeys().len() > rows) {
                        self.mutex.unlock();
                        return null;
                    }
                    entry.refs += 1;
                    entry.used = self.clock;
                    self.hits +%= 1;
                    self.mutex.unlock();
                    return .{ .cache = self, .entry = entry, .hit = true };
                }
            } else free = index;
        }
        self.misses +%= 1;
        const limit = @min(task_bytes, self.bytes);
        const index = free orelse self.oldest() orelse {
            self.mutex.unlock();
            return null;
        };
        if (limit < 8192) {
            self.mutex.unlock();
            return null;
        }
        const old = self.entries[index];
        const entry = self.a.create(Entry) catch {
            self.mutex.unlock();
            return null;
        };
        entry.* = .{ .key = key, .budget = .{ .backing = self.sharedAllocator(), .limit = limit }, .names = undefined, .used = self.clock };
        entry.names = .init(entry.budget.allocator());
        self.entries[index] = entry;
        self.mutex.unlock();
        if (old) |value| self.destroy(value);
        entry.hash = Hash.create(entry.budget.allocator(), .{ .bytes = limit, .rows = rows }) catch {
            var lease: Lease = .{ .cache = self, .entry = entry, .hit = false };
            lease.release();
            return null;
        };
        return .{ .cache = self, .entry = entry, .hit = false };
    }
    fn sharedAllocator(self: *Cache) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = freeBytes } };
    }
    // Allocations evict only unpinned entries, outside the directory lock.
    // This bounds aggregate live bytes while letting one build use spare space.
    fn makeRoom(self: *Cache, growth: usize) bool {
        for (0..self.entries.len + 1) |_| {
            self.lock();
            if (growth <= self.bytes -| self.live) {
                self.mutex.unlock();
                return true;
            }
            const index = self.oldest() orelse {
                self.mutex.unlock();
                return false;
            };
            const victim = self.entries[index].?;
            self.entries[index] = null;
            self.mutex.unlock();
            self.destroy(victim);
        }
        return false;
    }
    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Cache = @ptrCast(@alignCast(raw));
        if (!self.makeRoom(len)) return null;
        self.lock();
        defer self.mutex.unlock();
        if (len > self.bytes -| self.live) return null;
        const result = self.a.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        self.peak = @max(self.peak, self.live);
        return result;
    }
    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Cache = @ptrCast(@alignCast(raw));
        const growth = len -| memory.len;
        if (!self.makeRoom(growth)) return false;
        self.lock();
        defer self.mutex.unlock();
        if (growth > self.bytes -| self.live or !self.a.rawResize(memory, alignment, len, ra)) return false;
        self.live = self.live - memory.len + len;
        self.peak = @max(self.peak, self.live);
        return true;
    }
    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Cache = @ptrCast(@alignCast(raw));
        const growth = len -| memory.len;
        if (!self.makeRoom(growth)) return null;
        self.lock();
        defer self.mutex.unlock();
        if (growth > self.bytes -| self.live) return null;
        const result = self.a.rawRemap(memory, alignment, len, ra) orelse return null;
        self.live = self.live - memory.len + len;
        self.peak = @max(self.peak, self.live);
        return result;
    }
    fn freeBytes(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Cache = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        self.a.rawFree(memory, alignment, ra);
        self.live -= memory.len;
    }
};

test "distributed SQL build residency isolates keys rejects busy leases and discards incomplete builds" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .a = a, .bytes = 1024 * 1024 };
    defer cache.deinit();
    var first = cache.acquire(@splat(1), 256 * 1024, 100).?;
    try first.hash().add(&.{local.sql_scalar.Datum.json(.{ .integer = 7 })}, &.{local.sql_scalar.Datum.json(.{ .integer = 7 })});
    try std.testing.expect(cache.acquire(@splat(1), 256 * 1024, 100) == null);
    first.complete();
    first.release();
    var hit = cache.acquire(@splat(1), 256 * 1024, 100).?;
    try std.testing.expect(hit.hit);
    var probe = try hit.hash().probe(&.{local.sql_scalar.Datum.json(.{ .integer = 7 })});
    try std.testing.expect((try probe.next()) != null);
    hit.release();
    var other = cache.acquire(@splat(2), 256 * 1024, 100).?;
    try std.testing.expect(!other.hit);
    other.complete();
    other.release();
    var retained = cache.acquire(@splat(1), 256 * 1024, 100).?;
    try std.testing.expect(retained.hit);
    retained.release();
    var failed = cache.acquire(@splat(3), 256 * 1024, 100).?;
    try std.testing.expectError(error.OutOfMemory, failed.allocator().alloc(u8, 256 * 1024 + 1));
    failed.release();
    try std.testing.expect(cache.acquire(@splat(3), 256 * 1024, 100) == null);
}

test "distributed SQL prepared builds share concurrent probes and sealed owned filters within aggregate residency" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .a = a, .bytes = 1024 * 1024 };
    defer cache.deinit();
    var build = cache.acquire(@splat(4), cache.bytes, 1000).?;
    // One entry can use more than a quarter of the worker's residency.
    const wide = try build.allocator().alloc(u8, cache.bytes / 2);
    build.allocator().free(wide);
    var name = [_]u8{ 'i', 'd' };
    const filter = try build.createFilter(&.{.{ .name = &name, .type = .integer }});
    for (0..50) |i| {
        const row = [_]local.sql_scalar.Datum{local.sql_scalar.Datum.json(.{ .integer = @intCast(i) })};
        try build.hash().add(&row, &row);
        try filter.add(&row);
    }
    filter.sealed = true;
    build.complete();
    build.release();
    @memset(&name, 'x');
    var leases = [_]Cache.Lease{ cache.acquire(@splat(4), cache.bytes, 1000).?, cache.acquire(@splat(4), cache.bytes, 1000).? };
    try std.testing.expect(leases[0].hit and leases[1].hit);
    try std.testing.expect(leases[0].hash() == leases[1].hash());
    try std.testing.expect(leases[0].filter() == leases[1].filter());
    try std.testing.expectEqualStrings("id", leases[0].filter().?.columns[0].name);
    try std.testing.expectError(error.InvalidSqlBackendResponse, leases[0].hash().markMatched(0));
    const Worker = struct {
        fn run(lease: *Cache.Lease, failure: *?anyerror) void {
            defer lease.release();
            exercise(lease) catch |err| {
                failure.* = err;
            };
        }
        fn exercise(lease: *Cache.Lease) !void {
            var region = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer region.deinit();
            for (0..1000) |i| {
                _ = region.reset(.free_all);
                const row = [_]local.sql_scalar.Datum{local.sql_scalar.Datum.json(.{ .integer = @intCast(i % 50) })};
                if (!try lease.filter().?.contains(&row)) return error.MissingFilterKey;
                const probes = try lease.hash().probeBatch(region.allocator(), &.{&row});
                const match = (try probes[0].next()) orelse return error.MissingMatch;
                const values = try match.materializeValues(region.allocator());
                if (values[0].value.integer != @as(i64, @intCast(i % 50))) return error.WrongMatch;
                if (try probes[0].next() != null) return error.DuplicateMatch;
            }
        }
    };
    var failures: [2]?anyerror = @splat(null);
    const first = try std.Thread.spawn(.{}, Worker.run, .{ &leases[0], &failures[0] });
    const second = try std.Thread.spawn(.{}, Worker.run, .{ &leases[1], &failures[1] });
    first.join();
    second.join();
    for (failures) |failure| if (failure) |err| return err;
    try std.testing.expect(cache.peak > cache.bytes / 4);
    try std.testing.expect(cache.peak <= cache.bytes);
    var verify = cache.acquire(@splat(4), cache.bytes, 1000).?;
    defer verify.release();
    try std.testing.expectEqual(@as(u64, 0), verify.hash().probes);
}

test "distributed SQL prepared residency pins active readers and retries transient shared pressure" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .a = a, .bytes = 256 * 1024 };
    defer cache.deinit();
    var pinned = cache.acquire(@splat(5), cache.bytes, 10).?;
    const bytes = try pinned.allocator().alloc(u8, 200 * 1024);
    pinned.complete();
    var competing = cache.acquire(@splat(6), cache.bytes, 10).?;
    try std.testing.expectError(error.OutOfMemory, competing.allocator().alloc(u8, 100 * 1024));
    competing.release();
    // The active index survived pressure and can be borrowed concurrently.
    var reader = cache.acquire(@splat(5), cache.bytes, 10).?;
    try std.testing.expect(reader.hit);
    try std.testing.expect(reader.hash() == pinned.hash());
    reader.release();
    pinned.allocator().free(bytes);
    pinned.release();
    var retry = cache.acquire(@splat(6), cache.bytes, 10).?;
    const admitted = try retry.allocator().alloc(u8, 100 * 1024);
    retry.allocator().free(admitted);
    retry.complete();
    retry.release();
    try std.testing.expect(cache.peak <= cache.bytes);
}

test "distributed SQL prepared residency evicts idle builds to admit a larger entry" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .a = a, .bytes = 256 * 1024 };
    defer cache.deinit();
    var old = cache.acquire(@splat(7), cache.bytes, 10).?;
    _ = try old.entry.names.allocator().alloc(u8, 160 * 1024);
    old.complete();
    old.release();
    var larger = cache.acquire(@splat(8), cache.bytes, 10).?;
    _ = try larger.entry.names.allocator().alloc(u8, 160 * 1024);
    larger.complete();
    larger.release();
    var evicted = cache.acquire(@splat(7), cache.bytes, 10).?;
    try std.testing.expect(!evicted.hit);
    evicted.release();
    try std.testing.expect(cache.peak <= cache.bytes);
}

test "distributed SQL prepared residency preserves in-place growth and exact remap accounting" {
    var storage: [64 * 1024]u8 = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&storage);
    var cache: Cache = .{ .a = backing.allocator(), .bytes = 32 * 1024 };
    defer cache.deinit();
    var build = cache.acquire(@splat(9), cache.bytes, 10).?;
    const bytes = try build.allocator().alloc(u8, 128);
    const before = cache.live;
    const grown = try build.allocator().realloc(bytes, 256);
    try std.testing.expect(grown.ptr == bytes.ptr);
    try std.testing.expectEqual(before + 128, cache.live);
    const shrunk = try build.allocator().realloc(grown, 64);
    try std.testing.expect(shrunk.ptr == grown.ptr);
    try std.testing.expectEqual(before - 64, cache.live);
    build.allocator().free(shrunk);
    build.complete();
    build.release();
    try std.testing.expect(cache.peak <= cache.bytes);
}
