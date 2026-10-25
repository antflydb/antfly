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

//! Process-local immutable read snapshots. Handles are random bearer
//! capabilities, never content hashes shared implicitly across callers. This
//! store does not execute models, retain source credentials, or resume jobs.
const std = @import("std");
const A = std.mem.Allocator;
/// Reserved by the reader before processing sources. A 64-byte opaque handle
/// and a u64 TTL fit within this JSON decoration allowance.
pub const response_overhead: usize = 128;
/// Retention adds only the handle to the response. Keep structured numeric
/// tokens exact and leave the immutable stored reading untouched.
pub fn responseWithHandle(work: A, output: A, result: []const u8, handle: []const u8, ttl_ms: u64) ![]u8 {
    const envelope = try std.json.parseFromSliceLeaky(std.json.Value, work, result, .{ .parse_numbers = false });
    if (envelope != .object) return error.InvalidDocumentArtifact;
    const document = envelope.object.getPtr("document") orelse return error.InvalidDocumentArtifact;
    if (document.* != .object) return error.InvalidDocumentArtifact;
    try document.object.put(work, "artifact_id", .{ .string = handle });
    try document.object.put(work, "artifact_ttl_ms", .{ .integer = @intCast(ttl_ms) });
    return @import("output.zig").valueAlloc(output, envelope, std.math.add(usize, result.len, response_overhead) catch return error.DocumentOutputTooLarge);
}

pub const Charge = struct {
    ptr: ?*anyopaque = null,
    release_fn: ?*const fn (?*anyopaque) void = null,
    fn release(self: Charge) void {
        if (self.release_fn) |f| f(self.ptr);
    }
};
const Payload = struct {
    allocator: A,
    bytes: []u8,
    charge: Charge,
    references: std.atomic.Value(usize) = .init(1),
    fn release(self: *Payload) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const a = self.allocator;
        a.free(self.bytes);
        self.charge.release();
        a.destroy(self);
    }
};
/// Pins immutable bytes independently of cache membership. Readers release
/// the lock before allocation or model work; eviction cannot invalidate them.
pub const Snapshot = struct {
    owner: *Payload,
    pub fn bytes(self: Snapshot) []const u8 {
        return self.owner.bytes;
    }
    pub fn release(self: *Snapshot) void {
        self.owner.release();
        self.* = undefined;
    }
};
pub const retention_overhead = @sizeOf(Payload);
const Entry = struct { key: [64]u8, owner: *Payload, expires_ns: u64 };
pub const Store = struct {
    entries: std.ArrayList(Entry) = .empty,
    bytes: usize = 0,
    max_bytes: usize = 64 * 1024 * 1024,
    max_entries: usize = 128,
    mutex: std.atomic.Mutex = .unlocked,
    maintenance_io: ?std.Io = null,
    owned_io: ?*std.Io.Threaded = null,
    maintenance: std.Io.Group = .init,
    changed: std.Io.Event = .unset,
    stopping: bool = false,
    now_fn: ?*const fn () u64 = null,
    expired: [128][64]u8 = undefined,
    expired_len: usize = 0,
    expired_cursor: usize = 0,

    /// One lazy timer per store, using the host's pool when available. The
    /// fallback pool is stable and owned by the store, never by a request.
    pub fn startMaintenance(self: *Store, a: A, supplied: ?std.Io, now: *const fn () u64) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.maintenance_io != null) return;
        var owned: ?*std.Io.Threaded = null;
        const io = supplied orelse blk: {
            const pool = try a.create(std.Io.Threaded);
            pool.* = std.Io.Threaded.init(a, .{});
            owned = pool;
            break :blk pool.io();
        };
        errdefer if (owned) |pool| {
            pool.deinit();
            a.destroy(pool);
        };
        self.now_fn = now;
        try self.maintenance.concurrent(io, expireLoop, .{ self, a, io });
        self.owned_io = owned;
        self.maintenance_io = io;
    }
    fn expireLoop(self: *Store, a: A, io: std.Io) std.Io.Cancelable!void {
        while (true) {
            self.lock();
            if (self.stopping) {
                self.mutex.unlock();
                return;
            }
            const now = self.now_fn.?();
            self.pruneLocked(a, now);
            var wait_ns: u64 = 30 * std.time.ns_per_s;
            for (self.entries.items) |entry| wait_ns = @min(wait_ns, entry.expires_ns -| now);
            self.changed.reset();
            self.mutex.unlock();
            self.changed.waitTimeout(io, .{ .duration = .{ .raw = .fromNanoseconds(@intCast(wait_ns)), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => return error.Canceled,
            };
        }
    }
    fn pruneLocked(self: *Store, a: A, now: u64) void {
        var index: usize = 0;
        while (index < self.entries.items.len) {
            if (now >= self.entries.items[index].expires_ns) {
                self.expire(a, index);
            } else index += 1;
        }
    }
    fn expire(self: *Store, a: A, index: usize) void {
        self.expired[self.expired_cursor] = self.entries.items[index].key;
        self.expired_cursor = (self.expired_cursor + 1) % self.expired.len;
        self.expired_len = @min(self.expired.len, self.expired_len + 1);
        self.remove(a, index);
    }
    pub fn pruneExpired(self: *Store, a: A, now: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.pruneLocked(a, now);
    }
    /// Evict one oldest retained reading before retrying failed admission.
    /// Bounded work and no model manager calls while holding a manager lock.
    pub fn evictOldest(self: *Store, a: A) bool {
        self.lock();
        defer self.mutex.unlock();
        if (self.entries.items.len == 0) return false;
        self.remove(a, 0);
        return true;
    }

    fn lock(self: *Store) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn remove(self: *Store, a: A, i: usize) void {
        const entry = self.entries.orderedRemove(i);
        _ = a;
        self.bytes -= entry.owner.bytes.len;
        entry.owner.release();
    }
    /// Charge ownership transfers only when true is returned.
    pub fn put(self: *Store, a: A, key: [64]u8, payload: []const u8, expires_ns: u64, charge: Charge) !bool {
        if (payload.len > self.max_bytes or self.max_entries == 0) return false;
        self.lock();
        defer self.mutex.unlock();
        if (self.now_fn) |now| self.pruneLocked(a, now());
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &key)) return false;
        while (self.entries.items.len > 0 and (self.entries.items.len >= self.max_entries or payload.len > self.max_bytes -| self.bytes)) self.remove(a, 0);
        const copy = try a.dupe(u8, payload);
        errdefer a.free(copy);
        const owner = try a.create(Payload);
        errdefer a.destroy(owner);
        owner.* = .{ .allocator = a, .bytes = copy, .charge = charge };
        try self.entries.append(a, .{ .key = key, .owner = owner, .expires_ns = expires_ns });
        self.bytes += copy.len;
        if (self.maintenance_io) |io| self.changed.set(io);
        return true;
    }
    pub fn acquire(self: *Store, store_allocator: A, key: []const u8, now_ns: u64) !Snapshot {
        if (key.len != 64) return error.InvalidDocumentArtifact;
        for (key) |b| if (!std.ascii.isHex(b)) return error.InvalidDocumentArtifact;
        self.lock();
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, i| if (std.mem.eql(u8, &entry.key, key)) {
            if (now_ns >= entry.expires_ns) {
                self.expire(store_allocator, i);
                return error.DocumentArtifactExpired;
            }
            _ = entry.owner.references.fetchAdd(1, .monotonic);
            return .{ .owner = entry.owner };
        };
        for (self.expired[0..self.expired_len]) |expired_key| if (std.mem.eql(u8, &expired_key, key)) return error.DocumentArtifactExpired;
        return error.DocumentArtifactNotFound;
    }
    pub fn get(self: *Store, store_allocator: A, output: A, key: []const u8, now_ns: u64) ![]u8 {
        var snapshot = try self.acquire(store_allocator, key, now_ns);
        defer snapshot.release();
        return output.dupe(u8, snapshot.bytes());
    }
    pub fn deinit(self: *Store, a: A) void {
        self.lock();
        self.stopping = true;
        self.mutex.unlock();
        if (self.maintenance_io) |io| {
            self.changed.set(io);
            self.maintenance.cancel(io);
        }
        if (self.owned_io) |pool| {
            pool.deinit();
            a.destroy(pool);
        }
        while (self.entries.items.len > 0) self.remove(a, self.entries.items.len - 1);
        self.entries.deinit(a);
    }
};

test "document artifact retention is bounded and expired handles release their charge" {
    const Fake = struct {
        released: usize = 0,
        fn release(raw: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.released += 1;
        }
    };
    const a = std.testing.allocator;
    var store = Store{ .max_entries = 1, .max_bytes = 8 };
    defer store.deinit(a);
    var fake = Fake{};
    const charge = Charge{ .ptr = &fake, .release_fn = Fake.release };
    try std.testing.expect(try store.put(a, @splat('a'), "first", 10, charge));
    try std.testing.expect(try store.put(a, @splat('b'), "second", 20, charge));
    try std.testing.expectEqual(@as(usize, 1), fake.released);
    try std.testing.expectError(error.DocumentArtifactNotFound, store.get(a, a, &@as([64]u8, @splat('a')), 0));
    const bytes = try store.get(a, a, &@as([64]u8, @splat('b')), 19);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("second", bytes);
    try std.testing.expectError(error.DocumentArtifactExpired, store.get(a, a, &@as([64]u8, @splat('b')), 20));
    try std.testing.expectEqual(@as(usize, 2), fake.released);
    try std.testing.expectEqual(@as(usize, 0), store.bytes);
}

test "document artifact maintenance expires unread handles and wakes promptly on shutdown" {
    const Fixture = struct {
        released: std.atomic.Value(usize) = .init(0),
        fn release(raw: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            _ = self.released.fetchAdd(1, .release);
        }
        fn now() u64 {
            return @intCast(std.Io.Clock.awake.now(std.testing.io).nanoseconds);
        }
    };
    const a = std.testing.allocator;
    var store = Store{};
    var live = true;
    defer if (live) store.deinit(a);
    var fixture = Fixture{};
    try store.startMaintenance(a, std.testing.io, Fixture.now);
    try store.startMaintenance(a, std.testing.io, Fixture.now);
    try std.testing.expect(try store.put(a, @splat('a'), "unread", Fixture.now() + 5 * std.time.ns_per_ms, .{ .ptr = &fixture, .release_fn = Fixture.release }));
    const deadline = Fixture.now() + std.time.ns_per_s;
    while (fixture.released.load(.acquire) == 0 and Fixture.now() < deadline) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expectEqual(@as(usize, 1), fixture.released.load(.acquire));
    try std.testing.expectError(error.DocumentArtifactExpired, store.get(a, a, &@as([64]u8, @splat('a')), Fixture.now()));
    const start = Fixture.now();
    store.deinit(a);
    live = false;
    try std.testing.expect(Fixture.now() - start < std.time.ns_per_s);
}

test "document artifact pressure eviction releases oldest charge before replacement allocation" {
    const Fixture = struct {
        released: usize = 0,
        fn release(raw: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.released += 1;
        }
    };
    const a = std.testing.allocator;
    var store = Store{};
    defer store.deinit(a);
    var fixture = Fixture{};
    const charge = Charge{ .ptr = &fixture, .release_fn = Fixture.release };
    try std.testing.expect(try store.put(a, @splat('a'), "old", 100, charge));
    try std.testing.expect(try store.put(a, @splat('b'), "new", 200, charge));
    try std.testing.expect(store.evictOldest(a));
    try std.testing.expectEqual(@as(usize, 1), fixture.released);
    try std.testing.expectError(error.DocumentArtifactNotFound, store.get(a, a, &@as([64]u8, @splat('a')), 0));
    store.pruneExpired(a, 200);
    try std.testing.expectEqual(@as(usize, 2), fixture.released);
    try std.testing.expect(!store.evictOldest(a));
}

test "document artifact snapshots survive eviction and allocation callbacks can reclaim the cache" {
    const a = std.testing.allocator;
    var store = Store{};
    defer store.deinit(a);
    try std.testing.expect(try store.put(a, @splat('a'), "immutable", 100, .{}));
    var pinned = try store.acquire(a, &@as([64]u8, @splat('a')), 0);
    defer pinned.release();
    const Reclaiming = struct {
        store: *Store,
        reclaimed: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reclaimed = self.store.evictOldest(std.testing.allocator);
            self.store.pruneExpired(std.testing.allocator, 100);
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
    };
    var reclaiming = Reclaiming{ .store = &store };
    const output = A{ .ptr = &reclaiming, .vtable = &.{ .alloc = Reclaiming.alloc, .free = Reclaiming.free, .resize = Reclaiming.resize, .remap = Reclaiming.remap } };
    const copy = try store.get(a, output, &@as([64]u8, @splat('a')), 0);
    defer output.free(copy);
    try std.testing.expect(reclaiming.reclaimed);
    try std.testing.expectEqualStrings("immutable", copy);
    try std.testing.expectEqualStrings("immutable", pinned.bytes());
    try std.testing.expectError(error.DocumentArtifactNotFound, store.acquire(a, &@as([64]u8, @splat('a')), 0));
}

test "document retained responses preserve exact structured numeric tokens" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const original = "{\"data\":[{\"fields\":{\"amount\":9007199254740993.01}}],\"document\":{\"status\":\"complete\"}}";
    const response = try responseWithHandle(arena.allocator(), a, original, "handle", 300000);
    defer a.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "9007199254740993.01") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "\"artifact_id\":\"handle\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, original, "artifact_id") == null);
}

test "document artifact response fits its reserved bounded decoration allowance" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const original = "{\"data\":[],\"document\":{\"status\":\"complete\"}}";
    const handle: [64]u8 = @splat('a');
    const decorated = try responseWithHandle(arena.allocator(), a, original, &handle, std.math.maxInt(i64));
    defer a.free(decorated);
    try std.testing.expect(decorated.len <= original.len + response_overhead);
}
