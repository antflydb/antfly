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

//! Immutable normalized accepted cuts. Every admission validates the live WAL
//! floor/head, then shares final row images and the publication-specific physical
//! replacement set. No request capability or provider client survives the build.
const std = @import("std");
const local = @import("antfly_local_sources");
const ingestion = @import("../serverless/lake_ingestion.zig");
const wal = @import("../serverless/lake_wal.zig");
const Overlay = @import("lake_search_overlay.zig").Overlay;
const Set = @import("lake_index_physical_set.zig").Set;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
pub const MaskBuilder = struct { ptr: *anyopaque, build: *const fn (*anyopaque, A) anyerror!Set };
const Owner = struct {
    references: std.atomic.Value(usize) = .init(1),
    budget: local.sql_memory_budget,
    arena: std.heap.ArenaAllocator = undefined,
    masks: std.heap.ArenaAllocator = undefined,
    overlay: Overlay = undefined,
    mutex: std.atomic.Mutex = .unlocked,
    physical: ?Set = null,
    head: ?wal.Head = null,
    parent: ?*Owner = null,
    depth: u8 = 0,
    fn release(self: *Owner) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        self.masks.deinit();
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        const parent = self.parent;
        std.heap.page_allocator.destroy(self);
        if (parent) |owner| owner.release();
    }
};
pub const Pin = struct {
    owner: *Owner,
    pub fn deinit(self: Pin) void {
        self.owner.release();
    }
    pub fn overlay(self: Pin) Overlay {
        return self.owner.overlay;
    }
    /// One private build per immutable publication cut. Waiters check their own
    /// deadline, and failures leave no partially published mask or scratch.
    pub fn physical(self: Pin, builder: MaskBuilder, context: Context) !?*const Set {
        const owner = self.owner;
        while (!owner.mutex.tryLock()) {
            try context.ensureActive();
            if (context.io) |io| try io.sleep(.fromMilliseconds(1), .awake) else return null;
        }
        defer owner.mutex.unlock();
        try context.ensureActive();
        if (owner.physical == null) {
            owner.physical = builder.build(builder.ptr, owner.masks.allocator()) catch |err| {
                _ = owner.masks.reset(.free_all);
                if (err == error.OutOfMemory) return null;
                return err;
            };
        }
        return &owner.physical.?;
    }
};
pub const Cache = struct {
    const Entry = struct { identity: [32]u8, authority: [32]u8, owner: ?*Owner = null, used: u64 };
    mutex: std.atomic.Mutex = .unlocked,
    entries: [4]?Entry = @splat(null),
    tick: u64 = 0,
    builds: usize = 0,
    heap: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 256 * 1024 * 1024 },
    managed: ?local.storage_resource_manager.BudgetedAllocator = null,
    pub fn attach(self: *Cache, manager: *local.storage_resource_manager.ResourceManager) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.managed != null) return;
        std.debug.assert(self.heap.live == 0);
        self.managed = local.storage_resource_manager.BudgetedAllocator.init(manager, .full_text_segment_residency, @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), 1);
        self.heap.backing = self.managed.?.allocator();
    }
    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    pub fn deinit(self: *Cache) void {
        for (&self.entries) |*slot| if (slot.*) |entry| {
            std.debug.assert(entry.owner != null);
            entry.owner.?.release();
            slot.* = null;
        };
        std.debug.assert(self.heap.live == 0);
        if (self.managed) |*budget| budget.deinit();
    }
    pub fn key(authority: [32]u8, cut: u64, head: ?wal.Head) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&authority);
        var values: [16]u8 = undefined;
        std.mem.writeInt(u64, values[0..8], cut, .little);
        std.mem.writeInt(u64, values[8..16], if (head) |tail| tail.lsn else cut, .little);
        hash.update(&values);
        if (head) |tail| hash.update(tail.key);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn acquire(self: *Cache, authority: [32]u8, store: wal.Store, cut: u64, context: Context) !Pin {
        var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer scratch.deinit();
        const head = try store.pin(scratch.allocator(), cut);
        const identity = key(authority, cut, head);
        while (true) {
            try context.ensureActive();
            self.lock();
            self.tick +%= 1;
            var waiting = false;
            for (&self.entries) |*slot| if (slot.*) |*entry| {
                if (std.mem.eql(u8, &entry.identity, &identity)) {
                    if (entry.owner) |owner| {
                        _ = owner.references.fetchAdd(1, .monotonic);
                        entry.used = self.tick;
                        self.mutex.unlock();
                        return .{ .owner = owner };
                    }
                    waiting = true;
                    break;
                }
            };
            if (waiting) {
                self.mutex.unlock();
                if (context.io) |io| {
                    try io.sleep(.fromMilliseconds(1), .awake);
                    continue;
                }
                return self.build(self.heap.backing, store, cut, head, null);
            }
            var target: ?usize = null;
            var oldest: u64 = std.math.maxInt(u64);
            for (self.entries, 0..) |entry, i| {
                if (entry == null) {
                    target = i;
                    break;
                }
                if (entry.?.owner != null and entry.?.used <= oldest) {
                    target = i;
                    oldest = entry.?.used;
                }
            }
            const index = target orelse {
                self.mutex.unlock();
                return self.build(self.heap.backing, store, cut, head, null);
            };
            var previous: ?*Owner = null;
            if (head) |tail| for (self.entries) |slot| if (slot) |entry| if (entry.owner) |owner| {
                if (std.mem.eql(u8, &entry.authority, &authority) and owner.overlay.pending.base_lsn == cut and owner.head != null and owner.depth < 8 and owner.head.?.lsn < tail.lsn and (previous == null or previous.?.head.?.lsn < owner.head.?.lsn)) previous = owner;
            };
            if (previous) |owner| _ = owner.references.fetchAdd(1, .monotonic);
            defer if (previous) |owner| owner.release();
            const retired = self.entries[index];
            self.entries[index] = .{ .identity = identity, .authority = authority, .used = self.tick };
            self.builds += 1;
            self.mutex.unlock();
            if (retired) |entry| entry.owner.?.release();
            const pin = self.build(self.heap.allocator(), store, cut, head, previous) catch |err| {
                self.lock();
                self.entries[index] = null;
                self.mutex.unlock();
                if (err == error.OutOfMemory) return self.build(self.heap.backing, store, cut, head, previous);
                return err;
            };
            self.lock();
            self.entries[index].?.owner = pin.owner;
            _ = pin.owner.references.fetchAdd(1, .monotonic);
            self.mutex.unlock();
            return pin;
        }
    }
    fn build(_: *Cache, backing: A, store: wal.Store, cut: u64, head: ?wal.Head, previous: ?*Owner) !Pin {
        const owner = try std.heap.page_allocator.create(Owner);
        owner.* = .{ .budget = .{ .backing = backing, .limit = 128 * 1024 * 1024 } };
        owner.arena = .init(owner.budget.allocator());
        owner.masks = .init(owner.budget.allocator());
        errdefer owner.release();
        populate(owner, store, cut, head, previous) catch |err| {
            if (err == error.OutOfMemory and owner.budget.isExhausted()) return error.LakeOverlayTooLarge;
            return err;
        };
        return .{ .owner = owner };
    }
    fn populate(owner: *Owner, store: wal.Store, cut: u64, head: ?wal.Head, previous: ?*Owner) !void {
        const a = owner.arena.allocator();
        const extension = if (previous != null and head != null) try store.rangeAfterHead(a, previous.?.head.?, head.?) else null;
        if (previous != null and extension == null) _ = owner.arena.reset(.free_all);
        const suffix = extension orelse try store.rangePinned(a, cut, head);
        if (extension != null) {
            owner.parent = previous;
            owner.depth = previous.?.depth + 1;
            _ = previous.?.references.fetchAdd(1, .monotonic);
        }
        owner.head = if (head) |tail| .{ .lsn = tail.lsn, .key = try a.dupe(u8, tail.key) } else null;
        owner.overlay = try Overlay.init(a, try ingestion.normalizePendingWithBase(a, cut, suffix.lsn, if (extension != null) previous.?.overlay.pending else null, suffix.records));
    }
};

test "external lake bounded WAL cuts share normalization masks and immutable chain authority" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    const store: wal.Store = .{ .client = client, .bucket = "bucket", .prefix = "queue", .context = .{ .io = io } };
    const first = "{\"batch_id\":\"one\",\"source\":\"test\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1,\"body\":\"old\"}}]}";
    const second = "{\"batch_id\":\"two\",\"source\":\"test\",\"epoch\":\"1\",\"checkpoint\":\"2\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":1}},{\"op\":\"upsert\",\"row\":{\"id\":2,\"body\":\"new\"}}]}";
    _ = try store.append(sa, "one", first, 0, 0);
    var cache: Cache = .{};
    defer cache.deinit();
    var one = try cache.acquire(@splat(1), store, 0, .{ .io = io });
    defer one.deinit();
    const before = provider.gets;
    var same = try cache.acquire(@splat(1), store, 0, .{ .io = io });
    defer same.deinit();
    try std.testing.expectEqual(one.owner, same.owner);
    try std.testing.expectEqual(@as(usize, 2), provider.gets - before); // floor + tail only
    const Mask = struct {
        builds: usize = 0,
        fn build(raw: *anyopaque, alloc: A) !Set {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.builds += 1;
            var set = Set.init(alloc);
            try set.addRow("file", 7, 1 << 40);
            return set;
        }
    };
    var mask: Mask = .{};
    const physical = (try one.physical(.{ .ptr = &mask, .build = Mask.build }, .{ .io = io })).?;
    try std.testing.expectEqual(physical, (try same.physical(.{ .ptr = &mask, .build = Mask.build }, .{ .io = io })).?);
    try std.testing.expectEqual(@as(usize, 1), mask.builds);
    _ = try store.append(sa, "two", second, 1, 0);
    const before_extend = provider.gets;
    var newer = try cache.acquire(@splat(1), store, 0, .{ .io = io });
    defer newer.deinit();
    try std.testing.expect(one.owner != newer.owner);
    try std.testing.expectEqual(@as(usize, 4), provider.gets - before_extend); // floor, tail, one new record/payload
    try std.testing.expect(newer.owner.parent == one.owner);
    try std.testing.expectEqual(@as(usize, 1), one.overlay().rows.count());
    try std.testing.expectEqual(@as(u64, 2), newer.overlay().pending.lsn);
    try std.testing.expectEqual(@as(usize, 1), newer.overlay().rows.count());
    try std.testing.expectEqual(@as(usize, 2), newer.overlay().pending.changes.len);
    for (0..8) |i| {
        var authority: [32]u8 = @splat(0);
        authority[0] = @intCast(i + 2);
        const pin = try cache.acquire(authority, store, 0, .{ .io = io });
        pin.deinit();
    }
    try std.testing.expect(physical.contains("file", 7, 1 << 40)); // pinned eviction
    const prior_restore = try cache.acquire(@splat(1), store, 0, .{ .io = io });
    defer prior_restore.deinit();
    // Simulate a restored queue with a different chain at larger LSNs. An old
    // ordinal prefix is not ancestry evidence, even under the same authority.
    const fork: wal.Store = .{ .client = client, .bucket = "bucket", .prefix = "fork", .context = .{ .io = io } };
    _ = try fork.append(sa, "fork-one", second, 0, 0);
    _ = try fork.append(sa, "fork-two", first, 1, 0);
    _ = try fork.append(sa, "fork-three", second, 2, 0);
    const restored = try cache.acquire(@splat(1), fork, 0, .{ .io = io });
    defer restored.deinit();
    try std.testing.expect(restored.owner.parent == null);
    try std.testing.expectEqual(@as(u64, 3), restored.overlay().pending.lsn);
    var pressure: Cache = .{};
    defer pressure.deinit();
    pressure.heap.limit = 1;
    const private = try pressure.acquire(@splat(1), store, 0, .{ .io = io });
    defer private.deinit();
    try std.testing.expectEqual(@as(usize, 0), pressure.heap.live);
    try std.testing.expectEqual(@as(u64, 2), private.overlay().pending.lsn);
    const Worker = struct {
        fn acquire(shared: *Cache, queue: wal.Store, context: Context) !Pin {
            return shared.acquire(@splat(99), queue, 0, context);
        }
    };
    var first_flight = try io.concurrent(Worker.acquire, .{ &cache, store, Context{ .io = io } });
    var first_joined = false;
    defer if (!first_joined) {
        if (first_flight.cancel(io)) |pin| pin.deinit() else |_| {}
    };
    var second_flight = try io.concurrent(Worker.acquire, .{ &cache, store, Context{ .io = io } });
    var second_joined = false;
    defer if (!second_joined) {
        if (second_flight.cancel(io)) |pin| pin.deinit() else |_| {}
    };
    first_joined = true;
    const first_pin = try first_flight.await(io);
    defer first_pin.deinit();
    second_joined = true;
    const second_pin = try second_flight.await(io);
    defer second_pin.deinit();
    try std.testing.expect(first_pin.owner == second_pin.owner);
    var floor = try client.putObject("bucket", "queue/gc-floor", "1", .{});
    floor.deinit(a);
    try std.testing.expectError(error.LakeOverlayCoverageUnavailable, cache.acquire(@splat(1), store, 0, .{ .io = io }));
    try std.testing.expectError(error.DeadlineExceeded, same.physical(.{ .ptr = &mask, .build = Mask.build }, .{ .deadline_ns = 0 }));
}
