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

//! Immutable parsed metadata and decoded vectors. Leases pin buffers across
//! eviction; all payload allocations belong to the cache, never a request.
const std = @import("std");
const Budget = @import("../../sql/memory_budget.zig");
const A = std.mem.Allocator;
pub const Payload = union(enum) {
    /// Layer-owned immutable metadata. The type-domain is included in the key;
    /// values and every referenced byte must belong to this item's arena.
    extension: *anyopaque,
    snapshot: @import("lake_iceberg_snapshot.zig").SnapshotWithDeletePlan,
    prepared: *@import("lake_prepared_deletes.zig").Prepared,
    page_directory: @import("lake_parquet_metadata.zig").PageDirectory,
    footer: @import("lake_parquet_metadata.zig").ParsedFooter,
    dictionary: @import("lake_parquet_page.zig").Dictionary,
    columns: []const @import("../../storage/rowsource/types.zig").ColumnVector,
};
pub const Item = struct {
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    payload: Payload = .{ .columns = &.{} },
    // Snapshot-local canonical row-ID ordering and totals, computed once.
    file_order: ?[]usize = null,
    file_rank: ?[]usize = null,
    file_by_id: std.StringHashMapUnmanaged(usize) = .empty,
    estimated_rows: u64 = 0,
    estimated_bytes: u64 = 0,
    refs: usize = 1,
    cached: bool = false,
    /// A decoded page pins its immutable chunk dictionary in this cache.
    dependency: ?*Item = null,
    touched: u64 = 0,
    fn destroy(self: *Item, cache: *Cache) void {
        const parent = self.dependency;
        const a = self.budget.backing;
        if (self.payload == .page_directory) self.payload.page_directory.deinit();
        if (self.payload == .prepared) self.payload.prepared.destroy(self.budget.allocator());
        if (self.payload == .snapshot) self.payload.snapshot.deinit(self.budget.allocator());
        self.file_by_id.deinit(self.budget.allocator());
        if (self.file_rank) |rank| self.budget.allocator().free(rank);
        if (self.file_order) |order| self.budget.allocator().free(order);
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        a.destroy(self);
        if (parent) |item| cache.releaseLocked(item);
    }
};
pub const Lease = struct {
    cache: *Cache,
    item: *Item,
    pub fn retain(self: Lease) Lease {
        self.cache.lock();
        defer self.cache.mutex.unlock();
        self.item.refs += 1;
        return self;
    }
    pub fn release(self: Lease) void {
        self.cache.lock();
        defer self.cache.mutex.unlock();
        self.cache.releaseLocked(self.item);
    }
};
pub const Cache = struct {
    a: A,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Item) = .empty,
    max_bytes: usize = 64 * 1024 * 1024,
    max_entries: usize = 512,
    bytes: usize = 0,
    tick: u64 = 0,
    hits: u64 = 0,
    flights: std.AutoHashMapUnmanaged([32]u8, *Flight) = .empty,
    loading_bytes: usize = 0,
    max_loading_bytes: usize = 64 * 1024 * 1024,
    max_loaders: usize = 16,
    exclusive_loading: bool = false,
    exclusive_waiters: usize = 0,
    const Flight = struct {
        refs: usize = 1,
        done: std.Io.Event = .unset,
        finished: std.atomic.Value(bool) = .init(false),
        result: ?Lease = null,
        failure: ?anyerror = null,
    };
    pub const Loader = struct { ptr: *anyopaque, load: *const fn (*anyopaque, *Item) anyerror!void };
    /// One immutable value per concurrent cold key, including values that
    /// cannot be admitted to residency. Every waiter owns its cancellation.
    /// Reservations bound active decode work independently of resident bytes.
    pub fn acquire(self: *Cache, key: [32]u8, limit: usize, context: @import("lake_read_context.zig").Context, loader: Loader) !Lease {
        if (limit > self.max_loading_bytes) return error.SqlMemoryLimitExceeded;
        var exclusive = false;
        var registered = false;
        defer if (registered) {
            self.lock();
            self.exclusive_waiters -= 1;
            self.mutex.unlock();
        };
        while (true) {
            try context.ensureActive();
            if (self.lookup(key)) |lease| return lease;
            self.lock();
            if (exclusive and !registered) {
                self.exclusive_waiters += 1;
                registered = true;
            }
            if (self.flights.get(key)) |flight| {
                flight.refs += 1;
                self.mutex.unlock();
                defer self.releaseFlight(flight);
                const io = context.io orelse return error.SqlMemoryLimitExceeded;
                while (!flight.finished.load(.acquire)) {
                    try context.ensureActive();
                    flight.done.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                        error.Timeout => continue,
                        else => return err,
                    };
                }
                try context.ensureActive();
                if (flight.result) |lease| return lease.retain();
                const failure = flight.failure.?;
                // A canceled leader is not authority to cancel another reader.
                if (failure == error.DecodeAdmissionBusy) {
                    exclusive = true;
                    continue;
                }
                // Consumer allocation/materialization policy is deliberately
                // absent from physical page keys. Retry with this reader's
                // loader instead of inheriting another reader's smaller cap.
                if (failure == error.Canceled or failure == error.DeadlineExceeded or failure == error.OutOfMemory or failure == error.SqlMemoryLimitExceeded or failure == error.ParquetRowGroupTooLarge) continue;
                return failure;
            }
            if (self.exclusive_loading or (!exclusive and self.exclusive_waiters != 0) or (exclusive and self.flights.count() != 0) or self.flights.count() >= self.max_loaders) {
                self.mutex.unlock();
                const io = context.io orelse return error.SqlMemoryLimitExceeded;
                try io.sleep(.fromMilliseconds(10), .awake);
                continue;
            }
            const flight = self.a.create(Flight) catch |err| {
                self.mutex.unlock();
                return err;
            };
            flight.* = .{};
            self.flights.put(self.a, key, flight) catch |err| {
                self.a.destroy(flight);
                self.mutex.unlock();
                return err;
            };
            if (exclusive) {
                self.exclusive_waiters -= 1;
                registered = false;
                self.exclusive_loading = true;
                self.loading_bytes += limit;
            }
            self.mutex.unlock();
            defer self.releaseFlight(flight);
            const result = self.loadValue(key, limit, context, loader, exclusive);
            self.lock();
            _ = self.flights.remove(key);
            if (exclusive) {
                self.loading_bytes -= limit;
                self.exclusive_loading = false;
            }
            if (result) |lease| flight.result = lease else |err| flight.failure = err;
            // Publication and event synchronization make payloads visible;
            // the flight pins even an uncached result until the final waiter.
            flight.finished.store(true, .release);
            if (context.io) |io| flight.done.set(io);
            self.mutex.unlock();
            if (result) |lease| return lease.retain() else |err| {
                if (err == error.DecodeAdmissionBusy) {
                    exclusive = true;
                    continue;
                }
                return err;
            }
        }
    }
    fn loadValue(self: *Cache, key: [32]u8, limit: usize, context: @import("lake_read_context.zig").Context, loader: Loader, exclusive: bool) !Lease {
        if (self.lookup(key)) |lease| return lease;
        const lease = try self.create(limit);
        errdefer lease.release();
        if (!exclusive) lease.item.budget.admission = .{ .ptr = self, .reserve = reserveLoading, .release = releaseLoading };
        defer lease.item.budget.finishAdmission();
        loader.load(loader.ptr, lease.item) catch |err| {
            if (lease.item.budget.admission_exhausted) return error.DecodeAdmissionBusy;
            return err;
        };
        try context.ensureActive();
        lease.item.budget.finishAdmission();
        self.publish(key, lease);
        return lease;
    }
    fn reserveLoading(raw: *anyopaque, bytes: usize) bool {
        const self: *Cache = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (bytes > self.max_loading_bytes -| self.loading_bytes) return false;
        self.loading_bytes += bytes;
        return true;
    }
    fn releaseLoading(raw: *anyopaque, bytes: usize) void {
        const self: *Cache = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(bytes <= self.loading_bytes);
        self.loading_bytes -= bytes;
    }
    fn releaseFlight(self: *Cache, flight: *Flight) void {
        self.lock();
        defer self.mutex.unlock();
        flight.refs -= 1;
        if (flight.refs == 0) {
            if (flight.result) |lease| self.releaseLocked(lease.item);
            self.a.destroy(flight);
        }
    }
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn releaseLocked(self: *Cache, item: *Item) void {
        std.debug.assert(item.refs != 0);
        item.refs -= 1;
        if (item.refs == 0 and !item.cached) item.destroy(self);
    }
    pub fn depend(self: *Cache, page: Lease, dictionary: Lease) void {
        std.debug.assert(page.cache == self and dictionary.cache == self);
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(page.item.dependency == null);
        dictionary.item.refs += 1;
        page.item.dependency = dictionary.item;
    }
    pub fn deinit(self: *Cache) void {
        // Remove leaf pages first; their release makes dictionaries evictable.
        while (self.entries.count() != 0) {
            var it = self.entries.iterator();
            const key = while (it.next()) |entry| {
                if (entry.value_ptr.*.refs == 0) break entry.key_ptr.*;
            } else unreachable; // All request leases must already be closed.
            const removed = self.entries.fetchRemove(key).?;
            removed.value.cached = false;
            removed.value.destroy(self);
        }
        self.entries.deinit(self.a);
        std.debug.assert(self.flights.count() == 0);
        self.flights.deinit(self.a);
    }
    pub fn lookup(self: *Cache, key: [32]u8) ?Lease {
        self.lock();
        defer self.mutex.unlock();
        const item = self.entries.get(key) orelse return null;
        self.tick +%= 1;
        item.touched = self.tick;
        item.refs += 1;
        self.hits += 1;
        return .{ .cache = self, .item = item };
    }
    pub fn create(self: *Cache, limit: usize) !Lease {
        const item = try self.a.create(Item);
        item.* = .{ .budget = .{ .backing = self.a, .limit = limit }, .arena = undefined };
        item.arena = std.heap.ArenaAllocator.init(item.budget.allocator());
        return .{ .cache = self, .item = item };
    }
    /// Admission is optional. A saturated cache returns an uncached lease,
    /// reclaimed immediately when the scan releases it.
    pub fn publish(self: *Cache, key: [32]u8, lease: Lease) void {
        self.lock();
        defer self.mutex.unlock();
        const item = lease.item;
        const size = item.budget.live;
        if (size > self.max_bytes or self.max_entries == 0 or self.entries.contains(key)) return;
        while (size > self.max_bytes -| self.bytes or self.entries.count() >= self.max_entries) {
            var oldest: ?[32]u8 = null;
            var tick: u64 = std.math.maxInt(u64);
            var it = self.entries.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.*.refs != 0) continue;
                if (oldest == null or entry.value_ptr.*.touched < tick) {
                    oldest = entry.key_ptr.*;
                    tick = entry.value_ptr.*.touched;
                }
            }
            const removed = self.entries.fetchRemove(oldest orelse return).?;
            self.bytes -= removed.value.budget.live;
            removed.value.cached = false;
            removed.value.destroy(self);
        }
        self.entries.put(self.a, key, item) catch return;
        self.tick +%= 1;
        item.touched = self.tick;
        item.cached = true;
        self.bytes += size;
    }
};

test "external lake decoded leases preserve pinned buffers under bounded eviction" {
    var cache: Cache = .{ .a = std.testing.allocator, .max_entries = 1 };
    defer cache.deinit();
    const first = try cache.create(8192);
    const bytes = try first.item.arena.allocator().dupe(u8, "pinned");
    first.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(1), first);
    const hit = cache.lookup(@splat(1)).?;
    const second = try cache.create(8192);
    second.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(2), second);
    try std.testing.expect(!second.item.cached);
    second.release();
    first.release();
    try std.testing.expectEqualStrings("pinned", bytes);
    hit.release();
    const replacement = try cache.create(8192);
    replacement.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(2), replacement);
    replacement.release();
    try std.testing.expect(cache.lookup(@splat(1)) == null);
}

test "external lake page dependencies pin a single dictionary across eviction" {
    var cache: Cache = .{ .a = std.testing.allocator, .max_entries = 2 };
    defer cache.deinit();
    const dictionary = try cache.create(8192);
    const a = dictionary.item.arena.allocator();
    const entries = try a.alloc([]u8, 1);
    entries[0] = try a.dupe(u8, "shared dictionary");
    dictionary.item.payload = .{ .dictionary = .{ .bytes = entries } };
    cache.publish(@splat(1), dictionary);
    const page = try cache.create(8192);
    page.item.payload = .{ .columns = &.{} };
    cache.depend(page, dictionary);
    cache.publish(@splat(2), page);
    dictionary.release();
    const pinned = cache.lookup(@splat(2)).?;
    page.release();
    const extra = try cache.create(8192);
    extra.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(3), extra);
    try std.testing.expect(!extra.item.cached);
    extra.release();
    try std.testing.expectEqualStrings("shared dictionary", pinned.item.dependency.?.payload.dictionary.bytes[0]);
    pinned.release();
    const replacement = try cache.create(8192);
    replacement.item.payload = .{ .columns = &.{} };
    cache.publish(@splat(3), replacement);
    replacement.release();
    try std.testing.expect(cache.entries.count() <= 2);
}

test "external lake decoded cold loads share uncached results and independently cancel waiters" {
    const io = std.testing.io;
    const Worker = struct {
        cache: *Cache,
        entered: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        calls: std.atomic.Value(usize) = .init(0),
        cancel: std.atomic.Value(bool) = .init(false),
        fn load(raw: *anyopaque, item: *Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            self.entered.set(std.testing.io);
            try self.gate.wait(std.testing.io);
            const value = try item.arena.allocator().create(u64);
            value.* = 42;
            item.payload = .{ .extension = value };
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cancel.load(.acquire)) return error.DeadlineExceeded;
        }
        fn run(self: *@This(), cancelable: bool) anyerror!Lease {
            return self.cache.acquire(@splat(7), 1024, .{ .io = std.testing.io, .checkpoint = if (cancelable) .{ .ptr = self, .check = check } else null }, .{ .ptr = self, .load = load });
        }
    };
    var cache: Cache = .{ .a = std.testing.allocator, .max_entries = 0 };
    defer cache.deinit();
    var worker: Worker = .{ .cache = &cache };
    var leader = try io.concurrent(Worker.run, .{ &worker, false });
    defer worker.gate.set(io);
    try worker.entered.wait(io);
    var waiter = try io.concurrent(Worker.run, .{ &worker, false });
    var canceled = try io.concurrent(Worker.run, .{ &worker, true });
    while (true) {
        cache.lock();
        const ready = cache.flights.get(@splat(7)).?.refs == 3;
        cache.mutex.unlock();
        if (ready) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    worker.cancel.store(true, .release);
    try std.testing.expectError(error.DeadlineExceeded, canceled.await(io));
    worker.gate.set(io);
    const first = try leader.await(io);
    defer first.release();
    const second = try waiter.await(io);
    defer second.release();
    try std.testing.expect(first.item == second.item);
    try std.testing.expectEqual(@as(usize, 1), worker.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.loading_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.flights.count());
    try std.testing.expect(!first.item.cached);
}

test "external lake distinct cold keys overlap full-limit loaders with measured admission" {
    const io = std.testing.io;
    const Worker = struct {
        cache: *Cache,
        key: [32]u8,
        entered: std.Io.Event = .unset,
        gate: *std.Io.Event,
        fn load(raw: *anyopaque, item: *Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const value = try item.arena.allocator().alloc(u8, 512);
            @memset(value, 42);
            item.payload = .{ .extension = @ptrCast(value.ptr) };
            self.entered.set(std.testing.io);
            try self.gate.wait(std.testing.io);
        }
        fn run(self: *@This()) anyerror!Lease {
            return self.cache.acquire(self.key, self.cache.max_loading_bytes, .{ .io = std.testing.io }, .{ .ptr = self, .load = load });
        }
    };
    var cache: Cache = .{ .a = std.testing.allocator, .max_loading_bytes = 4096, .max_entries = 0 };
    defer cache.deinit();
    var gate: std.Io.Event = .unset;
    var left: Worker = .{ .cache = &cache, .key = @splat(1), .gate = &gate };
    var right: Worker = .{ .cache = &cache, .key = @splat(2), .gate = &gate };
    var first = try io.concurrent(Worker.run, .{&left});
    defer {
        gate.set(io);
        const result = first.await(io) catch null;
        if (result) |lease| lease.release();
    }
    var second = try io.concurrent(Worker.run, .{&right});
    defer {
        gate.set(io);
        const result = second.await(io) catch null;
        if (result) |lease| lease.release();
    }
    try left.entered.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(2) } });
    try right.entered.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(2) } });
    cache.lock();
    const active = cache.flights.count();
    const bytes = cache.loading_bytes;
    cache.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 2), active);
    try std.testing.expect(bytes > 0 and bytes <= cache.max_loading_bytes);
}

test "external lake contending decoders unwind capacity and retry without holding each other" {
    const io = std.testing.io;
    const Worker = struct {
        cache: *Cache,
        key: [32]u8,
        entered: std.Io.Event = .unset,
        grow: *std.Io.Event,
        decoded: *std.Io.Event,
        finish: *std.Io.Event,
        calls: std.atomic.Value(usize) = .init(0),
        fn load(raw: *anyopaque, item: *Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            const temporary = item.budget.allocator();
            const initial = try temporary.alloc(u8, 256);
            defer temporary.free(initial);
            self.entered.set(std.testing.io);
            try self.grow.wait(std.testing.io);
            const value = try temporary.alloc(u8, 2048);
            defer temporary.free(value);
            @memset(value, 42);
            self.decoded.set(std.testing.io);
            try self.finish.wait(std.testing.io);
        }
        fn run(self: *@This()) anyerror!Lease {
            return self.cache.acquire(self.key, 4096, .{ .io = std.testing.io }, .{ .ptr = self, .load = load });
        }
    };
    var cache: Cache = .{ .a = std.testing.allocator, .max_loading_bytes = 4096, .max_entries = 0 };
    defer cache.deinit();
    var grow: std.Io.Event = .unset;
    var decoded: std.Io.Event = .unset;
    var finish: std.Io.Event = .unset;
    var left: Worker = .{ .cache = &cache, .key = @splat(3), .grow = &grow, .decoded = &decoded, .finish = &finish };
    var right: Worker = .{ .cache = &cache, .key = @splat(4), .grow = &grow, .decoded = &decoded, .finish = &finish };
    var first = try io.concurrent(Worker.run, .{&left});
    var first_pending = true;
    defer if (first_pending) {
        grow.set(io);
        finish.set(io);
        const result = first.await(io) catch null;
        if (result) |lease| lease.release();
    };
    var second = try io.concurrent(Worker.run, .{&right});
    var second_pending = true;
    defer if (second_pending) {
        grow.set(io);
        finish.set(io);
        const result = second.await(io) catch null;
        if (result) |lease| lease.release();
    };
    try left.entered.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(2) } });
    try right.entered.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(2) } });
    grow.set(io);
    try decoded.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(2) } });
    var waiting = false;
    for (0..2000) |_| {
        cache.lock();
        waiting = cache.exclusive_waiters != 0;
        const bounded = cache.loading_bytes <= cache.max_loading_bytes;
        cache.mutex.unlock();
        try std.testing.expect(bounded);
        if (waiting) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(waiting);
    finish.set(io);
    first_pending = false;
    const first_result = try first.await(io);
    defer first_result.release();
    second_pending = false;
    const second_result = try second.await(io);
    defer second_result.release();
    try std.testing.expectEqual(@as(usize, 3), left.calls.load(.acquire) + right.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.loading_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.exclusive_waiters);
}

test "external lake shared decode failure does not impose a leader's smaller consumer budget" {
    const io = std.testing.io;
    const Worker = struct {
        cache: *Cache,
        entered: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        fn load(raw: *anyopaque, item: *Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.entered.set(std.testing.io);
            try self.gate.wait(std.testing.io);
            const value = try item.arena.allocator().alloc(u8, 512);
            @memset(value, 42);
            item.payload = .{ .extension = @ptrCast(value.ptr) };
        }
        fn run(self: *@This(), limit: usize) anyerror!Lease {
            return self.cache.acquire(@splat(5), limit, .{ .io = std.testing.io }, .{ .ptr = self, .load = load });
        }
    };
    var cache: Cache = .{ .a = std.testing.allocator, .max_entries = 0 };
    defer cache.deinit();
    var worker: Worker = .{ .cache = &cache };
    var leader = try io.concurrent(Worker.run, .{ &worker, 128 });
    var leader_pending = true;
    defer if (leader_pending) {
        worker.gate.set(io);
        const result = leader.await(io) catch null;
        if (result) |lease| lease.release();
    };
    try worker.entered.wait(io);
    var waiter = try io.concurrent(Worker.run, .{ &worker, 4096 });
    var waiter_pending = true;
    defer if (waiter_pending) {
        worker.gate.set(io);
        const result = waiter.await(io) catch null;
        if (result) |lease| lease.release();
    };
    var joined = false;
    for (0..2000) |_| {
        cache.lock();
        joined = cache.flights.get(@splat(5)).?.refs == 2;
        cache.mutex.unlock();
        if (joined) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(joined);
    worker.gate.set(io);
    leader_pending = false;
    try std.testing.expectError(error.OutOfMemory, leader.await(io));
    waiter_pending = false;
    const result = try waiter.await(io);
    defer result.release();
    try std.testing.expectEqual(@as(u8, 42), @as(*const u8, @ptrCast(result.item.payload.extension)).*);
    try std.testing.expectEqual(@as(usize, 0), cache.loading_bytes);
}
