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

//! Immutable query profiles and canonical pin membership. Entries are keyed by
//! exact profile identity and native head lineage, never by mutable revision
//! alone. Transactions and authorization grants do not escape into this cache.
const std = @import("std");
const builtin = @import("builtin");
const resolver = @import("antfly_resolver");
const A = std.mem.Allocator;
const heap = if (builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64) std.heap.wasm_allocator else if (builtin.single_threaded) std.heap.page_allocator else std.heap.smp_allocator;
pub const Graph = struct {
    aliases: []const resolver.KeyAlias,
    native_aliases: []const resolver.KeyAlias = &.{},
    removed: []const []const u8 = &.{},
    pairs: []const resolver.curation.Pair = &.{},
    composed: bool = false,
};
pub const Loader = struct { ptr: *anyopaque, load: *const fn (*anyopaque, A) anyerror!Graph, estimated_bytes: ?usize = null };
pub const Context = struct {
    references: std.atomic.Value(usize) = .init(1),
    arena: std.heap.ArenaAllocator,
    key: [32]u8,
    composed: bool,
    previous: ?*Context = null,
    next: ?*Context = null,
    static_reverse: std.StringHashMapUnmanaged(std.ArrayList(resolver.KeyAlias)) = .empty,
    static_targets: std.StringHashMapUnmanaged([]const u8) = .empty,
    native_targets: std.StringHashMapUnmanaged([]const u8) = .empty,
    removed: std.StringHashMapUnmanaged(void) = .empty,
    redirects: resolver.curation.RedirectIndex,
    pins: std.StringHashMapUnmanaged(std.ArrayList(resolver.curation.Pair)) = .empty,
    fn release(self: *Context) void {
        if (self.references.fetchSub(1, .acq_rel) == 1) {
            const owner = self.arena.child_allocator;
            self.arena.deinit();
            owner.destroy(self);
        }
    }
    fn build(owner: A, key: [32]u8, loader: Loader) !*Context {
        var arena = std.heap.ArenaAllocator.init(owner);
        errdefer arena.deinit();
        const a = arena.allocator();
        const graph = try loader.load(loader.ptr, a);
        var statics: std.StringHashMapUnmanaged([]const u8) = .empty;
        var natives: std.StringHashMapUnmanaged([]const u8) = .empty;
        var removed: std.StringHashMapUnmanaged(void) = .empty;
        var reverse: std.StringHashMapUnmanaged(std.ArrayList(resolver.KeyAlias)) = .empty;
        for (graph.aliases) |alias| {
            try statics.put(a, alias.key, alias.target);
            const entry = try reverse.getOrPut(a, alias.target);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(a, alias);
        }
        for (graph.native_aliases) |alias| try natives.put(a, alias.key, alias.target);
        for (graph.removed) |id| try removed.put(a, id, {});
        var effective: std.ArrayList(resolver.KeyAlias) = .empty;
        for (graph.aliases) |alias| if (!natives.contains(alias.key) and !removed.contains(alias.key)) try effective.append(a, alias);
        try effective.appendSlice(a, graph.native_aliases);
        const redirects = try resolver.curation.RedirectIndex.init(a, effective.items);
        var pins: std.StringHashMapUnmanaged(std.ArrayList(resolver.curation.Pair)) = .empty;
        for (graph.pairs) |pair| {
            const from = redirects.canonical(pair.from);
            const to = redirects.canonical(pair.to);
            for ([_][]const u8{ from, to }, 0..) |id, i| {
                if (i == 1 and std.mem.eql(u8, from, to)) continue;
                const entry = try pins.getOrPut(a, id);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                try entry.value_ptr.append(a, pair);
            }
        }
        const result = try owner.create(Context);
        result.* = .{ .arena = arena, .key = key, .composed = graph.composed, .static_reverse = reverse, .static_targets = statics, .native_targets = natives, .removed = removed, .redirects = redirects, .pins = pins };
        return result;
    }
};
pub const Lease = struct {
    context: *const Context,
    pub fn release(self: Lease) void {
        @constCast(self.context).release();
    }
};
pub const Cache = struct {
    allocator: A = heap,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Context) = .empty,
    oldest: ?*Context = null,
    newest: ?*Context = null,
    // Coalesce identical misses without holding the directory lock during I/O.
    // Colliding unrelated keys may serialize, but never share a snapshot.
    preparing: [64]std.atomic.Mutex = @splat(.unlocked),
    bytes: usize = 0,
    reserved: usize = 0,
    builds: std.atomic.Value(usize) = .init(0),
    budget_bytes: usize = max_bytes,
    pub const max_bytes = 64 * 1024 * 1024;
    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn weight(context: *const Context) usize {
        // Include control/index overhead, so empty entries consume budget too.
        return context.arena.queryCapacity() + @sizeOf(Context) + 128;
    }
    fn unlink(self: *Cache, context: *Context) void {
        if (context.previous) |prev| prev.next = context.next else self.oldest = context.next;
        if (context.next) |next| next.previous = context.previous else self.newest = context.previous;
    }
    fn touch(self: *Cache, context: *Context) void {
        context.previous = self.newest;
        context.next = null;
        if (self.newest) |last| last.next = context else self.oldest = context;
        self.newest = context;
    }
    fn find(self: *Cache, key: [32]u8) ?Lease {
        const context = self.entries.get(key) orelse return null;
        self.unlink(context);
        self.touch(context);
        _ = context.references.fetchAdd(1, .monotonic);
        return .{ .context = context };
    }
    pub fn deinit(self: *Cache) void {
        self.lock();
        const first = self.oldest;
        self.oldest = null;
        self.newest = null;
        self.bytes = 0;
        self.entries.deinit(self.allocator);
        self.entries = .empty;
        self.mutex.unlock();
        var current = first;
        while (current) |context| {
            current = context.next;
            context.release();
        }
    }
    pub fn acquire(self: *Cache, key: [32]u8, cacheable: bool, loader: Loader) !Lease {
        std.debug.assert(loader.estimated_bytes == null);
        return (try self.tryAcquire(key, cacheable, loader)).?;
    }
    /// A denied compilation reservation is a cache miss, not a service error.
    pub fn tryAcquire(self: *Cache, key: [32]u8, cacheable: bool, loader: Loader) !?Lease {
        if (cacheable) {
            self.lock();
            const hit = self.find(key);
            self.mutex.unlock();
            if (hit) |lease| return lease;
        }
        const stripe = &self.preparing[key[0] % self.preparing.len];
        if (cacheable) @import("antfly_platform").sync.lockYielding(stripe);
        defer if (cacheable) stripe.unlock();
        if (cacheable) {
            self.lock();
            const hit = self.find(key);
            self.mutex.unlock();
            if (hit) |lease| return lease;
        }
        const reservation = if (cacheable) loader.estimated_bytes orelse 0 else 0;
        if (reservation != 0) {
            self.lock();
            if (reservation > self.budget_bytes -| self.bytes -| self.reserved) {
                self.mutex.unlock();
                return null;
            }
            self.reserved += reservation;
            self.mutex.unlock();
        }
        defer if (reservation != 0) {
            self.lock();
            self.reserved -= reservation;
            self.mutex.unlock();
        };
        const context = try Context.build(self.allocator, key, loader);
        errdefer context.release();
        _ = self.builds.fetchAdd(1, .monotonic);
        const bytes = weight(context);
        if (!cacheable or bytes > self.budget_bytes) return .{ .context = context };
        self.lock();
        self.entries.ensureUnusedCapacity(self.allocator, 1) catch |err| {
            self.mutex.unlock();
            return err;
        };
        var retired: ?*Context = null;
        while (self.bytes + bytes > self.budget_bytes) {
            const victim = self.oldest.?;
            self.unlink(victim);
            _ = self.entries.remove(victim.key);
            self.bytes -= weight(victim);
            victim.next = retired;
            retired = victim;
        }
        self.entries.putAssumeCapacity(key, context);
        self.touch(context);
        self.bytes += bytes;
        _ = context.references.fetchAdd(1, .monotonic);
        self.mutex.unlock();
        // Reclaim potentially large arenas without blocking directory hits.
        while (retired) |victim| {
            retired = victim.next;
            victim.release();
        }
        return .{ .context = context };
    }
};
var shared: Cache = .{};
pub fn acquire(key: [32]u8, cacheable: bool, loader: Loader) !Lease {
    return shared.acquire(key, cacheable, loader);
}
pub fn tryAcquire(key: [32]u8, cacheable: bool, loader: Loader) !?Lease {
    return shared.tryAcquire(key, cacheable, loader);
}

// Each loader owns all returned strings in the supplied allocator. This fixture
// also verifies that eviction never leaves active query pages borrowing storage.
test "native resolver authority prepared query profiles survive eviction and failed builds" {
    const Fixture = struct {
        fn load(_: *anyopaque, a: A) !Graph {
            const aliases = try a.alloc(resolver.KeyAlias, 1);
            aliases[0] = .{ .key = try a.dupe(u8, "alias"), .target = try a.dupe(u8, "canonical") };
            const pairs = try a.alloc(resolver.curation.Pair, 1);
            pairs[0] = .{ .from = aliases[0].key, .to = try a.dupe(u8, "other") };
            return .{ .aliases = aliases, .pairs = pairs, .composed = true };
        }
        fn run(a: A) !void {
            var local: Cache = .{ .allocator = a };
            defer local.deinit();
            var unused: u8 = 0;
            const loader: Loader = .{ .ptr = &unused, .load = load };
            const first = try local.acquire(@splat(1), true, loader);
            defer first.release();
            const again = try local.acquire(@splat(1), true, loader);
            defer again.release();
            try std.testing.expect(first.context == again.context);
            try std.testing.expectEqual(@as(usize, 1), local.builds.load(.monotonic));
            local.deinit();
            try std.testing.expectEqual(@as(usize, 0), local.bytes);
            try std.testing.expectEqualStrings("canonical", first.context.redirects.canonical("alias"));
            try std.testing.expectEqual(@as(usize, 1), first.context.pins.get("canonical").?.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "native resolver authority prepared cache retains many small profiles and evicts by access within bytes" {
    const Empty = struct {
        fn load(_: *anyopaque, _: A) !Graph {
            return .{ .aliases = &.{} };
        }
    };
    var unused: u8 = 0;
    const loader: Loader = .{ .ptr = &unused, .load = Empty.load };
    var cache: Cache = .{ .allocator = std.testing.allocator };
    defer cache.deinit();
    for (0..2) |_| for (0..33) |i| {
        var key: [32]u8 = @splat(0);
        key[0] = @intCast(i);
        const lease = try cache.acquire(key, true, loader);
        lease.release();
    };
    try std.testing.expectEqual(@as(usize, 33), cache.builds.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 33), cache.entries.count());
    std.debug.print("PROFILE CACHE profiles=33 requests=66 builds={d}\n", .{cache.builds.load(.monotonic)});
    cache.deinit();
    const first = try cache.acquire(@splat(1), true, loader);
    defer first.release();
    cache.budget_bytes = Cache.weight(first.context) * 2;
    const second = try cache.acquire(@splat(2), true, loader);
    defer second.release();
    const hit = try cache.acquire(@splat(1), true, loader);
    hit.release();
    const third = try cache.acquire(@splat(3), true, loader);
    defer third.release();
    try std.testing.expect(cache.entries.contains(@splat(1)));
    try std.testing.expect(!cache.entries.contains(@splat(2)));
    try std.testing.expect(cache.bytes <= cache.budget_bytes);
    const builds = cache.builds.load(.monotonic);
    try std.testing.expect(try cache.tryAcquire(@splat(4), true, .{ .ptr = &unused, .load = Empty.load, .estimated_bytes = cache.budget_bytes + 1 }) == null);
    try std.testing.expectEqual(builds, cache.builds.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), cache.reserved);
    cache.deinit();
    cache.budget_bytes = Cache.max_bytes;
    const Failing = struct {
        fn load(_: *anyopaque, _: A) !Graph {
            return error.OutOfMemory;
        }
    };
    try std.testing.expectError(error.OutOfMemory, cache.tryAcquire(@splat(5), true, .{ .ptr = &unused, .load = Failing.load, .estimated_bytes = 4096 }));
    try std.testing.expectEqual(@as(usize, 0), cache.reserved);
    const retry = try cache.acquire(@splat(5), true, loader);
    retry.release();
    // An evicted context remains owned by its outstanding lease.
    try std.testing.expectEqual(@as(usize, 0), second.context.static_targets.count());
}

test "native resolver authority prepared cache coalesces concurrent misses" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const Fixture = struct {
        cache: Cache = .{ .allocator = std.testing.allocator },
        started: std.atomic.Value(u32) = .init(0),
        entered: std.atomic.Value(bool) = .init(false),
        proceed: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),
        fn load(ptr: *anyopaque, _: A) !Graph {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.proceed.load(.acquire)) @import("antfly_platform").time.yieldBriefly();
            return .{ .aliases = &.{} };
        }
        fn run(self: *@This()) void {
            _ = self.started.fetchAdd(1, .release);
            const lease = self.cache.acquire(@splat(1), true, .{ .ptr = self, .load = load }) catch {
                self.failed.store(true, .release);
                return;
            };
            lease.release();
        }
    };
    var fixture: Fixture = .{};
    defer fixture.cache.deinit();
    var threads: [8]std.Thread = undefined;
    var started: usize = 0;
    defer {
        fixture.proceed.store(true, .release);
        for (threads[0..started]) |thread| thread.join();
    }
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Fixture.run, .{&fixture});
        started += 1;
    }
    while (fixture.started.load(.acquire) != threads.len or !fixture.entered.load(.acquire)) @import("antfly_platform").time.yieldBriefly();
    fixture.proceed.store(true, .release);
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    try std.testing.expect(!fixture.failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), fixture.cache.builds.load(.monotonic));
}
