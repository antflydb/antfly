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
pub const QueryNode = struct { key: []const u8, canonical: []const u8, digest: [64]u8 = @splat(0) };
pub const NodeIndex = struct {
    nodes: []const QueryNode,
    by_key: std.StringHashMapUnmanaged(QueryNode),
    by_root: std.StringHashMapUnmanaged(std.ArrayList(QueryNode)),
    pub fn init(a: A, aliases: []const resolver.KeyAlias) !NodeIndex {
        const redirects = try resolver.curation.RedirectIndex.init(a, aliases);
        var keys: std.StringHashMapUnmanaged(QueryNode) = .empty;
        for (aliases) |alias| for ([_][]const u8{ alias.key, alias.target }) |id| {
            const entry = try keys.getOrPut(a, id);
            if (entry.found_existing) continue;
            var digest: [32]u8 = undefined;
            @import("antfly_hash").Sha256.hash(id, &digest, .{});
            entry.value_ptr.* = .{ .key = id, .canonical = redirects.canonical(id), .digest = std.fmt.bytesToHex(digest, .lower) };
        };
        const nodes = try a.alloc(QueryNode, keys.count());
        var iterator = keys.valueIterator();
        for (nodes) |*node| node.* = iterator.next().?.*;
        std.mem.sort(QueryNode, nodes, {}, struct {
            fn less(_: void, left: QueryNode, right: QueryNode) bool {
                return std.mem.lessThan(u8, &left.digest, &right.digest);
            }
        }.less);
        var roots: std.StringHashMapUnmanaged(std.ArrayList(QueryNode)) = .empty;
        for (nodes) |node| {
            const entry = try roots.getOrPut(a, node.canonical);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(a, node);
        }
        return .{ .nodes = nodes, .by_key = keys, .by_root = roots };
    }
    pub fn seek(self: NodeIndex, digest: []const u8, canonical: ?[]const u8) ?QueryNode {
        const nodes = if (canonical) |root| if (self.by_root.get(root)) |list| list.items else return null else self.nodes;
        var lo: usize = 0;
        var hi = nodes.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.lessThan(u8, &nodes[mid].digest, digest)) lo = mid + 1 else hi = mid;
        }
        return if (lo == nodes.len) null else nodes[lo];
    }
};
pub const Route = struct { root: []const u8, component: []const u8 };
pub const Graph = struct {
    routes: []const Route = &.{},
    aliases: []const resolver.KeyAlias = &.{},
    base: ?Lease = null,
    shared_profile: bool = false,
    native_aliases: []const resolver.KeyAlias = &.{},
    removed: []const []const u8 = &.{},
    pairs: []const resolver.curation.Pair = &.{},
    composed: bool = false,
};
pub const Loader = struct { ptr: *anyopaque, load: *const fn (*anyopaque, A) anyerror!Graph, estimated_bytes: ?usize = null };
const Budget = struct {
    allocator: A,
    references: std.atomic.Value(usize) = .init(1),
    live: std.atomic.Value(usize) = .init(0),
    fn release(self: *Budget) void {
        if (self.references.fetchSub(1, .acq_rel) == 1) self.allocator.destroy(self);
    }
};
pub const Redirects = struct {
    local: resolver.curation.RedirectIndex,
    base: ?*const Context = null,
    pub fn canonical(self: Redirects, key: []const u8) []const u8 {
        if (self.local.canonical_keys.get(key)) |known| return known;
        return if (self.base) |base| base.redirects.canonical(key) else key;
    }
};
pub const Context = struct {
    references: std.atomic.Value(usize) = .init(1),
    dependents: std.atomic.Value(usize) = .init(0),
    base: ?Lease = null,
    shared_profile: bool = false,
    arena: std.heap.ArenaAllocator,
    key: [32]u8,
    accounting: ?*Budget = null,
    charged: usize = 0,
    composed: bool,
    previous: ?*Context = null,
    next: ?*Context = null,
    static_reverse: std.StringHashMapUnmanaged(std.ArrayList(resolver.KeyAlias)) = .empty,
    static_targets: std.StringHashMapUnmanaged([]const u8) = .empty,
    nodes: NodeIndex,
    native_targets: std.StringHashMapUnmanaged([]const u8) = .empty,
    removed: std.StringHashMapUnmanaged(void) = .empty,
    redirects: Redirects,
    pins: std.StringHashMapUnmanaged(std.ArrayList(resolver.curation.Pair)) = .empty,
    routes: []const Route = &.{},
    pub fn routesFor(self: *const Context, root: []const u8) []const Route {
        var lo: usize = 0;
        var hi = self.routes.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.lessThan(u8, self.routes[mid].root, root)) lo = mid + 1 else hi = mid;
        }
        const first = lo;
        while (lo < self.routes.len and std.mem.eql(u8, self.routes[lo].root, root)) lo += 1;
        return self.routes[first..lo];
    }
    fn release(self: *Context) void {
        if (self.references.fetchSub(1, .acq_rel) == 1) {
            const owner = self.arena.child_allocator;
            const budget = self.accounting;
            const charged = self.charged;
            const base = self.base;
            self.arena.deinit();
            owner.destroy(self);
            if (budget) |accounting| {
                _ = accounting.live.fetchSub(charged, .acq_rel);
                accounting.release();
            }
            if (base) |parent| {
                _ = @constCast(parent.context).dependents.fetchSub(1, .acq_rel);
                parent.release();
            }
        }
    }
    fn build(owner: A, cache_key: [32]u8, loader: Loader) !*Context {
        var arena = std.heap.ArenaAllocator.init(owner);
        errdefer arena.deinit();
        const a = arena.allocator();
        const graph = try loader.load(loader.ptr, a);
        const base = if (graph.base) |lease| lease.dependency() else null;
        errdefer if (base) |parent| {
            _ = @constCast(parent.context).dependents.fetchSub(1, .acq_rel);
            parent.release();
        };
        const nodes = if (base) |parent| parent.context.nodes else try NodeIndex.init(a, graph.aliases);
        var statics: std.StringHashMapUnmanaged([]const u8) = if (base) |parent| parent.context.static_targets else .empty;
        var reverse: std.StringHashMapUnmanaged(std.ArrayList(resolver.KeyAlias)) = if (base) |parent| parent.context.static_reverse else .empty;
        var natives: std.StringHashMapUnmanaged([]const u8) = .empty;
        var removed: std.StringHashMapUnmanaged(void) = .empty;
        if (base == null) for (graph.aliases) |alias| {
            try statics.put(a, alias.key, alias.target);
            const entry = try reverse.getOrPut(a, alias.target);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(a, alias);
        };
        for (graph.native_aliases) |alias| try natives.put(a, alias.key, alias.target);
        for (graph.removed) |id| try removed.put(a, id, {});
        var redirects: Redirects = undefined;
        if (base) |parent| {
            var affected: std.StringHashMapUnmanaged(void) = .empty;
            var pending: std.ArrayList([]const u8) = .empty;
            for (graph.native_aliases) |alias| try pending.append(a, alias.key);
            try pending.appendSlice(a, graph.removed);
            var offset: usize = 0;
            while (offset < pending.items.len) : (offset += 1) {
                const id = pending.items[offset];
                if ((try affected.getOrPut(a, id)).found_existing) continue;
                if (parent.context.static_reverse.get(id)) |incoming| for (incoming.items) |edge| try pending.append(a, edge.key);
            }
            var local: resolver.curation.RedirectIndex = .{ .source = graph.native_aliases };
            var visiting: std.StringHashMapUnmanaged(void) = .empty;
            var path: std.ArrayList([]const u8) = .empty;
            var iterator = affected.keyIterator();
            while (iterator.next()) |start| {
                path.clearRetainingCapacity();
                var key = start.*;
                const terminal = walk: while (true) {
                    if (local.canonical_keys.get(key)) |known| break :walk known;
                    if (!affected.contains(key)) break :walk parent.context.redirects.canonical(key);
                    if ((try visiting.getOrPut(a, key)).found_existing) return error.InvalidResolverAliasCycle;
                    try path.append(a, key);
                    if (natives.get(key)) |target| key = target else {
                        if (removed.contains(key)) break :walk key;
                        key = statics.get(key) orelse break :walk key;
                    }
                };
                for (path.items) |id| {
                    try local.canonical_keys.put(a, id, terminal);
                    _ = visiting.remove(id);
                }
            }
            redirects = .{ .local = local, .base = parent.context };
        } else {
            var effective: std.ArrayList(resolver.KeyAlias) = .empty;
            for (graph.aliases) |alias| if (!natives.contains(alias.key) and !removed.contains(alias.key)) try effective.append(a, alias);
            try effective.appendSlice(a, graph.native_aliases);
            redirects = .{ .local = try resolver.curation.RedirectIndex.init(a, effective.items) };
        }
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
        result.* = .{ .arena = arena, .key = cache_key, .base = base, .shared_profile = graph.shared_profile, .composed = graph.composed, .static_reverse = reverse, .static_targets = statics, .nodes = nodes, .native_targets = natives, .removed = removed, .redirects = redirects, .pins = pins, .routes = graph.routes };
        return result;
    }
};
pub const Lease = struct {
    context: *const Context,
    fn dependency(self: Lease) Lease {
        _ = @constCast(self.context).references.fetchAdd(1, .monotonic);
        _ = @constCast(self.context).dependents.fetchAdd(1, .monotonic);
        return self;
    }
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
    accounting: ?*Budget = null,
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
    /// Stop builders before teardown; outstanding read leases may finish later.
    pub fn deinit(self: *Cache) void {
        self.lock();
        const first = self.oldest;
        self.oldest = null;
        self.newest = null;
        self.bytes = 0;
        self.entries.deinit(self.allocator);
        self.entries = .empty;
        const budget = self.accounting;
        self.accounting = null;
        self.mutex.unlock();
        defer if (budget) |accounting| accounting.release();
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
        self.lock();
        if (self.accounting == null) {
            const accounting = self.allocator.create(Budget) catch |err| {
                self.mutex.unlock();
                return err;
            };
            accounting.* = .{ .allocator = self.allocator };
            self.accounting = accounting;
        }
        const accounting = self.accounting.?;
        self.mutex.unlock();
        var reservation = if (cacheable) loader.estimated_bytes orelse 0 else 0;
        if (reservation > self.budget_bytes) return null;
        if (reservation != 0) while (true) {
            self.lock();
            if (reservation <= self.budget_bytes -| accounting.live.load(.acquire) -| self.reserved) {
                self.reserved += reservation;
                self.mutex.unlock();
                break;
            }
            // A cache reference is the only reference on an idle context.
            // Active leases remain charged even after retention eviction.
            var candidate = self.oldest;
            const victim = while (candidate) |context| : (candidate = context.next) {
                if (context.references.load(.acquire) == 1) break context;
            } else {
                self.mutex.unlock();
                return null;
            };
            self.unlink(victim);
            _ = self.entries.remove(victim.key);
            self.bytes -= weight(victim);
            self.mutex.unlock();
            victim.release();
        };
        defer if (reservation != 0) {
            self.lock();
            self.reserved -= reservation;
            self.mutex.unlock();
        };
        const context = try Context.build(self.allocator, key, loader);
        errdefer context.release();
        _ = self.builds.fetchAdd(1, .monotonic);
        const bytes = weight(context);
        self.lock();
        if (reservation != 0) self.reserved -= reservation;
        reservation = 0;
        context.accounting = accounting;
        context.charged = bytes;
        _ = accounting.references.fetchAdd(1, .monotonic);
        _ = accounting.live.fetchAdd(bytes, .release);
        self.mutex.unlock();
        if (!cacheable or bytes > self.budget_bytes) return .{ .context = context };
        self.lock();
        while (self.bytes + bytes > self.budget_bytes) {
            var candidate = self.oldest;
            const victim = while (candidate) |item| : (candidate = item.next) {
                if (item.dependents.load(.acquire) != 0) continue;
                if (item.shared_profile and item.references.load(.acquire) > 1) continue;
                break item;
            } else {
                self.mutex.unlock();
                return .{ .context = context };
            };
            self.unlink(victim);
            _ = self.entries.remove(victim.key);
            self.bytes -= weight(victim);
            self.mutex.unlock();
            // Dropping an idle overlay may make its shared base reclaimable.
            victim.release();
            self.lock();
        }
        self.entries.ensureUnusedCapacity(self.allocator, 1) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.entries.putAssumeCapacity(key, context);
        self.touch(context);
        self.bytes += bytes;
        _ = context.references.fetchAdd(1, .monotonic);
        self.mutex.unlock();
        return .{ .context = context };
    }
};
// One decoded-index budget: immutable bases/overlays and routing have separate
// admission lanes, so a fully leased graph tier can still warm a tiny route.
var shared: Cache = .{ .budget_bytes = 48 * 1024 * 1024 };
var routing: Cache = .{ .budget_bytes = 16 * 1024 * 1024 };
pub const RoutingScratch = struct {
    bytes: []u8,
    pub fn init() !RoutingScratch {
        return .{ .bytes = try heap.alloc(u8, 8 * 1024 * 1024) };
    }
    pub fn deinit(self: RoutingScratch) void {
        heap.free(self.bytes);
    }
};
pub fn tryRouting(key: [32]u8, loader: Loader) !?Lease {
    var bounded = loader;
    bounded.estimated_bytes = 12 * 1024 * 1024; // 8 MiB scratch + <=4 MiB frozen data.
    return routing.tryAcquire(key, true, bounded);
}
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

test "native resolver authority estimated admission reclaims idle contexts and accounts for evicted leases" {
    const Empty = struct {
        fn load(_: *anyopaque, _: A) !Graph {
            return .{ .aliases = &.{} };
        }
    };
    var unused: u8 = 0;
    const loader: Loader = .{ .ptr = &unused, .load = Empty.load };
    var cache: Cache = .{ .allocator = std.testing.allocator };
    defer cache.deinit();
    const seed = try cache.acquire(@splat(1), true, loader);
    const charge = Cache.weight(seed.context);
    seed.release();
    cache.budget_bytes = charge * 4;
    for (2..5) |i| {
        const lease = try cache.acquire(@splat(@intCast(i)), true, loader);
        lease.release();
    }
    const admitted = (try cache.tryAcquire(@splat(5), true, .{ .ptr = &unused, .load = Empty.load, .estimated_bytes = charge * 2 })).?;
    admitted.release();
    try std.testing.expectEqual(@as(usize, 5), cache.builds.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 3), cache.entries.count());
    try std.testing.expect(cache.accounting.?.live.load(.acquire) <= cache.budget_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.reserved);
    std.debug.print("ESTIMATED ADMISSION old_entries=4 new_builds=1 retained=3 denied=0\n", .{});
    cache.deinit();
    cache.budget_bytes = charge * 2;
    var first: ?Lease = try cache.acquire(@splat(1), true, loader);
    defer if (first) |lease| lease.release();
    var second: ?Lease = try cache.acquire(@splat(2), true, loader);
    defer if (second) |lease| lease.release();
    const third = try cache.acquire(@splat(3), true, loader);
    defer third.release();
    // Retention evicts first, but the active lease still owns its memory.
    try std.testing.expectEqual(charge * 2, cache.bytes);
    try std.testing.expectEqual(charge * 3, cache.accounting.?.live.load(.acquire));
    try std.testing.expect(try cache.tryAcquire(@splat(4), true, .{ .ptr = &unused, .load = Empty.load, .estimated_bytes = charge }) == null);
    first.?.release();
    first = null;
    second.?.release();
    second = null;
    const fourth = (try cache.tryAcquire(@splat(4), true, .{ .ptr = &unused, .load = Empty.load, .estimated_bytes = charge })).?;
    defer fourth.release();
    try std.testing.expectEqual(charge * 2, cache.accounting.?.live.load(.acquire));
}

test "native resolver authority overlays share profile ownership through allocation failures and eviction" {
    const Fixture = struct {
        base: Lease,
        fn profile(_: *anyopaque, a: A) !Graph {
            const aliases = try a.alloc(resolver.KeyAlias, 2);
            aliases[0] = .{ .key = "a", .target = "b" };
            aliases[1] = .{ .key = "b", .target = "root" };
            return .{ .aliases = aliases, .shared_profile = true };
        }
        fn overlay(ptr: *anyopaque, _: A) !Graph {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .base = self.base, .composed = true, .native_aliases = &.{.{ .key = "b", .target = "moved" }}, .pairs = &.{.{ .from = "a", .to = "other" }} };
        }
        fn run(a: A) !void {
            var local: Cache = .{ .allocator = a };
            defer local.deinit();
            var unused: u8 = 0;
            const base = try local.acquire(@splat(61), true, .{ .ptr = &unused, .load = profile });
            defer base.release();
            var fixture: @This() = .{ .base = base };
            const child = try local.acquire(@splat(62), true, .{ .ptr = &fixture, .load = overlay });
            defer child.release();
            try std.testing.expect(base.context.nodes.nodes.ptr == child.context.nodes.nodes.ptr);
            try std.testing.expectEqual(@as(usize, 1), base.context.dependents.load(.acquire));
            try std.testing.expectEqualStrings("moved", child.context.redirects.canonical("a"));
            try std.testing.expectEqualStrings("root", base.context.redirects.canonical("a"));
            try std.testing.expectEqual(@as(usize, 1), child.context.pins.get("moved").?.items.len);
            local.deinit();
            try std.testing.expectEqualStrings("moved", child.context.redirects.canonical("a"));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
