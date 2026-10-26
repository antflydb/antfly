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
pub const Loader = struct { ptr: *anyopaque, load: *const fn (*anyopaque, A) anyerror!Graph };
pub const Context = struct {
    references: std.atomic.Value(usize) = .init(1),
    arena: std.heap.ArenaAllocator,
    key: [32]u8,
    composed: bool,
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
        for (graph.aliases) |alias| try statics.put(a, alias.key, alias.target);
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
        result.* = .{ .arena = arena, .key = key, .composed = graph.composed, .static_targets = statics, .native_targets = natives, .removed = removed, .redirects = redirects, .pins = pins };
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
    entries: [32]?*Context = @splat(null),
    next: usize = 0,
    bytes: usize = 0,
    builds: std.atomic.Value(usize) = .init(0),
    pub const max_bytes = 64 * 1024 * 1024;
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn find(self: *Cache, key: [32]u8) ?Lease {
        for (self.entries) |entry| if (entry) |context| if (std.mem.eql(u8, &context.key, &key)) {
            _ = context.references.fetchAdd(1, .monotonic);
            return .{ .context = context };
        };
        return null;
    }
    fn remove(self: *Cache, index: usize) void {
        if (self.entries[index]) |context| {
            self.bytes -= context.arena.queryCapacity();
            context.release();
            self.entries[index] = null;
        }
    }
    pub fn deinit(self: *Cache) void {
        self.lock();
        defer self.mutex.unlock();
        for (0..self.entries.len) |i| self.remove(i);
    }
    pub fn acquire(self: *Cache, key: [32]u8, cacheable: bool, loader: Loader) !Lease {
        self.lock();
        if (cacheable) if (self.find(key)) |lease| {
            self.mutex.unlock();
            return lease;
        };
        self.mutex.unlock();
        // Building may read a native snapshot. Never hold the cache lock or a
        // writer lease while loading/compiling the immutable policy.
        const context = try Context.build(self.allocator, key, loader);
        _ = self.builds.fetchAdd(1, .monotonic);
        const bytes = context.arena.queryCapacity();
        self.lock();
        defer self.mutex.unlock();
        if (cacheable) if (self.find(key)) |lease| {
            context.release();
            return lease;
        };
        if (cacheable and bytes <= max_bytes) {
            while (self.bytes + bytes > max_bytes) {
                self.remove(self.next);
                self.next = (self.next + 1) % self.entries.len;
            }
            self.remove(self.next);
            self.entries[self.next] = context;
            self.bytes += bytes;
            self.next = (self.next + 1) % self.entries.len;
            _ = context.references.fetchAdd(1, .monotonic);
        }
        return .{ .context = context };
    }
};
var shared: Cache = .{};
pub fn acquire(key: [32]u8, cacheable: bool, loader: Loader) !Lease {
    return shared.acquire(key, cacheable, loader);
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
