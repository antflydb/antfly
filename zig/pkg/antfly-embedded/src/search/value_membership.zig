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

//! Optional, bounded, memory-only predicate facts for admitted immutable cuts.
//! Entries contain segment-local IDs, never source bodies or read capabilities.
const std = @import("std");
const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
const A = std.mem.Allocator;

pub const Scope = struct {
    identity: [32]u8,
    expires_ms: u64,
    pub fn init(path: []const u8, id: []const u8, namespace: anytype, expires_ms: u64) Scope {
        return .{ .identity = digest(.{ "logical-values-v1", path, id, namespace }), .expires_ms = expires_ms };
    }
};
fn digest(value: anytype) [32]u8 {
    var buffer: [1024]u8 = undefined;
    var writer: std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256) = .init(&buffer);
    std.json.Stringify.value(value, .{}, &writer.writer) catch unreachable;
    writer.writer.flush() catch unreachable;
    var result: [32]u8 = undefined;
    writer.hasher.final(&result);
    return result;
}
pub fn key(scope: Scope, index: ?[]const u8, segment: u64, count: u32, spec: anytype) [32]u8 {
    return digest(.{ scope.identity, index, segment, count, @tagName(spec.kind), spec.field, spec.text, spec.case_sensitive });
}
pub const Facts = struct {
    complete: bool = false,
    known: Bitmap,
    matched: Bitmap,
    pub fn init(a: A) Facts {
        return .{ .known = Bitmap.init(a), .matched = Bitmap.init(a) };
    }
    pub fn deinit(self: *Facts) void {
        self.known.deinit();
        self.matched.deinit();
    }
    pub fn clone(self: Facts, a: A) !Facts {
        var known = try self.known.cloneExact(a);
        errdefer known.deinit();
        return .{ .complete = self.complete, .known = known, .matched = try self.matched.cloneExact(a) };
    }
};
pub const Cache = struct {
    // Directory capacity, facts, retired reader pins and in-flight builders all
    // share one byte budget. Directory nodes live inside the immutable block.
    const Shared = struct {
        refs: std.atomic.Value(usize) = .init(1),
        block: []align(@alignOf(u64)) u8,
        arena: std.heap.FixedBufferAllocator,
        facts: Facts,
        key: [32]u8,
        expires_ms: u64,
        expiry_index: usize = 0,
        previous: ?*Shared = null,
        next: ?*Shared = null,
        hot: bool = false,
    };
    const Queue = struct { first: ?*Shared = null, last: ?*Shared = null };
    pub const Lease = struct {
        cache: *Cache,
        shared: *Shared,
        pub fn facts(self: Lease) *const Facts {
            return &self.shared.facts;
        }
        pub fn deinit(self: *Lease) void {
            self.cache.release(self.shared);
            self.* = undefined;
        }
    };
    budget: @import("../sql/memory_budget.zig"),
    entries: std.AutoHashMapUnmanaged([32]u8, *Shared) = .empty,
    cold: Queue = .{},
    hot: Queue = .{},
    expiry: std.ArrayListUnmanaged(*Shared) = .empty,
    expiry_comparisons: usize = 0, // Test instrumentation; no production increments.
    mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn release(self: *Cache, shared: *Shared) void {
        if (shared.refs.fetchSub(1, .acq_rel) == 1) self.budget.allocator().free(shared.block);
    }
    fn unlink(self: *Cache, shared: *Shared) void {
        const queue = if (shared.hot) &self.hot else &self.cold;
        if (shared.previous) |p| p.next = shared.next else queue.first = shared.next;
        if (shared.next) |n| n.previous = shared.previous else queue.last = shared.previous;
        shared.previous = null;
        shared.next = null;
    }
    fn append(self: *Cache, shared: *Shared) void {
        const queue = if (shared.hot) &self.hot else &self.cold;
        shared.previous = queue.last;
        shared.next = null;
        if (queue.last) |last| last.next = shared else queue.first = shared;
        queue.last = shared;
    }
    fn remove(self: *Cache, shared: *Shared) void {
        self.unlink(shared);
        self.removeExpiry(shared.expiry_index);
        std.debug.assert(self.entries.remove(shared.key));
        self.release(shared);
    }
    fn evict(self: *Cache) bool {
        // A one-shot scan replaces probationary entries before proven reuse.
        // Skip pinned blocks: removing their directory entry cannot free bytes.
        for ([_]?*Shared{ self.cold.first, self.hot.first }) |first| {
            var node = first;
            while (node) |shared| : (node = shared.next) {
                if (shared.refs.load(.acquire) != 1) continue;
                self.remove(shared);
                return true;
            }
        }
        return false;
    }
    fn earlier(self: *Cache, left: *Shared, right: *Shared) bool {
        if (@import("builtin").is_test) self.expiry_comparisons += 1;
        return left.expires_ms < right.expires_ms;
    }
    fn swapExpiry(self: *Cache, left: usize, right: usize) void {
        std.mem.swap(*Shared, &self.expiry.items[left], &self.expiry.items[right]);
        self.expiry.items[left].expiry_index = left;
        self.expiry.items[right].expiry_index = right;
    }
    fn riseExpiry(self: *Cache, start: usize) void {
        var index = start;
        while (index != 0) {
            const parent = (index - 1) / 2;
            if (!self.earlier(self.expiry.items[index], self.expiry.items[parent])) break;
            self.swapExpiry(index, parent);
            index = parent;
        }
    }
    fn removeExpiry(self: *Cache, index: usize) void {
        const last = self.expiry.pop().?;
        if (index == self.expiry.items.len) return;
        self.expiry.items[index] = last;
        last.expiry_index = index;
        if (index != 0 and self.earlier(last, self.expiry.items[(index - 1) / 2])) {
            self.riseExpiry(index);
            return;
        }
        var current = index;
        while (current * 2 + 1 < self.expiry.items.len) {
            var child = current * 2 + 1;
            if (child + 1 < self.expiry.items.len and self.earlier(self.expiry.items[child + 1], self.expiry.items[child])) child += 1;
            if (!self.earlier(self.expiry.items[child], self.expiry.items[current])) break;
            self.swapExpiry(current, child);
            current = child;
        }
    }
    fn expire(self: *Cache, now_ms: u64) void {
        // Indexed heap removal also covers replacement and eviction: no stale
        // tombstones accumulate, and staggered deadlines never scan live facts.
        while (self.expiry.items.len != 0 and self.expiry.items[0].expires_ms <= now_ms) self.remove(self.expiry.items[0]);
        if (self.entries.count() == 0) {
            self.entries.deinit(self.budget.allocator());
            self.entries = .empty;
            self.expiry.deinit(self.budget.allocator());
            self.expiry = .empty;
        }
    }
    pub fn deinit(self: *Cache) void {
        while (self.cold.first) |shared| self.remove(shared);
        while (self.hot.first) |shared| self.remove(shared);
        self.entries.deinit(self.budget.allocator());
        self.expiry.deinit(self.budget.allocator());
    }
    /// Lookup and pin are O(1), with no allocations or bitmap copies. Expiry
    /// removes only expired entries, with O(log N) work per removal.
    /// The cache must outlive leases, including leases retired by deinit.
    pub fn borrow(self: *Cache, identity: [32]u8, now_ms: u64) ?Lease {
        self.lock();
        defer self.mutex.unlock();
        self.expire(now_ms);
        const shared = self.entries.get(identity) orelse return null;
        self.unlink(shared);
        shared.hot = true;
        self.append(shared);
        _ = shared.refs.fetchAdd(1, .monotonic);
        return .{ .cache = self, .shared = shared };
    }
    pub fn get(self: *Cache, a: A, identity: [32]u8, now_ms: u64) !?Facts {
        var lease = self.borrow(identity, now_ms) orelse return null;
        defer lease.deinit();
        return lease.facts().clone(a) catch null;
    }
    pub fn put(self: *Cache, identity: [32]u8, expires_ms: u64, now_ms: u64, facts: Facts) void {
        if (expires_ms <= now_ms) return;
        const needed = @sizeOf(Shared) + facts.known.cloneStorageBound() + facts.matched.cloneStorageBound() + 32;
        if (needed > 2 * 1024 * 1024 or needed > self.budget.limit) return;
        self.lock();
        self.expire(now_ms);
        if (self.entries.get(identity)) |held| if (held.facts.complete) {
            self.mutex.unlock();
            return;
        };
        // Optional metadata growth cannot flush useful entries on failure.
        self.expiry.ensureUnusedCapacity(self.budget.allocator(), 1) catch {
            self.mutex.unlock();
            return;
        };
        self.entries.ensureUnusedCapacity(self.budget.allocator(), 1) catch {
            self.mutex.unlock();
            return;
        };
        var available = @import("../sql/memory_budget.zig").headroom(self.budget.allocator()).?;
        if (available < needed) {
            var iterator = self.entries.valueIterator();
            while (iterator.next()) |held| if (held.*.refs.load(.acquire) == 1) {
                available += held.*.block.len;
                if (available >= needed) break;
            };
        }
        if (available < needed) {
            self.mutex.unlock();
            return;
        }
        const block = while (true) {
            if (@import("../sql/memory_budget.zig").headroom(self.budget.allocator()).? >= needed) {
                break self.budget.allocator().alignedAlloc(u8, .of(u64), needed) catch {
                    self.mutex.unlock();
                    return;
                };
            }
            if (!self.evict()) {
                self.mutex.unlock();
                return;
            }
        };
        self.mutex.unlock();
        const shared: *Shared = @ptrCast(block.ptr);
        shared.* = .{ .block = block, .arena = .init(block[@sizeOf(Shared)..]), .facts = undefined, .key = identity, .expires_ms = expires_ms };
        shared.facts = facts.clone(shared.arena.allocator()) catch {
            self.release(shared);
            return;
        };
        self.lock();
        defer self.mutex.unlock();
        if (self.entries.get(identity)) |held| {
            if (held.facts.complete) {
                self.release(shared);
                return;
            }
            shared.hot = held.hot;
            self.remove(held);
        }
        // Other builders may have used the spare hash capacity while copying.
        self.expiry.ensureUnusedCapacity(self.budget.allocator(), 1) catch {
            self.release(shared);
            return;
        };
        self.entries.ensureUnusedCapacity(self.budget.allocator(), 1) catch {
            self.release(shared);
            return;
        };
        self.entries.putAssumeCapacity(identity, shared);
        self.append(shared);
        shared.expiry_index = self.expiry.items.len;
        self.expiry.appendAssumeCapacity(shared);
        self.riseExpiry(shared.expiry_index);
    }
};
// Storage callers must supply a scope installed by openQueryCut *after*
// validation. Ordinary mutable searches never consult this process cache.
pub var retained: Cache = .{ .budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 16 * 1024 * 1024 } };

test "logical value retained facts isolate predicates expire and survive pressure" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 8192 } };
    defer cache.deinit();
    var facts = Facts.init(a);
    defer facts.deinit();
    try facts.known.add(3);
    try facts.known.add(4);
    try facts.matched.add(4);
    const first = digest(.{"first"});
    cache.put(first, 20, 1, facts);
    var hit = (try cache.get(a, first, 2)).?;
    defer hit.deinit();
    try std.testing.expect(hit.known.contains(3) and !hit.matched.contains(3));
    try std.testing.expect(hit.matched.contains(4));
    try std.testing.expect(try cache.get(a, digest(.{"other"}), 2) == null);
    try std.testing.expect(try cache.get(a, first, 20) == null);
    // Copies remain owned after expiry/eviction; negative membership is retained.
    try std.testing.expect(hit.known.contains(3));
    var tiny: Cache = .{ .budget = .{ .backing = a, .limit = 1 } };
    defer tiny.deinit();
    tiny.put(first, 20, 1, facts);
    try std.testing.expect(try tiny.get(a, first, 2) == null);
}

test "logical value complete retained facts stay sparse and resist partial replacement" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 8192 } };
    defer cache.deinit();
    var complete = Facts.init(a);
    defer complete.deinit();
    complete.complete = true;
    try complete.matched.add(900_000_000);
    const identity = digest(.{"billion-document-segment"});
    cache.put(identity, 20, 1, complete);
    var partial = Facts.init(a);
    defer partial.deinit();
    try partial.known.add(1);
    cache.put(identity, 20, 2, partial);
    var hit = (try cache.get(a, identity, 3)).?;
    defer hit.deinit();
    try std.testing.expect(hit.complete and hit.matched.contains(900_000_000));
    try std.testing.expectEqual(@as(usize, 0), hit.known.cardinality());
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expect(try cache.get(failing.allocator(), identity, 3) == null);
}

test "logical value oversized admission preserves warm entries and pinned expiry" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 4 * 1024 * 1024 } };
    defer cache.deinit();
    var warm = Facts.init(a);
    defer warm.deinit();
    warm.complete = true;
    try warm.matched.addRange(0, 15_000_000);
    const first = digest(.{"warm-first"});
    const second = digest(.{"warm-second"});
    cache.put(first, 100, 1, warm);
    cache.put(second, 100, 1, warm);
    var oversized = Facts.init(a);
    defer oversized.deinit();
    oversized.complete = true;
    try oversized.matched.addRange(0, 20_000_000);
    cache.put(digest(.{"oversized"}), 100, 2, oversized);
    var pin = cache.borrow(first, 3).?;
    defer pin.deinit();
    var other = cache.borrow(second, 3).?;
    other.deinit();
    try std.testing.expect(cache.borrow(second, 100) == null);
    // Eviction released directory ownership only. Pins remain budgeted/valid.
    try std.testing.expect(cache.budget.live != 0);
    try std.testing.expect(pin.facts().complete and pin.facts().matched.contains(14_999_999));
    var no_memory = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, pin.facts().clone(no_memory.allocator()));
    try std.testing.expect(pin.facts().matched.contains(0));
}

test "logical value retained pins share storage across concurrent eviction" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 8192 } };
    defer cache.deinit();
    var facts = Facts.init(a);
    defer facts.deinit();
    try facts.matched.add(7);
    facts.complete = true;
    const identity = digest(.{"pinned"});
    cache.put(identity, 20, 1, facts);
    var first = cache.borrow(identity, 2).?;
    defer first.deinit();
    var second = cache.borrow(identity, 2).?;
    defer second.deinit();
    try std.testing.expect(first.facts() == second.facts());
    const Reader = struct {
        fn run(held: *const Lease) void {
            for (0..10000) |_| std.debug.assert(held.facts().matched.contains(7));
        }
        const Lease = Cache.Lease;
    };
    const thread = try std.Thread.spawn(.{}, Reader.run, .{&first});
    defer thread.join();
    try std.testing.expect(cache.borrow(identity, 20) == null);
    try std.testing.expect(second.facts().matched.contains(7));
}

test "logical value pinned budget refusal preserves useful unpinned entries" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 4096 } };
    defer cache.deinit();
    var large = Facts.init(a);
    defer large.deinit();
    try large.matched.addRange(0, 1000);
    const first = digest(.{"pinned-large"});
    cache.put(first, 100, 1, large);
    var pin = cache.borrow(first, 2).?;
    defer pin.deinit();
    var small = Facts.init(a);
    defer small.deinit();
    try small.matched.add(7);
    const second = digest(.{"useful-small"});
    cache.put(second, 100, 3, small);
    var prospective = Facts.init(a);
    defer prospective.deinit();
    try prospective.matched.addRange(0, 1500);
    cache.put(digest(.{"cannot-fit-around-pin"}), 100, 4, prospective);
    var kept = cache.borrow(second, 5).?;
    defer kept.deinit();
    try std.testing.expect(kept.facts().matched.contains(7));
    try std.testing.expect(cache.budget.live <= cache.budget.limit);
}

test "logical value byte budget admits thousands of tiny retained facts" {
    const a = std.testing.allocator;
    for ([_]usize{ 129, 4096 }) |count| {
        var cache: Cache = .{ .budget = .{ .backing = a, .limit = 16 * 1024 * 1024 } };
        defer cache.deinit();
        var facts = Facts.init(a);
        defer facts.deinit();
        facts.complete = true;
        try facts.matched.add(7);
        for (0..count) |i| cache.put(digest(.{i}), 100, 1, facts);
        var hits: usize = 0;
        for (0..count) |i| {
            var pin = cache.borrow(digest(.{i}), 2) orelse return error.TestUnexpectedResult;
            try std.testing.expect(pin.facts().matched.contains(7));
            pin.deinit();
            hits += 1;
        }
        try std.testing.expectEqual(count, hits);
        try std.testing.expectEqual(@as(u32, @intCast(count)), cache.entries.count());
        try std.testing.expect(cache.budget.live <= cache.budget.limit);
        std.debug.print("logical_membership_qualification keys={d} hits={d} peak_bytes={d}\n", .{ count, hits, cache.budget.peak });
        // Force actual eviction pressure while retaining spare hash capacity.
        cache.budget.limit = cache.budget.live + 1024;
        for (count..count + 12288) |i| cache.put(digest(.{i}), 100, 3, facts);
        var warm = cache.borrow(digest(.{@as(usize, 0)}), 4) orelse return error.TestUnexpectedResult;
        warm.deinit();
        try std.testing.expect(cache.budget.live <= cache.budget.limit);
        try std.testing.expect(cache.borrow(digest(.{@as(usize, 0)}), 100) == null);
        try std.testing.expectEqual(@as(usize, 0), cache.budget.live);
    }
}

test "logical value staggered expiry is logarithmic and keeps pins alive" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 4 * 1024 * 1024 } };
    defer cache.deinit();
    var facts = Facts.init(a);
    defer facts.deinit();
    try facts.known.add(7);
    const count = 4096;
    // Reverse insertion exercises both heap directions, independent of LRU.
    for (0..count) |i| cache.put(digest(.{i}), count - i + 10, 1, facts);
    var pin = cache.borrow(digest(.{count - 1}), 2).?;
    defer pin.deinit();
    cache.expiry_comparisons = 0;
    for (0..count) |i| {
        cache.expire(i + 11);
        try std.testing.expectEqual(count - i - 1, cache.entries.count());
        try std.testing.expectEqual(cache.entries.count(), cache.expiry.items.len);
        if (i + 1 < count) {
            var live = cache.borrow(digest(.{@as(usize, 0)}), i + 11).?;
            live.deinit();
        }
    }
    try std.testing.expect(cache.expiry_comparisons < count * 30);
    std.debug.print("logical_membership_expiry keys={d} comparisons={d} peak_bytes={d}\n", .{ count, cache.expiry_comparisons, cache.budget.peak });
    try std.testing.expect(pin.facts().known.contains(7));
    try std.testing.expect(cache.budget.live != 0);
}
