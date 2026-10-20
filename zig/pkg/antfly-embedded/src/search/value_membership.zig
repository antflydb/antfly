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
    // Cache ownership and reader pins share a single bounded allocation. Evicted
    // pinned entries continue counting against admission until their last reader.
    const Shared = struct {
        refs: std.atomic.Value(usize) = .init(1),
        block: []align(@alignOf(u64)) u8,
        arena: std.heap.FixedBufferAllocator,
        facts: Facts,
    };
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
    const Entry = struct { key: [32]u8, shared: *Shared, expires_ms: u64, age: u64 };
    budget: @import("../sql/memory_budget.zig"),
    entries: [128]?Entry = @splat(null),
    mutex: std.atomic.Mutex = .unlocked,
    tick: u64 = 0,
    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn release(self: *Cache, shared: *Shared) void {
        if (shared.refs.fetchSub(1, .acq_rel) == 1) self.budget.allocator().free(shared.block);
    }
    fn remove(self: *Cache, i: usize) void {
        const held = self.entries[i].?;
        self.entries[i] = null;
        self.release(held.shared);
    }
    fn evict(self: *Cache) bool {
        var oldest: ?usize = null;
        for (self.entries, 0..) |entry, i| if (entry) |held| {
            if (oldest == null or held.age < self.entries[oldest.?].?.age) oldest = i;
        };
        self.remove(oldest orelse return false);
        return true;
    }
    fn expire(self: *Cache, now_ms: u64) void {
        for (self.entries, 0..) |entry, i| if (entry) |held| {
            if (held.expires_ms <= now_ms) self.remove(i);
        };
    }
    pub fn deinit(self: *Cache) void {
        while (self.evict()) {}
    }
    /// No allocations or bitmap copies under the directory mutex. The cache
    /// must outlive its leases; expiry/eviction never invalidates borrowed facts.
    pub fn borrow(self: *Cache, identity: [32]u8, now_ms: u64) ?Lease {
        self.lock();
        defer self.mutex.unlock();
        self.expire(now_ms);
        self.tick +%= 1;
        for (&self.entries) |*entry| if (entry.*) |*held| {
            if (!std.mem.eql(u8, &held.key, &identity)) continue;
            held.age = self.tick;
            _ = held.shared.refs.fetchAdd(1, .monotonic);
            return .{ .cache = self, .shared = held.shared };
        };
        return null;
    }
    pub fn get(self: *Cache, a: A, identity: [32]u8, now_ms: u64) !?Facts {
        var lease = self.borrow(identity, now_ms) orelse return null;
        defer lease.deinit();
        return lease.facts().clone(a) catch null;
    }
    pub fn put(self: *Cache, identity: [32]u8, expires_ms: u64, now_ms: u64, facts: Facts) void {
        if (expires_ms <= now_ms) return;
        // Admit a conservative exact-clone bound BEFORE touching the directory.
        // A doomed oversized insertion cannot evict unrelated useful entries.
        const needed = @sizeOf(Shared) + facts.known.cloneStorageBound() + facts.matched.cloneStorageBound() + 32;
        if (needed > 2 * 1024 * 1024 or needed > self.budget.limit) return;
        self.lock();
        self.expire(now_ms);
        for (self.entries) |entry| if (entry) |held| {
            if (std.mem.eql(u8, &held.key, &identity) and held.shared.facts.complete) {
                self.mutex.unlock();
                return;
            }
        };
        // Pins and unpublished builders are not reclaimable. Reject doomed
        // admission before evicting even a single directory entry. Borrowing is
        // locked out here; concurrent pin releases can only add headroom.
        var available = @import("../sql/memory_budget.zig").headroom(self.budget.allocator()).?;
        for (self.entries) |entry| if (entry) |held| {
            if (held.shared.refs.load(.acquire) == 1) available += held.shared.block.len;
        };
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
        // The block is already charged to the process budget. Populate it while
        // other readers/writers proceed; FixedBufferAllocator cannot escape it.
        const shared: *Shared = @ptrCast(block.ptr);
        shared.* = .{ .block = block, .arena = .init(block[@sizeOf(Shared)..]), .facts = undefined };
        shared.facts = facts.clone(shared.arena.allocator()) catch {
            self.release(shared);
            return;
        };
        self.lock();
        defer self.mutex.unlock();
        self.tick +%= 1;
        for (self.entries, 0..) |entry, i| if (entry) |held| {
            if (std.mem.eql(u8, &held.key, &identity)) {
                if (held.shared.facts.complete) {
                    self.release(shared);
                    return;
                }
                self.remove(i);
                break;
            }
        };
        while (true) {
            for (&self.entries) |*entry| if (entry.* == null) {
                entry.* = .{ .key = identity, .shared = shared, .expires_ms = expires_ms, .age = self.tick };
                return;
            };
            if (!self.evict()) unreachable;
        }
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
