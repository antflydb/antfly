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
    fn clone(self: Facts, a: A) !Facts {
        var known = try self.known.clone(a);
        errdefer known.deinit();
        return .{ .complete = self.complete, .known = known, .matched = try self.matched.clone(a) };
    }
};
pub const Cache = struct {
    const Entry = struct { key: [32]u8, facts: Facts, expires_ms: u64, age: u64 };
    budget: @import("../sql/memory_budget.zig"),
    entries: [128]?Entry = @splat(null),
    mutex: std.atomic.Mutex = .unlocked,
    tick: u64 = 0,
    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn remove(self: *Cache, i: usize) void {
        self.entries[i].?.facts.deinit();
        self.entries[i] = null;
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
    pub fn get(self: *Cache, a: A, identity: [32]u8, now_ms: u64) !?Facts {
        self.lock();
        defer self.mutex.unlock();
        self.expire(now_ms);
        self.tick +%= 1;
        for (&self.entries) |*entry| if (entry.*) |*held| {
            if (!std.mem.eql(u8, &held.key, &identity)) continue;
            held.age = self.tick;
            return held.facts.clone(a) catch null;
        };
        return null;
    }
    /// Only completed successful segment evaluations publish facts. Concurrent
    /// builders may replace one another's subsets; this only loses cache hits.
    pub fn put(self: *Cache, identity: [32]u8, expires_ms: u64, now_ms: u64, facts: Facts) void {
        if (expires_ms <= now_ms) return;
        self.lock();
        defer self.mutex.unlock();
        self.expire(now_ms);
        self.tick +%= 1;
        for (self.entries, 0..) |entry, i| if (entry) |held| {
            if (std.mem.eql(u8, &held.key, &identity)) {
                // A constrained concurrent builder cannot replace a completed
                // immutable proof with a smaller subset.
                if (held.facts.complete) return;
                self.remove(i);
                break;
            }
        };
        while (true) {
            var free: ?usize = null;
            for (self.entries, 0..) |entry, i| if (entry == null) {
                free = i;
                break;
            };
            if (free == null) {
                if (!self.evict()) return;
                continue;
            }
            const before = self.budget.live;
            var copy = facts.clone(self.budget.allocator()) catch {
                if (!self.evict()) return;
                continue;
            };
            // Prevent one large predicate from displacing the entire cache.
            if (self.budget.live - before > 2 * 1024 * 1024) {
                copy.deinit();
                return;
            }
            self.entries[free.?] = .{ .key = identity, .facts = copy, .expires_ms = expires_ms, .age = self.tick };
            return;
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
