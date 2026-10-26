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

//! Immutable execution policy, qualified by exact content. Cache eviction drops
//! only the cache reference; active readers retain their own lease. No grants,
//! documents, model outputs, or mutable decision state live here.
const std = @import("std");
const builtin = @import("builtin");
const curation = @import("curation.zig");
const A = std.mem.Allocator;
const Hash = @import("antfly_hash").Sha256;
// Cached policies and active leases outlive individual operations. Keep their
// allocator in the process/instance domain, including threadless WASM.
const heap = if (builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64)
    std.heap.wasm_allocator
else if (builtin.single_threaded)
    std.heap.page_allocator
else
    std.heap.smp_allocator;
const Policy = struct {
    references: std.atomic.Value(usize) = .init(1),
    arena: std.heap.ArenaAllocator,
    digest: [32]u8,
    bytes: usize,
    previous: ?*Policy = null,
    next: ?*Policy = null,
    targets: std.StringHashMapUnmanaged([]const u8),
    aliases: []const curation.Redirect,
    redirects: curation.RedirectIndex,
    index: curation.Index,
    fn release(self: *Policy) void {
        if (self.references.fetchSub(1, .acq_rel) == 1) {
            const allocator = self.arena.child_allocator;
            self.arena.deinit();
            allocator.destroy(self);
        }
    }
};
pub const Lease = struct {
    policy: *Policy,
    pub fn aliases(self: Lease) []const curation.Redirect {
        return self.policy.aliases;
    }
    pub fn target(self: Lease, key: []const u8) ?[]const u8 {
        return self.policy.targets.get(key);
    }
    pub fn redirects(self: Lease) curation.RedirectIndex {
        return self.policy.redirects;
    }
    pub fn index(self: Lease) curation.Index {
        return self.policy.index;
    }
    pub fn release(self: Lease) void {
        self.policy.release();
    }
};
const Cache = struct {
    allocator: A = heap,
    mutex: std.atomic.Mutex = .unlocked,
    preparing: [64]std.atomic.Mutex = @splat(.unlocked),
    entries: std.AutoHashMapUnmanaged([32]u8, *Policy) = .empty,
    oldest: ?*Policy = null,
    newest: ?*Policy = null,
    bytes: usize = 0,
    const max_bytes = 32 * 1024 * 1024;
    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn unlink(self: *Cache, p: *Policy) void {
        if (p.previous) |prev| prev.next = p.next else self.oldest = p.next;
        if (p.next) |next| next.previous = p.previous else self.newest = p.previous;
    }
    fn touch(self: *Cache, p: *Policy) void {
        p.previous = self.newest;
        p.next = null;
        if (self.newest) |last| last.next = p else self.oldest = p;
        self.newest = p;
    }
    fn find(self: *Cache, digest: [32]u8) ?Lease {
        const p = self.entries.get(digest) orelse return null;
        self.unlink(p);
        self.touch(p);
        _ = p.references.fetchAdd(1, .monotonic);
        return .{ .policy = p };
    }
    fn deinit(self: *Cache) void {
        var item = self.oldest;
        self.oldest = null;
        self.newest = null;
        self.bytes = 0;
        self.entries.deinit(self.allocator);
        self.entries = .empty;
        while (item) |p| {
            item = p.next;
            p.release();
        }
    }
    fn acquire(self: *Cache, aliases: []const curation.Redirect, merges: []const curation.Redirect, suppress: []const u8, state: []const u8) !Lease {
        var hash = Hash.init(.{});
        hashBytes(&hash, suppress);
        hashBytes(&hash, state);
        for ([_][]const curation.Redirect{ aliases, merges }) |items| {
            var count: [8]u8 = undefined;
            std.mem.writeInt(u64, &count, items.len, .little);
            hash.update(&count);
            for (items) |item| {
                hashBytes(&hash, item.key);
                hashBytes(&hash, item.target);
            }
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return self.acquireKey(digest, aliases, merges, suppress, state);
    }
    fn acquireKey(self: *Cache, digest: [32]u8, aliases: []const curation.Redirect, merges: []const curation.Redirect, suppress: []const u8, state: []const u8) !Lease {
        self.lock();
        const hit = self.find(digest);
        self.mutex.unlock();
        if (hit) |lease| return lease;
        const stripe = &self.preparing[digest[0] % self.preparing.len];
        @import("antfly_platform").sync.lockYielding(stripe);
        defer stripe.unlock();
        self.lock();
        if (self.find(digest)) |lease| {
            self.mutex.unlock();
            return lease;
        }
        self.mutex.unlock();
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const a = arena.allocator();
        var index = try curation.Index.init(a, suppress, state);
        var curated: std.StringHashMapUnmanaged(void) = .empty;
        for (merges) |item| try curated.put(a, item.key, {});
        var effective: std.ArrayListUnmanaged(curation.Redirect) = .empty;
        for (aliases) |item| if (!index.removed_aliases.contains(item.key) and !curated.contains(item.key)) try effective.append(a, .{ .key = try a.dupe(u8, item.key), .target = try a.dupe(u8, item.target) });
        for (merges) |item| try effective.append(a, .{ .key = try a.dupe(u8, item.key), .target = try a.dupe(u8, item.target) });
        const redirects = try curation.RedirectIndex.init(a, effective.items);
        try index.bindRedirects(a, redirects);
        var targets: std.StringHashMapUnmanaged([]const u8) = .empty;
        for (effective.items) |edge| try targets.put(a, edge.key, edge.target);
        const policy = try self.allocator.create(Policy);
        const bytes = arena.queryCapacity() + @sizeOf(Policy) + 128;
        policy.* = .{ .arena = arena, .aliases = effective.items, .redirects = redirects, .index = index, .targets = targets, .digest = digest, .bytes = bytes };
        self.lock();
        var retired: ?*Policy = null;
        if (bytes <= max_bytes) {
            self.entries.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                self.mutex.unlock();
                self.allocator.destroy(policy);
                return err;
            };
            while (self.bytes + bytes > max_bytes) {
                const victim = self.oldest.?;
                self.unlink(victim);
                _ = self.entries.remove(victim.digest);
                self.bytes -= victim.bytes;
                victim.next = retired;
                retired = victim;
            }
            self.entries.putAssumeCapacity(digest, policy);
            self.touch(policy);
            self.bytes += bytes;
            _ = policy.references.fetchAdd(1, .monotonic);
        }
        self.mutex.unlock();
        while (retired) |victim| {
            retired = victim.next;
            victim.release();
        }
        return .{ .policy = policy };
    }
};
fn hashBytes(hash: *Hash, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}
var cache: Cache = .{};
pub fn acquire(aliases: []const curation.Redirect, merges: []const curation.Redirect, suppress: []const u8, state: []const u8) !Lease {
    return cache.acquire(aliases, merges, suppress, state);
}

/// Identity is the immutable alias/suppression declaration digest verified by
/// admission. Dynamic namespace state is never qualified by this identity.
pub fn acquireDeclared(identity: []const u8, aliases: []const curation.Redirect, suppress: []const u8) !Lease {
    var hash = Hash.init(.{});
    hashBytes(&hash, "configured-resolver-policy-v1");
    hashBytes(&hash, identity);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return cache.acquireKey(digest, aliases, &.{}, suppress, "");
}

test "compiled curation leases retain immutable policy across eviction and content changes" {
    var local: Cache = .{ .allocator = std.testing.allocator };
    defer local.deinit();
    const first = try local.acquire(&.{.{ .key = "a", .target = "b" }}, &.{}, "", "");
    defer first.release();
    const again = try local.acquire(&.{.{ .key = "a", .target = "b" }}, &.{}, "", "");
    defer again.release();
    try std.testing.expect(first.policy == again.policy);
    const changed = try local.acquire(&.{.{ .key = "a", .target = "b" }}, &.{}, "", "{\"removed_aliases\":[\"a\"]}");
    defer changed.release();
    try std.testing.expectEqual(@as(usize, 0), changed.aliases().len);
    for (0..33) |i| {
        var buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        const lease = try local.acquire(&.{.{ .key = key, .target = "b" }}, &.{}, "", "");
        lease.release();
    }
    try std.testing.expectEqualStrings("b", first.redirects().canonical("a"));
    try std.testing.expect(local.bytes <= Cache.max_bytes);
    try std.testing.expectEqual(@as(usize, 35), local.entries.count());
}

test "compiled curation policy cache respects allocation failure and pinned lease lifetime" {
    const Scenario = struct {
        fn run(a: A) !void {
            var local: Cache = .{ .allocator = a };
            defer local.deinit();
            const lease = try local.acquire(&.{.{ .key = "a", .target = "b" }}, &.{}, "", "");
            defer lease.release();
            local.deinit();
            try std.testing.expectEqual(@as(usize, 0), local.bytes);
            // The allocation authority travels with the immutable policy;
            // eviction can release the cache without invalidating this lease.
            try std.testing.expectEqualStrings("b", lease.redirects().canonical("a"));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}
