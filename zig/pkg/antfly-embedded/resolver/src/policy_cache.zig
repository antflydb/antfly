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
const curation = @import("curation.zig");
const A = std.mem.Allocator;
const builtin = @import("builtin");
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
const Entry = struct { digest: [32]u8, policy: *Policy, bytes: usize };
const Cache = struct {
    // This allocator must outlive every lease, even after cache eviction.
    allocator: A = heap,
    mutex: std.atomic.Mutex = .unlocked,
    entries: [32]?Entry = @splat(null),
    next: usize = 0,
    bytes: usize = 0,
    const max_bytes = 32 * 1024 * 1024;
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn find(self: *Cache, digest: [32]u8) ?Lease {
        for (self.entries) |entry| if (entry) |value| if (std.mem.eql(u8, &value.digest, &digest)) {
            _ = value.policy.references.fetchAdd(1, .monotonic);
            return .{ .policy = value.policy };
        };
        return null;
    }
    fn remove(self: *Cache, i: usize) void {
        if (self.entries[i]) |entry| {
            self.bytes -= entry.bytes;
            entry.policy.release();
            self.entries[i] = null;
        }
    }
    fn acquire(self: *Cache, aliases: []const curation.Redirect, merges: []const curation.Redirect, suppress: []const u8, state: []const u8) !Lease {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
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
        const policy = try self.allocator.create(Policy);
        policy.* = .{ .arena = arena, .aliases = effective.items, .redirects = redirects, .index = index };
        const bytes = arena.queryCapacity();
        self.lock();
        defer self.mutex.unlock();
        if (self.find(digest)) |lease| {
            policy.release();
            return lease;
        }
        if (bytes <= max_bytes) {
            while (self.bytes + bytes > max_bytes) {
                self.remove(self.next);
                self.next = (self.next + 1) % self.entries.len;
            }
            self.remove(self.next);
            self.entries[self.next] = .{ .digest = digest, .policy = policy, .bytes = bytes };
            self.bytes += bytes;
            self.next = (self.next + 1) % self.entries.len;
            _ = policy.references.fetchAdd(1, .monotonic);
        }
        return .{ .policy = policy };
    }
};
fn hashBytes(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}
var cache: Cache = .{};
pub fn acquire(aliases: []const curation.Redirect, merges: []const curation.Redirect, suppress: []const u8, state: []const u8) !Lease {
    return cache.acquire(aliases, merges, suppress, state);
}

test "compiled curation leases retain immutable policy across eviction and content changes" {
    var local: Cache = .{ .allocator = std.testing.allocator };
    defer for (0..local.entries.len) |i| local.remove(i);
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
}

test "compiled curation policy cache respects allocation failure and pinned lease lifetime" {
    const Scenario = struct {
        fn run(a: A) !void {
            var local: Cache = .{ .allocator = a };
            defer for (0..local.entries.len) |i| local.remove(i);
            const lease = try local.acquire(&.{.{ .key = "a", .target = "b" }}, &.{}, "", "");
            defer lease.release();
            for (0..local.entries.len) |i| local.remove(i);
            try std.testing.expectEqual(@as(usize, 0), local.bytes);
            // The allocation authority travels with the immutable policy;
            // eviction can release the cache without invalidating this lease.
            try std.testing.expectEqualStrings("b", lease.redirects().canonical("a"));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}
