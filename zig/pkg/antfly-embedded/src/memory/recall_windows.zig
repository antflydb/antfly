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

//! Bounded, data-only ranked windows. A cursor's content digest permits safe
//! reconstruction after eviction or ingress failover; no grant is cached here.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_candidates = 4096;
pub const page_size = 128;
pub const ttl_ms: i64 = 300000;
pub const Window = struct { keys: []const []const u8, vector: ?[]const f32, truncated: bool };
const Entry = struct { arena: std.heap.ArenaAllocator, digest: [32]u8, expires: i64, window: Window, bytes: usize };
const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: [32]?Entry = @splat(null),
    hand: usize = 0,
    bytes: usize = 0,
    const max_bytes = 32 * 1024 * 1024;
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn remove(self: *Cache, slot: usize) void {
        if (self.entries[slot]) |*entry| {
            self.bytes -= entry.bytes;
            entry.arena.deinit();
            self.entries[slot] = null;
        }
    }
    fn clone(a: A, window: Window) !Window {
        const keys = try a.alloc([]const u8, window.keys.len);
        for (keys, window.keys) |*out, key| out.* = try a.dupe(u8, key);
        return .{ .keys = keys, .vector = if (window.vector) |v| try a.dupe(f32, v) else null, .truncated = window.truncated };
    }
    fn get(self: *Cache, a: A, digest: [32]u8, now: i64) !?Window {
        self.lock();
        defer self.mutex.unlock();
        for (&self.entries, 0..) |*slot, i| if (slot.*) |*entry| {
            if (entry.expires <= now) {
                self.remove(i);
                continue;
            }
            if (std.mem.eql(u8, &entry.digest, &digest)) return try clone(a, entry.window);
        };
        return null;
    }
    fn put(self: *Cache, digest: [32]u8, now: i64, window: Window) !void {
        if (window.keys.len > max_candidates) return error.InvalidMemoryCursor;
        var arena = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
        errdefer arena.deinit();
        const owned = try clone(arena.allocator(), window);
        const bytes = arena.queryCapacity();
        if (bytes > max_bytes) return error.InvalidMemoryCursor;
        self.lock();
        defer self.mutex.unlock();
        while (self.bytes + bytes > max_bytes) {
            self.remove(self.hand);
            self.hand = (self.hand + 1) % self.entries.len;
        }
        self.remove(self.hand);
        self.entries[self.hand] = .{ .arena = arena, .digest = digest, .expires = now +| ttl_ms, .window = owned, .bytes = bytes };
        self.bytes += bytes;
        self.hand = (self.hand + 1) % self.entries.len;
    }
};
var cache: Cache = .{};
pub fn get(a: A, digest: [32]u8, now: i64) !?Window {
    return cache.get(a, digest, now);
}
pub fn put(digest: [32]u8, now: i64, window: Window) !void {
    return cache.put(digest, now, window);
}
pub fn fingerprint(a: A, scope: []const u8, window: Window) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ scope, window }, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}

test "ranked recall windows own their data, expire, and evict within a fixed budget" {
    var local: Cache = .{};
    defer for (0..local.entries.len) |i| local.remove(i);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const window: Window = .{ .keys = &.{ "a", "b" }, .vector = &.{ 0.8, 0.6 }, .truncated = false };
    const digest = try fingerprint(a, "space/principal/config", window);
    try local.put(digest, 10, window);
    const copy = (try local.get(a, digest, 11)).?;
    try std.testing.expectEqualStrings("a", copy.keys[0]);
    try std.testing.expectEqualSlices(f32, window.vector.?, copy.vector.?);
    try std.testing.expect((try local.get(a, digest, 10 + ttl_ms)) == null);
    for (0..33) |i| {
        const next = try fingerprint(a, try std.fmt.allocPrint(a, "{d}", .{i}), window);
        try local.put(next, 20, window);
    }
    try std.testing.expect(local.bytes <= Cache.max_bytes);
    try std.testing.expect((try local.get(a, digest, 21)) == null);
    const other = try fingerprint(a, "other space", window);
    try std.testing.expect(!std.mem.eql(u8, &digest, &other));
}

pub fn resetForTesting() void {
    if (!@import("builtin").is_test) @compileError("test-only cache reset");
    cache.lock();
    defer cache.mutex.unlock();
    for (0..cache.entries.len) |i| cache.remove(i);
}
