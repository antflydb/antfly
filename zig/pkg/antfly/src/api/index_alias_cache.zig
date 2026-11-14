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

//! Bounded query projection of immutable catalog bytes. Readiness evidence and
//! retry receipts never enter the query parser's allocation path on cache hits.
const std = @import("std");
const sync = @import("antfly_platform").sync;
const aliases = @import("antfly_local_sources").api_index_metadata.aliases;
const A = std.mem.Allocator;
const V = std.json.Value;

pub const Cache = struct {
    const Entry = struct {
        digest: [32]u8,
        bytes: []u8,
        references: std.atomic.Value(usize) = .init(1),
        fn release(self: *@This(), a: A) void {
            if (self.references.fetchSub(1, .acq_rel) == 1) {
                a.free(self.bytes);
                a.destroy(self);
            }
        }
    };
    const max_bytes = 1024 * 1024;
    mutex: std.atomic.Mutex = .unlocked,
    entries: [32]?*Entry = @splat(null),
    next: usize = 0,
    retained_bytes: usize = 0,

    pub fn deinit(self: *Cache, a: A) void {
        for (self.entries) |entry| if (entry) |value| value.release(a);
        self.* = .{};
    }

    pub fn project(self: *Cache, cache_allocator: A, out: A, bytes: []const u8) ![]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const hit: ?*Entry = found: {
            sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            for (self.entries) |entry| if (entry) |value| {
                if (std.mem.eql(u8, &value.digest, &digest)) {
                    _ = value.references.fetchAdd(1, .monotonic);
                    break :found value;
                }
            };
            break :found null;
        };
        if (hit) |value| {
            defer value.release(cache_allocator);
            return out.dupe(u8, value.bytes);
        }
        const compact = try projection(out, bytes);
        // Admission failure affects only the cache, never query availability.
        if (compact.len > max_bytes) return compact;
        const retained = cache_allocator.dupe(u8, compact) catch return compact;
        const new_entry = cache_allocator.create(Entry) catch {
            cache_allocator.free(retained);
            return compact;
        };
        new_entry.* = .{ .digest = digest, .bytes = retained };
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries) |entry| if (entry) |value| {
            if (std.mem.eql(u8, &value.digest, &digest)) {
                new_entry.release(cache_allocator);
                return compact;
            }
        };
        while (true) {
            if (self.entries[self.next]) |entry| {
                self.retained_bytes -= entry.bytes.len;
                entry.release(cache_allocator);
                self.entries[self.next] = null;
            }
            if (self.retained_bytes + retained.len <= max_bytes) break;
            self.next = (self.next + 1) % self.entries.len;
        }
        self.entries[self.next] = new_entry;
        self.retained_bytes += retained.len;
        self.next = (self.next + 1) % self.entries.len;
        return compact;
    }
};

fn projection(out: A, bytes: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(out);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a, bytes, .{});
    if (root != .object) return error.InvalidTableIndexMetadata;
    const declarations = root.object.get(aliases.key) orelse return out.dupe(u8, "{}");
    if (declarations != .object) return error.InvalidTableIndexMetadata;
    var compact: V = .{ .object = .empty };
    var configurations = root.object.iterator();
    while (configurations.next()) |entry| {
        if (@import("antfly_local_sources").api_index_metadata.isOpaqueEntry(entry.key_ptr.*)) continue;
        try compact.object.put(a, entry.key_ptr.*, entry.value_ptr.*);
    }
    var targets: V = .{ .object = .empty };
    var it = declarations.object.iterator();
    while (it.next()) |entry| {
        const target = try aliases.resolve(root, entry.key_ptr.*);
        var pointer: V = .{ .object = .empty };
        try pointer.object.put(a, "index", .{ .string = target });
        try targets.object.put(a, entry.key_ptr.*, pointer);
        try compact.object.put(a, target, root.object.get(target) orelse return error.InvalidTableIndexMetadata);
    }
    try compact.object.put(a, aliases.key, targets);
    return std.json.Stringify.valueAlloc(out, compact, .{});
}

test "index alias query cache excludes proofs and receipts and fences catalog changes" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(a);
    const first = "{\"a\":{\"type\":\"graph\"},\"b\":{\"type\":\"graph\"},\"_index_aliases\":{\"live\":{\"index\":\"a\",\"readiness\":{\"complete\":true}}},\"_index_alias_receipts\":{\"irrelevant\":true}}";
    const second = "{\"a\":{\"type\":\"graph\"},\"b\":{\"type\":\"graph\"},\"_index_aliases\":{\"live\":{\"index\":\"b\"}}}";
    const old = try cache.project(a, a, first);
    defer a.free(old);
    const hit = try cache.project(a, a, first);
    defer a.free(hit);
    try std.testing.expectEqualStrings(old, hit);
    try std.testing.expectEqual(old.len, cache.retained_bytes);
    try std.testing.expect(std.mem.indexOf(u8, old, "readiness") == null);
    try std.testing.expect(std.mem.indexOf(u8, old, aliases.receipts_key) == null);
    const new = try cache.project(a, a, second);
    defer a.free(new);
    var parsed = try std.json.parseFromSlice(V, a, old, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("a", try aliases.resolve(parsed.value, "live"));
    var updated = try std.json.parseFromSlice(V, a, new, .{});
    defer updated.deinit();
    try std.testing.expectEqualStrings("b", try aliases.resolve(updated.value, "live"));
}

test "index alias cached lookup allocates independently of retry history size" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(a);
    const evidence = try a.alloc(u8, 64 * 1024);
    defer a.free(evidence);
    @memset(evidence, 'x');
    const catalog = try std.fmt.allocPrint(a, "{{\"a\":{{\"type\":\"graph\"}},\"_index_aliases\":{{\"live\":{{\"index\":\"a\"}}}},\"_index_alias_receipts\":\"{s}\"}}", .{evidence});
    defer a.free(catalog);
    const miss = try cache.project(a, a, catalog);
    defer a.free(miss);
    var bytes: [128]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&bytes);
    const hit = try cache.project(a, fixed.allocator(), catalog);
    try std.testing.expectEqualStrings(miss, hit);
    try std.testing.expect(cache.retained_bytes < 128);
}
