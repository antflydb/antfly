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

//! Serialized-owner cache of decoded immutable generations. Fixed backing
//! buffers bound retained allocations; sessions always receive owned clones.
const std = @import("std");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Cache = struct {
    const entry_bytes = 256 * 1024;
    const Entry = struct { bytes: []u8, value: types.Manifest, used: u64 };
    entries: [2]?Entry = .{ null, null },
    tick: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    pub fn deinit(self: *Cache, a: A) void {
        for (&self.entries) |*entry| if (entry.*) |owned| {
            a.free(owned.bytes);
            entry.* = null;
        };
    }
    pub fn retainedBytes(self: *const Cache) usize {
        var bytes: usize = 0;
        for (self.entries) |entry| if (entry) |owned| {
            bytes += owned.bytes.len;
        };
        return bytes;
    }
    pub fn get(self: *Cache, a: A, namespace: []const u8, version: u64) !?types.Manifest {
        self.tick +%= 1;
        for (&self.entries) |*entry| if (entry.*) |*owned| {
            if (owned.value.version == version and std.mem.eql(u8, owned.value.namespace, namespace)) {
                const result = try types.cloneManifest(a, owned.value);
                owned.used = self.tick;
                self.hits +%= 1;
                return result;
            }
        };
        self.misses +%= 1;
        return null;
    }
    pub fn remember(self: *Cache, a: A, value: types.Manifest) void {
        var selected: usize = 0;
        for (self.entries, 0..) |entry, i| {
            if (entry == null) {
                selected = i;
                break;
            }
            if (entry.?.used < self.entries[selected].?.used) selected = i;
        }
        // Evict before allocating so peak backing storage remains 512 KiB.
        if (self.entries[selected]) |old| a.free(old.bytes);
        self.entries[selected] = null;
        const bytes = a.alloc(u8, entry_bytes) catch return;
        var fixed = std.heap.FixedBufferAllocator.init(bytes);
        const copy = types.cloneManifest(fixed.allocator(), value) catch {
            a.free(bytes);
            return;
        };
        self.tick +%= 1;
        self.entries[selected] = .{ .bytes = bytes, .value = copy, .used = self.tick };
    }
};

test "serverless decoded generations cache clones ownership and isolates namespaces" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(a);
    const manifest = types.Manifest{ .namespace = "a", .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 2, .stats = .{}, .artifacts = &.{} };
    cache.remember(a, manifest);
    var hit = (try cache.get(a, "a", 1)).?;
    defer hit.deinit(a);
    try std.testing.expectEqual(@as(u64, 2), hit.wal_end_lsn);
    try std.testing.expect((try cache.get(a, "b", 1)) == null);
    cache.remember(a, manifest);
    cache.remember(a, manifest);
    try std.testing.expect(cache.retainedBytes() <= 512 * 1024);
}
