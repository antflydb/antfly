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

//! Bounded optional advisory statistics. Cached bytes never carry authority.
const std = @import("std");
const time = @import("antfly_platform").time;
const max_bytes = @import("antfly_local_sources").api_sql_maintenance_contract.max_bytes;
const A = std.mem.Allocator;
pub const Cache = struct {
    const Entry = struct { table_id: u64, schema_version: u32, expires_ns: u64, token: u64, bytes: []u8 };
    mutex: std.Io.Mutex = .init,
    entries: [32]?Entry = @splat(null),
    next: usize = 0,
    next_token: u64 = 1,
    pub fn deinit(self: *Cache, a: A) void {
        for (&self.entries) |*slot| if (slot.*) |entry| {
            a.free(entry.bytes);
            slot.* = null;
        };
    }
    pub fn get(self: *Cache, io: std.Io, a: A, table_id: u64, schema_version: u32) !?[]u8 {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const now = time.monotonicNs();
        for (self.entries) |slot| if (slot) |entry| {
            if (entry.table_id == table_id and entry.schema_version == schema_version and entry.expires_ns > now) return try a.dupe(u8, entry.bytes);
        };
        return null;
    }
    /// A temporary negative entry makes concurrent cold queries decline this
    /// optional estimate instead of starting the same owner fanout repeatedly.
    pub fn claim(self: *Cache, io: std.Io, a: A, table_id: u64, schema_version: u32) !?u64 {
        const owned = try a.dupe(u8, "null");
        var retained = false;
        defer if (!retained) a.free(owned);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const now = time.monotonicNs();
        for (self.entries) |slot| if (slot) |entry| {
            if (entry.table_id == table_id and entry.schema_version == schema_version and entry.expires_ns > now) return null;
        };
        if (self.next_token == std.math.maxInt(u64)) return null;
        const token = self.next_token;
        self.next_token += 1;
        const index = for (self.entries, 0..) |slot, i| {
            if (slot) |entry| if (entry.table_id == table_id) break i;
        } else self.next;
        self.next = (index + 1) % self.entries.len;
        if (self.entries[index]) |previous| a.free(previous.bytes);
        self.entries[index] = .{ .table_id = table_id, .schema_version = schema_version, .expires_ns = now +| std.time.ns_per_s, .token = token, .bytes = owned };
        retained = true;
        return token;
    }
    pub fn complete(self: *Cache, io: std.Io, a: A, table_id: u64, schema_version: u32, token: u64, bytes: []const u8) !void {
        return self.store(io, a, table_id, schema_version, token, bytes);
    }
    pub fn put(self: *Cache, io: std.Io, a: A, table_id: u64, schema_version: u32, bytes: []const u8) !void {
        return self.store(io, a, table_id, schema_version, null, bytes);
    }
    fn store(self: *Cache, io: std.Io, a: A, table_id: u64, schema_version: u32, token: ?u64, bytes: []const u8) !void {
        if (bytes.len > max_bytes) return;
        const owned = try a.dupe(u8, bytes);
        var retained = false;
        defer if (!retained) a.free(owned);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const index = for (self.entries, 0..) |slot, i| {
            if (slot) |entry| if (entry.table_id == table_id) break i;
        } else self.next;
        if (token) |expected| {
            const current = self.entries[index] orelse return;
            if (current.table_id != table_id or current.schema_version != schema_version or current.token != expected) return;
        }
        if (self.next_token == std.math.maxInt(u64)) return;
        const revision = self.next_token;
        self.next_token += 1;
        self.next = (index + 1) % self.entries.len;
        if (self.entries[index]) |previous| a.free(previous.bytes);
        self.entries[index] = .{ .table_id = table_id, .schema_version = schema_version, .expires_ns = time.monotonicNs() +| (if (std.mem.eql(u8, bytes, "null")) @as(u64, 1) else 30) * std.time.ns_per_s, .token = revision, .bytes = owned };
        retained = true;
    }
};

test "SQL statistics cache owns bytes and fences schema identity with bounded slots" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(a);
    var buffer = [_]u8{ '1', '2', '3' };
    try cache.put(std.testing.io, a, 1, 7, &buffer);
    @memset(&buffer, 'x');
    const found = (try cache.get(std.testing.io, a, 1, 7)).?;
    defer a.free(found);
    try std.testing.expectEqualStrings("123", found);
    try std.testing.expect((try cache.get(std.testing.io, a, 1, 8)) == null);
    try cache.put(std.testing.io, a, 1, 8, "null");
    try std.testing.expect((try cache.get(std.testing.io, a, 1, 7)) == null);
    for (2..35) |id| try cache.put(std.testing.io, a, id, 1, "null");
    try std.testing.expect((try cache.get(std.testing.io, a, 1, 8)) == null);
}

test "SQL statistics cache cold claims coalesce and cannot overwrite explicit refresh" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(a);
    const token = (try cache.claim(std.testing.io, a, 9, 1)).?;
    try std.testing.expect((try cache.claim(std.testing.io, a, 9, 1)) == null);
    try cache.put(std.testing.io, a, 9, 1, "fresh");
    try cache.complete(std.testing.io, a, 9, 1, token, "stale");
    const value = (try cache.get(std.testing.io, a, 9, 1)).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("fresh", value);
}
