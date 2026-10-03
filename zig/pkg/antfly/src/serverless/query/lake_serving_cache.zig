// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Server-owned version-keyed range cache. The mutex protects memory only;
//! provider I/O runs outside it. Bytes never live in a request allocator.
const std = @import("std");
const parquet = @import("lake_parquet_rowgroup.zig");
const ranges = @import("lake_range_io.zig");
const Context = @import("lake_read_context.zig").Context;
const ObjectReader = @import("lake_object_reader.zig").ObjectStorageRangeReader;
const Allocator = std.mem.Allocator;
pub const Cache = struct {
    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.StringHashMapUnmanaged(Entry) = .empty,
    max_bytes: usize = 64 * 1024 * 1024,
    max_entries: usize = 4096,
    stats: Stats = .{},
    tick: u64 = 0,
    const Entry = struct { bytes: []u8, touched: u64 };
    pub const Stats = struct { hits: u64 = 0, misses: u64 = 0, stored_bytes: usize = 0, evictions: u64 = 0 };
    pub fn init(alloc: Allocator) Cache {
        return .{ .alloc = alloc };
    }
    pub fn deinit(self: *Cache) void {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.bytes);
        }
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }
    pub fn snapshot(self: *Cache) Stats {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return self.stats;
    }
    fn lookup(self: *Cache, alloc: Allocator, key: []const u8) !?[]u8 {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.entries.getPtr(key)) |entry| {
            self.tick +%= 1;
            entry.touched = self.tick;
            self.stats.hits += 1;
            return try alloc.dupe(u8, entry.bytes);
        }
        self.stats.misses += 1;
        return null;
    }
    fn store(self: *Cache, key: []const u8, bytes: []const u8) !void {
        if (bytes.len > self.max_bytes or self.max_entries == 0) return;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        // Concurrent misses may fetch the same immutable object range.
        if (self.entries.contains(key)) return;
        while (self.entries.count() != 0 and (self.stats.stored_bytes > self.max_bytes - bytes.len or self.entries.count() >= self.max_entries)) {
            var oldest: ?[]const u8 = null;
            var touched: u64 = std.math.maxInt(u64);
            var iter = self.entries.iterator();
            while (iter.next()) |entry| if (oldest == null or entry.value_ptr.touched < touched) {
                oldest = entry.key_ptr.*;
                touched = entry.value_ptr.touched;
            };
            const removed = self.entries.fetchRemove(oldest.?).?;
            self.stats.stored_bytes -= removed.value.bytes.len;
            self.stats.evictions += 1;
            self.alloc.free(removed.key);
            self.alloc.free(removed.value.bytes);
        }
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        const owned_bytes = try self.alloc.dupe(u8, bytes);
        errdefer self.alloc.free(owned_bytes);
        self.tick +%= 1;
        try self.entries.put(self.alloc, owned_key, .{ .bytes = owned_bytes, .touched = self.tick });
        self.stats.stored_bytes += bytes.len;
    }
};
pub const Reader = struct {
    cache: *Cache,
    base: ObjectReader,
    scope: [32]u8,
    context: Context,
    pub fn reader(self: *Reader) parquet.ObjectRangeReader {
        return .{ .ctx = self, .read_range_alloc = readRange, .read_planned_range_alloc = readPlanned };
    }
    fn readRange(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        return self.base.parquetReader().readAlloc(alloc, bucket, key, offset, len);
    }
    fn readPlanned(raw: *anyopaque, alloc: Allocator, read: ranges.RangeRead) ![]u8 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        try read.validate();
        // Unversioned reads must always reach the provider.
        if (read.object.version.etag.len == 0 and read.object.version.version_id.len == 0) return self.base.parquetReader().readPlannedAlloc(alloc, read);
        const range_key = try read.cacheKeyAlloc(alloc);
        defer alloc.free(range_key);
        const key = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ std.fmt.bytesToHex(self.scope, .lower), range_key });
        defer alloc.free(key);
        if (try self.cache.lookup(alloc, key)) |bytes| {
            errdefer alloc.free(bytes);
            try self.context.ensureActive();
            return bytes;
        }
        const bytes = try self.base.parquetReader().readPlannedAlloc(alloc, read);
        errdefer alloc.free(bytes);
        try self.context.ensureActive();
        // Cache admission is optional and never turns a successful read into
        // an allocation failure in a long-lived shared owner.
        self.cache.store(key, bytes) catch {};
        return bytes;
    }
};
test "external lake shared cache bounds memory and segregates versions and credential scopes" {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc);
    defer cache.deinit();
    cache.max_bytes = 6;
    try cache.store("scope-a:v1", "abc");
    const first = (try cache.lookup(alloc, "scope-a:v1")).?;
    defer alloc.free(first);
    try std.testing.expectEqualStrings("abc", first);
    try std.testing.expect((try cache.lookup(alloc, "scope-b:v1")) == null);
    try std.testing.expect((try cache.lookup(alloc, "scope-a:v2")) == null);
    try cache.store("scope-a:v2", "def");
    try cache.store("scope-b:v1", "ghi");
    const stats = cache.snapshot();
    try std.testing.expectEqual(@as(usize, 6), stats.stored_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.evictions);
}
