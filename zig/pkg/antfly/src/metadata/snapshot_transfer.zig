// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded, immutable metadata snapshot transfers. A token names one encoded
//! view; consumers never combine pages from different projection generations.
const std = @import("std");
pub const path = "/internal/v1/snapshots/read";
pub const page_bytes = 512 * 1024;
pub const max_snapshot_bytes = 64 * 1024 * 1024;
pub const max_retained_bytes = 128 * 1024 * 1024;
pub const ttl_ns = 30 * std.time.ns_per_s;
pub const Request = struct {
    token: u64 = 0,
    offset: usize = 0,
    release: bool = false,
    control: bool = true,
    linearizable: bool = false,
};
pub const Page = struct { token: u64, total: usize, bytes: []u8 };
pub const Cache = struct {
    const Entry = struct { token: u64, bytes: []u8, touched: u64 };
    mutex: std.Io.Mutex = .init,
    capture_mutex: std.Io.Mutex = .init,
    entries: [32]?Entry = @splat(null),
    retained: usize = 0,
    next_token: u64 = 0,

    pub fn deinit(self: *Cache, alloc: std.mem.Allocator) void {
        for (&self.entries) |*entry| self.remove(alloc, entry);
    }
    fn remove(self: *Cache, alloc: std.mem.Allocator, entry: *?Entry) void {
        if (entry.*) |e| {
            self.retained -= e.bytes.len;
            alloc.free(e.bytes);
            entry.* = null;
        }
    }
    pub fn install(self: *Cache, alloc: std.mem.Allocator, bytes: []u8, now: u64) !u64 {
        if (bytes.len > max_snapshot_bytes) return error.ResourceRequestTooLarge;
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        for (&self.entries) |*entry| if (entry.*) |e| {
            if (now -| e.touched >= ttl_ns) self.remove(alloc, entry);
        };
        if (self.retained + bytes.len > max_retained_bytes) return error.ResourceTemporarilyUnavailable;
        for (&self.entries) |*entry| if (entry.* == null) {
            self.next_token = @max(self.next_token +| 1, now);
            entry.* = .{ .token = self.next_token, .bytes = bytes, .touched = now };
            self.retained += bytes.len;
            return self.next_token;
        };
        return error.ResourceTemporarilyUnavailable;
    }
    pub fn read(self: *Cache, alloc: std.mem.Allocator, output: std.mem.Allocator, request: Request, now: u64) !Page {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        for (&self.entries) |*slot| if (slot.*) |*entry| {
            if (entry.token != request.token) continue;
            if (request.release or now -| entry.touched >= ttl_ns) {
                self.remove(alloc, slot);
                return error.CatalogGenerationChanged;
            }
            if (request.offset > entry.bytes.len) return error.InvalidRequest;
            entry.touched = now;
            return .{ .token = entry.token, .total = entry.bytes.len, .bytes = try output.dupe(u8, entry.bytes[request.offset..@min(request.offset +| page_bytes, entry.bytes.len)]) };
        };
        return error.CatalogGenerationChanged;
    }
};

pub fn encode(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    // Size before allocation, so even an oversized diagnostic response cannot
    // allocate an unbounded encoded buffer or monopolize the transfer cache.
    var buffer: [4096]u8 = undefined;
    var count = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(value, .{}, &count.writer);
    const size = count.fullCount();
    if (size > max_snapshot_bytes) return error.ResourceRequestTooLarge;
    const bytes = try alloc.alloc(u8, @intCast(size));
    errdefer alloc.free(bytes);
    var writer = std.Io.Writer.fixed(bytes);
    try std.json.Stringify.value(value, .{}, &writer);
    return bytes;
}

test "system catalog snapshot transfer pages retain one view and reject expired tokens" {
    const a = std.testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(a);
    const bytes = try a.alloc(u8, page_bytes + 3);
    @memset(bytes, 'x');
    const token = try cache.install(a, bytes, 1);
    const first = try cache.read(a, a, .{ .token = token }, 2);
    defer a.free(first.bytes);
    const last = try cache.read(a, a, .{ .token = token, .offset = page_bytes }, 3);
    defer a.free(last.bytes);
    try std.testing.expectEqual(page_bytes, first.bytes.len);
    try std.testing.expectEqual(@as(usize, 3), last.bytes.len);
    try std.testing.expectError(error.InvalidRequest, cache.read(a, a, .{ .token = token, .offset = bytes.len + 1 }, 4));
    try std.testing.expectError(error.CatalogGenerationChanged, cache.read(a, a, .{ .token = token }, ttl_ns + 4));
    try std.testing.expectEqual(@as(usize, 0), cache.retained);
}

test "system catalog snapshot transfer capacity is released and encoding is exact" {
    const a = std.testing.allocator;
    const encoded = try encode(a, .{ .name = "line\n雪", .value = @as(u64, 42) });
    defer a.free(encoded);
    const expected = try std.json.Stringify.valueAlloc(a, .{ .name = "line\n雪", .value = @as(u64, 42) }, .{});
    defer a.free(expected);
    try std.testing.expectEqualStrings(expected, encoded);
    var cache: Cache = .{};
    defer cache.deinit(a);
    var first: u64 = 0;
    for (0..32) |i| {
        const bytes = try a.dupe(u8, "{}");
        const token = try cache.install(a, bytes, i + 1);
        if (i == 0) first = token;
    }
    const rejected = try a.dupe(u8, "{}");
    defer a.free(rejected);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.install(a, rejected, 33));
    try std.testing.expectError(error.CatalogGenerationChanged, cache.read(a, a, .{ .token = first, .release = true }, 34));
    const replacement = try a.dupe(u8, "{}");
    _ = try cache.install(a, replacement, 35);
    try std.testing.expectEqual(@as(usize, 64), cache.retained);
}
