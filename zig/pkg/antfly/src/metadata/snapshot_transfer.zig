// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded, immutable metadata snapshot transfers. A token names one encoded
//! view; consumers never combine pages from different projection generations.
const std = @import("std");
pub const path = "/internal/v1/snapshots/read";
pub const page_bytes = 512 * 1024;
pub const max_snapshot_bytes = 64 * 1024 * 1024;
pub const control_snapshot_bytes = 16 * 1024 * 1024;
pub const control_retained_bytes = 128 * 1024 * 1024;
pub const diagnostic_retained_bytes = 96 * 1024 * 1024;
pub const max_retained_bytes = control_retained_bytes + diagnostic_retained_bytes;
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
    const Entry = struct { token: u64, bytes: []u8, touched: u64, capturing: bool = false };
    mutex: std.Io.Mutex = .init,
    entries: [32]?Entry = @splat(null),
    retained: usize = 0,
    next_token: u64 = 0,
    reserved: usize = 0,
    snapshot_limit: usize = max_snapshot_bytes,
    retained_limit: usize = max_retained_bytes,

    /// Reserve both the token and worst-case encoding before taking a metadata
    /// lease. Busy readers fail immediately; they never queue behind a capture.
    pub fn reserve(self: *Cache, alloc: std.mem.Allocator, now: u64) !u64 {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        for (&self.entries) |*entry| if (entry.*) |e| {
            if (!e.capturing and now -| e.touched >= ttl_ns) self.remove(alloc, entry);
        };
        if (self.retained + self.reserved + self.snapshot_limit > self.retained_limit) return error.ResourceTemporarilyUnavailable;
        for (&self.entries) |*entry| if (entry.* == null) {
            // Low bit identifies the admission lane, including continuations
            // whose JSON omits the original control flag.
            self.next_token = (self.next_token + 2) & std.math.maxInt(u63);
            if (self.next_token < 2) self.next_token += 2;
            entry.* = .{ .token = self.next_token, .bytes = &.{}, .touched = now, .capturing = true };
            self.reserved += self.snapshot_limit;
            return self.next_token;
        };
        return error.ResourceTemporarilyUnavailable;
    }

    pub fn cancel(self: *Cache, alloc: std.mem.Allocator, token: u64) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        for (&self.entries) |*entry| if (entry.*) |e| {
            if (e.token == token and e.capturing) self.remove(alloc, entry);
        };
    }

    /// Transfers ownership only on success. A failed capture releases its
    /// reservation through cancel, so retries cannot exhaust slots or bytes.
    pub fn publish(self: *Cache, bytes: []u8, token: u64, now: u64) !void {
        if (bytes.len > self.snapshot_limit) return error.ResourceRequestTooLarge;
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        for (&self.entries) |*slot| if (slot.*) |*entry| {
            if (entry.token != token or !entry.capturing) continue;
            self.reserved -= self.snapshot_limit;
            self.retained += bytes.len;
            entry.* = .{ .token = token, .bytes = bytes, .touched = now };
            return;
        };
        return error.CatalogGenerationChanged;
    }

    pub fn deinit(self: *Cache, alloc: std.mem.Allocator) void {
        for (&self.entries) |*entry| self.remove(alloc, entry);
    }
    fn remove(self: *Cache, alloc: std.mem.Allocator, entry: *?Entry) void {
        if (entry.*) |e| {
            if (e.capturing) self.reserved -= self.snapshot_limit;
            self.retained -= e.bytes.len;
            alloc.free(e.bytes);
            entry.* = null;
        }
    }
    pub fn install(self: *Cache, alloc: std.mem.Allocator, bytes: []u8, now: u64) !u64 {
        const token = try self.reserve(alloc, now);
        errdefer self.cancel(alloc, token);
        try self.publish(bytes, token, now);
        return token;
    }
    pub fn read(self: *Cache, alloc: std.mem.Allocator, output: std.mem.Allocator, request: Request, now: u64) !Page {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        for (&self.entries) |*slot| if (slot.*) |*entry| {
            if (entry.token != request.token or entry.capturing) continue;
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
    return encodeBounded(alloc, value, max_snapshot_bytes);
}

pub fn encodeBounded(alloc: std.mem.Allocator, value: anytype, limit: usize) ![]u8 {
    // Size before allocation, so even an oversized diagnostic response cannot
    // allocate an unbounded encoded buffer or monopolize the transfer cache.
    var buffer: [4096]u8 = undefined;
    var count = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(value, .{}, &count.writer);
    const size = count.fullCount();
    if (size > limit) return error.ResourceRequestTooLarge;
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

/// Control admission allows up to eight concurrent maximum-size captures.
/// Default clusters have several independent background control readers; two
/// reservations rejected ordinary probes even with tiny retained views. Keep
/// their 128 MiB budget independent of the 96 MiB diagnostic lane. Retained
/// encodings plus in-flight encoding reservations are bounded by 224 MiB.
pub const Transfers = struct {
    control: Cache = .{ .snapshot_limit = control_snapshot_bytes, .retained_limit = control_retained_bytes },
    diagnostic: Cache = .{ .next_token = 1, .retained_limit = diagnostic_retained_bytes },
    pub fn lane(self: *Transfers, request: Request) *Cache {
        if (request.token != 0) return if (request.token & 1 == 0) &self.control else &self.diagnostic;
        return if (request.control) &self.control else &self.diagnostic;
    }
    pub fn deinit(self: *Transfers, alloc: std.mem.Allocator) void {
        self.control.deinit(alloc);
        self.diagnostic.deinit(alloc);
    }
};

test "system catalog diagnostic reservations cannot block control and failure releases capacity" {
    const a = std.testing.allocator;
    var transfers: Transfers = .{};
    defer transfers.deinit(a);
    const diagnostic = transfers.lane(.{ .control = false });
    const token = try diagnostic.reserve(a, 1);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, diagnostic.reserve(a, 2));
    const control = transfers.lane(.{});
    const control_token = try control.reserve(a, 3);
    try std.testing.expect(transfers.lane(.{ .token = token }) == diagnostic);
    try std.testing.expect(transfers.lane(.{ .token = control_token }) == control);
    try std.testing.expect(transfers.lane(.{ .linearizable = true }) == control);
    // Failed/oversized captures reclaim their reservation without publishing.
    try std.testing.expectError(error.ResourceRequestTooLarge, encodeBounded(a, "too large", 1));
    diagnostic.cancel(a, token);
    try std.testing.expectEqual(@as(usize, 0), diagnostic.reserved);
    const replacement = try diagnostic.reserve(a, 4);
    const bytes = try a.dupe(u8, "{}");
    try diagnostic.publish(bytes, replacement, 5);
    try std.testing.expectEqual(@as(usize, 2), diagnostic.retained);
    try std.testing.expectEqual(@as(usize, 0), diagnostic.reserved);
    try std.testing.expectError(error.CatalogGenerationChanged, diagnostic.read(a, a, .{ .token = replacement, .release = true }, 6));
    control.cancel(a, control_token);
}

test "system catalog control admission accommodates cluster fan-in under diagnostics" {
    const a = std.testing.allocator;
    var transfers: Transfers = .{};
    defer transfers.deinit(a);
    const diagnostic = try transfers.diagnostic.reserve(a, 1);
    defer transfers.diagnostic.cancel(a, diagnostic);
    // An outstanding page must not consume a whole capture reservation.
    _ = try transfers.control.install(a, try a.dupe(u8, "{}"), 2);
    var tokens: [7]u64 = undefined;
    for (&tokens) |*token| token.* = try transfers.control.reserve(a, 3);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, transfers.control.reserve(a, 4));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, transfers.diagnostic.reserve(a, 4));
    try std.testing.expect(transfers.control.retained + transfers.control.reserved + transfers.diagnostic.reserved <= max_retained_bytes);
    for (tokens) |token| transfers.control.cancel(a, token);
    try std.testing.expectEqual(@as(usize, 0), transfers.control.reserved);
}
