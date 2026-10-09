// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded sequential input and owned immutable snapshots. Identity/context are
//! borrowed from the caller; snapshot addresses remain stable until destruction.
const std = @import("std");
const source = @import("source.zig");
const admission = @import("admission.zig");
pub const Sequential = struct {
    context: *anyopaque,
    /// Zero means final EOF; temporary lack of input must return an error.
    read: *const fn (*anyopaque, []u8, source.Control) anyerror!usize,
};
pub const Options = struct {
    max_bytes: usize = 64 * 1024 * 1024,
    max_reads: usize = 100_000,
    chunk_bytes: usize = 64 * 1024,
    control: source.Control = .{},
    admission_pool: ?*admission.Pool = null,
    source_limits: source.Limits = .{},
};
pub const Snapshot = struct {
    input: source.Source,
    bytes: []u8,
    reservation: admission.Token,
    pub fn copy(allocator: std.mem.Allocator, identity: []const u8, parts: []const []const u8, options: Options) !*Snapshot {
        var size: usize = 0;
        for (parts) |part| size = try std.math.add(usize, size, part.len);
        if (size > options.max_bytes) return error.ResourceLimitExceeded;
        try options.control.check();
        var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, size, @sizeOf(Snapshot)) }) else admission.Token{};
        errdefer reservation.deinit();
        const self = try allocator.create(Snapshot);
        errdefer allocator.destroy(self);
        const bytes = try allocator.alloc(u8, size);
        errdefer allocator.free(bytes);
        var cursor: usize = 0;
        for (parts) |part| {
            try options.control.check();
            @memcpy(bytes[cursor..][0..part.len], part);
            cursor += part.len;
        }
        self.* = .{ .bytes = bytes, .reservation = reservation, .input = .{ .allocator = allocator, .identity = identity, .storage = .{ .borrowed = bytes }, .limits = options.source_limits, .control = options.control, .admission_pool = options.admission_pool } };
        return self;
    }
    /// All readers and leases must be released before the backing snapshot.
    pub fn deinit(self: *Snapshot) void {
        std.debug.assert(self.input.retained_bytes == 0);
        const allocator = self.input.allocator;
        allocator.free(self.bytes);
        self.reservation.deinit();
        allocator.destroy(self);
    }
};
/// Explicit spool policy for a non-seekable source. No unbounded buffering or
/// assumed length. A completed snapshot feeds the existing MP4/WebM readers.
pub fn collect(allocator: std.mem.Allocator, identity: []const u8, provider: Sequential, options: Options) !*Snapshot {
    if (options.chunk_bytes == 0) return error.ResourceLimitExceeded;
    var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, options.max_bytes, @sizeOf(Snapshot)) }) else admission.Token{};
    errdefer reservation.deinit();
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(allocator);
    var reads: usize = 0;
    while (true) {
        try options.control.check();
        if (reads >= options.max_reads) return error.ResourceLimitExceeded;
        reads += 1;
        if (bytes.items.len == options.max_bytes) {
            var probe: [1]u8 = undefined;
            const count = try provider.read(provider.context, &probe, options.control);
            try options.control.check();
            if (count > 1) return error.InvalidSourceRead;
            if (count != 0) return error.ResourceLimitExceeded;
            break;
        }
        const requested = @min(options.chunk_bytes, options.max_bytes - bytes.items.len);
        try bytes.ensureTotalCapacityPrecise(allocator, bytes.items.len + requested);
        const target = bytes.allocatedSlice()[bytes.items.len..][0..requested];
        const count = try provider.read(provider.context, target, options.control);
        try options.control.check();
        if (count > requested) return error.InvalidSourceRead;
        if (count == 0) break;
        bytes.items.len += count;
    }
    const self = try allocator.create(Snapshot);
    errdefer allocator.destroy(self);
    const owned = try bytes.toOwnedSlice(allocator);
    errdefer allocator.free(owned);
    try reservation.resize(.{ .host_bytes = owned.len + @sizeOf(Snapshot) });
    self.* = .{ .bytes = owned, .reservation = reservation, .input = .{ .allocator = allocator, .identity = identity, .storage = .{ .borrowed = owned }, .limits = options.source_limits, .control = options.control, .admission_pool = options.admission_pool } };
    return self;
}
test "sequential spool handles short reads, exact EOF, limits and retained leases" {
    const Provider = struct {
        cursor: usize = 0,
        fn read(ctx: *anyopaque, out: []u8, control: source.Control) !usize {
            try control.check();
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const n = @min(@min(out.len, 2), 8 - self.cursor);
            @memcpy(out[0..n], "abcdefgh"[self.cursor..][0..n]);
            self.cursor += n;
            return n;
        }
    };
    var provider = Provider{};
    var pool = admission.Pool{ .limits = .{ .host_bytes = 4096 } };
    const snapshot = try collect(std.testing.allocator, "spooled", .{ .context = &provider, .read = Provider.read }, .{ .max_bytes = 8, .chunk_bytes = 3, .admission_pool = &pool });
    var head = try snapshot.input.read(0, 4);
    var tail = try snapshot.input.read(4, 4);
    try std.testing.expectEqualStrings("abcd", head.bytes);
    try std.testing.expectEqualStrings("efgh", tail.bytes);
    head.deinit();
    tail.deinit();
    snapshot.deinit();
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    provider = .{};
    try std.testing.expectError(error.ResourceLimitExceeded, collect(std.testing.allocator, "too-long", .{ .context = &provider, .read = Provider.read }, .{ .max_bytes = 7, .admission_pool = &pool }));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    const Harness = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var p = Provider{};
            const result = try collect(allocator, "failures", .{ .context = &p, .read = Provider.read }, .{ .max_bytes = 8, .chunk_bytes = 3 });
            result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "sequential invalid providers and cancellation release admission" {
    const a = std.testing.allocator;
    var pool = admission.Pool{ .limits = .{ .host_bytes = 4096 } };
    const Provider = struct {
        fn invalid(_: *anyopaque, out: []u8, _: source.Control) !usize {
            return out.len + 1;
        }
        fn cancel(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(error.InvalidSourceRead, collect(a, "invalid", .{ .context = &context, .read = Provider.invalid }, .{ .max_bytes = 128, .admission_pool = &pool }));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
    try std.testing.expectError(error.Cancelled, collect(a, "cancelled", .{ .context = &context, .read = Provider.invalid }, .{ .max_bytes = 128, .admission_pool = &pool, .control = .{ .check_fn = Provider.cancel } }));
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
}

test "sequential snapshots feed portable MP4 and WebM indexing" {
    const a = std.testing.allocator;
    const Provider = struct {
        bytes: []const u8,
        cursor: usize = 0,
        fn read(context: *anyopaque, out: []u8, control: source.Control) !usize {
            try control.check();
            const self: *@This() = @ptrCast(@alignCast(context));
            const count = @min(@min(out.len, 17), self.bytes.len - self.cursor);
            @memcpy(out[0..count], self.bytes[self.cursor..][0..count]);
            self.cursor += count;
            return count;
        }
    };
    inline for (.{ @embedFile("../testdata/fragmented.mp4"), @embedFile("../testdata/video.webm") }, 0..) |bytes, index| {
        var provider = Provider{ .bytes = bytes };
        const snapshot = try collect(a, "sequential-container", .{ .context = &provider, .read = Provider.read }, .{ .max_bytes = bytes.len, .chunk_bytes = 64 });
        defer snapshot.deinit();
        if (index == 0) {
            var reader = try @import("mp4.zig").Reader.init(a, &snapshot.input, .{});
            defer reader.deinit();
            try std.testing.expect(reader.packets.len > 0);
        } else {
            var reader = try @import("webm.zig").Reader.init(a, &snapshot.input, .{});
            defer reader.deinit();
            try std.testing.expect(reader.packets.len > 0);
        }
    }
}
