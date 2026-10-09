// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Immutable remote range adapters. Transport/authentication belong to the
//! caller; version and range validation cannot be bypassed by that transport.
const std = @import("std");
const source = @import("source.zig");
pub const Response = struct {
    status: u16,
    offset: u64,
    total_length: u64,
    bytes_written: usize,
    /// Length declared by Content-Range, independently of the copied body.
    range_length: usize,
    /// S3 version ID or strong HTTP ETag, according to the request identity.
    version: []const u8,
};
pub const VersionKind = enum { strong_etag, s3_version_id, gcs_generation };
pub const Request = struct { object: []const u8, version: []const u8, version_kind: VersionKind, offset: u64 };
pub const Transport = struct {
    context: *anyopaque,
    get: *const fn (*anyopaque, Request, []u8, source.Control) anyerror!Response,
};
pub const ObjectRange = struct {
    transport: Transport,
    /// Canonical object URL/key, borrowed for this adapter's lifetime.
    object: []const u8,
    version: []const u8,
    version_kind: VersionKind = .strong_etag,
    length: u64,
    max_bytes: u64 = 64 * 1024 * 1024,
    max_requests: usize = 100_000,
    transferred_bytes: u64 = 0,
    charged_bytes: u64 = 0,
    requests: usize = 0,
    fn validateVersion(self: *const ObjectRange) !void {
        if (self.version.len == 0) return error.ImmutableVersionRequired;
        switch (self.version_kind) {
            .strong_etag => {
                if (self.version.len < 2 or self.version[0] != '"' or self.version[self.version.len - 1] != '"') return error.ImmutableVersionRequired;
                for (self.version[1 .. self.version.len - 1]) |byte| if (byte < 0x21 or byte == 0x22 or byte == 0x7f) {
                    return error.ImmutableVersionRequired;
                };
            },
            .s3_version_id => if (std.mem.eql(u8, self.version, "null")) {
                return error.ImmutableVersionRequired;
            },
            .gcs_generation => {
                if (self.version[0] == '0') return error.ImmutableVersionRequired;
                for (self.version) |byte| if (byte < '0' or byte > '9') {
                    return error.ImmutableVersionRequired;
                };
                _ = std.fmt.parseInt(u64, self.version, 10) catch return error.ImmutableVersionRequired;
            },
        }
    }
    pub fn range(self: *ObjectRange) !source.Range {
        try self.validateVersion();
        return .{ .context = self, .read_at = readAt, .length = self.length };
    }
    pub fn readAt(context: *anyopaque, offset: u64, out: []u8, control: source.Control) !usize {
        const self: *ObjectRange = @ptrCast(@alignCast(context));
        try control.check();
        try self.validateVersion();
        if (offset > self.length or out.len > self.length - offset) return error.UnexpectedEndOfSource;
        if (out.len > self.max_bytes -| self.charged_bytes or self.requests >= self.max_requests) return error.ResourceLimitExceeded;
        self.requests += 1;
        // Charge the requested wire capacity even on transport failure. This
        // conservative bound prevents retries from resetting an I/O budget.
        self.charged_bytes += out.len;
        const reply = try self.transport.get(self.transport.context, .{ .object = self.object, .version = self.version, .version_kind = self.version_kind, .offset = offset }, out, control);
        self.transferred_bytes += @min(out.len, reply.bytes_written);
        try control.check();
        if (reply.status != 206 or reply.offset != offset or reply.total_length != self.length or reply.bytes_written > out.len or reply.bytes_written == 0 or reply.range_length != reply.bytes_written) return error.InvalidRemoteRange;
        if (!std.mem.eql(u8, reply.version, self.version)) return error.SourceVersionChanged;
        self.charged_bytes -= out.len - reply.bytes_written;
        return reply.bytes_written;
    }
};
/// One bounded forward read-ahead window coalesces nearby packet/probe reads.
/// Returned Source leases own their copies; eviction cannot invalidate them.
/// Accounting distinguishes logical Source bytes from provider wire bytes.
pub const ReadAhead = struct {
    allocator: std.mem.Allocator,
    upstream: source.Range,
    window_bytes: usize = 256 * 1024,
    max_bytes: u64 = 64 * 1024 * 1024,
    max_reads: usize = 100_000,
    admission_pool: ?*@import("admission.zig").Pool = null,
    reservation: @import("admission.zig").Token = .{},
    buffer: ?[]u8 = null,
    start: u64 = 0,
    valid: usize = 0,
    transferred_bytes: u64 = 0,
    charged_bytes: u64 = 0,
    reads: usize = 0,
    hits: usize = 0,
    pub fn range(self: *ReadAhead) source.Range {
        return .{ .context = self, .read_at = readAt, .length = self.upstream.length };
    }
    pub fn deinit(self: *ReadAhead) void {
        if (self.buffer) |bytes| self.allocator.free(bytes);
        self.reservation.deinit();
        self.buffer = null;
        self.valid = 0;
    }
    pub fn readAt(context: *anyopaque, offset: u64, out: []u8, control: source.Control) !usize {
        const self: *ReadAhead = @ptrCast(@alignCast(context));
        try control.check();
        if (offset > self.upstream.length or out.len > self.upstream.length - offset) return error.UnexpectedEndOfSource;
        if (out.len == 0) return 0;
        if (self.window_bytes == 0) return error.ResourceLimitExceeded;
        if (offset >= self.start and offset - self.start <= self.valid and out.len <= self.valid - @as(usize, @intCast(offset - self.start))) {
            const delta: usize = @intCast(offset - self.start);
            @memcpy(out, self.buffer.?[delta..][0..out.len]);
            self.hits += 1;
            return out.len;
        }
        if (self.buffer == null) {
            var reservation = if (self.admission_pool) |pool| try pool.acquire(.{ .host_bytes = self.window_bytes }) else @import("admission.zig").Token{};
            errdefer reservation.deinit();
            self.buffer = try self.allocator.alloc(u8, self.window_bytes);
            self.reservation = reservation;
        }
        self.valid = 0;
        errdefer self.valid = 0;
        self.start = offset;
        // Large callers receive a short read, keeping the cache allocation fixed.
        const wanted: usize = @intCast(@min(self.buffer.?.len, self.upstream.length - offset));
        if (wanted > self.max_bytes -| self.charged_bytes) return error.ResourceLimitExceeded;
        while (self.valid < wanted) {
            try control.check();
            if (self.reads >= self.max_reads) {
                self.valid = 0;
                return error.ResourceLimitExceeded;
            }
            self.reads += 1;
            const requested_bytes = wanted - self.valid;
            self.charged_bytes += requested_bytes;
            const n = self.upstream.read_at(self.upstream.context, offset + self.valid, self.buffer.?[self.valid..wanted], control) catch |err| {
                self.valid = 0;
                return err;
            };
            if (n == 0 or n > wanted - self.valid) {
                self.valid = 0;
                return error.InvalidSourceRead;
            }
            self.charged_bytes -= requested_bytes - n;
            self.transferred_bytes += n;
            self.valid += n;
        }
        control.check() catch |err| {
            self.valid = 0;
            return err;
        };
        const n = @min(out.len, self.valid);
        @memcpy(out[0..n], self.buffer.?[0..n]);
        return n;
    }
};
test "version pinned object reads coalesce and leases survive eviction" {
    const Fake = struct {
        version: []const u8 = "\"v1\"",
        fn get(ctx: *anyopaque, request: Request, out: []u8, _: source.Control) !Response {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const data = "abcdefghijklmnop";
            const offset = request.offset;
            @memcpy(out, data[@intCast(offset)..][0..out.len]);
            return .{ .status = 206, .offset = offset, .total_length = data.len, .bytes_written = out.len, .range_length = out.len, .version = self.version };
        }
    };
    var fake = Fake{};
    var object = ObjectRange{ .transport = .{ .context = &fake, .get = Fake.get }, .object = "s3://bucket/key", .version = "\"v1\"", .length = 16 };
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = try object.range(), .window_bytes = 8 };
    defer cache.deinit();
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "bucket/key@v1", .storage = .{ .range = cache.range() } };
    var first = try input.read(0, 3);
    defer first.deinit();
    var second = try input.read(3, 3);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), object.requests);
    try std.testing.expectEqual(@as(usize, 1), cache.hits);
    var third = try input.read(8, 2);
    defer third.deinit();
    try std.testing.expectEqualStrings("abc", first.bytes);
    try std.testing.expectEqualStrings("def", second.bytes);
    fake.version = "\"v2\"";
    try std.testing.expectError(error.SourceVersionChanged, input.read(0, 1));
    try std.testing.expectEqual(@as(usize, 8), input.retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.valid);
    object.version = "W/weak";
    try std.testing.expectError(error.ImmutableVersionRequired, object.range());
}

test "remote versions ranges wire budgets and cancellation fail closed" {
    const Fake = struct {
        status: u16 = 206,
        kind: VersionKind = .s3_version_id,
        fn get(ctx: *anyopaque, request: Request, out: []u8, _: source.Control) !Response {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (request.version_kind != self.kind) return error.WrongVersionCondition;
            @memset(out, 7);
            return .{ .status = self.status, .offset = request.offset, .total_length = 16, .bytes_written = out.len, .range_length = out.len, .version = request.version };
        }
    };
    var fake = Fake{};
    var object = ObjectRange{ .transport = .{ .context = &fake, .get = Fake.get }, .object = "s3://bucket/key", .version = "version-123", .version_kind = .s3_version_id, .length = 16, .max_bytes = 8 };
    var bytes: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try ObjectRange.readAt(&object, 0, &bytes, .{}));
    fake.status = 200;
    try std.testing.expectError(error.InvalidRemoteRange, ObjectRange.readAt(&object, 4, &bytes, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, ObjectRange.readAt(&object, 4, &bytes, .{}));
    try std.testing.expectEqual(@as(u64, 8), object.transferred_bytes);
    try std.testing.expectEqual(@as(usize, 2), object.requests);
    object.version = "null";
    try std.testing.expectError(error.ImmutableVersionRequired, object.range());
    object.version_kind = .gcs_generation;
    object.version = "01";
    try std.testing.expectError(error.ImmutableVersionRequired, object.range());
    object.version = "123";
    fake.kind = .gcs_generation;
    fake.status = 206;
    object.max_bytes = 32;
    var pool = @import("admission.zig").Pool{ .limits = .{ .host_bytes = 8 } };
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = try object.range(), .window_bytes = 8, .admission_pool = &pool };
    const Cancel = struct {
        calls: usize = 0,
        fn check(ctx: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ctx.?)));
            self.calls += 1;
            if (self.calls >= 5) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    try std.testing.expectError(error.Cancelled, ReadAhead.readAt(&cache, 0, &bytes, .{ .context = &cancel, .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 0), cache.valid);
    cache.deinit();
    cache.deinit();
    try std.testing.expectEqual(@import("admission.zig").Resources{}, pool.snapshot());
}
