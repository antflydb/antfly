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
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-object-snapshot-v1");
        hash.update(&.{@backingInt(self.version_kind)});
        for ([_][]const u8{ self.object, self.version }) |value| {
            hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, value.len)));
            hash.update(value);
        }
        hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, self.length)));
        return .{ .context = self, .read_at = readAt, .sparse_read_preferring = readPreferring, .length = self.length, .immutable_binding = hash.finalResult() };
    }
    fn readPreferring(context: *anyopaque, offset: u64, out: []u8, minimum: usize, control: source.Control) !usize {
        const self: *ObjectRange = @ptrCast(@alignCast(context));
        try control.check();
        const size: usize = @intCast(@min(out.len, self.max_bytes -| self.charged_bytes));
        if (size < minimum) return error.ResourceLimitExceeded;
        return readAt(context, offset, out[0..size], control);
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
    prefetch_limited: bool = false,
    sparse_pressure: u2 = 0,
    sparse_miss: ?u64 = null,
    pub fn range(self: *ReadAhead) source.Range {
        return .{ .context = self, .read_at = readAt, .sparse_read_at = readSparseAt, .sparse_read_preferring = if (self.upstream.accepts_full_read_size or self.upstream.sparse_read_preferring != null) readSparsePreferring else null, .isolated_read_at = readIsolatedAt, .length = self.upstream.length, .immutable_binding = self.upstream.immutable_binding };
    }
    pub fn deinit(self: *ReadAhead) void {
        if (self.buffer) |bytes| self.allocator.free(bytes);
        self.reservation.deinit();
        self.buffer = null;
        self.valid = 0;
    }
    pub fn readAt(context: *anyopaque, offset: u64, out: []u8, control: source.Control) !usize {
        const self: *ReadAhead = @ptrCast(@alignCast(context));
        self.sparse_pressure = 0;
        self.sparse_miss = null;
        return self.readWindow(offset, out, out.len, control, self.window_bytes);
    }
    fn readSparseAt(context: *anyopaque, offset: u64, out: []u8, control: source.Control) !usize {
        return readSparsePreferring(context, offset, out, out.len, control);
    }

    fn readSparsePreferring(context: *anyopaque, offset: u64, out: []u8, minimum: usize, control: source.Control) !usize {
        const self: *ReadAhead = @ptrCast(@alignCast(context));
        // Cache hits provide no evidence about upstream request density.
        if (out.len == 0 or self.cachedPrefix(offset, out.len) != 0) return self.readWindow(offset, out, minimum, control, 0);
        if (self.upstream.sparse_read_preferring == null and !self.upstream.accepts_full_read_size) return self.readWindow(offset, out, minimum, control, minimum);
        const requests_left = self.max_reads -| self.reads;
        const bytes_left = self.max_bytes -| self.charged_bytes;
        const source_left = self.upstream.length -| offset;
        if (self.sparse_miss) |previous| {
            if (offset <= previous) {
                self.sparse_pressure = 0;
            } else {
                const stride = offset - previous;
                if (source_left / stride >= requests_left) {
                    self.sparse_pressure +|= 1;
                } else if (stride > self.valid +| 256) {
                    // A dense miss naturally follows a much larger useful
                    // span. Decay only when the scan is jumping past it.
                    self.sparse_pressure -|= 1;
                }
            }
        }
        self.sparse_miss = offset;
        // Require corroborating misses, except when one more sparse request
        // would leave almost no room to recover. Decay on wider strides.
        const dense = if (requests_left != 0 and (self.sparse_pressure >= 2 or (requests_left <= 3 and self.sparse_pressure != 0)))
            bytes_left / requests_left + @intFromBool(bytes_left % requests_left != 0)
        else
            0;
        const fill = @min(self.window_bytes, @max(256, out.len, dense));
        return self.readWindow(offset, out, minimum, control, @intCast(fill));
    }
    fn readIsolatedAt(context: *anyopaque, offset: u64, out: []u8, control: source.Control) !usize {
        const self: *ReadAhead = @ptrCast(@alignCast(context));
        try control.check();
        if (offset > self.upstream.length or out.len > self.upstream.length - offset) return error.UnexpectedEndOfSource;
        if (out.len == 0) return 0;
        if (self.cachedPrefix(offset, out.len) != 0) return self.readWindow(offset, out, out.len, control, 0);
        const size: usize = @intCast(@min(out.len, self.max_bytes -| self.charged_bytes));
        if (size == 0) return error.RemoteReadBudgetExceeded;
        return self.fetchAt(offset, out[0..size], size, control);
    }

    fn fetchAt(self: *ReadAhead, offset: u64, out: []u8, minimum: usize, control: source.Control) !usize {
        if (self.reads >= self.max_reads) return error.RemoteRequestBudgetExceeded;
        self.reads += 1;
        self.charged_bytes += out.len;
        const result = if (minimum < out.len and self.upstream.sparse_read_preferring != null)
            self.upstream.sparse_read_preferring.?(self.upstream.context, offset, out, minimum, control)
        else
            self.upstream.read_at(self.upstream.context, offset, out, control);
        const n = result catch |err| {
            if (err == error.RemoteReadBudgetExceeded) self.charged_bytes -= out.len;
            return err;
        };
        if (n == 0 or n > out.len) return error.InvalidSourceRead;
        self.charged_bytes -= out.len - n;
        self.transferred_bytes += n;
        return n;
    }

    fn cachedPrefix(self: *const ReadAhead, offset: u64, count: usize) usize {
        if (offset < self.start or offset - self.start >= self.valid) return 0;
        return @min(count, self.valid - @as(usize, @intCast(offset - self.start)));
    }

    fn readWindow(self: *ReadAhead, offset: u64, out: []u8, minimum: usize, control: source.Control, fill_bytes: usize) !usize {
        try control.check();
        if (offset > self.upstream.length or out.len > self.upstream.length - offset) return error.UnexpectedEndOfSource;
        if (out.len == 0) return 0;
        if (self.window_bytes == 0) return error.InvalidRemoteCacheBudget;
        const cached = self.cachedPrefix(offset, out.len);
        if (cached != 0) {
            const delta: usize = @intCast(offset - self.start);
            @memcpy(out[0..cached], self.buffer.?[delta..][0..cached]);
            self.hits += 1;
            // Source completes a short read at offset + cached.
            return cached;
        }
        if (self.buffer == null) {
            var reservation = if (self.admission_pool) |pool| try pool.acquire(.{ .host_bytes = self.window_bytes }) else @import("admission.zig").Token{};
            errdefer reservation.deinit();
            self.buffer = try self.allocator.alloc(u8, self.window_bytes);
            self.reservation = reservation;
        }
        // Keep an existing prefix when extending the same window. Later
        // probes may ask for that prefix again before growing the suffix.
        const append = self.valid < self.window_bytes and offset >= self.start and offset - self.start == self.valid;
        const retained = if (append) self.valid else 0;
        const fill = if (self.prefetch_limited) @min(fill_bytes, out.len) else fill_bytes;
        const fetch: usize = @intCast(@min(fill, self.window_bytes - retained, self.upstream.length - offset, self.max_bytes -| self.charged_bytes));
        if (fetch == 0) return error.RemoteReadBudgetExceeded;
        self.valid = retained;
        if (!append) self.start = offset;
        errdefer self.valid = retained;
        var wanted = retained + fetch;
        const required = retained + @min(minimum, fetch);
        while (self.valid < wanted) {
            try control.check();
            if (self.reads >= self.max_reads) {
                if (self.valid >= required) break;
                return error.RemoteRequestBudgetExceeded;
            }
            // A rejected speculative fill can consume an upstream attempt.
            // Reserve the final attempt for bytes the caller actually needs.
            if (!self.upstream.accepts_full_read_size and self.upstream.sparse_read_preferring == null and self.max_reads - self.reads == 1) {
                if (self.valid >= required) break;
                wanted = required;
            }
            const n = self.fetchAt(self.start + self.valid, self.buffer.?[self.valid..wanted], required -| self.valid, control) catch |err| {
                if (err == error.RemoteReadBudgetExceeded) {
                    self.prefetch_limited = true;
                    if (self.valid >= required) break;
                    if (wanted > required) {
                        wanted = required;
                        continue;
                    }
                    if (self.valid > retained) break;
                }
                if (err == error.RemoteRequestBudgetExceeded and self.valid >= required) break;
                return err;
            };
            self.valid += n;
            if (self.valid >= required and (minimum < out.len or self.upstream.sparse_read_preferring != null)) break;
        }
        try control.check();
        const n = @min(out.len, self.valid - retained);
        @memcpy(out[0..n], self.buffer.?[retained..][0..n]);
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

test "range budgets bound fetched bytes independently of large object length" {
    const Provider = struct {
        fn read(_: *anyopaque, offset: u64, out: []u8, _: source.Control) !usize {
            for (out, 0..) |*byte, index| byte.* = @truncate(offset + index);
            return out.len;
        }
    };
    var owner: u8 = 0;
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &owner, .read_at = Provider.read, .length = 1 << 40 }, .window_bytes = 16, .max_bytes = 20, .max_reads = 2 };
    defer cache.deinit();
    var out: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try ReadAhead.readAt(&cache, 0, &out, .{}));
    try std.testing.expectEqual(@as(usize, 4), try ReadAhead.readAt(&cache, 8, &out, .{}));
    try std.testing.expectEqual(@as(usize, 1), cache.reads);
    // The final small read fits the remaining wire budget even though a full
    // speculative cache fill would exceed it.
    try std.testing.expectEqual(@as(usize, 4), try ReadAhead.readAt(&cache, 1000, &out, .{}));
    try std.testing.expectEqual(@as(u64, 20), cache.transferred_bytes);
    try std.testing.expectError(error.RemoteReadBudgetExceeded, ReadAhead.readAt(&cache, 2000, &out, .{}));
    cache.max_bytes = 100;
    try std.testing.expectError(error.RemoteRequestBudgetExceeded, ReadAhead.readAt(&cache, 2000, &out, .{}));
}

test "remote prefetch returns demanded bytes on the last short upstream request" {
    const Provider = struct {
        fn read(_: *anyopaque, _: u64, out: []u8, _: source.Control) !usize {
            const count = @min(8, out.len);
            @memset(out[0..count], 1);
            return count;
        }
    };
    var marker: u8 = 0;
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &marker, .read_at = Provider.read, .length = 1024 }, .max_reads = 1 };
    defer cache.deinit();
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "short-prefetch", .storage = .{ .range = cache.range() } };
    var lease = try input.readSparse(0, 4);
    defer lease.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 1, 1, 1, 1 }, lease.bytes);
    try std.testing.expectEqual(@as(usize, 4), cache.valid);
}

test "remote prefetch tolerates upstream quota only after satisfying demand" {
    const Provider = struct {
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: source.Control) !usize {
            const failure: *const anyerror = @ptrCast(@alignCast(raw));
            if (offset != 0) return failure.*;
            const count = @min(8, out.len);
            @memset(out[0..count], 1);
            return count;
        }
    };
    for ([_]anyerror{ error.RemoteReadBudgetExceeded, error.RemoteRequestBudgetExceeded, error.SourceVersionChanged }) |failure| {
        var upstream_error = failure;
        var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &upstream_error, .read_at = Provider.read, .length = 1024 } };
        defer cache.deinit();
        var out: [4]u8 = undefined;
        if (failure == error.SourceVersionChanged) {
            try std.testing.expectError(failure, ReadAhead.readAt(&cache, 0, &out, .{}));
        } else {
            try std.testing.expectEqual(out.len, try ReadAhead.readAt(&cache, 0, &out, .{}));
            try std.testing.expectEqualSlices(u8, &.{ 1, 1, 1, 1 }, &out);
            try std.testing.expectError(failure, ReadAhead.readAt(&cache, 64, &out, .{}));
        }
    }
}

test "remote retries an oversized prefetch with only the demand suffix" {
    const Upstream = struct {
        remaining: usize = 8,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (out.len > self.remaining) return error.RemoteReadBudgetExceeded;
            self.remaining -= out.len;
            for (out, 0..) |*byte, index| byte.* = @intCast(offset + index);
            return out.len;
        }
    };
    var provider = Upstream{};
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &provider, .read_at = Upstream.read, .length = 32 }, .max_bytes = 16, .max_reads = 4 };
    defer cache.deinit();
    var out: [4]u8 = undefined;
    try std.testing.expectEqual(out.len, try ReadAhead.readAt(&cache, 0, &out, .{}));
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 2, 3 }, &out);
    try std.testing.expectEqual(@as(usize, 2), cache.reads);
    try std.testing.expectEqual(@as(u64, 4), cache.charged_bytes);
    try std.testing.expectEqual(out.len, try ReadAhead.readAt(&cache, 4, &out, .{}));
    try std.testing.expectEqualSlices(u8, &.{ 4, 5, 6, 7 }, &out);
    try std.testing.expectEqual(@as(usize, 3), cache.reads);
    try std.testing.expectEqual(@as(u64, 8), cache.charged_bytes);
    try std.testing.expectError(error.RemoteReadBudgetExceeded, ReadAhead.readAt(&cache, 8, &out, .{}));
    try std.testing.expectEqual(@as(u64, 8), cache.transferred_bytes);
    try std.testing.expectEqual(@as(u64, 8), cache.charged_bytes);

    provider = .{};
    var last = ReadAhead{ .allocator = std.testing.allocator, .upstream = cache.upstream, .max_bytes = 16, .max_reads = 1 };
    defer last.deinit();
    try std.testing.expectEqual(out.len, try ReadAhead.readAt(&last, 0, &out, .{}));
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 2, 3 }, &out);
    try std.testing.expectEqual(@as(usize, 1), last.reads);
    try std.testing.expectEqual(@as(u64, 4), last.charged_bytes);
}

test "remote isolated look-behind preserves the forward cache" {
    const Provider = struct {
        fn read(_: *anyopaque, offset: u64, out: []u8, _: source.Control) !usize {
            for (out, 0..) |*byte, delta| byte.* = @truncate(offset + delta);
            return out.len;
        }
    };
    var marker: u8 = 0;
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &marker, .read_at = Provider.read, .length = 32 * 1024, .accepts_full_read_size = true }, .max_reads = 2 };
    defer cache.deinit();
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "look-behind", .storage = .{ .range = cache.range() } };
    var tail = try input.readSparse(8192, 8);
    defer tail.deinit();
    var member = try input.readIsolated(4096, 32);
    defer member.deinit();
    var next = try input.readSparse(8200, 8);
    defer next.deinit();
    try std.testing.expectEqual(@as(u64, 8192), cache.start);
    try std.testing.expectEqual(@as(u64, 8192), cache.sparse_miss.?);
    try std.testing.expectEqual(@as(u64, 288), cache.transferred_bytes);
    try std.testing.expectEqualSlices(u8, &.{ 8, 9, 10, 11, 12, 13, 14, 15 }, next.bytes);
}

test "expanding sparse probes fetch only uncached suffixes" {
    const Provider = struct {
        fetched: u64 = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8, _: source.Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.fetched, offset);
            for (out, 0..) |*byte, delta| byte.* = @truncate(offset + delta);
            self.fetched += out.len;
            return out.len;
        }
    };
    var provider = Provider{};
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &provider, .read_at = Provider.read, .length = 768, .accepts_full_read_size = true }, .max_bytes = 768 };
    defer cache.deinit();
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "expanding-headers", .storage = .{ .range = cache.range() } };
    var header = try input.readSparse(0, 10);
    header.deinit();
    for ([_]usize{ 128, 256, 512 }) |size| {
        var lease = try input.readSparse(10, size);
        defer lease.deinit();
        for (lease.bytes, 0..) |byte, delta| try std.testing.expectEqual(@as(u8, @truncate(10 + delta)), byte);
    }
    try std.testing.expectEqual(@as(u64, 768), cache.transferred_bytes);
    try std.testing.expectEqual(@as(usize, 3), cache.reads);
}

test "sparse scans decay dense prefetch after wider upstream misses" {
    const Provider = struct {
        fn read(_: *anyopaque, _: u64, out: []u8, _: source.Control) !usize {
            @memset(out, 0);
            return out.len;
        }
    };
    var marker: u8 = 0;
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &marker, .read_at = Provider.read, .length = 1024 * 1024, .accepts_full_read_size = true }, .max_bytes = 1024 * 1024, .max_reads = 1024 };
    defer cache.deinit();
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "mixed-density", .storage = .{ .range = cache.range() } };
    for ([_]u64{ 0, 300, 600, 64 * 1024, 128 * 1024 }) |offset| {
        var lease = try input.readSparse(offset, 4);
        defer lease.deinit();
        if (offset == 300) try std.testing.expectEqual(@as(usize, 256), cache.valid);
        if (offset == 600) try std.testing.expect(cache.valid > 256);
        if (offset == 128 * 1024) try std.testing.expectEqual(@as(usize, 256), cache.valid);
    }
}

test "sparse cache hits cannot spend the byte budget on dense prefetch" {
    const Provider = struct {
        fn read(_: *anyopaque, _: u64, out: []u8, _: source.Control) !usize {
            @memset(out, 0);
            return out.len;
        }
    };
    var marker: u8 = 0;
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &marker, .read_at = Provider.read, .length = 8 * 1024 * 1024, .accepts_full_read_size = true }, .max_bytes = 4096, .max_reads = 100_000 };
    defer cache.deinit();
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "sparse", .storage = .{ .range = cache.range() } };
    for ([_]u64{ 0, 4, 64 * 1024 }) |offset| {
        var lease = try input.readSparse(offset, 4);
        defer lease.deinit();
    }
    try std.testing.expectEqual(@as(usize, 2), cache.reads);
    try std.testing.expectEqual(@as(u64, 512), cache.transferred_bytes);
}

test "sparse forward scans coalesce before exhausting requests" {
    const Provider = struct {
        fn read(_: *anyopaque, offset: u64, out: []u8, _: source.Control) !usize {
            for (out, 0..) |*byte, index| byte.* = @truncate(offset + index);
            return out.len;
        }
    };
    var marker: u8 = 0;
    var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &marker, .read_at = Provider.read, .length = 8192, .accepts_full_read_size = true }, .window_bytes = 4096, .max_bytes = 8192, .max_reads = 4 };
    defer cache.deinit();
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "sparse", .storage = .{ .range = cache.range() } };
    var offset: u64 = 0;
    while (offset + 4 <= input.length()) : (offset += 300) {
        var lease = try input.readSparse(offset, 4);
        defer lease.deinit();
        try std.testing.expectEqual(@as(u8, @truncate(offset)), lease.bytes[0]);
    }
    try std.testing.expectEqual(@as(usize, 4), cache.reads);
    try std.testing.expect(cache.transferred_bytes <= 8192);
}

test "immutable snapshot bindings separate object version kind and length through read ahead" {
    var marker: u8 = 0;
    var object = ObjectRange{ .transport = .{ .context = &marker, .get = undefined }, .object = "s3://bucket/key", .version = "\"v1\"", .length = 16 };
    const initial = (try object.range()).immutable_binding.?;
    var read_ahead = ReadAhead{ .allocator = std.testing.allocator, .upstream = try object.range() };
    defer read_ahead.deinit();
    try std.testing.expectEqual(initial, read_ahead.range().immutable_binding.?);
    object.version = "\"v2\"";
    try std.testing.expect(!std.mem.eql(u8, &initial, &(try object.range()).immutable_binding.?));
    object.version = "\"v1\"";
    object.object = "s3://other/key";
    try std.testing.expect(!std.mem.eql(u8, &initial, &(try object.range()).immutable_binding.?));
    object.object = "s3://bucket/key";
    object.length += 1;
    try std.testing.expect(!std.mem.eql(u8, &initial, &(try object.range()).immutable_binding.?));
    object.length = 16;
    object.version_kind = .s3_version_id;
    try std.testing.expect(!std.mem.eql(u8, &initial, &(try object.range()).immutable_binding.?));
}

test "preferred sparse demand survives a byte cap or a final short reply" {
    const Provider = struct {
        fn read(_: *anyopaque, _: u64, out: []u8, _: source.Control) !usize {
            const count = @min(2, out.len);
            @memset(out[0..count], 42);
            return count;
        }
    };
    for ([_]u64{ 2, 512 }) |max_bytes| {
        var marker: u8 = 0;
        var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = .{ .context = &marker, .read_at = Provider.read, .length = 512, .accepts_full_read_size = true }, .max_bytes = max_bytes, .max_reads = 1 };
        defer cache.deinit();
        var input = source.Source{ .allocator = std.testing.allocator, .identity = "preferred-tail", .storage = .{ .range = cache.range() } };
        var tail = try input.readSparsePreferring(0, 2, 256, .{});
        defer tail.deinit();
        try std.testing.expectEqualSlices(u8, &.{ 42, 42 }, tail.bytes);
        try std.testing.expectEqual(@as(usize, 1), cache.reads);
        try std.testing.expectEqual(@as(u64, 2), cache.transferred_bytes);
    }
}

test "preferred object reads honor the wire allowance through read ahead" {
    const Provider = struct {
        fn get(_: *anyopaque, request: Request, out: []u8, _: source.Control) !Response {
            try std.testing.expectEqual(@as(usize, 3), out.len);
            @memcpy(out, "abc");
            return .{ .status = 206, .offset = request.offset, .total_length = 16, .bytes_written = out.len, .range_length = out.len, .version = request.version };
        }
    };
    var marker: u8 = 0;
    for ([_]bool{ false, true }) |wrapped| {
        var object = ObjectRange{ .transport = .{ .context = &marker, .get = Provider.get }, .object = "s3://bucket/key", .version = "\"v1\"", .length = 16, .max_bytes = 3, .max_requests = 1 };
        const upstream = try object.range();
        var cache = ReadAhead{ .allocator = std.testing.allocator, .upstream = upstream, .window_bytes = 16 };
        defer cache.deinit();
        var input = source.Source{ .allocator = std.testing.allocator, .identity = "bounded-object", .storage = .{ .range = if (wrapped) cache.range() else upstream } };
        var lease = try input.readSparsePreferring(0, 2, 8, .{});
        defer lease.deinit();
        try std.testing.expectEqualSlices(u8, "abc", lease.bytes);
        try std.testing.expectEqual(@as(usize, 1), object.requests);
        try std.testing.expectEqual(@as(u64, 3), object.charged_bytes);
    }
}
