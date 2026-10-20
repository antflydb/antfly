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

const std = @import("std");
const admission = @import("admission.zig");

/// Provider-defined cancellation/deadline check. Also passed into each read so
/// a blocking provider can check the same original deadline while doing I/O.
pub const Control = struct {
    context: ?*const anyopaque = null,
    check_fn: ?*const fn (?*const anyopaque) anyerror!void = null,
    pub fn check(self: Control) !void {
        if (self.check_fn) |callback| try callback(self.context);
    }
};
pub const Limits = struct {
    max_read_bytes: usize = 16 * 1024 * 1024,
    max_retained_bytes: usize = 32 * 1024 * 1024,
    max_total_bytes: u64 = 64 * 1024 * 1024,
    max_reads: usize = 100_000,
};
pub const Range = struct {
    /// Content digest or immutable object/version binding. Providers supplying
    /// this must validate that every returned range belongs to that snapshot.
    /// A display URL or unverified ETag is insufficient.
    immutable_binding: ?[32]u8 = null,
    /// Immutable local extents may lend a contiguous read without allocating.
    /// Null requests the ordinary read_at path (for example across extents).
    borrow_at: ?*const fn (*anyopaque, u64, usize, Control) anyerror!?[]const u8 = null,
    context: *anyopaque,
    /// May return a short read. Returning zero before the declared end is EOF.
    read_at: *const fn (*anyopaque, u64, []u8, Control) anyerror!usize,
    length: u64,
};
pub const Storage = union(enum) { borrowed: []const u8, range: Range };

/// A source is caller-owned, immovable while leases exist, and single-consumer.
/// Provider context and borrowed bytes must outlive this source and its leases.
/// Identity must name immutable content (e.g. an object version or digest).
pub const Source = struct {
    allocator: std.mem.Allocator,
    storage: Storage,
    identity: []const u8,
    limits: Limits = .{},
    control: Control = .{},
    admission_pool: ?*admission.Pool = null,
    retained_bytes: usize = 0,
    total_bytes: u64 = 0,
    reads: usize = 0,

    pub fn length(self: *const Source) u64 {
        return switch (self.storage) {
            .borrowed => |b| b.len,
            .range => |r| r.length,
        };
    }
    pub fn read(self: *Source, offset: u64, size: usize) !Lease {
        try self.control.check();
        if (offset > self.length() or size > self.length() - offset) return error.UnexpectedEndOfSource;
        if (size > self.limits.max_read_bytes or size > self.limits.max_retained_bytes -| self.retained_bytes or
            size > self.limits.max_total_bytes -| self.total_bytes) return error.ResourceLimitExceeded;
        if (self.reads >= self.limits.max_reads) return error.ResourceLimitExceeded;
        var reservation = if (self.admission_pool) |pool| try pool.acquire(.{ .host_bytes = size }) else admission.Token{};
        errdefer reservation.deinit();
        self.retained_bytes += size;
        errdefer self.retained_bytes -= size;
        switch (self.storage) {
            .borrowed => |bytes| {
                self.reads += 1;
                self.total_bytes += size;
                return .{ .bytes = bytes[@intCast(offset)..][0..size], .source = self, .reservation = reservation };
            },
            .range => |range| {
                if (range.borrow_at) |borrow| {
                    if (try borrow(range.context, offset, size, self.control)) |bytes| {
                        if (bytes.len != size) return error.InvalidSourceRead;
                        try self.control.check();
                        self.reads += 1;
                        self.total_bytes += size;
                        return .{ .bytes = bytes, .source = self, .reservation = reservation };
                    }
                }
                const owned = try self.allocator.alloc(u8, size);
                errdefer self.allocator.free(owned);
                var done: usize = 0;
                while (done < size) {
                    try self.control.check();
                    if (self.reads >= self.limits.max_reads) return error.ResourceLimitExceeded;
                    self.reads += 1;
                    const n = try range.read_at(range.context, offset + done, owned[done..], self.control);
                    if (n == 0) return error.UnexpectedEndOfSource;
                    if (n > size - done) return error.InvalidSourceRead;
                    self.total_bytes += n;
                    done += n;
                }
                try self.control.check();
                return .{ .bytes = owned, .source = self, .owned = owned, .reservation = reservation };
            },
        }
    }
};
/// Move-only by convention. Independent leases survive subsequent reads. Release
/// each exactly once; the source must outlive them. A failed read retains none.
pub const Lease = struct {
    bytes: []const u8,
    source: ?*Source,
    owned: ?[]u8 = null,
    reservation: admission.Token = .{},
    pub fn deinit(self: *Lease) void {
        if (self.source) |s| {
            s.retained_bytes -= self.bytes.len;
            if (self.owned) |owned| s.allocator.free(owned);
        }
        self.reservation.deinit();
        self.* = .{ .bytes = &.{}, .source = null };
    }
};
/// File adapter borrows both the file handle and Io implementation.
pub const FileRange = struct {
    file: std.Io.File,
    io: std.Io,
    pub fn readAt(context: *anyopaque, offset: u64, bytes: []u8, control: Control) !usize {
        try control.check();
        const self: *FileRange = @ptrCast(@alignCast(context));
        const n = try self.file.readPositional(self.io, &.{bytes}, offset);
        try control.check();
        return n;
    }
};

test "source independent leases, short reads, budgets, cancellation and cleanup" {
    const Provider = struct {
        bytes: []const u8,
        fn readAt(ctx: *anyopaque, offset: u64, out: []u8, control: Control) !usize {
            try control.check();
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const n = @min(@min(out.len, 2), self.bytes.len - @as(usize, @intCast(offset)));
            @memcpy(out[0..n], self.bytes[@intCast(offset)..][0..n]);
            return n;
        }
    };
    var provider = Provider{ .bytes = "abcdefgh" };
    var src = Source{ .allocator = std.testing.allocator, .identity = "fixture-v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.readAt, .length = 8 } }, .limits = .{ .max_retained_bytes = 6 } };
    var first = try src.read(0, 4);
    defer first.deinit();
    var second = try src.read(4, 2);
    try std.testing.expectEqualStrings("abcd", first.bytes);
    try std.testing.expectEqualStrings("ef", second.bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(6, 1));
    second.deinit();
    try std.testing.expectEqual(@as(usize, 4), src.retained_bytes);
    try std.testing.expectError(error.UnexpectedEndOfSource, src.read(7, 2));
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    src.control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, src.read(0, 1));
    first.deinit();
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}

test "source provider failure and allocation failure release retention" {
    const Fail = struct {
        fn readAt(_: *anyopaque, _: u64, _: []u8, _: Control) !usize {
            return error.SourceFailure;
        }
    };
    var ctx: u8 = 0;
    var src = Source{ .allocator = std.testing.allocator, .identity = "v1", .storage = .{ .range = .{ .context = &ctx, .read_at = Fail.readAt, .length = 10 } } };
    try std.testing.expectError(error.SourceFailure, src.read(0, 3));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    src.allocator = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, src.read(0, 3));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
}

test "source partial EOF, read count and total budgets clean up without refunding IO" {
    const Provider = struct {
        calls: usize = 0,
        eof: bool = false,
        fn readAt(ctx: *anyopaque, _: u64, out: []u8, _: Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (self.eof and self.calls > 1) return 0;
            out[0] = 'x';
            return 1;
        }
    };
    var provider = Provider{ .eof = true };
    var src = Source{ .allocator = std.testing.allocator, .identity = "v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.readAt, .length = 10 } }, .limits = .{ .max_total_bytes = 3 } };
    try std.testing.expectError(error.UnexpectedEndOfSource, src.read(0, 3));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), src.total_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(0, 3));
    provider = .{};
    src.limits.max_reads = src.reads + 1;
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(0, 2));
    try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
    try std.testing.expectEqual(@as(u64, 2), src.total_bytes);
}

test "file range adapter performs independent positional reads" {
    const io = std.testing.io;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const file = try dir.dir.createFile(io, "source", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "abcdefgh");
    var provider = FileRange{ .file = file, .io = io };
    var src = Source{ .allocator = std.testing.allocator, .identity = "immutable-file-v1", .storage = .{ .range = .{ .context = &provider, .read_at = FileRange.readAt, .length = 8 } } };
    var tail = try src.read(4, 4);
    defer tail.deinit();
    var head = try src.read(0, 4);
    defer head.deinit();
    try std.testing.expectEqualStrings("efgh", tail.bytes);
    try std.testing.expectEqualStrings("abcd", head.bytes);
}

test "source shared lease admission is retained and denied without provider reads" {
    var pool = admission.Pool{ .limits = .{ .host_bytes = 6 } };
    var src = Source{ .allocator = std.testing.allocator, .identity = "shared-v1", .storage = .{ .borrowed = "abcdefgh" }, .admission_pool = &pool };
    var first = try src.read(0, 4);
    defer first.deinit();
    const reads = src.reads;
    try std.testing.expectError(error.SharedAdmissionExceeded, src.read(4, 4));
    try std.testing.expectEqual(reads, src.reads);
    first.deinit();
    var retry = try src.read(4, 4);
    retry.deinit();
    try std.testing.expectEqual(admission.Resources{}, pool.snapshot());
}

test "source immutable range borrows enforce limits and fall back across extents" {
    const Provider = struct {
        borrows: usize = 0,
        copies: usize = 0,
        fn borrow(raw: *anyopaque, offset: u64, size: usize, _: Control) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.borrows += 1;
            if (offset < 4 and size > 4 - offset) return null;
            return "abcdefgh"[@intCast(offset)..][0..size];
        }
        fn copy(raw: *anyopaque, offset: u64, target: []u8, _: Control) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.copies += 1;
            @memcpy(target, "abcdefgh"[@intCast(offset)..][0..target.len]);
            return target.len;
        }
    };
    var provider = Provider{};
    var src = Source{ .allocator = std.testing.failing_allocator, .identity = "immutable-extents", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.copy, .borrow_at = Provider.borrow, .length = 8 } }, .limits = .{ .max_read_bytes = 4, .max_retained_bytes = 7, .max_total_bytes = 7, .max_reads = 2 } };
    var first = try src.read(0, 3);
    defer first.deinit();
    try std.testing.expect(first.owned == null);
    try std.testing.expectEqualStrings("abc", first.bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(0, 5));
    try std.testing.expectEqual(@as(usize, 1), provider.borrows);
    // The crossing read needs an owned lease; use a reclaiming allocator.
    src.allocator = std.testing.allocator;
    var crossing = try src.read(2, 4);
    defer crossing.deinit();
    try std.testing.expect(crossing.owned != null);
    try std.testing.expectEqualStrings("cdef", crossing.bytes);
    try std.testing.expectEqual(@as(usize, 1), provider.copies);
    try std.testing.expectError(error.ResourceLimitExceeded, src.read(7, 1));
    try std.testing.expectEqual(@as(usize, 2), provider.borrows);
    try std.testing.expectEqual(@as(u64, 7), src.total_bytes);
}
