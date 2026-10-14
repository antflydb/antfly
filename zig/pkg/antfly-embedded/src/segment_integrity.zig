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

//! Authenticated immutable artifact pages. The directory is authenticated by
//! the container metadata CRC before exposing this source. Only touched pages
//! are checked, and successful checks are shared across immutable snapshots.
const std = @import("std");
const resources = @import("storage/resource_manager.zig");
const Crc32 = @import("antfly_hash").Crc32;
const source_mod = @import("segment_source.zig");
const Allocator = std.mem.Allocator;
pub const page_size: usize = 64 * 1024;
pub const descriptor_size: usize = 20;

pub const Directory = struct {
    offset: u64,
    length: u64,
    checksum: u32,

    pub fn validate(self: Directory, source: source_mod.Source, index: u64) !void {
        if (self.offset > index or self.length != index - self.offset) return error.InvalidSegment;
        const pages = self.offset / page_size + @intFromBool(self.offset % page_size != 0);
        if (self.length != pages * 4) return error.InvalidSegment;
        var scratch: [8192]u8 = undefined;
        if (try source.checksum(self.offset, self.length, &scratch) != self.checksum) return error.CrcMismatch;
    }
    pub fn read(source: source_mod.Source, index: u64) !Directory {
        if (index > source.len() or descriptor_size > source.len() - index) return error.InvalidSegment;
        var bytes: [descriptor_size]u8 = undefined;
        try source.readInto(index, &bytes);
        const directory = Directory{ .offset = std.mem.readInt(u64, bytes[0..8], .big), .length = std.mem.readInt(u64, bytes[8..16], .big), .checksum = std.mem.readInt(u32, bytes[16..20], .big) };
        try directory.validate(source, index);
        return directory;
    }
};

pub const PagedSource = struct {
    allocator: Allocator,
    original: source_mod.Source,
    directory: Directory,
    validations: []std.atomic.Value(u8),
    owns_validations: bool = true,
    mutex: std.atomic.Mutex = .unlocked,
    io_mutex: std.atomic.Mutex = .unlocked,
    buffer: []u8 = &.{},
    cached_page: ?usize = null,
    budget: ?resources.BudgetedAllocator = null,
    reclaimer: u64 = 0,

    pub fn init(allocator: Allocator, original: source_mod.Source, directory: Directory) !*PagedSource {
        const self = try allocator.create(PagedSource);
        errdefer allocator.destroy(self);
        const states = try allocator.alloc(std.atomic.Value(u8), @intCast(directory.length / 4));
        for (states) |*state| state.* = .init(0);
        self.* = .{ .allocator = allocator, .original = original, .directory = directory, .validations = states };
        return self;
    }
    /// Borrow authenticated page states from an immutable, externally pinned
    /// reader. Payload caches and authority remain private to this facade.
    pub fn bind(self: *const PagedSource, allocator: Allocator, original: source_mod.Source) !*PagedSource {
        const bound = try allocator.create(PagedSource);
        bound.* = .{ .allocator = allocator, .original = original, .directory = self.directory, .validations = self.validations, .owns_validations = false };
        return bound;
    }
    // Closing the facade source and freeing decoder metadata are separate:
    // SegmentReader borrows its caller's source; standalone range readers own it.
    pub fn deinit(self: *PagedSource) void {
        const allocator = self.allocator;
        if (self.owns_validations) allocator.free(self.validations);
        if (self.budget) |*budget| budget.reservation.manager.unregisterReclaimer(self.reclaimer);
        self.bufferAllocator().free(self.buffer);
        if (self.budget) |*budget| budget.deinit();
        allocator.destroy(self);
    }
    pub fn source(self: *PagedSource) source_mod.Source {
        return .{ .ranges = .{ .ptr = self, .length = self.original.len(), .read_into = read, .visit_range = visit, .close = close, .prefetch = if (self.original == .ranges and self.original.ranges.prefetch != null) prefetch else null, .resource_manager = self.original.resourceManager() } };
    }
    fn prefetch(ptr: *anyopaque, offset: u64, length: u64) void {
        const self: *PagedSource = @ptrCast(@alignCast(ptr));
        self.original.prefetch(offset, length);
    }
    fn close(ptr: *anyopaque) void {
        const self: *PagedSource = @ptrCast(@alignCast(ptr));
        self.original.close();
    }

    fn bufferAllocator(self: *PagedSource) Allocator {
        return if (self.budget) |*budget| budget.allocator() else self.allocator;
    }
    fn ensureBudget(self: *PagedSource) !void {
        if (self.budget != null) return;
        const manager = self.original.resourceManager() orelse return;
        self.reclaimer = try manager.registerReclaimer(.lite_native_page_cache, self, reclaim);
        self.budget = resources.BudgetedAllocator.initReclaiming(manager, .lite_native_page_cache, self.allocator, 1);
        // Optional cache slabs grow only a few times; exact credits avoid
        // reserving a megabyte for every small artifact layer.
        self.budget.?.credit_quantum = 1;
    }
    fn reclaim(raw: *anyopaque, _: u64) u64 {
        const self: *PagedSource = @ptrCast(@alignCast(raw));
        if (!self.io_mutex.tryLock()) return 0;
        defer self.io_mutex.unlock();
        if (!self.mutex.tryLock()) return 0;
        defer self.mutex.unlock();
        const bytes = self.buffer.len;
        self.bufferAllocator().free(self.buffer);
        self.buffer = &.{};
        self.cached_page = null;
        // Authenticated states survive eviction of optional payload bytes.
        return bytes;
    }

    pub fn retainedBytes(self: *PagedSource) usize {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.buffer.len;
    }
    fn copyCached(self: *PagedSource, index: usize, within: usize, out: []u8) bool {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.cached_page != index) return false;
        @memcpy(out, self.buffer[within..][0..out.len]);
        return true;
    }
    fn readPage(self: *PagedSource, index: usize, within: usize, out: []u8) !void {
        const state = &self.validations[index];
        if (state.load(.acquire) == 2) return error.CrcMismatch;
        if (self.copyCached(index, within, out)) return;
        const offset = @as(u64, index) * page_size;
        if (state.load(.acquire) == 1) return self.readVerified(offset, @intCast(@min(page_size, self.directory.offset - offset)), within, out);
        // One cold validator owns the bounded page buffer. Verified reads and
        // cache hits never wait for backend I/O on this separate owner lock.
        @import("antfly_platform").sync.lockYielding(&self.io_mutex);
        defer self.io_mutex.unlock();
        if (state.load(.acquire) == 2) return error.CrcMismatch;
        if (self.copyCached(index, within, out)) return;
        if (state.load(.acquire) == 1) return self.readVerified(offset, @intCast(@min(page_size, self.directory.offset - offset)), within, out);
        const length: usize = @intCast(@min(page_size, self.directory.offset - offset));
        if (self.original == .contiguous) {
            const bytes = self.original.contiguous[@intCast(offset)..][0..length];
            const expected = self.original.contiguous[@intCast(self.directory.offset + @as(u64, index) * 4)..][0..4];
            if (Crc32.hash(bytes) != std.mem.readInt(u32, expected, .big)) {
                state.store(2, .release);
                return error.CrcMismatch;
            }
            state.store(1, .release);
            @memcpy(out, bytes[within..][0..out.len]);
            return;
        }
        // Only an explicit fused capability guarantees authentication and
        // delivery share the provider's cache/traversal. A checksum callback
        // alone may bypass that cache and duplicate cold and warm reads.
        if (self.original.ranges.read_authenticated) |read_authenticated| {
            var expected: [4]u8 = undefined;
            try self.original.readInto(self.directory.offset + @as(u64, index) * 4, &expected);
            read_authenticated(self.original.ranges.ptr, offset, length, within, out, std.mem.readInt(u32, &expected, .big)) catch |err| {
                if (err == error.CrcMismatch) state.store(2, .release);
                return err;
            };
            state.store(1, .release);
            return;
        }
        // Invalidate before touching bytes: failed/partial reads cannot expose
        // the old cache entry. Publish only after authentication completes.
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.cached_page = null;
        self.mutex.unlock();
        self.ensureBudget() catch return self.authenticateRange(index, offset, length, within, out);
        if (self.buffer.len == 0) {
            const buffer = self.bufferAllocator().alloc(u8, @intCast(@min(page_size, self.directory.offset))) catch {
                // The caller allocator may also impose a hard limit. This
                // buffer is optional regardless of who denied admission.
                // Mandatory authentication uses fixed worker scratch when
                // the optional shared cache is full. No uncharged heap page
                // survives pressure or scales with open reader count.
                return self.authenticateRange(index, offset, length, within, out);
            };
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            self.buffer = buffer;
            self.mutex.unlock();
        }
        try self.original.readInto(offset, self.buffer[0..length]);
        var expected: [4]u8 = undefined;
        try self.original.readInto(self.directory.offset + @as(u64, index) * 4, &expected);
        if (Crc32.hash(self.buffer[0..length]) != std.mem.readInt(u32, &expected, .big)) {
            state.store(2, .release);
            return error.CrcMismatch;
        }
        state.store(1, .release);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.cached_page = index;
        @memcpy(out, self.buffer[within..][0..out.len]);
    }
    fn readVerified(self: *PagedSource, offset: u64, length: usize, within: usize, out: []u8) !void {
        if (self.original == .ranges) if (self.original.ranges.read_authenticated) |read_authenticated| {
            return read_authenticated(self.original.ranges.ptr, offset, length, within, out, null);
        };
        return self.original.readInto(offset + within, out);
    }
    fn authenticateRange(self: *PagedSource, index: usize, offset: u64, length: usize, within: usize, out: []u8) !void {
        var scratch: [8192]u8 = undefined;
        const actual = try self.original.checksum(offset, length, &scratch);
        var expected: [4]u8 = undefined;
        try self.original.readInto(self.directory.offset + @as(u64, index) * 4, &expected);
        const state = &self.validations[index];
        if (actual != std.mem.readInt(u32, &expected, .big)) {
            state.store(2, .release);
            return error.CrcMismatch;
        }
        state.store(1, .release);
        try self.original.readInto(offset + within, out);
    }
    fn visit(ptr: *anyopaque, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        const self: *PagedSource = @ptrCast(@alignCast(ptr));
        const Forward = struct {
            context: *anyopaque,
            consume: *const fn (*anyopaque, u64, []const u8) anyerror!void,
            base: u64,
            fn visit(raw: *anyopaque, relative: u64, bytes: []const u8) !void {
                const state: *@This() = @ptrCast(@alignCast(raw));
                try state.consume(state.context, state.base + relative, bytes);
            }
        };
        var position: u64 = 0;
        while (position < length) {
            const absolute = offset + position;
            var forward = Forward{ .context = context, .consume = consume, .base = position };
            if (absolute >= self.directory.offset) {
                return self.original.visitRange(absolute, length - position, &forward, Forward.visit);
            }
            const index: usize = @intCast(absolute / page_size);
            const within: usize = @intCast(absolute % page_size);
            var take: usize = @intCast(@min(@min(length - position, page_size - within), self.directory.offset - absolute));
            const state = self.validations[index].load(.acquire);
            if (state == 2) return error.CrcMismatch;
            if (self.original == .contiguous) {
                // Authenticate the complete immutable page before borrowing it.
                // A zero-byte delivery checks CRC without copying page bytes.
                try self.readPage(index, 0, &.{});
                try self.original.visitRange(absolute, take, &forward, Forward.visit);
            } else if (state == 1 and self.original.ranges.visit_range != null) {
                // Verified adjacent pages share one native cursor traversal.
                // Stop before an unknown or failed page; neither may be exposed.
                const limit = @min(length - position, self.directory.offset - absolute);
                var next = index + 1;
                while (take < limit and next < self.validations.len and self.validations[next].load(.acquire) == 1) : (next += 1) {
                    take += @intCast(@min(limit - take, page_size));
                }
                try self.original.visitRange(absolute, take, &forward, Forward.visit);
            } else if (self.original.ranges.visit_range != null and @min(length - position, self.directory.offset - absolute) > page_size - within) {
                // Single-page reads keep fused cache admission; repeated small
                // visits can then borrow resident bytes without backing reads.
                // One forward traversal authenticates cold pages with bounded
                // private scratch, rather than reopening a cursor per page.
                const remaining = @min(length - position, self.directory.offset - absolute);
                try self.visitColdRange(absolute, remaining, context, position, consume);
                position += remaining;
                continue;
            } else {
                try self.visitBufferedPage(index, within, take, context, position, consume);
            }
            position += take;
        }
    }

    /// Scratch belongs to this traversal, not the reader or a shared cache.
    /// Provider and consumer callbacks run without our locks, so reentrant
    /// readers are safe. Immutable pages may be validated concurrently.
    noinline fn visitColdRange(self: *PagedSource, offset: u64, length: u64, context: *anyopaque, relative: u64, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        const Stream = struct {
            owner: *PagedSource,
            context: *anyopaque,
            consume: *const fn (*anyopaque, u64, []const u8) anyerror!void,
            requested_start: u64,
            requested_end: u64,
            relative: u64,
            start: u64,
            position: u64,
            buffering: bool = false,
            page: [page_size]u8 = undefined,
            crc_window: [1024]u8 = undefined,
            crc_start: usize = 0,
            crc_count: usize = 0,

            fn expected(self_: *@This(), index: usize) !u32 {
                if (index < self_.crc_start or index - self_.crc_start >= self_.crc_count) {
                    self_.crc_start = index;
                    self_.crc_count = @min(self_.crc_window.len / 4, self_.owner.validations.len - index);
                    try self_.owner.original.readInto(self_.owner.directory.offset + @as(u64, index) * 4, self_.crc_window[0 .. self_.crc_count * 4]);
                }
                return std.mem.readInt(u32, self_.crc_window[(index - self_.crc_start) * 4 ..][0..4], .big);
            }
            fn deliver(self_: *@This(), absolute: u64, bytes: []const u8) !void {
                const first = @max(absolute, self_.requested_start);
                const last = @min(absolute + bytes.len, self_.requested_end);
                if (first < last) try self_.consume(self_.context, self_.relative + first - self_.requested_start, bytes[@intCast(first - absolute)..@intCast(last - absolute)]);
            }
            fn visit(raw: *anyopaque, provider_relative: u64, bytes: []const u8) !void {
                const stream: *@This() = @ptrCast(@alignCast(raw));
                if (provider_relative != stream.position - stream.start) return error.InvalidSegment;
                var copied: usize = 0;
                while (copied < bytes.len) {
                    const index: usize = @intCast(stream.position / page_size);
                    const within: usize = @intCast(stream.position % page_size);
                    const page_start = @as(u64, index) * page_size;
                    const page_length: usize = @intCast(@min(page_size, stream.owner.directory.offset - page_start));
                    const take = @min(bytes.len - copied, page_length - within);
                    const validation = &stream.owner.validations[index];
                    const state = validation.load(.acquire);
                    if (state == 2) return error.CrcMismatch;
                    // Choose once at the page boundary. If another validator
                    // finishes midway, retain our cold prefix until delivery.
                    if (within == 0) stream.buffering = state != 1;
                    if (!stream.buffering) {
                        try stream.deliver(stream.position, bytes[copied..][0..take]);
                        stream.position += take;
                        copied += take;
                        continue;
                    }
                    // Always retain cold-page fragments until the full CRC is
                    // checked. Another validator completing midway cannot
                    // expose a suffix while dropping our buffered prefix.
                    @memcpy(stream.page[within..][0..take], bytes[copied..][0..take]);
                    stream.position += take;
                    copied += take;
                    if (within + take == page_length) {
                        if (state != 1) {
                            if (Crc32.hash(stream.page[0..page_length]) != try stream.expected(index)) {
                                validation.store(2, .release);
                                return error.CrcMismatch;
                            }
                            validation.store(1, .release);
                        }
                        try stream.deliver(page_start, stream.page[0..page_length]);
                    }
                }
            }
        };
        const start = offset / page_size * page_size;
        const end = @min(self.directory.offset, (offset + length + page_size - 1) / page_size * page_size);
        var stream = Stream{ .owner = self, .context = context, .consume = consume, .requested_start = offset, .requested_end = offset + length, .relative = relative, .start = start, .position = start };
        try self.original.visitRange(start, end - start, &stream, Stream.visit);
        if (stream.position != end) return error.InvalidSegment;
    }

    noinline fn visitBufferedPage(self: *PagedSource, index: usize, within: usize, take: usize, context: *anyopaque, relative: u64, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        var buffer: [page_size]u8 = undefined;
        try self.readPage(index, within, buffer[0..take]);
        try consume(context, relative, buffer[0..take]);
    }

    fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *PagedSource = @ptrCast(@alignCast(ptr));
        var copied: usize = 0;
        while (copied < out.len) {
            const position = offset + copied;
            if (position >= self.directory.offset) return self.original.readInto(position, out[copied..]);
            const index: usize = @intCast(position / page_size);
            const within: usize = @intCast(position % page_size);
            const take: usize = @intCast(@min(@min(out.len - copied, page_size - within), self.directory.offset - position));
            try self.readPage(index, within, out[copied..][0..take]);
            copied += take;
        }
    }
};

test "segment.fused provider authentication avoids duplicate slabs and fails closed" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, page_size + 4);
    defer a.free(bytes);
    @memset(bytes[0..page_size], 7);
    std.mem.writeInt(u32, bytes[page_size..][0..4], Crc32.hash(bytes[0..page_size]), .big);
    const Backend = struct {
        bytes: []u8,
        checksums: usize = 0,
        fail: bool = false,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn checksum(raw: *anyopaque, offset: u64, length: u64) !u32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checksums += 1;
            if (self.fail) return error.TestIoFailure;
            return Crc32.hash(self.bytes[@intCast(offset)..][0..@intCast(length)]);
        }
        fn authenticate(raw: *anyopaque, offset: u64, length: u64, within: usize, out: []u8, expected: ?u32) !void {
            if (expected) |crc| if (try checksum(raw, offset, length) != crc) return error.CrcMismatch;
            return read(raw, offset + within, out);
        }
        fn close(_: *anyopaque) void {}
    };
    var backend = Backend{ .bytes = bytes, .fail = true };
    const original = source_mod.Source{ .ranges = .{ .ptr = &backend, .length = bytes.len, .read_into = Backend.read, .checksum = Backend.checksum, .read_authenticated = Backend.authenticate, .close = Backend.close } };
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 4096 };
    const directory = Directory{ .offset = page_size, .length = 4, .checksum = Crc32.hash(bytes[page_size..]) };
    const paged = try PagedSource.init(budget.allocator(), original, directory);
    var alive = true;
    defer if (alive) paged.deinit();
    var out: [8]u8 = undefined;
    try std.testing.expectError(error.TestIoFailure, paged.source().readInto(3, &out));
    backend.fail = false;
    try paged.source().readInto(3, &out);
    try std.testing.expectEqualSlices(u8, bytes[3..11], &out);
    try std.testing.expectEqual(@as(usize, 2), backend.checksums);
    try paged.source().readInto(123, &out);
    try std.testing.expectEqual(@as(usize, 2), backend.checksums);
    try std.testing.expectEqual(@as(usize, 0), paged.retainedBytes());
    paged.deinit();
    alive = false;
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    const corrupt = try PagedSource.init(budget.allocator(), original, directory);
    defer corrupt.deinit();
    bytes[7] ^= 1;
    try std.testing.expectError(error.CrcMismatch, corrupt.source().readInto(3, &out));
    bytes[7] ^= 1;
    const calls = backend.checksums;
    try std.testing.expectError(error.CrcMismatch, corrupt.source().readInto(3, &out));
    try std.testing.expectEqual(calls, backend.checksums);
    try std.testing.expectEqual(@as(usize, 0), corrupt.retainedBytes());
}

test "segment borrowed spans authenticate cold pages and borrow verified ranges" {
    const a = std.testing.allocator;
    const payload_len = page_size + 37;
    const bytes = try a.alloc(u8, payload_len + 8);
    defer a.free(bytes);
    @memset(bytes[0..payload_len], 'p');
    std.mem.writeInt(u32, bytes[payload_len..][0..4], Crc32.hash(bytes[0..page_size]), .big);
    std.mem.writeInt(u32, bytes[payload_len + 4 ..][0..4], Crc32.hash(bytes[page_size..payload_len]), .big);
    const directory = Directory{ .offset = payload_len, .length = 8, .checksum = Crc32.hash(bytes[payload_len..]) };
    const Consumer = struct {
        expected: []const u8,
        borrowed: usize = 0,
        used: usize = 0,
        fn consume(raw: *anyopaque, relative: u64, span: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.used, relative);
            try std.testing.expectEqualSlices(u8, self.expected[self.used..][0..span.len], span);
            if (@intFromPtr(self.expected.ptr) + self.used == @intFromPtr(span.ptr)) self.borrowed += span.len;
            self.used += span.len;
        }
    };
    const Backend = struct {
        bytes: []const u8,
        reads: usize = 0,
        visits: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn visit(raw: *anyopaque, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.visits += 1;
            try consume(context, 0, self.bytes[@intCast(offset)..][0..@intCast(length)]);
        }
        fn close(_: *anyopaque) void {}
    };
    var backend = Backend{ .bytes = bytes };
    const ranged = source_mod.Source{ .ranges = .{ .ptr = &backend, .length = bytes.len, .read_into = Backend.read, .visit_range = Backend.visit, .close = Backend.close } };
    for ([_]source_mod.Source{ .{ .contiguous = bytes }, ranged }) |source| {
        const paged = try PagedSource.init(a, source, directory);
        defer paged.deinit();
        const view = try source_mod.View.init(paged.source(), 11, payload_len - 22);
        var consumer = Consumer{ .expected = bytes[11 .. payload_len - 11] };
        try view.visitRange(0, view.length, &consumer, Consumer.consume);
        try std.testing.expectEqual(view.length, consumer.used);
        if (source == .contiguous) try std.testing.expectEqual(view.length, consumer.borrowed);
        backend.reads = 0;
        backend.visits = 0;
        consumer.used = 0;
        consumer.borrowed = 0;
        try view.visitRange(0, view.length, &consumer, Consumer.consume);
        try std.testing.expectEqual(@as(usize, 0), backend.reads);
        try std.testing.expectEqual(view.length, consumer.borrowed);
        if (source == .ranges) try std.testing.expectEqual(@as(usize, 1), backend.visits);
    }
    bytes[11] ^= 1;
    const corrupt = try PagedSource.init(a, .{ .contiguous = bytes }, directory);
    defer corrupt.deinit();
    var consumer = Consumer{ .expected = bytes[11 .. payload_len - 11] };
    const view = try source_mod.View.init(corrupt.source(), 11, payload_len - 22);
    try std.testing.expectError(error.CrcMismatch, view.visitRange(0, view.length, &consumer, Consumer.consume));
    try std.testing.expectEqual(@as(usize, 0), consumer.used);
    try std.testing.expectError(error.CrcMismatch, view.visitRange(0, view.length, &consumer, Consumer.consume));
}

test "segment cold streaming handles fragmented pages, tails, reentrancy and failures within a bounded budget" {
    const a = std.testing.allocator;
    const payload_len = 2 * page_size + 37;
    const bytes = try a.alloc(u8, payload_len + 12);
    defer a.free(bytes);
    for (bytes[0..payload_len], 0..) |*byte, index| byte.* = @truncate(index);
    for (0..3) |index| {
        const start = index * page_size;
        std.mem.writeInt(u32, bytes[payload_len + index * 4 ..][0..4], Crc32.hash(bytes[start..@min(start + page_size, payload_len)]), .big);
    }
    const directory = Directory{ .offset = payload_len, .length = 12, .checksum = Crc32.hash(bytes[payload_len..]) };
    const Backend = struct {
        bytes: []const u8,
        visits: usize = 0,
        fail_directory: bool = false,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_directory and offset >= payload_len) return error.TestIoFailure;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn visit(raw: *anyopaque, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.visits += 1;
            var position: usize = 0;
            while (position < length) {
                const take: usize = @intCast(@min(997, length - position));
                try consume(context, position, self.bytes[@intCast(offset + position)..][0..take]);
                position += take;
            }
        }
        fn close(_: *anyopaque) void {}
    };
    const Consumer = struct {
        expected: []const u8,
        source: source_mod.Source,
        used: usize = 0,
        fail: bool = false,
        fn consume(raw: *anyopaque, relative: u64, span: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.used, relative);
            try std.testing.expectEqualSlices(u8, self.expected[self.used..][0..span.len], span);
            var nested: [7]u8 = undefined;
            try self.source.readInto(0, &nested);
            try std.testing.expectEqualSlices(u8, &.{ 0, 1, 2, 3, 4, 5, 6 }, &nested);
            if (self.fail) return error.TestConsumerFailure;
            self.used += span.len;
        }
    };
    var backend = Backend{ .bytes = bytes };
    const original = source_mod.Source{ .ranges = .{ .ptr = &backend, .length = bytes.len, .read_into = Backend.read, .visit_range = Backend.visit, .close = Backend.close } };
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 4096 };
    for ([_]usize{ 0, 11, page_size - 3, page_size + 5 }) |start| {
        const paged = try PagedSource.init(budget.allocator(), original, directory);
        defer paged.deinit();
        const end = bytes.len - 2; // includes the authenticated directory tail
        var consumer = Consumer{ .expected = bytes[start..end], .source = paged.source() };
        const visits = backend.visits;
        try paged.source().visitRange(start, end - start, &consumer, Consumer.consume);
        try std.testing.expectEqual(end - start, consumer.used);
        try std.testing.expectEqual(@as(usize, 2), backend.visits - visits);
        try std.testing.expectEqual(@as(usize, 0), paged.retainedBytes());
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    const paged = try PagedSource.init(budget.allocator(), original, directory);
    defer paged.deinit();
    var consumer = Consumer{ .expected = bytes[0..payload_len], .source = paged.source(), .fail = true };
    try std.testing.expectError(error.TestConsumerFailure, paged.source().visitRange(0, payload_len, &consumer, Consumer.consume));
    try std.testing.expectEqual(@as(usize, 0), consumer.used);
    consumer.fail = false;
    backend.fail_directory = true;
    try std.testing.expectError(error.TestIoFailure, paged.source().visitRange(0, payload_len, &consumer, Consumer.consume));
    try std.testing.expectEqual(page_size, consumer.used); // first page was already verified
    backend.fail_directory = false;
    consumer.used = 0;
    bytes[2 * page_size + 7] ^= 1;
    try std.testing.expectError(error.CrcMismatch, paged.source().visitRange(0, payload_len, &consumer, Consumer.consume));
    try std.testing.expectEqual(2 * page_size, consumer.used);
    bytes[2 * page_size + 7] ^= 1;
    consumer.used = 0;
    try std.testing.expectError(error.CrcMismatch, paged.source().visitRange(0, payload_len, &consumer, Consumer.consume));
    try std.testing.expectEqual(2 * page_size, consumer.used);
}

test "segment.mixed authentication borrows verified suffix after a cold prefix" {
    const a = std.testing.allocator;
    const pages = 64;
    const length = pages * page_size;
    const bytes = try a.alloc(u8, length + pages * 4);
    defer a.free(bytes);
    @memset(bytes[0..length], 'x');
    for (0..pages) |index| std.mem.writeInt(u32, bytes[length + index * 4 ..][0..4], Crc32.hash(bytes[index * page_size ..][0..page_size]), .big);
    const Backend = struct {
        bytes: []const u8,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn visit(raw: *anyopaque, offset: u64, size: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try consume(context, 0, self.bytes[@intCast(offset)..][0..@intCast(size)]);
        }
        fn close(_: *anyopaque) void {}
    };
    const Consumer = struct {
        expected: []const u8,
        copied: usize = 0,
        borrowed: usize = 0,
        fn consume(raw: *anyopaque, relative: u64, span: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualSlices(u8, self.expected[@intCast(relative)..][0..span.len], span);
            if (@intFromPtr(span.ptr) == @intFromPtr(self.expected.ptr) + relative) self.borrowed += span.len else self.copied += span.len;
        }
    };
    var backend = Backend{ .bytes = bytes };
    const original = source_mod.Source{ .ranges = .{ .ptr = &backend, .length = bytes.len, .read_into = Backend.read, .visit_range = Backend.visit, .close = Backend.close } };
    const paged = try PagedSource.init(a, original, .{ .offset = length, .length = pages * 4, .checksum = Crc32.hash(bytes[length..]) });
    defer paged.deinit();
    var one: [1]u8 = undefined;
    for (1..pages) |index| try paged.source().readInto(index * page_size, &one);
    var mixed = Consumer{ .expected = bytes[0..length] };
    try paged.source().visitRange(0, length, &mixed, Consumer.consume);
    var warm = Consumer{ .expected = bytes[0..length] };
    try paged.source().visitRange(0, length, &warm, Consumer.consume);
    std.debug.print("FRESH_MIXED_PAGES cold_pages=1 verified_pages=63 mixed_staged_bytes={d} mixed_borrowed_bytes={d} warm_staged_bytes={d} warm_borrowed_bytes={d}\n", .{ mixed.copied, mixed.borrowed, warm.copied, warm.borrowed });
    try std.testing.expectEqual(@as(usize, page_size), mixed.copied);
    try std.testing.expectEqual(@as(usize, length - page_size), mixed.borrowed);
    try std.testing.expectEqual(@as(usize, 0), warm.copied);
}
