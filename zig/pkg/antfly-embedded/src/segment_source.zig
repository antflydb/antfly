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

//! Immutable segment bytes without a contiguous-file requirement. Range
//! implementations own a generation pin until close; a reader consumes that
//! ownership only after successful initialization. Reads fill caller buffers,
//! so a cache cannot invalidate a borrowed slice underneath a decoder.
//! A contiguous source borrows its buffer. Range-source storage owners must
//! outlive the source unless that backend explicitly transfers an owner lease.
const std = @import("std");
const resources = @import("storage/resource_manager.zig");
const Crc32 = @import("antfly_hash").Crc32;

/// An immutable mapping plus an optional independent accounting lease. The
/// lease must outlive its provider and is released after the inode mapping.
pub const MappedArtifact = struct {
    bytes: []align(std.heap.page_size_min) u8,
    context: ?*anyopaque = null,
    release: ?*const fn (*anyopaque) void = null,

    pub fn deinit(self: *MappedArtifact) void {
        if (@import("builtin").os.tag != .freestanding) std.posix.munmap(self.bytes);
        if (self.release) |release| release(self.context.?);
        self.* = undefined;
    }
};

pub const Source = union(enum) {
    contiguous: []const u8,
    ranges: struct {
        ptr: *anyopaque,
        length: u64,
        read_into: *const fn (*anyopaque, u64, []u8) anyerror!void,
        close: *const fn (*anyopaque) void,
        /// Advisory bounded lookahead; errors belong to subsequent required reads.
        prefetch: ?*const fn (*anyopaque, u64, u64) void = null,
        checksum: ?*const fn (*anyopaque, u64, u64) anyerror!u32 = null,
        /// Authenticate an immutable page and deliver a subrange in one pass.
        /// Null expected CRC means this page was already authenticated. Output
        /// is unusable on error; providers must respect the same read authority.
        read_authenticated: ?*const fn (*anyopaque, u64, u64, usize, []u8, ?u32) anyerror!void = null,
        /// Visit immutable range bytes in order without restarting navigation.
        /// Visitor slices are borrowed only for the duration of the callback.
        visit_range: ?*const fn (*anyopaque, u64, u64, *anyopaque, *const fn (*anyopaque, u64, []const u8) anyerror!void) anyerror!void = null,
        retained_bytes: ?*const fn (*anyopaque) usize = null,
        /// Bind an independent query capability without changing the shared source.
        bind_read_context: ?*const fn (*anyopaque, std.mem.Allocator, *anyopaque) anyerror!Source = null,
        seal_read_context: ?*const fn (*anyopaque) void = null,
        quiesce_read_context: ?*const fn (*anyopaque) void = null,
        resource_manager: ?*resources.ResourceManager = null,
        read_io: ?std.Io = null,
        check_read_context: ?*const fn (*anyopaque) anyerror!void = null,
        /// Opt-in idle expiry for source caches. A successful acquisition
        /// protects all source operations until its matching release.
        acquire_use: ?*const fn (*anyopaque) bool = null,
        release_use: ?*const fn (*anyopaque) void = null,
        enable_idle_expiry: ?*const fn (*anyopaque) void = null,
    },

    pub fn acquireUse(self: Source) bool {
        return if (self == .ranges) if (self.ranges.acquire_use) |acquire| acquire(self.ranges.ptr) else true else true;
    }
    pub fn releaseUse(self: Source) void {
        if (self == .ranges) if (self.ranges.release_use) |release| release(self.ranges.ptr);
    }
    pub fn enableIdleExpiry(self: Source) void {
        if (self == .ranges) if (self.ranges.enable_idle_expiry) |enable| enable(self.ranges.ptr);
    }

    /// Stop query-owned work before its borrowed capability is released.
    pub fn quiesceReadContext(self: Source) void {
        if (self == .ranges) if (self.ranges.quiesce_read_context) |quiesce| quiesce(self.ranges.ptr);
    }

    pub fn resourceManager(self: Source) ?*resources.ResourceManager {
        return switch (self) {
            .contiguous => null,
            .ranges => |range| range.resource_manager,
        };
    }

    pub fn len(self: Source) u64 {
        return switch (self) {
            .contiguous => |bytes| bytes.len,
            .ranges => |range| range.length,
        };
    }

    /// Exact reads only. A backend must report truncation rather than return
    /// partially initialized output. Bounds are checked before calling it.
    pub fn readInto(self: Source, offset: u64, out: []u8) !void {
        if (offset > self.len() or out.len > self.len() - offset) return error.EndOfStream;
        if (out.len == 0) return;
        switch (self) {
            .contiguous => |bytes| @memcpy(out, bytes[@intCast(offset)..][0..out.len]),
            .ranges => |range| try range.read_into(range.ptr, offset, out),
        }
    }

    /// Visit bounded immutable spans. Consumers must discard private output on
    /// any error; providers may fail after delivering a prefix. Never retain slices.
    pub fn visitRange(self: Source, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        if (offset > self.len() or length > self.len() - offset) return error.EndOfStream;
        if (length == 0) return;
        const Checked = struct {
            context: *anyopaque,
            consume: *const fn (*anyopaque, u64, []const u8) anyerror!void,
            length: u64,
            position: u64 = 0,
            fn visit(raw: *anyopaque, relative: u64, bytes: []const u8) !void {
                const state: *@This() = @ptrCast(@alignCast(raw));
                if (relative != state.position or bytes.len == 0 or bytes.len > state.length - state.position) return error.InvalidData;
                var used: usize = 0;
                while (used < bytes.len) {
                    const take = @min(bytes.len - used, 64 * 1024);
                    try state.consume(state.context, state.position, bytes[used..][0..take]);
                    used += take;
                    state.position += take;
                }
            }
        };
        var checked = Checked{ .context = context, .consume = consume, .length = length };
        if (self == .contiguous) {
            try Checked.visit(&checked, 0, self.contiguous[@intCast(offset)..][0..@intCast(length)]);
        } else if (self.ranges.visit_range) |visit| {
            try visit(self.ranges.ptr, offset, length, &checked, Checked.visit);
        } else {
            try self.visitBuffered(offset, length, &checked, Checked.visit);
        }
        if (checked.position != length) return error.EndOfStream;
    }

    // Keep exceptional staging off the borrowed fast path's stack frame.
    noinline fn visitBuffered(self: Source, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        var buffer: [64 * 1024]u8 = undefined;
        var position: u64 = 0;
        while (position < length) {
            const take: usize = @intCast(@min(buffer.len, length - position));
            try self.readInto(offset + position, buffer[0..take]);
            try consume(context, position, buffer[0..take]);
            position += take;
        }
    }

    pub fn prefetch(self: Source, offset: u64, length: u64) void {
        if (offset > self.len() or length > self.len() - offset or length == 0) return;
        if (self == .ranges) if (self.ranges.prefetch) |hint| hint(self.ranges.ptr, offset, length);
    }

    pub fn retainedBytes(self: Source) usize {
        return switch (self) {
            .contiguous => 0,
            .ranges => |range| if (range.retained_bytes) |read| read(range.ptr) else 0,
        };
    }

    pub fn close(self: *Source) void {
        switch (self.*) {
            .contiguous => {},
            .ranges => |range| range.close(range.ptr),
        }
        self.* = .{ .contiguous = &.{} };
    }

    pub fn checksum(self: Source, offset: u64, length: u64, scratch: []u8) !u32 {
        if (offset > self.len() or length > self.len() - offset) return error.EndOfStream;
        if (scratch.len == 0) return error.InvalidScratch;
        if (self == .ranges) {
            if (self.ranges.checksum) |hash| return hash(self.ranges.ptr, offset, length);
        }
        var crc = Crc32.init();
        var pos: u64 = 0;
        while (pos < length) {
            const count: usize = @intCast(@min(scratch.len, length - pos));
            try self.readInto(offset + pos, scratch[0..count]);
            crc.update(scratch[0..count]);
            pos += count;
        }
        return crc.final();
    }
};

/// Decoder scratch has an explicit owner and is reusable between blocks.
/// Oversized buffers are freed on reset without consolidation; ordinary blocks
/// retain at most the configured limit. Returned values must use another
/// allocator whenever they outlive reset().
pub const Scratch = struct {
    arena: std.heap.ArenaAllocator,
    retained_limit: usize,

    pub fn init(backing: std.mem.Allocator, retained_limit: usize) Scratch {
        return .{ .arena = std.heap.ArenaAllocator.init(backing), .retained_limit = retained_limit };
    }

    pub fn allocator(self: *Scratch) std.mem.Allocator {
        return self.arena.allocator();
    }

    pub fn reset(self: *Scratch) void {
        // Checking first avoids allocating a consolidated oversized arena.
        if (self.arena.queryCapacity() > self.retained_limit) {
            _ = self.arena.reset(.free_all);
        } else {
            _ = self.arena.reset(.retain_capacity);
        }
    }

    pub fn deinit(self: *Scratch) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

test "segment source exact ranges propagate errors and bound checksum scratch" {
    const State = struct {
        bytes: []const u8,
        max_read: usize = 0,
        closed: bool = false,
        fail: bool = false,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.TestIoFailure;
            self.max_read = @max(self.max_read, out.len);
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
        }
    };
    const bytes = try std.testing.allocator.alloc(u8, 256 * 1024);
    defer std.testing.allocator.free(bytes);
    @memset(bytes, 'x');
    var state = State{ .bytes = bytes };
    var source = Source{ .ranges = .{ .ptr = &state, .length = bytes.len, .read_into = State.read, .close = State.close } };
    var scratch: [1024]u8 = undefined;
    try std.testing.expectEqual(Crc32.hash(bytes), try source.checksum(0, bytes.len, &scratch));
    try std.testing.expectEqual(scratch.len, state.max_read);
    try std.testing.expectError(error.EndOfStream, source.readInto(bytes.len, &scratch));
    state.fail = true;
    try std.testing.expectError(error.TestIoFailure, source.readInto(0, &scratch));
    source.close();
    try std.testing.expect(state.closed);
}

test "segment scratch reuses ordinary capacity and releases oversized requests" {
    var scratch = Scratch.init(std.testing.allocator, 64 * 1024);
    defer scratch.deinit();
    _ = try scratch.allocator().alloc(u8, 1024);
    scratch.reset();
    const retained = scratch.arena.queryCapacity();
    for (0..100) |_| {
        _ = try scratch.allocator().alloc(u8, 1024);
        scratch.reset();
        try std.testing.expectEqual(retained, scratch.arena.queryCapacity());
    }
    _ = try scratch.allocator().alloc(u8, 256 * 1024);
    scratch.reset();
    try std.testing.expectEqual(@as(usize, 0), scratch.arena.queryCapacity());
}

/// Query/merge-owned cache. Reads copy into caller buffers: eviction never
/// invalidates a decoder's slice. The source is borrowed and must outlive the
/// cache. Integrity scans bypass it to preserve useful random-read blocks.
pub const BlockCache = struct {
    const Slot = struct {
        bytes: []u8 = &.{},
        offset: u64 = 0,
        valid_len: usize = 0,
        age: u64 = 0,
        pins: usize = 0,
    };
    allocator: std.mem.Allocator,
    source: Source,
    block_size: usize,
    slots: [4]Slot = @splat(.{}),
    clock: u64 = 0,
    misses: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, source: Source, byte_budget: usize) !BlockCache {
        if (byte_budget < 4) return error.InvalidScratch;
        return .{ .allocator = allocator, .source = source, .block_size = @min(64 * 1024, byte_budget / 4) };
    }

    pub fn deinit(self: *BlockCache) void {
        for (self.slots) |slot| self.allocator.free(slot.bytes);
        self.* = undefined;
    }

    /// Borrowed adapter for decoders. Closing it does not close the cache or
    /// parent source; the cache must remain at a stable address until use ends.
    pub fn borrowedSource(self: *BlockCache) Source {
        return .{ .ranges = .{ .ptr = self, .length = self.source.len(), .read_into = readAdapter, .checksum = checksumAdapter, .close = closeAdapter, .prefetch = if (self.source == .ranges and self.source.ranges.prefetch != null) prefetchAdapter else null, .resource_manager = self.source.resourceManager() } };
    }

    fn prefetchAdapter(ptr: *anyopaque, offset: u64, length: u64) void {
        const self: *BlockCache = @ptrCast(@alignCast(ptr));
        self.source.prefetch(offset, length);
    }
    fn readAdapter(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *BlockCache = @ptrCast(@alignCast(ptr));
        try self.readInto(offset, out);
    }

    fn checksumAdapter(ptr: *anyopaque, offset: u64, length: u64) !u32 {
        const self: *BlockCache = @ptrCast(@alignCast(ptr));
        var buffer: [8192]u8 = undefined;
        return self.source.checksum(offset, length, &buffer);
    }

    fn closeAdapter(_: *anyopaque) void {}

    pub fn retainedBytes(self: *const BlockCache) usize {
        var size: usize = 0;
        for (self.slots) |slot| size += slot.bytes.len;
        return size;
    }

    pub fn readInto(self: *BlockCache, offset: u64, out: []u8) !void {
        if (offset > self.source.len() or out.len > self.source.len() - offset) return error.EndOfStream;
        var copied: usize = 0;
        while (copied < out.len) {
            const position = offset + copied;
            const block_offset = position - position % self.block_size;
            const within: usize = @intCast(position - block_offset);
            const slot = try self.load(block_offset);
            const take = @min(out.len - copied, slot.valid_len - within);
            @memcpy(out[copied..][0..take], slot.bytes[within..][0..take]);
            copied += take;
        }
    }

    fn load(self: *BlockCache, offset: u64) !*Slot {
        self.clock +%= 1;
        var oldest = &self.slots[0];
        for (&self.slots) |*slot| {
            if (slot.valid_len != 0 and slot.offset == offset) {
                slot.age = self.clock;
                return slot;
            }
            if (slot.age < oldest.age) oldest = slot;
        }
        // Invalidate BEFORE reading: a failed exact read can have modified
        // bytes. Retry must never serve the partially overwritten old block.
        oldest.valid_len = 0;
        oldest.age = 0;
        if (oldest.bytes.len == 0) oldest.bytes = try self.allocator.alloc(u8, self.block_size);
        const length: usize = @intCast(@min(self.block_size, self.source.len() - offset));
        try self.source.readInto(offset, oldest.bytes[0..length]);
        oldest.offset = offset;
        oldest.valid_len = length;
        oldest.age = self.clock;
        self.misses += 1;
        return oldest;
    }
};

/// Shared immutable range cache. Backend I/O owns a separate bounded fill
/// buffer; cache hits never wait for a cold read. Closing the adapter does not
/// close its source. The owner must exclude readers before deinit.
pub const ConcurrentBlockCache = struct {
    cache: BlockCache,
    mutex: std.atomic.Mutex = .unlocked,
    fill_mutex: std.atomic.Mutex = .unlocked,
    wait_mutex: std.Io.Mutex = .init,
    changed: std.Io.Condition = .init,
    flights: [4]Flight = @splat(.{}),
    budget: ?resources.BudgetedAllocator = null,
    reclaimer: u64 = 0,

    const Flight = struct { bytes: []u8 = &.{}, active: bool = false, key: ?u64 = null };

    pub fn init(allocator: std.mem.Allocator, source: Source, byte_budget: usize) !ConcurrentBlockCache {
        if (byte_budget < 8) return error.InvalidScratch;
        var cache = try BlockCache.init(allocator, source, byte_budget);
        cache.block_size = @intCast(@min(@min(64 * 1024, byte_budget / 8), @max(1, source.len())));
        if (cache.block_size >= 4096) cache.block_size -= cache.block_size % 4096;
        return .{ .cache = cache };
    }

    /// Reserve the complete bounded cache before sharing an immutable reader.
    /// No slab growth is then needed while its posting iterators run in parallel.
    pub fn preallocate(self: *ConcurrentBlockCache) !void {
        @import("antfly_platform").sync.lockYielding(&self.fill_mutex);
        defer self.fill_mutex.unlock();
        try self.ensureBudget();
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (&self.flights) |*flight| if (flight.bytes.len == 0) {
            flight.bytes = self.bufferAllocator().alloc(u8, self.cache.block_size) catch |err| {
                if (self.budget != null and self.budget.?.budget_denied) return error.CacheBudgetExceeded;
                return err;
            };
        };
        for (&self.cache.slots) |*slot| if (slot.bytes.len == 0) {
            slot.bytes = self.bufferAllocator().alloc(u8, self.cache.block_size) catch |err| {
                if (self.budget != null and self.budget.?.budget_denied) return error.CacheBudgetExceeded;
                return err;
            };
        };
    }

    pub fn deinit(self: *ConcurrentBlockCache) void {
        if (self.budget) |*budget| budget.reservation.manager.unregisterReclaimer(self.reclaimer);
        for (&self.flights) |*flight| self.bufferAllocator().free(flight.bytes);
        for (&self.cache.slots) |*slot| self.bufferAllocator().free(slot.bytes);
        if (self.budget) |*budget| budget.deinit();
        self.* = undefined;
    }

    fn bufferAllocator(self: *ConcurrentBlockCache) std.mem.Allocator {
        return if (self.budget) |*budget| budget.allocator() else self.cache.allocator;
    }

    // Register only after the owner is at its final address. Never wait for an
    // active owner during pressure reclamation, including our own cold fill.
    fn ensureBudget(self: *ConcurrentBlockCache) !void {
        if (self.budget != null) return;
        const manager = self.cache.source.resourceManager() orelse return;
        const identity = try manager.registerReclaimer(.lite_native_page_cache, self, reclaim);
        self.budget = resources.BudgetedAllocator.initReclaiming(manager, .lite_native_page_cache, self.cache.allocator, 1);
        // Optional cache slabs grow only a few times; exact credits avoid
        // reserving a megabyte for every small artifact layer.
        self.budget.?.credit_quantum = 1;
        self.reclaimer = identity;
    }

    fn reclaim(raw: *anyopaque, _: u64) u64 {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(raw));
        if (!self.fill_mutex.tryLock()) return 0;
        defer self.fill_mutex.unlock();
        if (!self.mutex.tryLock()) return 0;
        defer self.mutex.unlock();
        for (self.flights) |flight| if (flight.active) return 0;
        var bytes: usize = 0;
        for (&self.flights) |*flight| {
            bytes += flight.bytes.len;
            self.bufferAllocator().free(flight.bytes);
            flight.* = .{};
        }
        for (&self.cache.slots) |*slot| {
            if (slot.pins != 0) continue;
            bytes += slot.bytes.len;
            self.bufferAllocator().free(slot.bytes);
            slot.* = .{};
        }
        return bytes;
    }

    fn runtime(self: *ConcurrentBlockCache) ?std.Io {
        return if (self.cache.source == .ranges) self.cache.source.ranges.read_io else null;
    }
    pub fn borrowedSource(self: *ConcurrentBlockCache) Source {
        return .{ .ranges = .{ .ptr = self, .length = self.cache.source.len(), .read_into = readAdapter, .visit_range = visitAdapter, .checksum = checksumAdapter, .read_authenticated = authenticatedAdapter, .close = closeAdapter, .prefetch = if (self.cache.source == .ranges and self.cache.source.ranges.prefetch != null) prefetchAdapter else null, .resource_manager = self.cache.source.resourceManager(), .read_io = self.runtime(), .check_read_context = checkAdapter } };
    }
    fn checkAdapter(raw: *anyopaque) !void {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(raw));
        if (self.cache.source == .ranges) if (self.cache.source.ranges.check_read_context) |check| try check(self.cache.source.ranges.ptr);
    }
    // Called under mutex. A leased slab is immutable until its callback ends.
    fn victim(self: *ConcurrentBlockCache, offset: u64) ?*BlockCache.Slot {
        var oldest: ?*BlockCache.Slot = null;
        for (&self.cache.slots) |*slot| {
            if (slot.pins != 0) continue;
            if (slot.valid_len != 0 and slot.offset == offset) return slot;
            if (oldest == null or slot.age < oldest.?.age) oldest = slot;
        }
        return oldest;
    }

    fn visitAdapter(raw: *anyopaque, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(raw));
        return self.visitRange(offset, length, context, consume);
    }

    pub fn visitRange(self: *ConcurrentBlockCache, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        if (offset > self.cache.source.len() or length > self.cache.source.len() - offset) return error.EndOfStream;
        var position: u64 = 0;
        while (position < length) {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            var lease: ?*BlockCache.Slot = null;
            var end = offset + length;
            const absolute = offset + position;
            for (&self.cache.slots) |*slot| {
                if (slot.valid_len == 0) continue;
                if (absolute >= slot.offset and absolute - slot.offset < slot.valid_len) {
                    lease = slot;
                    break;
                }
                if (slot.offset > absolute) end = @min(end, slot.offset);
            }
            if (lease) |slot| {
                slot.pins += 1;
                self.cache.clock +%= 1;
                slot.age = self.cache.clock;
                const within: usize = @intCast(absolute - slot.offset);
                const take: usize = @intCast(@min(length - position, slot.valid_len - within));
                const bytes = slot.bytes[within..][0..take];
                self.mutex.unlock();
                // Neither cache lock is held across user code, including errors
                // and reentrant reads. Reclamation skips this pinned slab.
                const result = consume(context, position, bytes);
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                slot.pins -= 1;
                self.mutex.unlock();
                try result;
                position += take;
            } else {
                self.mutex.unlock();
                const Forward = struct {
                    context: *anyopaque,
                    consume: *const fn (*anyopaque, u64, []const u8) anyerror!void,
                    base: u64,
                    fn visit(ptr: *anyopaque, relative: u64, bytes: []const u8) !void {
                        const value: *@This() = @ptrCast(@alignCast(ptr));
                        return value.consume(value.context, value.base + relative, bytes);
                    }
                };
                var forward = Forward{ .context = context, .consume = consume, .base = position };
                try self.cache.source.visitRange(absolute, end - absolute, &forward, Forward.visit);
                position += end - absolute;
            }
        }
    }

    fn prefetchAdapter(ptr: *anyopaque, offset: u64, length: u64) void {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(ptr));
        self.cache.source.prefetch(offset, length);
    }
    fn readAdapter(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(ptr));
        try self.readInto(offset, out);
    }
    fn authenticatedAdapter(ptr: *anyopaque, offset: u64, length: u64, within: usize, out: []u8, expected: ?u32) !void {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(ptr));
        return self.readAuthenticated(offset, length, within, out, expected);
    }

    pub fn readAuthenticated(self: *ConcurrentBlockCache, offset: u64, length: u64, within: usize, out: []u8, expected: ?u32) !void {
        if (offset > self.cache.source.len() or length > self.cache.source.len() - offset or within > length or out.len > length - within) return error.EndOfStream;
        if (expected == null) return self.readCachedInto(offset + within, out);
        if (self.cache.source == .ranges) if (self.cache.source.ranges.visit_range) |visit| {
            const flight = try self.acquireFlight(null);
            defer self.releaseFlight(flight);
            var stream = AuthenticationStream{ .cache = self, .flight = flight, .offset = offset, .within = within, .out = out };
            try visit(self.cache.source.ranges.ptr, offset, length, &stream, AuthenticationStream.consume);
            if (stream.position != length) return error.EndOfStream;
            if (stream.used != 0) try self.publishFill(flight, stream.block, stream.used);
            if (stream.crc.final() != expected.?) return error.CrcMismatch;
            return;
        };
        var scratch: [8192]u8 = undefined;
        var crc = Crc32.init();
        var position: u64 = 0;
        while (position < length) {
            const count: usize = @intCast(@min(scratch.len, length - position));
            const bytes = scratch[0..count];
            try self.readCachedInto(offset + position, bytes);
            crc.update(bytes);
            const begin = @max(position, within);
            const end = @min(position + count, @as(u64, within) + out.len);
            if (begin < end) @memcpy(out[@intCast(begin - within)..][0..@intCast(end - begin)], bytes[@intCast(begin - position)..][0..@intCast(end - begin)]);
            position += count;
        }
        if (crc.final() != expected.?) return error.CrcMismatch;
    }

    const AuthenticationStream = struct {
        cache: *ConcurrentBlockCache,
        flight: *Flight,
        offset: u64,
        within: usize,
        out: []u8,
        position: u64 = 0,
        crc: Crc32 = Crc32.init(),
        block: u64 = 0,
        used: usize = 0,
        fn consume(raw: *anyopaque, relative: u64, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (relative != self.position) return error.InvalidData;
            self.crc.update(bytes);
            const begin = @max(relative, self.within);
            const end = @min(relative + bytes.len, @as(u64, self.within) + self.out.len);
            if (begin < end) @memcpy(self.out[@intCast(begin - self.within)..][0..@intCast(end - begin)], bytes[@intCast(begin - relative)..][0..@intCast(end - begin)]);
            self.position += bytes.len;
            if (self.flight.bytes.len == 0) return;
            var copied: usize = 0;
            while (copied < bytes.len) {
                const absolute = self.offset + relative + copied;
                const block = absolute - absolute % self.cache.cache.block_size;
                const within: usize = @intCast(absolute - block);
                const take = @min(bytes.len - copied, self.cache.cache.block_size - within);
                if (within == 0) {
                    self.block = block;
                    self.used = 0;
                }
                // A page can start inside a cache block. Do not publish an
                // uninitialized prefix outside the authenticated traversal.
                if (self.block == block and self.used == within) {
                    @memcpy(self.flight.bytes[within..][0..take], bytes[copied..][0..take]);
                    self.used += take;
                    if (self.used == self.cache.cache.block_size) {
                        try self.cache.publishFill(self.flight, block, self.used);
                        self.used = 0;
                    }
                }
                copied += take;
            }
        }
    };

    // Publication and allocator operations are short; backend I/O owns a flight
    // slab and never holds either cache mutex.
    fn publishFill(self: *ConcurrentBlockCache, flight: *Flight, offset: u64, length: usize) !void {
        @import("antfly_platform").sync.lockYielding(&self.fill_mutex);
        defer self.fill_mutex.unlock();
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const oldest = self.victim(offset) orelse return;
        if (oldest.valid_len != 0 and oldest.offset == offset and oldest.valid_len >= length) {
            self.cache.clock +%= 1;
            oldest.age = self.cache.clock;
            return;
        }
        if (oldest.bytes.len == 0) oldest.bytes = self.bufferAllocator().alloc(u8, self.cache.block_size) catch |err| {
            if (self.budget == null or !self.budget.?.budget_denied) return err;
            return;
        };
        std.mem.swap([]u8, &oldest.bytes, &flight.bytes);
        self.cache.clock +%= 1;
        oldest.offset = offset;
        oldest.valid_len = length;
        oldest.age = self.cache.clock;
        self.cache.misses += 1;
    }

    fn checksumAdapter(ptr: *anyopaque, offset: u64, length: u64) !u32 {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(ptr));
        var scratch: [8192]u8 = undefined;
        return self.cache.source.checksum(offset, length, &scratch);
    }
    fn closeAdapter(_: *anyopaque) void {}

    pub fn retainedBytes(self: *ConcurrentBlockCache) usize {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var bytes = self.cache.retainedBytes();
        for (self.flights) |flight| bytes += flight.bytes.len;
        return bytes;
    }

    fn copyCached(self: *ConcurrentBlockCache, offset: u64, within: usize, out: []u8) bool {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.cache.clock +%= 1;
        for (&self.cache.slots) |*slot| {
            if (slot.valid_len != 0 and slot.offset == offset and within <= slot.valid_len and out.len <= slot.valid_len - within) {
                slot.age = self.cache.clock;
                @memcpy(out, slot.bytes[within..][0..out.len]);
                return true;
            }
        }
        return false;
    }

    fn checkWaiting(self: *ConcurrentBlockCache) !void {
        if (self.cache.source == .ranges) if (self.cache.source.ranges.check_read_context) |check| try check(self.cache.source.ranges.ptr);
    }

    fn acquireFlight(self: *ConcurrentBlockCache, key: ?u64) !*Flight {
        const io = self.runtime();
        if (io) |runtime_io| try self.wait_mutex.lock(runtime_io);
        defer if (io) |runtime_io| self.wait_mutex.unlock(runtime_io);
        while (true) {
            @import("antfly_platform").sync.lockYielding(&self.fill_mutex);
            var same = false;
            var available: ?*Flight = null;
            for (&self.flights) |*flight| {
                if (key != null and flight.active and flight.key == key) same = true;
                if (!flight.active and available == null) available = flight;
            }
            if (!same) if (available) |flight| {
                self.ensureBudget() catch |err| {
                    self.fill_mutex.unlock();
                    return err;
                };
                if (flight.bytes.len == 0) {
                    const bytes = self.bufferAllocator().alloc(u8, self.cache.block_size) catch |err| blk: {
                        if (self.budget == null or !self.budget.?.budget_denied) {
                            self.fill_mutex.unlock();
                            return err;
                        }
                        break :blk flight.bytes;
                    };
                    @import("antfly_platform").sync.lockYielding(&self.mutex);
                    flight.bytes = bytes;
                    self.mutex.unlock();
                }
                flight.active = true;
                flight.key = key;
                self.fill_mutex.unlock();
                return flight;
            };
            self.fill_mutex.unlock();
            try self.checkWaiting();
            if (io) |runtime_io| {
                try self.changed.wait(runtime_io, &self.wait_mutex);
            } else if (@import("builtin").single_threaded) {
                // No other thread can release a flight while we wait. Keep
                // bounded admission explicit instead of spinning forever.
                return error.CacheBudgetExceeded;
            } else std.Thread.yield() catch {};
        }
    }
    fn releaseFlight(self: *ConcurrentBlockCache, flight: *Flight) void {
        const io = self.runtime();
        if (io) |runtime_io| self.wait_mutex.lockUncancelable(runtime_io);
        defer if (io) |runtime_io| self.wait_mutex.unlock(runtime_io);
        @import("antfly_platform").sync.lockYielding(&self.fill_mutex);
        flight.active = false;
        flight.key = null;
        self.fill_mutex.unlock();
        if (io) |runtime_io| self.changed.broadcast(runtime_io);
    }
    fn fillAndCopy(self: *ConcurrentBlockCache, offset: u64, within: usize, out: []u8) !void {
        const flight = try self.acquireFlight(offset);
        defer self.releaseFlight(flight);
        if (self.copyCached(offset, within, out)) return;
        if (flight.bytes.len == 0) return self.cache.source.readInto(offset + within, out);
        const length: usize = @intCast(@min(self.cache.block_size, self.cache.source.len() - offset));
        try self.cache.source.readInto(offset, flight.bytes[0..length]);
        @memcpy(out, flight.bytes[within..][0..out.len]);
        try self.publishFill(flight, offset, length);
    }

    pub fn readInto(self: *ConcurrentBlockCache, offset: u64, out: []u8) !void {
        if (offset > self.cache.source.len() or out.len > self.cache.source.len() - offset) return error.EndOfStream;
        // Large sequential reads already let the backend coalesce pages.
        // Do not split them into fills or evict useful point-read navigation.
        if (out.len / 2 >= self.cache.block_size) return self.cache.source.readInto(offset, out);
        return self.readCachedInto(offset, out);
    }

    fn readCachedInto(self: *ConcurrentBlockCache, offset: u64, out: []u8) !void {
        var copied: usize = 0;
        while (copied < out.len) {
            const position = offset + copied;
            const block = position - position % self.cache.block_size;
            const within: usize = @intCast(position - block);
            const take = @min(out.len - copied, self.cache.block_size - within);
            const part = out[copied..][0..take];
            if (!self.copyCached(block, within, part)) try self.fillAndCopy(block, within, part);
            copied += take;
        }
    }
};

/// Borrowed, bounded section of an immutable artifact. The owner of `source`
/// must outlive this view. All offsets are section-relative and checked before
/// being translated to native artifact offsets.
pub const View = struct {
    source: Source,
    offset: u64,
    length: u64,

    pub fn init(source: Source, offset: u64, length: u64) !View {
        if (offset > source.len() or length > source.len() - offset) return error.EndOfStream;
        return .{ .source = source, .offset = offset, .length = length };
    }

    pub fn visitRange(self: View, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        if (offset > self.length or length > self.length - offset) return error.EndOfStream;
        try self.source.visitRange(self.offset + offset, length, context, consume);
    }

    pub fn readInto(self: View, offset: u64, out: []u8) !void {
        if (offset > self.length or out.len > self.length - offset) return error.EndOfStream;
        try self.source.readInto(self.offset + offset, out);
    }
};

/// Optional metadata owner retained by iterators that escape a field scope.
/// Immutable contiguous readers need no lease; native readers release their
/// navigation and cache only after the last dependent iterator closes.
pub const SharedOwner = struct {
    ptr: *anyopaque,
    retain: *const fn (*anyopaque) void,
    release: *const fn (*anyopaque) void,
};

test "range visitors preserve borrowing bound spans and reject malformed providers" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 160 * 1024);
    defer a.free(bytes);
    @memset(bytes, 'v');
    const Backend = struct {
        bytes: []const u8,
        mode: enum { normal, gap, short, failure } = .normal,
        reads: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn visit(raw: *anyopaque, offset: u64, length: u64, context: *anyopaque, consume: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.mode == .short) return;
            if (self.mode == .failure) return error.TestIoFailure;
            try consume(context, if (self.mode == .gap) 1 else 0, self.bytes[@intCast(offset)..][0..@intCast(length)]);
        }
        fn close(_: *anyopaque) void {}
    };
    const Consumer = struct {
        bytes: []const u8,
        borrowed: bool,
        used: usize = 0,
        max_span: usize = 0,
        fn consume(raw: *anyopaque, relative: u64, span: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.used, relative);
            try std.testing.expectEqualSlices(u8, self.bytes[self.used..][0..span.len], span);
            if (self.borrowed) try std.testing.expectEqual(@intFromPtr(self.bytes.ptr) + self.used, @intFromPtr(span.ptr));
            self.used += span.len;
            self.max_span = @max(self.max_span, span.len);
        }
    };
    var backend = Backend{ .bytes = bytes };
    const source = Source{ .ranges = .{ .ptr = &backend, .length = bytes.len, .read_into = Backend.read, .visit_range = Backend.visit, .close = Backend.close } };
    const view = try View.init(source, 7, bytes.len - 14);
    var consumer = Consumer{ .bytes = bytes[7 .. bytes.len - 7], .borrowed = true };
    try view.visitRange(0, view.length, &consumer, Consumer.consume);
    try std.testing.expectEqual(@as(usize, 0), backend.reads);
    try std.testing.expectEqual(@as(usize, 64 * 1024), consumer.max_span);
    try std.testing.expectEqual(view.length, consumer.used);
    for ([_]Backend{ .{ .bytes = bytes, .mode = .gap }, .{ .bytes = bytes, .mode = .short }, .{ .bytes = bytes, .mode = .failure } }, 0..) |state, i| {
        backend = state;
        consumer.used = 0;
        const failure = [_]anyerror{ error.InvalidData, error.EndOfStream, error.TestIoFailure };
        try std.testing.expectError(failure[i], view.visitRange(0, view.length, &consumer, Consumer.consume));
        try std.testing.expectEqual(@as(usize, 0), consumer.used);
    }
    backend.mode = .normal;
    var fallback = source;
    fallback.ranges.visit_range = null;
    consumer.borrowed = false;
    consumer.used = 0;
    try (try View.init(fallback, 7, bytes.len - 14)).visitRange(0, view.length, &consumer, Consumer.consume);
    try std.testing.expectEqual(@as(usize, 3), backend.reads);
    try std.testing.expectError(error.EndOfStream, view.visitRange(view.length, 1, &consumer, Consumer.consume));
}

test "segment.cache leases survive reentrant fills pressure and callback errors" {
    const a = std.testing.allocator;
    var data: [512]u8 = undefined;
    for (&data, 0..) |*byte, i| byte.* = @truncate(i);
    var cache = try ConcurrentBlockCache.init(a, .{ .contiguous = &data }, 128);
    defer cache.deinit();
    var warm: [16]u8 = undefined;
    try cache.readInto(0, &warm);
    const Consumer = struct {
        cache: *ConcurrentBlockCache,
        original: []const u8,
        fail: bool = false,
        fn visit(raw: *anyopaque, _: u64, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualSlices(u8, self.original, bytes);
            // Recursively lease every resident slot, forcing the all-pinned
            // read fallback without holding either lock across callbacks.
            if (self.original[0] < 48) {
                const offset: u64 = @as(u64, self.original[0]) + 16;
                var next: [16]u8 = undefined;
                try self.cache.readInto(offset, &next);
                var child = @This(){ .cache = self.cache, .original = &next };
                try self.cache.visitRange(offset, 16, &child, visit);
            } else {
                var scratch: [16]u8 = undefined;
                try self.cache.readInto(64, &scratch);
            }
            const retained = self.cache.retainedBytes();
            const reclaimed = ConcurrentBlockCache.reclaim(self.cache, 0);
            try std.testing.expectEqual(retained - self.cache.retainedBytes(), reclaimed);
            try std.testing.expectEqualSlices(u8, self.original, bytes);
            if (self.fail) return error.TestVisitorFailure;
        }
    };
    var consumer = Consumer{ .cache = &cache, .original = &warm, .fail = true };
    try std.testing.expectError(error.TestVisitorFailure, cache.visitRange(0, 16, &consumer, Consumer.visit));
    for (cache.cache.slots) |slot| try std.testing.expectEqual(@as(usize, 0), slot.pins);
    _ = ConcurrentBlockCache.reclaim(&cache, 0);
    try std.testing.expectEqual(@as(usize, 0), cache.retainedBytes());
}
