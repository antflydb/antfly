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
        retained_bytes: ?*const fn (*anyopaque) usize = null,
        /// Bind an independent query capability without changing the shared source.
        bind_read_context: ?*const fn (*anyopaque, std.mem.Allocator, *anyopaque) anyerror!Source = null,
        seal_read_context: ?*const fn (*anyopaque) void = null,
        quiesce_read_context: ?*const fn (*anyopaque) void = null,
        resource_manager: ?*resources.ResourceManager = null,
        read_io: ?std.Io = null,
        check_read_context: ?*const fn (*anyopaque) anyerror!void = null,
    },

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
    fill: []u8 = &.{},
    budget: ?resources.BudgetedAllocator = null,
    reclaimer: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, source: Source, byte_budget: usize) !ConcurrentBlockCache {
        if (byte_budget < 5) return error.InvalidScratch;
        var cache = try BlockCache.init(allocator, source, byte_budget);
        cache.block_size = @intCast(@min(@min(64 * 1024, byte_budget / 5), @max(1, source.len())));
        if (cache.block_size >= 4096) cache.block_size -= cache.block_size % 4096;
        return .{ .cache = cache };
    }

    pub fn deinit(self: *ConcurrentBlockCache) void {
        if (self.budget) |*budget| budget.reservation.manager.unregisterReclaimer(self.reclaimer);
        self.bufferAllocator().free(self.fill);
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
        const bytes = self.cache.retainedBytes() + self.fill.len;
        self.bufferAllocator().free(self.fill);
        self.fill = &.{};
        for (&self.cache.slots) |*slot| {
            self.bufferAllocator().free(slot.bytes);
            slot.* = .{};
        }
        return bytes;
    }

    pub fn borrowedSource(self: *ConcurrentBlockCache) Source {
        return .{ .ranges = .{ .ptr = self, .length = self.cache.source.len(), .read_into = readAdapter, .checksum = checksumAdapter, .close = closeAdapter, .prefetch = if (self.cache.source == .ranges and self.cache.source.ranges.prefetch != null) prefetchAdapter else null, .resource_manager = self.cache.source.resourceManager() } };
    }
    fn prefetchAdapter(ptr: *anyopaque, offset: u64, length: u64) void {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(ptr));
        self.cache.source.prefetch(offset, length);
    }
    fn readAdapter(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *ConcurrentBlockCache = @ptrCast(@alignCast(ptr));
        try self.readInto(offset, out);
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
        return self.cache.retainedBytes() + self.fill.len;
    }

    fn copyCached(self: *ConcurrentBlockCache, offset: u64, within: usize, out: []u8) bool {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.cache.clock +%= 1;
        for (&self.cache.slots) |*slot| {
            if (slot.valid_len != 0 and slot.offset == offset) {
                slot.age = self.cache.clock;
                @memcpy(out, slot.bytes[within..][0..out.len]);
                return true;
            }
        }
        return false;
    }

    fn fillAndCopy(self: *ConcurrentBlockCache, offset: u64, within: usize, out: []u8) !void {
        @import("antfly_platform").sync.lockYielding(&self.fill_mutex);
        defer self.fill_mutex.unlock();
        if (self.copyCached(offset, within, out)) return;
        try self.ensureBudget();
        if (self.fill.len == 0) {
            const buffer = self.bufferAllocator().alloc(u8, self.cache.block_size) catch |err| {
                if (self.budget == null or !self.budget.?.budget_denied) return err;
                // Caching is optional: concurrent active fills can consume the
                // budget without preventing an otherwise valid range read.
                return self.cache.source.readInto(offset + within, out);
            };
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            self.fill = buffer;
            self.mutex.unlock();
        }
        const length: usize = @intCast(@min(self.cache.block_size, self.cache.source.len() - offset));
        // The old cache entry remains valid even when this read fails.
        try self.cache.source.readInto(offset, self.fill[0..length]);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var oldest = &self.cache.slots[0];
        for (&self.cache.slots) |*slot| if (slot.age < oldest.age) {
            oldest = slot;
        };
        if (oldest.bytes.len == 0) oldest.bytes = self.bufferAllocator().alloc(u8, self.cache.block_size) catch |err| {
            if (self.budget == null or !self.budget.?.budget_denied) return err;
            @memcpy(out, self.fill[within..][0..out.len]);
            return;
        };
        @memcpy(oldest.bytes[0..length], self.fill[0..length]);
        self.cache.clock +%= 1;
        oldest.offset = offset;
        oldest.valid_len = length;
        oldest.age = self.cache.clock;
        self.cache.misses += 1;
        @memcpy(out, oldest.bytes[within..][0..out.len]);
    }

    pub fn readInto(self: *ConcurrentBlockCache, offset: u64, out: []u8) !void {
        if (offset > self.cache.source.len() or out.len > self.cache.source.len() - offset) return error.EndOfStream;
        // Large sequential reads already let the backend coalesce pages.
        // Do not split them into fills or evict useful point-read navigation.
        if (out.len / 2 >= self.cache.block_size) return self.cache.source.readInto(offset, out);
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
