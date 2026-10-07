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
    // Closing the facade source and freeing decoder metadata are separate:
    // SegmentReader borrows its caller's source; standalone range readers own it.
    pub fn deinit(self: *PagedSource) void {
        const allocator = self.allocator;
        allocator.free(self.validations);
        if (self.budget) |*budget| budget.reservation.manager.unregisterReclaimer(self.reclaimer);
        self.bufferAllocator().free(self.buffer);
        if (self.budget) |*budget| budget.deinit();
        allocator.destroy(self);
    }
    pub fn source(self: *PagedSource) source_mod.Source {
        return .{ .ranges = .{ .ptr = self, .length = self.original.len(), .read_into = read, .close = close, .resource_manager = self.original.resourceManager() } };
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
        if (state.load(.acquire) == 1) return self.original.readInto(offset + within, out);
        // One cold validator owns the bounded page buffer. Verified reads and
        // cache hits never wait for backend I/O on this separate owner lock.
        @import("antfly_platform").sync.lockYielding(&self.io_mutex);
        defer self.io_mutex.unlock();
        if (state.load(.acquire) == 2) return error.CrcMismatch;
        if (self.copyCached(index, within, out)) return;
        if (state.load(.acquire) == 1) return self.original.readInto(offset + within, out);
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
        // Invalidate before touching bytes: failed/partial reads cannot expose
        // the old cache entry. Publish only after authentication completes.
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.cached_page = null;
        self.mutex.unlock();
        try self.ensureBudget();
        if (self.buffer.len == 0) {
            const buffer = self.bufferAllocator().alloc(u8, @intCast(@min(page_size, self.directory.offset))) catch |err| {
                if (self.budget == null or !self.budget.?.budget_denied) return err;
                // Mandatory authentication uses fixed worker scratch when
                // the optional shared cache is full. No uncharged heap page
                // survives pressure or scales with open reader count.
                var scratch: [8192]u8 = undefined;
                const actual = try self.original.checksum(offset, length, &scratch);
                var expected: [4]u8 = undefined;
                try self.original.readInto(self.directory.offset + @as(u64, index) * 4, &expected);
                if (actual != std.mem.readInt(u32, &expected, .big)) {
                    state.store(2, .release);
                    return error.CrcMismatch;
                }
                state.store(1, .release);
                try self.original.readInto(offset + within, out);
                return;
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
