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

//! Private seekable postings runs. No catalog/WAL entry is created; on POSIX
//! the descriptor owns an unlinked inode, and every exit closes the owner.
const std = @import("std");
const source = @import("segment_source.zig");
const Allocator = std.mem.Allocator;

pub const Run = if (@import("builtin").os.tag == .freestanding) struct {
    // Memory-only hosts split builds and enforce allocator admission. They
    // cannot create a seekable filesystem run through the native IO backend.
    pub fn create(_: Allocator, _: std.Io, _: []const u8) !*@This() {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn createWithResources(_: Allocator, _: std.Io, _: []const u8, _: ?*@import("storage/resource_manager.zig").ResourceManager) !*@This() {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn seal(_: *@This(), _: usize) !void {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn releaseRange(_: *@This(), _: usize) void {
        unreachable;
    }
    pub fn compact(_: *@This()) !void {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn sink(_: *@This()) @import("segment.zig").SegmentSink {
        unreachable;
    }
    pub fn deinit(_: *@This()) void {
        unreachable;
    }
    pub fn len(_: *@This()) usize {
        unreachable;
    }
    pub fn flush(_: *@This()) !void {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn appendSlice(_: *@This(), _: []const u8) !void {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn writeAt(_: *@This(), _: usize, _: []const u8) !void {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn view(_: *@This()) !source.View {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn sealedView(_: *@This()) !source.View {
        return error.NativePostingsRunsUnavailable;
    }
} else NativeRun;

const NativeRun = struct {
    const Extent = struct { logical: usize, physical: usize, length: usize };
    allocator: Allocator,
    io: std.Io,
    file: std.Io.File,
    path: ?[]u8,
    directory: []u8,
    length: usize = 0,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    persisted: usize = 0,
    active_logical: usize = 0,
    active_physical: usize = 0,
    extents: std.ArrayListUnmanaged(Extent) = .empty,
    capacity: ?@import("storage/resource_manager.zig").CapacityReservation = null,
    physical_peak: usize = 0,
    reclaimed_bytes: usize = 0,
    live_bytes: usize = 0,
    dead_extents: usize = 0,
    read_buffer: []u8 = &.{},
    read_pages: [4]struct { offset: usize = 0, length: usize = 0 } = @splat(.{}),
    next_read_page: usize = 0,
    read_calls: usize = 0,
    write_calls: usize = 0,

    pub fn create(allocator: Allocator, io: std.Io, directory: []const u8) !*Run {
        return createWithResources(allocator, io, directory, null);
    }
    pub fn createWithResources(allocator: Allocator, io: std.Io, directory: []const u8, manager: ?*@import("storage/resource_manager.zig").ResourceManager) !*Run {
        const self = try allocator.create(Run);
        errdefer allocator.destroy(self);
        const dir = try allocator.dupe(u8, directory);
        errdefer allocator.free(dir);
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const name = try std.fmt.allocPrint(allocator, ".antfly-postings-{x}", .{random});
        defer allocator.free(name);
        const path = try std.fs.path.join(allocator, &.{ directory, name });
        errdefer allocator.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
        errdefer file.close(io);
        errdefer std.Io.Dir.cwd().deleteFile(io, path) catch {};
        var claim: ?@import("storage/resource_manager.zig").CapacityReservation = null;
        if (manager) |resource_owner| {
            const domain = if (resource_owner.capacitySource()) |configured| configured.domain_id else std.hash.Wyhash.hash(0, "antfly private build scratch");
            claim = try resource_owner.reserveCapacity(allocator, domain, 0, .{}, 0);
        }
        self.* = .{ .allocator = allocator, .io = io, .file = file, .path = path, .directory = dir, .capacity = claim };
        if (comptime @import("builtin").os.tag != .windows) {
            std.Io.Dir.cwd().deleteFile(io, path) catch return self;
            allocator.free(path);
            self.path = null;
        }
        return self;
    }
    pub fn deinit(self: *Run) void {
        self.file.close(self.io);
        if (self.capacity) |*claim| claim.release();
        if (self.path) |path| {
            std.Io.Dir.cwd().deleteFile(self.io, path) catch {};
            self.allocator.free(path);
        }
        self.allocator.free(self.directory);
        self.allocator.free(self.read_buffer);
        self.buffer.deinit(self.allocator);
        self.extents.deinit(self.allocator);
        self.allocator.destroy(self);
    }
    pub fn len(self: *Run) usize {
        return self.length;
    }
    fn admit(self: *Run, future: usize) !void {
        const claim = if (self.capacity) |*c| c else return;
        if (future <= claim.bytes) return;
        const rounded = try std.math.add(usize, future, 65535) / 65536 * 65536;
        const filesystem = @import("antfly_platform").filesystem;
        if (comptime !filesystem.capacity_supported) {
            try claim.resize(rounded, .{}, 0);
            return;
        }
        const fs = try filesystem.capacity(self.directory);
        try claim.resize(rounded, .{ .available_bytes = fs.available_bytes, .capacity_bytes = fs.total_bytes }, 0);
    }
    fn materialized(self: *Run, bytes: usize) void {
        if (self.capacity) |*claim| claim.resize(claim.bytes -| bytes, .{}, 0) catch unreachable;
        self.physical_peak = @max(self.physical_peak, self.persisted);
    }
    pub fn flush(self: *Run) !void {
        if (self.buffer.items.len == 0) return;
        const bytes = self.buffer.items.len;
        try self.file.writePositionalAll(self.io, self.buffer.items, self.persisted);
        self.write_calls += 1;
        self.persisted += bytes;
        self.materialized(bytes);
        self.buffer.clearRetainingCapacity();
    }
    pub fn appendSlice(self: *Run, bytes: []const u8) !void {
        const next = try std.math.add(usize, self.length, bytes.len);
        if (bytes.len > 64 * 1024 - self.buffer.items.len) try self.flush();
        try self.admit(try std.math.add(usize, self.buffer.items.len, bytes.len));
        if (bytes.len >= 64 * 1024) {
            try self.file.writePositionalAll(self.io, bytes, self.persisted);
            self.write_calls += 1;
            self.persisted += bytes.len;
            self.materialized(bytes.len);
        } else try self.buffer.appendSlice(self.allocator, bytes);
        self.length = next;
    }
    pub fn writeAt(self: *Run, offset: usize, bytes: []const u8) !void {
        if (offset < self.active_logical or offset > self.length or bytes.len > self.length - offset) return error.EndOfStream;
        const physical = self.active_physical + offset - self.active_logical;
        if (physical >= self.persisted and bytes.len <= self.buffer.items.len -| (physical - self.persisted)) {
            @memcpy(self.buffer.items[physical - self.persisted ..][0..bytes.len], bytes);
            return;
        }
        try self.flush();
        self.read_pages = @splat(.{});
        try self.file.writePositionalAll(self.io, bytes, physical);
        self.write_calls += 1;
    }
    /// Seal before exposing a run. Logical coordinates remain stable while
    /// compaction moves live ranges into reclaimed physical file space.
    pub fn seal(self: *Run, start: usize) !void {
        if (start != self.active_logical) return error.InvalidData;
        try self.flush();
        try self.extents.append(self.allocator, .{ .logical = start, .physical = self.active_physical, .length = self.length - start });
        self.live_bytes += self.length - start;
        self.active_logical = self.length;
        self.active_physical = self.persisted;
    }
    pub fn releaseRange(self: *Run, start: usize) void {
        const extent = self.findExtent(start) orelse unreachable;
        std.debug.assert(extent.logical == start and extent.length != 0);
        self.live_bytes -= extent.length;
        extent.length = 0;
        self.dead_extents += 1;
    }
    fn findExtent(self: *Run, offset: u64) ?*Extent {
        var lo: usize = 0;
        var hi = self.extents.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.extents.items[mid].logical <= offset) lo = mid + 1 else hi = mid;
        }
        return if (lo == 0) null else &self.extents.items[lo - 1];
    }
    fn pruneDeadExtents(self: *Run) void {
        var kept: usize = 0;
        for (self.extents.items) |extent| if (extent.length != 0) {
            self.extents.items[kept] = extent;
            kept += 1;
        };
        self.extents.shrinkRetainingCapacity(kept);
        self.dead_extents = 0;
    }
    /// Repack only after dead space exceeds live space and one MiB. Each
    /// compaction reclaims at least half the file; copying uses fixed scratch.
    pub fn compact(self: *Run) !void {
        std.debug.assert(self.active_logical == self.length);
        const dead = self.persisted - self.live_bytes;
        if (dead < 1024 * 1024 or dead < self.live_bytes) {
            if (self.extents.items.len > 64 and self.dead_extents >= self.extents.items.len / 2) self.pruneDeadExtents();
            return;
        }
        self.read_pages = @splat(.{});
        var scratch: [64 * 1024]u8 = undefined;
        var end: usize = 0;
        for (self.extents.items) |*extent| {
            var copied: usize = 0;
            while (extent.physical != end and copied < extent.length) {
                const take = @min(scratch.len, extent.length - copied);
                if (try self.file.readPositionalAll(self.io, scratch[0..take], extent.physical + copied) != take) return error.EndOfStream;
                try self.file.writePositionalAll(self.io, scratch[0..take], end + copied);
                copied += take;
            }
            extent.physical = end;
            end += extent.length;
        }
        try self.file.setLength(self.io, end);
        self.persisted = end;
        self.active_physical = end;
        self.reclaimed_bytes += dead;
        self.pruneDeadExtents();
    }
    pub fn view(self: *Run) !source.View {
        try self.flush();
        return self.sealedView();
    }
    /// Already-sealed extents are durable in the descriptor. Reading them must
    /// not flush the active output while a carry copies its input sidecars.
    pub fn sealedView(self: *Run) !source.View {
        return source.View.init(.{ .ranges = .{ .ptr = self, .length = self.length, .read_into = read, .close = close } }, 0, self.length);
    }
    fn read(raw: *anyopaque, offset: u64, bytes: []u8) !void {
        const self: *Run = @ptrCast(@alignCast(raw));
        const extent = self.findExtent(offset) orelse return error.EndOfStream;
        const within = offset - extent.logical;
        if (extent.length == 0 or within > extent.length or bytes.len > extent.length - within) return error.EndOfStream;
        const physical: usize = @intCast(extent.physical + within);
        // Large sequential payloads bypass the small-read cache.
        if (bytes.len >= 16 * 1024) {
            self.read_calls += 1;
            if (try self.file.readPositionalAll(self.io, bytes, physical) != bytes.len) return error.EndOfStream;
            return;
        }
        if (self.read_buffer.len == 0) self.read_buffer = try self.allocator.alloc(u8, 64 * 1024);
        var copied: usize = 0;
        while (copied < bytes.len) {
            const position = physical + copied;
            const page_offset = position / (16 * 1024) * (16 * 1024);
            const in_page = position - page_offset;
            const take = @min(bytes.len - copied, 16 * 1024 - in_page);
            var slot: ?usize = null;
            for (self.read_pages, 0..) |page, i| if (page.offset == page_offset and page.length >= in_page + take) {
                slot = i;
                break;
            };
            const index = slot orelse blk: {
                const i = self.next_read_page;
                self.next_read_page = (i + 1) % self.read_pages.len;
                const length = @min(16 * 1024, self.persisted - page_offset);
                self.read_calls += 1;
                if (try self.file.readPositionalAll(self.io, self.read_buffer[i * 16 * 1024 ..][0..length], page_offset) != length) return error.EndOfStream;
                self.read_pages[i] = .{ .offset = page_offset, .length = length };
                break :blk i;
            };
            @memcpy(bytes[copied..][0..take], self.read_buffer[index * 16 * 1024 + in_page ..][0..take]);
            copied += take;
        }
    }
    fn close(_: *anyopaque) void {}
    pub fn sink(self: *Run) @import("segment.zig").SegmentSink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }
    fn owner(raw: *anyopaque) *Run {
        return @ptrCast(@alignCast(raw));
    }
    fn sinkLen(raw: *anyopaque) usize {
        return owner(raw).len();
    }
    fn sinkAppend(raw: *anyopaque, bytes: []const u8) !void {
        try owner(raw).appendSlice(bytes);
    }
    fn sinkByte(raw: *anyopaque, byte: u8) !void {
        try owner(raw).appendSlice(&.{byte});
    }
    fn sinkRepeat(raw: *anyopaque, byte: u8, count: usize) !void {
        var block: [4096]u8 = undefined;
        @memset(&block, byte);
        var left = count;
        while (left > 0) {
            const take = @min(left, block.len);
            try owner(raw).appendSlice(block[0..take]);
            left -= take;
        }
    }
    fn sinkWrite(raw: *anyopaque, offset: usize, bytes: []const u8) !void {
        try owner(raw).writeAt(offset, bytes);
    }
    fn sinkCrcPrefix(_: *anyopaque, _: usize) !u32 {
        return error.UnsupportedOperation;
    }
    fn sinkCrcRange(_: *anyopaque, _: usize, _: usize) !u32 {
        return error.UnsupportedOperation;
    }
    fn resident(raw: *anyopaque) usize {
        return owner(raw).buffer.capacity + owner(raw).read_buffer.len;
    }
    const sink_vtable = @import("segment.zig").SegmentSink.VTable{ .len = sinkLen, .append_slice = sinkAppend, .append_byte = sinkByte, .append_ntimes = sinkRepeat, .write_at = sinkWrite, .crc32_prefix = sinkCrcPrefix, .crc32_range = sinkCrcRange, .resident_bytes = resident };
};
