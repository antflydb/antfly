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
    pub fn compactStep(_: *@This(), _: usize) !bool {
        return error.NativePostingsRunsUnavailable;
    }
    pub fn compactionWorkBytes(_: *@This()) usize {
        return 0;
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
    compaction: ?struct {
        repack: bool,
        dead: usize,
        index: usize = 0,
        kept: usize = 0,
        copied: usize = 0,
        end: usize = 0,
    } = null,
    compaction_failed: bool = false,
    last_compaction_bytes: usize = 0,

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
        try self.requireAvailable();
        if (self.buffer.items.len == 0) return;
        const bytes = self.buffer.items.len;
        try self.file.writePositionalAll(self.io, self.buffer.items, self.persisted);
        self.write_calls += 1;
        self.persisted += bytes;
        self.materialized(bytes);
        self.buffer.clearRetainingCapacity();
    }
    pub fn appendSlice(self: *Run, bytes: []const u8) !void {
        try self.requireAvailable();
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
        try self.requireAvailable();
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
        try self.requireAvailable();
        if (start != self.active_logical) return error.InvalidData;
        try self.flush();
        try self.extents.append(self.allocator, .{ .logical = start, .physical = self.active_physical, .length = self.length - start });
        self.live_bytes += self.length - start;
        self.active_logical = self.length;
        self.active_physical = self.persisted;
    }
    pub fn releaseRange(self: *Run, start: usize) void {
        std.debug.assert(self.compaction == null and !self.compaction_failed);
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
    fn requireAvailable(self: *const Run) !void {
        if (self.compaction != null or self.compaction_failed) return error.InvalidData;
    }
    pub fn compactionWorkBytes(self: *const Run) usize {
        return self.last_compaction_bytes;
    }
    /// Repack only after dead space exceeds live space and one MiB. Each
    /// compaction reclaims at least half the file; copying uses fixed scratch.
    pub fn compact(self: *Run) !void {
        while (!try self.compactStep(64 * 1024)) {}
    }

    /// One descriptor or at most max_bytes of physical copying per turn.
    /// The private run is exclusively owned: reads/appends are forbidden
    /// until completion, including views obtained before compaction began.
    pub fn compactStep(self: *Run, max_bytes: usize) !bool {
        if (max_bytes == 0 or max_bytes > 64 * 1024 or self.compaction_failed) return error.InvalidData;
        std.debug.assert(self.active_logical == self.length);
        self.last_compaction_bytes = 0;
        if (self.compaction == null) {
            const dead = self.persisted - self.live_bytes;
            const repack = dead >= 1024 * 1024 and dead >= self.live_bytes;
            if (!repack and !(self.extents.items.len > 64 and self.dead_extents >= self.extents.items.len / 2)) return true;
            self.compaction = .{ .repack = repack, .dead = dead };
            self.read_pages = @splat(.{});
        }
        // An interrupted overlapping copy cannot safely replay its previous
        // source window. The owner must discard/rebuild this private run.
        errdefer self.compaction_failed = true;
        const state = &self.compaction.?;
        if (state.index == self.extents.items.len) {
            if (state.repack) {
                try self.file.setLength(self.io, state.end);
                self.persisted = state.end;
                self.active_physical = state.end;
                self.reclaimed_bytes += state.dead;
            }
            self.extents.shrinkRetainingCapacity(state.kept);
            self.dead_extents = 0;
            self.compaction = null;
            return true;
        }
        const extent = self.extents.items[state.index];
        if (extent.length == 0) {
            state.index += 1;
            return false;
        }
        var scratch: [64 * 1024]u8 = undefined;
        if (state.repack and extent.physical != state.end and state.copied < extent.length) {
            const take = @min(max_bytes, extent.length - state.copied);
            if (try self.file.readPositionalAll(self.io, scratch[0..take], extent.physical + state.copied) != take) return error.EndOfStream;
            try self.file.writePositionalAll(self.io, scratch[0..take], state.end + state.copied);
            state.copied += take;
            self.last_compaction_bytes = take;
            self.read_calls += 1;
            self.write_calls += 1;
            if (state.copied != extent.length) return false;
        }
        self.extents.items[state.kept] = .{ .logical = extent.logical, .physical = if (state.repack) state.end else extent.physical, .length = extent.length };
        state.kept += 1;
        state.index += 1;
        state.end += extent.length;
        state.copied = 0;
        return false;
    }
    pub fn view(self: *Run) !source.View {
        try self.flush();
        return self.sealedView();
    }
    /// Already-sealed extents are durable in the descriptor. Reading them must
    /// not flush the active output while a carry copies its input sidecars.
    pub fn sealedView(self: *Run) !source.View {
        try self.requireAvailable();
        return source.View.init(.{ .ranges = .{ .ptr = self, .length = self.length, .read_into = read, .close = close } }, 0, self.length);
    }
    fn read(raw: *anyopaque, offset: u64, bytes: []u8) !void {
        const self: *Run = @ptrCast(@alignCast(raw));
        try self.requireAvailable();
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

test "postings run compaction bounds physical copying and preserves logical ranges" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const io = std.testing.io;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("postings-compact-step");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, directory.path());
    const run = try Run.create(std.testing.allocator, io, directory.path());
    defer run.deinit();
    const block: [4096]u8 = @splat(11);
    try run.appendSlice(block[0..16]);
    try run.seal(0);
    const live_start = run.len();
    for (0..256) |i| {
        const payload: [4096]u8 = @splat(@intCast(i));
        try run.appendSlice(&payload);
    }
    try run.appendSlice("seventeen-bytes!!");
    try run.seal(live_start);
    const dead_start = run.len();
    for (0..512) |_| try run.appendSlice(&block);
    try run.seal(dead_start);
    const old_view = try run.sealedView();
    run.releaseRange(0);
    run.releaseRange(dead_start);
    try std.testing.expect(!try run.compactStep(4096));
    try std.testing.expectError(error.InvalidData, run.sealedView());
    try std.testing.expectError(error.InvalidData, run.appendSlice("forbidden"));
    var byte: [1]u8 = undefined;
    try std.testing.expectError(error.InvalidData, old_view.readInto(live_start, &byte));
    var turns: usize = 1;
    while (!try run.compactStep(4096)) {
        turns += 1;
        try std.testing.expect(run.last_compaction_bytes <= 4096);
        if (turns > 300) return error.TestUnexpectedResult;
    }
    try std.testing.expect(turns > 256);
    try std.testing.expectEqual(256 * 4096 + "seventeen-bytes!!".len, run.persisted);
    try std.testing.expectEqual(@as(usize, 2 * 1024 * 1024 + 16), run.reclaimed_bytes);
    try std.testing.expectEqual(@as(usize, 1), run.extents.items.len);
    var output: [4096]u8 = undefined;
    for (0..256) |i| {
        try old_view.readInto(live_start + i * 4096, &output);
        try std.testing.expect(std.mem.allEqual(u8, &output, @intCast(i)));
    }
    const tail = run.len();
    try run.appendSlice("tail");
    try run.seal(tail);
    const current = try run.sealedView();
    var suffix: [4]u8 = undefined;
    try current.readInto(tail, &suffix);
    try std.testing.expectEqualStrings("tail", &suffix);
}

test "postings run compaction slices descriptor retirement without rewriting small files" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const io = std.testing.io;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("postings-compact-metadata");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, directory.path());
    const run = try Run.create(std.testing.allocator, io, directory.path());
    defer run.deinit();
    for (0..128) |i| {
        const start = run.len();
        try run.appendSlice(&.{@intCast(i)});
        try run.seal(start);
    }
    for (0..64) |i| run.releaseRange(i * 2);
    var turns: usize = 0;
    while (true) {
        turns += 1;
        const done = try run.compactStep(1);
        try std.testing.expectEqual(@as(usize, 0), run.last_compaction_bytes);
        if (done) break;
        if (turns > 130) return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, 129), turns);
    try std.testing.expectEqual(@as(usize, 64), run.extents.items.len);
    try std.testing.expectEqual(@as(usize, 128), run.persisted);
    try std.testing.expectEqual(@as(usize, 0), run.reclaimed_bytes);
    const view = try run.sealedView();
    for (0..64) |i| {
        var byte: [1]u8 = undefined;
        try view.readInto(i * 2 + 1, &byte);
        try std.testing.expectEqual(@as(u8, @intCast(i * 2 + 1)), byte[0]);
    }
}

test "postings run compaction failure permanently rejects partially moved bytes" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const io = std.testing.io;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("postings-compact-fault");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(io, directory.path());
    const run = try Run.create(std.testing.allocator, io, directory.path());
    defer run.deinit();
    const block: [4096]u8 = @splat(12);
    for (0..512) |_| try run.appendSlice(&block);
    try run.seal(0);
    const live_start = run.len();
    try run.appendSlice(&block);
    try run.appendSlice(&block);
    try run.seal(live_start);
    run.releaseRange(0);
    try std.testing.expect(!try run.compactStep(4096)); // dead descriptor
    try std.testing.expect(!try run.compactStep(4096)); // first copied window
    // Private-file fault injection: the next source window is truncated.
    try run.file.setLength(io, live_start + 4096);
    try std.testing.expectError(error.EndOfStream, run.compactStep(4096));
    try std.testing.expectError(error.InvalidData, run.compactStep(4096));
    try std.testing.expectError(error.InvalidData, run.sealedView());
    try std.testing.expectError(error.InvalidData, run.appendSlice("no retry"));
}
