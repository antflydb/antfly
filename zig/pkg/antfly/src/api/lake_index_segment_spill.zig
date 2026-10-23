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

//! Buffered, quota-accounted native segment output shared by canonical and
//! cross-file compaction. Its unlinked spill file stays operation-private.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
pub const LocalSegment = struct {
    a: A,
    file: local.sql_spill.File,
    refs: usize = 1,
    test_ahead_reads: if (@import("builtin").is_test) std.atomic.Value(usize) else void = if (@import("builtin").is_test) .init(0) else {},
    statistics: ?*@import("lake_index_text_statistics.zig").Captured = null,
    store: *stores.ArtifactStore,
    mutex: std.Io.Mutex = .init,
    buffer: [16 * 1024]u8 = undefined,
    used: usize = 0,
    pub fn empty(a: A, manager: *local.sql_spill.Manager, store: *stores.ArtifactStore) !*LocalSegment {
        const self = try a.create(LocalSegment);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .file = try manager.create(), .store = store };
        return self;
    }
    pub fn create(a: A, manager: *local.sql_spill.Manager, store: *stores.ArtifactStore, bytes: []const u8) !*LocalSegment {
        const self = try empty(a, manager, store);
        errdefer self.release();
        try self.append(bytes);
        try self.seal();
        return self;
    }
    fn write(self: *LocalSegment, offset: u64, bytes: []const u8) !void {
        var phase = try self.store.beginIo(1);
        defer phase.abort();
        self.file.writeRaw(offset, bytes) catch |err| return phase.failed(err);
        try phase.finish();
    }
    fn flush(self: *LocalSegment) !void {
        if (self.used == 0) return;
        try self.write(self.file.size, self.buffer[0..self.used]);
        self.used = 0;
    }
    pub fn seal(self: *LocalSegment) !void {
        try self.flush();
        if (self.statistics) |captured| try captured.seal();
        var phase = try self.store.beginIo(1);
        defer phase.abort();
        self.file.seal() catch |err| return phase.failed(err);
        try phase.finish();
    }
    fn append(self: *LocalSegment, bytes: []const u8) !void {
        if (bytes.len > 32 * 1024 * 1024 -| (self.file.size + self.used)) return error.NativeLakeTextCorpusTooLarge;
        if (bytes.len > self.buffer.len - self.used) try self.flush();
        if (bytes.len >= self.buffer.len) return self.write(self.file.size, bytes);
        @memcpy(self.buffer[self.used..][0..bytes.len], bytes);
        self.used += bytes.len;
    }
    fn owner(raw: *anyopaque) *LocalSegment {
        return @ptrCast(@alignCast(raw));
    }
    fn length(raw: *anyopaque) usize {
        const self = owner(raw);
        return @intCast(self.file.size + self.used);
    }
    fn appendSlice(raw: *anyopaque, bytes: []const u8) !void {
        try owner(raw).append(bytes);
    }
    fn appendByte(raw: *anyopaque, byte: u8) !void {
        try owner(raw).append(&.{byte});
    }
    fn appendNTimes(raw: *anyopaque, byte: u8, count: usize) !void {
        var bytes: [4096]u8 = undefined;
        @memset(&bytes, byte);
        var remaining = count;
        while (remaining != 0) {
            const n = @min(remaining, bytes.len);
            try owner(raw).append(bytes[0..n]);
            remaining -= n;
        }
    }
    fn writeAt(raw: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self = owner(raw);
        if (offset > length(raw) or bytes.len > length(raw) - offset) return error.InvalidSegment;
        try self.flush();
        try self.write(offset, bytes);
    }
    fn crcRange(raw: *anyopaque, offset: usize, count: usize) !u32 {
        const self = owner(raw);
        try self.flush();
        var bytes: [8192]u8 = undefined;
        return self.source().checksum(offset, count, &bytes);
    }
    fn crcPrefix(raw: *anyopaque, count: usize) !u32 {
        return crcRange(raw, 0, count);
    }
    fn resident(raw: *anyopaque) usize {
        const self = owner(raw);
        return @sizeOf(LocalSegment) + self.file.write_buffer.len + self.file.read_buffer.len;
    }
    pub fn captureStatistics(self: *LocalSegment, manager: *local.sql_spill.Manager) void {
        std.debug.assert(self.statistics == null);
        self.statistics = @import("lake_index_text_statistics.zig").Captured.create(self.a, manager) catch null;
        if (self.statistics) |captured| captured.phase = self.store.io_phase;
    }
    pub fn sink(self: *LocalSegment) local.segment.SegmentSink {
        return .{ .term_observer = if (self.statistics) |captured| captured.observer() else null, .ptr = self, .vtable = &.{ .len = length, .append_slice = appendSlice, .append_byte = appendByte, .append_ntimes = appendNTimes, .write_at = writeAt, .crc32_prefix = crcPrefix, .crc32_range = crcRange, .resident_bytes = resident } };
    }
    pub fn retain(self: *LocalSegment) void {
        self.refs += 1;
    }
    pub fn release(self: *LocalSegment) void {
        self.refs -= 1;
        if (self.refs == 0) {
            if (self.statistics) |captured| captured.deinit();
            self.file.close();
            self.a.destroy(self);
        }
    }
    fn read(raw: *anyopaque, offset: u64, target: []u8) !void {
        const self: *LocalSegment = @ptrCast(@alignCast(raw));
        // Park CPU before waiting on the shared file, then drop its lock before
        // restoring CPU admission. A reader holding the last CPU permit cannot
        // block an I/O owner that needs that permit before releasing this file.
        var phase = try self.store.beginIo(1);
        defer phase.abort();
        self.readLocked(offset, target) catch |err| return phase.failed(err);
        try phase.finish();
    }
    /// Borrow a local immutable file through a separately admitted I/O task.
    /// Provider-backed sources cannot acquire this capability accidentally.
    pub fn readAheadSource(source_: local.index.SegmentSource) ?local.index.SegmentSource {
        if (source_ != .ranges or source_.ranges.read_into != read) return null;
        var result = source_;
        result.ranges.read_into = readUnadmitted;
        return result;
    }
    fn readUnadmitted(raw: *anyopaque, offset: u64, target_: []u8) !void {
        const self: *LocalSegment = @ptrCast(@alignCast(raw));
        if (@import("builtin").is_test) _ = self.test_ahead_reads.fetchAdd(1, .monotonic);
        try self.readLocked(offset, target_);
    }
    fn readLocked(self: *LocalSegment, offset: u64, target: []u8) !void {
        try self.mutex.lock(self.file.manager.io);
        defer self.mutex.unlock(self.file.manager.io);
        try self.file.readRaw(offset, target);
    }
    fn close(_: *anyopaque) void {}
    pub fn source(self: *LocalSegment) local.index.SegmentSource {
        return .{ .ranges = .{ .ptr = self, .length = self.file.size, .read_into = read, .close = close } };
    }
    pub fn materialize(self: *LocalSegment, a: A) ![]u8 {
        const bytes = try a.alloc(u8, @intCast(self.file.size));
        errdefer a.free(bytes);
        try read(self, 0, bytes);
        return bytes;
    }
};

/// Small outputs stay in memory. Crossing the resident threshold migrates the
/// prefix once into quota-accounted scratch; all later writes stay seekable.
pub const AdaptiveSink = struct {
    memory: local.segment.MemorySegmentSink,
    manager: *local.sql_spill.Manager,
    store: *stores.ArtifactStore,
    threshold: usize = 512 * 1024,
    staged: ?*LocalSegment = null,
    pub fn init(a: A, manager: *local.sql_spill.Manager, store: *stores.ArtifactStore) AdaptiveSink {
        return .{ .memory = .init(a), .manager = manager, .store = store };
    }
    pub fn deinit(self: *AdaptiveSink) void {
        if (self.staged) |staged| staged.release();
        self.memory.deinit();
    }
    fn owner(raw: *anyopaque) *AdaptiveSink {
        return @ptrCast(@alignCast(raw));
    }
    fn target(self: *AdaptiveSink) local.segment.SegmentSink {
        return if (self.staged) |staged| staged.sink() else self.memory.sink();
    }
    fn prepare(self: *AdaptiveSink, extra: usize) !void {
        if (self.staged != null or extra <= self.threshold -| self.memory.out.items.len) return;
        const staged = try LocalSegment.empty(self.memory.alloc, self.manager, self.store);
        errdefer staged.release();
        var sink_ = staged.sink();
        try sink_.appendSlice(self.memory.out.items);
        self.memory.out.clearAndFree(self.memory.alloc);
        self.staged = staged;
    }
    fn length(raw: *anyopaque) usize {
        var target_ = owner(raw).target();
        return target_.len();
    }
    fn appendSlice(raw: *anyopaque, bytes: []const u8) !void {
        const self = owner(raw);
        try self.prepare(bytes.len);
        var target_ = self.target();
        try target_.appendSlice(bytes);
    }
    fn appendByte(raw: *anyopaque, byte: u8) !void {
        return appendSlice(raw, &.{byte});
    }
    fn appendNTimes(raw: *anyopaque, byte: u8, count: usize) !void {
        const self = owner(raw);
        try self.prepare(count);
        var target_ = self.target();
        try target_.appendNTimes(byte, count);
    }
    fn writeAt(raw: *anyopaque, offset: usize, bytes: []const u8) !void {
        var target_ = owner(raw).target();
        try target_.writeAt(offset, bytes);
    }
    fn crcRange(raw: *anyopaque, offset: usize, count: usize) !u32 {
        var target_ = owner(raw).target();
        return target_.crc32Range(offset, count);
    }
    fn crcPrefix(raw: *anyopaque, count: usize) !u32 {
        return crcRange(raw, 0, count);
    }
    fn resident(raw: *anyopaque) usize {
        var target_ = owner(raw).target();
        return target_.residentBytes();
    }
    pub fn sink(self: *AdaptiveSink) local.segment.SegmentSink {
        return .{ .ptr = self, .vtable = &.{ .len = length, .append_slice = appendSlice, .append_byte = appendByte, .append_ntimes = appendNTimes, .write_at = writeAt, .crc32_prefix = crcPrefix, .crc32_range = crcRange, .resident_bytes = resident } };
    }
};

test "external lake segment spill releases file locks across admission and read failures" {
    const a = std.testing.allocator;
    var store: stores.ArtifactStore = undefined;
    store.io_phase = null;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check };
    defer manager.deinit();
    const segment = try LocalSegment.create(a, &manager, &store, "immutable native segment");
    defer segment.release();
    const Probe = struct {
        segment: *LocalSegment,
        begins: usize = 0,
        finishes: usize = 0,
        aborted: usize = 0,
        fn unlocked(self: *@This()) !void {
            if (!self.segment.mutex.tryLock()) return error.SegmentLockHeldAcrossAdmission;
            self.segment.mutex.unlock(std.testing.io);
        }
        fn begin(raw: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.unlocked();
            try std.testing.expectEqual(@as(usize, 1), count);
            self.begins += 1;
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.unlocked();
            self.finishes += 1;
        }
        fn abort(raw: *anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(raw))).aborted += 1;
        }
    };
    var probe: Probe = .{ .segment = segment };
    const phase: stores.IoPhase = .{ .io = std.testing.io, .ptr = &probe, .max_parallel = 1, .begin = Probe.begin, .finish = Probe.finish, .abort = Probe.abort };
    store.io_phase = &phase;
    var output: [9]u8 = undefined;
    try LocalSegment.read(segment, 0, &output);
    try std.testing.expectEqualStrings("immutable", &output);
    try std.testing.expectError(error.InvalidSqlSpill, LocalSegment.read(segment, segment.file.size + 1, &output));
    try probe.unlocked();
    try std.testing.expectEqual(@as(usize, 2), probe.begins);
    try std.testing.expectEqual(probe.begins, probe.finishes);
    try std.testing.expectEqual(@as(usize, 0), probe.aborted);
}
