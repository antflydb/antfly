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

//! Statement-owned temporary storage and bounded external merge runs. Files
//! are private, quota-controlled, snapshot-local and deleted on every unwind.
const std = @import("std");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const Row = operators.Row;
const Allocator = std.mem.Allocator;
const native_spill_supported = @import("builtin").os.tag != .freestanding;
pub const none = std.math.maxInt(u64);
const frame_bytes = 25;
const snappy = @import("../encoding/snappy.zig");

pub const Manager = struct {
    patterns: std.ArrayList(*scalar.PatternSet) = .empty,
    pattern_file: ?*File = null,
    alloc: Allocator,
    io: std.Io,
    context: *anyopaque,
    checkpoint: *const fn (*anyopaque) anyerror!void,
    root: []const u8 = "/tmp",
    max_bytes: u64 = 1024 * 1024 * 1024,
    max_record_bytes: usize = 4 * 1024 * 1024,
    /// Logical decoded array bounds are independent of encoded record bytes.
    /// The statement allocator additionally enforces actual resident memory.
    array_limits: @import("array_value.zig").Limits = .{},
    live_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    written_bytes: u64 = 0,
    merges: usize = 0,
    read_calls: u64 = 0,
    write_calls: u64 = 0,
    read_bytes: u64 = 0,
    buffer_bytes: usize = 4096,
    async_writes: bool = true,
    compression: enum { none, snappy } = .snappy,
    compressed_records: u64 = 0,
    dir: ?std.Io.Dir = null,
    parent: ?std.Io.Dir = null,
    directory_name: [43]u8 = undefined,
    sequence: u64 = 0,
    files: usize = 0,
    peak_files: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    allocation_mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *Manager) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn allocationLock(self: *Manager) void {
        while (!self.allocation_mutex.tryLock()) std.atomic.spinLoopHint();
    }
    /// Independent partition files share a statement arena and memory budget.
    /// Immutable parent identity for admission through this synchronizing wrapper.
    pub fn parentAllocator(a: std.mem.Allocator) ?std.mem.Allocator {
        if (a.vtable.alloc != allocate) return null;
        return @as(*Manager, @ptrCast(@alignCast(a.ptr))).alloc;
    }
    pub fn allocator(self: *Manager) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resizeAllocation, .remap = remapAllocation, .free = freeAllocation } };
    }
    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Manager = @ptrCast(@alignCast(raw));
        self.allocationLock();
        defer self.allocation_mutex.unlock();
        return self.alloc.rawAlloc(len, alignment, ra);
    }
    fn resizeAllocation(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Manager = @ptrCast(@alignCast(raw));
        self.allocationLock();
        defer self.allocation_mutex.unlock();
        return self.alloc.rawResize(bytes, alignment, len, ra);
    }
    fn remapAllocation(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Manager = @ptrCast(@alignCast(raw));
        self.allocationLock();
        defer self.allocation_mutex.unlock();
        return self.alloc.rawRemap(bytes, alignment, len, ra);
    }
    fn freeAllocation(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Manager = @ptrCast(@alignCast(raw));
        self.allocationLock();
        defer self.allocation_mutex.unlock();
        self.alloc.rawFree(bytes, alignment, ra);
    }
    fn increment(self: *Manager, comptime field: []const u8, value: anytype) void {
        self.lock();
        defer self.mutex.unlock();
        @field(self, field) += value;
    }
    fn reserve(self: *Manager, bytes: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        if (bytes > self.max_bytes -| self.live_bytes) return error.SqlProgramLimitExceeded;
        self.live_bytes += bytes;
        self.peak_bytes = @max(self.peak_bytes, self.live_bytes);
    }
    pub fn fileHeadroom(self: *Manager) usize {
        self.lock();
        defer self.mutex.unlock();
        return 64 - self.files;
    }
    pub fn admissionStats(self: *Manager) struct { peak_files: usize, peak_bytes: u64, max_bytes: u64 } {
        self.lock();
        defer self.mutex.unlock();
        return .{ .peak_files = self.peak_files, .peak_bytes = self.peak_bytes, .max_bytes = self.max_bytes };
    }
    fn releaseBytes(self: *Manager, bytes: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.live_bytes -= bytes;
    }
    fn closeFile(self: *Manager, bytes: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.live_bytes -= bytes;
        self.files -= 1;
    }
    pub fn registerPattern(self: *Manager, pattern: *scalar.PatternSet) !void {
        self.lock();
        defer self.mutex.unlock();
        try self.patterns.append(self.allocator(), pattern);
    }
    fn patternId(self: *Manager, pattern: *scalar.PatternSet) !usize {
        self.lock();
        defer self.mutex.unlock();
        for (self.patterns.items, 0..) |item, id| if (item == pattern) return id;
        return error.InvalidSqlSpill;
    }
    fn patternAt(self: *Manager, id: u64) !*scalar.PatternSet {
        self.lock();
        defer self.mutex.unlock();
        if (id >= self.patterns.items.len) return error.InvalidSqlSpill;
        return self.patterns.items[@intCast(id)];
    }
    pub fn check(self: *Manager) !void {
        try self.checkpoint(self.context);
    }
    fn open(self: *Manager) !void {
        if (comptime !native_spill_supported) return error.SqlProgramLimitExceeded;
        if (self.dir != null) return;
        try self.check();
        const parent = try std.Io.Dir.openDirAbsolute(self.io, self.root, .{});
        errdefer parent.close(self.io);
        var random: [16]u8 = undefined;
        try self.io.randomSecure(&random);
        _ = try std.fmt.bufPrint(&self.directory_name, "antfly-sql-{s}", .{std.fmt.bytesToHex(random, .lower)});
        try parent.createDir(self.io, &self.directory_name, .fromMode(0o700));
        errdefer parent.deleteTree(self.io, &self.directory_name) catch {};
        self.dir = try parent.openDir(self.io, &self.directory_name, .{});
        self.parent = parent;
    }
    pub fn create(self: *Manager) !File {
        // Freestanding builds have no native spill directory. Keep bounded
        // in-memory SQL available and report its existing limit on overflow.
        if (comptime !native_spill_supported) return error.SqlProgramLimitExceeded;
        self.lock();
        defer self.mutex.unlock();
        if (self.max_bytes == 0) return error.SqlProgramLimitExceeded;
        try self.open();
        if (self.files >= 64) return error.SqlProgramLimitExceeded;
        self.sequence += 1;
        var name: [24]u8 = undefined;
        const text = try std.fmt.bufPrint(&name, "{d}", .{self.sequence});
        const file = try self.dir.?.createFile(self.io, text, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
        // Runs are accessed through open handles only. Unlink immediately so
        // process crashes cannot leave row payloads behind on supported hosts.
        errdefer file.close(self.io);
        try self.dir.?.deleteFile(self.io, text);
        self.files += 1;
        self.peak_files = @max(self.peak_files, self.files);
        return .{ .manager = self, .file = file, .id = self.sequence };
    }
    pub fn deinit(self: *Manager) void {
        // Memory-only callers need no cancellation-capable Io. No resources
        // below this boundary can exist without a directory or pattern owner.
        if (self.dir == null and self.pattern_file == null and self.patterns.items.len == 0) {
            self.patterns.deinit(self.allocator());
            return;
        }
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        for (self.patterns.items) |pattern| pattern.close(pattern.ptr);
        self.patterns.deinit(self.alloc);
        self.patterns = .empty;
        if (self.pattern_file) |file| {
            file.close();
            self.alloc.destroy(file);
        }
        self.pattern_file = null;
        if (comptime !native_spill_supported) {
            std.debug.assert(self.dir == null and self.parent == null);
            return;
        }
        if (self.dir) |dir| dir.close(self.io);
        if (self.parent) |parent| {
            parent.deleteTree(self.io, &self.directory_name) catch {};
            parent.close(self.io);
        }
        self.dir = null;
        self.parent = null;
    }
};
pub const Decoded = struct { row: Row, next: u64, matched: bool, following: u64 };
pub const File = struct {
    manager: *Manager,
    file: std.Io.File,
    id: u64,
    size: u64 = 0,
    closed: bool = false,
    run_store: ?*RunStore = null,
    run_blocks: std.ArrayList(u64) = .empty,
    write_buffer: []u8 = &.{},
    write_start: u64 = 0,
    write_len: usize = 0,
    read_buffer: []u8 = &.{},
    read_start: u64 = none,
    read_len: usize = 0,
    buffer_bytes: ?usize = null,
    spare: []u8 = &.{},
    pending: ?*WriteJob = null,
    write_job: ?*WriteJob = null,
    const WriteJob = struct {
        io: std.Io,
        file: std.Io.File,
        buffer: []u8,
        len: usize,
        offset: u64,
        future: ?@import("parallel_scheduler.zig").Task(anyerror!void) = null,
        fn write(job: *WriteJob) anyerror!void {
            try job.file.writePositionalAll(job.io, job.buffer[0..job.len], job.offset);
        }
    };
    fn awaitWrite(self: *File) !void {
        const job = self.pending orelse return;
        const result = job.future.?.await(self.manager.io);
        self.pending = null;
        self.spare = job.buffer;
        try result;
    }
    // One in-flight buffer per file. Worker code owns stable bytes and never
    // touches the statement allocator, quota counters, or mutable operators.
    fn submit(self: *File) !void {
        if (self.write_len == 0) return;
        try self.awaitWrite();
        try self.manager.check();
        if (self.run_store == null and self.manager.async_writes and self.write_buffer.len >= 4096 and self.write_len >= self.write_buffer.len / 2) {
            const job = (try self.prepareAsync()) orelse {
                try self.writePhysical(self.write_buffer[0..self.write_len], self.write_start);
                self.manager.increment("write_calls", 1);
                self.write_len = 0;
                return;
            };
            job.* = .{ .io = self.manager.io, .file = self.file, .buffer = self.write_buffer, .len = self.write_len, .offset = self.write_start };
            const future = @import("parallel_scheduler.zig").global().submit(self.manager.io, job.buffer.len, WriteJob.write, .{job}) orelse {
                try WriteJob.write(job);
                self.manager.increment("write_calls", 1);
                self.write_len = 0;
                return;
            };
            job.future = future;
            self.pending = job;
            self.write_buffer = self.spare;
            self.spare = &.{};
        } else {
            try self.writePhysical(self.write_buffer[0..self.write_len], self.write_start);
        }
        self.manager.increment("write_calls", 1);
        self.write_len = 0;
    }
    fn prepareAsync(self: *File) !?*WriteJob {
        // Async buffering is optional. A known quota denial falls back before
        // touching the parent budget; actual allocator failures propagate.
        const required = (if (self.spare.len == 0) self.write_buffer.len else @as(usize, 0)) +
            (if (self.write_job == null) @sizeOf(WriteJob) else @as(usize, 0));
        if (required != 0) {
            const available = @import("memory_budget.zig").optionalHeadroom(self.manager.alloc) orelse return null;
            if (required > available) return null;
        }
        const fresh = self.spare.len == 0;
        if (fresh) self.spare = try self.manager.allocator().alloc(u8, self.write_buffer.len);
        errdefer if (fresh) {
            self.manager.allocator().free(self.spare);
            self.spare = &.{};
        };
        if (self.write_job) |job| return job;
        const job = try self.manager.allocator().create(WriteJob);
        self.write_job = job;
        return job;
    }
    /// Sorted runs become immutable before merging. Reclaim writer buffers
    /// so each merge head retains only its reader buffer and decoded record.
    pub fn seal(self: *File) !void {
        try self.flush();
        self.manager.allocator().free(self.write_buffer);
        self.write_buffer = &.{};
        self.manager.allocator().free(self.spare);
        self.spare = &.{};
        if (self.write_job) |job| self.manager.allocator().destroy(job);
        self.write_job = null;
    }
    pub fn flush(self: *File) !void {
        try self.submit();
        try self.awaitWrite();
    }
    fn bufferedWrite(self: *File, bytes: []const u8, offset: u64) !void {
        if (self.run_store) |store| {
            const end = try std.math.add(u64, offset, bytes.len);
            const needed = std.math.divCeil(u64, end, RunStore.block_bytes) catch unreachable;
            while (self.run_blocks.items.len < needed) {
                try self.run_blocks.ensureUnusedCapacity(self.manager.allocator(), 1);
                self.run_blocks.appendAssumeCapacity(try store.allocate());
            }
        }
        self.read_start = none;
        if (self.write_buffer.len == 0) self.write_buffer = try self.manager.allocator().alloc(u8, @max(1, self.buffer_bytes orelse self.manager.buffer_bytes));
        if (self.write_len != 0 and offset != self.write_start + self.write_len) try self.submit();
        if (bytes.len > self.write_buffer.len) {
            try self.flush();
            try self.writePhysical(bytes, offset);
            self.manager.increment("write_calls", 1);
            return;
        }
        if (bytes.len > self.write_buffer.len - self.write_len) try self.submit();
        if (self.write_len == 0) self.write_start = offset;
        @memcpy(self.write_buffer[self.write_len..][0..bytes.len], bytes);
        self.write_len += bytes.len;
    }
    fn writePhysical(self: *File, bytes: []const u8, offset: u64) !void {
        if (self.run_store == null) return self.file.writePositionalAll(self.manager.io, bytes, offset);
        var position: usize = 0;
        while (position < bytes.len) {
            const logical = offset + position;
            const block: usize = @intCast(logical / RunStore.block_bytes);
            const within = logical % RunStore.block_bytes;
            const n: usize = @intCast(@min(bytes.len - position, RunStore.block_bytes - within));
            if (block >= self.run_blocks.items.len) return error.InvalidSqlSpill;
            try self.file.writePositionalAll(self.manager.io, bytes[position..][0..n], self.run_blocks.items[block] + within);
            if (position != 0) self.manager.increment("write_calls", 1);
            position += n;
        }
    }
    fn readPhysical(self: *File, bytes: []u8, offset: u64) !usize {
        if (self.run_store == null) return self.file.readPositionalAll(self.manager.io, bytes, offset);
        var position: usize = 0;
        while (position < bytes.len) {
            const logical = offset + position;
            const block: usize = @intCast(logical / RunStore.block_bytes);
            const within = logical % RunStore.block_bytes;
            const n: usize = @intCast(@min(bytes.len - position, RunStore.block_bytes - within));
            if (block >= self.run_blocks.items.len) return error.InvalidSqlSpill;
            const count = try self.file.readPositionalAll(self.manager.io, bytes[position..][0..n], self.run_blocks.items[block] + within);
            if (count != n) return error.InvalidSqlSpill;
            if (position != 0) self.manager.increment("read_calls", 1);
            position += count;
        }
        return position;
    }
    fn bufferedRead(self: *File, offset: u64, bytes: []u8) !void {
        try self.flush();
        if (self.read_buffer.len == 0) self.read_buffer = try self.manager.allocator().alloc(u8, @max(1, self.buffer_bytes orelse self.manager.buffer_bytes));
        if (bytes.len > self.read_buffer.len) {
            const count = try self.readPhysical(bytes, offset);
            self.manager.increment("read_calls", 1);
            self.manager.increment("read_bytes", count);
            if (count != bytes.len) return error.InvalidSqlSpill;
            return;
        }
        if (self.read_start == none or offset < self.read_start or offset - self.read_start > self.read_len or bytes.len > self.read_len -| (offset - self.read_start)) {
            self.read_start = offset;
            self.read_len = @intCast(@min(self.read_buffer.len, self.size - offset));
            const count = try self.readPhysical(self.read_buffer[0..self.read_len], offset);
            self.manager.increment("read_calls", 1);
            self.manager.increment("read_bytes", count);
            if (count != self.read_len) return error.InvalidSqlSpill;
        }
        @memcpy(bytes, self.read_buffer[@intCast(offset - self.read_start)..][0..bytes.len]);
    }
    pub fn close(self: *File) void {
        if (self.closed) return;
        if (self.pending) |job| {
            job.future.?.cancel(self.manager.io) catch {};
            self.manager.allocator().free(job.buffer);
            self.pending = null;
        }
        if (self.write_job) |job| self.manager.allocator().destroy(job);
        self.write_job = null;
        if (self.run_store) |store| {
            store.release(self.run_blocks.items);
            self.run_blocks.deinit(self.manager.allocator());
        } else self.file.close(self.manager.io);
        self.manager.allocator().free(self.spare);
        self.manager.allocator().free(self.write_buffer);
        self.manager.allocator().free(self.read_buffer);
        if (self.run_store == null) self.manager.closeFile(self.size);
        self.closed = true;
    }
    pub fn append(self: *File, row: Row, link: u64) !u64 {
        return self.appendWithPressure(row, link, null);
    }
    pub fn appendWithPressure(self: *File, row: Row, link: u64, pressure: ?*CachePressure) !u64 {
        try self.manager.check();
        const a = if (pressure) |scope| scope.allocator() else self.manager.allocator();
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(a);
        var encoder: Encoder = .{ .manager = self.manager, .a = a, .bytes = &bytes, .limit = self.manager.max_record_bytes };
        try encoder.word(row.ordinal);
        try encoder.cells(row.values);
        try encoder.cells(row.keys);
        var compressed: ?[]u8 = null;
        defer if (compressed) |value| a.free(value);
        // Avoid codec work on short or apparently incompressible records.
        // Compression is an existing Snappy block format, never a new codec.
        if (self.manager.compression == .snappy and bytes.items.len >= 4096) {
            const sample = bytes.items[0..@min(bytes.items.len, 1024)];
            var repeated: usize = 0;
            for (sample[1..], sample[0 .. sample.len - 1]) |x, y| repeated += @intFromBool(x == y);
            if (repeated > sample.len / 4) compressed = try snappy.encode(a, bytes.items);
        }
        const compressed_record = compressed != null and compressed.?.len + 32 < bytes.items.len;
        const stored = if (compressed_record) compressed.? else bytes.items;
        const growth = stored.len + frame_bytes;
        if (self.run_store == null) try self.manager.reserve(growth);
        errdefer if (self.run_store == null) self.manager.releaseBytes(growth);
        var frame: [frame_bytes]u8 = @splat(0);
        std.mem.writeInt(u64, frame[0..8], stored.len, .little);
        frame[24] = if (compressed_record) 2 else 0;
        std.mem.writeInt(u64, frame[8..16], std.hash.Wyhash.hash(0, bytes.items), .little);
        std.mem.writeInt(u64, frame[16..24], link, .little);
        const offset = self.size;
        if (pressure) |scope| if (self.write_buffer.len == 0) scope.prepare(@max(1, self.buffer_bytes orelse self.manager.buffer_bytes));
        try self.bufferedWrite(&frame, offset);
        try self.bufferedWrite(stored, offset + frame_bytes);
        self.manager.increment("compressed_records", @intFromBool(compressed_record));
        self.size += growth;
        self.manager.increment("written_bytes", growth);
        return offset;
    }
    pub fn read(self: *File, a: Allocator, offset: u64) !Decoded {
        try self.manager.check();
        if (offset > self.size or self.size - offset < frame_bytes) return error.InvalidSqlSpill;
        var frame: [frame_bytes]u8 = undefined;
        try self.bufferedRead(offset, &frame);
        const len = std.mem.readInt(u64, frame[0..8], .little);
        if (len > self.manager.max_record_bytes or len > self.size - offset - frame_bytes) return error.InvalidSqlSpill;
        const bytes = try a.alloc(u8, @intCast(len));
        defer a.free(bytes);
        try self.bufferedRead(offset + frame_bytes, bytes);
        if (frame[24] > 3) return error.InvalidSqlSpill;
        const decoded: ?[]u8 = if (frame[24] & 2 != 0) blk: {
            const length = snappy.decodedLen(bytes) catch return error.InvalidSqlSpill;
            if (length > self.manager.max_record_bytes) return error.InvalidSqlSpill;
            break :blk snappy.decode(a, bytes) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidSqlSpill,
            };
        } else null;
        defer if (decoded) |value| a.free(value);
        const payload = decoded orelse bytes;
        if (std.hash.Wyhash.hash(0, payload) != std.mem.readInt(u64, frame[8..16], .little)) return error.InvalidSqlSpill;
        const link = std.mem.readInt(u64, frame[16..24], .little);
        if (link != none and link >= offset) return error.InvalidSqlSpill;
        var decoder: Decoder = .{ .manager = self.manager, .a = a, .bytes = payload };
        const ordinal = try decoder.word();
        const values = try decoder.cells();
        const keys = try decoder.cells();
        if (decoder.position != payload.len) return error.InvalidSqlSpill;
        return .{ .row = .{ .values = values, .keys = keys, .ordinal = ordinal }, .next = link, .matched = frame[24] & 1 != 0, .following = offset + frame_bytes + len };
    }
    /// Fixed-size operator state/index pages share the statement disk quota.
    pub fn writeRaw(self: *File, offset: u64, bytes: []const u8) !void {
        try self.manager.check();
        if (offset > self.size) return error.InvalidSqlSpill;
        const end = std.math.add(u64, offset, bytes.len) catch return error.SqlProgramLimitExceeded;
        const growth = end -| self.size;
        if (self.run_store == null) try self.manager.reserve(growth);
        errdefer if (self.run_store == null) self.manager.releaseBytes(growth);
        try self.bufferedWrite(bytes, offset);
        self.size = @max(self.size, end);
        self.manager.increment("written_bytes", bytes.len);
    }
    pub fn readRaw(self: *File, offset: u64, bytes: []u8) !void {
        try self.manager.check();
        if (offset > self.size or bytes.len > self.size - offset) return error.InvalidSqlSpill;
        try self.bufferedRead(offset, bytes);
    }
    pub fn match(self: *File, offset: u64) !void {
        try self.manager.check();
        if (offset > self.size or self.size - offset < frame_bytes) return error.InvalidSqlSpill;
        var flag: [1]u8 = undefined;
        try self.bufferedRead(offset + 24, &flag);
        if (flag[0] > 3) return error.InvalidSqlSpill;
        flag[0] |= 1;
        try self.bufferedWrite(&flag, offset + 24);
    }
};
const Encoder = struct {
    compact_arrays: bool = true,
    manager: ?*Manager = null,
    a: Allocator,
    bytes: *std.ArrayList(u8),
    limit: usize,
    base: usize = 0,
    scratch: ?Allocator = null,
    fn reserve(self: *Encoder, len: usize) ![]u8 {
        if (len > self.limit -| (self.bytes.items.len - self.base)) return error.SqlProgramLimitExceeded;
        const start = self.bytes.items.len;
        try self.bytes.resize(self.a, start + len);
        return self.bytes.items[start..];
    }
    fn append(self: *Encoder, bytes: []const u8) !void {
        if (bytes.len > self.limit -| (self.bytes.items.len - self.base)) return error.SqlProgramLimitExceeded;
        try self.bytes.appendSlice(self.a, bytes);
    }
    fn word(self: *Encoder, value: u64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .little);
        try self.append(&bytes);
    }
    fn text(self: *Encoder, bytes: []const u8) !void {
        try self.word(bytes.len);
        try self.append(bytes);
    }
    fn cells(self: *Encoder, values: []const Datum) anyerror!void {
        try self.word(values.len);
        for (values) |value| try self.cell(value);
    }
    fn cell(self: *Encoder, value: Datum) anyerror!void {
        try self.append(&.{@intFromBool(value.sql_null)});
        if (value.numeric) |number| {
            if (value.sql_null or value.array != null or value.patterns != null or value.value != .null) return error.SqlTypeMismatch;
            const numeric = @import("numeric_value.zig");
            const binary = @import("numeric_binary.zig");
            var context: numeric.Context = .{ .alloc = self.scratch orelse self.a, .max_output_bytes = self.limit };
            const length = try binary.encodedSize(&context, number.*);
            try self.append(&.{10});
            try self.word(length);
            var writer: std.Io.Writer = .fixed(try self.reserve(length));
            try binary.encode(&context, number.*, &writer);
        } else if (value.array) |array| {
            if (value.sql_null or value.patterns != null or value.value != .null) return error.SqlTypeMismatch;
            const validated = try array.validate(if (self.manager) |manager| manager.array_limits else .{});
            if (validated.dimensions.len != array.dimensions.len) return error.InvalidSqlArrayShape;
            if (self.compact_arrays and @import("array_compact.zig").supported(array.element_type)) {
                try self.append(&.{ 11, @backingInt(array.element_type) });
                if (array.stored) |stored| {
                    try self.text(stored.bytes);
                } else {
                    var prepared = try @import("array_storage.zig").Prepared.init(self.scratch orelse self.a, array.*, .{ .values = if (self.manager) |m| m.array_limits else .{}, .wire_bytes = self.limit });
                    defer prepared.deinit();
                    try self.word(prepared.encoded_size);
                    try prepared.writeInto(try self.reserve(prepared.encoded_size));
                }
                return;
            }
            try self.append(&.{ 9, @backingInt(array.element_type) });
            try self.word(array.dimensions.len);
            for (array.dimensions) |dimension| {
                try self.word(dimension.length);
                try self.word(@bitCast(@as(i64, dimension.lower)));
            }
            try self.word(array.cardinality());
            for (0..array.cardinality()) |index| try self.cell(array.flatCell(index));
        } else if (value.patterns) |pattern| {
            const id = try (self.manager orelse return error.InvalidSqlSpill).patternId(pattern);
            try self.append(&.{8});
            try self.word(id);
        } else try self.json(value.value, 0);
    }
    /// Frozen recursive JSON encoding directly from an admitted canonical
    /// token stream. Counts are patched after emission; no DOM is staged.
    fn jsonBytes(self: *Encoder, bytes: []const u8) !void {
        var scanner = std.json.Scanner.initCompleteInput(self.scratch orelse self.a, bytes);
        defer scanner.deinit();
        try self.jsonTokens(&scanner, 0);
        if (try scanner.next() != .end_of_document) return error.InvalidSqlSpill;
    }
    fn jsonTokens(self: *Encoder, scanner: *std.json.Scanner, depth: usize) anyerror!void {
        if (depth > 64) return error.SqlProgramLimitExceeded;
        const a = self.scratch orelse self.a;
        const token = try scanner.nextAllocMax(a, .alloc_if_needed, scanner.input.len);
        defer switch (token) {
            .allocated_string, .allocated_number => |payload| a.free(payload),
            else => {},
        };
        switch (token) {
            .object_begin, .array_begin => {
                const object = token == .object_begin;
                try self.append(&.{if (object) 7 else 6});
                const prefix = self.bytes.items.len;
                try self.word(0);
                var count: u64 = 0;
                const close: std.json.TokenType = if (object) .object_end else .array_end;
                while (try scanner.peekNextTokenType() != close) {
                    if (object) {
                        const key = try scanner.nextAllocMax(a, .alloc_if_needed, scanner.input.len);
                        defer if (key == .allocated_string) a.free(key.allocated_string);
                        try self.text(switch (key) {
                            .string, .allocated_string => |payload| payload,
                            else => return error.InvalidSqlSpill,
                        });
                    }
                    try self.jsonTokens(scanner, depth + 1);
                    count += 1;
                }
                _ = try scanner.next();
                std.mem.writeInt(u64, self.bytes.items[prefix..][0..8], count, .little);
            },
            .string, .allocated_string => |payload| try self.json(.{ .string = payload }, depth),
            .number, .allocated_number => |payload| try self.json(.{ .number_string = payload }, depth),
            .true => try self.json(.{ .bool = true }, depth),
            .false => try self.json(.{ .bool = false }, depth),
            .null => try self.json(.null, depth),
            else => return error.InvalidSqlSpill,
        }
    }
    fn json(self: *Encoder, value: std.json.Value, depth: usize) anyerror!void {
        if (depth > 64) return error.SqlProgramLimitExceeded;
        switch (value) {
            .null => try self.append(&.{0}),
            .bool => |v| try self.append(&.{ 1, @intFromBool(v) }),
            .integer => |v| {
                try self.append(&.{2});
                try self.word(@bitCast(v));
            },
            .float => |v| {
                try self.append(&.{3});
                try self.word(@bitCast(v));
            },
            .number_string => |v| {
                try self.append(&.{4});
                try self.text(v);
            },
            .string => |v| {
                try self.append(&.{5});
                try self.text(v);
            },
            .array => |v| {
                try self.append(&.{6});
                try self.word(v.items.len);
                for (v.items) |item| try self.json(item, depth + 1);
            },
            .object => |v| {
                try self.append(&.{7});
                try self.word(v.count());
                for (v.keys(), v.values()) |key, item| {
                    try self.text(key);
                    try self.json(item, depth + 1);
                }
            },
        }
    }
};
// One immutable block owner serializes lazy cache publication. Readers of
// separate cells may share its arena; neither an arena nor its caches race.
const JsonScratch = struct {
    a: Allocator,
    names: [65]std.StringHashMapUnmanaged(void) = @splat(.empty),
    fn deinit(self: *JsonScratch) void {
        for (&self.names) |*names| names.deinit(self.a);
    }
};
// Frozen number_string tags are opaque in the old decoder. Only valid JSON
// lexemes can advertise lossless text; other values retain scalar replay.
fn jsonNumber(bytes: []const u8) bool {
    var at: usize = 0;
    if (at < bytes.len and bytes[at] == '-') at += 1;
    if (at == bytes.len) return false;
    if (bytes[at] == '0') at += 1 else {
        if (bytes[at] < '1' or bytes[at] > '9') return false;
        while (at < bytes.len and std.ascii.isDigit(bytes[at])) at += 1;
    }
    if (at < bytes.len and bytes[at] == '.') {
        at += 1;
        const start = at;
        while (at < bytes.len and std.ascii.isDigit(bytes[at])) at += 1;
        if (at == start) return false;
    }
    if (at < bytes.len and (bytes[at] == 'e' or bytes[at] == 'E')) {
        at += 1;
        if (at < bytes.len and (bytes[at] == '+' or bytes[at] == '-')) at += 1;
        const start = at;
        while (at < bytes.len and std.ascii.isDigit(bytes[at])) at += 1;
        if (at == start) return false;
    }
    return at == bytes.len;
}
const JsonOwner = struct {
    arena: std.heap.ArenaAllocator,
    mutex: std.atomic.Mutex = .unlocked,
    // Only consumers crossing a scalar/text boundary allocate cache records.
    // Keys borrow validated, immutable ranges owned by the enclosing block.
    cached: std.AutoHashMapUnmanaged(usize, *FrozenJson) = .empty,
    fn get(self: *JsonOwner, bytes: []const u8, lossless: bool) !*FrozenJson {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.cached.get(@intFromPtr(bytes.ptr))) |entry| return entry;
        const a = self.arena.allocator();
        const entry = try a.create(FrozenJson);
        entry.* = .{ .owner = self, .bytes = bytes, .lossless = lossless };
        try self.cached.put(a, @intFromPtr(bytes.ptr), entry);
        return entry;
    }
    fn deinit(self: *JsonOwner) void {
        self.arena.deinit();
    }
};
// Offsets are relative to the immutable block wire, never producer pointers.
// A zero length denotes an ordinary scalar; the top bit records lossless text.
const JsonRange = packed struct {
    begin: u32 = 0,
    length: u31 = 0,
    lossless: bool = false,
    const sql_null = std.math.maxInt(u32);
    const json_null = sql_null - 1;
};
const FrozenJson = struct {
    owner: *JsonOwner,
    bytes: []const u8,
    lossless: bool,
    value: ?std.json.Value = null,
    text: ?[]const u8 = null,
    fn lock(self: *FrozenJson) void {
        while (!self.owner.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn materialize(self: *FrozenJson) !Datum {
        self.lock();
        defer self.owner.mutex.unlock();
        if (self.value == null) {
            var decoder: Decoder = .{ .validated_json = true, .a = self.owner.arena.allocator(), .bytes = self.bytes };
            self.value = try decoder.json(0);
        }
        return Datum.json(self.value.?);
    }
    fn encoded(self: *FrozenJson) !?[]const u8 {
        // Integer/float DOM tags must survive scalar access and frozen replay.
        // Canonical text cannot distinguish them from number-string tags.
        if (!self.lossless) return null;
        self.lock();
        defer self.owner.mutex.unlock();
        if (self.text == null) {
            const work = @import("memory_budget.zig").reclaimingAllocator(self.owner.arena.allocator());
            var output: std.Io.Writer.Allocating = .init(work);
            defer output.deinit();
            var json: std.json.Stringify = .{ .writer = &output.writer };
            var decoder: Decoder = .{ .a = work, .bytes = self.bytes };
            _ = decoder.walkJson(0, &json) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            self.text = try self.owner.arena.allocator().dupe(u8, output.written());
        }
        return self.text;
    }
};
/// The handle is issued only after recursive validation by the block decoder.
pub fn materializeFrozenJson(a: Allocator, value: @import("execution_batch.zig").FrozenJson) !Datum {
    var decoder: Decoder = .{ .validated_json = true, .a = a, .bytes = value.bytes };
    const result = try decoder.json(0);
    if (decoder.position != value.bytes.len) return error.InvalidSqlSpill;
    return Datum.json(result);
}
/// Write an already validated frozen value without a DOM or text buffer.
/// Writer failures remain writer failures; callers own quota/error mapping.
pub fn writeFrozenJson(writer: *std.Io.Writer, value: @import("execution_batch.zig").FrozenJson) !void {
    var scratch = std.heap.FixedBufferAllocator.init(&.{});
    var json: std.json.Stringify = .{ .writer = writer };
    var decoder: Decoder = .{ .a = scratch.allocator(), .bytes = value.bytes };
    _ = try decoder.walkJson(0, &json);
    if (decoder.position != value.bytes.len) return error.InvalidSqlSpill;
}
pub fn frozenJsonText(a: Allocator, value: @import("execution_batch.zig").FrozenJson) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    writeFrozenJson(&output.writer, value) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
    return output.toOwnedSlice();
}
/// Scalar consumers can construct incompatible native tags during validation.
/// Forwarding consumers retain every validated compound as frozen bytes.
pub const JsonConsumption = enum { scalar, forwarding };
const Decoder = struct {
    json_consumption: JsonConsumption = .scalar,
    validated_json: bool = false,
    eager_nontext: bool = false,
    borrow_compact: bool = false,
    json_owner: ?*JsonOwner = null,
    json_scratch: ?*JsonScratch = null,
    lazy_json: bool = false,
    cache_backing: ?Allocator = null,
    compact_arrays: bool = true,
    manager: ?*Manager = null,
    a: Allocator,
    bytes: []const u8,
    position: usize = 0,
    fn take(self: *Decoder, len: usize) ![]const u8 {
        if (len > self.bytes.len -| self.position) return error.InvalidSqlSpill;
        const bytes = self.bytes[self.position..][0..len];
        self.position += len;
        return bytes;
    }
    fn word(self: *Decoder) !u64 {
        return std.mem.readInt(u64, (try self.take(8))[0..8], .little);
    }
    fn count(self: *Decoder) !usize {
        const len = try self.word();
        if (len > self.bytes.len - self.position) return error.InvalidSqlSpill;
        return @intCast(len);
    }
    fn text(self: *Decoder) ![]u8 {
        return self.a.dupe(u8, try self.take(try self.count()));
    }
    fn byte(self: *Decoder) !u8 {
        return (try self.take(1))[0];
    }
    fn numeric(self: *Decoder, flag: u8) !Datum {
        if (flag != 0 or try self.byte() != 10) return error.InvalidSqlSpill;
        const bytes = try self.take(try self.count());
        const exact = @import("numeric_value.zig");
        const binary = @import("numeric_binary.zig");
        var context: exact.Context = .{ .alloc = self.a, .max_input_bytes = bytes.len };
        var owned = try binary.decode(&context, bytes, .{});
        errdefer owned.deinit();
        binary.verifyCanonical(&context, bytes, owned.value) catch |err| return switch (err) {
            error.InvalidSqlBinaryRepresentation => error.InvalidSqlSpill,
            else => err,
        };
        const value = try self.a.create(exact.Value);
        value.* = owned.value;
        return Datum.typedNumeric(value);
    }
    fn cells(self: *Decoder) anyerror![]Datum {
        const values = try self.a.alloc(Datum, try self.count());
        for (values) |*value| {
            const flag = try self.byte();
            if (flag > 1) return error.InvalidSqlSpill;
            if (self.position < self.bytes.len and self.bytes[self.position] == 11) {
                self.position += 1;
                if (!self.compact_arrays or flag != 0) return error.InvalidSqlSpill;
                const arrays = @import("array_value.zig");
                const kind = std.enums.fromInt(arrays.ElementType, try self.byte()) orelse return error.InvalidSqlSpill;
                if (!@import("array_compact.zig").supported(kind)) return error.InvalidSqlSpill;
                const bytes = try self.take(try self.count());
                const array = try self.a.create(arrays.Value);
                const storage = @import("array_storage.zig");
                const options: storage.Options = .{ .values = if (self.manager) |m| m.array_limits else .{}, .wire_bytes = bytes.len };
                array.* = (if (self.borrow_compact) storage.borrowLeaky(self.a, kind, bytes, options) else storage.viewLeaky(self.a, kind, bytes, options)) catch |err| return switch (err) {
                    error.OutOfMemory => err,
                    else => error.InvalidSqlSpill,
                };
                value.* = Datum.typedArray(array);
            } else if (self.position < self.bytes.len and self.bytes[self.position] == 10) {
                value.* = try self.numeric(flag);
            } else if (self.position < self.bytes.len and self.bytes[self.position] == 9) {
                self.position += 1;
                if (flag != 0) return error.InvalidSqlSpill;
                const arrays = @import("array_value.zig");
                const kind = std.enums.fromInt(arrays.ElementType, try self.byte()) orelse return error.InvalidSqlSpill;
                const rank = try self.count();
                if (rank > 6) return error.InvalidSqlSpill;
                const dimensions = try self.a.alloc(arrays.Dimension, rank);
                for (dimensions) |*dimension| {
                    dimension.length = std.math.cast(u32, try self.word()) orelse return error.InvalidSqlSpill;
                    const lower: i64 = @bitCast(try self.word());
                    dimension.lower = std.math.cast(i32, lower) orelse return error.InvalidSqlSpill;
                }
                // Array elements cannot recursively contain SQL arrays or
                // owner-bound pattern handles. Decode them without admitting
                // those tags, so forged nesting cannot exhaust the stack.
                const elements = try self.a.alloc(Datum, try self.count());
                for (elements) |*element| {
                    const null_flag = try self.byte();
                    if (null_flag > 1) return error.InvalidSqlSpill;
                    if (self.position < self.bytes.len and self.bytes[self.position] == 10) {
                        if (kind != .numeric) return error.InvalidSqlSpill;
                        element.* = try self.numeric(null_flag);
                    } else element.* = .{ .sql_null = null_flag == 1, .value = try self.json(0) };
                }
                const array = try self.a.create(arrays.Value);
                array.* = arrays.Value.init(kind, dimensions, elements, if (self.manager) |manager| manager.array_limits else .{}) catch return error.InvalidSqlSpill;
                // Only the canonical zero-dimensional empty encoding is
                // emitted. Do not silently normalize corrupted dimensions.
                if (array.dimensions.len != rank) return error.InvalidSqlSpill;
                value.* = Datum.typedArray(array);
            } else if (self.position < self.bytes.len and self.bytes[self.position] == 8) {
                self.position += 1;
                const id = try self.word();
                if (flag != 0) return error.InvalidSqlSpill;
                value.* = .{ .sql_null = false, .patterns = try (self.manager orelse return error.InvalidSqlSpill).patternAt(id) };
            } else value.* = .{ .sql_null = flag == 1, .value = try self.json(0) };
        }
        return values;
    }
    /// Validate every frozen token and duplicate key before publishing the
    /// block. Nested payloads remain ranges; no JSON Value tree is built.
    fn walkJson(self: *Decoder, depth: usize, output: ?*std.json.Stringify) anyerror!bool {
        if (depth > 64) return error.InvalidSqlSpill;
        switch (try self.byte()) {
            0 => {
                if (output) |writer_| try writer_.write(null);
                return true;
            },
            1 => {
                const byte_ = try self.byte();
                if (byte_ > 1) return error.InvalidSqlSpill;
                if (output) |writer_| try writer_.write(byte_ == 1);
                return true;
            },
            2 => {
                if (self.eager_nontext) return error.ScalarJsonRequired;
                const value: i64 = @bitCast(try self.word());
                if (output) |writer_| try writer_.write(value);
                return false;
            },
            3 => {
                if (self.eager_nontext) return error.ScalarJsonRequired;
                const value: f64 = @bitCast(try self.word());
                if (output) |writer_| try writer_.write(value);
                return false;
            },
            4, 5 => |tag_| {
                const bytes = try self.take(try self.count());
                if (output) |writer_| {
                    if (tag_ == 4) try writer_.write(std.json.Value{ .number_string = bytes }) else try writer_.write(bytes);
                }
                if (output != null) return true;
                const lossless = if (tag_ == 5) std.unicode.utf8ValidateSlice(bytes) else jsonNumber(bytes);
                if (self.eager_nontext and !lossless) return error.ScalarJsonRequired;
                return lossless;
            },
            6, 7 => |tag_| {
                const object = tag_ == 7;
                const count_ = try self.count();
                const names = if (object and output == null) &self.json_scratch.?.names[depth] else null;
                if (names) |map| map.clearRetainingCapacity();
                var lossless = true;
                if (output) |writer_| {
                    if (object) try writer_.beginObject() else try writer_.beginArray();
                }
                for (0..count_) |_| {
                    if (object) {
                        const key = try self.take(try self.count());
                        // Emission consumes an already validated immutable range.
                        if (output == null) {
                            const entry = try names.?.getOrPut(self.json_scratch.?.a, key);
                            if (entry.found_existing) return error.InvalidSqlSpill;
                        }
                        if (output == null) {
                            const valid = std.unicode.utf8ValidateSlice(key);
                            if (self.eager_nontext and !valid) return error.ScalarJsonRequired;
                            lossless = valid and lossless;
                        }
                        if (output) |writer_| try writer_.objectField(key);
                    }
                    const child = try self.walkJson(depth + 1, output);
                    lossless = child and lossless;
                }
                if (output) |writer_| {
                    if (object) try writer_.endObject() else try writer_.endArray();
                }
                return lossless;
            },
            else => return error.InvalidSqlSpill,
        }
    }
    fn json(self: *Decoder, depth: usize) anyerror!std.json.Value {
        if (depth > 64) return error.InvalidSqlSpill;
        return switch (try self.byte()) {
            0 => .null,
            1 => blk: {
                const v = try self.byte();
                if (v > 1) return error.InvalidSqlSpill;
                break :blk .{ .bool = v == 1 };
            },
            2 => .{ .integer = @bitCast(try self.word()) },
            3 => .{ .float = @bitCast(try self.word()) },
            4 => .{ .number_string = try self.text() },
            5 => .{ .string = try self.text() },
            6 => blk: {
                const items = try self.a.alloc(std.json.Value, try self.count());
                for (items) |*item| item.* = try self.json(depth + 1);
                break :blk .{ .array = std.array_list.Managed(std.json.Value).fromOwnedSlice(self.a, items) };
            },
            7 => blk: {
                const len = try self.count();
                var object: std.json.ObjectMap = .empty;
                for (0..len) |_| {
                    const key = try self.text();
                    if (!self.validated_json and object.contains(key)) return error.InvalidSqlSpill;
                    try object.put(self.a, key, try self.json(depth + 1));
                }
                break :blk .{ .object = object };
            },
            else => error.InvalidSqlSpill,
        };
    }
};

/// Versioned native column block shared by immutable artifacts and local spill
/// vectors. V1 is frozen: wire tag/layout changes require a new encoder version
/// and a decoder that retains V1 support. Pattern callbacks are never portable.
pub const ColumnarBlock = struct {
    values: []const Sequential.EncodedColumn,
    keys: []const Sequential.EncodedColumn,
    ordinals: []const u64,
    json_owner: ?*JsonOwner = null,
    /// Lazy cache payloads have a stable allocator independent of the page
    /// descriptor's address. Retire them before the enclosing metadata arena.
    pub fn deinit(self: ColumnarBlock) void {
        if (self.json_owner) |owner| owner.deinit();
    }
    pub fn isSqlNull(self: ColumnarBlock, row: usize, column: usize) !bool {
        if (row >= self.count() or column >= self.values.len) return error.InvalidSqlSpill;
        const stored = self.values[column];
        if (stored.values == .shared) {
            const shared = stored.values.shared;
            if (shared.bodies[row]) |body| return body.encoded.?.isSqlNull(0, 0);
            return shared.base.isSqlNull(row);
        }
        return stored.isSqlNull(row);
    }
    pub fn count(self: ColumnarBlock) usize {
        return self.ordinals.len;
    }
    pub fn cell(self: ColumnarBlock, row: usize, column: usize) !Datum {
        if (row >= self.count() or column >= self.values.len) return error.InvalidSqlSpill;
        return self.values[column].cell(row);
    }
    pub fn frozenJson(self: ColumnarBlock, row: usize, column: usize, keys: bool) !?@import("execution_batch.zig").FrozenJson {
        const columns = if (keys) self.keys else self.values;
        if (row >= self.count() or column >= columns.len) return error.InvalidSqlSpill;
        const stored = columns[column];
        return stored.frozen(row);
    }
    pub fn encodedJson(self: ColumnarBlock, row: usize, column: usize, keys: bool) !?[]const u8 {
        const columns = if (keys) self.keys else self.values;
        if (row >= self.count() or column >= columns.len) return error.InvalidSqlSpill;
        return columns[column].encodedJson(row);
    }
    /// Dictionary identities are local to this immutable block, including two
    /// distinct identities for SQL NULL and JSON null.
    pub fn dictionaryIdentity(self: ColumnarBlock, row: usize, column: usize, keys: bool) !?u64 {
        const columns = if (keys) self.keys else self.values;
        if (row >= self.count() or column >= columns.len) return error.InvalidSqlSpill;
        const stored = columns[column];
        if (stored.values != .dictionary) return null;
        const flag = (stored.flags[row / 4] >> @as(u3, @intCast(row % 4 * 2))) & 3;
        return if (flag != 0) flag - 1 else @as(u64, stored.values.dictionary.indices[row]) + 2;
    }
    pub fn dictionaryColumn(self: ColumnarBlock, a: Allocator, column: usize, keys: bool) !?@import("execution_batch.zig").Batch {
        const columns = if (keys) self.keys else self.values;
        if (column >= columns.len) return error.InvalidSqlSpill;
        const stored = columns[column];
        if (stored.values != .dictionary) return null;
        const entries = stored.values.dictionary;
        const values = try a.alloc(Datum, entries.size + 2);
        errdefer a.free(values);
        values[0] = .{};
        values[1] = Datum.json(.null);
        for (values[2..], 0..) |*value, index| value.* = Datum.json(entries.entry(index));
        const indices = try a.alloc(u32, self.count());
        for (indices, 0..) |*id, row| id.* = @intCast((try self.dictionaryIdentity(row, column, keys)).?);
        return .{ .dictionary = .{ .values = values, .indices = indices } };
    }
    pub fn keyCell(self: ColumnarBlock, row: usize, column: usize) !Datum {
        if (row >= self.count() or column >= self.keys.len) return error.InvalidSqlSpill;
        return self.keys[column].cell(row);
    }
};
const columnar_v1_magic = "NCB\x01";
pub const ColumnarVersion = enum { v1, v2 };
const columnar_v2_magic = "NCB\x02";
pub fn encodeColumnarBlockAlloc(a: Allocator, rows: []const Row, max_bytes: usize) ![]u8 {
    return encodeColumnarBlockVersionAlloc(a, rows, max_bytes, .v1);
}
pub fn encodeColumnarBlockVersionAlloc(a: Allocator, rows: []const Row, max_bytes: usize, version: ColumnarVersion) ![]u8 {
    if (rows.len == 0 or rows.len > 256 or rows[0].values.len > 1024 or rows[0].keys.len > 256) return error.InvalidSqlSpill;
    for (rows) |row| {
        if (row.values.len != rows[0].values.len or row.keys.len != rows[0].keys.len) return error.InvalidSqlSpill;
        for (row.values) |value| if (value.patterns != null) return error.InvalidSqlSpill;
        for (row.keys) |value| if (value.patterns != null) return error.InvalidSqlSpill;
    }
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(a);
    var encoder: Encoder = .{ .compact_arrays = version == .v2, .a = a, .bytes = &bytes, .limit = max_bytes };
    try encoder.append(if (version == .v1) columnar_v1_magic else columnar_v2_magic);
    try encoder.word(rows.len);
    try encoder.word(rows[0].values.len);
    try encoder.word(rows[0].keys.len);
    for (rows) |row| try encoder.word(row.ordinal);
    try Sequential.encodeColumns(&encoder, rows, false);
    try Sequential.encodeColumns(&encoder, rows, true);
    return bytes.toOwnedSlice(a);
}
/// Encode a borrowed batch one bounded column at a time. The encoded bytes
/// are identical to the row API; neither payloads nor a row matrix are staged.
pub fn encodeColumnarBatchVersionAlloc(a: Allocator, values: @import("execution_batch.zig").Batch, ordinals: []const u64, max_bytes: usize, version: ColumnarVersion) ![]u8 {
    const work = @import("memory_budget.zig").reclaimingAllocator(a);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(work);
    try appendColumnarBatchVersion(work, work, &bytes, values, ordinals, max_bytes, version);
    return if (work.ptr == a.ptr and work.vtable == a.vtable) bytes.toOwnedSlice(a) else a.dupe(u8, bytes.items);
}
/// Append directly to a caller-owned response, retaining frozen block bytes.
/// Output capacity belongs to output_alloc; scratch never enters that owner.
/// On failure the initialized output prefix is restored; capacity may grow.
pub fn appendColumnarBatchVersion(output_alloc: Allocator, scratch: Allocator, bytes: *std.ArrayList(u8), values: @import("execution_batch.zig").Batch, ordinals: []const u64, max_bytes: usize, version: ColumnarVersion) !void {
    return appendColumnarBatchVersionMode(output_alloc, scratch, bytes, values, ordinals, max_bytes, version, null);
}
// Canonical v2 singleton framing, shared by body emission and its streaming
// equality probe. No payload bytes or producer pointers are retained here.
fn singletonFrozenPrefix() [columnar_v2_magic.len + 42]u8 {
    var prefix: [columnar_v2_magic.len + 42]u8 = @splat(0);
    @memcpy(prefix[0..columnar_v2_magic.len], columnar_v2_magic);
    std.mem.writeInt(u64, prefix[columnar_v2_magic.len..][0..8], 1, .little);
    std.mem.writeInt(u64, prefix[columnar_v2_magic.len + 8 ..][0..8], 1, .little);
    prefix[columnar_v2_magic.len + 32] = 255;
    std.mem.writeInt(u64, prefix[columnar_v2_magic.len + 33 ..][0..8], 1, .little);
    return prefix;
}
fn appendColumnarBatchVersionMode(output_alloc: Allocator, scratch: Allocator, bytes: *std.ArrayList(u8), values: @import("execution_batch.zig").Batch, ordinals: []const u64, max_bytes: usize, version: ColumnarVersion, manager: ?*Manager) !void {
    if (values.len() == 0 or values.len() > 256 or values.width() > 1024 or ordinals.len != values.len()) return error.InvalidSqlSpill;
    const start = bytes.items.len;
    errdefer bytes.shrinkRetainingCapacity(start);
    var encoder: Encoder = .{ .manager = manager, .compact_arrays = version == .v2, .a = output_alloc, .scratch = @import("memory_budget.zig").reclaimingAllocator(scratch), .bytes = bytes, .base = start, .limit = max_bytes };
    try encoder.append(if (version == .v1) columnar_v1_magic else columnar_v2_magic);
    try encoder.word(values.len());
    try encoder.word(values.width());
    try encoder.word(0);
    for (ordinals) |ordinal| try encoder.word(ordinal);
    var physical_arena = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(scratch));
    defer physical_arena.deinit();
    const pa = physical_arena.allocator();
    const physical = try pa.alloc(?@import("execution_batch.zig").PhysicalColumn, values.width());
    for (physical, 0..) |*view, column| view.* = try values.physicalColumn(pa, column);
    try Sequential.encodeBatchColumns(&encoder, values, physical, 0, values.len());
}
/// Payload bytes and decoded metadata borrow the caller's page arena. Fetch
/// authenticated artifact bytes into that same arena before decoding, so no
/// payload copy or per-row cell array is needed. The caller resets the arena
/// only after all lanes drain, including on malformed input/allocation failure.
pub fn decodeColumnarBlockInArena(a: Allocator, bytes: []const u8, max_bytes: usize) !ColumnarBlock {
    return decodeColumnarBlockMode(a, null, bytes, max_bytes, .scalar);
}
/// cache_backing must outlive the block. Temporary decode budgets/region
/// descriptors are never retained. The caller retires block.deinit() before
/// freeing the borrowed wire and metadata arena, including failure unwinds.
pub fn decodeColumnarBlockWithCache(a: Allocator, cache_backing: Allocator, bytes: []const u8, max_bytes: usize) !ColumnarBlock {
    return decodeColumnarBlockMode(a, cache_backing, bytes, max_bytes, .scalar);
}
/// Decode for spill/RPC/native column consumers. Scalar access stays available,
/// but no compound tree is constructed until explicitly requested, even when
/// its native numeric tags cannot advertise lossless canonical text.
pub fn decodeColumnarBlockForForwarding(a: Allocator, cache_backing: Allocator, bytes: []const u8, max_bytes: usize) !ColumnarBlock {
    return decodeColumnarBlockMode(a, cache_backing, bytes, max_bytes, .forwarding);
}
fn decodeColumnarBlockMode(a: Allocator, cache_backing: ?Allocator, bytes: []const u8, max_bytes: usize, consumption: JsonConsumption) !ColumnarBlock {
    return decodeColumnarBlockManaged(a, cache_backing, bytes, max_bytes, consumption, null);
}
fn decodeColumnarBlockManaged(a: Allocator, cache_backing: ?Allocator, bytes: []const u8, max_bytes: usize, consumption: JsonConsumption, manager: ?*Manager) !ColumnarBlock {
    if (bytes.len > max_bytes or bytes.len < columnar_v1_magic.len) return error.InvalidSqlSpill;
    const compact = std.mem.startsWith(u8, bytes, columnar_v2_magic);
    if (!compact and !std.mem.startsWith(u8, bytes, columnar_v1_magic)) return error.InvalidSqlSpill;
    var json_scratch: JsonScratch = .{ .a = @import("memory_budget.zig").reclaimingAllocator(a) };
    defer json_scratch.deinit();
    var decoder: Decoder = .{ .manager = manager, .json_consumption = consumption, .json_scratch = &json_scratch, .lazy_json = cache_backing != null, .cache_backing = cache_backing, .borrow_compact = true, .compact_arrays = compact, .a = a, .bytes = bytes, .position = columnar_v1_magic.len };
    errdefer if (decoder.json_owner) |owner| owner.deinit();
    const count = try decoder.count();
    const width = try decoder.count();
    const key_width = try decoder.count();
    if (count == 0 or count > 256 or width > 1024 or key_width > 256) return error.InvalidSqlSpill;
    const ordinals = try a.alloc(u64, count);
    for (ordinals) |*ordinal| ordinal.* = try decoder.word();
    const values = try a.alloc(Sequential.EncodedColumn, width);
    const keys = try a.alloc(Sequential.EncodedColumn, key_width);
    for (values) |*column| column.* = try Sequential.EncodedColumn.decode(&decoder, count);
    for (keys) |*column| column.* = try Sequential.EncodedColumn.decode(&decoder, count);
    if (decoder.position != bytes.len) return error.InvalidSqlSpill;
    return .{ .values = values, .keys = keys, .ordinals = ordinals, .json_owner = decoder.json_owner };
}

/// A synchronous allocation scope that gives optional cache entries back before
/// forwarding actual growth to the statement allocator. No failed-allocation
/// retry and no callback under the parent budget/manager mutex. This descriptor
/// must never escape: retained arenas switch back to manager.allocator().
pub const CachePressure = struct {
    manager: *Manager,
    context: *anyopaque,
    evict: *const fn (*anyopaque) bool,
    backing: ?Allocator = null,
    bounded: bool = false,
    denied: bool = false,
    reserve: usize = 0,
    fn admit(self: *CachePressure, growth: usize) bool {
        self.denied = false;
        const required = growth +| self.reserve;
        self.prepare(required);
        if (self.bounded) if (@import("memory_budget.zig").headroom(self.manager.alloc)) |available| if (required > available) {
            self.denied = true;
            return false;
        };
        return true;
    }
    /// The optional payload owner may be published during the operation.
    pub fn forPayloads(manager: *Manager, payloads: *?*JoinPayloads) CachePressure {
        return .{ .manager = manager, .context = @ptrCast(payloads), .evict = evictPayloads };
    }
    fn evictPayloads(raw: *anyopaque) bool {
        const owner: *?*JoinPayloads = @ptrCast(@alignCast(raw));
        return if (owner.*) |payloads| payloads.evictOne() else false;
    }
    pub fn prepare(self: *CachePressure, growth: usize) void {
        while (@import("memory_budget.zig").headroom(self.manager.alloc)) |available| {
            if (growth <= available or !self.evict(self.context)) break;
        }
    }
    pub fn allocator(self: *CachePressure) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
    }
    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *CachePressure = @ptrCast(@alignCast(raw));
        if (!self.admit(len)) return null;
        return (self.backing orelse self.manager.allocator()).rawAlloc(len, alignment, ra);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *CachePressure = @ptrCast(@alignCast(raw));
        if (!self.admit(len -| bytes.len)) return false;
        return (self.backing orelse self.manager.allocator()).rawResize(bytes, alignment, len, ra);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *CachePressure = @ptrCast(@alignCast(raw));
        if (!self.admit(len -| bytes.len)) return null;
        return (self.backing orelse self.manager.allocator()).rawRemap(bytes, alignment, len, ra);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *CachePressure = @ptrCast(@alignCast(raw));
        (self.backing orelse self.manager.allocator()).rawFree(bytes, alignment, ra);
    }
};

/// Random-access native payloads for hash chains. The chain's separate small
/// key record owns the link/matched flag; failed key comparisons never read
/// this file. Offsets are physical, so reverse chains do not replay prefixes.
pub const JoinPayloads = struct {
    // Exact, canonical page bytes own their keys. Cache metadata and bytes are
    // bounded independently of relation cardinality, with no borrowed keys.
    const Cached = struct {
        bytes: []u8,
        offset: u64,
        hash: u64,
        // Scalar descriptors own no producer pointers: strings borrow bytes.
        scalar: []Datum = &.{},
        scalar_hash: u64 = 0,
    };
    const DiskCached = struct { hash: u64, length: usize, offset: u64, used: u64 };
    const CacheKind = enum { payload, body };
    const BodyKey = struct { bytes: []const u8, kind: u8, hash: u64 };
    const IndexCache = struct {
        entries: [32]struct { offset: u64 = none, header: [24]u8 = undefined } = @splat(.{}),
        fn slot(offset: u64) usize {
            return @intCast(std.hash.Wyhash.hash(0, std.mem.asBytes(&offset)) % 32);
        }
    };
    pub const page_bytes = 16 * 1024;
    pub const page_rows = 64;
    file: File,
    recent: [4]?Cached = @splat(null),
    next_recent: usize = 0,
    disk_recent: [16]?DiskCached = @splat(null),
    // Distinct parent manifests must not displace reusable canonical bodies.
    body_recent: [16]?DiskCached = @splat(null),
    body_root: u64 = none,
    index_cache: ?*IndexCache = null,
    disk_clock: u64 = 0,
    resolver: ?struct { ptr: *anyopaque, read: *const fn (*anyopaque, u64) anyerror!*Sequential.OwnedBlock } = null,
    body_only: bool = false,
    encoded_pages: usize = 0,
    const references_magic = "AFPR0001";
    const Binding = struct { row: usize, column: usize, offset: u64 };
    // Borrowed keys expire with one immutable producer cohort. The fixed ring
    // bounds scratch independently of row width and relation cardinality.
    pub const BodyReuse = struct {
        fingerprints: NativeFingerprint = .{},
        bodies: [64]?struct { bytes: []const u8, hash: u64, kind: u8, offset: u64 } = @splat(null),
        next: usize = 0,
        fn find(self: *BodyReuse, bytes: []const u8, kind: u8, hash: u64) ?u64 {
            for (self.bodies) |entry| if (entry) |body| {
                if (body.kind == kind and body.hash == hash and sameNativeBytes(body.bytes, bytes)) return body.offset;
            };
            return null;
        }
        fn put(self: *BodyReuse, bytes: []const u8, kind: u8, hash: u64, offset: u64) void {
            self.bodies[self.next] = .{ .bytes = bytes, .kind = kind, .hash = hash, .offset = offset };
            self.next = (self.next + 1) % self.bodies.len;
        }
    };
    fn lookupNativeBody(self: *JoinPayloads, key: BodyKey) !?u64 {
        try self.file.manager.check();
        var pressure: CachePressure = .{ .manager = self.file.manager, .context = self, .evict = evict };
        self.disk_clock +|= 1;
        // Empty slots imply a miss without any file read or index allocation.
        for (&self.body_recent) |*entry| if (entry.*) |*cached| {
            if (cached.hash != key.hash or cached.length != key.bytes.len) continue;
            if (self.file.read_buffer.len == 0) pressure.prepare(@max(1, self.file.buffer_bytes orelse self.file.manager.buffer_bytes));
            if (try self.sameNativeStored(cached.offset, key)) {
                cached.used = self.disk_clock;
                return cached.offset;
            }
        };
        var ready = self.body_root != none;
        if (!ready) {
            ready = true;
            for (self.body_recent) |entry| if (entry == null) {
                ready = false;
                break;
            };
        }
        if (!ready) return null;
        const buffer_bytes = @max(1, self.file.buffer_bytes orelse self.file.manager.buffer_bytes);
        pressure.prepare((if (self.file.read_buffer.len == 0) buffer_bytes else 0) + (if (self.file.write_buffer.len == 0) buffer_bytes else 0));
        try self.activateBodyIndex();
        try self.prepareIndexCache();
        return self.findBodyMatch(key.hash, key.bytes.len, .{ .native = key });
    }
    fn sameNativeStored(self: *JoinPayloads, offset: u64, key: BodyKey) !bool {
        // Native frame tags 2/3 extend the ordinary frame with a nine-byte
        // key descriptor. The physical body offset stays at the frame start.
        var frame: [26]u8 = undefined;
        try self.file.readRaw(offset, &frame);
        const header = frame[0..17];
        const framed_length = std.mem.readInt(u64, header[0..8], .little);
        if (header[16] < 2 or header[16] > 3 or framed_length < 9 or framed_length - 9 > self.file.manager.max_record_bytes or framed_length > self.file.size -| (offset + header.len)) return error.InvalidSqlSpill;
        const length = framed_length - 9;
        // The descriptor's high bit selects a text-key trailer containing an
        // eight-byte stored length and a Snappy block. Low bits retain the
        // exact native representation; raw keys keep their original layout.
        const compressed_key = frame[17] & 128 != 0;
        const kind = frame[17] & 127;
        if (kind < 1 or kind > 3 or (compressed_key and kind != 1)) return error.InvalidSqlSpill;
        if (kind != key.kind or std.mem.readInt(u64, frame[18..26], .little) != key.bytes.len) return false;
        if (key.kind == 1) {
            const key_offset = offset + header.len + framed_length;
            if (!compressed_key) return self.sameParts(key_offset, &.{key.bytes});
            var size: [8]u8 = undefined;
            try self.file.readRaw(key_offset, &size);
            const stored_length = std.mem.readInt(u64, &size, .little);
            if (stored_length > self.file.size -| (key_offset + size.len)) return error.InvalidSqlSpill;
            return self.sameCompressedParts(key_offset + size.len, stored_length, &.{key.bytes});
        }
        // This is the singleton mixed-column framing used by the shared v2
        // codec, followed by the exact borrowed frozen token stream.
        const prefix = singletonFrozenPrefix();
        if (header[16] == 2) {
            if (length != prefix.len + key.bytes.len) return false;
            return self.sameParts(offset + frame.len, &.{ &prefix, key.bytes });
        }
        return self.sameCompressedParts(offset + frame.len, length, &.{ &prefix, key.bytes });
    }
    fn sameCompressedParts(self: *JoinPayloads, offset: u64, length: u64, parts: []const []const u8) !bool {
        const Input = struct {
            file: *File,
            offset: u64,
            length: u64,
            pub fn readInto(self_: @This(), at: u64, out: []u8) !void {
                if (at > self_.length or out.len > self_.length - at) return error.InvalidSqlSpill;
                try self_.file.manager.check();
                try self_.file.readRaw(self_.offset + at, out);
            }
        };
        return snappy.equalsFromReader(Input{ .file = &self.file, .offset = offset, .length = length }, parts) catch |err| switch (err) {
            error.CorruptInput => error.InvalidSqlSpill,
            else => err,
        };
    }
    fn sameParts(self: *JoinPayloads, offset: u64, parts: []const []const u8) !bool {
        var buffer: [4096]u8 = undefined;
        var at = offset;
        for (parts) |part| {
            var done: usize = 0;
            while (done < part.len) {
                try self.file.manager.check();
                const count = @min(buffer.len, part.len - done);
                try self.file.readRaw(at, buffer[0..count]);
                if (!std.mem.eql(u8, buffer[0..count], part[done..][0..count])) return false;
                at += count;
                done += count;
            }
        }
        return true;
    }
    fn sameStored(self: *JoinPayloads, offset: u64, stored: []const u8, compressed: bool) !bool {
        var header: [17]u8 = undefined;
        try self.file.readRaw(offset, &header);
        if (header[16] != @intFromBool(compressed) or std.mem.readInt(u64, header[0..8], .little) != stored.len) return false;
        var buffer: [4096]u8 = undefined;
        var at: usize = 0;
        while (at < stored.len) {
            try self.file.manager.check();
            const count = @min(buffer.len, stored.len - at);
            try self.file.readRaw(offset + header.len + at, buffer[0..count]);
            if (!std.mem.eql(u8, buffer[0..count], stored[at..][0..count])) return false;
            at += count;
        }
        return true;
    }

    // A crit-bit tree keeps only its root resident. Branches (24 bytes) and
    // collision leaves (40 bytes) share the payload file and statement quota.
    // Hash bits strictly increase on a path, bounding traversal at 64 branches;
    // equal hashes use a backwards chain and still require exact stored bytes.
    const IndexStep = struct { link: u64, child: u64, bit: u8 };
    const IndexPath = struct { steps: [64]IndexStep = undefined, len: usize = 0 };
    const IndexLeaf = struct { offset: u64, hash: u64, length: u64, body: u64, next: u64 };
    fn prepareIndexCache(self: *JoinPayloads) !void {
        if (self.index_cache != null or self.body_root == none) return;
        // Hot branches are optional; leave headroom for required writer work.
        const available = @import("memory_budget.zig").headroom(self.file.manager.alloc) orelse return;
        if (available < 8 * @sizeOf(IndexCache)) return;
        const cache = try self.file.manager.allocator().create(IndexCache);
        cache.* = .{};
        self.index_cache = cache;
    }
    fn indexHeader(self: *JoinPayloads, offset: u64) ![24]u8 {
        if (self.index_cache) |cache| {
            const entry = cache.entries[IndexCache.slot(offset)];
            if (entry.offset == offset) return entry.header;
        }
        var header: [24]u8 = undefined;
        try self.file.readRaw(offset, &header);
        // Cache branches only; one-use leaves should not evict hot ancestors.
        if (std.mem.readInt(u64, header[0..8], .little) != 0) if (self.index_cache) |cache| {
            cache.entries[IndexCache.slot(offset)] = .{ .offset = offset, .header = header };
        };
        return header;
    }
    fn bodyLeaf(self: *JoinPayloads, hash: u64, path: ?*IndexPath) !?IndexLeaf {
        var at = self.body_root;
        var prior: ?u8 = null;
        var depth: usize = 0;
        while (at != none) {
            const header = try self.indexHeader(at);
            const tag = std.mem.readInt(u64, header[0..8], .little);
            if (tag == 0) {
                var tail: [16]u8 = undefined;
                try self.file.readRaw(at + header.len, &tail);
                return .{ .offset = at, .hash = std.mem.readInt(u64, header[8..16], .little), .length = std.mem.readInt(u64, header[16..24], .little), .body = std.mem.readInt(u64, tail[0..8], .little), .next = std.mem.readInt(u64, tail[8..16], .little) };
            }
            if (tag > 64 or depth == 64) return error.InvalidSqlSpill;
            const bit: u8 = @intCast(tag - 1);
            if (prior) |p| if (bit <= p) return error.InvalidSqlSpill;
            const right = hash & (@as(u64, 1) << @as(u6, @intCast(63 - bit))) != 0;
            const child = std.mem.readInt(u64, header[if (right) 16 else 8..][0..8], .little);
            if (path) |items| {
                items.steps[depth] = .{ .link = at + @as(u64, if (right) 16 else 8), .child = child, .bit = bit };
                items.len = depth + 1;
            }
            depth += 1;
            prior = bit;
            at = child;
        }
        return null;
    }
    fn activateBodyIndex(self: *JoinPayloads) !void {
        if (self.body_root != none) return;
        for (self.body_recent) |entry| if (entry == null) return;
        errdefer self.body_root = none;
        for (self.body_recent) |entry| {
            const cached = entry.?;
            try self.indexBody(cached.hash, cached.length, cached.offset);
        }
    }
    fn findBody(self: *JoinPayloads, hash: u64, length: usize, stored: []const u8, compressed: bool) !?u64 {
        return self.findBodyMatch(hash, length, .{ .stored = .{ .bytes = stored, .compressed = compressed } });
    }
    const BodyMatch = union(enum) { native: BodyKey, stored: struct { bytes: []const u8, compressed: bool } };
    fn findBodyMatch(self: *JoinPayloads, hash: u64, length: usize, match: BodyMatch) !?u64 {
        var leaf = (try self.bodyLeaf(hash, null)) orelse return null;
        while (true) {
            if (leaf.hash != hash) return null;
            if (leaf.length == length and try switch (match) {
                .native => |key| self.sameNativeStored(leaf.body, key),
                .stored => |wire| self.sameStored(leaf.body, wire.bytes, wire.compressed),
            }) return leaf.body;
            if (leaf.next == none) return null;
            if (leaf.next >= leaf.offset) return error.InvalidSqlSpill;
            var bytes: [40]u8 = undefined;
            try self.file.readRaw(leaf.next, &bytes);
            if (std.mem.readInt(u64, bytes[0..8], .little) != 0) return error.InvalidSqlSpill;
            leaf = .{ .offset = leaf.next, .hash = std.mem.readInt(u64, bytes[8..16], .little), .length = std.mem.readInt(u64, bytes[16..24], .little), .body = std.mem.readInt(u64, bytes[24..32], .little), .next = std.mem.readInt(u64, bytes[32..40], .little) };
        }
    }
    fn indexBody(self: *JoinPayloads, hash: u64, length: usize, body: u64) !void {
        var path: IndexPath = .{};
        const prior = try self.bodyLeaf(hash, &path);
        var leaf: [40]u8 = @splat(0);
        std.mem.writeInt(u64, leaf[8..16], hash, .little);
        std.mem.writeInt(u64, leaf[16..24], length, .little);
        std.mem.writeInt(u64, leaf[24..32], body, .little);
        std.mem.writeInt(u64, leaf[32..40], if (prior != null and prior.?.hash == hash) prior.?.offset else none, .little);
        const added = self.file.size;
        try self.file.writeRaw(added, &leaf);
        var published = added;
        var parent: ?u64 = null;
        if (prior) |existing| {
            if (existing.hash == hash) {
                if (path.len != 0) parent = path.steps[path.len - 1].link;
            } else {
                const bit: u8 = @intCast(@clz(hash ^ existing.hash));
                var subtree = self.body_root;
                for (path.steps[0..path.len]) |step| {
                    if (step.bit >= bit) break;
                    parent = step.link;
                    subtree = step.child;
                }
                const right = hash & (@as(u64, 1) << @as(u6, @intCast(63 - bit))) != 0;
                var branch: [24]u8 = undefined;
                std.mem.writeInt(u64, branch[0..8], @as(u64, bit) + 1, .little);
                std.mem.writeInt(u64, branch[8..16], if (right) subtree else added, .little);
                std.mem.writeInt(u64, branch[16..24], if (right) added else subtree, .little);
                published = self.file.size;
                try self.file.writeRaw(published, &branch);
            }
        }
        // Publish only complete nodes. Allocation/quota failures before this
        // point leave the old root/path readable; all orphan bytes stay charged.
        if (parent) |link| {
            var pointer: [8]u8 = undefined;
            std.mem.writeInt(u64, &pointer, published, .little);
            try self.file.writeRaw(link, &pointer);
            // A branch's child pointer changed. Update cached copies only after
            // successful publication; no borrowed node address escapes a read.
            if (self.index_cache) |cache| for (&cache.entries) |*entry| {
                if (entry.offset == none) continue;
                if (entry.offset + 8 == link or entry.offset + 16 == link) {
                    const at: usize = @intCast(link - entry.offset);
                    @memcpy(entry.header[at..][0..8], &pointer);
                }
            };
        } else self.body_root = published;
    }

    pub fn init(manager: *Manager) !JoinPayloads {
        var file = try manager.create();
        file.buffer_bytes = @min(4096, manager.buffer_bytes);
        return .{ .file = file };
    }
    pub fn close(self: *JoinPayloads) void {
        for (&self.recent) |*entry| self.drop(entry);
        if (self.index_cache) |cache| self.file.manager.allocator().destroy(cache);
        self.index_cache = null;
        self.file.close();
    }
    fn drop(self: *JoinPayloads, entry: *?Cached) void {
        if (entry.*) |cached| {
            self.file.manager.allocator().free(cached.scalar);
            self.file.manager.allocator().free(cached.bytes);
        }
        entry.* = null;
    }
    fn evict(raw: *anyopaque) bool {
        const self: *JoinPayloads = @ptrCast(@alignCast(raw));
        return self.evictOne();
    }
    pub fn evictOne(self: *JoinPayloads) bool {
        for (0..self.recent.len) |step| {
            const entry = &self.recent[(self.next_recent + step) % self.recent.len];
            if (entry.* != null) {
                self.drop(entry);
                return true;
            }
        }
        if (self.index_cache) |cache| {
            self.file.manager.allocator().destroy(cache);
            self.index_cache = null;
            return true;
        }
        return false;
    }
    // Only direct scalar rows opt in. Encoded JSON/arrays/patterns retain the
    // manager-aware codec and its validation. Hashing never grants equality:
    // compare exact types, NULL flags and float bits before reusing an offset.
    fn scalarHash(values: []const Datum) ?u64 {
        if (values.len == 0 or values.len > 16) return null;
        var hash = std.hash.Wyhash.init(values.len);
        for (values) |value| {
            if (Sequential.tag(value) == 255) return null;
            const tag: u8 = if (value.sql_null) 128 else Sequential.tag(value);
            hash.update(&.{tag});
            if (tag != 0 and tag != 128) {
                const physical = Sequential.physicalHash(value);
                hash.update(std.mem.asBytes(&physical));
            }
        }
        return hash.final();
    }
    fn scalarEqual(a: []const Datum, b: []const Datum) bool {
        if (a.len != b.len) return false;
        for (a, b) |x, y| {
            if (x.sql_null != y.sql_null) return false;
            if (x.sql_null) continue;
            if (x.value == .null and y.value == .null) continue;
            if (!Sequential.physicalEqual(x, y)) return false;
        }
        return true;
    }
    /// Ordinals belong to the separate key chain. Canonical zero ordinals let
    /// identical payload pages share storage even across producer leases.
    pub fn appendBatch(self: *JoinPayloads, values: @import("execution_batch.zig").Batch) !u64 {
        var reuse: BodyReuse = .{};
        return self.appendBatchWithReuse(values, &reuse);
    }
    pub fn appendBatchWithReuse(self: *JoinPayloads, values: @import("execution_batch.zig").Batch, reuse: *BodyReuse) !u64 {
        const Batch = @import("execution_batch.zig").Batch;
        if (values.len() == 0 or values.len() > page_rows or values.width() > 1024) return error.InvalidSqlSpill;
        // A singleton native cell can reuse the same body directly, without a
        // manifest or a separate codec path.
        if (values.len() == 1 and values.width() == 1) {
            const frozen = try values.frozenJson(0, 0);
            const text = if (frozen == null) try values.encodedJson(0, 0) else null;
            const native = if (frozen) |value| value.bytes else text;
            if (native) |bytes| if (bytes.len >= 256) {
                const kind: u8 = if (frozen) |value| 2 + @as(u8, @intFromBool(value.lossless_text)) else 1;
                const hash = reuse.fingerprints.bodyHash(bytes, kind);
                if (reuse.find(bytes, kind, hash)) |offset| return offset;
                const offset = try self.appendNativeBody(values, 0, 0, .{ .bytes = bytes, .kind = kind, .hash = hash });
                reuse.put(bytes, kind, hash, offset);
                return offset;
            };
            return self.appendInlineBatch(values, .payload, null);
        }
        var pressure: CachePressure = .{ .manager = self.file.manager, .context = self, .evict = evict };
        var arena = std.heap.ArenaAllocator.init(pressure.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        var bindings: std.ArrayList(Binding) = .empty;
        for (0..values.width()) |column| for (0..values.len()) |row| {
            try self.file.manager.check();
            const frozen = try values.frozenJson(row, column);
            const text = if (frozen == null) try values.encodedJson(row, column) else null;
            const bytes = if (frozen) |v| v.bytes else text orelse continue;
            if (bytes.len < 256) continue;
            const kind: u8 = if (frozen) |v| 2 + @as(u8, @intFromBool(v.lossless_text)) else 1;
            const hash = reuse.fingerprints.bodyHash(bytes, kind);
            const offset = reuse.find(bytes, kind, hash) orelse blk: {
                const offset = try self.appendNativeBody(values, row, column, .{ .bytes = bytes, .kind = kind, .hash = hash });
                // Publish only after the complete body has been written.
                reuse.put(bytes, kind, hash, offset);
                break :blk offset;
            };
            try bindings.append(a, .{ .row = row, .column = column, .offset = offset });
        };
        if (bindings.items.len == 0) return self.appendInlineBatch(values, .payload, null);
        const View = struct {
            source: *const Batch,
            bindings: []const Binding,
            fn bound(self_: *const @This(), row: usize, column: usize) bool {
                var low: usize = 0;
                var high = self_.bindings.len;
                const key = column * page_rows + row;
                while (low < high) {
                    const mid = low + (high - low) / 2;
                    const item = self_.bindings[mid];
                    const value = item.column * page_rows + item.row;
                    if (value < key) low = mid + 1 else high = mid;
                }
                return low < self_.bindings.len and self_.bindings[low].row == row and self_.bindings[low].column == column;
            }
            fn cell(raw: *anyopaque, alloc: Allocator, row: usize, column: usize) anyerror!Datum {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                return if (self_.bound(row, column)) Datum{} else self_.source.cell(alloc, row, column);
            }
            fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                return if (self_.bound(row, column)) null else self_.source.frozenJson(row, column);
            }
            fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                return if (self_.bound(row, column)) null else self_.source.encodedJson(row, column);
            }
        };
        var view: View = .{ .source = &values, .bindings = bindings.items };
        var base: std.ArrayList(u8) = .empty;
        const ordinals: [page_rows]u64 = @splat(0);
        try appendColumnarBatchVersionMode(a, a, &base, .{ .reader = .{ .ptr = &view, .read = View.cell, .read_frozen = View.frozen, .read_json = View.json, .count = values.len(), .width = values.width() } }, ordinals[0..values.len()], self.file.manager.max_record_bytes, .v2, self.file.manager);
        var output: std.ArrayList(u8) = .empty;
        var encoder: Encoder = .{ .a = a, .bytes = &output, .limit = self.file.manager.max_record_bytes };
        try encoder.append(references_magic);
        try encoder.word(base.items.len);
        try encoder.append(base.items);
        try encoder.word(bindings.items.len);
        for (bindings.items) |binding| {
            try encoder.word(binding.row);
            try encoder.word(binding.column);
            try encoder.word(binding.offset);
        }
        return self.appendWire(a, output.items, &.{}, null, &pressure, .payload, null);
    }
    fn appendNativeBody(self: *JoinPayloads, values: @import("execution_batch.zig").Batch, row: usize, column: usize, key: BodyKey) !u64 {
        const Batch = @import("execution_batch.zig").Batch;
        const One = struct {
            source: *const Batch,
            row: usize,
            column: usize,
            fn cell(raw: *anyopaque, alloc: Allocator, _: usize, _: usize) anyerror!Datum {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                return self_.source.cell(alloc, self_.row, self_.column);
            }
            fn getFrozen(raw: *anyopaque, _: usize, _: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                return self_.source.frozenJson(self_.row, self_.column);
            }
            fn json(raw: *anyopaque, _: usize, _: usize) anyerror!?[]const u8 {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                return self_.source.encodedJson(self_.row, self_.column);
            }
        };
        var one: One = .{ .source = &values, .row = row, .column = column };

        return (try self.lookupNativeBody(key)) orelse try self.appendInlineBatch(.{ .reader = .{ .ptr = &one, .read = One.cell, .read_frozen = One.getFrozen, .read_json = One.json, .count = 1, .width = 1 } }, .body, key);
    }
    /// One immutable producer cohort, with one bounded content memo shared by
    /// packing and every physical append. Count distinct bodies per page so a
    /// repeated body does not force one parent page per otherwise narrow row.
    pub fn appendCohort(self: *JoinPayloads, values: @import("execution_batch.zig").Batch, positions: []const usize, references: []PayloadReader.Reference, reuse: *BodyReuse, target_bytes: usize, ordinary_bytes: ?[]const usize) !void {
        if (positions.len > page_rows or references.len != positions.len or target_bytes == 0) return error.InvalidSqlSpill;
        for (positions) |position| if (position >= values.len()) return error.InvalidSqlSpill;
        if (ordinary_bytes) |sizes| if (sizes.len != positions.len) return error.InvalidSqlSpill;
        var pressure: CachePressure = .{ .manager = self.file.manager, .context = self, .evict = evict };
        var arena = std.heap.ArenaAllocator.init(pressure.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        var cells = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(a));
        defer cells.deinit();
        var begin: usize = 0;
        while (begin < positions.len) {
            try self.file.manager.check();
            var end = begin;
            var bytes: usize = 0;
            var ordinary: usize = 0;
            const target = @min(target_bytes, self.file.manager.max_record_bytes / 4);
            const Body = struct { bytes: []const u8, hash: u64, kind: u8 };
            var bodies: [64]Body = undefined;
            var body_count: usize = 0;
            while (end < positions.len) {
                _ = cells.reset(.free_all);
                var cost: usize = 32;
                if (ordinary_bytes) |sizes| cost +|= sizes[end];
                var new_bodies: usize = 0;
                const before = body_count;
                for (0..values.width()) |column| {
                    const row = positions[end];
                    const frozen = try values.frozenJson(row, column);
                    const text = if (frozen == null) try values.encodedJson(row, column) else null;
                    const native = if (frozen) |v| v.bytes else text;
                    if (native) |body| {
                        if (body.len >= 256) {
                            const kind: u8 = if (frozen) |v| 2 + @as(u8, @intFromBool(v.lossless_text)) else 1;
                            const hash = reuse.fingerprints.bodyHash(body, kind);
                            const repeated = for (bodies[0..body_count]) |prior| {
                                if (prior.kind == kind and prior.hash == hash and std.mem.eql(u8, prior.bytes, body)) break true;
                            } else false;
                            cost +|= 24;
                            if (!repeated) {
                                new_bodies +|= body.len;
                                if (body_count < bodies.len) {
                                    bodies[body_count] = .{ .bytes = body, .hash = hash, .kind = kind };
                                    body_count += 1;
                                }
                            }
                            continue;
                        }
                        cost +|= body.len;
                        continue;
                    }
                    if (ordinary_bytes == null) cost +|= try values.cellBytes(cells.allocator(), row, column);
                }
                if (end != begin and (cost > target -| ordinary or (new_bodies != 0 and cost +| new_bodies > target -| bytes))) {
                    body_count = before;
                    break;
                }
                ordinary +|= cost;
                bytes +|= cost +| new_bodies;
                end += 1;
            }
            const offset = try self.appendBatchWithReuse(try values.select(a, positions[begin..end]), reuse);
            for (references[begin..end], 0..) |*reference, index| reference.* = .{ .offset = offset, .index = index };
            begin = end;
        }
    }

    fn appendInlineBatch(self: *JoinPayloads, values: @import("execution_batch.zig").Batch, cache_kind: CacheKind, body_key: ?BodyKey) !u64 {
        if (values.len() == 0 or values.len() > page_rows) return error.InvalidSqlSpill;
        const manager = self.file.manager;
        try manager.check();
        const scalar_values: []const Datum = if (values == .rows and values.len() == 1) values.rows[0] else &.{};
        const scalar_hash = scalarHash(scalar_values);
        if (scalar_hash) |key| for (self.recent) |entry| {
            if (entry) |cached| if (cached.bytes.len <= manager.max_record_bytes and cached.scalar_hash == key and scalarEqual(cached.scalar, scalar_values)) return cached.offset;
        };
        var pressure: CachePressure = .{ .manager = manager, .context = self, .evict = evict };
        var arena = std.heap.ArenaAllocator.init(pressure.allocator());
        defer arena.deinit();
        var output: std.ArrayList(u8) = .empty;
        const temporary = @import("memory_budget.zig").reclaimingAllocator(arena.allocator());
        defer output.deinit(temporary);
        const ordinals: [page_rows]u64 = @splat(0);
        if (body_key != null and body_key.?.kind >= 2) {
            var encoder: Encoder = .{ .a = temporary, .bytes = &output, .limit = manager.max_record_bytes };
            try encoder.append(&singletonFrozenPrefix());
            try encoder.append(body_key.?.bytes);
        } else try appendColumnarBatchVersionMode(temporary, temporary, &output, values, ordinals[0..values.len()], manager.max_record_bytes, .v2, manager);
        return self.appendWire(arena.allocator(), output.items, scalar_values, scalar_hash, &pressure, cache_kind, body_key);
    }
    fn appendWire(self: *JoinPayloads, a: Allocator, wire: []const u8, scalar_values: []const Datum, scalar_hash: ?u64, pressure: *CachePressure, cache_kind: CacheKind, body_key: ?BodyKey) !u64 {
        const manager = self.file.manager;
        self.encoded_pages += 1;
        const hash = std.hash.Wyhash.hash(0, wire);
        if (body_key == null) for (self.recent) |entry| if (entry) |cached| {
            if (cached.hash == hash and std.mem.eql(u8, cached.bytes, wire)) return cached.offset;
        };
        const work = @import("memory_budget.zig").reclaimingAllocator(a);
        const compressed = if (self.file.manager.compression == .snappy and wire.len >= 1024) try snappy.encode(work, wire) else null;
        defer if (compressed) |bytes| work.free(bytes);
        const use_compressed = compressed != null and compressed.?.len + 32 < wire.len;
        const stored = if (use_compressed) compressed.? else wire;
        self.disk_clock +|= 1;
        const disk_recent = if (cache_kind == .body) &self.body_recent else &self.disk_recent;
        if (body_key == null) for (disk_recent) |*entry| if (entry.*) |*cached| {
            if (cached.hash != hash or cached.length != wire.len) continue;
            if (self.file.read_buffer.len == 0) pressure.prepare(@max(1, self.file.buffer_bytes orelse manager.buffer_bytes));
            if (try self.sameStored(cached.offset, stored, use_compressed)) {
                cached.used = self.disk_clock;
                return cached.offset;
            }
        };
        // Only miss publication compresses the exact source key. Scratch is
        // reclaimed immediately; a match never stages compressed or decoded
        // key-sized output. Incompressible/small keys retain their raw layout.
        const compressed_key = if (body_key != null and body_key.?.kind == 1 and manager.compression == .snappy and body_key.?.bytes.len >= 1024) try snappy.encode(work, body_key.?.bytes) else null;
        defer if (compressed_key) |bytes| work.free(bytes);
        const use_compressed_key = compressed_key != null and compressed_key.?.len + 8 + 32 < body_key.?.bytes.len;
        var header: [17]u8 = undefined;
        const native_bytes: usize = if (body_key != null) 9 else 0;
        header[16] = @as(u8, if (body_key != null) 2 else 0) + @intFromBool(use_compressed);
        std.mem.writeInt(u64, header[0..8], stored.len + native_bytes, .little);
        std.mem.writeInt(u64, header[8..16], hash, .little);
        if (self.file.write_buffer.len == 0) pressure.prepare(@max(1, self.file.buffer_bytes orelse manager.buffer_bytes));
        const offset = self.file.size;
        try self.file.writeRaw(offset, &header);
        if (body_key) |key| {
            var native: [9]u8 = undefined;
            native[0] = key.kind | @as(u8, if (use_compressed_key) 128 else 0);
            std.mem.writeInt(u64, native[1..9], key.bytes.len, .little);
            try self.file.writeRaw(offset + header.len, &native);
        }
        try self.file.writeRaw(offset + header.len + native_bytes, stored);
        if (body_key) |key| {
            // Frozen keys already occur verbatim in the body. Text keys retain
            // exact spelling in quota-charged spill bytes, without a heap map
            // or a borrowed producer address surviving the append.
            if (key.kind == 1) {
                if (use_compressed_key) {
                    var length: [8]u8 = undefined;
                    std.mem.writeInt(u64, &length, compressed_key.?.len, .little);
                    try self.file.writeRaw(self.file.size, &length);
                    try self.file.writeRaw(self.file.size, compressed_key.?);
                } else try self.file.writeRaw(self.file.size, key.bytes);
            }
            if (self.body_root != none) try self.indexBody(key.hash, key.bytes.len, offset);
        }
        self.file.manager.increment("compressed_records", @intFromBool(use_compressed));
        var victim: usize = 0;
        for (disk_recent, 0..) |entry, index| {
            if (entry == null) {
                victim = index;
                break;
            }
            if (entry.?.used < disk_recent[victim].?.used) victim = index;
        }
        disk_recent[victim] = .{ .hash = if (body_key) |key| key.hash else hash, .length = if (body_key) |key| key.bytes.len else wire.len, .offset = offset, .used = self.disk_clock };
        // Cache publication is optional and occurs after required I/O. Evict
        // the replaced entry before admission, and never poison the statement
        // budget by probing a known policy denial with an allocation.
        self.drop(&self.recent[self.next_recent]);
        const key_bytes = if (scalar_hash != null) scalar_values.len * @sizeOf(Datum) else 0;
        const available = @import("memory_budget.zig").headroom(manager.alloc);
        if (wire.len + key_bytes <= page_bytes and (available == null or wire.len + key_bytes <= available.?)) {
            const bytes = try manager.allocator().dupe(u8, wire);
            errdefer manager.allocator().free(bytes);
            const cells = try manager.allocator().alloc(Datum, if (scalar_hash != null) scalar_values.len else 0);
            errdefer manager.allocator().free(cells);
            if (cells.len != 0) {
                // Only primitive, one-row pages reach this decoder. Metadata
                // lives on the stack; retained string slices borrow bytes.
                var scratch: [8192]u8 = undefined;
                var fixed = std.heap.FixedBufferAllocator.init(&scratch);
                const block = try decodeColumnarBlockManaged(fixed.allocator(), null, bytes, manager.max_record_bytes, .forwarding, manager);
                for (cells, 0..) |*cell, col| cell.* = try block.cell(0, col);
            }
            self.recent[self.next_recent] = .{ .bytes = bytes, .offset = offset, .hash = hash, .scalar = cells, .scalar_hash = scalar_hash orelse 0 };
            self.next_recent = (self.next_recent + 1) % self.recent.len;
        }
        return offset;
    }
    pub fn read(self: *JoinPayloads, offset: u64) !*Sequential.OwnedBlock {
        var pressure: CachePressure = .{ .manager = self.file.manager, .context = self, .evict = evict };
        return self.readWithPressure(offset, &pressure);
    }
    pub fn readWithPressure(self: *JoinPayloads, offset: u64, pressure: ?*CachePressure) !*Sequential.OwnedBlock {
        if (pressure) |scope| if (self.file.read_buffer.len == 0) {
            if (!scope.admit(@max(1, self.file.buffer_bytes orelse self.file.manager.buffer_bytes))) return error.OutOfMemory;
        };
        var header: [17]u8 = undefined;
        try self.file.readRaw(offset, &header);
        const framed_length = std.mem.readInt(u64, header[0..8], .little);
        const native_bytes: usize = if (header[16] >= 2) 9 else 0;
        if (header[16] > 3 or framed_length < native_bytes or framed_length - native_bytes > self.file.manager.max_record_bytes or framed_length > self.file.size -| (offset + header.len)) return error.InvalidSqlSpill;
        const length = framed_length - native_bytes;
        const data_offset = offset + header.len + native_bytes;
        const stable = self.file.manager.allocator();
        const a = if (pressure) |scope| scope.allocator() else stable;
        const owner = try a.create(Sequential.OwnedBlock);
        owner.* = .{ .a = stable, .arena = .init(a), .physical_offset = offset };
        errdefer owner.release();
        // Published blocks and lazy JSON owners may outlive the hash join and
        // this stack scope. Frees/growth use the identical stable backing.
        defer owner.arena.child_allocator = stable;
        const wire = if (header[16] & 1 == 1) blk: {
            // Compressed input is decode scratch, not retained page storage.
            const encoded = try a.alloc(u8, @intCast(length));
            defer a.free(encoded);
            try self.file.readRaw(data_offset, encoded);
            const decoded_len = snappy.decodedLen(encoded) catch return error.InvalidSqlSpill;
            if (decoded_len > self.file.manager.max_record_bytes) return error.InvalidSqlSpill;
            break :blk snappy.decode(owner.arena.allocator(), encoded) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidSqlSpill,
            };
        } else blk: {
            const encoded = try owner.arena.allocator().alloc(u8, @intCast(length));
            try self.file.readRaw(data_offset, encoded);
            break :blk encoded;
        };
        if (std.hash.Wyhash.hash(0, wire) != std.mem.readInt(u64, header[8..16], .little)) return error.InvalidSqlSpill;
        if (std.mem.startsWith(u8, wire, references_magic)) {
            if (self.body_only) return error.InvalidSqlSpill;
            var decoder: Decoder = .{ .a = owner.arena.allocator(), .bytes = wire, .position = references_magic.len };
            const base = try decoder.take(try decoder.count());
            owner.encoded = try decodeColumnarBlockManaged(owner.arena.allocator(), stable, base, self.file.manager.max_record_bytes, .forwarding, self.file.manager);
            const count = try decoder.count();
            if (count == 0 or count > owner.width() * owner.count()) return error.InvalidSqlSpill;
            var offsets: std.ArrayList(u64) = .empty;
            var previous: ?usize = null;
            for (0..count) |_| {
                try self.file.manager.check();
                const row = try decoder.word();
                const column = try decoder.word();
                const body_offset = try decoder.word();
                if (row >= owner.count() or column >= owner.width() or body_offset >= offset) return error.InvalidSqlSpill;
                const key = column * page_rows + row;
                if (previous) |prior| if (key <= prior) return error.InvalidSqlSpill;
                previous = @intCast(key);
                const stored = &@constCast(owner.encoded.?.values)[@intCast(column)];
                if (stored.values != .shared) {
                    const shared = try owner.arena.allocator().create(Sequential.EncodedColumn.SharedColumn);
                    const bodies = try owner.arena.allocator().alloc(?*Sequential.OwnedBlock, owner.count());
                    @memset(bodies, null);
                    shared.* = .{ .base = stored.*, .bodies = bodies };
                    stored.* = .{ .values = .{ .shared = shared } };
                }
                if (!stored.values.shared.base.isSqlNull(@intCast(row))) return error.InvalidSqlSpill;
                const body = for (offsets.items, owner.dependencies.items) |prior, body| {
                    if (prior == body_offset) break body;
                } else blk: {
                    const body = if (self.resolver) |resolver| try resolver.read(resolver.ptr, body_offset) else body: {
                        self.body_only = true;
                        defer self.body_only = false;
                        break :body try self.readWithPressure(body_offset, pressure);
                    };
                    errdefer body.release();
                    if (body.count() != 1 or body.width() != 1 or body.dependencies.items.len != 0) return error.InvalidSqlSpill;
                    try offsets.append(owner.arena.allocator(), body_offset);
                    try owner.dependencies.append(a, body);
                    break :blk body;
                };
                stored.values.shared.bodies[@intCast(row)] = body;
            }
            if (decoder.position != wire.len) return error.InvalidSqlSpill;
        } else {
            owner.encoded = try decodeColumnarBlockManaged(owner.arena.allocator(), stable, wire, self.file.manager.max_record_bytes, .forwarding, self.file.manager);
        }
        if (owner.count() == 0 or owner.count() > page_rows) return error.InvalidSqlSpill;
        return owner;
    }
};

pub const Sequential = struct {
    file: File,
    size: u64 = 0,
    readers: usize = 0,
    block_bytes: usize,
    buffer_bytes: ?usize = null,
    write_arena: std.heap.ArenaAllocator,
    read_arena: std.heap.ArenaAllocator,
    pending: std.ArrayList(Row) = .empty,
    pending_bytes: usize = 0,
    read_rows: []const Row = &.{},
    read_single: [1]Row = undefined,
    read_single_offset: ?u64 = null,
    read_first: u64 = 0,
    read_offset: u64 = 0,
    pub fn init(manager: *Manager, bytes: usize) !Sequential {
        return .{ .file = try manager.create(), .block_bytes = @max(128, @min(bytes, manager.max_record_bytes / 4)), .write_arena = .init(manager.allocator()), .read_arena = .init(manager.allocator()) };
    }
    pub fn close(self: *Sequential) void {
        std.debug.assert(self.readers == 0);
        self.pending.deinit(self.file.manager.allocator());
        self.write_arena.deinit();
        self.read_arena.deinit();
        self.file.close();
    }
    pub fn append(self: *Sequential, row: Row, link: u64) !u64 {
        if (self.readers != 0) return error.InvalidSqlSpill;
        if (link != none) return error.InvalidSqlSpill;
        try self.file.manager.check();
        var bytes: usize = @sizeOf(Row);
        for (row.values) |value| bytes +|= try operators.datumBytes(value);
        for (row.keys) |value| bytes +|= try operators.datumBytes(value);
        if (self.pending.items.len != 0 and (self.pending.items.len >= 256 or bytes > self.block_bytes -| self.pending_bytes or row.values.len != self.pending.items[0].values.len or row.keys.len != self.pending.items[0].keys.len)) try self.flush();
        if (bytes > self.block_bytes / 2) {
            try self.flush();
            self.file.buffer_bytes = self.buffer_bytes;
            _ = try self.file.append(row, none);
            const offset = self.size;
            self.size += 1;
            return offset;
        }
        const a = self.write_arena.allocator();
        const values = try a.alloc(Datum, row.values.len);
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.values, values) |value, *out| out.* = try operators.cloneDatum(a, value);
        for (row.keys, keys) |value, *out| out.* = try operators.cloneDatum(a, value);
        try self.pending.append(self.file.manager.allocator(), .{ .values = values, .keys = keys, .ordinal = row.ordinal });
        self.pending_bytes +|= bytes;
        const offset = self.size;
        self.size += 1;
        return offset;
    }
    /// Encode borrowed columns synchronously. Neither payloads nor row arrays
    /// enter the per-run staging arena; physical dictionary values stay borrowed.
    pub fn appendBatch(self: *Sequential, values: @import("execution_batch.zig").Batch, keys: @import("execution_batch.zig").Batch, ordinals: []const u64) !void {
        if (values.len() != keys.len() or values.len() != ordinals.len) return error.InvalidSqlSpill;
        try self.flush();
        var physical_arena = std.heap.ArenaAllocator.init(self.file.manager.allocator());
        defer physical_arena.deinit();
        const pa = physical_arena.allocator();
        const Physical = @import("execution_batch.zig").PhysicalColumn;
        const physical_values = try pa.alloc(?Physical, values.width());
        const physical_keys = try pa.alloc(?Physical, keys.width());
        for (physical_values, 0..) |*view, column| {
            view.* = try values.physicalColumn(pa, column);
            if (view.*) |native| try native.validate();
        }
        for (physical_keys, 0..) |*view, column| {
            view.* = try keys.physicalColumn(pa, column);
            if (view.*) |native| try native.validate();
        }
        var begin: usize = 0;
        while (begin < values.len()) {
            try self.file.manager.check();
            var scratch = std.heap.ArenaAllocator.init(self.file.manager.allocator());
            defer scratch.deinit();
            const a = scratch.allocator();
            var sizing = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(self.file.manager.allocator()));
            defer sizing.deinit();
            var end = begin;
            var estimated: usize = 24;
            while (end < values.len() and end - begin < 256) {
                _ = sizing.reset(.retain_capacity);
                var cost: usize = 8;
                for (physical_values, 0..) |view, column| cost +|= if (view) |native| try native.bytes(end) else try values.cellBytes(sizing.allocator(), end, column);
                for (physical_keys, 0..) |view, column| cost +|= if (view) |native| try native.bytes(end) else try keys.cellBytes(sizing.allocator(), end, column);
                if (end != begin and cost > self.block_bytes -| estimated) break;
                estimated +|= cost;
                end += 1;
            }
            var bytes: std.ArrayList(u8) = .empty;
            var encoder: Encoder = .{ .manager = self.file.manager, .a = a, .bytes = &bytes, .limit = self.file.manager.max_record_bytes };
            try encoder.word(end - begin);
            try encoder.word(values.width());
            try encoder.word(keys.width());
            for (ordinals[begin..end]) |ordinal| try encoder.word(ordinal);
            try encodeBatchColumns(&encoder, values, physical_values, begin, end);
            try encodeBatchColumns(&encoder, keys, physical_keys, begin, end);
            try self.writeEncoded(bytes.items);
            self.size += end - begin;
            begin = end;
        }
    }
    fn encodeBatchColumns(encoder: *Encoder, batch: @import("execution_batch.zig").Batch, physical_columns: []const ?@import("execution_batch.zig").PhysicalColumn, begin: usize, end: usize) !void {
        var scratch = std.heap.ArenaAllocator.init(encoder.scratch orelse encoder.a);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        // One bounded column of borrowed cells feeds the shared physical codec.
        // Dictionary choice, SQL NULL and JSON null retain exactly one format.
        var cells: [256]Datum = undefined;
        var rows: [256]Row = undefined;
        var positions: [256]usize = undefined;
        for (positions[0 .. end - begin], begin..) |*position, index| position.* = index;
        const selected = if (begin == 0 and end == batch.len()) batch else try batch.select(temporary, positions[0 .. end - begin]);
        var column_scratch = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(encoder.scratch orelse encoder.a));
        defer column_scratch.deinit();
        for (0..batch.width()) |column| {
            if (physical_columns[column]) |view| {
                const native = view.slice(begin, end);
                try encodeNativeColumn(encoder, native);
                continue;
            }
            if (!column_scratch.reset(.{ .retain_with_limit = 256 * 1024 })) return error.OutOfMemory;
            const column_alloc = column_scratch.allocator();
            var frozen: [256]?@import("execution_batch.zig").FrozenJson = undefined;
            var encoded: [256]?[]const u8 = undefined;
            var has_encoded = false;
            for (begin..end, 0..) |index, lane| {
                frozen[lane] = try batch.frozenJson(index, column);
                encoded[lane] = if (frozen[lane] == null) try batch.encodedJson(index, column) else null;
                has_encoded = has_encoded or frozen[lane] != null or encoded[lane] != null;
            }
            if (has_encoded) {
                // An encoded object/array forces the same mixed-cell framing
                // as the parsed codec; scalar/SQL-null lanes remain unchanged.
                try encoder.append(&.{255});
                for (begin..end, 0..) |index, lane| {
                    if (frozen[lane]) |json| {
                        try encoder.word(1);
                        try encoder.append(&.{0});
                        try encoder.append(json.bytes);
                    } else if (encoded[lane]) |json| {
                        try encoder.word(1);
                        try encoder.append(&.{0});
                        try encoder.jsonBytes(json);
                    } else try encoder.cells(&.{try batch.cell(column_alloc, index, column)});
                }
                continue;
            }
            const dictionary = try selected.dictionaryColumn(column_alloc, column);
            if (dictionary) |physical| if (physical != .dictionary or physical.len() != end - begin) return error.InvalidSqlSpill;
            defer if (dictionary) |physical| if (selected != .dictionary) {
                column_alloc.free(physical.dictionary.values);
                column_alloc.free(physical.dictionary.indices);
            };
            if (dictionary) |physical| if (try encodeDictionaryColumn(encoder, physical)) continue;
            for (begin..end, 0..) |index, lane| {
                cells[lane] = if (dictionary) |physical| try physical.cell(column_alloc, lane, 0) else try batch.cell(column_alloc, index, column);
                rows[lane] = .{ .values = cells[lane..][0..1], .keys = &.{}, .ordinal = 0 };
            }
            try encodeColumns(encoder, rows[0 .. end - begin], false);
        }
    }
    fn nativeBytes(comptime T: type, value: T) usize {
        return if (T == []const u8) 8 +| value.len else if (T == bool) 1 else 8;
    }
    fn nativeWord(encoder: *Encoder, comptime T: type, value: T) !void {
        if (T == []const u8) return encoder.text(value);
        if (T == bool) return encoder.append(&.{@intFromBool(value)});
        return encoder.word(@bitCast(value));
    }
    fn nativeEqual(comptime T: type, left: T, right: T) bool {
        if (T == []const u8) return std.mem.eql(u8, left, right);
        if (T == f64) return @as(u64, @bitCast(left)) == @as(u64, @bitCast(right));
        return left == right;
    }
    fn encodeNativeValues(encoder: *Encoder, comptime T: type, view: @import("execution_batch.zig").PhysicalColumn, values: []const T, dictionary_indices: ?[]const u32, kind_: u8) !void {
        var entries: [256]T = undefined;
        var indices: [256]u16 = undefined;
        var slots: [512]u16 = @splat(std.math.maxInt(u16));
        var unique: usize = 0;
        var present: usize = 0;
        var expanded: usize = 0;
        var compact: usize = 0;
        for (0..view.len()) |lane| {
            const row = view.rowAt(lane);
            if (view.vector.nulls.isNull(row)) continue;
            const value = values[if (dictionary_indices) |ids| ids[row] else row];
            const hash = if (T == []const u8) std.hash.Wyhash.hash(0, value) else std.hash.Wyhash.hash(0, std.mem.asBytes(&value));
            var slot: usize = @intCast(hash & 511);
            while (slots[slot] != std.math.maxInt(u16) and !nativeEqual(T, entries[slots[slot]], value)) slot = (slot + 1) & 511;
            if (slots[slot] == std.math.maxInt(u16)) {
                slots[slot] = @intCast(unique);
                entries[unique] = value;
                unique += 1;
                compact +|= nativeBytes(T, value);
            }
            indices[lane] = slots[slot];
            expanded +|= nativeBytes(T, value);
            present += 1;
        }
        const encoded_dictionary = present != 0 and compact +| present *| 2 +| 9 < expanded;
        try encoder.append(&.{if (encoded_dictionary) 6 else kind_});
        for (0..(view.len() + 3) / 4) |group| {
            var flags: u8 = 0;
            for (0..4) |lane| {
                const index = group * 4 + lane;
                if (index >= view.len()) break;
                if (view.vector.nulls.isNull(view.rowAt(index))) flags |= @as(u8, 1) << @as(u3, @intCast(lane * 2));
            }
            try encoder.append(&.{flags});
        }
        if (encoded_dictionary) {
            try encoder.append(&.{kind_});
            try encoder.word(unique);
            for (entries[0..unique]) |value| try nativeWord(encoder, T, value);
            for (0..view.len()) |lane| {
                const row = view.rowAt(lane);
                if (view.vector.nulls.isNull(row)) continue;
                var bytes: [2]u8 = undefined;
                std.mem.writeInt(u16, &bytes, indices[lane], .little);
                try encoder.append(&bytes);
            }
        } else for (0..view.len()) |lane| {
            const row = view.rowAt(lane);
            if (view.vector.nulls.isNull(row)) continue;
            try nativeWord(encoder, T, values[if (dictionary_indices) |ids| ids[row] else row]);
        }
    }
    fn encodeNativeColumn(encoder: *Encoder, view: @import("execution_batch.zig").PhysicalColumn) !void {
        if (view.len() > 256) return error.InvalidSqlSpill;
        switch (view.vector.values) {
            .i64 => |v| try encodeNativeValues(encoder, i64, view, v, null, 2),
            .f64 => |v| try encodeNativeValues(encoder, f64, view, v, null, 3),
            .bool => |v| try encodeNativeValues(encoder, bool, view, v, null, 1),
            .bytes => |v| try encodeNativeValues(encoder, []const u8, view, v, null, 5),
            .dictionary_i64 => |v| try encodeNativeValues(encoder, i64, view, v.values, v.indices, 2),
            .dictionary_f64 => |v| try encodeNativeValues(encoder, f64, view, v.values, v.indices, 3),
            .dictionary_bytes => |v| try encodeNativeValues(encoder, []const u8, view, v.values, v.indices, 5),
            else => return error.InvalidSqlSpill,
        }
    }
    /// First-use ordering matches the frozen row codec. Hash each referenced
    /// dictionary entry once, including coalescing equal source entries.
    fn encodeDictionaryColumn(encoder: *Encoder, physical: @import("execution_batch.zig").Batch) !bool {
        const input = physical.dictionary;
        if (input.values.len > 256 or input.indices.len > 256) return false;
        var remap: [256]u16 = @splat(std.math.maxInt(u16));
        var flags: [256]u8 = undefined;
        var sizes: [256]usize = undefined;
        var entries: [256]Datum = undefined;
        var slots: [512]u16 = @splat(std.math.maxInt(u16));
        var indices: [256]u16 = undefined;
        var kind: ?u8 = null;
        var unique: usize = 0;
        var present: usize = 0;
        var expanded_bytes: usize = 0;
        var unique_bytes: usize = 0;
        for (input.indices, 0..) |id, lane| {
            if (id >= input.values.len) return error.InvalidSqlSpill;
            if (remap[id] == std.math.maxInt(u16)) {
                const value = input.values[id];
                flags[id] = if (value.sql_null) 1 else if (value.value == .null and value.array == null and value.numeric == null and value.patterns == null) 2 else 0;
                if (flags[id] != 0) {
                    remap[id] = 256;
                } else {
                    const actual = tag(value);
                    if (actual < 1 or actual > 5 or (kind != null and kind.? != actual)) return false;
                    kind = actual;
                    sizes[id] = physicalBytes(value);
                    var slot: usize = @intCast(physicalHash(value) & 511);
                    while (slots[slot] != std.math.maxInt(u16) and !physicalEqual(entries[slots[slot]], value)) slot = (slot + 1) & 511;
                    if (slots[slot] == std.math.maxInt(u16)) {
                        slots[slot] = @intCast(unique);
                        entries[unique] = value;
                        unique += 1;
                        unique_bytes +|= sizes[id];
                    }
                    remap[id] = slots[slot];
                }
            }
            indices[lane] = remap[id];
            if (flags[id] == 0) {
                present += 1;
                expanded_bytes +|= sizes[id];
            }
        }
        const type_ = kind orelse 0;
        const dictionary = present != 0 and unique_bytes +| (present * 2) +| 9 < expanded_bytes;
        try encoder.append(&.{if (dictionary) 6 else type_});
        for (0..(input.indices.len + 3) / 4) |group| {
            var bits: u8 = 0;
            for (0..4) |lane| {
                const index = group * 4 + lane;
                if (index >= input.indices.len) break;
                bits |= flags[input.indices[index]] << @as(u3, @intCast(lane * 2));
            }
            try encoder.append(&.{bits});
        }
        if (dictionary) {
            try encoder.append(&.{type_});
            try encoder.word(unique);
            for (entries[0..unique]) |value| try encodePhysical(encoder, type_, value);
            var encoded: [512]u8 = undefined;
            var used: usize = 0;
            for (input.indices, 0..) |id, lane| if (flags[id] == 0) {
                std.mem.writeInt(u16, encoded[used..][0..2], indices[lane], .little);
                used += 2;
            };
            try encoder.append(encoded[0..used]);
        } else for (input.indices) |id| if (flags[id] == 0) try encodePhysical(encoder, type_, input.values[id]);
        return true;
    }
    fn tag(value: Datum) u8 {
        if (value.patterns != null or value.array != null or value.numeric != null) return 255;
        return switch (value.value) {
            .null => 0,
            .bool => 1,
            .integer => 2,
            .float => 3,
            .number_string => 4,
            .string => 5,
            else => 255,
        };
    }
    fn physicalEqual(a: Datum, b: Datum) bool {
        if (std.meta.activeTag(a.value) != std.meta.activeTag(b.value)) return false;
        return switch (a.value) {
            .bool => |v| v == b.value.bool,
            .integer => |v| v == b.value.integer,
            .float => |v| @as(u64, @bitCast(v)) == @as(u64, @bitCast(b.value.float)),
            .string => |v| std.mem.eql(u8, v, b.value.string),
            .number_string => |v| std.mem.eql(u8, v, b.value.number_string),
            else => false,
        };
    }
    fn physicalHash(value: Datum) u64 {
        return switch (value.value) {
            .bool => |v| @intFromBool(v),
            .integer => |v| std.hash.Wyhash.hash(0, std.mem.asBytes(&v)),
            .float => |v| std.hash.Wyhash.hash(0, std.mem.asBytes(&v)),
            .string, .number_string => |v| std.hash.Wyhash.hash(0, v),
            else => unreachable,
        };
    }
    fn encodePhysical(encoder: *Encoder, kind: u8, value: Datum) !void {
        switch (kind) {
            1 => try encoder.append(&.{@intFromBool(value.value.bool)}),
            2 => try encoder.word(@bitCast(value.value.integer)),
            3 => try encoder.word(@bitCast(value.value.float)),
            4 => try encoder.text(value.value.number_string),
            5 => try encoder.text(value.value.string),
            else => return error.InvalidSqlSpill,
        }
    }
    fn physicalBytes(value: Datum) usize {
        return switch (value.value) {
            .bool => 1,
            .integer, .float => 8,
            .string, .number_string => |v| 8 +| v.len,
            else => unreachable,
        };
    }
    fn encodeColumns(encoder: *Encoder, rows: []const Row, keys: bool) !void {
        const width = if (keys) rows[0].keys.len else rows[0].values.len;
        for (0..width) |column| {
            var kind: ?u8 = null;
            for (rows) |row| {
                const value = (if (keys) row.keys else row.values)[column];
                if (value.sql_null or (value.value == .null and value.patterns == null and value.array == null and value.numeric == null)) continue;
                const actual = tag(value);
                kind = if (kind == null or kind.? == actual) actual else 255;
            }
            const type_ = kind orelse 0;
            var entries: [256]Datum = undefined;
            var indices: [256]u16 = undefined;
            var slots: [512]u16 = @splat(std.math.maxInt(u16));
            var unique: usize = 0;
            var present: usize = 0;
            var expanded_bytes: usize = 0;
            var unique_bytes: usize = 0;
            if (type_ >= 1 and type_ <= 5) for (rows, 0..) |row, index| {
                const value = (if (keys) row.keys else row.values)[column];
                if (value.sql_null or value.value == .null) continue;
                present += 1;
                expanded_bytes +|= physicalBytes(value);
                var slot: usize = @intCast(physicalHash(value) & 511);
                while (slots[slot] != std.math.maxInt(u16) and !physicalEqual(entries[slots[slot]], value)) slot = (slot + 1) & 511;
                if (slots[slot] == std.math.maxInt(u16)) {
                    slots[slot] = @intCast(unique);
                    entries[unique] = value;
                    unique += 1;
                    unique_bytes +|= physicalBytes(value);
                }
                indices[index] = slots[slot];
            };
            const dictionary = present != 0 and unique_bytes +| (present * 2) +| 9 < expanded_bytes;
            try encoder.append(&.{if (dictionary) 6 else type_});
            if (type_ == 255) {
                for (rows) |row| try encoder.cells((if (keys) row.keys else row.values)[column..][0..1]);
                continue;
            }
            for (0..(rows.len + 3) / 4) |group| {
                var flags: u8 = 0;
                for (0..4) |lane| {
                    const index = group * 4 + lane;
                    if (index >= rows.len) break;
                    const value = (if (keys) rows[index].keys else rows[index].values)[column];
                    const flag: u8 = if (value.sql_null) 1 else if (value.value == .null) 2 else 0;
                    flags |= flag << @as(u3, @intCast(lane * 2));
                }
                try encoder.append(&.{flags});
            }
            if (dictionary) {
                try encoder.append(&.{type_});
                try encoder.word(unique);
                for (entries[0..unique]) |value| try encodePhysical(encoder, type_, value);
                for (rows, 0..) |row, index| {
                    const value = (if (keys) row.keys else row.values)[column];
                    if (value.sql_null or value.value == .null) continue;
                    var bytes: [2]u8 = undefined;
                    std.mem.writeInt(u16, &bytes, indices[index], .little);
                    try encoder.append(&bytes);
                }
            } else for (rows) |row| {
                const value = (if (keys) row.keys else row.values)[column];
                if (value.sql_null or value.value == .null) continue;
                try encodePhysical(encoder, type_, value);
            }
        }
    }
    pub fn flush(self: *Sequential) !void {
        const rows = self.pending.items;
        if (rows.len == 0) return;
        if (rows.len == 1) {
            self.file.buffer_bytes = self.buffer_bytes;
            _ = try self.file.append(rows[0], none);
            self.pending.clearRetainingCapacity();
            _ = self.write_arena.reset(.free_all);
            self.pending_bytes = 0;
            return;
        }
        const manager = self.file.manager;
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(manager.allocator());
        var encoder: Encoder = .{ .manager = manager, .a = manager.allocator(), .bytes = &bytes, .limit = manager.max_record_bytes };
        try encoder.word(rows.len);
        try encoder.word(rows[0].values.len);
        try encoder.word(rows[0].keys.len);
        for (rows) |row| try encoder.word(row.ordinal);
        try encodeColumns(&encoder, rows, false);
        try encodeColumns(&encoder, rows, true);
        try self.writeEncoded(bytes.items);
        self.pending.clearRetainingCapacity();
        _ = self.write_arena.reset(.free_all);
        self.pending_bytes = 0;
    }
    fn writeEncoded(self: *Sequential, bytes: []const u8) !void {
        const manager = self.file.manager;
        const compressed = if (manager.compression == .snappy and bytes.len >= 1024) try snappy.encode(manager.allocator(), bytes) else null;
        defer if (compressed) |value| manager.allocator().free(value);
        const use_compressed = compressed != null and compressed.?.len + 32 < bytes.len;
        const stored = if (use_compressed) compressed.? else bytes;
        var header: [17]u8 = undefined;
        std.mem.writeInt(u64, header[0..8], stored.len, .little);
        std.mem.writeInt(u64, header[8..16], std.hash.Wyhash.hash(0, bytes), .little);
        header[16] = @intFromBool(use_compressed);
        self.file.buffer_bytes = self.buffer_bytes;
        const offset = self.file.size;
        try self.file.writeRaw(offset, &header);
        try self.file.writeRaw(offset + header.len, stored);
        manager.compressed_records += @intFromBool(use_compressed);
    }
    pub const Position = struct { row: u64, byte: u64 };
    /// Flush a column-block boundary for indexed replay without sealing the run.
    pub fn replayBoundary(self: *Sequential) !Position {
        try self.flush();
        try self.file.flush();
        return .{ .row = self.size, .byte = self.file.size };
    }
    /// Independent read state over a sealed, borrowed file. The owner closes
    /// after every reader; reader teardown never closes the shared descriptor.
    pub const Reader = struct {
        run: Sequential,
        offset: u64,
        end: u64,
        pub fn next(self: *Reader, maximum: usize) !?Block {
            if (self.offset == self.end) return null;
            const block = try self.run.readBatchBorrowed(self.offset, @intCast(@min(maximum, self.end - self.offset)));
            self.offset = block.following;
            return block;
        }
        /// Compact physical blocks; no intermediate Datum row matrix. Boundaries
        /// supplied by replayBoundary() are always physical record boundaries.
        pub fn nextOwned(self: *Reader) !?*OwnedBlock {
            if (self.offset == self.end) return null;
            const block = try self.run.readOwnedBlock(self.offset);
            self.offset += block.count();
            return block;
        }
        pub fn deinit(self: *Reader) void {
            self.run.file.manager.allocator().free(self.run.file.read_buffer);
            self.run.read_arena.deinit();
            self.run.write_arena.deinit();
        }
    };
    pub fn reader(self: *Sequential, a: Allocator, begin: Position, end: u64) !Reader {
        try self.seal();
        return self.readerSealed(a, begin, end);
    }
    /// Independent cursors over an already frozen run. Unlike reader(), this
    /// never flushes or mutates the owner's writer state and can be called by
    /// concurrent replay consumers. The owner joins them before writing/close.
    pub fn readerSealed(self: *const Sequential, a: Allocator, begin: Position, end: u64) !Reader {
        std.debug.assert(self.pending.items.len == 0 and self.file.write_buffer.len == 0 and self.file.write_job == null);
        if (begin.row > end or end > self.size or begin.byte > self.file.size) return error.InvalidSqlSpill;
        var file = self.file;
        file.read_buffer = &.{};
        file.read_start = none;
        file.read_len = 0;
        return .{ .run = .{ .file = file, .size = end, .block_bytes = self.block_bytes, .read_arena = .init(a), .write_arena = .init(a), .read_first = begin.row, .read_offset = begin.byte }, .offset = begin.row, .end = end };
    }
    pub fn seal(self: *Sequential) !void {
        try self.flush();
        self.pending.clearAndFree(self.file.manager.allocator());
        try self.file.seal();
    }
    fn decodeColumns(decoder: *Decoder, rows: []Row, keys: bool) !void {
        const width = if (keys) rows[0].keys.len else rows[0].values.len;
        for (0..width) |column| {
            const decoded = try EncodedColumn.decode(decoder, rows.len);
            for (rows, 0..) |row, index| @constCast(if (keys) row.keys else row.values)[column] = try decoded.cell(index);
        }
    }
    /// Start a new sequential pass; borrowed rows from the old pass expire.
    pub fn rewind(self: *Sequential) void {
        self.read_first = 0;
        self.read_offset = 0;
        self.read_rows = &.{};
        self.read_single_offset = null;
    }
    pub fn readBorrowed(self: *Sequential, offset: u64) !Decoded {
        try self.seal();
        return readState(self, &self.file, self.size, offset);
    }

    /// Independent replay positions and decoded blocks over one sealed run.
    /// The owner remains at a stable address and outlives every reader. A
    /// reader must not move after its first read (JSON arrays retain its arena).
    pub const ReplayReader = struct {
        source: *Sequential,
        file: File,
        read_arena: std.heap.ArenaAllocator,
        read_rows: []const Row = &.{},
        read_single: [1]Row = undefined,
        read_single_offset: ?u64 = null,
        read_first: u64 = 0,
        read_offset: u64 = 0,

        compact: ?*OwnedBlock = null,
        compact_first: u64 = 0,
        compact_offset: u64 = 0,
        compact_values: []Datum = &.{},
        compact_keys: []Datum = &.{},

        fn prepareCurrent(self: *ReplayReader, offset: u64) !void {
            if (offset >= self.source.size) return error.InvalidSqlSpill;
            const cached = if (self.compact) |block| offset >= self.compact_first and offset - self.compact_first < block.count() else false;
            if (!cached) {
                var physical: u64 = if (offset == 0) 0 else self.compact_offset;
                if (offset != 0 and (self.compact == null or offset != self.compact_first + self.compact.?.count())) return error.InvalidSqlSpill;
                const a = self.source.file.manager.allocator();
                const block = try a.create(OwnedBlock);
                errdefer a.destroy(block);
                block.* = .{ .a = a, .arena = .init(a) };
                errdefer block.arena.deinit();
                const owned = block.arena.allocator();
                try decodeCompactAt(&self.file, self.source.size, &physical, block, owned, offset);
                if (self.compact) |old| old.release();
                self.compact = block;
                self.compact_first = offset;
                self.compact_offset = physical;
                self.compact_values = &.{};
                self.compact_keys = &.{};
            }
        }
        /// One-row adapter over retained compact columns. Payloads borrow the
        /// current block; the descriptor slices expire on this reader's next
        /// readCurrent call. Other replay readers have independent storage.
        pub fn readCurrent(self: *ReplayReader, offset: u64) !Decoded {
            try self.prepareCurrent(offset);
            const block = self.compact.?;
            const owned = block.arena.allocator();
            if (self.compact_values.len != block.width()) self.compact_values = try owned.alloc(Datum, block.width());
            if (self.compact_keys.len != block.keyWidth()) self.compact_keys = try owned.alloc(Datum, block.keyWidth());
            const lane: usize = @intCast(offset - self.compact_first);
            for (self.compact_values, 0..) |*value, column| value.* = try block.cell(lane, column);
            for (self.compact_keys, 0..) |*value, column| value.* = try block.keyCell(lane, column);
            return .{ .row = .{ .values = self.compact_values, .keys = self.compact_keys, .ordinal = block.ordinal(lane) }, .next = none, .matched = false, .following = offset + 1 };
        }

        /// Keep compact columns through batch consumers. The borrowed block
        /// expires on the next block load; only selection descriptors use a.
        pub fn readBatchCurrent(self: *ReplayReader, a: Allocator, offset: u64, maximum: usize) !struct { values: @import("execution_batch.zig").Batch, following: u64 } {
            if (maximum == 0) return error.InvalidSqlLimit;
            try self.prepareCurrent(offset);
            const block = self.compact.?;
            const begin: usize = @intCast(offset - self.compact_first);
            const count = @min(maximum, block.count() - begin);
            const Batch = @import("execution_batch.zig").Batch;
            const source = try a.create(Batch);
            source.* = try block.batch(a, false);
            const selection = try a.alloc(usize, count);
            for (selection, begin..) |*position, index| position.* = index;
            return .{ .values = try source.select(a, selection), .following = offset + count };
        }

        pub fn readBorrowed(self: *ReplayReader, offset: u64) !Decoded {
            return readState(self, &self.file, self.source.size, offset);
        }

        pub fn deinit(self: *ReplayReader) void {
            if (self.compact) |block| block.release();
            self.read_arena.deinit();
            self.file.manager.allocator().free(self.file.read_buffer);
            self.source.readers -= 1;
            self.* = undefined;
        }
    };

    pub fn openReader(self: *Sequential) !ReplayReader {
        try self.seal();
        self.readers += 1;
        var file = self.file;
        file.read_buffer = &.{};
        file.read_start = none;
        file.read_len = 0;
        return .{ .source = self, .file = file, .read_arena = .init(self.file.manager.allocator()) };
    }

    fn readState(self: anytype, file: *File, size: u64, offset: u64) !Decoded {
        if (offset >= size) return error.InvalidSqlSpill;
        if (self.read_single_offset == offset) return .{ .row = self.read_single[0], .next = none, .matched = false, .following = offset + 1 };
        if (offset == 0 and self.read_first != 0) {
            self.read_first = 0;
            self.read_offset = 0;
            self.read_rows = &.{};
        }
        if (self.read_rows.len == 0 or offset == self.read_first + self.read_rows.len) {
            self.read_single_offset = null;
            self.read_first = offset;
            _ = self.read_arena.reset(.free_all);
            const owned = self.read_arena.allocator();
            var header: [17]u8 = undefined;
            try file.readRaw(self.read_offset, &header);
            // File records have the sentinel link's 0xff at this byte;
            // typed blocks use only 0/1. Both retain checksum validation.
            if (header[16] == 255) {
                var record = try file.read(owned, self.read_offset);
                if (record.next != none or record.matched) return error.InvalidSqlSpill;
                self.read_offset = record.following;
                self.read_first = offset + 1;
                self.read_rows = &.{};
                record.following = offset + 1;
                self.read_single[0] = record.row;
                self.read_single_offset = offset;
                return record;
            }
            const len = std.mem.readInt(u64, header[0..8], .little);
            if (header[16] > 1 or len > file.manager.max_record_bytes or len > file.size -| (self.read_offset + header.len)) return error.InvalidSqlSpill;
            const encoded = try owned.alloc(u8, @intCast(len));
            try file.readRaw(self.read_offset + header.len, encoded);
            const payload = if (header[16] != 0) blk: {
                if (try snappy.decodedLen(encoded) > file.manager.max_record_bytes) return error.InvalidSqlSpill;
                break :blk try snappy.decode(owned, encoded);
            } else encoded;
            if (std.hash.Wyhash.hash(0, payload) != std.mem.readInt(u64, header[8..16], .little)) return error.InvalidSqlSpill;
            var decoder: Decoder = .{ .borrow_compact = true, .manager = file.manager, .a = owned, .bytes = payload };
            const count = try decoder.count();
            const width = try decoder.count();
            const key_width = try decoder.count();
            if (count == 0 or count > 256 or count > size - offset or width > 1024 or key_width > 256) return error.InvalidSqlSpill;
            const rows = try owned.alloc(Row, count);
            // Decode a complete block into contiguous cell storage. Borrowed
            // row spans only slice these allocations, including partial retries.
            const values = try owned.alloc(Datum, count * width);
            const keys = try owned.alloc(Datum, count * key_width);
            for (rows, 0..) |*row, index| row.* = .{ .ordinal = try decoder.word(), .values = values[index * width ..][0..width], .keys = keys[index * key_width ..][0..key_width] };
            try decodeColumns(&decoder, rows, false);
            try decodeColumns(&decoder, rows, true);
            if (decoder.position != payload.len) return error.InvalidSqlSpill;
            self.read_rows = rows;
            self.read_offset += header.len + len;
        }
        if (offset < self.read_first or offset >= self.read_first + self.read_rows.len) return error.InvalidSqlSpill;
        return .{ .row = self.read_rows[@intCast(offset - self.read_first)], .next = none, .matched = false, .following = offset + 1 };
    }
    /// Decode once per typed block; copy only at an ownership boundary.
    /// Borrowed rows expire when a different block is loaded or the file closes.
    pub fn read(self: *Sequential, a: Allocator, offset: u64) !Decoded {
        const decoded = try self.readBorrowed(offset);
        const row = decoded.row;
        const values = try a.alloc(Datum, row.values.len);
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.values, values) |value, *out| out.* = try operators.cloneDatum(a, value);
        for (row.keys, keys) |value, *out| out.* = try operators.cloneDatum(a, value);
        var result = decoded;
        result.row = .{ .values = values, .keys = keys, .ordinal = row.ordinal };
        return result;
    }

    pub const Block = struct { rows: []const Row, following: u64 };
    /// Compact views over a validated typed record. Primitive payloads remain
    /// in the decoded buffer; only legacy heterogeneous cells need Datum tags.
    const EncodedColumn = struct {
        const Dictionary = struct {
            kind: u8,
            size: usize,
            bytes: []const u8 = &.{},
            texts: []const []const u8 = &.{},
            indices: []const u16,
            fn entry(self: Dictionary, index: usize) std.json.Value {
                return switch (self.kind) {
                    1 => .{ .bool = self.bytes[index] == 1 },
                    2 => .{ .integer = @bitCast(std.mem.readInt(u64, self.bytes[index * 8 ..][0..8], .little)) },
                    3 => .{ .float = @bitCast(std.mem.readInt(u64, self.bytes[index * 8 ..][0..8], .little)) },
                    4 => .{ .number_string = self.texts[index] },
                    5 => .{ .string = self.texts[index] },
                    else => unreachable,
                };
            }
        };
        flags: []const u8 = &.{},
        values: union(enum) {
            empty,
            fixed: struct { bytes: []const u8, positions: []const u16, kind: u8 },
            texts: struct { values: []const []const u8, decimal: bool },
            dynamic: struct { cells: []const Datum = &.{}, ranges: []JsonRange = &.{}, wire: []const u8 = &.{}, owner: ?*JsonOwner = null },
            shared: *SharedColumn,
            dictionary: Dictionary,
        },
        const SharedColumn = struct { base: EncodedColumn, bodies: []?*OwnedBlock };
        fn isSqlNull(self: EncodedColumn, index: usize) bool {
            if (self.values == .dynamic) {
                const dynamic = self.values.dynamic;
                if (dynamic.ranges.len != 0) {
                    const range = dynamic.ranges[index];
                    if (range.length != 0 or range.begin == JsonRange.json_null) return false;
                    if (range.begin == JsonRange.sql_null) return true;
                }
                return dynamic.cells[index].sql_null;
            }
            return ((self.flags[index / 4] >> @as(u3, @intCast(index % 4 * 2))) & 3) == 1;
        }
        fn frozen(self: EncodedColumn, index: usize) ?@import("execution_batch.zig").FrozenJson {
            if (self.values == .shared) {
                const shared = self.values.shared;
                if (shared.bodies[index]) |body| return body.encoded.?.values[0].frozen(0);
                return shared.base.frozen(index);
            }
            if (self.values != .dynamic or self.values.dynamic.ranges.len == 0) return null;
            const dynamic = self.values.dynamic;
            const range = dynamic.ranges[index];
            if (range.length == 0) return null;
            return .{ .bytes = dynamic.wire[range.begin..][0..range.length], .lossless_text = range.lossless };
        }
        fn jsonAt(self: EncodedColumn, index: usize) !?*FrozenJson {
            const value = self.frozen(index) orelse return null;
            return try self.values.dynamic.owner.?.get(value.bytes, value.lossless_text);
        }
        fn encodedJson(self: EncodedColumn, index: usize) anyerror!?[]const u8 {
            if (self.values == .shared) {
                const shared = self.values.shared;
                if (shared.bodies[index]) |body| return body.encoded.?.encodedJson(0, 0, false);
                return shared.base.encodedJson(index);
            }
            const json = (try self.jsonAt(index)) orelse return null;
            return json.encoded();
        }
        fn cell(self: EncodedColumn, index: usize) anyerror!Datum {
            if (self.values == .shared) {
                const shared = self.values.shared;
                if (shared.bodies[index]) |body| return body.cell(0, 0);
                return shared.base.cell(index);
            }
            if (self.values == .dynamic) {
                const dynamic = self.values.dynamic;
                if (dynamic.ranges.len != 0) {
                    const range = dynamic.ranges[index];
                    if (range.length == 0 and range.begin >= JsonRange.json_null) return .{ .sql_null = range.begin == JsonRange.sql_null };
                }
                if (try self.jsonAt(index)) |json| return json.materialize();
                return self.values.dynamic.cells[index];
            }
            const flag = (self.flags[index / 4] >> @as(u3, @intCast(index % 4 * 2))) & 3;
            if (flag != 0) return .{ .sql_null = flag == 1 };
            return Datum.json(switch (self.values) {
                .empty => .null,
                .fixed => |v| switch (v.kind) {
                    1 => .{ .bool = v.bytes[v.positions[index]] == 1 },
                    2 => .{ .integer = @bitCast(std.mem.readInt(u64, v.bytes[@as(usize, v.positions[index]) * 8 ..][0..8], .little)) },
                    3 => .{ .float = @bitCast(std.mem.readInt(u64, v.bytes[@as(usize, v.positions[index]) * 8 ..][0..8], .little)) },
                    else => unreachable,
                },
                .texts => |v| if (v.decimal) .{ .number_string = v.values[index] } else .{ .string = v.values[index] },
                .dictionary => |v| v.entry(v.indices[index]),
                .dynamic, .shared => unreachable,
            });
        }
        fn decode(decoder: *Decoder, count: usize) !EncodedColumn {
            const kind = try decoder.byte();
            if (kind == 255) {
                var values: []Datum = &.{};
                var ranges: []JsonRange = &.{};
                for (0..count) |row| {
                    const start = decoder.position;
                    if (try decoder.count() != 1) return error.InvalidSqlSpill;
                    const flag = try decoder.byte();
                    if (flag > 1) return error.InvalidSqlSpill;
                    if (decoder.lazy_json and decoder.position < decoder.bytes.len and decoder.bytes[decoder.position] == 0) {
                        decoder.position += 1;
                        if (ranges.len == 0) {
                            ranges = try decoder.a.alloc(JsonRange, count);
                            @memset(ranges, .{});
                        }
                        ranges[row] = .{ .begin = if (flag == 1) JsonRange.sql_null else JsonRange.json_null };
                        continue;
                    }
                    if (decoder.lazy_json and flag == 0 and decoder.position < decoder.bytes.len and (decoder.bytes[decoder.position] == 6 or decoder.bytes[decoder.position] == 7)) {
                        const begin = decoder.position;
                        decoder.eager_nontext = decoder.json_consumption == .scalar;
                        const lossless = decoder.walkJson(0, null) catch |err| switch (err) {
                            error.ScalarJsonRequired => false,
                            else => return err,
                        };
                        decoder.eager_nontext = false;
                        if (decoder.json_owner == null) {
                            const owner = try decoder.a.create(JsonOwner);
                            owner.* = .{ .arena = .init(decoder.cache_backing.?) };
                            decoder.json_owner = owner;
                        }
                        if (ranges.len == 0) {
                            ranges = try decoder.a.alloc(JsonRange, count);
                            @memset(ranges, .{});
                        }
                        var materialized: ?std.json.Value = null;
                        if (!lossless and decoder.json_consumption == .scalar) {
                            decoder.position = begin;
                            const metadata = decoder.a;
                            decoder.a = decoder.json_owner.?.arena.allocator();
                            defer decoder.a = metadata;
                            materialized = try decoder.json(0);
                        }
                        if (begin > std.math.maxInt(u32) or decoder.position - begin > std.math.maxInt(u31)) return error.InvalidSqlSpill;
                        ranges[row] = .{ .begin = @intCast(begin), .length = @intCast(decoder.position - begin), .lossless = lossless };
                        if (materialized) |value| {
                            const json = try decoder.json_owner.?.get(decoder.bytes[begin..decoder.position], lossless);
                            json.value = value;
                        }
                    } else {
                        if (values.len == 0) {
                            values = try decoder.a.alloc(Datum, count);
                            @memset(values, .{});
                        }
                        decoder.position = start;
                        const one = try decoder.cells();
                        if (one.len != 1) return error.InvalidSqlSpill;
                        values[row] = one[0];
                    }
                }
                return .{ .values = .{ .dynamic = .{ .cells = values, .ranges = ranges, .wire = decoder.bytes, .owner = decoder.json_owner } } };
            }
            if (kind > 6) return error.InvalidSqlSpill;
            const flags = try decoder.take((count + 3) / 4);
            for (0..count) |row| if (((flags[row / 4] >> @as(u3, @intCast(row % 4 * 2))) & 3) == 3) return error.InvalidSqlSpill;
            if (kind == 6) {
                const base = try decoder.byte();
                if (base < 1 or base > 5) return error.InvalidSqlSpill;
                const size = try decoder.count();
                if (size == 0 or size > count) return error.InvalidSqlSpill;
                var fixed: []const u8 = &.{};
                var texts: []const []const u8 = &.{};
                if (base <= 3) {
                    fixed = try decoder.take(size * (if (base == 1) @as(usize, 1) else 8));
                    if (base == 1) for (fixed) |boolean| {
                        if (boolean > 1) return error.InvalidSqlSpill;
                    };
                } else {
                    const entries = try decoder.a.alloc([]const u8, size);
                    for (entries) |*entry| entry.* = try decoder.take(try decoder.count());
                    texts = entries;
                }
                const indices = try decoder.a.alloc(u16, count);
                for (indices, 0..) |*id, row| {
                    id.* = 0;
                    if (((flags[row / 4] >> @as(u3, @intCast(row % 4 * 2))) & 3) != 0) continue;
                    id.* = std.mem.readInt(u16, (try decoder.take(2))[0..2], .little);
                    if (id.* >= size) return error.InvalidSqlSpill;
                }
                return .{ .flags = flags, .values = .{ .dictionary = .{ .kind = base, .size = size, .bytes = fixed, .texts = texts, .indices = indices } } };
            }
            if (kind == 0) return .{ .flags = flags, .values = .empty };
            if (kind <= 3) {
                const positions = try decoder.a.alloc(u16, count);
                const begin = decoder.position;
                var physical: u16 = 0;
                for (positions, 0..) |*position, row| {
                    position.* = physical;
                    if (((flags[row / 4] >> @as(u3, @intCast(row % 4 * 2))) & 3) != 0) continue;
                    if (kind == 1) {
                        if (try decoder.byte() > 1) return error.InvalidSqlSpill;
                    } else _ = try decoder.word();
                    physical += 1;
                }
                return .{ .flags = flags, .values = .{ .fixed = .{ .bytes = decoder.bytes[begin..decoder.position], .positions = positions, .kind = kind } } };
            }
            const values = try decoder.a.alloc([]const u8, count);
            for (values, 0..) |*value, row| {
                value.* = "";
                if (((flags[row / 4] >> @as(u3, @intCast(row % 4 * 2))) & 3) == 0) value.* = try decoder.take(try decoder.count());
            }
            return .{ .flags = flags, .values = .{ .texts = .{ .values = values, .decimal = kind == 4 } } };
        }
    };
    pub const OwnedBlock = struct {
        a: Allocator,
        arena: std.heap.ArenaAllocator,
        rows: []const Row = &.{},
        encoded: ?ColumnarBlock = null,
        refs: std.atomic.Value(usize) = .init(1),
        // Shared native bodies are immutable, individually leased blocks.
        dependencies: std.ArrayList(*OwnedBlock) = .empty,
        physical_offset: ?u64 = null,
        /// Include separately owned lazy DOM/text caches in retention policy.
        /// The page's immutable metadata arena has one owner; cache publication
        /// synchronizes through JsonOwner just as scalar/text access does.
        fn ownRetainedBytes(self: *const OwnedBlock) usize {
            var bytes = @sizeOf(OwnedBlock) +| self.arena.queryCapacity();
            if (self.encoded) |encoded| if (encoded.json_owner) |owner| {
                while (!owner.mutex.tryLock()) std.atomic.spinLoopHint();
                defer owner.mutex.unlock();
                bytes +|= owner.arena.queryCapacity();
            };
            return bytes +| self.dependencies.capacity *| @sizeOf(*OwnedBlock);
        }
        pub fn retainedBytes(self: *const OwnedBlock) usize {
            var bytes = self.ownRetainedBytes();
            for (self.dependencies.items) |body| bytes +|= body.retainedBytes();
            return bytes;
        }
        pub fn count(self: *const OwnedBlock) usize {
            return if (self.encoded) |v| v.ordinals.len else self.rows.len;
        }
        pub fn width(self: *const OwnedBlock) usize {
            return if (self.encoded) |v| v.values.len else self.rows[0].values.len;
        }
        pub fn cell(self: *const OwnedBlock, index: usize, column: usize) !Datum {
            if (index >= self.count() or column >= self.width()) return error.InvalidSqlSpill;
            return if (self.encoded) |v| v.values[column].cell(index) else self.rows[index].values[column];
        }
        pub fn keyWidth(self: *const OwnedBlock) usize {
            return if (self.encoded) |v| v.keys.len else self.rows[0].keys.len;
        }
        pub fn ordinal(self: *const OwnedBlock, index: usize) u64 {
            return if (self.encoded) |v| v.ordinals[index] else self.rows[index].ordinal;
        }
        pub fn keyCell(self: *const OwnedBlock, index: usize, column: usize) !Datum {
            if (index >= self.count() or column >= self.keyWidth()) return error.InvalidSqlSpill;
            return if (self.encoded) |v| v.keys[column].cell(index) else self.rows[index].keys[column];
        }
        pub fn batch(self: *OwnedBlock, a: Allocator, keys: bool) !@import("execution_batch.zig").Batch {
            const View = struct {
                block: *OwnedBlock,
                keys: bool,
                fn cell(raw: *anyopaque, _: Allocator, row_index: usize, column: usize) anyerror!Datum {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    return if (view.keys) view.block.keyCell(row_index, column) else view.block.cell(row_index, column);
                }
                fn json(raw: *anyopaque, row_index: usize, column: usize) anyerror!?[]const u8 {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    const encoded = view.block.encoded orelse return null;
                    return encoded.encodedJson(row_index, column, view.keys);
                }
                fn frozen(raw: *anyopaque, row_index: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    const encoded = view.block.encoded orelse return null;
                    return encoded.frozenJson(row_index, column, view.keys);
                }
                fn identity(raw: *anyopaque, row_index: usize, column: usize) anyerror!?u64 {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    const encoded = view.block.encoded orelse return null;
                    return encoded.dictionaryIdentity(row_index, column, view.keys);
                }
                fn physical(raw: *anyopaque, alloc: Allocator, column: usize) anyerror!?@import("execution_batch.zig").PhysicalColumn {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    const encoded = view.block.encoded orelse return null;
                    const columns = if (view.keys) encoded.keys else encoded.values;
                    if (column >= columns.len) return error.InvalidSqlSpill;
                    const stored = columns[column];
                    const wire_tag = switch (stored.values) {
                        .fixed => |v| v.kind,
                        .dictionary => |v| v.kind,
                        .texts => |v| if (v.decimal) return null else @as(u8, 5),
                        else => return null,
                    };
                    for (0..encoded.count()) |row_index| {
                        const flag = (stored.flags[row_index / 4] >> @as(u3, @intCast(row_index % 4 * 2))) & 3;
                        if (flag > 1) return null;
                    }
                    const kind_: @import("../storage/rowsource/types.zig").ColumnKind = switch (wire_tag) {
                        1 => .bool,
                        2 => .i64,
                        3 => .f64,
                        5 => .bytes,
                        else => return null,
                    };
                    var block = view.block.*;
                    if (view.keys) block.encoded = .{ .values = encoded.keys, .keys = &.{}, .ordinals = encoded.ordinals };
                    const vector = try block.columnVector(alloc, column, kind_);
                    return .{ .vector = vector, .range = .{ .count = encoded.count() }, .kind = switch (kind_) {
                        .bool => .boolean,
                        .i64 => .integer,
                        .f64 => .number,
                        .bytes => .string,
                        else => unreachable,
                    }, .element = if (kind_ == .f64) .float64 else null };
                }
                fn decimal(raw: *anyopaque, alloc: Allocator, column: usize, selected: ?[]const usize) anyerror!?@import("execution_batch.zig").DecimalColumn {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    if (view.keys) return null;
                    return view.block.decimalColumn(alloc, column, selected);
                }
                fn dictionary(raw: *anyopaque, alloc: Allocator, column: usize) anyerror!?@import("execution_batch.zig").Batch {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    const encoded = view.block.encoded orelse return null;
                    return encoded.dictionaryColumn(alloc, column, view.keys);
                }
            };
            const view = try a.create(View);
            view.* = .{ .block = self, .keys = keys };
            return .{ .reader = .{ .ptr = view, .read = View.cell, .read_physical = View.physical, .read_decimal_selected = View.decimal, .read_identity = View.identity, .read_json = if (keys) null else View.json, .read_frozen = if (keys) null else View.frozen, .read_dictionary = View.dictionary, .count = self.count(), .width = if (keys) self.keyWidth() else self.width() } };
        }
        pub fn decimalColumn(self: *const OwnedBlock, a: Allocator, column: usize, selected: ?[]const usize) !?@import("execution_batch.zig").DecimalColumn {
            const count_rows = if (selected) |indices| indices.len else self.count();
            var coefficients: []i128 = &.{};
            var scales: []u16 = &.{};
            var nulls: []u8 = &.{};
            var keep = false;
            defer if (!keep) {
                a.free(coefficients);
                a.free(scales);
                a.free(nulls);
            };
            coefficients = try a.alloc(i128, count_rows);
            scales = try a.alloc(u16, count_rows);
            nulls = try a.alloc(u8, count_rows);
            for (coefficients, scales, nulls, 0..) |*coefficient, *scale, *flag, row_index| {
                const source_row = if (selected) |indices| indices[row_index] else row_index;
                // Optional decimal views must never materialize unrelated JSON.
                if (self.encoded) |encoded| if (try encoded.frozenJson(source_row, column, false) != null) return null;
                const datum = try self.cell(source_row, column);
                flag.* = @intFromBool(datum.sql_null);
                coefficient.* = 0;
                scale.* = 0;
                if (datum.sql_null) continue;
                const numeric = @import("numeric_value.zig");
                var parsed: ?numeric.Owned = null;
                defer if (parsed) |*owned| owned.deinit();
                const number = datum.numeric orelse blk: {
                    if (datum.value != .number_string) return null;
                    var context: numeric.Context = .{ .alloc = a };
                    parsed = try numeric.parse(&context, datum.value.number_string);
                    break :blk &parsed.?.value;
                };
                if (number.kind != .finite or number.scale > 38) return null;
                scale.* = number.scale;
                coefficient.* = @import("numeric_value.zig").coefficientAtScale(number.*, number.scale) orelse return null;
                if (@abs(coefficient.*) >= @as(u128, 100000000000000000000000000000000000000)) return null;
            }
            keep = true;
            return .{ .physical = .{ .vector = .{ .name = "", .values = .{ .decimal128 = .{ .values = coefficients, .precision = 38, .scale = 0 } }, .nulls = .{ .bytes = nulls } }, .range = .{ .count = count_rows }, .kind = .number, .element = .numeric }, .scales = scales };
        }
        /// Export a typed column directly from validated spill buffers. Strings
        /// and string dictionaries borrow the block; numeric wire values are
        /// decoded once without constructing Datum dictionaries or row matrices.
        /// Descriptor arrays belong to a; payloads expire with this block.
        pub fn columnVector(self: *const OwnedBlock, a: Allocator, column: usize, kind: @import("../storage/rowsource/types.zig").ColumnKind) !@import("../storage/rowsource/types.zig").ColumnVector {
            const types = @import("../storage/rowsource/types.zig");
            if (column >= self.width()) return error.InvalidSqlSpill;
            const count_rows = self.count();
            const nulls = try a.alloc(u8, count_rows);
            errdefer a.free(nulls);
            const encoded = if (self.encoded) |block| block.values[column] else null;
            for (nulls, 0..) |*flag, row_index| {
                if (encoded) |stored| {
                    if (stored.values != .dynamic and stored.values != .shared) {
                        const null_tag = (stored.flags[row_index / 4] >> @as(u3, @intCast(row_index % 4 * 2))) & 3;
                        // A native typed source has a separate SQL NULL bitmap;
                        // an untyped JSON-null lane cannot become a numeric value.
                        if (null_tag > 1) return error.InvalidSqlSpill;
                        flag.* = @intFromBool(null_tag == 1);
                        continue;
                    }
                }
                flag.* = @intFromBool((try self.cell(row_index, column)).sql_null);
            }
            const expected: u8 = switch (kind) {
                .bool => 1,
                .i64 => 2,
                .f64 => 3,
                .bytes, .json => 5,
                else => return error.InvalidSqlSpill,
            };
            if (encoded) |stored| {
                if (stored.values == .dictionary and kind != .json and kind != .bool) {
                    const dictionary = stored.values.dictionary;
                    if (dictionary.kind != expected) return error.InvalidSqlSpill;
                    const ids = try a.alloc(u32, count_rows);
                    errdefer a.free(ids);
                    for (ids, dictionary.indices) |*id, value| id.* = value;
                    const values: types.ColumnValues = switch (kind) {
                        .i64 => blk: {
                            const entries = try a.alloc(i64, dictionary.size);
                            for (entries, 0..) |*entry, i| entry.* = std.mem.readInt(i64, dictionary.bytes[i * 8 ..][0..8], .little);
                            break :blk .{ .dictionary_i64 = .{ .values = entries, .indices = ids } };
                        },
                        .f64 => blk: {
                            const entries = try a.alloc(f64, dictionary.size);
                            for (entries, 0..) |*entry, i| entry.* = @bitCast(std.mem.readInt(u64, dictionary.bytes[i * 8 ..][0..8], .little));
                            break :blk .{ .dictionary_f64 = .{ .values = entries, .indices = ids } };
                        },
                        .bytes => .{ .dictionary_bytes = .{ .values = dictionary.texts, .indices = ids } },
                        else => unreachable,
                    };
                    return .{ .name = "", .values = values, .nulls = .{ .bytes = nulls } };
                }
                if (stored.values == .texts and !stored.values.texts.decimal and (kind == .bytes or kind == .json)) {
                    return .{ .name = "", .values = if (kind == .json) .{ .json = stored.values.texts.values } else .{ .bytes = stored.values.texts.values }, .nulls = .{ .bytes = nulls } };
                }
                if (stored.values == .fixed) {
                    const fixed = stored.values.fixed;
                    if (fixed.kind != expected) return error.InvalidSqlSpill;
                    const values: types.ColumnValues = switch (kind) {
                        .i64 => blk: {
                            const entries = try a.alloc(i64, count_rows);
                            for (entries, nulls, fixed.positions) |*entry, flag, position| entry.* = if (flag != 0) 0 else std.mem.readInt(i64, fixed.bytes[@as(usize, position) * 8 ..][0..8], .little);
                            break :blk .{ .i64 = entries };
                        },
                        .f64 => blk: {
                            const entries = try a.alloc(f64, count_rows);
                            for (entries, nulls, fixed.positions) |*entry, flag, position| entry.* = if (flag != 0) 0 else @bitCast(std.mem.readInt(u64, fixed.bytes[@as(usize, position) * 8 ..][0..8], .little));
                            break :blk .{ .f64 = entries };
                        },
                        .bool => blk: {
                            const entries = try a.alloc(bool, count_rows);
                            for (entries, nulls, fixed.positions) |*entry, flag, position| entry.* = flag == 0 and fixed.bytes[position] == 1;
                            break :blk .{ .bool = entries };
                        },
                        else => return error.InvalidSqlSpill,
                    };
                    return .{ .name = "", .values = values, .nulls = .{ .bytes = nulls } };
                }
            }
            // Legacy scalar records, all-NULL columns and boolean dictionaries
            // keep the same typed contract without exposing heterogeneous cells.
            const values: types.ColumnValues = switch (kind) {
                .i64 => blk: {
                    const entries = try a.alloc(i64, count_rows);
                    errdefer a.free(entries);
                    for (entries, nulls, 0..) |*entry, flag, i| {
                        const value = (try self.cell(i, column)).value;
                        entry.* = if (flag != 0) 0 else if (value == .integer) value.integer else return error.InvalidSqlSpill;
                    }
                    break :blk .{ .i64 = entries };
                },
                .f64 => blk: {
                    const entries = try a.alloc(f64, count_rows);
                    errdefer a.free(entries);
                    for (entries, nulls, 0..) |*entry, flag, i| {
                        const value = (try self.cell(i, column)).value;
                        entry.* = if (flag != 0) 0 else if (value == .float) value.float else return error.InvalidSqlSpill;
                    }
                    break :blk .{ .f64 = entries };
                },
                .bool => blk: {
                    const entries = try a.alloc(bool, count_rows);
                    errdefer a.free(entries);
                    for (entries, nulls, 0..) |*entry, flag, i| {
                        const value = (try self.cell(i, column)).value;
                        entry.* = if (flag != 0) false else if (value == .bool) value.bool else return error.InvalidSqlSpill;
                    }
                    break :blk .{ .bool = entries };
                },
                .bytes, .json => blk: {
                    const entries = try a.alloc([]const u8, count_rows);
                    errdefer a.free(entries);
                    for (entries, nulls, 0..) |*entry, flag, i| {
                        const value = (try self.cell(i, column)).value;
                        entry.* = if (flag != 0) "" else if (value == .string) value.string else return error.InvalidSqlSpill;
                    }
                    break :blk if (kind == .json) .{ .json = entries } else .{ .bytes = entries };
                },
                else => return error.InvalidSqlSpill,
            };
            return .{ .name = "", .values = values, .nulls = .{ .bytes = nulls } };
        }

        pub fn keyRowAlloc(self: *OwnedBlock, a: Allocator, index: usize) !Row {
            if (index >= self.count()) return error.InvalidSqlSpill;
            const keys = try a.alloc(Datum, self.keyWidth());
            for (keys, 0..) |*key, column| key.* = try self.keyCell(index, column);
            return .{ .values = &.{}, .keys = keys, .ordinal = self.ordinal(index) };
        }
        pub fn keyRow(self: *OwnedBlock, index: usize) !Row {
            if (index >= self.count()) return error.InvalidSqlSpill;
            if (self.encoded) |v| {
                const keys = try self.arena.allocator().alloc(Datum, v.keys.len);
                for (keys, v.keys) |*key, column| key.* = try column.cell(index);
                return .{ .values = &.{}, .keys = keys, .ordinal = v.ordinals[index] };
            }
            return self.rows[index];
        }
        pub fn row(self: *const OwnedBlock, a: Allocator, index: usize) !Row {
            if (index >= self.count()) return error.InvalidSqlSpill;
            if (self.encoded) |v| {
                const values = try a.alloc(Datum, v.values.len);
                errdefer a.free(values);
                const keys = try a.alloc(Datum, v.keys.len);
                for (values, v.values) |*value, column| value.* = try column.cell(index);
                for (keys, v.keys) |*key, column| key.* = try column.cell(index);
                return .{ .values = values, .keys = keys, .ordinal = v.ordinals[index] };
            }
            return self.rows[index];
        }
        pub fn retain(self: *OwnedBlock) void {
            _ = self.refs.fetchAdd(1, .monotonic);
        }
        pub fn release(self: *OwnedBlock) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const a = self.a;
            if (self.encoded) |block| block.deinit();
            for (self.dependencies.items) |body| body.release();
            self.dependencies.deinit(a);
            self.arena.deinit();
            a.destroy(self);
        }
    };
    pub const InputBlock = union(enum) {
        owned: *OwnedBlock,
        borrowed: OwnedBlock,
        pub fn view(self: *InputBlock) *OwnedBlock {
            return switch (self.*) {
                .owned => |block| block,
                .borrowed => |*block| block,
            };
        }
        pub fn deinit(self: *InputBlock) void {
            switch (self.*) {
                .owned => |block| block.release(),
                .borrowed => |*block| {
                    // Metadata/wire borrow the file's read arena, but lazy
                    // scalar/text caches have their own allocation lifetime.
                    if (block.encoded) |encoded| encoded.deinit();
                    block.encoded = null;
                },
            }
        }
    };
    /// Retaining consumers finish copying state before advancing this source.
    /// Singleton and already-expanded records borrow the existing read arena;
    /// typed blocks retain their compact column payloads through admission.
    pub fn readInputBlock(self: *Sequential, offset: u64) !InputBlock {
        try self.seal();
        if (offset >= self.size) return error.InvalidSqlSpill;
        const expanded = (self.read_rows.len != 0 and offset >= self.read_first and offset < self.read_first + self.read_rows.len) or self.read_single_offset == offset;
        var singleton = false;
        if (!expanded and offset == self.read_first) {
            var header: [17]u8 = undefined;
            try self.file.readRaw(self.read_offset, &header);
            singleton = header[16] == 255;
        }
        if (!expanded and !singleton) return .{ .owned = try self.readOwnedBlock(offset) };
        const batch_rows = try self.readBatchBorrowed(offset, 256);
        const a = self.file.manager.allocator();
        return .{ .borrowed = .{ .a = a, .arena = .init(a), .rows = batch_rows.rows } };
    }

    /// Compact input valid until the next physical pull from this source.
    /// Probe consumers reuse this storage after draining all matches, avoiding
    /// one heap lease and a full row matrix for every sequential block.
    pub fn readInputBlockBorrowed(self: *Sequential, offset: u64) !InputBlock {
        try self.seal();
        if (offset >= self.size) return error.InvalidSqlSpill;
        if (self.read_rows.len != 0 or self.read_single_offset == offset or offset != self.read_first)
            return self.readInputBlock(offset);
        var header: [17]u8 = undefined;
        try self.file.readRaw(self.read_offset, &header);
        if (header[16] == 255) return self.readInputBlock(offset);
        const a = self.file.manager.allocator();
        if (!self.read_arena.reset(.{ .retain_with_limit = 16 * 1024 })) return error.OutOfMemory;
        var block: OwnedBlock = .{ .a = a, .arena = .init(a) };
        try self.decodeCompactBlock(&block, self.read_arena.allocator(), offset);
        return .{ .borrowed = block };
    }
    fn decodeCompactBlock(self: *Sequential, owner: *OwnedBlock, owned: Allocator, offset: u64) !void {
        try self.seal();
        if (offset >= self.size) return error.InvalidSqlSpill;
        try decodeCompactAt(&self.file, self.size, &self.read_offset, owner, owned, offset);
        self.read_first = offset + owner.count();
        self.read_single_offset = null;
    }
    fn decodeCompactAt(file: *File, size: u64, physical: *u64, owner: *OwnedBlock, owned: Allocator, offset: u64) !void {
        if (offset >= size) return error.InvalidSqlSpill;
        var header: [17]u8 = undefined;
        try file.readRaw(physical.*, &header);
        if (header[16] == 255) {
            var decoded = try file.read(owned, physical.*);
            if (decoded.next != none or decoded.matched) return error.InvalidSqlSpill;
            physical.* = decoded.following;
            decoded.following = offset + 1;
            owner.rows = try owned.dupe(Row, &.{decoded.row});
        } else {
            const len = std.mem.readInt(u64, header[0..8], .little);
            if (header[16] > 1 or len > file.manager.max_record_bytes or len > file.size -| (physical.* + header.len)) return error.InvalidSqlSpill;
            const encoded = try owned.alloc(u8, @intCast(len));
            try file.readRaw(physical.* + header.len, encoded);
            const payload = if (header[16] == 1) blk: {
                if (try snappy.decodedLen(encoded) > file.manager.max_record_bytes) return error.InvalidSqlSpill;
                break :blk try snappy.decode(owned, encoded);
            } else encoded;
            if (std.hash.Wyhash.hash(0, payload) != std.mem.readInt(u64, header[8..16], .little)) return error.InvalidSqlSpill;
            var json_scratch: JsonScratch = .{ .a = @import("memory_budget.zig").reclaimingAllocator(owned) };
            defer json_scratch.deinit();
            var decoder: Decoder = .{ .json_consumption = .forwarding, .json_scratch = &json_scratch, .lazy_json = true, .cache_backing = owner.a, .borrow_compact = true, .manager = file.manager, .a = owned, .bytes = payload };
            const count_rows = try decoder.count();
            const width_values = try decoder.count();
            const width_keys = try decoder.count();
            if (count_rows == 0 or count_rows > 256 or count_rows > size - offset or width_values > 1024 or width_keys > 256) return error.InvalidSqlSpill;
            const ordinals = try owned.alloc(u64, count_rows);
            for (ordinals) |*ordinal| ordinal.* = try decoder.word();
            errdefer if (decoder.json_owner) |json_owner| json_owner.deinit();
            const values = try owned.alloc(EncodedColumn, width_values);
            const keys = try owned.alloc(EncodedColumn, width_keys);
            for (values) |*column| column.* = try EncodedColumn.decode(&decoder, count_rows);
            decoder.json_consumption = .scalar;
            for (keys) |*column| column.* = try EncodedColumn.decode(&decoder, count_rows);
            if (decoder.position != payload.len) return error.InvalidSqlSpill;
            owner.encoded = .{ .values = values, .keys = keys, .ordinals = ordinals, .json_owner = decoder.json_owner };
            physical.* += header.len + len;
        }
    }
    /// Transfer a decoded arena without copying its cell payloads. The file
    /// retains only its forward physical offset; the lease owns this block.
    pub fn readOwnedBlock(self: *Sequential, offset: u64) !*OwnedBlock {
        return self.readOwnedBlockWithPressure(offset, null);
    }
    fn readOwnedBlockWithPressure(self: *Sequential, offset: u64, pressure: ?*CachePressure) !*OwnedBlock {
        const a = self.file.manager.allocator();
        const scoped = if (pressure) |scope| scope.allocator() else a;
        if (pressure) |scope| if (self.file.read_buffer.len == 0) {
            if (!scope.admit(@max(1, self.file.buffer_bytes orelse self.file.manager.buffer_bytes))) return error.OutOfMemory;
        };
        const owner = try scoped.create(OwnedBlock);
        errdefer a.destroy(owner);
        // A scalar merge may already have borrowed this record. Transfer its
        // arena once; subsequent records decode directly into compact columns.
        if (self.read_rows.len == 0 and self.read_single_offset != offset and offset == self.read_first) {
            owner.* = .{ .a = a, .arena = .init(scoped) };
            defer owner.arena.child_allocator = a;
            errdefer owner.arena.deinit();
            try self.decodeCompactBlock(owner, owner.arena.allocator(), offset);
            return owner;
        }
        self.read_arena.child_allocator = scoped;
        defer self.read_arena.child_allocator = a;
        const block = try self.readBatchBorrowed(offset, 256);
        // Typed block descriptors already belong to the transferred arena.
        // Only the legacy single-row descriptor lives inline in Sequential.
        const rows = if (self.read_rows.len != 0) block.rows else try self.read_arena.allocator().dupe(Row, block.rows);
        owner.* = .{ .a = a, .arena = self.read_arena, .rows = rows };
        owner.arena.child_allocator = a;
        self.read_arena = .init(a);
        self.read_rows = &.{};
        self.read_single_offset = null;
        self.read_first = offset + rows.len;
        return owner;
    }
    /// A span of the current decoded block. It expires when another block is
    /// loaded; retaining operators admit the whole span before advancing.
    pub fn readBatchBorrowed(self: *Sequential, offset: u64, maximum: usize) !Block {
        if (maximum == 0) return error.InvalidSqlLimit;
        const first = try self.readBorrowed(offset);
        if (self.read_rows.len == 0) {
            self.read_single[0] = first.row;
            return .{ .rows = &self.read_single, .following = first.following };
        }
        const begin: usize = @intCast(offset - self.read_first);
        const count = @min(maximum, self.read_rows.len - begin);
        return .{ .rows = self.read_rows[begin..][0..count], .following = offset + count };
    }
};

/// Immutable runs write directly into reusable 64 KiB extents in one anonymous
/// file. The map scales with disk quota, not rows; unrelated runs read/write
/// disjoint extents concurrently. Only extent allocation and release serialize.
/// Bounded radix streams backed by reusable extents in one anonymous file.
/// Child partitions borrow the same store and close before their parent owner.
pub const Partitions = struct {
    store: *RunStore,
    owner: bool,
    streams: [64]?Sequential = @splat(null),
    pub fn init(manager: *Manager) !Partitions {
        const store = try manager.allocator().create(RunStore);
        errdefer manager.allocator().destroy(store);
        store.* = .{ .file = try manager.create() };
        return .{ .store = store, .owner = true };
    }
    pub fn child(self: *Partitions) Partitions {
        return .{ .store = self.store, .owner = false };
    }
    pub fn deinit(self: *Partitions) void {
        for (0..self.streams.len) |index| self.close(index);
        if (self.owner) self.store.deinit();
    }
    pub fn close(self: *Partitions, index: usize) void {
        if (self.streams[index]) |*stream| stream.close();
        self.streams[index] = null;
    }
    pub fn appendBatch(self: *Partitions, index: usize, values: @import("execution_batch.zig").Batch, keys: @import("execution_batch.zig").Batch, ordinals: []const u64) !void {
        if (index >= self.streams.len or values.len() != keys.len() or values.len() != ordinals.len) return error.InvalidSqlSpill;
        if (values.len() == 0) return;
        if (self.streams[index] == null) self.streams[index] = self.store.run(64 * 1024);
        try self.streams[index].?.appendBatch(values, keys, ordinals);
    }
    pub fn seal(self: *Partitions) !void {
        for (&self.streams) |*stream| if (stream.*) |*file| try file.seal();
    }
};

const RunStore = struct {
    const block_bytes: u64 = 64 * 1024;
    file: File,
    free: std.AutoHashMapUnmanaged(u64, void) = .empty,
    references: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *RunStore) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn run(self: *RunStore, block_size: usize) Sequential {
        self.lock();
        self.references += 1;
        self.mutex.unlock();
        const manager = self.file.manager;
        return .{ .file = .{ .manager = manager, .file = self.file.file, .id = self.file.id, .run_store = self }, .block_bytes = block_size, .write_arena = .init(manager.allocator()), .read_arena = .init(manager.allocator()) };
    }
    fn allocate(self: *RunStore) !u64 {
        self.lock();
        defer self.mutex.unlock();
        var available = self.free.keyIterator();
        if (available.next()) |key| {
            const offset = key.*;
            _ = self.free.remove(offset);
            return offset;
        }
        const end = try std.math.add(u64, self.file.size, block_bytes);
        // Reserve free-map capacity now: close/cancellation never allocate.
        try self.free.ensureTotalCapacity(self.file.manager.allocator(), std.math.cast(u32, end / block_bytes) orelse return error.SqlProgramLimitExceeded);
        try self.file.manager.reserve(block_bytes);
        errdefer self.file.manager.releaseBytes(block_bytes);
        try self.file.file.setLength(self.file.manager.io, end);
        const offset = self.file.size;
        self.file.size = end;
        return offset;
    }
    fn release(self: *RunStore, blocks: []const u64) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.references != 0);
        self.references -= 1;
        for (blocks) |offset| self.free.putAssumeCapacityNoClobber(offset, {});
        var end = self.file.size;
        while (end != 0 and self.free.contains(end - block_bytes)) end -= block_bytes;
        if (end == self.file.size) return;
        const protection = self.file.manager.io.swapCancelProtection(.blocked);
        defer _ = self.file.manager.io.swapCancelProtection(protection);
        self.file.file.setLength(self.file.manager.io, end) catch return;
        var offset = end;
        while (offset < self.file.size) : (offset += block_bytes) _ = self.free.remove(offset);
        self.file.manager.releaseBytes(self.file.size - end);
        self.file.size = end;
    }
    fn deinit(self: *RunStore) void {
        std.debug.assert(self.references == 0);
        const manager = self.file.manager;
        self.free.deinit(manager.allocator());
        self.file.close();
        manager.allocator().destroy(self);
    }
};

// Fingerprints borrow only the current immutable producer lease.
const NativeFingerprint = struct {
    bodies: [64]?struct { pointer: usize, len: usize, kind: u8, hash: u64 } = @splat(null),
    next: usize = 0,
    fn bodyHash(self: *NativeFingerprint, bytes: []const u8, kind: u8) u64 {
        for (self.bodies) |entry| if (entry) |body| if (body.pointer == @intFromPtr(bytes.ptr) and body.len == bytes.len and body.kind == kind) return body.hash;
        const hash = std.hash.Wyhash.hash(kind, bytes);
        self.bodies[self.next] = .{ .pointer = @intFromPtr(bytes.ptr), .len = bytes.len, .kind = kind, .hash = hash };
        self.next = (self.next + 1) % self.bodies.len;
        return hash;
    }
    fn rowHash(self: *NativeFingerprint, a: Allocator, batch: @import("execution_batch.zig").Batch, row: usize) !?u64 {
        var hash = std.hash.Wyhash.init(batch.width());
        for (0..batch.width()) |column| {
            const part: u64 = if (try batch.frozenJson(row, column)) |frozen| self.bodyHash(frozen.bytes, 2 + @as(u8, @intFromBool(frozen.lossless_text))) else if (try batch.encodedJson(row, column)) |text| self.bodyHash(text, 1) else blk: {
                const value = try batch.cell(a, row, column);
                if (value.patterns != null or value.array != null or value.numeric != null or Sequential.tag(value) == 255) return null;
                const tag: u8 = if (value.sql_null) 128 else Sequential.tag(value);
                hash.update(&.{tag});
                break :blk if (value.sql_null or tag == 0) @as(u64, 0) else Sequential.physicalHash(value);
            };
            hash.update(std.mem.asBytes(&part));
        }
        return hash.final();
    }
};
fn sameNativeBytes(left: []const u8, right: []const u8) bool {
    return left.len == right.len and (left.ptr == right.ptr or std.mem.eql(u8, left, right));
}
fn nativeRowsEqual(a: Allocator, batch: @import("execution_batch.zig").Batch, left: usize, right: usize) !bool {
    for (0..batch.width()) |column| {
        const lf = try batch.frozenJson(left, column);
        const rf = try batch.frozenJson(right, column);
        if (lf != null or rf != null) {
            if (lf == null or rf == null or lf.?.version != rf.?.version or lf.?.lossless_text != rf.?.lossless_text or !sameNativeBytes(lf.?.bytes, rf.?.bytes)) return false;
        } else {
            const lt = try batch.encodedJson(left, column);
            const rt = try batch.encodedJson(right, column);
            if (lt != null or rt != null) {
                if (lt == null or rt == null or !sameNativeBytes(lt.?, rt.?)) return false;
            } else {
                const l = try batch.cell(a, left, column);
                const r = try batch.cell(a, right, column);
                if (l.sql_null != r.sql_null or (!l.sql_null and !(l.value == .null and r.value == .null) and !Sequential.physicalEqual(l, r))) return false;
            }
        }
    }
    return true;
}

/// Independent read state over sealed payload pages. Closing a reader releases
/// its cache/buffer only; the enclosing sort/cursor owns the shared file.
pub const PayloadReader = struct {
    pages: JoinPayloads,
    cache: [4]?struct { offset: u64, block: *Sequential.OwnedBlock, used: u64 } = @splat(null),
    clock: u64 = 0,
    reads: usize = 0,
    window_reductions: usize = 0,
    reserve_bytes: usize = 0,
    row_bytes: usize = 0,
    pub const Reference = struct {
        offset: u64,
        index: usize,
        pub fn decode(values: []const Datum) !Reference {
            if (values.len != 2 or values[0].sql_null or values[1].sql_null or values[0].value != .integer or values[1].value != .integer) return error.InvalidSqlSpill;
            return .{ .offset = std.math.cast(u64, values[0].value.integer) orelse return error.InvalidSqlSpill, .index = std.math.cast(usize, values[1].value.integer) orelse return error.InvalidSqlSpill };
        }
    };
    pub const Payload = struct {
        block: *Sequential.OwnedBlock,
        index: usize,
        pub fn release(self: Payload) void {
            self.block.release();
        }
    };
    pub fn init(source: *const JoinPayloads) PayloadReader {
        std.debug.assert(source.file.write_buffer.len == 0 and source.file.write_job == null);
        var pages: JoinPayloads = .{ .file = source.file };
        pages.file.read_buffer = &.{};
        pages.file.read_start = none;
        pages.file.read_len = 0;
        return .{ .pages = pages };
    }
    pub fn deinit(self: *PayloadReader) void {
        for (&self.cache) |*entry| if (entry.*) |cached| {
            cached.block.release();
            entry.* = null;
        };
        self.pages.file.manager.allocator().free(self.pages.file.read_buffer);
    }
    fn evict(raw: *anyopaque) bool {
        const self: *PayloadReader = @ptrCast(@alignCast(raw));
        var oldest: ?usize = null;
        for (self.cache, 0..) |entry, index| if (entry) |cached| {
            if (oldest == null or cached.used < self.cache[oldest.?].?.used) oldest = index;
        };
        const index = oldest orelse return false;
        self.cache[index].?.block.release();
        self.cache[index] = null;
        return true;
    }
    fn retained(self: *const PayloadReader) usize {
        var bytes: usize = 0;
        for (self.cache) |entry| if (entry) |cached| {
            bytes +|= cached.block.retainedBytes();
        };
        return bytes;
    }
    fn resolveBody(raw: *anyopaque, offset: u64) anyerror!*Sequential.OwnedBlock {
        const self: *PayloadReader = @ptrCast(@alignCast(raw));
        self.pages.body_only = true;
        defer self.pages.body_only = false;
        const payload = try self.readWindow(.{ .offset = offset, .index = 0 });
        return payload.block;
    }
    pub fn read(self: *PayloadReader, reference: Reference) !Payload {
        return self.readWindow(reference) catch |err| return if (err == error.SqlPayloadWindowFull) error.SqlProgramLimitExceeded else err;
    }
    fn readWindow(self: *PayloadReader, reference: Reference) anyerror!Payload {
        self.pages.resolver = .{ .ptr = self, .read = resolveBody };
        self.clock +|= 1;
        // On misses keep existing owners until decode admission needs them:
        // a new parent may reuse a body already leased by a cached parent.
        const known = for (self.cache) |entry| {
            if (entry) |cached| if (cached.offset == reference.offset) break true;
        } else false;
        // Include growth of lazy scalar caches since the preceding pull.
        while (known and self.retained() > 64 * 1024) {
            var same: bool = true;
            for (self.cache) |entry| if (entry) |cached| {
                if (cached.offset != reference.offset) same = false;
            };
            if (same or !evict(self)) break;
        }
        for (&self.cache) |*entry| if (entry.*) |*cached| if (cached.offset == reference.offset) {
            if (reference.index >= cached.block.count()) return error.InvalidSqlSpill;
            cached.used = self.clock;
            cached.block.retain();
            return .{ .block = cached.block, .index = reference.index };
        };
        for (&self.cache) |*entry| if (entry.*) |*cached| {
            for (cached.block.dependencies.items) |body| if (body.physical_offset == reference.offset) {
                if (reference.index >= body.count()) return error.InvalidSqlSpill;
                cached.used = self.clock;
                body.retain();
                return .{ .block = body, .index = reference.index };
            };
        };
        var pressure: CachePressure = .{ .manager = self.pages.file.manager, .context = self, .evict = evict, .bounded = true, .reserve = self.reserve_bytes };
        const block = self.pages.readWithPressure(reference.offset, &pressure) catch |err| {
            if (err == error.OutOfMemory and pressure.denied) return error.SqlPayloadWindowFull;
            return err;
        };
        errdefer block.release();
        if (reference.index >= block.count()) return error.InvalidSqlSpill;
        self.reads += 1;
        while (block.retainedBytes() +| self.retained() > 64 * 1024) if (!evict(self)) break;
        var slot: ?usize = null;
        for (self.cache, 0..) |entry, i| if (entry == null) {
            slot = i;
            break;
        };
        if (slot == null) {
            _ = evict(self);
            for (self.cache, 0..) |entry, i| if (entry == null) {
                slot = i;
                break;
            };
        }
        self.cache[slot.?] = .{ .offset = reference.offset, .block = block, .used = self.clock };
        block.retain();
        return .{ .block = block, .index = reference.index };
    }
    /// Advisory wire-based estimate. Actual allocations and retained page
    /// bytes are authoritative; gatherForDelivery can shorten a cold-page window.
    pub fn estimate(self: *PayloadReader, offset: u64) !usize {
        for (self.cache) |entry| if (entry) |cached| if (cached.offset == offset) return cached.block.retainedBytes();
        var pressure: CachePressure = .{ .manager = self.pages.file.manager, .context = self, .evict = evict };
        if (self.pages.file.read_buffer.len == 0) pressure.prepare(@max(1, self.pages.file.buffer_bytes orelse self.pages.file.manager.buffer_bytes));
        var header: [17]u8 = undefined;
        try self.pages.file.readRaw(offset, &header);
        var length = std.mem.readInt(u64, header[0..8], .little);
        const native_bytes: usize = if (header[16] >= 2) 9 else 0;
        if (header[16] > 3 or length < native_bytes or length - native_bytes > self.pages.file.manager.max_record_bytes or length > self.pages.file.size -| (offset + header.len)) return error.InvalidSqlSpill;
        length -= native_bytes;
        if (header[16] & 1 == 1) {
            var size_prefix: [10]u8 = undefined;
            const count: usize = @intCast(@min(length, size_prefix.len));
            try self.pages.file.readRaw(offset + header.len + native_bytes, size_prefix[0..count]);
            length = snappy.decodedLen(size_prefix[0..count]) catch return error.InvalidSqlSpill;
            if (length > self.pages.file.manager.max_record_bytes) return error.InvalidSqlSpill;
        }
        // Include a logical-row allowance for wide sparse columns. Frozen
        // cells and referenced bodies need not scale with wire bytes; actual
        // decoded owners and allocation admission bound gatherForDelivery.
        return @max(@as(usize, @intCast(length)) *| 2 +| 4096, self.row_bytes *| 2 +| 4096);
    }
    /// Gather once per physical page, restore input order, and return one owned
    /// reference per lane. Callers bound the footprint before consuming input.
    pub const Page = struct {
        values: @import("execution_batch.zig").Batch,
        payloads: []Payload,
        a: Allocator,
        pub fn release(self: Page) void {
            for (self.payloads) |payload| payload.release();
            self.a.free(self.payloads);
            self.a.destroy(@as(*PageView, @ptrCast(@alignCast(self.values.reader.ptr))));
        }
    };
    pub fn target(self: *PayloadReader, byte_limit: usize) usize {
        var limit = @min(@max(1, byte_limit), 128 * 1024);
        if (@import("memory_budget.zig").headroom(self.pages.file.manager.alloc)) |available| limit = @min(limit, available);
        return limit;
    }
    pub fn prefix(self: *PayloadReader, references: []const Reference, byte_limit: usize) !usize {
        const limit = self.target(byte_limit);
        var bytes: usize = 0;
        var count: usize = 0;
        for (references, 0..) |reference, index| {
            const repeated = for (references[0..index]) |prior| {
                if (prior.offset == reference.offset) break true;
            } else false;
            const cost = if (repeated) 0 else try self.estimate(reference.offset);
            if (index != 0 and cost > limit -| bytes) break;
            bytes +|= cost;
            count += 1;
        }
        return count;
    }
    /// References are cheap to inspect. Select a bounded prefix before any
    /// decode, gather in physical order, and expose frozen cells in rank order.
    pub fn nativePage(self: *PayloadReader, a: Allocator, input: @import("execution_batch.zig").Batch, width: usize, byte_limit: usize) !Page {
        if (input.width() != 2) return error.InvalidSqlSpill;
        var references: [64]Reference = undefined;
        const maximum = @min(input.len(), references.len);
        for (references[0..maximum], 0..) |*reference, row| reference.* = try Reference.decode(&.{ try input.cell(a, row, 0), try input.cell(a, row, 1) });
        const count = try self.prefix(references[0..maximum], byte_limit);
        var storage: [64]Payload = undefined;
        const gathered = try self.gatherForDelivery(&storage, references[0..count], byte_limit, @sizeOf(Payload), @sizeOf(PageView), 0);
        const selected = gathered.storage[0..gathered.count];
        errdefer for (selected) |payload| payload.release();
        for (selected) |payload| if (payload.block.width() != width) return error.InvalidSqlSpill;
        return ownPage(self.pages.file.manager.allocator(), selected, width);
    }
    fn ownPage(stable: Allocator, selected: []const Payload, width: usize) !Page {
        const payloads = try stable.dupe(Payload, selected);
        errdefer stable.free(payloads);
        const view = try stable.create(PageView);
        view.* = .{ .payloads = payloads };
        return .{ .values = .{ .reader = .{ .ptr = view, .read = PageView.cell, .read_frozen = PageView.frozen, .read_json = PageView.json, .read_identity = PageView.identity, .read_dictionary = PageView.dictionary, .count = payloads.len, .width = width } }, .payloads = payloads, .a = stable };
    }
    const PageView = struct {
        payloads: []const Payload,
        fn shared(self_: *const @This()) bool {
            if (self_.payloads.len == 0) return false;
            for (self_.payloads) |payload| if (payload.block != self_.payloads[0].block) return false;
            return true;
        }
        fn identity(raw: *anyopaque, row: usize, column: usize) anyerror!?u64 {
            const self_: *@This() = @ptrCast(@alignCast(raw));
            if (!self_.shared()) return null;
            const payload = self_.payloads[row];
            return if (payload.block.encoded) |encoded| encoded.dictionaryIdentity(payload.index, column, false) else null;
        }
        fn dictionary(raw: *anyopaque, alloc: Allocator, column: usize) anyerror!?@import("execution_batch.zig").Batch {
            const self_: *@This() = @ptrCast(@alignCast(raw));
            if (!self_.shared()) return null;
            const Batch = @import("execution_batch.zig").Batch;
            const source = try alloc.create(Batch);
            source.* = try self_.payloads[0].block.batch(alloc, false);
            const indices = try alloc.alloc(usize, self_.payloads.len);
            for (indices, self_.payloads) |*index, payload| index.* = payload.index;
            return (try source.select(alloc, indices)).dictionaryColumn(alloc, column);
        }
        fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            const self_: *@This() = @ptrCast(@alignCast(raw));
            const payload = self_.payloads[row];
            return payload.block.cell(payload.index, column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self_: *@This() = @ptrCast(@alignCast(raw));
            const payload = self_.payloads[row];
            return if (payload.block.encoded) |encoded| encoded.frozenJson(payload.index, column, false) else null;
        }
        fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
            const self_: *@This() = @ptrCast(@alignCast(raw));
            const payload = self_.payloads[row];
            return if (payload.block.encoded) |encoded| encoded.encodedJson(payload.index, column, false) else null;
        }
    };
    // A sort owns real key leases that cannot be discarded when a gather
    // shrinks. Native/replay delivery has no such backlog. Account for the
    // caller's fixed backlog separately from each candidate's output arrays.
    fn gatherForDelivery(self: *PayloadReader, storage: []Payload, references: []const Reference, byte_limit: usize, output_row_bytes: usize, extra_bytes: usize, backlog_bytes: usize) !Gathered {
        if (references.len > 64 or storage.len < references.len) return error.InvalidSqlLimit;
        var count = references.len;
        const previous_reserve = self.reserve_bytes;
        defer self.reserve_bytes = previous_reserve;
        while (true) {
            self.reserve_bytes = previous_reserve + backlog_bytes + count * output_row_bytes + extra_bytes;
            const gathered = self.gather(storage[0..count], references[0..count], byte_limit) catch |err| {
                if (err != error.SqlPayloadWindowFull) return err;
                if (count <= 1) return error.SqlProgramLimitExceeded;
                self.window_reductions += 1;
                count = (count + 1) / 2;
                continue;
            };
            const payloads = gathered.storage[0..gathered.count];
            errdefer for (payloads) |payload| payload.release();
            // Cache hits perform no decode allocations, so they bypass the
            // read scope's admission check. Admit final descriptors as well;
            // evict unneeded cache owners while the selected leases stay pinned.
            self.reserve_bytes = previous_reserve + backlog_bytes + payloads.len * output_row_bytes + extra_bytes;
            var pressure: CachePressure = .{ .manager = self.pages.file.manager, .context = self, .evict = evict, .bounded = true, .reserve = self.reserve_bytes };
            if (payloads.len != 0 and !pressure.admit(0)) {
                if (count <= 1) return error.SqlProgramLimitExceeded;
                for (payloads) |payload| payload.release();
                self.window_reductions += 1;
                count = (count + 1) / 2;
                continue;
            }
            if (payloads.len < references.len) self.window_reductions += 1;
            return gathered;
        }
    }
    const Gathered = struct { storage: []Payload, count: usize };
    // Preserve a completed rank prefix instead of discarding decoded pages
    // when allocation/byte admission ends a physically ordered gather early.
    fn completedPrefix(output: []Payload, order: []const usize, done: usize) ?Gathered {
        var ready: [64]bool = @splat(false);
        for (order[0..done]) |index| ready[index] = true;
        var count: usize = 0;
        while (count < output.len and ready[count]) count += 1;
        if (count == 0) return null;
        for (order[0..done]) |index| if (index >= count) output[index].release();
        return .{ .storage = output, .count = count };
    }
    fn ownerBytes(block: *Sequential.OwnedBlock, owners: *[128]*Sequential.OwnedBlock, count: *usize) usize {
        for (owners[0..count.*]) |prior| if (prior == block) return 0;
        // Overflow falls back to conservative accounting, never undercounting.
        if (count.* < owners.len) {
            owners[count.*] = block;
            count.* += 1;
        }
        var bytes = block.ownRetainedBytes();
        for (block.dependencies.items) |body| bytes +|= ownerBytes(body, owners, count);
        return bytes;
    }
    fn gather(self: *PayloadReader, output: []Payload, references: []const Reference, byte_limit: usize) !Gathered {
        std.debug.assert(output.len == references.len and references.len <= 64);
        var order: [64]usize = undefined;
        for (order[0..references.len], 0..) |*index, i| index.* = i;
        std.mem.sort(usize, order[0..references.len], references, struct {
            fn less(refs: []const Reference, left: usize, right: usize) bool {
                return refs[left].offset < refs[right].offset;
            }
        }.less);
        var done: usize = 0;
        errdefer for (order[0..done]) |index| output[index].release();
        var bytes: usize = 0;
        var previous: ?u64 = null;
        var owners: [128]*Sequential.OwnedBlock = undefined;
        var owner_count: usize = 0;
        const target_bytes = self.target(byte_limit);
        for (order[0..references.len]) |index| {
            const payload = self.readWindow(references[index]) catch |err| {
                if (err == error.SqlPayloadWindowFull) if (completedPrefix(output, order[0..references.len], done)) |prefix_| return prefix_;
                return err;
            };
            if (previous == null or previous.? != references[index].offset) {
                const cost = ownerBytes(payload.block, &owners, &owner_count);
                if (done != 0 and cost > target_bytes -| bytes) {
                    payload.release();
                    if (completedPrefix(output, order[0..references.len], done)) |prefix_| return prefix_;
                    return error.SqlPayloadWindowFull;
                }
                bytes +|= cost;
                previous = references[index].offset;
            }
            output[index] = payload;
            done += 1;
        }
        return .{ .storage = output, .count = references.len };
    }
};

pub const Sort = struct {
    const Entry = struct { normalized: ?@import("sort_key.zig").Key, position: usize, ordinal: u64 };
    // Keep resident/narrow sorts inline. At the first spill of a wide run,
    // migrate payloads once to physical pages and sort two-integer references
    // in an ordinary narrow Sort. Later merge passes never rewrite documents.
    // Once an inline run exists, keep that representation for the whole sort.
    allow_indirect: bool = true,
    wide_values: bool = false,
    order_sort: ?*Sort = null,
    payloads: ?*JoinPayloads = null,
    payload_reader: ?*PayloadReader = null,
    deferred_references: []?RowLease = &.{},
    deferred_index: usize = 0,
    deferred_count: usize = 0,
    payload_reads: usize = 0,
    manager: *Manager,
    a: Allocator,
    orders: []const operators.Order,
    memory_bytes: usize,
    merge_fan_in: usize = 8,
    /// One ascending, already memcomparable byte key (native index builders).
    encoded_bytes: bool = false,
    arena: std.heap.ArenaAllocator,
    // Sorting moves only memcomparable prefixes, stable payload positions and
    // ordinals. Typed stores retain payloads and authoritative fallback keys.
    rows: std.ArrayList(Entry) = .empty,
    values: @import("typed_store.zig").Store,
    keys: @import("typed_store.zig").Store,
    estimated: usize = 0,
    runs: [32]?Sequential = @splat(null),
    /// Cohort callers also reserve file capacity for pending runs and merges.
    run_limit: usize = 32,
    /// Archive retained runs into anonymous reusable extents. Cohorts retain
    /// balanced logical levels while reserving only one descriptor per sort.
    shared_runs: bool = false,
    run_store: ?*RunStore = null,
    owns_run_store: bool = true,
    run_levels: [32]u8 = @splat(0),
    outputs: [8]?Sequential = @splat(null),
    heads: [8]?Decoded = @splat(null),
    owned_heads: [8]?*Sequential.OwnedBlock = @splat(null),
    owned_positions: [8]usize = @splat(0),
    pending_heads: [8]bool = @splat(false),
    leased_reads: bool = false,
    head_arenas: [8]std.heap.ArenaAllocator = undefined,
    output_count: usize = 0,
    max_row_bytes: usize = 0,
    max_payload_bytes: usize = 0,
    offset: u64 = 0,
    finished: bool = false,
    total: usize = 0,
    /// The caller guarantees input order according to `orders`. Stream one
    /// run without sorting or repeatedly merging already ordered prefixes.
    presorted: bool = false,
    presorted_run: ?Sequential = null,
    parallel_runs: bool = true,
    pending_run: ?*RunJob = null,
    parallel_runs_started: usize = 0,
    parallel_merges_started: usize = 0,
    radix_runs: usize = 0,
    const MergeJob = struct {
        sort: Sort,
        inputs: [8]?Sequential = @splat(null),
        count: usize = 0,
        task: ?@import("parallel_scheduler.zig").Task(anyerror!Sequential) = null,
        fn run(self: *MergeJob) anyerror!Sequential {
            var pointers: [8]*Sequential = undefined;
            for (self.inputs[0..self.count], pointers[0..self.count]) |*file, *pointer| pointer.* = &file.*.?;
            return self.sort.mergeMany(pointers[0..self.count]);
        }
        fn close(self: *MergeJob) void {
            if (self.task) |*task| {
                if (task.cancel(self.sort.manager.io)) |result| {
                    var file = result;
                    file.close();
                } else |_| {}
            }
            for (self.inputs[0..self.count]) |*file| if (file.*) |*open| open.close();
            const a = self.sort.a;
            self.sort.deinit();
            a.destroy(self);
        }
    };
    const RunJob = struct {
        sort: Sort,
        task: ?@import("parallel_scheduler.zig").Task(anyerror!Sequential) = null,
        fn run(self: *RunJob) anyerror!Sequential {
            try self.sort.sortRows();
            var file = try self.sort.createRun();
            errdefer file.close();
            try self.sort.writeRows(&file);
            try file.seal();
            return file;
        }
        fn destroy(self: *RunJob) void {
            const a = self.sort.a;
            self.sort.arena.deinit();
            self.sort.values.deinit();
            self.sort.keys.deinit();
            self.sort.rows.deinit(a);
            a.destroy(self);
        }
    };
    fn collectRun(self: *Sort) !void {
        const job = self.pending_run orelse return;
        const result = job.task.?.await(self.manager.io);
        self.pending_run = null;
        job.destroy();
        try self.admitRun(try result);
    }
    pub fn init(a: Allocator, manager: *Manager, orders: []const operators.Order, memory_bytes: usize) Sort {
        _ = a;
        const backing = manager.allocator();
        return .{ .manager = manager, .a = backing, .orders = orders, .memory_bytes = memory_bytes, .arena = std.heap.ArenaAllocator.init(backing), .values = .init(backing), .keys = .init(backing) };
    }
    pub fn deinit(self: *Sort) void {
        for (self.deferred_references[self.deferred_index..self.deferred_count]) |lease| lease.?.release();
        self.a.free(self.deferred_references);
        if (self.payload_reader) |reader| {
            reader.deinit();
            self.a.destroy(reader);
        }
        if (self.order_sort) |inner| {
            inner.deinit();
            self.a.destroy(inner);
        }
        if (self.payloads) |payloads| {
            payloads.close();
            self.a.destroy(payloads);
        }
        if (self.presorted_run) |*run| run.close();
        if (self.pending_run) |job| {
            if (job.task.?.cancel(self.manager.io)) |value| {
                var file = value;
                file.close();
            } else |_| {}
            job.destroy();
            self.pending_run = null;
        }
        for (self.owned_heads) |block| if (block) |owner| owner.release();
        self.arena.deinit();
        self.values.deinit();
        self.keys.deinit();
        self.rows.deinit(self.a);
        for (&self.runs) |*run| if (run.*) |*file| file.close();
        for (self.outputs[0..self.output_count], self.head_arenas[0..self.output_count]) |*file, *arena| {
            if (file.*) |*open_file| open_file.close();
            arena.deinit();
        }
        if (self.owns_run_store) if (self.run_store) |store| store.deinit();
    }
    pub fn add(self: *Sort, input: Row) anyerror!void {
        if (self.finished) return error.InvalidSqlBackendResponse;
        if (self.order_sort != null) {
            try self.appendIndirect(.{ .rows = &.{input.values} }, &.{input.keys}, &.{input.ordinal});
            self.total += 1;
            return;
        }
        var row = input;
        row.normalized = try self.normalize(row.keys);
        if (self.finished or row.keys.len != self.orders.len) return error.InvalidSqlBackendResponse;
        for (row.keys) |key| if (!key.sql_null) {
            _ = try scalar.compareDatums(key, key);
        };
        try self.manager.check();
        var bytes: usize = @sizeOf(Row);
        for (row.values) |v| bytes +|= try operators.datumBytes(v);
        self.wide_values = self.wide_values or bytes > 1024;
        self.max_payload_bytes = @max(self.max_payload_bytes, bytes);
        for (row.keys) |v| bytes +|= try operators.datumBytes(v);
        self.max_row_bytes = @max(self.max_row_bytes, bytes);
        if (self.presorted) {
            self.allow_indirect = false;
            if (self.presorted_run == null) self.presorted_run = try self.createRun();
            _ = try self.presorted_run.?.append(row, none);
            self.total += 1;
            return;
        }
        try self.collectPresortedRun();
        if (self.allow_indirect and self.wide_values and bytes > self.memory_bytes / 3) {
            try self.startIndirect();
            try self.appendIndirect(.{ .rows = &.{row.values} }, &.{row.keys}, &.{row.ordinal});
            self.total += 1;
            return;
        }
        if (bytes > self.memory_bytes / 3) {
            // A legal row may exceed the preferred in-memory run size. Encode
            // it directly as a singleton run without copying it into the heap;
            // record encoding and merge heads use the statement allocator.
            try self.flush();
            try self.collectRun();
            const run = blk: {
                var file = try self.createRun();
                errdefer file.close();
                _ = try file.append(row, none);
                try file.seal();
                break :blk file;
            };
            try self.admitRun(run);
            self.total += 1;
            return;
        }
        if (self.rows.items.len != 0 and (self.values.columns.len != row.values.len or self.keys.columns.len != row.keys.len)) try self.flush();
        if (self.order_sort != null) return self.add(row);
        var retained = @sizeOf(Entry) +| try self.values.appendBytes(row.values) +| try self.keys.appendBytes(row.keys);
        if (self.rows.items.len != 0 and (retained > self.memory_bytes / (if (self.parallel_runs and self.memory_bytes >= 128 * 1024) @as(usize, 8) else 4) -| self.estimated)) {
            try self.flush();
            if (self.order_sort != null) return self.add(row);
            retained = @sizeOf(Entry) +| try self.values.appendBytes(row.values) +| try self.keys.appendBytes(row.keys);
        }
        try self.rows.ensureUnusedCapacity(self.a, 1);
        const position = self.values.len;
        _ = try self.values.append(row.values);
        _ = try self.keys.append(row.keys);
        self.rows.appendAssumeCapacity(.{ .position = position, .ordinal = row.ordinal, .normalized = row.normalized });
        self.estimated +|= retained;
        self.total += 1;
    }
    /// Retain compact payload columns, evaluating only sort keys as Datums.
    /// Oversized singleton runs encode directly without constructing a DOM.
    pub fn addBatch(self: *Sort, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum, ordinals: []const u64) anyerror!void {
        if (self.finished or batch.len() != keys.len or keys.len != ordinals.len) return error.InvalidSqlBackendResponse;
        if (self.order_sort != null) {
            try self.appendIndirect(batch, keys, ordinals);
            self.total += batch.len();
            return;
        }
        var begin: usize = 0;
        while (begin < batch.len()) {
            const end = @min(batch.len(), begin + 64);
            var lease = @import("typed_store.zig").Store.JsonLease.init(self.a);
            defer lease.deinit();
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            for (begin..end) |index| {
                try self.manager.check();
                _ = scratch.reset(.free_all);
                const a = scratch.allocator();
                const row_keys = keys[index];
                if (row_keys.len != self.orders.len) return error.InvalidSqlBackendResponse;
                for (row_keys) |key| if (!key.sql_null) {
                    _ = try scalar.compareDatums(key, key);
                };
                const normalized = try self.normalize(row_keys);
                const selected = try batch.select(a, &.{index});
                if (self.order_sort != null) {
                    return self.addIndirectTail(batch, index, keys, ordinals);
                }
                var bytes: usize = @sizeOf(Row);
                for (0..batch.width()) |column| bytes +|= try selected.cellBytes(a, 0, column);
                self.wide_values = self.wide_values or bytes > 1024;
                self.max_payload_bytes = @max(self.max_payload_bytes, bytes);
                for (row_keys) |key| bytes +|= try operators.datumBytes(key);
                self.max_row_bytes = @max(self.max_row_bytes, bytes);
                if (self.presorted) {
                    self.allow_indirect = false;
                    if (self.presorted_run == null) self.presorted_run = try self.createRun();
                    try self.presorted_run.?.appendBatch(selected, .{ .rows = &.{row_keys} }, ordinals[index..][0..1]);
                    self.total += 1;
                    continue;
                }
                try self.collectPresortedRun();
                if (self.rows.items.len != 0 and (self.values.columns.len != batch.width() or self.keys.columns.len != self.orders.len)) {
                    try self.flush();
                    lease.forgetOwned();
                }
                if (self.order_sort != null) {
                    return self.addIndirectTail(batch, index, keys, ordinals);
                }
                const target = self.memory_bytes / (if (self.parallel_runs and self.memory_bytes >= 128 * 1024) @as(usize, 8) else 4);
                var metadata = if (self.values.initialized) @as(usize, 0) else @import("typed_store.zig").columnMetadataBytes(batch.width());
                var retained = metadata +| @sizeOf(Entry) +| try self.values.batchAppendBytesLeased(a, selected, target, &lease) +| try self.keys.appendBytes(row_keys);
                if (self.rows.items.len != 0 and retained > target -| self.estimated) {
                    try self.flush();
                    lease.forgetOwned();
                    if (self.order_sort != null) {
                        return self.addIndirectTail(batch, index, keys, ordinals);
                    }
                    metadata = @import("typed_store.zig").columnMetadataBytes(batch.width());
                    retained = metadata +| @sizeOf(Entry) +| try self.values.batchAppendBytesLeased(a, selected, target, &lease) +| try self.keys.appendBytes(row_keys);
                }
                if (retained > target and self.allow_indirect and self.wide_values) {
                    try self.startIndirect();
                    return self.addIndirectTail(batch, index, keys, ordinals);
                } else if (retained > target) {
                    try self.collectRun();
                    const run = blk: {
                        var file = try self.createRun();
                        errdefer file.close();
                        try file.appendBatch(selected, .{ .rows = &.{row_keys} }, ordinals[index..][0..1]);
                        try file.seal();
                        break :blk file;
                    };
                    try self.admitRun(run);
                } else {
                    try self.rows.ensureUnusedCapacity(self.a, 1);
                    const position = self.values.len;
                    try self.values.appendBatchLeased(selected, &lease);
                    _ = try self.keys.append(row_keys);
                    self.rows.appendAssumeCapacity(.{ .position = position, .ordinal = ordinals[index], .normalized = normalized });
                    self.estimated +|= retained;
                }
                self.total += 1;
            }
            begin = end;
        }
    }
    fn addIndirectTail(self: *Sort, batch: @import("execution_batch.zig").Batch, begin: usize, keys: []const []const Datum, ordinals: []const u64) !void {
        var cursor = begin;
        while (cursor < batch.len()) {
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            const end = @min(batch.len(), cursor + 64);
            var indices: [64]usize = undefined;
            for (indices[0 .. end - cursor], cursor..) |*index, i| index.* = i;
            try self.appendIndirect(try batch.select(scratch.allocator(), indices[0 .. end - cursor]), keys[cursor..end], ordinals[cursor..end]);
            self.total += end - cursor;
            cursor = end;
        }
    }
    fn startIndirect(self: *Sort) anyerror!void {
        if (self.order_sort != null) return;
        std.debug.assert(self.allow_indirect);
        const inner = try self.a.create(Sort);
        inner.* = Sort.init(self.a, self.manager, self.orders, self.memory_bytes);
        inner.allow_indirect = false;
        inner.encoded_bytes = self.encoded_bytes;
        inner.parallel_runs = self.parallel_runs;
        inner.shared_runs = self.shared_runs;
        inner.run_limit = self.run_limit;
        inner.merge_fan_in = self.merge_fan_in;
        self.order_sort = inner;
        var begin: usize = 0;
        while (begin < self.rows.items.len) {
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            const a = scratch.allocator();
            const end = @min(self.rows.items.len, begin + 64);
            var positions: [64]usize = undefined;
            var keys: [64][]const Datum = undefined;
            var ordinals: [64]u64 = undefined;
            for (self.rows.items[begin..end], 0..) |entry, i| {
                positions[i] = entry.position;
                keys[i] = try self.keys.row(a, entry.position);
                ordinals[i] = entry.ordinal;
            }
            const values: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &self.values, .count = self.values.len } };
            try self.appendIndirect(try values.select(a, positions[0 .. end - begin]), keys[0 .. end - begin], ordinals[0 .. end - begin]);
            begin = end;
        }
        self.values.deinit();
        self.keys.deinit();
        self.values = .init(self.a);
        self.keys = .init(self.a);
        self.rows.clearAndFree(self.a);
        _ = self.arena.reset(.free_all);
        self.estimated = 0;
    }
    fn appendIndirect(self: *Sort, values: @import("execution_batch.zig").Batch, keys: []const []const Datum, ordinals: []const u64) anyerror!void {
        if (values.len() != keys.len or keys.len != ordinals.len) return error.InvalidSqlBackendResponse;
        if (values.len() > 64) {
            var begin: usize = 0;
            while (begin < values.len()) {
                var scratch = std.heap.ArenaAllocator.init(self.a);
                defer scratch.deinit();
                const end = @min(values.len(), begin + 64);
                var positions: [64]usize = undefined;
                for (positions[0 .. end - begin], begin..) |*position, i| position.* = i;
                try self.appendIndirect(try values.select(scratch.allocator(), positions[0 .. end - begin]), keys[begin..end], ordinals[begin..end]);
                begin = end;
            }
            return;
        }
        // Validate before publishing payload bytes, including a singleton key.
        for (keys) |row| {
            if (row.len != self.orders.len) return error.InvalidSqlBackendResponse;
            _ = try self.normalize(row);
            for (row) |key| if (!key.sql_null) {
                _ = try scalar.compareDatums(key, key);
            };
        }
        if (self.payloads == null) {
            const owner = try self.a.create(JoinPayloads);
            errdefer self.a.destroy(owner);
            owner.* = try JoinPayloads.init(self.manager);
            self.payloads = owner;
        }
        // Exact native/primitive row fingerprints share offsets within this
        // immutable producer lease. No borrowed identity survives the call.
        var unique: [64]usize = undefined;
        var hashes: [64]?u64 = undefined;
        var remap: [64]usize = undefined;
        var count: usize = 0;
        var cells = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(self.a));
        defer cells.deinit();
        var reuse: JoinPayloads.BodyReuse = .{};
        for (0..values.len()) |row| {
            try self.manager.check();
            _ = cells.reset(.free_all);
            const temporary = cells.allocator();
            var row_bytes: usize = @sizeOf(Row);
            for (0..values.width()) |column| row_bytes +|= try values.cellBytes(temporary, row, column);
            self.max_payload_bytes = @max(self.max_payload_bytes, row_bytes);
            for (keys[row]) |key| row_bytes +|= try operators.datumBytes(key);
            self.max_row_bytes = @max(self.max_row_bytes, row_bytes);
            const hash = try reuse.fingerprints.rowHash(temporary, values, row);
            const prior = if (hash) |value| blk: {
                for (0..count) |i| if (hashes[i] != null and hashes[i].? == value) {
                    _ = cells.reset(.free_all);
                    if (try nativeRowsEqual(cells.allocator(), values, unique[i], row)) break :blk i;
                };
                break :blk null;
            } else @as(?usize, null);
            if (prior) |index| remap[row] = index else {
                remap[row] = count;
                unique[count] = row;
                hashes[count] = hash;
                count += 1;
            }
        }
        var selected_refs: [64]PayloadReader.Reference = undefined;
        try self.payloads.?.appendCohort(values, unique[0..count], selected_refs[0..count], &reuse, 32 * 1024, null);
        var references: [64][2]Datum = undefined;
        for (references[0..count], selected_refs[0..count]) |*reference, selected| reference.* = .{ Datum.json(.{ .integer = std.math.cast(i64, selected.offset) orelse return error.SqlProgramLimitExceeded }), Datum.json(.{ .integer = @intCast(selected.index) }) };
        var rows: [64][]const Datum = undefined;
        for (rows[0..values.len()], remap[0..values.len()]) |*row, index| row.* = &references[index];
        try self.order_sort.?.addBatch(.{ .rows = rows[0..values.len()] }, keys, ordinals);
    }
    pub fn payloadReader(self: *Sort) !*PayloadReader {
        if (self.payload_reader == null) {
            const reader = try self.a.create(PayloadReader);
            reader.* = .init(self.payloads.?);
            reader.row_bytes = self.max_payload_bytes;
            self.payload_reader = reader;
        }
        return self.payload_reader.?;
    }
    fn readPayload(self: *Sort, values: []const Datum) !PayloadReader.Payload {
        const reader = try self.payloadReader();
        defer self.payload_reads = reader.reads;
        return reader.read(try PayloadReader.Reference.decode(values));
    }
    /// Transfer the sealed payload file to a replay owner. Ordered references
    /// can then be spooled independently, without rewriting any document bytes.
    pub fn takePayloads(self: *Sort) !?*JoinPayloads {
        if (self.order_sort == null) return null;
        try self.finishImpl(true);
        const pages = self.payloads;
        self.payloads = null;
        return pages;
    }
    fn deferReferences(self: *Sort, leases: []?RowLease) !void {
        if (leases.len == 0) return;
        var pending: [64]?RowLease = @splat(null);
        const remaining = self.deferred_count - self.deferred_index;
        const total = leases.len + remaining;
        std.debug.assert(total <= pending.len);
        const destination = if (self.deferred_references.len >= total) self.deferred_references else try self.a.alloc(?RowLease, total);
        for (leases, pending[0..leases.len]) |*source, *dest| {
            dest.* = source.*;
            source.* = null;
        }
        @memcpy(pending[leases.len..][0..remaining], self.deferred_references[self.deferred_index..self.deferred_count]);
        @memcpy(destination[0..total], pending[0..total]);
        if (destination.ptr != self.deferred_references.ptr) self.a.free(self.deferred_references);
        self.deferred_references = destination;
        self.deferred_index = 0;
        self.deferred_count = total;
    }
    pub fn nextReference(self: *Sort) anyerror!?RowLease {
        return self.nextReferenceWithPressure(null);
    }
    fn nextReferenceWithPressure(self: *Sort, pressure: ?*CachePressure) anyerror!?RowLease {
        try self.finishImpl(true);
        if (self.deferred_index < self.deferred_count) {
            const index = self.deferred_index;
            self.deferred_index += 1;
            return self.deferred_references[index].?;
        }
        return self.order_sort.?.nextLeasedWithPressure(pressure);
    }
    fn normalize(self: *const Sort, keys: []const Datum) !?@import("sort_key.zig").Key {
        if (!self.encoded_bytes) return @import("sort_key.zig").encode(keys, self.orders);
        if (keys.len != 1 or self.orders.len != 1 or self.orders[0].descending or keys[0].sql_null or keys[0].numeric != null or keys[0].array != null or keys[0].value != .string) return error.InvalidSqlBackendResponse;
        const bytes = keys[0].value.string;
        var key: @import("sort_key.zig").Key = .{ .types = std.math.maxInt(u64), .len = @intCast(@min(32, bytes.len)), .complete = bytes.len <= 32 };
        @memcpy(key.bytes[0..key.len], bytes[0..key.len]);
        return key;
    }
    fn compare(self: *const Sort, left: Row, right: Row) !std.math.Order {
        if (!self.encoded_bytes) return operators.compareRows(left, right, self.orders);
        const order = std.mem.order(u8, left.keys[0].value.string, right.keys[0].value.string);
        return if (order == .eq) std.math.order(left.ordinal, right.ordinal) else order;
    }
    fn lessEncoded(self: *Sort, left: Entry, right: Entry) bool {
        // Typed byte columns return borrowed slices without row reconstruction.
        const l = self.keys.cell(self.a, left.position, 0) catch unreachable;
        const r = self.keys.cell(self.a, right.position, 0) catch unreachable;
        const order = std.mem.order(u8, l.value.string, r.value.string);
        return if (order == .eq) left.ordinal < right.ordinal else order == .lt;
    }
    fn radixEncoded(self: *Sort) !bool {
        if (self.rows.items.len < 1024) return false;
        const scratch = try self.a.alloc(Entry, self.rows.items.len);
        defer self.a.free(scratch);
        var source = self.rows.items;
        var target = scratch;
        // 0 is end-of-key, 1..256 are bytes. This preserves prefix ordering,
        // embedded zeroes and variable lengths without re-escaping tuples.
        var pass: usize = 40;
        while (pass != 0) {
            pass -= 1;
            var counts: [257]usize = @splat(0);
            for (source) |row| counts[encodedByte(row, pass)] += 1;
            if (for (counts) |count| {
                if (count == source.len) break true;
            } else false) continue;
            var offsets: [257]usize = undefined;
            var offset: usize = 0;
            for (counts, &offsets) |count, *start| {
                start.* = offset;
                offset += count;
            }
            for (source) |row| {
                const byte = encodedByte(row, pass);
                target[offsets[byte]] = row;
                offsets[byte] += 1;
            }
            std.mem.swap([]Entry, &source, &target);
        }
        if (source.ptr != self.rows.items.ptr) @memcpy(self.rows.items, source);
        // Only equal truncated prefixes need authoritative full-byte compares.
        var begin: usize = 0;
        while (begin < source.len) {
            var end = begin + 1;
            const first = self.rows.items[begin].normalized.?;
            while (end < source.len) : (end += 1) {
                const key = self.rows.items[end].normalized.?;
                if (key.len != first.len or !std.mem.eql(u8, key.bytes[0..key.len], first.bytes[0..first.len])) break;
            }
            if (for (self.rows.items[begin..end]) |row| {
                if (!row.normalized.?.complete) break true;
            } else false) std.sort.pdq(Entry, self.rows.items[begin..end], self, lessEncoded);
            begin = end;
        }
        self.radix_runs += 1;
        return true;
    }
    fn encodedByte(row: Entry, pass: usize) usize {
        const key = row.normalized.?;
        return if (pass < 32) (if (pass < key.len) @as(usize, key.bytes[pass]) + 1 else 0) else @as(u8, @truncate(row.ordinal >> @as(u6, @intCast((39 - pass) * 8))));
    }
    fn radixRows(self: *Sort) !bool {
        if (self.encoded_bytes) return self.radixEncoded();
        if (self.rows.items.len < 1024) return false;
        const first = self.rows.items[0].normalized orelse return false;
        if (!first.complete) return false;
        for (self.rows.items) |row| {
            const key = row.normalized orelse return false;
            if (!key.complete or key.types != first.types or key.len != first.len) return false;
        }
        const scratch = try self.a.alloc(Entry, self.rows.items.len);
        defer self.a.free(scratch);
        var source = self.rows.items;
        var target = scratch;
        // Stable LSD passes: ordinal is the final SQL tie breaker, followed
        // by complete memcomparable bytes. Mixed/truncated keys use pdq.
        var pass: usize = 8 + first.len;
        while (pass != 0) {
            pass -= 1;
            var counts: [256]usize = @splat(0);
            for (source) |row| counts[radixByte(row, pass, first.len)] += 1;
            if (for (counts) |count| {
                if (count == source.len) break true;
            } else false) continue;
            var offsets: [256]usize = undefined;
            var offset: usize = 0;
            for (counts, &offsets) |count, *start| {
                start.* = offset;
                offset += count;
            }
            for (source) |row| {
                const byte = radixByte(row, pass, first.len);
                target[offsets[byte]] = row;
                offsets[byte] += 1;
            }
            std.mem.swap([]Entry, &source, &target);
        }
        if (source.ptr != self.rows.items.ptr) @memcpy(self.rows.items, source);
        self.radix_runs += 1;
        return true;
    }
    fn radixByte(row: Entry, pass: usize, length: usize) u8 {
        return if (pass < length) row.normalized.?.bytes[pass] else @truncate(row.ordinal >> @as(u6, @intCast((7 - (pass - length)) * 8)));
    }
    fn entryRow(self: *Sort, a: Allocator, entry: Entry, payload: bool) !Row {
        return .{ .values = if (payload) try self.values.row(a, entry.position) else &.{}, .keys = try self.keys.row(a, entry.position), .ordinal = entry.ordinal, .normalized = entry.normalized };
    }
    fn sortRows(self: *Sort) !void {
        if (try self.radixRows()) return;
        if (self.encoded_bytes) {
            std.sort.pdq(Entry, self.rows.items, self, lessEncoded);
            return;
        }
        const Comparator = struct {
            sort: *Sort,
            scratch: std.heap.ArenaAllocator,
            err: ?anyerror = null,
            fn less(comparator: *@This(), left: Entry, right: Entry) bool {
                _ = comparator.scratch.reset(.retain_capacity);
                const a = comparator.scratch.allocator();
                return comparator.compare(a, left, right) catch |err| {
                    comparator.err = comparator.err orelse err;
                    return left.ordinal < right.ordinal;
                };
            }
            fn compare(comparator: *@This(), a: Allocator, left: Entry, right: Entry) !bool {
                if (left.normalized) |lkey| if (right.normalized) |rkey| if (@import("sort_key.zig").compare(lkey, rkey)) |order| {
                    return if (order == .eq) left.ordinal < right.ordinal else order == .lt;
                };
                return (try operators.compareRows(try comparator.sort.entryRow(a, left, false), try comparator.sort.entryRow(a, right, false), comparator.sort.orders)) == .lt;
            }
        };
        var comparator: Comparator = .{ .sort = self, .scratch = .init(self.a) };
        defer comparator.scratch.deinit();
        std.sort.pdq(Entry, self.rows.items, &comparator, Comparator.less);
        if (comparator.err) |err| return err;
    }
    fn writeRows(self: *Sort, file: *Sequential) !void {
        var begin: usize = 0;
        while (begin < self.rows.items.len) {
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            const a = scratch.allocator();
            const end = @min(self.rows.items.len, begin + 256);
            var positions: [256]usize = undefined;
            var ordinals: [256]u64 = undefined;
            for (self.rows.items[begin..end], 0..) |entry, index| {
                positions[index] = entry.position;
                ordinals[index] = entry.ordinal;
            }
            const values: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &self.values, .count = self.values.len } };
            const keys: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &self.keys, .count = self.keys.len } };
            try file.appendBatch(try values.select(a, positions[0 .. end - begin]), try keys.select(a, positions[0 .. end - begin]), ordinals[0 .. end - begin]);
            begin = end;
        }
    }
    fn flush(self: *Sort) anyerror!void {
        try self.collectRun();
        if (self.rows.items.len == 0) return;
        if (self.allow_indirect and self.wide_values) return self.startIndirect();
        self.allow_indirect = false;
        if (self.parallel_runs and self.memory_bytes >= 128 * 1024) {
            try self.ensureRunStore();
            const job = try self.a.create(RunJob);
            job.* = .{ .sort = .{ .manager = self.manager, .a = self.a, .orders = self.orders, .encoded_bytes = self.encoded_bytes, .memory_bytes = self.memory_bytes / 2, .arena = self.arena, .rows = self.rows, .values = self.values, .keys = self.keys, .parallel_runs = false, .shared_runs = self.shared_runs, .run_store = self.run_store, .owns_run_store = false } };
            if (@import("parallel_scheduler.zig").global().submit(self.manager.io, self.estimated +| self.blockBytes() * 4, RunJob.run, .{job})) |task| {
                job.task = task;
                self.pending_run = job;
                self.parallel_runs_started += 1;
                self.arena = .init(self.a);
                self.rows = .empty;
                self.values = .init(self.a);
                self.keys = .init(self.a);
                self.estimated = 0;
                return;
            }
            // Admission declined: original buffers still belong to this sort.
            self.a.destroy(job);
        }
        try self.sortRows();
        var run = try self.createRun();
        var transferred = false;
        errdefer if (!transferred) run.close();
        try self.writeRows(&run);
        try run.seal();
        _ = self.arena.reset(.free_all);
        self.values.deinit();
        self.keys.deinit();
        self.values = .init(self.a);
        self.keys = .init(self.a);
        self.rows.clearAndFree(self.a);
        self.estimated = 0;
        transferred = true;
        return self.admitRun(run);
    }
    fn ensureRunStore(self: *Sort) !void {
        if (!self.shared_runs or self.run_store != null) return;
        std.debug.assert(self.owns_run_store);
        const store = try self.a.create(RunStore);
        errdefer self.a.destroy(store);
        store.* = .{ .file = try self.manager.create() };
        self.run_store = store;
    }
    fn createRun(self: *Sort) !Sequential {
        try self.ensureRunStore();
        if (self.shared_runs) return self.run_store.?.run(self.blockBytes());
        return Sequential.init(self.manager, self.blockBytes());
    }
    fn collectPresortedRun(self: *Sort) !void {
        if (self.presorted_run) |*run| {
            try run.seal();
            const owned = run.*;
            self.presorted_run = null;
            try self.admitRun(owned);
        }
    }
    fn admitRun(self: *Sort, input: Sequential) !void {
        self.allow_indirect = false;
        std.debug.assert(self.run_limit >= 1 and self.run_limit <= self.runs.len);
        var run = input;
        errdefer run.close();
        std.debug.assert(!self.shared_runs or run.file.run_store == self.run_store);
        var level: u8 = 0;
        const fan_in = self.fanIn();
        while (true) {
            var indices: [8]usize = undefined;
            var count: usize = 0;
            var empty: ?usize = null;
            for (self.runs[0..self.run_limit], self.run_levels[0..self.run_limit], 0..) |slot, candidate_level, index| {
                if (slot == null) {
                    empty = index;
                } else if (candidate_level == level and count < fan_in - 1) {
                    indices[count] = index;
                    count += 1;
                }
            }
            if (count < fan_in - 1 and empty != null) {
                self.runs[empty.?] = run;
                self.run_levels[empty.?] = level;
                return;
            }
            // A full directory can occur with many unequal levels. Compact
            // the smallest levels first rather than repeatedly rewriting the
            // largest run; fan-in still obeys the decoded-head memory budget.
            if (empty == null and count < fan_in - 1) {
                count = 0;
                var order: [32]usize = undefined;
                for (order[0..self.run_limit], 0..) |*index, i| index.* = i;
                std.mem.sort(usize, order[0..self.run_limit], self, struct {
                    fn less(sort: *Sort, a: usize, b: usize) bool {
                        return sort.run_levels[a] < sort.run_levels[b];
                    }
                }.less);
                for (order[0 .. fan_in - 1]) |index| {
                    indices[count] = index;
                    count += 1;
                    level = @max(level, self.run_levels[index]);
                }
            }
            var inputs: [8]*Sequential = undefined;
            inputs[0] = &run;
            for (indices[0..count], inputs[1 .. count + 1]) |index, *file| file.* = &self.runs[index].?;
            const combined = try self.mergeMany(inputs[0 .. count + 1]);
            run.close();
            for (indices[0..count]) |index| {
                self.runs[index].?.close();
                self.runs[index] = null;
            }
            run = combined;
            level = std.math.add(u8, level, 1) catch return error.SqlProgramLimitExceeded;
        }
    }
    fn blockBytes(self: *const Sort) usize {
        // Below 64 KiB there is insufficient workspace to amortize block
        // decoding across merge heads. A 128-byte target selects records.
        if (self.memory_bytes < 64 * 1024) return 128;
        return @min(32 * 1024, @max(128, self.memory_bytes / 64));
    }
    fn fanIn(self: *const Sort) usize {
        const block_workspace = if (self.blockBytes() > 128) self.blockBytes() *| 8 else 0;
        const head_bytes = self.max_row_bytes *| 4 +| self.manager.buffer_bytes *| 2 +| block_workspace +| 512;
        // Keep one retained run outside a full-directory compaction. Otherwise
        // a small directory with fan-in = run_limit + 1 rewrites its entire
        // growing prefix whenever the next run arrives.
        return @min(@max(@as(usize, 2), self.run_limit), @min(@min(self.outputs.len, @max(@as(usize, 2), self.merge_fan_in)), @max(@as(usize, 2), self.memory_bytes / @max(1, head_bytes))));
    }
    fn readRun(self: *Sort, file: *Sequential, a: Allocator, offset: u64) !Decoded {
        if (offset == 0) try file.seal();
        _ = a;
        var decoded = try file.readBorrowed(offset);
        decoded.row.normalized = try self.normalize(decoded.row.keys);
        return decoded;
    }
    const MergeRows = struct {
        const Location = struct { block: *Sequential.OwnedBlock, row: usize };
        locations: []const Location,
        keys: bool,
        fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const location = self.locations[row];
            return if (self.keys) location.block.keyCell(location.row, column) else location.block.cell(location.row, column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const location = self.locations[row];
            return if (location.block.encoded) |block| block.frozenJson(location.row, column, self.keys) else null;
        }
        fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const location = self.locations[row];
            return if (location.block.encoded) |block| block.encodedJson(location.row, column, self.keys) else null;
        }
        fn batch(self: *@This()) @import("execution_batch.zig").Batch {
            const block = self.locations[0].block;
            return .{ .reader = .{ .ptr = self, .read = cell, .read_frozen = frozen, .read_json = json, .width = if (self.keys) block.keyWidth() else block.width(), .count = self.locations.len } };
        }
        fn write(locations: []const Location, ordinals: []const u64, output: *Sequential) !void {
            if (locations.len == 0) return;
            var values: MergeRows = .{ .locations = locations, .keys = false };
            var keys: MergeRows = .{ .locations = locations, .keys = true };
            try output.appendBatch(values.batch(), keys.batch(), ordinals);
        }
    };
    fn mergeMany(self: *Sort, inputs: []const *Sequential) !Sequential {
        std.debug.assert(inputs.len >= 2 and inputs.len <= self.fanIn());
        var output = try self.createRun();
        errdefer output.close();
        var arenas: [8]std.heap.ArenaAllocator = undefined;
        var heads: [8]?Row = @splat(null);
        var blocks: [8]?*Sequential.OwnedBlock = @splat(null);
        var positions: [8]usize = @splat(0);
        var offsets: [8]u64 = @splat(0);
        for (arenas[0..inputs.len]) |*arena| arena.* = .init(self.a);
        defer for (arenas[0..inputs.len]) |*arena| arena.deinit();
        defer for (blocks) |block| if (block) |owner| owner.release();
        for (inputs, 0..) |file, index| if (file.size != 0) {
            try file.seal();
            blocks[index] = try file.readOwnedBlock(0);
            var row = try blocks[index].?.keyRowAlloc(arenas[index].allocator(), 0);
            row.normalized = try self.normalize(row.keys);
            heads[index] = row;
        };
        var pending: [64]MergeRows.Location = undefined;
        var ordinals: [64]u64 = undefined;
        var count: usize = 0;
        while (true) {
            try self.manager.check();
            var selected: ?usize = null;
            for (heads[0..inputs.len], 0..) |head, index| if (head) |row| {
                if (selected == null or (try self.compare(row, heads[selected.?].?)) == .lt) selected = index;
            };
            const index = selected orelse break;
            const block = blocks[index].?;
            pending[count] = .{ .block = block, .row = positions[index] };
            ordinals[count] = heads[index].?.ordinal;
            count += 1;
            positions[index] += 1;
            offsets[index] += 1;
            // Pending rows borrow current input blocks. Flush before retiring
            // any block, so wide payloads never need extra pinned cohorts.
            if (count == pending.len or positions[index] == block.count()) {
                try MergeRows.write(pending[0..count], ordinals[0..count], &output);
                count = 0;
            }
            if (positions[index] == block.count()) {
                blocks[index] = null;
                block.release();
                if (offsets[index] < inputs[index].size) {
                    blocks[index] = try inputs[index].readOwnedBlock(offsets[index]);
                    positions[index] = 0;
                }
            }
            _ = arenas[index].reset(.retain_capacity);
            heads[index] = if (blocks[index]) |next_block| blk: {
                var row = try next_block.keyRowAlloc(arenas[index].allocator(), positions[index]);
                row.normalized = try self.normalize(row.keys);
                break :blk row;
            } else null;
        }
        try MergeRows.write(pending[0..count], ordinals[0..count], &output);
        self.manager.increment("merges", 1);
        return output;
    }
    fn compactParallel(self: *Sort) !void {
        if (!self.parallel_runs or self.memory_bytes < 512 * 1024) return;
        while (true) {
            var indices: [32]usize = undefined;
            var count: usize = 0;
            for (self.runs, 0..) |file, index| if (file != null) {
                indices[count] = index;
                count += 1;
            };
            if (count <= self.fanIn()) return;
            var jobs: [2]?*MergeJob = @splat(null);
            defer for (jobs) |job| if (job) |owner| owner.close();
            var claimed: usize = 0;
            for (&jobs) |*slot| {
                var local = Sort.init(self.a, self.manager, self.orders, self.memory_bytes / 2);
                local.encoded_bytes = self.encoded_bytes;
                local.max_row_bytes = self.max_row_bytes;
                local.merge_fan_in = self.merge_fan_in;
                local.parallel_runs = false;
                local.shared_runs = self.shared_runs;
                local.run_store = self.run_store;
                local.owns_run_store = false;
                const width = @min(local.fanIn(), count - claimed);
                if (width < 2) {
                    local.deinit();
                    break;
                }
                const job = try self.a.create(MergeJob);
                job.* = .{ .sort = local, .count = width };
                slot.* = job;
                for (0..width) |i| {
                    const index = indices[claimed + i];
                    job.inputs[i] = self.runs[index];
                    self.runs[index] = null;
                }
                claimed += width;
                job.task = @import("parallel_scheduler.zig").global().submit(self.manager.io, self.memory_bytes / 2, MergeJob.run, .{job});
                if (job.task != null) self.parallel_merges_started += 1;
            }
            var failure: ?anyerror = null;
            for (jobs) |job| if (job) |owner| {
                const result = if (owner.task) |*task| task.await(self.manager.io) else owner.run();
                owner.task = null;
                var combined = result catch |err| {
                    failure = failure orelse err;
                    continue;
                };
                if (failure != null) {
                    combined.close();
                    continue;
                }
                const empty = for (self.runs, 0..) |file, index| {
                    if (file == null) break index;
                } else unreachable;
                self.runs[empty] = combined;
            };
            if (failure) |err| return err;
        }
    }
    pub fn finish(self: *Sort) !void {
        return self.finishImpl(false);
    }
    fn finishImpl(self: *Sort, leased: bool) anyerror!void {
        return self.finishImplWithPressure(leased, null);
    }
    fn finishDelivery(self: *Sort, reserve: usize) anyerror!void {
        var pressure: CachePressure = .{ .manager = self.manager, .context = self, .evict = keepCollectedPayloads, .bounded = true, .reserve = reserve };
        self.finishImplWithPressure(true, &pressure) catch |err| {
            if (err == error.OutOfMemory and pressure.denied) return error.SqlProgramLimitExceeded;
            return err;
        };
    }
    fn finishImplWithPressure(self: *Sort, leased: bool, pressure: ?*CachePressure) anyerror!void {
        if (self.order_sort) |inner| {
            if (!self.finished) if (self.payloads) |payloads| {
                try payloads.file.seal();
                while (payloads.evictOne()) {}
            };
            try inner.finishImplWithPressure(leased, pressure);
            self.parallel_runs_started = inner.parallel_runs_started;
            self.parallel_merges_started = inner.parallel_merges_started;
            self.radix_runs = inner.radix_runs;
            self.finished = true;
            return;
        }
        if (self.finished) return;
        try self.collectPresortedRun();
        if (self.pending_run != null) {
            try self.flush();
            try self.collectRun();
        }
        const has_runs = for (self.runs) |run| {
            if (run != null) break true;
        } else false;
        if (!has_runs) {
            try self.sortRows();
            self.finished = true;
            return;
        }
        try self.flush();
        try self.collectRun();
        try self.compactParallel();
        // Bound decoded heads and I/O buffers; stream the final merge instead
        // of writing and rereading another complete sorted run.
        const fan_in = self.fanIn();
        errdefer {
            for (self.outputs[0..self.output_count]) |*file| if (file.*) |*active| active.close();
            self.output_count = 0;
        }
        for (&self.runs) |*slot| if (slot.*) |*run| {
            if (self.output_count == fan_in) {
                var inputs: [8]*Sequential = undefined;
                for (self.outputs[0..self.output_count], inputs[0..self.output_count]) |*file, *input| input.* = &file.*.?;
                const combined = try self.mergeMany(inputs[0..self.output_count]);
                for (self.outputs[0..self.output_count]) |*file| {
                    file.*.?.close();
                    file.* = null;
                }
                self.outputs[0] = combined;
                self.output_count = 1;
            }
            self.outputs[self.output_count] = run.*;
            self.output_count += 1;
            slot.* = null;
        };
        for (self.head_arenas[0..self.output_count]) |*arena| arena.* = std.heap.ArenaAllocator.init(self.a);
        errdefer for (self.head_arenas[0..self.output_count]) |*arena| arena.deinit();
        for (self.outputs[0..self.output_count], self.head_arenas[0..self.output_count], self.heads[0..self.output_count], 0..) |*file, *arena, *head, index| {
            if (leased and file.*.?.size != 0) {
                const block = try file.*.?.readOwnedBlockWithPressure(0, pressure);
                self.owned_heads[index] = block;
                self.owned_positions[index] = 0;
                arena.child_allocator = if (pressure) |scope| scope.allocator() else self.a;
                defer arena.child_allocator = self.a;
                var row = try block.keyRowAlloc(arena.allocator(), 0);
                row.normalized = try self.normalize(row.keys);
                head.* = .{ .row = row, .next = none, .matched = false, .following = 1 };
            } else head.* = if (file.*.?.size != 0) try self.readRun(&file.*.?, arena.allocator(), 0) else null;
        }
        self.leased_reads = leased;
        self.finished = true;
    }
    /// Replay a finished owned-row stream without rebuilding or materializing it.
    /// Leased streams have outstanding block ownership and cannot be rewound.
    pub fn rewind(self: *Sort) anyerror!void {
        if (self.order_sort) |inner| return inner.rewind();
        try self.finish();
        if (self.leased_reads) return error.UnsupportedSpillRewind;
        try self.manager.checkpoint(self.manager.context);
        self.offset = 0;
        for (self.outputs[0..self.output_count], self.head_arenas[0..self.output_count], self.heads[0..self.output_count]) |*file, *arena, *head| {
            head.* = null;
            _ = arena.reset(.retain_capacity);
            file.*.?.rewind();
            head.* = if (file.*.?.size != 0) try self.readRun(&file.*.?, arena.allocator(), 0) else null;
        }
    }
    pub fn next(self: *Sort, a: Allocator) !?Row {
        return self.nextImpl(a, true);
    }
    /// Final delivery discards sort keys at the ownership boundary.
    pub fn nextValues(self: *Sort, a: Allocator) !?Row {
        return self.nextImpl(a, false);
    }
    pub const RowLease = struct {
        row: Row,
        block: ?*Sequential.OwnedBlock = null,
        index: usize = 0,
        key_block: ?*Sequential.OwnedBlock = null,
        key_index: ?usize = null,
        values: ?*const @import("typed_store.zig").Store = null,
        keys: ?*const @import("typed_store.zig").Store = null,
        pub fn cell(self: RowLease, column: usize) !Datum {
            return if (self.block) |block| block.cell(self.index, column) else if (self.values) |values| values.cell(values.a, self.index, column) else self.row.values[column];
        }
        /// Merge consumers can inspect typed keys without reconstructing an
        /// entire row or duplicating keys in the stored payload columns.
        pub fn keyCell(self: RowLease, column: usize) !Datum {
            return if (self.key_block) |block| block.keyCell(self.key_index.?, column) else if (self.keys) |keys| keys.cell(keys.a, self.key_index orelse self.index, column) else if (self.block) |block| block.keyCell(self.index, column) else self.row.keys[column];
        }
        pub fn release(self: RowLease) void {
            if (self.key_block) |block| block.release();
            if (self.block) |block| block.release();
        }
    };
    /// Sorted delivery holds decoded run blocks through the transport page.
    /// Only row descriptors are gathered; wide payload bytes remain in place.
    pub fn skip(self: *Sort) anyerror!bool {
        try self.finishImpl(true);
        if (self.order_sort != null) {
            const lease = (try self.nextReference()) orelse return false;
            lease.release();
            return true;
        }
        const lease = (try self.nextLeased()) orelse return false;
        lease.release();
        return true;
    }
    /// Payload-only pages for ordinary sorts. Resident ranks borrow a constant
    /// span descriptor; external ranks own compact (block,index) descriptors.
    /// The enclosing cursor pins resident stores until all pages are released.
    pub const ValuesPage = union(enum) {
        blocks: PayloadReader.Page,
        memory: struct { view: *ResidentView, a: Allocator },
        pub fn batch(self: ValuesPage) @import("execution_batch.zig").Batch {
            return switch (self) {
                .blocks => |page| page.values,
                .memory => |owner| owner.view.batch(),
            };
        }
        pub fn release(self: ValuesPage) void {
            switch (self) {
                .blocks => |page| page.release(),
                .memory => |owner| owner.a.destroy(owner.view),
            }
        }
    };
    const ResidentView = struct {
        sort: *Sort,
        begin: usize,
        count: usize,
        fn position(self: *const @This(), row: usize) usize {
            return self.sort.rows.items[self.begin + row].position;
        }
        fn cell(raw: *anyopaque, a: Allocator, row: usize, column: usize) anyerror!Datum {
            const view: *@This() = @ptrCast(@alignCast(raw));
            return view.sort.values.cell(a, view.position(row), column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const view: *@This() = @ptrCast(@alignCast(raw));
            return view.sort.values.frozenJson(view.position(row), column);
        }
        fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
            const view: *@This() = @ptrCast(@alignCast(raw));
            return view.sort.values.encodedJson(view.position(row), column);
        }
        fn identity(raw: *anyopaque, row: usize, column: usize) anyerror!?u64 {
            const view: *@This() = @ptrCast(@alignCast(raw));
            return view.sort.values.dictionaryId(view.position(row), column);
        }
        fn dictionary(raw: *anyopaque, a: Allocator, column: usize) anyerror!?@import("execution_batch.zig").Batch {
            const view: *@This() = @ptrCast(@alignCast(raw));
            const Batch = @import("execution_batch.zig").Batch;
            const source = try a.create(Batch);
            source.* = .{ .retained = .{ .store = &view.sort.values, .count = view.sort.values.len } };
            const positions = try a.alloc(usize, view.count);
            for (positions, 0..) |*out, row| out.* = view.position(row);
            return (try source.select(a, positions)).dictionaryColumn(a, column);
        }
        fn batch(self: *@This()) @import("execution_batch.zig").Batch {
            return .{ .reader = .{ .ptr = self, .read = cell, .read_frozen = frozen, .read_json = json, .read_identity = identity, .read_dictionary = dictionary, .count = self.count, .width = self.sort.values.columns.len } };
        }
    };
    pub fn nextValuesPage(self: *Sort, maximum: usize, byte_limit: usize) anyerror!ValuesPage {
        if (maximum == 0 or self.order_sort != null) return error.InvalidSqlLimit;
        try self.finishDelivery(@sizeOf(PayloadReader.Payload) + @sizeOf(PayloadReader.PageView));
        const stable = self.manager.allocator();
        if (self.output_count == 0) {
            var pressure: CachePressure = .{ .manager = self.manager, .context = self, .evict = keepCollectedPayloads, .bounded = true };
            if (!pressure.admit(@sizeOf(ResidentView))) return error.SqlProgramLimitExceeded;
            const view = try stable.create(ResidentView);
            errdefer stable.destroy(view);
            view.* = .{ .sort = self, .begin = @intCast(self.offset), .count = @min(@min(64, maximum), self.rows.items.len - @as(usize, @intCast(self.offset))) };
            var bytes: usize = 0;
            const batch = view.batch();
            var count: usize = 0;
            while (count < batch.len()) {
                for (0..batch.width()) |column| bytes +|= try self.values.cellBytes(stable, view.position(count), column);
                count += 1;
                if (bytes >= byte_limit) break;
            }
            view.count = count;
            self.offset += count;
            return .{ .memory = .{ .view = view, .a = stable } };
        }
        var payloads: [64]PayloadReader.Payload = undefined;
        var count: usize = 0;
        errdefer for (payloads[0..count]) |payload| payload.release();
        var bytes: usize = 0;
        var owners: [64]*Sequential.OwnedBlock = undefined;
        var owner_count: usize = 0;
        var retained: usize = 0;
        while (count < @min(maximum, payloads.len)) {
            var pressure: CachePressure = .{ .manager = self.manager, .context = self, .evict = keepCollectedPayloads, .bounded = true, .reserve = (count + 1) * @sizeOf(PayloadReader.Payload) + @sizeOf(PayloadReader.PageView) };
            if (!pressure.admit(0)) {
                if (count == 0) return error.SqlProgramLimitExceeded;
                break;
            }
            const lease = (self.nextLeasedWithPressure(&pressure) catch |err| {
                if (err != error.OutOfMemory or !pressure.denied) return err;
                if (count == 0) return error.SqlProgramLimitExceeded;
                break;
            }) orelse break;
            errdefer lease.release();
            const block = lease.block orelse return error.InvalidSqlSpill;
            for (0..block.width()) |column| {
                const frozen = if (block.encoded) |encoded| try encoded.frozenJson(lease.index, column, false) else null;
                const json = if (frozen == null and block.encoded != null) try block.encoded.?.encodedJson(lease.index, column, false) else null;
                bytes +|= if (frozen) |value| value.bytes.len else if (json) |value| value.len else try operators.datumBytes(try lease.cell(column));
            }
            if (lease.key_block) |keys| keys.release();
            const known = for (owners[0..owner_count]) |owner| {
                if (owner == block) break true;
            } else false;
            if (!known) {
                owners[owner_count] = block;
                owner_count += 1;
                retained +|= block.retainedBytes() +| @sizeOf(Sequential.OwnedBlock);
            }
            payloads[count] = .{ .block = block, .index = lease.index };
            count += 1;
            if (bytes >= byte_limit or retained +| (count * @sizeOf(PayloadReader.Payload) + @sizeOf(PayloadReader.PageView)) >= byte_limit) break;
        }
        const page = try PayloadReader.ownPage(stable, payloads[0..count], if (count == 0) 0 else payloads[0].block.width());
        return .{ .blocks = page };
    }
    /// Pull a bounded rank window, hydrate each distinct physical page once,
    /// and restore rank order. A bounded key backlog preserves unread lanes when
    /// physical allocation admission shortens the estimated window.
    pub const Page = struct {
        rows: []RowLease,
        a: Allocator,
        values: @import("execution_batch.zig").Batch,
        pub fn release(self: Page) void {
            self.a.destroy(@as(*PageView, @ptrCast(@alignCast(self.values.reader.ptr))));
            for (self.rows) |lease| lease.release();
            self.a.free(self.rows);
        }
    };
    pub fn nextPageLeased(self: *Sort, maximum: usize, byte_limit: usize) anyerror!Page {
        if (maximum == 0) return error.InvalidSqlLimit;
        try self.finishDelivery(@sizeOf(RowLease) + @sizeOf(PageView));
        const reader = try self.payloadReader();
        const limit = reader.target(byte_limit);
        var keys: [64]?RowLease = @splat(null);
        defer for (keys) |lease| if (lease) |value| value.release();
        var references: [64]PayloadReader.Reference = undefined;
        var count: usize = 0;
        var bytes: usize = 0;
        const logical_rows = @max(1, byte_limit / @max(1, self.max_payload_bytes));
        while (count < @min(@min(maximum, logical_rows), keys.len)) {
            // Crossing a physical key boundary must leave room to hydrate the
            // already collected payload prefix. Within a resident key block,
            // only descriptor growth needs admission.
            const prefix_bytes = if (count != 0 and self.deferred_index == self.deferred_count and self.order_sort.?.keyBlockBoundary()) bytes else 0;
            var pressure: CachePressure = .{ .manager = self.manager, .context = reader, .evict = if (count == 0) PayloadReader.evict else keepCollectedPayloads, .bounded = true, .reserve = prefix_bytes +| ((count + 1) * @sizeOf(RowLease) + @sizeOf(PageView)) };
            const lease = (self.nextReferenceWithPressure(&pressure) catch |err| {
                if (err != error.OutOfMemory or !pressure.denied) return err;
                if (count != 0) break;
                return error.SqlProgramLimitExceeded;
            }) orelse break;
            errdefer lease.release();
            const reference = try PayloadReader.Reference.decode(&.{ try lease.cell(0), try lease.cell(1) });
            const repeated = for (references[0..count]) |prior| {
                if (prior.offset == reference.offset) break true;
            } else false;
            const cost = if (repeated) 0 else try reader.estimate(reference.offset);
            if (count != 0 and cost > limit -| bytes) {
                var pending: [1]?RowLease = .{lease};
                try self.deferReferences(&pending);
                break;
            }
            keys[count] = lease;
            references[count] = reference;
            count += 1;
            bytes +|= cost;
        }
        const pending = count + self.deferred_count - self.deferred_index;
        const backlog_bytes = if (pending > self.deferred_references.len) pending * @sizeOf(?RowLease) else 0;
        var storage: [64]PayloadReader.Payload = undefined;
        const gathered = try reader.gatherForDelivery(&storage, references[0..count], byte_limit, @sizeOf(RowLease), @sizeOf(PageView), backlog_bytes);
        const payloads = gathered.storage[0..gathered.count];
        errdefer for (payloads) |payload| payload.release();
        const stable = self.manager.allocator();
        const output = try stable.alloc(RowLease, payloads.len);
        errdefer stable.free(output);
        const view = try stable.create(PageView);
        errdefer stable.destroy(view);
        try self.deferReferences(keys[payloads.len..count]);
        self.payload_reads = reader.reads;
        for (output, payloads, keys[0..payloads.len]) |*result, payload, *key| {
            const lease = key.*.?;
            result.* = .{ .row = lease.row, .block = payload.block, .index = payload.index, .key_block = lease.block, .key_index = lease.index, .keys = lease.keys };
            key.* = null;
        }
        view.* = .{ .rows = output };
        return .{ .rows = output, .a = stable, .values = .{ .reader = .{ .ptr = view, .read = PageView.cell, .read_frozen = PageView.frozen, .read_json = PageView.json, .read_identity = PageView.identity, .read_dictionary = PageView.dictionary, .count = output.len, .width = if (output.len == 0) 0 else output[0].block.?.width() } } };
    }
    const PageView = struct {
        rows: []const RowLease,
        fn shared(self: *const @This()) bool {
            if (self.rows.len == 0) return false;
            for (self.rows) |row| if (row.block != self.rows[0].block) return false;
            return true;
        }
        fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.rows[row].cell(column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const lease = self.rows[row];
            return lease.block.?.encoded.?.frozenJson(lease.index, column, false);
        }
        fn json(raw: *anyopaque, row: usize, column: usize) anyerror!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const lease = self.rows[row];
            return lease.block.?.encoded.?.encodedJson(lease.index, column, false);
        }
        fn identity(raw: *anyopaque, row: usize, column: usize) anyerror!?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.shared()) return null;
            const lease = self.rows[row];
            return lease.block.?.encoded.?.dictionaryIdentity(lease.index, column, false);
        }
        fn dictionary(raw: *anyopaque, a: Allocator, column: usize) anyerror!?@import("execution_batch.zig").Batch {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.shared()) return null;
            const Batch = @import("execution_batch.zig").Batch;
            const source = try a.create(Batch);
            source.* = try self.rows[0].block.?.batch(a, false);
            const indices = try a.alloc(usize, self.rows.len);
            for (indices, self.rows) |*index, row| index.* = row.index;
            return (try source.select(a, indices)).dictionaryColumn(a, column);
        }
    };
    // Refresh only at the next pull. The final row of a block is returnable
    // before the following block is allocated, and a denied refresh consumes
    // no row. Published arenas always use the stable statement allocator.
    fn refreshLeasedHeads(self: *Sort, pressure: ?*CachePressure) !void {
        for (self.pending_heads[0..self.output_count], 0..) |pending, index| if (pending) {
            const prior = self.heads[index].?;
            if (self.owned_heads[index] == null) {
                self.owned_heads[index] = try self.outputs[index].?.readOwnedBlockWithPressure(prior.following, pressure);
                self.owned_positions[index] = 0;
            }
            const arena = &self.head_arenas[index];
            arena.child_allocator = if (pressure) |scope| scope.allocator() else self.a;
            defer arena.child_allocator = self.a;
            _ = arena.reset(.retain_capacity);
            var row = try self.owned_heads[index].?.keyRowAlloc(arena.allocator(), self.owned_positions[index]);
            row.normalized = try self.normalize(row.keys);
            self.heads[index] = .{ .row = row, .next = none, .matched = false, .following = prior.following + 1 };
            self.pending_heads[index] = false;
        };
    }
    fn keepCollectedPayloads(_: *anyopaque) bool {
        return false;
    }
    fn keyBlockBoundary(self: *Sort) bool {
        for (self.pending_heads[0..self.output_count], self.owned_heads[0..self.output_count]) |pending, owner| if (pending and owner == null) return true;
        return false;
    }
    pub fn nextLeased(self: *Sort) anyerror!?RowLease {
        return self.nextLeasedWithPressure(null);
    }
    fn nextLeasedWithPressure(self: *Sort, pressure: ?*CachePressure) anyerror!?RowLease {
        try self.finishImpl(true);
        if (self.order_sort != null) {
            const keys = (try self.nextReference()) orelse return null;
            errdefer keys.release();
            var reference: [2]Datum = undefined;
            for (&reference, 0..) |*value, column| value.* = try keys.cell(column);
            const payload = try self.readPayload(&reference);
            return .{ .row = keys.row, .block = payload.block, .index = payload.index, .key_block = keys.block, .key_index = keys.index, .keys = keys.keys };
        }
        if (!self.leased_reads) {
            for (self.heads[0..self.output_count], 0..) |head, index| if (head) |value| {
                self.owned_heads[index] = try self.outputs[index].?.readOwnedBlockWithPressure(value.following - 1, pressure);
                self.owned_positions[index] = 0;
            };
            self.leased_reads = true;
        }
        try self.refreshLeasedHeads(pressure);
        var selected: ?usize = null;
        for (self.heads[0..self.output_count], 0..) |head, index| if (head) |value| {
            if (selected == null or (try self.compare(value.row, self.heads[selected.?].?.row)) == .lt) selected = index;
        };
        if (selected) |index| {
            const head = self.heads[index].?;
            const owner = self.owned_heads[index].?;
            const position = self.owned_positions[index];
            owner.retain();
            errdefer owner.release();
            self.owned_positions[index] += 1;
            const exhausted = self.owned_positions[index] == owner.count();
            if (exhausted) {
                self.owned_heads[index] = null;
                owner.release();
            }
            self.pending_heads[index] = !exhausted or head.following < self.outputs[index].?.size;
            if (!self.pending_heads[index]) self.heads[index] = null;
            // A delivery lease exposes payload cells and ordinal. Merge keys
            // belong to the lane's reusable scratch, never to a payload lease.
            return .{ .row = .{ .values = &.{}, .keys = &.{}, .ordinal = head.row.ordinal }, .block = owner, .index = position };
        }
        if (self.offset < self.rows.items.len) {
            const entry = self.rows.items[@intCast(self.offset)];
            self.offset += 1;
            return .{ .row = .{ .values = &.{}, .keys = &.{}, .ordinal = entry.ordinal }, .values = &self.values, .keys = &self.keys, .index = entry.position };
        }
        return null;
    }
    fn nextImpl(self: *Sort, a: Allocator, retain_keys: bool) anyerror!?Row {
        if (self.order_sort) |inner| {
            if (self.deferred_index < self.deferred_count) {
                const lease = (try self.nextLeased()) orelse return null;
                defer lease.release();
                const values = try a.alloc(Datum, lease.block.?.width());
                for (values, 0..) |*value, column| value.* = try operators.cloneDatum(a, try lease.cell(column));
                const keys: []Datum = if (retain_keys) try a.alloc(Datum, self.orders.len) else &.{};
                for (keys, 0..) |*key, column| key.* = try operators.cloneDatum(a, try lease.keyCell(column));
                return .{ .values = values, .keys = keys, .ordinal = lease.row.ordinal };
            }
            try self.finishImpl(false);
            const reference = (try inner.nextImpl(a, retain_keys)) orelse return null;
            defer a.free(reference.values);
            const payload = try self.readPayload(reference.values);
            defer payload.block.release();
            const values = try a.alloc(Datum, payload.block.width());
            for (values, 0..) |*value, column| value.* = try operators.cloneDatum(a, try payload.block.cell(payload.index, column));
            return .{ .values = values, .keys = reference.keys, .ordinal = reference.ordinal };
        }
        if (self.leased_reads) {
            const lease = (try self.nextLeased()) orelse return null;
            defer lease.release();
            const row = if (lease.block) |block| try block.row(a, lease.index) else if (lease.values != null) try self.entryRow(a, .{ .position = lease.index, .ordinal = lease.row.ordinal, .normalized = null }, true) else lease.row;
            defer {
                if (lease.values != null or (if (lease.block) |block| block.encoded != null else false)) {
                    a.free(row.values);
                    a.free(row.keys);
                }
            }
            const values = try a.alloc(Datum, row.values.len);
            for (row.values, values) |value, *out| out.* = try operators.cloneDatum(a, value);
            const keys: []Datum = if (retain_keys) try a.alloc(Datum, row.keys.len) else &.{};
            for (keys, row.keys[0..keys.len]) |*out, value| out.* = try operators.cloneDatum(a, value);
            return .{ .values = values, .keys = keys, .ordinal = row.ordinal };
        }
        try self.finish();
        if (self.output_count != 0) {
            var selected: ?usize = null;
            for (self.heads[0..self.output_count], 0..) |head, index| if (head) |value| {
                if (selected == null or (try self.compare(value.row, self.heads[selected.?].?.row)) == .lt) selected = index;
            };
            const index = selected orelse return null;
            const head = self.heads[index].?;
            const values = try a.alloc(Datum, head.row.values.len);
            const keys: []Datum = if (retain_keys) try a.alloc(Datum, head.row.keys.len) else &.{};
            for (head.row.values, values) |value, *out| out.* = try operators.cloneDatum(a, value);
            if (retain_keys) {
                for (head.row.keys, keys) |value, *out| out.* = try operators.cloneDatum(a, value);
            }
            _ = self.head_arenas[index].reset(.retain_capacity);
            self.heads[index] = if (head.following < self.outputs[index].?.size) try self.readRun(&self.outputs[index].?, self.head_arenas[index].allocator(), head.following) else null;
            return .{ .values = values, .keys = keys, .ordinal = head.row.ordinal };
        }
        if (self.offset < self.rows.items.len) {
            const row = try self.entryRow(a, self.rows.items[@intCast(self.offset)], true);
            defer a.free(row.values);
            defer a.free(row.keys);
            self.offset += 1;
            const values = try a.alloc(Datum, row.values.len);
            const keys: []Datum = if (retain_keys) try a.alloc(Datum, row.keys.len) else &.{};
            for (row.values, values) |v, *out| out.* = try operators.cloneDatum(a, v);
            if (retain_keys) {
                for (row.keys, keys) |v, *out| out.* = try operators.cloneDatum(a, v);
            }
            return .{ .values = values, .keys = keys, .ordinal = row.ordinal };
        }
        return null;
    }
};

test "SQL spill runs merge under bounded memory preserve exact datum tags and clean up" {
    var quota: @import("memory_budget.zig") = .{ .backing = std.heap.page_allocator, .limit = 64 * 1024 };
    defer std.debug.assert(quota.live == 0);
    const a = quota.allocator();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var sorter = Sort.init(a, &manager, &.{.{}}, 8192);
    defer sorter.deinit();
    for (0..200) |i| {
        const key = Datum.json(.{ .integer = @intCast(199 - i) });
        try sorter.add(.{ .values = &.{ Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.null), .{}, Datum.json(.{ .number_string = "1.0000000000000001" }) }, .keys = &.{key}, .ordinal = i });
    }
    for (0..200) |i| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const row = (try sorter.next(arena.allocator())).?;
        try std.testing.expectEqual(@as(i64, @intCast(i)), row.keys[0].value.integer);
        try std.testing.expectEqual(@as(i64, 9007199254740993), row.values[0].value.integer);
        try std.testing.expect(!row.values[1].sql_null and row.values[2].sql_null);
        try std.testing.expectEqualStrings("1.0000000000000001", row.values[3].value.number_string);
    }
    try std.testing.expect((try sorter.next(a)) == null);
    try std.testing.expect(manager.merges > 0);
    sorter.deinit();
    sorter = Sort.init(a, &manager, &.{.{}}, 8192);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL typed array spill codec preserves complete cells and rejects every truncated prefix under allocation faults" {
    const Harness = struct {
        fn run(a: Allocator) !void {
            const arrays = @import("array_value.zig");
            const Hook = struct {
                fn check(_: *anyopaque) !void {}
            };
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
            defer manager.deinit();
            const value = try arrays.Value.init(.jsonb, &.{ .{ .length = 2, .lower = -1 }, .{ .length = 2, .lower = 0 } }, &.{ Datum.json(.null), .{}, Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .string = "é" }) }, .{});
            var bytes: std.ArrayList(u8) = .empty;
            defer bytes.deinit(a);
            var encoder: Encoder = .{ .manager = &manager, .a = a, .bytes = &bytes, .limit = manager.max_record_bytes };
            try encoder.cells(&.{Datum.typedArray(&value)});
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var decoder: Decoder = .{ .manager = &manager, .a = arena.allocator(), .bytes = bytes.items };
            const cells = try decoder.cells();
            try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(Datum.typedArray(&value), cells[0]));
            try std.testing.expectEqual(try scalar.semanticHashDatum(Datum.typedArray(&value)), try scalar.semanticHashDatum(cells[0]));
            try std.testing.expect(!cells[0].array.?.flatCell(0).sql_null);
            try std.testing.expect(cells[0].array.?.flatCell(1).sql_null);
            for (0..bytes.items.len) |length| {
                _ = arena.reset(.free_all);
                decoder = .{ .manager = &manager, .a = arena.allocator(), .bytes = bytes.items[0..length] };
                if (decoder.cells()) |_| return error.ExpectedInvalidArraySpill else |err| {
                    if (err == error.OutOfMemory) return err;
                    try std.testing.expectEqual(error.InvalidSqlSpill, err);
                }
            }
            const kind = bytes.items[10];
            bytes.items[10] = 255;
            _ = arena.reset(.free_all);
            decoder = .{ .manager = &manager, .a = arena.allocator(), .bytes = bytes.items };
            if (decoder.cells()) |_| return error.ExpectedInvalidArraySpill else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidSqlSpill, err);
            }
            bytes.items[10] = kind;
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed array row and column spill preserve dimensions and NULL provenance" {
    const a = std.testing.allocator;
    const arrays = @import("array_value.zig");
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const value = try arrays.Value.init(.int32, &.{.{ .length = 2, .lower = -3 }}, &.{ Datum.json(.{ .integer = 1 }), .{} }, .{});
    var file = try manager.create();
    defer file.close();
    const row: Row = .{ .values = &.{ Datum.typedArray(&value), Datum.json(.null), .{} }, .keys = &.{Datum.typedArray(&value)}, .ordinal = 7 };
    const offset = try file.append(row, none);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const decoded = try file.read(arena.allocator(), offset);
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(row.values[0], decoded.row.values[0]));
    var sequential = try Sequential.init(&manager, 64 * 1024);
    defer sequential.close();
    _ = try sequential.append(row, none);
    _ = try sequential.append(row, none);
    try sequential.seal();
    for (0..2) |ordinal| {
        const column_row = try sequential.readBorrowed(ordinal);
        try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(row.values[0], column_row.row.values[0]));
        try std.testing.expectEqual(@as(i32, -3), column_row.row.keys[0].array.?.dimensions[0].lower);
        try std.testing.expect(!column_row.row.values[1].sql_null and column_row.row.values[1].array == null);
        try std.testing.expect(column_row.row.values[2].sql_null);
    }
    // Wire quotas do not cap expanded cell sizes. A compact NULL array fits
    // a 512-byte record even though its decoded Datum payload exceeds it.
    manager.max_record_bytes = 512;
    const nulls: [32]Datum = @splat(.{});
    const compact = try arrays.Value.init(.int32, &.{.{ .length = 32 }}, &nulls, .{});
    try std.testing.expect(try operators.datumBytes(Datum.typedArray(&compact)) > manager.max_record_bytes);
    manager.array_limits.elements = 16;
    try std.testing.expectError(error.SqlProgramLimitExceeded, file.append(.{ .values = &.{Datum.typedArray(&compact)}, .keys = &.{}, .ordinal = 8 }, none));
    manager.array_limits.elements = 65536;
    const compact_offset = try file.append(.{ .values = &.{Datum.typedArray(&compact)}, .keys = &.{}, .ordinal = 8 }, none);
    const compact_row = try file.read(arena.allocator(), compact_offset);
    try std.testing.expectEqual(@as(usize, 32), compact_row.row.values[0].array.?.cardinality());
}

test "SQL spill quotas cancellation and corrupt records fail without leaked files" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(raw: *anyopaque) !void {
            const canceled: *bool = @ptrCast(@alignCast(raw));
            if (canceled.*) return error.Cancelled;
        }
    };
    var canceled = false;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &canceled, .checkpoint = Hook.check, .max_bytes = 128 };
    defer manager.deinit();
    var file = try manager.create();
    defer file.close();
    _ = try file.append(.{ .values = &.{Datum.json(.{ .integer = 7 })}, .keys = &.{}, .ordinal = 0 }, none);
    const saved_size = file.size;
    try std.testing.expectError(error.SqlProgramLimitExceeded, file.append(.{ .values = &.{Datum.json(.{ .string = "this row is intentionally longer than the remaining temporary storage quota" })}, .keys = &.{}, .ordinal = 1 }, none));
    try std.testing.expectEqual(saved_size, file.size);
    canceled = true;
    try std.testing.expectError(error.Cancelled, file.read(a, 0));
    canceled = false;
    try file.flush();
    try file.file.writePositionalAll(manager.io, &.{255}, frame_bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.InvalidSqlSpill, file.read(arena.allocator(), 0));
    file.close();
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    const name = manager.directory_name;
    manager.deinit();
    const parent = try std.Io.Dir.openDirAbsolute(std.testing.io, "/tmp", .{});
    defer parent.close(std.testing.io);
    try std.testing.expectError(error.FileNotFound, parent.openDir(std.testing.io, &name, .{}));
}

test "SQL compressed spill validates decoded quotas and preserves matched flags" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var file = try manager.create();
    defer file.close();
    const text: [8192]u8 = @splat('x');
    const offset = try file.append(.{ .values = &.{Datum.json(.{ .string = &text })}, .keys = &.{Datum.json(.{ .integer = 7 })}, .ordinal = 11 }, none);
    try std.testing.expectEqual(@as(u64, 1), manager.compressed_records);
    try std.testing.expect(file.size < 1024);
    try file.match(offset);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const row = try file.read(arena.allocator(), offset);
    try std.testing.expect(row.matched);
    try std.testing.expectEqualStrings(&text, row.row.values[0].value.string);
    try std.testing.expectEqual(@as(u64, 11), row.row.ordinal);
    manager.max_record_bytes = 1024;
    try std.testing.expectError(error.InvalidSqlSpill, file.read(arena.allocator(), offset));
}

test "SQL buffered spill joins outstanding writes on cancelled close" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(raw: *anyopaque) !void {
            if (@as(*bool, @ptrCast(@alignCast(raw))).*) return error.QueryCanceled;
        }
    };
    var canceled = false;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &canceled, .checkpoint = Hook.check };
    defer manager.deinit();
    var file = try manager.create();
    defer file.close();
    const bytes: [4096]u8 = @splat('x');
    try file.writeRaw(0, &bytes);
    try file.writeRaw(4096, &bytes);
    canceled = true;
    try std.testing.expectError(error.QueryCanceled, file.flush());
    file.close();
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
}

test "SQL multiway spill reduces rewrite bytes under the same memory budget" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var written: [2]u64 = undefined;
    for ([_]usize{ 2, 8 }, 0..) |fan_in, run| {
        var budget: @import("memory_budget.zig") = .{ .backing = std.heap.page_allocator, .limit = 256 * 1024 };
        const a = budget.allocator();
        {
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var sort = Sort.init(a, &manager, &.{.{}}, 64 * 1024);
            sort.merge_fan_in = fan_in;
            defer sort.deinit();
            for (0..8192) |index| {
                const value = Datum.json(.{ .integer = @intCast(8191 - index) });
                try sort.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = index });
            }
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            for (0..8192) |index| {
                _ = scratch.reset(.retain_capacity);
                const row = (try sort.next(scratch.allocator())).?;
                try std.testing.expectEqual(@as(i64, @intCast(index)), row.values[0].value.integer);
            }
            try std.testing.expect((try sort.next(a)) == null);
            written[run] = manager.written_bytes;
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expect(written[1] < written[0] * 3 / 4);
    std.debug.print("SQL spill merge bytes: binary={d} multiway={d}\n", .{ written[0], written[1] });
}

test "SQL typed sequential blocks preserve tags nulls compression restart and quota" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 64 * 1024);
    defer file.close();
    for (0..512) |index| _ = try file.append(.{ .values = &.{ Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.null), .{}, Datum.json(.{ .number_string = "1.0000000000000001" }), Datum.json(.{ .string = "repeated native block value" }) }, .keys = &.{Datum.json(.{ .integer = @intCast(index) })}, .ordinal = index }, none);
    try file.seal();
    try std.testing.expect(manager.compressed_records != 0);
    for (0..2) |_| for (0..512) |index| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const row = try file.read(arena.allocator(), index);
        try std.testing.expectEqual(@as(u64, index), row.row.ordinal);
        try std.testing.expectEqual(@as(i64, 9007199254740993), row.row.values[0].value.integer);
        try std.testing.expect(!row.row.values[1].sql_null and row.row.values[1].value == .null);
        try std.testing.expect(row.row.values[2].sql_null);
        try std.testing.expectEqualStrings("1.0000000000000001", row.row.values[3].value.number_string);
        try std.testing.expectEqual(@as(i64, @intCast(index)), row.row.keys[0].value.integer);
    };
    try std.testing.expect(manager.live_bytes <= manager.max_bytes);
    manager.max_bytes = manager.live_bytes;
    _ = try file.append(.{ .values = &.{Datum.json(.{ .integer = 1 })}, .keys = &.{}, .ordinal = 512 }, none);
    try std.testing.expectError(error.SqlProgramLimitExceeded, file.flush());
}

test "SQL sequential runs mix wide records and typed blocks across restarts" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 4096);
    defer file.close();
    const wide: [4096]u8 = @splat('w');
    for (0..17) |index| {
        const value = if (index % 8 == 0) Datum.json(.{ .string = &wide }) else Datum.json(.{ .integer = @intCast(index) });
        _ = try file.append(.{ .values = &.{value}, .keys = &.{}, .ordinal = index }, none);
    }
    for (0..2) |_| for (0..17) |index| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const row = try file.read(arena.allocator(), index);
        try std.testing.expectEqual(@as(u64, index), row.row.ordinal);
        try std.testing.expectEqual(@as(u64, index + 1), row.following);
        if (index % 8 == 0) try std.testing.expectEqualStrings(&wide, row.row.values[0].value.string) else try std.testing.expectEqual(@as(i64, @intCast(index)), row.row.values[0].value.integer);
    };
}

test "SQL concurrent spill files enforce one quota and reclaim all reservations" {
    const Worker = struct {
        fn check(_: *anyopaque) !void {}
        fn run(manager: *Manager) anyerror!usize {
            var file = try manager.create();
            defer file.close();
            const bytes: [4096]u8 = @splat(42);
            var accepted: usize = 0;
            for (0..128) |_| {
                file.writeRaw(file.size, &bytes) catch |err| switch (err) {
                    error.SqlProgramLimitExceeded => break,
                    else => return err,
                };
                accepted += 1;
            }
            try file.flush();
            return accepted;
        }
    };
    var dummy: u8 = 0;
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 1024 * 1024 };
    var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = Worker.check, .max_bytes = 64 * 1024, .async_writes = false };
    defer manager.deinit();
    var tasks: [4]std.Io.Future(anyerror!usize) = undefined;
    var started: usize = 0;
    defer for (tasks[0..started]) |*task| {
        _ = task.cancel(std.testing.io) catch 0;
    };
    for (&tasks) |*task| {
        task.* = try std.testing.io.concurrent(Worker.run, .{&manager});
        started += 1;
    }
    var accepted: usize = 0;
    for (&tasks) |*task| accepted += try task.await(std.testing.io);
    started = 0;
    try std.testing.expect(accepted != 0);
    try std.testing.expect(manager.peak_bytes <= manager.max_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "SQL parallel sort runs preserve stable order and join on early close" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    for ([_]bool{ false, true }) |early| {
        var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 2 * 1024 * 1024 };
        const a = budget.allocator();
        {
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
            defer manager.deinit();
            var sort = Sort.init(a, &manager, &.{.{}}, 256 * 1024);
            defer sort.deinit();
            for (0..2048) |index| {
                const value = Datum.json(.{ .integer = @intCast(2047 - index) });
                try sort.add(.{ .values = &.{value}, .keys = &.{value}, .ordinal = index });
            }
            try std.testing.expect(sort.parallel_runs_started != 0);
            if (!early) {
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                for (0..2048) |index| {
                    _ = scratch.reset(.retain_capacity);
                    const row = (try sort.next(scratch.allocator())).?;
                    try std.testing.expectEqual(@as(i64, @intCast(index)), row.values[0].value.integer);
                    try std.testing.expectEqual(@as(u64, 2047 - index), row.ordinal);
                }
                try std.testing.expect((try sort.next(a)) == null);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
}

test "SQL radix sort preserves native key direction and ordinal ties" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    for ([_]bool{ false, true }) |descending| {
        var sort = Sort.init(a, &manager, &.{.{ .descending = descending }}, 8 * 1024 * 1024);
        defer sort.deinit();
        sort.parallel_runs = false;
        for (0..2048) |i| try sort.add(.{ .values = &.{}, .keys = &.{Datum.json(.{ .integer = @as(i64, @intCast((2047 - i) % 256)) - 128 })}, .ordinal = 2047 - i });
        var previous: ?Row = null;
        var prior = std.heap.ArenaAllocator.init(a);
        defer prior.deinit();
        var current = std.heap.ArenaAllocator.init(a);
        defer current.deinit();
        var count: usize = 0;
        while (true) {
            _ = current.reset(.retain_capacity);
            const row = (try sort.next(current.allocator())) orelse break;
            if (previous) |old| try std.testing.expect((try operators.compareRows(old, row, sort.orders)) == .lt);
            _ = prior.reset(.retain_capacity);
            previous = .{ .values = &.{}, .keys = try prior.allocator().dupe(Datum, row.keys), .ordinal = row.ordinal };
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 2048), count);
        try std.testing.expectEqual(@as(usize, 1), sort.radix_runs);
    }
}

test "SQL independent merge compaction jobs preserve sorted output and release files" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    {
        var sort = Sort.init(a, &manager, &.{.{}}, 1024 * 1024);
        defer sort.deinit();
        sort.merge_fan_in = 2;
        // Supply independent runs to ensure two merge lanes can be admitted.
        for (0..8) |run| {
            var file = try Sequential.init(&manager, 4096);
            errdefer file.close();
            for (0..128) |i| _ = try file.append(.{ .values = &.{}, .keys = &.{Datum.json(.{ .integer = @intCast(i * 8 + run) })}, .ordinal = i * 8 + run }, none);
            try file.seal();
            sort.runs[run] = file;
        }
        var count: usize = 0;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        while (true) {
            _ = arena.reset(.retain_capacity);
            const row = (try sort.next(arena.allocator())) orelse break;
            try std.testing.expectEqual(@as(i64, @intCast(count)), row.keys[0].value.integer);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1024), count);
        try std.testing.expect(sort.parallel_merges_started >= 2);
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL borrowed spill blocks support partial admission retries and rewinds" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 4096);
    defer file.close();
    const wide: [4096]u8 = @splat('w');
    for (0..37) |index| _ = try file.append(.{ .values = &.{if (index % 8 == 0) Datum.json(.{ .string = &wide }) else Datum.json(.{ .integer = @intCast(index) })}, .keys = &.{}, .ordinal = index }, none);
    for (0..2) |_| {
        file.rewind();
        var offset: u64 = 0;
        while (offset < file.size) {
            const first = try file.readBatchBorrowed(offset, 7);
            const again = try file.readBatchBorrowed(offset, 7);
            try std.testing.expectEqual(first.rows.len, again.rows.len);
            try std.testing.expectEqual(offset, again.rows[0].ordinal);
            // Simulate bounded hash admission accepting only a prefix.
            const accepted = @min(@as(usize, 2), again.rows.len);
            for (again.rows[0..accepted], 0..) |row, i| try std.testing.expectEqual(offset + i, row.ordinal);
            offset += accepted;
        }
        try std.testing.expectEqual(@as(u64, 37), offset);
    }
}

fn ownedCompactBlockScenario(a: Allocator) !void {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 64 * 1024);
    var open = true;
    defer if (open) file.close();
    for (0..300) |index| _ = try file.append(.{ .values = &.{
        Datum.json(.{ .integer = 9007199254740993 + @as(i64, @intCast(index)) }),
        Datum.json(.{ .float = if (index % 2 == 0) -0.0 else 1.5 }),
        if (index % 3 == 0) Datum{} else Datum.json(.null),
        Datum.json(.{ .string = "embedded\x00text" }),
        Datum.json(.{ .number_string = "1.0000000000000001" }),
        if (index % 2 == 0) Datum.json(.{ .bool = true }) else Datum.json(.{ .integer = -1 }),
    }, .keys = &.{Datum.json(.{ .integer = @intCast(index) })}, .ordinal = index }, none);
    try file.seal();
    const first = try file.readOwnedBlock(0);
    defer first.release();
    try std.testing.expect(first.encoded != null);
    const second = try file.readOwnedBlock(first.count());
    defer second.release();
    file.rewind();
    _ = try file.readBorrowed(0);
    const transferred = try file.readOwnedBlock(1);
    defer transferred.release();
    try std.testing.expect(transferred.encoded == null);
    const resumed = try file.readOwnedBlock(1 + transferred.count());
    defer resumed.release();
    try std.testing.expect(resumed.encoded != null);
    try std.testing.expectEqual(9007199254740993 + @as(i64, @intCast(first.count())), (try resumed.cell(0, 0)).value.integer);
    // Closing the source releases neither lease's backing payload.
    file.close();
    open = false;
    for ([_]*Sequential.OwnedBlock{ first, second }, 0..) |block, block_index| {
        for (0..block.count()) |row| {
            const index = row + (if (block_index == 0) @as(usize, 0) else first.count());
            try std.testing.expectEqual(9007199254740993 + @as(i64, @intCast(index)), (try block.cell(row, 0)).value.integer);
            if (index % 2 == 0) try std.testing.expect(std.math.signbit((try block.cell(row, 1)).value.float));
            const nullable = try block.cell(row, 2);
            try std.testing.expectEqual(index % 3 == 0, nullable.sql_null);
            try std.testing.expect(nullable.value == .null);
            try std.testing.expectEqualStrings("embedded\x00text", (try block.cell(row, 3)).value.string);
            try std.testing.expectEqualStrings("1.0000000000000001", (try block.cell(row, 4)).value.number_string);
            try std.testing.expectEqual(index % 2 == 0, (try block.cell(row, 5)).value == .bool);
            const keys = try block.keyRow(row);
            try std.testing.expectEqual(@as(i64, @intCast(index)), keys.keys[0].value.integer);
        }
    }
}
test "SQL compact owned spill blocks retain exact cells after source close and unwind every allocation" {
    try ownedCompactBlockScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, ownedCompactBlockScenario, .{});
}

fn dictionarySpillScenario(a: Allocator) !void {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 64 * 1024);
    defer file.close();
    for (0..128) |row| _ = try file.append(.{
        .values = &.{
            if (row % 7 == 0) Datum{} else Datum.json(.{ .integer = 9007199254740993 }),
            if (row % 11 == 0) Datum.json(.null) else Datum.json(.{ .string = "long repeated string with embedded\x00NUL preserving exact bytes" }),
            Datum.json(.{ .float = if (row % 2 == 0) -0.0 else 0.0 }),
        },
        .keys = &.{Datum.json(.{ .integer = @intCast(row % 3) })},
        .ordinal = row,
    }, none);
    const block = try file.readOwnedBlock(0);
    defer block.release();
    try std.testing.expect(block.encoded != null);
    try std.testing.expect(block.encoded.?.values[0].values == .dictionary);
    try std.testing.expect(block.encoded.?.values[1].values == .dictionary);
    try std.testing.expect(block.encoded.?.values[2].values == .dictionary);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try block.batch(arena.allocator(), false);
    const encoded = (try batch.dictionaryColumn(arena.allocator(), 0)).?;
    for (0..block.count()) |row| {
        const integer = try batch.cell(a, row, 0);
        try std.testing.expectEqual(row % 7 == 0, integer.sql_null);
        if (!integer.sql_null) try std.testing.expectEqual(@as(i64, 9007199254740993), integer.value.integer);
        try std.testing.expectEqualDeep(integer, try encoded.cell(a, row, 0));
        const number = try block.cell(row, 2);
        try std.testing.expectEqual(@as(u64, if (row % 2 == 0) 1 << 63 else 0), @as(u64, @bitCast(number.value.float)));
        try std.testing.expectEqual(@as(u64, row), block.ordinal(row));
    }
    file.rewind();
    const expanded = try file.readBatchBorrowed(0, 256);
    for (expanded.rows, 0..) |row, index| for (row.values, 0..) |value, column| try std.testing.expectEqualDeep(try block.cell(index, column), value);
}
test "SQL dictionary spill preserves exact numeric bits strings and both NULL domains" {
    try dictionarySpillScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, dictionarySpillScenario, .{});
}

test "SQL dictionary spill rejects invalid IDs counts tags and truncated payloads" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    // Dictionary integer column, one entry, one present row.
    const valid = [_]u8{ 6, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    for (0..4) |mutation| {
        var bytes = valid;
        const length: usize = if (mutation == 3) bytes.len - 1 else bytes.len;
        switch (mutation) {
            0 => bytes[19] = 1,
            1 => bytes[3] = 2,
            2 => bytes[2] = 255,
            else => {},
        }
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var decoder: Decoder = .{ .manager = &manager, .a = arena.allocator(), .bytes = bytes[0..length] };
        try std.testing.expectError(error.InvalidSqlSpill, Sequential.EncodedColumn.decode(&decoder, 1));
    }
}

fn retainingInputScenario(a: Allocator) !void {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 1024);
    defer file.close();
    const wide: [4096]u8 = @splat('x');
    for (0..21) |index| _ = try file.append(.{ .values = &.{Datum.json(.{ .string = if (index == 10) &wide else "small" })}, .keys = &.{Datum.json(.{ .integer = @intCast(index) })}, .ordinal = index }, none);
    // The wide row is always a singleton; additional singleton tails depend
    // on Datum width and the bounded writer's physical block packing. Prove
    // that every singleton is borrowed, not a fixed incidental tail count.
    var singletons: usize = 0;
    {
        var reader = try file.openReader();
        defer reader.deinit();
        for (0..21) |index| {
            _ = try reader.readBorrowed(index);
            singletons += @intFromBool(reader.read_single_offset == index);
        }
    }
    try std.testing.expect(singletons > 0);
    for ([_]bool{ false, true }) |borrow_compact| {
        file.rewind();
        var offset: usize = 0;
        var owned: usize = 0;
        var borrowed: usize = 0;
        while (offset < 21) {
            var input = if (borrow_compact) try file.readInputBlockBorrowed(offset) else try file.readInputBlock(offset);
            defer input.deinit();
            if (input == .owned) owned += 1 else borrowed += 1;
            const block = input.view();
            for (0..block.count()) |index| {
                try std.testing.expectEqual(@as(u64, offset + index), block.ordinal(index));
                try std.testing.expectEqual(@as(i64, @intCast(offset + index)), (try block.keyCell(index, 0)).value.integer);
                try std.testing.expectEqualStrings(if (offset + index == 10) &wide else "small", (try block.cell(index, 0)).value.string);
            }
            offset += block.count();
        }
        if (borrow_compact) {
            try std.testing.expectEqual(@as(usize, 0), owned);
            try std.testing.expect(borrowed > 1);
        } else {
            try std.testing.expect(owned != 0);
            try std.testing.expectEqual(singletons, borrowed);
        }
    }
}
test "SQL retaining spill consumers borrow singleton records between compact blocks" {
    try retainingInputScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, retainingInputScenario, .{});
}

test "SQL NUMERIC column spills preserve canonical bytes scale arrays and ownership" {
    const Harness = struct {
        fn run(a: Allocator) !void {
            const numeric_value = @import("numeric_value.zig");
            const arrays = @import("array_value.zig");
            var context: numeric_value.Context = .{ .alloc = a };
            var number = try numeric_value.parse(&context, "9007199254740993.1200");
            defer number.deinit();
            const datum = Datum.typedNumeric(&number.value);
            const array = try arrays.Value.init(.numeric, &.{.{ .length = 2, .lower = -1 }}, &.{ datum, .{} }, .{});
            const rows = [_]Row{.{ .values = &.{ datum, Datum.typedArray(&array) }, .keys = &.{datum}, .ordinal = 17 }};
            const bytes = try encodeColumnarBlockAlloc(a, &rows, 4096);
            defer a.free(bytes);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const block = try decodeColumnarBlockInArena(arena.allocator(), bytes, 4096);
            defer block.deinit();
            const restored = try block.cell(0, 0);
            try std.testing.expect(restored.numeric != null and !restored.sql_null);
            try std.testing.expect(restored.numeric.?.digits.ptr != number.value.digits.ptr);
            try std.testing.expectEqual(@as(u16, 4), restored.numeric.?.scale);
            try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(restored, datum));
            try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(try block.keyCell(0, 0), datum));
            const restored_array = (try block.cell(0, 1)).array.?;
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try array.compare(restored_array.*, &work));
            // Fault injection can fail before the decoder reaches the corrupt
            // suffix. Preserve that allocation error for the failure harness.
            if (decodeColumnarBlockInArena(arena.allocator(), bytes[0 .. bytes.len - 1], 4096)) |_| {
                return error.TestExpectedError;
            } else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidSqlSpill, err);
            }
        }
    };
    try Harness.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL native immutable column block retains the frozen v1 integer layout" {
    const a = std.testing.allocator;
    const rows = [_]Row{
        .{ .values = &.{Datum.json(.{ .integer = 9007199254740993 })}, .keys = &.{}, .ordinal = 0 },
        .{ .values = &.{Datum.json(.{ .integer = -9007199254740993 })}, .keys = &.{}, .ordinal = 1 },
    };
    const golden = "\x4e\x43\x42\x01\x02\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x20\x00\xff\xff\xff\xff\xff\xff\xdf\xff";
    const bytes = try encodeColumnarBlockAlloc(a, &rows, 4096);
    defer a.free(bytes);
    try std.testing.expectEqualSlices(u8, golden, bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const block = try decodeColumnarBlockInArena(arena.allocator(), golden, 4096);
    defer block.deinit();
    try std.testing.expectEqual(@as(i64, -9007199254740993), (try block.cell(1, 0)).value.integer);
    try std.testing.expectEqual(@as(u64, 1), block.ordinals[1]);
}

fn portableColumnBlockScenario(a: Allocator) !void {
    var cells: [32][4]Datum = undefined;
    var keys: [32][1]Datum = undefined;
    var rows: [32]Row = undefined;
    for (&rows, 0..) |*row, index| {
        cells[index] = .{
            Datum.json(.{ .integer = if (index % 2 == 0) 9007199254740993 else -9007199254740993 }),
            Datum.json(.{ .float = if (index % 2 == 0) -0.0 else 0.0 }),
            Datum.json(.{ .string = "a long repeated payload with an embedded\x00 byte" }),
            Datum.json(.{ .number_string = "123456789012345678901234567890.123456789" }),
        };
        keys[index][0] = if (index % 2 == 0) Datum{} else Datum.json(.null);
        row.* = .{ .values = &cells[index], .keys = &keys[index], .ordinal = index };
    }
    const bytes = try encodeColumnarBlockAlloc(a, &rows, 1024 * 1024);
    defer a.free(bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const block = try decodeColumnarBlockInArena(arena.allocator(), bytes, 1024 * 1024);
    defer block.deinit();
    try std.testing.expectEqual(@as(usize, rows.len), block.count());
    try std.testing.expect(block.values[0].values == .dictionary);
    try std.testing.expect(block.values[2].values == .dictionary);
    for (rows, 0..) |row, index| {
        try std.testing.expectEqual(row.ordinal, block.ordinals[index]);
        try std.testing.expectEqual(row.values[0].value.integer, (try block.cell(index, 0)).value.integer);
        try std.testing.expectEqual(@as(u64, @bitCast(row.values[1].value.float)), @as(u64, @bitCast((try block.cell(index, 1)).value.float)));
        try std.testing.expectEqualStrings(row.values[2].value.string, (try block.cell(index, 2)).value.string);
        try std.testing.expectEqualStrings(row.values[3].value.number_string, (try block.cell(index, 3)).value.number_string);
        try std.testing.expectEqual(row.keys[0].sql_null, (try block.keyCell(index, 0)).sql_null);
    }
    const dictionary = (try block.dictionaryColumn(arena.allocator(), 0, false)).?;
    try std.testing.expectEqual(try block.dictionaryIdentity(0, 0, false), try block.dictionaryIdentity(2, 0, false));
    try std.testing.expect((try block.dictionaryIdentity(0, 0, false)).? != (try block.dictionaryIdentity(1, 0, false)).?);
    const selected = try dictionary.select(arena.allocator(), &.{ 31, 0, 2 });
    try std.testing.expectEqual(@as(i64, -9007199254740993), (try selected.cell(arena.allocator(), 0, 0)).value.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try selected.cell(arena.allocator(), 2, 0)).value.integer);
    const borrowed = (try block.cell(0, 2)).value.string;
    try std.testing.expect(@intFromPtr(borrowed.ptr) >= @intFromPtr(bytes.ptr) and @intFromPtr(borrowed.ptr) + borrowed.len <= @intFromPtr(bytes.ptr) + bytes.len);
}

test "SQL native immutable column blocks borrow dictionaries and preserve null and float identity" {
    try portableColumnBlockScenario(std.testing.allocator);
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), portableColumnBlockScenario, .{});
}

test "SQL native immutable column blocks reject truncation versions and callbacks" {
    const a = std.testing.allocator;
    const rows = [_]Row{.{ .values = &.{Datum.json(.{ .string = "binary\x00 payload" })}, .keys = &.{Datum.json(.null)}, .ordinal = 9 }};
    const bytes = try encodeColumnarBlockAlloc(a, &rows, 4096);
    defer a.free(bytes);
    for (0..bytes.len) |len| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        try std.testing.expectError(error.InvalidSqlSpill, decodeColumnarBlockInArena(arena.allocator(), bytes[0..len], 4096));
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const bad = try arena.allocator().dupe(u8, bytes);
    bad[3] = 255;
    try std.testing.expectError(error.InvalidSqlSpill, decodeColumnarBlockInArena(arena.allocator(), bad, 4096));
    try std.testing.expectError(error.InvalidSqlSpill, decodeColumnarBlockInArena(arena.allocator(), bytes, bytes.len - 1));
    var callback: scalar.PatternSet = undefined;
    const callback_rows = [_]Row{.{ .values = &.{.{ .patterns = &callback, .sql_null = false }}, .keys = &.{}, .ordinal = 0 }};
    try std.testing.expectError(error.InvalidSqlSpill, encodeColumnarBlockAlloc(a, &callback_rows, 4096));
}

test "SQL spill independent replay readers seek column block boundaries and retain their own buffers" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var run = try Sequential.init(&manager, 4096);
    defer run.close();
    for (0..100) |i| _ = try run.append(.{ .values = &.{Datum.json(.{ .integer = @intCast(i) })}, .keys = &.{}, .ordinal = i }, none);
    const boundary = try run.replayBoundary();
    for (100..200) |i| _ = try run.append(.{ .values = &.{Datum.json(.{ .integer = @intCast(i) })}, .keys = &.{}, .ordinal = i }, none);
    var first = try run.reader(a, .{ .row = 0, .byte = 0 }, boundary.row);
    defer first.deinit();
    var second = try run.reader(a, boundary, run.size);
    defer second.deinit();
    for (0..100) |i| {
        const x = (try first.next(1)).?;
        const y = (try second.next(1)).?;
        try std.testing.expectEqual(@as(i64, @intCast(i)), x.rows[0].values[0].value.integer);
        try std.testing.expectEqual(@as(i64, @intCast(i + 100)), y.rows[0].values[0].value.integer);
    }
    try std.testing.expectEqual(null, try first.next(1));
    try std.testing.expectEqual(null, try second.next(1));
}

test "SQL typed sort keeps wide payloads in memory and leases columns across pulls" {
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 2 * 1024 * 1024 };
    const a = budget.allocator();
    var context: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check };
    defer manager.deinit();
    var sort = Sort.init(a, &manager, &.{.{}}, 2 * 1024 * 1024);
    defer sort.deinit();
    sort.parallel_runs = false;
    for (0..2048) |i| {
        const key = Datum.json(.{ .integer = @intCast(2047 - i) });
        var row: [16]Datum = undefined;
        for (&row, 0..) |*value, column| value.* = Datum.json(.{ .integer = 9007199254740993 + key.value.integer * 16 + @as(i64, @intCast(column)) });
        try sort.add(.{ .values = &row, .keys = &.{key}, .ordinal = i });
    }
    const first = (try sort.nextLeased()).?;
    defer first.release();
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(usize, 1), sort.radix_runs);
    for (1..2048) |i| {
        const lease = (try sort.nextLeased()).?;
        defer lease.release();
        try std.testing.expectEqual(@as(i64, 9007199254740993 + @as(i64, @intCast(i * 16 + 15))), (try lease.cell(15)).value.integer);
    }
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try first.cell(0)).value.integer);
    try std.testing.expect((try sort.nextLeased()) == null);
}

test "SQL selected typed spill preserves dictionary integers and both null domains without evaluating excluded IDs" {
    const a = std.testing.allocator;
    var context: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 32 * 1024);
    defer file.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var indices: [300]u32 = undefined;
    var selection: [299]usize = undefined;
    var ordinals: [299]u64 = undefined;
    for (&indices, 0..) |*id, i| id.* = @intCast(i % 3);
    indices[5] = 999;
    for (&selection, &ordinals, 0..) |*position, *ordinal, i| {
        position.* = if (i < 5) i else i + 1;
        ordinal.* = position.*;
    }
    const batch: @import("execution_batch.zig").Batch = .{ .dictionary = .{ .values = &.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.null) }, .indices = &indices } };
    const selected = try batch.select(arena.allocator(), &selection);
    const dictionary = (try selected.dictionaryColumn(arena.allocator(), 0)).?;
    try std.testing.expectEqual(@as(usize, 3), dictionary.dictionary.values.len);
    try file.appendBatch(selected, .{ .vectors = .{ .values = &.{}, .count = selection.len } }, &ordinals);
    try file.seal();
    for (selection, 0..) |position, offset| {
        const row = (try file.readBorrowed(offset)).row;
        try std.testing.expectEqual(@as(u64, position), row.ordinal);
        const value = row.values[0];
        if (position % 3 == 0) try std.testing.expectEqual(@as(i64, 9007199254740993), value.value.integer) else {
            try std.testing.expect(value.value == .null);
            try std.testing.expectEqual(position % 3 == 1, value.sql_null);
        }
    }
}

fn typedSpillVectorScenario(a: Allocator) !void {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 64 * 1024);
    defer file.close();
    for (0..128) |row| _ = try file.append(.{ .values = &.{
        if (row % 7 == 0) Datum{} else Datum.json(.{ .integer = 9007199254740993 }),
        Datum.json(.{ .float = if (row % 2 == 0) -0.0 else 0.0 }),
        if (row % 11 == 0) Datum{} else Datum.json(.{ .string = "repeated long string with embedded\x00NUL" }),
        Datum.json(.{ .integer = @intCast(row) }),
        Datum.json(.{ .bool = row % 3 == 0 }),
        Datum{},
        Datum.json(.{ .string = if (row % 2 == 0) "{\"a\":1}" else "null" }),
    }, .keys = &.{}, .ordinal = row }, none);
    const block = try file.readOwnedBlock(0);
    defer block.release();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const integer = try block.columnVector(out, 0, .i64);
    const floating = try block.columnVector(out, 1, .f64);
    const text = try block.columnVector(out, 2, .bytes);
    const unique = try block.columnVector(out, 3, .i64);
    const boolean = try block.columnVector(out, 4, .bool);
    const missing = try block.columnVector(out, 5, .f64);
    const json = try block.columnVector(out, 6, .json);
    try std.testing.expect(integer.values == .dictionary_i64);
    try std.testing.expect(floating.values == .dictionary_f64);
    try std.testing.expect(text.values == .dictionary_bytes);
    try std.testing.expect(unique.values == .i64);
    for (0..block.count()) |row| {
        try std.testing.expectEqual(row % 7 == 0, integer.nulls.isNull(row));
        if (!integer.nulls.isNull(row)) try std.testing.expectEqual(@as(i64, 9007199254740993), try integer.integerAt(row));
        const actual = floating.values.dictionary_f64.at(row);
        try std.testing.expectEqual(@as(u64, @bitCast((try block.cell(row, 1)).value.float)), @as(u64, @bitCast(actual)));
        try std.testing.expectEqual(row % 11 == 0, text.nulls.isNull(row));
        if (!text.nulls.isNull(row)) try std.testing.expectEqualStrings((try block.cell(row, 2)).value.string, text.values.dictionary_bytes.at(row));
        try std.testing.expectEqual(@as(i64, @intCast(row)), try unique.integerAt(row));
        try std.testing.expectEqual(row % 3 == 0, boolean.values.bool[row]);
        try std.testing.expect(missing.nulls.isNull(row));
        try std.testing.expectEqualStrings((try block.cell(row, 6)).value.string, json.values.json[row]);
    }
    try std.testing.expectError(error.InvalidSqlSpill, block.columnVector(out, 3, .f64));
}

test "SQL typed spill vectors preserve dictionaries nulls exact integers and float bits under allocation failure" {
    try typedSpillVectorScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, typedSpillVectorScenario, .{});
}

test "SQL fused sort cohorts reserve shared file capacity for merges" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    for ([_]usize{ 4, 24, 32 }) |run_limit| {
        var storage: [8]Sort = undefined;
        const sorts = storage[0..if (run_limit == 24) @as(usize, 2) else 8];
        for (sorts) |*sort| {
            sort.* = Sort.init(a, &manager, &.{.{}}, 4096);
            sort.run_limit = run_limit;
            sort.shared_runs = run_limit == 32;
            sort.parallel_runs = false;
        }
        defer for (sorts) |*sort| sort.deinit();
        for (0..512) |i| for (sorts) |*sort| {
            try sort.add(.{ .values = &.{}, .keys = &.{Datum.json(.{ .integer = @intCast(511 - i) })}, .ordinal = i });
        };
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        for (sorts) |*sort| {
            for (0..512) |i| {
                _ = arena.reset(.retain_capacity);
                const row = (try sort.next(arena.allocator())).?;
                try std.testing.expectEqual(@as(i64, @intCast(i)), row.keys[0].value.integer);
            }
            try std.testing.expect((try sort.next(arena.allocator())) == null);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL spill sorts singleton rows larger than the preferred run buffer" {
    const a = std.testing.allocator;
    var quota: @import("memory_budget.zig") = .{ .backing = a, .limit = 512 * 1024 };
    var dummy: u8 = 0;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var manager: Manager = .{ .alloc = quota.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .max_record_bytes = quota.limit };
    defer manager.deinit();
    var sort = Sort.init(quota.allocator(), &manager, &.{.{}}, 4096);
    defer sort.deinit();
    const text = try a.alloc(u8, 32 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    for (0..3) |i| try sort.add(.{ .values = &.{Datum.json(.{ .string = text })}, .keys = &.{Datum.json(.{ .integer = @intCast(3 - i) })}, .ordinal = i });
    for (0..3) |i| {
        var arena = std.heap.ArenaAllocator.init(quota.allocator());
        defer arena.deinit();
        const row = (try sort.next(arena.allocator())).?;
        try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.keys[0].value.integer);
        try std.testing.expectEqualStrings(text, row.values[0].value.string);
    }
    var arena = std.heap.ArenaAllocator.init(quota.allocator());
    defer arena.deinit();
    try std.testing.expect((try sort.next(arena.allocator())) == null);
    try std.testing.expect(quota.peak <= quota.limit);
}

fn sharedSortFaultScenario(a: Allocator) !void {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    {
        var sort = Sort.init(a, &manager, &.{.{}}, 1024);
        defer sort.deinit();
        sort.shared_runs = true;
        sort.parallel_runs = false;
        for (0..24) |i| try sort.add(.{ .values = &.{Datum.json(.{ .integer = 9007199254740993 })}, .keys = &.{Datum.json(.{ .integer = @intCast(23 - i) })}, .ordinal = i });
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        for (0..24) |i| {
            _ = arena.reset(.free_all);
            const row = (try sort.next(arena.allocator())).?;
            try std.testing.expectEqual(@as(i64, @intCast(i)), row.keys[0].value.integer);
            try std.testing.expectEqual(@as(i64, 9007199254740993), row.values[0].value.integer);
        }
        try std.testing.expect((try sort.next(arena.allocator())) == null);
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
}
test "SQL shared spill extents unwind every allocation failure" {
    try sharedSortFaultScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, sharedSortFaultScenario, .{});
}

test "SQL shared logical runs avoid repeatedly rewriting large retained runs" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var written: [2]u64 = undefined;
    for (0..2) |mode| {
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
        defer manager.deinit();
        {
            var sort = Sort.init(a, &manager, &.{.{}}, 4096);
            defer sort.deinit();
            sort.parallel_runs = false;
            sort.shared_runs = mode == 1;
            if (mode == 0) sort.run_limit = 4;
            for (0..4096) |i| try sort.add(.{ .values = &.{}, .keys = &.{Datum.json(.{ .integer = @intCast(4095 - i) })}, .ordinal = i });
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            for (0..4096) |i| {
                _ = arena.reset(.retain_capacity);
                const row = (try sort.next(arena.allocator())).?;
                try std.testing.expectEqual(@as(i64, @intCast(i)), row.keys[0].value.integer);
            }
            written[mode] = manager.written_bytes;
        }
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        try std.testing.expectEqual(@as(usize, 0), manager.files);
    }
    std.debug.print("Shared spill runs: legacy_written={d} balanced_written={d}\n", .{ written[0], written[1] });
    try std.testing.expect(written[1] < written[0]);
}

test "SQL dynamic exact keys native byte sort handles variable prefixes long ties and spill rewind" {
    const a = std.testing.allocator;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    // Exercise the radix path in memory and the same order across run merges.
    for ([_]usize{ 8 * 1024 * 1024, 16 * 1024 }) |memory_bytes| {
        var sort = Sort.init(a, &manager, &.{.{}}, memory_bytes);
        defer sort.deinit();
        sort.encoded_bytes = true;
        sort.parallel_runs = false;
        sort.shared_runs = true;
        const texts = [_][]const u8{ "", "a", "a\x00", "a\x00b", "a\xff", "abcdefghijklmnopqrstuvwx123456789", "abcdefghijklmnopqrstuvwx123456789\x00", "abcdefghijklmnopqrstuvwx1234567890" };
        for (0..4096) |i| {
            const ordinal = 4095 - i;
            try sort.add(.{ .keys = &.{Datum.json(.{ .string = texts[ordinal % texts.len] })}, .values = &.{}, .ordinal = ordinal });
        }
        try sort.finish();
        if (memory_bytes > 1024 * 1024) try std.testing.expect(sort.radix_runs != 0);
        for (0..2) |_| {
            var count: usize = 0;
            var previous: ?[]u8 = null;
            defer if (previous) |bytes| a.free(bytes);
            var prior_ordinal: u64 = 0;
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.retain_capacity);
                const row = try sort.next(scratch.allocator()) orelse break;
                const key = row.keys[0].value.string;
                if (previous) |old| {
                    const order = std.mem.order(u8, old, key);
                    try std.testing.expect(order == .lt or (order == .eq and prior_ordinal < row.ordinal));
                    a.free(old);
                }
                previous = null;
                previous = try a.dupe(u8, key);
                prior_ordinal = row.ordinal;
                count += 1;
            }
            try std.testing.expectEqual(@as(usize, 4096), count);
            try sort.rewind();
        }
    }
}

test "SQL narrow spill directories bound growing-prefix rewrites" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    const count = 32768;
    const payload: [128]u8 = @splat('x');
    var dummy: u8 = 0;
    var written: [2]u64 = undefined;
    for ([_]usize{ 4, 24 }, 0..) |limit, variant| {
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
        defer manager.deinit();
        var sort = Sort.init(a, &manager, &.{.{}}, 512 * 1024);
        sort.run_limit = limit;
        sort.parallel_runs = false;
        defer sort.deinit();
        for (0..count) |index| {
            try sort.add(.{ .values = &.{Datum.json(.{ .string = &payload })}, .keys = &.{Datum.json(.{ .integer = @intCast(count - 1 - index) })}, .ordinal = index });
        }
        if (limit == 4) try std.testing.expectEqual(@as(usize, 4), sort.fanIn());
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (0..count) |index| {
            _ = scratch.reset(.retain_capacity);
            const row = (try sort.next(scratch.allocator())).?;
            try std.testing.expectEqual(@as(i64, @intCast(index)), row.keys[0].value.integer);
            try std.testing.expectEqualStrings(&payload, row.values[0].value.string);
        }
        try std.testing.expect((try sort.next(a)) == null);
        written[variant] = manager.written_bytes;
        if (limit == 4) try std.testing.expect(manager.peak_files <= 8);
    }
    std.debug.print("SQL narrow directory rewrite bytes: narrow={d} wide={d}\n", .{ written[0], written[1] });
    try std.testing.expect(written[0] * 2 <= written[1] * 3);
}

test "SQL presorted streams retain one run without merge rewrites" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
    defer manager.deinit();
    var sort = Sort.init(a, &manager, &.{.{}}, 128 * 1024);
    sort.presorted = true;
    defer sort.deinit();
    for (0..32768) |index| try sort.add(.{ .keys = &.{Datum.json(.{ .integer = @intCast(index) })}, .values = &.{Datum.json(.{ .integer = @intCast(index * 3) })}, .ordinal = index });
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (0..32768) |index| {
        _ = scratch.reset(.retain_capacity);
        const row = (try sort.next(scratch.allocator())).?;
        try std.testing.expectEqual(@as(i64, @intCast(index)), row.keys[0].value.integer);
        try std.testing.expectEqual(@as(i64, @intCast(index * 3)), row.values[0].value.integer);
    }
    try std.testing.expect((try sort.next(a)) == null);
    try std.testing.expectEqual(@as(usize, 1), manager.peak_files);
    try std.testing.expectEqual(@as(usize, 0), manager.merges);
}

test "SQL compact columnar arrays preserve v1 compatibility and avoid element expansion" {
    const a = std.testing.allocator;
    const arrays = @import("array_value.zig");
    const cells = try a.alloc(Datum, 32768);
    defer a.free(cells);
    @memset(cells, .json(.{ .integer = 42 }));
    const array = try arrays.Value.init(.int64, &.{.{ .length = 32768, .lower = -3 }}, cells, .{});
    const rows = [_]Row{.{ .values = &.{.typedArray(&array)}, .keys = &.{}, .ordinal = 7 }};
    var peaks: [2]usize = undefined;
    var sizes: [2]usize = undefined;
    for ([_]ColumnarVersion{ .v1, .v2 }, 0..) |version, i| {
        const bytes = try encodeColumnarBlockVersionAlloc(a, &rows, 8 * 1024 * 1024, version);
        defer a.free(bytes);
        sizes[i] = bytes.len;
        var nr: @import("../storage/lite/test_allocator.zig").NoResizeAllocator = .{ .backing = a };
        var budget: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = nr.allocator() };
        {
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            const block = try decodeColumnarBlockInArena(arena.allocator(), bytes, 8 * 1024 * 1024);
            defer block.deinit();
            const decoded = (try block.cell(0, 0)).array.?;
            try std.testing.expectEqual(version == .v2, decoded.stored != null);
            try std.testing.expectEqual(@as(usize, 32768), decoded.cardinality());
            try std.testing.expectEqual(@as(i64, 42), decoded.flatCell(32767).value.integer);
            try std.testing.expectEqual(@as(i32, -3), decoded.dimensions[0].lower);
            peaks[i] = budget.peak;
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        if (version == .v2) {
            // A v1 decoder must reject the v2-only tag, even if framing was
            // maliciously relabeled. Existing NCB1 bytes/goldens stay frozen.
            bytes[3] = 1;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            try std.testing.expectError(error.InvalidSqlSpill, decodeColumnarBlockInArena(arena.allocator(), bytes, 8 * 1024 * 1024));
        }
    }
    try std.testing.expect(peaks[1] < peaks[0] / 4);
    try std.testing.expect(sizes[1] < sizes[0]);
    std.debug.print("SQL compact columnar: v1_decode_peak={} v2_decode_peak={} v1_bytes={} v2_bytes={}\n", .{ peaks[0], peaks[1], sizes[0], sizes[1] });
}

test "SQL compact columnar arrays reject truncation and unwind allocation failures" {
    const Probe = struct {
        fn run(a: Allocator) !void {
            var source = try @import("array_text.zig").decode(a, .text, "[-1:1]={\"é\",NULL,\"retained\"}", .{});
            defer source.deinit();
            const input = [_]Row{.{ .values = &.{.typedArray(&source.value)}, .keys = &.{}, .ordinal = 9 }};
            const bytes = try encodeColumnarBlockVersionAlloc(a, &input, 4096, .v2);
            defer a.free(bytes);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const block = try decodeColumnarBlockInArena(arena.allocator(), bytes, 4096);
            defer block.deinit();
            const output = (try block.cell(0, 0)).array.?;
            try std.testing.expect(output.stored != null);
            try std.testing.expectEqualStrings("retained", output.flatCell(2).value.string);
            // Record decoders free their temporary input after returning.
            // The default decoder must own compact payload bytes separately.
            var record: std.ArrayList(u8) = .empty;
            defer record.deinit(a);
            var writer: Encoder = .{ .a = a, .bytes = &record, .limit = 4096 };
            try writer.cells(&.{.typedArray(&source.value)});
            var reader: Decoder = .{ .a = arena.allocator(), .bytes = record.items };
            const kept = try reader.cells();
            @memset(record.items, 0);
            try std.testing.expectEqualStrings("retained", kept[0].array.?.flatCell(2).value.string);
            for (0..bytes.len) |end| {
                var partial = std.heap.ArenaAllocator.init(a);
                defer partial.deinit();
                if (decodeColumnarBlockInArena(partial.allocator(), bytes[0..end], 4096)) |_| return error.ExpectedTruncation else |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => try std.testing.expectEqual(error.InvalidSqlSpill, err),
                }
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

fn physicalSpillScenario(a: Allocator) !void {
    const Batch = @import("execution_batch.zig").Batch;
    const Reader = struct {
        fn check(_: *anyopaque) !void {}
        fn read(_: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
            return error.UnexpectedScalarCell;
        }
        fn physical(_: *anyopaque, _: Allocator, column: usize) !?@import("execution_batch.zig").PhysicalColumn {
            const types = @import("../storage/rowsource/types.zig");
            const vector: types.ColumnVector = switch (column) {
                0 => .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 9007199254740993, -7 }, .indices = &.{ 0, 99, 1, 0 } } }, .nulls = .{ .bytes = &.{ 0, 1, 0, 0 } } },
                1 => .{ .name = "f", .values = .{ .f64 = &.{ -0.0, 0, std.math.nan(f64), std.math.inf(f64) } } },
                else => .{ .name = "s", .values = .{ .dictionary_bytes = .{ .values = &.{ "alpha", "beta" }, .indices = &.{ 0, 1, 0, 1 } } } },
            };
            return .{ .vector = vector, .selection = &.{ 3, 0, 1, 2, 3, 0, 1, 2 }, .kind = switch (column) {
                0 => .integer,
                1 => .number,
                else => .string,
            }, .element = if (column == 1) .float64 else null };
        }
    };
    var raw: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &raw, .checkpoint = Reader.check, .async_writes = false, .compression = .none };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 4096);
    defer file.close();
    const values: Batch = .{ .reader = .{ .ptr = &raw, .read = Reader.read, .read_physical = Reader.physical, .count = 8, .width = 3 } };
    const keys: Batch = .{ .vectors = .{ .values = &.{}, .count = 8 } };
    try file.appendBatch(values, keys, &.{ 100, 103, 106, 109, 112, 115, 118, 121 });
    var input = try file.readInputBlock(0);
    defer input.deinit();
    const block = input.view();
    try std.testing.expectEqual(@as(usize, 8), block.count());
    for (0..3) |column| {
        const source = (try Reader.physical(&raw, a, column)).?;
        for (0..8) |row| {
            const expected = try source.cell(row);
            const actual = try block.cell(row, column);
            try std.testing.expectEqual(expected.sql_null, actual.sql_null);
            if (column == 1) try std.testing.expectEqual(@as(u64, @bitCast(expected.value.float)), @as(u64, @bitCast(actual.value.float))) else try std.testing.expectEqualDeep(expected, actual);
            try std.testing.expectEqual(@as(u64, 100 + row * 3), block.ordinal(row));
        }
    }
}
test "SQL spill physical kernels encode selected dictionaries without scalar cells" {
    try physicalSpillScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, physicalSpillScenario, .{});
}

test "SQL column batch encoding preserves both frozen codecs without a row matrix" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const rows = try owned.alloc(Row, 256);
    const ordinals = try owned.alloc(u64, rows.len);
    const vectors = try owned.alloc([]const Datum, 24);
    for (vectors, 0..) |*column, c| {
        const cells = try owned.alloc(Datum, rows.len);
        for (cells, 0..) |*cell, r| cell.* = if (r % 7 == 0) .{} else if (r % 11 == 0) Datum.json(.null) else if (c % 2 == 0) Datum.json(.{ .integer = @intCast(r % 3) }) else Datum.json(.{ .string = "dictionary string" });
        column.* = cells;
    }
    for (rows, ordinals, 0..) |*row, *ordinal, r| {
        const values = try owned.alloc(Datum, vectors.len);
        for (values, vectors) |*value, column| value.* = column[r];
        ordinal.* = r + 42;
        row.* = .{ .values = values, .keys = &.{}, .ordinal = ordinal.* };
    }
    const batch: @import("execution_batch.zig").Batch = .{ .vectors = .{ .values = vectors, .count = rows.len } };
    for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
        const old = try encodeColumnarBlockVersionAlloc(a, rows, 8 << 20, version);
        defer a.free(old);
        const direct = try encodeColumnarBatchVersionAlloc(a, batch, ordinals, 8 << 20, version);
        defer a.free(direct);
        try std.testing.expectEqualSlices(u8, old, direct);
    }
}

test "SQL compact replay keeps one row of descriptors across independent readers and rewind" {
    const Probe = struct {
        fn run(a: Allocator, count_rows: usize) !void {
            const Hook = struct {
                fn check(_: *anyopaque) !void {}
            };
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .snappy, .async_writes = false };
            defer manager.deinit();
            var file = try Sequential.init(&manager, 1024 * 1024);
            defer file.close();
            var cells: [32]Datum = undefined;
            for (0..count_rows) |r| {
                for (&cells, 0..) |*cell, c| cell.* = Datum.json(.{ .integer = @intCast(r * 32 + c) });
                _ = try file.append(.{ .values = &cells, .keys = &.{Datum.json(.{ .integer = @intCast(r) })}, .ordinal = r }, none);
            }
            var first = try file.openReader();
            defer first.deinit();
            var second = try file.openReader();
            defer second.deinit();
            const pinned = try first.readCurrent(0);
            for (0..count_rows) |r| {
                const row = (try second.readCurrent(r)).row;
                try std.testing.expectEqual(@as(u64, r), row.ordinal);
                try std.testing.expectEqual(@as(i64, @intCast(r * 32 + 31)), row.values[31].value.integer);
                try std.testing.expectEqual(@as(usize, 32), second.compact_values.len);
                try std.testing.expect(second.compact.?.encoded != null);
            }
            try std.testing.expectEqual(@as(i64, 0), pinned.row.values[0].value.integer);
            const rewind = (try second.readCurrent(0)).row;
            try std.testing.expectEqual(@as(i64, 31), rewind.values[31].value.integer);
            try std.testing.expectError(error.InvalidSqlSpill, second.readCurrent(count_rows));
            var batch_reader = try file.openReader();
            defer batch_reader.deinit();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var offset: u64 = 0;
            while (offset < count_rows) {
                _ = scratch.reset(.free_all);
                const batch = try batch_reader.readBatchCurrent(scratch.allocator(), offset, 17);
                try std.testing.expect(batch.values != .rows);
                try std.testing.expectEqual(@as(usize, 0), batch_reader.compact_values.len);
                for (0..batch.values.len()) |lane| try std.testing.expectEqual(@as(i64, @intCast((offset + lane) * 32 + 31)), (try batch.values.cell(scratch.allocator(), lane, 31)).value.integer);
                offset = batch.following;
            }
        }
    };
    try Probe.run(std.testing.allocator, 300);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(stable.allocator(), Probe.run, .{9});
}

test "SQL column batch encoding retires allocating reader payloads per column" {
    const Probe = struct {
        fn run(backing: Allocator) !void {
            const Source = struct {
                fn cell(_: *anyopaque, a: Allocator, _: usize, _: usize) !Datum {
                    const text = try a.alloc(u8, 1024);
                    @memset(text, 'x');
                    return Datum.json(.{ .string = text });
                }
            };
            var dummy: u8 = 0;
            var budget: @import("memory_budget.zig") = .{ .backing = backing, .limit = 512 * 1024 };
            const a = budget.allocator();
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &dummy, .read = Source.cell, .count = 16, .width = 128 } };
            var ordinals: [16]u64 = undefined;
            for (&ordinals, 0..) |*ordinal, i| ordinal.* = i;
            const bytes = try encodeColumnarBatchVersionAlloc(a, batch, &ordinals, 2 << 20, .v2);
            a.free(bytes);
            // Gathering every reader payload would retain 2 MiB before
            // encoding a 137 KiB dictionary block. Only one column stays live.
            try std.testing.expectEqual(@as(usize, 0), budget.live);
        }
    };
    try Probe.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(stable.allocator(), Probe.run, .{});
}

test "SQL physical dictionary emission preserves frozen entry order nulls duplicate values and fallback" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            const Batch = @import("execution_batch.zig").Batch;
            for ([_][]const Datum{
                &.{ .{}, Datum.json(.null), Datum.json(.{ .string = "long dictionary payload" }), Datum.json(.{ .string = "long dictionary payload" }), Datum.json(.{ .string = "second dictionary payload" }) },
                &.{ .{}, Datum.json(.null), Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .integer = -7 }) },
                &.{ .{}, Datum.json(.null), Datum.json(.{ .float = -0.0 }), Datum.json(.{ .float = 0.0 }), Datum.json(.{ .float = std.math.nan(f64) }) },
                &.{ .{}, Datum.json(.null), Datum.json(.{ .bool = true }), Datum.json(.{ .bool = false }), Datum.json(.{ .bool = true }) },
                &.{ .{}, Datum.json(.null), Datum.json(.{ .integer = 7 }), Datum.json(.{ .string = "mixed" }), Datum.json(.{ .bool = true }) },
                &.{ .{}, Datum.json(.null), .{}, Datum.json(.null), .{} },
            }) |values| {
                var indices: [65]u32 = undefined;
                var rows: [65]Row = undefined;
                var cells: [65]Datum = undefined;
                var ordinals: [65]u64 = undefined;
                for (&indices, &rows, &cells, &ordinals, 0..) |*id, *row, *cell, *ordinal, i| {
                    id.* = @intCast((i * 3) % values.len);
                    cell.* = values[id.*];
                    ordinal.* = i + 1;
                    row.* = .{ .values = @as(*const Datum, cell)[0..1], .keys = &.{}, .ordinal = ordinal.* };
                }
                const batch: Batch = .{ .dictionary = .{ .values = values, .indices = &indices } };
                for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
                    const expected = try encodeColumnarBlockVersionAlloc(a, &rows, 1 << 20, version);
                    defer a.free(expected);
                    const actual = try encodeColumnarBatchVersionAlloc(a, batch, &ordinals, 1 << 20, version);
                    defer a.free(actual);
                    try std.testing.expectEqualSlices(u8, expected, actual);
                }
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL column batch region owners bound actual backing memory and retain only returned wire" {
    const Source = struct {
        fn cell(_: *anyopaque, a: Allocator, _: usize, _: usize) !Datum {
            const bytes = try a.alloc(u8, 32 * 1024);
            @memset(bytes, 'x');
            return Datum.json(.{ .string = bytes });
        }
    };
    var dummy: u8 = 0;
    var parent: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 16 << 20 };
    var region = std.heap.ArenaAllocator.init(parent.allocator());
    defer region.deinit();
    var budget: @import("memory_budget.zig") = .{ .backing = region.allocator(), .limit = 16 << 20 };
    const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &dummy, .read = Source.cell, .count = 16, .width = 128 } };
    var ordinals: [16]u64 = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = i;
    const bytes = try encodeColumnarBatchVersionAlloc(budget.allocator(), batch, &ordinals, 16 << 20, .v2);
    defer budget.allocator().free(bytes);
    try std.testing.expectEqual(@as(usize, bytes.len), budget.live);
    try std.testing.expect(parent.peak < 16 << 20);
    std.debug.print("region encoder: inner_peak={d} backing_peak={d} returned={d}\n", .{ budget.peak, parent.peak, bytes.len });
}

test "SQL column batch direct append preserves frozen prefixes quotas and allocator ownership" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const aa = arena.allocator();
            const array = try @import("array_text.zig").decodeLeaky(aa, .int64, "{9007199254740993,NULL,-7}", .{ .compact = true });
            const rows = [_]Row{ .{ .values = &.{Datum.typedArray(&array.value)}, .keys = &.{}, .ordinal = 9 }, .{ .values = &.{.{}}, .keys = &.{}, .ordinal = 10 } };
            const batch: @import("execution_batch.zig").Batch = .{ .rows = &.{ rows[0].values, rows[1].values } };
            for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
                const expected = try encodeColumnarBlockVersionAlloc(aa, &rows, 1 << 20, version);
                var output: std.ArrayList(u8) = .empty;
                defer output.deinit(a);
                try output.appendSlice(a, "existing response");
                try appendColumnarBatchVersion(a, aa, &output, batch, &.{ 9, 10 }, expected.len, version);
                try std.testing.expectEqualSlices(u8, "existing response", output.items[0..17]);
                try std.testing.expectEqualSlices(u8, expected, output.items[17..]);
                const before = output.items.len;
                // Force the quota failure before any output growth allocation.
                try std.testing.expectError(error.SqlProgramLimitExceeded, appendColumnarBatchVersion(a, aa, &output, batch, &.{ 9, 10 }, 0, version));
                try std.testing.expectEqual(before, output.items.len);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL encoded JSON batch passthrough preserves frozen versions mappings and scalar fallbacks" {
    const Fixture = struct {
        fn run(backing: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const View = struct {
                reads: usize = 0,
                fn json(_: *anyopaque, row: usize, _: usize) !?[]const u8 {
                    return switch (row) {
                        0 => "{\"x\":[9007199254740993,1e-3,true,null],\"s\":\"é\\n\"}",
                        1 => "[1,{\"a\":false}]",
                        else => null,
                    };
                }
                fn cell(raw: *anyopaque, alloc: Allocator, row: usize, column: usize) !Datum {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.reads += 1;
                    if (try json(raw, row, column)) |bytes| return Datum.json(try std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{ .parse_numbers = false }));
                    return if (row == 2) Datum{} else Datum.json(.null);
                }
            };
            var view: View = .{};
            const source: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &view, .read = View.cell, .read_json = View.json, .width = 1, .count = 4 } };
            const wrapped: @import("execution_batch.zig").Batch = .{ .columns = .{ .definitions = &.{.{ .name = "j", .type = .json }}, .page = .{ .native = .{ .values = &source, .names = &.{"j"} }, .selection = &.{ 1, 0, 1, 3, 2 } } } };
            const mapped: @import("execution_batch.zig").Batch = .{ .mapped = .{ .source = &wrapped, .ordinals = &.{0}, .types = &.{.{ .kind = .json }}, .selection = &.{ 0, 1, 2, 3, 4 } } };
            const selected = try mapped.select(a, &.{ 0, 1, 2, 3, 4 });
            var rows: [5]Row = undefined;
            for (&rows, 0..) |*row, i| {
                const cells = try a.alloc(Datum, 1);
                cells[0] = try selected.cell(a, i, 0);
                row.* = .{ .values = cells, .keys = &.{}, .ordinal = i };
            }
            for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
                view.reads = 0;
                const actual = try encodeColumnarBatchVersionAlloc(a, selected, &.{ 0, 1, 2, 3, 4 }, 1 << 20, version);
                try std.testing.expectEqual(@as(usize, 2), view.reads);
                const expected = try encodeColumnarBlockVersionAlloc(a, &rows, 1 << 20, version);
                try std.testing.expectEqualSlices(u8, expected, actual);
            }
            var store = @import("typed_store.zig").Store.init(backing);
            defer store.deinit();
            view.reads = 0;
            try store.appendBatch(selected);
            try std.testing.expectEqual(@as(usize, 2), view.reads);
            const retained: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &store, .count = 5 } };
            for (0..5) |i| {
                const before = try selected.encodedJson(i, 0);
                const after = try retained.encodedJson(i, 0);
                try std.testing.expectEqual(before != null, after != null);
                if (before) |bytes| try std.testing.expectEqualStrings(bytes, after.?);
            }
            for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
                const actual = try encodeColumnarBatchVersionAlloc(a, retained, &.{ 0, 1, 2, 3, 4 }, 1 << 20, version);
                try std.testing.expectEqualSlices(u8, try encodeColumnarBlockVersionAlloc(a, &rows, 1 << 20, version), actual);
            }
            _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
            try std.testing.expectEqual(@as(i64, 7), (try store.cell(a, 5, 0)).value.integer);
            try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(try store.cell(a, 0, 0), rows[0].values[0]));
            const cast: @import("execution_batch.zig").Batch = .{ .mapped = .{ .source = &source, .ordinals = &.{0}, .types = &.{.{ .kind = .string }}, .selection = &.{0} } };
            try std.testing.expect(try cast.encodedJson(0, 0) == null);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL lazy JSON blocks validate before publication preserve frozen tags and own cache lifetimes" {
    const Fixture = struct {
        fn run(backing: Allocator) !void {
            var source = std.heap.ArenaAllocator.init(backing);
            defer source.deinit();
            const a = source.allocator();
            const input = "{\"alpha\":[9007199254740993,1e-3,\"é\\n\"],\"bravo\":[]}";
            const root = try std.json.parseFromSliceLeaky(std.json.Value, a, input, .{ .parse_numbers = false });
            const rows = [_]Row{ .{ .values = &.{Datum.json(root)}, .keys = &.{}, .ordinal = 1 }, .{ .values = &.{.{}}, .keys = &.{}, .ordinal = 2 }, .{ .values = &.{Datum.json(.null)}, .keys = &.{}, .ordinal = 3 } };
            for ([_]bool{ false, true }) |forwarding| for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
                const bytes = try encodeColumnarBlockVersionAlloc(a, &rows, 1 << 20, version);
                var metadata = std.heap.ArenaAllocator.init(backing);
                defer metadata.deinit();
                const block = if (forwarding) try decodeColumnarBlockForForwarding(metadata.allocator(), backing, bytes, 1 << 20) else try decodeColumnarBlockWithCache(metadata.allocator(), backing, bytes, 1 << 20);
                defer block.deinit();
                const frozen = (try block.values[0].jsonAt(0)).?;
                try std.testing.expect(frozen.value == null and frozen.text == null);
                try std.testing.expect(!try block.isSqlNull(0, 0));
                try std.testing.expect(try block.isSqlNull(1, 0));
                try std.testing.expect(!try block.isSqlNull(2, 0));
                try std.testing.expectEqualStrings(input, (try block.encodedJson(0, 0, false)).?);
                try std.testing.expect(frozen.value == null);
                const Reader = struct {
                    block: ColumnarBlock,
                    fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) !Datum {
                        const self: *@This() = @ptrCast(@alignCast(raw));
                        return self.block.cell(row, column);
                    }
                    fn json(raw: *anyopaque, row: usize, column: usize) !?[]const u8 {
                        const self: *@This() = @ptrCast(@alignCast(raw));
                        return self.block.encodedJson(row, column, false);
                    }
                };
                var reader: Reader = .{ .block = block };
                const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_json = Reader.json, .width = 1, .count = rows.len } };
                const replay = try encodeColumnarBatchVersionAlloc(a, batch, &.{ 1, 2, 3 }, 1 << 20, version);
                try std.testing.expectEqualSlices(u8, bytes, replay);
                try std.testing.expect(frozen.value == null);
                const cell = try block.cell(0, 0);
                try std.testing.expectEqualStrings("9007199254740993", cell.value.object.get("alpha").?.array.items[0].number_string);
                try std.testing.expect(frozen.value != null);
                const bad = try a.dupe(u8, bytes);
                const second_key = std.mem.indexOf(u8, bad, "bravo").?;
                @memcpy(bad[second_key..][0..5], "alpha");
                for ([_][]const u8{ bad, bytes[0 .. bytes.len - 1] }) |invalid| {
                    const decoded = if (forwarding) decodeColumnarBlockForForwarding(metadata.allocator(), backing, invalid, 1 << 20) else decodeColumnarBlockWithCache(metadata.allocator(), backing, invalid, 1 << 20);
                    if (decoded) |unexpected| {
                        unexpected.deinit();
                        return error.ExpectedInvalidSpill;
                    } else |err| {
                        if (err == error.OutOfMemory) return err;
                        try std.testing.expectEqual(error.InvalidSqlSpill, err);
                    }
                }
            };
            // An integer DOM tag cannot silently become number_string on replay.
            var object: std.json.ObjectMap = .empty;
            try object.put(a, "x", .{ .integer = 7 });
            const typed = try encodeColumnarBlockVersionAlloc(a, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 4096, .v1);
            const block = try decodeColumnarBlockWithCache(a, backing, typed, 4096);
            defer block.deinit();
            try std.testing.expect(try block.encodedJson(0, 0, false) == null);
            try std.testing.expectEqual(@as(i64, 7), (try block.cell(0, 0)).value.object.get("x").?.integer);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL encoded JSON sequential spill sizes without materialization and keeps replay lazy" {
    const Probe = struct {
        reads: usize = 0,
        fn check(_: *anyopaque) !void {}
        fn encoded(_: *anyopaque, _: usize, _: usize) !?[]const u8 {
            return "{\"x\":[1,2,3]}";
        }
        fn cell(raw: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return error.UnexpectedScalarMaterialization;
        }
    };
    var probe: Probe = .{};
    const a = std.testing.allocator;
    const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &probe, .read = Probe.cell, .read_json = Probe.encoded, .count = 64, .width = 1 } };
    var ordinals: [64]u64 = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = i;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &probe, .checkpoint = Probe.check, .compression = .none, .async_writes = false };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 1 << 20);
    defer file.close();
    try file.appendBatch(batch, batch, &ordinals);
    try file.seal();
    var reader = try file.openReader();
    defer reader.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const restored = try reader.readBatchCurrent(arena.allocator(), 0, 64);
    try std.testing.expectEqualStrings("{\"x\":[1,2,3]}", (try restored.values.encodedJson(0, 0)).?);
    const frozen = (try reader.compact.?.encoded.?.values[0].jsonAt(0)).?;
    try std.testing.expect(frozen.value == null);
    const key_batch = try reader.compact.?.batch(arena.allocator(), true);
    try std.testing.expect(try key_batch.encodedJson(0, 0) == null);
    const key = try key_batch.cell(arena.allocator(), 0, 0);
    try std.testing.expectEqualStrings("1", key.value.object.get("x").?.array.items[0].number_string);
    try std.testing.expect((try reader.compact.?.encoded.?.keys[0].jsonAt(0)).?.value != null);
    try std.testing.expectEqual(@as(usize, 0), probe.reads);
    std.debug.print("ENCODED_SPILL scalar_materializations={d} rows=64\n", .{probe.reads});
}

test "native projection refinements benchmark lazy JSON blocks" {
    const Probe = struct {
        const Measurement = struct { ns: i96, peak: usize };
        fn now() i96 {
            if (comptime @import("builtin").link_libc and (@import("builtin").os.tag == .macos or @import("builtin").os.tag == .linux)) {
                var stamp: std.c.timespec = undefined;
                if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("thread CPU clock unavailable");
                return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
            }
            return @import("antfly_platform").time.monotonicNs();
        }
        fn measure(lazy: bool, bytes: []const u8) !Measurement {
            var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 128 << 20 };
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            const start = now();
            const block = if (lazy) try decodeColumnarBlockWithCache(arena.allocator(), budget.allocator(), bytes, 16 << 20) else try decodeColumnarBlockInArena(arena.allocator(), bytes, 16 << 20);
            defer block.deinit();
            const elapsed = now() - start;
            try std.testing.expectEqual(@as(usize, 8), block.count());
            std.mem.doNotOptimizeAway(block.values);
            if (lazy) try std.testing.expect((try block.values[0].jsonAt(0)).?.value == null);
            return .{ .ns = elapsed, .peak = budget.peak };
        }
    };
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    const a = source.allocator();
    var input: std.Io.Writer.Allocating = .init(a);
    try input.writer.writeByte('[');
    for (0..4096) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":9007199254740993,\"s\":\"nested value\"}");
    }
    try input.writer.writeByte(']');
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, input.written(), .{ .parse_numbers = false });
    var rows: [8]Row = undefined;
    const cells = [_]Datum{Datum.json(root)};
    for (&rows, 0..) |*row, i| row.* = .{ .values = &cells, .keys = &.{}, .ordinal = i };
    const bytes = try encodeColumnarBlockVersionAlloc(a, &rows, 16 << 20, .v2);
    _ = try Probe.measure(false, bytes);
    _ = try Probe.measure(true, bytes);
    for (0..5) |sample| {
        const first = try Probe.measure(sample % 2 != 0, bytes);
        const second = try Probe.measure(sample % 2 == 0, bytes);
        const before = if (sample % 2 == 0) first else second;
        const after = if (sample % 2 == 0) second else first;
        std.debug.print("LAZY_JSON_DECODE sample={d} before_cpu_ns={d} after_cpu_ns={d} before_peak={d} after_peak={d} wire={d} rows=8\n", .{ sample, before.ns, after.ns, before.peak, after.peak, bytes.len });
    }
}

test "native projection refinements benchmark numeric JSON consumption" {
    const Probe = struct {
        const Measurement = struct { ns: i96, peak: usize };
        fn now() i96 {
            if (comptime @import("builtin").link_libc and (@import("builtin").os.tag == .macos or @import("builtin").os.tag == .linux)) {
                var stamp: std.c.timespec = undefined;
                if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("thread CPU clock unavailable");
                return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
            }
            return @import("antfly_platform").time.monotonicNs();
        }
        fn measure(lazy: bool, bytes: []const u8) !Measurement {
            var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 128 << 20 };
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            const start = now();
            const block = if (lazy) try decodeColumnarBlockWithCache(arena.allocator(), budget.allocator(), bytes, 16 << 20) else try decodeColumnarBlockInArena(arena.allocator(), bytes, 16 << 20);
            defer block.deinit();
            for (0..block.count()) |row| {
                _ = try block.cell(row, 0);
            }
            const elapsed = now() - start;
            try std.testing.expectEqual(@as(usize, 8), block.count());
            std.mem.doNotOptimizeAway(block.values);

            return .{ .ns = elapsed, .peak = budget.peak };
        }
    };
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    const a = source.allocator();
    var input: std.Io.Writer.Allocating = .init(a);
    try input.writer.writeByte('[');
    for (0..4096) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":9007199254740993,\"s\":\"nested value\"}");
    }
    try input.writer.writeByte(']');
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, input.written(), .{ .parse_numbers = true });
    var rows: [8]Row = undefined;
    const cells = [_]Datum{Datum.json(root)};
    for (&rows, 0..) |*row, i| row.* = .{ .values = &cells, .keys = &.{}, .ordinal = i };
    const bytes = try encodeColumnarBlockVersionAlloc(a, &rows, 16 << 20, .v2);
    _ = try Probe.measure(false, bytes);
    _ = try Probe.measure(true, bytes);
    for (0..5) |sample| {
        const first = try Probe.measure(sample % 2 != 0, bytes);
        const second = try Probe.measure(sample % 2 == 0, bytes);
        const before = if (sample % 2 == 0) first else second;
        const after = if (sample % 2 == 0) second else first;
        std.debug.print("NUMERIC_JSON_CONSUME sample={d} before_cpu_ns={d} after_cpu_ns={d} before_peak={d} after_peak={d} wire={d} rows=8\n", .{ sample, before.ns, after.ns, before.peak, after.peak, bytes.len });
    }
}

test "SQL lazy JSON frozen transfer retains numeric tags without text or DOM replay" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
                var source = std.heap.ArenaAllocator.init(a);
                var source_live = true;
                defer if (source_live) source.deinit();
                const sa = source.allocator();
                const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"x\":[9007199254740993,1.25],\"s\":\"value\"}", .{});
                const datum = Datum.json(root);
                const rows = [_]Row{.{ .values = &.{datum}, .keys = &.{}, .ordinal = 0 }};
                const bytes = try encodeColumnarBlockVersionAlloc(sa, &rows, 1 << 20, version);
                const block = try decodeColumnarBlockWithCache(sa, a, bytes, 1 << 20);
                var block_live = true;
                defer if (block_live) block.deinit();
                const Probe = struct {
                    block: ColumnarBlock,
                    fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
                        return error.UnexpectedScalarMaterialization;
                    }
                    fn json(_: *anyopaque, _: usize, _: usize) !?[]const u8 {
                        return error.UnexpectedTextMaterialization;
                    }
                    fn frozen(raw: *anyopaque, row: usize, column: usize) !?@import("execution_batch.zig").FrozenJson {
                        return @as(*@This(), @ptrCast(@alignCast(raw))).block.frozenJson(row, column, false);
                    }
                };
                var probe: Probe = .{ .block = block };
                const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &probe, .read = Probe.cell, .read_json = Probe.json, .read_frozen = Probe.frozen, .count = 1, .width = 1 } };
                const mapped: @import("execution_batch.zig").Batch = .{ .mapped = .{ .source = &batch, .ordinals = &.{0}, .types = &.{.{ .kind = .json }}, .selection = &.{0} } };
                const selected = try mapped.select(sa, &.{0});
                const projected = try selected.project(sa, 0);
                var store = @import("typed_store.zig").Store.init(a);
                defer store.deinit();
                try store.appendBatch(projected);
                // Re-encode after destroying the producer to check that no
                // frozen bytes or cache allocator borrow escaped its lease.
                try std.testing.expect((try block.values[0].jsonAt(0)).?.value != null);
                try std.testing.expect((try block.values[0].jsonAt(0)).?.text == null);
                const expected_bytes = try a.dupe(u8, bytes);
                defer a.free(expected_bytes);
                const expected_text = try std.json.Stringify.valueAlloc(a, root, .{});
                defer a.free(expected_text);
                block.deinit();
                block_live = false;
                source.deinit();
                source_live = false;
                const retained: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &store, .count = 1 } };
                const replay = try encodeColumnarBatchVersionAlloc(a, retained, &.{0}, 1 << 20, version);
                defer a.free(replay);
                try std.testing.expectEqualSlices(u8, expected_bytes, replay);
                var consumer = std.heap.ArenaAllocator.init(a);
                defer consumer.deinit();
                const ca = consumer.allocator();
                const value = try store.cell(ca, 0, 0);
                try std.testing.expectEqual(@as(i64, 9007199254740993), value.value.object.get("x").?.array.items[0].integer);
                try std.testing.expectEqual(@as(f64, 1.25), value.value.object.get("x").?.array.items[1].float);
                const frozen = (try store.frozenJson(0, 0)).?;
                try std.testing.expect(!frozen.lossless_text);
                const text = try frozenJsonText(ca, frozen);
                try std.testing.expectEqualStrings(expected_text, text);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "native projection refinements benchmark frozen JSON transfer" {
    const Probe = struct {
        block: ColumnarBlock,
        forward: bool,
        const Measurement = struct { ns: i96, peak: usize };
        fn now() i96 {
            if (comptime @import("builtin").link_libc and (@import("builtin").os.tag == .macos or @import("builtin").os.tag == .linux)) {
                var stamp: std.c.timespec = undefined;
                if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("thread CPU clock unavailable");
                return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
            }
            return @import("antfly_platform").time.monotonicNs();
        }
        fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) !Datum {
            return @as(*@This(), @ptrCast(@alignCast(raw))).block.cell(row, column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) !?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.forward) self.block.frozenJson(row, column, false) else null;
        }
        fn measure(forward: bool, bytes: []const u8) !Measurement {
            var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 128 << 20 };
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            const start = now();
            // Compare the old eager transfer with the production lazy/frozen
            // path. Both validate the same frame and emit identical v2 bytes.
            const block = if (forward) try decodeColumnarBlockWithCache(arena.allocator(), budget.allocator(), bytes, 16 << 20) else try decodeColumnarBlockInArena(arena.allocator(), bytes, 16 << 20);
            defer block.deinit();
            var probe: @This() = .{ .block = block, .forward = forward };
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &probe, .read = cell, .read_frozen = frozen, .count = block.count(), .width = block.values.len } };
            const replay = try encodeColumnarBatchVersionAlloc(budget.allocator(), batch, block.ordinals, 16 << 20, .v2);
            defer budget.allocator().free(replay);
            const elapsed = now() - start;
            try std.testing.expectEqualSlices(u8, bytes, replay);
            if (forward) try std.testing.expectEqual(@as(usize, 0), block.json_owner.?.cached.count());
            return .{ .ns = elapsed, .peak = budget.peak };
        }
    };
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    const a = source.allocator();
    var input: std.Io.Writer.Allocating = .init(a);
    try input.writer.writeByte('[');
    for (0..4096) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":9007199254740993,\"s\":\"nested value\"}");
    }
    try input.writer.writeByte(']');
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, input.written(), .{ .parse_numbers = false });
    const cells = [_]Datum{Datum.json(root)};
    var rows: [8]Row = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .values = &cells, .keys = &.{}, .ordinal = i };
    const bytes = try encodeColumnarBlockVersionAlloc(a, &rows, 16 << 20, .v2);
    _ = try Probe.measure(false, bytes);
    _ = try Probe.measure(true, bytes);
    for (0..5) |sample| {
        const first = try Probe.measure(sample % 2 != 0, bytes);
        const second = try Probe.measure(sample % 2 == 0, bytes);
        const before = if (sample % 2 == 0) first else second;
        const after = if (sample % 2 == 0) second else first;
        try std.testing.expect(after.peak < before.peak);
        std.debug.print("FROZEN_JSON_TRANSFER sample={d} before_cpu_ns={d} after_cpu_ns={d} before_peak={d} after_peak={d} wire={d} rows=8\n", .{ sample, before.ns, after.ns, before.peak, after.peak, bytes.len });
    }
}

test "SQL lazy JSON forwarding retains numeric tags without constructing scalar trees" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            for ([_]ColumnarVersion{ .v1, .v2 }) |version| {
                var source = std.heap.ArenaAllocator.init(a);
                var source_live = true;
                defer if (source_live) source.deinit();
                const sa = source.allocator();
                const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"x\":[9007199254740993,1.25],\"s\":\"value\"}", .{});
                const datum = Datum.json(root);
                const rows = [_]Row{.{ .values = &.{datum}, .keys = &.{}, .ordinal = 0 }};
                const bytes = try encodeColumnarBlockVersionAlloc(sa, &rows, 1 << 20, version);
                const block = try decodeColumnarBlockForForwarding(sa, a, bytes, 1 << 20);
                var block_live = true;
                defer if (block_live) block.deinit();
                const Probe = struct {
                    block: ColumnarBlock,
                    fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) !Datum {
                        return error.UnexpectedScalarMaterialization;
                    }
                    fn json(_: *anyopaque, _: usize, _: usize) !?[]const u8 {
                        return error.UnexpectedTextMaterialization;
                    }
                    fn frozen(raw: *anyopaque, row: usize, column: usize) !?@import("execution_batch.zig").FrozenJson {
                        return @as(*@This(), @ptrCast(@alignCast(raw))).block.frozenJson(row, column, false);
                    }
                };
                var probe: Probe = .{ .block = block };
                const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &probe, .read = Probe.cell, .read_json = Probe.json, .read_frozen = Probe.frozen, .count = 1, .width = 1 } };
                const mapped: @import("execution_batch.zig").Batch = .{ .mapped = .{ .source = &batch, .ordinals = &.{0}, .types = &.{.{ .kind = .json }}, .selection = &.{0} } };
                const selected = try mapped.select(sa, &.{0});
                const projected = try selected.project(sa, 0);
                var store = @import("typed_store.zig").Store.init(a);
                defer store.deinit();
                try store.appendBatch(projected);
                // Re-encode after destroying the producer to check that no
                // frozen bytes or cache allocator borrow escaped its lease.
                try std.testing.expect((try block.values[0].jsonAt(0)).?.value == null);
                try std.testing.expect((try block.values[0].jsonAt(0)).?.text == null);
                const expected_bytes = try a.dupe(u8, bytes);
                defer a.free(expected_bytes);
                const expected_text = try std.json.Stringify.valueAlloc(a, root, .{});
                defer a.free(expected_text);
                block.deinit();
                block_live = false;
                source.deinit();
                source_live = false;
                const retained: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &store, .count = 1 } };
                const replay = try encodeColumnarBatchVersionAlloc(a, retained, &.{0}, 1 << 20, version);
                defer a.free(replay);
                try std.testing.expectEqualSlices(u8, expected_bytes, replay);
                var consumer = std.heap.ArenaAllocator.init(a);
                defer consumer.deinit();
                const ca = consumer.allocator();
                const value = try store.cell(ca, 0, 0);
                try std.testing.expectEqual(@as(i64, 9007199254740993), value.value.object.get("x").?.array.items[0].integer);
                try std.testing.expectEqual(@as(f64, 1.25), value.value.object.get("x").?.array.items[1].float);
                const frozen = (try store.frozenJson(0, 0)).?;
                try std.testing.expect(!frozen.lossless_text);
                const text = try frozenJsonText(ca, frozen);
                try std.testing.expectEqualStrings(expected_text, text);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "native projection refinements benchmark native numeric forwarding" {
    const Probe = struct {
        block: ColumnarBlock,
        forward: bool,
        const Measurement = struct { ns: i96, peak: usize, decode_peak: usize };
        fn now() i96 {
            if (comptime @import("builtin").link_libc and (@import("builtin").os.tag == .macos or @import("builtin").os.tag == .linux)) {
                var stamp: std.c.timespec = undefined;
                if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("thread CPU clock unavailable");
                return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
            }
            return @import("antfly_platform").time.monotonicNs();
        }
        fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) !Datum {
            return @as(*@This(), @ptrCast(@alignCast(raw))).block.cell(row, column);
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) !?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.forward) self.block.frozenJson(row, column, false) else null;
        }
        fn measure(forward: bool, bytes: []const u8) !Measurement {
            var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 128 << 20 };
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            const start = now();
            // Compare the old eager transfer with the production lazy/frozen
            // path. Both validate the same frame and emit identical v2 bytes.
            const block = if (forward) try decodeColumnarBlockForForwarding(arena.allocator(), budget.allocator(), bytes, 16 << 20) else try decodeColumnarBlockInArena(arena.allocator(), bytes, 16 << 20);
            defer block.deinit();
            const decode_peak = budget.peak;
            var probe: @This() = .{ .block = block, .forward = forward };
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &probe, .read = cell, .read_frozen = frozen, .count = block.count(), .width = block.values.len } };
            const replay = try encodeColumnarBatchVersionAlloc(budget.allocator(), batch, block.ordinals, 16 << 20, .v2);
            defer budget.allocator().free(replay);
            const elapsed = now() - start;
            try std.testing.expectEqualSlices(u8, bytes, replay);
            if (forward) try std.testing.expectEqual(@as(usize, 0), block.json_owner.?.cached.count());
            return .{ .ns = elapsed, .peak = budget.peak, .decode_peak = decode_peak };
        }
    };
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    const a = source.allocator();
    var input: std.Io.Writer.Allocating = .init(a);
    try input.writer.writeByte('[');
    for (0..4096) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":9007199254740993,\"s\":\"nested value\"}");
    }
    try input.writer.writeByte(']');
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, input.written(), .{ .parse_numbers = true });
    const cells = [_]Datum{Datum.json(root)};
    var rows: [8]Row = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .values = &cells, .keys = &.{}, .ordinal = i };
    const bytes = try encodeColumnarBlockVersionAlloc(a, &rows, 16 << 20, .v2);
    _ = try Probe.measure(false, bytes);
    _ = try Probe.measure(true, bytes);
    for (0..5) |sample| {
        const first = try Probe.measure(sample % 2 != 0, bytes);
        const second = try Probe.measure(sample % 2 == 0, bytes);
        const before = if (sample % 2 == 0) first else second;
        const after = if (sample % 2 == 0) second else first;
        try std.testing.expect(after.peak < before.peak);
        std.debug.print("NUMERIC_FROZEN_TRANSFER sample={d} before_cpu_ns={d} after_cpu_ns={d} before_peak={d} after_peak={d} before_decode_peak={d} after_decode_peak={d} wire={d} rows=8\n", .{ sample, before.ns, after.ns, before.peak, after.peak, before.decode_peak, after.decode_peak, bytes.len });
    }
}

test "SQL borrowed compact blocks retire lazy scalar cache allocations" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"x\":[9007199254740993,1.25],\"s\":\"owned value\"}", .{});
            const values = [_][]const Datum{ &.{Datum.json(root)}, &.{Datum.json(root)} };
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 8 << 20 };
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            {
                var file = try Sequential.init(&manager, 128);
                defer file.close();
                try file.appendBatch(.{ .rows = &values }, .{ .rows = &.{ &.{}, &.{} } }, &.{ 0, 1 });
                var input = try file.readInputBlockBorrowed(0);
                var live = true;
                defer if (live) input.deinit();
                try std.testing.expect(input == .borrowed);
                const before = budget.live;
                _ = try input.view().cell(0, 0);
                try std.testing.expect(budget.live > before);
                input.deinit();
                live = false;
                try std.testing.expectEqual(before, budget.live);
            }
            try std.testing.expectEqual(@as(usize, 0), budget.live);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL native chain payloads retain physical offsets validate corruption and cancellation" {
    const Hook = struct {
        fn check(raw: *anyopaque) !void {
            const canceled: *bool = @ptrCast(@alignCast(raw));
            if (canceled.*) return error.QueryCanceled;
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        const a = std.testing.allocator;
        var canceled = false;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &canceled, .checkpoint = Hook.check, .compression = if (compressed) .snappy else .none, .async_writes = false };
        defer manager.deinit();
        var file = try JoinPayloads.init(&manager);
        defer file.close();
        var text: [8192]u8 = @splat('x');
        const row = [_]Datum{Datum.json(.{ .string = &text })};
        const first = try file.appendBatch(.{ .rows = &.{&row} });
        try std.testing.expectEqual(first, try file.appendBatch(.{ .rows = &.{&row} }));
        text[0] = 'z';
        const second = try file.appendBatch(.{ .rows = &.{&row} });
        try std.testing.expect(first != second);
        @memset(&text, 'y');
        const later = try file.read(second);
        defer later.release();
        const earlier = try file.read(first);
        defer earlier.release();
        try std.testing.expectEqual(@as(u64, 0), later.ordinal(0));
        try std.testing.expectEqual(@as(u64, 0), earlier.ordinal(0));
        try std.testing.expectEqual(@as(u8, 'x'), (try earlier.cell(0, 0)).value.string[0]);
        try std.testing.expectEqual(@as(u8, 'z'), (try later.cell(0, 0)).value.string[0]);
        canceled = true;
        try std.testing.expectError(error.QueryCanceled, file.read(first));
        canceled = false;
        var header: [17]u8 = undefined;
        try file.file.readRaw(first, &header);
        try std.testing.expectEqual(@intFromBool(compressed), header[16]);
        try file.file.writeRaw(first + 16, &.{2});
        try std.testing.expectError(error.InvalidSqlSpill, file.read(first));
        try file.file.writeRaw(first + 16, header[16..17]);
        header[8] ^= 1;
        try file.file.writeRaw(first + 8, header[8..16]);
        try std.testing.expectError(error.InvalidSqlSpill, file.read(first));
    }
}

test "SQL native payload pages enforce statement array limits on write and read" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    var file = try JoinPayloads.init(&manager);
    defer file.close();
    const arrays = @import("array_value.zig");
    const elements: [32]Datum = @splat(.{});
    const array = try arrays.Value.init(.int32, &.{.{ .length = 32 }}, &elements, .{});
    const values = [_]Datum{Datum.typedArray(&array)};
    manager.array_limits.elements = 16;
    try std.testing.expectError(error.SqlProgramLimitExceeded, file.appendBatch(.{ .rows = &.{&values} }));
    try std.testing.expectEqual(@as(u64, 0), file.file.size);
    manager.array_limits.elements = 32;
    const offset = try file.appendBatch(.{ .rows = &.{&values} });
    const block = try file.read(offset);
    defer block.release();
    try std.testing.expectEqual(@as(usize, 32), (try block.cell(0, 0)).array.?.cardinality());
    manager.array_limits.elements = 16;
    try std.testing.expectError(error.InvalidSqlSpill, file.read(offset));
}

test "SQL native payload write cache yields to required allocations without exhausting statement quotas" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, compressed: bool, measure: bool) !void {
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 64 * 1024 };
            defer std.debug.assert(budget.live == 0);
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false };
            defer manager.deinit();
            var payloads = try JoinPayloads.init(&manager);
            defer payloads.close();
            var body: [12000]u8 = @splat('x');
            var offsets: [10]u64 = undefined;
            for (&offsets, 0..) |*offset, index| {
                body[0] = @intCast('a' + index);
                offset.* = try payloads.appendBatch(.{ .rows = &.{&.{Datum.json(.{ .string = &body })}} });
            }
            @memset(&body, 'z');
            for (offsets, 0..) |offset, index| {
                const block = try payloads.read(offset);
                defer block.release();
                const value = try block.cell(0, 0);
                try std.testing.expectEqual(@as(usize, 12000), value.value.string.len);
                try std.testing.expectEqual(@as(u8, @intCast('a' + index)), value.value.string[0]);
            }
            try std.testing.expect(!budget.isExhausted());
            if (measure) std.debug.print("SPILL_WRITE_QUOTA compressed={} rows=10 limit={d} peak={d} encoded_pages={d}\n", .{ compressed, budget.limit, budget.peak, payloads.encoded_pages });
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        try Fixture.run(std.testing.allocator, compressed, true);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ compressed, false });
    }
}

test "SQL native scalar payload reuse precedes serialization and preserves exact representations" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
    defer manager.deinit();
    var payloads = try JoinPayloads.init(&manager);
    defer payloads.close();
    var body: [8192]u8 = @splat('x');
    const values = [_]Datum{ Datum.json(.{ .string = &body }), Datum.json(.{ .float = 0.0 }), Datum.json(.null), .{} };
    const offset = try payloads.appendBatch(.{ .rows = &.{&values} });
    for (0..63) |_| try std.testing.expectEqual(offset, try payloads.appendBatch(.{ .rows = &.{&values} }));
    try std.testing.expectEqual(@as(usize, 1), payloads.encoded_pages);
    const record_limit = manager.max_record_bytes;
    manager.max_record_bytes = 100;
    try std.testing.expectError(error.SqlProgramLimitExceeded, payloads.appendBatch(.{ .rows = &.{&values} }));
    manager.max_record_bytes = record_limit;
    body[0] = 'y';
    const different = try payloads.appendBatch(.{ .rows = &.{&values} });
    try std.testing.expect(different != offset);
    body[0] = 'x';
    try std.testing.expectEqual(offset, try payloads.appendBatch(.{ .rows = &.{&values} }));
    var changed = values;
    changed[1] = Datum.json(.{ .float = -0.0 });
    try std.testing.expect(offset != try payloads.appendBatch(.{ .rows = &.{&changed} }));
    changed = values;
    changed[2] = .{};
    try std.testing.expect(offset != try payloads.appendBatch(.{ .rows = &.{&changed} }));
    // A matching fingerprint never substitutes for the owned exact key.
    changed = values;
    changed[0] = Datum.json(.{ .number_string = &body });
    payloads.recent[0].?.scalar_hash = JoinPayloads.scalarHash(&changed).?;
    try std.testing.expect(offset != try payloads.appendBatch(.{ .rows = &.{&changed} }));
    const block = try payloads.read(offset);
    defer block.release();
    try std.testing.expectEqual(@as(u8, 'x'), (try block.cell(0, 0)).value.string[0]);
    std.debug.print("SPILL_SCALAR_REUSE repeated_rows=64 encoded_pages=1\n", .{});
}

test "SQL native async spill fallback preserves parent admission and reports genuine allocator failures" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, measure: bool) !void {
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 56000 };
            defer std.debug.assert(budget.live == 0);
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .snappy, .async_writes = true };
            defer manager.deinit();
            var payloads = try JoinPayloads.init(&manager);
            defer payloads.close();
            var body: [12000]u8 = @splat('x');
            for (0..20) |index| {
                body[0] = @intCast('a' + index);
                _ = try payloads.appendBatch(.{ .rows = &.{&.{Datum.json(.{ .string = &body })}} });
            }
            try payloads.file.flush();
            try std.testing.expect(!budget.isExhausted());
            if (measure) std.debug.print("SPILL_ASYNC_ADMISSION rows=20 limit={d} peak={d} exhausted={}\n", .{ budget.limit, budget.peak, budget.exhausted });
        }
    };
    try Fixture.run(std.testing.allocator, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{false});
}

test "SQL native optional async buffers decline opaque enclosing quotas" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]bool{ false, true }) |nested| {
        var storage: [65536]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&storage);
        var budget: @import("memory_budget.zig") = .{ .backing = fixed.allocator(), .limit = storage.len };
        const a = if (nested) budget.allocator() else fixed.allocator();
        var dummy: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
        defer manager.deinit();
        var file = try manager.create();
        defer file.close();
        const data: [2048]u8 = @splat(42);
        try file.writeRaw(0, &data);
        const ballast = try fixed.allocator().alloc(u8, storage.len - fixed.end_index - 1024);
        defer fixed.allocator().free(ballast);
        try file.flush();
        try std.testing.expect(file.write_job == null);
        try std.testing.expect(!budget.isExhausted());
        var actual: [2048]u8 = undefined;
        // Direct physical verification needs no optional read buffer.
        try std.testing.expectEqual(actual.len, try file.readPhysical(&actual, 0));
        try std.testing.expectEqualSlices(u8, &data, &actual);
    }
}

test "SQL native batch sort preserves frozen payloads through singleton runs and merges" {
    const Reader = struct {
        frozen: @import("execution_batch.zig").FrozenJson,
        fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) anyerror!Datum {
            return error.UnexpectedPayloadMaterialization;
        }
        fn payload(raw: *anyopaque, _: usize, _: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.frozen;
        }
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, count: usize) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const body: [1024]u8 = @splat('x');
            var object: std.json.ObjectMap = .empty;
            try object.put(sa, "payload", .{ .string = &body });
            const wire = try encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
            const block = try decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
            defer block.deinit();
            var reader: @This() = .{ .frozen = (try block.frozenJson(0, 0, false)).? };
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var sort = Sort.init(a, &manager, &.{.{}}, 4096);
            defer sort.deinit();
            sort.parallel_runs = false;
            sort.run_limit = 3;
            const keys = try sa.alloc([]const Datum, count);
            const ordinals = try sa.alloc(u64, count);
            for (keys, ordinals, 0..) |*key, *ordinal, index| {
                const values = try sa.alloc(Datum, 1);
                values[0] = Datum.json(.{ .integer = @intCast(index % 3) });
                key.* = values;
                ordinal.* = index;
            }
            try sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = cell, .read_frozen = payload, .count = count, .width = 1 } }, keys, ordinals);
            var seen: usize = 0;
            var previous_key: i64 = -1;
            var previous_ordinal: u64 = 0;
            while (try sort.nextLeased()) |lease| {
                defer lease.release();
                const owned = lease.block.?;
                try std.testing.expect((try owned.encoded.?.frozenJson(lease.index, 0, false)) != null);
                const key = (try lease.keyCell(0)).value.integer;
                try std.testing.expect(key >= previous_key);
                if (key == previous_key) try std.testing.expect(lease.row.ordinal > previous_ordinal);
                previous_key = key;
                previous_ordinal = lease.row.ordinal;
                seen += 1;
            }
            try std.testing.expectEqual(count, seen);
            if (count > 3) try std.testing.expect(manager.merges > 0);
        }
    };
    try Reader.run(std.testing.allocator, 24);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Reader.run, .{@as(usize, 4)});
}

test "SQL native batch sort admission reuses string dictionaries in linear reads" {
    const Reader = struct {
        body: []const u8,
        reads: usize = 0,
        fn read(raw: *anyopaque, _: Allocator, _: usize, _: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return Datum.json(.{ .string = self.body });
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]usize{ 4096, 262144 }) |memory| for ([_]bool{ false, true }) |scalar_input| {
        const count: usize = 64;
        const body: [12288]u8 = @splat('x');
        var reader: Reader = .{ .body = &body };
        var dummy: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        var sort = Sort.init(a, &manager, &.{.{}}, memory);
        defer sort.deinit();
        sort.parallel_runs = false;
        var keys: [64][]const Datum = undefined;
        var key_values: [64]Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (0..count) |index| {
            key_values[index] = Datum.json(.{ .integer = @intCast(count - index) });
            keys[index] = key_values[index..][0..1];
            ordinals[index] = index;
        }
        if (scalar_input) {
            for (0..count) |index| {
                const datum = try Reader.read(&reader, a, index, 0);
                try sort.add(.{ .values = &.{datum}, .keys = keys[index], .ordinal = ordinals[index] });
            }
        } else {
            try sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.read, .count = count, .width = 1 } }, keys[0..count], ordinals[0..count]);
        }
        const source_reads = reader.reads;
        try std.testing.expect(source_reads <= count * 8);
        if (memory == 262144) {
            try std.testing.expect(sort.order_sort == null);
            try std.testing.expectEqual(@as(u64, 0), manager.written_bytes);
        }
        var seen: usize = 0;
        while (try sort.nextLeased()) |lease| {
            defer lease.release();
            try std.testing.expectEqual(@as(i64, @intCast(seen + 1)), (try lease.keyCell(0)).value.integer);
            try std.testing.expectEqualStrings(&body, (try lease.cell(0)).value.string);
            seen += 1;
        }
        try std.testing.expectEqual(count, seen);
        std.debug.print("NATIVE_SORT_ADMISSION rows={d} memory={d} scalar={} source_reads={d} written={d} merges={d}\n", .{ count, memory, scalar_input, source_reads, manager.written_bytes, manager.merges });
    };
}

test "SQL native indirect sort writes payloads once skips hydration and preserves replay under allocation faults" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, count: usize, indirect: bool, compression: @FieldType(Manager, "compression"), memory: usize) !u64 {
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = compression, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var sort = Sort.init(a, &manager, &.{.{}}, memory);
            defer sort.deinit();
            sort.parallel_runs = false;
            sort.allow_indirect = indirect;
            var body: [4096]u8 = @splat('x');
            for (0..count) |i| {
                std.mem.writeInt(u64, body[0..8], i, .little);
                const key = Datum.json(.{ .integer = @intCast(count - i) });
                try sort.add(.{ .values = &.{ Datum.json(.{ .string = &body }), Datum.json(.null), .{}, Datum.json(.{ .number_string = "1.0000000000000001" }) }, .keys = &.{key}, .ordinal = i });
            }
            try sort.finish();
            if (indirect) try std.testing.expect(sort.order_sort != null);
            // Scalar replay and leased delivery share the same payload references.
            for (0..2) |_| {
                for (0..count) |i| {
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const row = (try sort.next(arena.allocator())).?;
                    try std.testing.expectEqual(@as(i64, @intCast(i + 1)), row.keys[0].value.integer);
                    try std.testing.expectEqual(@as(u64, count - i - 1), std.mem.readInt(u64, row.values[0].value.string[0..8], .little));
                    try std.testing.expect(!row.values[1].sql_null and row.values[2].sql_null);
                    try std.testing.expectEqualStrings("1.0000000000000001", row.values[3].value.number_string);
                }
                try std.testing.expect((try sort.next(a)) == null);
                try sort.rewind();
            }
            const prior_reads = sort.payload_reads;
            for (0..count / 2) |_| try std.testing.expect(try sort.skip());
            try std.testing.expectEqual(prior_reads, sort.payload_reads);
            var i = count / 2;
            while (try sort.nextLeased()) |lease| : (i += 1) {
                defer lease.release();
                try std.testing.expectEqual(@as(i64, @intCast(i + 1)), (try lease.keyCell(0)).value.integer);
                try std.testing.expectEqual(@as(u64, count - i - 1), std.mem.readInt(u64, (try lease.cell(0)).value.string[0..8], .little));
                try std.testing.expectEqual(@as(u64, count - i - 1), lease.row.ordinal);
            }
            try std.testing.expectEqual(count, i);
            const written = manager.written_bytes;
            sort.deinit();
            sort = Sort.init(a, &manager, &.{.{}}, memory);
            try std.testing.expectEqual(@as(usize, 0), manager.files);
            try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
            return written;
        }
        fn fault(a: Allocator, memory: usize) !void {
            _ = try run(a, 5, true, .none, memory);
        }
    };
    for ([_]@FieldType(Manager, "compression"){ .none, .snappy }) |compression| {
        const indirect = try Harness.run(std.testing.allocator, 64, true, compression, 4096);
        const inline_bytes = try Harness.run(std.testing.allocator, 64, false, compression, 4096);
        try std.testing.expect(indirect < inline_bytes);
        if (compression == .none) try std.testing.expect(indirect < inline_bytes / 2);
        std.debug.print("INDIRECT_SORT compression={s} rows=64 written={d} inline={d}\n", .{ @tagName(compression), indirect, inline_bytes });
    }
    // Cover both immediate indirection and migration of a populated run.
    for ([_]usize{ 4096, 32768 }) |memory| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.fault, .{memory});
}

test "SQL native indirect sort shares frozen payload offsets within a producer lease" {
    const Reader = struct {
        frozen: @import("execution_batch.zig").FrozenJson,
        fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) anyerror!Datum {
            return error.UnexpectedMaterialization;
        }
        fn get(raw: *anyopaque, _: usize, _: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.frozen;
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    const sa = source.allocator();
    const body = try sa.alloc(u8, 65536);
    @memset(body, 'x');
    var object: std.json.ObjectMap = .empty;
    try object.put(sa, "body", .{ .string = body });
    const wire = try encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
    var block = try decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
    defer block.deinit();
    var reader: Reader = .{ .frozen = (try block.frozenJson(0, 0, false)).? };
    for ([_]u64{ 512 * 1024, 32 * 1024 * 1024 }) |quota| {
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128, .max_bytes = quota };
        defer manager.deinit();
        var sort = Sort.init(a, &manager, &.{.{}}, 8192);
        defer sort.deinit();
        sort.parallel_runs = false;
        var key_values: [64]Datum = undefined;
        var keys: [64][]const Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (0..64) |i| {
            key_values[i] = Datum.json(.{ .integer = @intCast(64 - i) });
            keys[i] = key_values[i..][0..1];
            ordinals[i] = i;
        }
        const failure: ?anyerror = null;
        sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.get, .count = 64, .width = 1 } }, &keys, &ordinals) catch |err| {
            return err;
        };
        var count: usize = 0;
        if (failure == null) {
            while (try sort.nextLeased()) |lease| {
                lease.release();
                count += 1;
            }
        }
        try std.testing.expect(failure == null);
        try std.testing.expectEqual(@as(usize, 1), sort.payloads.?.encoded_pages);
        try std.testing.expectEqual(@as(usize, 1), sort.payload_reads);
        try std.testing.expectEqual(@as(usize, 64), count);
        try std.testing.expect(manager.written_bytes < 128 * 1024);
        std.debug.print("NATIVE_SORT_SHARED frozen={d} quota={d} error={?} pages={d} written={d} reads={d} rows={d}\n", .{ reader.frozen.bytes.len, quota, failure, if (sort.payloads) |payloads| payloads.encoded_pages else @as(usize, 0), manager.written_bytes, sort.payload_reads, count });
    }
}

test "SQL native payload read cache evicts cold pages before a recurring hot pair" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    {
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Hook.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        var sort = Sort.init(a, &manager, &.{.{}}, 4096);
        defer sort.deinit();
        sort.parallel_runs = false;
        var body: [2048]u8 = @splat('x');
        for (0..60) |i| {
            const page = i / 10;
            const lane = i % 10;
            std.mem.writeInt(u64, body[0..8], page, .little);
            const rank = if (page < 4) i else if (page == 4) 40 + lane * 2 else 41 + lane * 2;
            try sort.add(.{ .values = &.{Datum.json(.{ .string = &body })}, .keys = &.{Datum.json(.{ .integer = @intCast(rank) })}, .ordinal = i });
        }
        const payload_bytes = sort.payloads.?.file.size;
        var seen: usize = 0;
        while (try sort.nextLeased()) |lease| {
            defer lease.release();
            try std.testing.expectEqual(@as(i64, @intCast(seen)), (try lease.keyCell(0)).value.integer);
            seen += 1;
        }
        try std.testing.expectEqual(@as(usize, 6), sort.payload_reads);
        std.debug.print("NATIVE_SORT_LRU rows={d} payload_bytes={d} reads={d}\n", .{ seen, payload_bytes, sort.payload_reads });
    }
}
test "SQL native grouped sort output hydrates interleaved pages once per bounded window" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var token: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Hook.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
    defer manager.deinit();
    var sort = Sort.init(a, &manager, &.{.{}}, 8192);
    defer sort.deinit();
    sort.parallel_runs = false;
    try sort.startIndirect();
    // Interleave two wide pages after a sequential prefix. Their combined
    // decoded footprint exceeds the read cache but fits one output window.
    const count: usize = 160;
    var bodies: [count][3000]u8 = @splat(@splat('x'));
    var cells: [count]Datum = undefined;
    var rows: [count][]const Datum = undefined;
    var key_values: [count]Datum = undefined;
    var keys: [count][]const Datum = undefined;
    var ordinals: [count]u64 = undefined;
    for (0..count) |i| {
        std.mem.writeInt(u64, bodies[i][0..8], i, .little);
        cells[i] = Datum.json(.{ .string = &bodies[i] });
        rows[i] = cells[i..][0..1];
        // Page grouping is ten rows per 32 KiB target.
        const page = i / 10;
        const lane = i % 10;
        const rank = if (page < 3) i else if (page == 3) 30 + lane * 2 else if (page == 4) 31 + lane * 2 else 50 + (page - 5) * 10 + lane;
        key_values[i] = Datum.json(.{ .integer = @intCast(rank) });
        keys[i] = key_values[i..][0..1];
        ordinals[i] = i;
    }
    // Use actual production addBatch once indirect mode is active.
    try sort.addBatch(.{ .rows = &rows }, &keys, &ordinals);
    var seen: usize = 0;
    while (true) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const page_delivery = try sort.nextPageLeased(64, 128 * 1024);
        defer page_delivery.release();
        const page = page_delivery.rows;
        if (page.len == 0) break;
        for (page) |lease| {
            try std.testing.expectEqual(@as(i64, @intCast(seen)), (try lease.keyCell(0)).value.integer);
            const original = std.mem.readInt(u64, (try lease.cell(0)).value.string[0..8], .little);
            try std.testing.expectEqual(original, lease.row.ordinal);
            seen += 1;
        }
    }
    try std.testing.expect(sort.payload_reads <= 20);
    try std.testing.expectEqual(count, seen);
    std.debug.print("NATIVE_SORT_GATHER rows={d} pages={d} reads={d} written={d}\n", .{ seen, sort.payloads.?.encoded_pages, sort.payload_reads, manager.written_bytes });
}

test "SQL native grouped sort preserves pending references scalar skips and leases on every failure" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, measure: bool) !void {
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var sort = Sort.init(a, &manager, &.{.{}}, 4096);
            defer sort.deinit();
            sort.parallel_runs = false;
            var body: [2048]u8 = @splat('x');
            for (0..12) |i| {
                std.mem.writeInt(u64, body[0..8], i, .little);
                try sort.add(.{ .values = &.{Datum.json(.{ .string = &body })}, .keys = &.{Datum.json(.{ .integer = @intCast(12 - i) })}, .ordinal = i });
            }
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const first_delivery = try sort.nextPageLeased(64, 10 * 1024);
            defer first_delivery.release();
            const first = first_delivery.rows;
            try std.testing.expect(first.len > 0 and first.len < 12);
            for (first, 0..) |lease, i| try std.testing.expectEqual(@as(i64, @intCast(i + 1)), (try lease.keyCell(0)).value.integer);
            try std.testing.expect(sort.deferred_index < sort.deferred_count);
            const reads = sort.payload_reads;
            try std.testing.expect(try sort.skip());
            try std.testing.expectEqual(reads, sort.payload_reads);
            const scalar_row = (try sort.next(scratch.allocator())).?;
            try std.testing.expectEqual(@as(i64, @intCast(first.len + 2)), scalar_row.keys[0].value.integer);
            var seen = first.len + 2;
            while (true) {
                const page_delivery = try sort.nextPageLeased(3, 16 * 1024);
                defer page_delivery.release();
                const page = page_delivery.rows;
                if (page.len == 0) break;
                for (page) |lease| {
                    try std.testing.expectEqual(@as(i64, @intCast(seen + 1)), (try lease.keyCell(0)).value.integer);
                    seen += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, 12), seen);
            // Payload and key blocks independently own storage after the sort
            // retires. The manager remains alive until these leases release.
            sort.deinit();
            sort = Sort.init(a, &manager, &.{.{}}, 4096);
            try std.testing.expectEqual(@as(u64, 11), std.mem.readInt(u64, (try first[0].cell(0)).value.string[0..8], .little));
            try std.testing.expectEqual(@as(usize, 0), manager.files);
            if (measure) std.debug.print("NATIVE_GATHER_PENDING first_rows={d} total={d} preserved=true\n", .{ first.len, seen });
        }
    };
    try Harness.run(std.testing.allocator, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{false});
}

test "SQL native indirect row sharing preserves JSON null SQL null and float representations" {
    const Reader = struct {
        text: []const u8,
        fn json(raw: *anyopaque, _: usize, column: usize) anyerror!?[]const u8 {
            return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).text else null;
        }
        fn cell(_: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            if (column == 0) return error.UnexpectedMaterialization;
            return switch (row % 4) {
                0 => .{},
                1 => Datum.json(.null),
                2 => Datum.json(.{ .float = -0.0 }),
                else => Datum.json(.{ .float = 0.0 }),
            };
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const text = try arena.allocator().alloc(u8, 12002);
    @memset(text, 'x');
    text[0] = '"';
    text[text.len - 1] = '"';
    var reader: Reader = .{ .text = text };
    var token: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = .none, .async_writes = false };
    defer manager.deinit();
    var sort = Sort.init(a, &manager, &.{.{}}, 4096);
    defer sort.deinit();
    sort.parallel_runs = false;
    const key = Datum.json(.{ .integer = 0 });
    const keys: [64][]const Datum = @splat(&.{key});
    var ordinals: [64]u64 = undefined;
    for (&ordinals, 0..) |*ordinal, i| ordinal.* = i;
    try sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_json = Reader.json, .count = 64, .width = 2 } }, &keys, &ordinals);
    var index: usize = 0;
    while (try sort.nextLeased()) |lease| : (index += 1) {
        defer lease.release();
        const value = try lease.cell(1);
        switch (index % 4) {
            0 => try std.testing.expect(value.sql_null),
            1 => try std.testing.expect(!value.sql_null and value.value == .null),
            2 => try std.testing.expectEqual(@as(u64, 0x8000000000000000), @as(u64, @bitCast(value.value.float))),
            else => try std.testing.expectEqual(@as(u64, 0), @as(u64, @bitCast(value.value.float))),
        }
        try std.testing.expectEqual(@as(u64, @intCast(index)), lease.row.ordinal);
    }
    try std.testing.expectEqual(@as(usize, 64), index);
    try std.testing.expect(sort.payloads.?.encoded_pages <= 4);
}

test "SQL native grouped output reserves decoded metadata for wide sparse rows" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator) !void {
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 192 * 1024 };
            defer std.debug.assert(budget.live == 0);
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var sort = Sort.init(budget.allocator(), &manager, &.{.{}}, 4096);
            defer sort.deinit();
            sort.parallel_runs = false;
            var body: [12000]u8 = @splat('x');
            var values: [512]Datum = @splat(.{});
            for (0..4) |i| {
                std.mem.writeInt(u64, body[0..8], i, .little);
                values[0] = Datum.json(.{ .string = &body });
                try sort.add(.{ .values = &values, .keys = &.{Datum.json(.{ .integer = @intCast(4 - i) })}, .ordinal = i });
            }
            var seen: usize = 0;
            while (true) {
                var scratch = std.heap.ArenaAllocator.init(budget.allocator());
                defer scratch.deinit();
                const page_delivery = try sort.nextPageLeased(64, 512 * 1024);
                defer page_delivery.release();
                const page = page_delivery.rows;
                if (page.len == 0) break;
                try std.testing.expectEqual(@as(usize, 1), page.len);
                try std.testing.expectEqual(@as(i64, @intCast(seen + 1)), (try page[0].keyCell(0)).value.integer);
                try std.testing.expect((try page[0].cell(511)).sql_null);
                try std.testing.expectEqual(@as(u64, 3 - seen), std.mem.readInt(u64, (try page[0].cell(0)).value.string[0..8], .little));
                seen += 1;
            }
            try std.testing.expectEqual(@as(usize, 4), seen);
            try std.testing.expect(!budget.isExhausted());
        }
    };
    try Harness.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL native spill shares large bodies across distinct IDs" {
    const Reader = struct {
        frozen: @import("execution_batch.zig").FrozenJson,
        fn cell(_: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            if (column == 1) return Datum.json(.{ .integer = @intCast(row) });
            return error.UnexpectedMaterialization;
        }
        fn get(raw: *anyopaque, _: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (column == 0) self.frozen else null;
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    const sa = source.allocator();
    const body = try sa.alloc(u8, 65536);
    @memset(body, 'x');
    var object: std.json.ObjectMap = .empty;
    try object.put(sa, "body", .{ .string = body });
    const wire = try encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
    var block = try decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
    defer block.deinit();
    var reader: Reader = .{ .frozen = (try block.frozenJson(0, 0, false)).? };
    for ([_]u64{ 512 * 1024, 32 * 1024 * 1024 }) |quota| {
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128, .max_bytes = quota };
        defer manager.deinit();
        var sort = Sort.init(a, &manager, &.{.{}}, 8192);
        defer sort.deinit();
        sort.parallel_runs = false;
        var key_values: [64]Datum = undefined;
        var keys: [64][]const Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (0..64) |i| {
            key_values[i] = Datum.json(.{ .integer = @intCast(64 - i) });
            keys[i] = key_values[i..][0..1];
            ordinals[i] = i;
        }
        const failure: ?anyerror = null;
        sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.get, .count = 64, .width = 2 } }, &keys, &ordinals) catch |err| {
            return err;
        };
        var count: usize = 0;
        if (failure == null) {
            while (try sort.nextLeased()) |lease| {
                lease.release();
                count += 1;
            }
        }
        try std.testing.expect(failure == null);

        try std.testing.expectEqual(@as(usize, 64), count);

        try std.testing.expect(manager.written_bytes < 128 * 1024);
        std.debug.print("SHARED_BODY_DISTINCT frozen={d} quota={d} error={?} pages={d} written={d} reads={d} rows={d}\n", .{ reader.frozen.bytes.len, quota, failure, if (sort.payloads) |payloads| payloads.encoded_pages else @as(usize, 0), manager.written_bytes, sort.payload_reads, count });
    }
}

test "SQL native grouped frozen cells fit quotas that allow scalar reads" {
    const Reader = struct {
        fn check(_: *anyopaque) !void {}
        fn cell(_: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            if (column == 16) return Datum.json(.{ .integer = @intCast(row) });
            return error.UnexpectedMaterialization;
        }
        fn frozen(_: *anyopaque, _: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            return if (column == 16) null else .{ .bytes = &.{ 6, 0, 0, 0, 0, 0, 0, 0, 0 }, .lossless_text = true };
        }
    };
    const a = std.testing.allocator;
    for ([_]usize{ 512 * 1024, 768 * 1024 }) |quota| for ([_]bool{ false, true }) |grouped| {
        var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = quota };
        defer std.debug.assert(budget.live == 0);
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        var sort = Sort.init(budget.allocator(), &manager, &.{.{}}, 4096);
        defer sort.deinit();
        sort.parallel_runs = false;
        const prefix_body: [4096]u8 = @splat('p');
        var prefix_values: [17]Datum = @splat(.{});
        prefix_values[0] = Datum.json(.{ .string = &prefix_body });
        try sort.add(.{ .values = &prefix_values, .keys = &.{Datum.json(.{ .integer = 999 })}, .ordinal = 128 });
        var key_values: [128]Datum = undefined;
        var keys: [128][]const Datum = undefined;
        var ordinals: [128]u64 = undefined;
        for (0..128) |i| {
            key_values[i] = Datum.json(.{ .integer = @intCast(i % 64 * 2 + i / 64) });
            keys[i] = key_values[i..][0..1];
            ordinals[i] = i;
        }
        var reader: u8 = 0;
        try sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.frozen, .count = 128, .width = 17 } }, &keys, &ordinals);
        var seen: usize = 0;
        var failure: ?anyerror = null;
        while (true) {
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            if (grouped) {
                const page = sort.nextPageLeased(64, 128 * 1024) catch |err| {
                    failure = err;
                    break;
                };
                defer page.release();
                if (page.rows.len == 0) break;
                seen += page.rows.len;
            } else {
                const lease = (sort.nextLeased() catch |err| {
                    failure = err;
                    break;
                }) orelse break;
                lease.release();
                seen += 1;
            }
        }
        try std.testing.expect(failure == null);
        try std.testing.expectEqual(@as(usize, 129), seen);
        try std.testing.expect(!budget.isExhausted());
        std.debug.print("COMPACT_FROZEN quota={d} grouped={} seen={d} peak={d} reads={d} pages={d}\n", .{ quota, grouped, seen, budget.peak, sort.payload_reads, sort.payloads.?.encoded_pages });
    };
}

test "SQL native body references shrink real allocation windows and outlive all spill owners" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, large: bool, compressed: bool) !void {
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 128 * 1024 };
            defer std.debug.assert(budget.live == 0);
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            var payloads = try JoinPayloads.init(&manager);
            defer payloads.close();
            const count: usize = if (large) 6 else 3;
            var references: [6][2]Datum = undefined;
            var rows: [6][]const Datum = undefined;
            for (0..count) |index| {
                const body = try source.allocator().alloc(u8, if (large) 20000 else 512);
                @memset(body, @intCast('a' + index));
                var object: std.json.ObjectMap = .empty;
                try object.put(source.allocator(), "body", .{ .string = body });
                const bytes = try encodeColumnarBlockVersionAlloc(source.allocator(), &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                const block = try decodeColumnarBlockForForwarding(source.allocator(), a, bytes, 1 << 20);
                defer block.deinit();
                const Reader = struct {
                    frozen: @import("execution_batch.zig").FrozenJson,
                    id: usize,
                    fn getFrozen(raw: *anyopaque, _: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                        return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).frozen else null;
                    }
                    fn cell(raw: *anyopaque, _: Allocator, _: usize, column: usize) anyerror!Datum {
                        if (column == 0) return error.UnexpectedMaterialization;
                        return Datum.json(.{ .integer = @intCast(@as(*@This(), @ptrCast(@alignCast(raw))).id) });
                    }
                };
                var reader: Reader = .{ .frozen = (try block.frozenJson(0, 0, false)).?, .id = index };
                const offset = try payloads.appendBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.getFrozen, .count = 1, .width = 2 } });
                references[index] = .{ Datum.json(.{ .integer = @intCast(offset) }), Datum.json(.{ .integer = 0 }) };
                rows[index] = &references[index];
            }
            source.deinit();
            source = .init(a);
            try payloads.file.seal();
            var reader = PayloadReader.init(&payloads);
            defer reader.deinit();
            var held: ?PayloadReader.Payload = null;
            defer if (held) |payload| payload.release();
            var seen: usize = 0;
            while (seen < count) {
                var scratch = std.heap.ArenaAllocator.init(budget.allocator());
                defer scratch.deinit();
                const page = try reader.nativePage(scratch.allocator(), .{ .rows = rows[seen..count] }, 2, 128 * 1024);
                defer page.release();
                try std.testing.expect(page.values.len() > 0);
                for (0..page.values.len()) |lane| {
                    try std.testing.expectEqual(@as(i64, @intCast(seen + lane)), (try page.values.cell(scratch.allocator(), lane, 1)).value.integer);
                    try std.testing.expect((try page.values.frozenJson(lane, 0)) != null);
                }
                if (held == null) {
                    held = page.payloads[0];
                    held.?.block.retain();
                }
                seen += page.values.len();
            }
            if (large) try std.testing.expect(reader.window_reductions > 0);
            try std.testing.expect(!budget.isExhausted());
            const reductions = reader.window_reductions;
            reader.deinit();
            reader = PayloadReader.init(&payloads);
            payloads.close();
            try std.testing.expectEqual(@as(usize, 0), manager.files);
            try std.testing.expectEqual(@as(i64, 0), (try held.?.block.cell(held.?.index, 1)).value.integer);
            const frozen = (try held.?.block.encoded.?.frozenJson(held.?.index, 0, false)).?;
            const decoded = try materializeFrozenJson(source.allocator(), frozen);
            try std.testing.expectEqual(@as(u8, 'a'), decoded.value.object.get("body").?.string[0]);
            if (large) std.debug.print("PAYLOAD_ADMISSION compressed={} rows={d} reductions={d} peak={d}\n", .{ compressed, seen, reductions, budget.peak });
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        try Harness.run(std.testing.allocator, true, compressed);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{ false, compressed });
    }
}

test "SQL native shared bodies cross producer leases preserve nulls and reject stale pointer identities" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, large: bool, compressed: bool) !void {
            const Batch = @import("execution_batch.zig").Batch;
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const body = try source.allocator().alloc(u8, if (large) 65536 else 512);
            @memset(body, 'x');
            var object: std.json.ObjectMap = .empty;
            try object.put(source.allocator(), "body", .{ .string = body });
            const bytes = try encodeColumnarBlockVersionAlloc(source.allocator(), &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
            const block = try decodeColumnarBlockForForwarding(source.allocator(), a, bytes, 1 << 20);
            var live = true;
            defer if (live) block.deinit();
            const View = struct {
                value: @import("execution_batch.zig").FrozenJson,
                base: usize = 0,
                fn frozen(raw: *anyopaque, _: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                    return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).value else null;
                }
                fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
                    const self_: *@This() = @ptrCast(@alignCast(raw));
                    return switch (column) {
                        0 => error.UnexpectedMaterialization,
                        1 => Datum.json(.{ .integer = @intCast(self_.base + row) }),
                        2 => .{},
                        3 => Datum.json(.null),
                        else => Datum.json(.{ .float = if (row % 2 == 0) -0.0 else 0.0 }),
                    };
                }
            };
            var view: View = .{ .value = (try block.frozenJson(0, 0, false)).? };
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128, .max_bytes = 512 * 1024 };
            defer manager.deinit();
            var file = try JoinPayloads.init(&manager);
            defer file.close();
            const count: usize = if (large) 64 else 3;
            const batch: Batch = .{ .reader = .{ .ptr = &view, .read = View.cell, .read_frozen = View.frozen, .count = count, .width = 5 } };
            var offsets: [3]u64 = undefined;
            for (&offsets, 0..) |*offset, cohort| {
                view.base = cohort * count;
                if (cohort == 2) @constCast(view.value.bytes)[view.value.bytes.len - 1] = 'z';
                offset.* = try file.appendBatch(batch);
            }
            block.deinit();
            live = false;
            source.deinit();
            source = .init(a);
            try file.file.seal();
            var reader = PayloadReader.init(&file);
            defer reader.deinit();
            const first = try reader.read(.{ .offset = offsets[0], .index = 0 });
            defer first.release();
            const second = try reader.read(.{ .offset = offsets[1], .index = count - 1 });
            defer second.release();
            try std.testing.expect(first.block.dependencies.items[0] == second.block.dependencies.items[0]);
            const third = try reader.read(.{ .offset = offsets[2], .index = 0 });
            defer third.release();
            try std.testing.expect(first.block.dependencies.items[0] != third.block.dependencies.items[0]);
            try std.testing.expectEqual(@as(i64, 0), (try first.block.cell(0, 1)).value.integer);
            try std.testing.expectEqual(@as(i64, @intCast(count * 2 - 1)), (try second.block.cell(count - 1, 1)).value.integer);
            try std.testing.expectEqual(@as(i64, @intCast(count * 2)), (try third.block.cell(0, 1)).value.integer);
            try std.testing.expect(try first.block.encoded.?.isSqlNull(0, 2));
            try std.testing.expect(!try first.block.encoded.?.isSqlNull(0, 3));
            try std.testing.expect((try first.block.cell(0, 2)).sql_null);
            try std.testing.expect(!(try first.block.cell(0, 3)).sql_null);
            try std.testing.expectEqual(@as(u64, 0x8000000000000000), @as(u64, @bitCast((try first.block.cell(0, 4)).value.float)));
            const original = (try first.block.encoded.?.frozenJson(0, 0, false)).?;
            const changed = (try third.block.encoded.?.frozenJson(0, 0, false)).?;
            try std.testing.expectEqual(@as(u8, 'x'), original.bytes[original.bytes.len - 1]);
            try std.testing.expectEqual(@as(u8, 'z'), changed.bytes[changed.bytes.len - 1]);
            try std.testing.expectEqual(@as(usize, 5), reader.reads);
            if (large) {
                try std.testing.expect(manager.written_bytes < 160 * 1024);
                std.debug.print("BODY_LEASE_REUSE compressed={} rows={d} writes={d} reads={d} shared=true\n", .{ compressed, count * 3, manager.written_bytes, reader.reads });
            }
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        try Harness.run(std.testing.allocator, true, compressed);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{ false, compressed });
    }
}

test "SQL native sorted allocation admission preserves deferred rank order on every failure" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, large: bool) !void {
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 128 * 1024 };
            defer std.debug.assert(budget.live == 0);
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var sort = Sort.init(budget.allocator(), &manager, &.{.{}}, 4096);
            defer sort.deinit();
            sort.parallel_runs = false;
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const count: usize = if (large) 6 else 3;
            const View = struct {
                value: @import("execution_batch.zig").FrozenJson,
                id: usize,
                fn frozen(raw: *anyopaque, _: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                    return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).value else null;
                }
                fn cell(raw: *anyopaque, _: Allocator, _: usize, column: usize) anyerror!Datum {
                    if (column == 0) return error.UnexpectedMaterialization;
                    return Datum.json(.{ .integer = @intCast(@as(*@This(), @ptrCast(@alignCast(raw))).id) });
                }
            };
            for (0..count) |index| {
                const body = try source.allocator().alloc(u8, if (large) 20000 else 1536);
                @memset(body, @intCast('a' + index));
                var object: std.json.ObjectMap = .empty;
                try object.put(source.allocator(), "body", .{ .string = body });
                const bytes = try encodeColumnarBlockVersionAlloc(source.allocator(), &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                const block = try decodeColumnarBlockForForwarding(source.allocator(), a, bytes, 1 << 20);
                defer block.deinit();
                var view: View = .{ .value = (try block.frozenJson(0, 0, false)).?, .id = index };
                try sort.addBatch(.{ .reader = .{ .ptr = &view, .read = View.cell, .read_frozen = View.frozen, .count = 1, .width = 2 } }, &.{&.{Datum.json(.{ .integer = @intCast(count - index - 1) })}}, &.{index});
            }
            source.deinit();
            source = .init(a);
            // Exercise the authority boundary with an intentionally low hint.
            // Correct ranks and progress must depend on real allocation
            // admission even when an advisory estimate cannot describe a page.
            sort.max_payload_bytes = 1;
            var seen: usize = 0;
            while (true) {
                var scratch = std.heap.ArenaAllocator.init(budget.allocator());
                defer scratch.deinit();
                const page_delivery = try sort.nextPageLeased(64, 128 * 1024);
                defer page_delivery.release();
                const page = page_delivery.rows;
                if (page.len == 0) break;
                for (page) |lease| {
                    try std.testing.expectEqual(@as(i64, @intCast(seen)), (try lease.keyCell(0)).value.integer);
                    try std.testing.expectEqual(@as(i64, @intCast(count - seen - 1)), (try lease.cell(1)).value.integer);
                    seen += 1;
                }
            }
            try std.testing.expectEqual(count, seen);
            try std.testing.expect(!budget.isExhausted());
            if (large) {
                try std.testing.expect(sort.payload_reader.?.window_reductions > 0);
                std.debug.print("SORT_ADMISSION rows={d} reductions={d} peak={d}\n", .{ seen, sort.payload_reader.?.window_reductions, budget.peak });
            }
        }
    };
    try Harness.run(std.testing.allocator, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{false});
}

test "SQL native cold delivery reserves only current descriptors" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]usize{ 4300, 4608, 4800, 4900, 5000, 12 * 1024 }) |available| for ([_]usize{ 1, 64 }) |lanes| for ([_]bool{ false, true }) |warm| {
        var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 64 * 1024 };
        defer std.debug.assert(budget.live == 0);
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = Hook.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        var pages = try JoinPayloads.init(&manager);
        defer pages.close();
        const body: [2048]u8 = @splat('x');
        const offset = try pages.appendBatch(.{ .rows = &.{&.{Datum.json(.{ .string = &body })}} });
        try pages.file.seal();
        while (pages.evictOne()) {}
        var reader = PayloadReader.init(&pages);
        defer reader.deinit();
        var refs: [64][2]Datum = undefined;
        var rows: [64][]const Datum = undefined;
        for (&refs, &rows) |*reference, *row| {
            reference.* = .{ Datum.json(.{ .integer = @intCast(offset) }), Datum.json(.{ .integer = 0 }) };
            row.* = reference;
        }
        const ballast = try manager.allocator().alloc(u8, @import("memory_budget.zig").headroom(manager.alloc).? - available);
        defer manager.allocator().free(ballast);
        if (warm) {
            const payload = try reader.read(.{ .offset = offset, .index = 0 });
            payload.release();
        }
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        const page = try reader.nativePage(scratch.allocator(), .{ .rows = rows[0..lanes] }, 1, 128 * 1024);
        defer page.release();
        const seen = page.values.len();
        try std.testing.expectEqual(@as(usize, 2048), (try page.values.cell(scratch.allocator(), 0, 0)).value.string.len);
        try std.testing.expect(seen > 0);
        if (available == 12 * 1024) try std.testing.expectEqual(lanes, seen);
        try std.testing.expect(!budget.isExhausted());
        std.debug.print("NATIVE_DELIVERY available={d} lanes={d} warm={} seen={d} peak={d} reductions={d} headroom={d} poisoned={}\n", .{ available, lanes, warm, seen, budget.peak, reader.window_reductions, @import("memory_budget.zig").headroom(manager.alloc).?, budget.isExhausted() });
    };
}

test "SQL native equal bodies share preparation across source addresses" {
    const Reader = struct {
        values: [64]@import("execution_batch.zig").FrozenJson,
        fn get(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).values[row] else null;
        }
        fn cell(_: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            if (column == 0) return error.UnexpectedMaterialization;
            return Datum.json(.{ .integer = @intCast(row) });
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |distinct_addresses| {
        var source = std.heap.ArenaAllocator.init(a);
        defer source.deinit();
        const sa = source.allocator();
        const body = try sa.alloc(u8, 65536);
        @memset(body, 'x');
        var object: std.json.ObjectMap = .empty;
        try object.put(sa, "body", .{ .string = body });
        const wire = try encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
        const block = try decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
        defer block.deinit();
        const frozen = (try block.frozenJson(0, 0, false)).?;
        var reader: Reader = undefined;
        for (&reader.values) |*value| {
            value.* = frozen;
            if (distinct_addresses) value.bytes = try sa.dupe(u8, frozen.bytes);
        }
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        var sort = Sort.init(a, &manager, &.{.{}}, 8192);
        defer sort.deinit();
        sort.parallel_runs = false;
        var key_values: [64]Datum = undefined;
        var keys: [64][]const Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (0..64) |i| {
            key_values[i] = Datum.json(.{ .integer = @intCast(64 - i) });
            keys[i] = key_values[i..][0..1];
            ordinals[i] = i;
        }
        try sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.get, .count = 64, .width = 2 } }, &keys, &ordinals);
        try std.testing.expectEqual(@as(usize, 2), sort.payloads.?.encoded_pages);
        try std.testing.expect(manager.read_bytes < 8192);
        try std.testing.expect(manager.written_bytes < 80 * 1024);
        std.debug.print("BODY_PREPARATION distinct_addresses={} frozen={d} encode_attempts={d} read_bytes={d} read_calls={d} written={d}\n", .{ distinct_addresses, frozen.bytes.len, sort.payloads.?.encoded_pages, manager.read_bytes, manager.read_calls, manager.written_bytes });
        var count: usize = 0;
        while (try sort.nextLeased()) |lease| {
            defer lease.release();
            try std.testing.expectEqual(@as(i64, @intCast(63 - count)), (try lease.cell(1)).value.integer);
            try std.testing.expectEqualSlices(u8, frozen.bytes, (try lease.block.?.encoded.?.frozenJson(lease.index, 0, false)).?.bytes);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 64), count);
    }
}

test "SQL native body cache survives distinct parent pages across producer cohorts" {
    const Reader = struct {
        values: [64]@import("execution_batch.zig").FrozenJson,
        fn get(raw: *anyopaque, row: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).values[row] else null;
        }
        fn cell(_: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            if (column == 0) return error.UnexpectedMaterialization;
            return Datum.json(.{ .integer = @intCast(row) });
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]usize{ 8, 9, 16, 17 }) |cardinality| for ([_]u64{ 512 * 1024, 32 * 1024 * 1024 }) |quota| for ([_]bool{ false, true }) |compressed| {
        var source = std.heap.ArenaAllocator.init(a);
        defer source.deinit();
        const sa = source.allocator();
        const body = try sa.alloc(u8, 20000);
        @memset(body, 'x');
        var object: std.json.ObjectMap = .empty;
        try object.put(sa, "body", .{ .string = body });
        const wire = try encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
        const block = try decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
        defer block.deinit();
        const frozen = (try block.frozenJson(0, 0, false)).?;
        var variants: [17]@import("execution_batch.zig").FrozenJson = undefined;
        for (variants[0..cardinality], 0..) |*value, i| {
            value.* = frozen;
            const owned = try sa.dupe(u8, frozen.bytes);
            owned[owned.len - 1] = @intCast('a' + i);
            value.bytes = owned;
        }
        var reader: Reader = undefined;
        for (&reader.values, 0..) |*value, i| value.* = variants[i % cardinality];
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128, .max_bytes = quota };
        defer manager.deinit();
        var sort = Sort.init(a, &manager, &.{.{}}, 8192);
        defer sort.deinit();
        sort.parallel_runs = false;
        var key_values: [64]Datum = undefined;
        var keys: [64][]const Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (0..64) |i| {
            key_values[i] = Datum.json(.{ .integer = @intCast(64 - i) });
            keys[i] = key_values[i..][0..1];
            ordinals[i] = i;
        }
        for (0..2) |_| try sort.addBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.get, .count = 64, .width = 2 } }, &keys, &ordinals);
        try std.testing.expectEqual(128 + cardinality, sort.payloads.?.encoded_pages);
        try std.testing.expect(manager.written_bytes < 512 * 1024);
        try std.testing.expectEqual(cardinality > 16, sort.payloads.?.body_root != none);
        std.debug.print("BODY_CACHE compressed={} bodies={d} quota={d} encode_attempts={d} read_bytes={d} written={d}\n", .{ compressed, cardinality, quota, sort.payloads.?.encoded_pages, manager.read_bytes, manager.written_bytes });
        var count: usize = 0;
        while (try sort.nextLeased()) |lease| {
            defer lease.release();
            const id = (try lease.cell(1)).value.integer;
            try std.testing.expectEqual(@as(i64, @intCast(63 - count / 2)), id);
            const body_value = (try lease.block.?.encoded.?.frozenJson(lease.index, 0, false)).?;
            try std.testing.expectEqualSlices(u8, variants[@as(usize, @intCast(id)) % cardinality].bytes, body_value.bytes);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 128), count);
    };
}

test "SQL native disk body index resolves radix paths and exact hash collisions on every failure" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, large: bool) !void {
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 128 * 1024 };
            defer std.debug.assert(budget.live == 0);
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = check, .compression = .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var pages = try JoinPayloads.init(&manager);
            defer pages.close();
            const count: usize = if (large) 257 else 7;
            var offsets: [257]u64 = undefined;
            for (0..count) |i| {
                var body: [64]u8 = @splat('x');
                std.mem.writeInt(u64, body[0..8], i, .little);
                var frame: [17]u8 = @splat(0);
                std.mem.writeInt(u64, frame[0..8], body.len, .little);
                const hash: u64 = if (i < 3) 17 else std.hash.Wyhash.hash(0, std.mem.asBytes(&i));
                const offset = pages.file.size;
                offsets[i] = offset;
                try pages.file.writeRaw(offset, &frame);
                try pages.file.writeRaw(offset + frame.len, &body);
                try pages.indexBody(hash, body.len, offset);
                try pages.prepareIndexCache();
                try std.testing.expectEqual(offset, (try pages.findBody(hash, body.len, &body, false)).?);
            }
            try pages.file.seal();
            for (0..count) |i| {
                var body: [64]u8 = @splat('x');
                std.mem.writeInt(u64, body[0..8], i, .little);
                const hash: u64 = if (i < 3) 17 else std.hash.Wyhash.hash(0, std.mem.asBytes(&i));
                try std.testing.expectEqual(offsets[i], (try pages.findBody(hash, body.len, &body, false)).?);
            }
            const missing: [64]u8 = @splat('z');
            try std.testing.expectEqual(@as(?u64, null), try pages.findBody(17, missing.len, &missing, false));
            try std.testing.expectEqual(@as(?u64, null), try pages.findBody(17, missing.len + 1, &missing, false));
            try std.testing.expect(!budget.isExhausted());
            if (large) std.debug.print("BODY_INDEX entries={d} resident_peak={d} disk={d} collisions=3\n", .{ count, budget.peak, pages.file.size });
        }
    };
    try Harness.run(std.testing.allocator, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{false});
}

test "SQL native delivery descriptors outlive caller scratch readers and files on every failure" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator, compressed: bool) !void {
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var pages = try JoinPayloads.init(&manager);
            defer pages.close();
            const body: [2048]u8 = @splat('x');
            const offset = try pages.appendBatch(.{ .rows = &.{&.{Datum.json(.{ .string = &body })}} });
            try pages.file.seal();
            var reader = PayloadReader.init(&pages);
            defer reader.deinit();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const refs = @import("execution_batch.zig").Batch{ .rows = &.{&.{ Datum.json(.{ .integer = @intCast(offset) }), Datum.json(.{ .integer = 0 }) }} };
            const first = try reader.nativePage(scratch.allocator(), refs, 1, 128 * 1024);
            defer first.release();
            const second = try reader.nativePage(scratch.allocator(), refs, 1, 128 * 1024);
            defer second.release();
            try std.testing.expect(first.payloads.ptr != second.payloads.ptr);
            scratch.deinit();
            scratch = .init(a);
            reader.deinit();
            reader = PayloadReader.init(&pages);
            pages.close();
            try std.testing.expectEqual(@as(usize, 0), manager.files);
            try std.testing.expectEqualStrings(&body, (try first.values.cell(scratch.allocator(), 0, 0)).value.string);
            try std.testing.expectEqualStrings(&body, (try second.values.cell(scratch.allocator(), 0, 0)).value.string);
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        try Harness.run(std.testing.allocator, compressed);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{compressed});
    }
}

test "SQL bounded native delivery repeated producer bodies lookup before encoding" {
    const Frozen = @import("execution_batch.zig").FrozenJson;
    const Reader = struct {
        value: Frozen,
        base: usize = 0,
        fn check(_: *anyopaque) !void {}
        fn frozen(raw: *anyopaque, _: usize, column: usize) anyerror!?Frozen {
            return if (column == 0) @as(*@This(), @ptrCast(@alignCast(raw))).value else null;
        }
        fn cell(raw: *anyopaque, _: Allocator, row: usize, column: usize) anyerror!Datum {
            if (column == 0) return error.UnexpectedMaterialization;
            const self: *@This() = @ptrCast(@alignCast(raw));
            return Datum.json(.{ .integer = @intCast(self.base + row) });
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |compressed| {
        var source = std.heap.ArenaAllocator.init(a);
        defer source.deinit();
        const sa = source.allocator();
        const body = try sa.alloc(u8, 65536);
        @memset(body, 'x');
        var object: std.json.ObjectMap = .empty;
        try object.put(sa, "body", .{ .string = body });
        const wire = try encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
        const block = try decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
        defer block.deinit();
        var view: Reader = .{ .value = (try block.frozenJson(0, 0, false)).? };
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        var pages = try JoinPayloads.init(&manager);
        defer pages.close();
        var last: u64 = 0;
        for (0..32) |cohort| {
            view.base = cohort * 64;
            last = try pages.appendBatch(.{ .reader = .{ .ptr = &view, .read = Reader.cell, .read_frozen = Reader.frozen, .count = 64, .width = 2 } });
        }
        try std.testing.expectEqual(@as(usize, 33), pages.encoded_pages);
        std.debug.print("NATIVE_BODY_LOOKUP compressed={} cohorts=32 frozen={d} attempts={d} reads={d} calls={d} writes={d}\n", .{ compressed, view.value.bytes.len, pages.encoded_pages, manager.read_bytes, manager.read_calls, manager.written_bytes });
        try pages.file.seal();
        var reader = PayloadReader.init(&pages);
        defer reader.deinit();
        const value = try reader.read(.{ .offset = last, .index = 63 });
        defer value.release();
        try std.testing.expectEqual(@as(i64, 2047), (try value.block.cell(63, 1)).value.integer);
        try std.testing.expectEqualSlices(u8, view.value.bytes, (try value.block.encoded.?.frozenJson(63, 0, false)).?.bytes);
    }
}

test "SQL bounded native delivery content keys survive producer reuse text spelling collisions and allocation faults" {
    const Harness = struct {
        fn check(_: *anyopaque) !void {}
        const Reader = struct {
            bytes: []const u8,
            kind: u8,
            fn frozen(raw: *anyopaque, _: usize, _: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return if (self.kind == 1) null else .{ .bytes = self.bytes, .lossless_text = self.kind == 3 };
            }
            fn json(raw: *anyopaque, _: usize, _: usize) anyerror!?[]const u8 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return if (self.kind == 1) self.bytes else null;
            }
            fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) anyerror!Datum {
                return error.UnexpectedMaterialization;
            }
        };
        fn run(a: Allocator, compressed: bool) !void {
            var token: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false, .buffer_bytes = 128 };
            defer manager.deinit();
            var pages = try JoinPayloads.init(&manager);
            defer pages.close();
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const text = try source.allocator().alloc(u8, 4096);
            @memset(text, 'x');
            const json = try std.fmt.allocPrint(source.allocator(), "{{\"body\":\"{s}\"}}", .{text});
            var object: std.json.ObjectMap = .empty;
            try object.put(source.allocator(), "body", .{ .string = text });
            const wire = try encodeColumnarBlockVersionAlloc(source.allocator(), &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, manager.max_record_bytes, .v2);
            const block = try decodeColumnarBlockForForwarding(source.allocator(), a, wire, manager.max_record_bytes);
            defer block.deinit();
            const frozen = (try block.frozenJson(0, 0, false)).?;
            var offsets: [20]u64 = undefined;
            for (&offsets, 0..) |*offset, i| {
                const bytes = if (i < 2) json else try source.allocator().dupe(u8, frozen.bytes);
                const kind: u8 = if (i < 2) 1 else if (i == 2) 2 else 3;
                if (i == 1) bytes[json.len - 3] = 'y';
                if (i >= 4) @constCast(bytes)[bytes.len - 1] = @intCast(i + 32);
                var reader: Reader = .{ .bytes = bytes, .kind = kind };
                const key: JoinPayloads.BodyKey = .{ .bytes = bytes, .kind = kind, .hash = if (i < 5) 17 else i * 1024 };
                try std.testing.expectEqual(@as(?u64, null), try pages.lookupNativeBody(key));
                offset.* = try pages.appendInlineBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.frozen, .read_json = Reader.json, .count = 1, .width = 1 } }, .body, key);
                try std.testing.expectEqual(offset.*, (try pages.lookupNativeBody(key)).?);
                // Index lookups must retain exact representation, even with
                // an intentionally colliding hash and otherwise identical bytes.
                if (i == 3) try std.testing.expect(offset.* != offsets[2]);
            }
            try std.testing.expect(pages.body_root != none);
            // First frozen body's hash bucket is now cold in the 16-entry LRU.
            const original: JoinPayloads.BodyKey = .{ .bytes = frozen.bytes, .kind = 2, .hash = 17 };
            const before = pages.encoded_pages;
            try std.testing.expectEqual(offsets[2], (try pages.lookupNativeBody(original)).?);
            try std.testing.expectEqual(before, pages.encoded_pages);
            var missing = try source.allocator().dupe(u8, frozen.bytes);
            missing[missing.len - 1] ^= 1;
            try std.testing.expectEqual(@as(?u64, null), try pages.lookupNativeBody(.{ .bytes = missing, .kind = 2, .hash = 17 }));
            try std.testing.expectEqual(@as(?u64, null), try pages.lookupNativeBody(.{ .bytes = frozen.bytes, .kind = 1, .hash = 17 }));
        }
    };
    for ([_]bool{ false, true }) |compressed| {
        try Harness.run(std.testing.allocator, compressed);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{compressed});
    }
}

test "SQL compact delivery text key compression amplification" {
    const Reader = struct {
        text: []const u8,
        frozen_value: @import("execution_batch.zig").FrozenJson,
        frozen_mode: bool,
        id: usize,
        fn check(_: *anyopaque) !void {}
        fn cell(raw: *anyopaque, _: Allocator, _: usize, column: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (column == 0) return error.UnexpectedMaterialization;
            return Datum.json(.{ .integer = @intCast(self.id) });
        }
        fn frozen(raw: *anyopaque, _: usize, column: usize) anyerror!?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (column == 0 and self.frozen_mode) self.frozen_value else null;
        }
        fn json(raw: *anyopaque, _: usize, column: usize) anyerror!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (column == 0 and !self.frozen_mode) self.text else null;
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |compressed| for ([_]bool{ false, true }) |repeated| for ([_]bool{ false, true }) |frozen_mode| {
        var token: u8 = 0;
        var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = if (compressed) .snappy else .none, .max_bytes = if (compressed) 256 * 1024 else 32 * 1024 * 1024, .async_writes = false, .buffer_bytes = 128 };
        defer manager.deinit();
        var pages = try JoinPayloads.init(&manager);
        defer pages.close();
        var last: u64 = 0;
        for (0..32) |i| {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const body = try sa.alloc(u8, 65536);
            @memset(body, 'x');
            const id: usize = if (repeated) 0 else i;
            body[0] = @as(u8, 'a') + @as(u8, @intCast(id / 26));
            body[1] = @as(u8, 'a') + @as(u8, @intCast(id % 26));
            const text = try std.fmt.allocPrint(sa, "{{\"body\":\"{s}\"}}", .{body});
            var object: std.json.ObjectMap = .empty;
            try object.put(sa, "body", .{ .string = body });
            const wire = try encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
            const block = try decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
            defer block.deinit();
            var reader: Reader = .{ .text = text, .frozen_value = (try block.frozenJson(0, 0, false)).?, .frozen_mode = frozen_mode, .id = i };
            last = try pages.appendBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_json = Reader.json, .read_frozen = Reader.frozen, .count = 1, .width = 2 } });
        }
        if (compressed) try std.testing.expect(manager.written_bytes < 256 * 1024);
        std.debug.print("COMPRESSED_TEXT_KEY compressed={} repeated={} frozen={} attempts={d} reads={d} calls={d} writes={d}\n", .{ compressed, repeated, frozen_mode, pages.encoded_pages, manager.read_bytes, manager.read_calls, manager.written_bytes });
        try pages.file.seal();
        var reader = PayloadReader.init(&pages);
        defer reader.deinit();
        const payload = try reader.read(.{ .offset = last, .index = 0 });
        defer payload.release();
        try std.testing.expectEqual(@as(i64, 31), (try payload.block.cell(0, 1)).value.integer);
        const expected: []const u8 = if (repeated) "aa" else "bf";
        const decoded = try payload.block.cell(0, 0);
        const body = decoded.value.object.get("body").?.string;
        try std.testing.expectEqualStrings(expected, body[0..2]);
    };
}

test "SQL compact delivery compressed text keys verify with fixed headroom and reject corrupt frames" {
    const Reader = struct {
        text: []const u8,
        fn check(_: *anyopaque) !void {}
        fn cell(_: *anyopaque, _: Allocator, _: usize, _: usize) anyerror!Datum {
            return error.UnexpectedMaterialization;
        }
        fn json(raw: *anyopaque, _: usize, _: usize) anyerror!?[]const u8 {
            return @as(*@This(), @ptrCast(@alignCast(raw))).text;
        }
    };
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    const bytes = try source.allocator().alloc(u8, 65536);
    @memset(bytes, 'x');
    const text = try std.fmt.allocPrint(source.allocator(), "{{\"body\":\"{s}\"}}", .{bytes});
    var reader: Reader = .{ .text = text };
    var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 2 * 1024 * 1024 };
    defer std.debug.assert(budget.live == 0);
    var token: u8 = 0;
    var manager: Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &token, .checkpoint = Reader.check, .compression = .snappy, .async_writes = false, .buffer_bytes = 128 };
    defer manager.deinit();
    var pages = try JoinPayloads.init(&manager);
    defer pages.close();
    const key: JoinPayloads.BodyKey = .{ .bytes = text, .kind = 1, .hash = 17 };
    const offset = try pages.appendNativeBody(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_json = Reader.json, .count = 1, .width = 1 } }, 0, 0, key);
    try std.testing.expectEqual(offset, (try pages.lookupNativeBody(key)).?);
    var frame: [26]u8 = undefined;
    try pages.file.readRaw(offset, &frame);
    try std.testing.expectEqual(@as(u8, 129), frame[17]);
    const key_offset = offset + 17 + std.mem.readInt(u64, frame[0..8], .little);
    var original: [8]u8 = undefined;
    try pages.file.readRaw(key_offset, &original);
    const stored_length = std.mem.readInt(u64, &original, .little);
    try std.testing.expect(stored_length < text.len / 10);
    const missing = try source.allocator().dupe(u8, text);
    missing[text.len - 3] = 'y';
    {
        const limit = budget.limit;
        budget.limit = budget.live + 32;
        defer budget.limit = limit;
        try std.testing.expectEqual(offset, (try pages.lookupNativeBody(key)).?);
        try std.testing.expectEqual(@as(?u64, null), try pages.lookupNativeBody(.{ .bytes = missing, .kind = 1, .hash = 17 }));
        try std.testing.expectEqual(@as(?u64, null), try pages.lookupNativeBody(.{ .bytes = text, .kind = 2, .hash = 17 }));
        try std.testing.expect(!budget.isExhausted());
    }
    var corrupt: [8]u8 = undefined;
    std.mem.writeInt(u64, &corrupt, pages.file.size + 1, .little);
    try pages.file.writeRaw(key_offset, &corrupt);
    try std.testing.expectError(error.InvalidSqlSpill, pages.lookupNativeBody(key));
    try pages.file.writeRaw(key_offset, &original);
    try std.testing.expectEqual(offset, (try pages.lookupNativeBody(key)).?);
    // An invalid COPY tag after the valid decoded-size preamble cannot be
    // treated as an equal key, even with an equal hash and length descriptor.
    var encoded: [5]u8 = undefined;
    try pages.file.readRaw(key_offset + 8, &encoded);
    var prefix: usize = 0;
    while (encoded[prefix] & 128 != 0) : (prefix += 1) {}
    prefix += 1;
    const bad_copy = [_]u8{ 1, 0 };
    try pages.file.writeRaw(key_offset + 8 + prefix, &bad_copy);
    try std.testing.expectError(error.InvalidSqlSpill, pages.lookupNativeBody(key));
}

test "SQL native spill decimal views retain selected coefficients display scales and NULLs" {
    const a = std.testing.allocator;
    var context: u8 = 0;
    var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    var file = try Sequential.init(&manager, 4096);
    defer file.close();
    const numeric = @import("numeric_value.zig");
    var numbers: numeric.Context = .{ .alloc = a };
    var first = try numeric.parse(&numbers, "1.2300");
    defer first.deinit();
    var second = try numeric.parse(&numbers, "-4.5");
    defer second.deinit();
    for ([_]Datum{ Datum.typedNumeric(&first.value), .{}, Datum.typedNumeric(&second.value) }, 0..) |datum, i| _ = try file.append(.{ .values = &.{datum}, .keys = &.{}, .ordinal = i }, none);
    try file.seal();
    var block = try file.readInputBlock(0);
    defer block.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const view = (try block.view().decimalColumn(arena.allocator(), 0, &.{ 2, 1, 0, 2 })).?;
    try std.testing.expectEqualSlices(i128, &.{ -45, 0, 12300, -45 }, view.physical.vector.values.decimal128.values);
    try std.testing.expectEqualSlices(u16, &.{ 1, 0, 4, 1 }, view.scales);
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 0, 0 }, view.physical.vector.nulls.bytes);
}

test "SQL shared radix partitions bound descriptors and reclaim canceled extents" {
    const Check = struct {
        canceled: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
    };
    var check: Check = .{};
    var manager: Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &check, .checkpoint = Check.check, .async_writes = false };
    defer manager.deinit();
    {
        var partitions = try Partitions.init(&manager);
        defer partitions.deinit();
        for (0..64) |index| try partitions.appendBatch(index, .{ .rows = &.{&.{Datum.json(.{ .integer = @intCast(index) })}} }, .{ .rows = &.{&.{}} }, &.{index});
        try partitions.seal();
        try std.testing.expectEqual(@as(usize, 1), manager.peak_files);
        var child = partitions.child();
        defer child.deinit();
        for (0..64) |index| {
            const row = (try partitions.streams[index].?.readBatchBorrowed(0, 256)).rows[0];
            try std.testing.expectEqual(@as(i64, @intCast(index)), row.values[0].value.integer);
            if (index == 0) try child.appendBatch(0, .{ .rows = &.{row.values} }, .{ .rows = &.{row.keys} }, &.{row.ordinal});
            partitions.close(index);
        }
        try child.seal();
        try std.testing.expectEqual(@as(i64, 0), (try child.streams[0].?.readBatchBorrowed(0, 1)).rows[0].values[0].value.integer);
        check.canceled = true;
        try std.testing.expectError(error.Canceled, child.appendBatch(1, .{ .rows = &.{&.{Datum.json(.{ .integer = 1 })}} }, .{ .rows = &.{&.{}} }, &.{1}));
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 1), manager.peak_files);
}

test "SQL shared radix partitions unwind allocation faults and disk admission" {
    const Check = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: Allocator) !void {
            var dummy: u8 = 0;
            var manager: Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .async_writes = false };
            defer manager.deinit();
            var parent = try Partitions.init(&manager);
            defer parent.deinit();
            try parent.appendBatch(0, .{ .rows = &.{&.{Datum.json(.{ .string = "checkpoint" })}} }, .{ .rows = &.{&.{}} }, &.{0});
            var child = parent.child();
            defer child.deinit();
            try child.appendBatch(3, .{ .rows = &.{&.{Datum.json(.{ .integer = 2 })}} }, .{ .rows = &.{&.{}} }, &.{1});
            try child.seal();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    var dummy: u8 = 0;
    var manager: Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check, .max_bytes = 64 * 1024, .async_writes = false };
    defer manager.deinit();
    {
        var partitions = try Partitions.init(&manager);
        defer partitions.deinit();
        try partitions.appendBatch(0, .{ .rows = &.{&.{Datum.json(.{ .integer = 1 })}} }, .{ .rows = &.{&.{}} }, &.{0});
        try std.testing.expectError(error.SqlProgramLimitExceeded, partitions.appendBatch(1, .{ .rows = &.{&.{Datum.json(.{ .integer = 2 })}} }, .{ .rows = &.{&.{}} }, &.{1}));
    }
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
}
