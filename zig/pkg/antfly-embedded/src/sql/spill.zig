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
            const job = self.prepareAsync() orelse {
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
    fn prepareAsync(self: *File) ?*WriteJob {
        if (self.spare.len == 0) self.spare = self.manager.allocator().alloc(u8, self.write_buffer.len) catch return null;
        if (self.write_job) |job| return job;
        const job = self.manager.allocator().create(WriteJob) catch {
            self.manager.allocator().free(self.spare);
            self.spare = &.{};
            return null;
        };
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
        try self.manager.check();
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(self.manager.allocator());
        var encoder: Encoder = .{ .manager = self.manager, .a = self.manager.allocator(), .bytes = &bytes, .limit = self.manager.max_record_bytes };
        try encoder.word(row.ordinal);
        try encoder.cells(row.values);
        try encoder.cells(row.keys);
        var compressed: ?[]u8 = null;
        defer if (compressed) |value| self.manager.allocator().free(value);
        // Avoid codec work on short or apparently incompressible records.
        // Compression is an existing Snappy block format, never a new codec.
        if (self.manager.compression == .snappy and bytes.items.len >= 4096) {
            const sample = bytes.items[0..@min(bytes.items.len, 1024)];
            var repeated: usize = 0;
            for (sample[1..], sample[0 .. sample.len - 1]) |x, y| repeated += @intFromBool(x == y);
            if (repeated > sample.len / 4) compressed = try snappy.encode(self.manager.allocator(), bytes.items);
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
    fn deinit(self: *JsonOwner) void {
        self.arena.deinit();
    }
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
pub fn frozenJsonText(a: Allocator, value: @import("execution_batch.zig").FrozenJson) ![]const u8 {
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    var json: std.json.Stringify = .{ .writer = &output.writer };
    var decoder: Decoder = .{ .a = a, .bytes = value.bytes };
    _ = decoder.walkJson(0, &json) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
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
        if (stored.values == .dynamic) return stored.values.dynamic.cells[row].sql_null;
        return ((stored.flags[row / 4] >> @as(u3, @intCast(row % 4 * 2))) & 3) == 1;
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
        if (stored.values != .dynamic or stored.values.dynamic.json.len == 0) return null;
        const json = stored.values.dynamic.json[row] orelse return null;
        return .{ .bytes = json.bytes, .lossless_text = json.lossless };
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
    if (values.len() == 0 or values.len() > 256 or values.width() > 1024 or ordinals.len != values.len()) return error.InvalidSqlSpill;
    const start = bytes.items.len;
    errdefer bytes.shrinkRetainingCapacity(start);
    var encoder: Encoder = .{ .compact_arrays = version == .v2, .a = output_alloc, .scratch = @import("memory_budget.zig").reclaimingAllocator(scratch), .bytes = bytes, .base = start, .limit = max_bytes };
    try encoder.append(if (version == .v1) columnar_v1_magic else columnar_v2_magic);
    try encoder.word(values.len());
    try encoder.word(values.width());
    try encoder.word(0);
    for (ordinals) |ordinal| try encoder.word(ordinal);
    try Sequential.encodeBatchColumns(&encoder, values, 0, values.len());
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
    if (bytes.len > max_bytes or bytes.len < columnar_v1_magic.len) return error.InvalidSqlSpill;
    const compact = std.mem.startsWith(u8, bytes, columnar_v2_magic);
    if (!compact and !std.mem.startsWith(u8, bytes, columnar_v1_magic)) return error.InvalidSqlSpill;
    var json_scratch: JsonScratch = .{ .a = @import("memory_budget.zig").reclaimingAllocator(a) };
    defer json_scratch.deinit();
    var decoder: Decoder = .{ .json_consumption = consumption, .json_scratch = &json_scratch, .lazy_json = cache_backing != null, .cache_backing = cache_backing, .borrow_compact = true, .compact_arrays = compact, .a = a, .bytes = bytes, .position = columnar_v1_magic.len };
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

/// Sequential native runs use typed column blocks when multiple rows fit;
/// wide rows retain the compact record framing without extra staging copies.
/// Random-access hash chains
/// keep File's independently framed records. Logical offsets here are row
/// ordinals; readers consume blocks in order and may restart at ordinal zero.
/// Random-access native payloads for hash chains. The chain's separate small
/// key record owns the link/matched flag; failed key comparisons never read
/// this file. Offsets are physical, so reverse chains do not replay prefixes.
pub const JoinPayloads = struct {
    file: File,
    pub fn init(manager: *Manager) !JoinPayloads {
        var file = try manager.create();
        file.buffer_bytes = @min(4096, manager.buffer_bytes);
        return .{ .file = file };
    }
    pub fn close(self: *JoinPayloads) void {
        self.file.close();
    }
    pub fn append(self: *JoinPayloads, values: @import("execution_batch.zig").Batch, ordinal: u64) !u64 {
        if (values.len() != 1) return error.InvalidSqlSpill;
        var arena = std.heap.ArenaAllocator.init(self.file.manager.allocator());
        defer arena.deinit();
        const wire = try encodeColumnarBatchVersionAlloc(arena.allocator(), values, &.{ordinal}, self.file.manager.max_record_bytes, .v2);
        const compressed = if (self.file.manager.compression == .snappy and wire.len >= 1024) try snappy.encode(arena.allocator(), wire) else null;
        const use_compressed = compressed != null and compressed.?.len + 32 < wire.len;
        const stored = if (use_compressed) compressed.? else wire;
        var header: [17]u8 = undefined;
        header[16] = @intFromBool(use_compressed);
        std.mem.writeInt(u64, header[0..8], stored.len, .little);
        std.mem.writeInt(u64, header[8..16], std.hash.Wyhash.hash(0, wire), .little);
        const offset = self.file.size;
        try self.file.writeRaw(offset, &header);
        try self.file.writeRaw(offset + header.len, stored);
        self.file.manager.increment("compressed_records", @intFromBool(use_compressed));
        return offset;
    }
    pub fn read(self: *JoinPayloads, offset: u64) !*Sequential.OwnedBlock {
        var header: [17]u8 = undefined;
        try self.file.readRaw(offset, &header);
        const length = std.mem.readInt(u64, header[0..8], .little);
        if (header[16] > 1 or length > self.file.manager.max_record_bytes or length > self.file.size -| (offset + header.len)) return error.InvalidSqlSpill;
        const a = self.file.manager.allocator();
        const owner = try a.create(Sequential.OwnedBlock);
        owner.* = .{ .a = a, .arena = .init(a) };
        errdefer owner.release();
        const encoded = try owner.arena.allocator().alloc(u8, @intCast(length));
        try self.file.readRaw(offset + header.len, encoded);
        const wire = if (header[16] == 1) blk: {
            const decoded_len = snappy.decodedLen(encoded) catch return error.InvalidSqlSpill;
            if (decoded_len > self.file.manager.max_record_bytes) return error.InvalidSqlSpill;
            break :blk snappy.decode(owner.arena.allocator(), encoded) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidSqlSpill,
            };
        } else encoded;
        if (std.hash.Wyhash.hash(0, wire) != std.mem.readInt(u64, header[8..16], .little)) return error.InvalidSqlSpill;
        owner.encoded = try decodeColumnarBlockForForwarding(owner.arena.allocator(), a, wire, self.file.manager.max_record_bytes);
        if (owner.count() != 1) return error.InvalidSqlSpill;
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
                for (0..values.width()) |column| cost +|= try values.cellBytes(sizing.allocator(), end, column);
                for (0..keys.width()) |column| cost +|= try keys.cellBytes(sizing.allocator(), end, column);
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
            try encodeBatchColumns(&encoder, values, begin, end);
            try encodeBatchColumns(&encoder, keys, begin, end);
            try self.writeEncoded(bytes.items);
            self.size += end - begin;
            begin = end;
        }
    }
    fn encodeBatchColumns(encoder: *Encoder, batch: @import("execution_batch.zig").Batch, begin: usize, end: usize) !void {
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
                try decodeCompactAt(&self.source.file, self.source.size, &physical, block, owned, offset);
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
            return readState(self, &self.source.file, self.source.size, offset);
        }

        pub fn deinit(self: *ReplayReader) void {
            if (self.compact) |block| block.release();
            self.read_arena.deinit();
            self.source.readers -= 1;
            self.* = undefined;
        }
    };

    pub fn openReader(self: *Sequential) !ReplayReader {
        try self.seal();
        self.readers += 1;
        return .{ .source = self, .read_arena = .init(self.file.manager.allocator()) };
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
            dynamic: struct { cells: []const Datum, json: []?*FrozenJson = &.{} },
            dictionary: Dictionary,
        },
        fn encodedJson(self: EncodedColumn, index: usize) !?[]const u8 {
            if (self.values != .dynamic or self.values.dynamic.json.len == 0) return null;
            const json = self.values.dynamic.json[index] orelse return null;
            return json.encoded();
        }
        fn cell(self: EncodedColumn, index: usize) !Datum {
            if (self.values == .dynamic) {
                const dynamic = self.values.dynamic;
                if (dynamic.json.len != 0) if (dynamic.json[index]) |json| return json.materialize();
                return dynamic.cells[index];
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
                .dynamic => unreachable,
            });
        }
        fn decode(decoder: *Decoder, count: usize) !EncodedColumn {
            const kind = try decoder.byte();
            if (kind == 255) {
                const values = try decoder.a.alloc(Datum, count);
                var lazy: []?*FrozenJson = &.{};
                for (values, 0..) |*value, row| {
                    const start = decoder.position;
                    // Preserve ordinary scalar/array/pattern decoding. Only
                    // non-NULL compound JSON uses the immutable range path.
                    if (try decoder.count() != 1) return error.InvalidSqlSpill;
                    const flag = try decoder.byte();
                    if (flag > 1) return error.InvalidSqlSpill;
                    if (decoder.lazy_json and flag == 0 and decoder.position < decoder.bytes.len and (decoder.bytes[decoder.position] == 6 or decoder.bytes[decoder.position] == 7)) {
                        const begin = decoder.position;
                        // Stop at the first tag that requires scalar semantics.
                        // Validate the remainder while constructing that scalar
                        // once, instead of validating then replaying the tree.
                        decoder.eager_nontext = decoder.json_consumption == .scalar;
                        const lossless = decoder.walkJson(0, null) catch |err| switch (err) {
                            error.ScalarJsonRequired => false,
                            else => return err,
                        };
                        decoder.eager_nontext = false;
                        if (decoder.json_owner == null) {
                            const owner = try decoder.a.create(JsonOwner);
                            // The enclosing statement/page allocator retains
                            // admission authority. Do not impose a second,
                            // arbitrary DOM-expansion limit on legal values.
                            owner.* = .{ .arena = .init(decoder.cache_backing.?) };
                            decoder.json_owner = owner;
                        }
                        if (lazy.len == 0) {
                            lazy = try decoder.a.alloc(?*FrozenJson, count);
                            @memset(lazy, null);
                        }
                        var materialized: ?std.json.Value = null;
                        if (!lossless and decoder.json_consumption == .scalar) {
                            decoder.position = begin;
                            const metadata = decoder.a;
                            decoder.a = decoder.json_owner.?.arena.allocator();
                            defer decoder.a = metadata;
                            materialized = try decoder.json(0);
                        }
                        const json = try decoder.a.create(FrozenJson);
                        json.* = .{ .owner = decoder.json_owner.?, .bytes = decoder.bytes[begin..decoder.position], .lossless = lossless, .value = materialized };
                        lazy[row] = json;
                        value.* = Datum.json(.null);
                    } else {
                        decoder.position = start;
                        const one = try decoder.cells();
                        if (one.len != 1) return error.InvalidSqlSpill;
                        value.* = one[0];
                    }
                }
                return .{ .values = .{ .dynamic = .{ .cells = values, .json = lazy } } };
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
                fn dictionary(raw: *anyopaque, alloc: Allocator, column: usize) anyerror!?@import("execution_batch.zig").Batch {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    const encoded = view.block.encoded orelse return null;
                    return encoded.dictionaryColumn(alloc, column, view.keys);
                }
            };
            const view = try a.create(View);
            view.* = .{ .block = self, .keys = keys };
            // Ordering/hash keys need scalar semantics. Keep their first
            // materialization cached in the block instead of retaining text
            // which a key store would parse again for every comparison.
            return .{ .reader = .{ .ptr = view, .read = View.cell, .read_identity = View.identity, .read_dictionary = View.dictionary, .read_json = if (keys) null else View.json, .read_frozen = if (keys) null else View.frozen, .count = self.count(), .width = if (keys) self.keyWidth() else self.width() } };
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
                    if (stored.values != .dynamic) {
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
        const a = self.file.manager.allocator();
        const owner = try a.create(OwnedBlock);
        errdefer a.destroy(owner);
        // A scalar merge may already have borrowed this record. Transfer its
        // arena once; subsequent records decode directly into compact columns.
        if (self.read_rows.len == 0 and self.read_single_offset != offset and offset == self.read_first) {
            owner.* = .{ .a = a, .arena = .init(a) };
            errdefer owner.arena.deinit();
            try self.decodeCompactBlock(owner, owner.arena.allocator(), offset);
            return owner;
        }
        const block = try self.readBatchBorrowed(offset, 256);
        // Typed block descriptors already belong to the transferred arena.
        // Only the legacy single-row descriptor lives inline in Sequential.
        const rows = if (self.read_rows.len != 0) block.rows else try self.read_arena.allocator().dupe(Row, block.rows);
        owner.* = .{ .a = a, .arena = self.read_arena, .rows = rows };
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

pub const Sort = struct {
    const Entry = struct { normalized: ?@import("sort_key.zig").Key, position: usize, ordinal: u64 };
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
    leased_reads: bool = false,
    head_arenas: [8]std.heap.ArenaAllocator = undefined,
    output_count: usize = 0,
    max_row_bytes: usize = 0,
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
    pub fn add(self: *Sort, input: Row) !void {
        var row = input;
        row.normalized = try self.normalize(row.keys);
        if (self.finished or row.keys.len != self.orders.len) return error.InvalidSqlBackendResponse;
        for (row.keys) |key| if (!key.sql_null) {
            _ = try scalar.compareDatums(key, key);
        };
        try self.manager.check();
        var bytes: usize = @sizeOf(Row);
        for (row.values) |v| bytes +|= try operators.datumBytes(v);
        for (row.keys) |v| bytes +|= try operators.datumBytes(v);
        self.max_row_bytes = @max(self.max_row_bytes, bytes);
        if (self.presorted) {
            if (self.presorted_run == null) self.presorted_run = try self.createRun();
            _ = try self.presorted_run.?.append(row, none);
            self.total += 1;
            return;
        }
        try self.collectPresortedRun();
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
        var retained = @sizeOf(Entry) +| try self.values.appendBytes(row.values) +| try self.keys.appendBytes(row.keys);
        if (self.rows.items.len != 0 and (retained > self.memory_bytes / (if (self.parallel_runs and self.memory_bytes >= 128 * 1024) @as(usize, 8) else 4) -| self.estimated)) {
            try self.flush();
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
    fn flush(self: *Sort) !void {
        try self.collectRun();
        if (self.rows.items.len == 0) return;
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
    fn mergeMany(self: *Sort, inputs: []const *Sequential) !Sequential {
        std.debug.assert(inputs.len >= 2 and inputs.len <= self.fanIn());
        var output = try self.createRun();
        errdefer output.close();
        var arenas: [8]std.heap.ArenaAllocator = undefined;
        var heads: [8]?Decoded = @splat(null);
        for (arenas[0..inputs.len]) |*arena| arena.* = std.heap.ArenaAllocator.init(self.a);
        defer for (arenas[0..inputs.len]) |*arena| arena.deinit();
        for (inputs, arenas[0..inputs.len], heads[0..inputs.len]) |file, *arena, *head|
            head.* = if (file.size != 0) try self.readRun(file, arena.allocator(), 0) else null;
        while (true) {
            try self.manager.check();
            var selected: ?usize = null;
            for (heads[0..inputs.len], 0..) |head, index| if (head) |value| {
                if (selected == null or (try self.compare(value.row, heads[selected.?].?.row)) == .lt) selected = index;
            };
            const index = selected orelse break;
            const head = heads[index].?;
            _ = try output.append(head.row, none);
            _ = arenas[index].reset(.retain_capacity);
            heads[index] = if (head.following < inputs[index].size) try self.readRun(inputs[index], arenas[index].allocator(), head.following) else null;
        }
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
    fn finishImpl(self: *Sort, leased: bool) !void {
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
                const block = try file.*.?.readOwnedBlock(0);
                self.owned_heads[index] = block;
                self.owned_positions[index] = 0;
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
    pub fn rewind(self: *Sort) !void {
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
        values: ?*const @import("typed_store.zig").Store = null,
        keys: ?*const @import("typed_store.zig").Store = null,
        pub fn cell(self: RowLease, column: usize) !Datum {
            return if (self.block) |block| block.cell(self.index, column) else if (self.values) |values| values.cell(values.a, self.index, column) else self.row.values[column];
        }
        /// Merge consumers can inspect typed keys without reconstructing an
        /// entire row or duplicating keys in the stored payload columns.
        pub fn keyCell(self: RowLease, column: usize) !Datum {
            return if (self.block) |block| block.keyCell(self.index, column) else if (self.keys) |keys| keys.cell(keys.a, self.index, column) else self.row.keys[column];
        }
        pub fn release(self: RowLease) void {
            if (self.block) |block| block.release();
        }
    };
    /// Sorted delivery holds decoded run blocks through the transport page.
    /// Only row descriptors are gathered; wide payload bytes remain in place.
    pub fn nextLeased(self: *Sort) !?RowLease {
        try self.finishImpl(true);
        if (!self.leased_reads) {
            for (self.heads[0..self.output_count], 0..) |head, index| if (head) |value| {
                self.owned_heads[index] = try self.outputs[index].?.readOwnedBlock(value.following - 1);
                self.owned_positions[index] = 0;
            };
            self.leased_reads = true;
        }
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
            if (self.owned_positions[index] == owner.count()) {
                self.owned_heads[index] = null;
                owner.release();
                if (head.following < self.outputs[index].?.size) {
                    self.owned_heads[index] = try self.outputs[index].?.readOwnedBlock(head.following);
                    self.owned_positions[index] = 0;
                }
            }
            self.heads[index] = if (self.owned_heads[index]) |block| blk: {
                _ = self.head_arenas[index].reset(.retain_capacity);
                var row = try block.keyRowAlloc(self.head_arenas[index].allocator(), self.owned_positions[index]);
                row.normalized = try self.normalize(row.keys);
                break :blk .{ .row = row, .next = none, .matched = false, .following = head.following + 1 };
            } else null;
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
    fn nextImpl(self: *Sort, a: Allocator, retain_keys: bool) !?Row {
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
                const frozen = block.values[0].values.dynamic.json[0].?;
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
    const frozen = reader.compact.?.encoded.?.values[0].values.dynamic.json[0].?;
    try std.testing.expect(frozen.value == null);
    const key_batch = try reader.compact.?.batch(arena.allocator(), true);
    try std.testing.expect(try key_batch.encodedJson(0, 0) == null);
    const key = try key_batch.cell(arena.allocator(), 0, 0);
    try std.testing.expectEqualStrings("1", key.value.object.get("x").?.array.items[0].number_string);
    try std.testing.expect(reader.compact.?.encoded.?.keys[0].values.dynamic.json[0].?.value != null);
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
            if (lazy) try std.testing.expect(block.values[0].values.dynamic.json[0].?.value == null);
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
                try std.testing.expect(block.values[0].values.dynamic.json[0].?.value != null);
                try std.testing.expect(block.values[0].values.dynamic.json[0].?.text == null);
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
            if (forward) for (block.values[0].values.dynamic.json) |json| {
                try std.testing.expect(json.?.value == null);
                try std.testing.expect(json.?.text == null);
            };
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
                try std.testing.expect(block.values[0].values.dynamic.json[0].?.value == null);
                try std.testing.expect(block.values[0].values.dynamic.json[0].?.text == null);
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
            if (forward) for (block.values[0].values.dynamic.json) |json| {
                try std.testing.expect(json.?.value == null);
                try std.testing.expect(json.?.text == null);
            };
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
        const first = try file.append(.{ .rows = &.{&row} }, 7);
        const second = try file.append(.{ .rows = &.{&row} }, 11);
        @memset(&text, 'y');
        const later = try file.read(second);
        defer later.release();
        const earlier = try file.read(first);
        defer earlier.release();
        try std.testing.expectEqual(@as(u64, 11), later.ordinal(0));
        try std.testing.expectEqual(@as(u64, 7), earlier.ordinal(0));
        try std.testing.expectEqual(@as(u8, 'x'), (try earlier.cell(0, 0)).value.string[0]);
        try std.testing.expectEqual(@as(u8, 'x'), (try later.cell(0, 0)).value.string[0]);
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
