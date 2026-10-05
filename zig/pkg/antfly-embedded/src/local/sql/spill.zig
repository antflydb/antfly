// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Statement-owned temporary storage and bounded external merge runs. Files
//! are private, quota-controlled, snapshot-local and deleted on every unwind.
const std = @import("std");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const Row = operators.Row;
const Allocator = std.mem.Allocator;
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
        self.lock();
        defer self.mutex.unlock();
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
        return .{ .manager = self, .file = file, .id = self.sequence };
    }
    pub fn deinit(self: *Manager) void {
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
        if (self.manager.async_writes and self.write_buffer.len >= 4096 and self.write_len >= self.write_buffer.len / 2) {
            const job = self.prepareAsync() orelse {
                try self.file.writePositionalAll(self.manager.io, self.write_buffer[0..self.write_len], self.write_start);
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
            try self.file.writePositionalAll(self.manager.io, self.write_buffer[0..self.write_len], self.write_start);
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
        self.read_start = none;
        if (self.write_buffer.len == 0) self.write_buffer = try self.manager.allocator().alloc(u8, @max(1, self.buffer_bytes orelse self.manager.buffer_bytes));
        if (self.write_len != 0 and offset != self.write_start + self.write_len) try self.submit();
        if (bytes.len > self.write_buffer.len) {
            try self.flush();
            try self.file.writePositionalAll(self.manager.io, bytes, offset);
            self.manager.increment("write_calls", 1);
            return;
        }
        if (bytes.len > self.write_buffer.len - self.write_len) try self.submit();
        if (self.write_len == 0) self.write_start = offset;
        @memcpy(self.write_buffer[self.write_len..][0..bytes.len], bytes);
        self.write_len += bytes.len;
    }
    fn bufferedRead(self: *File, offset: u64, bytes: []u8) !void {
        try self.flush();
        if (self.read_buffer.len == 0) self.read_buffer = try self.manager.allocator().alloc(u8, @max(1, self.buffer_bytes orelse self.manager.buffer_bytes));
        if (bytes.len > self.read_buffer.len) {
            const count = try self.file.readPositionalAll(self.manager.io, bytes, offset);
            self.manager.increment("read_calls", 1);
            self.manager.increment("read_bytes", count);
            if (count != bytes.len) return error.InvalidSqlSpill;
            return;
        }
        if (self.read_start == none or offset < self.read_start or offset - self.read_start > self.read_len or bytes.len > self.read_len -| (offset - self.read_start)) {
            self.read_start = offset;
            self.read_len = @intCast(@min(self.read_buffer.len, self.size - offset));
            const count = try self.file.readPositionalAll(self.manager.io, self.read_buffer[0..self.read_len], offset);
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
        self.file.close(self.manager.io);
        self.manager.allocator().free(self.spare);
        self.manager.allocator().free(self.write_buffer);
        self.manager.allocator().free(self.read_buffer);
        self.manager.closeFile(self.size);
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
        try self.manager.reserve(growth);
        errdefer self.manager.releaseBytes(growth);
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
        try self.manager.reserve(growth);
        errdefer self.manager.releaseBytes(growth);
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
    manager: *Manager,
    a: Allocator,
    bytes: *std.ArrayList(u8),
    limit: usize,
    fn append(self: *Encoder, bytes: []const u8) !void {
        if (bytes.len > self.limit -| self.bytes.items.len) return error.SqlProgramLimitExceeded;
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
    fn cells(self: *Encoder, values: []const Datum) !void {
        try self.word(values.len);
        for (values) |value| {
            try self.append(&.{@intFromBool(value.sql_null)});
            if (value.patterns) |pattern| {
                const id = try self.manager.patternId(pattern);
                try self.append(&.{8});
                try self.word(id);
            } else try self.json(value.value, 0);
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
const Decoder = struct {
    manager: *Manager,
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
    fn cells(self: *Decoder) ![]Datum {
        const values = try self.a.alloc(Datum, try self.count());
        for (values) |*value| {
            const flag = try self.byte();
            if (flag > 1) return error.InvalidSqlSpill;
            if (self.position < self.bytes.len and self.bytes[self.position] == 8) {
                self.position += 1;
                const id = try self.word();
                if (flag != 0) return error.InvalidSqlSpill;
                value.* = .{ .sql_null = false, .patterns = try self.manager.patternAt(id) };
            } else value.* = .{ .sql_null = flag == 1, .value = try self.json(0) };
        }
        return values;
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
                    if (object.contains(key)) return error.InvalidSqlSpill;
                    try object.put(self.a, key, try self.json(depth + 1));
                }
                break :blk .{ .object = object };
            },
            else => error.InvalidSqlSpill,
        };
    }
};

/// Sequential native runs use typed column blocks when multiple rows fit;
/// wide rows retain the compact record framing without extra staging copies.
/// Random-access hash chains
/// keep File's independently framed records. Logical offsets here are row
/// ordinals; readers consume blocks in order and may restart at ordinal zero.
pub const Sequential = struct {
    file: File,
    size: u64 = 0,
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
        self.pending.deinit(self.file.manager.allocator());
        self.write_arena.deinit();
        self.read_arena.deinit();
        self.file.close();
    }
    pub fn append(self: *Sequential, row: Row, link: u64) !u64 {
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
    fn tag(value: Datum) u8 {
        if (value.patterns != null) return 255;
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
    fn encodeColumns(encoder: *Encoder, rows: []const Row, keys: bool) !void {
        const width = if (keys) rows[0].keys.len else rows[0].values.len;
        for (0..width) |column| {
            var kind: ?u8 = null;
            for (rows) |row| {
                const value = (if (keys) row.keys else row.values)[column];
                if (value.sql_null or (value.value == .null and value.patterns == null)) continue;
                const actual = tag(value);
                kind = if (kind == null or kind.? == actual) actual else 255;
            }
            const type_ = kind orelse 0;
            try encoder.append(&.{type_});
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
            for (rows) |row| {
                const value = (if (keys) row.keys else row.values)[column];
                if (value.sql_null or value.value == .null) continue;
                switch (type_) {
                    1 => try encoder.append(&.{@intFromBool(value.value.bool)}),
                    2 => try encoder.word(@bitCast(value.value.integer)),
                    3 => try encoder.word(@bitCast(value.value.float)),
                    4 => try encoder.text(value.value.number_string),
                    5 => try encoder.text(value.value.string),
                    else => return error.InvalidSqlSpill,
                }
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
        const compressed = if (manager.compression == .snappy and bytes.items.len >= 1024) try snappy.encode(manager.allocator(), bytes.items) else null;
        defer if (compressed) |value| manager.allocator().free(value);
        const use_compressed = compressed != null and compressed.?.len + 32 < bytes.items.len;
        const stored = if (use_compressed) compressed.? else bytes.items;
        var header: [17]u8 = undefined;
        std.mem.writeInt(u64, header[0..8], stored.len, .little);
        std.mem.writeInt(u64, header[8..16], std.hash.Wyhash.hash(0, bytes.items), .little);
        header[16] = @intFromBool(use_compressed);
        self.file.buffer_bytes = self.buffer_bytes;
        const offset = self.file.size;
        try self.file.writeRaw(offset, &header);
        try self.file.writeRaw(offset + header.len, stored);
        manager.compressed_records += @intFromBool(use_compressed);
        self.pending.clearRetainingCapacity();
        _ = self.write_arena.reset(.free_all);
        self.pending_bytes = 0;
    }
    pub fn seal(self: *Sequential) !void {
        try self.flush();
        self.pending.clearAndFree(self.file.manager.allocator());
        try self.file.seal();
    }
    fn decodeColumns(decoder: *Decoder, rows: []Row, keys: bool) !void {
        const width = if (keys) rows[0].keys.len else rows[0].values.len;
        for (0..width) |column| {
            const type_ = try decoder.byte();
            if (type_ == 255) {
                for (rows) |row| {
                    const values = try decoder.cells();
                    if (values.len != 1) return error.InvalidSqlSpill;
                    @constCast(if (keys) row.keys else row.values)[column] = values[0];
                }
                continue;
            }
            if (type_ > 5) return error.InvalidSqlSpill;
            const len = (rows.len + 3) / 4;
            if (len > decoder.bytes.len - decoder.position) return error.InvalidSqlSpill;
            const flags = decoder.bytes[decoder.position..][0..len];
            decoder.position += len;
            for (rows, 0..) |row, index| {
                const flag = (flags[index / 4] >> @as(u3, @intCast((index % 4) * 2))) & 3;
                if (flag == 3) return error.InvalidSqlSpill;
                const value = &@constCast(if (keys) row.keys else row.values)[column];
                value.* = .{ .sql_null = flag == 1 };
                if (flag != 0) continue;
                value.value = switch (type_) {
                    0 => .null,
                    1 => blk: {
                        const boolean = try decoder.byte();
                        if (boolean > 1) return error.InvalidSqlSpill;
                        break :blk .{ .bool = boolean == 1 };
                    },
                    2 => .{ .integer = @bitCast(try decoder.word()) },
                    3 => .{ .float = @bitCast(try decoder.word()) },
                    4 => .{ .number_string = try decoder.text() },
                    5 => .{ .string = try decoder.text() },
                    else => unreachable,
                };
            }
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
        if (offset >= self.size) return error.InvalidSqlSpill;
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
            try self.file.readRaw(self.read_offset, &header);
            // File records have the sentinel link's 0xff at this byte;
            // typed blocks use only 0/1. Both retain checksum validation.
            if (header[16] == 255) {
                var record = try self.file.read(owned, self.read_offset);
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
            if (header[16] > 1 or len > self.file.manager.max_record_bytes or len > self.file.size -| (self.read_offset + header.len)) return error.InvalidSqlSpill;
            const encoded = try owned.alloc(u8, @intCast(len));
            try self.file.readRaw(self.read_offset + header.len, encoded);
            const payload = if (header[16] != 0) blk: {
                if (try snappy.decodedLen(encoded) > self.file.manager.max_record_bytes) return error.InvalidSqlSpill;
                break :blk try snappy.decode(owned, encoded);
            } else encoded;
            if (std.hash.Wyhash.hash(0, payload) != std.mem.readInt(u64, header[8..16], .little)) return error.InvalidSqlSpill;
            var decoder: Decoder = .{ .manager = self.file.manager, .a = owned, .bytes = payload };
            const count = try decoder.count();
            const width = try decoder.count();
            const key_width = try decoder.count();
            if (count == 0 or count > 256 or count > self.size - offset or width > 1024 or key_width > 256) return error.InvalidSqlSpill;
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

pub const Sort = struct {
    manager: *Manager,
    a: Allocator,
    orders: []const operators.Order,
    memory_bytes: usize,
    merge_fan_in: usize = 8,
    arena: std.heap.ArenaAllocator,
    rows: std.ArrayList(Row) = .empty,
    estimated: usize = 0,
    runs: [32]?Sequential = @splat(null),
    run_levels: [32]u8 = @splat(0),
    outputs: [8]?Sequential = @splat(null),
    heads: [8]?Decoded = @splat(null),
    head_arenas: [8]std.heap.ArenaAllocator = undefined,
    output_count: usize = 0,
    max_row_bytes: usize = 0,
    offset: u64 = 0,
    finished: bool = false,
    total: usize = 0,
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
            var file = try Sequential.init(self.sort.manager, self.sort.blockBytes());
            errdefer file.close();
            for (self.sort.rows.items) |row| {
                try self.sort.manager.check();
                _ = try file.append(row, none);
            }
            try file.seal();
            return file;
        }
        fn destroy(self: *RunJob) void {
            const a = self.sort.a;
            self.sort.arena.deinit();
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
        return .{ .manager = manager, .a = backing, .orders = orders, .memory_bytes = memory_bytes, .arena = std.heap.ArenaAllocator.init(backing) };
    }
    pub fn deinit(self: *Sort) void {
        if (self.pending_run) |job| {
            if (job.task.?.cancel(self.manager.io)) |value| {
                var file = value;
                file.close();
            } else |_| {}
            job.destroy();
            self.pending_run = null;
        }
        self.arena.deinit();
        self.rows.deinit(self.a);
        for (&self.runs) |*run| if (run.*) |*file| file.close();
        for (self.outputs[0..self.output_count], self.head_arenas[0..self.output_count]) |*file, *arena| {
            if (file.*) |*open_file| open_file.close();
            arena.deinit();
        }
    }
    pub fn add(self: *Sort, input: Row) !void {
        var row = input;
        row.normalized = @import("sort_key.zig").encode(row.keys, self.orders);
        if (self.finished or row.keys.len != self.orders.len) return error.InvalidSqlBackendResponse;
        for (row.keys) |key| if (!key.sql_null) {
            _ = try scalar.compare(key.value, key.value);
        };
        try self.manager.check();
        var bytes: usize = @sizeOf(Row);
        for (row.values) |v| bytes +|= try operators.datumBytes(v);
        for (row.keys) |v| bytes +|= try operators.datumBytes(v);
        if (bytes > self.memory_bytes / 3) return error.SqlProgramLimitExceeded;
        self.max_row_bytes = @max(self.max_row_bytes, bytes);
        if (self.rows.items.len != 0 and (bytes > self.memory_bytes / (if (self.parallel_runs and self.memory_bytes >= 128 * 1024) @as(usize, 8) else 4) -| self.estimated)) try self.flush();
        const a = self.arena.allocator();
        const values = try a.alloc(Datum, row.values.len);
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.values, values) |v, *out| out.* = try operators.cloneDatum(a, v);
        for (row.keys, keys) |v, *out| out.* = try operators.cloneDatum(a, v);
        try self.rows.append(self.a, .{ .values = values, .keys = keys, .ordinal = row.ordinal, .normalized = row.normalized });
        self.estimated += bytes;
        self.total += 1;
    }
    fn radixRows(self: *Sort) !bool {
        if (self.rows.items.len < 1024) return false;
        const first = self.rows.items[0].normalized orelse return false;
        if (!first.complete) return false;
        for (self.rows.items) |row| {
            const key = row.normalized orelse return false;
            if (!key.complete or key.types != first.types or key.len != first.len) return false;
        }
        const scratch = try self.a.alloc(Row, self.rows.items.len);
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
            std.mem.swap([]Row, &source, &target);
        }
        if (source.ptr != self.rows.items.ptr) @memcpy(self.rows.items, source);
        self.radix_runs += 1;
        return true;
    }
    fn radixByte(row: Row, pass: usize, length: usize) u8 {
        return if (pass < length) row.normalized.?.bytes[pass] else @truncate(row.ordinal >> @as(u6, @intCast((7 - (pass - length)) * 8)));
    }
    fn sortRows(self: *Sort) !void {
        if (try self.radixRows()) return;
        const Comparator = struct {
            orders: []const operators.Order,
            err: ?anyerror = null,
            fn less(comparator: *@This(), left: Row, right: Row) bool {
                return (operators.compareRows(left, right, comparator.orders) catch |err| {
                    comparator.err = err;
                    return left.ordinal < right.ordinal;
                }) == .lt;
            }
        };
        var comparator: Comparator = .{ .orders = self.orders };
        std.sort.pdq(Row, self.rows.items, &comparator, Comparator.less);
        if (comparator.err) |err| return err;
    }
    fn flush(self: *Sort) !void {
        try self.collectRun();
        if (self.rows.items.len == 0) return;
        if (self.parallel_runs and self.memory_bytes >= 128 * 1024) {
            const job = try self.a.create(RunJob);
            job.* = .{ .sort = .{ .manager = self.manager, .a = self.a, .orders = self.orders, .memory_bytes = self.memory_bytes / 2, .arena = self.arena, .rows = self.rows, .parallel_runs = false } };
            if (@import("parallel_scheduler.zig").global().submit(self.manager.io, self.estimated +| self.blockBytes() * 4, RunJob.run, .{job})) |task| {
                job.task = task;
                self.pending_run = job;
                self.parallel_runs_started += 1;
                self.arena = .init(self.a);
                self.rows = .empty;
                self.estimated = 0;
                return;
            }
            // Admission declined: original buffers still belong to this sort.
            self.a.destroy(job);
        }
        try self.sortRows();
        var run = try Sequential.init(self.manager, self.blockBytes());
        var transferred = false;
        errdefer if (!transferred) run.close();
        for (self.rows.items) |row| _ = try run.append(row, none);
        try run.seal();
        _ = self.arena.reset(.free_all);
        self.rows.clearAndFree(self.a);
        self.estimated = 0;
        transferred = true;
        return self.admitRun(run);
    }
    fn admitRun(self: *Sort, input: Sequential) !void {
        var run = input;
        errdefer run.close();
        var level: u8 = 0;
        const fan_in = self.fanIn();
        while (true) {
            var indices: [8]usize = undefined;
            var count: usize = 0;
            var empty: ?usize = null;
            for (self.runs, self.run_levels, 0..) |slot, candidate_level, index| {
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
                for (&order, 0..) |*index, i| index.* = i;
                std.mem.sort(usize, &order, self, struct {
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
        return @min(@min(self.outputs.len, @max(@as(usize, 2), self.merge_fan_in)), @max(@as(usize, 2), self.memory_bytes / @max(1, head_bytes)));
    }
    fn readRun(self: *Sort, file: *Sequential, a: Allocator, offset: u64) !Decoded {
        if (offset == 0) try file.seal();
        _ = a;
        var decoded = try file.readBorrowed(offset);
        decoded.row.normalized = @import("sort_key.zig").encode(decoded.row.keys, self.orders);
        return decoded;
    }
    fn mergeMany(self: *Sort, inputs: []const *Sequential) !Sequential {
        std.debug.assert(inputs.len >= 2 and inputs.len <= self.fanIn());
        var output = try Sequential.init(self.manager, self.blockBytes());
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
                if (selected == null or (try operators.compareRows(value.row, heads[selected.?].?.row, self.orders)) == .lt) selected = index;
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
                local.max_row_bytes = self.max_row_bytes;
                local.merge_fan_in = self.merge_fan_in;
                local.parallel_runs = false;
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
        if (self.finished) return;
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
        for (self.outputs[0..self.output_count], self.head_arenas[0..self.output_count], self.heads[0..self.output_count]) |*file, *arena, *head| {
            head.* = if (file.*.?.size != 0) try self.readRun(&file.*.?, arena.allocator(), 0) else null;
        }
        self.finished = true;
    }
    pub fn next(self: *Sort, a: Allocator) !?Row {
        return self.nextImpl(a, true);
    }
    /// Final delivery discards sort keys at the ownership boundary.
    pub fn nextValues(self: *Sort, a: Allocator) !?Row {
        return self.nextImpl(a, false);
    }
    fn nextImpl(self: *Sort, a: Allocator, retain_keys: bool) !?Row {
        try self.finish();
        if (self.output_count != 0) {
            var selected: ?usize = null;
            for (self.heads[0..self.output_count], 0..) |head, index| if (head) |value| {
                if (selected == null or (try operators.compareRows(value.row, self.heads[selected.?].?.row, self.orders)) == .lt) selected = index;
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
            const row = self.rows.items[@intCast(self.offset)];
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
