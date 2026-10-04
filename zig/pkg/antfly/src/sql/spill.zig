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
            if (self.spare.len == 0) self.spare = try self.manager.alloc.alloc(u8, self.write_buffer.len);
            const job = self.write_job orelse blk: {
                const created = try self.manager.alloc.create(WriteJob);
                self.write_job = created;
                break :blk created;
            };
            job.* = .{ .io = self.manager.io, .file = self.file, .buffer = self.write_buffer, .len = self.write_len, .offset = self.write_start };
            const future = @import("parallel_scheduler.zig").global().submit(self.manager.io, job.buffer.len, WriteJob.write, .{job}) orelse {
                try WriteJob.write(job);
                self.manager.write_calls += 1;
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
        self.manager.write_calls += 1;
        self.write_len = 0;
    }
    pub fn flush(self: *File) !void {
        try self.submit();
        try self.awaitWrite();
    }
    fn bufferedWrite(self: *File, bytes: []const u8, offset: u64) !void {
        self.read_start = none;
        if (self.write_buffer.len == 0) self.write_buffer = try self.manager.alloc.alloc(u8, @max(1, self.buffer_bytes orelse self.manager.buffer_bytes));
        if (self.write_len != 0 and offset != self.write_start + self.write_len) try self.submit();
        if (bytes.len > self.write_buffer.len) {
            try self.flush();
            try self.file.writePositionalAll(self.manager.io, bytes, offset);
            self.manager.write_calls += 1;
            return;
        }
        if (bytes.len > self.write_buffer.len - self.write_len) try self.submit();
        if (self.write_len == 0) self.write_start = offset;
        @memcpy(self.write_buffer[self.write_len..][0..bytes.len], bytes);
        self.write_len += bytes.len;
    }
    fn bufferedRead(self: *File, offset: u64, bytes: []u8) !void {
        try self.flush();
        if (self.read_buffer.len == 0) self.read_buffer = try self.manager.alloc.alloc(u8, @max(1, self.buffer_bytes orelse self.manager.buffer_bytes));
        if (bytes.len > self.read_buffer.len) {
            const count = try self.file.readPositionalAll(self.manager.io, bytes, offset);
            self.manager.read_calls += 1;
            self.manager.read_bytes += count;
            if (count != bytes.len) return error.InvalidSqlSpill;
            return;
        }
        if (self.read_start == none or offset < self.read_start or offset - self.read_start > self.read_len or bytes.len > self.read_len -| (offset - self.read_start)) {
            self.read_start = offset;
            self.read_len = @intCast(@min(self.read_buffer.len, self.size - offset));
            const count = try self.file.readPositionalAll(self.manager.io, self.read_buffer[0..self.read_len], offset);
            self.manager.read_calls += 1;
            self.manager.read_bytes += count;
            if (count != self.read_len) return error.InvalidSqlSpill;
        }
        @memcpy(bytes, self.read_buffer[@intCast(offset - self.read_start)..][0..bytes.len]);
    }
    pub fn close(self: *File) void {
        if (self.closed) return;
        if (self.pending) |job| {
            job.future.?.cancel(self.manager.io) catch {};
            self.manager.alloc.free(job.buffer);
            self.pending = null;
        }
        if (self.write_job) |job| self.manager.alloc.destroy(job);
        self.write_job = null;
        self.file.close(self.manager.io);
        self.manager.alloc.free(self.spare);
        self.manager.alloc.free(self.write_buffer);
        self.manager.alloc.free(self.read_buffer);
        self.manager.live_bytes -= self.size;
        self.manager.files -= 1;
        self.closed = true;
    }
    pub fn append(self: *File, row: Row, link: u64) !u64 {
        try self.manager.check();
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(self.manager.alloc);
        var encoder: Encoder = .{ .manager = self.manager, .a = self.manager.alloc, .bytes = &bytes, .limit = self.manager.max_record_bytes };
        try encoder.word(row.ordinal);
        try encoder.cells(row.values);
        try encoder.cells(row.keys);
        var compressed: ?[]u8 = null;
        defer if (compressed) |value| self.manager.alloc.free(value);
        // Avoid codec work on short or apparently incompressible records.
        // Compression is an existing Snappy block format, never a new codec.
        if (self.manager.compression == .snappy and bytes.items.len >= 4096) {
            const sample = bytes.items[0..@min(bytes.items.len, 1024)];
            var repeated: usize = 0;
            for (sample[1..], sample[0 .. sample.len - 1]) |x, y| repeated += @intFromBool(x == y);
            if (repeated > sample.len / 4) compressed = try snappy.encode(self.manager.alloc, bytes.items);
        }
        const compressed_record = compressed != null and compressed.?.len + 32 < bytes.items.len;
        const stored = if (compressed_record) compressed.? else bytes.items;
        const growth = stored.len + frame_bytes;
        if (growth > self.manager.max_bytes -| self.manager.live_bytes) return error.SqlProgramLimitExceeded;
        var frame: [frame_bytes]u8 = @splat(0);
        std.mem.writeInt(u64, frame[0..8], stored.len, .little);
        frame[24] = if (compressed_record) 2 else 0;
        std.mem.writeInt(u64, frame[8..16], std.hash.Wyhash.hash(0, bytes.items), .little);
        std.mem.writeInt(u64, frame[16..24], link, .little);
        const offset = self.size;
        try self.bufferedWrite(&frame, offset);
        try self.bufferedWrite(stored, offset + frame_bytes);
        self.manager.compressed_records += @intFromBool(compressed_record);
        self.size += growth;
        self.manager.live_bytes += growth;
        self.manager.peak_bytes = @max(self.manager.peak_bytes, self.manager.live_bytes);
        self.manager.written_bytes += growth;
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
        if (growth > self.manager.max_bytes -| self.manager.live_bytes) return error.SqlProgramLimitExceeded;
        try self.bufferedWrite(bytes, offset);
        self.size = @max(self.size, end);
        self.manager.live_bytes += growth;
        self.manager.peak_bytes = @max(self.manager.peak_bytes, self.manager.live_bytes);
        self.manager.written_bytes += bytes.len;
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
                const id = for (self.manager.patterns.items, 0..) |item, id| {
                    if (item == pattern) break id;
                } else return error.InvalidSqlSpill;
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
                if (id >= self.manager.patterns.items.len or flag != 0) return error.InvalidSqlSpill;
                value.* = .{ .sql_null = false, .patterns = self.manager.patterns.items[@intCast(id)] };
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

pub const Sort = struct {
    manager: *Manager,
    a: Allocator,
    orders: []const operators.Order,
    memory_bytes: usize,
    arena: std.heap.ArenaAllocator,
    rows: std.ArrayList(Row) = .empty,
    estimated: usize = 0,
    runs: [32]?File = @splat(null),
    outputs: [8]?File = @splat(null),
    heads: [8]?Decoded = @splat(null),
    head_arenas: [8]std.heap.ArenaAllocator = undefined,
    output_count: usize = 0,
    max_row_bytes: usize = 0,
    offset: u64 = 0,
    finished: bool = false,
    total: usize = 0,
    pub fn init(a: Allocator, manager: *Manager, orders: []const operators.Order, memory_bytes: usize) Sort {
        return .{ .manager = manager, .a = a, .orders = orders, .memory_bytes = memory_bytes, .arena = std.heap.ArenaAllocator.init(a) };
    }
    pub fn deinit(self: *Sort) void {
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
        if (self.rows.items.len != 0 and (bytes > self.memory_bytes / 4 -| self.estimated)) try self.flush();
        const a = self.arena.allocator();
        const values = try a.alloc(Datum, row.values.len);
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.values, values) |v, *out| out.* = try operators.cloneDatum(a, v);
        for (row.keys, keys) |v, *out| out.* = try operators.cloneDatum(a, v);
        try self.rows.append(self.a, .{ .values = values, .keys = keys, .ordinal = row.ordinal, .normalized = row.normalized });
        self.estimated += bytes;
        self.total += 1;
    }
    fn sortRows(self: *Sort) !void {
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
        if (self.rows.items.len == 0) return;
        try self.sortRows();
        var run = try self.manager.create();
        errdefer run.close();
        for (self.rows.items) |row| _ = try run.append(row, none);
        _ = self.arena.reset(.free_all);
        self.rows.clearAndFree(self.a);
        self.estimated = 0;
        for (&self.runs) |*slot| {
            if (slot.*) |*old| {
                const combined = try self.merge(old, &run);
                old.close();
                run.close();
                slot.* = null;
                run = combined;
            } else {
                slot.* = run;
                return;
            }
        }
        return error.SqlProgramLimitExceeded;
    }
    fn readRun(self: *Sort, file: *File, a: Allocator, offset: u64) !Decoded {
        var decoded = try file.read(a, offset);
        decoded.row.normalized = @import("sort_key.zig").encode(decoded.row.keys, self.orders);
        return decoded;
    }
    fn merge(self: *Sort, left: *File, right: *File) !File {
        var output = try self.manager.create();
        errdefer output.close();
        var la = std.heap.ArenaAllocator.init(self.a);
        defer la.deinit();
        var ra = std.heap.ArenaAllocator.init(self.a);
        defer ra.deinit();
        var lp: u64 = 0;
        var rp: u64 = 0;
        var l: ?Decoded = if (left.size != 0) try self.readRun(left, la.allocator(), 0) else null;
        var r: ?Decoded = if (right.size != 0) try self.readRun(right, ra.allocator(), 0) else null;
        while (l != null or r != null) {
            try self.manager.check();
            if (r == null or (l != null and (try operators.compareRows(l.?.row, r.?.row, self.orders)) != .gt)) {
                _ = try output.append(l.?.row, none);
                lp = l.?.following;
                _ = la.reset(.retain_capacity);
                l = if (lp < left.size) try self.readRun(left, la.allocator(), lp) else null;
            } else {
                _ = try output.append(r.?.row, none);
                rp = r.?.following;
                _ = ra.reset(.retain_capacity);
                r = if (rp < right.size) try self.readRun(right, ra.allocator(), rp) else null;
            }
        }
        self.manager.merges += 1;
        return output;
    }
    pub fn finish(self: *Sort) !void {
        if (self.finished) return;
        const has_runs = for (self.runs) |run| {
            if (run != null) break true;
        } else false;
        if (!has_runs) {
            try self.sortRows();
            self.finished = true;
            return;
        }
        try self.flush();
        // Bound decoded heads and I/O buffers; stream the final merge instead
        // of writing and rereading another complete sorted run.
        const head_bytes = self.max_row_bytes *| 4 +| self.manager.buffer_bytes *| 2 +| 512;
        const fan_in = @min(self.outputs.len, @max(@as(usize, 2), self.memory_bytes / @max(1, head_bytes)));
        errdefer {
            for (self.outputs[0..self.output_count]) |*file| if (file.*) |*active| active.close();
            self.output_count = 0;
        }
        for (&self.runs) |*slot| if (slot.*) |*run| {
            if (self.output_count == fan_in) {
                const combined = try self.merge(&self.outputs[0].?, &self.outputs[1].?);
                self.outputs[0].?.close();
                self.outputs[1].?.close();
                self.outputs[0] = combined;
                self.output_count -= 1;
                self.outputs[1] = self.outputs[self.output_count];
                self.outputs[self.output_count] = null;
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
        try self.finish();
        if (self.output_count != 0) {
            var selected: ?usize = null;
            for (self.heads[0..self.output_count], 0..) |head, index| if (head) |value| {
                if (selected == null or (try operators.compareRows(value.row, self.heads[selected.?].?.row, self.orders)) == .lt) selected = index;
            };
            const index = selected orelse return null;
            const head = self.heads[index].?;
            const values = try a.alloc(Datum, head.row.values.len);
            const keys = try a.alloc(Datum, head.row.keys.len);
            for (head.row.values, values) |value, *out| out.* = try operators.cloneDatum(a, value);
            for (head.row.keys, keys) |value, *out| out.* = try operators.cloneDatum(a, value);
            _ = self.head_arenas[index].reset(.retain_capacity);
            self.heads[index] = if (head.following < self.outputs[index].?.size) try self.readRun(&self.outputs[index].?, self.head_arenas[index].allocator(), head.following) else null;
            return .{ .values = values, .keys = keys, .ordinal = head.row.ordinal };
        }
        if (self.offset < self.rows.items.len) {
            const row = self.rows.items[@intCast(self.offset)];
            self.offset += 1;
            const values = try a.alloc(Datum, row.values.len);
            const keys = try a.alloc(Datum, row.keys.len);
            for (row.values, values) |v, *out| out.* = try operators.cloneDatum(a, v);
            for (row.keys, keys) |v, *out| out.* = try operators.cloneDatum(a, v);
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
    const text = [_]u8{'x'} ** 8192;
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
    const bytes = [_]u8{'x'} ** 4096;
    try file.writeRaw(0, &bytes);
    try file.writeRaw(4096, &bytes);
    canceled = true;
    try std.testing.expectError(error.QueryCanceled, file.flush());
    file.close();
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
}
