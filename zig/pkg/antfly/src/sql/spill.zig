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

pub const Manager = struct {
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
    pub fn close(self: *File) void {
        if (self.closed) return;
        self.file.close(self.manager.io);
        self.manager.live_bytes -= self.size;
        self.manager.files -= 1;
        self.closed = true;
    }
    pub fn append(self: *File, row: Row, link: u64) !u64 {
        try self.manager.check();
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(self.manager.alloc);
        var encoder: Encoder = .{ .a = self.manager.alloc, .bytes = &bytes, .limit = self.manager.max_record_bytes };
        try encoder.word(row.ordinal);
        try encoder.cells(row.values);
        try encoder.cells(row.keys);
        const growth = bytes.items.len + frame_bytes;
        if (growth > self.manager.max_bytes -| self.manager.live_bytes) return error.SqlProgramLimitExceeded;
        var frame: [frame_bytes]u8 = @splat(0);
        std.mem.writeInt(u64, frame[0..8], bytes.items.len, .little);
        std.mem.writeInt(u64, frame[8..16], std.hash.Wyhash.hash(0, bytes.items), .little);
        std.mem.writeInt(u64, frame[16..24], link, .little);
        const offset = self.size;
        try self.file.writePositionalAll(self.manager.io, &frame, offset);
        try self.file.writePositionalAll(self.manager.io, bytes.items, offset + frame_bytes);
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
        if (try self.file.readPositionalAll(self.manager.io, &frame, offset) != frame.len) return error.InvalidSqlSpill;
        const len = std.mem.readInt(u64, frame[0..8], .little);
        if (len > self.manager.max_record_bytes or len > self.size - offset - frame_bytes) return error.InvalidSqlSpill;
        const bytes = try a.alloc(u8, @intCast(len));
        defer a.free(bytes);
        if (try self.file.readPositionalAll(self.manager.io, bytes, offset + frame_bytes) != bytes.len) return error.InvalidSqlSpill;
        if (std.hash.Wyhash.hash(0, bytes) != std.mem.readInt(u64, frame[8..16], .little)) return error.InvalidSqlSpill;
        const link = std.mem.readInt(u64, frame[16..24], .little);
        if (link != none and link >= offset) return error.InvalidSqlSpill;
        var decoder: Decoder = .{ .a = a, .bytes = bytes };
        const ordinal = try decoder.word();
        const values = try decoder.cells();
        const keys = try decoder.cells();
        if (decoder.position != bytes.len or frame[24] > 1) return error.InvalidSqlSpill;
        return .{ .row = .{ .values = values, .keys = keys, .ordinal = ordinal }, .next = link, .matched = frame[24] == 1, .following = offset + frame_bytes + len };
    }
    pub fn match(self: *File, offset: u64) !void {
        try self.manager.check();
        if (offset > self.size or self.size - offset < frame_bytes) return error.InvalidSqlSpill;
        try self.file.writePositionalAll(self.manager.io, &.{1}, offset + 24);
    }
};
const Encoder = struct {
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
            try self.json(value.value, 0);
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
            value.* = .{ .sql_null = flag == 1, .value = try self.json(0) };
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
    output: ?File = null,
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
        if (self.output) |*file| file.close();
    }
    pub fn add(self: *Sort, row: Row) !void {
        if (self.finished or row.keys.len != self.orders.len) return error.InvalidSqlBackendResponse;
        for (row.keys) |key| if (!key.sql_null) {
            _ = try scalar.compare(key.value, key.value);
        };
        try self.manager.check();
        var bytes: usize = @sizeOf(Row);
        for (row.values) |v| bytes +|= try operators.datumBytes(v);
        for (row.keys) |v| bytes +|= try operators.datumBytes(v);
        if (bytes > self.memory_bytes / 4) return error.SqlProgramLimitExceeded;
        if (self.rows.items.len != 0 and (bytes > self.memory_bytes / 4 -| self.estimated)) try self.flush();
        const a = self.arena.allocator();
        const values = try a.alloc(Datum, row.values.len);
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.values, values) |v, *out| out.* = try operators.cloneDatum(a, v);
        for (row.keys, keys) |v, *out| out.* = try operators.cloneDatum(a, v);
        try self.rows.append(self.a, .{ .values = values, .keys = keys, .ordinal = row.ordinal });
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
    fn merge(self: *Sort, left: *File, right: *File) !File {
        var output = try self.manager.create();
        errdefer output.close();
        var la = std.heap.ArenaAllocator.init(self.a);
        defer la.deinit();
        var ra = std.heap.ArenaAllocator.init(self.a);
        defer ra.deinit();
        var lp: u64 = 0;
        var rp: u64 = 0;
        var l: ?Decoded = if (left.size != 0) try left.read(la.allocator(), 0) else null;
        var r: ?Decoded = if (right.size != 0) try right.read(ra.allocator(), 0) else null;
        while (l != null or r != null) {
            try self.manager.check();
            if (r == null or (l != null and (try operators.compareRows(l.?.row, r.?.row, self.orders)) != .gt)) {
                _ = try output.append(l.?.row, none);
                lp = l.?.following;
                _ = la.reset(.free_all);
                l = if (lp < left.size) try left.read(la.allocator(), lp) else null;
            } else {
                _ = try output.append(r.?.row, none);
                rp = r.?.following;
                _ = ra.reset(.free_all);
                r = if (rp < right.size) try right.read(ra.allocator(), rp) else null;
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
        for (&self.runs) |*slot| if (slot.*) |*run| {
            if (self.output) |*old| {
                const combined = try self.merge(old, run);
                old.close();
                run.close();
                self.output = combined;
            } else self.output = run.*;
            slot.* = null;
        };
        self.finished = true;
    }
    pub fn next(self: *Sort, a: Allocator) !?Row {
        try self.finish();
        if (self.output) |*file| {
            if (self.offset == file.size) return null;
            const row = try file.read(a, self.offset);
            self.offset = row.following;
            return row.row;
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
