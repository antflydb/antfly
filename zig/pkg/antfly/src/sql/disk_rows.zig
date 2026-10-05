// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Indexed statement-local rows and integer arrays. Window partitions use
//! bounded row caches; payloads and offset/state directories live on disk.
const std = @import("std");
const spill = @import("spill.zig");
const operators = @import("operators.zig");
const Datum = @import("scalar.zig").Datum;
const A = std.mem.Allocator;
pub const Integers = struct {
    file: spill.File,
    len: usize = 0,
    buffer: []u8,
    block: ?usize = null,
    valid: usize = 0,
    dirty: bool = false,
    pub fn init(manager: *spill.Manager) !Integers {
        var file = try manager.create();
        errdefer file.close();
        return .{ .file = file, .buffer = try manager.alloc.alloc(u8, @min(4096, @max(128, manager.max_record_bytes / 16))) };
    }
    pub fn deinit(self: *Integers) void {
        self.file.manager.alloc.free(self.buffer);
        self.file.close();
    }
    fn load(self: *Integers, offset: usize) !usize {
        try self.file.manager.check();
        const block = offset / self.buffer.len * self.buffer.len;
        if (self.block != block) {
            if (self.dirty) try self.file.writeRaw(self.block.?, self.buffer[0..self.valid]);
            self.dirty = false;
            self.block = block;
            self.valid = @intCast(@min(self.buffer.len, self.file.size -| block));
            if (self.valid != 0) try self.file.readRaw(block, self.buffer[0..self.valid]);
        }
        return offset - block;
    }
    pub fn append(self: *Integers, value: usize) !void {
        const position = try self.load(self.len * 8);
        std.mem.writeInt(u64, self.buffer[position..][0..8], value, .little);
        self.valid = @max(self.valid, position + 8);
        self.dirty = true;
        self.len += 1;
    }
    pub fn at(self: *Integers, index: usize) !usize {
        if (index >= self.len) return error.InvalidSqlSpill;
        const position = try self.load(index * 8);
        if (position + 8 > self.valid) return error.InvalidSqlSpill;
        return std.math.cast(usize, std.mem.readInt(u64, self.buffer[position..][0..8], .little)) orelse error.InvalidSqlSpill;
    }
    pub fn set(self: *Integers, index: usize, value: usize) !void {
        if (index >= self.len) return error.InvalidSqlSpill;
        const position = try self.load(index * 8);
        std.mem.writeInt(u64, self.buffer[position..][0..8], value, .little);
        self.dirty = true;
    }
};
pub const Identity = struct {
    len: usize,
    pub fn at(self: Identity, index: usize) !usize {
        if (index >= self.len) return error.InvalidSqlSpill;
        return index;
    }
};
pub const Rows = struct {
    a: A,
    file: spill.File,
    offsets: Integers,
    len: usize = 0,
    width: usize,
    cache: [4]Entry,
    tick: u64 = 0,
    updates: ?Updates = null,
    columnar: ?@import("column_spill.zig").Store = null,
    cell_arena: std.heap.ArenaAllocator,
    const Updates = struct { file: spill.File, offsets: Integers, first_column: usize };
    const Entry = struct { arena: std.heap.ArenaAllocator, index: ?usize = null, row: operators.Row = undefined, touched: u64 = 0 };
    pub fn init(a: A, manager: *spill.Manager, width: usize) !Rows {
        var file = try manager.create();
        errdefer file.close();
        const offsets = try Integers.init(manager);
        var result = Rows{ .a = a, .file = file, .offsets = offsets, .width = width, .cell_arena = .init(a), .cache = undefined };
        for (&result.cache) |*entry| entry.* = .{ .arena = std.heap.ArenaAllocator.init(a) };
        return result;
    }
    pub fn deinit(self: *Rows) void {
        for (&self.cache) |*entry| entry.arena.deinit();
        self.cell_arena.deinit();
        if (self.columnar) |*store| store.deinit();
        if (self.updates) |*updates| {
            updates.offsets.deinit();
            updates.file.close();
        }
        self.offsets.deinit();
        self.file.close();
    }
    pub fn enableColumns(self: *Rows) !void {
        if (self.len != 0 or self.columnar != null) return error.InvalidSqlSpill;
        self.columnar = try @import("column_spill.zig").Store.init(self.a, self.file.manager, self.width);
    }
    /// Window outputs use cell records instead of rewriting the input payload
    /// for every function. The fixed offset directory shares the spill quota.
    pub fn enableColumnUpdates(self: *Rows, first_column: usize) !void {
        if (first_column > self.width or self.updates != null) return error.InvalidSqlSpill;
        var file = try self.file.manager.create();
        errdefer file.close();
        var offsets = try Integers.init(self.file.manager);
        errdefer offsets.deinit();
        const count = std.math.mul(usize, self.len, self.width - first_column) catch return error.SqlProgramLimitExceeded;
        if (@as(u128, count) * 8 > self.file.manager.max_bytes -| self.file.manager.live_bytes) return error.SqlProgramLimitExceeded;
        for (0..count) |_| try offsets.append(std.math.maxInt(usize));
        self.updates = .{ .file = file, .offsets = offsets, .first_column = first_column };
    }
    pub fn append(self: *Rows, input: operators.Row) !void {
        if (self.updates != null) return error.InvalidSqlSpill;
        if (input.values.len != self.width) return error.InvalidSqlSpill;
        if (self.columnar) |*store| {
            try store.append(input);
            self.len += 1;
            return;
        }
        const offset = try self.file.append(.{ .values = input.values, .keys = &.{}, .ordinal = input.ordinal }, spill.none);
        try self.offsets.append(@intCast(offset));
        self.len += 1;
    }
    pub fn row(self: *Rows, index: usize) !operators.Row {
        if (index >= self.len) return error.InvalidSqlSpill;
        self.tick +%= 1;
        var oldest: *Entry = &self.cache[0];
        for (&self.cache) |*entry| {
            if (entry.index == index) {
                entry.touched = self.tick;
                return entry.row;
            }
            if (entry.index == null or entry.touched < oldest.touched) oldest = entry;
        }
        _ = oldest.arena.reset(.free_all);
        oldest.index = null;
        const decoded: spill.Decoded = if (self.columnar) |*store| blk: {
            const values = try oldest.arena.allocator().alloc(Datum, self.width);
            for (values, 0..) |*value, column| value.* = try operators.cloneDatum(oldest.arena.allocator(), try store.cell(index, column));
            break :blk .{ .row = .{ .values = values, .keys = &.{}, .ordinal = try store.ordinals.at(index) }, .next = spill.none, .matched = false, .following = 0 };
        } else try self.file.read(oldest.arena.allocator(), try self.offsets.at(index));
        if (decoded.row.values.len != self.width) return error.InvalidSqlSpill;
        oldest.row = decoded.row;
        if (self.updates) |*updates| {
            const values = try oldest.arena.allocator().dupe(Datum, decoded.row.values);
            for (updates.first_column..self.width) |column| {
                const offset = try updates.offsets.at(index * (self.width - updates.first_column) + column - updates.first_column);
                if (offset == std.math.maxInt(usize)) continue;
                const cell_record = try updates.file.read(oldest.arena.allocator(), offset);
                if (cell_record.row.values.len != 1 or cell_record.row.ordinal != index) return error.InvalidSqlSpill;
                values[column] = cell_record.row.values[0];
            }
            oldest.row.values = values;
        }
        oldest.index = index;
        oldest.touched = self.tick;
        return oldest.row;
    }
    pub fn cell(self: *Rows, index: usize, column: usize) !Datum {
        if (column >= self.width) return error.InvalidSqlSpill;
        if (index >= self.len) return error.InvalidSqlSpill;
        if (self.columnar) |*store| {
            if (self.updates) |*updates| if (column >= updates.first_column) {
                const offset = try updates.offsets.at(index * (self.width - updates.first_column) + column - updates.first_column);
                if (offset != std.math.maxInt(usize)) {
                    _ = self.cell_arena.reset(.free_all);
                    const decoded = try updates.file.read(self.cell_arena.allocator(), offset);
                    if (decoded.row.values.len != 1 or decoded.row.ordinal != index) return error.InvalidSqlSpill;
                    return decoded.row.values[0];
                }
            };
            return store.cell(index, column);
        }
        return (try self.row(index)).values[column];
    }
    pub fn setCell(self: *Rows, index: usize, column: usize, value: Datum) !void {
        if (column >= self.width) return error.InvalidSqlSpill;
        if (index >= self.len) return error.InvalidSqlSpill;
        if (self.updates) |*updates| {
            if (column < updates.first_column) return error.InvalidSqlSpill;
            const offset = try updates.file.append(.{ .values = &.{value}, .keys = &.{}, .ordinal = index }, spill.none);
            try updates.offsets.set(index * (self.width - updates.first_column) + column - updates.first_column, @intCast(offset));
            self.invalidate(index);
            return;
        }
        // Column inputs are immutable; updates require an explicit sidecar.
        if (self.columnar != null) return error.InvalidSqlSpill;
        const before = try self.row(index);
        const values = try self.a.dupe(Datum, before.values);
        defer self.a.free(values);
        values[column] = value;
        const offset = try self.file.append(.{ .values = values, .keys = &.{}, .ordinal = before.ordinal }, spill.none);
        try self.offsets.set(index, @intCast(offset));
        self.invalidate(index);
    }
    fn invalidate(self: *Rows, index: usize) void {
        for (&self.cache) |*entry| if (entry.index == index) {
            entry.index = null;
            _ = entry.arena.reset(.free_all);
        };
    }
};

/// A permutation over one immutable payload and its shared output sidecars.
/// Views own only ordinal directories; window updates address original rows.
pub const View = struct {
    source: *Rows,
    indices: *Integers,
    begin: usize = 0,
    len: usize,
    pub fn row(self: *View, index: usize) !operators.Row {
        if (index >= self.len) return error.InvalidSqlSpill;
        return self.source.row(try self.indices.at(self.begin + index));
    }
    pub fn cell(self: *View, index: usize, column: usize) !Datum {
        if (index >= self.len) return error.InvalidSqlSpill;
        return self.source.cell(try self.indices.at(self.begin + index), column);
    }
    pub fn setCell(self: *View, index: usize, column: usize, value: Datum) !void {
        if (index >= self.len) return error.InvalidSqlSpill;
        try self.source.setCell(try self.indices.at(self.begin + index), column, value);
    }
};
pub fn isDisk(comptime T: type) bool {
    return T == *Rows or T == *View;
}

/// Small write-back cache for fixed window state. The underlying file is
/// already sized and charged to the statement quota before cache admission.
pub const RawCache = struct {
    file: spill.File,
    a: A,
    pages: []Page,
    tick: u64 = 0,
    const Page = struct { bytes: [4096]u8 = undefined, block: ?u64 = null, valid: usize = 0, dirty: bool = false, touched: u64 = 0 };
    pub fn init(a: A, file: spill.File) !*RawCache {
        const self = try a.create(RawCache);
        errdefer a.destroy(self);
        const pages = try a.alloc(Page, 4);
        for (pages) |*page| page.* = .{};
        self.* = .{ .a = a, .file = file, .pages = pages };
        return self;
    }
    pub fn close(self: *RawCache) void {
        const a = self.a;
        self.file.close();
        a.free(self.pages);
        a.destroy(self);
    }
    fn load(self: *RawCache, block: u64) !*Page {
        try self.file.manager.check();
        self.tick +%= 1;
        var oldest = &self.pages[0];
        for (self.pages) |*page| {
            if (page.block == block) {
                page.touched = self.tick;
                return page;
            }
            if (page.block == null or page.touched < oldest.touched) oldest = page;
        }
        if (oldest.dirty) try self.file.writeRaw(oldest.block.?, oldest.bytes[0..oldest.valid]);
        oldest.block = null;
        oldest.dirty = false;
        const size: usize = @intCast(@min(4096, self.file.size -| block));
        try self.file.readRaw(block, oldest.bytes[0..size]);
        oldest.block = block;
        oldest.valid = size;
        oldest.touched = self.tick;
        return oldest;
    }
    pub fn readRaw(self: *RawCache, offset: u64, bytes: []u8) !void {
        if (offset > self.file.size or bytes.len > self.file.size - offset) return error.InvalidSqlSpill;
        var copied: usize = 0;
        while (copied < bytes.len) {
            const position = offset + copied;
            const block = position / 4096 * 4096;
            const page = try self.load(block);
            const start: usize = @intCast(position - block);
            const count = @min(bytes.len - copied, page.valid - start);
            @memcpy(bytes[copied..][0..count], page.bytes[start..][0..count]);
            copied += count;
        }
    }
    pub fn writeRaw(self: *RawCache, offset: u64, bytes: []const u8) !void {
        if (offset > self.file.size or bytes.len > self.file.size - offset) return error.InvalidSqlSpill;
        var copied: usize = 0;
        while (copied < bytes.len) {
            const position = offset + copied;
            const block = position / 4096 * 4096;
            const page = try self.load(block);
            const start: usize = @intCast(position - block);
            const count = @min(bytes.len - copied, page.valid - start);
            @memcpy(page.bytes[start..][0..count], bytes[copied..][0..count]);
            page.dirty = true;
            copied += count;
        }
    }
};

test "SQL window cell updates preserve wide rows without rewriting input payloads" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    var rows = try Rows.init(a, &manager, 3);
    defer rows.deinit();
    const text = [_]u8{'x'} ** 8192;
    for (0..64) |index| try rows.append(.{ .values = &.{ Datum.json(.{ .string = &text }), .{}, .{} }, .keys = &.{}, .ordinal = index + 100 });
    const original_bytes = rows.file.size;
    try rows.enableColumnUpdates(1);
    for (0..64) |index| {
        try rows.setCell(index, 1, Datum.json(.{ .integer = @intCast(index) }));
        try rows.setCell(index, 2, if (index % 2 == 0) Datum.json(.null) else .{});
    }
    try std.testing.expectEqual(original_bytes, rows.file.size);
    try std.testing.expect(rows.updates.?.file.size < original_bytes / 16);
    for (0..64) |index| {
        const row_value = try rows.row(index);
        try std.testing.expectEqual(@as(u64, index + 100), row_value.ordinal);
        try std.testing.expectEqualStrings(&text, row_value.values[0].value.string);
        try std.testing.expectEqual(@as(i64, @intCast(index)), row_value.values[1].value.integer);
        try std.testing.expectEqual(index % 2 != 0, row_value.values[2].sql_null);
    }
}

test "SQL window permutations share wide payloads and original-row sidecars" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    const text = try a.alloc(u8, 64 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    var source = try Rows.init(a, &manager, 2);
    defer source.deinit();
    for (0..3) |index| try source.append(.{ .values = &.{ Datum.json(.{ .string = text }), .{} }, .keys = &.{}, .ordinal = index });
    const payload_bytes = source.file.size;
    try source.enableColumnUpdates(1);
    var first = try Integers.init(&manager);
    defer first.deinit();
    var second = try Integers.init(&manager);
    defer second.deinit();
    for ([_]usize{ 2, 0, 1 }) |i| try first.append(i);
    for ([_]usize{ 1, 2, 0 }) |i| try second.append(i);
    var left: View = .{ .source = &source, .indices = &first, .len = 3 };
    var right: View = .{ .source = &source, .indices = &second, .len = 3 };
    try left.setCell(0, 1, Datum.json(.{ .integer = 42 }));
    try std.testing.expectEqual(@as(i64, 42), (try right.cell(1, 1)).value.integer);
    try right.setCell(2, 1, Datum.json(.{ .integer = 7 }));
    try std.testing.expectEqual(@as(i64, 7), (try left.cell(1, 1)).value.integer);
    try std.testing.expectEqual(@as(u64, 2), (try right.row(1)).ordinal);
    try std.testing.expectEqualStrings(text, (try right.cell(1, 0)).value.string);
    try std.testing.expectEqual(payload_bytes, source.file.size);
}

test "SQL window column blocks skip wide payloads and preserve shared updates" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var rows = try Rows.init(std.testing.allocator, &manager, 3);
    defer rows.deinit();
    try rows.enableColumns();
    const payload: [8192]u8 = @splat('x');
    for (0..128) |index| try rows.append(.{ .values = &.{ Datum.json(.{ .integer = @intCast(index) }), Datum.json(.{ .string = &payload }), .{} }, .keys = &.{}, .ordinal = index });
    try std.testing.expectError(error.InvalidSqlSpill, rows.setCell(0, 0, Datum.json(.{ .integer = 99 })));
    try rows.enableColumnUpdates(2);
    const before = manager.read_bytes;
    for (0..128) |index| try std.testing.expectEqual(@as(i64, @intCast(index)), (try rows.cell(index, 0)).value.integer);
    try std.testing.expect(manager.read_bytes - before < payload.len * 128 / 8);
    try rows.setCell(42, 2, Datum.json(.{ .integer = 99 }));
    try std.testing.expectEqual(@as(i64, 99), (try rows.cell(42, 2)).value.integer);
    try std.testing.expectEqualStrings(&payload, (try rows.row(42)).values[1].value.string);
}

fn columnAllocationScenario(a: A) !void {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var rows = try Rows.init(a, &manager, 2);
    defer rows.deinit();
    try rows.enableColumns();
    for (0..3) |index| try rows.append(.{ .values = &.{ Datum.json(.{ .integer = @intCast(index) }), .{} }, .keys = &.{}, .ordinal = index });
    try rows.enableColumnUpdates(1);
    _ = try rows.cell(0, 0);
    try rows.setCell(1, 1, Datum.json(.{ .integer = 7 }));
    try std.testing.expectEqual(@as(i64, 7), (try rows.row(1)).values[1].value.integer);
}
test "SQL window column blocks unwind every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, columnAllocationScenario, .{});
}

test "SQL wide column rows retain a full active block set without repeated decoding" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var rows = try Rows.init(a, &manager, 8);
    defer rows.deinit();
    try rows.enableColumns();
    for (0..128) |index| {
        var values: [8]Datum = undefined;
        for (&values, 0..) |*value, column| value.* = Datum.json(.{ .integer = @intCast(index * 8 + column) });
        try rows.append(.{ .values = &values, .keys = &.{}, .ordinal = index });
    }
    try rows.columnar.?.flush();
    for (0..128) |index| {
        const row = try rows.row(index);
        for (row.values, 0..) |value, column| try std.testing.expectEqual(@as(i64, @intCast(index * 8 + column)), value.value.integer);
    }
    // Each record should be decoded once, regardless of the bounded block
    // size. The four-entry implementation decoded 1024 records here.
    try std.testing.expectEqual(rows.columnar.?.directory.len, rows.columnar.?.decodes);
}
