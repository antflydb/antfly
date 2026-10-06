// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Statement-owned blocking results shared by nested relations and public streams.
const std = @import("std");

pub const Cursor = struct {
    manager: @import("spill.zig").Manager,
    shared: ?*@import("spill.zig").Manager = null,
    a: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    width: usize,
    rows: @import("spill.zig").Sequential,
    index: usize = 0,
    sorted: ?*@import("operators.zig").TopK = null,
    sorted_rows: []const @import("operators.zig").Row = &.{},
    sorted_offset: usize = 0,
    sorted_count: usize = 0,
    memory_leased: bool = false,
    block: ?*@import("spill.zig").Sequential.OwnedBlock = null,
    block_begin: usize = 0,
    const Location = struct {
        block: ?*@import("spill.zig").Sequential.OwnedBlock = null,
        index: usize = 0,
        values: []const @import("scalar.zig").Datum = &.{},
        fn cell(self: Location, column: usize) !@import("scalar.zig").Datum {
            return if (self.block) |block| block.cell(self.index, column) else self.values[column];
        }
    };
    pub const Lease = struct {
        values: @import("execution_batch.zig").Batch,
        owner: ?*Cursor = null,
        block: ?*@import("spill.zig").Sequential.OwnedBlock = null,
        blocks: []const *@import("spill.zig").Sequential.OwnedBlock = &.{},
        pub fn deinit(self: Lease) void {
            if (self.block) |block| block.release();
            for (self.blocks) |block| block.release();
            if (self.owner) |owner| owner.close();
        }
    };
    /// Page descriptors use the caller's arena. Pages retain the cursor through
    /// terminal cleanup; release them before the statement allocator or shared
    /// spill owner closes. Payloads remain in decoded blocks or the sort owner.
    pub fn nextLease(self: *Cursor, a: std.mem.Allocator, maximum: usize, byte_limit: usize) !Lease {
        if (maximum == 0) return error.InvalidSqlLimit;
        if (self.index == self.count()) return .{ .values = .{ .rows = &.{} } };
        if (self.sorted) |top| {
            if (top.external == null) self.memory_leased = true;
            const capacity = @min(maximum, self.count() - self.index);
            const rows = try a.alloc(Location, capacity);
            const blocks = try a.alloc(*@import("spill.zig").Sequential.OwnedBlock, capacity);
            var held: usize = 0;
            errdefer for (blocks[0..held]) |block| block.release();
            var count_rows: usize = 0;
            var bytes: usize = 0;
            var leased_bytes: usize = capacity *| (@sizeOf(Location) + @sizeOf(*@import("spill.zig").Sequential.OwnedBlock));
            while (count_rows < capacity) {
                if (top.external) |sort| {
                    while (self.sorted_offset != 0) : (self.sorted_offset -= 1) {
                        const skipped = (try sort.nextLeased()) orelse return error.InvalidSqlSpill;
                        skipped.release();
                    }
                    const lease = (try sort.nextLeased()) orelse return error.InvalidSqlSpill;
                    rows[count_rows] = .{ .block = lease.block, .index = lease.index, .values = lease.row.values };
                    if (lease.block) |block| {
                        const existing = for (blocks[0..held]) |prior| {
                            if (prior == block) break true;
                        } else false;
                        if (existing) block.release() else {
                            blocks[held] = block;
                            held += 1;
                            leased_bytes +|= block.arena.queryCapacity() +| @sizeOf(@import("spill.zig").Sequential.OwnedBlock);
                        }
                    }
                } else rows[count_rows] = .{ .values = self.sorted_rows[self.sorted_offset + self.index].values };
                for (0..self.width) |column| bytes +|= try @import("operators.zig").datumBytes(try rows[count_rows].cell(column));
                self.index += 1;
                count_rows += 1;
                if (bytes >= byte_limit or leased_bytes >= byte_limit) break;
            }
            const View = struct {
                rows: []const Location,
                fn cell(raw: *anyopaque, _: std.mem.Allocator, row: usize, column: usize) anyerror!@import("scalar.zig").Datum {
                    const view: *@This() = @ptrCast(@alignCast(raw));
                    return view.rows[row].cell(column);
                }
            };
            const view = try a.create(View);
            view.* = .{ .rows = rows[0..count_rows] };
            _ = self.refs.fetchAdd(1, .monotonic);
            return .{ .values = .{ .reader = .{ .ptr = view, .read = View.cell, .count = count_rows, .width = self.width } }, .blocks = blocks[0..held], .owner = self };
        }
        try self.loadBlock();
        const block = self.block.?;
        const begin = self.index - self.block_begin;
        var end = begin;
        var bytes: usize = 0;
        while (end < block.count() and end - begin < maximum) {
            for (0..self.width) |column| bytes +|= try @import("operators.zig").datumBytes(try block.cell(end, column));
            end += 1;
            if (bytes >= byte_limit) break;
        }
        const View = struct {
            block: *@import("spill.zig").Sequential.OwnedBlock,
            begin: usize,
            fn cell(raw: *anyopaque, _: std.mem.Allocator, row: usize, column: usize) anyerror!@import("scalar.zig").Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.block.cell(view.begin + row, column);
            }
        };
        const view = try a.create(View);
        view.* = .{ .block = block, .begin = begin };
        block.retain();
        _ = self.refs.fetchAdd(1, .monotonic);
        self.index += end - begin;
        return .{ .values = .{ .reader = .{ .ptr = view, .read = View.cell, .count = end - begin, .width = self.width } }, .block = block, .owner = self };
    }
    fn loadBlock(self: *Cursor) !void {
        if (self.block) |block| {
            if (self.index >= self.block_begin and self.index - self.block_begin < block.count()) return;
            block.release();
            self.block = null;
        }
        self.block = try self.rows.readOwnedBlock(self.index);
        self.block_begin = self.index;
    }
    /// Borrow the statement spill owner, sharing its quota and cancellation.
    pub fn create(a: std.mem.Allocator, manager: *@import("spill.zig").Manager, width: usize) !*Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        self.* = .{ .manager = undefined, .shared = manager, .a = a, .width = width, .rows = try @import("spill.zig").Sequential.init(manager, @max(128, manager.buffer_bytes * 2)) };
        return self;
    }
    pub fn next(self: *Cursor, a: std.mem.Allocator) !?[]const @import("scalar.zig").Datum {
        if (self.index == self.count()) return null;
        const row = try self.read(a);
        if (self.ownsRead()) {
            self.index += 1;
            return row.values;
        }
        const values = try a.alloc(@import("scalar.zig").Datum, row.values.len);
        for (row.values, values) |value, *out| out.* = try @import("operators.zig").cloneDatum(a, value);
        if (self.sorted) |top| if (top.external == null and !self.memory_leased) top.releaseFinishedRow(self.sorted_offset + self.index);
        self.index += 1;
        return values;
    }
    /// Gather blocking output into retained typed columns once. Spill row
    /// payloads live only in scratch; transport pages own their column store.
    pub fn nextBatch(self: *Cursor, a: std.mem.Allocator, maximum: usize, byte_limit: usize) !*@import("typed_store.zig").Store {
        const store = try a.create(@import("typed_store.zig").Store);
        errdefer a.destroy(store);
        store.* = .init(a);
        errdefer store.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        var bytes: usize = 0;
        while (self.index < self.count() and store.len < maximum) {
            _ = scratch.reset(.retain_capacity);
            const row = try self.read(scratch.allocator());
            for (row.values) |value| bytes +|= try @import("operators.zig").datumBytes(value);
            _ = try store.append(row.values);
            if (self.sorted) |top| if (top.external == null and !self.memory_leased) top.releaseFinishedRow(self.sorted_offset + self.index);
            self.index += 1;
            if (bytes >= byte_limit) break;
        }
        return store;
    }
    pub fn count(self: *const Cursor) usize {
        return if (self.sorted != null) self.sorted_count else @intCast(self.rows.size);
    }
    pub fn takeSorted(raw: *anyopaque, top: *@import("operators.zig").TopK, offset: usize, limit: usize, implicit: bool) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.sorted != null or self.rows.size != 0) return error.InvalidSqlBackendResponse;
        const total = if (top.external) |sort| @min(sort.total, top.capacity) else top.count;
        const available = total -| offset;
        if (implicit and available > limit) return error.SqlResultTooLarge;
        const owned = try self.a.create(@import("operators.zig").TopK);
        errdefer self.a.destroy(owned);
        // Finish before moving: memory rows borrow the heap's stable arenas.
        const memory_rows = if (top.external == null) try top.finish(self.a) else &.{};
        owned.* = top.*;
        top.* = .{ .alloc = top.alloc, .entries = &.{}, .orders = &.{}, .max_bytes = 0, .retained_bytes = 0 };
        self.sorted = owned;
        self.sorted_rows = memory_rows;
        self.sorted_offset = @min(offset, total);
        self.sorted_count = @min(available, limit);
        if (owned.external == null) for (0..self.sorted_offset) |index| owned.releaseFinishedRow(index);
    }
    pub fn ownsRead(self: *const Cursor) bool {
        return if (self.sorted) |top| top.external != null else false;
    }
    pub fn read(self: *Cursor, a: std.mem.Allocator) !@import("operators.zig").Row {
        if (self.sorted) |top| {
            if (top.external) |sort| {
                var scratch = std.heap.ArenaAllocator.init(self.a);
                defer scratch.deinit();
                while (self.sorted_offset != 0) : (self.sorted_offset -= 1) {
                    _ = scratch.reset(.free_all);
                    _ = (try sort.nextValues(scratch.allocator())) orelse return error.InvalidSqlSpill;
                }
                return (try sort.nextValues(a)) orelse error.InvalidSqlSpill;
            }
            return self.sorted_rows[self.sorted_offset + self.index];
        }
        if (self.block != null) {
            try self.loadBlock();
            return self.block.?.row(a, self.index - self.block_begin);
        }
        return (try self.rows.readBorrowed(self.index)).row;
    }
    pub fn append(raw: *anyopaque, values: []const @import("scalar.zig").Datum) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (values.len != self.width or self.sorted != null) return error.InvalidSqlBackendResponse;
        _ = try self.rows.append(.{ .values = values, .keys = &.{}, .ordinal = self.rows.size }, @import("spill.zig").none);
    }
    pub fn close(self: *Cursor) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.a;
        if (self.block) |block| block.release();
        if (self.sorted) |top| {
            a.free(self.sorted_rows);
            top.deinit();
            a.destroy(top);
        }
        self.rows.close();
        if (self.shared == null) self.manager.deinit();
        a.destroy(self);
    }
};

test "SQL blocking result blocks preserve exact values and avoid row directories" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    {
        const cursor = try Cursor.create(a, &manager, 3);
        defer cursor.close();
        for (0..1024) |_| try Cursor.append(cursor, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.null) });
        try std.testing.expectEqual(@as(usize, 1), manager.files);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var count: usize = 0;
        while (true) {
            _ = arena.reset(.retain_capacity);
            const row = (try cursor.next(arena.allocator())) orelse break;
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].value.integer);
            try std.testing.expect(row[1].sql_null);
            try std.testing.expect(!row[2].sql_null and row[2].value == .null);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1024), count);
        try std.testing.expect(manager.read_calls < 128);
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL blocking leases survive subsequent pulls for sorted and sequential wide results" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const spill = @import("spill.zig");
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]enum { sequential, memory, external }{ .sequential, .memory, .external }) |mode| {
        const sorted = mode != .sequential;
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
        defer manager.deinit();
        const cursor = try Cursor.create(a, &manager, 3);
        var cursor_owned = true;
        defer if (cursor_owned) cursor.close();
        var top = try @import("operators.zig").TopK.initWithSpill(a, 1000, &.{.{}}, if (mode == .external) 256 * 1024 else 8 * 1024 * 1024, &manager);
        defer top.deinit();
        var payload: [8192]u8 = @splat('x');
        for (0..300) |index| {
            const key = Datum.json(.{ .integer = @intCast(if (sorted) 299 - index else index) });
            const values = &.{ key, Datum.json(.{ .string = &payload }), Datum{} };
            if (sorted) try top.add(.{ .values = values, .keys = &.{key}, .ordinal = index }) else try Cursor.append(cursor, values);
        }
        if (sorted) {
            try std.testing.expectEqual(mode == .external, top.external != null);
            try Cursor.takeSorted(cursor, &top, 0, 300, false);
        }
        var first_arena = std.heap.ArenaAllocator.init(a);
        defer first_arena.deinit();
        const first = try cursor.nextLease(first_arena.allocator(), 17, 32768);
        defer first.deinit();
        try std.testing.expect(first.values.len() > 0);
        const first_value = try first.values.cell(a, 0, 1);
        try std.testing.expectEqual(@as(i64, 0), (try first.values.cell(a, 0, 0)).value.integer);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var count_rows = first.values.len();
        while (cursor.index < cursor.count()) {
            _ = arena.reset(.free_all);
            if (count_rows % 5 == 0) {
                const row = (try cursor.next(arena.allocator())).?;
                try std.testing.expectEqual(@as(i64, @intCast(count_rows)), row[0].value.integer);
                count_rows += 1;
            } else {
                const page = try cursor.nextLease(arena.allocator(), 17, 32768);
                defer page.deinit();
                for (0..page.values.len()) |row| {
                    try std.testing.expectEqual(@as(i64, @intCast(count_rows)), (try page.values.cell(a, row, 0)).value.integer);
                    try std.testing.expect((try page.values.cell(a, row, 2)).sql_null);
                    count_rows += 1;
                }
            }
        }
        try std.testing.expectEqual(@as(usize, 300), count_rows);
        cursor_owned = false;
        cursor.close();
        try std.testing.expectEqualStrings(&payload, first_value.value.string);
        try std.testing.expectEqualStrings(&payload, (try first.values.cell(a, 0, 1)).value.string);
    }
}
