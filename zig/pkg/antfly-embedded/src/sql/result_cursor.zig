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

//! Statement-owned blocking results shared by nested relations and public streams.
const native_platform = @import("antfly_platform");
const std = @import("std");

pub const Cursor = struct {
    manager: @import("spill.zig").Manager,
    shared: ?*@import("spill.zig").Manager = null,
    a: std.mem.Allocator,
    width: usize,
    rows: @import("spill.zig").Sequential,
    index: usize = 0,
    sorted: ?*@import("operators.zig").TopK = null,
    sorted_rows: []const @import("operators.zig").Row = &.{},
    sorted_offset: usize = 0,
    sorted_count: usize = 0,
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
        if (self.sorted) |top| if (top.external == null) top.releaseFinishedRow(self.sorted_offset + self.index);
        self.index += 1;
        return values;
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
        return (try self.rows.readBorrowed(self.index)).row;
    }
    pub fn append(raw: *anyopaque, values: []const @import("scalar.zig").Datum) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (values.len != self.width or self.sorted != null) return error.InvalidSqlBackendResponse;
        _ = try self.rows.append(.{ .values = values, .keys = &.{}, .ordinal = self.rows.size }, @import("spill.zig").none);
    }
    pub fn close(self: *Cursor) void {
        const a = self.a;
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
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = native_platform.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
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
