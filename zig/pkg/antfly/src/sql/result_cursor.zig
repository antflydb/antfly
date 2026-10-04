// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Statement-owned blocking results shared by nested relations and public streams.
const std = @import("std");

pub const Cursor = struct {
    manager: @import("spill.zig").Manager,
    shared: ?*@import("spill.zig").Manager = null,
    rows: @import("disk_rows.zig").Rows,
    index: usize = 0,
    sorted: ?*@import("operators.zig").TopK = null,
    sorted_rows: []const @import("operators.zig").Row = &.{},
    sorted_offset: usize = 0,
    sorted_count: usize = 0,
    /// Borrow the statement spill owner, sharing its quota and cancellation.
    pub fn create(a: std.mem.Allocator, manager: *@import("spill.zig").Manager, width: usize) !*Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        self.* = .{ .manager = undefined, .shared = manager, .rows = try @import("disk_rows.zig").Rows.init(a, manager, width) };
        return self;
    }
    pub fn next(self: *Cursor, a: std.mem.Allocator) !?[]const @import("scalar.zig").Datum {
        if (self.index == self.count()) return null;
        const row = try self.read(a);
        const values = try a.alloc(@import("scalar.zig").Datum, row.values.len);
        for (row.values, values) |value, *out| out.* = try @import("operators.zig").cloneDatum(a, value);
        if (self.sorted) |top| if (top.external == null) top.releaseFinishedRow(self.sorted_offset + self.index);
        self.index += 1;
        return values;
    }
    pub fn count(self: *const Cursor) usize {
        return if (self.sorted != null) self.sorted_count else self.rows.len;
    }
    pub fn takeSorted(raw: *anyopaque, top: *@import("operators.zig").TopK, offset: usize, limit: usize, implicit: bool) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.sorted != null or self.rows.len != 0) return error.InvalidSqlBackendResponse;
        const total = if (top.external) |sort| @min(sort.total, top.capacity) else top.count;
        const available = total -| offset;
        if (implicit and available > limit) return error.SqlResultTooLarge;
        const owned = try self.rows.a.create(@import("operators.zig").TopK);
        errdefer self.rows.a.destroy(owned);
        // Finish before moving: memory rows borrow the heap's stable arenas.
        const memory_rows = if (top.external == null) try top.finish(self.rows.a) else &.{};
        owned.* = top.*;
        top.* = .{ .alloc = top.alloc, .entries = &.{}, .orders = &.{}, .max_bytes = 0, .retained_bytes = 0 };
        self.sorted = owned;
        self.sorted_rows = memory_rows;
        self.sorted_offset = @min(offset, total);
        self.sorted_count = @min(available, limit);
        if (owned.external == null) for (0..self.sorted_offset) |index| owned.releaseFinishedRow(index);
    }
    pub fn read(self: *Cursor, a: std.mem.Allocator) !@import("operators.zig").Row {
        if (self.sorted) |top| {
            if (top.external) |sort| {
                var scratch = std.heap.ArenaAllocator.init(self.rows.a);
                defer scratch.deinit();
                while (self.sorted_offset != 0) : (self.sorted_offset -= 1) {
                    _ = scratch.reset(.free_all);
                    _ = (try sort.next(scratch.allocator())) orelse return error.InvalidSqlSpill;
                }
                return (try sort.next(a)) orelse error.InvalidSqlSpill;
            }
            return self.sorted_rows[self.sorted_offset + self.index];
        }
        return self.rows.row(self.index);
    }
    pub fn append(raw: *anyopaque, values: []const @import("scalar.zig").Datum) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        try self.rows.append(.{ .values = values, .keys = &.{}, .ordinal = self.rows.len });
    }
    pub fn close(self: *Cursor) void {
        const a = self.rows.a;
        if (self.sorted) |top| {
            a.free(self.sorted_rows);
            top.deinit();
            a.destroy(top);
        }
        self.rows.deinit();
        if (self.shared == null) self.manager.deinit();
        a.destroy(self);
    }
};
