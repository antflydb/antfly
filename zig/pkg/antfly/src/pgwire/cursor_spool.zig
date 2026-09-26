// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Connection-budgeted typed cursor rows. Spooling never reexecutes SQL and
//! preserves SQL NULL separately from a JSON null value.
const std = @import("std");
const backend = @import("backend.zig");
const Budget = @import("budget.zig").Budget;
pub const Row = struct { values: []const std.json.Value, nulls: []const bool };
pub const Store = struct {
    arena: std.heap.ArenaAllocator,
    rows: std.ArrayList(Row) = .empty,
    position: i64 = 0,

    pub fn init(budget: *Budget) Store {
        return .{ .arena = std.heap.ArenaAllocator.init(budget.allocator()) };
    }
    pub fn deinit(self: *Store) void {
        self.arena.deinit();
    }
    pub fn append(self: *Store, result: backend.Result, max_rows: usize) !void {
        if (result.rows.len > max_rows -| self.rows.items.len) return error.ProgramLimitExceeded;
        const alloc = self.arena.allocator();
        if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
        for (result.rows, 0..) |row, i| {
            if (row.len != result.columns.len) return error.InvalidResult;
            const cells = try alloc.alloc(std.json.Value, row.len);
            const nulls = try alloc.alloc(bool, row.len);
            if (result.sql_nulls) |flags| if (flags[i].len != row.len) return error.InvalidResult;
            for (row, cells, nulls, 0..) |value, *cell, *is_null, j| {
                cell.* = try clone(alloc, value, 0);
                is_null.* = if (result.sql_nulls) |flags| flags[i][j] else value == .null;
            }
            try self.rows.append(alloc, .{ .values = cells, .nulls = nulls });
        }
    }
};

fn clone(alloc: std.mem.Allocator, value: std.json.Value, depth: usize) !std.json.Value {
    if (depth > 128) return error.ProgramLimitExceeded;
    return switch (value) {
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        .number_string => |text| .{ .number_string = try alloc.dupe(u8, text) },
        .array => |array| blk: {
            var result = std.array_list.Managed(std.json.Value).init(alloc);
            for (array.items) |element| try result.append(try clone(alloc, element, depth + 1));
            break :blk .{ .array = result };
        },
        .object => |object| blk: {
            var result: std.json.ObjectMap = .empty;
            var iterator = object.iterator();
            while (iterator.next()) |entry| try result.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try clone(alloc, entry.value_ptr.*, depth + 1));
            break :blk .{ .object = result };
        },
        else => value,
    };
}

test "pgwire cursor spool bounds bytes rows and preserves typed nulls" {
    var budget: Budget = .{ .child = std.testing.allocator, .limit = 4096 };
    {
        var spool = Store.init(&budget);
        defer spool.deinit();
        const result: backend.Result = .{ .columns = &.{.{ .name = "j", .type = .json }}, .rows = &.{ &.{.null}, &.{.null} }, .sql_nulls = &.{ &.{false}, &.{true} }, .command_tag = "SELECT" };
        try spool.append(result, 2);
        try std.testing.expect(!spool.rows.items[0].nulls[0]);
        try std.testing.expect(spool.rows.items[1].nulls[0]);
        try std.testing.expectError(error.ProgramLimitExceeded, spool.append(result, 2));
        const large = try std.testing.allocator.alloc(u8, 8192);
        defer std.testing.allocator.free(large);
        try std.testing.expectError(error.OutOfMemory, spool.append(.{ .columns = result.columns, .rows = &.{&.{.{ .string = large }}}, .command_tag = "SELECT" }, 3));
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used);
}
