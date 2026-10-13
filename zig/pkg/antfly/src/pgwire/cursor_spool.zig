// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
        if (result.rowCount() > max_rows -| self.rows.items.len) return error.ProgramLimitExceeded;
        const alloc = self.arena.allocator();
        if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
        for (0..result.rowCount()) |i| {
            const cells = try alloc.alloc(std.json.Value, result.columns.len);
            const nulls = try alloc.alloc(bool, result.columns.len);
            for (cells, nulls, 0..) |*cell, *is_null, j| {
                const value = try result.cell(alloc, i, j);
                if (value.array) |array| {
                    if (value.sql_null or value.value != .null or value.numeric != null or result.columns[j].type != .array or result.columns[j].element_type != array.element_type) return error.InvalidResult;
                    cell.* = try @import("antfly_local_sources").sql_array_wire.toJsonLeaky(alloc, array.*, .{});
                } else if (value.numeric) |number| {
                    if (value.sql_null or value.value != .null or result.columns[j].type != .number or result.columns[j].element_type != .numeric) return error.InvalidResult;
                    var context: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = alloc };
                    cell.* = .{ .string = try @import("antfly_local_sources").sql_numeric_value.format(&context, number.*) };
                } else cell.* = try clone(alloc, value.value, 0);
                is_null.* = value.sql_null;
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

test "pgwire cursor spool owns native arrays and exact numerics after page retirement" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    var budget: Budget = .{ .child = a, .limit = 32 * 1024 };
    var spool = Store.init(&budget);
    defer spool.deinit();
    {
        var array = try sources.sql_array_text.decode(a, .jsonb, "{\"null\",NULL,\"{\\\"text\\\":\\\"owned\\\"}\"}", .{});
        defer array.deinit();
        var context: sources.sql_numeric_value.Context = .{ .alloc = a };
        var numeric = try sources.sql_numeric_value.parse(&context, "12345678901234567890.123456789");
        defer numeric.deinit();
        const Reader = struct {
            array: *const sources.sql_array_value.Value,
            numeric: *const sources.sql_numeric_value.Value,
            fn read(raw: *anyopaque, _: std.mem.Allocator, _: usize, column: usize) !backend.Cell {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return if (column == 0) .{ .array = self.array, .sql_null = false } else .{ .numeric = self.numeric, .sql_null = false };
            }
        };
        var reader: Reader = .{ .array = &array.value, .numeric = &numeric.value };
        const result: backend.Result = .{
            .columns = &.{ .{ .name = "a", .type = .array, .element_type = .jsonb }, .{ .name = "n", .type = .number, .element_type = .numeric } },
            .cells = .{ .context = &reader, .count = 1, .width = 2, .read = Reader.read },
            .command_tag = "SELECT",
        };
        try spool.append(result, 1);
    }
    const row = spool.rows.items[0];
    var decoded = try sources.sql_array_wire.decode(a, .jsonb, row.values[0], .{});
    defer decoded.deinit();
    try std.testing.expect(!decoded.value.elements[0].sql_null and decoded.value.elements[0].value == .null);
    try std.testing.expect(decoded.value.elements[1].sql_null);
    try std.testing.expectEqualStrings("owned", decoded.value.elements[2].value.object.get("text").?.string);
    try std.testing.expectEqualStrings("12345678901234567890.123456789", row.values[1].string);
}
