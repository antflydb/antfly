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
// A page owns its prepared payloads. Type tags live once per column; scalar
// cells remain plain JSON values and native cells retain compact pointers.
const ColumnPayload = union(enum) {
    json: struct { values: []std.json.Value, nulls: []bool },
    array: []?*const @import("antfly_local_sources").sql_array_value.Value,
    numeric: []?*const @import("antfly_local_sources").sql_numeric_value.Value,
};
const Page = struct {
    arena: std.heap.ArenaAllocator,
    begin: usize,
    count: usize,
    columns: []ColumnPayload,
};
pub const Store = struct {
    budget: *Budget,
    pages: std.ArrayList(*Page) = .empty,
    columns: ?[]const backend.Column = null,
    row_count: usize = 0,
    cached_page: usize = 0,
    position: i64 = 0,

    pub fn init(budget: *Budget) Store {
        return .{ .budget = budget };
    }
    pub fn deinit(self: *Store) void {
        const a = self.budget.allocator();
        for (self.pages.items) |page| {
            page.arena.deinit();
            a.destroy(page);
        }
        self.pages.deinit(a);
    }
    pub fn cell(self: *Store, row: usize, column: usize) !backend.Cell {
        const definitions = self.columns orelse return error.InvalidResult;
        if (row >= self.row_count or column >= definitions.len) return error.InvalidResult;
        var page = self.pages.items[self.cached_page];
        if (row < page.begin or row - page.begin >= page.count) {
            var lo: usize = 0;
            var hi = self.pages.items.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const candidate = self.pages.items[mid];
                if (row < candidate.begin) hi = mid else if (row - candidate.begin >= candidate.count) lo = mid + 1 else {
                    lo = mid;
                    break;
                }
            }
            if (lo >= self.pages.items.len) return error.InvalidResult;
            self.cached_page = lo;
            page = self.pages.items[lo];
        }
        const index = row - page.begin;
        return switch (page.columns[column]) {
            .json => |data| .{ .value = data.values[index], .sql_null = data.nulls[index] },
            .array => |values| if (values[index]) |value| .{ .array = value, .sql_null = false } else .{ .sql_null = true },
            .numeric => |values| if (values[index]) |value| .{ .numeric = value, .sql_null = false } else .{ .sql_null = true },
        };
    }
    pub fn append(self: *Store, result: backend.Result, max_rows: usize) !void {
        const count = result.rowCount();
        if (count > max_rows -| self.row_count) return error.ProgramLimitExceeded;
        if (self.columns) |definitions| {
            if (definitions.len != result.columns.len) return error.InvalidResult;
            for (definitions, result.columns) |old, next| if (old.type != next.type or old.element_type != next.element_type or !std.meta.eql(old.numeric_modifier, next.numeric_modifier) or !std.mem.eql(u8, old.name, next.name)) return error.InvalidResult;
        }
        if (result.sql_nulls) |flags| if (flags.len != result.rows.len) return error.InvalidResult;
        if (count == 0) return;
        const backing = self.budget.allocator();
        try self.pages.ensureUnusedCapacity(backing, 1);
        const page = try backing.create(Page);
        page.* = .{ .arena = .init(backing), .begin = self.row_count, .count = count, .columns = &.{} };
        errdefer {
            page.arena.deinit();
            backing.destroy(page);
        }
        const a = page.arena.allocator();
        const definitions = if (self.columns) |existing| existing else owned: {
            const copied = try a.dupe(backend.Column, result.columns);
            for (copied) |*definition| definition.name = try a.dupe(u8, definition.name);
            break :owned copied;
        };
        page.columns = try a.alloc(ColumnPayload, definitions.len);
        const sources = @import("antfly_local_sources");
        for (definitions, page.columns) |definition, *payload| {
            _ = try @import("values.zig").columnOid(definition);
            payload.* = if (definition.type == .array) .{ .array = try a.alloc(?*const sources.sql_array_value.Value, count) } else if (definition.type == .number and definition.element_type == .numeric) .{ .numeric = try a.alloc(?*const sources.sql_numeric_value.Value, count) } else .{ .json = .{ .values = try a.alloc(std.json.Value, count), .nulls = try a.alloc(bool, count) } };
        }
        for (0..count) |row| for (definitions, page.columns, 0..) |definition, *payload, column| {
            const value = try result.cell(a, row, column);
            if (value.sql_null and (value.value != .null or value.array != null or value.numeric != null)) return error.InvalidResult;
            switch (payload.*) {
                .json => |data| {
                    if (value.array != null or value.numeric != null) return error.InvalidResult;
                    data.values[row] = try clone(a, value.value, 0);
                    data.nulls[row] = value.sql_null;
                },
                .array => |values| {
                    if (value.sql_null) {
                        values[row] = null;
                        continue;
                    }
                    if (value.numeric != null) return error.InvalidResult;
                    var view: ?sources.sql_array_wire.Borrowed = null;
                    defer if (view) |*owned| owned.deinit();
                    const array = if (value.array) |native| native else decoded: {
                        view = try sources.sql_array_wire.decodeBorrowed(a, definition.element_type orelse return error.InvalidResult, value.value, .{});
                        break :decoded &view.?.value;
                    };
                    if ((value.array != null and value.value != .null) or array.element_type != definition.element_type) return error.InvalidResult;
                    _ = try array.validate(.{});
                    values[row] = (try sources.sql_operators.cloneDatum(a, .{ .array = array, .sql_null = false })).array.?;
                },
                .numeric => |values| {
                    if (value.sql_null) {
                        values[row] = null;
                        continue;
                    }
                    if (value.array != null) return error.InvalidResult;
                    if (value.numeric) |number| {
                        if (value.value != .null) return error.InvalidResult;
                        values[row] = (try sources.sql_operators.cloneDatum(a, .{ .numeric = number, .sql_null = false })).numeric.?;
                    } else {
                        if (value.value != .string) return error.InvalidResult;
                        var context: sources.sql_numeric_value.Context = .{ .alloc = a };
                        const parsed = try sources.sql_numeric_value.parse(&context, value.value.string);
                        const number = try a.create(sources.sql_numeric_value.Value);
                        number.* = parsed.value;
                        values[row] = number;
                    }
                },
            }
        };
        // Publish only after the entire page validates and owns its payloads.
        self.pages.appendAssumeCapacity(page);
        self.columns = definitions;
        self.row_count += count;
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
        try std.testing.expect(!(try spool.cell(0, 0)).sql_null);
        try std.testing.expect((try spool.cell(1, 0)).sql_null);
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
    const decoded = (try spool.cell(0, 0)).array.?;
    try std.testing.expect(!decoded.elements[0].sql_null and decoded.elements[0].value == .null);
    try std.testing.expect(decoded.elements[1].sql_null);
    try std.testing.expectEqualStrings("owned", decoded.elements[2].value.object.get("text").?.string);
    var context: sources.sql_numeric_value.Context = .{ .alloc = a };
    const text = try sources.sql_numeric_value.format(&context, (try spool.cell(0, 1)).numeric.?.*);
    defer a.free(text);
    try std.testing.expectEqualStrings("12345678901234567890.123456789", text);
}

test "pgwire native spool is compact validates descriptors and releases every allocation" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const sources = @import("antfly_local_sources");
            var array = try sources.sql_array_text.decode(alloc, .int64, "[0:2]={1,NULL,3}", .{});
            defer array.deinit();
            var context: sources.sql_numeric_value.Context = .{ .alloc = alloc };
            var numeric = try sources.sql_numeric_value.parse(&context, "1.2300");
            defer numeric.deinit();
            const Reader = struct {
                array: *const sources.sql_array_value.Value,
                numeric: *const sources.sql_numeric_value.Value,
                fn read(ptr: *anyopaque, _: std.mem.Allocator, _: usize, column: usize) !backend.Cell {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    return if (column == 0) .{ .array = self.array, .sql_null = false } else .{ .numeric = self.numeric, .sql_null = false };
                }
            };
            var reader: Reader = .{ .array = &array.value, .numeric = &numeric.value };
            var budget: Budget = .{ .child = alloc, .limit = 64 * 1024 };
            {
                var spool = Store.init(&budget);
                defer spool.deinit();
                const columns = [_]backend.Column{ .{ .name = "a", .type = .array, .element_type = .int64 }, .{ .name = "n", .type = .number, .element_type = .numeric } };
                const result: backend.Result = .{ .columns = &columns, .command_tag = "SELECT", .cells = .{ .context = &reader, .count = 1, .width = 2, .read = Reader.read } };
                try spool.append(result, 4);
                const copied = (try spool.cell(0, 0)).array.?;
                try std.testing.expect(copied != &array.value);
                try std.testing.expectEqual(@as(i32, 0), copied.dimensions[0].lower);
                try std.testing.expect(copied.elements[1].sql_null);
                var wrong = columns;
                wrong[0].element_type = .int32;
                var changed = result;
                changed.columns = &wrong;
                try std.testing.expectError(error.InvalidResult, spool.append(changed, 4));
            }
            try std.testing.expectEqual(@as(usize, 0), budget.used);
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "pgwire native spool allocation measurement" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    const elements = try a.alloc(sources.sql_array_value.Element, 4096);
    defer a.free(elements);
    for (elements, 0..) |*element, i| element.* = .{ .value = .{ .integer = @intCast(i) }, .sql_null = false };
    const value: sources.sql_array_value.Value = .{ .element_type = .int64, .dimensions = &.{.{ .length = 4096, .lower = 0 }}, .elements = elements };
    const Reader = struct {
        value: *const sources.sql_array_value.Value,
        fn read(ptr: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !backend.Cell {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .array = self.value, .sql_null = false };
        }
    };
    var reader: Reader = .{ .value = &value };
    var peaks: [2]usize = undefined;
    var calls: [2]usize = undefined;
    for (0..2) |mode| {
        var counter: sources.storage_lite_test_allocator.BudgetAllocator = .{ .backing = a };
        var budget: Budget = .{ .child = counter.allocator(), .limit = 16 * 1024 * 1024 };
        {
            var spool = Store.init(&budget);
            defer spool.deinit();
            if (mode == 0) {
                var legacy = std.heap.ArenaAllocator.init(budget.allocator());
                defer legacy.deinit();
                _ = try sources.sql_array_wire.toJsonLeaky(legacy.allocator(), value, .{});
            } else {
                try spool.append(.{ .columns = &.{.{ .name = "a", .type = .array, .element_type = .int64 }}, .command_tag = "SELECT", .cells = .{ .context = &reader, .count = 1, .width = 1, .read = Reader.read } }, 1);
                var work: sources.sql_array_value.Budget = .{};
                try std.testing.expectEqual(std.math.Order.eq, try value.compare((try spool.cell(0, 0)).array.?.*, &work));
            }
            peaks[mode] = counter.peak;
            calls[mode] = counter.alloc_calls;
        }
        try std.testing.expectEqual(@as(usize, 0), counter.live);
        try std.testing.expectEqual(@as(usize, 0), budget.used);
    }
    try std.testing.expect(peaks[1] < peaks[0]);
    std.debug.print("PGWIRE_NATIVE_SPOOL elements=4096 before_peak={d} after_peak={d} before_allocations={d} after_allocations={d}\n", .{ peaks[0], peaks[1], calls[0], calls[1] });
}

test "pgwire cursor spool page crossings and failed append preserve published rows" {
    var budget: Budget = .{ .child = std.testing.allocator, .limit = 64 * 1024 };
    var spool = Store.init(&budget);
    defer spool.deinit();
    const columns = &[_]backend.Column{.{ .name = "j", .type = .json }};
    try spool.append(.{ .columns = columns, .rows = &.{&.{.{ .integer = 1 }}}, .command_tag = "SELECT" }, 10);
    try spool.append(.{ .columns = columns, .rows = &.{ &.{.{ .integer = 2 }}, &.{.null} }, .sql_nulls = &.{ &.{false}, &.{true} }, .command_tag = "SELECT" }, 10);
    try std.testing.expectEqual(@as(i64, 2), (try spool.cell(1, 0)).value.integer);
    try std.testing.expectEqual(@as(i64, 1), (try spool.cell(0, 0)).value.integer);
    try std.testing.expect((try spool.cell(2, 0)).sql_null);
    try std.testing.expectError(error.InvalidResult, spool.append(.{ .columns = columns, .rows = &.{ &.{.{ .integer = 3 }}, &.{.{ .integer = 4 }} }, .sql_nulls = &.{ &.{false}, &.{true} }, .command_tag = "SELECT" }, 10));
    try std.testing.expectEqual(@as(usize, 3), spool.row_count);
    try spool.append(.{ .columns = columns, .rows = &.{&.{.{ .integer = 5 }}}, .command_tag = "SELECT" }, 10);
    try std.testing.expectEqual(@as(i64, 5), (try spool.cell(3, 0)).value.integer);
    try std.testing.expectEqual(@as(i64, 1), (try spool.cell(0, 0)).value.integer);
}
