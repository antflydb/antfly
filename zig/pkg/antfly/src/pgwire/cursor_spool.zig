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
const Work = @import("antfly_local_sources").sql_operator_work;
const backend = @import("backend.zig");
const Budget = @import("budget.zig").Budget;
// A page owns its prepared payloads. Type tags live once per column; scalar
// cells remain plain JSON values and native cells retain compact pointers.
const ColumnPayload = union(enum) {
    json: struct { values: []std.json.Value, nulls: []bool, encoded: []?[]const u8, frozen: []?@import("antfly_local_sources").sql_execution_batch.FrozenJson },
    array: []?*const @import("antfly_local_sources").sql_array_value.Value,
    numeric: []?*const @import("antfly_local_sources").sql_numeric_value.Value,
};
const Page = struct {
    arena: std.heap.ArenaAllocator,
    begin: usize,
    count: usize,
    columns: []ColumnPayload,
};
// Identity is valid only during one append lease. Entries point to page-owned
// copies; the fixed table bounds scratch independently of page cardinality.
const PayloadMemo = struct {
    const Key = struct { pointer: usize, length: usize, kind: enum { frozen, json, array, numeric }, flags: u8 = 0 };
    const Entry = struct { key: Key, value: backend.Cell };
    entries: [64]?Entry = @splat(null),
    fn slot(key: Key) usize {
        var hash = std.hash.Wyhash.init(0);
        std.hash.autoHash(&hash, key);
        return @intCast((hash.final() % 32) * 2);
    }
    fn get(self: *const PayloadMemo, key: Key) ?backend.Cell {
        for (self.entries[slot(key)..][0..2]) |optional| if (optional) |entry| {
            if (std.meta.eql(entry.key, key)) return entry.value;
        };
        return null;
    }
    fn put(self: *PayloadMemo, key: Key, value: backend.Cell) void {
        for (self.entries[slot(key)..][0..2]) |*entry| {
            if (entry.* == null or std.meta.eql(entry.*.?.key, key)) {
                entry.* = .{ .key = key, .value = value };
                return;
            }
        }
        self.entries[slot(key)] = .{ .key = key, .value = value };
    }
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
            .json => |data| if (data.frozen.len != 0 and data.frozen[index] != null) .{ .frozen_json = data.frozen[index], .sql_null = false } else if (data.encoded.len != 0 and data.encoded[index] != null) .{ .json_bytes = data.encoded[index], .sql_null = false } else .{ .value = data.values[index], .sql_null = data.nulls[index] },
            .array => |values| if (values[index]) |value| .{ .array = value, .sql_null = false } else .{ .sql_null = true },
            .numeric => |values| if (values[index]) |value| .{ .numeric = value, .sql_null = false } else .{ .sql_null = true },
        };
    }
    pub fn append(self: *Store, result: backend.Result, max_rows: usize) !void {
        return self.appendControlled(result, max_rows, null);
    }
    pub fn appendControlled(self: *Store, result: backend.Result, max_rows: usize, control: ?backend.ReadControl) !void {
        try Work.poll(control);
        const count = result.rowCount();
        if (count > max_rows -| self.row_count) return error.ProgramLimitExceeded;
        if (self.columns) |definitions| {
            if (definitions.len != result.columns.len) return error.InvalidResult;
            for (definitions, result.columns) |old, next| {
                try Work.poll(control);
                if (old.type != next.type or old.element_type != next.element_type or !std.meta.eql(old.numeric_modifier, next.numeric_modifier) or !try Work.equalBytes(control, old.name, next.name)) return error.InvalidResult;
            }
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
            const copied = try Work.dupe(backend.Column, a, result.columns, control);
            for (copied) |*definition| definition.name = try Work.dupe(u8, a, definition.name, control);
            break :owned copied;
        };
        page.columns = try a.alloc(ColumnPayload, definitions.len);
        const sources = @import("antfly_local_sources");
        for (definitions, page.columns) |definition, *payload| {
            try Work.poll(control);
            _ = try @import("values.zig").columnOid(definition);
            payload.* = if (definition.type == .array) .{ .array = try a.alloc(?*const sources.sql_array_value.Value, count) } else if (definition.type == .number and definition.element_type == .numeric) .{ .numeric = try a.alloc(?*const sources.sql_numeric_value.Value, count) } else .{ .json = .{ .values = try a.alloc(std.json.Value, count), .nulls = try a.alloc(bool, count), .encoded = if (definition.type == .json) try a.alloc(?[]const u8, count) else &.{}, .frozen = if (definition.type == .json) try a.alloc(?sources.sql_execution_batch.FrozenJson, count) else &.{} } };
        }
        var memo: PayloadMemo = .{};
        for (0..count) |row| for (definitions, page.columns, 0..) |definition, *payload, column| {
            try Work.poll(control);
            var numeric_context: sources.sql_numeric_value.Context = .{ .alloc = a, .remaining = std.math.maxInt(u64), .ptr = if (control) |owner| owner.ptr else null, .checkpoint = if (control) |owner| owner.checkpoint else null };
            const parent = if (control != null) &numeric_context else null;
            const value = try result.cellControlled(a, row, column, control);
            if (value.sql_null and (value.value != .null or value.array != null or value.numeric != null or value.json_bytes != null or value.frozen_json != null)) return error.InvalidResult;
            if (value.frozen_json != null and (definition.type != .json or value.value != .null or value.array != null or value.numeric != null or value.json_bytes != null)) return error.InvalidResult;
            switch (payload.*) {
                .json => |data| {
                    if (value.array != null or value.numeric != null) return error.InvalidResult;
                    if (data.frozen.len != 0) data.frozen[row] = null;
                    if (value.frozen_json) |frozen| {
                        var owned = frozen;
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(frozen.bytes.ptr), .length = frozen.bytes.len, .kind = .frozen, .flags = @as(u8, @backingInt(frozen.version)) * 2 + @intFromBool(frozen.lossless_text) };
                        owned.bytes = if (memo.get(key)) |cached| cached.frozen_json.?.bytes else try Work.dupe(u8, a, frozen.bytes, control);
                        memo.put(key, .{ .frozen_json = owned, .sql_null = false });
                        data.frozen[row] = owned;
                        data.encoded[row] = null;
                        data.values[row] = .null;
                    } else if (value.json_bytes) |bytes| {
                        if (definition.type != .json or value.value != .null) return error.InvalidResult;
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(bytes.ptr), .length = bytes.len, .kind = .json };
                        data.encoded[row] = if (memo.get(key)) |cached| cached.json_bytes.? else try Work.dupe(u8, a, bytes, control);
                        memo.put(key, .{ .json_bytes = data.encoded[row], .sql_null = false });
                        data.values[row] = .null;
                    } else {
                        if (data.encoded.len != 0) data.encoded[row] = null;
                        data.values[row] = try cloneControlled(a, value.value, 0, control);
                    }
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
                        view = try sources.sql_array_wire.decodeBorrowed(a, definition.element_type orelse return error.InvalidResult, value.value, .{ .context = parent });
                        break :decoded &view.?.value;
                    };
                    if ((value.array != null and value.value != .null) or array.element_type != definition.element_type) return error.InvalidResult;
                    if (value.array) |native| {
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(native), .length = 0, .kind = .array };
                        if (memo.get(key)) |cached| {
                            // One immutable append lease owns this identity.
                            // Shape/type checks above still apply to every cell.
                            values[row] = cached.array.?;
                        } else {
                            var array_work: sources.sql_array_value.Budget = .{ .remaining = (sources.sql_array_value.Limits{}).work, .shared = parent };
                            const validated = try array.validateWithBudget(.{}, &array_work);
                            values[row] = (try sources.sql_operators.cloneDatumControlled(a, .{ .array = &validated, .sql_null = false }, control)).array.?;
                            memo.put(key, .{ .array = values[row], .sql_null = false });
                        }
                    } else {
                        var array_work: sources.sql_array_value.Budget = .{ .remaining = (sources.sql_array_value.Limits{}).work, .shared = parent };
                        const validated = try array.validateWithBudget(.{}, &array_work);
                        values[row] = (try sources.sql_operators.cloneDatumControlled(a, .{ .array = &validated, .sql_null = false }, control)).array.?;
                    }
                },
                .numeric => |values| {
                    if (value.sql_null) {
                        values[row] = null;
                        continue;
                    }
                    if (value.array != null) return error.InvalidResult;
                    if (value.numeric) |number| {
                        if (value.value != .null) return error.InvalidResult;
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(number), .length = 0, .kind = .numeric };
                        values[row] = if (memo.get(key)) |cached| cached.numeric.? else (try sources.sql_operators.cloneDatumControlled(a, .{ .numeric = number, .sql_null = false }, control)).numeric.?;
                        memo.put(key, .{ .numeric = values[row], .sql_null = false });
                    } else {
                        if (value.value != .string) return error.InvalidResult;
                        var context: sources.sql_numeric_value.Context = .{ .alloc = a, .ptr = if (control) |owner| owner.ptr else null, .checkpoint = if (control) |owner| owner.checkpoint else null };
                        const parsed = try sources.sql_numeric_value.parse(&context, value.value.string);
                        const number = try a.create(sources.sql_numeric_value.Value);
                        number.* = parsed.value;
                        values[row] = number;
                    }
                },
            }
        };
        try Work.poll(control);
        // Publish only after the entire page validates and owns its payloads.
        self.pages.appendAssumeCapacity(page);
        self.columns = definitions;
        self.row_count += count;
    }
};

/// Deep copy into an arena; partial allocation is reclaimed by that arena.
/// Used by both cursor spooling and stream request snapshots.
pub fn cloneValue(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    return clone(alloc, value, 0);
}

fn clone(alloc: std.mem.Allocator, value: std.json.Value, depth: usize) !std.json.Value {
    return cloneControlled(alloc, value, depth, null);
}
fn cloneControlled(alloc: std.mem.Allocator, value: std.json.Value, depth: usize, control: ?backend.ReadControl) !std.json.Value {
    try Work.poll(control);
    if (depth > 128) return error.ProgramLimitExceeded;
    return switch (value) {
        .string => |text| .{ .string = try Work.dupe(u8, alloc, text, control) },
        .number_string => |text| .{ .number_string = try Work.dupe(u8, alloc, text, control) },
        .array => |array| blk: {
            var result = std.array_list.Managed(std.json.Value).init(alloc);
            for (array.items) |element| try result.append(try cloneControlled(alloc, element, depth + 1, control));
            break :blk .{ .array = result };
        },
        .object => |object| blk: {
            var result: std.json.ObjectMap = .empty;
            var iterator = object.iterator();
            while (iterator.next()) |entry| try result.put(alloc, try Work.dupe(u8, alloc, entry.key_ptr.*, control), try cloneControlled(alloc, entry.value_ptr.*, depth + 1, control));
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
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
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
                const result: backend.Result = .{ .columns = &columns, .command_tag = "SELECT", .cells = .{ .context = &reader, .count = 3, .width = 2, .read = Reader.read } };
                try spool.append(result, 8);
                const copied = (try spool.cell(0, 0)).array.?;
                try std.testing.expect(copied != &array.value);
                try std.testing.expect(copied == (try spool.cell(2, 0)).array.?);
                try std.testing.expect((try spool.cell(0, 1)).numeric.? == (try spool.cell(2, 1)).numeric.?);
                try std.testing.expectEqual(@as(i32, 0), copied.dimensions[0].lower);
                try std.testing.expect(copied.elements[1].sql_null);
                var wrong = columns;
                wrong[0].element_type = .int32;
                var changed = result;
                changed.columns = &wrong;
                try std.testing.expectError(error.InvalidResult, spool.append(changed, 8));
            }
            try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
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
        try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
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

test "pgwire encoded JSONB spool owns bytes and preserves SQL and JSON nulls on every unwind" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var budget: Budget = .{ .child = a, .limit = 1 << 20 };
            var store = Store.init(&budget);
            defer store.deinit();
            var bytes: [7]u8 = "{\"x\":1}".*;
            const Reader = struct {
                bytes: []const u8,
                fn read(raw: *anyopaque, _: std.mem.Allocator, row: usize, _: usize) !backend.Cell {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    return switch (row) {
                        0 => .{ .json_bytes = self.bytes, .sql_null = false },
                        1 => .{ .value = .null, .sql_null = false },
                        else => .{ .sql_null = true },
                    };
                }
            };
            var reader: Reader = .{ .bytes = &bytes };
            try store.append(.{ .columns = &.{.{ .name = "j", .type = .json }}, .cells = .{ .context = &reader, .count = 3, .width = 1, .read = Reader.read }, .command_tag = "SELECT" }, 3);
            bytes[5] = '7';
            try std.testing.expectEqualStrings("{\"x\":1}", (try store.cell(0, 0)).json_bytes.?);
            try std.testing.expect(!(try store.cell(1, 0)).sql_null);
            try std.testing.expect((try store.cell(2, 0)).sql_null);
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "pgwire cursor spool owns frozen JSON leases and unwinds allocation failures" {
    const Fixture = struct {
        value: @import("antfly_local_sources").sql_execution_batch.FrozenJson,
        fn read(raw: *anyopaque, _: std.mem.Allocator, row: usize, _: usize) !backend.Cell {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (row < 64) .{ .frozen_json = self.value, .sql_null = false } else .{ .sql_null = true };
        }
        fn run(a: std.mem.Allocator, measure: bool) !void {
            var budget: Budget = .{ .child = a, .limit = 1 << 20 };
            defer std.debug.assert(budget.used.load(.acquire) == 0);
            var store = Store.init(&budget);
            defer store.deinit();
            const sources = @import("antfly_local_sources");
            {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const sa = arena.allocator();
                const body = try sa.alloc(u8, 12000);
                @memset(body, 'x');
                var object: std.json.ObjectMap = .empty;
                try object.put(sa, "x", .{ .string = body });
                const root: std.json.Value = .{ .object = object };
                const wire = try sources.sql_spill.encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{sources.sql_scalar.Datum.json(root)}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                const block = try sources.sql_spill.decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
                defer block.deinit();
                var fixture: @This() = .{ .value = (try block.frozenJson(0, 0, false)).? };
                try store.append(.{ .columns = &.{.{ .name = "j", .type = .json }}, .cells = .{ .context = &fixture, .count = 65, .width = 1, .read = read }, .command_tag = "SELECT" }, 65);
            }
            const cell = try store.cell(0, 0);
            try std.testing.expect(cell.frozen_json != null);
            try std.testing.expect((try store.cell(64, 0)).sql_null);
            try std.testing.expect(cell.frozen_json.?.bytes.ptr == (try store.cell(63, 0)).frozen_json.?.bytes.ptr);
            try std.testing.expect(budget.used.load(.acquire) < 64 * 1024);
            if (measure) std.debug.print("PGWIRE_SHARED_FROZEN rows=64 payload_bytes={d} retained={d} shared=true\n", .{ cell.frozen_json.?.bytes.len, budget.used.load(.acquire) });
            var output: [12008]u8 = undefined;
            var writer: std.Io.Writer = .fixed(&output);
            var scratch = std.heap.FixedBufferAllocator.init(&.{});
            try @import("values.zig").encodeCellInto(scratch.allocator(), &writer, .{ .name = "j", .type = .json }, 0, cell, output.len);
            try std.testing.expectEqual(@as(usize, 12008), writer.buffered().len);
            try std.testing.expectEqual(@as(u8, 'x'), writer.buffered()[12005]);
        }
    };
    try Fixture.run(std.testing.allocator, true);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{false});
}

test "pgwire prepared array validation memo checks each cell and expires at append boundary" {
    const sources = @import("antfly_local_sources");
    const Element = sources.sql_array_value.Element;
    var elements = [_]Element{Element.json(.{ .integer = 7 })};
    const array = try sources.sql_array_value.Value.init(.int64, &.{.{ .length = 1 }}, &elements, .{});
    const Reader = struct {
        array: *const sources.sql_array_value.Value,
        malformed_second: bool = false,
        fn read(raw: *anyopaque, _: std.mem.Allocator, row: usize, _: usize) !backend.Cell {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .array = self.array, .sql_null = false, .value = if (self.malformed_second and row == 1) .{ .integer = 1 } else .null };
        }
    };
    var reader: Reader = .{ .array = &array };
    var budget: Budget = .{ .child = std.testing.allocator, .limit = 1024 * 1024 };
    var spool = Store.init(&budget);
    defer spool.deinit();
    const result: backend.Result = .{ .columns = &.{.{ .name = "a", .type = .array, .element_type = .int64 }}, .command_tag = "SELECT", .cells = .{ .context = &reader, .count = 2, .width = 1, .read = Reader.read } };
    try spool.append(result, 100);
    try std.testing.expect((try spool.cell(0, 0)).array.? == (try spool.cell(1, 0)).array.?);
    reader.malformed_second = true;
    try std.testing.expectError(error.InvalidResult, spool.append(result, 100));
    try std.testing.expectEqual(@as(usize, 2), spool.row_count);
    reader.malformed_second = false;
    // Reusing a source address in a new lease must not reuse prior validation.
    elements[0] = Element.json(.{ .string = "invalid" });
    try std.testing.expectError(error.SqlTypeMismatch, spool.append(result, 100));
    try std.testing.expectEqual(@as(usize, 2), spool.row_count);
    try std.testing.expectEqual(@as(i64, 7), (try spool.cell(0, 0)).array.?.flatCell(0).value.integer);
}

test "pgwire prepared array validation memo measurement" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    const elements = try a.alloc(sources.sql_array_value.Element, 65536);
    defer a.free(elements);
    for (elements, 0..) |*element, i| element.* = .{ .value = .{ .integer = @intCast(i) }, .sql_null = false };
    const array: sources.sql_array_value.Value = .{ .element_type = .int64, .dimensions = &.{.{ .length = 65536 }}, .elements = elements };
    const Reader = struct {
        array: *const sources.sql_array_value.Value,
        fn read(raw: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !backend.Cell {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .array = self.array, .sql_null = false };
        }
    };
    var reader: Reader = .{ .array = &array };
    for ([_]usize{ 1, 64 }) |count| {
        var elapsed: i128 = 0;
        for (0..5) |_| {
            var budget: Budget = .{ .child = a, .limit = 32 * 1024 * 1024 };
            var spool = Store.init(&budget);
            defer spool.deinit();
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            try spool.append(.{ .columns = &.{.{ .name = "a", .type = .array, .element_type = .int64 }}, .command_tag = "SELECT", .cells = .{ .context = &reader, .count = count, .width = 1, .read = Reader.read } }, count);
            elapsed += std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
            try std.testing.expect((try spool.cell(0, 0)).array.? == (try spool.cell(count - 1, 0)).array.?);
        }
        std.debug.print("PGWIRE_PREPARED_ARRAY_MEMO rows={} elements=65536 iterations=5 elapsed_ns={} one_owned_copy=true\n", .{ count, elapsed });
    }
}

test "pgwire consumer spool cancels copies before publication and owns typed retry payloads" {
    const Fixture = struct {
        fn run(backing: std.mem.Allocator) !void {
            var source = std.heap.ArenaAllocator.init(backing);
            defer source.deinit();
            const a = source.allocator();
            const sources = @import("antfly_local_sources");
            const body = try a.alloc(u8, 32768);
            @memset(body, 'x');
            body[0] = '"';
            body[body.len - 1] = '"';
            const array = try sources.sql_array_value.Value.init(.text, &.{.{ .length = 1, .lower = -2 }}, &.{.{ .value = .{ .string = body }, .sql_null = false }}, .{});
            var context: sources.sql_numeric_value.Context = .{ .alloc = a };
            const number = try sources.sql_numeric_value.parse(&context, "12345678901234567890.125");
            const State = struct {
                body: []const u8,
                array: *const sources.sql_array_value.Value,
                numeric: *const sources.sql_numeric_value.Value,
                reads: usize = 0,
                after_read: usize = 0,
                stop: usize = std.math.maxInt(usize),
                fn checkpoint(raw: ?*anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw.?));
                    if (self.reads != 0) {
                        self.after_read += 1;
                        if (self.after_read == self.stop) return error.Cancelled;
                    }
                }
                fn read(_: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !backend.Cell {
                    return error.UncontrolledCellRead;
                }
                fn readControlled(raw: *anyopaque, _: std.mem.Allocator, _: usize, column: usize, control: ?backend.ReadControl) !backend.Cell {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    const owner = control orelse return error.MissingConsumer;
                    try owner.checkpoint(owner.ptr);
                    self.reads += 1;
                    return switch (column) {
                        0 => .{ .json_bytes = self.body, .sql_null = false },
                        1 => .{ .value = .{ .string = self.body }, .sql_null = false },
                        2 => .{ .array = self.array, .sql_null = false },
                        3 => .{ .numeric = self.numeric, .sql_null = false },
                        else => .{ .sql_null = true },
                    };
                }
                fn asControl(self: *@This()) backend.ReadControl {
                    return .{ .ptr = self, .checkpoint = checkpoint };
                }
            };
            var state: State = .{ .body = body, .array = &array, .numeric = &number.value, .stop = 8 };
            const result: backend.Result = .{ .columns = &.{ .{ .name = "j", .type = .json }, .{ .name = "s", .type = .string }, .{ .name = "a", .type = .array, .element_type = .text }, .{ .name = "n", .type = .number, .element_type = .numeric }, .{ .name = "null", .type = .json } }, .cells = .{ .context = &state, .count = 1, .width = 5, .read = State.read, .read_controlled = State.readControlled }, .command_tag = "SELECT" };
            var budget: Budget = .{ .child = backing, .limit = 1 << 22 };
            defer std.debug.assert(budget.used.load(.acquire) == 0);
            var spool = Store.init(&budget);
            defer spool.deinit();
            try spool.pages.ensureUnusedCapacity(budget.allocator(), 1);
            const baseline = budget.used.load(.acquire);
            if (spool.appendControlled(result, 1, state.asControl())) |_| {
                return error.TestUnexpectedResult;
            } else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.Cancelled, err);
            }
            try std.testing.expectEqual(@as(usize, 8), state.after_read);
            try std.testing.expectEqual(@as(usize, 0), spool.row_count);
            try std.testing.expectEqual(@as(usize, 0), spool.pages.items.len);
            try std.testing.expect(spool.columns == null);
            try std.testing.expectEqual(baseline, budget.used.load(.acquire));
            state.reads = 0;
            state.after_read = 0;
            state.stop = std.math.maxInt(usize);
            try spool.appendControlled(result, 1, state.asControl());
            try std.testing.expectEqual(@as(usize, 5), state.reads);
            @memset(body, 'z');
            try std.testing.expectEqual(@as(u8, 'x'), (try spool.cell(0, 0)).json_bytes.?[16000]);
            try std.testing.expectEqual(@as(u8, 'x'), (try spool.cell(0, 1)).value.string[16000]);
            const copied_array = (try spool.cell(0, 2)).array.?;
            try std.testing.expectEqual(@as(i32, -2), copied_array.dimensions[0].lower);
            try std.testing.expectEqual(@as(u8, 'x'), copied_array.flatCell(0).value.string[16000]);
            const copied_number = (try spool.cell(0, 3)).numeric.?;
            try std.testing.expectEqualSlices(u16, number.value.digits, copied_number.digits);
            try std.testing.expect((try spool.cell(0, 4)).sql_null);
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
