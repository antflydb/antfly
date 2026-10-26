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

//! Connection-budgeted typed cursor rows. Spooling never reexecutes SQL and
//! preserves SQL NULL separately from a JSON null value.
const std = @import("std");
const backend = @import("backend.zig");
const Budget = @import("budget.zig").Budget;
// A page owns its prepared payloads. Type tags live once per column; scalar
// cells remain plain JSON values and native cells retain compact pointers.
const ColumnPayload = union(enum) {
    json: struct { values: []std.json.Value, nulls: []bool, encoded: []?[]const u8, frozen: []?@import("antfly_local_sources").sql_execution_batch.FrozenJson },
    array: []?*const @import("antfly_local_sources").sql_array_value.Value,
    numeric: []?*const @import("antfly_local_sources").sql_numeric_value.Value,
    range: []?*const @import("antfly_local_sources").sql_range_value.Value,
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
    const Key = struct { pointer: usize, length: usize, kind: enum { frozen, json, array, numeric, range }, flags: u8 = 0 };
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
/// Connection-local references share immutable pages between a SQL cursor and
/// an extended FETCH portal. CLOSE releases its reference; a suspended portal
/// retains the already evaluated result. All storage and handle bytes count
/// against the same connection cursor budget. No cross-thread atomics needed.
pub const Shared = struct {
    refs: usize = 1,
    store: Store,
    pub fn create(budget: *Budget) !*Shared {
        const owned = try budget.allocator().create(Shared);
        owned.* = .{ .store = Store.init(budget) };
        return owned;
    }
    pub fn retain(self: *Shared) *Shared {
        self.refs += 1;
        return self;
    }
    pub fn release(self: *Shared) void {
        std.debug.assert(self.refs != 0);
        self.refs -= 1;
        if (self.refs == 0) {
            const alloc = self.store.budget.allocator();
            self.store.deinit();
            alloc.destroy(self);
        }
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
            .range => |values| if (values[index]) |value| .{ .range = value, .sql_null = false } else .{ .sql_null = true },
        };
    }
    pub fn append(self: *Store, result: backend.Result, max_rows: usize) !void {
        const count = result.rowCount();
        if (count > max_rows -| self.row_count) return error.ProgramLimitExceeded;
        if (self.columns) |definitions| {
            if (definitions.len != result.columns.len) return error.InvalidResult;
            for (definitions, result.columns) |old, next| if (!backend.sameColumnType(old, next) or !std.mem.eql(u8, old.name, next.name)) return error.InvalidResult;
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
            payload.* = if (sources.sql_range_value.kindFromType(definition.type) != null) .{ .range = try a.alloc(?*const sources.sql_range_value.Value, count) } else if (definition.type == .array) .{ .array = try a.alloc(?*const sources.sql_array_value.Value, count) } else if (definition.type == .number and definition.element_type == .numeric) .{ .numeric = try a.alloc(?*const sources.sql_numeric_value.Value, count) } else .{ .json = .{ .values = try a.alloc(std.json.Value, count), .nulls = try a.alloc(bool, count), .encoded = if (definition.type == .json) try a.alloc(?[]const u8, count) else &.{}, .frozen = if (definition.type == .json) try a.alloc(?sources.sql_execution_batch.FrozenJson, count) else &.{} } };
        }
        var memo: PayloadMemo = .{};
        for (0..count) |row| for (definitions, page.columns, 0..) |definition, *payload, column| {
            const value = try result.cell(a, row, column);
            if (value.sql_null and (value.value != .null or value.array != null or value.numeric != null or value.range != null or value.json_bytes != null or value.frozen_json != null)) return error.InvalidResult;
            if (value.frozen_json != null and (definition.type != .json or value.value != .null or value.array != null or value.numeric != null or value.range != null or value.json_bytes != null)) return error.InvalidResult;
            switch (payload.*) {
                .json => |data| {
                    if (value.array != null or value.numeric != null or value.range != null) return error.InvalidResult;
                    if (data.frozen.len != 0) data.frozen[row] = null;
                    if (value.frozen_json) |frozen| {
                        var owned = frozen;
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(frozen.bytes.ptr), .length = frozen.bytes.len, .kind = .frozen, .flags = @as(u8, @backingInt(frozen.version)) * 2 + @intFromBool(frozen.lossless_text) };
                        owned.bytes = if (memo.get(key)) |cached| cached.frozen_json.?.bytes else try a.dupe(u8, frozen.bytes);
                        memo.put(key, .{ .frozen_json = owned, .sql_null = false });
                        data.frozen[row] = owned;
                        data.encoded[row] = null;
                        data.values[row] = .null;
                    } else if (value.json_bytes) |bytes| {
                        if (definition.type != .json or value.value != .null) return error.InvalidResult;
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(bytes.ptr), .length = bytes.len, .kind = .json };
                        data.encoded[row] = if (memo.get(key)) |cached| cached.json_bytes.? else try a.dupe(u8, bytes);
                        memo.put(key, .{ .json_bytes = data.encoded[row], .sql_null = false });
                        data.values[row] = .null;
                    } else {
                        if (data.encoded.len != 0) data.encoded[row] = null;
                        data.values[row] = try clone(a, value.value, 0);
                    }
                    data.nulls[row] = value.sql_null;
                },
                .array => |values| {
                    if (value.sql_null) {
                        values[row] = null;
                        continue;
                    }
                    if (value.numeric != null or value.range != null) return error.InvalidResult;
                    var view: ?sources.sql_array_wire.Borrowed = null;
                    defer if (view) |*owned| owned.deinit();
                    const array = if (value.array) |native| native else decoded: {
                        view = try sources.sql_array_wire.decodeBorrowed(a, definition.element_type orelse return error.InvalidResult, value.value, .{});
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
                            const validated = try array.validate(.{});
                            values[row] = (try sources.sql_operators.cloneDatum(a, .{ .array = &validated, .sql_null = false })).array.?;
                            memo.put(key, .{ .array = values[row], .sql_null = false });
                        }
                    } else {
                        const validated = try array.validate(.{});
                        values[row] = (try sources.sql_operators.cloneDatum(a, .{ .array = &validated, .sql_null = false })).array.?;
                    }
                },
                .range => |values| {
                    if (value.sql_null) {
                        values[row] = null;
                        continue;
                    }
                    if (value.array != null or value.numeric != null or value.json_bytes != null or value.frozen_json != null) return error.InvalidResult;
                    const ranges = sources.sql_range_value;
                    var context: ranges.Context = .{ .alloc = a };
                    const kind = ranges.kindFromType(definition.type) orelse return error.InvalidResult;
                    if (value.range) |range| {
                        if (value.value != .null or range.kind != kind) return error.InvalidResult;
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(range), .length = 0, .kind = .range };
                        if (memo.get(key)) |cached| {
                            values[row] = cached.range.?;
                        } else {
                            var canonical = try ranges.canonical(&context, range.*);
                            values[row] = (try sources.sql_operators.cloneDatum(a, sources.sql_scalar.Datum.typedRange(&canonical))).range().?;
                            memo.put(key, .{ .range = values[row], .sql_null = false });
                        }
                    } else {
                        if (value.value != .string) return error.InvalidResult;
                        const parsed = try a.create(ranges.Value);
                        parsed.* = try ranges.parseLeaky(&context, kind, value.value.string);
                        values[row] = parsed;
                    }
                },
                .numeric => |values| {
                    if (value.sql_null) {
                        values[row] = null;
                        continue;
                    }
                    if (value.array != null or value.range != null) return error.InvalidResult;
                    if (value.numeric) |number| {
                        if (value.value != .null) return error.InvalidResult;
                        const key: PayloadMemo.Key = .{ .pointer = @intFromPtr(number), .length = 0, .kind = .numeric };
                        values[row] = if (memo.get(key)) |cached| cached.numeric.? else (try sources.sql_operators.cloneDatum(a, .{ .numeric = number, .sql_null = false })).numeric.?;
                        memo.put(key, .{ .numeric = values[row], .sql_null = false });
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
            defer std.debug.assert(budget.used == 0);
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
            try std.testing.expect(budget.used < 64 * 1024);
            if (measure) std.debug.print("PGWIRE_SHARED_FROZEN rows=64 payload_bytes={d} retained={d} shared=true\n", .{ cell.frozen_json.?.bytes.len, budget.used });
            // PostgreSQL JSONB emits the space after the object colon; the
            // compact API-JSON payload length is not the pgwire frame length.
            var output: [12009]u8 = undefined;
            var writer: std.Io.Writer = .fixed(&output);
            var scratch = std.heap.FixedBufferAllocator.init(&.{});
            try @import("values.zig").encodeCellInto(scratch.allocator(), &writer, .{ .name = "j", .type = .json }, 0, cell, output.len);
            try std.testing.expectEqual(@as(usize, 12009), writer.buffered().len);
            try std.testing.expect(std.mem.startsWith(u8, writer.buffered(), "{\"x\": \""));
            try std.testing.expect(std.mem.endsWith(u8, writer.buffered(), "\"}"));
            try std.testing.expectEqual(@as(u8, 'x'), writer.buffered()[12006]);
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

test "pgwire shared cursor pages retain FETCH rows after CLOSE without copying or escaping the budget" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            var budget: Budget = .{ .child = a, .limit = 1024 * 1024 };
            const cursor = try Shared.create(&budget);
            var cursor_live = true;
            defer if (cursor_live) cursor.release();
            try cursor.store.append(.{
                .columns = &.{.{ .name = "n", .type = .integer }},
                .rows = &.{ &.{.{ .integer = 9007199254740993 }}, &.{.null} },
                .command_tag = "SELECT 2",
            }, 2);
            const used = budget.used;
            // Each portal retains the existing pages in O(1) space; no row,
            // payload, directory or reference allocation occurs on capture.
            var portals: [64]*Shared = undefined;
            for (&portals) |*portal| portal.* = cursor.retain();
            try std.testing.expectEqual(used, budget.used);
            cursor_live = false;
            cursor.release();
            for (portals) |portal| {
                const cell = try portal.store.cell(0, 0);
                try std.testing.expectEqual(@as(i64, 9007199254740993), cell.value.integer);
                try std.testing.expect(!cell.sql_null);
                try std.testing.expect((try portal.store.cell(1, 0)).sql_null);
                portal.release();
            }
            try std.testing.expectEqual(@as(usize, 0), budget.used);
        }
    };
    try Check.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "pgwire cursor spool owns native range endpoints and preserves empty and SQL NULL" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    const ranges = sources.sql_range_value;
    var budget: Budget = .{ .child = a, .limit = 1024 * 1024 };
    var spool = Store.init(&budget);
    defer spool.deinit();
    {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var context: ranges.Context = .{ .alloc = arena.allocator() };
        var range = try ranges.parseLeaky(&context, .numrange, "[1.12345678901234567890,99.001)");
        const Rows = struct {
            range: *const ranges.Value,
            fn read(raw: *anyopaque, _: std.mem.Allocator, row: usize, _: usize) !backend.Cell {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return if (row == 0) .{ .range = self.range, .sql_null = false } else .{ .sql_null = true };
            }
        };
        var rows: Rows = .{ .range = &range };
        try spool.append(.{ .columns = &.{.{ .name = "r", .type = .numrange }}, .cells = .{ .context = &rows, .count = 2, .width = 1, .read = Rows.read }, .command_tag = "SELECT 2" }, 3);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var context: ranges.Context = .{ .alloc = arena.allocator() };
    try std.testing.expectEqualStrings("[1.12345678901234567890,99.001)", try ranges.formatLeaky(&context, (try spool.cell(0, 0)).range.?.*));
    try std.testing.expect((try spool.cell(1, 0)).sql_null);
    try spool.append(.{ .columns = &.{.{ .name = "r", .type = .numrange }}, .rows = &.{&.{.{ .string = "empty" }}}, .command_tag = "SELECT 1" }, 3);
    try std.testing.expect((try spool.cell(2, 0)).range.?.empty);
}
