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

//! Column-oriented retained SQL state. Primitive values never retain Datum
//! tags; complex or heterogeneous columns retain native JSON values.
//! Datum rows are reconstructed only at an expression/result boundary. Validity is a packed SQL-null bitmap.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;
const Dictionary = struct {
    values: std.ArrayList([]const u8) = .empty,
    indices: std.ArrayList(u32) = .empty,
    lookup: std.StringHashMapUnmanaged(u32) = .empty,
    flat: ?std.ArrayList([]const u8) = null,
    const empty: Dictionary = .{};
    fn deinit(self: *Dictionary, a: A) void {
        self.values.deinit(a);
        self.indices.deinit(a);
        self.lookup.deinit(a);
        if (self.flat) |*values| values.deinit(a);
    }
    fn append(self: *Dictionary, a: A, owned: A, value: ?[]const u8) !void {
        // High-cardinality columns revert to flat storage after a bounded
        // sample; dictionary overhead must not penalize unique identifiers.
        if (self.flat == null and self.values.items.len >= 512 and self.values.items.len > self.indices.items.len / 2) {
            var flat: std.ArrayList([]const u8) = .empty;
            errdefer flat.deinit(a);
            try flat.ensureTotalCapacity(a, self.indices.items.len + 1);
            for (self.indices.items) |index| flat.appendAssumeCapacity(self.values.items[index]);
            self.values.clearAndFree(a);
            self.indices.clearAndFree(a);
            self.lookup.clearAndFree(a);
            self.flat = flat;
        }
        if (self.flat) |*values| {
            try values.append(a, if (value) |bytes| try owned.dupe(u8, bytes) else &.{});
            return;
        }
        const text = value orelse {
            try self.indices.append(a, 0);
            return;
        };
        if (self.lookup.get(text)) |index| {
            try self.indices.append(a, index);
            return;
        }
        const index = std.math.cast(u32, self.values.items.len) orelse return error.SqlProgramLimitExceeded;
        const bytes = try owned.dupe(u8, text);
        try self.values.append(a, bytes);
        try self.lookup.put(a, bytes, index);
        try self.indices.append(a, index);
    }
    fn resizeNulls(self: *Dictionary, a: A, count: usize) !void {
        try self.indices.resize(a, count);
        @memset(self.indices.items, 0);
    }
    fn getText(self: Dictionary, row: usize) []const u8 {
        return if (self.flat) |values| values.items[row] else self.values.items[self.indices.items[row]];
    }
};
const Column = struct {
    const Values = union(enum) {
        unknown,
        integers: std.ArrayList(i64),
        numbers: std.ArrayList(f64),
        booleans: std.ArrayList(bool),
        strings: Dictionary,
        decimals: Dictionary,
        encoded: std.ArrayList(std.json.Value),
    };
    values: Values = .unknown,
    nulls: std.ArrayList(u64) = .empty,
    patterns: std.ArrayList(?*scalar.PatternSet) = .empty,
    fn deinit(self: *Column, a: A) void {
        switch (self.values) {
            .unknown => {},
            inline else => |*values| values.deinit(a),
        }
        self.nulls.deinit(a);
        self.patterns.deinit(a);
    }
    fn isNull(self: Column, row: usize) bool {
        return self.nulls.items[row / 64] & (@as(u64, 1) << @as(u6, @intCast(row % 64))) != 0;
    }
    fn cell(self: Column, a: A, row: usize) !Datum {
        if (self.isNull(row)) return .{};
        _ = a;
        const value: std.json.Value = switch (self.values) {
            .unknown => return error.InvalidSqlBackendResponse,
            .integers => |v| .{ .integer = v.items[row] },
            .numbers => |v| .{ .float = v.items[row] },
            .booleans => |v| .{ .bool = v.items[row] },
            .strings => |v| .{ .string = v.getText(row) },
            .decimals => |v| .{ .number_string = v.getText(row) },
            .encoded => |v| v.items[row],
        };
        return .{ .value = value, .sql_null = false, .patterns = if (self.patterns.items.len == 0) null else self.patterns.items[row] };
    }
    fn append(self: *Column, a: A, owned: A, scratch: A, row: usize, value: Datum) !void {
        if (row % 64 == 0) try self.nulls.append(a, 0);
        if (value.sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
        if (self.patterns.items.len != 0 or value.patterns != null) {
            if (self.patterns.items.len == 0) {
                try self.patterns.resize(a, row);
                @memset(self.patterns.items, null);
            }
            try self.patterns.append(a, value.patterns);
        }
        if (self.values == .unknown and !value.sql_null) {
            self.values = switch (value.value) {
                .integer => .{ .integers = .empty },
                .float => .{ .numbers = .empty },
                .bool => .{ .booleans = .empty },
                .string => .{ .strings = .empty },
                .number_string => .{ .decimals = .empty },
                else => .{ .encoded = .empty },
            };
            switch (self.values) {
                .unknown => unreachable,
                .strings, .decimals => |*values| try values.resizeNulls(a, row),
                inline else => |*values| {
                    try values.resize(a, row);
                    @memset(values.items, switch (@typeInfo(@TypeOf(values.items)).pointer.child) {
                        i64, f64 => 0,
                        bool => false,
                        std.json.Value => .null,
                        else => &.{},
                    });
                },
            }
        }
        const compatible = value.sql_null or switch (self.values) {
            .unknown => true,
            .integers => value.value == .integer,
            .numbers => value.value == .float,
            .booleans => value.value == .bool,
            .strings => value.value == .string,
            .decimals => value.value == .number_string,
            .encoded => true,
        };
        if (!compatible) {
            var encoded: std.ArrayList(std.json.Value) = .empty;
            errdefer encoded.deinit(a);
            try encoded.ensureTotalCapacity(a, row + 1);
            for (0..row) |i| {
                const prior = try self.cell(scratch, i);
                encoded.appendAssumeCapacity(if (prior.sql_null) .null else prior.value);
            }
            switch (self.values) {
                .unknown => unreachable,
                inline else => |*values| values.deinit(a),
            }
            self.values = .{ .encoded = encoded };
        }
        switch (self.values) {
            .unknown => {},
            .integers => |*v| try v.append(a, if (value.sql_null) 0 else value.value.integer),
            .numbers => |*v| try v.append(a, if (value.sql_null) 0 else value.value.float),
            .booleans => |*v| try v.append(a, !value.sql_null and value.value.bool),
            .strings => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.string),
            .decimals => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.number_string),
            .encoded => |*v| try v.append(a, if (value.sql_null) .null else (try @import("operators.zig").cloneDatum(owned, value)).value),
        }
    }
};
pub const Store = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    columns: []Column = &.{},
    len: usize = 0,
    initialized: bool = false,
    failed: bool = false,
    pub fn init(a: A) Store {
        return .{ .a = a, .arena = .init(a) };
    }
    pub fn deinit(self: *Store) void {
        for (self.columns) |*column| column.deinit(self.a);
        self.a.free(self.columns);
        self.arena.deinit();
    }
    /// Conservative admission estimate that recognizes already retained
    /// dictionary bytes. Capacity growth is still enforced by the allocator.
    pub fn appendBytes(self: *const Store, values: []const Datum) !usize {
        var bytes: usize = 0;
        for (values, 0..) |value, index| {
            var repeated = false;
            if (!value.sql_null and index < self.columns.len) {
                const column = self.columns[index];
                if (column.values == .strings and value.value == .string) repeated = column.values.strings.lookup.contains(value.value.string);
                if (column.values == .decimals and value.value == .number_string) repeated = column.values.decimals.lookup.contains(value.value.number_string);
            }
            bytes +|= if (repeated) @sizeOf(Datum) else try @import("operators.zig").datumBytes(value);
        }
        return bytes;
    }
    /// Column-major payload admission. No per-row Datum slices or arenas;
    /// primitive and dictionary state is retained directly in each column.
    pub fn appendBatch(self: *Store, batch: @import("execution_batch.zig").Batch) !void {
        if (self.failed or (self.initialized and self.columns.len != batch.width())) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (!self.initialized) {
            self.columns = try self.a.alloc(Column, batch.width());
            @memset(self.columns, .{});
            self.initialized = true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.columns, 0..) |*column, ordinal| for (0..batch.len()) |index| {
            _ = scratch.reset(.retain_capacity);
            const value = try batch.cell(scratch.allocator(), index, ordinal);
            try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, value);
        };
        self.len += batch.len();
    }
    pub fn append(self: *Store, values: []const Datum) !usize {
        if (self.failed or (self.initialized and values.len != self.columns.len)) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (!self.initialized) {
            self.columns = try self.a.alloc(Column, values.len);
            @memset(self.columns, .{});
            self.initialized = true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.columns, values) |*column, value| try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len, value);
        const index = self.len;
        self.len += 1;
        return index;
    }
    pub fn appendRagged(self: *Store, values: []const Datum) !usize {
        if (!self.initialized or values.len == self.columns.len) return self.append(values);
        errdefer self.failed = true;
        if (values.len > self.columns.len) {
            const wider = try self.a.alloc(Column, values.len);
            @memcpy(wider[0..self.columns.len], self.columns);
            @memset(wider[self.columns.len..], .{});
            const prior_width = self.columns.len;
            self.a.free(self.columns);
            self.columns = wider;
            for (self.columns[prior_width..]) |*column| for (0..self.len) |index| try column.append(self.a, self.arena.allocator(), self.a, index, .{});
        }
        const padded = try self.a.alloc(Datum, self.columns.len);
        defer self.a.free(padded);
        @memset(padded, .{});
        @memcpy(padded[0..values.len], values);
        return self.append(padded);
    }
    pub fn rowWidth(self: *const Store, a: A, index: usize, width: usize) ![]const Datum {
        if (self.failed or index >= self.len or width > self.columns.len) return error.InvalidSqlBackendResponse;
        const result = try a.alloc(Datum, width);
        errdefer a.free(result);
        for (result, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return result;
    }
    pub fn cell(self: *const Store, a: A, index: usize, column: usize) !Datum {
        if (self.failed or index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        return self.columns[column].cell(a, index);
    }
    pub fn row(self: *const Store, a: A, index: usize) ![]const Datum {
        if (self.failed or index >= self.len) return error.InvalidSqlBackendResponse;
        const result = try a.alloc(Datum, self.columns.len);
        errdefer a.free(result);
        for (result, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return result;
    }
    pub fn equal(self: *const Store, a: A, row_index: usize, values: []const Datum, null_equal: bool) !bool {
        if (self.failed or values.len != self.columns.len or row_index >= self.len) return error.InvalidSqlBackendResponse;
        for (values, 0..) |value, column| {
            const stored = self.columns[column];
            const is_null = stored.isNull(row_index);
            if (is_null or value.sql_null) {
                if (!null_equal or is_null != value.sql_null) return false;
                continue;
            }
            // Dispatch on the retained physical type, without constructing a
            // Datum or invoking JSON comparison for homogeneous primitive keys.
            const equal_ = switch (stored.values) {
                .integers => |v| if (value.value == .integer) v.items[row_index] == value.value.integer else null,
                .numbers => |v| if (value.value == .float and std.math.isFinite(v.items[row_index]) and std.math.isFinite(value.value.float)) v.items[row_index] == value.value.float else null,
                .booleans => |v| if (value.value == .bool) v.items[row_index] == value.value.bool else null,
                .strings => |v| if (value.value == .string) std.mem.eql(u8, v.getText(row_index), value.value.string) else null,
                else => null,
            };
            if (equal_) |matches| {
                if (!matches) return false;
            } else if ((try scalar.compare((try stored.cell(a, row_index)).value, value.value)) != .eq) return false;
        }
        return true;
    }
};
test "SQL typed store preserves bitmaps ownership mixed exact numerics and JSON null" {
    const a = std.testing.allocator;
    var store = Store.init(a);
    defer store.deinit();
    for (0..130) |i| _ = try store.append(&.{ if (i % 7 == 0) .{} else Datum.json(.{ .integer = @intCast(i) }), Datum.json(.{ .string = "owned" }) });
    _ = try store.append(&.{ Datum.json(.{ .float = 1.5 }), Datum.json(.null) });
    _ = try store.append(&.{ Datum.json(.{ .integer = 9007199254740993 }), .{} });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (0..130) |i| {
        const value = try store.cell(arena.allocator(), i, 0);
        try std.testing.expectEqual(i % 7 == 0, value.sql_null);
        if (!value.sql_null) try std.testing.expectEqual(@as(i64, @intCast(i)), value.value.integer);
    }
    try std.testing.expect(!(try store.cell(arena.allocator(), 130, 1)).sql_null);
    try std.testing.expect((try store.cell(arena.allocator(), 131, 1)).sql_null);
    try std.testing.expect(try store.equal(arena.allocator(), 131, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, true));
}

test "SQL typed store promotions and ragged widths release every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var store = Store.init(a);
            defer store.deinit();
            _ = try store.appendRagged(&.{ .{}, Datum.json(.{ .string = "owned" }) });
            _ = try store.appendRagged(&.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.{ .bool = true }) });
            _ = try store.appendRagged(&.{ Datum.json(.{ .float = 2.5 }), Datum.json(.null) });
            const cells = try store.rowWidth(a, 0, 2);
            defer a.free(cells);
            try std.testing.expect(cells[0].sql_null);
            try std.testing.expectEqualStrings("owned", cells[1].value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed dictionaries retain repeated payloads once and preserve promotion" {
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 256 * 1024 };
    const a = budget.allocator();
    {
        var store = Store.init(a);
        defer store.deinit();
        var payload: [1024]u8 = @splat('x');
        for (0..10_000) |index| _ = try store.append(&.{if (index % 7 == 0) Datum{} else Datum.json(.{ .string = &payload })});
        try std.testing.expectEqual(@as(usize, 1), store.columns[0].values.strings.values.items.len);
        payload[0] = 'y';
        try std.testing.expectEqual(@as(u8, 'x'), (try store.cell(a, 1, 0)).value.string[0]);
        try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

fn batchAllocationScenario(a: A) !void {
    var store = Store.init(a);
    defer store.deinit();
    const rows = [_][]const Datum{
        &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .string = "shared" }) },
        &.{ .{}, Datum.json(.{ .string = "shared" }) },
        &.{ Datum.json(.{ .float = 2.5 }), Datum.json(.{ .string = "unique" }) },
    };
    try store.appendBatch(.{ .rows = &rows });
    for (rows, 0..) |row, index| for (row, 0..) |expected, column| {
        const actual = try store.cell(a, index, column);
        try std.testing.expectEqualDeep(expected.value, actual.value);
        try std.testing.expectEqual(expected.sql_null, actual.sql_null);
    };
}
test "SQL retained column batch promotion unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, batchAllocationScenario, .{});
}
