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

//! Immutable typed SQL arrays, distinct from JSON arrays. Elements are flat,
//! row-major prepared values; bounds and SQL NULL provenance are authoritative.
//! Borrowed views require a pinned owner. Owned copies include nested payloads.
const std = @import("std");
const scalar = @import("scalar.zig");
const json_order = @import("json_order.zig");
const operators = @import("operators.zig");
const MemoryBudget = @import("memory_budget.zig");
const Allocator = std.mem.Allocator;
pub const Element = scalar.Datum;
pub const Budget = json_order.Budget;

/// Element widths are part of the type, not inferred from JSON payload shape.
/// Text uses the binary/C collation. Other collations require a bound codec.
pub const ElementType = enum {
    text,
    int16,
    int32,
    int64,
    float32,
    float64,
    boolean,
    uuid,
    jsonb,

    pub fn oid(self: ElementType) u32 {
        return switch (self) {
            .text => 25,
            .int16 => 21,
            .int32 => 23,
            .int64 => 20,
            .float32 => 700,
            .float64 => 701,
            .boolean => 16,
            .uuid => 2950,
            .jsonb => 3802,
        };
    }
    pub fn arrayOid(self: ElementType) u32 {
        return switch (self) {
            .text => 1009,
            .int16 => 1005,
            .int32 => 1007,
            .int64 => 1016,
            .float32 => 1021,
            .float64 => 1022,
            .boolean => 1000,
            .uuid => 2951,
            .jsonb => 3807,
        };
    }
};

pub const Dimension = struct { length: u32, lower: i32 = 1 };
pub const Limits = struct { elements: usize = 65536, bytes: usize = 8 * 1024 * 1024, work: usize = 1_048_576 };
pub const Comparison = enum {
    eq,
    ne,
    lt,
    le,
    gt,
    ge,

    pub fn accepts(self: Comparison, order: std.math.Order) bool {
        return switch (self) {
            .eq => order == .eq,
            .ne => order != .eq,
            .lt => order == .lt,
            .le => order != .gt,
            .gt => order == .gt,
            .ge => order != .lt,
        };
    }
};
pub const Quantifier = enum { any, all };

pub const Value = struct {
    element_type: ElementType,
    dimensions: []const Dimension,
    elements: []const Element,

    /// Validate borrowed prepared cells before they enter an operator. Empty
    /// arrays have no dimensions, including an input shape containing a zero.
    pub fn init(kind: ElementType, dimensions: []const Dimension, elements: []const Element, limits: Limits) !Value {
        var work: Budget = .{ .remaining = limits.work };
        return initWithBudget(kind, dimensions, elements, limits, &work);
    }

    /// Ingress codecs share parsing and typed validation under one work budget.
    pub fn initWithBudget(kind: ElementType, dimensions: []const Dimension, elements: []const Element, limits: Limits, work: *Budget) !Value {
        if (dimensions.len > 6 or elements.len > limits.elements) return error.SqlProgramLimitExceeded;
        var empty = dimensions.len == 0;
        for (dimensions) |dimension| {
            if (dimension.length == 0) empty = true;
            // PostgreSQL requires the exclusive upper end to fit int32,
            // not merely the last valid subscript. Dimension lengths and
            // intermediate row-major products are also signed-int32 bounded.
            if (dimension.length > std.math.maxInt(i32) or @as(i64, dimension.lower) + dimension.length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        }
        var count: usize = @intFromBool(dimensions.len != 0);
        for (dimensions) |dimension| {
            count = std.math.mul(usize, count, dimension.length) catch return error.SqlProgramLimitExceeded;
            if (count > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        }
        if (count != elements.len) return error.InvalidSqlArrayShape;
        var bytes: usize = @sizeOf(Value) + dimensions.len * @sizeOf(Dimension);
        if (bytes > limits.bytes) return error.SqlProgramLimitExceeded;
        for (elements) |element| {
            try validateElement(kind, element, work);
            bytes = std.math.add(usize, bytes, try operators.datumBytes(element)) catch return error.SqlProgramLimitExceeded;
            if (bytes > limits.bytes) return error.SqlProgramLimitExceeded;
        }
        return .{ .element_type = kind, .dimensions = if (empty) &.{} else dimensions, .elements = elements };
    }

    pub fn length(self: Value, dimension: i32) ?u32 {
        if (dimension <= 0 or dimension > self.dimensions.len) return null;
        return self.dimensions[@intCast(dimension - 1)].length;
    }

    pub fn cardinality(self: Value) usize {
        return self.elements.len;
    }

    pub fn rank(self: Value) ?usize {
        return if (self.dimensions.len == 0) null else self.dimensions.len;
    }

    /// Strict scalar comparisons implement SQL three-valued ANY/ALL. A NULL
    /// array is handled by the caller, separately from this non-NULL view.
    /// Empty arrays retain their quantifier identity even for a NULL scalar.
    pub fn quantified(self: Value, scalar_value: Element, comparison: Comparison, quantifier: Quantifier, work: *Budget) !?bool {
        // The SQL probe can have a wider numeric type than the array cells:
        // comparing bigint against int[] must not narrow/overflow the probe.
        try validateElement(switch (self.element_type) {
            .int16, .int32 => .int64,
            .float32 => .float64,
            else => self.element_type,
        }, scalar_value, work);
        var saw_null = false;
        for (self.elements) |element| {
            try work.consume(1);
            if (scalar_value.sql_null or element.sql_null) {
                saw_null = true;
                continue;
            }
            const matched = comparison.accepts(try compareElement(self.element_type, scalar_value, element, work));
            if (quantifier == .any and matched) return true;
            if (quantifier == .all and !matched) return false;
        }
        return if (saw_null) null else quantifier == .all;
    }

    pub fn lower(self: Value, dimension: i32) ?i32 {
        if (dimension <= 0 or dimension > self.dimensions.len) return null;
        return self.dimensions[@intCast(dimension - 1)].lower;
    }

    pub fn upper(self: Value, dimension: i32) ?i32 {
        if (dimension <= 0 or dimension > self.dimensions.len) return null;
        const axis = self.dimensions[@intCast(dimension - 1)];
        return @intCast(@as(i64, axis.lower) + axis.length - 1);
    }

    pub fn at(self: Value, subscripts: []const i32) ?Element {
        if (subscripts.len == 0 or subscripts.len != self.dimensions.len) return null;
        var offset: usize = 0;
        for (self.dimensions, subscripts) |dimension, subscript| {
            const position = @as(i64, subscript) - dimension.lower;
            if (position < 0 or position >= dimension.length) return null;
            offset = offset * dimension.length + @as(usize, @intCast(position));
        }
        return self.elements[offset];
    }

    pub fn compare(self: Value, other: Value, budget: *Budget) !std.math.Order {
        try budget.consume(1);
        if (self.element_type != other.element_type) return error.SqlTypeMismatch;
        for (self.elements[0..@min(self.elements.len, other.elements.len)], other.elements[0..@min(self.elements.len, other.elements.len)]) |a, b| {
            const order = try compareElement(self.element_type, a, b, budget);
            if (order != .eq) return order;
        }
        const count = std.math.order(self.elements.len, other.elements.len);
        if (count != .eq) return count;
        const rank_order = std.math.order(self.dimensions.len, other.dimensions.len);
        if (rank_order != .eq) return rank_order;
        for (self.dimensions, other.dimensions) |a, b| {
            const order = std.math.order(a.length, b.length);
            if (order != .eq) return order;
        }
        for (self.dimensions, other.dimensions) |a, b| {
            const order = std.math.order(a.lower, b.lower);
            if (order != .eq) return order;
        }
        return .eq;
    }

    pub fn semanticHash(self: Value, budget: *Budget) !u64 {
        try budget.consume(1);
        var hash = std.hash.Wyhash.init(0x4152524159);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, self.element_type.oid(), .little);
        hash.update(&bytes);
        std.mem.writeInt(u64, &bytes, self.dimensions.len, .little);
        hash.update(&bytes);
        std.mem.writeInt(u64, &bytes, self.elements.len, .little);
        hash.update(&bytes);
        for (self.dimensions) |dimension| {
            std.mem.writeInt(u32, bytes[0..4], dimension.length, .little);
            std.mem.writeInt(i32, bytes[4..8], dimension.lower, .little);
            hash.update(&bytes);
        }
        for (self.elements) |element| {
            std.mem.writeInt(u64, &bytes, try hashElement(self.element_type, element, budget), .little);
            hash.update(&bytes);
        }
        return hash.final();
    }
};

fn validateElement(kind: ElementType, element: Element, budget: *Budget) !void {
    try budget.consume(1);
    if (element.patterns != null or element.array != null) return error.SqlTypeMismatch;
    if (element.sql_null) {
        if (element.value != .null) return error.InvalidSqlArrayShape;
        return;
    }
    const value = element.value;
    switch (kind) {
        .text => {
            if (value != .string) return error.SqlTypeMismatch;
            try budget.consume(value.string.len);
            if (!std.unicode.utf8ValidateSlice(value.string) or std.mem.indexOfScalar(u8, value.string, 0) != null) return error.SqlTypeMismatch;
        },
        .uuid => {
            if (value != .string or value.string.len != 36) return error.SqlTypeMismatch;
            try budget.consume(value.string.len);
            for (value.string, 0..) |byte, index| {
                if (index == 8 or index == 13 or index == 18 or index == 23) {
                    if (byte != '-') return error.SqlTypeMismatch;
                } else if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.SqlTypeMismatch;
            }
        },
        .int16, .int32, .int64 => {
            if (value != .integer) return error.SqlTypeMismatch;
            if (kind == .int16 and std.math.cast(i16, value.integer) == null) return error.SqlNumericOutOfRange;
            if (kind == .int32 and std.math.cast(i32, value.integer) == null) return error.SqlNumericOutOfRange;
        },
        .float32, .float64 => {
            if (value != .float) return error.SqlTypeMismatch;
            if (kind == .float32 and std.math.isFinite(value.float)) {
                const narrowed: f32 = @floatCast(value.float);
                if (!std.math.isFinite(narrowed)) return error.SqlNumericOutOfRange;
                if (@as(f64, narrowed) != value.float) return error.SqlTypeMismatch;
            }
        },
        .boolean => if (value != .bool) return error.SqlTypeMismatch,
        .jsonb => {
            try validateJsonText(value, budget, 0);
            _ = try json_order.hash(value, budget, 0);
        },
    }
}

/// JSONB strings and object keys have PostgreSQL text-domain restrictions,
/// even when the generic JSON representation permits embedded NUL bytes.
fn validateJsonText(value: std.json.Value, budget: *Budget, depth: usize) !void {
    try budget.consume(1);
    if (depth > 64) return error.SqlProgramLimitExceeded;
    switch (value) {
        .string => |text| try validateText(text, budget),
        .array => |items| for (items.items) |item| try validateJsonText(item, budget, depth + 1),
        .object => |items| {
            for (items.keys(), items.values()) |key, item| {
                try validateText(key, budget);
                try validateJsonText(item, budget, depth + 1);
            }
        },
        else => {},
    }
}

fn validateText(text: []const u8, budget: *Budget) !void {
    try budget.consume(text.len);
    if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.SqlTypeMismatch;
}

fn compareElement(kind: ElementType, a: Element, b: Element, budget: *Budget) !std.math.Order {
    try budget.consume(1);
    if (a.sql_null or b.sql_null) return if (a.sql_null == b.sql_null) .eq else if (a.sql_null) .gt else .lt;
    if (kind == .float32 or kind == .float64) {
        const an = std.math.isNan(a.value.float);
        const bn = std.math.isNan(b.value.float);
        if (an or bn) return if (an == bn) .eq else if (an) .gt else .lt;
        return std.math.order(a.value.float, b.value.float);
    }
    return switch (kind) {
        .int16, .int32, .int64 => std.math.order(a.value.integer, b.value.integer),
        .boolean => std.math.order(@intFromBool(a.value.bool), @intFromBool(b.value.bool)),
        .text, .uuid => blk: {
            try budget.consume(@min(a.value.string.len, b.value.string.len));
            break :blk std.mem.order(u8, a.value.string, b.value.string);
        },
        .jsonb => json_order.compare(a.value, b.value, budget, 0),
        .float32, .float64 => unreachable,
    };
}

fn hashElement(kind: ElementType, element: Element, budget: *Budget) !u64 {
    try budget.consume(1);
    if (element.sql_null) return 0x53514c4e554c4c;
    if (kind == .int16 or kind == .int32 or kind == .int64) {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, element.value.integer, .little);
        return std.hash.Wyhash.hash(2, &bytes);
    }
    if (kind == .float32 or kind == .float64) {
        const value = element.value.float;
        const bits: u64 = if (std.math.isNan(value)) 0x7ff8000000000000 else if (value == 0) 0 else @bitCast(value);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, bits, .little);
        return std.hash.Wyhash.hash(1, &bytes);
    }
    return json_order.hash(element.value, budget, 0);
}

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    budget: *MemoryBudget,
    value: Value,

    pub fn init(backing: Allocator, value: Value, limits: Limits) !Owned {
        const validated = try Value.init(value.element_type, value.dimensions, value.elements, limits);
        const budget = try backing.create(MemoryBudget);
        errdefer backing.destroy(budget);
        budget.* = .{ .backing = backing, .limit = limits.bytes };
        var arena = std.heap.ArenaAllocator.init(budget.allocator());
        errdefer arena.deinit();
        const owned = arena.allocator();
        const dimensions = owned.dupe(Dimension, validated.dimensions) catch |err| return allocationError(budget, err);
        const elements = owned.alloc(Element, value.elements.len) catch |err| return allocationError(budget, err);
        for (value.elements, elements) |element, *out| out.* = operators.cloneDatum(owned, element) catch |err| return allocationError(budget, err);
        return .{ .arena = arena, .budget = budget, .value = .{ .element_type = value.element_type, .dimensions = dimensions, .elements = elements } };
    }

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        const backing = self.budget.backing;
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self.budget);
        self.* = undefined;
    }
};

fn allocationError(budget: *MemoryBudget, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
}

test "SQL typed array quantified comparisons match PostgreSQL reference contracts" {
    const Case = struct {
        sql: []const u8,
        values: []const ?i64,
        lower: i32 = 1,
        shape: ?[]const u32 = null,
        scalar: ?i64,
        comparison: Comparison,
        quantifier: Quantifier,
        expected: ?bool,
    };
    const Fixture = struct { reference: []const u8, scope: []const u8, entries: []const Case };
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", fixture.value.reference);
    try std.testing.expectEqual(@as(usize, 18), fixture.value.entries.len);
    for (fixture.value.entries) |case| {
        const elements = try a.alloc(Element, case.values.len);
        defer a.free(elements);
        for (case.values, elements) |value, *out| out.* = if (value) |integer| Element.json(.{ .integer = integer }) else .{};
        var dimensions: [6]Dimension = undefined;
        const rank_count: usize = if (case.shape) |shape| shape.len else 1;
        if (case.shape) |shape| {
            for (shape, dimensions[0..rank_count]) |length_value, *out| out.* = .{ .length = length_value, .lower = case.lower };
        } else dimensions[0] = .{ .length = @intCast(elements.len), .lower = case.lower };
        const value = try Value.init(.int32, dimensions[0..rank_count], elements, .{});
        var work: Budget = .{};
        const actual = try value.quantified(if (case.scalar) |integer| Element.json(.{ .integer = integer }) else .{}, case.comparison, case.quantifier, &work);
        try std.testing.expectEqual(case.expected, actual);
    }
}

test "SQL typed arrays preserve dimensions lower bounds widths and NULL provenance" {
    const elements: []const Element = &.{ Element.json(.{ .integer = 1 }), .{}, Element.json(.{ .integer = 3 }), Element.json(.{ .integer = 4 }) };
    const value = try Value.init(.int32, &.{ .{ .length = 2, .lower = -1 }, .{ .length = 2, .lower = 0 } }, elements, .{});
    try std.testing.expectEqual(@as(?u32, 2), value.length(1));
    try std.testing.expectEqual(@as(?i32, -1), value.lower(1));
    try std.testing.expectEqual(@as(?i32, 0), value.upper(1));
    try std.testing.expectEqual(@as(i64, 4), value.at(&.{ 0, 1 }).?.value.integer);
    try std.testing.expect(value.at(&.{ -1, 1 }).?.sql_null);
    try std.testing.expect(value.at(&.{ -2, 0 }) == null);
    try std.testing.expect(value.at(&.{0}) == null);
    const empty = try Value.init(.text, &.{.{ .length = 0 }}, &.{}, .{});
    try std.testing.expectEqual(@as(usize, 0), empty.dimensions.len);
    try std.testing.expectEqual(@as(?u32, null), empty.length(1));
    try std.testing.expectEqual(@as(?usize, null), empty.rank());
    try std.testing.expectEqual(@as(usize, 0), empty.cardinality());
    try std.testing.expectEqual(@as(?usize, 2), value.rank());
    try std.testing.expectEqual(@as(usize, 4), value.cardinality());
    try std.testing.expectError(error.InvalidSqlArrayShape, Value.init(.int32, &.{.{ .length = 3 }}, elements, .{}));
    try std.testing.expectError(error.SqlNumericOutOfRange, Value.init(.int16, &.{.{ .length = 1 }}, &.{Element.json(.{ .integer = 32768 })}, .{}));
    try std.testing.expectError(error.SqlNumericOutOfRange, Value.init(.int32, &.{.{ .length = 1 }}, &.{Element.json(.{ .integer = 2147483648 })}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 2, .lower = std.math.maxInt(i32) }}, elements[0..2], .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 1, .lower = std.math.maxInt(i32) }}, elements[0..1], .{}));
    const last = try Value.init(.int32, &.{.{ .length = 1, .lower = std.math.maxInt(i32) - 1 }}, elements[0..1], .{});
    try std.testing.expectEqual(@as(?i32, std.math.maxInt(i32) - 1), last.upper(1));
    try std.testing.expectError(error.SqlTypeMismatch, Value.init(.text, &.{.{ .length = 1 }}, &.{Element.json(.null)}, .{}));
    try std.testing.expectError(error.SqlTypeMismatch, Value.init(.text, &.{.{ .length = 1 }}, &.{Element.json(.{ .string = "a\x00b" })}, .{}));
    try std.testing.expectError(error.SqlTypeMismatch, Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.{ .string = "a\x00b" })}, .{}));
    try std.testing.expectError(error.SqlTypeMismatch, Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.{ .string = "\xff" })}, .{}));
    try std.testing.expectError(error.SqlNumericOutOfRange, Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.{ .float = std.math.inf(f64) })}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{ .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 } }, elements[0..1], .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 4 }}, elements, .{ .work = 3 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 4 }}, elements, .{ .bytes = 1 }));
    const json_null = try Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.null)}, .{});
    const sql_null = try Value.init(.jsonb, &.{.{ .length = 1 }}, &.{.{}}, .{});
    var budget: Budget = .{};
    try std.testing.expectEqual(std.math.Order.lt, try json_null.compare(sql_null, &budget));
    try std.testing.expect(try json_null.semanticHash(&budget) != try sql_null.semanticHash(&budget));
}

test "SQL typed array owned copies and membership clean up under allocation faults" {
    const Harness = struct {
        fn run(a: Allocator) !void {
            var payload: [4]u8 = "éx!".*;
            const view = try Value.init(.text, &.{.{ .length = 3, .lower = -3 }}, &.{ Element.json(.{ .string = &payload }), .{}, Element.json(.{ .string = &payload }) }, .{});
            var owned = try Owned.init(a, view, .{});
            defer owned.deinit();
            @memset(&payload, 'z');
            var index = try Membership.init(a, owned.value, .{});
            defer index.deinit();
            var work: Budget = .{};
            try std.testing.expectEqual(@as(?i32, -3), try index.firstPosition(Element.json(.{ .string = "éx!" }), &work));
            try std.testing.expectEqual(@as(?i32, -2), try index.firstPosition(.{}, &work));
            try std.testing.expectEqual(@as(usize, 1), index.entries.items.len);
            const query = try Value.init(.text, &.{.{ .length = 2 }}, &.{ Element.json(.{ .string = "éx!" }), Element.json(.{ .string = "éx!" }) }, .{});
            try std.testing.expect(try index.contains(query, &work));
            try std.testing.expect(try index.overlaps(query, &work));
            const null_query = try Value.init(.text, &.{.{ .length = 1 }}, &.{.{}}, .{});
            try std.testing.expect(!try index.contains(null_query, &work));
            try std.testing.expect(!try index.overlaps(null_query, &work));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    const value = try Value.init(.int64, &.{.{ .length = 2 }}, &.{ Element.json(.{ .integer = 1 }), Element.json(.{ .integer = 2 }) }, .{});
    try std.testing.expectError(error.SqlProgramLimitExceeded, Owned.init(std.testing.allocator, value, .{ .bytes = 128 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Membership.init(std.testing.allocator, value, .{ .bytes = 128 }));
    var index = try Membership.init(std.testing.allocator, value, .{});
    defer index.deinit();
    var no_work: Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, index.contains(value, &no_work));
}

test "SQL typed array hashing follows PostgreSQL float and shape equality" {
    const a = try Value.init(.float64, &.{.{ .length = 3 }}, &.{ Element.json(.{ .float = -0.0 }), Element.json(.{ .float = std.math.inf(f64) }), Element.json(.{ .float = std.math.nan(f64) }) }, .{});
    const b = try Value.init(.float64, &.{.{ .length = 3 }}, &.{ Element.json(.{ .float = 0.0 }), Element.json(.{ .float = std.math.inf(f64) }), Element.json(.{ .float = -std.math.nan(f64) }) }, .{});
    var work: Budget = .{};
    try std.testing.expectEqual(std.math.Order.eq, try a.compare(b, &work));
    try std.testing.expectEqual(try a.semanticHash(&work), try b.semanticHash(&work));
    const shifted = try Value.init(.float64, &.{.{ .length = 3, .lower = 2 }}, b.elements, .{});
    try std.testing.expectEqual(std.math.Order.lt, try a.compare(shifted, &work));
    try std.testing.expect(try a.semanticHash(&work) != try shifted.semanticHash(&work));
    var index = try Membership.init(std.testing.allocator, a, .{});
    defer index.deinit();
    try std.testing.expect(try index.contains(b, &work));
}

test "SQL typed array prepared membership bounds repeated probe work without allocations" {
    const a = std.testing.allocator;
    const source_cells = try a.alloc(Element, 4096);
    defer a.free(source_cells);
    for (source_cells, 0..) |*cell, ordinal| cell.* = Element.json(.{ .integer = @intCast(ordinal) });
    var probe_cells: [256]Element = undefined;
    for (&probe_cells, 0..) |*cell, ordinal| cell.* = Element.json(.{ .integer = @intCast(3840 + ordinal) });
    const source = try Value.init(.int32, &.{.{ .length = 4096 }}, source_cells, .{});
    const probes = try Value.init(.int32, &.{.{ .length = 256 }}, &probe_cells, .{});
    const prepare_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var index = try Membership.init(a, source, .{ .bytes = 512 * 1024 });
    defer index.deinit();
    const prepared_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - prepare_start;
    const retained = index.budget.live;
    var work: Budget = .{ .remaining = 20_000_000 };
    const scan_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10) |_| for (probes.elements) |probe| {
        var matched = false;
        for (source.elements) |candidate| if (try compareElement(.int32, probe, candidate, &work) == .eq) {
            matched = true;
            break;
        };
        try std.testing.expect(matched);
    };
    const scan_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - scan_start;
    const indexed_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    work = .{ .remaining = 100000 };
    for (0..10) |_| try std.testing.expect(try index.contains(probes, &work));
    const indexed_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - indexed_start;
    try std.testing.expectEqual(retained, index.budget.live);
    try std.testing.expect(work.remaining > 90000);
    std.debug.print("SQL array membership: source_elements=4096 probe_elements=256 iterations=10 prepare_ns={} scan_ns={} indexed_ns={} retained_bytes={} probe_allocated_bytes=0\n", .{ prepared_ns, scan_ns, indexed_ns, retained });
}

/// Reusable equality membership, O(n) build and expected O(m) probes. The
/// immutable array owner must outlive this index. NULLs never satisfy @>/&&;
/// firstPosition separately uses IS NOT DISTINCT FROM semantics.
pub const Membership = struct {
    const Entry = struct { ordinal: usize, next: ?usize };
    budget: *MemoryBudget,
    value: Value,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    entries: std.ArrayList(Entry) = .empty,
    first_null: ?usize = null,

    pub fn init(backing: Allocator, value: Value, limits: Limits) !Membership {
        const validated = try Value.init(value.element_type, value.dimensions, value.elements, limits);
        const budget = try backing.create(MemoryBudget);
        errdefer backing.destroy(budget);
        budget.* = .{ .backing = backing, .limit = limits.bytes };
        var index: Membership = .{ .budget = budget, .value = validated };
        // Mutable build buffers use the quota allocator directly: hash-table
        // and entry-list growth must reclaim replaced capacity, not accumulate
        // obsolete buffers in the immutable value's arena.
        const a = budget.allocator();
        errdefer index.heads.deinit(a);
        errdefer index.entries.deinit(a);
        var work: Budget = .{ .remaining = limits.work };
        for (value.elements, 0..) |element, ordinal| {
            if (element.sql_null) {
                if (index.first_null == null) index.first_null = ordinal;
                continue;
            }
            const hash = try hashElement(value.element_type, element, &work);
            if (try index.find(element, hash, &work) != null) continue;
            const slot = index.entries.items.len;
            const entry: Entry = .{ .ordinal = ordinal, .next = index.heads.get(hash) };
            index.entries.append(a, entry) catch |err| return allocationError(budget, err);
            index.heads.put(a, hash, slot) catch |err| return allocationError(budget, err);
        }
        return index;
    }

    pub fn deinit(self: *Membership) void {
        self.heads.deinit(self.budget.allocator());
        self.entries.deinit(self.budget.allocator());
        const backing = self.budget.backing;
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self.budget);
        self.* = undefined;
    }

    fn find(self: *const Membership, element: Element, hash: u64, budget: *Budget) !?usize {
        var cursor = self.heads.get(hash);
        while (cursor) |slot| {
            const entry = self.entries.items[slot];
            if (try compareElement(self.value.element_type, self.value.elements[entry.ordinal], element, budget) == .eq) return entry.ordinal;
            cursor = entry.next;
        }
        return null;
    }

    pub fn contains(self: *const Membership, other: Value, work: *Budget) !bool {
        if (self.value.element_type != other.element_type) return error.SqlTypeMismatch;
        for (other.elements) |element| {
            try work.consume(1);
            if (element.sql_null) return false;
            if (try self.find(element, try hashElement(other.element_type, element, work), work) == null) return false;
        }
        return true;
    }

    pub fn overlaps(self: *const Membership, other: Value, work: *Budget) !bool {
        if (self.value.element_type != other.element_type) return error.SqlTypeMismatch;
        for (other.elements) |element| {
            try work.consume(1);
            if (element.sql_null) continue;
            if (try self.find(element, try hashElement(other.element_type, element, work), work) != null) return true;
        }
        return false;
    }

    pub fn firstPosition(self: *const Membership, element: Element, work: *Budget) !?i32 {
        try validateElement(self.value.element_type, element, work);
        if (self.value.dimensions.len > 1) return error.InvalidSqlArrayShape;
        if (self.value.dimensions.len == 0) return null;
        const ordinal = if (element.sql_null) self.first_null else try self.find(element, try hashElement(self.value.element_type, element, work), work);
        return if (ordinal) |at| @intCast(@as(i64, self.value.dimensions[0].lower) + @as(i64, @intCast(at))) else null;
    }
};
