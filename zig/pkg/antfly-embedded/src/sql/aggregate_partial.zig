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

//! Versioned exact reducer interchange shared by spill and worker handoffs.
//! The fixed prefix stores native i128/f64 states; variable typed values retain
//! SQL NULL separately from JSON null. Decode proves the aggregate signature
//! before importing any state. Callers own decoded states and encoded bytes.
const std = @import("std");
const operators = @import("operators.zig");
const Datum = @import("scalar.zig").Datum;
const A = std.mem.Allocator;
const magic = "AGS\x01";

// Wire IDs are explicit: AST enum insertion/reordering cannot reinterpret a
// persisted state. Changing these IDs or the layout requires a new version.
fn kindId(kind: operators.Aggregate.Kind) u8 {
    return switch (kind) {
        .count => 1,
        .sum => 2,
        .avg => 3,
        .min => 4,
        .max => 5,
        .bool_and => 6,
        .bool_or => 7,
        .pattern_set => 8,
    };
}
fn typeId(type_: ?@import("ast.zig").ColumnType) u8 {
    return if (type_) |kind| switch (kind) {
        .string => 1,
        .integer => 2,
        .number => 3,
        .boolean => 4,
        .datetime => 5,
        .json => 6,
        .uuid => 7,
        .array => 8,
    } else 255;
}

pub fn encode(a: A, states: []const operators.Aggregate) ![]const Datum {
    const values = try a.alloc(Datum, states.len);
    var done: usize = 0;
    errdefer {
        for (values[0..done]) |value| a.free(value.value.string);
        a.free(values);
    }
    for (states, values) |state, *value| {
        value.* = try cell(a, state);
        done += 1;
    }
    return values;
}

const Encoder = struct {
    a: A,
    bytes: std.ArrayList(u8) = .empty,
    fn raw(self: *Encoder, value: []const u8) !void {
        try self.bytes.appendSlice(self.a, value);
    }
    fn integer(self: *Encoder, comptime T: type, value: T) !void {
        var bytes: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &bytes, value, .little);
        try self.raw(&bytes);
    }
    fn text(self: *Encoder, value: []const u8) !void {
        try self.integer(u64, @intCast(value.len));
        try self.raw(value);
    }
    fn datum(self: *Encoder, value: Datum) !void {
        if (value.patterns != null) return error.InvalidSqlSpill;
        if (value.sql_null) return self.raw(&.{0});
        if (value.array != null or value.numeric != null) {
            // Reuse the portable typed-block codec; a typed value's scalar
            // JSON placeholder is not its value or its element identity.
            const encoded = try @import("spill.zig").encodeColumnarBlockAlloc(self.a, &.{.{ .values = &.{value}, .keys = &.{}, .ordinal = 0 }}, 16 << 20);
            defer self.a.free(encoded);
            try self.raw(&.{8});
            return self.text(encoded);
        }
        switch (value.value) {
            .null => try self.raw(&.{1}),
            .bool => |v| try self.raw(&.{ 2, @intFromBool(v) }),
            .integer => |v| {
                try self.raw(&.{3});
                try self.integer(i64, v);
            },
            .float => |v| {
                try self.raw(&.{4});
                try self.integer(u64, @bitCast(v));
            },
            .string => |v| {
                try self.raw(&.{5});
                try self.text(v);
            },
            .number_string => |v| {
                try self.raw(&.{6});
                try self.text(v);
            },
            else => {
                const json = try std.json.Stringify.valueAlloc(self.a, value.value, .{});
                defer self.a.free(json);
                try self.raw(&.{7});
                try self.text(json);
            },
        }
    }
};
const Decoder = struct {
    a: A,
    bytes: []const u8,
    position: usize = 0,
    fn raw(self: *Decoder, size: usize) ![]const u8 {
        if (size > self.bytes.len - self.position) return error.InvalidSqlSpill;
        const value = self.bytes[self.position..][0..size];
        self.position += size;
        return value;
    }
    fn integer(self: *Decoder, comptime T: type) !T {
        return std.mem.readInt(T, (try self.raw(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn text(self: *Decoder) ![]const u8 {
        const size = std.math.cast(usize, try self.integer(u64)) orelse return error.InvalidSqlSpill;
        return self.raw(size);
    }
    fn datum(self: *Decoder) !Datum {
        return switch (try self.integer(u8)) {
            0 => .{},
            1 => Datum.json(.null),
            2 => blk: {
                const value = try self.integer(u8);
                if (value > 1) return error.InvalidSqlSpill;
                break :blk Datum.json(.{ .bool = value == 1 });
            },
            3 => Datum.json(.{ .integer = try self.integer(i64) }),
            4 => blk: {
                const value: f64 = @bitCast(try self.integer(u64));
                if (!std.math.isFinite(value)) return error.InvalidSqlSpill;
                break :blk Datum.json(.{ .float = value });
            },
            5 => Datum.json(.{ .string = try self.text() }),
            6 => Datum.json(.{ .number_string = try self.text() }),
            7 => Datum.json(try std.json.parseFromSliceLeaky(std.json.Value, self.a, try self.text(), .{ .allocate = .alloc_always, .parse_numbers = false })),
            8 => blk: {
                const block = try @import("spill.zig").decodeColumnarBlockInArena(self.a, try self.text(), 16 << 20);
                if (block.count() != 1 or block.values.len != 1 or block.keys.len != 0) return error.InvalidSqlSpill;
                const value = try block.cell(0, 0);
                if (value.sql_null or (value.array == null and value.numeric == null)) return error.InvalidSqlSpill;
                break :blk value;
            },
            else => error.InvalidSqlSpill,
        };
    }
};

pub fn cell(a: A, state: operators.Aggregate) !Datum {
    var encoder: Encoder = .{ .a = a };
    errdefer encoder.bytes.deinit(a);
    try encoder.raw(magic);
    try encoder.raw(&.{ kindId(state.kind), typeId(state.input_type), @intFromBool(state.distinct), @intFromBool(state.boolean) });
    try encoder.integer(u64, state.count);
    try encoder.integer(i128, state.integer_sum);
    inline for (.{ state.number_sum, state.compensation, state.mean }) |value| try encoder.integer(u64, @bitCast(value));
    try encoder.datum(if (state.selected) |v| v.row.values[0] else .{});
    try encoder.integer(u64, @intCast(state.distinct_values.items.len));
    for (state.distinct_values.items) |entry| try encoder.datum(entry.row.row.values[0]);
    const null_position: u64 = if (state.patterns) |patterns| blk: {
        for (patterns.values.items, 0..) |value, index| if (value == .null) break :blk @intCast(index);
        break :blk std.math.maxInt(u64);
    } else std.math.maxInt(u64);
    try encoder.integer(u64, null_position);
    return Datum.json(.{ .string = try encoder.bytes.toOwnedSlice(a) });
}

pub fn decode(a: A, value: Datum, spec: operators.AggregateSpec) !operators.Aggregate {
    if (value.sql_null or value.patterns != null or value.value != .string) return error.InvalidSqlSpill;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var decoder: Decoder = .{ .a = scratch.allocator(), .bytes = value.value.string };
    if (!std.mem.eql(u8, try decoder.raw(4), magic)) return error.InvalidSqlSpill;
    const signature = try decoder.raw(4);
    if (signature[0] != kindId(spec.kind) or signature[1] != typeId(spec.input_type) or signature[2] != @intFromBool(spec.distinct) or signature[3] > 1) return error.InvalidSqlSpill;
    const count = try decoder.integer(u64);
    if (count > std.math.maxInt(i64)) return error.InvalidSqlSpill;
    const sum = try decoder.integer(i128);
    var numbers: [3]f64 = undefined;
    for (&numbers) |*v| {
        v.* = @bitCast(try decoder.integer(u64));
        if (!std.math.isFinite(v.*)) return error.InvalidSqlSpill;
    }
    const selected = try decoder.datum();
    if (!selected.sql_null) if (spec.input_type) |type_| {
        const valid = switch (type_) {
            .integer => selected.value == .integer,
            .number => selected.numeric != null or selected.value == .integer or selected.value == .float,
            .boolean => selected.value == .bool,
            .string, .datetime, .uuid => selected.value == .string,
            .json => true,
            .array => selected.array != null,
        };
        if (!valid) return error.InvalidSqlSpill;
    };
    const members = try decoder.integer(u64);
    if (members > decoder.bytes.len - decoder.position) return error.InvalidSqlSpill;
    var state = try operators.Aggregate.init(a, spec.kind, spec.input_type);
    errdefer state.deinit();
    state.distinct = spec.distinct;
    for (0..@as(usize, @intCast(members))) |_| try state.update(try decoder.datum());
    const null_position = try decoder.integer(u64);
    const has_null = null_position != std.math.maxInt(u64);
    if (decoder.position != decoder.bytes.len or (has_null and (spec.kind != .pattern_set or null_position > members))) return error.InvalidSqlSpill;
    if (has_null) {
        try state.update(.{});
        const items = state.patterns.?.values.items;
        const index: usize = @intCast(null_position);
        std.mem.copyBackwards(std.json.Value, items[index + 1 ..], items[index .. items.len - 1]);
        items[index] = .null;
    }
    if (spec.distinct) {
        if (state.count != count or state.integer_sum != sum or state.number_sum != numbers[0] or state.compensation != numbers[1] or state.mean != numbers[2] or state.boolean != (signature[3] == 1)) return error.InvalidSqlSpill;
    } else if (members != 0) return error.InvalidSqlSpill;
    state.count = count;
    state.integer_sum = sum;
    state.number_sum = numbers[0];
    state.compensation = numbers[1];
    state.mean = numbers[2];
    state.boolean = signature[3] == 1;
    if (!selected.sql_null) {
        if ((spec.kind != .min and spec.kind != .max) or count == 0) return error.InvalidSqlSpill;
        const Owner = @TypeOf(state.selected.?);
        const owned = try Owner.init(a, .{ .values = &.{selected}, .keys = &.{}, .ordinal = 0 });
        if (state.selected) |*old| old.deinit();
        state.selected = owned;
    } else if ((spec.kind == .min or spec.kind == .max) and count != 0) return error.InvalidSqlSpill;
    return state;
}

test "SQL NUMERIC aggregate extrema partials retain exact values and display scale" {
    const a = std.testing.allocator;
    const exact = @import("numeric_value.zig");
    var context: exact.Context = .{ .alloc = a };
    var number = try exact.parse(&context, "9007199254740993.1200");
    defer number.deinit();
    var state = try operators.Aggregate.init(a, .min, .number);
    defer state.deinit();
    try state.update(Datum.typedNumeric(&number.value));
    const encoded = try cell(a, state);
    defer a.free(encoded.value.string);
    var restored = try decode(a, encoded, .{ .kind = .min, .input_type = .number });
    defer restored.deinit();
    const result = try restored.finish();
    try std.testing.expect(result.numeric != null and !result.sql_null);
    try std.testing.expectEqual(@as(u16, 4), result.numeric.?.scale);
    try std.testing.expectEqual(std.math.Order.eq, try @import("scalar.zig").compareDatums(result, Datum.typedNumeric(&number.value)));
}

/// Merge complete decoded states. DISTINCT uses membership, while ordinary
/// numeric states merge without narrowing intermediate sums.
pub fn merge(target: *operators.Aggregate, source: operators.Aggregate) !void {
    if (source.distinct) {
        if (source.patterns) |patterns| {
            for (patterns.values.items) |value| try target.update(if (value == .null) Datum{} else Datum.json(value));
        } else for (source.distinct_values.items) |entry| try target.update(entry.row.row.values[0]);
        return;
    }
    if (source.count == 0) return;
    const total = std.math.add(u64, target.count, source.count) catch return error.SqlNumericOutOfRange;
    if (total > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
    switch (target.kind) {
        .count => {},
        .sum => if (target.input_type == .integer) {
            target.integer_sum = std.math.add(i128, target.integer_sum, source.integer_sum) catch return error.SqlNumericOutOfRange;
        } else if (target.count == 0) {
            // Restoring the first partial is an ownership transfer of exact
            // numeric state. Reapplying its compensation can round a persisted
            // answer differently before any other contribution is merged.
            target.number_sum = source.number_sum;
            target.compensation = source.compensation;
        } else {
            // Include the incoming compensation when composing local sums.
            for ([_]f64{ source.number_sum, -source.compensation }) |value| {
                const adjusted = value - target.compensation;
                const sum = target.number_sum + adjusted;
                if (!std.math.isFinite(sum)) return error.SqlNumericOutOfRange;
                target.compensation = (sum - target.number_sum) - adjusted;
                target.number_sum = sum;
            }
        },
        .avg => {
            const fraction = @as(f64, @floatFromInt(source.count)) / @as(f64, @floatFromInt(total));
            const delta = source.mean - target.mean;
            target.mean = if (target.count == 0) source.mean else if (std.math.isFinite(delta)) target.mean + delta * fraction else target.mean * (1 - fraction) + source.mean * fraction;
            if (!std.math.isFinite(target.mean)) return error.SqlNumericOutOfRange;
        },
        .bool_and => target.boolean = target.boolean and source.boolean,
        .bool_or => target.boolean = target.boolean or source.boolean,
        .min, .max => try target.update(source.selected.?.row.values[0]),
        .pattern_set => return error.InvalidSqlSpill,
    }
    target.count = total;
}

fn codecScenario(backing: A, corruptions: bool) !void {
    // Arena growth must have a stable allocation count across injected runs.
    var vtable = backing.vtable.*;
    vtable.resize = A.noResize;
    vtable.remap = A.noRemap;
    var arena = std.heap.ArenaAllocator.init(.{ .ptr = backing.ptr, .vtable = &vtable });
    defer arena.deinit();
    const a = arena.allocator();
    const object = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"k\":null,\"n\":9007199254740993}", .{ .parse_numbers = false });
    const cases = [_]struct { spec: operators.AggregateSpec, inputs: []const Datum }{
        .{ .spec = .{ .kind = .sum, .input_type = .integer }, .inputs = &.{ Datum.json(.{ .integer = std.math.maxInt(i64) }), Datum.json(.{ .integer = std.math.maxInt(i64) }), .{} } },
        .{ .spec = .{ .kind = .sum, .input_type = .number }, .inputs = &.{ Datum.json(.{ .float = 1.0 }), Datum.json(.{ .float = 0.1 }) } },
        .{ .spec = .{ .kind = .avg, .input_type = .number }, .inputs = &.{ Datum.json(.{ .float = 3.5 }), Datum.json(.{ .float = 8.0 }) } },
        .{ .spec = .{ .kind = .min, .input_type = .string }, .inputs = &.{ Datum.json(.{ .string = "z" }), Datum.json(.{ .string = "a\x00b" }) } },
        .{ .spec = .{ .kind = .max, .input_type = .json }, .inputs = &.{Datum.json(.null)} },
        .{ .spec = .{ .kind = .min, .input_type = .json }, .inputs = &.{Datum.json(object)} },
        .{ .spec = .{ .kind = .bool_and, .input_type = .boolean }, .inputs = &.{ Datum.json(.{ .bool = true }), Datum.json(.{ .bool = false }), .{} } },
        .{ .spec = .{ .kind = .sum, .input_type = .integer, .distinct = true }, .inputs = &.{ Datum.json(.{ .integer = 7 }), Datum.json(.{ .integer = 7 }), Datum.json(.{ .integer = 9 }), .{} } },
        .{ .spec = .{ .kind = .pattern_set, .input_type = .string, .distinct = true }, .inputs = &.{ Datum.json(.{ .string = "%cat%" }), Datum.json(.{ .string = "%cat%" }), .{} } },
    };
    for (cases) |case| {
        var state = try operators.Aggregate.init(a, case.spec.kind, case.spec.input_type);
        defer state.deinit();
        state.distinct = case.spec.distinct;
        for (case.inputs) |value| try state.update(value);
        const encoded = try cell(a, state);
        var decoded = try decode(a, encoded, case.spec);
        defer decoded.deinit();
        try std.testing.expectEqual(state.count, decoded.count);
        try std.testing.expectEqual(state.integer_sum, decoded.integer_sum);
        try std.testing.expectEqual(state.number_sum, decoded.number_sum);
        try std.testing.expectEqual(state.compensation, decoded.compensation);
        try std.testing.expectEqual(state.mean, decoded.mean);
        if (state.selected) |v| {
            const result = decoded.selected.?.row.values[0];
            try std.testing.expectEqual(v.row.values[0].sql_null, result.sql_null);
            const expected_json = try std.json.Stringify.valueAlloc(a, v.row.values[0].value, .{});
            const actual_json = try std.json.Stringify.valueAlloc(a, result.value, .{});
            try std.testing.expectEqualStrings(expected_json, actual_json);
        }
        if (state.patterns) |v| try std.testing.expectEqual(v.has_null, decoded.patterns.?.has_null);
        try std.testing.expectEqual(state.distinct_values.items.len, decoded.distinct_values.items.len);
        // Every incomplete record and mismatched semantic signature fails closed.
        if (corruptions) for (0..encoded.value.string.len) |length| try std.testing.expectError(error.InvalidSqlSpill, decode(a, Datum.json(.{ .string = encoded.value.string[0..length] }), case.spec));
        var incompatible = case.spec;
        incompatible.distinct = !incompatible.distinct;
        try std.testing.expectError(error.InvalidSqlSpill, decode(a, encoded, incompatible));
    }
}
test "SQL binary aggregate codec preserves exact states and rejects incomplete or mismatched records" {
    try codecScenario(std.testing.allocator, true);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, codecScenario, .{false});
}

// Frozen empty COUNT record: explicit kind=1/type=255, zero native state,
// SQL-null extremum, no distinct members, and no pattern NULL position.
test "SQL aggregate typed-array partials use the shared portable block codec" {
    const Scenario = struct {
        fn run(a: A) !void {
            var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -3 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
            var state = try operators.Aggregate.init(a, .min, .array);
            defer state.deinit();
            try state.update(Datum.typedArray(&array));
            const encoded = try cell(a, state);
            defer a.free(encoded.value.string);
            var decoded = try decode(a, encoded, .{ .kind = .min, .input_type = .array });
            defer decoded.deinit();
            const value = decoded.selected.?.row.values[0];
            try std.testing.expectEqual(@as(i32, -3), value.array.?.dimensions[0].lower);
            try std.testing.expectEqual(@as(i64, 9007199254740993), value.array.?.elements[0].value.integer);
            try std.testing.expect(value.array.?.elements[1].sql_null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "SQL aggregate wire signature uses stable explicit IDs" {
    const a = std.testing.allocator;
    var state = try operators.Aggregate.init(a, .count, null);
    defer state.deinit();
    const encoded = try cell(a, state);
    defer a.free(encoded.value.string);
    var expected: [73]u8 = @splat(0);
    @memcpy(expected[0..8], "AGS\x01\x01\xff\x00\x00");
    @memset(expected[65..], 255);
    try std.testing.expectEqualSlices(u8, &expected, encoded.value.string);
    var decoded = try decode(a, Datum.json(.{ .string = &expected }), .{ .kind = .count });
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 0), decoded.count);
    try std.testing.expectEqual(@as(u8, 3), typeId(.number));
    try std.testing.expectEqual(@as(u8, 7), typeId(.uuid));
}
