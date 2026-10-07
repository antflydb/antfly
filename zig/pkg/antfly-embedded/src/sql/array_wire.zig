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

//! Lossless ordinal SQL-array envelope. Element type belongs to the bound
//! column descriptor, never to JSON shape inference. Values are flat row-major
//! cells; dimensions and SQL NULL flags are authoritative. SQL NULL arrays
//! remain outer null cells, not this non-NULL envelope.
const std = @import("std");
const arrays = @import("array_value.zig");
const casts = @import("builtin_cast.zig");
const operators = @import("operators.zig");
const MemoryBudget = @import("memory_budget.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const Options = struct { values: arrays.Limits = .{}, wire_bytes: usize = 8 * 1024 * 1024 };

const View = struct {
    value: arrays.Value,
    pub fn jsonStringify(self: View, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("dimensions");
        try writer.beginArray();
        for (self.value.dimensions) |dimension| try writer.write(.{ .length = dimension.length, .lower_bound = dimension.lower });
        try writer.endArray();
        try writer.objectField("values");
        try writer.beginArray();
        for (self.value.elements) |element| {
            if (element.sql_null) {
                try writer.write(null);
            } else switch (self.value.element_type) {
                .int16, .int32, .int64 => {
                    var buffer: [20]u8 = undefined;
                    try writer.write(std.fmt.bufPrint(&buffer, "{d}", .{element.value.integer}) catch unreachable);
                },
                .float32, .float64 => {
                    const number = element.value.float;
                    if (std.math.isFinite(number)) try writer.write(number) else try writer.write(if (std.math.isNan(number)) "NaN" else if (number < 0) "-Infinity" else "Infinity");
                },
                else => try writer.write(element.value),
            }
        }
        try writer.endArray();
        try writer.objectField("sql_nulls");
        try writer.beginArray();
        for (self.value.elements) |element| try writer.write(element.sql_null);
        try writer.endArray();
        try writer.endObject();
    }
};

/// All validation/admission precedes destination writes. Counting and emission
/// share one view, avoiding per-element JSON serialization/parse allocations.
fn encodedSize(value: arrays.Value, options: Options) !usize {
    var work: arrays.Budget = .{ .remaining = options.values.work };
    const canonical = try arrays.Value.initWithBudget(value.element_type, value.dimensions, value.elements, options.values, &work);
    if (canonical.dimensions.len != value.dimensions.len) return error.InvalidSqlArrayShape;
    var size: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(View{ .value = value }, .{}, &size.writer);
    if (size.count > options.wire_bytes or size.count > work.remaining / 2) return error.SqlProgramLimitExceeded;
    return std.math.cast(usize, size.count) orelse error.SqlProgramLimitExceeded;
}

pub fn encode(value: arrays.Value, writer: *std.Io.Writer, options: Options) !void {
    _ = try encodedSize(value, options);
    try std.json.Stringify.value(View{ .value = value }, .{}, writer);
}

/// Exact one-allocation output for owners which need retained bytes. Unlike a
/// growing writer, allocation failures retain their allocator error identity.
pub fn encodeAlloc(a: A, value: arrays.Value, options: Options) ![]u8 {
    const bytes = try a.alloc(u8, try encodedSize(value, options));
    errdefer a.free(bytes);
    var writer: std.Io.Writer = .fixed(bytes);
    std.json.Stringify.value(View{ .value = value }, .{}, &writer) catch unreachable;
    std.debug.assert(writer.end == bytes.len);
    return bytes;
}

/// Direct ordinal JSON materialization for result owners. There is no
/// stringify/parse round trip. The caller owns a bounded region and discards
/// it on failure; all retained payloads and managed-array allocators are owned.
pub fn toJsonLeaky(a: A, value: arrays.Value, options: Options) !Json {
    _ = try encodedSize(value, options);
    const axes = try a.alloc(Json, value.dimensions.len);
    for (value.dimensions, axes) |dimension, *axis| {
        var object: std.json.ObjectMap = .empty;
        try object.put(a, "length", .{ .integer = dimension.length });
        try object.put(a, "lower_bound", .{ .integer = dimension.lower });
        axis.* = .{ .object = object };
    }
    const values = try a.alloc(Json, value.elements.len);
    const nulls = try a.alloc(Json, value.elements.len);
    for (value.elements, values, nulls) |cell, *out, *flag| {
        flag.* = .{ .bool = cell.sql_null };
        out.* = if (cell.sql_null) .null else switch (value.element_type) {
            .int16, .int32, .int64 => .{ .string = try std.fmt.allocPrint(a, "{d}", .{cell.value.integer}) },
            .float32, .float64 => if (std.math.isFinite(cell.value.float)) cell.value else .{ .string = if (std.math.isNan(cell.value.float)) "NaN" else if (cell.value.float < 0) "-Infinity" else "Infinity" },
            else => (try operators.cloneDatum(a, cell)).value,
        };
    }
    var envelope: std.json.ObjectMap = .empty;
    try envelope.put(a, "dimensions", .{ .array = std.json.Array.fromOwnedSlice(a, axes) });
    try envelope.put(a, "values", .{ .array = std.json.Array.fromOwnedSlice(a, values) });
    try envelope.put(a, "sql_nulls", .{ .array = std.json.Array.fromOwnedSlice(a, nulls) });
    return .{ .object = envelope };
}

fn arrayField(object: std.json.ObjectMap, name: []const u8) ![]const Json {
    const field = object.get(name) orelse return error.InvalidSqlArrayShape;
    if (field != .array) return error.InvalidSqlArrayShape;
    return field.array.items;
}

fn readElement(kind: arrays.ElementType, raw: Json, sql_null: bool) !arrays.Element {
    if (sql_null) {
        if (raw != .null) return error.InvalidSqlArrayShape;
        return .{};
    }
    return arrays.Element.json(switch (kind) {
        .int16, .int32, .int64 => blk: {
            // Decimal strings are mandatory, even for small values. A JSON
            // number may already have lost precision in an upstream client.
            if (raw != .string) return error.SqlTypeMismatch;
            if (raw.string.len == 0) return error.SqlInvalidTextRepresentation;
            const digits = raw.string[@intFromBool(raw.string[0] == '-')..];
            if (digits.len == 0 or (digits.len > 1 and digits[0] == '0')) return error.SqlInvalidTextRepresentation;
            for (digits) |digit| if (!std.ascii.isDigit(digit)) return error.SqlInvalidTextRepresentation;
            const parsed = std.fmt.parseInt(i64, raw.string, 10) catch |err| return switch (err) {
                error.Overflow => error.SqlNumericOutOfRange,
                else => error.SqlInvalidTextRepresentation,
            };
            var buffer: [20]u8 = undefined;
            const canonical = std.fmt.bufPrint(&buffer, "{d}", .{parsed}) catch unreachable;
            if (!std.mem.eql(u8, canonical, raw.string)) return error.SqlInvalidTextRepresentation;
            break :blk .{ .integer = try casts.checkedInteger(parsed, kind) };
        },
        .float32, .float64 => blk: {
            if (raw == .string and !std.mem.eql(u8, raw.string, "NaN") and !std.mem.eql(u8, raw.string, "Infinity") and !std.mem.eql(u8, raw.string, "-Infinity")) return error.SqlInvalidTextRepresentation;
            if (raw != .string and raw != .float and raw != .integer and raw != .number_string) return error.SqlTypeMismatch;
            break :blk .{ .float = if (kind == .float32) try casts.floatValue(f32, raw) else try casts.floatValue(f64, raw) };
        },
        else => raw,
    });
}

fn dimensionInteger(raw: Json) !i64 {
    return switch (raw) {
        .integer => |number| number,
        .number_string => |text| if (text.len <= 20) std.fmt.parseInt(i64, text, 10) catch error.InvalidSqlArrayShape else error.InvalidSqlArrayShape,
        else => error.InvalidSqlArrayShape,
    };
}

/// The caller owns a bounded allocation region and discards it on failure.
/// An allocation-free validation pass rejects malformed envelopes before any
/// cell/payload storage is retained. Every retained payload is then cloned.
pub fn decodeLeaky(a: A, kind: arrays.ElementType, input: Json, options: Options) !arrays.Value {
    return decodeCells(true, a, kind, input, options);
}

/// The cell/axis buffers are owned; text and JSONB payloads borrow the pinned
/// input envelope. Suitable for immediate transport encoding, not retention.
pub const Borrowed = struct {
    value: arrays.Value,
    allocator: A,
    pub fn deinit(self: *Borrowed) void {
        self.allocator.free(self.value.dimensions);
        self.allocator.free(self.value.elements);
        self.* = undefined;
    }
};

pub fn decodeBorrowed(a: A, kind: arrays.ElementType, input: Json, options: Options) !Borrowed {
    return .{ .value = try decodeCells(false, a, kind, input, options), .allocator = a };
}

fn decodeCells(comptime own_payloads: bool, a: A, kind: arrays.ElementType, input: Json, options: Options) !arrays.Value {
    if (input != .object or input.object.count() != 3) return error.InvalidSqlArrayShape;
    const axes = try arrayField(input.object, "dimensions");
    const values = try arrayField(input.object, "values");
    const nulls = try arrayField(input.object, "sql_nulls");
    if (axes.len > 6 or values.len > options.values.elements) return error.SqlProgramLimitExceeded;
    if (values.len != nulls.len) return error.InvalidSqlArrayShape;
    var dimensions: [6]arrays.Dimension = undefined;
    var count: usize = @intFromBool(axes.len != 0);
    for (axes, dimensions[0..axes.len]) |axis, *dimension| {
        if (axis != .object or axis.object.count() != 2) return error.InvalidSqlArrayShape;
        const length = axis.object.get("length") orelse return error.InvalidSqlArrayShape;
        const lower = axis.object.get("lower_bound") orelse return error.InvalidSqlArrayShape;
        dimension.* = .{ .length = std.math.cast(u32, try dimensionInteger(length)) orelse return error.InvalidSqlArrayShape, .lower = std.math.cast(i32, try dimensionInteger(lower)) orelse return error.InvalidSqlArrayShape };
        // Empty arrays have rank zero. Refuse noncanonical zero-length axes.
        if (dimension.length == 0) return error.InvalidSqlArrayShape;
        if (dimension.length > std.math.maxInt(i32) or @as(i64, dimension.lower) + dimension.length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        count = std.math.mul(usize, count, dimension.length) catch return error.SqlProgramLimitExceeded;
        if (count > options.values.elements) return error.SqlProgramLimitExceeded;
    }
    if (count != values.len) return error.InvalidSqlArrayShape;
    var work: arrays.Budget = .{ .remaining = options.values.work };
    var bytes: usize = @sizeOf(arrays.Value) + axes.len * @sizeOf(arrays.Dimension);
    if (bytes > options.values.bytes) return error.SqlProgramLimitExceeded;
    for (values, nulls) |raw, flag| {
        if (flag != .bool) return error.InvalidSqlArrayShape;
        if (casts.integral(kind) or casts.floating(kind)) switch (raw) {
            .string, .number_string => |text| try work.consume(text.len),
            else => {},
        };
        const cell = try readElement(kind, raw, flag.bool);
        // Reuse the complete typed-array domain validator (JSONB recursion,
        // UTF-8, widths, NULL ownership and finite float4 representation).
        _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{cell}, options.values, &work);
        bytes = std.math.add(usize, bytes, try operators.datumBytes(cell)) catch return error.SqlProgramLimitExceeded;
        if (bytes > options.values.bytes) return error.SqlProgramLimitExceeded;
    }
    var wire_size: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(input, .{}, &wire_size.writer);
    if (wire_size.count > options.wire_bytes or wire_size.count > work.remaining / 2) return error.SqlProgramLimitExceeded;
    const cells = try a.alloc(arrays.Element, values.len);
    errdefer if (!own_payloads) a.free(cells);
    for (values, nulls, cells) |raw, flag, *cell| {
        const decoded = try readElement(kind, raw, flag.bool);
        cell.* = if (own_payloads) try operators.cloneDatum(a, decoded) else decoded;
    }
    const owned_dimensions = try a.dupe(arrays.Dimension, dimensions[0..axes.len]);
    return .{ .element_type = kind, .dimensions = owned_dimensions, .elements = cells };
}

/// Stable quota owner, including arena capacity and failure cleanup.
pub fn decode(backing: A, kind: arrays.ElementType, input: Json, options: Options) !arrays.Owned {
    const budget = try backing.create(MemoryBudget);
    errdefer backing.destroy(budget);
    budget.* = .{ .backing = backing, .limit = options.values.bytes };
    const arena = budget.allocator().create(std.heap.ArenaAllocator) catch |err| return quotaError(budget, err);
    errdefer budget.allocator().destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(budget.allocator());
    errdefer arena.deinit();
    const value = decodeLeaky(arena.allocator(), kind, input, options) catch |err| return quotaError(budget, err);
    return .{ .arena = arena, .budget = budget, .value = value };
}

fn quotaError(budget: *MemoryBudget, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
}

test "SQL borrowed array envelopes preserve pinned payloads and unwind both flat allocations" {
    const Faults = struct {
        fn run(a: A, input: Json) !void {
            var view = try decodeBorrowed(a, .jsonb, input, .{});
            defer view.deinit();
            const values = input.object.get("values").?.array.items;
            try std.testing.expect(view.value.elements[2].value.object.keys().ptr == values[2].object.keys().ptr);
            try std.testing.expect(!view.value.elements[0].sql_null and view.value.elements[1].sql_null);
        }
    };
    const a = std.testing.allocator;
    var owner: std.heap.ArenaAllocator = .init(a);
    defer owner.deinit();
    var original = try @import("array_text.zig").decode(a, .jsonb, "{\"null\",NULL,\"{\\\"a\\\":[1,2]}\"}", .{});
    defer original.deinit();
    const input = try toJsonLeaky(owner.allocator(), original.value, .{});
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{input});
}

test "SQL array envelope matches PostgreSQL binary values without losing bounds or NULL provenance" {
    const Fixture = struct {
        entries: []const struct { input: []const u8, element_type: arrays.ElementType, binary: []const u8 },
    };
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_text_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        var original = try @import("array_text.zig").decode(a, entry.element_type, entry.input, .{});
        defer original.deinit();
        var encoded = std.Io.Writer.Allocating.init(a);
        defer encoded.deinit();
        try encode(original.value, &encoded.writer, .{});
        const parsed = try std.json.parseFromSlice(Json, a, encoded.written(), .{ .allocate = .alloc_always });
        defer parsed.deinit();
        var decoded = try decode(a, entry.element_type, parsed.value, .{});
        defer decoded.deinit();
        var binary = std.Io.Writer.Allocating.init(a);
        defer binary.deinit();
        try @import("array_binary.zig").encode(decoded.value, &binary.writer, .{});
        const expected = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(expected);
        _ = try std.fmt.hexToBytes(expected, entry.binary);
        if (entry.element_type == .jsonb) {
            var pg = try @import("array_binary.zig").decode(a, .jsonb, expected, .{});
            defer pg.deinit();
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try pg.value.compare(decoded.value, &work));
        } else try std.testing.expectEqualSlices(u8, expected, binary.written());
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const materialized = try toJsonLeaky(arena.allocator(), original.value, .{});
        const json = try std.json.Stringify.valueAlloc(a, materialized, .{});
        defer a.free(json);
        try std.testing.expectEqualStrings(encoded.written(), json);
    }
}

test "SQL array envelope rejects malformed or over-budget inputs before allocating cells" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{\"dimensions\":[],\"values\":[\"1\"],\"sql_nulls\":[false]}",
        "{\"dimensions\":[{\"length\":1,\"lower_bound\":1}],\"values\":[\"1\"],\"sql_nulls\":[]}",
        "{\"dimensions\":[{\"length\":1,\"lower_bound\":1}],\"values\":[\"1\"],\"sql_nulls\":[true]}",
        "{\"dimensions\":[{\"length\":0,\"lower_bound\":1}],\"values\":[],\"sql_nulls\":[]}",
    }) |input| {
        const parsed = try std.json.parseFromSlice(Json, a, input, .{});
        defer parsed.deinit();
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        try std.testing.expectError(error.InvalidSqlArrayShape, decodeLeaky(failing.allocator(), .int64, parsed.value, .{}));
        try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    }
    const value = try arrays.Value.init(.int64, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .integer = 9007199254740993 })}, .{});
    var counter: std.Io.Writer.Discarding = .init(&.{});
    try encode(value, &counter.writer, .{});
    var output: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&output);
    try std.testing.expectError(error.SqlProgramLimitExceeded, encode(value, &writer, .{ .wire_bytes = @intCast(counter.count - 1) }));
    try std.testing.expectEqual(@as(usize, 0), writer.end);
}

test "SQL array envelope owns JSONB payloads and unwinds all allocation failures" {
    const Fixture = struct {
        fn run(a: A) !void {
            var source = try @import("array_text.zig").decode(a, .jsonb, "[0:2]={\"{\\\"nested\\\":[1,2]}\",\"null\",NULL}", .{});
            defer source.deinit();
            const encoded = try encodeAlloc(a, source.value, .{});
            defer a.free(encoded);
            const parsed = try std.json.parseFromSlice(Json, a, encoded, .{ .parse_numbers = false });
            var decoded = decode(a, .jsonb, parsed.value, .{}) catch |err| {
                parsed.deinit();
                return err;
            };
            defer decoded.deinit();
            parsed.deinit();
            try std.testing.expectEqual(@as(i32, 0), decoded.value.dimensions[0].lower);
            try std.testing.expectEqualStrings("2", decoded.value.elements[0].value.object.get("nested").?.array.items[1].number_string);
            try std.testing.expect(!decoded.value.elements[1].sql_null);
            try std.testing.expect(decoded.value.elements[1].value == .null);
            try std.testing.expect(decoded.value.elements[2].sql_null);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const materialized = blk: {
                var transient = try arrays.Owned.init(a, decoded.value, .{});
                defer transient.deinit();
                break :blk try toJsonLeaky(arena.allocator(), transient.value, .{});
            };
            var again = try decode(a, .jsonb, materialized, .{});
            defer again.deinit();
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try again.value.compare(decoded.value, &work));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL array envelope enforces element domains and admission before payload ownership" {
    const a = std.testing.allocator;
    for ([_]struct { kind: arrays.ElementType, cell: []const u8, err: anyerror }{
        .{ .kind = .int64, .cell = "9007199254740993", .err = error.SqlTypeMismatch },
        .{ .kind = .int64, .cell = "\"+1\"", .err = error.SqlInvalidTextRepresentation },
        .{ .kind = .int64, .cell = "\"01\"", .err = error.SqlInvalidTextRepresentation },
        .{ .kind = .int64, .cell = "\"9223372036854775808\"", .err = error.SqlNumericOutOfRange },
        .{ .kind = .int64, .cell = "\"-9223372036854775809\"", .err = error.SqlNumericOutOfRange },
        .{ .kind = .int64, .cell = "\"111111111111111111111111\"", .err = error.SqlNumericOutOfRange },
        .{ .kind = .int64, .cell = "\"11111111111111111111111x\"", .err = error.SqlInvalidTextRepresentation },
        .{ .kind = .int16, .cell = "\"32768\"", .err = error.SqlNumericOutOfRange },
        .{ .kind = .float64, .cell = "\"0.1\"", .err = error.SqlInvalidTextRepresentation },
        .{ .kind = .text, .cell = "null", .err = error.SqlTypeMismatch },
        .{ .kind = .boolean, .cell = "\"true\"", .err = error.SqlTypeMismatch },
    }) |case| {
        const input = try std.fmt.allocPrint(a, "{{\"dimensions\":[{{\"length\":1,\"lower_bound\":1}}],\"values\":[{s}],\"sql_nulls\":[false]}}", .{case.cell});
        defer a.free(input);
        const parsed = try std.json.parseFromSlice(Json, a, input, .{});
        defer parsed.deinit();
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        try std.testing.expectError(case.err, decodeLeaky(failing.allocator(), case.kind, parsed.value, .{}));
        try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    }
    const input = try std.json.parseFromSlice(Json, a, "{\"dimensions\":[],\"values\":[],\"sql_nulls\":[]}", .{});
    defer input.deinit();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.SqlProgramLimitExceeded, decodeLeaky(failing.allocator(), .int64, input.value, .{ .values = .{ .bytes = 0 } }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, decodeLeaky(failing.allocator(), .int64, input.value, .{ .wire_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
}

test "SQL array envelope streaming benchmark retains no per-cell serialization buffers" {
    const count = 16384;
    const a = std.testing.allocator;
    const cells = try a.alloc(arrays.Element, count);
    defer a.free(cells);
    for (cells, 0..) |*cell, index| cell.* = if (index % 16 == 0) .{} else arrays.Element.json(.{ .integer = @intCast(index) });
    const value = try arrays.Value.init(.int64, &.{.{ .length = count, .lower = -7 }}, cells, .{});
    var writer: std.Io.Writer.Discarding = .init(&.{});
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..16) |_| try encode(value, &writer.writer, .{});
    std.debug.print("SQL array envelope stream: cells={} bytes={} elapsed_ns={} encode_allocations=0\n", .{ count * 16, writer.count, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
}
