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

const std = @import("std");
const Type = @import("backend.zig").Type;
const Column = @import("backend.zig").Column;
const Parameter = @import("backend.zig").Parameter;

pub fn parameterOid(descriptor: Parameter) !u32 {
    try @import("antfly_local_sources").sql_scalar.validateParameterType(descriptor);
    if (descriptor.kind == .array) return (descriptor.element_type orelse return error.UnsupportedParameterType).arrayOid();
    if (descriptor.element_type) |element| return element.oid();
    return if (descriptor.kind) |kind| switch (kind) {
        inline else => |tag| oid(@field(Type, @tagName(tag))),
    } else 0;
}

pub fn parameterFromOid(value: u32) !Parameter {
    const kind = try fromOid(value);
    const Element = @import("antfly_local_sources").sql_array_value.ElementType;
    const element: ?Element = switch (value) {
        16, 1000 => .boolean,
        21, 1005 => .int16,
        23, 1007 => .int32,
        20, 1016 => .int64,
        700, 1021 => .float32,
        701, 1022 => .float64,
        25, 1043, 1009 => .text,
        2950, 2951 => .uuid,
        3802, 3807 => .jsonb,
        1700, 1231 => .numeric,
        else => null,
    };
    return .{ .kind = switch (kind) {
        .unknown => null,
        inline else => |tag| @field(@import("antfly_local_sources").sql_ast.ColumnType, @tagName(tag)),
    }, .element_type = element };
}

pub fn oid(kind: Type) !u32 {
    return switch (kind) {
        .boolean => 16,
        .integer => 20,
        .number => 701,
        .datetime => 1184,
        .json => 3802,
        .uuid => 2950,
        .string, .unknown => 25,
        .array => error.UnsupportedParameterType,
    };
}

pub fn columnOid(column: Column) !u32 {
    if (column.type == .array) return (column.element_type orelse return error.InvalidResult).arrayOid();
    if (column.element_type) |element| {
        try @import("antfly_local_sources").sql_scalar.validateParameterType(.{ .kind = switch (column.type) {
            .unknown => null,
            inline else => |tag| @field(@import("antfly_local_sources").sql_ast.ColumnType, @tagName(tag)),
        }, .element_type = element });
        return element.oid();
    }
    return oid(column.type);
}

pub fn columnModifier(column: Column) !i32 {
    const modifier = column.numeric_modifier orelse return -1;
    if ((column.type != .number and column.type != .array) or column.element_type != .numeric) return error.InvalidResult;
    return modifier.postgres() catch return error.InvalidResult;
}

pub fn columnTypeSize(column: Column) i16 {
    if (column.type == .array) return -1;
    if (column.element_type) |element| return switch (element) {
        .int16 => 2,
        .int32, .float32 => 4,
        .int64, .float64 => 8,
        .boolean => 1,
        .uuid => 16,
        .text, .jsonb, .numeric => -1,
    };
    return typeSize(column.type);
}

fn encodeNarrowScalar(writer: *std.Io.Writer, element: @import("antfly_local_sources").sql_array_value.ElementType, format: u16, value: std.json.Value) !void {
    switch (element) {
        .int16 => {
            const number = std.math.cast(i16, try integer(value)) orelse return error.InvalidResult;
            if (format == 0) try writer.print("{d}", .{number}) else try writer.writeInt(i16, number, .big);
        },
        .int32 => {
            const number = std.math.cast(i32, try integer(value)) orelse return error.InvalidResult;
            if (format == 0) try writer.print("{d}", .{number}) else try writer.writeInt(i32, number, .big);
        },
        .float32 => {
            const number: f64 = switch (value) {
                .float => |n| n,
                .integer => |n| @floatFromInt(n),
                .number_string, .string => |n| std.fmt.parseFloat(f64, n) catch return error.InvalidResult,
                else => return error.InvalidResult,
            };
            const narrowed: f32 = @floatCast(number);
            if (std.math.isFinite(number) and (!std.math.isFinite(narrowed) or (number != 0 and narrowed == 0))) return error.InvalidResult;
            if (format == 0) {
                if (std.math.isNan(narrowed)) try writer.writeAll("NaN") else if (std.math.isInf(narrowed)) try writer.writeAll(if (narrowed < 0) "-Infinity" else "Infinity") else try writer.print("{d}", .{narrowed});
            } else try writer.writeInt(u32, @bitCast(narrowed), .big);
        },
        else => unreachable,
    }
}

/// Array payloads are validated against the bound descriptor, never inferred
/// from JSON shape. Only two flat buffers are retained transiently; text and
/// JSONB payloads borrow the live result owner and stream directly to pgwire.
pub fn encodeColumnInto(a: std.mem.Allocator, writer: *std.Io.Writer, column: Column, format: u16, value: std.json.Value, wire_bytes: usize) !void {
    if (format > 1) return error.UnsupportedResultFormat;
    _ = try columnOid(column);
    if (column.type == .number and column.element_type == .numeric) {
        if (value != .string) return error.InvalidResult;
        const sources = @import("antfly_local_sources");
        var context: sources.sql_numeric_value.Context = .{ .alloc = a, .max_output_bytes = wire_bytes };
        var number = try sources.sql_numeric_value.parse(&context, value.string);
        defer number.deinit();
        if (format == 0) return sources.sql_numeric_value.write(&context, number.value, writer);
        return sources.sql_numeric_binary.encode(&context, number.value, writer);
    }
    if (column.type != .array) {
        if (column.element_type) |element| switch (element) {
            .int16, .int32, .float32 => {
                var size: std.Io.Writer.Discarding = .init(&.{});
                try encodeNarrowScalar(&size.writer, element, format, value);
                if (size.count > wire_bytes) return error.ProgramLimitExceeded;
                return encodeNarrowScalar(writer, element, format, value);
            },
            else => {},
        };
        var size: std.Io.Writer.Discarding = .init(&.{});
        try encodeInto(a, &size.writer, column.type, format, value);
        if (size.count > wire_bytes) return error.ProgramLimitExceeded;
        return encodeInto(a, writer, column.type, format, value);
    }
    const sources = @import("antfly_local_sources");
    // Envelope bytes and PostgreSQL output bytes are different formats. A
    // small binary result must not be rejected because its JSON metadata is
    // larger than the frame allowance. Admission below bounds actual output.
    var view = try sources.sql_array_wire.decodeBorrowed(a, column.element_type orelse return error.InvalidResult, value, .{});
    defer view.deinit();
    if (format == 0)
        try sources.sql_array_text.encode(view.value, writer, .{ .wire_bytes = wire_bytes })
    else
        try sources.sql_array_binary.encode(view.value, writer, .{ .wire_bytes = wire_bytes });
}

pub fn fromOid(value: u32) !Type {
    return switch (value) {
        0 => .unknown,
        16 => .boolean,
        20, 21, 23 => .integer,
        700, 701, 1700 => .number,
        25, 1043 => .string,
        1184 => .datetime,
        114, 3802 => .json,
        2950 => .uuid,
        1000, 1005, 1007, 1016, 1021, 1022, 1009, 2951, 3807, 1231 => .array,
        else => error.UnsupportedParameterType,
    };
}

pub fn typeSize(kind: Type) i16 {
    return switch (kind) {
        .boolean => 1,
        .uuid => 16,
        .integer, .number, .datetime => 8,
        else => -1,
    };
}

pub fn decode(alloc: std.mem.Allocator, param_oid: u32, format: u16, bytes: []const u8) !std.json.Value {
    const kind = try fromOid(param_oid);
    if (format > 1) return error.UnsupportedParameterFormat;
    if (param_oid == 1700) {
        const sources = @import("antfly_local_sources");
        var context: sources.sql_numeric_value.Context = .{ .alloc = alloc };
        var number = if (format == 0) try sources.sql_numeric_value.parse(&context, bytes) else try sources.sql_numeric_binary.decode(&context, bytes, .{});
        defer number.deinit();
        return .{ .number_string = try sources.sql_numeric_value.format(&context, number.value) };
    }
    if (kind == .array) {
        const sources = @import("antfly_local_sources");
        const element = (try parameterFromOid(param_oid)).element_type orelse return error.UnsupportedParameterType;
        var decoded = if (format == 0) try sources.sql_array_text.decode(alloc, element, bytes, .{}) else try sources.sql_array_binary.decode(alloc, element, bytes, .{});
        defer decoded.deinit();
        return sources.sql_array_wire.toJsonLeaky(alloc, decoded.value, .{});
    }
    if (format == 0) {
        if (!std.unicode.utf8ValidateSlice(bytes) or std.mem.indexOfScalar(u8, bytes, 0) != null) return error.InvalidParameter;
        return switch (kind) {
            .integer => .{ .integer = std.fmt.parseInt(i64, bytes, 10) catch return error.InvalidParameter },
            .boolean => .{ .bool = if (std.ascii.eqlIgnoreCase(bytes, "true") or std.mem.eql(u8, bytes, "t") or std.mem.eql(u8, bytes, "1")) true else if (std.ascii.eqlIgnoreCase(bytes, "false") or std.mem.eql(u8, bytes, "f") or std.mem.eql(u8, bytes, "0")) false else return error.InvalidParameter },
            .number => blk: {
                const value = try parseJson(alloc, bytes);
                if (value != .number_string) return error.InvalidParameter;
                break :blk value;
            },
            .json => try parseJson(alloc, bytes),
            .uuid => .{ .string = @import("antfly_local_sources").common_uuid.canonicalAlloc(alloc, bytes) catch |err| switch (err) {
                error.InvalidUuid => return error.InvalidParameter,
                else => return err,
            } },
            else => .{ .string = try alloc.dupe(u8, bytes) },
        };
    }
    return switch (param_oid) {
        16 => if (bytes.len == 1 and bytes[0] <= 1) .{ .bool = bytes[0] == 1 } else error.InvalidParameter,
        21 => if (bytes.len == 2) .{ .integer = std.mem.readInt(i16, bytes[0..2], .big) } else error.InvalidParameter,
        23 => if (bytes.len == 4) .{ .integer = std.mem.readInt(i32, bytes[0..4], .big) } else error.InvalidParameter,
        20 => if (bytes.len == 8) .{ .integer = std.mem.readInt(i64, bytes[0..8], .big) } else error.InvalidParameter,
        700 => if (bytes.len == 4) try finite(@as(f32, @bitCast(std.mem.readInt(u32, bytes[0..4], .big)))) else error.InvalidParameter,
        701 => if (bytes.len == 8) try finite(@as(f64, @bitCast(std.mem.readInt(u64, bytes[0..8], .big)))) else error.InvalidParameter,
        1184 => blk: {
            if (bytes.len != 8) return error.InvalidParameter;
            const micros = std.mem.readInt(i64, bytes[0..8], .big);
            const nanos = std.math.cast(u64, (@as(i128, micros) + 946684800000000) * 1000) orelse return error.InvalidParameter;
            break :blk try timestampValue(alloc, nanos);
        },
        114 => try parseJson(alloc, bytes),
        3802 => if (bytes.len > 0 and bytes[0] == 1) try parseJson(alloc, bytes[1..]) else error.InvalidParameter,
        2950 => if (bytes.len == 16) .{ .string = blk: {
            const canonical = @import("antfly_local_sources").common_uuid.format(bytes[0..16].*);
            break :blk try alloc.dupe(u8, &canonical);
        } } else error.InvalidParameter,
        25, 1043 => if (std.unicode.utf8ValidateSlice(bytes) and std.mem.indexOfScalar(u8, bytes, 0) == null) .{ .string = try alloc.dupe(u8, bytes) } else error.InvalidParameter,
        // PostgreSQL NUMERIC binary is not IEEE float. Reject it explicitly;
        // its exact text representation remains supported without rounding.
        else => error.UnsupportedParameterFormat,
    };
}

fn finite(value: f64) !std.json.Value {
    if (!std.math.isFinite(value)) return error.InvalidParameter;
    return .{ .float = value };
}

fn parseJson(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Value {
    // Bound nesting before constructing values: backend result serialization
    // must not recurse to attacker-chosen stack depth even below the byte cap.
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes) |byte| {
        if (quoted) {
            if (escaped) {
                escaped = false;
                continue;
            }
            if (byte == '\\') {
                escaped = true;
                continue;
            }
            if (byte == '"') quoted = false;
        } else switch (byte) {
            '"' => quoted = true,
            '[', '{' => {
                depth += 1;
                if (depth > 64) return error.InvalidParameter;
            },
            ']', '}' => {
                if (depth == 0) return error.InvalidParameter;
                depth -= 1;
            },
            else => {},
        }
    }
    return std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{
        .allocate = .alloc_always,
        .parse_numbers = false,
    }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidParameter,
    };
}

pub fn integer(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |v| v,
        .number_string, .string => |v| std.fmt.parseInt(i64, v, 10) catch error.InvalidResult,
        else => error.InvalidResult,
    };
}

pub fn encode(alloc: std.mem.Allocator, kind: Type, format: u16, value: std.json.Value) ![]const u8 {
    if (kind == .array) return error.InvalidResult;
    if (format > 1) return error.UnsupportedResultFormat;
    if (format == 0) return switch (kind) {
        .boolean => if (value == .bool) try alloc.dupe(u8, if (value.bool) "t" else "f") else error.InvalidResult,
        .integer => try std.fmt.allocPrint(alloc, "{d}", .{try integer(value)}),
        .datetime => try timestampText(alloc, value),
        .json => try std.json.Stringify.valueAlloc(alloc, value, .{}),
        .uuid => if (value == .string) @import("antfly_local_sources").common_uuid.canonicalAlloc(alloc, value.string) catch |err| switch (err) {
            error.InvalidUuid => return error.InvalidResult,
            else => return err,
        } else error.InvalidResult,
        else => if (value == .string) try alloc.dupe(u8, value.string) else try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
    return switch (kind) {
        .array => error.InvalidResult,
        .boolean => if (value == .bool) try alloc.dupe(u8, &.{@intFromBool(value.bool)}) else error.InvalidResult,
        .integer => try encodeInteger(alloc, try integer(value)),
        .number => blk: {
            const number = switch (value) {
                .float => |v| v,
                .integer => |v| @as(f64, @floatFromInt(v)),
                .number_string, .string => |v| std.fmt.parseFloat(f64, v) catch return error.InvalidResult,
                else => return error.InvalidResult,
            };
            if (!std.math.isFinite(number)) return error.InvalidResult;
            break :blk try encodeInteger(alloc, @bitCast(number));
        },
        .datetime => blk: {
            const nanos = try timestampNanos(value);
            if (@mod(nanos, 1000) != 0) return error.UnsupportedResultPrecision;
            break :blk try encodeInteger(alloc, @as(i64, @intCast(nanos / 1000)) - 946684800000000);
        },
        .json => blk: {
            const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(json);
            break :blk try std.mem.concat(alloc, u8, &.{ &.{1}, json });
        },
        .uuid => if (value == .string) blk: {
            const parsed = @import("antfly_local_sources").common_uuid.parse(value.string) catch return error.InvalidResult;
            break :blk try alloc.dupe(u8, &parsed);
        } else error.InvalidResult,
        .string => if (value == .string) try alloc.dupe(u8, value.string) else error.InvalidResult,
        .unknown => if (value == .string) try alloc.dupe(u8, value.string) else try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

/// Append a cell directly to a reusable DataRow buffer. Primitive and JSON
/// values require no intermediate encoded allocation.
pub fn encodeInto(a: std.mem.Allocator, writer: *std.Io.Writer, kind: Type, format: u16, value: std.json.Value) !void {
    if (kind == .array) return error.InvalidResult;
    if (format > 1) return error.UnsupportedResultFormat;
    if (format == 0) {
        switch (kind) {
            .boolean => {
                if (value != .bool) return error.InvalidResult;
                try writer.writeAll(if (value.bool) "t" else "f");
            },
            .integer => try writer.print("{d}", .{try integer(value)}),
            .datetime => {
                const text = try timestampText(a, value);
                defer a.free(text);
                try writer.writeAll(text);
            },
            .uuid => {
                if (value != .string) return error.InvalidResult;
                const canonical = @import("antfly_local_sources").common_uuid.format(@import("antfly_local_sources").common_uuid.parse(value.string) catch return error.InvalidResult);
                try writer.writeAll(&canonical);
            },
            .json => try std.json.Stringify.value(value, .{}, writer),
            else => if (value == .string) try writer.writeAll(value.string) else try std.json.Stringify.value(value, .{}, writer),
        }
        return;
    }
    switch (kind) {
        .array => return error.InvalidResult,
        .boolean => {
            if (value != .bool) return error.InvalidResult;
            try writer.writeByte(@intFromBool(value.bool));
        },
        .integer => try writer.writeInt(i64, try integer(value), .big),
        .number => {
            const number: f64 = switch (value) {
                .float => |n| n,
                .integer => |n| @floatFromInt(n),
                .number_string, .string => |n| std.fmt.parseFloat(f64, n) catch return error.InvalidResult,
                else => return error.InvalidResult,
            };
            if (!std.math.isFinite(number)) return error.InvalidResult;
            try writer.writeInt(u64, @bitCast(number), .big);
        },
        .datetime => {
            const nanos = try timestampNanos(value);
            if (@mod(nanos, 1000) != 0) return error.UnsupportedResultPrecision;
            try writer.writeInt(i64, @as(i64, @intCast(nanos / 1000)) - 946684800000000, .big);
        },
        .json => {
            try writer.writeByte(1);
            try std.json.Stringify.value(value, .{}, writer);
        },
        .uuid => {
            if (value != .string) return error.InvalidResult;
            const parsed = @import("antfly_local_sources").common_uuid.parse(value.string) catch return error.InvalidResult;
            try writer.writeAll(&parsed);
        },
        .string => {
            if (value != .string) return error.InvalidResult;
            try writer.writeAll(value.string);
        },
        .unknown => if (value == .string) try writer.writeAll(value.string) else try std.json.Stringify.value(value, .{}, writer),
    }
}

fn encodeInteger(alloc: std.mem.Allocator, value: i64) ![]const u8 {
    const bytes = try alloc.alloc(u8, 8);
    std.mem.writeInt(i64, bytes[0..8], value, .big);
    return bytes;
}

fn timestampText(alloc: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    // ISO strings remain strings; integer cells are the native nanosecond
    // representation, not PostgreSQL's microseconds-since-2000 wire value.
    if (value == .string and std.mem.indexOfScalar(u8, value.string, 'T') != null)
        return alloc.dupe(u8, value.string);
    const ns = try timestampNanos(value);
    if (@mod(ns, 1000) != 0) return error.UnsupportedResultPrecision;
    const epoch = std.time.epoch.EpochSeconds{ .secs = @intCast(@divTrunc(ns, std.time.ns_per_s)) };
    const year_day = epoch.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const seconds = epoch.getDaySeconds();
    return std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
        year_day.year,                                                    month_day.month.numeric(),    month_day.day_index + 1,
        seconds.getHoursIntoDay(),                                        seconds.getMinutesIntoHour(), seconds.getSecondsIntoMinute(),
        @as(u32, @intCast(@divTrunc(@mod(ns, std.time.ns_per_s), 1000))),
    });
}

/// The protocol carries exact native unsigned nanoseconds until the native
/// adapter normalizes typed parameters to the engine's canonical ISO string.
pub fn timestampNanos(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |ns| std.math.cast(u64, ns) orelse error.InvalidResult,
        .number_string => |text| std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidResult,
        else => error.InvalidResult,
    };
}

pub fn timestampValue(alloc: std.mem.Allocator, nanos: u64) !std.json.Value {
    if (std.math.cast(i64, nanos)) |signed| return .{ .integer = signed };
    return .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{nanos}) };
}

test "pgwire real special results preserve PostgreSQL text and binary representations" {
    const a = std.testing.allocator;
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    const column: Column = .{ .name = "v", .type = .number, .element_type = .float32 };
    for ([_]f64{ std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64) }, [_][]const u8{ "NaN", "Infinity", "-Infinity" }) |value, text| {
        output.writer.end = 0;
        try encodeColumnInto(a, &output.writer, column, 0, .{ .float = value }, 128);
        try std.testing.expectEqualStrings(text, output.written());
        output.writer.end = 0;
        try encodeColumnInto(a, &output.writer, column, 1, .{ .float = value }, 128);
        try std.testing.expectEqual(@as(usize, 4), output.written().len);
        const restored: f32 = @bitCast(std.mem.readInt(u32, output.written()[0..4], .big));
        if (std.math.isNan(value)) try std.testing.expect(std.math.isNan(restored)) else try std.testing.expectEqual(value, @as(f64, restored));
        output.writer.end = 0;
        try std.testing.expectError(error.ProgramLimitExceeded, encodeColumnInto(a, &output.writer, column, 1, .{ .float = value }, 3));
        try std.testing.expectEqual(@as(usize, 0), output.written().len);
    }
}

test "SQL pgwire scalar builtin descriptors match binary widths and round trip values" {
    const a = std.testing.allocator;
    const Element = @import("antfly_local_sources").sql_array_value.ElementType;
    const cases = [_]struct { kind: Type, element: Element, oid: u32, size: i16, value: std.json.Value, expected: std.json.Value }{
        .{ .kind = .integer, .element = .int16, .oid = 21, .size = 2, .value = .{ .integer = -1234 }, .expected = .{ .integer = -1234 } },
        .{ .kind = .integer, .element = .int32, .oid = 23, .size = 4, .value = .{ .integer = -2000000000 }, .expected = .{ .integer = -2000000000 } },
        .{ .kind = .integer, .element = .int64, .oid = 20, .size = 8, .value = .{ .integer = 9007199254740993 }, .expected = .{ .integer = 9007199254740993 } },
        .{ .kind = .number, .element = .float32, .oid = 700, .size = 4, .value = .{ .float = 0.1 }, .expected = .{ .float = @as(f32, 0.1) } },
        .{ .kind = .number, .element = .float64, .oid = 701, .size = 8, .value = .{ .float = 1.25 }, .expected = .{ .float = 1.25 } },
    };
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    for (cases) |case| {
        const column: Column = .{ .name = "v", .type = case.kind, .element_type = case.element };
        try std.testing.expectEqual(case.oid, try columnOid(column));
        try std.testing.expectEqual(case.size, columnTypeSize(column));
        output.writer.end = 0;
        try encodeColumnInto(a, &output.writer, column, 1, case.value, 8);
        try std.testing.expectEqual(@as(usize, @intCast(case.size)), output.written().len);
        const restored = try decode(a, case.oid, 1, output.written());
        switch (case.expected) {
            .integer => |expected| try std.testing.expectEqual(expected, restored.integer),
            .float => |expected| try std.testing.expectEqual(expected, restored.float),
            else => unreachable,
        }
        output.writer.end = 0;
        try std.testing.expectError(error.ProgramLimitExceeded, encodeColumnInto(a, &output.writer, column, 1, case.value, @intCast(case.size - 1)));
        try std.testing.expectEqual(@as(usize, 0), output.written().len);
        try encodeColumnInto(a, &output.writer, column, 0, case.value, 128);
        if (case.element == .float32) try std.testing.expectEqualStrings("0.1", output.written());
    }
    for ([_]struct { column: Column, value: std.json.Value }{
        .{ .column = .{ .name = "v", .type = .integer, .element_type = .int16 }, .value = .{ .integer = 32768 } },
        .{ .column = .{ .name = "v", .type = .integer, .element_type = .int32 }, .value = .{ .integer = 2147483648 } },
        .{ .column = .{ .name = "v", .type = .number, .element_type = .float32 }, .value = .{ .float = 1e100 } },
    }) |case| {
        output.writer.end = 0;
        try std.testing.expectError(error.InvalidResult, encodeColumnInto(a, &output.writer, case.column, 1, case.value, 128));
        try std.testing.expectEqual(@as(usize, 0), output.written().len);
    }
}

test "pgwire NUMERIC preserves scalar array OIDs scale and exact binary payloads" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    var context: sources.sql_numeric_value.Context = .{ .alloc = a };
    var number = try sources.sql_numeric_value.parse(&context, "9007199254740993.1200");
    defer number.deinit();
    const bytes = try sources.sql_numeric_binary.encodeAlloc(&context, number.value);
    defer a.free(bytes);
    const column: Column = .{ .name = "n", .type = .number, .element_type = .numeric };
    try std.testing.expectEqual(@as(u32, 1700), try columnOid(column));
    try std.testing.expectEqual(@as(i16, -1), columnTypeSize(column));
    try std.testing.expectEqual(@as(u32, 1231), try parameterOid(try parameterFromOid(1231)));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const decoded = try decode(arena.allocator(), 1700, 1, bytes);
    try std.testing.expectEqualStrings("9007199254740993.1200", decoded.number_string);
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    try encodeColumnInto(a, &output.writer, column, 1, .{ .string = decoded.number_string }, 4096);
    try std.testing.expectEqualSlices(u8, bytes, output.written());
    output.writer.end = 0;
    try std.testing.expectError(error.SqlProgramLimitExceeded, encodeColumnInto(a, &output.writer, column, 1, .{ .string = decoded.number_string }, 1));
    try std.testing.expectEqual(@as(usize, 0), output.written().len);
}

test "pgwire typed parameters preserve exact integers and reject binary guesses" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try decode(alloc, 20, 0, "9007199254740993")).integer);
    try std.testing.expectEqualStrings("9007199254740993.125", (try decode(alloc, 1700, 0, "9007199254740993.125")).number_string);
    try std.testing.expectError(error.SqlProtocolViolation, decode(alloc, 1700, 1, "123"));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 16, 1, &.{2}));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 20, 0, "1; DROP TABLE x"));
    const encoded = try encode(alloc, .integer, 1, .{ .string = "9007199254740993" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), std.mem.readInt(i64, encoded[0..8], .big));
}

test "pgwire UUID text and binary preserve canonical typed value" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const canonical = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    const input = "{A0EEBC999C0B4EF8BB6D6BB9BD380A11}";
    try std.testing.expectEqual(@as(u32, 2950), oid(.uuid));
    try std.testing.expectEqual(@as(i16, 16), typeSize(.uuid));
    try std.testing.expectEqualStrings(canonical, (try decode(alloc, 2950, 0, input)).string);
    const binary = try encode(alloc, .uuid, 1, .{ .string = input });
    try std.testing.expectEqual(@as(usize, 16), binary.len);
    try std.testing.expectEqualStrings(canonical, (try decode(alloc, 2950, 1, binary)).string);
    try std.testing.expectEqualStrings(canonical, try encode(alloc, .uuid, 0, .{ .string = input }));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 2950, 0, "not-a-uuid"));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 2950, 1, binary[0..15]));
}

test "pgwire JSON parameter nesting is bounded independently of frame bytes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var bytes: [130]u8 = undefined;
    @memset(bytes[0..65], '[');
    @memset(bytes[65..], ']');
    try std.testing.expectError(error.InvalidParameter, decode(arena.allocator(), 3802, 0, &bytes));
    try std.testing.expectEqualStrings("[{]}", (try decode(arena.allocator(), 3802, 0, "\"[{]}\"")).string);
}

test "pgwire timestamp conversion uses postgres epoch and refuses precision loss" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const value = std.json.Value{ .integer = 946684800000000000 };
    try std.testing.expectEqualStrings("2000-01-01T00:00:00.000000Z", try encode(alloc, .datetime, 0, value));
    const binary = try encode(alloc, .datetime, 1, value);
    try std.testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, binary[0..8], .big));
    try std.testing.expectEqual(value.integer, (try decode(alloc, 1184, 1, binary)).integer);
    try std.testing.expectError(error.UnsupportedResultPrecision, encode(alloc, .datetime, 0, .{ .integer = value.integer + 1 }));
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 1184, 1, &.{ 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }));
    var before_epoch: [8]u8 = undefined;
    std.mem.writeInt(i64, &before_epoch, -946684800000001, .big);
    try std.testing.expectError(error.InvalidParameter, decode(alloc, 1184, 1, &before_epoch));
    const high_nanos = std.math.maxInt(u64) / 1000 * 1000;
    const high = try timestampValue(alloc, high_nanos);
    try std.testing.expect(high == .number_string);
    const high_binary = try encode(alloc, .datetime, 1, high);
    try std.testing.expectEqual(high_nanos, try timestampNanos(try decode(alloc, 1184, 1, high_binary)));
    try std.testing.expectError(error.UnsupportedResultPrecision, encode(alloc, .datetime, 1, try timestampValue(alloc, std.math.maxInt(u64))));
}

test "pgwire direct cell encoding matches allocated text and binary formats" {
    const a = std.testing.allocator;
    const Case = struct { kind: Type, value: std.json.Value };
    for ([_]Case{
        .{ .kind = .integer, .value = .{ .integer = 9007199254740993 } },
        .{ .kind = .number, .value = .{ .number_string = "1.0000000000000001" } },
        .{ .kind = .boolean, .value = .{ .bool = false } },
        .{ .kind = .string, .value = .{ .string = "native row" } },
        .{ .kind = .json, .value = .null },
        .{ .kind = .uuid, .value = .{ .string = "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11" } },
        .{ .kind = .datetime, .value = .{ .integer = 946684800123456000 } },
    }) |case| for ([_]u16{ 0, 1 }) |format| {
        const expected = try encode(a, case.kind, format, case.value);
        defer a.free(expected);
        var out = std.Io.Writer.Allocating.init(a);
        defer out.deinit();
        try encodeInto(a, &out.writer, case.kind, format, case.value);
        try std.testing.expectEqualSlices(u8, expected, out.written());
    };
}

test "pgwire array columns retain element OIDs and lossless text and binary payloads" {
    const sources = @import("antfly_local_sources");
    const a = std.testing.allocator;
    const Case = struct { kind: sources.sql_array_value.ElementType, oid: u32, input: []const u8 };
    for ([_]Case{
        .{ .kind = .text, .oid = 1009, .input = "[0:2]={\"NULL\",NULL,\"a\\\"b\\\\c\"}" },
        .{ .kind = .int16, .oid = 1005, .input = "{-32768,NULL,32767}" },
        .{ .kind = .int32, .oid = 1007, .input = "{-2147483648,NULL,2147483647}" },
        .{ .kind = .int64, .oid = 1016, .input = "{-9223372036854775808,NULL,9223372036854775807}" },
        .{ .kind = .float32, .oid = 1021, .input = "{NaN,Infinity,-Infinity,NULL,1.5}" },
        .{ .kind = .float64, .oid = 1022, .input = "{NaN,Infinity,-Infinity,NULL,1.5}" },
        .{ .kind = .boolean, .oid = 1000, .input = "{t,NULL,f}" },
        .{ .kind = .uuid, .oid = 2951, .input = "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL}" },
        .{ .kind = .jsonb, .oid = 3807, .input = "{\"null\",NULL,\"{\\\"x\\\":[1,2]}\"}" },
    }) |case| {
        var owner: std.heap.ArenaAllocator = .init(a);
        defer owner.deinit();
        var value = try sources.sql_array_text.decode(a, case.kind, case.input, .{});
        defer value.deinit();
        const envelope = try sources.sql_array_wire.toJsonLeaky(owner.allocator(), value.value, .{});
        const column: Column = .{ .name = "items", .type = .array, .element_type = case.kind };
        try std.testing.expectEqual(case.oid, try columnOid(column));
        try std.testing.expectEqual(case.oid, try parameterOid(try parameterFromOid(case.oid)));
        for ([_]u16{ 0, 1 }) |format| {
            var expected: std.Io.Writer.Allocating = .init(a);
            defer expected.deinit();
            if (format == 0) try sources.sql_array_text.encode(value.value, &expected.writer, .{}) else try sources.sql_array_binary.encode(value.value, &expected.writer, .{});
            const parameter = try decode(owner.allocator(), case.oid, format, expected.written());
            var parameter_view = try sources.sql_array_wire.decode(a, case.kind, parameter, .{});
            defer parameter_view.deinit();
            var parameter_wire: std.Io.Writer.Allocating = .init(a);
            defer parameter_wire.deinit();
            if (format == 0) try sources.sql_array_text.encode(parameter_view.value, &parameter_wire.writer, .{}) else try sources.sql_array_binary.encode(parameter_view.value, &parameter_wire.writer, .{});
            try std.testing.expectEqualSlices(u8, expected.written(), parameter_wire.written());
            var actual: std.Io.Writer.Allocating = .init(a);
            defer actual.deinit();
            try encodeColumnInto(a, &actual.writer, column, format, envelope, 8 * 1024 * 1024);
            try std.testing.expectEqualSlices(u8, expected.written(), actual.written());
            actual.writer.end = 0;
            try std.testing.expectError(error.InvalidResult, encodeColumnInto(a, &actual.writer, .{ .name = "items", .type = .array }, format, envelope, 1024));
            try std.testing.expectEqual(@as(usize, 0), actual.written().len);
            try std.testing.expectError(error.SqlProgramLimitExceeded, encodeColumnInto(a, &actual.writer, column, format, envelope, 1));
            try std.testing.expectEqual(@as(usize, 0), actual.written().len);
        }
    }
    try std.testing.expectError(error.UnsupportedParameterType, oid(.array));
    try std.testing.expectError(error.InvalidResult, columnOid(.{ .name = "missing", .type = .array }));
    try std.testing.expectEqual(Type.array, try fromOid(1016));
}
