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
const temporal = @import("antfly_local_sources").sql_temporal_value;
const temporal_text = @import("antfly_local_sources").sql_temporal_text;

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

fn encodeTypedScalar(writer: *std.Io.Writer, element: @import("antfly_local_sources").sql_array_value.ElementType, format: u16, value: std.json.Value) !void {
    switch (element) {
        .int16 => {
            const number = std.math.cast(i16, try integer(value)) orelse return error.InvalidResult;
            if (format == 0) try writer.print("{d}", .{number}) else try writer.writeInt(i16, number, .big);
        },
        .int32 => {
            const number = std.math.cast(i32, try integer(value)) orelse return error.InvalidResult;
            if (format == 0) try writer.print("{d}", .{number}) else try writer.writeInt(i32, number, .big);
        },
        .float64 => {
            const number: f64 = switch (value) {
                .float => |n| n,
                .integer => |n| @floatFromInt(n),
                .number_string, .string => |n| std.fmt.parseFloat(f64, n) catch return error.InvalidResult,
                else => return error.InvalidResult,
            };
            if (format == 0) {
                if (std.math.isNan(number)) try writer.writeAll("NaN") else if (std.math.isInf(number)) try writer.writeAll(if (number < 0) "-Infinity" else "Infinity") else try writer.print("{d}", .{number});
            } else try writer.writeInt(u64, @bitCast(number), .big);
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
// A zero-buffer forwarding writer bounds output before the destination grows.
// Encoding runs once, including JSON; no full-cell sizing traversal is needed.
const BoundedWriter = struct {
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{}, .end = 0 },
    destination: *std.Io.Writer,
    remaining: usize,
    exceeded: bool = false,
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
        var count = std.math.mul(usize, data[data.len - 1].len, splat) catch return self.reject();
        for (data[0 .. data.len - 1]) |part| count = std.math.add(usize, count, part.len) catch return self.reject();
        if (count > self.remaining) return self.reject();
        const written = try self.destination.writeSplat(data, splat);
        self.remaining -= written;
        return written;
    }
    fn reject(self: *@This()) std.Io.Writer.Error {
        self.exceeded = true;
        return error.WriteFailed;
    }
};

pub fn encodeColumnInto(a: std.mem.Allocator, writer: *std.Io.Writer, column: Column, format: u16, value: std.json.Value, wire_bytes: usize) !void {
    var bounded: BoundedWriter = .{ .destination = writer, .remaining = wire_bytes };
    encodeColumnUnchecked(a, &bounded.writer, column, format, value, wire_bytes) catch |err| return if (bounded.exceeded) error.ProgramLimitExceeded else err;
}

/// Native pages retain typed arrays and numerics through the wire boundary.
pub fn encodeCellInto(a: std.mem.Allocator, writer: *std.Io.Writer, column: Column, format: u16, cell: @import("backend.zig").Cell, wire_bytes: usize) !void {
    if (cell.sql_null or format > 1) return error.InvalidResult;
    if (cell.array == null and cell.numeric == null) return encodeColumnInto(a, writer, column, format, cell.value, wire_bytes);
    _ = try columnOid(column);
    if (cell.value != .null or (cell.array != null and cell.numeric != null)) return error.InvalidResult;
    const sources = @import("antfly_local_sources");
    var bounded: BoundedWriter = .{ .destination = writer, .remaining = wire_bytes };
    if (cell.array) |array| {
        if (column.type != .array or column.element_type != array.element_type) return error.InvalidResult;
        if (format == 0) sources.sql_array_text.encode(a, array.*, &bounded.writer, .{ .wire_bytes = wire_bytes }) catch |err| return if (bounded.exceeded) error.ProgramLimitExceeded else err else sources.sql_array_binary.encode(a, array.*, &bounded.writer, .{ .wire_bytes = wire_bytes }) catch |err| return if (bounded.exceeded) error.ProgramLimitExceeded else err;
    } else if (cell.numeric) |number| {
        if (column.type != .number or column.element_type != .numeric) return error.InvalidResult;
        var context: sources.sql_numeric_value.Context = .{ .alloc = a, .max_output_bytes = wire_bytes };
        if (format == 0) sources.sql_numeric_value.write(&context, number.*, &bounded.writer) catch |err| return if (bounded.exceeded) error.ProgramLimitExceeded else err else sources.sql_numeric_binary.encode(&context, number.*, &bounded.writer) catch |err| return if (bounded.exceeded) error.ProgramLimitExceeded else err;
    }
}

fn encodeColumnUnchecked(a: std.mem.Allocator, writer: *std.Io.Writer, column: Column, format: u16, value: std.json.Value, wire_bytes: usize) !void {
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
            .int16, .int32, .float32, .float64 => {
                return encodeTypedScalar(writer, element, format, value);
            },
            else => {},
        };
        return encodeInto(a, writer, column.type, format, value);
    }
    const sources = @import("antfly_local_sources");
    // Envelope bytes and PostgreSQL output bytes are different formats. A
    // small binary result must not be rejected because its JSON metadata is
    // larger than the frame allowance. Admission below bounds actual output.
    var view = try sources.sql_array_wire.decodeBorrowed(a, column.element_type orelse return error.InvalidResult, value, .{});
    defer view.deinit();
    if (format == 0)
        try sources.sql_array_text.encode(a, view.value, writer, .{ .wire_bytes = wire_bytes })
    else
        try sources.sql_array_binary.encode(a, view.value, writer, .{ .wire_bytes = wire_bytes });
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

/// Region-owned typed ingress. PostgreSQL arrays and NUMERIC stay native;
/// JSON/JSONB remains a JSON datum, including a non-SQL-null JSON null.
pub fn decodeNative(alloc: std.mem.Allocator, param_oid: u32, format: u16, bytes: []const u8) !@import("antfly_local_sources").sql_scalar.Datum {
    var context: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = alloc };
    return decodeNativeWithContext(alloc, param_oid, format, bytes, &context);
}

pub fn decodeNativeWithContext(alloc: std.mem.Allocator, param_oid: u32, format: u16, bytes: []const u8, parent: *@import("antfly_local_sources").sql_numeric_value.Context) !@import("antfly_local_sources").sql_scalar.Datum {
    try parent.charge(0);
    const src = @import("antfly_local_sources");
    const descriptor = try parameterFromOid(param_oid);
    if (format > 1) return error.UnsupportedParameterFormat;
    if (param_oid == 700 or param_oid == 701) {
        var work: src.sql_array_value.Budget = .{ .shared = parent };
        const kind: src.sql_array_value.ElementType = if (param_oid == 700) .float32 else .float64;
        return if (format == 0) src.sql_array_text.decodeElementLeaky(alloc, kind, bytes, &work) else src.sql_array_binary.decodeElementLeaky(alloc, kind, bytes, &work);
    }
    if (param_oid == 1700) {
        var context: src.sql_numeric_value.Context = .{ .alloc = alloc, .parent = parent };
        const number = if (format == 0) try src.sql_numeric_value.parse(&context, bytes) else try src.sql_numeric_binary.decode(&context, bytes, .{});
        const value = try alloc.create(src.sql_numeric_value.Value);
        value.* = number.value;
        return src.sql_scalar.Datum.typedNumeric(value);
    }
    if (descriptor.kind == .array) {
        const kind = descriptor.element_type orelse return error.UnsupportedParameterType;
        const array = if (format == 0) (try src.sql_array_text.decodeLeaky(alloc, kind, bytes, .{ .compact = true, .context = parent })).value else (try src.sql_array_binary.decodeLeaky(alloc, kind, bytes, .{ .compact = true, .context = parent })).value;
        const value = try alloc.create(src.sql_array_value.Value);
        value.* = array;
        return src.sql_scalar.Datum.typedArray(value);
    }
    // Timestamp decoding already normalizes PostgreSQL epoch/precision. The
    // SQL frame consumes canonical text, not the transport's integer sentinel.
    const raw = try decodeWithContext(alloc, param_oid, format, bytes, parent);
    if (descriptor.kind == .datetime) {
        const value = try alloc.create(temporal.Value);
        value.* = .{ .timestamp_tz = try timestampFromValue(raw) };
        return src.sql_scalar.Datum.typedTemporal(value);
    }
    return src.sql_scalar.Datum.json(raw);
}

pub fn decode(alloc: std.mem.Allocator, param_oid: u32, format: u16, bytes: []const u8) !std.json.Value {
    var context: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = alloc };
    return decodeWithContext(alloc, param_oid, format, bytes, &context);
}

/// One Bind owner is shared by all parameters and every nested typed codec.
/// Neither a new parameter nor conversion into the backend envelope renews it.
/// The allocator belongs to the retained portal region, never to this context.
pub fn decodeWithContext(alloc: std.mem.Allocator, param_oid: u32, format: u16, bytes: []const u8, context: *@import("antfly_local_sources").sql_numeric_value.Context) !std.json.Value {
    try context.charge(0);
    var offset: usize = 0;
    while (offset < bytes.len) {
        const end = offset + @min(bytes.len - offset, 256);
        try context.charge(end - offset);
        offset = end;
    }
    const kind = try fromOid(param_oid);
    if (format > 1) return error.UnsupportedParameterFormat;
    if (param_oid == 3802) {
        const sources = @import("antfly_local_sources");
        var work: sources.sql_array_value.Budget = .{ .shared = context };
        const datum = if (format == 0)
            try sources.sql_array_text.decodeElementLeaky(alloc, .jsonb, bytes, &work)
        else
            try sources.sql_array_binary.decodeElementLeaky(alloc, .jsonb, bytes, &work);
        return datum.value;
    }
    if (param_oid == 1700) {
        const sources = @import("antfly_local_sources");
        var numeric: sources.sql_numeric_value.Context = .{ .alloc = alloc, .parent = context };
        var number = if (format == 0) try sources.sql_numeric_value.parse(&numeric, bytes) else try sources.sql_numeric_binary.decode(&numeric, bytes, .{});
        defer number.deinit();
        return .{ .number_string = try sources.sql_numeric_value.format(&numeric, number.value) };
    }
    if (kind == .array) {
        const sources = @import("antfly_local_sources");
        const element = (try parameterFromOid(param_oid)).element_type orelse return error.UnsupportedParameterType;
        var decoded = if (format == 0) try sources.sql_array_text.decode(alloc, element, bytes, .{ .context = context }) else try sources.sql_array_binary.decode(alloc, element, bytes, .{ .context = context });
        defer decoded.deinit();
        return sources.sql_array_wire.toJsonLeaky(alloc, decoded.value, .{ .context = context });
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
            const value = temporal.decode(.timestamp_tz, bytes) catch return error.InvalidParameter;
            break :blk try timestampEnvelope(alloc, value.timestamp_tz);
        },
        114 => try parseJson(alloc, bytes),
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
            break :blk try encodeInteger(alloc, (try timestampFromValue(value)).micros);
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
pub fn encodeInto(_: std.mem.Allocator, writer: *std.Io.Writer, kind: Type, format: u16, value: std.json.Value) !void {
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
                var text: [temporal_text.output_capacity]u8 = undefined;
                try writer.writeAll(try temporal_text.format(.{ .timestamp_tz = try timestampFromValue(value) }, &text));
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
            try writer.writeInt(i64, (try timestampFromValue(value)).micros, .big);
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
    var text: [temporal_text.output_capacity]u8 = undefined;
    return alloc.dupe(u8, try temporal_text.format(.{ .timestamp_tz = try timestampFromValue(value) }, &text));
}

/// Exact signed transport nanoseconds, not the PostgreSQL binary epoch.
/// i128 covers every finite PostgreSQL timestamp; infinities stay distinct.
pub fn timestampNanos(value: std.json.Value) !i128 {
    return switch (value) {
        .integer => |ns| ns,
        .number_string => |text| std.fmt.parseInt(i128, text, 10) catch error.InvalidResult,
        else => error.InvalidResult,
    };
}

pub fn timestampValue(alloc: std.mem.Allocator, nanos: i128) !std.json.Value {
    if (std.math.cast(i64, nanos)) |signed| return .{ .integer = signed };
    return .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{nanos}) };
}

pub fn timestampEnvelope(alloc: std.mem.Allocator, value: temporal.Timestamp) !std.json.Value {
    _ = try temporal.Timestamp.init(value.micros);
    if (value.unixNanoseconds()) |ns| return timestampValue(alloc, ns);
    return .{ .string = try alloc.dupe(u8, if (value.infinity() < 0) "-infinity" else "infinity") };
}

fn timestampFromNanos(ns: i128) !temporal.Timestamp {
    return temporal.Timestamp.fromUnixNanoseconds(ns) catch |err| switch (err) {
        error.TemporalPrecisionLoss => error.UnsupportedResultPrecision,
        else => error.InvalidResult,
    };
}

/// Canonical PostgreSQL UTC text and the existing native RFC3339 boundary
/// share one exact value. This is not a general SQL timestamp literal parser.
pub fn timestampFromValue(value: std.json.Value) !temporal.Timestamp {
    if (value == .string) {
        if (temporal_text.parseCanonical(.timestamp_tz, value.string)) |parsed| return parsed.timestamp_tz else |_| {}
        const ns = @import("antfly_local_sources").datetime.parseDateTimeToSignedNs(value.string) orelse return error.InvalidResult;
        return timestampFromNanos(ns);
    }
    return timestampFromNanos(try timestampNanos(value));
}

/// Normalize a result without narrowing finite time or converting infinity
/// into a SQL NULL. Legacy sub-microsecond cells remain exact until the wire
/// encoder explicitly refuses unsupported precision.
pub fn timestampResult(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    if (value == .null) return value;
    if (value == .string) {
        if (temporal_text.parseCanonical(.timestamp_tz, value.string)) |parsed| return timestampEnvelope(alloc, parsed.timestamp_tz) else |_| {}
        const ns = @import("antfly_local_sources").datetime.parseDateTimeToSignedNs(value.string) orelse return error.InvalidResult;
        return timestampValue(alloc, ns);
    }
    return timestampValue(alloc, try timestampNanos(value));
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

test "pgwire Bind admission shares cumulative work and sticky cancellation across typed codecs" {
    const a = std.testing.allocator;
    const Context = @import("antfly_local_sources").sql_numeric_value.Context;
    var wide: [1026]u8 = undefined;
    @memset(&wide, 'a');
    wide[0] = '"';
    wide[wide.len - 1] = '"';
    const sources = @import("antfly_local_sources");
    var source = try sources.sql_array_text.decode(a, .jsonb, "{1.20,null,NULL}", .{});
    defer source.deinit();
    var binary: std.Io.Writer.Allocating = .init(a);
    defer binary.deinit();
    try sources.sql_array_binary.encode(a, source.value, &binary.writer, .{});
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = 0,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    for ([_]struct { oid: u32, format: u16 = 0, bytes: []const u8 }{
        .{ .oid = 20, .format = 1, .bytes = "\x00\x00\x00\x00\x00\x00\x00\x01" },
        .{ .oid = 25, .bytes = "leaf" },
        .{ .oid = 1700, .bytes = "1.20" },
        .{ .oid = 3802, .bytes = "{\"k\":1.20}" },
        .{ .oid = 3802, .format = 1, .bytes = "\x01{\"k\":1.20}" },
        .{ .oid = 3802, .bytes = &wide },
        .{ .oid = 3807, .bytes = "{1.20,null,NULL}" },
        .{ .oid = 3807, .format = 1, .bytes = binary.written() },
        .{ .oid = 1231, .bytes = "{1.20,NULL}" },
    }) |case| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        var observed: Control = .{};
        var parent: Context = .{ .alloc = arena.allocator(), .remaining = 1024 * 1024, .checkpoint = Control.check, .ptr = &observed };
        var child: Context = .{ .alloc = arena.allocator(), .remaining = parent.remaining, .parent = &parent };
        _ = try decodeWithContext(arena.allocator(), case.oid, case.format, case.bytes, &child);
        const spent = 1024 * 1024 - parent.remaining;
        try std.testing.expect(spent >= case.bytes.len);
        try std.testing.expectEqual(parent.remaining, child.remaining);
        var limited: Context = .{ .alloc = arena.allocator(), .remaining = spent * 2 - 1 };
        _ = try decodeWithContext(arena.allocator(), case.oid, case.format, case.bytes, &limited);
        try std.testing.expectError(error.SqlProgramLimitExceeded, decodeWithContext(arena.allocator(), case.oid, case.format, case.bytes, &limited));
        const remaining = limited.remaining;
        try std.testing.expectError(error.SqlProgramLimitExceeded, decodeWithContext(.failing, case.oid, case.format, case.bytes, &limited));
        try std.testing.expectEqual(remaining, limited.remaining);
        for (1..observed.polls + 1) |cut| {
            var control: Control = .{ .fail_at = cut };
            var canceled: Context = .{ .alloc = arena.allocator(), .checkpoint = Control.check, .ptr = &control };
            try std.testing.expectError(error.QueryCanceled, decodeWithContext(arena.allocator(), case.oid, case.format, case.bytes, &canceled));
            const left = canceled.remaining;
            try std.testing.expectError(error.QueryCanceled, decodeWithContext(.failing, case.oid, case.format, case.bytes, &canceled));
            try std.testing.expectEqual(left, canceled.remaining);
        }
    }
    var healthy: Context = .{ .alloc = .failing };
    try std.testing.expectError(error.OutOfMemory, decodeWithContext(.failing, 3802, 0, "{\"k\":1}", &healthy));
    try std.testing.expect(healthy.failure == null);
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

test "pgwire Bind fixed width admission scales without decoder allocations" {
    const Context = @import("antfly_local_sources").sql_numeric_value.Context;
    for ([_]usize{ 1, 16, 256 }) |count| {
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..1000) |_| {
            var work: Context = .{ .alloc = .failing, .remaining = count * 8 };
            for (0..count) |_| {
                const value = try decodeWithContext(.failing, 20, 1, "\x00\x00\x00\x00\x00\x00\x00\x01", &work);
                try std.testing.expectEqual(@as(i64, 1), value.integer);
            }
            try std.testing.expectEqual(@as(u64, 0), work.remaining);
        }
        std.debug.print("pgwire fixed-width admission: parameters={d} iterations=1000 work_per_bind={d} decoder_allocations=0 elapsed_ns={d}\n", .{ count, count * 8, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
    }
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
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(arena.allocator(), 3802, 0, &bytes));
    try std.testing.expectEqualStrings("[{]}", (try decode(arena.allocator(), 3802, 0, "\"[{]}\"")).string);
}

test "pgwire timestamp conversion uses postgres epoch and refuses precision loss" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const value = std.json.Value{ .integer = 946684800000000000 };
    try std.testing.expectEqualStrings("2000-01-01 00:00:00+00", try encode(alloc, .datetime, 0, value));
    const binary = try encode(alloc, .datetime, 1, value);
    try std.testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, binary[0..8], .big));
    try std.testing.expectEqual(value.integer, (try decode(alloc, 1184, 1, binary)).integer);
    try std.testing.expectError(error.UnsupportedResultPrecision, encode(alloc, .datetime, 0, .{ .integer = value.integer + 1 }));
    try std.testing.expectEqualStrings("infinity", (try decode(alloc, 1184, 1, &.{ 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff })).string);
    var before_epoch: [8]u8 = undefined;
    std.mem.writeInt(i64, &before_epoch, -946684800000001, .big);
    try std.testing.expectEqual(@as(i128, -1000), try timestampNanos(try decode(alloc, 1184, 1, &before_epoch)));
    const high_nanos = std.math.maxInt(u64) / 1000 * 1000;
    const high = try timestampValue(alloc, high_nanos);
    try std.testing.expect(high == .number_string);
    const high_binary = try encode(alloc, .datetime, 1, high);
    try std.testing.expectEqual(@as(i128, high_nanos), try timestampNanos(try decode(alloc, 1184, 1, high_binary)));
    try std.testing.expectError(error.UnsupportedResultPrecision, encode(alloc, .datetime, 1, try timestampValue(alloc, std.math.maxInt(u64))));
}

test "pgwire timestamp codecs match full-domain PostgreSQL fixtures with allocation-free streaming" {
    const Entry = struct { kind: temporal.Kind, binary: []const u8, text: []const u8 };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, std.testing.allocator, @import("antfly_local_sources").sql_temporal_reference, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn run(a: std.mem.Allocator, entries: []const Entry) !void {
            const alloc = a;
            var count: usize = 0;
            for (entries) |entry| {
                if (entry.kind != .timestamp_tz) continue;
                count += 1;
                var frame: [8]u8 = undefined;
                const expected = try std.fmt.hexToBytes(&frame, entry.binary);
                const value = try decode(alloc, 1184, 1, expected);
                defer switch (value) {
                    .string, .number_string => |bytes| alloc.free(bytes),
                    else => {},
                };
                const encoded_binary = try encode(alloc, .datetime, 1, value);
                defer alloc.free(encoded_binary);
                try std.testing.expectEqualSlices(u8, expected, encoded_binary);
                const encoded_text = try encode(alloc, .datetime, 0, value);
                defer alloc.free(encoded_text);
                try std.testing.expectEqualStrings(entry.text, encoded_text);
                const canonical = try timestampResult(alloc, .{ .string = entry.text });
                defer switch (canonical) {
                    .string, .number_string => |bytes| alloc.free(bytes),
                    else => {},
                };
                const canonical_binary = try encode(alloc, .datetime, 1, canonical);
                defer alloc.free(canonical_binary);
                try std.testing.expectEqualSlices(u8, expected, canonical_binary);
                try std.testing.expectEqual((try temporal.decode(.timestamp_tz, expected)).timestamp_tz.micros, (try timestampFromValue(value)).micros);
                var none = std.heap.FixedBufferAllocator.init(&.{});
                var storage: [128]u8 = undefined;
                var output = std.Io.Writer.fixed(&storage);
                try encodeColumnInto(none.allocator(), &output, .{ .name = "v", .type = .datetime }, 0, value, entry.text.len);
                try std.testing.expectEqualStrings(entry.text, output.buffered());
                output.end = 0;
                try encodeColumnInto(none.allocator(), &output, .{ .name = "v", .type = .datetime }, 1, value, 8);
                try std.testing.expectEqualSlices(u8, expected, output.buffered());
                output.end = 0;
                try std.testing.expectError(error.ProgramLimitExceeded, encodeColumnInto(none.allocator(), &output, .{ .name = "v", .type = .datetime }, 0, value, entry.text.len - 1));
                try std.testing.expectEqual(@as(usize, 0), output.buffered().len);
                for (0..8) |length| try std.testing.expectError(error.InvalidParameter, decode(alloc, 1184, 1, expected[0..length]));
            }
            try std.testing.expectEqual(@as(usize, 36), count);
        }
    };
    try Run.run(std.testing.allocator, fixture.value.entries);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{fixture.value.entries});
    var invalid: [8]u8 = undefined;
    std.mem.writeInt(i64, &invalid, temporal.end_timestamp, .big);
    try std.testing.expectError(error.InvalidParameter, decode(std.testing.allocator, 1184, 1, &invalid));
    for ([_]std.json.Value{ .{ .integer = -1 }, .{ .number_string = "946684800000000001" }, .{ .string = "2000-01-01T00:00:00.000000001Z" } }) |value| {
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var storage: [128]u8 = @splat(0xa5);
        var output = std.Io.Writer.fixed(&storage);
        for ([_]u16{ 0, 1 }) |format| {
            try std.testing.expectError(error.UnsupportedResultPrecision, encodeInto(none.allocator(), &output, .datetime, format, value));
            try std.testing.expectEqual(@as(usize, 0), output.buffered().len);
            for (storage) |byte| try std.testing.expectEqual(@as(u8, 0xa5), byte);
        }
    }
}

test "pgwire timestamp streaming throughput uses fixed caller storage without heap" {
    const cases = [_]std.json.Value{
        .{ .integer = 946684800000000000 },
        .{ .integer = -1000 },
        .{ .number_string = "18000000000123456000" },
        .{ .string = "infinity" },
        .{ .string = "0001-01-01 00:00:00+00 BC" },
        .{ .string = "294276-12-31 23:59:59.999999+00" },
    };
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var storage: [128]u8 = undefined;
    var output = std.Io.Writer.fixed(&storage);
    var checksum: u64 = 0;
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..100_000) |index| {
        for ([_]u16{ 0, 1 }) |format| {
            output.end = 0;
            try encodeInto(none.allocator(), &output, .datetime, format, cases[index % cases.len]);
            checksum +%= output.buffered()[0];
        }
    }
    try std.testing.expect(checksum != 0);
    std.debug.print("pgwire timestamp streaming: values=100000 cell_encodes=200000 buffer_bytes=128 heap_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
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

test "pgwire JSONB parameter admission shares scalar and array domain errors" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    for ([_]struct { input: []const u8, code: []const u8 }{
        .{ .input = "1e131072", .code = "22003" },
        .{ .input = "0e-16384", .code = "22003" },
        .{ .input = "{\"k\":1e131072,\"k\":2}", .code = "22003" },
        .{ .input = "{\"k\":\"\\u0000\",\"k\":2}", .code = "22P05" },
        .{ .input = "01", .code = "22P02" },
    }) |entry| for ([_]u16{ 0, 1 }) |format| {
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        const r = region.allocator();
        var binary: std.Io.Writer.Allocating = .init(r);
        try binary.writer.writeByte(1);
        try binary.writer.writeAll(entry.input);
        const input = if (format == 0) entry.input else binary.written();
        if (decode(r, 3802, format, input)) |_| return error.ExpectedPostgresRejection else |err| try std.testing.expectEqualStrings(entry.code, sources.sql_errors.describe(err).code);
    };
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const value = try decode(region.allocator(), 3802, 0, "{\"k\":1,\"\\u006b\":1.20}");
    try std.testing.expectEqual(@as(usize, 1), value.object.count());
    try std.testing.expectEqualStrings("1.20", value.object.get("k").?.number_string);
    try std.testing.expectError(error.SqlProtocolViolation, decode(region.allocator(), 3802, 1, ""));
    try std.testing.expectError(error.SqlJsonbUnsupportedVersion, decode(region.allocator(), 3802, 1, "\x02null"));
    try std.testing.expectError(error.SqlInvalidTextRepresentation, decode(region.allocator(), 3802, 1, "\x01"));
    try std.testing.expectError(error.SqlInvalidTextEncoding, decode(region.allocator(), 3802, 1, "\x01\"\xff\""));
}

test "pgwire JSONB array columns emit exact PostgreSQL display and binary payloads" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const owned = region.allocator();
    // Independently captured in sql_jsonb_array_storage_reference.json.
    const text = "[-2:0]={1.20,\"null\",NULL}";
    const hex = "000000010000000100000eda00000003fffffffe0000000501312e323000000005016e756c6cffffffff";
    var source = try sources.sql_array_text.decode(a, .jsonb, text, .{});
    defer source.deinit();
    const envelope = try sources.sql_array_wire.toJsonLeaky(owned, source.value, .{});
    const column: Column = .{ .name = "j", .type = .array, .element_type = .jsonb };
    try std.testing.expectEqual(@as(u32, 3807), try columnOid(column));
    const bytes = try a.alloc(u8, hex.len / 2);
    defer a.free(bytes);
    _ = try std.fmt.hexToBytes(bytes, hex);
    for ([_]u16{ 0, 1 }) |format| {
        const expected = if (format == 0) text else bytes;
        var actual: std.Io.Writer.Allocating = .init(a);
        defer actual.deinit();
        try encodeColumnInto(a, &actual.writer, column, format, envelope, expected.len);
        try std.testing.expectEqualSlices(u8, expected, actual.written());
        actual.writer.end = 0;
        try std.testing.expectError(error.SqlProgramLimitExceeded, encodeColumnInto(a, &actual.writer, column, format, envelope, expected.len - 1));
        try std.testing.expectEqual(@as(usize, 0), actual.written().len);
    }
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
            if (format == 0) try sources.sql_array_text.encode(a, value.value, &expected.writer, .{}) else try sources.sql_array_binary.encode(a, value.value, &expected.writer, .{});
            const parameter = try decode(owner.allocator(), case.oid, format, expected.written());
            var parameter_view = try sources.sql_array_wire.decode(a, case.kind, parameter, .{});
            defer parameter_view.deinit();
            var parameter_wire: std.Io.Writer.Allocating = .init(a);
            defer parameter_wire.deinit();
            if (format == 0) try sources.sql_array_text.encode(a, parameter_view.value, &parameter_wire.writer, .{}) else try sources.sql_array_binary.encode(a, parameter_view.value, &parameter_wire.writer, .{});
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

test "pgwire native array encoding avoids JSON conversion workspace" {
    const a = std.testing.allocator;
    const sources = @import("antfly_local_sources");
    const elements = try a.alloc(sources.sql_array_value.Element, 4096);
    defer a.free(elements);
    for (elements, 0..) |*element, i| element.* = .{ .value = if (i % 17 == 0) .null else .{ .integer = @intCast(i) }, .sql_null = i % 17 == 0 };
    const array: sources.sql_array_value.Value = .{ .element_type = .int64, .dimensions = &.{.{ .length = @intCast(elements.len), .lower = -7 }}, .elements = elements };
    const column: Column = .{ .name = "a", .type = .array, .element_type = .int64 };
    const Counter = sources.storage_lite_test_allocator.BudgetAllocator;
    var peaks: [2]usize = undefined;
    var calls: [2]usize = undefined;
    var hashes: [2]u64 = undefined;
    for (0..2) |mode| {
        var measured: Counter = .{ .backing = a };
        var output: std.Io.Writer.Allocating = .init(measured.allocator());
        {
            var arena = std.heap.ArenaAllocator.init(measured.allocator());
            defer arena.deinit();
            if (mode == 0) {
                const envelope = try sources.sql_array_wire.toJsonLeaky(arena.allocator(), array, .{});
                try encodeColumnInto(arena.allocator(), &output.writer, column, 1, envelope, 8 * 1024 * 1024);
            } else {
                try encodeCellInto(arena.allocator(), &output.writer, column, 1, .{ .array = &array, .sql_null = false }, 8 * 1024 * 1024);
                const cold = measured.alloc_calls;
                for (0..300) |_| {
                    output.writer.end = 0;
                    try encodeCellInto(arena.allocator(), &output.writer, column, 1, .{ .array = &array, .sql_null = false }, 8 * 1024 * 1024);
                }
                try std.testing.expectEqual(cold, measured.alloc_calls);
            }
            hashes[mode] = std.hash.Wyhash.hash(0, output.written());
            peaks[mode] = measured.peak;
            calls[mode] = measured.alloc_calls;
        }
        output.deinit();
        try std.testing.expectEqual(@as(usize, 0), measured.live);
    }
    try std.testing.expectEqual(hashes[0], hashes[1]);
    try std.testing.expect(peaks[1] < peaks[0]);
    try std.testing.expect(calls[1] < calls[0]);
    std.debug.print("PGWIRE_NATIVE_ARRAY elements=4096 before_peak={d} after_peak={d} before_allocations={d} after_allocations={d} warm_repetitions=300 warm_allocations=0\n", .{ peaks[0], peaks[1], calls[0], calls[1] });
}

test "pgwire native parameter decoding keeps arrays NUMERIC and JSON null typed" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const owned = arena.allocator();
            const array = try decodeNative(owned, 1016, 0, "[-1:1]={9007199254740993,NULL,-7}");
            try std.testing.expectEqual(@as(i64, 9007199254740993), array.array.?.flatCell(0).value.integer);
            try std.testing.expect(array.array.?.flatCell(1).sql_null);
            try std.testing.expectEqual(@as(i32, -1), array.array.?.dimensions[0].lower);
            const number = try decodeNative(owned, 1700, 0, "9007199254740993.1200");
            try std.testing.expect(number.numeric != null and number.value == .null and !number.sql_null);
            const json = try decodeNative(owned, 3802, 1, "\x01null");
            try std.testing.expect(json.value == .null and !json.sql_null);
            var bytes: std.Io.Writer.Allocating = .init(owned);
            @import("antfly_local_sources").sql_array_binary.encode(owned, array.array.?.*, &bytes.writer, .{}) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            const binary = try decodeNative(owned, 1016, 1, bytes.written());
            var work: @import("antfly_local_sources").sql_array_value.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try array.array.?.compare(binary.array.?.*, &work));
            bytes.writer.end = 0;
            var context: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = owned };
            @import("antfly_local_sources").sql_numeric_binary.encode(&context, number.numeric.?.*, &bytes.writer) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            const binary_number = try decodeNative(owned, 1700, 1, bytes.written());
            try std.testing.expectEqual(std.math.Order.eq, try @import("antfly_local_sources").sql_scalar.compareDatums(number, binary_number));
        }
    };
    var stable: @import("antfly_local_sources").storage_lite_test_allocator.NoResizeAllocator = .{ .backing = std.testing.allocator };
    try std.testing.checkAllAllocationFailures(stable.allocator(), Probe.run, .{});
}

test "pgwire native parameter decoding avoids public array envelope allocations" {
    const a = std.testing.allocator;
    const src = @import("antfly_local_sources");
    const test_alloc = src.storage_lite_test_allocator;
    const elements = try a.alloc(src.sql_array_value.Element, 32768);
    defer a.free(elements);
    @memset(elements, .json(.{ .integer = 42 }));
    const array: src.sql_array_value.Value = .{ .element_type = .int64, .dimensions = &.{.{ .length = 32768 }}, .elements = elements };
    var wire_bytes: std.Io.Writer.Allocating = .init(a);
    defer wire_bytes.deinit();
    try src.sql_array_binary.encode(a, array, &wire_bytes.writer, .{});
    var peaks: [2]usize = undefined;
    for ([_]bool{ false, true }, &peaks) |native, *peak| {
        var nr: test_alloc.NoResizeAllocator = .{ .backing = a };
        var budget: test_alloc.BudgetAllocator = .{ .backing = nr.allocator() };
        {
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            if (native) {
                const result = try decodeNative(arena.allocator(), 1016, 1, wire_bytes.written());
                try std.testing.expectEqual(@as(usize, 32768), result.array.?.cardinality());
            } else {
                const result = try decode(arena.allocator(), 1016, 1, wire_bytes.written());
                try std.testing.expectEqual(@as(usize, 32768), result.object.get("values").?.array.items.len);
            }
            peak.* = budget.peak;
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expect(peaks[1] < peaks[0] / 2);
    std.debug.print("PGWIRE_NATIVE_PARAMETERS public_peak={d} native_peak={d}\n", .{ peaks[0], peaks[1] });
}

test "pgwire native scalar float parameters roundtrip through typed frames in text and binary" {
    const src = @import("antfly_local_sources");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for ([_]u32{ 700, 701 }) |type_oid| for ([_][]const u8{ "1.25", "NaN", "Infinity", "-Infinity" }) |input| {
        const parameter = try parameterFromOid(type_oid);
        const cell = try decodeNative(arena.allocator(), type_oid, 0, input);
        var frame = try src.sql_parameter_frame.Frame.prepare(a, &.{.{ .kind = .number, .element_type = parameter.element_type }}, &.{.{ .datum = cell }}, .{});
        defer frame.deinit();
        var output: std.Io.Writer.Allocating = .init(arena.allocator());
        try encodeCellInto(arena.allocator(), &output.writer, .{ .name = "n", .type = .number, .element_type = parameter.element_type }, 1, .{ .value = frame.values[0].value, .sql_null = frame.values[0].sql_null }, 1024);
        const binary_cell = try decodeNative(arena.allocator(), type_oid, 1, output.written());
        try std.testing.expectEqual(std.math.Order.eq, try src.sql_scalar.compareDatums(frame.values[0], binary_cell));
    };
}
