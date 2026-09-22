// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Schema-bound SQL projection over native document snapshots. Shape is derived
//! only from declarations, never from sampled rows. All returned values belong
//! to the supplied request/page arena; a backend must retain its native read
//! transaction across pages and must not use this decoder to manufacture one.
const std = @import("std");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");
const typed_json = @import("../storage/typed_json.zig");
const datetime = @import("../datetime.zig");
const Json = std.json.Value;

/// Generic over the parsed native schema to keep the SQL decoder independent
/// of native storage/build dependencies. Unspecified/dynamic fields are not
/// columns. Conflicting declarations have a stable JSON supertype.
pub fn deriveColumns(alloc: std.mem.Allocator, schema: anytype) ![]const catalog.Column {
    const Accumulator = struct { column: catalog.Column, required_count: usize };
    var entries: std.StringArrayHashMapUnmanaged(Accumulator) = .empty;
    defer entries.deinit(alloc);
    for (schema.document_schemas) |document| {
        for (document.properties) |property| {
            if (std.mem.eql(u8, property.name, "_id")) continue;
            const kind = propertyType(property);
            const required = for (document.required_fields) |name| {
                if (std.mem.eql(u8, name, property.name)) break true;
            } else false;
            const nullable = !required or property.allows_null or property.field_type == null;
            const entry = try entries.getOrPut(alloc, property.name);
            if (!entry.found_existing) entry.value_ptr.* = .{
                .column = .{ .name = property.name, .path = property.name, .type = kind, .nullable = nullable },
                .required_count = @intFromBool(!nullable),
            } else {
                if (entry.value_ptr.column.type != kind) {
                    entry.value_ptr.column.type = .json;
                    entry.value_ptr.column.nullable = true;
                }
                entry.value_ptr.column.nullable = entry.value_ptr.column.nullable or nullable;
                entry.value_ptr.required_count += @intFromBool(!nullable);
            }
        }
    }
    const columns = try alloc.alloc(catalog.Column, entries.count());
    for (entries.values(), columns) |entry, *column| {
        column.* = entry.column;
        column.nullable = entry.column.nullable or entry.required_count != schema.document_schemas.len;
        column.name = try alloc.dupe(u8, entry.column.name);
        column.path = column.name;
    }
    std.mem.sort(catalog.Column, columns, {}, struct {
        fn lessThan(_: void, a: catalog.Column, b: catalog.Column) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);
    return columns;
}

fn propertyType(property: anytype) ast.ColumnType {
    const name = property.field_type orelse return .json;
    if (std.mem.eql(u8, name, "integer") or property.integer_only) return .integer;
    if (std.mem.eql(u8, name, "number") or std.mem.eql(u8, name, "numeric")) return .number;
    if (std.mem.eql(u8, name, "boolean")) return .boolean;
    if (std.mem.eql(u8, name, "datetime")) return .datetime;
    if (std.mem.eql(u8, name, "string") or std.mem.eql(u8, name, "text") or
        std.mem.eql(u8, name, "keyword") or std.mem.eql(u8, name, "html") or std.mem.eql(u8, name, "link")) return .string;
    return .json;
}

pub fn decode(alloc: std.mem.Allocator, table: catalog.Table, id: []const u8, version: u64, bytes: []const u8, fields: []const []const u8) !catalog.Row {
    return (try Projection.init(alloc, table, fields)).decode(alloc, id, version, bytes);
}

/// Native document nulls have no SQL-null bitmap. A missing field is SQL NULL;
/// an explicit null in a JSON column is a JSON datum. Scalar explicit null is
/// SQL NULL. Names are literal root members, not dotted path expressions.
pub fn projectValue(alloc: std.mem.Allocator, table: catalog.Table, id: []const u8, version: u64, root: Json, fields: []const []const u8) !catalog.Row {
    return (try Projection.init(alloc, table, fields)).projectValue(alloc, id, version, root);
}

/// Compile once per scan, not once per row. Binding is O(schema + projection)
/// and decoding O(projected fields + selected JSON bytes), independent of the
/// width of the declared schema. The scan arena owns all names and slots.
pub const Projection = struct {
    columns: []const catalog.Column,

    pub fn init(alloc: std.mem.Allocator, table: catalog.Table, fields: []const []const u8) !Projection {
        var declared: std.StringHashMapUnmanaged(catalog.Column) = .empty;
        defer declared.deinit(alloc);
        try declared.ensureTotalCapacity(alloc, @intCast(table.columns.len));
        for (table.columns) |column| try declared.put(alloc, column.name, column);
        var selected: std.StringArrayHashMapUnmanaged(catalog.Column) = .empty;
        defer selected.deinit(alloc);
        for (fields) |name| {
            if (std.mem.eql(u8, name, "_id")) continue;
            const column = declared.get(name) orelse return error.UndefinedColumn;
            try selected.put(alloc, name, column);
        }
        const columns = try alloc.alloc(catalog.Column, selected.count());
        for (selected.values(), columns) |column, *out| {
            out.* = column;
            out.name = try alloc.dupe(u8, column.name);
            out.path = if (std.mem.eql(u8, column.name, column.path)) out.name else try alloc.dupe(u8, column.path);
        }
        return .{ .columns = columns };
    }

    pub fn decode(self: Projection, alloc: std.mem.Allocator, id: []const u8, version: u64, bytes: []const u8) !catalog.Row {
        var parsed = std.json.parseFromSlice(Json, alloc, bytes, .{ .parse_numbers = false }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSqlBackendResponse,
        };
        defer parsed.deinit();
        return self.projectValue(alloc, id, version, parsed.value);
    }

    pub fn projectValue(self: Projection, alloc: std.mem.Allocator, id: []const u8, version: u64, root: Json) !catalog.Row {
        if (root != .object) return error.InvalidSqlBackendResponse;
        var object: std.json.ObjectMap = .empty;
        try object.ensureTotalCapacity(alloc, @intCast(self.columns.len));
        const nulls = try alloc.alloc(bool, self.columns.len);
        for (self.columns, nulls) |column, *sql_null| {
            const raw = root.object.get(column.path);
            sql_null.* = raw == null or (raw.? == .null and column.type != .json);
            const value: Json = if (sql_null.*) .null else try coerce(alloc, raw.?, column.type);
            try object.put(alloc, try alloc.dupe(u8, column.name), value);
        }
        return .{ .id = try alloc.dupe(u8, id), .version = version, .value = .{ .object = object }, .sql_nulls = nulls };
    }
};

fn coerce(alloc: std.mem.Allocator, value: Json, kind: ast.ColumnType) !Json {
    return switch (kind) {
        .json => typed_json.clone(alloc, value),
        .integer => switch (value) {
            .integer => value,
            .number_string => |text| .{ .integer = try exactInteger(text) },
            else => error.SqlTypeMismatch,
        },
        .number => blk: {
            const number: f64 = switch (value) {
                .integer => |integer| @floatFromInt(integer),
                .float => |number| number,
                .number_string => |text| std.fmt.parseFloat(f64, text) catch return error.SqlTypeMismatch,
                else => return error.SqlTypeMismatch,
            };
            if (!std.math.isFinite(number)) return error.SqlNumericOutOfRange;
            break :blk .{ .float = number };
        },
        .boolean => if (value == .bool) value else error.SqlTypeMismatch,
        .string => if (value == .string) typed_json.clone(alloc, value) else error.SqlTypeMismatch,
        .datetime => blk: {
            const ns: u64 = switch (value) {
                .integer => |integer| std.math.cast(u64, integer) orelse return error.SqlTypeMismatch,
                .number_string => |text| std.fmt.parseInt(u64, text, 10) catch return error.SqlTypeMismatch,
                .string => |text| datetime.parseDateTimeToNs(text) orelse (std.fmt.parseInt(u64, text, 10) catch return error.SqlTypeMismatch),
                else => return error.SqlTypeMismatch,
            };
            break :blk .{ .string = try datetime.formatDateTimeNsAlloc(alloc, ns) };
        },
    };
}

/// The input is an already validated JSON numeric token. Accept integral
/// decimal/exponent spellings without rounding through binary floating point.
fn exactInteger(text: []const u8) !i64 {
    if (text.len == 0) return error.SqlTypeMismatch;
    const negative = text[0] == '-';
    var at: usize = @intFromBool(negative);
    var digits: usize = 0;
    var before: ?usize = null;
    var first: ?usize = null;
    var last: usize = 0;
    var magnitude: u64 = 0;
    while (at < text.len and text[at] != 'e' and text[at] != 'E') : (at += 1) {
        if (text[at] == '.') {
            if (before != null) return error.SqlTypeMismatch;
            before = digits;
            continue;
        }
        if (!std.ascii.isDigit(text[at])) return error.SqlTypeMismatch;
        if (text[at] != '0') {
            if (first == null) first = digits;
            last = digits;
        }
        digits += 1;
    }
    if (digits == 0) return error.SqlTypeMismatch;
    if (first == null) return 0;
    const exponent: i64 = if (at < text.len) std.fmt.parseInt(i64, text[at + 1 ..], 10) catch return error.SqlNumericOutOfRange else 0;
    const scale = std.math.add(i64, exponent, @as(i64, @intCast(before orelse digits)) - @as(i64, @intCast(last + 1))) catch return error.SqlNumericOutOfRange;
    if (scale < 0) return error.SqlTypeMismatch;
    if (last - first.? + 1 > 19 or scale > 19 - (last - first.? + 1)) return error.SqlNumericOutOfRange;
    var position: usize = 0;
    for (text[@intFromBool(negative)..at]) |digit| {
        if (digit == '.') continue;
        if (position >= first.? and position <= last) magnitude = magnitude * 10 + digit - '0';
        position += 1;
    }
    for (0..@intCast(scale)) |_| magnitude *= 10;
    if (negative) {
        if (magnitude == @as(u64, 1) << 63) return std.math.minInt(i64);
        const signed = std.math.cast(i64, magnitude) orelse return error.SqlNumericOutOfRange;
        return -signed;
    }
    return std.math.cast(i64, magnitude) orelse error.SqlNumericOutOfRange;
}

test "SQL document projection preserves missing null exact JSON and literal names" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{
        .{ .name = "id", .path = "id", .type = .integer },
        .{ .name = "payload", .path = "payload", .type = .json },
        .{ .name = "absent", .path = "absent", .type = .json },
        .{ .name = "nested", .path = "nested", .type = .json },
        .{ .name = "a.b", .path = "a.b", .type = .integer },
        .{ .name = "scalar", .path = "scalar", .type = .string },
    } };
    const row = try decode(arena.allocator(), table, "key", 42,
        \\{"id":9007199254740993.0,"payload":null,"nested":{"n":18446744073709551615},"a.b":7,"a":{"b":9},"scalar":null,"ignored":false}
    , &.{ "_id", "id", "payload", "absent", "nested", "a.b", "scalar", "id" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try row.cell("id")).value.integer);
    try std.testing.expect(!(try row.cell("payload")).sql_null);
    try std.testing.expect((try row.cell("absent")).sql_null);
    try std.testing.expect((try row.cell("scalar")).sql_null);
    try std.testing.expectEqualStrings("18446744073709551615", (try row.cell("nested")).value.object.get("n").?.number_string);
    try std.testing.expectEqual(@as(i64, 7), (try row.cell("a.b")).value.integer);
    try std.testing.expectEqualStrings("key", (try row.cell("_id")).value.string);
    try std.testing.expectEqual(@as(usize, 6), row.value.object.count());
    try std.testing.expectError(error.UndefinedColumn, decode(arena.allocator(), table, "key", 1, "{}", &.{"unknown"}));
    try std.testing.expectError(error.InvalidSqlBackendResponse, decode(arena.allocator(), table, "key", 1, "[]", &.{}));
}

test "SQL document integer decoding is exact and bounded" {
    for ([_]struct { text: []const u8, value: i64 }{
        .{ .text = "9007199254740993.0", .value = 9007199254740993 },
        .{ .text = "-9223372036854775808", .value = std.math.minInt(i64) },
        .{ .text = "9223372036854775807", .value = std.math.maxInt(i64) },
        .{ .text = "12.3400e2", .value = 1234 },
        .{ .text = "0e99999999999999999999999", .value = 0 },
    }) |case| try std.testing.expectEqual(case.value, try exactInteger(case.text));
    try std.testing.expectError(error.SqlTypeMismatch, exactInteger("1.0000000000000001"));
    try std.testing.expectError(error.SqlNumericOutOfRange, exactInteger("9223372036854775808"));
    try std.testing.expectError(error.SqlNumericOutOfRange, exactInteger("1e99999999999999999999999"));
}

test "SQL document declared shape and projection clean up allocation failures" {
    const Fixture = struct {
        const Property = struct { name: []const u8, field_type: ?[]const u8 = null, integer_only: bool = false, allows_null: bool = false };
        const Document = struct { properties: []const Property, required_fields: []const []const u8 = &.{} };
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const alloc = arena.allocator();
            const columns = try deriveColumns(alloc, .{ .document_schemas = &[_]Document{
                .{ .properties = &.{ .{ .name = "id", .field_type = "integer" }, .{ .name = "mixed", .field_type = "integer" }, .{ .name = "nested", .field_type = "object" } }, .required_fields = &.{ "id", "mixed" } },
                .{ .properties = &.{ .{ .name = "id", .field_type = "integer" }, .{ .name = "mixed", .field_type = "string" } }, .required_fields = &.{ "id", "mixed" } },
            } });
            const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = columns };
            try std.testing.expect(!(try table.column("id")).nullable);
            try std.testing.expectEqual(ast.ColumnType.json, (try table.column("mixed")).type);
            try std.testing.expect((try table.column("mixed")).nullable);
            try std.testing.expect((try table.column("nested")).nullable);
            const row = try decode(alloc, table, "one", 5, "{\"id\":1,\"mixed\":\"text\",\"nested\":{\"exact\":9007199254740993}}", &.{ "id", "mixed", "nested" });
            try std.testing.expectEqualStrings("9007199254740993", (try row.cell("nested")).value.object.get("exact").?.number_string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
