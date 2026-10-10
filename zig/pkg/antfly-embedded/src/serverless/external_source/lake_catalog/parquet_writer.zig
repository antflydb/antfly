// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded native Iceberg primitive-row Parquet writer. PLAIN pages and
//! compact-Thrift metadata are independently readable by Arrow. Iceberg IDs
//! are preserved; unsupported nested/decimal types fail before any upload.
const std = @import("std");
const m = @import("metadata.zig");
const Context = @import("types.zig").Context;
const A = std.mem.Allocator;
const V = std.json.Value;
const List = std.ArrayList(u8);
pub const max_rows = 16384;
pub const max_bytes = 32 * 1024 * 1024;
const Column = struct { name: []const u8, id: i32, required: bool, physical: i32, converted: ?i32, adjusted_utc: ?bool = null, is_time: bool = false, offset: usize = 0, size: usize = 0, nulls: usize = 0 };
const Wire = struct {
    a: A,
    bytes: List = .empty,
    fn byte(w: *Wire, b: u8) !void {
        try w.bytes.append(w.a, b);
    }
    fn raw(w: *Wire, b: []const u8) !void {
        try w.bytes.appendSlice(w.a, b);
    }
    fn varint(w: *Wire, input: u64) !void {
        var n = input;
        while (n >= 128) : (n >>= 7) try w.byte(@as(u8, @truncate(n)) | 128);
        try w.byte(@intCast(n));
    }
    fn number(w: *Wire, n: i64) !void {
        try w.varint(@as(u64, @bitCast(n << 1)) ^ @as(u64, @bitCast(n >> 63)));
    }
    fn field(w: *Wire, prev: *u8, id: u8, kind: u8) !void {
        const delta = id - prev.*;
        if (delta <= 15) try w.byte((delta << 4) | kind) else {
            try w.byte(kind);
            try w.number(id);
        }
        prev.* = id;
    }
    fn integer(w: *Wire, prev: *u8, id: u8, kind: u8, n: i64) !void {
        try w.field(prev, id, kind);
        try w.number(n);
    }
    fn binary(w: *Wire, b: []const u8) !void {
        try w.varint(b.len);
        try w.raw(b);
    }
    fn string(w: *Wire, prev: *u8, id: u8, b: []const u8) !void {
        try w.field(prev, id, 8);
        try w.binary(b);
    }
    fn list(w: *Wire, kind: u8, len: usize) !void {
        if (len < 15) try w.byte((@as(u8, @intCast(len)) << 4) | kind) else {
            try w.byte(0xf0 | kind);
            try w.varint(len);
        }
    }
    fn little(w: *Wire, comptime T: type, n: T) !void {
        var b: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &b, n, .little);
        try w.raw(&b);
    }
};
fn column(field: V) !Column {
    const kind = try m.str(try m.get(field, "type"));
    const name = try m.str(try m.get(field, "name"));
    const id = try m.int(try m.get(field, "id"));
    if (id <= 0 or id > std.math.maxInt(i32) or name.len == 0) return error.InvalidLakeMetadata;
    const required = try m.get(field, "required");
    if (required != .bool) return error.InvalidLakeMetadata;
    var c: Column = .{ .name = name, .id = @intCast(id), .required = required.bool, .physical = 0, .converted = null };
    if (std.mem.eql(u8, kind, "boolean")) c.physical = 0 else if (std.mem.eql(u8, kind, "int") or std.mem.eql(u8, kind, "date")) {
        c.physical = 1;
        if (std.mem.eql(u8, kind, "date")) c.converted = 6;
    } else if (std.mem.eql(u8, kind, "long") or std.mem.eql(u8, kind, "time") or std.mem.eql(u8, kind, "timestamp") or std.mem.eql(u8, kind, "timestamptz")) {
        c.physical = 2;
        if (std.mem.eql(u8, kind, "time")) {
            c.converted = 8;
            c.adjusted_utc = false;
            c.is_time = true;
        } else if (!std.mem.eql(u8, kind, "long")) {
            c.converted = 10;
            c.adjusted_utc = std.mem.eql(u8, kind, "timestamptz");
        }
    } else if (std.mem.eql(u8, kind, "float")) c.physical = 4 else if (std.mem.eql(u8, kind, "double")) c.physical = 5 else if (std.mem.eql(u8, kind, "string") or std.mem.eql(u8, kind, "binary")) {
        c.physical = 6;
        if (std.mem.eql(u8, kind, "string")) c.converted = 0;
    } else return error.UnsupportedLakeWriteType;
    return c;
}
fn value(w: *Wire, c: Column, v: V, boolean_count: *usize, boolean_byte: *u8) !void {
    switch (c.physical) {
        0 => {
            if (v != .bool) return error.InvalidLakeRow;
            if (v.bool) boolean_byte.* |= @as(u8, 1) << @as(u3, @intCast(boolean_count.* % 8));
            boolean_count.* += 1;
            if (boolean_count.* % 8 == 0) {
                try w.byte(boolean_byte.*);
                boolean_byte.* = 0;
            }
        },
        1 => {
            const n = m.int(v) catch return error.InvalidLakeRow;
            if (n < std.math.minInt(i32) or n > std.math.maxInt(i32)) return error.InvalidLakeRow;
            try w.little(i32, @intCast(n));
        },
        2 => try w.little(i64, m.int(v) catch return error.InvalidLakeRow),
        4, 5 => {
            const n: f64 = switch (v) {
                .integer => @floatFromInt(v.integer),
                .float => v.float,
                else => return error.InvalidLakeRow,
            };
            if (!std.math.isFinite(n)) return error.InvalidLakeRow;
            if (c.physical == 4) {
                const f: f32 = @floatCast(n);
                if (!std.math.isFinite(f)) return error.InvalidLakeRow;
                try w.little(u32, @bitCast(f));
            } else try w.little(u64, @bitCast(n));
        },
        6 => {
            if (v != .string) return error.InvalidLakeRow;
            const b = v.string;
            if (b.len > max_bytes) return error.LakeWriteTooLarge;
            if (c.converted == 0 and !std.unicode.utf8ValidateSlice(b)) return error.InvalidLakeRow;
            try w.little(u32, @intCast(b.len));
            try w.raw(b);
        },
        else => unreachable,
    }
}
pub fn encode(a: A, schema: V, rows: []const V, context: Context) ![]u8 {
    if (rows.len == 0 or rows.len > max_rows) return error.LakeWriteTooLarge;
    const fields = try m.get(schema, "fields");
    if (fields != .array or fields.array.items.len == 0 or fields.array.items.len > 1024) return error.InvalidLakeMetadata;
    const cols = try a.alloc(Column, fields.array.items.len);
    defer a.free(cols);
    for (fields.array.items, cols, 0..) |f, *c, idx| {
        c.* = try column(f);
        for (cols[0..idx]) |prior| if (prior.id == c.id or std.mem.eql(u8, prior.name, c.name)) return error.InvalidLakeMetadata;
    }
    // Full row images: unknown fields are an error, rather than silently lost.
    for (rows) |r| {
        if (r != .object) return error.InvalidLakeRow;
        for (r.object.keys()) |key| {
            var found = false;
            for (cols) |c| if (std.mem.eql(u8, c.name, key)) {
                found = true;
                break;
            };
            if (!found) return error.InvalidLakeRow;
        }
    }
    var out: Wire = .{ .a = a };
    errdefer out.bytes.deinit(a);
    try out.raw("PAR1");
    for (cols) |*c| {
        try context.ensureActive();
        var payload: Wire = .{ .a = a };
        defer payload.bytes.deinit(a);
        if (!c.required) {
            // RLE runs of definition levels, bit width 1. DataPage V1 prefixes
            // the encoded level stream with its byte length.
            var levels: Wire = .{ .a = a };
            defer levels.bytes.deinit(a);
            var start: usize = 0;
            while (start < rows.len) {
                const present = (rows[start].object.get(c.name) orelse .null) != .null;
                var end = start + 1;
                while (end < rows.len and ((rows[end].object.get(c.name) orelse .null) != .null) == present) : (end += 1) {}
                try levels.varint(@as(u64, @intCast(end - start)) << 1);
                try levels.byte(@intFromBool(present));
                start = end;
            }
            try payload.little(u32, @intCast(levels.bytes.items.len));
            try payload.raw(levels.bytes.items);
        }
        var bool_count: usize = 0;
        var bool_byte: u8 = 0;
        for (rows) |r| {
            const v = r.object.get(c.name) orelse .null;
            if (v == .null) {
                if (c.required) return error.InvalidLakeRow;
                c.nulls += 1;
            } else try value(&payload, c.*, v, &bool_count, &bool_byte);
            if (payload.bytes.items.len > max_bytes) return error.LakeWriteTooLarge;
        }
        if (bool_count % 8 != 0) try payload.byte(bool_byte);
        c.offset = out.bytes.items.len;
        var p: u8 = 0;
        try out.integer(&p, 1, 5, 0); // DATA_PAGE
        try out.integer(&p, 2, 5, @intCast(payload.bytes.items.len));
        try out.integer(&p, 3, 5, @intCast(payload.bytes.items.len));
        try out.field(&p, 5, 12);
        var d: u8 = 0;
        try out.integer(&d, 1, 5, @intCast(rows.len));
        try out.integer(&d, 2, 5, 0);
        try out.integer(&d, 3, 5, 3);
        try out.integer(&d, 4, 5, 3);
        try out.byte(0);
        try out.byte(0);
        try out.raw(payload.bytes.items);
        c.size = out.bytes.items.len - c.offset;
        if (out.bytes.items.len > max_bytes) return error.LakeWriteTooLarge;
    }
    const footer_start = out.bytes.items.len;
    var f: u8 = 0;
    try out.integer(&f, 1, 5, 1);
    try out.field(&f, 2, 9);
    try out.list(12, cols.len + 1);
    var root: u8 = 0;
    try out.string(&root, 4, "schema");
    try out.integer(&root, 5, 5, @intCast(cols.len));
    try out.byte(0);
    for (cols) |c| {
        var s: u8 = 0;
        try out.integer(&s, 1, 5, c.physical);
        try out.integer(&s, 3, 5, if (c.required) 0 else 1);
        try out.string(&s, 4, c.name);
        if (c.converted) |conv| try out.integer(&s, 6, 5, conv);
        try out.integer(&s, 9, 5, c.id);
        if (c.adjusted_utc) |adjusted| {
            try out.field(&s, 10, 12); // LogicalType
            var logical: u8 = 0;
            try out.field(&logical, if (c.is_time) 7 else 8, 12);
            var temporal: u8 = 0;
            try out.field(&temporal, 1, if (adjusted) 1 else 2);
            try out.field(&temporal, 2, 12); // TimeUnit
            var unit: u8 = 0;
            try out.field(&unit, 2, 12); // MICROS
            try out.byte(0);
            try out.byte(0);
            try out.byte(0);
            try out.byte(0);
        }
        try out.byte(0);
    }
    try out.integer(&f, 3, 6, @intCast(rows.len));
    try out.field(&f, 4, 9);
    try out.list(12, 1);
    var rg: u8 = 0;
    try out.field(&rg, 1, 9);
    try out.list(12, cols.len);
    var total: usize = 0;
    for (cols) |c| {
        total += c.size;
        var cc: u8 = 0;
        try out.integer(&cc, 2, 6, @intCast(c.offset));
        try out.field(&cc, 3, 12);
        var cm: u8 = 0;
        try out.integer(&cm, 1, 5, c.physical);
        try out.field(&cm, 2, 9);
        try out.list(5, 2);
        try out.number(0);
        try out.number(3);
        try out.field(&cm, 3, 9);
        try out.list(8, 1);
        try out.binary(c.name);
        try out.integer(&cm, 4, 5, 0);
        try out.integer(&cm, 5, 6, @intCast(rows.len));
        try out.integer(&cm, 6, 6, @intCast(c.size));
        try out.integer(&cm, 7, 6, @intCast(c.size));
        try out.integer(&cm, 9, 6, @intCast(c.offset));
        try out.field(&cm, 12, 12);
        var stats: u8 = 0;
        try out.integer(&stats, 3, 6, @intCast(c.nulls));
        try out.byte(0);
        try out.byte(0);
        try out.byte(0);
    }
    try out.integer(&rg, 2, 6, @intCast(total));
    try out.integer(&rg, 3, 6, @intCast(rows.len));
    try out.byte(0);
    try out.string(&f, 6, "Antfly native lake writer");
    try out.byte(0);
    try out.little(u32, @intCast(out.bytes.items.len - footer_start));
    try out.raw("PAR1");
    try context.ensureActive();
    return out.bytes.toOwnedSlice(a);
}

test "native parquet writer validates nullable full images and Iceberg field IDs" {
    const a = std.testing.allocator;
    var schema = try std.json.parseFromSlice(V, a, "{\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true},{\"id\":2,\"name\":\"body\",\"type\":\"string\",\"required\":false}]}", .{});
    defer schema.deinit();
    var rows = try std.json.parseFromSlice(V, a, "[{\"id\":1,\"body\":\"hello\"},{\"id\":2,\"body\":null}]", .{});
    defer rows.deinit();
    const bytes = try encode(a, schema.value, rows.value.array.items, .{});
    defer a.free(bytes);
    try std.testing.expectEqualStrings("PAR1", bytes[0..4]);
    const footer = try @import("../../query/lake_parquet_footer.zig").parseFooterPreflight(bytes.len, 0, bytes);
    try std.testing.expect(footer.footer_metadata_len > 0);
    try std.testing.expectError(error.InvalidLakeRow, encode(a, schema.value, &.{.{ .object = .empty }}, .{}));
}
