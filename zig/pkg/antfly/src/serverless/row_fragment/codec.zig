// Copyright 2026 Antfly, Inc.
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
const Allocator = std.mem.Allocator;
const row_fragment = @import("types.zig");

const magic = "AFRF";
const version: u32 = 1;

const cell_null: u8 = 0;
const cell_present: u8 = 1;

pub fn encodeAlloc(alloc: Allocator, fragment: row_fragment.Fragment) ![]u8 {
    try fragment.validate();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, magic);
    try appendU32(alloc, &out, version);
    try appendBytes(alloc, &out, fragment.schema_fingerprint);
    try appendU32(alloc, &out, @intCast(fragment.row_refs.len));
    try appendU32(alloc, &out, @intCast(fragment.columns.len));

    for (fragment.row_refs) |row_ref| {
        try appendU64(alloc, &out, row_ref.ordinal);
        try appendBytes(alloc, &out, row_ref.key);
    }

    for (fragment.columns) |column| {
        try appendBytes(alloc, &out, column.name);
        try out.append(alloc, @intFromEnum(column.kind));
        try appendU32(alloc, &out, @intCast(column.values.len));
        for (column.values) |value| {
            try encodeCell(alloc, &out, column.kind, value);
        }
    }

    return try out.toOwnedSlice(alloc);
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !row_fragment.Fragment {
    var cursor: usize = 0;
    if (bytes.len < magic.len + 4) return error.InvalidRowFragment;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidRowFragmentMagic;
    cursor += magic.len;
    const got_version = try readU32(bytes, &cursor);
    if (got_version != version) return error.UnsupportedRowFragmentVersion;

    const schema_fingerprint = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(schema_fingerprint);
    const row_count = try readU32(bytes, &cursor);
    const column_count = try readU32(bytes, &cursor);

    const row_refs = try alloc.alloc(row_fragment.RowRef, row_count);
    errdefer alloc.free(row_refs);
    var initialized_row_refs: usize = 0;
    errdefer {
        for (row_refs[0..initialized_row_refs]) |*row_ref| row_ref.deinit(alloc);
    }

    for (row_refs) |*row_ref| {
        row_ref.* = .{
            .ordinal = try readU64(bytes, &cursor),
            .key = try readBytesAlloc(alloc, bytes, &cursor),
        };
        initialized_row_refs += 1;
    }

    const columns = try alloc.alloc(row_fragment.Column, column_count);
    errdefer alloc.free(columns);
    var initialized_columns: usize = 0;
    errdefer {
        for (columns[0..initialized_columns]) |*column| column.deinit(alloc);
    }

    for (columns) |*column| {
        const name = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer alloc.free(name);
        if (cursor >= bytes.len) return error.InvalidRowFragment;
        const kind = try decodeColumnKind(bytes[cursor]);
        cursor += 1;
        const value_count = try readU32(bytes, &cursor);
        if (value_count != row_count) return error.InvalidRowFragment;
        const values = try alloc.alloc(row_fragment.CellValue, value_count);
        errdefer alloc.free(values);
        var initialized_values: usize = 0;
        errdefer {
            for (values[0..initialized_values]) |*value| value.deinit(alloc);
        }
        for (values) |*value| {
            value.* = try decodeCellAlloc(alloc, bytes, &cursor, kind);
            initialized_values += 1;
        }
        column.* = .{
            .name = name,
            .kind = kind,
            .values = values,
        };
        initialized_columns += 1;
    }

    if (cursor != bytes.len) return error.InvalidRowFragment;

    var fragment = row_fragment.Fragment{
        .schema_fingerprint = schema_fingerprint,
        .row_refs = row_refs,
        .columns = columns,
    };
    errdefer fragment.deinit(alloc);
    try fragment.validate();
    return fragment;
}

fn encodeCell(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kind: row_fragment.ColumnKind,
    value: row_fragment.CellValue,
) !void {
    switch (value) {
        .null => try out.append(alloc, cell_null),
        .bytes => |bytes| {
            if (kind != .bytes) return error.InvalidRowFragment;
            try out.append(alloc, cell_present);
            try appendBytes(alloc, out, bytes);
        },
        .json => |bytes| {
            if (kind != .json) return error.InvalidRowFragment;
            try out.append(alloc, cell_present);
            try appendBytes(alloc, out, bytes);
        },
        .i64 => |int| {
            if (kind != .i64) return error.InvalidRowFragment;
            try out.append(alloc, cell_present);
            try appendI64(alloc, out, int);
        },
        .f64 => |float| {
            if (kind != .f64) return error.InvalidRowFragment;
            try out.append(alloc, cell_present);
            try appendU64(alloc, out, @bitCast(float));
        },
        .bool => |boolean| {
            if (kind != .bool) return error.InvalidRowFragment;
            try out.append(alloc, cell_present);
            try out.append(alloc, @intFromBool(boolean));
        },
        .vector_f32 => |vector| {
            if (kind != .vector_f32) return error.InvalidRowFragment;
            try out.append(alloc, cell_present);
            try appendU32(alloc, out, @intCast(vector.len));
            for (vector) |item| try appendU32(alloc, out, @bitCast(item));
        },
    }
}

fn decodeCellAlloc(
    alloc: Allocator,
    bytes: []const u8,
    cursor: *usize,
    kind: row_fragment.ColumnKind,
) !row_fragment.CellValue {
    if (cursor.* >= bytes.len) return error.InvalidRowFragment;
    const tag = bytes[cursor.*];
    cursor.* += 1;
    if (tag == cell_null) return .null;
    if (tag != cell_present) return error.InvalidRowFragment;
    return switch (kind) {
        .bytes => .{ .bytes = try readBytesAlloc(alloc, bytes, cursor) },
        .json => .{ .json = try readBytesAlloc(alloc, bytes, cursor) },
        .i64 => .{ .i64 = try readI64(bytes, cursor) },
        .f64 => .{ .f64 = @bitCast(try readU64(bytes, cursor)) },
        .bool => blk: {
            if (cursor.* >= bytes.len) return error.InvalidRowFragment;
            const raw = bytes[cursor.*];
            cursor.* += 1;
            break :blk switch (raw) {
                0 => .{ .bool = false },
                1 => .{ .bool = true },
                else => error.InvalidRowFragment,
            };
        },
        .vector_f32 => blk: {
            const len = try readU32(bytes, cursor);
            const vector = try alloc.alloc(f32, len);
            errdefer alloc.free(vector);
            for (vector) |*value| value.* = @bitCast(try readU32(bytes, cursor));
            break :blk .{ .vector_f32 = vector };
        },
    };
}

fn decodeColumnKind(raw: u8) !row_fragment.ColumnKind {
    return switch (raw) {
        1 => .bytes,
        2 => .json,
        3 => .i64,
        4 => .f64,
        5 => .bool,
        6 => .vector_f32,
        else => error.InvalidRowFragment,
    };
}

fn appendBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendU32(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn readBytesAlloc(alloc: Allocator, bytes: []const u8, cursor: *usize) ![]u8 {
    const len = try readU32(bytes, cursor);
    if (cursor.* + len > bytes.len) return error.InvalidRowFragment;
    const out = try alloc.dupe(u8, bytes[cursor.* .. cursor.* + len]);
    cursor.* += len;
    return out;
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendI64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    try appendU64(alloc, out, @bitCast(value));
}

fn readU32(bytes: []const u8, cursor: *usize) !u32 {
    if (cursor.* + 4 > bytes.len) return error.InvalidRowFragment;
    const value = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    return value;
}

fn readU64(bytes: []const u8, cursor: *usize) !u64 {
    if (cursor.* + 8 > bytes.len) return error.InvalidRowFragment;
    const value = std.mem.readInt(u64, bytes[cursor.*..][0..8], .little);
    cursor.* += 8;
    return value;
}

fn readI64(bytes: []const u8, cursor: *usize) !i64 {
    return @bitCast(try readU64(bytes, cursor));
}

test "row fragment codec round-trips typed columns" {
    const alloc = std.testing.allocator;
    var fragment = row_fragment.Fragment{
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .row_refs = try alloc.alloc(row_fragment.RowRef, 2),
        .columns = try alloc.alloc(row_fragment.Column, 4),
    };
    defer fragment.deinit(alloc);

    fragment.row_refs[0] = .{ .key = try alloc.dupe(u8, "row:a"), .ordinal = 0 };
    fragment.row_refs[1] = .{ .key = try alloc.dupe(u8, "row:b"), .ordinal = 1 };

    fragment.columns[0] = .{
        .name = try alloc.dupe(u8, "tenant"),
        .kind = .bytes,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[0].values[0] = .{ .bytes = try alloc.dupe(u8, "t1") };
    fragment.columns[0].values[1] = .{ .bytes = try alloc.dupe(u8, "t2") };

    fragment.columns[1] = .{
        .name = try alloc.dupe(u8, "amount"),
        .kind = .i64,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[1].values[0] = .{ .i64 = 10 };
    fragment.columns[1].values[1] = .{ .i64 = 20 };

    fragment.columns[2] = .{
        .name = try alloc.dupe(u8, "attrs"),
        .kind = .json,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[2].values[0] = .{ .json = try alloc.dupe(u8, "{\"plan\":\"pro\"}") };
    fragment.columns[2].values[1] = .null;

    fragment.columns[3] = .{
        .name = try alloc.dupe(u8, "embedding"),
        .kind = .vector_f32,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[3].values[0] = .{ .vector_f32 = try alloc.dupe(f32, &.{ 1.0, 0.0 }) };
    fragment.columns[3].values[1] = .{ .vector_f32 = try alloc.dupe(f32, &.{ 0.5, 0.5 }) };

    const encoded = try encodeAlloc(alloc, fragment);
    defer alloc.free(encoded);

    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqualStrings("schema-v1", decoded.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), decoded.rowCount());
    try std.testing.expectEqualStrings("row:b", decoded.row_refs[1].key);
    try std.testing.expectEqualStrings("amount", decoded.columns[1].name);
    try std.testing.expectEqual(@as(i64, 20), decoded.columns[1].values[1].i64);
    try std.testing.expect(decoded.columns[2].values[1] == .null);
    try std.testing.expectEqual(@as(f32, 0.5), decoded.columns[3].values[1].vector_f32[0]);
    try std.testing.expectEqual(@as(f32, 0.5), decoded.columns[3].values[1].vector_f32[1]);
}
