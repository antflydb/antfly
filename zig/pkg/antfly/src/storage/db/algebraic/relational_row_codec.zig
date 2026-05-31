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

//! On-disk codec for a relational document's typed columns — the authoritative
//! value stored in the synchronous key-value store for a relational table.
//!
//! In relational mode the KV source of truth is *not* a JSON blob: it is the
//! document's projected typed columns, serialized by this codec. `db.get`
//! decodes the row and reconstructs canonical JSON on read.
//!
//! A document is one KV pair (one packed row), not a key-range of per-column
//! pairs: every synchronous reader (point lookup, read-modify-write transform,
//! vector `include_stored`) consumes the whole document, so a packed value is a
//! single atomic lookup/write and keeps shard splits boundary-agnostic. The
//! columnar predicate-pushdown tier lives in the search segments, not here.
//!
//! **Self-describing on purpose.** Each cell stores the column's JSON `path`,
//! physical `value_type`, the `is_json` flag, and the typed value — everything
//! reconstruction needs. So the KV read chokepoint decodes and reconstructs
//! without a schema lookup, and reconstruction works even while the table schema
//! is mid-change. The value representation is `typed_doc_values` (the same types
//! the search segments persist), and the per-value formatter (`appendCellValue`)
//! is shared with the segment read path so a document reconstructs *byte for
//! byte identically* whether served from columns in a segment or from the KV
//! store. That single-formatter guarantee is the whole point of this layer.
//!
//! Format (little-endian):
//!   magic   [4] = "AROW"
//!   version u32 = 1
//!   count   u32              -- number of cells (present columns only)
//!   per cell:
//!     path_len   u32, path bytes
//!     flags      u8          -- bit0: is_json
//!     value_type u8          -- typed_doc_values.ValueType tag
//!     payload:
//!       u64_val   : 8 bytes
//!       f64_val   : 8 bytes (bitcast)
//!       bool_val  : 1 byte
//!       geo_point : 16 bytes (lat f64, lon f64)
//!       bytes_val : u32 len + len bytes
//!
//! Cells are stored in declared-column order, and only present columns are
//! stored (absent nullable columns are skipped) — matching the segment path,
//! which emits columns in order and skips absent ones, so the reconstructed JSON
//! is identical.
//!
//! Relational mode is new, so there is no legacy on-disk format to stay
//! compatible with; a single row version is assumed.

const std = @import("std");
const Allocator = std.mem.Allocator;
const typed_dv = @import("../../../section/typed_doc_values.zig");

pub const magic: [4]u8 = "AROW".*;
pub const version: u32 = 1;

const flag_is_json: u8 = 1;

/// One reconstructable column value. Owns nothing: `path` and (for `bytes_val`)
/// the value bytes borrow either the caller's buffers (when serializing) or the
/// decoded `Row` storage (when reading).
pub const Cell = struct {
    /// Dotted JSON path the value is emitted under during reconstruction.
    path: []const u8,
    value_type: typed_dv.ValueType,
    /// When true a `bytes_val` payload is already valid JSON (embedded
    /// verbatim); otherwise it is a plain string (JSON-escaped on read).
    is_json: bool = false,
    value: typed_dv.TypedValue,
};

/// True if `value` begins with the typed-row magic. Lets the KV read chokepoint
/// tell a serialized relational row apart from any other stored value without a
/// schema lookup.
pub fn looksLikeRow(value: []const u8) bool {
    return value.len >= magic.len and std.mem.eql(u8, value[0..magic.len], &magic);
}

/// Serialize a document's cells. Caller owns the result.
pub fn serialize(alloc: Allocator, cells: []const Cell) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, &magic);
    try appendU32(alloc, &buf, version);
    try appendU32(alloc, &buf, @intCast(cells.len));

    for (cells) |c| {
        try appendStr(alloc, &buf, c.path);
        try buf.append(alloc, if (c.is_json) flag_is_json else 0);
        try buf.append(alloc, @intFromEnum(c.value_type));
        switch (c.value) {
            .u64_val => |v| try appendU64(alloc, &buf, v),
            .f64_val => |v| try appendU64(alloc, &buf, @bitCast(v)),
            .bool_val => |v| try buf.append(alloc, if (v) 1 else 0),
            .geo_point => |gp| {
                try appendU64(alloc, &buf, @bitCast(gp.lat));
                try appendU64(alloc, &buf, @bitCast(gp.lon));
            },
            .bytes_val => |bytes| {
                try appendU32(alloc, &buf, @intCast(bytes.len));
                try buf.appendSlice(alloc, bytes);
            },
        }
    }

    return try buf.toOwnedSlice(alloc);
}

/// A decoded row. `cells` and the `path`/`bytes_val` slices they reference
/// borrow `data` (the stored value), so the row is valid only while `data` is.
pub const Row = struct {
    cells: []Cell,

    pub fn deinit(self: *Row, alloc: Allocator) void {
        alloc.free(self.cells);
    }
};

/// Decode a row. The returned cell slice is heap-allocated (free via
/// `Row.deinit`); `path` and `bytes_val` borrow `data`.
pub fn deserialize(alloc: Allocator, data: []const u8) !Row {
    if (data.len < magic.len + 8) return error.InvalidRelationalRow;
    if (!std.mem.eql(u8, data[0..magic.len], &magic)) return error.InvalidRelationalRow;
    var pos: usize = magic.len;
    const ver = readU32(data, &pos);
    if (ver != version) return error.UnsupportedRelationalRowVersion;
    const count = readU32(data, &pos);

    const cells = try alloc.alloc(Cell, count);
    errdefer alloc.free(cells);

    var i: usize = 0;
    while (i < count) : (i += 1) {
        cells[i] = try readCellAt(data, &pos);
    }

    return .{ .cells = cells };
}

/// Decode the cell at `pos.*`, advancing `pos`. Shared by full deserialization
/// and the single-column accessor. The `path`/`bytes_val` slices borrow `data`.
fn readCellAt(data: []const u8, pos: *usize) !Cell {
    const path = try readStr(data, pos);
    if (pos.* + 2 > data.len) return error.InvalidRelationalRow;
    const flags = data[pos.*];
    const value_type = valueTypeFromByte(data[pos.* + 1]) orelse return error.InvalidRelationalRow;
    pos.* += 2;

    const value: typed_dv.TypedValue = switch (value_type) {
        .u64_val => .{ .u64_val = try readU64Checked(data, pos) },
        .f64_val => .{ .f64_val = @bitCast(try readU64Checked(data, pos)) },
        .bool_val => blk: {
            if (pos.* + 1 > data.len) return error.InvalidRelationalRow;
            const b = data[pos.*] != 0;
            pos.* += 1;
            break :blk .{ .bool_val = b };
        },
        .geo_point => blk: {
            const lat: f64 = @bitCast(try readU64Checked(data, pos));
            const lon: f64 = @bitCast(try readU64Checked(data, pos));
            break :blk .{ .geo_point = .{ .lat = lat, .lon = lon } };
        },
        .bytes_val => blk: {
            if (pos.* + 4 > data.len) return error.InvalidRelationalRow;
            const len = readU32(data, pos);
            if (pos.* + len > data.len) return error.InvalidRelationalRow;
            const bytes = data[pos.* .. pos.* + len];
            pos.* += len;
            break :blk .{ .bytes_val = bytes };
        },
    };

    return .{
        .path = path,
        .value_type = value_type,
        .is_json = (flags & flag_is_json) != 0,
        .value = value,
    };
}

/// Look up a single column by its JSON path directly from a serialized row,
/// without allocating the full cell array. Returns the decoded cell (its
/// `path`/`bytes_val` borrow `value`) or null if the row has no such column.
/// This is the Seam B accessor: a field-scoped consumer (e.g. an enrichment
/// `source_field`) reads one column instead of reconstructing the whole
/// document. Returns null for a non-row value.
pub fn findCellByPath(value: []const u8, path: []const u8) !?Cell {
    if (!looksLikeRow(value)) return null;
    if (value.len < magic.len + 8) return error.InvalidRelationalRow;
    var pos: usize = magic.len;
    const ver = readU32(value, &pos);
    if (ver != version) return error.UnsupportedRelationalRowVersion;
    const count = readU32(value, &pos);

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const cell = try readCellAt(value, &pos);
        if (std.mem.eql(u8, cell.path, path)) return cell;
    }
    return null;
}

/// Reconstruct a document's canonical JSON directly from a serialized typed-row
/// value. Schema-free. Caller owns the returned bytes.
pub fn reconstructValueAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    var row = try deserialize(alloc, value);
    defer row.deinit(alloc);
    return try reconstructDocumentAlloc(alloc, row.cells);
}

/// Materialize a stored document value as JSON: a typed row is reconstructed to
/// canonical JSON; anything else (a JSON blob) is returned as an owned copy.
/// This is the single seam every document-value reader routes a raw store value
/// through, so none of them need to know the storage format. Detection is
/// schema-free via the row magic and never collides with a real JSON document
/// (which starts with '{'). Caller owns the returned bytes.
pub fn materializeDocumentValueAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    if (looksLikeRow(value)) return try reconstructValueAlloc(alloc, value);
    return try alloc.dupe(u8, value);
}

/// As `materializeDocumentValueAlloc`, but takes ownership of `value`: a typed
/// row is reconstructed and `value` is freed; a JSON blob is returned as-is
/// without an extra copy. Convenient at read sites that already own the bytes.
pub fn materializeOwnedDocumentValueAlloc(alloc: Allocator, value: []u8) ![]u8 {
    if (looksLikeRow(value)) {
        defer alloc.free(value);
        return try reconstructValueAlloc(alloc, value);
    }
    return value;
}

/// Reconstruct a document's canonical JSON from decoded cells. Schema-free.
/// Caller owns the returned bytes.
pub fn reconstructDocumentAlloc(alloc: Allocator, cells: []const Cell) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    for (cells, 0..) |c, i| {
        try appendCellValue(alloc, &out, c.path, c.value_type, c.is_json, c.value, i > 0);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

/// Append one `"path": value` pair to `out`. This is the single canonical
/// per-value formatter shared by the KV read path and the segment read path, so
/// a column reconstructs identically regardless of where its value came from.
pub fn appendCellValue(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    path: []const u8,
    value_type: typed_dv.ValueType,
    is_json: bool,
    value: typed_dv.TypedValue,
    needs_comma: bool,
) !void {
    if (needs_comma) try out.append(alloc, ',');
    try appendJsonString(alloc, out, path);
    try out.append(alloc, ':');
    switch (value_type) {
        .f64_val => try appendFmt(alloc, out, "{d}", .{value.f64_val}),
        .u64_val => try appendFmt(alloc, out, "{d}", .{value.u64_val}),
        .bool_val => try out.appendSlice(alloc, if (value.bool_val) "true" else "false"),
        .geo_point => try appendFmt(alloc, out, "{{\"lat\":{d},\"lon\":{d}}}", .{ value.geo_point.lat, value.geo_point.lon }),
        .bytes_val => {
            if (is_json) {
                try out.appendSlice(alloc, value.bytes_val); // already canonical JSON
            } else {
                try appendJsonString(alloc, out, value.bytes_val);
            }
        },
    }
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn appendFmt(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), comptime fmt: []const u8, args: anytype) !void {
    const text = try std.fmt.allocPrint(alloc, fmt, args);
    defer alloc.free(text);
    try out.appendSlice(alloc, text);
}

fn valueTypeFromByte(tag: u8) ?typed_dv.ValueType {
    if (tag >= std.meta.fields(typed_dv.ValueType).len) return null;
    return @enumFromInt(tag);
}

fn appendU32(alloc: Allocator, buf: *std.ArrayListUnmanaged(u8), val: u32) !void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, val, .little);
    try buf.appendSlice(alloc, &tmp);
}

fn appendU64(alloc: Allocator, buf: *std.ArrayListUnmanaged(u8), val: u64) !void {
    var tmp: [8]u8 = undefined;
    std.mem.writeInt(u64, &tmp, val, .little);
    try buf.appendSlice(alloc, &tmp);
}

fn appendStr(alloc: Allocator, buf: *std.ArrayListUnmanaged(u8), s: []const u8) !void {
    try appendU32(alloc, buf, @intCast(s.len));
    try buf.appendSlice(alloc, s);
}

fn readU32(data: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return val;
}

fn readU64Checked(data: []const u8, pos: *usize) !u64 {
    if (pos.* + 8 > data.len) return error.InvalidRelationalRow;
    const val = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

fn readStr(data: []const u8, pos: *usize) ![]const u8 {
    if (pos.* + 4 > data.len) return error.InvalidRelationalRow;
    const len = readU32(data, pos);
    if (pos.* + len > data.len) return error.InvalidRelationalRow;
    const s = data[pos.*..][0..len];
    pos.* += len;
    return s;
}

test "relational row codec round-trips every value type and reconstructs canonical JSON" {
    const alloc = std.testing.allocator;
    const cells = [_]Cell{
        .{ .path = "id", .value_type = .bytes_val, .value = .{ .bytes_val = "abc" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 12.5 } },
        .{ .path = "ts", .value_type = .u64_val, .value = .{ .u64_val = 1000 } },
        .{ .path = "active", .value_type = .bool_val, .value = .{ .bool_val = true } },
        .{ .path = "loc", .value_type = .geo_point, .value = .{ .geo_point = .{ .lat = 1.5, .lon = -2.5 } } },
        .{ .path = "payload", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = "{\"k\":1}" } },
    };

    const encoded = try serialize(alloc, &cells);
    defer alloc.free(encoded);
    try std.testing.expect(looksLikeRow(encoded));

    var row = try deserialize(alloc, encoded);
    defer row.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 6), row.cells.len);
    try std.testing.expectEqualStrings("id", row.cells[0].path);
    try std.testing.expectEqualStrings("abc", row.cells[0].value.bytes_val);
    try std.testing.expectEqual(@as(f64, 12.5), row.cells[1].value.f64_val);
    try std.testing.expectEqual(@as(u64, 1000), row.cells[2].value.u64_val);
    try std.testing.expect(row.cells[3].value.bool_val);
    try std.testing.expectEqual(@as(f64, -2.5), row.cells[4].value.geo_point.lon);
    try std.testing.expect(row.cells[5].is_json);

    const json = try reconstructDocumentAlloc(alloc, row.cells);
    defer alloc.free(json);
    try std.testing.expectEqualStrings(
        "{\"id\":\"abc\",\"amount\":12.5,\"ts\":1000,\"active\":true,\"loc\":{\"lat\":1.5,\"lon\":-2.5},\"payload\":{\"k\":1}}",
        json,
    );
}

test "relational row codec reconstructs an empty row" {
    const alloc = std.testing.allocator;
    const encoded = try serialize(alloc, &.{});
    defer alloc.free(encoded);
    var row = try deserialize(alloc, encoded);
    defer row.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), row.cells.len);
    const json = try reconstructDocumentAlloc(alloc, row.cells);
    defer alloc.free(json);
    try std.testing.expectEqualStrings("{}", json);
}

test "relational row codec escapes string paths and values" {
    const alloc = std.testing.allocator;
    const cells = [_]Cell{
        .{ .path = "na\"me", .value_type = .bytes_val, .value = .{ .bytes_val = "a\"b" } },
    };
    const encoded = try serialize(alloc, &cells);
    defer alloc.free(encoded);
    var row = try deserialize(alloc, encoded);
    defer row.deinit(alloc);
    const json = try reconstructDocumentAlloc(alloc, row.cells);
    defer alloc.free(json);
    try std.testing.expectEqualStrings("{\"na\\\"me\":\"a\\\"b\"}", json);
}

test "relational row codec rejects bad magic, version, and truncation" {
    const alloc = std.testing.allocator;
    try std.testing.expect(!looksLikeRow("XXXX"));
    try std.testing.expect(!looksLikeRow("AR"));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, "XXXXxxxxxxxx"));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, "AROW"));

    // Valid header claiming one cell but no cell bytes -> truncation error.
    var buf: [12]u8 = undefined;
    @memcpy(buf[0..4], &magic);
    std.mem.writeInt(u32, buf[4..8], version, .little);
    std.mem.writeInt(u32, buf[8..12], 1, .little);
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, &buf));

    // Unsupported version.
    var verbuf: [12]u8 = undefined;
    @memcpy(verbuf[0..4], &magic);
    std.mem.writeInt(u32, verbuf[4..8], version + 1, .little);
    std.mem.writeInt(u32, verbuf[8..12], 0, .little);
    try std.testing.expectError(error.UnsupportedRelationalRowVersion, deserialize(alloc, &verbuf));
}

test "findCellByPath reads a single column without full deserialization" {
    const alloc = std.testing.allocator;
    const cells = [_]Cell{
        .{ .path = "id", .value_type = .bytes_val, .value = .{ .bytes_val = "abc" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 12.5 } },
        .{ .path = "active", .value_type = .bool_val, .value = .{ .bool_val = true } },
    };
    const encoded = try serialize(alloc, &cells);
    defer alloc.free(encoded);

    const id = (try findCellByPath(encoded, "id")).?;
    try std.testing.expectEqual(typed_dv.ValueType.bytes_val, id.value_type);
    try std.testing.expectEqualStrings("abc", id.value.bytes_val);

    const amount = (try findCellByPath(encoded, "amount")).?;
    try std.testing.expectEqual(@as(f64, 12.5), amount.value.f64_val);

    try std.testing.expect((try findCellByPath(encoded, "missing")) == null);
    // Non-row value yields null, not an error.
    try std.testing.expect((try findCellByPath("{\"id\":\"x\"}", "id")) == null);
}
