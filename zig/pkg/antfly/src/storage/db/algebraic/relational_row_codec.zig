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

//! On-disk codec for a relational document's typed columns.
//!
//! In relational mode the authoritative source of truth in the synchronous
//! key-value store is *not* a JSON blob — it is the projected typed columns of
//! the document (`schema_capability.RelationalRow`), serialized by this codec.
//! `db.get` decodes the row and reconstructs canonical JSON on read via
//! `schema_capability.reconstructRelationalDocumentAlloc`.
//!
//! A document is one KV pair (one packed row), not a key-range of per-column
//! pairs: every synchronous reader (point lookup, read-modify-write transform,
//! vector `include_stored`) consumes the whole document, so a packed value is a
//! single atomic lookup/write and keeps shard splits boundary-agnostic. The
//! columnar/predicate-pushdown tier lives in the search segments, not here.
//!
//! Format (little-endian):
//!   magic   [4] = "AROW"
//!   version u32 = 1
//!   count   u32              -- number of *present* cells
//!   per cell:
//!     column_index u32
//!     flags        u8        -- bit0: is_json
//!     value_type   u8        -- PhysicalType tag
//!     payload:
//!       u64_val   : 8 bytes
//!       f64_val   : 8 bytes (bitcast)
//!       bool_val  : 1 byte
//!       geo_point : 16 bytes (lat f64, lon f64)
//!       bytes_val : u32 len + len bytes
//!
//! Absent nullable columns produce no cell (matching projection semantics).
//! The codec round-trips the row schema-free; JSON reconstruction is a separate
//! step that consults the column plan for paths and scalar formatting.
//!
//! Relational mode is new, so there is no legacy on-disk format to stay
//! compatible with; a single row version is assumed. The 4-byte magic also lets
//! the KV read chokepoint distinguish a typed row from any other value.

const std = @import("std");
const Allocator = std.mem.Allocator;
const schema_capability = @import("schema_capability.zig");

const RelationalRow = schema_capability.RelationalRow;
const RelationalCell = schema_capability.RelationalCell;
const ColumnValue = schema_capability.ColumnValue;
const PhysicalType = schema_capability.PhysicalType;

pub const magic: [4]u8 = "AROW".*;
pub const version: u32 = 1;

const flag_is_json: u8 = 1;

/// True if `value` begins with the typed-row magic. Lets the KV read chokepoint
/// tell a serialized relational row apart from any other stored value without a
/// schema lookup.
pub fn looksLikeRow(value: []const u8) bool {
    return value.len >= magic.len and std.mem.eql(u8, value[0..magic.len], &magic);
}

/// Serialize the present cells of a row. Caller owns the result.
pub fn serialize(alloc: Allocator, row: RelationalRow) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, &magic);
    try appendU32(alloc, &buf, version);

    var present_count: u32 = 0;
    for (row.cells) |c| {
        if (c.present) present_count += 1;
    }
    try appendU32(alloc, &buf, present_count);

    for (row.cells) |c| {
        if (!c.present) continue;
        try appendU32(alloc, &buf, @intCast(c.column));
        try buf.append(alloc, if (c.is_json) flag_is_json else 0);
        try buf.append(alloc, @intFromEnum(@as(PhysicalType, c.value)));
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

/// Deserialize a row. The returned row owns its `bytes_val` payloads (in
/// `bytes_pool`); free via `RelationalRow.deinit`. The cells are emitted in
/// serialized order (which is column order, as projection produces them).
pub fn deserialize(alloc: Allocator, data: []const u8) !RelationalRow {
    if (data.len < magic.len + 8) return error.InvalidRelationalRow;
    if (!std.mem.eql(u8, data[0..magic.len], &magic)) return error.InvalidRelationalRow;
    var pos: usize = magic.len;
    const ver = readU32(data, &pos);
    if (ver != version) return error.UnsupportedRelationalRowVersion;
    const count = readU32(data, &pos);

    const cells = try alloc.alloc(RelationalCell, count);
    errdefer alloc.free(cells);
    var pool = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (pool.items) |buffer| alloc.free(buffer);
        pool.deinit(alloc);
    }

    var i: usize = 0;
    while (i < count) : (i += 1) {
        if (pos + 6 > data.len) return error.InvalidRelationalRow;
        const column_index = readU32(data, &pos);
        const flags = data[pos];
        pos += 1;
        const value_type = physicalTypeFromByte(data[pos]) orelse return error.InvalidRelationalRow;
        pos += 1;

        const value: ColumnValue = switch (value_type) {
            .u64_val => .{ .u64_val = try readU64Checked(data, &pos) },
            .f64_val => .{ .f64_val = @bitCast(try readU64Checked(data, &pos)) },
            .bool_val => blk: {
                if (pos + 1 > data.len) return error.InvalidRelationalRow;
                const b = data[pos] != 0;
                pos += 1;
                break :blk .{ .bool_val = b };
            },
            .geo_point => blk: {
                const lat: f64 = @bitCast(try readU64Checked(data, &pos));
                const lon: f64 = @bitCast(try readU64Checked(data, &pos));
                break :blk .{ .geo_point = .{ .lat = lat, .lon = lon } };
            },
            .bytes_val => blk: {
                if (pos + 4 > data.len) return error.InvalidRelationalRow;
                const len = readU32(data, &pos);
                if (pos + len > data.len) return error.InvalidRelationalRow;
                const owned = try alloc.dupe(u8, data[pos .. pos + len]);
                errdefer alloc.free(owned);
                try pool.append(alloc, owned);
                pos += len;
                break :blk .{ .bytes_val = owned };
            },
        };

        cells[i] = .{
            .column = column_index,
            .present = true,
            .is_json = (flags & flag_is_json) != 0,
            .value = value,
        };
    }

    return .{ .cells = cells, .bytes_pool = try pool.toOwnedSlice(alloc) };
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

fn physicalTypeFromByte(tag: u8) ?PhysicalType {
    if (tag >= std.meta.fields(PhysicalType).len) return null;
    return @enumFromInt(tag);
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

test "relational row codec round-trips every physical type" {
    const alloc = std.testing.allocator;
    var pool = [_][]u8{
        try alloc.dupe(u8, "hello world"),
        try alloc.dupe(u8, "{\"k\":1}"),
    };
    var row = RelationalRow{
        .cells = try alloc.dupe(RelationalCell, &.{
            .{ .column = 0, .present = true, .value = .{ .bytes_val = pool[0] } },
            .{ .column = 1, .present = true, .value = .{ .f64_val = 12.5 } },
            .{ .column = 2, .present = true, .value = .{ .u64_val = 1000 } },
            .{ .column = 3, .present = true, .value = .{ .bool_val = true } },
            .{ .column = 4, .present = true, .value = .{ .geo_point = .{ .lat = 1.5, .lon = -2.5 } } },
            .{ .column = 5, .present = true, .is_json = true, .value = .{ .bytes_val = pool[1] } },
        }),
        .bytes_pool = try alloc.dupe([]u8, &pool),
    };
    defer row.deinit(alloc);

    const encoded = try serialize(alloc, row);
    defer alloc.free(encoded);
    try std.testing.expect(looksLikeRow(encoded));

    var decoded = try deserialize(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 6), decoded.cells.len);
    try std.testing.expectEqualStrings("hello world", decoded.cell(0).?.value.bytes_val);
    try std.testing.expectEqual(@as(f64, 12.5), decoded.cell(1).?.value.f64_val);
    try std.testing.expectEqual(@as(u64, 1000), decoded.cell(2).?.value.u64_val);
    try std.testing.expect(decoded.cell(3).?.value.bool_val);
    try std.testing.expectEqual(@as(f64, 1.5), decoded.cell(4).?.value.geo_point.lat);
    try std.testing.expectEqual(@as(f64, -2.5), decoded.cell(4).?.value.geo_point.lon);
    try std.testing.expect(decoded.cell(5).?.is_json);
    try std.testing.expectEqualStrings("{\"k\":1}", decoded.cell(5).?.value.bytes_val);
}

test "relational row codec omits absent cells" {
    const alloc = std.testing.allocator;
    const row = RelationalRow{
        .cells = try alloc.dupe(RelationalCell, &.{
            .{ .column = 0, .present = true, .value = .{ .u64_val = 7 } },
            .{ .column = 1, .present = false, .value = .{ .bool_val = false } },
            .{ .column = 2, .present = true, .value = .{ .bool_val = true } },
        }),
        .bytes_pool = &.{},
    };
    defer alloc.free(row.cells);

    const encoded = try serialize(alloc, row);
    defer alloc.free(encoded);

    var decoded = try deserialize(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), decoded.cells.len);
    try std.testing.expect(decoded.cell(0) != null);
    try std.testing.expect(decoded.cell(1) == null);
    try std.testing.expect(decoded.cell(2) != null);
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
