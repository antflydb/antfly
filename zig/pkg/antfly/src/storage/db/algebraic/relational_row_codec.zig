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

//! Candidate on-disk codec for a relational document's typed columns. Public
//! relational schema mutations remain gated until this payload is wired
//! through the synchronous base-row write/read lifecycle.
//!
//! Once that lifecycle is enabled, the base row is the document's projected
//! typed columns rather than a JSON blob; `db.get` will decode the row and
//! reconstruct JSON on read.
//!
//! A document is one relational base-row pair (one packed row), not a key-range
//! of per-column pairs: every synchronous reader (point lookup,
//! read-modify-write transform, vector `include_stored`) consumes the whole
//! document, so a packed value is a single atomic lookup/write and keeps shard
//! splits boundary-agnostic. The relational base-store facade exposes row and
//! column scans over this packed representation while the physical column layout
//! evolves.
//!
//! **Self-describing on purpose.** Each cell stores the column's JSON `path`,
//! physical `value_type`, the `is_json` flag, and the typed value — everything
//! reconstruction needs. So base-row readers decode and reconstruct without a
//! schema lookup, and reconstruction works even while the table schema is
//! mid-change. The value representation is `typed_doc_values` (the same types
//! the search segments persist), and the per-value formatter (`appendCellValue`)
//! is shared with the segment read path so a document reconstructs *byte for
//! byte identically* whether served from columns in a segment or from the
//! relational row store. That single-formatter guarantee is the whole point of
//! this layer.
//!
//! Format (little-endian):
//!   magic   [4] = "AROW"
//!   version u32 = 1
//!   count   u32              -- number of cells (present columns only)
//!   per cell:
//!     path_len   u32, path bytes
//!     flags      u8          -- bit0: is_json, bit1: is_null
//!     value_type u8          -- typed_doc_values.ValueType tag
//!     payload (omitted when is_null):
//!       u64_val   : 8 bytes
//!       i64_val   : 8 bytes (two's-complement bit pattern)
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
const geo_mod = @import("../../../search/geo.zig");
const typed_dv = @import("../../../section/typed_doc_values.zig");

pub const magic: [4]u8 = "AROW".*;
pub const version: u32 = 1;

const flag_is_json: u8 = 1;
const flag_is_null: u8 = 2;
const known_flags: u8 = flag_is_json | flag_is_null;
const min_encoded_cell_len: usize = @sizeOf(u32) + 2;

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
    /// Distinguishes an explicitly stored JSON null from an absent column.
    /// `value_type` retains the declared physical type; `value` is ignored.
    is_null: bool = false,
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
    const encoded_len = try serializedLenWithAllocator(alloc, cells);
    const buf = try alloc.alloc(u8, encoded_len);
    errdefer alloc.free(buf);
    _ = serializeIntoUnchecked(buf, cells);
    return buf;
}

/// Return the exact number of bytes required by `serializeInto`.
pub fn serializedLen(cells: []const Cell) !usize {
    var stack = std.heap.stackFallback(2048, std.heap.page_allocator);
    return serializedLenWithAllocator(stack.get(), cells);
}

fn serializedLenWithAllocator(alloc: Allocator, cells: []const Cell) !usize {
    try validateCells(alloc, cells);
    const count = std.math.cast(u32, cells.len) orelse return error.InvalidRelationalRow;
    _ = count;
    var encoded_len: usize = magic.len + 2 * @sizeOf(u32);
    for (cells) |cell| {
        _ = std.math.cast(u32, cell.path.len) orelse return error.InvalidRelationalRow;
        encoded_len = try serializedLenAdd(encoded_len, @sizeOf(u32) + cell.path.len + 2);
        if (cell.is_null) continue;
        encoded_len = try serializedLenAdd(encoded_len, switch (cell.value) {
            .u64_val, .i64_val, .f64_val => @sizeOf(u64),
            .bool_val => 1,
            .geo_point => 2 * @sizeOf(u64),
            .bytes_val => |bytes| blk: {
                _ = std.math.cast(u32, bytes.len) orelse return error.InvalidRelationalRow;
                break :blk @sizeOf(u32) + bytes.len;
            },
        });
    }
    return encoded_len;
}

/// Serialize into caller-owned storage and return the initialized prefix.
pub fn serializeInto(buf: []u8, cells: []const Cell) ![]u8 {
    const encoded_len = try serializedLen(cells);
    if (buf.len < encoded_len) return error.NoSpaceLeft;
    return serializeIntoUnchecked(buf[0..encoded_len], cells);
}

fn serializeIntoUnchecked(buf: []u8, cells: []const Cell) []u8 {
    const encoded_len = buf.len;
    const out = buf[0..encoded_len];
    var pos: usize = 0;
    @memcpy(out[pos..][0..magic.len], &magic);
    pos += magic.len;
    writeU32(out, &pos, version);
    writeU32(out, &pos, @intCast(cells.len));
    for (cells) |cell| {
        writeU32(out, &pos, @intCast(cell.path.len));
        @memcpy(out[pos..][0..cell.path.len], cell.path);
        pos += cell.path.len;
        out[pos] = (if (cell.is_json) flag_is_json else 0) |
            (if (cell.is_null) flag_is_null else 0);
        out[pos + 1] = @intFromEnum(cell.value_type);
        pos += 2;
        if (cell.is_null) continue;
        switch (cell.value) {
            .u64_val => |value| writeU64(out, &pos, value),
            .i64_val => |value| writeU64(out, &pos, @bitCast(value)),
            .f64_val => |value| writeU64(out, &pos, @bitCast(value)),
            .bool_val => |value| {
                out[pos] = if (value) 1 else 0;
                pos += 1;
            },
            .geo_point => |value| {
                writeU64(out, &pos, @bitCast(value.lat));
                writeU64(out, &pos, @bitCast(value.lon));
            },
            .bytes_val => |bytes| {
                writeU32(out, &pos, @intCast(bytes.len));
                @memcpy(out[pos..][0..bytes.len], bytes);
                pos += bytes.len;
            },
        }
    }
    std.debug.assert(pos == out.len);
    return out;
}

fn serializedLenAdd(current: usize, additional: usize) !usize {
    return std.math.add(usize, current, additional) catch error.InvalidRelationalRow;
}

fn cellValueMatchesType(cell: Cell) bool {
    if (cell.is_json and cell.value_type != .bytes_val) return false;
    if (cell.is_null) return true;
    const matches = switch (cell.value_type) {
        .u64_val => cell.value == .u64_val,
        .i64_val => cell.value == .i64_val,
        .f64_val => cell.value == .f64_val,
        .bytes_val => cell.value == .bytes_val,
        .geo_point => cell.value == .geo_point,
        .bool_val => cell.value == .bool_val,
    };
    return matches;
}

fn cellValueIsSerializable(cell: Cell) bool {
    if (cell.is_null) return true;
    return switch (cell.value) {
        .f64_val => |value| std.math.isFinite(value),
        .geo_point => |value| geo_mod.latitudeIsValid(value.lat) and geo_mod.longitudeIsValid(value.lon),
        else => true,
    };
}

fn validateCell(alloc: Allocator, cell: Cell) !void {
    if (!std.unicode.utf8ValidateSlice(cell.path) or
        !cellValueMatchesType(cell) or
        !cellValueIsSerializable(cell)) return error.InvalidRelationalRow;
    if (!cell.is_null and cell.value == .bytes_val) {
        if (cell.is_json) {
            if (!(try std.json.validate(alloc, cell.value.bytes_val))) return error.InvalidRelationalRow;
        } else if (!std.unicode.utf8ValidateSlice(cell.value.bytes_val)) {
            return error.InvalidRelationalRow;
        }
    }
}

fn validateCells(alloc: Allocator, cells: []const Cell) !void {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.ensureTotalCapacity(alloc, std.math.cast(u32, cells.len) orelse return error.InvalidRelationalRow);
    for (cells) |cell| {
        try validateCell(alloc, cell);
        const entry = try seen.getOrPut(alloc, cell.path);
        if (entry.found_existing) return error.InvalidRelationalRow;
    }
}

fn writeU32(buf: []u8, pos: *usize, value: u32) void {
    std.mem.writeInt(u32, buf[pos.*..][0..@sizeOf(u32)], value, .little);
    pos.* += @sizeOf(u32);
}

fn writeU64(buf: []u8, pos: *usize, value: u64) void {
    std.mem.writeInt(u64, buf[pos.*..][0..@sizeOf(u64)], value, .little);
    pos.* += @sizeOf(u64);
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
    var cursor = try RowCursor.init(alloc, data);
    defer cursor.deinit();
    const cells = try alloc.alloc(Cell, cursor.count);
    errdefer alloc.free(cells);

    for (cells) |*cell| cell.* = (try cursor.next()) orelse return error.InvalidRelationalRow;
    try cursor.finish();

    return .{ .cells = cells };
}

/// Validate a serialized row without allocating a decoded cell array. This is
/// used by paths that only need a few cells but must still reject malformed
/// authoritative packed rows before trusting derived index payloads.
pub fn validate(data: []const u8) !void {
    var stack = std.heap.stackFallback(2048, std.heap.page_allocator);
    var cursor = try RowCursor.init(stack.get(), data);
    defer cursor.deinit();
    while (try cursor.next()) |_| {}
    try cursor.finish();
}

fn validateCellCount(remaining: usize, count: u32) !void {
    if (count > remaining / min_encoded_cell_len) return error.InvalidRelationalRow;
}

pub const CellLookup = struct {
    path: []const u8,
    alternate_path: []const u8 = "",
};

/// Collect several cells from one serialized row scan. This validates the row
/// while scanning, but only copies matching cell descriptors into `out`; the
/// returned cells still borrow `value`.
pub fn collectCellsByLookup(value: []const u8, lookups: []const CellLookup, out: []?Cell) !void {
    if (lookups.len != out.len) return error.InvalidArgument;
    for (out) |*item| item.* = null;
    var stack = std.heap.stackFallback(2048, std.heap.page_allocator);
    var cursor = try RowCursor.init(stack.get(), value);
    defer cursor.deinit();
    while (try cursor.next()) |cell| {
        for (lookups, 0..) |lookup, lookup_index| {
            if (out[lookup_index] != null) continue;
            if (std.mem.eql(u8, cell.path, lookup.path) or
                (lookup.alternate_path.len != 0 and std.mem.eql(u8, cell.path, lookup.alternate_path)))
            {
                out[lookup_index] = cell;
            }
        }
    }
    try cursor.finish();
}

const RowCursor = struct {
    alloc: Allocator,
    data: []const u8,
    pos: usize,
    count: u32,
    read_count: u32 = 0,
    seen: std.StringHashMapUnmanaged(void) = .empty,

    fn init(alloc: Allocator, data: []const u8) !RowCursor {
        if (data.len < magic.len + 8) return error.InvalidRelationalRow;
        if (!std.mem.eql(u8, data[0..magic.len], &magic)) return error.InvalidRelationalRow;
        var pos: usize = magic.len;
        const ver = readU32(data, &pos);
        if (ver != version) return error.UnsupportedRelationalRowVersion;
        const count = readU32(data, &pos);
        try validateCellCount(data.len - pos, count);
        var cursor = RowCursor{ .alloc = alloc, .data = data, .pos = pos, .count = count };
        errdefer cursor.seen.deinit(alloc);
        try cursor.seen.ensureTotalCapacity(alloc, count);
        return cursor;
    }

    fn deinit(self: *RowCursor) void {
        self.seen.deinit(self.alloc);
    }

    fn next(self: *RowCursor) !?Cell {
        if (self.read_count == self.count) return null;
        const cell = try readCellAt(self.data, &self.pos);
        try validateCell(self.alloc, cell);
        const entry = try self.seen.getOrPut(self.alloc, cell.path);
        if (entry.found_existing) return error.InvalidRelationalRow;
        self.read_count += 1;
        return cell;
    }

    fn finish(self: RowCursor) !void {
        if (self.read_count != self.count or self.pos != self.data.len) return error.InvalidRelationalRow;
    }
};

/// Decode the cell at `pos.*`, advancing `pos`. Shared by full deserialization
/// and the single-column accessor. The `path`/`bytes_val` slices borrow `data`.
fn readCellAt(data: []const u8, pos: *usize) !Cell {
    const path = try readStr(data, pos);
    if (pos.* + 2 > data.len) return error.InvalidRelationalRow;
    const flags = data[pos.*];
    const value_type = valueTypeFromByte(data[pos.* + 1]) orelse return error.InvalidRelationalRow;
    pos.* += 2;
    if (flags & ~known_flags != 0) return error.InvalidRelationalRow;
    if (flags & flag_is_json != 0 and value_type != .bytes_val) return error.InvalidRelationalRow;

    const is_null = flags & flag_is_null != 0;
    const value: typed_dv.TypedValue = if (is_null) zeroValue(value_type) else switch (value_type) {
        .u64_val => .{ .u64_val = try readU64Checked(data, pos) },
        .i64_val => .{ .i64_val = @bitCast(try readU64Checked(data, pos)) },
        .f64_val => .{ .f64_val = @bitCast(try readU64Checked(data, pos)) },
        .bool_val => blk: {
            if (pos.* + 1 > data.len) return error.InvalidRelationalRow;
            if (data[pos.*] > 1) return error.InvalidRelationalRow;
            const b = data[pos.*] == 1;
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

    const cell: Cell = .{
        .path = path,
        .value_type = value_type,
        .is_json = (flags & flag_is_json) != 0,
        .is_null = is_null,
        .value = value,
    };
    if (!std.unicode.utf8ValidateSlice(cell.path) or !cellValueIsSerializable(cell)) {
        return error.InvalidRelationalRow;
    }
    return cell;
}

fn zeroValue(value_type: typed_dv.ValueType) typed_dv.TypedValue {
    return switch (value_type) {
        .u64_val => .{ .u64_val = 0 },
        .i64_val => .{ .i64_val = 0 },
        .f64_val => .{ .f64_val = 0 },
        .bytes_val => .{ .bytes_val = "" },
        .geo_point => .{ .geo_point = .{ .lat = 0, .lon = 0 } },
        .bool_val => .{ .bool_val = false },
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
    var stack = std.heap.stackFallback(2048, std.heap.page_allocator);
    var cursor = try RowCursor.init(stack.get(), value);
    defer cursor.deinit();
    var found: ?Cell = null;
    while (try cursor.next()) |cell| {
        if (found == null and std.mem.eql(u8, cell.path, path)) found = cell;
    }
    try cursor.finish();
    return found;
}

/// Reconstruct a document's canonical JSON directly from a serialized typed-row
/// value. Schema-free. Caller owns the returned bytes.
pub fn reconstructValueAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    var cursor = try RowCursor.init(alloc, value);
    defer cursor.deinit();
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    var needs_comma = false;
    while (try cursor.next()) |cell| {
        try appendValidatedCellValue(alloc, &out, cell, needs_comma);
        needs_comma = true;
    }
    try cursor.finish();
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

/// Materialize a document-mode stored value as JSON by returning an owned copy.
/// Relational rows must go through `reconstructValueAlloc` at a relational row
/// keyspace read seam; this generic document path intentionally has no AROW
/// fallback because relational mode is a new format with no legacy primary-row
/// compatibility contract.
pub fn materializeDocumentValueAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    return try alloc.dupe(u8, value);
}

/// As `materializeDocumentValueAlloc`, but takes ownership of `value`.
pub fn materializeOwnedDocumentValueAlloc(alloc: Allocator, value: []u8) ![]u8 {
    _ = alloc;
    return value;
}

/// Reconstruct a document's canonical JSON from decoded cells. Schema-free.
/// Caller owns the returned bytes.
pub fn reconstructDocumentAlloc(alloc: Allocator, cells: []const Cell) ![]u8 {
    try validateCells(alloc, cells);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    for (cells, 0..) |c, i| {
        try appendValidatedCellValue(alloc, &out, c, i > 0);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

/// Append one `"path": value` pair to `out`. This is the single canonical
/// per-value formatter shared by the relational base-row read path and the
/// segment read path, so a column reconstructs identically regardless of where
/// its value came from.
pub fn appendCellValue(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    path: []const u8,
    value_type: typed_dv.ValueType,
    is_json: bool,
    is_null: bool,
    value: typed_dv.TypedValue,
    needs_comma: bool,
) !void {
    const cell: Cell = .{
        .path = path,
        .value_type = value_type,
        .is_json = is_json,
        .is_null = is_null,
        .value = value,
    };
    try validateCell(alloc, cell);
    return try appendValidatedCellValue(alloc, out, cell, needs_comma);
}

fn appendValidatedCellValue(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    cell: Cell,
    needs_comma: bool,
) !void {
    if (needs_comma) try out.append(alloc, ',');
    try appendJsonString(alloc, out, cell.path);
    try out.append(alloc, ':');
    if (cell.is_null) {
        try out.appendSlice(alloc, "null");
        return;
    }
    switch (cell.value_type) {
        .f64_val => try appendFmt(alloc, out, "{d}", .{cell.value.f64_val}),
        .u64_val => try appendFmt(alloc, out, "{d}", .{cell.value.u64_val}),
        .i64_val => try appendFmt(alloc, out, "{d}", .{cell.value.i64_val}),
        .bool_val => try out.appendSlice(alloc, if (cell.value.bool_val) "true" else "false"),
        .geo_point => try appendFmt(alloc, out, "{{\"lat\":{d},\"lon\":{d}}}", .{ cell.value.geo_point.lat, cell.value.geo_point.lon }),
        .bytes_val => {
            if (cell.is_json) {
                // RowCursor/appendCellValue validates this exactly once before
                // reaching the hot formatting path.
                try out.appendSlice(alloc, cell.value.bytes_val);
            } else {
                try appendJsonString(alloc, out, cell.value.bytes_val);
            }
        },
    }
}

pub fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidRelationalRow;
    try out.append(alloc, '"');
    for (value) |byte| {
        switch (byte) {
            '"' => try out.appendSlice(alloc, "\\\""),
            '\\' => try out.appendSlice(alloc, "\\\\"),
            0x08 => try out.appendSlice(alloc, "\\b"),
            0x0c => try out.appendSlice(alloc, "\\f"),
            '\n' => try out.appendSlice(alloc, "\\n"),
            '\r' => try out.appendSlice(alloc, "\\r"),
            '\t' => try out.appendSlice(alloc, "\\t"),
            0x00...0x07, 0x0b, 0x0e...0x1f => {
                try out.appendSlice(alloc, "\\u00");
                try out.append(alloc, hexDigit(byte >> 4));
                try out.append(alloc, hexDigit(byte & 0x0f));
            },
            else => try out.append(alloc, byte),
        }
    }
    try out.append(alloc, '"');
}

fn appendFmt(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), comptime fmt: []const u8, args: anytype) !void {
    var stack: [256]u8 = undefined;
    const text = std.fmt.bufPrint(&stack, fmt, args) catch |err| switch (err) {
        error.NoSpaceLeft => {
            const heap_text = try std.fmt.allocPrint(alloc, fmt, args);
            defer alloc.free(heap_text);
            try out.appendSlice(alloc, heap_text);
            return;
        },
    };
    try out.appendSlice(alloc, text);
}

fn hexDigit(value: u8) u8 {
    return if (value < 10) '0' + value else 'a' + (value - 10);
}

fn valueTypeFromByte(tag: u8) ?typed_dv.ValueType {
    if (tag >= std.meta.fields(typed_dv.ValueType).len) return null;
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

test "relational row codec preserves explicit null cells" {
    const alloc = std.testing.allocator;
    const encoded = try serialize(alloc, &.{
        .{ .path = "name", .value_type = .bytes_val, .is_null = true, .value = .{ .bytes_val = "ignored" } },
        .{ .path = "payload", .value_type = .bytes_val, .is_json = true, .is_null = true, .value = .{ .bytes_val = "ignored" } },
        .{ .path = "active", .value_type = .bool_val, .is_null = true, .value = .{ .bool_val = true } },
    });
    defer alloc.free(encoded);

    var row = try deserialize(alloc, encoded);
    defer row.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), row.cells.len);
    for (row.cells) |cell| try std.testing.expect(cell.is_null);

    const reconstructed = try reconstructValueAlloc(alloc, encoded);
    defer alloc.free(reconstructed);
    try std.testing.expectEqualStrings("{\"name\":null,\"payload\":null,\"active\":null}", reconstructed);

    var segment_json = std.ArrayListUnmanaged(u8).empty;
    defer segment_json.deinit(alloc);
    try segment_json.append(alloc, '{');
    try appendCellValue(alloc, &segment_json, "name", .bytes_val, false, true, .{ .bytes_val = "ignored" }, false);
    try appendCellValue(alloc, &segment_json, "active", .bool_val, false, true, .{ .bool_val = true }, true);
    try segment_json.append(alloc, '}');
    try std.testing.expectEqualStrings("{\"name\":null,\"active\":null}", segment_json.items);
}

test "relational row codec escapes string paths and values" {
    const alloc = std.testing.allocator;
    const cells = [_]Cell{
        .{ .path = "na\"me", .value_type = .bytes_val, .value = .{ .bytes_val = "a\"b" } },
        .{ .path = "slash\\path", .value_type = .bytes_val, .value = .{ .bytes_val = "line\n tab\t nul\x00" } },
    };
    const encoded = try serialize(alloc, &cells);
    defer alloc.free(encoded);
    var row = try deserialize(alloc, encoded);
    defer row.deinit(alloc);
    const json = try reconstructDocumentAlloc(alloc, row.cells);
    defer alloc.free(json);
    try std.testing.expectEqualStrings("{\"na\\\"me\":\"a\\\"b\",\"slash\\\\path\":\"line\\n tab\\t nul\\u0000\"}", json);
}

test "relational row codec rejects bad magic, version, and truncation" {
    const alloc = std.testing.allocator;
    try std.testing.expect(!looksLikeRow("XXXX"));
    try std.testing.expect(!looksLikeRow("AR"));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, "XXXXxxxxxxxx"));
    try std.testing.expectError(error.InvalidRelationalRow, validate("XXXXxxxxxxxx"));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, "AROW"));
    try std.testing.expectError(error.InvalidRelationalRow, validate("AROW"));

    // Valid header claiming one cell but no cell bytes -> truncation error.
    var buf: [12]u8 = undefined;
    @memcpy(buf[0..4], &magic);
    std.mem.writeInt(u32, buf[4..8], version, .little);
    std.mem.writeInt(u32, buf[8..12], 1, .little);
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, &buf));
    try std.testing.expectError(error.InvalidRelationalRow, validate(&buf));

    // Unsupported version.
    var verbuf: [12]u8 = undefined;
    @memcpy(verbuf[0..4], &magic);
    std.mem.writeInt(u32, verbuf[4..8], version + 1, .little);
    std.mem.writeInt(u32, verbuf[8..12], 0, .little);
    try std.testing.expectError(error.UnsupportedRelationalRowVersion, deserialize(alloc, &verbuf));
    try std.testing.expectError(error.UnsupportedRelationalRowVersion, validate(&verbuf));

    // Reject an attacker-controlled count before trying to allocate it.
    std.mem.writeInt(u32, buf[8..12], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, &buf));
    try std.testing.expectError(error.InvalidRelationalRow, validate(&buf));
}

test "relational row codec rejects non-canonical and trailing encodings" {
    const alloc = std.testing.allocator;
    const cells = [_]Cell{
        .{ .path = "", .value_type = .bool_val, .value = .{ .bool_val = true } },
    };
    const encoded = try serialize(alloc, &cells);
    defer alloc.free(encoded);

    var malformed: [20]u8 = undefined;
    @memcpy(malformed[0..encoded.len], encoded);

    malformed[16] = 0x80; // unknown flag bit
    try std.testing.expectError(error.InvalidRelationalRow, validate(malformed[0..encoded.len]));

    malformed[16] = 0;
    malformed[18] = 2; // booleans are canonically 0 or 1
    try std.testing.expectError(error.InvalidRelationalRow, validate(malformed[0..encoded.len]));

    @memcpy(malformed[0..encoded.len], encoded);
    malformed[encoded.len] = 0;
    try std.testing.expectError(error.InvalidRelationalRow, validate(malformed[0 .. encoded.len + 1]));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, malformed[0 .. encoded.len + 1]));
    try std.testing.expectError(error.InvalidRelationalRow, findCellByPath(malformed[0 .. encoded.len + 1], ""));
}

test "relational row codec rejects mismatched and non-JSON-safe cells" {
    const alloc = std.testing.allocator;
    const mismatched = [_]Cell{
        .{ .path = "value", .value_type = .u64_val, .value = .{ .bool_val = true } },
    };
    try std.testing.expectError(error.InvalidRelationalRow, serializedLen(&mismatched));

    const non_finite = [_]Cell{
        .{ .path = "value", .value_type = .f64_val, .value = .{ .f64_val = std.math.nan(f64) } },
    };
    try std.testing.expectError(error.InvalidRelationalRow, serialize(alloc, &non_finite));

    const invalid_json = [_]Cell{
        .{ .path = "payload", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = "{" } },
    };
    try std.testing.expectError(error.InvalidRelationalRow, serialize(alloc, &invalid_json));
    try std.testing.expectError(error.InvalidRelationalRow, reconstructDocumentAlloc(alloc, &invalid_json));

    const invalid_utf8 = [_]Cell{
        .{ .path = "value", .value_type = .bytes_val, .value = .{ .bytes_val = "\xff" } },
    };
    try std.testing.expectError(error.InvalidRelationalRow, serialize(alloc, &invalid_utf8));
    try std.testing.expectError(error.InvalidRelationalRow, reconstructDocumentAlloc(alloc, &invalid_utf8));

    const invalid_geopoint = [_]Cell{
        .{ .path = "location", .value_type = .geo_point, .value = .{ .geo_point = .{ .lat = 91, .lon = 0 } } },
    };
    try std.testing.expectError(error.InvalidRelationalRow, serialize(alloc, &invalid_geopoint));

    const invalid_path = [_]Cell{
        .{ .path = "\xff", .value_type = .bool_val, .value = .{ .bool_val = true } },
    };
    try std.testing.expectError(error.InvalidRelationalRow, serialize(alloc, &invalid_path));

    const duplicate_paths = [_]Cell{
        .{ .path = "value", .value_type = .bool_val, .value = .{ .bool_val = true } },
        .{ .path = "value", .value_type = .bool_val, .value = .{ .bool_val = false } },
    };
    try std.testing.expectError(error.InvalidRelationalRow, serialize(alloc, &duplicate_paths));
    try std.testing.expectError(error.InvalidRelationalRow, reconstructDocumentAlloc(alloc, &duplicate_paths));

    const finite = [_]Cell{
        .{ .path = "", .value_type = .f64_val, .value = .{ .f64_val = 1 } },
    };
    const corrupted = try serialize(alloc, &finite);
    defer alloc.free(corrupted);
    std.mem.writeInt(u64, corrupted[18..26], @bitCast(std.math.nan(f64)), .little);
    try std.testing.expectError(error.InvalidRelationalRow, validate(corrupted));
}

test "relational row readers reject corrupt JSON and duplicate paths" {
    const alloc = std.testing.allocator;
    const text_row = try serialize(alloc, &.{
        .{ .path = "title", .value_type = .bytes_val, .value = .{ .bytes_val = "a" } },
    });
    defer alloc.free(text_row);
    text_row[text_row.len - 1] = 0xff;
    try std.testing.expectError(error.InvalidRelationalRow, validate(text_row));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, text_row));
    try std.testing.expectError(error.InvalidRelationalRow, findCellByPath(text_row, "title"));

    const json_row = try serialize(alloc, &.{
        .{ .path = "payload", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = "{}" } },
    });
    defer alloc.free(json_row);
    const json_offset = std.mem.indexOf(u8, json_row, "{}") orelse return error.TestUnexpectedResult;
    json_row[json_offset + 1] = 'x';
    try std.testing.expectError(error.InvalidRelationalRow, validate(json_row));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, json_row));
    try std.testing.expectError(error.InvalidRelationalRow, findCellByPath(json_row, "payload"));

    const duplicate_row = try serialize(alloc, &.{
        .{ .path = "a", .value_type = .bool_val, .value = .{ .bool_val = true } },
        .{ .path = "b", .value_type = .bool_val, .value = .{ .bool_val = false } },
    });
    defer alloc.free(duplicate_row);
    // Header (12) + first bool cell (8) + second path length (4).
    duplicate_row[24] = 'a';
    try std.testing.expectError(error.InvalidRelationalRow, validate(duplicate_row));
    try std.testing.expectError(error.InvalidRelationalRow, deserialize(alloc, duplicate_row));
    try std.testing.expectError(error.InvalidRelationalRow, findCellByPath(duplicate_row, "a"));
    var found: [1]?Cell = undefined;
    try std.testing.expectError(
        error.InvalidRelationalRow,
        collectCellsByLookup(duplicate_row, &.{.{ .path = "a" }}, &found),
    );
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
    try validate(encoded);

    const id = (try findCellByPath(encoded, "id")).?;
    try std.testing.expectEqual(typed_dv.ValueType.bytes_val, id.value_type);
    try std.testing.expectEqualStrings("abc", id.value.bytes_val);

    const amount = (try findCellByPath(encoded, "amount")).?;
    try std.testing.expectEqual(@as(f64, 12.5), amount.value.f64_val);

    try std.testing.expect((try findCellByPath(encoded, "missing")) == null);
    // Non-row value yields null, not an error.
    try std.testing.expect((try findCellByPath("{\"id\":\"x\"}", "id")) == null);
}

test "collectCellsByLookup validates and reads several columns in one scan" {
    const alloc = std.testing.allocator;
    const cells = [_]Cell{
        .{ .path = "id", .value_type = .bytes_val, .value = .{ .bytes_val = "abc" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 12.5 } },
        .{ .path = "payload.note", .value_type = .bytes_val, .value = .{ .bytes_val = "memo" } },
    };
    const encoded = try serialize(alloc, &cells);
    defer alloc.free(encoded);

    const lookups = [_]CellLookup{
        .{ .path = "id" },
        .{ .path = "note", .alternate_path = "payload.note" },
        .{ .path = "missing" },
    };
    var found: [lookups.len]?Cell = undefined;
    try collectCellsByLookup(encoded, lookups[0..], found[0..]);
    try std.testing.expectEqualStrings("abc", found[0].?.value.bytes_val);
    try std.testing.expectEqualStrings("memo", found[1].?.value.bytes_val);
    try std.testing.expect(found[2] == null);

    var bad: [1]?Cell = undefined;
    try std.testing.expectError(error.InvalidRelationalRow, collectCellsByLookup("not-a-row", lookups[0..1], bad[0..]));
    try std.testing.expectError(error.InvalidArgument, collectCellsByLookup(encoded, lookups[0..2], bad[0..]));
}
