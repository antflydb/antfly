//! Relational column manifest, persisted inside a segment.
//!
//! A relational table's documents are stored only as typed columns
//! (`typed_doc_values`), never as a stored-doc JSON blob. To reconstruct a
//! document's JSON from those columns a reader needs the column catalog —
//! each column's segment field name, its JSON path, the physical value type
//! it was persisted as, and whether its bytes are raw JSON (`is_json`) vs a
//! plain string. That catalog is this manifest.
//!
//! The manifest is written into the segment by the build path (which has the
//! runtime schema), carried forward verbatim through merge, and read back at
//! the single `SegmentReader.storedDocDecompressed` chokepoint to drive
//! reconstruction — so every body read (query, merge, shard split) is served
//! from columns without any of those call sites needing schema access.
//!
//! This module deliberately depends only on `std` and `typed_doc_values` so it
//! can be imported by both `segment.zig` (the reader) and `document_mapper.zig`
//! (the writer/Phase-5 seam) without an import cycle. It does NOT reference the
//! runtime schema (`AntflyType`): the physical `value_type` plus the `is_json`
//! flag carry everything reconstruction needs.
//!
//! Relational mode is new, so there is no legacy on-disk format to stay
//! compatible with; a single manifest version is assumed.

const std = @import("std");
const typed_dv = @import("typed_doc_values.zig");

const Allocator = std.mem.Allocator;

/// Reserved segment field name the manifest section is attached to. Mirrors the
/// `doc_ordinals_field` convention (leading NUL keeps it out of the user field
/// namespace).
pub const manifest_field = "\x00__antfly_relational_manifest";

const manifest_magic: [4]u8 = "ARMF".*; // AntFly Relational ManiFest
const manifest_version: u32 = 1;

/// One reconstructable column. Owns nothing; `name`/`path` borrow either the
/// segment bytes (when read) or the caller's buffers (when written).
pub const ManifestColumn = struct {
    /// Segment field name the column's `typed_doc_values` section lives under.
    name: []const u8,
    /// Dotted JSON path the value is emitted under during reconstruction.
    path: []const u8,
    /// Physical type the column was persisted as.
    value_type: typed_dv.ValueType,
    /// When true the stored `bytes_val` is already valid JSON (embedded
    /// verbatim); otherwise bytes are a plain string (JSON-escaped on read).
    is_json: bool,
};

/// Serialize a column catalog into a manifest section. Caller owns the result.
pub fn serialize(alloc: Allocator, columns: []const ManifestColumn) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, &manifest_magic);
    try appendU32(alloc, &buf, manifest_version);
    try appendU32(alloc, &buf, @intCast(columns.len));
    for (columns) |column| {
        try appendStr(alloc, &buf, column.name);
        try appendStr(alloc, &buf, column.path);
        try buf.append(alloc, @intFromEnum(column.value_type));
        try buf.append(alloc, if (column.is_json) 1 else 0);
    }
    return try buf.toOwnedSlice(alloc);
}

/// A parsed manifest. `columns` and the strings they reference borrow `data`
/// (the segment bytes), so the manifest is valid only while `data` is.
pub const Manifest = struct {
    columns: []ManifestColumn,

    pub fn deinit(self: *Manifest, alloc: Allocator) void {
        alloc.free(self.columns);
    }
};

/// Parse a manifest section. The returned column slice is heap-allocated (free
/// via `Manifest.deinit`); the `name`/`path` slices borrow `data`.
pub fn parse(alloc: Allocator, data: []const u8) !Manifest {
    if (data.len < 12) return error.InvalidData;
    if (!std.mem.eql(u8, data[0..4], &manifest_magic)) return error.InvalidData;
    var pos: usize = 4;
    const ver = readU32(data, &pos);
    if (ver != manifest_version) return error.UnsupportedVersion;
    const count = readU32(data, &pos);

    var columns = try alloc.alloc(ManifestColumn, count);
    errdefer alloc.free(columns);

    for (0..count) |i| {
        const name = try readStr(data, &pos);
        const path = try readStr(data, &pos);
        if (pos + 2 > data.len) return error.InvalidData;
        const value_type: typed_dv.ValueType = @enumFromInt(data[pos]);
        const is_json = data[pos + 1] != 0;
        pos += 2;
        columns[i] = .{
            .name = name,
            .path = path,
            .value_type = value_type,
            .is_json = is_json,
        };
    }
    return .{ .columns = columns };
}

/// Reconstruct a relational document's JSON from a segment's typed columns,
/// driven by the manifest. `reader` is anything exposing
/// `getSection(field_name, .typed_doc_values) ?[]const u8` (a `SegmentReader`);
/// `local_doc_id` is the segment-local doc id the columns are keyed by. Columns
/// with no section / no value at that id (absent nullable values) are omitted.
/// Caller owns the returned bytes.
pub fn reconstructDocumentAlloc(
    alloc: Allocator,
    reader: anytype,
    columns: []const ManifestColumn,
    local_doc_id: u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    var emitted: usize = 0;
    for (columns) |column| {
        const section = reader.getSection(column.name, .typed_doc_values) orelse continue;
        const dv = typed_dv.TypedDocValuesReader.init(alloc, section) catch continue;
        if (try appendColumn(alloc, &out, column, &dv, local_doc_id, emitted > 0)) {
            emitted += 1;
        }
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendColumn(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    column: ManifestColumn,
    dv: *const typed_dv.TypedDocValuesReader,
    local_doc_id: u32,
    needs_comma: bool,
) !bool {
    switch (column.value_type) {
        .f64_val => {
            const v = (try dv.getF64(local_doc_id)) orelse return false;
            try appendKey(alloc, out, column.path, needs_comma);
            try appendFmt(alloc, out, "{d}", .{v});
        },
        .u64_val => {
            const v = (try dv.getU64(local_doc_id)) orelse return false;
            try appendKey(alloc, out, column.path, needs_comma);
            // datetime columns persist epoch-ns as a bit-cast i64.
            try appendFmt(alloc, out, "{d}", .{@as(i64, @bitCast(v))});
        },
        .bool_val => {
            const v = (try dv.getBool(local_doc_id)) orelse return false;
            try appendKey(alloc, out, column.path, needs_comma);
            try out.appendSlice(alloc, if (v) "true" else "false");
        },
        .geo_point => {
            const gp = (try dv.getGeoPoint(local_doc_id)) orelse return false;
            try appendKey(alloc, out, column.path, needs_comma);
            try appendFmt(alloc, out, "{{\"lat\":{d},\"lon\":{d}}}", .{ gp.lat, gp.lon });
        },
        .bytes_val => {
            const bytes = (try dv.getBytes(local_doc_id)) orelse return false;
            defer alloc.free(bytes);
            try appendKey(alloc, out, column.path, needs_comma);
            if (column.is_json) {
                try out.appendSlice(alloc, bytes); // already canonical JSON
            } else {
                try appendJsonString(alloc, out, bytes);
            }
        },
    }
    return true;
}

fn appendKey(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), path: []const u8, needs_comma: bool) !void {
    if (needs_comma) try out.append(alloc, ',');
    try appendJsonString(alloc, out, path);
    try out.append(alloc, ':');
}

fn appendFmt(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), comptime fmt: []const u8, args: anytype) !void {
    const text = try std.fmt.allocPrint(alloc, fmt, args);
    defer alloc.free(text);
    try out.appendSlice(alloc, text);
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn appendU32(alloc: Allocator, buf: *std.ArrayListUnmanaged(u8), val: u32) !void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, val, .little);
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

fn readStr(data: []const u8, pos: *usize) ![]const u8 {
    if (pos.* + 4 > data.len) return error.InvalidData;
    const len = readU32(data, pos);
    if (pos.* + len > data.len) return error.InvalidData;
    const s = data[pos.*..][0..len];
    pos.* += len;
    return s;
}

test "relational manifest round-trips through serialize/parse" {
    const alloc = std.testing.allocator;
    const columns = [_]ManifestColumn{
        .{ .name = "title", .path = "title", .value_type = .bytes_val, .is_json = false },
        .{ .name = "amount", .path = "amount", .value_type = .f64_val, .is_json = false },
        .{ .name = "meta", .path = "meta", .value_type = .bytes_val, .is_json = true },
        .{ .name = "ts", .path = "created.at", .value_type = .u64_val, .is_json = false },
    };
    const bytes = try serialize(alloc, &columns);
    defer alloc.free(bytes);

    var manifest = try parse(alloc, bytes);
    defer manifest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 4), manifest.columns.len);
    try std.testing.expectEqualStrings("title", manifest.columns[0].name);
    try std.testing.expectEqual(typed_dv.ValueType.bytes_val, manifest.columns[0].value_type);
    try std.testing.expect(!manifest.columns[0].is_json);
    try std.testing.expectEqual(typed_dv.ValueType.f64_val, manifest.columns[1].value_type);
    try std.testing.expect(manifest.columns[2].is_json);
    try std.testing.expectEqualStrings("created.at", manifest.columns[3].path);
    try std.testing.expectEqual(typed_dv.ValueType.u64_val, manifest.columns[3].value_type);
}

test "parse rejects bad magic and truncated data" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidData, parse(alloc, "XXXX____"));
    try std.testing.expectError(error.InvalidData, parse(alloc, "AR"));
}
