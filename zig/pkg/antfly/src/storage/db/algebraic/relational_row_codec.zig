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
//! Version 2 binds a row to an immutable schema epoch and stores values by stable column ordinal,
//! so paths and types are not repeated in every row. Each row deterministically
//! chooses the smaller of a dense fixed/offset body and a sparse ordinal/payload
//! directory; both retain targeted projection without penalizing wide optional
//! schemas.
//!
//! AROW v2 is the first supported relational row format. Rows always carry a
//! schema version, semantic content hash, a canonical dense or sparse ordinal
//! body, and a physical checksum. Dense rows carry presence/null bitmaps;
//! sparse rows encode presence in their directory and the null bit alongside
//! each ordinal so physical size remains proportional to row density. There is
//! no schemaless or pre-v2 compatibility path.

const std = @import("std");
const Allocator = std.mem.Allocator;
const geo_mod = @import("../../../search/geo.zig");
const typed_dv = @import("../../../section/typed_doc_values.zig");
const runtime_schema = @import("../../schema.zig");
const document_content_hash = @import("../document_content_hash.zig");

pub const magic: [4]u8 = "AROW".*;
pub const ordinal_version: u32 = 2;

pub const semantic_hash_len: usize = std.crypto.hash.Blake3.digest_length;
// magic + format + schema epoch + reserved capabilities + semantic hash +
// resolved logical write timestamp. Bitmap and section sizes are schema-derived
// and are deliberately omitted. The timestamp is physical/system metadata: it
// participates in the checksum but never in the semantic content hash.
const ordinal_semantic_hash_offset: usize = 16;
const ordinal_write_timestamp_offset: usize = ordinal_semantic_hash_offset + semantic_hash_len;
const ordinal_header_len: usize = ordinal_write_timestamp_offset + @sizeOf(u64);
const checksum_len: usize = @sizeOf(u32);
const capability_sparse_slots: u32 = 1;
const known_ordinal_capabilities: u32 = capability_sparse_slots;
const sparse_entry_len: usize = @sizeOf(u32) * 2; // ordinal + payload start
const sparse_null_flag: u32 = 1 << 31;
const sparse_ordinal_mask: u32 = ~sparse_null_flag;

/// One reconstructable column value. Owns nothing: `path` and (for `bytes_val`)
/// the value bytes borrow either the caller's buffers (when serializing) or the
/// decoded `Row` storage (when reading).
pub const Cell = struct {
    /// Stable schema ordinal. Prepared rows are kept in ascending ordinal order,
    /// allowing sparse encoding to scale with fields present rather than schema
    /// width. `path` remains only for JSON reconstruction after decode.
    ordinal: u32,
    /// Dotted JSON path the value is emitted under during reconstruction.
    path: []const u8,
    value_type: typed_dv.ValueType,
    /// When true a `bytes_val` payload is already valid JSON (embedded
    /// verbatim); otherwise it is a plain string (JSON-escaped on read).
    is_json: bool = false,
    /// The byte payload is a canonical sequence of little-endian f32 values
    /// which reconstructs as a JSON number array rather than a JSON string.
    is_dense_vector: bool = false,
    /// Distinguishes an explicitly stored JSON null from an absent column.
    /// `value_type` retains the declared physical type; `value` is ignored.
    is_null: bool = false,
    value: typed_dv.TypedValue,
};

/// Immutable addressing metadata compiled once per schema epoch. It turns a
/// column ordinal into either a fixed byte offset or a variable offset-table
/// slot without rescanning all preceding columns for every projected value.
pub const PhysicalLayout = struct {
    alloc: Allocator,
    schema_version: u32,
    column_count: u32,
    bitmap_len: u32,
    fixed_len: u32,
    variable_count: u32,
    fixed_offsets: []u32,
    variable_indexes: []u32,
    /// Column ordinals sorted by logical name for stable semantic hashing.
    hash_ordinals: []u32,
    /// Required columns, in physical ordinal order, for sparse validation.
    required_ordinals: []u32,

    const not_applicable = std.math.maxInt(u32);

    pub fn init(alloc: Allocator, table_schema: runtime_schema.TableSchema) !PhysicalLayout {
        const columns = table_schema.relational_columns;
        if (columns.len > sparse_null_flag) return error.InvalidSchema;
        const column_count = std.math.cast(u32, columns.len) orelse return error.InvalidSchema;
        const fixed_offsets = try alloc.alloc(u32, columns.len);
        errdefer alloc.free(fixed_offsets);
        const variable_indexes = try alloc.alloc(u32, columns.len);
        errdefer alloc.free(variable_indexes);
        const hash_ordinals = try alloc.alloc(u32, columns.len);
        errdefer alloc.free(hash_ordinals);
        var required_count: usize = 0;
        for (columns) |column| required_count += @intFromBool(column.required);
        const required_ordinals = try alloc.alloc(u32, required_count);
        errdefer alloc.free(required_ordinals);
        var fixed_pos: usize = 0;
        var variable_index: usize = 0;
        var required_index: usize = 0;
        for (columns, 0..) |column, ordinal| {
            hash_ordinals[ordinal] = @intCast(ordinal);
            if (column.required) {
                required_ordinals[required_index] = @intCast(ordinal);
                required_index += 1;
            }
            if (isVariableColumn(column)) {
                fixed_offsets[ordinal] = not_applicable;
                variable_indexes[ordinal] = std.math.cast(u32, variable_index) orelse return error.InvalidSchema;
                variable_index += 1;
            } else {
                fixed_offsets[ordinal] = std.math.cast(u32, fixed_pos) orelse return error.InvalidSchema;
                variable_indexes[ordinal] = not_applicable;
                fixed_pos = std.math.add(usize, fixed_pos, fixedColumnWidth(column)) catch return error.InvalidSchema;
            }
        }
        std.mem.sort(u32, hash_ordinals, columns, struct {
            fn lessThan(schema_columns: []const runtime_schema.RelationalColumn, lhs: u32, rhs: u32) bool {
                return std.mem.lessThan(u8, schema_columns[lhs].name, schema_columns[rhs].name);
            }
        }.lessThan);
        return .{
            .alloc = alloc,
            .schema_version = table_schema.version,
            .column_count = column_count,
            .bitmap_len = std.math.cast(u32, (columns.len + 7) / 8) orelse return error.InvalidSchema,
            .fixed_len = std.math.cast(u32, fixed_pos) orelse return error.InvalidSchema,
            .variable_count = std.math.cast(u32, variable_index) orelse return error.InvalidSchema,
            .fixed_offsets = fixed_offsets,
            .variable_indexes = variable_indexes,
            .hash_ordinals = hash_ordinals,
            .required_ordinals = required_ordinals,
        };
    }

    pub fn deinit(self: *PhysicalLayout) void {
        self.alloc.free(self.fixed_offsets);
        self.alloc.free(self.variable_indexes);
        self.alloc.free(self.hash_ordinals);
        self.alloc.free(self.required_ordinals);
        self.* = undefined;
    }

    /// Resolve a logical top-level name without rebuilding a map per request.
    /// `hash_ordinals` is already name-sorted for semantic hashing, so it also
    /// serves as the immutable epoch's projection index.
    pub fn ordinalForName(
        self: *const PhysicalLayout,
        columns: []const runtime_schema.RelationalColumn,
        name: []const u8,
    ) ?usize {
        if (columns.len != self.hash_ordinals.len) return null;
        var lower: usize = 0;
        var upper: usize = self.hash_ordinals.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            const ordinal: usize = self.hash_ordinals[middle];
            switch (std.mem.order(u8, columns[ordinal].name, name)) {
                .lt => lower = middle + 1,
                .gt => upper = middle,
                .eq => return ordinal,
            }
        }
        return null;
    }
};

/// A request's exact field selection resolved once against an immutable schema
/// epoch. Ordinals retain caller order and are deduplicated during compilation.
pub const OrdinalProjectionPlan = struct {
    alloc: Allocator,
    schema_version: u32,
    ordinals: []u32,

    pub fn init(
        alloc: Allocator,
        table_schema: runtime_schema.TableSchema,
        layout: *const PhysicalLayout,
        fields: []const []const u8,
    ) !OrdinalProjectionPlan {
        if (layout.schema_version != table_schema.version or
            layout.column_count != table_schema.relational_columns.len)
            return error.RelationalRowSchemaMismatch;
        const ordinals = try alloc.alloc(u32, fields.len);
        errdefer alloc.free(ordinals);
        var emitted = std.AutoHashMapUnmanaged(u32, void).empty;
        defer emitted.deinit(alloc);
        try emitted.ensureTotalCapacity(alloc, std.math.cast(u32, fields.len) orelse
            return error.InvalidArgument);
        var count: usize = 0;
        for (fields) |field| {
            const ordinal = layout.ordinalForName(table_schema.relational_columns, field) orelse continue;
            const result = emitted.getOrPutAssumeCapacity(@intCast(ordinal));
            if (result.found_existing) continue;
            ordinals[count] = @intCast(ordinal);
            count += 1;
        }
        return .{
            .alloc = alloc,
            .schema_version = table_schema.version,
            .ordinals = try alloc.realloc(ordinals, count),
        };
    }

    pub fn deinit(self: *OrdinalProjectionPlan) void {
        self.alloc.free(self.ordinals);
        self.* = undefined;
    }
};

/// True if `value` begins with the typed-row magic. Lets the KV read chokepoint
/// tell a serialized relational row apart from any other stored value without a
/// schema lookup.
pub fn looksLikeRow(value: []const u8) bool {
    return value.len >= magic.len and std.mem.eql(u8, value[0..magic.len], &magic);
}

/// Encode the schema-bound ordinal row format. Paths and physical types live
/// in the immutable schema layout rather than being repeated in every row.
/// Offsets are emitted for variable-width ordinals; a compiled physical layout
/// supplies direct fixed offsets and variable-table indexes for projection.
pub fn serializeOrdinal(
    alloc: Allocator,
    schema_version: u32,
    columns: []const runtime_schema.RelationalColumn,
    cells: []const Cell,
    semantic_hash: [semantic_hash_len]u8,
) ![]u8 {
    return try serializeOrdinalInternal(alloc, schema_version, columns, cells, semantic_hash, null, true, true);
}

/// Trusted encoder for cells projected from an already-parsed and validated
/// PreparedRow. It retains type/layout checks but does not parse JSON-backed
/// cells again merely to prove that generated canonical JSON is valid.
pub fn serializePreparedOrdinal(
    alloc: Allocator,
    schema_version: u32,
    columns: []const runtime_schema.RelationalColumn,
    cells: []const Cell,
    semantic_hash: [semantic_hash_len]u8,
) ![]u8 {
    return try serializeOrdinalInternal(alloc, schema_version, columns, cells, semantic_hash, null, false, true);
}

pub fn serializePreparedOrdinalWithLayout(
    alloc: Allocator,
    schema_version: u32,
    columns: []const runtime_schema.RelationalColumn,
    cells: []const Cell,
    semantic_hash: [semantic_hash_len]u8,
    layout: *const PhysicalLayout,
) ![]u8 {
    if (layout.schema_version != schema_version or layout.column_count != columns.len)
        return error.RelationalRowSchemaMismatch;
    return try serializeOrdinalInternal(alloc, schema_version, columns, cells, semantic_hash, layout, false, true);
}

/// PreparedRow computes the logical digest from the same resolved ordinal
/// values immediately after body construction. Leave the checksum slot pending
/// so installing that digest performs the row's only CRC pass.
pub fn serializePreparedOrdinalDeferredHash(
    alloc: Allocator,
    schema_version: u32,
    columns: []const runtime_schema.RelationalColumn,
    cells: []const Cell,
    layout: ?*const PhysicalLayout,
) ![]u8 {
    const zero_hash = std.mem.zeroes([semantic_hash_len]u8);
    if (layout) |compiled| {
        if (compiled.schema_version != schema_version or compiled.column_count != columns.len)
            return error.RelationalRowSchemaMismatch;
    }
    return try serializeOrdinalInternal(alloc, schema_version, columns, cells, zero_hash, layout, false, false);
}

fn serializeOrdinalInternal(
    alloc: Allocator,
    schema_version: u32,
    columns: []const runtime_schema.RelationalColumn,
    cells: []const Cell,
    semantic_hash: [semantic_hash_len]u8,
    layout: ?*const PhysicalLayout,
    validate_json_cells: bool,
    finalize_checksum: bool,
) ![]u8 {
    if (columns.len > sparse_null_flag) return error.InvalidRelationalRow;
    _ = std.math.cast(u32, columns.len) orelse return error.InvalidRelationalRow;
    const bitmap_len: usize = if (layout) |compiled| compiled.bitmap_len else (columns.len + 7) / 8;
    const fixed_len: usize = if (layout) |compiled| compiled.fixed_len else try fixedSectionLen(columns);
    const variable_count: usize = if (layout) |compiled| compiled.variable_count else countVariableColumns(columns);
    const offsets_len = if (variable_count == 0)
        0
    else
        try std.math.mul(usize, variable_count + 1, @sizeOf(u32));
    var variable_payload_len: usize = 0;
    var sparse_payload_len: usize = 0;
    var previous_ordinal: ?u32 = null;
    for (cells) |cell| {
        if (cell.ordinal >= columns.len or
            (previous_ordinal != null and cell.ordinal <= previous_ordinal.?))
            return error.InvalidRelationalRow;
        previous_ordinal = cell.ordinal;
        const column = columns[cell.ordinal];
        if (!std.mem.eql(u8, cell.path, column.path) or
            cell.value_type != columnValueType(column.column_type) or
            cell.is_json != column.is_json or
            cell.is_dense_vector != (column.column_type == .dense_vector))
            return error.InvalidRelationalRow;
        if (validate_json_cells) try validateCell(alloc, cell);
        if (!cell.is_null and isVariableColumn(column)) {
            const bytes = switch (cell.value) {
                .bytes_val => |value| value,
                else => return error.InvalidRelationalRow,
            };
            variable_payload_len = std.math.add(usize, variable_payload_len, bytes.len) catch
                return error.InvalidRelationalRow;
        }
        if (!cell.is_null) {
            const payload_len = if (isVariableColumn(column)) cell.value.bytes_val.len else fixedColumnWidth(column);
            sparse_payload_len = std.math.add(usize, sparse_payload_len, payload_len) catch
                return error.InvalidRelationalRow;
        }
    }
    _ = std.math.cast(u32, variable_payload_len) orelse return error.InvalidRelationalRow;
    _ = std.math.cast(u32, sparse_payload_len) orelse return error.InvalidRelationalRow;

    const bitmap_sections_len = std.math.mul(usize, 2, bitmap_len) catch return error.InvalidRelationalRow;
    const dense_static_len = try serializedLenAdd(fixed_len, offsets_len);
    const dense_body_len = try serializedLenAdd(dense_static_len, variable_payload_len);
    const sparse_entries_len = try std.math.mul(usize, cells.len, sparse_entry_len);
    const sparse_directory_len = try serializedLenAdd(
        @sizeOf(u32) * 2,
        sparse_entries_len,
    );
    const sparse_body_len = try serializedLenAdd(sparse_directory_len, sparse_payload_len);
    // The representation is selected solely from schema and logical presence,
    // making canonical re-encoding deterministic across processes.
    const dense_storage_len = try serializedLenAdd(bitmap_sections_len, dense_body_len);
    const sparse = sparse_body_len < dense_storage_len;
    const capabilities: u32 = if (sparse) capability_sparse_slots else 0;
    const total_len = try serializedLenAdd(
        ordinal_header_len + checksum_len,
        if (sparse) sparse_body_len else dense_storage_len,
    );
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);
    var pos: usize = 0;
    @memcpy(out[pos..][0..magic.len], &magic);
    pos += magic.len;
    writeU32(out, &pos, ordinal_version);
    writeU32(out, &pos, schema_version);
    writeU32(out, &pos, capabilities);
    @memcpy(out[pos..][0..semantic_hash.len], &semantic_hash);
    pos += semantic_hash.len;
    writeU64(out, &pos, 0);
    if (sparse) {
        writeU32(out, &pos, @intCast(cells.len));
        const entries_start = pos;
        pos += sparse_entries_len;
        const terminal_offset_pos = pos;
        pos += @sizeOf(u32);
        const sparse_payload = out[pos..][0..sparse_payload_len];
        pos += sparse_payload_len;

        var payload_pos: usize = 0;
        for (cells, 0..) |cell, cell_index| {
            const ordinal: usize = cell.ordinal;
            const column = columns[ordinal];
            const entry_pos = entries_start + cell_index * sparse_entry_len;
            const encoded_ordinal: u32 = @as(u32, @intCast(ordinal)) |
                (if (cell.is_null) sparse_null_flag else 0);
            std.mem.writeInt(u32, out[entry_pos..][0..4], encoded_ordinal, .little);
            std.mem.writeInt(u32, out[entry_pos + 4 ..][0..4], @intCast(payload_pos), .little);
            if (!cell.is_null) {
                if (isVariableColumn(column)) {
                    const bytes = cell.value.bytes_val;
                    @memcpy(sparse_payload[payload_pos..][0..bytes.len], bytes);
                    payload_pos += bytes.len;
                } else {
                    const width = fixedColumnWidth(column);
                    try encodeOrdinalFixed(sparse_payload[payload_pos..][0..width], cell);
                    payload_pos += width;
                }
            }
        }
        std.mem.writeInt(u32, out[terminal_offset_pos..][0..4], @intCast(payload_pos), .little);
        std.debug.assert(payload_pos == sparse_payload_len);
    } else {
        const present = out[pos..][0..bitmap_len];
        @memset(present, 0);
        pos += bitmap_len;
        const nulls = out[pos..][0..bitmap_len];
        @memset(nulls, 0);
        pos += bitmap_len;
        const fixed = out[pos..][0..fixed_len];
        @memset(fixed, 0);
        pos += fixed_len;
        const offsets_start = pos;
        pos += offsets_len;
        const variable_payload = out[pos..][0..variable_payload_len];
        pos += variable_payload_len;

        var cell_index: usize = 0;
        var fixed_pos: usize = 0;
        var variable_index: usize = 0;
        var variable_pos: usize = 0;
        for (columns, 0..) |column, ordinal| {
            const variable = isVariableColumn(column);
            if (variable) std.mem.writeInt(u32, out[offsets_start + variable_index * 4 ..][0..4], @intCast(variable_pos), .little);
            if (cell_index < cells.len) {
                const cell = cells[cell_index];
                if (cell.ordinal == ordinal) {
                    setBitmapBit(present, ordinal);
                    if (cell.is_null) {
                        setBitmapBit(nulls, ordinal);
                    } else if (variable) {
                        const bytes = cell.value.bytes_val;
                        @memcpy(variable_payload[variable_pos..][0..bytes.len], bytes);
                        variable_pos += bytes.len;
                    } else {
                        const width = fixedColumnWidth(column);
                        try encodeOrdinalFixed(fixed[fixed_pos..][0..width], cell);
                    }
                    cell_index += 1;
                }
            }
            if (variable) {
                variable_index += 1;
                std.mem.writeInt(u32, out[offsets_start + variable_index * 4 ..][0..4], @intCast(variable_pos), .little);
            } else fixed_pos += fixedColumnWidth(column);
        }
        std.debug.assert(fixed_pos == fixed_len and variable_index == variable_count and variable_pos == variable_payload_len);
    }
    const checksum = if (finalize_checksum) std.hash.Crc32.hash(out[0..pos]) else 0;
    writeU32(out, &pos, checksum);
    std.debug.assert(pos == out.len);
    return out;
}

pub fn rowSchemaVersion(value: []const u8) !u32 {
    if (!looksLikeRow(value)) return error.InvalidRelationalRow;
    if (value.len < 8) return error.InvalidRelationalRow;
    var pos: usize = magic.len;
    const row_version = readU32(value, &pos);
    if (row_version != ordinal_version or value.len < ordinal_header_len + checksum_len)
        return error.UnsupportedRelationalRowVersion;
    return readU32(value, &pos);
}

/// Return the canonical logical digest embedded by AROW v2. Verify the physical
/// checksum before trusting it: a same-content overwrite should repair a
/// damaged row, not mistake an intact digest header for intact payload bytes.
pub fn rowSemanticHash(value: []const u8) ![semantic_hash_len]u8 {
    const digest = try rowSemanticHashTrusted(value);
    const stored_checksum = std.mem.readInt(u32, value[value.len - checksum_len ..][0..checksum_len], .little);
    if (std.hash.Crc32.hash(value[0 .. value.len - checksum_len]) != stored_checksum)
        return error.RelationalRowChecksumMismatch;
    return digest;
}

/// Read the embedded semantic digest after the caller has already performed a
/// strict row decode in the same storage snapshot.
pub fn rowSemanticHashTrusted(value: []const u8) ![semantic_hash_len]u8 {
    if (!looksLikeRow(value)) return error.InvalidRelationalRow;
    if (value.len < 8) return error.InvalidRelationalRow;
    var pos: usize = magic.len;
    const row_version = readU32(value, &pos);
    if (row_version != ordinal_version or value.len < ordinal_header_len + checksum_len)
        return error.UnsupportedRelationalRowVersion;
    _ = readU32(value, &pos); // schema version
    if (readU32(value, &pos) & ~known_ordinal_capabilities != 0) return error.UnsupportedRelationalRowVersion;
    std.debug.assert(pos == ordinal_semantic_hash_offset);
    var digest: [semantic_hash_len]u8 = undefined;
    @memcpy(&digest, value[pos..][0..semantic_hash_len]);
    return digest;
}

/// Return the resolved timestamp used for TTL visibility. The strict variant
/// authenticates the complete physical row before exposing system metadata.
pub fn rowWriteTimestampNs(value: []const u8) !u64 {
    const timestamp = try rowWriteTimestampNsTrusted(value);
    const stored_checksum = std.mem.readInt(u32, value[value.len - checksum_len ..][0..checksum_len], .little);
    if (std.hash.Crc32.hash(value[0 .. value.len - checksum_len]) != stored_checksum)
        return error.RelationalRowChecksumMismatch;
    return timestamp;
}

pub fn rowWriteTimestampNsTrusted(value: []const u8) !u64 {
    _ = try rowSemanticHashTrusted(value);
    return std.mem.readInt(u64, value[ordinal_write_timestamp_offset..][0..@sizeOf(u64)], .little);
}

/// Install the semantic digest after a PreparedRow has encoded its already
/// validated ordinal cells. Updating the physical checksum keeps the operation
/// equivalent to supplying the digest to the encoder without a second row
/// traversal.
pub fn setOrdinalSemanticHash(value: []u8, digest: [semantic_hash_len]u8) !void {
    const timestamp_ns = try rowWriteTimestampNsTrusted(value);
    try finalizeOrdinalMetadata(value, digest, timestamp_ns);
}

/// Install all mutable header metadata and authenticate the physical row in a
/// single pass. PreparedRow deliberately leaves the checksum pending until the
/// request timestamp is known so large JSON/vector/blob payloads are not swept
/// twice merely to patch two adjacent header fields.
pub fn finalizeOrdinalMetadata(
    value: []u8,
    digest: [semantic_hash_len]u8,
    timestamp_ns: u64,
) !void {
    if (value.len < ordinal_header_len + checksum_len or !looksLikeRow(value))
        return error.InvalidRelationalRow;
    var pos: usize = magic.len;
    if (readU32(value, &pos) != ordinal_version) return error.UnsupportedRelationalRowVersion;
    _ = readU32(value, &pos);
    if (readU32(value, &pos) & ~known_ordinal_capabilities != 0) return error.UnsupportedRelationalRowVersion;
    @memcpy(value[ordinal_semantic_hash_offset..][0..semantic_hash_len], &digest);
    std.mem.writeInt(u64, value[ordinal_write_timestamp_offset..][0..@sizeOf(u64)], timestamp_ns, .little);
    const checksum = std.hash.Crc32.hash(value[0 .. value.len - checksum_len]);
    std.mem.writeInt(u32, value[value.len - checksum_len ..][0..checksum_len], checksum, .little);
}

/// Install request-resolved system metadata after preparation. This remains
/// separate from the semantic digest so identical logical rows retain the same
/// user-visible hash when their write/expiry timestamp changes.
pub fn setOrdinalWriteTimestampNs(value: []u8, timestamp_ns: u64) !void {
    const digest = try rowSemanticHashTrusted(value);
    try finalizeOrdinalMetadata(value, digest, timestamp_ns);
}

pub fn reconstructOrdinalValueAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
) ![]u8 {
    const parsed = try parseOrdinal(value, table_schema);
    return try reconstructParsedOrdinalValueAlloc(alloc, parsed, table_schema);
}

fn reconstructParsedOrdinalValueAlloc(
    alloc: Allocator,
    parsed: ParsedOrdinal,
    table_schema: runtime_schema.TableSchema,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var needs_comma = false;
    var iterator = OrdinalCellIterator{ .parsed = parsed, .table_schema = table_schema };
    while (try iterator.next()) |cell| {
        try appendValidatedCellValue(alloc, &out, cell, needs_comma);
        needs_comma = true;
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn reconstructOrdinalValueWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) ![]u8 {
    const parsed = try parseOrdinalWithLayoutRead(value, table_schema, layout);
    return try reconstructParsedOrdinalValueAlloc(alloc, parsed, table_schema);
}

/// Logical JSON source plus its already-decoded tree for callers that need
/// both representations without reparsing the reconstructed document.
pub const MaterializedOrdinalDocument = struct {
    json: []u8,
    root: std.json.Value,
    root_arena: *std.heap.ArenaAllocator,

    pub fn retainedBytes(self: *const @This()) usize {
        return self.json.len +| self.root_arena.queryCapacity();
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.json);
        self.root_arena.deinit();
        alloc.destroy(self.root_arena);
        self.* = undefined;
    }
};

/// Owned logical tree for consumers that never persist the reconstructed JSON
/// source (for example full-text backfill). Keeping this separate prevents an
/// O(row bytes) stringify allocation for every row in a rebuild.
pub const MaterializedOrdinalRoot = struct {
    root: std.json.Value,
    root_arena: *std.heap.ArenaAllocator,

    pub fn retainedBytes(self: *const @This()) usize {
        return self.root_arena.queryCapacity();
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.root_arena.deinit();
        alloc.destroy(self.root_arena);
        self.* = undefined;
    }
};

pub fn materializeOrdinalRootWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !MaterializedOrdinalRoot {
    const parsed = try parseOrdinalWithLayoutRead(value, table_schema, layout);
    return try materializeParsedOrdinalRootAlloc(alloc, parsed, table_schema);
}

/// Restore boundary that authenticates and canonically validates one physical
/// row while constructing the public-validator tree from the same typed-cell
/// traversal. This avoids separately decoding the row for integrity and schema
/// validation while preserving the semantic-hash/physical-checksum split.
pub fn validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !MaterializedOrdinalRoot {
    var iterator = try canonicalOrdinalCellIteratorWithLayout(value, table_schema, layout);
    const stored_hash = iterator.semanticHash();
    const root_arena = try alloc.create(std.heap.ArenaAllocator);
    root_arena.* = std.heap.ArenaAllocator.init(alloc);
    errdefer {
        root_arena.deinit();
        alloc.destroy(root_arena);
    }
    const root_alloc = root_arena.allocator();
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };

    if (iterator.isSparse()) {
        const cells = try alloc.alloc(Cell, iterator.presentCount());
        defer alloc.free(cells);
        var count: usize = 0;
        while (try iterator.next()) |cell| : (count += 1) {
            cells[count] = cell;
            const column = table_schema.relational_columns[cell.ordinal];
            const logical = try ownedJsonValueFromCellAlloc(root_alloc, column, cell);
            if (cell.is_json and !cell.is_null) {
                const canonical = try document_content_hash.canonicalJsonValueAlloc(root_alloc, logical);
                if (!std.mem.eql(u8, canonical, cell.value.bytes_val)) return error.NonCanonicalRelationalRow;
            }
            try root.object.put(root_alloc, try root_alloc.dupe(u8, column.name), logical);
        }
        std.debug.assert(count == cells.len);
        std.mem.sort(Cell, cells, table_schema.relational_columns, struct {
            fn lessThan(columns: []const runtime_schema.RelationalColumn, lhs: Cell, rhs: Cell) bool {
                return std.mem.lessThan(u8, columns[lhs.ordinal].name, columns[rhs.ordinal].name);
            }
        }.lessThan);
        const computed_hash = try document_content_hash.hashRelationalSparseCellsCanonical(cells, table_schema);
        if (!std.mem.eql(u8, &stored_hash, &computed_hash)) return error.RelationalRowSemanticHashMismatch;
    } else {
        const cells = try alloc.alloc(?Cell, table_schema.relational_columns.len);
        defer alloc.free(cells);
        @memset(cells, null);
        while (try iterator.next()) |cell| {
            cells[cell.ordinal] = cell;
            const column = table_schema.relational_columns[cell.ordinal];
            const logical = try ownedJsonValueFromCellAlloc(root_alloc, column, cell);
            if (cell.is_json and !cell.is_null) {
                const canonical = try document_content_hash.canonicalJsonValueAlloc(root_alloc, logical);
                if (!std.mem.eql(u8, canonical, cell.value.bytes_val)) return error.NonCanonicalRelationalRow;
            }
            try root.object.put(root_alloc, try root_alloc.dupe(u8, column.name), logical);
        }
        const computed_hash = try document_content_hash.hashRelationalCellsWithOrdinals(
            alloc,
            cells,
            table_schema,
            layout.hash_ordinals,
        );
        if (!std.mem.eql(u8, &stored_hash, &computed_hash)) return error.RelationalRowSemanticHashMismatch;
    }
    return .{ .root = root, .root_arena = root_arena };
}

fn materializeParsedOrdinalRootAlloc(
    alloc: Allocator,
    parsed: ParsedOrdinal,
    table_schema: runtime_schema.TableSchema,
) !MaterializedOrdinalRoot {
    var cells = OrdinalCellIterator{ .parsed = parsed, .table_schema = table_schema };
    const root_arena = try alloc.create(std.heap.ArenaAllocator);
    root_arena.* = std.heap.ArenaAllocator.init(alloc);
    errdefer {
        root_arena.deinit();
        alloc.destroy(root_arena);
    }
    const root_alloc = root_arena.allocator();
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    while (try cells.next()) |cell| {
        const column = table_schema.relational_columns[cell.ordinal];
        const logical = try ownedJsonValueFromCellAlloc(root_alloc, column, cell);
        try root.object.put(root_alloc, try root_alloc.dupe(u8, column.name), logical);
    }
    return .{ .root = root, .root_arena = root_arena };
}

pub fn materializeOrdinalDocumentWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !MaterializedOrdinalDocument {
    var cells = try ordinalCellIteratorWithLayout(value, table_schema, layout);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    const root_arena = try alloc.create(std.heap.ArenaAllocator);
    root_arena.* = std.heap.ArenaAllocator.init(alloc);
    errdefer {
        root_arena.deinit();
        alloc.destroy(root_arena);
    }
    const root_alloc = root_arena.allocator();
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    try out.append(alloc, '{');
    var needs_comma = false;
    while (try cells.next()) |cell| {
        const column = table_schema.relational_columns[cell.ordinal];
        try appendValidatedCellValue(alloc, &out, cell, needs_comma);
        needs_comma = true;

        const logical = try ownedJsonValueFromCellAlloc(root_alloc, column, cell);
        // Schema names outlive this projection arena. Keep them borrowed instead
        // of retaining a second copy of every field name.
        try root.object.put(root_alloc, try root_alloc.dupe(u8, column.name), logical);
    }
    try out.append(alloc, '}');
    return .{ .json = try out.toOwnedSlice(alloc), .root = root, .root_arena = root_arena };
}

fn ownedJsonValueFromCellAlloc(
    alloc: Allocator,
    column: runtime_schema.RelationalColumn,
    cell: Cell,
) !std.json.Value {
    if (cell.is_null) return .null;
    return switch (column.column_type) {
        .datetime => if (cell.value.u64_val <= std.math.maxInt(i64))
            .{ .integer = @intCast(cell.value.u64_val) }
        else
            .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{cell.value.u64_val}) },
        .integer => .{ .integer = cell.value.i64_val },
        .number => .{ .float = cell.value.f64_val },
        .boolean => .{ .bool = cell.value.bool_val },
        // Row payloads may be backed by a scan cursor and must be retained.
        .string, .blob, .geoshape => .{ .string = try alloc.dupe(u8, cell.value.bytes_val) },
        .json => try std.json.parseFromSliceLeaky(std.json.Value, alloc, cell.value.bytes_val, .{
            .allocate = .alloc_always,
            .parse_numbers = false,
        }),
        .geopoint => blk: {
            var object = std.json.ObjectMap.empty;
            try object.put(alloc, "lat", .{ .float = cell.value.geo_point.lat });
            try object.put(alloc, "lon", .{ .float = cell.value.geo_point.lon });
            break :blk .{ .object = object };
        },
        .dense_vector => blk: {
            var array = std.json.Array.init(alloc);
            errdefer array.deinit();
            const bytes = cell.value.bytes_val;
            try array.ensureTotalCapacity(bytes.len / @sizeOf(f32));
            var pos: usize = 0;
            while (pos < bytes.len) : (pos += @sizeOf(f32)) {
                const number: f32 = @bitCast(std.mem.readInt(u32, bytes[pos..][0..4], .little));
                array.appendAssumeCapacity(.{ .float = number });
            }
            break :blk .{ .array = array };
        },
    };
}

pub fn validateOrdinal(value: []const u8, table_schema: runtime_schema.TableSchema) !void {
    const parsed = try parseOrdinal(value, table_schema);
    var iterator = OrdinalCellIterator{ .parsed = parsed, .table_schema = table_schema };
    while (try iterator.next()) |_| {}
}

pub fn validateOrdinalWithLayout(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !void {
    const parsed = try parseOrdinalWithLayout(value, table_schema, layout);
    var iterator = OrdinalCellIterator{ .parsed = parsed, .table_schema = table_schema };
    while (try iterator.next()) |_| {}
}

/// Strictly validate once and expose borrowed typed cells by ordinal. Restore,
/// scrubbing, and projected readers can then operate without reconstructing a
/// complete JSON object or reparsing scalar values.
pub fn collectOrdinalCellsWithLayout(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
    cells: []?Cell,
) ![semantic_hash_len]u8 {
    if (cells.len != table_schema.relational_columns.len) return error.InvalidArgument;
    @memset(cells, null);
    var iterator = try canonicalOrdinalCellIteratorWithLayout(value, table_schema, layout);
    while (try iterator.next()) |cell| cells[cell.ordinal] = cell;
    return iterator.semanticHash();
}

pub fn findCellByOrdinal(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    ordinal: usize,
) !?Cell {
    if (ordinal >= table_schema.relational_columns.len) return null;
    const parsed = try parseOrdinal(value, table_schema);
    return try findParsedOrdinalCell(parsed, table_schema.relational_columns, ordinal);
}

pub fn findCellByOrdinalWithLayout(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
    ordinal: usize,
) !?Cell {
    if (ordinal >= table_schema.relational_columns.len) return null;
    const parsed = try parseOrdinalWithLayoutRead(value, table_schema, layout);
    return try findParsedOrdinalCell(parsed, table_schema.relational_columns, ordinal);
}

/// Reconstruct only selected top-level columns. Callers preflight that field
/// expressions are exact names (no exclusions, wildcards, or nested paths).
pub fn projectOrdinalFieldsWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
    fields: []const []const u8,
) ![]u8 {
    var plan = try OrdinalProjectionPlan.init(alloc, table_schema, layout, fields);
    defer plan.deinit();
    return try projectOrdinalPlanWithLayoutAlloc(alloc, value, table_schema, layout, plan);
}

pub fn projectOrdinalPlanWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
    plan: OrdinalProjectionPlan,
) ![]u8 {
    if (plan.schema_version != table_schema.version) return error.RelationalRowSchemaMismatch;
    const parsed = try parseOrdinalWithLayoutRead(value, table_schema, layout);
    return try projectParsedOrdinalPlanAlloc(alloc, parsed, table_schema, plan);
}

fn projectParsedOrdinalPlanAlloc(
    alloc: Allocator,
    parsed: ParsedOrdinal,
    table_schema: runtime_schema.TableSchema,
    plan: OrdinalProjectionPlan,
) ![]u8 {
    if (plan.schema_version != table_schema.version) return error.RelationalRowSchemaMismatch;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var needs_comma = false;
    for (plan.ordinals) |ordinal_raw| {
        const ordinal: usize = ordinal_raw;
        const cell = (try findParsedOrdinalCell(parsed, table_schema.relational_columns, ordinal)) orelse continue;
        try appendValidatedCellValue(alloc, &out, cell, needs_comma);
        needs_comma = true;
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

/// Multi-column projection after the exact row bytes were authenticated by a
/// storage block/page checksum or a strict AROW checksum read in the same
/// snapshot. Header, bitmap, target-slot, type, and JSON bounds remain checked;
/// unrelated payload and sparse-directory entries are not rescanned.
pub fn projectOrdinalFieldsTrustedWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
    fields: []const []const u8,
) ![]u8 {
    var plan = try OrdinalProjectionPlan.init(alloc, table_schema, layout, fields);
    defer plan.deinit();
    return try projectOrdinalPlanTrustedWithLayoutAlloc(alloc, value, table_schema, layout, plan);
}

pub fn projectOrdinalPlanTrustedWithLayoutAlloc(
    alloc: Allocator,
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
    plan: OrdinalProjectionPlan,
) ![]u8 {
    if (layout.schema_version != table_schema.version or
        layout.column_count != table_schema.relational_columns.len or
        plan.schema_version != table_schema.version)
        return error.RelationalRowSchemaMismatch;
    const parsed = try parseOrdinalInternal(value, table_schema, layout, false, false);
    return try projectParsedOrdinalPlanAlloc(alloc, parsed, table_schema, plan);
}

/// Project one column after the containing storage page/block has already
/// authenticated the row. Structural header and target-slot bounds remain
/// checked, but this avoids hashing and validating unrelated payload bytes.
pub fn findCellByOrdinalTrusted(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
    ordinal: usize,
) !?Cell {
    if (ordinal >= table_schema.relational_columns.len) return null;
    const parsed = try parseOrdinalInternal(value, table_schema, layout, false, false);
    return try findParsedOrdinalCell(parsed, table_schema.relational_columns, ordinal);
}

const ParsedOrdinal = struct {
    value: []const u8,
    present: []const u8,
    nulls: []const u8,
    fixed: []const u8,
    offsets_start: usize,
    payload: []const u8,
    capabilities: u32,
    sparse_entries_start: usize = 0,
    sparse_entry_count: usize = 0,
    sparse_terminal_offset_pos: usize = 0,
    semantic_hash: [semantic_hash_len]u8,
    write_timestamp_ns: u64,
    layout: ?*const PhysicalLayout = null,
};

/// One validated row traversal. Dense rows advance through the schema bitmap;
/// sparse rows advance directly through their ordinal directory, keeping
/// restore, hashing, and index replay proportional to the fields present.
pub const OrdinalCellIterator = struct {
    parsed: ParsedOrdinal,
    table_schema: runtime_schema.TableSchema,
    next_ordinal: usize = 0,
    next_sparse_entry: usize = 0,

    pub fn isSparse(self: *const @This()) bool {
        return self.parsed.capabilities & capability_sparse_slots != 0;
    }

    pub fn presentCount(self: *const @This()) usize {
        return if (self.isSparse()) self.parsed.sparse_entry_count else bitmapPopulation(self.parsed.present);
    }

    pub fn semanticHash(self: *const @This()) [semantic_hash_len]u8 {
        return self.parsed.semantic_hash;
    }

    pub fn next(self: *@This()) !?Cell {
        if (self.isSparse()) {
            if (self.next_sparse_entry >= self.parsed.sparse_entry_count) return null;
            const entry_index = self.next_sparse_entry;
            self.next_sparse_entry += 1;
            const ordinal: usize = sparseOrdinalAt(self.parsed, entry_index);
            if (ordinal >= self.table_schema.relational_columns.len) return error.InvalidRelationalRow;
            const column = self.table_schema.relational_columns[ordinal];
            const payload = try sparsePayloadSliceAtIndex(self.parsed, entry_index);
            return try ordinalCellFromPayload(column, ordinal, payload, sparseEntryIsNull(self.parsed, entry_index));
        }

        while (self.next_ordinal < self.table_schema.relational_columns.len) {
            const ordinal = self.next_ordinal;
            self.next_ordinal += 1;
            if (!bitmapBit(self.parsed.present, ordinal)) continue;
            const column = self.table_schema.relational_columns[ordinal];
            const payload = if (self.parsed.layout != null)
                try ordinalPayloadSliceChecked(self.parsed, column, ordinal)
            else
                ordinalPayloadSlice(self.parsed, self.table_schema.relational_columns, column, ordinal);
            return try ordinalCellFromPayload(column, ordinal, payload, bitmapBit(self.parsed.nulls, ordinal));
        }
        return null;
    }
};

pub fn ordinalCellIteratorWithLayout(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !OrdinalCellIterator {
    return .{
        .parsed = try parseOrdinalWithLayoutRead(value, table_schema, layout),
        .table_schema = table_schema,
    };
}

pub fn canonicalOrdinalCellIteratorWithLayout(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !OrdinalCellIterator {
    return .{
        .parsed = try parseOrdinalWithLayout(value, table_schema, layout),
        .table_schema = table_schema,
    };
}

/// Parsed, immutable addressing view over one AROW. Filter, projection, and
/// index execution can resolve several ordinals after paying header/bitmap
/// validation once, without materializing the enclosing JSON object.
pub const OrdinalRowView = struct {
    parsed: ParsedOrdinal,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,

    pub fn ordinalForName(self: OrdinalRowView, name: []const u8) ?usize {
        return self.layout.ordinalForName(self.table_schema.relational_columns, name);
    }

    pub fn findCell(self: OrdinalRowView, ordinal: usize) !?Cell {
        if (ordinal >= self.table_schema.relational_columns.len) return null;
        return try findParsedOrdinalCell(self.parsed, self.table_schema.relational_columns, ordinal);
    }

    /// Shared logical interpretation for projection and predicate execution.
    /// Composite values belong to the caller's arena, never to a scan cursor.
    pub fn materializeCellAlloc(self: OrdinalRowView, alloc: Allocator, cell: Cell) !std.json.Value {
        return try ownedJsonValueFromCellAlloc(alloc, self.table_schema.relational_columns[cell.ordinal], cell);
    }

    pub fn semanticHash(self: OrdinalRowView) [semantic_hash_len]u8 {
        return self.parsed.semantic_hash;
    }

    pub fn writeTimestampNs(self: OrdinalRowView) u64 {
        return self.parsed.write_timestamp_ns;
    }

    pub fn reconstructValueAlloc(self: OrdinalRowView, alloc: Allocator) ![]u8 {
        return try reconstructParsedOrdinalValueAlloc(alloc, self.parsed, self.table_schema);
    }

    pub fn materializeRootAlloc(self: OrdinalRowView, alloc: Allocator) !MaterializedOrdinalRoot {
        return try materializeParsedOrdinalRootAlloc(alloc, self.parsed, self.table_schema);
    }

    pub fn projectAlloc(self: OrdinalRowView, alloc: Allocator, plan: OrdinalProjectionPlan) ![]u8 {
        return try projectParsedOrdinalPlanAlloc(alloc, self.parsed, self.table_schema, plan);
    }
};

pub fn ordinalRowView(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !OrdinalRowView {
    return .{
        .parsed = try parseOrdinalWithLayoutRead(value, table_schema, layout),
        .table_schema = table_schema,
        .layout = layout,
    };
}

/// Storage pages with authenticated values may omit the row CRC pass. Bounds,
/// schema identity, capabilities, and addressed cell types remain validated.
pub fn ordinalRowViewTrusted(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !OrdinalRowView {
    if (layout.schema_version != table_schema.version or
        layout.column_count != table_schema.relational_columns.len)
        return error.RelationalRowSchemaMismatch;
    return .{
        .parsed = try parseOrdinalInternal(value, table_schema, layout, false, false),
        .table_schema = table_schema,
        .layout = layout,
    };
}

fn parseOrdinal(value: []const u8, table_schema: runtime_schema.TableSchema) !ParsedOrdinal {
    return try parseOrdinalInternal(value, table_schema, null, true, true);
}

fn parseOrdinalWithLayout(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !ParsedOrdinal {
    if (layout.schema_version != table_schema.version or
        layout.column_count != table_schema.relational_columns.len)
        return error.RelationalRowSchemaMismatch;
    return try parseOrdinalInternal(value, table_schema, layout, true, true);
}

fn parseOrdinalWithLayoutRead(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: *const PhysicalLayout,
) !ParsedOrdinal {
    if (layout.schema_version != table_schema.version or
        layout.column_count != table_schema.relational_columns.len)
        return error.RelationalRowSchemaMismatch;
    return try parseOrdinalInternal(value, table_schema, layout, true, false);
}

fn parseOrdinalInternal(
    value: []const u8,
    table_schema: runtime_schema.TableSchema,
    layout: ?*const PhysicalLayout,
    verify_checksum: bool,
    canonical: bool,
) !ParsedOrdinal {
    if (value.len < ordinal_header_len + checksum_len or !looksLikeRow(value)) return error.InvalidRelationalRow;
    if (verify_checksum) {
        const stored_checksum = std.mem.readInt(u32, value[value.len - checksum_len ..][0..checksum_len], .little);
        if (std.hash.Crc32.hash(value[0 .. value.len - checksum_len]) != stored_checksum) return error.RelationalRowChecksumMismatch;
    }
    var pos: usize = magic.len;
    if (readU32(value, &pos) != ordinal_version) return error.UnsupportedRelationalRowVersion;
    if (readU32(value, &pos) != table_schema.version) return error.RelationalRowSchemaMismatch;
    const capabilities = readU32(value, &pos);
    if (capabilities & ~known_ordinal_capabilities != 0) return error.UnsupportedRelationalRowVersion;
    const sparse = capabilities & capability_sparse_slots != 0;
    const bitmap_len: usize = if (layout) |compiled| compiled.bitmap_len else (table_schema.relational_columns.len + 7) / 8;
    const expected_bitmap_len: usize = if (layout) |compiled| compiled.bitmap_len else (table_schema.relational_columns.len + 7) / 8;
    if (bitmap_len != expected_bitmap_len) return error.InvalidRelationalRow;
    const fixed_len: usize = if (layout) |compiled| compiled.fixed_len else (fixedSectionLen(table_schema.relational_columns) catch return error.InvalidRelationalRow);
    const expected_fixed_len: usize = if (layout) |compiled| compiled.fixed_len else (fixedSectionLen(table_schema.relational_columns) catch return error.InvalidRelationalRow);
    if (fixed_len != expected_fixed_len)
        return error.RelationalRowSchemaMismatch;
    const variable_count: usize = if (layout) |compiled| compiled.variable_count else countVariableColumns(table_schema.relational_columns);
    const expected_variable_count: usize = if (layout) |compiled| compiled.variable_count else countVariableColumns(table_schema.relational_columns);
    if (variable_count != expected_variable_count) return error.RelationalRowSchemaMismatch;
    var semantic_hash: [semantic_hash_len]u8 = undefined;
    @memcpy(&semantic_hash, value[pos..][0..semantic_hash_len]);
    pos += semantic_hash_len;
    const write_timestamp_ns = readU64(value, &pos);
    var present: []const u8 = &.{};
    var nulls: []const u8 = &.{};
    if (!sparse) {
        const bitmap_sections_len = std.math.mul(usize, 2, bitmap_len) catch return error.InvalidRelationalRow;
        if (bitmap_sections_len > value.len - checksum_len -| pos) return error.InvalidRelationalRow;
        present = value[pos..][0..bitmap_len];
        pos += bitmap_len;
        nulls = value[pos..][0..bitmap_len];
        pos += bitmap_len;
        for (nulls, present) |null_byte, present_byte| if (null_byte & ~present_byte != 0) return error.InvalidRelationalRow;
        if (table_schema.relational_columns.len % 8 != 0 and bitmap_len != 0) {
            const used_bits: u3 = @intCast(table_schema.relational_columns.len % 8);
            const unused_mask: u8 = ~((@as(u8, 1) << used_bits) - 1);
            if (present[bitmap_len - 1] & unused_mask != 0 or nulls[bitmap_len - 1] & unused_mask != 0)
                return error.InvalidRelationalRow;
        }
    }
    var parsed: ParsedOrdinal = undefined;
    if (sparse) {
        if (@sizeOf(u32) * 2 > value.len - checksum_len -| pos) return error.InvalidRelationalRow;
        const entry_count: usize = readU32(value, &pos);
        if (entry_count > table_schema.relational_columns.len) return error.InvalidRelationalRow;
        const entries_len = std.math.mul(usize, entry_count, sparse_entry_len) catch return error.InvalidRelationalRow;
        const directory_len = std.math.add(usize, entries_len, @sizeOf(u32)) catch return error.InvalidRelationalRow;
        if (directory_len > value.len - checksum_len -| pos) return error.InvalidRelationalRow;
        const entries_start = pos;
        const terminal_offset_pos = pos + entries_len;
        pos += directory_len;
        const payload = value[pos .. value.len - checksum_len];
        const terminal_offset = std.mem.readInt(u32, value[terminal_offset_pos..][0..4], .little);
        if (terminal_offset != payload.len) return error.InvalidRelationalRow;
        if (verify_checksum or canonical) {
            var previous_ordinal: ?u32 = null;
            var previous_offset: u32 = 0;
            for (0..entry_count) |entry_index| {
                const entry_pos = entries_start + entry_index * sparse_entry_len;
                const encoded_ordinal = std.mem.readInt(u32, value[entry_pos..][0..4], .little);
                const ordinal = encoded_ordinal & sparse_ordinal_mask;
                const offset = std.mem.readInt(u32, value[entry_pos + 4 ..][0..4], .little);
                if (ordinal >= table_schema.relational_columns.len or
                    (previous_ordinal != null and ordinal <= previous_ordinal.?) or
                    offset < previous_offset or offset > payload.len)
                    return error.InvalidRelationalRow;
                previous_ordinal = ordinal;
                previous_offset = offset;
            }
            if (terminal_offset < previous_offset)
                return error.InvalidRelationalRow;
        }
        parsed = .{
            .value = value,
            .present = present,
            .nulls = nulls,
            .fixed = &.{},
            .offsets_start = 0,
            .payload = payload,
            .capabilities = capabilities,
            .sparse_entries_start = entries_start,
            .sparse_entry_count = entry_count,
            .sparse_terminal_offset_pos = terminal_offset_pos,
            .semantic_hash = semantic_hash,
            .write_timestamp_ns = write_timestamp_ns,
            .layout = layout,
        };
        if (canonical) {
            for (0..entry_count) |entry_index| {
                const ordinal: usize = sparseOrdinalAt(parsed, entry_index);
                const slot = try sparsePayloadSliceAtIndex(parsed, entry_index);
                const column = table_schema.relational_columns[ordinal];
                if (sparseEntryIsNull(parsed, entry_index)) {
                    if (slot.len != 0) return error.NonCanonicalRelationalRow;
                } else if (!isVariableColumn(column) and slot.len != fixedColumnWidth(column)) {
                    return error.InvalidRelationalRow;
                }
            }
        }
    } else {
        const offsets_len = if (variable_count == 0)
            0
        else
            std.math.mul(usize, variable_count + 1, @sizeOf(u32)) catch return error.InvalidRelationalRow;
        const fixed_start = pos;
        const offsets_start = std.math.add(usize, fixed_start, fixed_len) catch return error.InvalidRelationalRow;
        const payload_start = std.math.add(usize, offsets_start, offsets_len) catch return error.InvalidRelationalRow;
        if (payload_start > value.len - checksum_len) return error.InvalidRelationalRow;
        const payload_len = value.len - checksum_len - payload_start;
        if (canonical) {
            if (variable_count == 0) {
                if (payload_len != 0) return error.InvalidRelationalRow;
            } else {
                var previous: u32 = 0;
                for (0..variable_count + 1) |ordinal| {
                    const offset = std.mem.readInt(u32, value[offsets_start + ordinal * 4 ..][0..4], .little);
                    if (offset < previous or offset > payload_len) return error.InvalidRelationalRow;
                    previous = offset;
                }
                if (previous != payload_len) return error.InvalidRelationalRow;
            }
        }
        parsed = .{
            .value = value,
            .present = present,
            .nulls = nulls,
            .fixed = value[fixed_start..][0..fixed_len],
            .offsets_start = offsets_start,
            .payload = value[payload_start..][0..payload_len],
            .capabilities = capabilities,
            .semantic_hash = semantic_hash,
            .write_timestamp_ns = write_timestamp_ns,
            .layout = layout,
        };
    }
    // Unset and null slots must have a unique zero/empty physical encoding.
    // This makes byte equality meaningful during restore verification.
    if (canonical and sparse) {
        if (layout) |compiled| {
            for (compiled.required_ordinals) |ordinal| {
                if (!parsedHasOrdinal(parsed, ordinal)) return error.InvalidRelationalRow;
            }
        } else {
            for (table_schema.relational_columns, 0..) |column, ordinal| {
                if (column.required and !parsedHasOrdinal(parsed, ordinal)) return error.InvalidRelationalRow;
            }
        }
        for (0..parsed.sparse_entry_count) |entry_index| {
            const ordinal = sparseOrdinalAt(parsed, entry_index);
            const column = table_schema.relational_columns[ordinal];
            if (sparseEntryIsNull(parsed, entry_index) and !column.allows_null) return error.InvalidRelationalRow;
        }
    } else if (canonical) {
        for (table_schema.relational_columns, 0..) |column, ordinal| {
            if (!bitmapBit(present, ordinal)) {
                if (column.required) return error.InvalidRelationalRow;
            } else if (bitmapBit(nulls, ordinal) and !column.allows_null) {
                return error.InvalidRelationalRow;
            }
            if (bitmapBit(present, ordinal) and !bitmapBit(nulls, ordinal)) continue;
            const slot = ordinalPayloadSlice(parsed, table_schema.relational_columns, column, ordinal);
            if (isVariableColumn(column)) {
                if (slot.len != 0) return error.NonCanonicalRelationalRow;
            } else {
                for (slot) |byte| if (byte != 0) return error.NonCanonicalRelationalRow;
            }
        }
    }
    if (canonical and sparse != (try shouldUseSparseRepresentation(parsed, table_schema.relational_columns, fixed_len, variable_count)))
        return error.NonCanonicalRelationalRow;
    return parsed;
}

fn shouldUseSparseRepresentation(
    parsed: ParsedOrdinal,
    columns: []const runtime_schema.RelationalColumn,
    fixed_len: usize,
    variable_count: usize,
) !bool {
    var dense_variable_payload_len: usize = 0;
    var sparse_payload_len: usize = 0;
    if (parsed.capabilities & capability_sparse_slots != 0) {
        for (0..parsed.sparse_entry_count) |entry_index| {
            const ordinal = sparseOrdinalAt(parsed, entry_index);
            if (sparseEntryIsNull(parsed, entry_index)) continue;
            const slot = try sparsePayloadSliceAtIndex(parsed, entry_index);
            if (isVariableColumn(columns[ordinal]))
                dense_variable_payload_len = std.math.add(usize, dense_variable_payload_len, slot.len) catch
                    return error.InvalidRelationalRow;
            sparse_payload_len = std.math.add(usize, sparse_payload_len, slot.len) catch
                return error.InvalidRelationalRow;
        }
    } else {
        for (columns, 0..) |column, ordinal| {
            if (!bitmapBit(parsed.present, ordinal) or bitmapBit(parsed.nulls, ordinal)) continue;
            const slot = ordinalPayloadSlice(parsed, columns, column, ordinal);
            if (isVariableColumn(column)) dense_variable_payload_len = std.math.add(usize, dense_variable_payload_len, slot.len) catch
                return error.InvalidRelationalRow;
            sparse_payload_len = std.math.add(usize, sparse_payload_len, slot.len) catch
                return error.InvalidRelationalRow;
        }
    }
    const dense_offsets_len = if (variable_count == 0) 0 else std.math.mul(usize, variable_count + 1, @sizeOf(u32)) catch return error.InvalidRelationalRow;
    const dense_static_len = std.math.add(usize, fixed_len, dense_offsets_len) catch return error.InvalidRelationalRow;
    const dense_body_len = std.math.add(usize, dense_static_len, dense_variable_payload_len) catch
        return error.InvalidRelationalRow;
    const entry_count = if (parsed.capabilities & capability_sparse_slots != 0)
        parsed.sparse_entry_count
    else
        bitmapPopulation(parsed.present);
    const sparse_directory_len = std.math.add(
        usize,
        @sizeOf(u32) * 2,
        std.math.mul(usize, entry_count, sparse_entry_len) catch return error.InvalidRelationalRow,
    ) catch return error.InvalidRelationalRow;
    const sparse_body_len = std.math.add(usize, sparse_directory_len, sparse_payload_len) catch
        return error.InvalidRelationalRow;
    const bitmap_len = (columns.len + 7) / 8;
    const dense_bitmap_len = std.math.mul(usize, 2, bitmap_len) catch return error.InvalidRelationalRow;
    const dense_storage_len = std.math.add(usize, dense_bitmap_len, dense_body_len) catch
        return error.InvalidRelationalRow;
    return sparse_body_len < dense_storage_len;
}

fn ordinalCell(
    parsed: ParsedOrdinal,
    columns: []const runtime_schema.RelationalColumn,
    column: runtime_schema.RelationalColumn,
    ordinal: usize,
) !Cell {
    const is_null = parsedOrdinalIsNull(parsed, ordinal);
    const payload = ordinalPayloadSlice(parsed, columns, column, ordinal);
    return try ordinalCellFromPayload(column, ordinal, payload, is_null);
}

fn findParsedOrdinalCell(
    parsed: ParsedOrdinal,
    columns: []const runtime_schema.RelationalColumn,
    ordinal: usize,
) !?Cell {
    const column = columns[ordinal];
    if (parsed.capabilities & capability_sparse_slots != 0) {
        const entry_index = sparseEntryIndex(parsed, ordinal) orelse return null;
        const payload = try sparsePayloadSliceAtIndex(parsed, entry_index);
        return try ordinalCellFromPayload(column, ordinal, payload, sparseEntryIsNull(parsed, entry_index));
    }
    if (!bitmapBit(parsed.present, ordinal)) return null;
    const payload = if (parsed.layout != null)
        try ordinalPayloadSliceChecked(parsed, column, ordinal)
    else
        ordinalPayloadSlice(parsed, columns, column, ordinal);
    return try ordinalCellFromPayload(column, ordinal, payload, bitmapBit(parsed.nulls, ordinal));
}

fn ordinalCellFromPayload(
    column: runtime_schema.RelationalColumn,
    ordinal: usize,
    payload: []const u8,
    is_null: bool,
) !Cell {
    const value_type = columnValueType(column.column_type);
    const typed_value = if (is_null)
        zeroValue(value_type)
    else
        try decodeOrdinalPayload(payload, value_type);
    const cell: Cell = .{
        .ordinal = @intCast(ordinal),
        .path = column.path,
        .value_type = value_type,
        .is_json = column.is_json,
        .is_dense_vector = column.column_type == .dense_vector,
        .is_null = is_null,
        .value = typed_value,
    };
    if (!cellValueIsSerializable(cell)) return error.InvalidRelationalRow;
    if (!is_null and value_type == .bytes_val) {
        if (column.column_type == .dense_vector) {
            try validateDenseVectorBytes(cell.value.bytes_val);
        } else if (column.is_json) {
            if (!(try std.json.validate(std.heap.page_allocator, cell.value.bytes_val))) return error.InvalidRelationalRow;
        } else if (!std.unicode.utf8ValidateSlice(cell.value.bytes_val) and
            column.column_type != .blob and column.column_type != .dense_vector)
        {
            return error.InvalidRelationalRow;
        }
    }
    return cell;
}

fn ordinalPayloadSliceChecked(
    parsed: ParsedOrdinal,
    column: runtime_schema.RelationalColumn,
    ordinal: usize,
) ![]const u8 {
    if (parsed.capabilities & capability_sparse_slots != 0)
        return sparsePayloadSlice(parsed, ordinal) orelse return error.InvalidRelationalRow;
    const layout = parsed.layout orelse return error.InvalidRelationalRow;
    if (isVariableColumn(column)) {
        const index: usize = layout.variable_indexes[ordinal];
        const start: usize = @intCast(std.mem.readInt(u32, parsed.value[parsed.offsets_start + index * 4 ..][0..4], .little));
        const end: usize = @intCast(std.mem.readInt(u32, parsed.value[parsed.offsets_start + (index + 1) * 4 ..][0..4], .little));
        if (start > end or end > parsed.payload.len) return error.InvalidRelationalRow;
        return parsed.payload[start..end];
    }
    const start: usize = layout.fixed_offsets[ordinal];
    const width = fixedColumnWidth(column);
    if (start > parsed.fixed.len or width > parsed.fixed.len - start) return error.InvalidRelationalRow;
    return parsed.fixed[start..][0..width];
}

fn ordinalPayloadSlice(
    parsed: ParsedOrdinal,
    columns: []const runtime_schema.RelationalColumn,
    column: runtime_schema.RelationalColumn,
    ordinal: usize,
) []const u8 {
    if (parsed.capabilities & capability_sparse_slots != 0)
        return sparsePayloadSlice(parsed, ordinal) orelse &.{};
    if (isVariableColumn(column)) {
        const index: usize = if (parsed.layout) |layout| layout.variable_indexes[ordinal] else variableIndexBefore(columns, ordinal);
        const start: usize = @intCast(std.mem.readInt(u32, parsed.value[parsed.offsets_start + index * 4 ..][0..4], .little));
        const end: usize = @intCast(std.mem.readInt(u32, parsed.value[parsed.offsets_start + (index + 1) * 4 ..][0..4], .little));
        return parsed.payload[start..end];
    }
    const start: usize = if (parsed.layout) |layout| layout.fixed_offsets[ordinal] else fixedOffsetBefore(columns, ordinal);
    return parsed.fixed[start..][0..fixedColumnWidth(column)];
}

fn sparsePayloadSlice(parsed: ParsedOrdinal, target_ordinal: usize) ?[]const u8 {
    var lower: usize = 0;
    var upper: usize = parsed.sparse_entry_count;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        const entry_pos = parsed.sparse_entries_start + middle * sparse_entry_len;
        const ordinal: usize = std.mem.readInt(u32, parsed.value[entry_pos..][0..4], .little) & sparse_ordinal_mask;
        if (ordinal < target_ordinal) {
            lower = middle + 1;
        } else if (ordinal > target_ordinal) {
            upper = middle;
        } else {
            const start: usize = std.mem.readInt(u32, parsed.value[entry_pos + 4 ..][0..4], .little);
            const end: usize = if (middle + 1 < parsed.sparse_entry_count)
                std.mem.readInt(u32, parsed.value[entry_pos + sparse_entry_len + 4 ..][0..4], .little)
            else
                std.mem.readInt(u32, parsed.value[parsed.sparse_terminal_offset_pos..][0..4], .little);
            if (start > end or end > parsed.payload.len) return null;
            return parsed.payload[start..end];
        }
    }
    return null;
}

fn sparseOrdinalAt(parsed: ParsedOrdinal, entry_index: usize) usize {
    std.debug.assert(entry_index < parsed.sparse_entry_count);
    const entry_pos = parsed.sparse_entries_start + entry_index * sparse_entry_len;
    return std.mem.readInt(u32, parsed.value[entry_pos..][0..4], .little) & sparse_ordinal_mask;
}

fn sparseEntryIsNull(parsed: ParsedOrdinal, entry_index: usize) bool {
    std.debug.assert(entry_index < parsed.sparse_entry_count);
    const entry_pos = parsed.sparse_entries_start + entry_index * sparse_entry_len;
    return std.mem.readInt(u32, parsed.value[entry_pos..][0..4], .little) & sparse_null_flag != 0;
}

fn sparseEntryIndex(parsed: ParsedOrdinal, target_ordinal: usize) ?usize {
    var lower: usize = 0;
    var upper: usize = parsed.sparse_entry_count;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        const ordinal = sparseOrdinalAt(parsed, middle);
        if (ordinal < target_ordinal) {
            lower = middle + 1;
        } else if (ordinal > target_ordinal) {
            upper = middle;
        } else {
            return middle;
        }
    }
    return null;
}

fn parsedHasOrdinal(parsed: ParsedOrdinal, ordinal: usize) bool {
    if (parsed.capabilities & capability_sparse_slots != 0)
        return sparseEntryIndex(parsed, ordinal) != null;
    return bitmapBit(parsed.present, ordinal);
}

fn parsedOrdinalIsNull(parsed: ParsedOrdinal, ordinal: usize) bool {
    if (parsed.capabilities & capability_sparse_slots != 0) {
        const entry_index = sparseEntryIndex(parsed, ordinal) orelse return false;
        return sparseEntryIsNull(parsed, entry_index);
    }
    return bitmapBit(parsed.nulls, ordinal);
}

fn sparsePayloadSliceAtIndex(parsed: ParsedOrdinal, entry_index: usize) ![]const u8 {
    if (entry_index >= parsed.sparse_entry_count) return error.InvalidRelationalRow;
    const entry_pos = parsed.sparse_entries_start + entry_index * sparse_entry_len;
    const start: usize = std.mem.readInt(u32, parsed.value[entry_pos + 4 ..][0..4], .little);
    const end: usize = if (entry_index + 1 < parsed.sparse_entry_count)
        std.mem.readInt(u32, parsed.value[entry_pos + sparse_entry_len + 4 ..][0..4], .little)
    else
        std.mem.readInt(u32, parsed.value[parsed.sparse_terminal_offset_pos..][0..4], .little);
    if (start > end or end > parsed.payload.len) return error.InvalidRelationalRow;
    return parsed.payload[start..end];
}

fn bitmapPopulation(bitmap: []const u8) usize {
    var count: usize = 0;
    for (bitmap) |byte| count += @popCount(byte);
    return count;
}

fn encodeOrdinalFixed(out: []u8, cell: Cell) !void {
    var pos: usize = 0;
    switch (cell.value) {
        .u64_val => |value| writeU64(out, &pos, value),
        .i64_val => |value| writeU64(out, &pos, @bitCast(value)),
        .f64_val => |value| writeU64(out, &pos, @bitCast(value)),
        .bool_val => |value| {
            if (out.len != 1) return error.InvalidRelationalRow;
            out[0] = @intFromBool(value);
            pos = 1;
        },
        .geo_point => |value| {
            writeU64(out, &pos, @bitCast(value.lat));
            writeU64(out, &pos, @bitCast(value.lon));
        },
        .bytes_val, .numeric_val => return error.InvalidRelationalRow,
    }
    if (pos != out.len) return error.InvalidRelationalRow;
}

fn appendOrdinalPayload(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), cell: Cell) !void {
    var fixed: [17]u8 = undefined;
    var pos: usize = 0;
    switch (cell.value) {
        .u64_val => |value| writeU64(&fixed, &pos, value),
        .i64_val => |value| writeU64(&fixed, &pos, @bitCast(value)),
        .f64_val => |value| writeU64(&fixed, &pos, @bitCast(value)),
        .bool_val => |value| {
            fixed[0] = @intFromBool(value);
            pos = 1;
        },
        .geo_point => |value| {
            writeU64(&fixed, &pos, @bitCast(value.lat));
            writeU64(&fixed, &pos, @bitCast(value.lon));
        },
        .numeric_val => |value| switch (value) {
            .u64_val => |number| {
                fixed[0] = 0;
                pos = 1;
                writeU64(&fixed, &pos, number);
            },
            .i64_val => |number| {
                fixed[0] = 1;
                pos = 1;
                writeU64(&fixed, &pos, @bitCast(number));
            },
            .f64_val => |number| {
                fixed[0] = 2;
                pos = 1;
                writeU64(&fixed, &pos, @bitCast(number));
            },
        },
        .bytes_val => |bytes| return try out.appendSlice(alloc, bytes),
    }
    try out.appendSlice(alloc, fixed[0..pos]);
}

fn decodeOrdinalPayload(payload: []const u8, value_type: typed_dv.ValueType) !typed_dv.TypedValue {
    return switch (value_type) {
        .u64_val => if (payload.len == 8) .{ .u64_val = std.mem.readInt(u64, payload[0..8], .little) } else error.InvalidRelationalRow,
        .i64_val => if (payload.len == 8) .{ .i64_val = @bitCast(std.mem.readInt(u64, payload[0..8], .little)) } else error.InvalidRelationalRow,
        .f64_val => if (payload.len == 8) .{ .f64_val = @bitCast(std.mem.readInt(u64, payload[0..8], .little)) } else error.InvalidRelationalRow,
        .bool_val => if (payload.len == 1 and payload[0] <= 1) .{ .bool_val = payload[0] == 1 } else error.InvalidRelationalRow,
        .geo_point => if (payload.len == 16) .{ .geo_point = .{
            .lat = @bitCast(std.mem.readInt(u64, payload[0..8], .little)),
            .lon = @bitCast(std.mem.readInt(u64, payload[8..16], .little)),
        } } else error.InvalidRelationalRow,
        .bytes_val => .{ .bytes_val = payload },
        .numeric_val => if (payload.len != 9) error.InvalidRelationalRow else switch (payload[0]) {
            0 => .{ .numeric_val = .{ .u64_val = std.mem.readInt(u64, payload[1..9], .little) } },
            1 => .{ .numeric_val = .{ .i64_val = @bitCast(std.mem.readInt(u64, payload[1..9], .little)) } },
            2 => .{ .numeric_val = .{ .f64_val = @bitCast(std.mem.readInt(u64, payload[1..9], .little)) } },
            else => error.InvalidRelationalRow,
        },
    };
}

fn columnValueType(column_type: runtime_schema.RelationalColumnType) typed_dv.ValueType {
    return switch (column_type) {
        .datetime => .u64_val,
        .integer => .i64_val,
        .number => .f64_val,
        .boolean => .bool_val,
        .geopoint => .geo_point,
        .string, .blob, .geoshape, .json, .dense_vector => .bytes_val,
    };
}

fn isVariableColumn(column: runtime_schema.RelationalColumn) bool {
    return switch (column.column_type) {
        .string, .blob, .geoshape, .json, .dense_vector => true,
        .datetime, .integer, .number, .boolean, .geopoint => false,
    };
}

fn fixedColumnWidth(column: runtime_schema.RelationalColumn) usize {
    return switch (column.column_type) {
        .datetime, .integer, .number => 8,
        .boolean => 1,
        .geopoint => 16,
        .string, .blob, .geoshape, .json, .dense_vector => 0,
    };
}

fn fixedSectionLen(columns: []const runtime_schema.RelationalColumn) !usize {
    var result: usize = 0;
    for (columns) |column| result = std.math.add(usize, result, fixedColumnWidth(column)) catch
        return error.InvalidRelationalRow;
    return result;
}

fn countVariableColumns(columns: []const runtime_schema.RelationalColumn) usize {
    var count: usize = 0;
    for (columns) |column| count += @intFromBool(isVariableColumn(column));
    return count;
}

fn fixedOffsetBefore(columns: []const runtime_schema.RelationalColumn, ordinal: usize) usize {
    var result: usize = 0;
    for (columns[0..ordinal]) |column| result += fixedColumnWidth(column);
    return result;
}

fn variableIndexBefore(columns: []const runtime_schema.RelationalColumn, ordinal: usize) usize {
    var result: usize = 0;
    for (columns[0..ordinal]) |column| result += @intFromBool(isVariableColumn(column));
    return result;
}

fn setBitmapBit(bitmap: []u8, ordinal: usize) void {
    bitmap[ordinal / 8] |= @as(u8, 1) << @intCast(ordinal % 8);
}

fn bitmapBit(bitmap: []const u8, ordinal: usize) bool {
    return bitmap[ordinal / 8] & (@as(u8, 1) << @intCast(ordinal % 8)) != 0;
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
        .numeric_val => cell.value == .numeric_val,
    };
    return matches;
}

fn cellValueIsSerializable(cell: Cell) bool {
    if (cell.is_null) return true;
    return switch (cell.value) {
        .f64_val => |value| std.math.isFinite(value),
        .geo_point => |value| geo_mod.latitudeIsValid(value.lat) and geo_mod.longitudeIsValid(value.lon),
        .numeric_val => |value| switch (value) {
            .f64_val => |number| std.math.isFinite(number),
            else => true,
        },
        else => true,
    };
}

fn validateCell(alloc: Allocator, cell: Cell) !void {
    if (!std.unicode.utf8ValidateSlice(cell.path) or
        !cellValueMatchesType(cell) or
        !cellValueIsSerializable(cell)) return error.InvalidRelationalRow;
    if (!cell.is_null and cell.value == .bytes_val) {
        if (cell.is_dense_vector) {
            try validateDenseVectorBytes(cell.value.bytes_val);
        } else if (cell.is_json) {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{ .parse_numbers = false }) catch
                return error.InvalidRelationalRow;
            defer parsed.deinit();
            const canonical = try document_content_hash.canonicalJsonValueAlloc(alloc, parsed.value);
            defer alloc.free(canonical);
            if (!std.mem.eql(u8, canonical, cell.value.bytes_val)) return error.NonCanonicalRelationalRow;
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

fn zeroValue(value_type: typed_dv.ValueType) typed_dv.TypedValue {
    return switch (value_type) {
        .u64_val => .{ .u64_val = 0 },
        .i64_val => .{ .i64_val = 0 },
        .f64_val => .{ .f64_val = 0 },
        .bytes_val => .{ .bytes_val = "" },
        .geo_point => .{ .geo_point = .{ .lat = 0, .lon = 0 } },
        .bool_val => .{ .bool_val = false },
        .numeric_val => .{ .numeric_val = .{ .u64_val = 0 } },
    };
}

/// Materialize a document-mode stored value as JSON by returning an owned copy.
/// Relational rows require an immutable schema layout at their read seam; this
/// generic document path intentionally has no AROW fallback.
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
        .numeric_val => switch (cell.value.numeric_val) {
            .u64_val => |number| try appendFmt(alloc, out, "{d}", .{number}),
            .i64_val => |number| try appendFmt(alloc, out, "{d}", .{number}),
            .f64_val => |number| try appendFmt(alloc, out, "{d}", .{number}),
        },
        .bool_val => try out.appendSlice(alloc, if (cell.value.bool_val) "true" else "false"),
        .geo_point => try appendFmt(alloc, out, "{{\"lat\":{d},\"lon\":{d}}}", .{ cell.value.geo_point.lat, cell.value.geo_point.lon }),
        .bytes_val => {
            if (cell.is_dense_vector) {
                try appendDenseVectorJson(alloc, out, cell.value.bytes_val);
            } else if (cell.is_json) {
                // The ordinal parser and appendCellValue validate this before
                // reaching the hot formatting path.
                try out.appendSlice(alloc, cell.value.bytes_val);
            } else {
                try appendJsonString(alloc, out, cell.value.bytes_val);
            }
        },
    }
}

/// Lower a validated logical embedding into AROW's canonical binary payload.
/// Keeping this conversion in the codec gives writers and restore verification
/// one physical definition and lets index rebuilds bypass JSON parsing.
pub fn encodeDenseVectorValueAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    if (value != .array) return error.InvalidBatchRequest;
    const byte_len = std.math.mul(usize, value.array.items.len, @sizeOf(f32)) catch
        return error.InvalidBatchRequest;
    const bytes = try alloc.alloc(u8, byte_len);
    errdefer alloc.free(bytes);
    for (value.array.items, 0..) |item, index| {
        const narrowed = try denseVectorElement(item);
        std.mem.writeInt(u32, bytes[index * 4 ..][0..4], @bitCast(narrowed), .little);
    }
    return bytes;
}

/// Round the API number directly into the canonical vector domain. Going via
/// f64 double-rounds decimal literals near an f32 midpoint.
pub fn denseVectorElement(value: std.json.Value) !f32 {
    const number: f32 = switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| @floatCast(float),
        .number_string => |text| std.fmt.parseFloat(f32, text) catch return error.InvalidBatchRequest,
        else => return error.InvalidBatchRequest,
    };
    if (!std.math.isFinite(number)) return error.InvalidBatchRequest;
    return number;
}

pub fn decodeDenseVectorValueAlloc(alloc: Allocator, bytes: []const u8) ![]f32 {
    try validateDenseVectorBytes(bytes);
    const vector = try alloc.alloc(f32, bytes.len / @sizeOf(f32));
    errdefer alloc.free(vector);
    for (vector, 0..) |*value, index|
        value.* = @bitCast(std.mem.readInt(u32, bytes[index * 4 ..][0..4], .little));
    return vector;
}

fn validateDenseVectorBytes(bytes: []const u8) !void {
    if (bytes.len % @sizeOf(f32) != 0) return error.InvalidRelationalRow;
    var offset: usize = 0;
    while (offset < bytes.len) : (offset += 4) {
        const value: f32 = @bitCast(std.mem.readInt(u32, bytes[offset..][0..4], .little));
        if (!std.math.isFinite(value)) return error.InvalidRelationalRow;
    }
}

fn appendDenseVectorJson(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try validateDenseVectorBytes(bytes);
    try out.append(alloc, '[');
    var offset: usize = 0;
    while (offset < bytes.len) : (offset += 4) {
        if (offset != 0) try out.append(alloc, ',');
        const value: f32 = @bitCast(std.mem.readInt(u32, bytes[offset..][0..4], .little));
        try appendFmt(alloc, out, "{d}", .{value});
    }
    try out.append(alloc, ']');
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

fn readU32(data: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return val;
}

fn readU64(data: []const u8, pos: *usize) u64 {
    const val = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

test "ordinal rows bind layout support projection checksum and canonical bytes" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "name", .path = "name", .column_type = .string, .required = true },
        .{ .name = "count", .path = "count", .column_type = .integer, .required = true },
        .{ .name = "active", .path = "active", .column_type = .boolean, .allows_null = true },
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true },
        .{ .name = "note", .path = "note", .column_type = .string },
    };
    const schema = runtime_schema.TableSchema{
        .version = 7,
        .storage_mode = .relational,
        .relational_columns = &columns,
    };
    const cells = [_]Cell{
        .{ .ordinal = 0, .path = "name", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
        .{ .ordinal = 1, .path = "count", .value_type = .i64_val, .value = .{ .i64_val = -42 } },
        .{ .ordinal = 2, .path = "active", .value_type = .bool_val, .is_null = true, .value = .{ .bool_val = false } },
        .{ .ordinal = 3, .path = "payload", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = "{\"x\":1}" } },
    };

    const semantic_hash = [_]u8{0x5a} ** semantic_hash_len;
    const encoded = try serializeOrdinal(alloc, schema.version, &columns, &cells, semantic_hash);
    defer alloc.free(encoded);
    try std.testing.expectEqual(@as(usize, 99), encoded.len);
    const stored_semantic_hash = try rowSemanticHash(encoded);
    try std.testing.expectEqualSlices(u8, &semantic_hash, &stored_semantic_hash);
    try std.testing.expectEqual(@as(u64, 0), try rowWriteTimestampNs(encoded));
    try setOrdinalWriteTimestampNs(encoded, 123_456);
    try std.testing.expectEqual(@as(u64, 123_456), try rowWriteTimestampNs(encoded));
    const timestamped_semantic_hash = try rowSemanticHash(encoded);
    try std.testing.expectEqualSlices(u8, &semantic_hash, &timestamped_semantic_hash);
    var physical_layout = try PhysicalLayout.init(alloc, schema);
    defer physical_layout.deinit();
    const trusted_projected = (try findCellByOrdinalTrusted(encoded, schema, &physical_layout, 1)).?;
    try std.testing.expectEqual(@as(i64, -42), trusted_projected.value.i64_val);
    try std.testing.expectEqual(@as(u32, 7), try rowSchemaVersion(encoded));
    try validateOrdinal(encoded, schema);
    const projected = (try findCellByOrdinal(encoded, schema, 1)).?;
    try std.testing.expectEqual(@as(i64, -42), projected.value.i64_val);
    try std.testing.expect((try findCellByOrdinal(encoded, schema, 4)) == null);
    const projected_json = try projectOrdinalFieldsWithLayoutAlloc(
        alloc,
        encoded,
        schema,
        &physical_layout,
        &.{ "payload", "name" },
    );
    defer alloc.free(projected_json);
    try std.testing.expectEqualStrings("{\"payload\":{\"x\":1},\"name\":\"alpha\"}", projected_json);
    const trusted_projected_json = try projectOrdinalFieldsTrustedWithLayoutAlloc(
        alloc,
        encoded,
        schema,
        &physical_layout,
        &.{ "payload", "name" },
    );
    defer alloc.free(trusted_projected_json);
    try std.testing.expectEqualStrings(projected_json, trusted_projected_json);

    const json = try reconstructOrdinalValueAlloc(alloc, encoded, schema);
    defer alloc.free(json);
    try std.testing.expectEqualStrings("{\"name\":\"alpha\",\"count\":-42,\"active\":null,\"payload\":{\"x\":1}}", json);

    var wrong_schema = schema;
    wrong_schema.version = 8;
    try std.testing.expectError(error.RelationalRowSchemaMismatch, validateOrdinal(encoded, wrong_schema));

    const corrupt = try alloc.dupe(u8, encoded);
    defer alloc.free(corrupt);
    corrupt[corrupt.len - checksum_len - 1] ^= 1;
    try std.testing.expectError(error.RelationalRowChecksumMismatch, validateOrdinal(corrupt, schema));
    try std.testing.expectError(error.RelationalRowChecksumMismatch, rowSemanticHash(corrupt));

    const unsupported = try alloc.dupe(u8, encoded);
    defer alloc.free(unsupported);
    std.mem.writeInt(u32, unsupported[12..16], 1 << 31, .little);
    const unsupported_checksum = std.hash.Crc32.hash(unsupported[0 .. unsupported.len - checksum_len]);
    std.mem.writeInt(u32, unsupported[unsupported.len - checksum_len ..][0..checksum_len], unsupported_checksum, .little);
    try std.testing.expectError(error.UnsupportedRelationalRowVersion, rowSemanticHash(unsupported));
    try std.testing.expectError(error.UnsupportedRelationalRowVersion, validateOrdinal(unsupported, schema));

    // A null fixed-width slot must remain zero even under a valid checksum.
    const noncanonical = try alloc.dupe(u8, encoded);
    defer alloc.free(noncanonical);
    const fixed_start = ordinal_header_len + 2; // two one-byte bitmaps
    noncanonical[fixed_start + 8] = 1; // boolean slot after the i64 slot
    const checksum = std.hash.Crc32.hash(noncanonical[0 .. noncanonical.len - checksum_len]);
    std.mem.writeInt(u32, noncanonical[noncanonical.len - checksum_len ..][0..checksum_len], checksum, .little);
    try std.testing.expectError(error.NonCanonicalRelationalRow, validateOrdinal(noncanonical, schema));
}

test "ordinal rows use sparse slots for wide optional schemas" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "c0", .path = "c0", .column_type = .integer },
        .{ .name = "c1", .path = "c1", .column_type = .integer },
        .{ .name = "c2", .path = "c2", .column_type = .integer },
        .{ .name = "c3", .path = "c3", .column_type = .integer },
        .{ .name = "c4", .path = "c4", .column_type = .integer },
        .{ .name = "c5", .path = "c5", .column_type = .integer },
        .{ .name = "c6", .path = "c6", .column_type = .integer },
        .{ .name = "c7", .path = "c7", .column_type = .integer },
        .{ .name = "c8", .path = "c8", .column_type = .integer },
        .{ .name = "c9", .path = "c9", .column_type = .integer },
        .{ .name = "c10", .path = "c10", .column_type = .integer },
        .{ .name = "c11", .path = "c11", .column_type = .integer },
        .{ .name = "c12", .path = "c12", .column_type = .integer },
        .{ .name = "c13", .path = "c13", .column_type = .integer },
        .{ .name = "c14", .path = "c14", .column_type = .integer },
        .{ .name = "c15", .path = "c15", .column_type = .integer, .allows_null = true },
    };
    const schema: runtime_schema.TableSchema = .{
        .version = 9,
        .storage_mode = .relational,
        .relational_columns = &columns,
    };
    const cells = [_]Cell{
        .{ .ordinal = 15, .path = "c15", .value_type = .i64_val, .value = .{ .i64_val = 42 } },
    };
    const encoded = try serializeOrdinal(alloc, schema.version, &columns, &cells, [_]u8{0x33} ** semantic_hash_len);
    defer alloc.free(encoded);
    try std.testing.expectEqual(capability_sparse_slots, std.mem.readInt(u32, encoded[12..16], .little));
    try std.testing.expectEqual(
        ordinal_header_len + @sizeOf(u32) + sparse_entry_len + @sizeOf(u32) + @sizeOf(i64) + checksum_len,
        encoded.len,
    );
    try validateOrdinal(encoded, schema);
    try std.testing.expect((try findCellByOrdinal(encoded, schema, 0)) == null);
    try std.testing.expectEqual(@as(i64, 42), (try findCellByOrdinal(encoded, schema, 15)).?.value.i64_val);

    const null_cells = [_]Cell{
        .{ .ordinal = 15, .path = "c15", .value_type = .i64_val, .is_null = true, .value = .{ .i64_val = 0 } },
    };
    const encoded_null = try serializeOrdinal(alloc, schema.version, &columns, &null_cells, [_]u8{0x44} ** semantic_hash_len);
    defer alloc.free(encoded_null);
    try std.testing.expectEqual(
        ordinal_header_len + @sizeOf(u32) + sparse_entry_len + @sizeOf(u32) + checksum_len,
        encoded_null.len,
    );
    const decoded_null = (try findCellByOrdinal(encoded_null, schema, 15)).?;
    try std.testing.expect(decoded_null.is_null);
}

test "ordinal root materialization preserves exact nested JSON numbers" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true },
    };
    const schema: runtime_schema.TableSchema = .{
        .version = 10,
        .storage_mode = .relational,
        .relational_columns = &columns,
    };
    const cells = [_]Cell{.{
        .ordinal = 0,
        .path = "payload",
        .value_type = .bytes_val,
        .is_json = true,
        .value = .{ .bytes_val = "{\"exact\":9007199254740993}" },
    }};
    const encoded = try serializeOrdinal(alloc, schema.version, &columns, &cells, [_]u8{0x41} ** semantic_hash_len);
    defer alloc.free(encoded);
    var layout = try PhysicalLayout.init(alloc, schema);
    defer layout.deinit();
    var materialized = try materializeOrdinalRootWithLayoutAlloc(alloc, encoded, schema, &layout);
    defer materialized.deinit(alloc);
    const payload = materialized.root.object.get("payload").?;
    try std.testing.expectEqualStrings(
        "9007199254740993",
        payload.object.get("exact").?.number_string,
    );
}

test "ordinal rows store dense vectors as canonical binary values" {
    const alloc = std.testing.allocator;
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "embedding", .path = "embedding", .column_type = .dense_vector },
        .{ .name = "name", .path = "name", .column_type = .string },
    };
    const schema: runtime_schema.TableSchema = .{
        .version = 11,
        .storage_mode = .relational,
        .relational_columns = &columns,
    };
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "[1.5,-2,0.25]", .{});
    defer parsed.deinit();
    const vector_bytes = try encodeDenseVectorValueAlloc(alloc, parsed.value);
    defer alloc.free(vector_bytes);
    const cells = [_]Cell{
        .{
            .ordinal = 0,
            .path = "embedding",
            .value_type = .bytes_val,
            .is_dense_vector = true,
            .value = .{ .bytes_val = vector_bytes },
        },
        .{ .ordinal = 1, .path = "name", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    };

    const encoded = try serializeOrdinal(alloc, schema.version, &columns, &cells, [_]u8{0x44} ** semantic_hash_len);
    defer alloc.free(encoded);
    const projected = (try findCellByOrdinal(encoded, schema, 0)).?;
    try std.testing.expect(projected.is_dense_vector);
    try std.testing.expect(!projected.is_json);
    try std.testing.expectEqualSlices(u8, vector_bytes, projected.value.bytes_val);
    const vector = try decodeDenseVectorValueAlloc(alloc, projected.value.bytes_val);
    defer alloc.free(vector);
    try std.testing.expectEqualSlices(f32, &.{ 1.5, -2, 0.25 }, vector);

    const reconstructed = try reconstructOrdinalValueAlloc(alloc, encoded, schema);
    defer alloc.free(reconstructed);
    try std.testing.expectEqualStrings("{\"embedding\":[1.5,-2,0.25],\"name\":\"alpha\"}", reconstructed);

    var layout = try PhysicalLayout.init(alloc, schema);
    defer layout.deinit();
    var materialized = try materializeOrdinalDocumentWithLayoutAlloc(alloc, encoded, schema, &layout);
    defer materialized.deinit(alloc);
    try std.testing.expectEqualStrings(reconstructed, materialized.json);
    try std.testing.expect(materialized.retainedBytes() > materialized.json.len);
    const materialized_vector = materialized.root.object.get("embedding") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), materialized_vector.array.items.len);
    try std.testing.expectEqual(@as(f64, -2), materialized_vector.array.items[1].float);
    try std.testing.expectEqualStrings("alpha", materialized.root.object.get("name").?.string);

    const non_finite = [_]u8{ 0, 0, 0x80, 0x7f };
    const invalid_cells = [_]Cell{.{
        .ordinal = 0,
        .path = "embedding",
        .value_type = .bytes_val,
        .is_dense_vector = true,
        .value = .{ .bytes_val = &non_finite },
    }};
    try std.testing.expectError(
        error.InvalidRelationalRow,
        serializeOrdinal(alloc, schema.version, &columns, &invalid_cells, [_]u8{0x55} ** semantic_hash_len),
    );
}
