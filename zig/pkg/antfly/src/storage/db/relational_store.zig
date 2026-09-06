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
const document_mapper = @import("document_mapper.zig");
const document_content_hash = @import("document_content_hash.zig");
const internal_keys = @import("../internal_keys.zig");
const schema = @import("../schema.zig");
const row_codec = @import("algebraic/relational_row_codec.zig");

const Allocator = std.mem.Allocator;

/// The narrow storage boundary for relational base rows. Keeping key and value
/// selection here prevents document callers from accidentally falling back to
/// the legacy JSON keyspace or treating packed rows as JSON.
pub fn keyAlloc(alloc: Allocator, document_key: []const u8) ![]u8 {
    return try internal_keys.relationalRowKeyAlloc(alloc, document_key);
}

pub fn encodeValueForSchemaAlloc(
    alloc: Allocator,
    document_json: []const u8,
    table_schema: schema.TableSchema,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, document_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    return try document_mapper.buildRelationalRowValueForSchemaFromParsedAlloc(alloc, parsed.value, table_schema);
}

/// Encode an already-parsed logical row. Restore and prepared-write callers
/// use this to avoid a second JSON parse after public-schema validation.
pub fn encodeParsedValueForSchemaAlloc(
    alloc: Allocator,
    parsed_value: std.json.Value,
    table_schema: schema.TableSchema,
) ![]u8 {
    return try document_mapper.buildRelationalRowValueForSchemaFromParsedAlloc(alloc, parsed_value, table_schema);
}

pub fn decodeValueForSchemaAlloc(
    alloc: Allocator,
    packed_row: []const u8,
    table_schema: schema.TableSchema,
) ![]u8 {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    return try document_mapper.materializeRelationalRowValueForSchemaAlloc(alloc, packed_row, table_schema);
}

pub fn decodeValueForSchemaAndLayoutAlloc(
    alloc: Allocator,
    packed_row: []const u8,
    table_schema: schema.TableSchema,
    layout: *const row_codec.PhysicalLayout,
) ![]u8 {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    _ = try row_codec.rowSchemaVersion(packed_row);
    return try row_codec.reconstructOrdinalValueWithLayoutAlloc(alloc, packed_row, table_schema, layout);
}

pub fn materializeDocumentForSchemaAndLayoutAlloc(
    alloc: Allocator,
    packed_row: []const u8,
    table_schema: schema.TableSchema,
    layout: *const row_codec.PhysicalLayout,
) !row_codec.MaterializedOrdinalDocument {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    _ = try row_codec.rowSchemaVersion(packed_row);
    return try row_codec.materializeOrdinalDocumentWithLayoutAlloc(alloc, packed_row, table_schema, layout);
}

pub fn materializeRootForSchemaAndLayoutAlloc(
    alloc: Allocator,
    packed_row: []const u8,
    table_schema: schema.TableSchema,
    layout: *const row_codec.PhysicalLayout,
) !row_codec.MaterializedOrdinalRoot {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    _ = try row_codec.rowSchemaVersion(packed_row);
    return try row_codec.materializeOrdinalRootWithLayoutAlloc(alloc, packed_row, table_schema, layout);
}

pub fn validateCanonicalAndMaterializeRootForSchemaAndLayoutAlloc(
    alloc: Allocator,
    packed_row: []const u8,
    table_schema: schema.TableSchema,
    layout: *const row_codec.PhysicalLayout,
) !row_codec.MaterializedOrdinalRoot {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    _ = try row_codec.rowSchemaVersion(packed_row);
    return try row_codec.validateCanonicalAndMaterializeOrdinalRootWithLayoutAlloc(
        alloc,
        packed_row,
        table_schema,
        layout,
    );
}

pub fn rowSchemaVersion(packed_row: []const u8) !u32 {
    return try document_mapper.relationalRowSchemaVersion(packed_row);
}

pub fn rowSemanticHash(packed_row: []const u8) ![std.crypto.hash.Blake3.digest_length]u8 {
    return try row_codec.rowSemanticHash(packed_row);
}

pub fn rowSemanticHashTrusted(packed_row: []const u8) ![std.crypto.hash.Blake3.digest_length]u8 {
    return try row_codec.rowSemanticHashTrusted(packed_row);
}

pub fn rowWriteTimestampNs(packed_row: []const u8) !u64 {
    return try row_codec.rowWriteTimestampNs(packed_row);
}

pub fn rowWriteTimestampNsTrusted(packed_row: []const u8) !u64 {
    return try row_codec.rowWriteTimestampNsTrusted(packed_row);
}

pub fn validateValueForSchema(packed_row: []const u8, table_schema: schema.TableSchema) !void {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    try document_mapper.validateRelationalRowValueForSchema(packed_row, table_schema);
}

pub fn validateValueForSchemaAndLayout(
    packed_row: []const u8,
    table_schema: schema.TableSchema,
    layout: *const row_codec.PhysicalLayout,
) !void {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    _ = try row_codec.rowSchemaVersion(packed_row);
    try row_codec.validateOrdinalWithLayout(packed_row, table_schema, layout);
}

/// Validate AROW v2 directly from typed ordinal cells, including its semantic
/// digest and canonical nested JSON representation. This is the restore/scrub
/// boundary and deliberately avoids a whole-document JSON reconstruction.
pub fn validateCanonicalValueForSchemaAndLayout(
    alloc: Allocator,
    packed_row: []const u8,
    table_schema: schema.TableSchema,
    layout: *const row_codec.PhysicalLayout,
) !void {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    _ = try row_codec.rowSchemaVersion(packed_row);
    var iterator = try row_codec.canonicalOrdinalCellIteratorWithLayout(packed_row, table_schema, layout);
    const stored_hash = iterator.semanticHash();
    if (iterator.isSparse()) {
        const cells = try alloc.alloc(row_codec.Cell, iterator.presentCount());
        defer alloc.free(cells);
        var count: usize = 0;
        while (try iterator.next()) |cell| : (count += 1) cells[count] = cell;
        std.debug.assert(count == cells.len);
        for (cells) |cell| {
            if (!cell.is_json or cell.is_null) continue;
            try validateCanonicalJsonCell(alloc, cell.value.bytes_val);
        }
        std.mem.sort(row_codec.Cell, cells, table_schema.relational_columns, struct {
            fn lessThan(columns: []const schema.RelationalColumn, lhs: row_codec.Cell, rhs: row_codec.Cell) bool {
                return std.mem.lessThan(u8, columns[lhs.ordinal].name, columns[rhs.ordinal].name);
            }
        }.lessThan);
        const computed_hash = try document_content_hash.hashRelationalSparseCellsCanonical(cells, table_schema);
        if (!std.mem.eql(u8, &stored_hash, &computed_hash)) return error.RelationalRowSemanticHashMismatch;
        return;
    }

    const cells = try alloc.alloc(?row_codec.Cell, table_schema.relational_columns.len);
    defer alloc.free(cells);
    @memset(cells, null);
    while (try iterator.next()) |cell| cells[cell.ordinal] = cell;
    // The cell hash intentionally consumes canonical JSON bytes directly on
    // this hot validation path. Establish that physical invariant before
    // comparing the logical digest so equivalent-but-noncanonical JSON is
    // diagnosed as a physical representation error, not content corruption.
    for (cells) |maybe_cell| {
        const cell = maybe_cell orelse continue;
        if (!cell.is_json or cell.is_null) continue;
        try validateCanonicalJsonCell(alloc, cell.value.bytes_val);
    }
    const computed_hash = try document_content_hash.hashRelationalCellsWithOrdinals(
        alloc,
        cells,
        table_schema,
        layout.hash_ordinals,
    );
    if (!std.mem.eql(u8, &stored_hash, &computed_hash)) return error.RelationalRowSemanticHashMismatch;
}

fn validateCanonicalJsonCell(alloc: Allocator, encoded: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{ .parse_numbers = false });
    defer parsed.deinit();
    const canonical = try document_content_hash.canonicalJsonValueAlloc(alloc, parsed.value);
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, encoded)) return error.NonCanonicalRelationalRow;
}

/// Materialize a logical document encountered during a mixed internal-key scan.
/// The keyspace, rather than value sniffing, selects strict packed-row decoding.
pub fn materializeStoredValueAlloc(
    alloc: Allocator,
    store_key: []const u8,
    stored_value: []const u8,
) ![]u8 {
    if (internal_keys.isRelationalRowKey(store_key)) return error.RelationalRowSchemaMismatch;
    return try alloc.dupe(u8, stored_value);
}

/// Materialize an owned store value without copying document-mode metadata or
/// artifact payloads. Only the relational-row keyspace selects packed decoding.
pub fn materializeOwnedStoredValueAlloc(
    alloc: Allocator,
    store_key: []const u8,
    stored_value: []u8,
) ![]u8 {
    if (internal_keys.isRelationalRowKey(store_key)) {
        alloc.free(stored_value);
        return error.RelationalRowSchemaMismatch;
    }
    return stored_value;
}

test "relational store facade round trips an authoritative row" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
        .{ .name = "count", .path = "count", .column_type = .integer, .required = true },
    };
    const table_schema: schema.TableSchema = .{
        .version = 1,
        .storage_mode = .relational,
        .relational_columns = &columns,
    };
    const encoded = try encodeValueForSchemaAlloc(alloc, "{\"id\":\"a\",\"count\":9007199254740993}", table_schema);
    defer alloc.free(encoded);
    try std.testing.expectEqual(@as(u32, 1), try rowSchemaVersion(encoded));
    const decoded = try decodeValueForSchemaAlloc(alloc, encoded, table_schema);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"count\":9007199254740993}", decoded);

    const key = try keyAlloc(alloc, "row:a");
    defer alloc.free(key);
    try std.testing.expect(internal_keys.isRelationalRowKey(key));
    try std.testing.expectError(error.RelationalRowSchemaMismatch, materializeStoredValueAlloc(alloc, key, encoded));
    const owned = try alloc.dupe(u8, encoded);
    try std.testing.expectError(error.RelationalRowSchemaMismatch, materializeOwnedStoredValueAlloc(alloc, key, owned));
}

test "restore validation binds semantic hash and canonical nested JSON" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .object },
    };
    const table_schema: schema.TableSchema = .{
        .version = 11,
        .storage_mode = .relational,
        .relational_columns = &columns,
    };
    var layout = try row_codec.PhysicalLayout.init(alloc, table_schema);
    defer layout.deinit();

    const encoded = try encodeValueForSchemaAlloc(
        alloc,
        "{\"payload\":{\"b\":2,\"a\":1},\"id\":\"row\"}",
        table_schema,
    );
    defer alloc.free(encoded);
    try validateCanonicalValueForSchemaAndLayout(alloc, encoded, table_schema, &layout);

    const wrong_hash = try alloc.dupe(u8, encoded);
    defer alloc.free(wrong_hash);
    try row_codec.setOrdinalSemanticHash(wrong_hash, [_]u8{0x7c} ** std.crypto.hash.Blake3.digest_length);
    try std.testing.expectError(
        error.RelationalRowSemanticHashMismatch,
        validateCanonicalValueForSchemaAndLayout(alloc, wrong_hash, table_schema, &layout),
    );

    const noncanonical = try alloc.dupe(u8, encoded);
    defer alloc.free(noncanonical);
    const canonical_json = "{\"a\":1,\"b\":2}";
    const json_start = std.mem.indexOf(u8, noncanonical, canonical_json) orelse return error.TestExpectedEqual;
    @memcpy(noncanonical[json_start..][0..canonical_json.len], "{\"b\":2,\"a\":1}");
    try row_codec.setOrdinalSemanticHash(noncanonical, try row_codec.rowSemanticHash(encoded));
    try std.testing.expectError(
        error.NonCanonicalRelationalRow,
        validateCanonicalValueForSchemaAndLayout(alloc, noncanonical, table_schema, &layout),
    );
}
