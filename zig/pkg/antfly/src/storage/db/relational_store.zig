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
const internal_keys = @import("../internal_keys.zig");
const schema = @import("../schema.zig");

const Allocator = std.mem.Allocator;

/// The narrow storage boundary for relational base rows. Keeping key and value
/// selection here prevents document callers from accidentally falling back to
/// the legacy JSON keyspace or treating packed rows as JSON.
pub fn keyAlloc(alloc: Allocator, document_key: []const u8) ![]u8 {
    return try internal_keys.relationalRowKeyAlloc(alloc, document_key);
}

pub fn encodeValueAlloc(
    alloc: Allocator,
    document_json: []const u8,
    columns: []const schema.RelationalColumn,
) ![]u8 {
    return try document_mapper.buildRelationalRowValueAlloc(alloc, document_json, columns);
}

pub fn decodeValueAlloc(alloc: Allocator, packed_row: []const u8) ![]u8 {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    return try document_mapper.materializeRelationalRowValueAlloc(alloc, packed_row);
}

pub fn validateValue(packed_row: []const u8) !void {
    if (!document_mapper.isRelationalRowValue(packed_row)) return error.InvalidFormat;
    try document_mapper.validateRelationalRowValue(packed_row);
}

/// Materialize a logical document encountered during a mixed internal-key scan.
/// The keyspace, rather than value sniffing, selects strict packed-row decoding.
pub fn materializeStoredValueAlloc(
    alloc: Allocator,
    store_key: []const u8,
    stored_value: []const u8,
) ![]u8 {
    if (internal_keys.isRelationalRowKey(store_key)) return try decodeValueAlloc(alloc, stored_value);
    return try alloc.dupe(u8, stored_value);
}

/// Materialize an owned store value without copying document-mode metadata or
/// artifact payloads. Only the relational-row keyspace selects packed decoding.
pub fn materializeOwnedStoredValueAlloc(
    alloc: Allocator,
    store_key: []const u8,
    stored_value: []u8,
) ![]u8 {
    if (!internal_keys.isRelationalRowKey(store_key)) return stored_value;
    defer alloc.free(stored_value);
    return try decodeValueAlloc(alloc, stored_value);
}

test "relational store facade round trips an authoritative row" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
        .{ .name = "count", .path = "count", .column_type = .integer, .required = true },
    };
    const encoded = try encodeValueAlloc(alloc, "{\"id\":\"a\",\"count\":9007199254740993}", &columns);
    defer alloc.free(encoded);
    const decoded = try decodeValueAlloc(alloc, encoded);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"count\":9007199254740993}", decoded);

    const key = try keyAlloc(alloc, "row:a");
    defer alloc.free(key);
    try std.testing.expect(internal_keys.isRelationalRowKey(key));
}
