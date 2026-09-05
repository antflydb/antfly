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
const schema_api = @import("../../schema/mod.zig");
const runtime_schema = @import("../schema.zig");

pub const Digest = [std.crypto.hash.Blake3.digest_length]u8;

/// Hash the user-visible content of a JSON document. Object field order does
/// not affect the digest, while array order and JSON scalar kinds do. Antfly's
/// injected identity and timestamp fields are excluded at every object level to
/// match linear-merge comparison semantics.
pub fn hashJson(alloc: std.mem.Allocator, raw: []const u8) !Digest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    return try hashParsedValue(alloc, parsed.value);
}

pub fn hashParsedValue(alloc: std.mem.Allocator, value: std.json.Value) !Digest {
    var hasher = std.crypto.hash.Blake3.init(.{});
    try hashValue(alloc, &hasher, value);
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

/// Hash a relational document after applying the schema's logical coercions.
/// This operates on the request's existing parse tree, so `1`, `1.0`, and a
/// future physical row encoding produce the same digest when the declared
/// column type gives them the same logical value.
pub fn hashRelationalParsedValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    table_schema: runtime_schema.TableSchema,
) !Digest {
    if (value != .object) return error.InvalidBatchRequest;
    const ordinals = try alloc.alloc(u32, table_schema.relational_columns.len);
    defer alloc.free(ordinals);
    for (ordinals, 0..) |*ordinal, index| ordinal.* = @intCast(index);
    std.mem.sort(u32, ordinals, table_schema.relational_columns, struct {
        fn lessThan(columns: []const runtime_schema.RelationalColumn, lhs: u32, rhs: u32) bool {
            return std.mem.lessThan(u8, columns[lhs].name, columns[rhs].name);
        }
    }.lessThan);

    return try hashRelationalParsedValueWithOrdinals(alloc, value, table_schema, ordinals);
}

/// Allocation-free schema-ordering variant used by immutable schema epochs.
pub fn hashRelationalParsedValueWithOrdinals(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    table_schema: runtime_schema.TableSchema,
    hash_ordinals: []const u32,
) !Digest {
    if (value != .object or hash_ordinals.len != table_schema.relational_columns.len)
        return error.InvalidBatchRequest;
    var count: usize = 0;
    for (hash_ordinals) |ordinal| {
        if (ordinal >= table_schema.relational_columns.len) return error.InvalidBatchRequest;
        const column = table_schema.relational_columns[ordinal];
        if (!isIgnoredSystemField(column.name) and value.object.get(column.name) != null) count += 1;
    }

    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("o");
    hashInt(&hasher, u64, @intCast(count));
    for (hash_ordinals) |ordinal| {
        const column = table_schema.relational_columns[ordinal];
        if (isIgnoredSystemField(column.name) or value.object.get(column.name) == null) continue;
        hashBytes(&hasher, column.name);
        try hashRelationalColumnValue(alloc, &hasher, value.object.get(column.name).?, column);
    }
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

/// Hash values already resolved to schema ordinals by PreparedRow. This keeps
/// logical hashing independent of the physical codec without repeating object
/// lookups or field discovery after validation/encoding preparation.
pub fn hashRelationalPreparedValuesWithOrdinals(
    alloc: std.mem.Allocator,
    values: []const ?std.json.Value,
    table_schema: runtime_schema.TableSchema,
    hash_ordinals: []const u32,
) !Digest {
    return try hashRelationalPreparedValuesCanonicalWithOrdinals(
        alloc,
        values,
        null,
        table_schema,
        hash_ordinals,
    );
}

/// Prepared-row variant that consumes the canonical JSON bytes produced by
/// physical encoding. JSON subdocuments are therefore sorted and serialized
/// once, rather than walking and sorting the same tree again for hashing.
pub fn hashRelationalPreparedValuesCanonicalWithOrdinals(
    alloc: std.mem.Allocator,
    values: []const ?std.json.Value,
    canonical_json_values: ?[]const ?[]const u8,
    table_schema: runtime_schema.TableSchema,
    hash_ordinals: []const u32,
) !Digest {
    if (values.len != table_schema.relational_columns.len or
        hash_ordinals.len != table_schema.relational_columns.len or
        (canonical_json_values != null and canonical_json_values.?.len != values.len))
        return error.InvalidBatchRequest;

    var count: usize = 0;
    for (hash_ordinals) |ordinal| {
        if (ordinal >= values.len) return error.InvalidBatchRequest;
        const column = table_schema.relational_columns[ordinal];
        if (!isIgnoredSystemField(column.name) and values[ordinal] != null) count += 1;
    }

    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("o");
    hashInt(&hasher, u64, @intCast(count));
    for (hash_ordinals) |ordinal| {
        const value = values[ordinal] orelse continue;
        const column = table_schema.relational_columns[ordinal];
        if (isIgnoredSystemField(column.name)) continue;
        hashBytes(&hasher, column.name);
        if (column.is_json or column.column_type == .json) {
            if (canonical_json_values) |canonical_values| {
                const canonical = canonical_values[ordinal] orelse return error.InvalidBatchRequest;
                hashCanonicalJsonBytes(&hasher, canonical);
            } else {
                try hashRelationalColumnValue(alloc, &hasher, value, column);
            }
        } else {
            try hashRelationalColumnValue(alloc, &hasher, value, column);
        }
    }
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

/// Hash schema-ordinal codec cells without reconstructing a JSON document.
/// The cell type is intentionally generic to keep the logical hash layer from
/// depending on the physical row codec.
pub fn hashRelationalCellsWithOrdinals(
    _: std.mem.Allocator,
    cells: anytype,
    table_schema: runtime_schema.TableSchema,
    hash_ordinals: []const u32,
) !Digest {
    if (cells.len != table_schema.relational_columns.len or
        hash_ordinals.len != table_schema.relational_columns.len)
        return error.InvalidBatchRequest;
    var count: usize = 0;
    for (hash_ordinals) |ordinal| {
        if (ordinal >= cells.len) return error.InvalidBatchRequest;
        if (!isIgnoredSystemField(table_schema.relational_columns[ordinal].name) and cells[ordinal] != null) count += 1;
    }

    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("o");
    hashInt(&hasher, u64, @intCast(count));
    for (hash_ordinals) |ordinal| {
        const cell = cells[ordinal] orelse continue;
        const column = table_schema.relational_columns[ordinal];
        if (isIgnoredSystemField(column.name)) continue;
        hashBytes(&hasher, column.name);
        if (cell.is_null) {
            hasher.update("n");
            continue;
        }
        if (column.is_json or column.column_type == .json) {
            hashJsonCellBytes(&hasher, cell.value.bytes_val);
            continue;
        }
        switch (column.column_type) {
            .string, .blob, .geoshape => {
                hasher.update("s");
                hashBytes(&hasher, cell.value.bytes_val);
            },
            .boolean => hasher.update(if (cell.value.bool_val) "b1" else "b0"),
            .integer => {
                hasher.update("i");
                hashInt(&hasher, i64, cell.value.i64_val);
            },
            .number => {
                hasher.update("f");
                const number = cell.value.f64_val;
                hashInt(&hasher, u64, if (number == 0) 0 else @bitCast(number));
            },
            .datetime => {
                hasher.update("t");
                hashInt(&hasher, u64, cell.value.u64_val);
            },
            .geopoint => {
                hasher.update("g");
                const point = cell.value.geo_point;
                hashInt(&hasher, u64, if (point.lat == 0) 0 else @bitCast(point.lat));
                hashInt(&hasher, u64, if (point.lon == 0) 0 else @bitCast(point.lon));
            },
            .json => unreachable,
        }
    }
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

fn hashJsonCellBytes(hasher: *std.crypto.hash.Blake3, encoded: []const u8) void {
    hashCanonicalJsonBytes(hasher, encoded);
}

fn hashCanonicalJsonBytes(hasher: *std.crypto.hash.Blake3, encoded: []const u8) void {
    hasher.update("j");
    hashBytes(hasher, encoded);
}

pub fn canonicalJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try writeCanonicalJsonValue(alloc, &writer.writer, value);
    return try writer.toOwnedSlice();
}

fn writeCanonicalJsonValue(alloc: std.mem.Allocator, writer: *std.Io.Writer, value: std.json.Value) !void {
    switch (value) {
        .array => |array| {
            try writer.writeByte('[');
            for (array.items, 0..) |item, index| {
                if (index != 0) try writer.writeByte(',');
                try writeCanonicalJsonValue(alloc, writer, item);
            }
            try writer.writeByte(']');
        },
        .object => |object| {
            const keys = try alloc.alloc([]const u8, object.count());
            defer alloc.free(keys);
            var index: usize = 0;
            var iterator = object.iterator();
            while (iterator.next()) |entry| : (index += 1) keys[index] = entry.key_ptr.*;
            std.mem.sort([]const u8, keys, {}, lessThanString);
            try writer.writeByte('{');
            for (keys, 0..) |key, key_index| {
                if (key_index != 0) try writer.writeByte(',');
                try std.json.Stringify.value(key, .{}, writer);
                try writer.writeByte(':');
                try writeCanonicalJsonValue(alloc, writer, object.get(key).?);
            }
            try writer.writeByte('}');
        },
        else => try std.json.Stringify.value(value, .{}, writer),
    }
}

pub fn hashRelationalJson(
    alloc: std.mem.Allocator,
    raw: []const u8,
    table_schema: runtime_schema.TableSchema,
) !Digest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .parse_numbers = false });
    defer parsed.deinit();
    return try hashRelationalParsedValue(alloc, parsed.value, table_schema);
}

fn hashRelationalColumnValue(
    alloc: std.mem.Allocator,
    hasher: *std.crypto.hash.Blake3,
    value: std.json.Value,
    column: runtime_schema.RelationalColumn,
) !void {
    if (value == .null) {
        hasher.update("n");
        return;
    }
    if (column.is_json or column.column_type == .json) {
        const canonical = try canonicalJsonValueAlloc(alloc, value);
        defer alloc.free(canonical);
        hashCanonicalJsonBytes(hasher, canonical);
        return;
    }
    switch (column.column_type) {
        .string, .blob, .geoshape => switch (value) {
            .string => |text| {
                hasher.update("s");
                hashBytes(hasher, text);
            },
            else => return error.InvalidBatchRequest,
        },
        .boolean => switch (value) {
            .bool => |flag| hasher.update(if (flag) "b1" else "b0"),
            else => return error.InvalidBatchRequest,
        },
        .integer => {
            const number = schema_api.documentIntegerToI64(value) orelse return error.InvalidBatchRequest;
            hasher.update("i");
            hashInt(hasher, i64, number);
        },
        .number => {
            const number = schema_api.documentNumberToF64(value) orelse return error.InvalidBatchRequest;
            hasher.update("f");
            hashInt(hasher, u64, if (number == 0) 0 else @bitCast(number));
        },
        .datetime => {
            const timestamp = schema_api.documentDateTimeToNs(value) orelse return error.InvalidBatchRequest;
            hasher.update("t");
            hashInt(hasher, u64, timestamp);
        },
        .geopoint => {
            if (value != .object or value.object.count() != 2) return error.InvalidBatchRequest;
            const lat = schema_api.documentNumberToF64(value.object.get("lat") orelse return error.InvalidBatchRequest) orelse return error.InvalidBatchRequest;
            const lon = schema_api.documentNumberToF64(value.object.get("lon") orelse return error.InvalidBatchRequest) orelse return error.InvalidBatchRequest;
            hasher.update("g");
            hashInt(hasher, u64, if (lat == 0) 0 else @bitCast(lat));
            hashInt(hasher, u64, if (lon == 0) 0 else @bitCast(lon));
        },
        .json => unreachable,
    }
}

fn hashValue(
    alloc: std.mem.Allocator,
    hasher: *std.crypto.hash.Blake3,
    value: std.json.Value,
) !void {
    switch (value) {
        .null => hasher.update("n"),
        .bool => |flag| hasher.update(if (flag) "b1" else "b0"),
        .integer => |number| {
            hasher.update("i");
            hashInt(hasher, i64, number);
        },
        .float => |number| {
            hasher.update("f");
            // JSON equality treats negative and positive zero as equal.
            const bits: u64 = if (number == 0) 0 else @bitCast(number);
            hashInt(hasher, u64, bits);
        },
        .number_string => |number| {
            hasher.update("r");
            hashBytes(hasher, number);
        },
        .string => |string| {
            hasher.update("s");
            hashBytes(hasher, string);
        },
        .array => |array| {
            hasher.update("a");
            hashInt(hasher, u64, @intCast(array.items.len));
            for (array.items) |item| try hashValue(alloc, hasher, item);
        },
        .object => |object| {
            hasher.update("o");
            const keys = try alloc.alloc([]const u8, comparableObjectFieldCount(object));
            defer alloc.free(keys);

            var key_index: usize = 0;
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                if (isIgnoredSystemField(entry.key_ptr.*)) continue;
                keys[key_index] = entry.key_ptr.*;
                key_index += 1;
            }
            std.mem.sort([]const u8, keys, {}, lessThanString);
            hashInt(hasher, u64, @intCast(keys.len));
            for (keys) |key| {
                hashBytes(hasher, key);
                try hashValue(alloc, hasher, object.get(key).?);
            }
        },
    }
}

fn hashInt(hasher: *std.crypto.hash.Blake3, comptime T: type, value: T) void {
    var encoded: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &encoded, value, .little);
    hasher.update(&encoded);
}

fn hashBytes(hasher: *std.crypto.hash.Blake3, value: []const u8) void {
    hashInt(hasher, u64, @intCast(value.len));
    hasher.update(value);
}

fn comparableObjectFieldCount(object: std.json.ObjectMap) usize {
    var count: usize = 0;
    var iterator = object.iterator();
    while (iterator.next()) |entry| {
        if (!isIgnoredSystemField(entry.key_ptr.*)) count += 1;
    }
    return count;
}

fn isIgnoredSystemField(field: []const u8) bool {
    return std.mem.eql(u8, field, "_timestamp") or std.mem.eql(u8, field, "_id");
}

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

test "document content hash ignores field order and injected system fields" {
    const left = try hashJson(std.testing.allocator,
        \\{"title":"alpha","nested":{"count":2,"_timestamp":1},"tags":["a","b"]}
    );
    const right = try hashJson(std.testing.allocator,
        \\{"tags":["a","b"],"_id":"doc:a","nested":{"_timestamp":9,"count":2},"title":"alpha","_timestamp":1234}
    );
    try std.testing.expectEqualSlices(u8, &left, &right);
}

test "document content hash preserves meaningful JSON differences" {
    const base = try hashJson(std.testing.allocator,
        \\{"title":"alpha","tags":["a","b"]}
    );
    const changed = try hashJson(std.testing.allocator,
        \\{"title":"alpha","tags":["b","a"]}
    );
    try std.testing.expect(!std.mem.eql(u8, &base, &changed));
}

test "relational content hash follows typed logical values" {
    const schema = runtime_schema.TableSchema{
        .version = 7,
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "score", .path = "score", .column_type = .number, .required = true },
            .{ .name = "when", .path = "when", .column_type = .datetime, .required = true },
        },
    };
    const numeric = try hashRelationalJson(std.testing.allocator, "{\"score\":1,\"when\":1000}", schema);
    const equivalent = try hashRelationalJson(std.testing.allocator, "{\"when\":\"1970-01-01T00:00:00.000001Z\",\"score\":1.0}", schema);
    try std.testing.expectEqualSlices(u8, &numeric, &equivalent);
}

test "relational JSON column hash uses canonical physical bytes" {
    const schema = runtime_schema.TableSchema{
        .version = 8,
        .storage_mode = .relational,
        .relational_columns = &.{
            .{
                .name = "payload",
                .path = "payload",
                .column_type = .json,
                .is_json = true,
                .json_kind = .object,
                .required = true,
            },
        },
    };
    const left = try hashRelationalJson(std.testing.allocator, "{\"payload\":{\"b\":2,\"a\":1}}", schema);
    const right = try hashRelationalJson(std.testing.allocator, "{\"payload\":{\"a\":1,\"b\":2}}", schema);
    try std.testing.expectEqualSlices(u8, &left, &right);
}
