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
        try hashRelationalCellCanonical(&hasher, cell, column);
    }
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

/// Hash only the cells present in a prepared row. Callers provide cells sorted
/// by logical column name, which preserves the canonical object hash while
/// keeping both work and scratch proportional to row density instead of schema
/// width. JSON cell bytes must already be canonical.
pub fn hashRelationalSparseCellsCanonical(
    cells: anytype,
    table_schema: runtime_schema.TableSchema,
) !Digest {
    var count: usize = 0;
    var previous_name: ?[]const u8 = null;
    for (cells) |cell| {
        if (cell.ordinal >= table_schema.relational_columns.len) return error.InvalidBatchRequest;
        const column = table_schema.relational_columns[cell.ordinal];
        if (previous_name) |name| if (!std.mem.lessThan(u8, name, column.name))
            return error.InvalidBatchRequest;
        previous_name = column.name;
        if (!isIgnoredSystemField(column.name)) count += 1;
    }

    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("o");
    hashInt(&hasher, u64, @intCast(count));
    for (cells) |cell| {
        const column = table_schema.relational_columns[cell.ordinal];
        if (isIgnoredSystemField(column.name)) continue;
        hashBytes(&hasher, column.name);
        try hashRelationalCellCanonical(&hasher, cell, column);
    }
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

/// Hash sparse cells that remain in the physical encoder's required ordinal
/// order. The immutable layout supplies the canonical logical-name order, so a
/// prepared row does not have to reorder its comparatively large `Cell`
/// records for hashing and then reorder them again for encoding.
pub fn hashRelationalSparseOrdinalCellsCanonical(
    cells: anytype,
    table_schema: runtime_schema.TableSchema,
    hash_ordinals: []const u32,
) !Digest {
    if (hash_ordinals.len != table_schema.relational_columns.len)
        return error.InvalidBatchRequest;

    var count: usize = 0;
    var previous_ordinal: ?u32 = null;
    for (cells) |cell| {
        if (cell.ordinal >= table_schema.relational_columns.len or
            (previous_ordinal != null and cell.ordinal <= previous_ordinal.?))
            return error.InvalidBatchRequest;
        previous_ordinal = cell.ordinal;
        if (!isIgnoredSystemField(table_schema.relational_columns[cell.ordinal].name)) count += 1;
    }

    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("o");
    hashInt(&hasher, u64, @intCast(count));
    for (hash_ordinals) |ordinal| {
        if (ordinal >= table_schema.relational_columns.len) return error.InvalidBatchRequest;

        // Fully dense rows map ordinal to cell index directly. Partially dense
        // rows use binary lookup, preserving scratch proportional to row
        // density instead of allocating a schema-width map for every batch row.
        const cell_index = if (cells.len == table_schema.relational_columns.len) blk: {
            if (cells[ordinal].ordinal != ordinal) return error.InvalidBatchRequest;
            break :blk @as(usize, ordinal);
        } else blk: {
            var low: usize = 0;
            var high: usize = cells.len;
            while (low < high) {
                const middle = low + (high - low) / 2;
                if (cells[middle].ordinal < ordinal)
                    low = middle + 1
                else
                    high = middle;
            }
            if (low >= cells.len or cells[low].ordinal != ordinal) continue;
            break :blk low;
        };

        const column = table_schema.relational_columns[ordinal];
        if (isIgnoredSystemField(column.name)) continue;
        hashBytes(&hasher, column.name);
        const cell = cells[cell_index];
        try hashRelationalCellCanonical(&hasher, cell, column);
    }
    var digest: Digest = undefined;
    hasher.final(&digest);
    return digest;
}

fn hashRelationalCellCanonical(
    hasher: *std.crypto.hash.Blake3,
    cell: anytype,
    column: runtime_schema.RelationalColumn,
) !void {
    if (cell.is_null) {
        hasher.update("n");
        return;
    }
    if (column.is_json or column.column_type == .json) {
        hashJsonCellBytes(hasher, cell.value.bytes_val);
        return;
    }
    switch (column.column_type) {
        .string, .blob, .geoshape => {
            hasher.update("s");
            hashBytes(hasher, cell.value.bytes_val);
        },
        .boolean => hasher.update(if (cell.value.bool_val) "b1" else "b0"),
        .integer => {
            hasher.update("i");
            hashInt(hasher, i64, cell.value.i64_val);
        },
        .number => {
            hasher.update("f");
            const number = cell.value.f64_val;
            hashInt(hasher, u64, if (number == 0) 0 else @bitCast(number));
        },
        .datetime => {
            hasher.update("t");
            hashInt(hasher, u64, cell.value.u64_val);
        },
        .geopoint => {
            hasher.update("g");
            const point = cell.value.geo_point;
            hashInt(hasher, u64, if (point.lat == 0) 0 else @bitCast(point.lat));
            hashInt(hasher, u64, if (point.lon == 0) 0 else @bitCast(point.lon));
        },
        .dense_vector => try hashDenseVectorBytes(hasher, cell.value.bytes_val),
        .json => unreachable,
    }
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
        .number_string => |number| try writeCanonicalJsonNumber(alloc, writer, number),
        else => try std.json.Stringify.value(value, .{}, writer),
    }
}

/// Emit one exact JSON number in a unique scientific representation without
/// converting through a bounded integer or binary float. The coefficient and
/// exponent remain arbitrary precision, so `1`, `1.0`, `10e-1`, and `1e0`
/// become the same physical bytes while very large values retain every digit.
fn writeCanonicalJsonNumber(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    raw: []const u8,
) !void {
    if (raw.len == 0) return error.InvalidJsonNumber;
    var pos: usize = 0;
    const negative = raw[pos] == '-';
    if (negative) {
        pos += 1;
        if (pos == raw.len) return error.InvalidJsonNumber;
    }

    const exponent_marker = std.mem.indexOfAnyPos(u8, raw, pos, "eE") orelse raw.len;
    const mantissa = raw[pos..exponent_marker];
    const decimal = std.mem.indexOfScalar(u8, mantissa, '.');
    const fractional_digits = if (decimal) |index| mantissa.len - index - 1 else 0;

    var digits = std.ArrayListUnmanaged(u8).empty;
    defer digits.deinit(alloc);
    try digits.ensureTotalCapacity(alloc, mantissa.len);
    for (mantissa) |byte| {
        if (byte == '.') continue;
        if (byte < '0' or byte > '9') return error.InvalidJsonNumber;
        digits.appendAssumeCapacity(byte);
    }
    if (digits.items.len == 0) return error.InvalidJsonNumber;

    var first: usize = 0;
    while (first < digits.items.len and digits.items[first] == '0') first += 1;
    if (first == digits.items.len) {
        try writer.writeByte('0');
        return;
    }
    var end = digits.items.len;
    while (end > first and digits.items[end - 1] == '0') end -= 1;
    const trailing_zeros = digits.items.len - end;
    const significant = digits.items[first..end];

    var exponent_negative = false;
    var exponent_digits: []const u8 = "0";
    if (exponent_marker != raw.len) {
        var exponent_pos = exponent_marker + 1;
        if (exponent_pos == raw.len) return error.InvalidJsonNumber;
        if (raw[exponent_pos] == '+' or raw[exponent_pos] == '-') {
            exponent_negative = raw[exponent_pos] == '-';
            exponent_pos += 1;
        }
        if (exponent_pos == raw.len) return error.InvalidJsonNumber;
        exponent_digits = normalizedDecimalMagnitude(raw[exponent_pos..]) catch
            return error.InvalidJsonNumber;
        if (exponent_digits.len == 1 and exponent_digits[0] == '0') exponent_negative = false;
    }

    const fractional_adjustment = std.math.cast(i64, fractional_digits) orelse
        return error.InvalidJsonNumber;
    const trailing_adjustment = std.math.cast(i64, trailing_zeros) orelse
        return error.InvalidJsonNumber;
    const significant_adjustment = std.math.cast(i64, significant.len - 1) orelse
        return error.InvalidJsonNumber;
    const adjustment = std.math.sub(i64, trailing_adjustment, fractional_adjustment) catch
        return error.InvalidJsonNumber;
    const scientific_adjustment = std.math.add(i64, adjustment, significant_adjustment) catch
        return error.InvalidJsonNumber;
    const scientific_exponent = try addSignedDecimalSmallAlloc(
        alloc,
        exponent_negative,
        exponent_digits,
        scientific_adjustment,
    );
    defer alloc.free(scientific_exponent);

    if (negative) try writer.writeByte('-');
    const compact_exponent = std.fmt.parseInt(i32, scientific_exponent, 10) catch null;
    if (compact_exponent) |exponent| {
        // Match the conventional JSON/JCS readability window while retaining
        // an exact arbitrary-precision coefficient outside it.
        if (exponent >= -6 and exponent < 21) {
            const decimal_position: i64 = @as(i64, exponent) + 1;
            if (decimal_position <= 0) {
                try writer.writeAll("0.");
                var zeros: i64 = -decimal_position;
                while (zeros > 0) : (zeros -= 1) try writer.writeByte('0');
                try writer.writeAll(significant);
            } else if (decimal_position >= significant.len) {
                try writer.writeAll(significant);
                var zeros: usize = @intCast(decimal_position - @as(i64, @intCast(significant.len)));
                while (zeros > 0) : (zeros -= 1) try writer.writeByte('0');
            } else {
                const split: usize = @intCast(decimal_position);
                try writer.writeAll(significant[0..split]);
                try writer.writeByte('.');
                try writer.writeAll(significant[split..]);
            }
            return;
        }
    }

    try writer.writeByte(significant[0]);
    if (significant.len > 1) {
        try writer.writeByte('.');
        try writer.writeAll(significant[1..]);
    }
    if (!std.mem.eql(u8, scientific_exponent, "0")) {
        try writer.writeByte('e');
        try writer.writeAll(scientific_exponent);
    }
}

fn normalizedDecimalMagnitude(raw: []const u8) ![]const u8 {
    if (raw.len == 0) return error.InvalidJsonNumber;
    for (raw) |byte| if (byte < '0' or byte > '9') return error.InvalidJsonNumber;
    var first: usize = 0;
    while (first + 1 < raw.len and raw[first] == '0') first += 1;
    return raw[first..];
}

fn compareDecimalMagnitudes(lhs: []const u8, rhs: []const u8) std.math.Order {
    if (lhs.len < rhs.len) return .lt;
    if (lhs.len > rhs.len) return .gt;
    return std.mem.order(u8, lhs, rhs);
}

fn addDecimalMagnitudesAlloc(alloc: std.mem.Allocator, lhs: []const u8, rhs: []const u8) ![]u8 {
    const capacity = @max(lhs.len, rhs.len) + 1;
    const out = try alloc.alloc(u8, capacity);
    var out_pos = capacity;
    var lhs_pos = lhs.len;
    var rhs_pos = rhs.len;
    var carry: u8 = 0;
    while (lhs_pos != 0 or rhs_pos != 0 or carry != 0) {
        const left = if (lhs_pos != 0) blk: {
            lhs_pos -= 1;
            break :blk lhs[lhs_pos] - '0';
        } else 0;
        const right = if (rhs_pos != 0) blk: {
            rhs_pos -= 1;
            break :blk rhs[rhs_pos] - '0';
        } else 0;
        const sum = left + right + carry;
        out_pos -= 1;
        out[out_pos] = '0' + sum % 10;
        carry = sum / 10;
    }
    std.mem.copyForwards(u8, out[0 .. capacity - out_pos], out[out_pos..]);
    return alloc.realloc(out, capacity - out_pos);
}

/// Subtract rhs from lhs. The caller establishes lhs >= rhs.
fn subtractDecimalMagnitudesAlloc(alloc: std.mem.Allocator, lhs: []const u8, rhs: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, lhs.len);
    var lhs_pos = lhs.len;
    var rhs_pos = rhs.len;
    var borrow: i16 = 0;
    while (lhs_pos != 0) {
        lhs_pos -= 1;
        const left: i16 = @as(i16, lhs[lhs_pos] - '0') - borrow;
        const right: i16 = if (rhs_pos != 0) blk: {
            rhs_pos -= 1;
            break :blk rhs[rhs_pos] - '0';
        } else 0;
        if (left < right) {
            out[lhs_pos] = @intCast('0' + left + 10 - right);
            borrow = 1;
        } else {
            out[lhs_pos] = @intCast('0' + left - right);
            borrow = 0;
        }
    }
    std.debug.assert(borrow == 0 and rhs_pos == 0);
    var first: usize = 0;
    while (first + 1 < out.len and out[first] == '0') first += 1;
    if (first != 0) std.mem.copyForwards(u8, out[0 .. out.len - first], out[first..]);
    return alloc.realloc(out, out.len - first);
}

fn addSignedDecimalSmallAlloc(
    alloc: std.mem.Allocator,
    lhs_negative: bool,
    lhs_magnitude: []const u8,
    rhs: i64,
) ![]u8 {
    var rhs_buffer: [32]u8 = undefined;
    const rhs_negative = rhs < 0;
    const rhs_magnitude_value: u64 = if (rhs_negative) @intCast(-rhs) else @intCast(rhs);
    const rhs_magnitude = try std.fmt.bufPrint(&rhs_buffer, "{d}", .{rhs_magnitude_value});
    const lhs_zero = lhs_magnitude.len == 1 and lhs_magnitude[0] == '0';
    const rhs_zero = rhs_magnitude_value == 0;
    const lhs_sign: i2 = if (lhs_zero) 0 else if (lhs_negative) -1 else 1;
    const rhs_sign: i2 = if (rhs_zero) 0 else if (rhs_negative) -1 else 1;

    var magnitude: []u8 = undefined;
    var negative = false;
    if (lhs_sign == 0) {
        magnitude = try alloc.dupe(u8, rhs_magnitude);
        negative = rhs_sign < 0;
    } else if (rhs_sign == 0) {
        magnitude = try alloc.dupe(u8, lhs_magnitude);
        negative = lhs_sign < 0;
    } else if (lhs_sign == rhs_sign) {
        magnitude = try addDecimalMagnitudesAlloc(alloc, lhs_magnitude, rhs_magnitude);
        negative = lhs_sign < 0;
    } else switch (compareDecimalMagnitudes(lhs_magnitude, rhs_magnitude)) {
        .eq => return try alloc.dupe(u8, "0"),
        .gt => {
            magnitude = try subtractDecimalMagnitudesAlloc(alloc, lhs_magnitude, rhs_magnitude);
            negative = lhs_sign < 0;
        },
        .lt => {
            magnitude = try subtractDecimalMagnitudesAlloc(alloc, rhs_magnitude, lhs_magnitude);
            negative = rhs_sign < 0;
        },
    }
    errdefer alloc.free(magnitude);
    if (!negative) return magnitude;
    const signed = try alloc.alloc(u8, magnitude.len + 1);
    signed[0] = '-';
    @memcpy(signed[1..], magnitude);
    alloc.free(magnitude);
    return signed;
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
        .dense_vector => {
            if (value != .array) return error.InvalidBatchRequest;
            hasher.update("v");
            hashInt(hasher, u64, @intCast(value.array.items.len));
            for (value.array.items) |item| {
                const number = schema_api.documentNumberToF64(item) orelse return error.InvalidBatchRequest;
                const narrowed: f32 = @floatCast(number);
                if (!std.math.isFinite(narrowed)) return error.InvalidBatchRequest;
                hashInt(hasher, u32, if (narrowed == 0) 0 else @bitCast(narrowed));
            }
        },
        .json => unreachable,
    }
}

fn hashDenseVectorBytes(hasher: *std.crypto.hash.Blake3, bytes: []const u8) !void {
    if (bytes.len % @sizeOf(f32) != 0) return error.InvalidRelationalRow;
    hasher.update("v");
    hashInt(hasher, u64, @intCast(bytes.len / @sizeOf(f32)));
    var offset: usize = 0;
    while (offset < bytes.len) : (offset += 4) {
        const value: f32 = @bitCast(std.mem.readInt(u32, bytes[offset..][0..4], .little));
        if (!std.math.isFinite(value)) return error.InvalidRelationalRow;
        hashInt(hasher, u32, if (value == 0) 0 else @bitCast(value));
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

test "canonical JSON normalizes equivalent number spellings losslessly" {
    const alloc = std.testing.allocator;
    const inputs = [_][]const u8{
        "{\"n\":1}",
        "{\"n\":1.0}",
        "{\"n\":10e-1}",
        "{\"n\":0.1e1}",
    };
    for (inputs) |input| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, input, .{ .parse_numbers = false });
        defer parsed.deinit();
        const canonical = try canonicalJsonValueAlloc(alloc, parsed.value);
        defer alloc.free(canonical);
        try std.testing.expectEqualStrings("{\"n\":1}", canonical);
    }

    var negative_zero = try std.json.parseFromSlice(std.json.Value, alloc, "-0.000e999", .{ .parse_numbers = false });
    defer negative_zero.deinit();
    const canonical_zero = try canonicalJsonValueAlloc(alloc, negative_zero.value);
    defer alloc.free(canonical_zero);
    try std.testing.expectEqualStrings("0", canonical_zero);

    var huge = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        "10e999999999999999999998",
        .{ .parse_numbers = false },
    );
    defer huge.deinit();
    const canonical_huge = try canonicalJsonValueAlloc(alloc, huge.value);
    defer alloc.free(canonical_huge);
    try std.testing.expectEqualStrings("1e999999999999999999999", canonical_huge);
}

test "relational JSON semantic hash ignores number spelling" {
    const schema = runtime_schema.TableSchema{
        .version = 9,
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
    const left = try hashRelationalJson(std.testing.allocator, "{\"payload\":{\"n\":1}}", schema);
    const right = try hashRelationalJson(std.testing.allocator, "{\"payload\":{\"n\":1.0e0}}", schema);
    try std.testing.expectEqualSlices(u8, &left, &right);
}
