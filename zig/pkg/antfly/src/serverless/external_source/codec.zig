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
const external_source = @import("types.zig");

const magic = "AFXS";
const version: u32 = 14;

pub const DecodeLimits = struct {
    max_artifact_bytes: usize = 256 * 1024 * 1024,
    max_struct_allocation_bytes: usize = 256 * 1024 * 1024,

    pub fn validate(self: DecodeLimits) !void {
        if (self.max_artifact_bytes == 0 or self.max_struct_allocation_bytes == 0) {
            return error.InvalidExternalSourceInventory;
        }
    }
};

const DecodeBudget = struct {
    remaining_struct_bytes: usize,

    fn admitCount(
        self: *DecodeBudget,
        comptime T: type,
        bytes: []const u8,
        cursor: usize,
        raw_count: u32,
    ) !usize {
        if (cursor > bytes.len) return error.InvalidExternalSourceInventory;
        const count: usize = @intCast(raw_count);
        // Every encoded record consumes at least one byte. This cheap check
        // rejects forged counts before asking the allocator for a large slice.
        if (count > bytes.len - cursor) return error.InvalidExternalSourceInventory;
        const allocation_bytes = std.math.mul(usize, count, @sizeOf(T)) catch
            return error.ExternalSourceInventoryTooLarge;
        if (allocation_bytes > self.remaining_struct_bytes) return error.ExternalSourceInventoryTooLarge;
        self.remaining_struct_bytes -= allocation_bytes;
        return count;
    }
};

pub fn encodeAlloc(alloc: Allocator, inventory: external_source.Inventory) ![]u8 {
    try inventory.validate();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, magic);
    try appendU32(alloc, &out, version);
    try out.append(alloc, @intFromEnum(inventory.format));
    try appendBytes(alloc, &out, inventory.source_id);
    try appendBytes(alloc, &out, inventory.source_uri);
    try appendBytes(alloc, &out, inventory.snapshot_id);
    try appendBytes(alloc, &out, inventory.schema_fingerprint);
    try appendU32(alloc, &out, @intCast(inventory.files.len));

    for (inventory.files) |file| {
        try appendBytes(alloc, &out, file.file_id);
        try appendBytes(alloc, &out, file.object_uri);
        try appendBytes(alloc, &out, file.etag);
        try appendBytes(alloc, &out, file.version_id);
        try appendU64(alloc, &out, file.byte_len);
        try appendU64(alloc, &out, file.row_count);
        try appendOptionalI64(alloc, &out, file.data_sequence_number);
        try appendOptionalI32(alloc, &out, file.partition_spec_id);
        try appendU32(
            alloc,
            &out,
            if (file.partition_field_count != 0) file.partition_field_count else @intCast(file.partition_values.len),
        );
        try appendU32(alloc, &out, @intCast(file.partition_values.len));
        for (file.partition_values) |partition| {
            try appendBytes(alloc, &out, partition.column_id);
            try appendBytes(alloc, &out, partition.string_value);
        }
        try appendU32(alloc, &out, @intCast(file.row_groups.len));
        for (file.row_groups) |row_group| {
            try appendU32(alloc, &out, row_group.ordinal);
            try appendU64(alloc, &out, row_group.row_count);
            try appendU64(alloc, &out, row_group.file_offset);
            try appendU64(alloc, &out, row_group.total_byte_len);
            try appendU32(alloc, &out, @intCast(row_group.column_chunks.len));
            for (row_group.column_chunks) |chunk| {
                try appendBytes(alloc, &out, chunk.column_id);
                try appendU64(alloc, &out, chunk.file_offset);
                try appendU64(alloc, &out, chunk.compressed_len);
                try appendU64(alloc, &out, chunk.uncompressed_len);
                try appendBytes(alloc, &out, chunk.compression_codec);
                try appendBytes(alloc, &out, chunk.encoding);
                try appendBytes(alloc, &out, chunk.physical_type);
                try appendI32(alloc, &out, chunk.type_length);
                try appendBytes(alloc, &out, chunk.logical_type);
                try appendI32(alloc, &out, chunk.decimal_precision);
                try appendI32(alloc, &out, chunk.decimal_scale);
                try appendOptionalI64(alloc, &out, chunk.stats_min_i64);
                try appendOptionalI64(alloc, &out, chunk.stats_max_i64);
                try appendOptionalBytes(alloc, &out, chunk.stats_min_bytes);
                try appendOptionalBytes(alloc, &out, chunk.stats_max_bytes);
                try appendOptionalBool(alloc, &out, chunk.stats_min_bool);
                try appendOptionalBool(alloc, &out, chunk.stats_max_bool);
                try appendOptionalF64(alloc, &out, chunk.stats_min_f64);
                try appendOptionalF64(alloc, &out, chunk.stats_max_f64);
                try out.append(alloc, if (chunk.nullable) 1 else 0);
                try appendOptionalI32(alloc, &out, chunk.field_id);
            }
        }
    }

    return try out.toOwnedSlice(alloc);
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !external_source.Inventory {
    return try decodeAllocWithLimits(alloc, bytes, .{});
}

pub fn decodeAllocWithLimits(
    alloc: Allocator,
    bytes: []const u8,
    limits: DecodeLimits,
) !external_source.Inventory {
    try limits.validate();
    if (bytes.len > limits.max_artifact_bytes) return error.ExternalSourceInventoryTooLarge;
    var budget = DecodeBudget{ .remaining_struct_bytes = limits.max_struct_allocation_bytes };
    var cursor: usize = 0;
    if (bytes.len < magic.len + 4) return error.InvalidExternalSourceInventory;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidExternalSourceInventoryMagic;
    cursor += magic.len;
    const got_version = try readU32(bytes, &cursor);
    if (got_version != 2 and got_version != 3 and got_version != 4 and got_version != 5 and got_version != 6 and got_version != 7 and got_version != 8 and got_version != 9 and got_version != 10 and got_version != 11 and got_version != 12 and got_version != 13 and got_version != version) return error.UnsupportedExternalSourceInventoryVersion;
    if (cursor >= bytes.len) return error.InvalidExternalSourceInventory;
    const format = try decodeFormat(bytes[cursor]);
    cursor += 1;

    const source_id = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(source_id);
    const source_uri = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(source_uri);
    const snapshot_id = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try readBytesAlloc(alloc, bytes, &cursor);
    errdefer alloc.free(schema_fingerprint);
    const raw_file_count = try readU32(bytes, &cursor);
    const file_count = try budget.admitCount(external_source.FileEntry, bytes, cursor, raw_file_count);

    const files = try alloc.alloc(external_source.FileEntry, file_count);
    errdefer alloc.free(files);
    var initialized_files: usize = 0;
    errdefer {
        for (files[0..initialized_files]) |*file| file.deinit(alloc);
    }

    for (files) |*file| {
        var keep_file = false;
        const file_id = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer if (!keep_file) alloc.free(file_id);
        const object_uri = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer if (!keep_file) alloc.free(object_uri);
        const etag = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer if (!keep_file and etag.len > 0) alloc.free(etag);
        const version_id = try readBytesAlloc(alloc, bytes, &cursor);
        errdefer if (!keep_file and version_id.len > 0) alloc.free(version_id);
        const byte_len = try readU64(bytes, &cursor);
        const row_count = try readU64(bytes, &cursor);
        const data_sequence_number: ?i64 = if (got_version >= 13) try readOptionalI64(bytes, &cursor) else null;
        const partition_spec_id: ?i32 = if (got_version >= 14) try readOptionalI32(bytes, &cursor) else null;
        const encoded_partition_field_count: ?u32 = if (got_version >= 14) try readU32(bytes, &cursor) else null;
        const partition_count = if (got_version >= 8) blk: {
            const raw_count = try readU32(bytes, &cursor);
            break :blk try budget.admitCount(external_source.PartitionValue, bytes, cursor, raw_count);
        } else 0;
        const partition_values = try alloc.alloc(external_source.PartitionValue, partition_count);
        var initialized_partitions: usize = 0;
        errdefer if (!keep_file) {
            for (partition_values[0..initialized_partitions]) |*partition| partition.deinit(alloc);
            alloc.free(partition_values);
        };
        for (partition_values) |*partition| {
            var keep_partition = false;
            const column_id = try readBytesAlloc(alloc, bytes, &cursor);
            errdefer if (!keep_partition) alloc.free(column_id);
            const string_value = try readBytesAlloc(alloc, bytes, &cursor);
            errdefer if (!keep_partition) alloc.free(string_value);
            partition.* = .{
                .column_id = column_id,
                .string_value = string_value,
            };
            keep_partition = true;
            initialized_partitions += 1;
        }
        const raw_row_group_count = try readU32(bytes, &cursor);
        const row_group_count = try budget.admitCount(external_source.RowGroup, bytes, cursor, raw_row_group_count);
        const row_groups = try alloc.alloc(external_source.RowGroup, row_group_count);
        var initialized_row_groups: usize = 0;
        errdefer if (!keep_file) {
            for (row_groups[0..initialized_row_groups]) |*row_group| row_group.deinit(alloc);
            alloc.free(row_groups);
        };
        for (row_groups) |*row_group| {
            var keep_row_group = false;
            const ordinal = try readU32(bytes, &cursor);
            const row_group_rows = try readU64(bytes, &cursor);
            const file_offset = try readU64(bytes, &cursor);
            const total_byte_len = try readU64(bytes, &cursor);
            const raw_column_chunk_count = try readU32(bytes, &cursor);
            const column_chunk_count = try budget.admitCount(external_source.ColumnChunk, bytes, cursor, raw_column_chunk_count);
            const column_chunks = try alloc.alloc(external_source.ColumnChunk, column_chunk_count);
            var initialized_chunks: usize = 0;
            errdefer if (!keep_row_group) {
                for (column_chunks[0..initialized_chunks]) |*chunk| chunk.deinit(alloc);
                alloc.free(column_chunks);
            };
            for (column_chunks) |*chunk| {
                var keep_chunk = false;
                const column_id = try readBytesAlloc(alloc, bytes, &cursor);
                errdefer if (!keep_chunk) alloc.free(column_id);
                const chunk_file_offset = try readU64(bytes, &cursor);
                const compressed_len = try readU64(bytes, &cursor);
                const uncompressed_len = try readU64(bytes, &cursor);
                const compression_codec = try readBytesAlloc(alloc, bytes, &cursor);
                errdefer if (!keep_chunk and compression_codec.len > 0) alloc.free(compression_codec);
                const encoding = try readBytesAlloc(alloc, bytes, &cursor);
                errdefer if (!keep_chunk and encoding.len > 0) alloc.free(encoding);
                const physical_type: []u8 = if (got_version >= 3) try readBytesAlloc(alloc, bytes, &cursor) else @constCast(&.{});
                errdefer if (!keep_chunk and physical_type.len > 0) alloc.free(physical_type);
                const type_length: i32 = if (got_version >= 6) try readI32(bytes, &cursor) else 0;
                const logical_type: []u8 = if (got_version >= 4) try readBytesAlloc(alloc, bytes, &cursor) else @constCast(&.{});
                errdefer if (!keep_chunk and logical_type.len > 0) alloc.free(logical_type);
                const decimal_precision: i32 = if (got_version >= 5) try readI32(bytes, &cursor) else 0;
                const decimal_scale: i32 = if (got_version >= 5) try readI32(bytes, &cursor) else 0;
                const stats_min_i64: ?i64 = if (got_version >= 7) try readOptionalI64(bytes, &cursor) else null;
                const stats_max_i64: ?i64 = if (got_version >= 7) try readOptionalI64(bytes, &cursor) else null;
                const stats_min_bytes: ?[]u8 = if (got_version >= 9) try readOptionalBytesAlloc(alloc, bytes, &cursor) else null;
                errdefer if (!keep_chunk) if (stats_min_bytes) |value| alloc.free(value);
                const stats_max_bytes: ?[]u8 = if (got_version >= 9) try readOptionalBytesAlloc(alloc, bytes, &cursor) else null;
                errdefer if (!keep_chunk) if (stats_max_bytes) |value| alloc.free(value);
                const stats_min_bool: ?bool = if (got_version >= 10) try readOptionalBool(bytes, &cursor) else null;
                const stats_max_bool: ?bool = if (got_version >= 10) try readOptionalBool(bytes, &cursor) else null;
                const stats_min_f64: ?f64 = if (got_version >= 11) try readOptionalF64(bytes, &cursor) else null;
                const stats_max_f64: ?f64 = if (got_version >= 11) try readOptionalF64(bytes, &cursor) else null;
                const nullable = if (got_version >= 3) blk: {
                    if (cursor >= bytes.len) return error.InvalidExternalSourceInventory;
                    const raw = bytes[cursor];
                    cursor += 1;
                    if (raw > 1) return error.InvalidExternalSourceInventory;
                    break :blk raw == 1;
                } else false;
                const field_id: ?i32 = if (got_version >= 12) try readOptionalI32(bytes, &cursor) else null;
                chunk.* = .{
                    .column_id = column_id,
                    .file_offset = chunk_file_offset,
                    .compressed_len = compressed_len,
                    .uncompressed_len = uncompressed_len,
                    .compression_codec = compression_codec,
                    .encoding = encoding,
                    .physical_type = physical_type,
                    .type_length = type_length,
                    .logical_type = logical_type,
                    .decimal_precision = decimal_precision,
                    .decimal_scale = decimal_scale,
                    .stats_min_i64 = stats_min_i64,
                    .stats_max_i64 = stats_max_i64,
                    .stats_min_bytes = stats_min_bytes,
                    .stats_max_bytes = stats_max_bytes,
                    .stats_min_bool = stats_min_bool,
                    .stats_max_bool = stats_max_bool,
                    .stats_min_f64 = stats_min_f64,
                    .stats_max_f64 = stats_max_f64,
                    .nullable = nullable,
                    .field_id = field_id,
                };
                keep_chunk = true;
                initialized_chunks += 1;
            }
            row_group.* = .{
                .ordinal = ordinal,
                .row_count = row_group_rows,
                .file_offset = file_offset,
                .total_byte_len = total_byte_len,
                .column_chunks = column_chunks,
            };
            keep_row_group = true;
            initialized_row_groups += 1;
        }
        file.* = .{
            .file_id = file_id,
            .object_uri = object_uri,
            .etag = etag,
            .version_id = version_id,
            .byte_len = byte_len,
            .row_count = row_count,
            .data_sequence_number = data_sequence_number,
            .partition_spec_id = partition_spec_id,
            .partition_field_count = encoded_partition_field_count orelse @intCast(partition_count),
            .partition_values = partition_values,
            .row_groups = row_groups,
        };
        keep_file = true;
        initialized_files += 1;
    }

    if (cursor != bytes.len) return error.InvalidExternalSourceInventory;

    var inventory = external_source.Inventory{
        .format = format,
        .source_id = source_id,
        .source_uri = source_uri,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .files = files,
    };
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}

fn decodeFormat(raw: u8) !external_source.Format {
    return switch (raw) {
        1 => .parquet,
        2 => .iceberg,
        3 => .lance,
        else => error.InvalidExternalSourceInventory,
    };
}

fn appendBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendU32(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn readBytesAlloc(alloc: Allocator, bytes: []const u8, cursor: *usize) ![]u8 {
    const len = try readU32(bytes, cursor);
    if (cursor.* > bytes.len or len > bytes.len - cursor.*) return error.InvalidExternalSourceInventory;
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

fn appendI32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendOptionalI32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: ?i32) !void {
    if (value) |got| {
        try out.append(alloc, 1);
        try appendI32(alloc, out, got);
    } else {
        try out.append(alloc, 0);
    }
}

fn appendI64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendOptionalI64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: ?i64) !void {
    if (value) |got| {
        try out.append(alloc, 1);
        try appendI64(alloc, out, got);
    } else {
        try out.append(alloc, 0);
    }
}

fn appendOptionalBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: ?[]const u8) !void {
    if (value) |bytes| {
        try out.append(alloc, 1);
        try appendBytes(alloc, out, bytes);
    } else {
        try out.append(alloc, 0);
    }
}

fn appendOptionalBool(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: ?bool) !void {
    if (value) |boolean| {
        try out.append(alloc, 1);
        try out.append(alloc, if (boolean) 1 else 0);
    } else {
        try out.append(alloc, 0);
    }
}

fn appendOptionalF64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: ?f64) !void {
    if (value) |number| {
        try out.append(alloc, 1);
        try appendU64(alloc, out, @bitCast(number));
    } else {
        try out.append(alloc, 0);
    }
}

fn readU32(bytes: []const u8, cursor: *usize) !u32 {
    if (cursor.* + 4 > bytes.len) return error.InvalidExternalSourceInventory;
    const value = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    return value;
}

fn readU64(bytes: []const u8, cursor: *usize) !u64 {
    if (cursor.* + 8 > bytes.len) return error.InvalidExternalSourceInventory;
    const value = std.mem.readInt(u64, bytes[cursor.*..][0..8], .little);
    cursor.* += 8;
    return value;
}

fn readI32(bytes: []const u8, cursor: *usize) !i32 {
    if (cursor.* + 4 > bytes.len) return error.InvalidExternalSourceInventory;
    const value = std.mem.readInt(i32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    return value;
}

fn readOptionalI32(bytes: []const u8, cursor: *usize) !?i32 {
    if (cursor.* >= bytes.len) return error.InvalidExternalSourceInventory;
    const raw = bytes[cursor.*];
    cursor.* += 1;
    return switch (raw) {
        0 => null,
        1 => try readI32(bytes, cursor),
        else => error.InvalidExternalSourceInventory,
    };
}

fn readI64(bytes: []const u8, cursor: *usize) !i64 {
    if (cursor.* + 8 > bytes.len) return error.InvalidExternalSourceInventory;
    const value = std.mem.readInt(i64, bytes[cursor.*..][0..8], .little);
    cursor.* += 8;
    return value;
}

fn readOptionalI64(bytes: []const u8, cursor: *usize) !?i64 {
    if (cursor.* >= bytes.len) return error.InvalidExternalSourceInventory;
    const raw = bytes[cursor.*];
    cursor.* += 1;
    return switch (raw) {
        0 => null,
        1 => try readI64(bytes, cursor),
        else => error.InvalidExternalSourceInventory,
    };
}

fn readOptionalBytesAlloc(alloc: Allocator, bytes: []const u8, cursor: *usize) !?[]u8 {
    if (cursor.* >= bytes.len) return error.InvalidExternalSourceInventory;
    const tag = bytes[cursor.*];
    cursor.* += 1;
    if (tag == 0) return null;
    if (tag != 1) return error.InvalidExternalSourceInventory;
    return try readBytesAlloc(alloc, bytes, cursor);
}

fn readOptionalBool(bytes: []const u8, cursor: *usize) !?bool {
    if (cursor.* >= bytes.len) return error.InvalidExternalSourceInventory;
    const tag = bytes[cursor.*];
    cursor.* += 1;
    if (tag == 0) return null;
    if (tag != 1) return error.InvalidExternalSourceInventory;
    if (cursor.* >= bytes.len) return error.InvalidExternalSourceInventory;
    const value = bytes[cursor.*];
    cursor.* += 1;
    return switch (value) {
        0 => false,
        1 => true,
        else => error.InvalidExternalSourceInventory,
    };
}

fn readOptionalF64(bytes: []const u8, cursor: *usize) !?f64 {
    if (cursor.* >= bytes.len) return error.InvalidExternalSourceInventory;
    const tag = bytes[cursor.*];
    cursor.* += 1;
    if (tag == 0) return null;
    if (tag != 1) return error.InvalidExternalSourceInventory;
    return @bitCast(try readU64(bytes, cursor));
}

test "external source inventory codec round-trips file inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-123"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "file-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events/file-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-file-a"),
        .version_id = try alloc.dupe(u8, "version-file-a"),
        .byte_len = 1024,
        .row_count = 2,
        .data_sequence_number = 42,
        .partition_spec_id = 7,
        .partition_field_count = 1,
        .partition_values = try alloc.dupe(external_source.PartitionValue, &[_]external_source.PartitionValue{.{
            .column_id = try alloc.dupe(u8, "region"),
            .string_value = try alloc.dupe(u8, "us-west"),
        }}),
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{
                .ordinal = 0,
                .row_count = 2,
                .file_offset = 4,
                .total_byte_len = 512,
                .column_chunks = try alloc.dupe(external_source.ColumnChunk, &[_]external_source.ColumnChunk{.{
                    .column_id = try alloc.dupe(u8, "amount"),
                    .file_offset = 128,
                    .compressed_len = 64,
                    .uncompressed_len = 256,
                    .compression_codec = try alloc.dupe(u8, "zstd"),
                    .encoding = try alloc.dupe(u8, "plain"),
                    .physical_type = try alloc.dupe(u8, "int64"),
                    .type_length = 0,
                    .logical_type = try alloc.dupe(u8, "timestamp_micros"),
                    .decimal_precision = 0,
                    .decimal_scale = 0,
                    .field_id = 2,
                    .stats_min_i64 = 10,
                    .stats_max_i64 = 20,
                    .stats_min_bytes = try alloc.dupe(u8, "acct:a"),
                    .stats_max_bytes = try alloc.dupe(u8, "acct:z"),
                    .stats_min_bool = true,
                    .stats_max_bool = true,
                    .stats_min_f64 = 1.25,
                    .stats_max_f64 = 2.5,
                    .nullable = true,
                }}),
            },
        }),
    };

    const encoded = try encodeAlloc(alloc, inventory);
    defer alloc.free(encoded);

    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, decoded.format);
    try std.testing.expectEqualStrings("iceberg-123", decoded.snapshot_id);
    try std.testing.expectEqual(@as(usize, 1), decoded.files.len);
    try std.testing.expectEqualStrings("file-a.parquet", decoded.files[0].file_id);
    try std.testing.expectEqualStrings("etag-file-a", decoded.files[0].etag);
    try std.testing.expectEqualStrings("version-file-a", decoded.files[0].version_id);
    try std.testing.expectEqual(@as(?i64, 42), decoded.files[0].data_sequence_number);
    try std.testing.expectEqual(@as(?i32, 7), decoded.files[0].partition_spec_id);
    try std.testing.expectEqual(@as(u32, 1), decoded.files[0].partition_field_count);
    try std.testing.expectEqual(@as(usize, 1), decoded.files[0].partition_values.len);
    try std.testing.expectEqualStrings("region", decoded.files[0].partition_values[0].column_id);
    try std.testing.expectEqualStrings("us-west", decoded.files[0].partition_values[0].string_value);
    try std.testing.expectEqual(@as(u64, 2), decoded.files[0].row_groups[0].row_count);
    try std.testing.expectEqual(@as(u64, 512), decoded.files[0].row_groups[0].total_byte_len);
    try std.testing.expectEqual(@as(usize, 1), decoded.files[0].row_groups[0].column_chunks.len);
    try std.testing.expectEqualStrings("amount", decoded.files[0].row_groups[0].column_chunks[0].column_id);
    try std.testing.expectEqual(@as(u64, 128), decoded.files[0].row_groups[0].column_chunks[0].file_offset);
    try std.testing.expectEqualStrings("zstd", decoded.files[0].row_groups[0].column_chunks[0].compression_codec);
    try std.testing.expectEqualStrings("int64", decoded.files[0].row_groups[0].column_chunks[0].physical_type);
    try std.testing.expectEqual(@as(i32, 0), decoded.files[0].row_groups[0].column_chunks[0].type_length);
    try std.testing.expectEqualStrings("timestamp_micros", decoded.files[0].row_groups[0].column_chunks[0].logical_type);
    try std.testing.expectEqual(@as(i32, 0), decoded.files[0].row_groups[0].column_chunks[0].decimal_precision);
    try std.testing.expectEqual(@as(i32, 0), decoded.files[0].row_groups[0].column_chunks[0].decimal_scale);
    try std.testing.expectEqual(@as(?i32, 2), decoded.files[0].row_groups[0].column_chunks[0].field_id);
    try std.testing.expectEqual(@as(?i64, 10), decoded.files[0].row_groups[0].column_chunks[0].stats_min_i64);
    try std.testing.expectEqual(@as(?i64, 20), decoded.files[0].row_groups[0].column_chunks[0].stats_max_i64);
    try std.testing.expectEqualStrings("acct:a", decoded.files[0].row_groups[0].column_chunks[0].stats_min_bytes.?);
    try std.testing.expectEqualStrings("acct:z", decoded.files[0].row_groups[0].column_chunks[0].stats_max_bytes.?);
    try std.testing.expectEqual(@as(?bool, true), decoded.files[0].row_groups[0].column_chunks[0].stats_min_bool);
    try std.testing.expectEqual(@as(?bool, true), decoded.files[0].row_groups[0].column_chunks[0].stats_max_bool);
    try std.testing.expectEqual(@as(?f64, 1.25), decoded.files[0].row_groups[0].column_chunks[0].stats_min_f64);
    try std.testing.expectEqual(@as(?f64, 2.5), decoded.files[0].row_groups[0].column_chunks[0].stats_max_f64);
    try std.testing.expect(decoded.files[0].row_groups[0].column_chunks[0].nullable);
}

test "external source inventory codec rejects forged counts before allocation" {
    const alloc = std.testing.allocator;
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);
    try encoded.appendSlice(alloc, magic);
    try appendU32(alloc, &encoded, version);
    try encoded.append(alloc, @intFromEnum(external_source.Format.parquet));
    try appendBytes(alloc, &encoded, "source");
    try appendBytes(alloc, &encoded, "s3://bucket/source");
    try appendBytes(alloc, &encoded, "snapshot");
    try appendBytes(alloc, &encoded, "schema");
    try appendU32(alloc, &encoded, std.math.maxInt(u32));

    try std.testing.expectError(error.InvalidExternalSourceInventory, decodeAlloc(alloc, encoded.items));
}
