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

//! Parquet compact-Thrift footer metadata parsing.
//!
//! This is the bridge from validated Parquet footer bytes to Antfly's external
//! inventory row-group and column-chunk model. Data page decoding is separate.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_source = @import("../external_source/types.zig");

const CompactType = enum(u4) {
    stop = 0,
    boolean_true = 1,
    boolean_false = 2,
    byte = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    struct_ = 12,
};

pub const ParsedFooter = struct {
    version: i32,
    row_count: u64,
    row_groups: []external_source.RowGroup,

    pub fn deinit(self: *ParsedFooter, alloc: Allocator) void {
        for (self.row_groups) |*row_group| row_group.deinit(alloc);
        alloc.free(self.row_groups);
        self.* = undefined;
    }
};

pub fn parseFooterMetadataAlloc(
    alloc: Allocator,
    footer_metadata: []const u8,
    file_len: u64,
) !ParsedFooter {
    if (footer_metadata.len == 0) return error.InvalidParquetMetadata;
    var reader = Reader{ .bytes = footer_metadata };
    var footer = try parseFileMetadata(alloc, &reader, file_len);
    errdefer footer.deinit(alloc);
    if (reader.cursor != reader.bytes.len) return error.InvalidParquetMetadata;
    return footer;
}

fn parseFileMetadata(alloc: Allocator, reader: *Reader, file_len: u64) !ParsedFooter {
    var previous_field_id: i16 = 0;
    var version: ?i32 = null;
    var row_count: ?u64 = null;
    var row_groups: ?[]external_source.RowGroup = null;
    errdefer if (row_groups) |groups| {
        for (groups) |*group| group.deinit(alloc);
        alloc.free(groups);
    };

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => version = try reader.readRequiredI32(field.type),
            3 => row_count = try reader.readRequiredU64(field.type),
            4 => {
                if (field.type != .list) return error.InvalidParquetMetadata;
                row_groups = try parseRowGroupList(alloc, reader, file_len);
            },
            else => try reader.skip(field.type),
        }
    }

    const got_version = version orelse return error.InvalidParquetMetadata;
    const got_row_count = row_count orelse return error.InvalidParquetMetadata;
    const got_row_groups = row_groups orelse return error.InvalidParquetMetadata;

    var total_rows: u64 = 0;
    for (got_row_groups) |group| total_rows += group.row_count;
    if (got_row_groups.len != 0 and total_rows != got_row_count) return error.InvalidParquetMetadata;
    row_groups = null;

    return .{
        .version = got_version,
        .row_count = got_row_count,
        .row_groups = got_row_groups,
    };
}

fn parseRowGroupList(alloc: Allocator, reader: *Reader, file_len: u64) ![]external_source.RowGroup {
    const list = try reader.readListHeader();
    if (list.elem_type != .struct_) return error.InvalidParquetMetadata;

    const row_groups = try alloc.alloc(external_source.RowGroup, list.len);
    errdefer alloc.free(row_groups);
    var initialized: usize = 0;
    errdefer {
        for (row_groups[0..initialized]) |*group| group.deinit(alloc);
    }

    for (row_groups, 0..) |*group, idx| {
        group.* = try parseRowGroup(alloc, reader, file_len, @intCast(idx));
        initialized += 1;
    }
    return row_groups;
}

fn parseRowGroup(
    alloc: Allocator,
    reader: *Reader,
    file_len: u64,
    fallback_ordinal: u32,
) !external_source.RowGroup {
    var previous_field_id: i16 = 0;
    var row_count: ?u64 = null;
    var total_byte_len: u64 = 0;
    var file_offset: u64 = 0;
    var ordinal: u32 = fallback_ordinal;
    var column_chunks: ?[]external_source.ColumnChunk = null;
    errdefer if (column_chunks) |chunks| {
        for (chunks) |*chunk| chunk.deinit(alloc);
        alloc.free(chunks);
    };

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => {
                if (field.type != .list) return error.InvalidParquetMetadata;
                column_chunks = try parseColumnChunkList(alloc, reader, file_len);
            },
            2 => total_byte_len = try reader.readRequiredU64(field.type),
            3 => row_count = try reader.readRequiredU64(field.type),
            5 => file_offset = try reader.readRequiredU64(field.type),
            7 => ordinal = @intCast(try reader.readRequiredU64(field.type)),
            else => try reader.skip(field.type),
        }
    }

    const got_chunks = column_chunks orelse return error.InvalidParquetMetadata;
    column_chunks = null;
    const group = external_source.RowGroup{
        .ordinal = ordinal,
        .row_count = row_count orelse return error.InvalidParquetMetadata,
        .file_offset = file_offset,
        .total_byte_len = total_byte_len,
        .column_chunks = got_chunks,
    };
    group.validate(file_len) catch return error.InvalidParquetMetadata;
    return group;
}

fn parseColumnChunkList(alloc: Allocator, reader: *Reader, file_len: u64) ![]external_source.ColumnChunk {
    const list = try reader.readListHeader();
    if (list.elem_type != .struct_) return error.InvalidParquetMetadata;

    const chunks = try alloc.alloc(external_source.ColumnChunk, list.len);
    errdefer alloc.free(chunks);
    var initialized: usize = 0;
    errdefer {
        for (chunks[0..initialized]) |*chunk| chunk.deinit(alloc);
    }

    for (chunks) |*chunk| {
        chunk.* = try parseColumnChunk(alloc, reader, file_len);
        initialized += 1;
    }
    return chunks;
}

fn parseColumnChunk(alloc: Allocator, reader: *Reader, file_len: u64) !external_source.ColumnChunk {
    var previous_field_id: i16 = 0;
    var chunk_file_offset: ?u64 = null;
    var metadata: ?ColumnMetadata = null;
    errdefer if (metadata) |*meta| meta.deinit(alloc);

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            2 => chunk_file_offset = try reader.readRequiredU64(field.type),
            3 => {
                if (field.type != .struct_) return error.InvalidParquetMetadata;
                metadata = try parseColumnMetadata(alloc, reader);
            },
            else => try reader.skip(field.type),
        }
    }

    var meta = metadata orelse return error.InvalidParquetMetadata;
    metadata = null;
    errdefer meta.deinit(alloc);
    const start_offset = meta.dictionary_page_offset orelse meta.data_page_offset orelse chunk_file_offset orelse return error.InvalidParquetMetadata;
    const chunk = external_source.ColumnChunk{
        .column_id = meta.column_id,
        .file_offset = start_offset,
        .compressed_len = meta.total_compressed_size,
        .uncompressed_len = meta.total_uncompressed_size,
        .compression_codec = meta.compression_codec,
        .encoding = meta.encoding,
    };
    chunk.validate(file_len) catch return error.InvalidParquetMetadata;
    meta.disown();
    return chunk;
}

const ColumnMetadata = struct {
    column_id: []u8,
    compression_codec: []u8,
    encoding: []u8,
    total_compressed_size: u64,
    total_uncompressed_size: u64,
    data_page_offset: ?u64 = null,
    dictionary_page_offset: ?u64 = null,

    fn deinit(self: *ColumnMetadata, alloc: Allocator) void {
        if (self.column_id.len > 0) alloc.free(self.column_id);
        if (self.compression_codec.len > 0) alloc.free(self.compression_codec);
        if (self.encoding.len > 0) alloc.free(self.encoding);
        self.* = undefined;
    }

    fn disown(self: *ColumnMetadata) void {
        self.column_id = &.{};
        self.compression_codec = &.{};
        self.encoding = &.{};
    }
};

fn parseColumnMetadata(alloc: Allocator, reader: *Reader) !ColumnMetadata {
    var previous_field_id: i16 = 0;
    var column_id: ?[]u8 = null;
    var compression_codec: ?[]u8 = null;
    var encoding: []u8 = &.{};
    var total_compressed_size: ?u64 = null;
    var total_uncompressed_size: ?u64 = null;
    var data_page_offset: ?u64 = null;
    var dictionary_page_offset: ?u64 = null;
    errdefer if (column_id) |value| alloc.free(value);
    errdefer if (compression_codec) |value| alloc.free(value);
    errdefer if (encoding.len > 0) alloc.free(encoding);

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            2 => encoding = try parseFirstEncodingAlloc(alloc, reader, field.type),
            3 => column_id = try parsePathInSchemaAlloc(alloc, reader, field.type),
            4 => compression_codec = try compressionCodecNameAlloc(alloc, try reader.readRequiredI32(field.type)),
            6 => total_uncompressed_size = try reader.readRequiredU64(field.type),
            7 => total_compressed_size = try reader.readRequiredU64(field.type),
            9 => data_page_offset = try reader.readRequiredU64(field.type),
            11 => dictionary_page_offset = try reader.readRequiredU64(field.type),
            else => try reader.skip(field.type),
        }
    }

    return .{
        .column_id = column_id orelse return error.InvalidParquetMetadata,
        .compression_codec = compression_codec orelse try alloc.dupe(u8, "unknown"),
        .encoding = encoding,
        .total_compressed_size = total_compressed_size orelse return error.InvalidParquetMetadata,
        .total_uncompressed_size = total_uncompressed_size orelse 0,
        .data_page_offset = data_page_offset,
        .dictionary_page_offset = dictionary_page_offset,
    };
}

fn parsePathInSchemaAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) ![]u8 {
    if (field_type != .list) return error.InvalidParquetMetadata;
    const list = try reader.readListHeader();
    if (list.elem_type != .binary) return error.InvalidParquetMetadata;
    if (list.len == 0) return error.InvalidParquetMetadata;

    var parts = try alloc.alloc([]u8, list.len);
    defer alloc.free(parts);
    var initialized: usize = 0;
    errdefer {
        for (parts[0..initialized]) |part| alloc.free(part);
    }
    var total_len: usize = 0;
    for (parts) |*part| {
        part.* = try reader.readBinaryAlloc(alloc);
        if (part.len == 0) return error.InvalidParquetMetadata;
        total_len += part.len;
        initialized += 1;
    }
    const out = try alloc.alloc(u8, total_len + parts.len - 1);
    var cursor: usize = 0;
    for (parts, 0..) |part, idx| {
        if (idx != 0) {
            out[cursor] = '.';
            cursor += 1;
        }
        @memcpy(out[cursor .. cursor + part.len], part);
        cursor += part.len;
    }
    for (parts) |part| alloc.free(part);
    return out;
}

fn parseFirstEncodingAlloc(alloc: Allocator, reader: *Reader, field_type: CompactType) ![]u8 {
    if (field_type != .list) return error.InvalidParquetMetadata;
    const list = try reader.readListHeader();
    if (list.elem_type != .i32) return error.InvalidParquetMetadata;
    var first: ?i32 = null;
    for (0..list.len) |idx| {
        const value = try reader.readI32();
        if (idx == 0) first = value;
    }
    return try alloc.dupe(u8, encodingName(first orelse -1));
}

fn compressionCodecNameAlloc(alloc: Allocator, codec: i32) ![]u8 {
    return try alloc.dupe(u8, switch (codec) {
        0 => "uncompressed",
        1 => "snappy",
        2 => "gzip",
        3 => "lzo",
        4 => "brotli",
        5 => "lz4",
        6 => "zstd",
        7 => "lz4_raw",
        else => "unknown",
    });
}

fn encodingName(encoding: i32) []const u8 {
    return switch (encoding) {
        0 => "plain",
        1 => "plain_dictionary",
        2 => "rle",
        3 => "bit_packed",
        4 => "delta_binary_packed",
        5 => "delta_length_byte_array",
        6 => "delta_byte_array",
        7 => "rle_dictionary",
        8 => "byte_stream_split",
        else => "unknown",
    };
}

const Field = struct {
    id: i16,
    type: CompactType,
};

const ListHeader = struct {
    elem_type: CompactType,
    len: usize,
};

const Reader = struct {
    bytes: []const u8,
    cursor: usize = 0,

    fn readFieldHeader(self: *Reader, previous_field_id: *i16) !?Field {
        const raw = try self.readByte();
        const field_type: CompactType = @enumFromInt(raw & 0x0f);
        if (field_type == .stop) return null;
        const delta: i16 = @intCast(raw >> 4);
        const field_id = if (delta == 0) try self.readI16() else previous_field_id.* + delta;
        previous_field_id.* = field_id;
        return .{ .id = field_id, .type = field_type };
    }

    fn readListHeader(self: *Reader) !ListHeader {
        const raw = try self.readByte();
        const elem_type: CompactType = @enumFromInt(raw & 0x0f);
        const inline_len = raw >> 4;
        const len = if (inline_len == 15) try self.readVarintUsize() else inline_len;
        return .{ .elem_type = elem_type, .len = len };
    }

    fn readRequiredI32(self: *Reader, field_type: CompactType) !i32 {
        if (field_type != .i32) return error.InvalidParquetMetadata;
        return try self.readI32();
    }

    fn readRequiredU64(self: *Reader, field_type: CompactType) !u64 {
        if (field_type != .i64) return error.InvalidParquetMetadata;
        const value = try self.readI64();
        if (value < 0) return error.InvalidParquetMetadata;
        return @intCast(value);
    }

    fn readI16(self: *Reader) !i16 {
        return @intCast(zigzagDecode(try self.readVarintU64()));
    }

    fn readI32(self: *Reader) !i32 {
        return @intCast(zigzagDecode(try self.readVarintU64()));
    }

    fn readI64(self: *Reader) !i64 {
        return zigzagDecode(try self.readVarintU64());
    }

    fn readBinaryAlloc(self: *Reader, alloc: Allocator) ![]u8 {
        const len = try self.readVarintUsize();
        if (len > self.bytes.len - self.cursor) return error.InvalidParquetMetadata;
        const out = try alloc.dupe(u8, self.bytes[self.cursor .. self.cursor + len]);
        self.cursor += len;
        return out;
    }

    fn readByte(self: *Reader) !u8 {
        if (self.cursor >= self.bytes.len) return error.InvalidParquetMetadata;
        const out = self.bytes[self.cursor];
        self.cursor += 1;
        return out;
    }

    fn readVarintUsize(self: *Reader) !usize {
        return std.math.cast(usize, try self.readVarintU64()) orelse error.InvalidParquetMetadata;
    }

    fn readVarintU64(self: *Reader) !u64 {
        var shift: u6 = 0;
        var result: u64 = 0;
        while (true) {
            const byte = try self.readByte();
            result |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return result;
            if (shift >= 63) return error.InvalidParquetMetadata;
            shift += 7;
        }
    }

    fn skip(self: *Reader, field_type: CompactType) !void {
        switch (field_type) {
            .stop => {},
            .boolean_true, .boolean_false => {},
            .byte => _ = try self.readByte(),
            .i16 => _ = try self.readI16(),
            .i32 => _ = try self.readI32(),
            .i64 => _ = try self.readI64(),
            .double => {
                if (self.bytes.len - self.cursor < 8) return error.InvalidParquetMetadata;
                self.cursor += 8;
            },
            .binary => {
                const len = try self.readVarintUsize();
                if (len > self.bytes.len - self.cursor) return error.InvalidParquetMetadata;
                self.cursor += len;
            },
            .list, .set => {
                const list = try self.readListHeader();
                for (0..list.len) |_| try self.skip(list.elem_type);
            },
            .map => {
                const count = try self.readVarintUsize();
                if (count == 0) return;
                const types = try self.readByte();
                const key_type: CompactType = @enumFromInt(types >> 4);
                const value_type: CompactType = @enumFromInt(types & 0x0f);
                for (0..count) |_| {
                    try self.skip(key_type);
                    try self.skip(value_type);
                }
            },
            .struct_ => {
                var previous_field_id: i16 = 0;
                while (try self.readFieldHeader(&previous_field_id)) |field| try self.skip(field.type);
            },
        }
    }
};

fn zigzagDecode(raw: u64) i64 {
    return @as(i64, @bitCast(raw >> 1)) ^ -@as(i64, @intCast(raw & 1));
}

fn appendField(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, previous: *i16, id: i16, field_type: CompactType) !void {
    const delta = id - previous.*;
    if (delta > 0 and delta <= 15) {
        try out.append(alloc, (@as(u8, @intCast(delta)) << 4) | @intFromEnum(field_type));
    } else {
        try out.append(alloc, @intFromEnum(field_type));
        try appendI16(out, alloc, id);
    }
    previous.* = id;
}

fn appendStop(out: *std.ArrayListUnmanaged(u8), alloc: Allocator) !void {
    try out.append(alloc, 0);
}

fn appendListHeader(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, elem_type: CompactType, len: usize) !void {
    if (len < 15) {
        try out.append(alloc, (@as(u8, @intCast(len)) << 4) | @as(u8, @intFromEnum(elem_type)));
    } else {
        try out.append(alloc, 0xf0 | @as(u8, @intFromEnum(elem_type)));
        try appendVarint(out, alloc, len);
    }
}

fn appendI16(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i16) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI32(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i32) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI64(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i64) !void {
    try appendZigzag(out, alloc, value);
}

fn appendZigzag(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: anytype) !void {
    const Int = @TypeOf(value);
    const Unsigned = std.meta.Int(.unsigned, @bitSizeOf(Int));
    const encoded: Unsigned = @bitCast((value << 1) ^ (value >> (@bitSizeOf(Int) - 1)));
    try appendVarint(out, alloc, encoded);
}

fn appendVarint(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: anytype) !void {
    var remaining: u64 = @intCast(value);
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn appendBinary(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    try appendVarint(out, alloc, bytes.len);
    try out.appendSlice(alloc, bytes);
}

test "parquet metadata parser extracts row groups and column chunks" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnMetadataFixture(alloc);
    defer bytes.deinit(alloc);

    var footer = try parseFooterMetadataAlloc(alloc, bytes.items, 1024);
    defer footer.deinit(alloc);

    try std.testing.expectEqual(@as(i32, 1), footer.version);
    try std.testing.expectEqual(@as(u64, 2), footer.row_count);
    try std.testing.expectEqual(@as(usize, 1), footer.row_groups.len);
    try std.testing.expectEqual(@as(u32, 0), footer.row_groups[0].ordinal);
    try std.testing.expectEqual(@as(u64, 2), footer.row_groups[0].row_count);
    try std.testing.expectEqual(@as(u64, 100), footer.row_groups[0].file_offset);
    try std.testing.expectEqual(@as(u64, 80), footer.row_groups[0].total_byte_len);
    try std.testing.expectEqual(@as(usize, 1), footer.row_groups[0].column_chunks.len);
    try std.testing.expectEqualStrings("amount", footer.row_groups[0].column_chunks[0].column_id);
    try std.testing.expectEqualStrings("zstd", footer.row_groups[0].column_chunks[0].compression_codec);
    try std.testing.expectEqualStrings("plain", footer.row_groups[0].column_chunks[0].encoding);
    try std.testing.expectEqual(@as(u64, 100), footer.row_groups[0].column_chunks[0].file_offset);
    try std.testing.expectEqual(@as(u64, 40), footer.row_groups[0].column_chunks[0].compressed_len);
}

test "parquet metadata parser rejects inconsistent row counts and ranges" {
    const alloc = std.testing.allocator;
    var bytes = try buildSingleColumnMetadataFixture(alloc);
    defer bytes.deinit(alloc);

    try std.testing.expectError(error.InvalidParquetMetadata, parseFooterMetadataAlloc(alloc, bytes.items, 120));

    // Patch FileMetaData.num_rows from 2 to 3. This offset is stable for the
    // fixture: field1(version) is two bytes, field3 header is the third byte,
    // and the value byte follows it.
    bytes.items[3] = 6;
    try std.testing.expectError(error.InvalidParquetMetadata, parseFooterMetadataAlloc(alloc, bytes.items, 1024));
}

fn buildSingleColumnMetadataFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var file_prev: i16 = 0;
    try appendField(&out, alloc, &file_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &file_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &file_prev, 4, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var rg_prev: i16 = 0;
    try appendField(&out, alloc, &rg_prev, 1, .list);
    try appendListHeader(&out, alloc, .struct_, 1);

    var chunk_prev: i16 = 0;
    try appendField(&out, alloc, &chunk_prev, 2, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &chunk_prev, 3, .struct_);

    var meta_prev: i16 = 0;
    try appendField(&out, alloc, &meta_prev, 1, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &meta_prev, 2, .list);
    try appendListHeader(&out, alloc, .i32, 1);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &meta_prev, 3, .list);
    try appendListHeader(&out, alloc, .binary, 1);
    try appendBinary(&out, alloc, "amount");
    try appendField(&out, alloc, &meta_prev, 4, .i32);
    try appendI32(&out, alloc, 6);
    try appendField(&out, alloc, &meta_prev, 5, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &meta_prev, 6, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &meta_prev, 7, .i64);
    try appendI64(&out, alloc, 40);
    try appendField(&out, alloc, &meta_prev, 9, .i64);
    try appendI64(&out, alloc, 120);
    try appendField(&out, alloc, &meta_prev, 11, .i64);
    try appendI64(&out, alloc, 100);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);

    try appendField(&out, alloc, &rg_prev, 2, .i64);
    try appendI64(&out, alloc, 80);
    try appendField(&out, alloc, &rg_prev, 3, .i64);
    try appendI64(&out, alloc, 2);
    try appendField(&out, alloc, &rg_prev, 5, .i64);
    try appendI64(&out, alloc, 100);
    try appendField(&out, alloc, &rg_prev, 7, .i64);
    try appendI64(&out, alloc, 0);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}
