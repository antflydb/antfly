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

//! Minimal Parquet page header parsing and PLAIN value decoding.
//!
//! This supports the first flat required-column scan paths. Repetition levels
//! and nested data are separate scanner layers.

const std = @import("std");
const Allocator = std.mem.Allocator;
const snappy = @import("../../encoding/snappy.zig");

/// Parquet pages are normally around 1 MiB. Keep enough headroom for existing
/// writers while preventing an external page header or compressed payload from
/// requesting process-scale allocations.
pub const max_uncompressed_page_bytes: usize = 64 * 1024 * 1024;
/// A compact RLE level stream can claim billions of values while remaining a
/// few bytes long. Bound decoded vectors independently from payload bytes.
pub const max_values_per_page: usize = max_uncompressed_page_bytes / @sizeOf(i64);
pub const max_pages_per_column_chunk: usize = 1_000_000;

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

pub const PageType = enum(i32) {
    data_page = 0,
    index_page = 1,
    dictionary_page = 2,
    data_page_v2 = 3,
};

pub const Encoding = enum(i32) {
    plain = 0,
    plain_dictionary = 1,
    rle = 2,
    bit_packed = 3,
    delta_binary_packed = 4,
    delta_length_byte_array = 5,
    delta_byte_array = 6,
    rle_dictionary = 7,
    byte_stream_split = 8,
};

pub const CompressionCodec = enum {
    uncompressed,
    snappy,
    gzip,
    zstd,
};

pub const Header = struct {
    page_type: PageType,
    uncompressed_page_size: u32,
    compressed_page_size: u32,
    value_count: u32,
    encoding: Encoding,
    definition_level_bytes: usize = 0,
    repetition_level_bytes: usize = 0,
    data_payload_offset: usize = 0,
    data_is_compressed: bool = true,

    pub fn validatePlainDictionary(self: Header) !void {
        try self.validateResourceLimits();
        if (self.page_type != .dictionary_page) return error.UnsupportedParquetPage;
        if (self.encoding != .plain) return error.UnsupportedParquetPage;
    }

    pub fn validatePlainRequired(self: Header) !void {
        try self.validateResourceLimits();
        if (self.page_type != .data_page and self.page_type != .data_page_v2) return error.UnsupportedParquetPage;
        if (self.encoding != .plain) return error.UnsupportedParquetPage;
    }

    pub fn validateDictionaryRequired(self: Header) !void {
        try self.validateResourceLimits();
        if (self.page_type != .data_page and self.page_type != .data_page_v2) return error.UnsupportedParquetPage;
        if (self.encoding != .rle_dictionary and self.encoding != .plain_dictionary) return error.UnsupportedParquetPage;
    }

    pub fn validateUncompressedPlainRequired(self: Header) !void {
        try self.validatePlainRequired();
        if (self.compressed_page_size != self.uncompressed_page_size) return error.UnsupportedParquetPage;
    }

    pub fn validateUncompressedDictionaryRequired(self: Header) !void {
        try self.validateDictionaryRequired();
        if (self.compressed_page_size != self.uncompressed_page_size) return error.UnsupportedParquetPage;
    }

    pub fn validateResourceLimits(self: Header) !void {
        if (self.value_count > max_values_per_page) return error.ParquetPageTooLarge;
        if (self.uncompressed_page_size > max_uncompressed_page_bytes) return error.ParquetPageTooLarge;
    }
};

pub const ParsedHeader = struct {
    header: Header,
    header_len: usize,
};

pub const ColumnChunkResourceUsage = struct {
    data_value_count: u64 = 0,
    uncompressed_bytes: usize = 0,
    page_count: usize = 0,
};

/// Validate every page boundary and compute cumulative decode work before any
/// page payload is decompressed or any output vector is allocated.
pub fn inspectColumnChunkResourceUsage(column_chunk_bytes: []const u8) !ColumnChunkResourceUsage {
    if (column_chunk_bytes.len == 0) return error.InvalidParquetPage;
    var usage = ColumnChunkResourceUsage{};
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        if (usage.page_count == max_pages_per_column_chunk) return error.ParquetPageTooLarge;
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        cursor = std.math.add(usize, cursor, parsed.header_len) catch return error.InvalidParquetPage;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        cursor += parsed.header.compressed_page_size;
        usage.uncompressed_bytes = std.math.add(
            usize,
            usage.uncompressed_bytes,
            parsed.header.uncompressed_page_size,
        ) catch return error.ParquetColumnChunkTooLarge;
        switch (parsed.header.page_type) {
            .data_page, .data_page_v2 => usage.data_value_count = std.math.add(
                u64,
                usage.data_value_count,
                parsed.header.value_count,
            ) catch return error.ParquetColumnChunkTooLarge,
            .dictionary_page, .index_page => {},
        }
        usage.page_count += 1;
    }
    return usage;
}

const PagePayload = struct {
    bytes: []u8,

    fn deinit(self: PagePayload, alloc: Allocator) void {
        alloc.free(self.bytes);
    }
};

pub const NullableI64Values = struct {
    values: []i64,
    nulls: []u8,

    pub fn deinit(self: *NullableI64Values, alloc: Allocator) void {
        alloc.free(self.values);
        alloc.free(self.nulls);
        self.* = undefined;
    }
};

pub const NullableF64Values = struct {
    values: []f64,
    nulls: []u8,

    pub fn deinit(self: *NullableF64Values, alloc: Allocator) void {
        alloc.free(self.values);
        alloc.free(self.nulls);
        self.* = undefined;
    }
};

pub const NullableBoolValues = struct {
    values: []bool,
    nulls: []u8,

    pub fn deinit(self: *NullableBoolValues, alloc: Allocator) void {
        alloc.free(self.values);
        alloc.free(self.nulls);
        self.* = undefined;
    }
};

pub const NullableByteArrayValues = struct {
    values: [][]u8,
    nulls: []u8,

    pub fn deinit(self: *NullableByteArrayValues, alloc: Allocator) void {
        freePlainByteArrays(alloc, self.values);
        alloc.free(self.nulls);
        self.* = undefined;
    }
};

pub fn parsePageHeader(bytes: []const u8) !ParsedHeader {
    var reader = Reader{ .bytes = bytes };
    const header = try parseHeaderStruct(&reader);
    try header.validateResourceLimits();
    return .{ .header = header, .header_len = reader.cursor };
}

pub fn decodePlainI64Alloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]i64 {
    try header.validatePlainRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    const count: usize = @intCast(header.value_count);
    const needed = count * 8;
    if (payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(i64, count);
    for (values, 0..) |*value, idx| {
        value.* = std.mem.readInt(i64, payload[idx * 8 .. idx * 8 + 8][0..8], .little);
    }
    return values;
}

pub fn scanUncompressedPlainI64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]i64 {
    return try scanPlainI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn decodePlainInt96TimestampNsAlloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]i64 {
    try header.validatePlainRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    const count: usize = @intCast(header.value_count);
    const needed = count * 12;
    if (payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(i64, count);
    for (values, 0..) |*value, idx| {
        const offset = idx * 12;
        value.* = try int96TimestampNs(payload[offset .. offset + 12][0..12]);
    }
    return values;
}

pub fn scanUncompressedPlainInt96TimestampNsColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]i64 {
    return try scanPlainInt96TimestampNsColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn decodePlainF64Alloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]f64 {
    try header.validatePlainRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    const count: usize = @intCast(header.value_count);
    const needed = count * 8;
    if (payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(f64, count);
    for (values, 0..) |*value, idx| {
        const bits = std.mem.readInt(u64, payload[idx * 8 .. idx * 8 + 8][0..8], .little);
        value.* = @bitCast(bits);
    }
    return values;
}

pub fn scanUncompressedPlainF64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]f64 {
    return try scanPlainF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn decodePlainI32AsI64Alloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]i64 {
    try header.validatePlainRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    const count: usize = @intCast(header.value_count);
    const needed = count * 4;
    if (payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(i64, count);
    for (values, 0..) |*value, idx| {
        value.* = std.mem.readInt(i32, payload[idx * 4 .. idx * 4 + 4][0..4], .little);
    }
    return values;
}

pub fn scanUncompressedPlainI32AsI64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]i64 {
    return try scanPlainI32AsI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn decodePlainF32AsF64Alloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]f64 {
    try header.validatePlainRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    const count: usize = @intCast(header.value_count);
    const needed = count * 4;
    if (payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(f64, count);
    for (values, 0..) |*value, idx| {
        const bits = std.mem.readInt(u32, payload[idx * 4 .. idx * 4 + 4][0..4], .little);
        value.* = @floatCast(@as(f32, @bitCast(bits)));
    }
    return values;
}

pub fn scanUncompressedPlainF32AsF64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]f64 {
    return try scanPlainF32AsF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn decodePlainBoolAlloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]bool {
    try header.validatePlainRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    const count: usize = @intCast(header.value_count);
    const needed = (count + 7) / 8;
    if (payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(bool, count);
    for (values, 0..) |*value, idx| {
        value.* = readBitPackedBool(payload, idx);
    }
    return values;
}

pub fn scanUncompressedPlainBoolColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]bool {
    return try scanPlainBoolColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanPlainBoolColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]bool {
    var values = std.ArrayListUnmanaged(bool).empty;
    errdefer values.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainBoolAlloc(alloc, parsed.header, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanPlainF32AsF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]f64 {
    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainF32AsF64Alloc(alloc, parsed.header, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanPlainI32AsI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]i64 {
    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainI32AsI64Alloc(alloc, parsed.header, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanPlainI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]i64 {
    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainI64Alloc(alloc, parsed.header, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanPlainInt96TimestampNsColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]i64 {
    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainInt96TimestampNsAlloc(alloc, parsed.header, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanPlainF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]f64 {
    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainF64Alloc(alloc, parsed.header, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn decodePlainI64DictionaryPageAlloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]i64 {
    try header.validatePlainDictionary();
    const count: usize = @intCast(header.value_count);
    const needed = count * 8;
    if (page_payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(i64, count);
    for (values, 0..) |*value, idx| {
        value.* = std.mem.readInt(i64, page_payload[idx * 8 .. idx * 8 + 8][0..8], .little);
    }
    return values;
}

pub fn decodePlainF64DictionaryPageAlloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]f64 {
    try header.validatePlainDictionary();
    const count: usize = @intCast(header.value_count);
    const needed = count * 8;
    if (page_payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(f64, count);
    for (values, 0..) |*value, idx| {
        const bits = std.mem.readInt(u64, page_payload[idx * 8 .. idx * 8 + 8][0..8], .little);
        value.* = @bitCast(bits);
    }
    return values;
}

pub fn decodePlainF32DictionaryPageAsF64Alloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]f64 {
    try header.validatePlainDictionary();
    const count: usize = @intCast(header.value_count);
    const needed = count * 4;
    if (page_payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(f64, count);
    for (values, 0..) |*value, idx| {
        const bits = std.mem.readInt(u32, page_payload[idx * 4 .. idx * 4 + 4][0..4], .little);
        value.* = @floatCast(@as(f32, @bitCast(bits)));
    }
    return values;
}

pub fn decodePlainI32DictionaryPageAsI64Alloc(alloc: Allocator, header: Header, page_payload: []const u8) ![]i64 {
    try header.validatePlainDictionary();
    const count: usize = @intCast(header.value_count);
    const needed = count * 4;
    if (page_payload.len < needed) return error.InvalidParquetPage;
    const values = try alloc.alloc(i64, count);
    for (values, 0..) |*value, idx| {
        value.* = std.mem.readInt(i32, page_payload[idx * 4 .. idx * 4 + 4][0..4], .little);
    }
    return values;
}

pub fn decodeDictionaryI64DataPageAlloc(
    alloc: Allocator,
    header: Header,
    dictionary: []const i64,
    page_payload: []const u8,
) ![]i64 {
    try header.validateDictionaryRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    if (payload.len == 0) return error.InvalidParquetPage;
    const bit_width_raw = payload[0];
    if (bit_width_raw > 32) return error.UnsupportedParquetPage;
    const bit_width: u6 = @intCast(bit_width_raw);
    const row_count: usize = @intCast(header.value_count);
    const indexes = try decodeHybridIndexesAlloc(alloc, payload[1..], bit_width, row_count);
    defer alloc.free(indexes);
    const values = try alloc.alloc(i64, row_count);
    errdefer alloc.free(values);
    for (indexes, values) |index, *value| {
        if (index >= dictionary.len) return error.InvalidParquetPage;
        value.* = dictionary[@intCast(index)];
    }
    return values;
}

pub fn decodeDictionaryF64DataPageAlloc(
    alloc: Allocator,
    header: Header,
    dictionary: []const f64,
    page_payload: []const u8,
) ![]f64 {
    try header.validateDictionaryRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    if (payload.len == 0) return error.InvalidParquetPage;
    const bit_width_raw = payload[0];
    if (bit_width_raw > 32) return error.UnsupportedParquetPage;
    const bit_width: u6 = @intCast(bit_width_raw);
    const row_count: usize = @intCast(header.value_count);
    const indexes = try decodeHybridIndexesAlloc(alloc, payload[1..], bit_width, row_count);
    defer alloc.free(indexes);
    const values = try alloc.alloc(f64, row_count);
    errdefer alloc.free(values);
    for (indexes, values) |index, *value| {
        if (index >= dictionary.len) return error.InvalidParquetPage;
        value.* = dictionary[@intCast(index)];
    }
    return values;
}

pub fn decodeOptionalDictionaryI64V2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    dictionary: []const i64,
    page_payload: []const u8,
) !NullableI64Values {
    try header.validateDictionaryRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    var present_count: usize = 0;
    for (definition_levels) |definition_level| {
        if (definition_level > 1) return error.InvalidParquetPage;
        if (definition_level == 1) present_count += 1;
    }

    const payload = page_payload[header.data_payload_offset..];
    if (present_count > 0 and payload.len == 0) return error.InvalidParquetPage;
    const bit_width_raw: u8 = if (present_count == 0) 0 else payload[0];
    if (bit_width_raw > 32) return error.UnsupportedParquetPage;
    const bit_width: u6 = @intCast(bit_width_raw);
    const indexes = try decodeHybridIndexesAlloc(alloc, if (present_count == 0) &.{} else payload[1..], bit_width, present_count);
    defer alloc.free(indexes);

    const values = try alloc.alloc(i64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var present_idx: usize = 0;
    for (definition_levels, 0..) |definition_level, idx| {
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                const dictionary_index = indexes[present_idx];
                present_idx += 1;
                if (dictionary_index >= dictionary.len) return error.InvalidParquetPage;
                values[idx] = dictionary[@intCast(dictionary_index)];
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn decodeOptionalDictionaryF64V2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    dictionary: []const f64,
    page_payload: []const u8,
) !NullableF64Values {
    try header.validateDictionaryRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    var present_count: usize = 0;
    for (definition_levels) |definition_level| {
        if (definition_level > 1) return error.InvalidParquetPage;
        if (definition_level == 1) present_count += 1;
    }

    const payload = page_payload[header.data_payload_offset..];
    if (present_count > 0 and payload.len == 0) return error.InvalidParquetPage;
    const bit_width_raw: u8 = if (present_count == 0) 0 else payload[0];
    if (bit_width_raw > 32) return error.UnsupportedParquetPage;
    const bit_width: u6 = @intCast(bit_width_raw);
    const indexes = try decodeHybridIndexesAlloc(alloc, if (present_count == 0) &.{} else payload[1..], bit_width, present_count);
    defer alloc.free(indexes);

    const values = try alloc.alloc(f64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var present_idx: usize = 0;
    for (definition_levels, 0..) |definition_level, idx| {
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                const dictionary_index = indexes[present_idx];
                present_idx += 1;
                if (dictionary_index >= dictionary.len) return error.InvalidParquetPage;
                values[idx] = dictionary[@intCast(dictionary_index)];
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn scanUncompressedDictionaryI64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]i64 {
    return try scanDictionaryI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedDictionaryF64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]f64 {
    return try scanDictionaryF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedDictionaryF32AsF64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]f64 {
    return try scanDictionaryF32AsF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedOptionalDictionaryI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableI64Values {
    return try scanOptionalDictionaryI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedOptionalDictionaryF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableF64Values {
    return try scanOptionalDictionaryF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedOptionalDictionaryF32AsF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableF64Values {
    return try scanOptionalDictionaryF32AsF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedDictionaryI32AsI64ColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![]i64 {
    return try scanDictionaryI32AsI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedOptionalDictionaryI32AsI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableI64Values {
    return try scanOptionalDictionaryI32AsI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanDictionaryI32AsI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]i64 {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainI32DictionaryPageAsI64Alloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeDictionaryI64DataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanOptionalDictionaryI32AsI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableI64Values {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainI32DictionaryPageAsI64Alloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalDictionaryI64V2HybridLevelsAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanDictionaryI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]i64 {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainI64DictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeDictionaryI64DataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanDictionaryF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]f64 {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainF64DictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeDictionaryF64DataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanDictionaryF32AsF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![]f64 {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainF32DictionaryPageAsF64Alloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeDictionaryF64DataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanOptionalDictionaryI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableI64Values {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainI64DictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalDictionaryI64V2HybridLevelsAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn decodeHybridLevelsAlloc(
    alloc: Allocator,
    encoded: []const u8,
    bit_width: u4,
    expected_count: usize,
) ![]u8 {
    if (expected_count > max_values_per_page) return error.ParquetPageTooLarge;
    if (expected_count == 0) return try alloc.alloc(u8, 0);
    if (bit_width > 8) return error.UnsupportedParquetPage;
    const levels = try alloc.alloc(u8, expected_count);
    errdefer alloc.free(levels);

    var reader = LevelReader{ .bytes = encoded };
    var out_idx: usize = 0;
    while (out_idx < expected_count) {
        const header = try reader.readVarintU64();
        if (header == 0) return error.InvalidParquetPage;
        if ((header & 1) == 0) {
            const run_len: usize = std.math.cast(usize, header >> 1) orelse return error.InvalidParquetPage;
            if (run_len == 0) return error.InvalidParquetPage;
            const value = try reader.readFixedWidthValue(bit_width);
            if (run_len > expected_count - out_idx) return error.InvalidParquetPage;
            @memset(levels[out_idx..][0..run_len], value);
            out_idx += run_len;
        } else {
            const group_count: usize = std.math.cast(usize, header >> 1) orelse return error.InvalidParquetPage;
            if (group_count == 0) return error.InvalidParquetPage;
            const value_count = group_count * 8;
            const byte_count = try packedByteCount(value_count, bit_width);
            if (byte_count > reader.bytes.len - reader.cursor) return error.InvalidParquetPage;
            const packed_bytes = reader.bytes[reader.cursor..][0..byte_count];
            reader.cursor += byte_count;
            const to_copy = @min(value_count, expected_count - out_idx);
            for (0..to_copy) |idx| {
                const value = try readPackedValue(packed_bytes, idx, bit_width);
                levels[out_idx + idx] = value;
            }
            out_idx += to_copy;
        }
    }
    if (reader.cursor != reader.bytes.len) return error.InvalidParquetPage;
    return levels;
}

pub fn decodeHybridIndexesAlloc(
    alloc: Allocator,
    encoded: []const u8,
    bit_width: u6,
    expected_count: usize,
) ![]u32 {
    if (expected_count > max_values_per_page) return error.ParquetPageTooLarge;
    if (expected_count == 0) return try alloc.alloc(u32, 0);
    if (bit_width > 32) return error.UnsupportedParquetPage;
    const indexes = try alloc.alloc(u32, expected_count);
    errdefer alloc.free(indexes);

    var reader = LevelReader{ .bytes = encoded };
    var out_idx: usize = 0;
    while (out_idx < expected_count) {
        const header = try reader.readVarintU64();
        if (header == 0) return error.InvalidParquetPage;
        if ((header & 1) == 0) {
            const run_len: usize = std.math.cast(usize, header >> 1) orelse return error.InvalidParquetPage;
            if (run_len == 0) return error.InvalidParquetPage;
            const value = try reader.readFixedWidthValueU32(bit_width);
            if (run_len > expected_count - out_idx) return error.InvalidParquetPage;
            @memset(indexes[out_idx..][0..run_len], value);
            out_idx += run_len;
        } else {
            const group_count: usize = std.math.cast(usize, header >> 1) orelse return error.InvalidParquetPage;
            if (group_count == 0) return error.InvalidParquetPage;
            const value_count = group_count * 8;
            const byte_count = try packedByteCount(value_count, bit_width);
            if (byte_count > reader.bytes.len - reader.cursor) return error.InvalidParquetPage;
            const packed_bytes = reader.bytes[reader.cursor..][0..byte_count];
            reader.cursor += byte_count;
            const to_copy = @min(value_count, expected_count - out_idx);
            for (0..to_copy) |idx| {
                indexes[out_idx + idx] = try readPackedValueU32(packed_bytes, idx, bit_width);
            }
            out_idx += to_copy;
        }
    }
    if (reader.cursor != reader.bytes.len) return error.InvalidParquetPage;
    return indexes;
}

fn int96TimestampNs(bytes: *const [12]u8) !i64 {
    const nanos_of_day = std.mem.readInt(u64, bytes[0..8], .little);
    const julian_day = std.mem.readInt(u32, bytes[8..12], .little);
    const ns_per_day_u64: u64 = 86_400_000_000_000;
    if (nanos_of_day >= ns_per_day_u64) return error.InvalidParquetPage;
    const ns_per_day: i128 = ns_per_day_u64;
    const unix_days: i128 = @as(i128, julian_day) - 2_440_588;
    const timestamp_ns = unix_days * ns_per_day + @as(i128, nanos_of_day);
    return std.math.cast(i64, timestamp_ns) orelse error.InvalidParquetPage;
}

pub fn decodeOptionalPlainI64V2ByteLevelsAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableI64Values {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes != row_count) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;

    const values = try alloc.alloc(i64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (0..row_count) |idx| {
        const definition_level = page_payload[idx];
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                if (data_cursor + 8 > page_payload.len) return error.InvalidParquetPage;
                values[idx] = std.mem.readInt(i64, page_payload[data_cursor..][0..8], .little);
                data_cursor += 8;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn decodeOptionalPlainI64V2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableI64Values {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    const values = try alloc.alloc(i64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (definition_levels, 0..) |definition_level, idx| {
        if (definition_level > 1) return error.InvalidParquetPage;
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                if (data_cursor + 8 > page_payload.len) return error.InvalidParquetPage;
                values[idx] = std.mem.readInt(i64, page_payload[data_cursor..][0..8], .little);
                data_cursor += 8;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn decodeOptionalPlainInt96TimestampNsV2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableI64Values {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    const values = try alloc.alloc(i64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (definition_levels, 0..) |definition_level, idx| {
        if (definition_level > 1) return error.InvalidParquetPage;
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                if (data_cursor + 12 > page_payload.len) return error.InvalidParquetPage;
                values[idx] = try int96TimestampNs(page_payload[data_cursor..][0..12]);
                data_cursor += 12;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn decodeOptionalPlainF64V2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableF64Values {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    const values = try alloc.alloc(f64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (definition_levels, 0..) |definition_level, idx| {
        if (definition_level > 1) return error.InvalidParquetPage;
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                if (data_cursor + 8 > page_payload.len) return error.InvalidParquetPage;
                const bits = std.mem.readInt(u64, page_payload[data_cursor..][0..8], .little);
                values[idx] = @bitCast(bits);
                data_cursor += 8;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn decodeOptionalPlainI32V2HybridLevelsAsI64Alloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableI64Values {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    const values = try alloc.alloc(i64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (definition_levels, 0..) |definition_level, idx| {
        if (definition_level > 1) return error.InvalidParquetPage;
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                if (data_cursor + 4 > page_payload.len) return error.InvalidParquetPage;
                values[idx] = std.mem.readInt(i32, page_payload[data_cursor..][0..4], .little);
                data_cursor += 4;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn scanUncompressedOptionalPlainI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableI64Values {
    return try scanOptionalPlainI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedOptionalPlainInt96TimestampNsColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableI64Values {
    return try scanOptionalPlainInt96TimestampNsColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanOptionalPlainI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableI64Values {
    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalPlainI64V2HybridLevelsAlloc(alloc, parsed.header, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanOptionalPlainInt96TimestampNsColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableI64Values {
    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalPlainInt96TimestampNsV2HybridLevelsAlloc(alloc, parsed.header, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanUncompressedOptionalPlainI32AsI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableI64Values {
    return try scanOptionalPlainI32AsI64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanOptionalPlainI32AsI64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableI64Values {
    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalPlainI32V2HybridLevelsAsI64Alloc(alloc, parsed.header, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn decodeOptionalPlainF32V2HybridLevelsAsF64Alloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableF64Values {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    const values = try alloc.alloc(f64, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (definition_levels, 0..) |definition_level, idx| {
        if (definition_level > 1) return error.InvalidParquetPage;
        switch (definition_level) {
            0 => {
                values[idx] = 0;
                nulls[idx] = 1;
            },
            1 => {
                if (data_cursor + 4 > page_payload.len) return error.InvalidParquetPage;
                const bits = std.mem.readInt(u32, page_payload[data_cursor..][0..4], .little);
                values[idx] = @floatCast(@as(f32, @bitCast(bits)));
                data_cursor += 4;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn decodeOptionalPlainBoolV2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableBoolValues {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    var present_count: usize = 0;
    for (definition_levels) |definition_level| {
        if (definition_level > 1) return error.InvalidParquetPage;
        if (definition_level == 1) present_count += 1;
    }

    const payload = page_payload[header.data_payload_offset..];
    const needed = (present_count + 7) / 8;
    if (payload.len < needed) return error.InvalidParquetPage;

    const values = try alloc.alloc(bool, row_count);
    errdefer alloc.free(values);
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var present_idx: usize = 0;
    for (definition_levels, 0..) |definition_level, idx| {
        switch (definition_level) {
            0 => {
                values[idx] = false;
                nulls[idx] = 1;
            },
            1 => {
                values[idx] = readBitPackedBool(payload, present_idx);
                present_idx += 1;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn scanUncompressedOptionalPlainF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableF64Values {
    return try scanOptionalPlainF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedOptionalPlainF32AsF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableF64Values {
    return try scanOptionalPlainF32AsF64ColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanUncompressedOptionalPlainBoolColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableBoolValues {
    return try scanOptionalPlainBoolColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanOptionalPlainBoolColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableBoolValues {
    var values = std.ArrayListUnmanaged(bool).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalPlainBoolV2HybridLevelsAlloc(alloc, parsed.header, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanOptionalPlainF32AsF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableF64Values {
    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalPlainF32V2HybridLevelsAsF64Alloc(alloc, parsed.header, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanOptionalPlainF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableF64Values {
    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalPlainF64V2HybridLevelsAlloc(alloc, parsed.header, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanOptionalDictionaryF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableF64Values {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainF64DictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalDictionaryF64V2HybridLevelsAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanOptionalDictionaryF32AsF64ColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableF64Values {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainF32DictionaryPageAsF64Alloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer alloc.free(dictionary);

    var values = std.ArrayListUnmanaged(f64).empty;
    errdefer values.deinit(alloc);
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        var page_values = try decodeOptionalDictionaryF64V2HybridLevelsAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer page_values.deinit(alloc);
        try values.appendSlice(alloc, page_values.values);
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer alloc.free(out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn decodePlainByteArraysAlloc(alloc: Allocator, header: Header, page_payload: []const u8) ![][]u8 {
    try header.validatePlainRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    var cursor = header.data_payload_offset;
    const count: usize = @intCast(header.value_count);
    const values = try alloc.alloc([]u8, count);
    errdefer alloc.free(values);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
    }

    for (values) |*value| {
        if (cursor + 4 > page_payload.len) return error.InvalidParquetPage;
        const len = std.mem.readInt(u32, page_payload[cursor .. cursor + 4][0..4], .little);
        cursor += 4;
        if (len > page_payload.len - cursor) return error.InvalidParquetPage;
        value.* = try alloc.dupe(u8, page_payload[cursor .. cursor + len]);
        cursor += len;
        initialized += 1;
    }
    return values;
}

pub fn decodeOptionalPlainByteArraysV2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
) !NullableByteArrayValues {
    try header.validatePlainRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    const values = try alloc.alloc([]u8, row_count);
    errdefer alloc.free(values);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
    }
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (definition_levels, 0..) |definition_level, idx| {
        if (definition_level > 1) return error.InvalidParquetPage;
        switch (definition_level) {
            0 => {
                values[idx] = try alloc.alloc(u8, 0);
                initialized += 1;
                nulls[idx] = 1;
            },
            1 => {
                if (data_cursor + 4 > page_payload.len) return error.InvalidParquetPage;
                const len = std.mem.readInt(u32, page_payload[data_cursor .. data_cursor + 4][0..4], .little);
                data_cursor += 4;
                if (len > page_payload.len - data_cursor) return error.InvalidParquetPage;
                values[idx] = try alloc.dupe(u8, page_payload[data_cursor .. data_cursor + len]);
                initialized += 1;
                data_cursor += len;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn scanUncompressedPlainByteArrayColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![][]u8 {
    return try scanPlainByteArrayColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanPlainByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![][]u8 {
    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainByteArraysAlloc(alloc, parsed.header, payload.bytes);
        defer alloc.free(page_values);
        try values.ensureUnusedCapacity(alloc, page_values.len);
        for (page_values) |value| {
            values.appendAssumeCapacity(value);
        }
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanUncompressedOptionalPlainByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableByteArrayValues {
    return try scanOptionalPlainByteArrayColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanOptionalPlainByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableByteArrayValues {
    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeOptionalPlainByteArraysV2HybridLevelsAlloc(alloc, parsed.header, payload.bytes);
        defer {
            alloc.free(page_values.values);
            alloc.free(page_values.nulls);
        }
        try values.ensureUnusedCapacity(alloc, page_values.values.len);
        for (page_values.values) |value| {
            values.appendAssumeCapacity(value);
        }
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer freePlainByteArrays(alloc, out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn decodePlainByteArrayDictionaryPageAlloc(alloc: Allocator, header: Header, page_payload: []const u8) ![][]u8 {
    try header.validatePlainDictionary();
    return try decodePlainByteArraysAlloc(alloc, .{
        .page_type = .data_page,
        .uncompressed_page_size = header.uncompressed_page_size,
        .compressed_page_size = header.compressed_page_size,
        .value_count = header.value_count,
        .encoding = .plain,
    }, page_payload);
}

pub fn decodePlainFixedLenByteArraysAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
    type_length: usize,
) ![][]u8 {
    try header.validatePlainRequired();
    if (type_length == 0) return error.InvalidParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    var cursor = header.data_payload_offset;
    const count: usize = @intCast(header.value_count);
    const values = try alloc.alloc([]u8, count);
    errdefer alloc.free(values);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
    }

    for (values) |*value| {
        if (type_length > page_payload.len - cursor) return error.InvalidParquetPage;
        value.* = try alloc.dupe(u8, page_payload[cursor .. cursor + type_length]);
        cursor += type_length;
        initialized += 1;
    }
    return values;
}

pub fn decodeOptionalPlainFixedLenByteArraysV2HybridLevelsAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
    type_length: usize,
) !NullableByteArrayValues {
    try header.validatePlainRequired();
    if (type_length == 0) return error.InvalidParquetPage;
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    const values = try alloc.alloc([]u8, row_count);
    errdefer alloc.free(values);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
    }
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var data_cursor = header.data_payload_offset;
    for (definition_levels, 0..) |definition_level, idx| {
        if (definition_level > 1) return error.InvalidParquetPage;
        switch (definition_level) {
            0 => {
                values[idx] = try alloc.alloc(u8, 0);
                initialized += 1;
                nulls[idx] = 1;
            },
            1 => {
                if (type_length > page_payload.len - data_cursor) return error.InvalidParquetPage;
                values[idx] = try alloc.dupe(u8, page_payload[data_cursor .. data_cursor + type_length]);
                initialized += 1;
                data_cursor += type_length;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn decodePlainFixedLenByteArrayDictionaryPageAlloc(
    alloc: Allocator,
    header: Header,
    page_payload: []const u8,
    type_length: usize,
) ![][]u8 {
    try header.validatePlainDictionary();
    return try decodePlainFixedLenByteArraysAlloc(alloc, .{
        .page_type = .data_page,
        .uncompressed_page_size = header.uncompressed_page_size,
        .compressed_page_size = header.compressed_page_size,
        .value_count = header.value_count,
        .encoding = .plain,
    }, page_payload, type_length);
}

pub fn decodeDictionaryByteArrayDataPageAlloc(
    alloc: Allocator,
    header: Header,
    dictionary: []const []const u8,
    page_payload: []const u8,
) ![][]u8 {
    try header.validateDictionaryRequired();
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;
    const payload = page_payload[header.data_payload_offset..];
    if (payload.len == 0) return error.InvalidParquetPage;
    const bit_width_raw = payload[0];
    if (bit_width_raw > 32) return error.UnsupportedParquetPage;
    const bit_width: u6 = @intCast(bit_width_raw);
    const row_count: usize = @intCast(header.value_count);
    const indexes = try decodeHybridIndexesAlloc(alloc, payload[1..], bit_width, row_count);
    defer alloc.free(indexes);

    const values = try alloc.alloc([]u8, row_count);
    errdefer alloc.free(values);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
    }
    for (indexes, values) |index, *value| {
        if (index >= dictionary.len) return error.InvalidParquetPage;
        value.* = try alloc.dupe(u8, dictionary[@intCast(index)]);
        initialized += 1;
    }
    return values;
}

pub fn decodeOptionalDictionaryByteArrayDataPageAlloc(
    alloc: Allocator,
    header: Header,
    dictionary: []const []const u8,
    page_payload: []const u8,
) !NullableByteArrayValues {
    try header.validateDictionaryRequired();
    if (header.page_type != .data_page_v2) return error.UnsupportedParquetPage;
    if (header.repetition_level_bytes != 0) return error.UnsupportedParquetPage;
    const row_count: usize = @intCast(header.value_count);
    if (header.definition_level_bytes == 0) return error.UnsupportedParquetPage;
    if (header.definition_level_bytes > page_payload.len) return error.InvalidParquetPage;
    if (header.data_payload_offset > page_payload.len) return error.InvalidParquetPage;

    const definition_levels = try decodeHybridLevelsAlloc(alloc, page_payload[0..header.definition_level_bytes], 1, row_count);
    defer alloc.free(definition_levels);

    var present_count: usize = 0;
    for (definition_levels) |definition_level| {
        if (definition_level > 1) return error.InvalidParquetPage;
        if (definition_level == 1) present_count += 1;
    }

    const payload = page_payload[header.data_payload_offset..];
    if (present_count > 0 and payload.len == 0) return error.InvalidParquetPage;
    const bit_width_raw: u8 = if (present_count == 0) 0 else payload[0];
    if (bit_width_raw > 32) return error.UnsupportedParquetPage;
    const bit_width: u6 = @intCast(bit_width_raw);
    const indexes = try decodeHybridIndexesAlloc(alloc, if (present_count == 0) &.{} else payload[1..], bit_width, present_count);
    defer alloc.free(indexes);

    const values = try alloc.alloc([]u8, row_count);
    errdefer alloc.free(values);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
    }
    const nulls = try alloc.alloc(u8, row_count);
    errdefer alloc.free(nulls);

    var present_idx: usize = 0;
    for (definition_levels, 0..) |definition_level, idx| {
        switch (definition_level) {
            0 => {
                values[idx] = try alloc.alloc(u8, 0);
                initialized += 1;
                nulls[idx] = 1;
            },
            1 => {
                const dictionary_index = indexes[present_idx];
                present_idx += 1;
                if (dictionary_index >= dictionary.len) return error.InvalidParquetPage;
                values[idx] = try alloc.dupe(u8, dictionary[@intCast(dictionary_index)]);
                initialized += 1;
                nulls[idx] = 0;
            },
            else => return error.InvalidParquetPage,
        }
    }

    return .{
        .values = values,
        .nulls = nulls,
    };
}

pub fn scanUncompressedDictionaryByteArrayColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![][]u8 {
    return try scanDictionaryByteArrayColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanPlainFixedLenByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
    type_length: usize,
) ![][]u8 {
    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodePlainFixedLenByteArraysAlloc(alloc, parsed.header, payload.bytes, type_length);
        defer alloc.free(page_values);
        try values.ensureUnusedCapacity(alloc, page_values.len);
        for (page_values) |value| {
            values.appendAssumeCapacity(value);
        }
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanOptionalPlainFixedLenByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
    type_length: usize,
) !NullableByteArrayValues {
    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validatePlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeOptionalPlainFixedLenByteArraysV2HybridLevelsAlloc(alloc, parsed.header, payload.bytes, type_length);
        defer {
            alloc.free(page_values.values);
            alloc.free(page_values.nulls);
        }
        try values.ensureUnusedCapacity(alloc, page_values.values.len);
        for (page_values.values) |value| {
            values.appendAssumeCapacity(value);
        }
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer freePlainByteArrays(alloc, out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanDictionaryFixedLenByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
    type_length: usize,
) ![][]u8 {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainFixedLenByteArrayDictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes, type_length);
    defer freePlainByteArrays(alloc, dictionary);

    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeDictionaryByteArrayDataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer alloc.free(page_values);
        try values.ensureUnusedCapacity(alloc, page_values.len);
        for (page_values) |value| {
            values.appendAssumeCapacity(value);
        }
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanOptionalDictionaryFixedLenByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
    type_length: usize,
) !NullableByteArrayValues {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainFixedLenByteArrayDictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes, type_length);
    defer freePlainByteArrays(alloc, dictionary);

    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeOptionalDictionaryByteArrayDataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer {
            alloc.free(page_values.values);
            alloc.free(page_values.nulls);
        }
        try values.ensureUnusedCapacity(alloc, page_values.values.len);
        for (page_values.values) |value| {
            values.appendAssumeCapacity(value);
        }
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer freePlainByteArrays(alloc, out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn scanDictionaryByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) ![][]u8 {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainByteArrayDictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer freePlainByteArrays(alloc, dictionary);

    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeDictionaryByteArrayDataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer alloc.free(page_values);
        try values.ensureUnusedCapacity(alloc, page_values.len);
        for (page_values) |value| {
            values.appendAssumeCapacity(value);
        }
    }
    return try values.toOwnedSlice(alloc);
}

pub fn scanUncompressedOptionalDictionaryByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
) !NullableByteArrayValues {
    return try scanOptionalDictionaryByteArrayColumnChunkAlloc(alloc, column_chunk_bytes, .uncompressed);
}

pub fn scanOptionalDictionaryByteArrayColumnChunkAlloc(
    alloc: Allocator,
    column_chunk_bytes: []const u8,
    compression: CompressionCodec,
) !NullableByteArrayValues {
    var cursor: usize = 0;
    if (cursor >= column_chunk_bytes.len) return error.InvalidParquetPage;

    const parsed_dictionary = try parsePageHeader(column_chunk_bytes[cursor..]);
    try parsed_dictionary.header.validatePlainDictionary();
    cursor += parsed_dictionary.header_len;
    if (parsed_dictionary.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
    const compressed_dictionary_payload = column_chunk_bytes[cursor .. cursor + parsed_dictionary.header.compressed_page_size];
    cursor += parsed_dictionary.header.compressed_page_size;
    const dictionary_payload = try decodePagePayloadAlloc(alloc, parsed_dictionary.header, compression, compressed_dictionary_payload);
    defer dictionary_payload.deinit(alloc);
    const dictionary = try decodePlainByteArrayDictionaryPageAlloc(alloc, parsed_dictionary.header, dictionary_payload.bytes);
    defer freePlainByteArrays(alloc, dictionary);

    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var nulls = std.ArrayListUnmanaged(u8).empty;
    errdefer nulls.deinit(alloc);

    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateDictionaryRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const compressed_payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;
        const payload = try decodePagePayloadAlloc(alloc, parsed.header, compression, compressed_payload);
        defer payload.deinit(alloc);

        const page_values = try decodeOptionalDictionaryByteArrayDataPageAlloc(alloc, parsed.header, dictionary, payload.bytes);
        defer {
            alloc.free(page_values.values);
            alloc.free(page_values.nulls);
        }
        try values.ensureUnusedCapacity(alloc, page_values.values.len);
        for (page_values.values) |value| {
            values.appendAssumeCapacity(value);
        }
        try nulls.appendSlice(alloc, page_values.nulls);
    }

    const out_values = try values.toOwnedSlice(alloc);
    errdefer freePlainByteArrays(alloc, out_values);
    const out_nulls = try nulls.toOwnedSlice(alloc);
    errdefer alloc.free(out_nulls);
    return .{
        .values = out_values,
        .nulls = out_nulls,
    };
}

pub fn freePlainByteArrays(alloc: Allocator, values: [][]u8) void {
    for (values) |value| alloc.free(value);
    alloc.free(values);
}

fn decodePagePayloadAlloc(
    alloc: Allocator,
    header: Header,
    compression: CompressionCodec,
    compressed_payload: []const u8,
) !PagePayload {
    if (compressed_payload.len != header.compressed_page_size) return error.InvalidParquetPage;
    const expected_len: usize = @intCast(header.uncompressed_page_size);
    if (expected_len > max_uncompressed_page_bytes) return error.ParquetPageTooLarge;

    const level_prefix_len = if (header.page_type == .data_page_v2) header.data_payload_offset else 0;
    if (level_prefix_len > expected_len or level_prefix_len > compressed_payload.len) return error.InvalidParquetPage;
    if (compression == .uncompressed and header.compressed_page_size != header.uncompressed_page_size) {
        return error.UnsupportedParquetPage;
    }
    const values_are_compressed = compression != .uncompressed and
        (header.page_type != .data_page_v2 or header.data_is_compressed);
    if (!values_are_compressed) {
        if (compressed_payload.len != expected_len) return error.InvalidParquetPage;
        return .{ .bytes = try alloc.dupe(u8, compressed_payload) };
    }

    const decoded_values = try decodeCompressedExactAlloc(
        alloc,
        compression,
        compressed_payload[level_prefix_len..],
        expected_len - level_prefix_len,
    );
    if (level_prefix_len == 0) return .{ .bytes = decoded_values };
    defer alloc.free(decoded_values);

    const bytes = try alloc.alloc(u8, expected_len);
    @memcpy(bytes[0..level_prefix_len], compressed_payload[0..level_prefix_len]);
    @memcpy(bytes[level_prefix_len..], decoded_values);
    return .{ .bytes = bytes };
}

fn decodeCompressedExactAlloc(
    alloc: Allocator,
    compression: CompressionCodec,
    compressed: []const u8,
    expected_len: usize,
) ![]u8 {
    const read_limit = std.math.add(usize, expected_len, 1) catch return error.ParquetPageTooLarge;
    const decoded = switch (compression) {
        .uncompressed => return error.InvalidParquetPage,
        .snappy => blk: {
            if (try snappy.decodedLen(compressed) != expected_len) return error.InvalidParquetPage;
            break :blk try snappy.decode(alloc, compressed);
        },
        .gzip => blk: {
            var in: std.Io.Reader = .fixed(compressed);
            var window: [std.compress.flate.max_window_len]u8 = undefined;
            var decompress: std.compress.flate.Decompress = .init(&in, .gzip, &window);
            break :blk try decompress.reader.allocRemaining(alloc, .limited(read_limit));
        },
        .zstd => blk: {
            var in: std.Io.Reader = .fixed(compressed);
            var window: [std.compress.zstd.default_window_len + std.compress.zstd.block_size_max]u8 = undefined;
            var decompress: std.compress.zstd.Decompress = .init(&in, &window, .{});
            break :blk try decompress.reader.allocRemaining(alloc, .limited(read_limit));
        },
    };
    errdefer alloc.free(decoded);
    if (decoded.len != expected_len) return error.InvalidParquetPage;
    return decoded;
}

fn parseHeaderStruct(reader: *Reader) !Header {
    var previous_field_id: i16 = 0;
    var page_type: ?PageType = null;
    var uncompressed_page_size: ?u32 = null;
    var compressed_page_size: ?u32 = null;
    var data_page: ?DataPageHeader = null;
    var dictionary_page: ?DataPageHeader = null;
    var data_page_v2: ?DataPageHeader = null;

    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => page_type = try pageTypeFromInt(try reader.readRequiredI32(field.type)),
            2 => uncompressed_page_size = try reader.readRequiredU32(field.type),
            3 => compressed_page_size = try reader.readRequiredU32(field.type),
            5 => {
                if (field.type != .struct_) return error.InvalidParquetPage;
                data_page = try parseDataPageHeader(reader);
            },
            7 => {
                if (field.type != .struct_) return error.InvalidParquetPage;
                dictionary_page = try parseDictionaryPageHeader(reader);
            },
            8 => {
                if (field.type != .struct_) return error.InvalidParquetPage;
                data_page_v2 = try parseDataPageHeaderV2(reader);
            },
            else => try reader.skip(field.type),
        }
    }

    const got_page_type = page_type orelse return error.InvalidParquetPage;
    const page_header = switch (got_page_type) {
        .data_page => data_page orelse return error.InvalidParquetPage,
        .dictionary_page => dictionary_page orelse return error.InvalidParquetPage,
        .data_page_v2 => data_page_v2 orelse return error.InvalidParquetPage,
        else => return error.UnsupportedParquetPage,
    };
    return .{
        .page_type = got_page_type,
        .uncompressed_page_size = uncompressed_page_size orelse return error.InvalidParquetPage,
        .compressed_page_size = compressed_page_size orelse return error.InvalidParquetPage,
        .value_count = page_header.value_count,
        .encoding = page_header.encoding,
        .definition_level_bytes = page_header.definition_level_bytes,
        .repetition_level_bytes = page_header.repetition_level_bytes,
        .data_payload_offset = page_header.data_payload_offset,
        .data_is_compressed = page_header.data_is_compressed,
    };
}

const DataPageHeader = struct {
    value_count: u32,
    encoding: Encoding,
    definition_level_bytes: usize = 0,
    repetition_level_bytes: usize = 0,
    data_payload_offset: usize = 0,
    data_is_compressed: bool = true,
};

fn parseDataPageHeader(reader: *Reader) !DataPageHeader {
    var previous_field_id: i16 = 0;
    var value_count: ?u32 = null;
    var encoding: ?Encoding = null;
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => value_count = try reader.readRequiredU32(field.type),
            2 => encoding = try encodingFromInt(try reader.readRequiredI32(field.type)),
            else => try reader.skip(field.type),
        }
    }
    return .{
        .value_count = value_count orelse return error.InvalidParquetPage,
        .encoding = encoding orelse return error.InvalidParquetPage,
    };
}

fn parseDictionaryPageHeader(reader: *Reader) !DataPageHeader {
    var previous_field_id: i16 = 0;
    var value_count: ?u32 = null;
    var encoding: ?Encoding = null;
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => value_count = try reader.readRequiredU32(field.type),
            2 => encoding = try encodingFromInt(try reader.readRequiredI32(field.type)),
            else => try reader.skip(field.type),
        }
    }
    return .{
        .value_count = value_count orelse return error.InvalidParquetPage,
        .encoding = encoding orelse return error.InvalidParquetPage,
    };
}

fn parseDataPageHeaderV2(reader: *Reader) !DataPageHeader {
    var previous_field_id: i16 = 0;
    var value_count: ?u32 = null;
    var encoding: ?Encoding = null;
    var definition_level_bytes: usize = 0;
    var repetition_level_bytes: usize = 0;
    var data_is_compressed = true;
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => value_count = try reader.readRequiredU32(field.type),
            4 => encoding = try encodingFromInt(try reader.readRequiredI32(field.type)),
            5 => definition_level_bytes = @intCast(try reader.readRequiredU32(field.type)),
            6 => repetition_level_bytes = @intCast(try reader.readRequiredU32(field.type)),
            7 => data_is_compressed = try reader.readRequiredBool(field.type),
            else => try reader.skip(field.type),
        }
    }
    return .{
        .value_count = value_count orelse return error.InvalidParquetPage,
        .encoding = encoding orelse return error.InvalidParquetPage,
        .definition_level_bytes = definition_level_bytes,
        .repetition_level_bytes = repetition_level_bytes,
        .data_payload_offset = definition_level_bytes + repetition_level_bytes,
        .data_is_compressed = data_is_compressed,
    };
}

fn pageTypeFromInt(raw: i32) !PageType {
    return switch (raw) {
        0 => .data_page,
        1 => .index_page,
        2 => .dictionary_page,
        3 => .data_page_v2,
        else => error.InvalidParquetPage,
    };
}

fn encodingFromInt(raw: i32) !Encoding {
    return switch (raw) {
        0 => .plain,
        1 => .plain_dictionary,
        2 => .rle,
        3 => .bit_packed,
        4 => .delta_binary_packed,
        5 => .delta_length_byte_array,
        6 => .delta_byte_array,
        7 => .rle_dictionary,
        8 => .byte_stream_split,
        else => error.InvalidParquetPage,
    };
}

const Field = struct {
    id: i16,
    type: CompactType,
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

    fn readRequiredI32(self: *Reader, field_type: CompactType) !i32 {
        if (field_type != .i32) return error.InvalidParquetPage;
        return try self.readI32();
    }

    fn readRequiredU32(self: *Reader, field_type: CompactType) !u32 {
        const value = try self.readRequiredI32(field_type);
        if (value < 0) return error.InvalidParquetPage;
        return @intCast(value);
    }

    fn readRequiredBool(_: *Reader, field_type: CompactType) !bool {
        return switch (field_type) {
            .boolean_true => true,
            .boolean_false => false,
            else => error.InvalidParquetPage,
        };
    }

    fn readI16(self: *Reader) !i16 {
        return @intCast(zigzagDecode(try self.readVarintU64()));
    }

    fn readI32(self: *Reader) !i32 {
        return @intCast(zigzagDecode(try self.readVarintU64()));
    }

    fn readByte(self: *Reader) !u8 {
        if (self.cursor >= self.bytes.len) return error.InvalidParquetPage;
        const out = self.bytes[self.cursor];
        self.cursor += 1;
        return out;
    }

    fn readVarintUsize(self: *Reader) !usize {
        return std.math.cast(usize, try self.readVarintU64()) orelse error.InvalidParquetPage;
    }

    fn readVarintU64(self: *Reader) !u64 {
        var shift: u6 = 0;
        var result: u64 = 0;
        while (true) {
            const byte = try self.readByte();
            result |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return result;
            if (shift >= 63) return error.InvalidParquetPage;
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
                if (self.bytes.len - self.cursor < 8) return error.InvalidParquetPage;
                self.cursor += 8;
            },
            .binary => {
                const len = try self.readVarintUsize();
                if (len > self.bytes.len - self.cursor) return error.InvalidParquetPage;
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

    fn readI64(self: *Reader) !i64 {
        return zigzagDecode(try self.readVarintU64());
    }

    fn readListHeader(self: *Reader) !struct { elem_type: CompactType, len: usize } {
        const raw = try self.readByte();
        const elem_type: CompactType = @enumFromInt(raw & 0x0f);
        const inline_len = raw >> 4;
        const len = if (inline_len == 15) try self.readVarintUsize() else inline_len;
        return .{ .elem_type = elem_type, .len = len };
    }
};

const LevelReader = struct {
    bytes: []const u8,
    cursor: usize = 0,

    fn readVarintU64(self: *LevelReader) !u64 {
        var shift: u6 = 0;
        var result: u64 = 0;
        while (true) {
            if (self.cursor >= self.bytes.len) return error.InvalidParquetPage;
            const byte = self.bytes[self.cursor];
            self.cursor += 1;
            result |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return result;
            if (shift >= 63) return error.InvalidParquetPage;
            shift += 7;
        }
    }

    fn readFixedWidthValue(self: *LevelReader, bit_width: u4) !u8 {
        if (bit_width == 0) return 0;
        const byte_count = fixedWidthByteCount(bit_width);
        if (byte_count > self.bytes.len - self.cursor) return error.InvalidParquetPage;
        var value: u16 = 0;
        for (self.bytes[self.cursor..][0..byte_count], 0..) |byte, idx| {
            value |= @as(u16, byte) << @intCast(idx * 8);
        }
        self.cursor += byte_count;
        if (value > std.math.maxInt(u8)) return error.InvalidParquetPage;
        return @intCast(value);
    }

    fn readFixedWidthValueU32(self: *LevelReader, bit_width: u6) !u32 {
        if (bit_width == 0) return 0;
        const byte_count = fixedWidthByteCount(bit_width);
        if (byte_count > 4) return error.UnsupportedParquetPage;
        if (byte_count > self.bytes.len - self.cursor) return error.InvalidParquetPage;
        var value: u32 = 0;
        for (self.bytes[self.cursor..][0..byte_count], 0..) |byte, idx| {
            value |= @as(u32, byte) << @intCast(idx * 8);
        }
        self.cursor += byte_count;
        return value;
    }
};

fn fixedWidthByteCount(bit_width: anytype) usize {
    return (@as(usize, bit_width) + 7) / 8;
}

fn packedByteCount(value_count: usize, bit_width: anytype) !usize {
    const bits = std.math.mul(usize, value_count, bit_width) catch return error.InvalidParquetPage;
    return (bits + 7) / 8;
}

fn readPackedValue(packed_bytes: []const u8, value_idx: usize, bit_width: u4) !u8 {
    if (bit_width == 0) return 0;
    var value: u16 = 0;
    const start_bit = value_idx * @as(usize, bit_width);
    for (0..@as(usize, bit_width)) |bit_offset| {
        const absolute_bit = start_bit + bit_offset;
        const byte_idx = absolute_bit / 8;
        if (byte_idx >= packed_bytes.len) return error.InvalidParquetPage;
        const bit_idx: u3 = @intCast(absolute_bit % 8);
        const bit = (packed_bytes[byte_idx] >> bit_idx) & 1;
        value |= @as(u16, bit) << @intCast(bit_offset);
    }
    if (value > std.math.maxInt(u8)) return error.InvalidParquetPage;
    return @intCast(value);
}

fn readPackedValueU32(packed_bytes: []const u8, value_idx: usize, bit_width: u6) !u32 {
    if (bit_width == 0) return 0;
    var value: u32 = 0;
    const start_bit = value_idx * @as(usize, bit_width);
    for (0..@as(usize, bit_width)) |bit_offset| {
        const absolute_bit = start_bit + bit_offset;
        const byte_idx = absolute_bit / 8;
        if (byte_idx >= packed_bytes.len) return error.InvalidParquetPage;
        const bit_idx: u3 = @intCast(absolute_bit % 8);
        const bit = (packed_bytes[byte_idx] >> bit_idx) & 1;
        value |= @as(u32, bit) << @intCast(bit_offset);
    }
    return value;
}

fn readBitPackedBool(packed_bytes: []const u8, value_idx: usize) bool {
    const bit_idx: u3 = @intCast(value_idx % 8);
    return ((packed_bytes[value_idx / 8] >> bit_idx) & 1) != 0;
}

fn zigzagDecode(raw: u64) i64 {
    return @as(i64, @bitCast(raw >> 1)) ^ -@as(i64, @intCast(raw & 1));
}

fn appendField(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, previous: *i16, id: i16, field_type: CompactType) !void {
    const delta = id - previous.*;
    if (delta > 0 and delta <= 15) {
        try out.append(alloc, (@as(u8, @intCast(delta)) << 4) | @as(u8, @intFromEnum(field_type)));
    } else {
        try out.append(alloc, @intFromEnum(field_type));
        try appendI16(out, alloc, id);
    }
    previous.* = id;
}

fn appendStop(out: *std.ArrayListUnmanaged(u8), alloc: Allocator) !void {
    try out.append(alloc, 0);
}

fn appendI16(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i16) !void {
    try appendZigzag(out, alloc, value);
}

fn appendI32(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: i32) !void {
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

fn buildDataPageHeaderFixture(alloc: Allocator, value_count: i32, compressed_size: i32) !std.ArrayListUnmanaged(u8) {
    return try buildDataPageHeaderFixtureWithSizesAndEncoding(alloc, value_count, compressed_size, compressed_size, 0);
}

fn buildDataPageHeaderFixtureWithEncoding(
    alloc: Allocator,
    value_count: i32,
    compressed_size: i32,
    encoding: i32,
) !std.ArrayListUnmanaged(u8) {
    return try buildDataPageHeaderFixtureWithSizesAndEncoding(alloc, value_count, compressed_size, compressed_size, encoding);
}

fn buildDataPageHeaderFixtureWithSizesAndEncoding(
    alloc: Allocator,
    value_count: i32,
    uncompressed_size: i32,
    compressed_size: i32,
    encoding: i32,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var page_prev: i16 = 0;
    try appendField(&out, alloc, &page_prev, 1, .i32);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &page_prev, 2, .i32);
    try appendI32(&out, alloc, uncompressed_size);
    try appendField(&out, alloc, &page_prev, 3, .i32);
    try appendI32(&out, alloc, compressed_size);
    try appendField(&out, alloc, &page_prev, 5, .struct_);

    var data_prev: i16 = 0;
    try appendField(&out, alloc, &data_prev, 1, .i32);
    try appendI32(&out, alloc, value_count);
    try appendField(&out, alloc, &data_prev, 2, .i32);
    try appendI32(&out, alloc, encoding);
    try appendField(&out, alloc, &data_prev, 3, .i32);
    try appendI32(&out, alloc, 2);
    try appendField(&out, alloc, &data_prev, 4, .i32);
    try appendI32(&out, alloc, 2);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}

fn buildDictionaryPageHeaderFixture(alloc: Allocator, value_count: i32, compressed_size: i32) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var page_prev: i16 = 0;
    try appendField(&out, alloc, &page_prev, 1, .i32);
    try appendI32(&out, alloc, 2);
    try appendField(&out, alloc, &page_prev, 2, .i32);
    try appendI32(&out, alloc, compressed_size);
    try appendField(&out, alloc, &page_prev, 3, .i32);
    try appendI32(&out, alloc, compressed_size);
    try appendField(&out, alloc, &page_prev, 7, .struct_);

    var dictionary_prev: i16 = 0;
    try appendField(&out, alloc, &dictionary_prev, 1, .i32);
    try appendI32(&out, alloc, value_count);
    try appendField(&out, alloc, &dictionary_prev, 2, .i32);
    try appendI32(&out, alloc, 0);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}

fn buildDictionaryDataPageHeaderFixture(alloc: Allocator, value_count: i32, compressed_size: i32) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var page_prev: i16 = 0;
    try appendField(&out, alloc, &page_prev, 1, .i32);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &page_prev, 2, .i32);
    try appendI32(&out, alloc, compressed_size);
    try appendField(&out, alloc, &page_prev, 3, .i32);
    try appendI32(&out, alloc, compressed_size);
    try appendField(&out, alloc, &page_prev, 5, .struct_);

    var data_prev: i16 = 0;
    try appendField(&out, alloc, &data_prev, 1, .i32);
    try appendI32(&out, alloc, value_count);
    try appendField(&out, alloc, &data_prev, 2, .i32);
    try appendI32(&out, alloc, 7);
    try appendField(&out, alloc, &data_prev, 3, .i32);
    try appendI32(&out, alloc, 2);
    try appendField(&out, alloc, &data_prev, 4, .i32);
    try appendI32(&out, alloc, 2);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}

fn buildDataPageV2HeaderFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    return try buildDataPageV2HeaderFixtureWithLevels(alloc, 2, 18, 1, 1);
}

fn buildDataPageV2HeaderFixtureWithLevels(
    alloc: Allocator,
    value_count: i32,
    compressed_size: i32,
    definition_level_bytes: i32,
    repetition_level_bytes: i32,
) !std.ArrayListUnmanaged(u8) {
    return try buildDataPageV2HeaderFixtureWithLevelsAndEncoding(alloc, value_count, compressed_size, definition_level_bytes, repetition_level_bytes, 0);
}

fn buildDataPageV2HeaderFixtureWithLevelsAndEncoding(
    alloc: Allocator,
    value_count: i32,
    compressed_size: i32,
    definition_level_bytes: i32,
    repetition_level_bytes: i32,
    encoding: i32,
) !std.ArrayListUnmanaged(u8) {
    return try buildDataPageV2HeaderFixtureWithCompression(
        alloc,
        value_count,
        compressed_size,
        compressed_size,
        definition_level_bytes,
        repetition_level_bytes,
        encoding,
        null,
    );
}

fn buildDataPageV2HeaderFixtureWithCompression(
    alloc: Allocator,
    value_count: i32,
    uncompressed_size: i32,
    compressed_size: i32,
    definition_level_bytes: i32,
    repetition_level_bytes: i32,
    encoding: i32,
    is_compressed: ?bool,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var page_prev: i16 = 0;
    try appendField(&out, alloc, &page_prev, 1, .i32);
    try appendI32(&out, alloc, 3);
    try appendField(&out, alloc, &page_prev, 2, .i32);
    try appendI32(&out, alloc, uncompressed_size);
    try appendField(&out, alloc, &page_prev, 3, .i32);
    try appendI32(&out, alloc, compressed_size);
    try appendField(&out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(&out, alloc, &data_prev, 1, .i32);
    try appendI32(&out, alloc, value_count);
    try appendField(&out, alloc, &data_prev, 2, .i32);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &data_prev, 3, .i32);
    try appendI32(&out, alloc, value_count);
    try appendField(&out, alloc, &data_prev, 4, .i32);
    try appendI32(&out, alloc, encoding);
    try appendField(&out, alloc, &data_prev, 5, .i32);
    try appendI32(&out, alloc, definition_level_bytes);
    try appendField(&out, alloc, &data_prev, 6, .i32);
    try appendI32(&out, alloc, repetition_level_bytes);
    if (is_compressed) |value| {
        try appendField(&out, alloc, &data_prev, 7, if (value) .boolean_true else .boolean_false);
    }
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}

test "parquet page parser decodes plain i64 data page values" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageHeaderFixture(alloc, 3, 24);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);
    try std.testing.expectEqual(@as(usize, header_bytes.items.len), parsed.header_len);
    try std.testing.expectEqual(PageType.data_page, parsed.header.page_type);
    try std.testing.expectEqual(Encoding.plain, parsed.header.encoding);
    try std.testing.expectEqual(@as(u32, 3), parsed.header.value_count);

    const payload = [_]u8{
        1,    0,    0,    0,    0,    0,    0,    0,
        2,    0,    0,    0,    0,    0,    0,    0,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    };
    const values = try decodePlainI64Alloc(alloc, parsed.header, &payload);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, -1 }, values);
}

test "parquet page parser rejects value counts above the decoded-vector budget" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageHeaderFixture(alloc, max_values_per_page + 1, 1);
    defer header_bytes.deinit(alloc);
    try std.testing.expectError(error.ParquetPageTooLarge, parsePageHeader(header_bytes.items));
    try std.testing.expectError(
        error.ParquetPageTooLarge,
        decodeHybridLevelsAlloc(alloc, &[_]u8{ 2, 0 }, 1, max_values_per_page + 1),
    );
}

test "parquet page parser decodes plain byte array values" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageHeaderFixture(alloc, 2, 17);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);

    const payload = [_]u8{
        5, 0, 0, 0, 'a', 'l', 'p', 'h', 'a',
        4, 0, 0, 0, 'b', 'e', 't', 'a',
    };
    const values = try decodePlainByteArraysAlloc(alloc, parsed.header, &payload);
    defer freePlainByteArrays(alloc, values);
    try std.testing.expectEqualStrings("alpha", values[0]);
    try std.testing.expectEqualStrings("beta", values[1]);
}

test "parquet page parser handles v2 level prefix before plain values" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageV2HeaderFixture(alloc);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);
    try std.testing.expectEqual(PageType.data_page_v2, parsed.header.page_type);
    try std.testing.expectEqual(@as(usize, 2), parsed.header.data_payload_offset);

    const payload = [_]u8{
        0xaa, 0xbb,
        7,    0,
        0,    0,
        0,    0,
        0,    0,
        8,    0,
        0,    0,
        0,    0,
        0,    0,
    };
    const values = try decodePlainI64Alloc(alloc, parsed.header, &payload);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 8 }, values);
}

test "parquet page parser decodes hybrid rle and bit-packed levels" {
    const alloc = std.testing.allocator;
    const rle = [_]u8{
        6, 1,
        4, 0,
    };
    const rle_levels = try decodeHybridLevelsAlloc(alloc, &rle, 1, 5);
    defer alloc.free(rle_levels);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 1, 1, 0, 0 }, rle_levels);

    const two_bit_rle = [_]u8{ 6, 2 };
    const two_bit_rle_levels = try decodeHybridLevelsAlloc(alloc, &two_bit_rle, 2, 3);
    defer alloc.free(two_bit_rle_levels);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 2, 2, 2 }, two_bit_rle_levels);

    const bit_packed = [_]u8{
        3, 0b00010101,
    };
    const packed_levels = try decodeHybridLevelsAlloc(alloc, &bit_packed, 1, 5);
    defer alloc.free(packed_levels);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 0, 1, 0, 1 }, packed_levels);

    try std.testing.expectError(error.InvalidParquetPage, decodeHybridLevelsAlloc(alloc, &[_]u8{3}, 1, 3));
    try std.testing.expectError(error.InvalidParquetPage, decodeHybridLevelsAlloc(alloc, &[_]u8{ 2, 1, 0 }, 1, 1));
}

test "parquet page parser decodes optional plain i64 v2 byte definition levels" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageV2HeaderFixtureWithLevels(alloc, 3, 19, 3, 0);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);
    try std.testing.expectEqual(PageType.data_page_v2, parsed.header.page_type);
    try std.testing.expectEqual(@as(usize, 3), parsed.header.definition_level_bytes);
    try std.testing.expectEqual(@as(usize, 0), parsed.header.repetition_level_bytes);
    try std.testing.expectEqual(@as(usize, 3), parsed.header.data_payload_offset);

    const payload = [_]u8{
        1, 0, 1,
        7, 0, 0,
        0, 0, 0,
        0, 0, 9,
        0, 0, 0,
        0, 0, 0,
        0,
    };
    var decoded = try decodeOptionalPlainI64V2ByteLevelsAlloc(alloc, parsed.header, &payload);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 0, 9 }, decoded.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, decoded.nulls);
}

test "parquet page parser decodes optional plain i64 v2 hybrid definition levels" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageV2HeaderFixtureWithLevels(alloc, 3, 18, 2, 0);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);

    const payload = [_]u8{
        3, 0b00000101,
        7, 0,
        0, 0,
        0, 0,
        0, 0,
        9, 0,
        0, 0,
        0, 0,
        0, 0,
    };
    var decoded = try decodeOptionalPlainI64V2HybridLevelsAlloc(alloc, parsed.header, &payload);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 0, 9 }, decoded.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, decoded.nulls);
}

test "parquet page parser decodes optional plain f64 v2 hybrid definition levels" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageV2HeaderFixtureWithLevels(alloc, 3, 18, 2, 0);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);

    var payload = [_]u8{
        3, 0b00000101,
        0, 0,
        0, 0,
        0, 0,
        0, 0,
        0, 0,
        0, 0,
        0, 0,
        0, 0,
    };
    std.mem.writeInt(u64, payload[2..10][0..8], @bitCast(@as(f64, 1.5)), .little);
    std.mem.writeInt(u64, payload[10..18][0..8], @bitCast(@as(f64, 2.25)), .little);

    var decoded = try decodeOptionalPlainF64V2HybridLevelsAlloc(alloc, parsed.header, &payload);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.5, 0, 2.25 }, decoded.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, decoded.nulls);
}

test "parquet page parser decodes optional plain boolean v2 hybrid definition levels" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageV2HeaderFixtureWithLevels(alloc, 4, 3, 2, 0);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);

    const payload = [_]u8{
        3,          0b00001101,
        0b00000110,
    };
    var decoded = try decodeOptionalPlainBoolV2HybridLevelsAlloc(alloc, parsed.header, &payload);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, false, true, true }, decoded.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0, 0 }, decoded.nulls);
}

test "parquet page scanner concatenates uncompressed plain i64 pages" {
    const alloc = std.testing.allocator;
    var header_a = try buildDataPageHeaderFixture(alloc, 2, 16);
    defer header_a.deinit(alloc);
    var header_b = try buildDataPageHeaderFixture(alloc, 1, 8);
    defer header_b.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header_a.items);
    try chunk.appendSlice(alloc, &[_]u8{
        10, 0, 0, 0, 0, 0, 0, 0,
        20, 0, 0, 0, 0, 0, 0, 0,
    });
    try chunk.appendSlice(alloc, header_b.items);
    try chunk.appendSlice(alloc, &[_]u8{
        30, 0, 0, 0, 0, 0, 0, 0,
    });

    const values = try scanUncompressedPlainI64ColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, values);
}

test "parquet page scanner decodes int96 timestamp pages as ns" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageHeaderFixture(alloc, 2, 24);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    var payload: [24]u8 = undefined;
    std.mem.writeInt(u64, payload[0..8], 1_000_000_000, .little);
    std.mem.writeInt(u32, payload[8..12], 2_440_588, .little);
    std.mem.writeInt(u64, payload[12..20], 2_000_000_000, .little);
    std.mem.writeInt(u32, payload[20..24], 2_440_589, .little);
    try chunk.appendSlice(alloc, &payload);

    const values = try scanUncompressedPlainInt96TimestampNsColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1_000_000_000, 86_402_000_000_000 }, values);
}

test "parquet page scanner decodes plain f64 pages" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageHeaderFixture(alloc, 2, 16);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    var payload: [16]u8 = undefined;
    std.mem.writeInt(u64, payload[0..8], @bitCast(@as(f64, 1.5)), .little);
    std.mem.writeInt(u64, payload[8..16], @bitCast(@as(f64, 2.25)), .little);
    try chunk.appendSlice(alloc, &payload);

    const values = try scanUncompressedPlainF64ColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.5, 2.25 }, values);
}

test "parquet page scanner decodes plain f32 pages as f64" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageHeaderFixture(alloc, 2, 8);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    var payload: [8]u8 = undefined;
    std.mem.writeInt(u32, payload[0..4], @bitCast(@as(f32, 1.25)), .little);
    std.mem.writeInt(u32, payload[4..8], @bitCast(@as(f32, 2.5)), .little);
    try chunk.appendSlice(alloc, &payload);

    const values = try scanUncompressedPlainF32AsF64ColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 2.5 }, values);
}

test "parquet page scanner decodes plain boolean pages" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageHeaderFixture(alloc, 5, 1);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    try chunk.append(alloc, 0b00001101);

    const values = try scanUncompressedPlainBoolColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(bool, &[_]bool{ true, false, true, true, false }, values);
}

test "parquet page scanner decodes optional int96 timestamp v2 pages as ns" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageV2HeaderFixtureWithLevels(alloc, 3, 26, 2, 0);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    var payload: [26]u8 = undefined;
    payload[0] = 3;
    payload[1] = 0b00000101;
    std.mem.writeInt(u64, payload[2..10], 1_000_000_000, .little);
    std.mem.writeInt(u32, payload[10..14], 2_440_588, .little);
    std.mem.writeInt(u64, payload[14..22], 2_000_000_000, .little);
    std.mem.writeInt(u32, payload[22..26], 2_440_589, .little);
    try chunk.appendSlice(alloc, &payload);

    var values = try scanUncompressedOptionalPlainInt96TimestampNsColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1_000_000_000, 0, 86_402_000_000_000 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, values.nulls);
}

test "parquet page scanner decodes optional plain f32 v2 pages as f64" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageV2HeaderFixtureWithLevels(alloc, 3, 10, 2, 0);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    var payload: [10]u8 = undefined;
    payload[0] = 3;
    payload[1] = 0b00000101;
    std.mem.writeInt(u32, payload[2..6], @bitCast(@as(f32, 1.25)), .little);
    std.mem.writeInt(u32, payload[6..10], @bitCast(@as(f32, 2.5)), .little);
    try chunk.appendSlice(alloc, &payload);

    var values = try scanUncompressedOptionalPlainF32AsF64ColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 0, 2.5 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, values.nulls);
}

test "parquet page scanner decodes optional plain boolean v2 pages" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageV2HeaderFixtureWithLevels(alloc, 4, 3, 2, 0);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,          0b00001101,
        0b00000110,
    });

    var values = try scanUncompressedOptionalPlainBoolColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(bool, &[_]bool{ false, false, true, true }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0, 0 }, values.nulls);
}

test "parquet page scanner decodes snappy plain i64 pages" {
    const alloc = std.testing.allocator;
    const payload = [_]u8{
        10, 0, 0, 0, 0, 0, 0, 0,
        20, 0, 0, 0, 0, 0, 0, 0,
    };
    const compressed = try snappy.encode(alloc, &payload);
    defer alloc.free(compressed);
    var header = try buildDataPageHeaderFixtureWithSizesAndEncoding(
        alloc,
        2,
        @intCast(payload.len),
        @intCast(compressed.len),
        0,
    );
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    try chunk.appendSlice(alloc, compressed);

    const values = try scanPlainI64ColumnChunkAlloc(alloc, chunk.items, .snappy);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20 }, values);
    try std.testing.expectError(error.UnsupportedParquetPage, scanUncompressedPlainI64ColumnChunkAlloc(alloc, chunk.items));
}

test "parquet page scanner keeps v2 levels uncompressed and honors is_compressed" {
    const alloc = std.testing.allocator;
    const definition_levels = [_]u8{ 3, 0b00000101 };
    const values = [_]u8{
        10, 0, 0, 0, 0, 0, 0, 0,
        20, 0, 0, 0, 0, 0, 0, 0,
    };
    const compressed_values = try snappy.encode(alloc, &values);
    defer alloc.free(compressed_values);

    var compressed_header = try buildDataPageV2HeaderFixtureWithCompression(
        alloc,
        3,
        @intCast(definition_levels.len + values.len),
        @intCast(definition_levels.len + compressed_values.len),
        definition_levels.len,
        0,
        0,
        true,
    );
    defer compressed_header.deinit(alloc);
    var compressed_chunk = std.ArrayListUnmanaged(u8).empty;
    defer compressed_chunk.deinit(alloc);
    try compressed_chunk.appendSlice(alloc, compressed_header.items);
    try compressed_chunk.appendSlice(alloc, &definition_levels);
    try compressed_chunk.appendSlice(alloc, compressed_values);

    var compressed_result = try scanOptionalPlainI64ColumnChunkAlloc(alloc, compressed_chunk.items, .snappy);
    defer compressed_result.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 0, 20 }, compressed_result.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, compressed_result.nulls);

    var raw_header = try buildDataPageV2HeaderFixtureWithCompression(
        alloc,
        3,
        @intCast(definition_levels.len + values.len),
        @intCast(definition_levels.len + values.len),
        definition_levels.len,
        0,
        0,
        false,
    );
    defer raw_header.deinit(alloc);
    var raw_chunk = std.ArrayListUnmanaged(u8).empty;
    defer raw_chunk.deinit(alloc);
    try raw_chunk.appendSlice(alloc, raw_header.items);
    try raw_chunk.appendSlice(alloc, &definition_levels);
    try raw_chunk.appendSlice(alloc, &values);

    var raw_result = try scanOptionalPlainI64ColumnChunkAlloc(alloc, raw_chunk.items, .snappy);
    defer raw_result.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 0, 20 }, raw_result.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, raw_result.nulls);
}

test "parquet page decoder rejects oversized declared output before decompression" {
    try std.testing.expectError(error.ParquetPageTooLarge, decodePagePayloadAlloc(
        std.testing.allocator,
        .{
            .page_type = .data_page,
            .uncompressed_page_size = max_uncompressed_page_bytes + 1,
            .compressed_page_size = 1,
            .value_count = 1,
            .encoding = .plain,
        },
        .snappy,
        &[_]u8{0},
    ));
}

test "parquet page scanner decodes gzip plain i64 pages" {
    const alloc = std.testing.allocator;
    const payload = [_]u8{
        10, 0, 0, 0, 0, 0, 0, 0,
        20, 0, 0, 0, 0, 0, 0, 0,
    };
    var out_buf: [256]u8 = undefined;
    var out: std.Io.Writer = .fixed(&out_buf);
    var hist: [std.compress.flate.max_window_len]u8 = undefined;
    var compressor = try std.compress.flate.Compress.init(&out, hist[0..], .gzip, .default);
    try compressor.writer.writeAll(&payload);
    try compressor.finish();
    const compressed = out.buffered();

    var header = try buildDataPageHeaderFixtureWithSizesAndEncoding(
        alloc,
        2,
        @intCast(payload.len),
        @intCast(compressed.len),
        0,
    );
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    try chunk.appendSlice(alloc, compressed);

    const values = try scanPlainI64ColumnChunkAlloc(alloc, chunk.items, .gzip);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20 }, values);
    try std.testing.expectError(error.UnsupportedParquetPage, scanUncompressedPlainI64ColumnChunkAlloc(alloc, chunk.items));
}

test "parquet page scanner decodes zstd plain i64 pages" {
    const alloc = std.testing.allocator;
    const payload = [_]u8{
        10, 0, 0, 0, 0, 0, 0, 0,
        20, 0, 0, 0, 0, 0, 0, 0,
    };
    const compressed = [_]u8{
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x81, 0x00, 0x00, 0x0a, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0xd7, 0x96, 0xbf, 0xba,
    };

    var header = try buildDataPageHeaderFixtureWithSizesAndEncoding(
        alloc,
        2,
        @intCast(payload.len),
        @intCast(compressed.len),
        0,
    );
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    try chunk.appendSlice(alloc, &compressed);

    const values = try scanPlainI64ColumnChunkAlloc(alloc, chunk.items, .zstd);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20 }, values);
    try std.testing.expectError(error.UnsupportedParquetPage, scanUncompressedPlainI64ColumnChunkAlloc(alloc, chunk.items));
}

test "parquet page scanner decodes dictionary i64 pages" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 3, 24);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDictionaryDataPageHeaderFixture(alloc, 4, 4);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        100, 0, 0, 0, 0, 0, 0, 0,
        110, 0, 0, 0, 0, 0, 0, 0,
        120, 0, 0, 0, 0, 0, 0, 0,
    });
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        2,
        3,
        0b10010010,
        0,
    });

    const values = try scanUncompressedDictionaryI64ColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 120, 100, 110, 120 }, values);

    const parsed_data = try parsePageHeader(data_header.items);
    const invalid_payload = [_]u8{ 1, 3, 0b00000010 };
    try std.testing.expectError(
        error.InvalidParquetPage,
        decodeDictionaryI64DataPageAlloc(alloc, parsed_data.header, &[_]i64{100}, &invalid_payload),
    );
}

test "parquet page scanner decodes optional dictionary i64 v2 pages" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 3, 24);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDataPageV2HeaderFixtureWithLevelsAndEncoding(alloc, 4, 6, 2, 0, 7);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        100, 0, 0, 0, 0, 0, 0, 0,
        110, 0, 0, 0, 0, 0, 0, 0,
        120, 0, 0, 0, 0, 0, 0, 0,
    });
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,          0b00001101,
        2,          3,
        0b00011000, 0,
    });

    var values = try scanUncompressedOptionalDictionaryI64ColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 0, 120, 110 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0, 0 }, values.nulls);
}

test "parquet page scanner decodes dictionary f64 pages" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 2, 16);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDictionaryDataPageHeaderFixture(alloc, 3, 3);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    var dictionary_payload: [16]u8 = undefined;
    std.mem.writeInt(u64, dictionary_payload[0..8], @bitCast(@as(f64, 1.25)), .little);
    std.mem.writeInt(u64, dictionary_payload[8..16], @bitCast(@as(f64, 2.5)), .little);
    try chunk.appendSlice(alloc, &dictionary_payload);
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        1,
        3,
        0b00000110,
    });

    const values = try scanUncompressedDictionaryF64ColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 2.5, 2.5 }, values);
}

test "parquet page scanner decodes optional dictionary f64 v2 pages" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 2, 16);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDataPageV2HeaderFixtureWithLevelsAndEncoding(alloc, 3, 5, 2, 0, 7);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    var dictionary_payload: [16]u8 = undefined;
    std.mem.writeInt(u64, dictionary_payload[0..8], @bitCast(@as(f64, 0.125)), .little);
    std.mem.writeInt(u64, dictionary_payload[8..16], @bitCast(@as(f64, 0.5)), .little);
    try chunk.appendSlice(alloc, &dictionary_payload);
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,          0b00000101,
        1,          3,
        0b00000010,
    });

    var values = try scanUncompressedOptionalDictionaryF64ColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 0.125, 0, 0.5 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, values.nulls);
}

test "parquet page scanner decodes dictionary f32 pages as f64" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 2, 8);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDictionaryDataPageHeaderFixture(alloc, 3, 3);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    var dictionary_payload: [8]u8 = undefined;
    std.mem.writeInt(u32, dictionary_payload[0..4], @bitCast(@as(f32, 1.25)), .little);
    std.mem.writeInt(u32, dictionary_payload[4..8], @bitCast(@as(f32, 2.5)), .little);
    try chunk.appendSlice(alloc, &dictionary_payload);
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        1,
        3,
        0b00000110,
    });

    const values = try scanUncompressedDictionaryF32AsF64ColumnChunkAlloc(alloc, chunk.items);
    defer alloc.free(values);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 1.25, 2.5, 2.5 }, values);
}

test "parquet page scanner decodes optional dictionary f32 v2 pages as f64" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 2, 8);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDataPageV2HeaderFixtureWithLevelsAndEncoding(alloc, 3, 5, 2, 0, 7);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    var dictionary_payload: [8]u8 = undefined;
    std.mem.writeInt(u32, dictionary_payload[0..4], @bitCast(@as(f32, 0.125)), .little);
    std.mem.writeInt(u32, dictionary_payload[4..8], @bitCast(@as(f32, 0.5)), .little);
    try chunk.appendSlice(alloc, &dictionary_payload);
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,          0b00000101,
        1,          3,
        0b00000010,
    });

    var values = try scanUncompressedOptionalDictionaryF32AsF64ColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(f64, &[_]f64{ 0.125, 0, 0.5 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, values.nulls);
}

test "parquet page scanner decodes optional plain i32 v2 pages" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageV2HeaderFixtureWithLevels(alloc, 3, 10, 2, 0);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3, 0b00000101,
        7, 0,
        0, 0,
        9, 0,
        0, 0,
    });

    var values = try scanUncompressedOptionalPlainI32AsI64ColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 7, 0, 9 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, values.nulls);
}

test "parquet page scanner decodes optional dictionary i32 v2 pages" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 3, 12);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDataPageV2HeaderFixtureWithLevelsAndEncoding(alloc, 4, 6, 2, 0, 7);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        100, 0, 0, 0,
        110, 0, 0, 0,
        120, 0, 0, 0,
    });
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,          0b00001101,
        2,          3,
        0b00011000, 0,
    });

    var values = try scanUncompressedOptionalDictionaryI32AsI64ColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 100, 0, 120, 110 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0, 0 }, values.nulls);
}

test "parquet page scanner decodes dictionary byte array pages" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 2, 12);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDictionaryDataPageHeaderFixture(alloc, 3, 3);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        2, 0, 0, 0, 't', '1',
        2, 0, 0, 0, 't', '2',
    });
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        1,
        3,
        0b00000110,
    });

    const values = try scanUncompressedDictionaryByteArrayColumnChunkAlloc(alloc, chunk.items);
    defer freePlainByteArrays(alloc, values);
    try std.testing.expectEqualStrings("t1", values[0]);
    try std.testing.expectEqualStrings("t2", values[1]);
    try std.testing.expectEqualStrings("t2", values[2]);

    const parsed_data = try parsePageHeader(data_header.items);
    const invalid_payload = [_]u8{ 1, 3, 0b00000010 };
    try std.testing.expectError(
        error.InvalidParquetPage,
        decodeDictionaryByteArrayDataPageAlloc(alloc, parsed_data.header, &[_][]const u8{"t1"}, &invalid_payload),
    );
}

test "parquet page scanner decodes optional plain byte array v2 pages" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageV2HeaderFixtureWithLevels(alloc, 3, 13, 2, 0);
    defer header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,   0b00000101,
        2,   0,
        0,   0,
        'h', 'i',
        1,   0,
        0,   0,
        'z',
    });

    var values = try scanUncompressedOptionalPlainByteArrayColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualStrings("hi", values.values[0]);
    try std.testing.expectEqualStrings("", values.values[1]);
    try std.testing.expectEqualStrings("z", values.values[2]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, values.nulls);
}

test "parquet page scanner decodes optional dictionary byte array v2 pages" {
    const alloc = std.testing.allocator;
    var dictionary_header = try buildDictionaryPageHeaderFixture(alloc, 2, 12);
    defer dictionary_header.deinit(alloc);
    var data_header = try buildDataPageV2HeaderFixtureWithLevelsAndEncoding(alloc, 3, 5, 2, 0, 7);
    defer data_header.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, dictionary_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        2, 0, 0, 0, 't', '1',
        2, 0, 0, 0, 't', '2',
    });
    try chunk.appendSlice(alloc, data_header.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,          0b00000101,
        1,          3,
        0b00000010,
    });

    var values = try scanUncompressedOptionalDictionaryByteArrayColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualStrings("t1", values.values[0]);
    try std.testing.expectEqualStrings("", values.values[1]);
    try std.testing.expectEqualStrings("t2", values.values[2]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0 }, values.nulls);
}

test "parquet page scanner concatenates optional plain i64 pages" {
    const alloc = std.testing.allocator;
    var header_a = try buildDataPageV2HeaderFixtureWithLevels(alloc, 2, 10, 2, 0);
    defer header_a.deinit(alloc);
    var header_b = try buildDataPageV2HeaderFixtureWithLevels(alloc, 2, 18, 2, 0);
    defer header_b.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header_a.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,  1,
        10, 0,
        0,  0,
        0,  0,
        0,  0,
    });
    try chunk.appendSlice(alloc, header_b.items);
    try chunk.appendSlice(alloc, &[_]u8{
        3,  3,
        30, 0,
        0,  0,
        0,  0,
        0,  0,
        40, 0,
        0,  0,
        0,  0,
        0,  0,
    });

    var values = try scanUncompressedOptionalPlainI64ColumnChunkAlloc(alloc, chunk.items);
    defer values.deinit(alloc);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 0, 30, 40 }, values.values);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1, 0, 0 }, values.nulls);
}

test "parquet page scanner concatenates uncompressed plain byte array pages" {
    const alloc = std.testing.allocator;
    var header_a = try buildDataPageHeaderFixture(alloc, 1, 9);
    defer header_a.deinit(alloc);
    var header_b = try buildDataPageHeaderFixture(alloc, 1, 9);
    defer header_b.deinit(alloc);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, header_a.items);
    try chunk.appendSlice(alloc, &[_]u8{ 5, 0, 0, 0, 'a', 'l', 'p', 'h', 'a' });
    try chunk.appendSlice(alloc, header_b.items);
    try chunk.appendSlice(alloc, &[_]u8{ 5, 0, 0, 0, 'o', 'm', 'e', 'g', 'a' });

    const values = try scanUncompressedPlainByteArrayColumnChunkAlloc(alloc, chunk.items);
    defer freePlainByteArrays(alloc, values);
    try std.testing.expectEqualStrings("alpha", values[0]);
    try std.testing.expectEqualStrings("omega", values[1]);
}

test "parquet page scanner rejects compressed pages for now" {
    const alloc = std.testing.allocator;
    var header = try buildDataPageHeaderFixture(alloc, 1, 8);
    defer header.deinit(alloc);
    var parsed = try parsePageHeader(header.items);
    parsed.header.uncompressed_page_size = 16;
    try std.testing.expectError(error.UnsupportedParquetPage, parsed.header.validateUncompressedPlainRequired());

    // Patch the top-level uncompressed_page_size in the encoded fixture from 8
    // to 16. The field value byte follows three bytes: field1+value, field2.
    header.items[3] = 32;
    try header.appendSlice(alloc, &[_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 });
    try std.testing.expectError(error.UnsupportedParquetPage, scanUncompressedPlainI64ColumnChunkAlloc(alloc, header.items));
}

test "parquet page parser rejects unsupported pages and truncated payloads" {
    const alloc = std.testing.allocator;
    var header_bytes = try buildDataPageHeaderFixture(alloc, 2, 16);
    defer header_bytes.deinit(alloc);
    const parsed = try parsePageHeader(header_bytes.items);
    const short = [_]u8{ 1, 0, 0, 0 };
    try std.testing.expectError(error.InvalidParquetPage, decodePlainI64Alloc(alloc, parsed.header, &short));

    var unsupported = parsed.header;
    unsupported.encoding = .rle_dictionary;
    const payload = [_]u8{0} ** 16;
    try std.testing.expectError(error.UnsupportedParquetPage, decodePlainI64Alloc(alloc, unsupported, &payload));
}
