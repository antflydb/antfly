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
//! This supports the first uncompressed required-column scan path. Dictionary
//! decoding, repetition/definition levels, compression codecs, and nested data
//! are separate scanner layers.

const std = @import("std");
const Allocator = std.mem.Allocator;

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

pub const Header = struct {
    page_type: PageType,
    uncompressed_page_size: u32,
    compressed_page_size: u32,
    value_count: u32,
    encoding: Encoding,
    data_payload_offset: usize = 0,

    pub fn validatePlainRequired(self: Header) !void {
        if (self.page_type != .data_page and self.page_type != .data_page_v2) return error.UnsupportedParquetPage;
        if (self.encoding != .plain) return error.UnsupportedParquetPage;
    }

    pub fn validateUncompressedPlainRequired(self: Header) !void {
        try self.validatePlainRequired();
        if (self.compressed_page_size != self.uncompressed_page_size) return error.UnsupportedParquetPage;
    }
};

pub const ParsedHeader = struct {
    header: Header,
    header_len: usize,
};

pub fn parsePageHeader(bytes: []const u8) !ParsedHeader {
    var reader = Reader{ .bytes = bytes };
    const header = try parseHeaderStruct(&reader);
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
    var values = std.ArrayListUnmanaged(i64).empty;
    errdefer values.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateUncompressedPlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;

        const page_values = try decodePlainI64Alloc(alloc, parsed.header, payload);
        defer alloc.free(page_values);
        try values.appendSlice(alloc, page_values);
    }
    return try values.toOwnedSlice(alloc);
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

pub fn scanUncompressedPlainByteArrayColumnChunkAlloc(alloc: Allocator, column_chunk_bytes: []const u8) ![][]u8 {
    var values = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var cursor: usize = 0;
    while (cursor < column_chunk_bytes.len) {
        const parsed = try parsePageHeader(column_chunk_bytes[cursor..]);
        try parsed.header.validateUncompressedPlainRequired();
        cursor += parsed.header_len;
        if (parsed.header.compressed_page_size > column_chunk_bytes.len - cursor) return error.InvalidParquetPage;
        const payload = column_chunk_bytes[cursor .. cursor + parsed.header.compressed_page_size];
        cursor += parsed.header.compressed_page_size;

        const page_values = try decodePlainByteArraysAlloc(alloc, parsed.header, payload);
        defer alloc.free(page_values);
        try values.ensureUnusedCapacity(alloc, page_values.len);
        for (page_values) |value| {
            values.appendAssumeCapacity(value);
        }
    }
    return try values.toOwnedSlice(alloc);
}

pub fn freePlainByteArrays(alloc: Allocator, values: [][]u8) void {
    for (values) |value| alloc.free(value);
    alloc.free(values);
}

fn parseHeaderStruct(reader: *Reader) !Header {
    var previous_field_id: i16 = 0;
    var page_type: ?PageType = null;
    var uncompressed_page_size: ?u32 = null;
    var compressed_page_size: ?u32 = null;
    var data_page: ?DataPageHeader = null;
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
        .data_page_v2 => data_page_v2 orelse return error.InvalidParquetPage,
        else => return error.UnsupportedParquetPage,
    };
    return .{
        .page_type = got_page_type,
        .uncompressed_page_size = uncompressed_page_size orelse return error.InvalidParquetPage,
        .compressed_page_size = compressed_page_size orelse return error.InvalidParquetPage,
        .value_count = page_header.value_count,
        .encoding = page_header.encoding,
        .data_payload_offset = page_header.data_payload_offset,
    };
}

const DataPageHeader = struct {
    value_count: u32,
    encoding: Encoding,
    data_payload_offset: usize = 0,
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

fn parseDataPageHeaderV2(reader: *Reader) !DataPageHeader {
    var previous_field_id: i16 = 0;
    var value_count: ?u32 = null;
    var encoding: ?Encoding = null;
    var definition_level_bytes: usize = 0;
    var repetition_level_bytes: usize = 0;
    while (try reader.readFieldHeader(&previous_field_id)) |field| {
        switch (field.id) {
            1 => value_count = try reader.readRequiredU32(field.type),
            4 => encoding = try encodingFromInt(try reader.readRequiredI32(field.type)),
            5 => definition_level_bytes = @intCast(try reader.readRequiredU32(field.type)),
            6 => repetition_level_bytes = @intCast(try reader.readRequiredU32(field.type)),
            else => try reader.skip(field.type),
        }
    }
    return .{
        .value_count = value_count orelse return error.InvalidParquetPage,
        .encoding = encoding orelse return error.InvalidParquetPage,
        .data_payload_offset = definition_level_bytes + repetition_level_bytes,
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
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &data_prev, 3, .i32);
    try appendI32(&out, alloc, 2);
    try appendField(&out, alloc, &data_prev, 4, .i32);
    try appendI32(&out, alloc, 2);
    try appendStop(&out, alloc);

    try appendStop(&out, alloc);
    return out;
}

fn buildDataPageV2HeaderFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var page_prev: i16 = 0;
    try appendField(&out, alloc, &page_prev, 1, .i32);
    try appendI32(&out, alloc, 3);
    try appendField(&out, alloc, &page_prev, 2, .i32);
    try appendI32(&out, alloc, 18);
    try appendField(&out, alloc, &page_prev, 3, .i32);
    try appendI32(&out, alloc, 18);
    try appendField(&out, alloc, &page_prev, 8, .struct_);

    var data_prev: i16 = 0;
    try appendField(&out, alloc, &data_prev, 1, .i32);
    try appendI32(&out, alloc, 2);
    try appendField(&out, alloc, &data_prev, 2, .i32);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &data_prev, 3, .i32);
    try appendI32(&out, alloc, 2);
    try appendField(&out, alloc, &data_prev, 4, .i32);
    try appendI32(&out, alloc, 0);
    try appendField(&out, alloc, &data_prev, 5, .i32);
    try appendI32(&out, alloc, 1);
    try appendField(&out, alloc, &data_prev, 6, .i32);
    try appendI32(&out, alloc, 1);
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
