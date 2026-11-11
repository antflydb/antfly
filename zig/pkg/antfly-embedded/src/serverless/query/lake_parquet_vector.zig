// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Retains one repeated-float page and one vector across page boundaries.
const std = @import("std");
const page = @import("lake_parquet_page.zig");
const external = @import("../external_source/types.zig");
const A = std.mem.Allocator;
pub const Reader = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    output: std.heap.ArenaAllocator,
    dictionary: ?[]f64 = null,
    repetitions: []const u8 = &.{},
    definitions: []const u8 = &.{},
    values: []const f64 = &.{},
    position: usize = 0,
    value_position: usize = 0,
    maximum_definition: u8,
    null_definition: u8,
    vectors: []const []const f32 = &.{},
    nulls: []const u8 = &.{},
    pending: std.ArrayList(f32) = .empty,
    eof: bool = false,
    pub const Source = struct { ptr: *anyopaque, next: *const fn (*anyopaque, *Reader) anyerror!bool };
    pub const Row = struct { values: []const f32, is_null: bool };
    pub fn init(a: A, logical: []const u8) !Reader {
        var parts = std.mem.splitScalar(u8, logical, ':');
        if (!std.mem.eql(u8, parts.next() orelse "", "vector_f32")) return error.InvalidParquetMetadata;
        const maximum = try std.fmt.parseInt(u8, parts.next() orelse return error.InvalidParquetMetadata, 10);
        const optional = try std.fmt.parseInt(u8, parts.next() orelse return error.InvalidParquetMetadata, 10);
        if (optional > 1 or maximum < optional + 1 or maximum > optional + 2 or parts.next() != null) return error.InvalidParquetMetadata;
        return .{ .a = a, .arena = .init(a), .output = .init(a), .maximum_definition = maximum, .null_definition = optional };
    }
    pub fn deinit(self: *Reader) void {
        if (self.dictionary) |dictionary| self.a.free(dictionary);
        self.pending.deinit(self.a);
        self.output.deinit();
        self.arena.deinit();
    }
    pub fn decode(self: *Reader, chunk: external.ColumnChunk, original: page.Header, encoded: []const u8) !bool {
        _ = self.arena.reset(.free_all);
        const a = self.arena.allocator();
        const payload = try page.decodePagePayloadAlloc(a, original, try @import("lake_parquet_rowgroup.zig").compressionCodecForColumnChunk(chunk), encoded);
        if (original.page_type == .dictionary_page) {
            if (self.dictionary != null) return error.InvalidParquetPage;
            self.dictionary = if (std.mem.eql(u8, chunk.physical_type, "float")) try page.decodePlainF32DictionaryPageAsF64Alloc(self.a, original, payload.bytes) else try page.decodePlainF64DictionaryPageAlloc(self.a, original, payload.bytes);
            return false;
        }
        if (original.page_type != .data_page and original.page_type != .data_page_v2) return error.UnsupportedParquetPage;
        if (original.value_count == 0) return error.InvalidParquetPage;
        var offset: usize = 0;
        const repetitions = try levelBytes(original, payload.bytes, &offset, true);
        const definitions = try levelBytes(original, payload.bytes, &offset, false);
        self.repetitions = try page.decodeHybridLevelsAlloc(a, repetitions, 1, original.value_count);
        const width: u4 = @intCast(std.math.log2_int(u8, self.maximum_definition) + 1);
        self.definitions = try page.decodeHybridLevelsAlloc(a, definitions, width, original.value_count);
        var present: u32 = 0;
        for (self.repetitions, self.definitions) |repetition, definition| {
            if (repetition > 1 or definition > self.maximum_definition) return error.InvalidParquetPage;
            if (definition == self.maximum_definition) present += 1;
        }
        var header = original;
        header.value_count = present;
        header.data_payload_offset = 0;
        header.repetition_level_bytes = 0;
        header.definition_level_bytes = 0;
        self.values = if (present == 0) &.{} else if (header.encoding == .rle_dictionary or header.encoding == .plain_dictionary)
            try page.decodeDictionaryF64DataPageAlloc(a, header, self.dictionary orelse return error.InvalidParquetPage, payload.bytes[offset..])
        else if (std.mem.eql(u8, chunk.physical_type, "float")) try page.decodePlainF32AsF64Alloc(a, header, payload.bytes[offset..]) else try page.decodePlainF64Alloc(a, header, payload.bytes[offset..]);
        self.position = 0;
        self.value_position = 0;
        return true;
    }
    fn levelBytes(header: page.Header, payload: []const u8, offset: *usize, repetition: bool) ![]const u8 {
        const len: usize = if (header.page_type == .data_page_v2) (if (repetition) header.repetition_level_bytes else header.definition_level_bytes) else length: {
            if (header.definition_encoding != .rle or payload.len -| offset.* < 4) return error.InvalidParquetPage;
            const value = std.mem.readInt(u32, payload[offset.*..][0..4], .little);
            offset.* += 4;
            break :length value;
        };
        if (len > payload.len -| offset.*) return error.InvalidParquetPage;
        const result = payload[offset.*..][0..len];
        offset.* += len;
        return result;
    }
    pub fn available(self: *Reader, source: Source) !bool {
        while (self.position == self.repetitions.len) {
            if (self.value_position != self.values.len) return error.InvalidParquetPage;
            if (self.eof) return false;
            if (!try source.next(source.ptr, self)) {
                self.eof = true;
                return false;
            }
        }
        return true;
    }
    pub fn nextRow(self: *Reader, source: Source, out: A) !?Row {
        if (!try self.available(source)) return null;
        if (self.repetitions[self.position] != 0) return error.InvalidParquetPage;
        const is_null = self.definitions[self.position] < self.null_definition;
        self.pending.clearRetainingCapacity();
        while (true) {
            const current = self.definitions[self.position];
            if (current == self.maximum_definition) {
                if (self.pending.items.len >= 65536) return error.InvalidVectorDimensions;
                const value: f32 = @floatCast(self.values[self.value_position]);
                if (!std.math.isFinite(value)) return error.InvalidVectorValue;
                try self.pending.append(self.a, value);
                self.value_position += 1;
            } else if (current >= self.null_definition + 1 or self.pending.items.len != 0) return error.InvalidVectorValue;
            self.position += 1;
            if (!try self.available(source) or self.repetitions[self.position] == 0) break;
            if (current != self.maximum_definition) return error.InvalidParquetPage;
        }
        return .{ .values = try out.dupe(f32, self.pending.items), .is_null = is_null };
    }
};

test "Parquet vector lists carry a row across V1 pages and preserve V2 null and empty rows" {
    const a = std.testing.allocator;
    const chunk: external.ColumnChunk = .{ .column_id = @constCast("v"), .file_offset = 0, .compressed_len = 0, .physical_type = @constCast("float") };
    const Source = struct {
        cursor: usize = 0,
        header: page.Header,
        pages: []const []const u8,
        fn next(raw: *anyopaque, reader: *Reader) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cursor == self.pages.len) return false;
            const bytes = self.pages[self.cursor];
            self.cursor += 1;
            return reader.decode(chunk, self.header, bytes);
        }
    };
    // Two physical pages divide the vector [1,2,3] after its second value.
    const first = [_]u8{ 4, 0, 0, 0, 2, 0, 2, 1, 2, 0, 0, 0, 4, 3, 0, 0, 128, 63, 0, 0, 0, 64 };
    const second = [_]u8{ 4, 0, 0, 0, 2, 1, 2, 0, 2, 0, 0, 0, 4, 3, 0, 0, 64, 64, 0, 0, 128, 64 };
    var source: Source = .{ .header = .{ .page_type = .data_page, .encoding = .plain, .value_count = 2, .compressed_page_size = first.len, .uncompressed_page_size = first.len }, .pages = &.{ &first, &second } };
    var reader = try Reader.init(a, "vector_f32:3:1");
    defer reader.deinit();
    const input: Reader.Source = .{ .ptr = &source, .next = Source.next };
    const row = (try reader.nextRow(input, a)).?;
    defer a.free(row.values);
    try std.testing.expect(!row.is_null);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, row.values);
    const last = (try reader.nextRow(input, a)).?;
    defer a.free(last.values);
    try std.testing.expectEqualSlices(f32, &.{4}, last.values);
    try std.testing.expect(try reader.nextRow(input, a) == null);

    const nullable = [_]u8{ 6, 0, 2, 1, 2, 0, 2, 1, 4, 3, 0, 0, 160, 64, 0, 0, 192, 64 };
    source = .{ .header = .{ .page_type = .data_page_v2, .encoding = .plain, .value_count = 4, .compressed_page_size = nullable.len, .uncompressed_page_size = nullable.len, .repetition_level_bytes = 4, .definition_level_bytes = 6, .data_payload_offset = 10 }, .pages = &.{&nullable} };
    var optional = try Reader.init(a, "vector_f32:3:1");
    defer optional.deinit();
    const null_row = (try optional.nextRow(input, a)).?;
    defer a.free(null_row.values);
    try std.testing.expect(null_row.is_null);
    const empty = (try optional.nextRow(input, a)).?;
    defer a.free(empty.values);
    try std.testing.expect(!empty.is_null and empty.values.len == 0);
    const values = (try optional.nextRow(input, a)).?;
    defer a.free(values.values);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6 }, values.values);
    try std.testing.expect(try optional.nextRow(input, a) == null);
}

test "Parquet vector lists reject null coordinates and orphaned continuations" {
    const Source = struct {
        sent: bool = false,
        orphan: bool,
        fn next(raw: *anyopaque, reader: *Reader) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.sent) return false;
            self.sent = true;
            const encoded = [_]u8{ 2, @intFromBool(self.orphan), 2, 2 };
            return reader.decode(.{ .column_id = @constCast("v"), .file_offset = 0, .compressed_len = 4, .physical_type = @constCast("float") }, .{ .page_type = .data_page_v2, .encoding = .plain, .value_count = 1, .compressed_page_size = 4, .uncompressed_page_size = 4, .repetition_level_bytes = 2, .definition_level_bytes = 2, .data_payload_offset = 4 }, &encoded);
        }
    };
    for ([_]bool{ false, true }) |orphan| {
        var source: Source = .{ .orphan = orphan };
        var reader = try Reader.init(std.testing.allocator, "vector_f32:3:1");
        defer reader.deinit();
        try std.testing.expectError(if (orphan) error.InvalidParquetPage else error.InvalidVectorValue, reader.nextRow(.{ .ptr = &source, .next = Source.next }, std.testing.allocator));
    }
}
