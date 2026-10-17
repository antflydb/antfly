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

//! Cursor-owned projection pages retain wire payloads and row coordinates.
//! Only selected values cross the native/decimal interpretation boundary;
//! complete framing is validated once and no partial vector enters a cache.
const std = @import("std");
const page = @import("lake_parquet_page.zig");
const parquet = @import("lake_parquet_rowgroup.zig");
const types = @import("../../storage/rowsource/types.zig");
const Chunk = @import("../external_source/types.zig").ColumnChunk;
const A = std.mem.Allocator;
const Self = @This();
const absent = std.math.maxInt(u32);
a: A,
budget: @import("../../sql/memory_budget.zig"),
arena: std.heap.ArenaAllocator = undefined,
chunk: Chunk,
kind: enum { integer, number, boolean, bytes },
positions: []u32 = &.{},
indices: []u32 = &.{},
offsets: []u32 = &.{},
data: []const u8 = &.{},
dictionary: ?page.Dictionary = null,
width: usize = 0,
count: usize,
converted: usize = 0,

pub fn create(a: A, chunk: Chunk, header: page.Header, encoded: []const u8, dictionary: ?page.Dictionary, limit: usize) !?*Self {
    if (header.encoding != .plain and header.encoding != .plain_dictionary and header.encoding != .rle_dictionary) return null;
    if (header.repetition_level_bytes != 0) return null;
    const kind: @FieldType(Self, "kind") = if (std.mem.eql(u8, chunk.physical_type, "int64") or std.mem.eql(u8, chunk.physical_type, "int32")) .integer else if (std.mem.eql(u8, chunk.physical_type, "float") or std.mem.eql(u8, chunk.physical_type, "double")) .number else if (std.mem.eql(u8, chunk.physical_type, "boolean")) .boolean else if (std.mem.eql(u8, chunk.physical_type, "byte_array") or std.mem.eql(u8, chunk.physical_type, "fixed_len_byte_array")) .bytes else return null;
    // Keep unsupported logical interpretations on the established decoder.
    const logical = chunk.logical_type;
    if (logical.len != 0 and !std.mem.eql(u8, logical, "string") and !std.mem.eql(u8, logical, "decimal") and !std.mem.startsWith(u8, logical, "int") and !std.mem.startsWith(u8, logical, "timestamp_")) return null;
    if (std.mem.eql(u8, logical, "decimal") and (chunk.decimal_precision < 1 or chunk.decimal_precision > 38 or chunk.decimal_scale < 0)) return null;
    if (kind == .bytes and std.mem.eql(u8, chunk.physical_type, "fixed_len_byte_array") and !std.mem.eql(u8, logical, "decimal")) return null;
    if (header.encoding != .plain and (dictionary == null or kind == .boolean)) return null;
    if (header.encoding != .plain) {
        const valid = switch (kind) {
            .integer => dictionary.? == .i64,
            .number => dictionary.? == .f64,
            .bytes => dictionary.? == .bytes,
            .boolean => false,
        };
        if (!valid) return error.InvalidParquetPage;
    }
    const self = try a.create(Self);
    self.* = .{ .a = a, .budget = .{ .backing = a, .limit = limit }, .chunk = chunk, .kind = kind, .dictionary = if (header.encoding == .plain) null else dictionary, .count = header.value_count };
    self.arena = .init(self.budget.allocator());
    errdefer self.destroy();
    const alloc = self.arena.allocator();
    const payload = try page.decodePagePayloadAlloc(alloc, header, try parquet.compressionCodecForColumnChunk(chunk), encoded);
    var data: []const u8 = payload.bytes;
    self.positions = try alloc.alloc(u32, self.count);
    var present: usize = 0;
    if (chunk.nullable) {
        const flat = try page.flatOptionalPage(header, data);
        if (flat.header.definition_level_bytes == 0 or flat.header.data_payload_offset > flat.payload.len) return error.InvalidParquetPage;
        const levels = try page.decodeHybridLevelsAlloc(alloc, flat.payload[0..flat.header.definition_level_bytes], 1, self.count);
        for (levels, self.positions) |level, *position| {
            if (level > 1) return error.InvalidParquetPage;
            position.* = if (level == 0) absent else @intCast(present);
            present += level;
        }
        data = flat.payload[flat.header.data_payload_offset..];
    } else {
        for (self.positions, 0..) |*position, i| position.* = @intCast(i);
        present = self.count;
    }
    self.data = data;
    if (header.encoding != .plain) {
        if (present == 0) return self;
        if (data.len == 0 or data[0] > 32) return error.InvalidParquetPage;
        self.indices = try page.decodeHybridIndexesAlloc(alloc, data[1..], @intCast(data[0]), present);
        // All IDs are structural evidence, even when their values are deferred.
        const size = switch (dictionary.?) {
            inline else => |values| values.len,
        };
        for (self.indices) |id| if (id >= size) return error.InvalidParquetPage;
        return self;
    }
    self.width = if (std.mem.eql(u8, chunk.physical_type, "int32") or std.mem.eql(u8, chunk.physical_type, "float")) 4 else if (kind == .integer or kind == .number) 8 else if (std.mem.eql(u8, chunk.physical_type, "fixed_len_byte_array")) std.math.cast(usize, chunk.type_length) orelse return error.InvalidParquetPage else 0;
    if (kind == .bytes and self.width == 0) {
        self.offsets = try alloc.alloc(u32, present);
        var offset: usize = 0;
        for (self.offsets) |*position| {
            if (offset > data.len or data.len - offset < 4) return error.InvalidParquetPage;
            position.* = std.math.cast(u32, offset) orelse return error.ParquetPageTooLarge;
            const length = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            if (length > data.len - offset) return error.InvalidParquetPage;
            offset += length;
        }
        if (offset != data.len) return error.InvalidParquetPage;
    } else {
        const expected = if (kind == .boolean) (present + 7) / 8 else std.math.mul(usize, present, self.width) catch return error.InvalidParquetPage;
        if (self.width == 0 and kind != .boolean) return error.InvalidParquetPage;
        if (data.len != expected) return error.InvalidParquetPage;
    }
    return self;
}
pub fn stabilize(self: *Self, a: A) void {
    self.a = a;
    self.budget.backing = a;
}
pub fn destroyWithAllocator(self: *Self, a: A) void {
    self.budget.backing = a;
    self.arena.deinit();
    a.destroy(self);
}
pub fn destroy(self: *Self) void {
    const a = self.a;
    self.arena.deinit();
    a.destroy(self);
}
fn integer(self: *Self, position: usize) !i64 {
    if (self.dictionary) |dictionary| return switch (dictionary) {
        .i64 => |values| values[self.indices[position]],
        else => error.InvalidParquetPage,
    };
    const offset = position * self.width;
    return if (self.width == 4) std.mem.readInt(i32, self.data[offset..][0..4], .little) else std.mem.readInt(i64, self.data[offset..][0..8], .little);
}
fn number(self: *Self, position: usize) !f64 {
    if (self.dictionary) |dictionary| return switch (dictionary) {
        .f64 => |values| values[self.indices[position]],
        else => error.InvalidParquetPage,
    };
    const offset = position * self.width;
    return if (self.width == 4) @as(f32, @bitCast(std.mem.readInt(u32, self.data[offset..][0..4], .little))) else @as(f64, @bitCast(std.mem.readInt(u64, self.data[offset..][0..8], .little)));
}
fn bytes(self: *Self, position: usize) ![]const u8 {
    if (self.dictionary) |dictionary| return switch (dictionary) {
        .bytes => |values| values[self.indices[position]],
        else => error.InvalidParquetPage,
    };
    if (self.width != 0) return self.data[position * self.width ..][0..self.width];
    const offset = self.offsets[position];
    const length = std.mem.readInt(u32, self.data[offset..][0..4], .little);
    return self.data[offset + 4 ..][0..length];
}
pub fn project(self: *Self, a: A, begin: usize, keep: []const bool) !types.ColumnVector {
    if (begin > self.count or keep.len > self.count - begin) return error.InvalidParquetPage;
    const decimal = std.mem.eql(u8, self.chunk.logical_type, "decimal");
    const nulls = try a.alloc(u8, keep.len);
    var vector: types.ColumnVector = .{ .name = self.chunk.column_id, .nulls = .{ .bytes = nulls }, .values = undefined };
    if (!decimal and !std.mem.startsWith(u8, self.chunk.logical_type, "timestamp_")) if (self.dictionary) |dictionary| {
        // Preserve dictionary identities through projection so downstream
        // expression/group kernels can work on referenced entries once.
        const indices = try a.alloc(u32, keep.len);
        for (keep, nulls, indices, begin..) |selected, *flag, *id, row| {
            const position = self.positions[row];
            flag.* = @intFromBool(!selected or position == absent);
            id.* = if (flag.* == 0) self.indices[position] else 0;
            if (flag.* == 0) self.converted += 1;
        }
        vector.values = switch (dictionary) {
            .i64 => |values| .{ .dictionary_i64 = .{ .values = values, .indices = indices } },
            .f64 => |values| .{ .dictionary_f64 = .{ .values = values, .indices = indices } },
            .bytes => |values| .{ .dictionary_bytes = .{ .values = values, .indices = indices } },
        };
        return vector;
    };
    vector.values = if (decimal or self.kind == .bytes) .{ .bytes = try a.alloc([]const u8, keep.len) } else switch (self.kind) {
        .integer => .{ .i64 = try a.alloc(i64, keep.len) },
        .number => .{ .f64 = try a.alloc(f64, keep.len) },
        .boolean => .{ .bool = try a.alloc(bool, keep.len) },
        .bytes => unreachable,
    };
    for (keep, nulls, begin.., 0..) |selected, *flag, row, lane| {
        const position = self.positions[row];
        flag.* = @intFromBool(!selected or position == absent);
        switch (vector.values) {
            .bytes => |values_| {
                const values = @constCast(values_);
                if (flag.* != 0) {
                    values[lane] = "";
                    continue;
                }
                if (decimal) {
                    const unscaled: i128 = if (self.kind == .integer) try self.integer(position) else try @import("lake_decimal.zig").signedBigEndian(try self.bytes(position));
                    values[lane] = try @import("lake_decimal.zig").formatAlloc(a, unscaled, self.chunk.decimal_precision, self.chunk.decimal_scale);
                } else values[lane] = try self.bytes(position);
            },
            .i64 => |values_| {
                var value: i64 = if (flag.* != 0) 0 else try self.integer(position);
                if (flag.* == 0) {
                    const factor: i64 = if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_millis")) 1_000_000 else if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_micros")) 1_000 else 1;
                    value = std.math.mul(i64, value, factor) catch return error.InvalidParquetPage;
                }
                @constCast(values_)[lane] = value;
            },
            .f64 => |values_| @constCast(values_)[lane] = if (flag.* != 0) 0 else try self.number(position),
            .bool => |values_| @constCast(values_)[lane] = flag.* == 0 and (self.data[position / 8] & (@as(u8, 1) << @as(u3, @intCast(position % 8)))) != 0,
            else => unreachable,
        }
        if (flag.* == 0) self.converted += 1;
    }
    return vector;
}

test "lake SQL selected page owns framing borrows strings and preserves nullable coordinates" {
    const Helper = struct {
        fn run(a: A) !void {
            // Nullable V1 levels: present, NULL, present. Only one wide value
            // survives; the other stays as an immutable wire slice.
            const payload = [_]u8{ 6, 0, 0, 0, 2, 1, 2, 0, 2, 1, 3, 0, 0, 0, 'o', 'n', 'e', 3, 0, 0, 0, 't', 'w', 'o' };
            const header: page.Header = .{ .page_type = .data_page, .compressed_page_size = payload.len, .uncompressed_page_size = payload.len, .value_count = 3, .encoding = .plain };
            const chunk: Chunk = .{ .column_id = @constCast("s"), .file_offset = 0, .compressed_len = payload.len, .physical_type = @constCast("byte_array"), .nullable = true };
            const selected = (try create(a, chunk, header, &payload, null, 65536)).?;
            defer selected.destroy();
            var output = std.heap.ArenaAllocator.init(a);
            defer output.deinit();
            const vector = try selected.project(output.allocator(), 0, &.{ false, true, true });
            try std.testing.expect(vector.nulls.isNull(0) and vector.nulls.isNull(1) and !vector.nulls.isNull(2));
            try std.testing.expectEqualStrings("two", vector.values.bytes[2]);
            try std.testing.expectEqual(@as(usize, 1), selected.converted);
            // A later cohort can select an earlier value from the same page.
            const first = try selected.project(output.allocator(), 0, &.{true});
            try std.testing.expectEqualStrings("one", first.values.bytes[0]);
            var dictionary_values = [_][]u8{ @constCast("red"), @constCast("blue") };
            const encoded_ids = [_]u8{ 1, 2, 1, 2, 0 };
            var dictionary_header = header;
            dictionary_header.encoding = .rle_dictionary;
            dictionary_header.value_count = 2;
            dictionary_header.compressed_page_size = encoded_ids.len;
            dictionary_header.uncompressed_page_size = encoded_ids.len;
            var dictionary_chunk = chunk;
            dictionary_chunk.nullable = false;
            try std.testing.expectError(error.InvalidParquetPage, create(a, dictionary_chunk, dictionary_header, &encoded_ids, .{ .i64 = &.{} }, 65536));
            const borrowed = (try create(a, dictionary_chunk, dictionary_header, &encoded_ids, .{ .bytes = &dictionary_values }, 65536)).?;
            defer borrowed.destroy();
            const dictionary_vector = try borrowed.project(output.allocator(), 0, &.{ true, false });
            try std.testing.expectEqualSlices(u32, &.{ 1, 0 }, dictionary_vector.values.dictionary_bytes.indices);
            try std.testing.expect(@intFromPtr(dictionary_vector.values.dictionary_bytes.values.ptr) == @intFromPtr(dictionary_values[0..].ptr));
            try std.testing.expect(!dictionary_vector.nulls.isNull(0) and dictionary_vector.nulls.isNull(1));
        }
    };
    try Helper.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Helper.run, .{});
}
