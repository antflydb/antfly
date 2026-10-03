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

//! Typed columnar doc value storage.
//!
//! Unlike the untyped doc_values.zig, this stores a type tag per column,
//! enabling SIMD-friendly packed numeric access for aggregations and
//! range queries without per-value parsing.
//!
//! Supported types:
//!   - u64: unsigned 64-bit integers
//!   - i64: signed 64-bit integers
//!   - f64: 64-bit floating point
//!   - bytes: variable-length byte strings
//!   - geo_point: packed (lat, lon) as two f64s = 16 bytes
//!   - bool: single byte (0 or 1)
//!   - numeric: per-value tagged i64/u64/f64, preserving exact integer domains
//!
//! Wire format:
//!   [value_type: u8]
//!   [numChunks: u32 LE]
//!   [chunkOffset_0: u64 LE] ... [chunkOffset_N: u64 LE]
//!   [chunk_0_data: Snappy compressed] ...
//!
//! Per chunk (uncompressed):
//!   [numDocs: u32 LE]
//!   [docIDs: u32 LE × numDocs]
//!   For fixed-size types: [values: packed × numDocs]
//!   For bytes: per doc [valueLen: u32 LE][value: bytes]

const std = @import("std");
const Allocator = std.mem.Allocator;
const snappy = @import("../encoding/snappy.zig");

pub const ValueType = enum(u8) {
    u64_val = 0,
    f64_val = 1,
    bytes_val = 2,
    geo_point = 3,
    bool_val = 4,
    i64_val = 5,
    numeric_val = 6,
};

pub const NumericValue = union(enum(u8)) {
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
};

fn reverseOrder(order: std.math.Order) std.math.Order {
    return switch (order) {
        .lt => .gt,
        .eq => .eq,
        .gt => .lt,
    };
}

fn compareI64ToF64(a: i64, b: f64) std.math.Order {
    const min_i64_f = -9223372036854775808.0;
    const max_i64_plus_one_f = 9223372036854775808.0;
    if (b < min_i64_f) return .gt;
    if (b >= max_i64_plus_one_f) return .lt;
    if (b == min_i64_f) return std.math.order(a, std.math.minInt(i64));
    const truncated: i64 = @intFromFloat(b);
    const truncated_f: f64 = @floatFromInt(truncated);
    if (b == truncated_f) return std.math.order(a, truncated);
    if (b > 0) return if (a <= truncated) .lt else .gt;
    return if (a < truncated) .lt else .gt;
}

fn compareU64ToF64(a: u64, b: f64) std.math.Order {
    if (b < 0) return .gt;
    if (b >= 18446744073709551616.0) return .lt;
    const truncated: u64 = @intFromFloat(b);
    const truncated_f: f64 = @floatFromInt(truncated);
    if (b == truncated_f) return std.math.order(a, truncated);
    return if (a <= truncated) .lt else .gt;
}

pub fn compareNumericValues(a: NumericValue, b: NumericValue) std.math.Order {
    return switch (a) {
        .i64_val => |av| switch (b) {
            .i64_val => |bv| std.math.order(av, bv),
            .u64_val => |bv| if (av < 0) .lt else std.math.order(@as(u64, @intCast(av)), bv),
            .f64_val => |bv| compareI64ToF64(av, bv),
        },
        .u64_val => |av| switch (b) {
            .i64_val => |bv| if (bv < 0) .gt else std.math.order(av, @as(u64, @intCast(bv))),
            .u64_val => |bv| std.math.order(av, bv),
            .f64_val => |bv| compareU64ToF64(av, bv),
        },
        .f64_val => |av| switch (b) {
            .i64_val => |bv| reverseOrder(compareI64ToF64(bv, av)),
            .u64_val => |bv| reverseOrder(compareU64ToF64(bv, av)),
            .f64_val => |bv| std.math.order(av, bv),
        },
    };
}

pub fn numericValueAsF64(value: NumericValue) f64 {
    return switch (value) {
        .u64_val => |number| @floatFromInt(number),
        .i64_val => |number| @floatFromInt(number),
        .f64_val => |number| number,
    };
}

pub const GeoPoint = struct {
    lat: f64,
    lon: f64,
};

pub const TypedValue = union(enum) {
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
    bytes_val: []const u8,
    geo_point: GeoPoint,
    bool_val: bool,
    numeric_val: NumericValue,
};

fn typedValueMatchesValueType(value: TypedValue, value_type: ValueType) bool {
    return switch (value_type) {
        .u64_val => value == .u64_val,
        .i64_val => value == .i64_val,
        .f64_val => value == .f64_val,
        .bytes_val => value == .bytes_val,
        .geo_point => value == .geo_point,
        .bool_val => value == .bool_val,
        .numeric_val => value == .numeric_val,
    };
}

fn typedValueIsSerializable(value: TypedValue) bool {
    return switch (value) {
        .f64_val => |v| std.math.isFinite(v),
        .geo_point => |v| std.math.isFinite(v.lat) and std.math.isFinite(v.lon),
        .numeric_val => |v| switch (v) {
            .f64_val => |number| std.math.isFinite(number),
            else => true,
        },
        else => true,
    };
}

fn decodeSerializableF64(raw: [8]u8) !f64 {
    const value: f64 = @bitCast(raw);
    if (!std.math.isFinite(value)) return error.InvalidData;
    return value;
}

fn decodeSerializableBool(raw: u8) !bool {
    return switch (raw) {
        0 => false,
        1 => true,
        else => error.InvalidData,
    };
}

fn parseValueType(raw: u8) !ValueType {
    return switch (raw) {
        @backingInt(ValueType.u64_val) => .u64_val,
        @backingInt(ValueType.f64_val) => .f64_val,
        @backingInt(ValueType.bytes_val) => .bytes_val,
        @backingInt(ValueType.geo_point) => .geo_point,
        @backingInt(ValueType.bool_val) => .bool_val,
        @backingInt(ValueType.i64_val) => .i64_val,
        @backingInt(ValueType.numeric_val) => .numeric_val,
        else => error.InvalidData,
    };
}

fn valuesStartForDocIds(chunk_data: []const u8, num_docs: u32) !usize {
    const num_docs_usize: usize = @intCast(num_docs);
    if (num_docs_usize > (std.math.maxInt(usize) - 4) / 4) return error.InvalidData;
    const values_start = 4 + num_docs_usize * 4;
    if (values_start > chunk_data.len) return error.InvalidData;
    return values_start;
}

fn fixedValueSpanStart(chunk_data: []const u8, num_docs: u32, value_width: usize) !usize {
    const values_start = try valuesStartForDocIds(chunk_data, num_docs);
    const num_docs_usize: usize = @intCast(num_docs);
    if (value_width != 0 and num_docs_usize > (std.math.maxInt(usize) - values_start) / value_width) return error.InvalidData;
    const values_bytes = num_docs_usize * value_width;
    if (values_bytes > chunk_data.len - values_start) return error.InvalidData;
    return values_start;
}

fn fixedValueOffset(chunk_data: []const u8, num_docs: u32, pos: u32, value_width: usize) !usize {
    if (pos >= num_docs) return error.InvalidData;
    const values_start = try fixedValueSpanStart(chunk_data, num_docs, value_width);
    return values_start + @as(usize, @intCast(pos)) * value_width;
}

/// Default number of documents per chunk.
pub const default_chunk_size: u32 = 1024;

// ============================================================================
// Writer
// ============================================================================

pub const TypedDocValuesWriter = struct {
    alloc: Allocator,
    value_type: ValueType,
    chunk_size: u32,
    entries: std.ArrayListUnmanaged(Entry),
    last_doc_id: ?u32 = null,

    const Entry = struct {
        doc_id: u32,
        value: TypedValue,
        // For bytes_val, we own the data
        owned_bytes: ?[]u8 = null,
    };

    pub fn init(alloc: Allocator, value_type: ValueType, chunk_size: u32) TypedDocValuesWriter {
        return .{
            .alloc = alloc,
            .value_type = value_type,
            .chunk_size = chunk_size,
            .entries = .empty,
        };
    }

    pub fn deinit(self: *TypedDocValuesWriter) void {
        for (self.entries.items) |*e| {
            if (e.owned_bytes) |b| self.alloc.free(b);
        }
        self.entries.deinit(self.alloc);
    }

    pub fn estimatedMemoryBytes(self: *const TypedDocValuesWriter) u64 {
        var total: u64 = @as(u64, @intCast(self.entries.capacity)) * @sizeOf(Entry);
        for (self.entries.items) |entry| {
            if (entry.owned_bytes) |bytes| total +|= @intCast(bytes.len);
        }
        return total;
    }

    pub fn add(self: *TypedDocValuesWriter, doc_id: u32, value: TypedValue) !void {
        if (!typedValueMatchesValueType(value, self.value_type)) return error.InvalidData;
        if (!typedValueIsSerializable(value)) return error.InvalidData;
        if (self.last_doc_id) |last_doc_id| {
            if (doc_id <= last_doc_id) return error.InvalidData;
        }

        var entry = Entry{ .doc_id = doc_id, .value = value };
        // For bytes, dupe the data so we own it
        if (value == .bytes_val) {
            const owned = try self.alloc.dupe(u8, value.bytes_val);
            entry.owned_bytes = owned;
            entry.value = .{ .bytes_val = owned };
        }
        try self.entries.append(self.alloc, entry);
        self.last_doc_id = doc_id;
    }

    /// Build serialized typed doc values section. Caller owns returned bytes.
    pub fn build(self: *TypedDocValuesWriter) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);

        const num_entries: u32 = @intCast(self.entries.items.len);
        const num_chunks: u32 = if (num_entries == 0) 0 else (num_entries - 1) / self.chunk_size + 1;

        // Header: value_type + num_chunks
        try out.append(self.alloc, @backingInt(self.value_type));
        try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, num_chunks))));

        // Reserve space for chunk offset table
        const offset_table_start = out.items.len;
        const offset_table_bytes = @as(usize, num_chunks) * 8;
        try out.appendNTimes(self.alloc, 0, offset_table_bytes);

        // Write chunks
        for (0..num_chunks) |chunk_idx| {
            const start = @as(usize, chunk_idx) * self.chunk_size;
            const end = @min(start + self.chunk_size, num_entries);
            const chunk_entries = self.entries.items[start..end];

            // Build uncompressed chunk
            var chunk_data = std.ArrayListUnmanaged(u8).empty;
            defer chunk_data.deinit(self.alloc);

            const chunk_doc_count: u32 = @intCast(chunk_entries.len);
            try chunk_data.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, chunk_doc_count))));

            // Doc IDs
            for (chunk_entries) |e| {
                try chunk_data.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, e.doc_id))));
            }

            // Values (type-specific)
            for (chunk_entries) |e| {
                try self.writeValue(&chunk_data, e.value);
            }

            // Snappy compress
            const compressed = try snappy.encode(self.alloc, chunk_data.items);
            defer self.alloc.free(compressed);
            try out.appendSlice(self.alloc, compressed);

            // Write chunk end offset
            const chunk_end: u64 = @intCast(out.items.len);
            const off_pos = offset_table_start + chunk_idx * 8;
            out.items[off_pos..][0..8].* = @bitCast(std.mem.nativeToLittle(u64, chunk_end));
        }

        return try self.alloc.dupe(u8, out.items);
    }

    fn writeValue(self: *TypedDocValuesWriter, out: *std.ArrayListUnmanaged(u8), value: TypedValue) !void {
        switch (self.value_type) {
            .u64_val => {
                const v = value.u64_val;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, v))));
            },
            .i64_val => {
                const v = value.i64_val;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(std.mem.nativeToLittle(i64, v))));
            },
            .f64_val => {
                const v = value.f64_val;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(v)));
            },
            .geo_point => {
                const gp = value.geo_point;
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(gp.lat)));
                try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(gp.lon)));
            },
            .bool_val => {
                try out.append(self.alloc, if (value.bool_val) 1 else 0);
            },
            .bytes_val => {
                const bytes = value.bytes_val;
                const len: u32 = @intCast(bytes.len);
                try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, len))));
                try out.appendSlice(self.alloc, bytes);
            },
            .numeric_val => switch (value.numeric_val) {
                .u64_val => |v| {
                    try out.append(self.alloc, 0);
                    try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, v))));
                },
                .i64_val => |v| {
                    try out.append(self.alloc, 1);
                    try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(std.mem.nativeToLittle(i64, v))));
                },
                .f64_val => |v| {
                    try out.append(self.alloc, 2);
                    try out.appendSlice(self.alloc, &@as([8]u8, @bitCast(v)));
                },
            },
        }
    }
};

// ============================================================================
// Reader
// ============================================================================

pub const TypedDocValuesReader = struct {
    alloc: Allocator,
    data: []const u8,
    value_type: ValueType,
    num_chunks: u32,
    chunk_offsets: []const u8, // raw offset table bytes

    pub fn init(alloc: Allocator, data: []const u8) !TypedDocValuesReader {
        if (data.len < 5) return error.InvalidData;
        const value_type = try parseValueType(data[0]);
        const num_chunks = std.mem.readInt(u32, data[1..5], .little);
        if (@as(usize, num_chunks) > (std.math.maxInt(usize) - 5) / 8) return error.InvalidData;
        const offset_table_end = 5 + @as(usize, num_chunks) * 8;
        if (data.len < offset_table_end) return error.InvalidData;

        return .{
            .alloc = alloc,
            .data = data,
            .value_type = value_type,
            .num_chunks = num_chunks,
            .chunk_offsets = data[5..offset_table_end],
        };
    }

    fn chunkEndOffset(self: *const TypedDocValuesReader, chunk_idx: u32) u64 {
        const off = @as(usize, chunk_idx) * 8;
        return std.mem.readInt(u64, self.chunk_offsets[off..][0..8], .little);
    }

    fn chunkStartOffset(self: *const TypedDocValuesReader, chunk_idx: u32) u64 {
        if (chunk_idx == 0) return 5 + @as(u64, self.num_chunks) * 8;
        return self.chunkEndOffset(chunk_idx - 1);
    }

    fn chunkRange(self: *const TypedDocValuesReader, chunk_idx: u32) !struct { start: usize, end: usize } {
        if (chunk_idx >= self.num_chunks) return error.InvalidData;
        const start = self.chunkStartOffset(chunk_idx);
        const end = self.chunkEndOffset(chunk_idx);
        if (start > end) return error.InvalidData;
        if (end > self.data.len) return error.InvalidData;
        return .{ .start = @intCast(start), .end = @intCast(end) };
    }

    /// Decompress a chunk and return its raw bytes. Caller owns result.
    fn decompressChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u8 {
        const range = try self.chunkRange(chunk_idx);
        const compressed = self.data[range.start..range.end];
        return snappy.decode(self.alloc, compressed);
    }

    pub const DecodedChunk = struct {
        alloc: Allocator,
        data: []u8,
        value_type: ValueType,
        num_docs: u32,
        values_start: usize,

        pub const Entry = struct {
            doc_id: u32,
            value: TypedValue,
        };

        pub const Iterator = struct {
            chunk: *const DecodedChunk,
            pos: u32 = 0,
            bytes_cursor: usize,

            pub fn next(self: *Iterator) !?Entry {
                if (self.pos >= self.chunk.num_docs) return null;
                const pos: usize = @intCast(self.pos);
                const doc_offset = 4 + pos * 4;
                const doc_id = std.mem.readInt(u32, self.chunk.data[doc_offset..][0..4], .little);
                const value: TypedValue = switch (self.chunk.value_type) {
                    .u64_val => .{ .u64_val = std.mem.readInt(u64, self.chunk.data[self.chunk.values_start + pos * 8 ..][0..8], .little) },
                    .i64_val => .{ .i64_val = std.mem.readInt(i64, self.chunk.data[self.chunk.values_start + pos * 8 ..][0..8], .little) },
                    .f64_val => .{ .f64_val = try decodeSerializableF64(self.chunk.data[self.chunk.values_start + pos * 8 ..][0..8].*) },
                    .geo_point => blk: {
                        const value_offset = self.chunk.values_start + pos * 16;
                        break :blk .{ .geo_point = .{
                            .lat = try decodeSerializableF64(self.chunk.data[value_offset..][0..8].*),
                            .lon = try decodeSerializableF64(self.chunk.data[value_offset + 8 ..][0..8].*),
                        } };
                    },
                    .bool_val => .{ .bool_val = try decodeSerializableBool(self.chunk.data[self.chunk.values_start + pos]) },
                    .bytes_val => blk: {
                        if (self.bytes_cursor + 4 > self.chunk.data.len) return error.InvalidData;
                        const value_len = std.mem.readInt(u32, self.chunk.data[self.bytes_cursor..][0..4], .little);
                        self.bytes_cursor += 4;
                        if (value_len > self.chunk.data.len - self.bytes_cursor) return error.InvalidData;
                        const bytes = self.chunk.data[self.bytes_cursor..][0..value_len];
                        self.bytes_cursor += value_len;
                        break :blk .{ .bytes_val = bytes };
                    },
                    .numeric_val => .{ .numeric_val = try decodeNumericValue(self.chunk.data[self.chunk.values_start + pos * 9 ..][0..9].*) },
                };
                self.pos += 1;
                return .{ .doc_id = doc_id, .value = value };
            }
        };

        pub fn deinit(self: *DecodedChunk) void {
            self.alloc.free(self.data);
            self.* = undefined;
        }

        pub fn iterator(self: *const DecodedChunk) Iterator {
            return .{ .chunk = self, .bytes_cursor = self.values_start };
        }
    };

    /// Decode and validate one complete chunk for sequential consumers such as
    /// segment merge. This avoids the point-lookup API's repeated scan and
    /// decompression of every preceding chunk for each document.
    pub fn decodeChunk(self: *const TypedDocValuesReader, chunk_idx: u32) !DecodedChunk {
        const chunk_data = try self.decompressChunk(chunk_idx);
        errdefer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        try self.validateChunkValuePayload(chunk_data, num_docs);
        return .{
            .alloc = self.alloc,
            .data = chunk_data,
            .value_type = self.value_type,
            .num_docs = num_docs,
            .values_start = try valuesStartForDocIds(chunk_data, num_docs),
        };
    }

    /// Find which chunk contains a given doc_id by scanning chunk doc IDs.
    /// Returns (chunk_idx, position_within_chunk).
    pub fn findDoc(self: *const TypedDocValuesReader, doc_id: u32) !?struct { chunk_idx: u32, pos: u32, chunk_data: []u8 } {
        for (0..self.num_chunks) |ci| {
            const chunk_data = try self.decompressChunk(@intCast(ci));
            if (chunk_data.len < 4) {
                self.alloc.free(chunk_data);
                return error.InvalidData;
            }
            const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
            _ = valuesStartForDocIds(chunk_data, num_docs) catch |err| {
                self.alloc.free(chunk_data);
                return err;
            };
            for (0..num_docs) |i| {
                const off = 4 + i * 4;
                const did = std.mem.readInt(u32, chunk_data[off..][0..4], .little);
                if (did == doc_id) {
                    return .{ .chunk_idx = @intCast(ci), .pos = @intCast(i), .chunk_data = chunk_data };
                }
            }
            self.alloc.free(chunk_data);
        }
        return null;
    }

    /// Get a single u64 value for a doc.
    pub fn getU64(self: *const TypedDocValuesReader, doc_id: u32) !?u64 {
        if (self.value_type != .u64_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 8);
        return std.mem.readInt(u64, found.chunk_data[val_off..][0..8], .little);
    }

    /// Get a single i64 value for a doc.
    pub fn getI64(self: *const TypedDocValuesReader, doc_id: u32) !?i64 {
        if (self.value_type != .i64_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 8);
        return std.mem.readInt(i64, found.chunk_data[val_off..][0..8], .little);
    }

    /// Get a single f64 value for a doc.
    pub fn getF64(self: *const TypedDocValuesReader, doc_id: u32) !?f64 {
        if (self.value_type != .f64_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 8);
        return try decodeSerializableF64(found.chunk_data[val_off..][0..8].*);
    }

    pub fn getNumeric(self: *const TypedDocValuesReader, doc_id: u32) !?NumericValue {
        if (self.value_type != .numeric_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 9);
        return try decodeNumericValue(found.chunk_data[val_off..][0..9].*);
    }

    /// Get a single GeoPoint value for a doc.
    pub fn getGeoPoint(self: *const TypedDocValuesReader, doc_id: u32) !?GeoPoint {
        if (self.value_type != .geo_point) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 16);
        const lat = try decodeSerializableF64(found.chunk_data[val_off..][0..8].*);
        const lon = try decodeSerializableF64(found.chunk_data[val_off + 8 ..][0..8].*);
        return .{ .lat = lat, .lon = lon };
    }

    /// Get a single bool value for a doc.
    pub fn getBool(self: *const TypedDocValuesReader, doc_id: u32) !?bool {
        if (self.value_type != .bool_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const val_off = try fixedValueOffset(found.chunk_data, num_docs, found.pos, 1);
        return try decodeSerializableBool(found.chunk_data[val_off]);
    }

    /// Get a single bytes value for a doc. Caller owns the returned slice.
    pub fn getBytesAlloc(self: *const TypedDocValuesReader, doc_id: u32) !?[]u8 {
        if (self.value_type != .bytes_val) return error.InvalidData;
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        var cursor: usize = 4 + @as(usize, num_docs) * 4;
        for (0..found.pos) |_| {
            if (cursor + 4 > found.chunk_data.len) return error.InvalidData;
            const value_len = std.mem.readInt(u32, found.chunk_data[cursor..][0..4], .little);
            cursor += 4;
            if (value_len > found.chunk_data.len - cursor) return error.InvalidData;
            cursor += value_len;
        }
        if (cursor + 4 > found.chunk_data.len) return error.InvalidData;
        const value_len = std.mem.readInt(u32, found.chunk_data[cursor..][0..4], .little);
        cursor += 4;
        if (value_len > found.chunk_data.len - cursor) return error.InvalidData;
        return try self.alloc.dupe(u8, found.chunk_data[cursor..][0..value_len]);
    }

    /// Read all u64 values in a chunk. Caller owns returned slice.
    pub fn readU64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u64 {
        if (self.value_type != .u64_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
        const result = try self.alloc.alloc(u64, num_docs);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = std.mem.readInt(u64, chunk_data[off..][0..8], .little);
        }
        return result;
    }

    /// Read all i64 values in a chunk. Caller owns returned slice.
    pub fn readI64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]i64 {
        if (self.value_type != .i64_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
        const result = try self.alloc.alloc(i64, num_docs);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = std.mem.readInt(i64, chunk_data[off..][0..8], .little);
        }
        return result;
    }

    /// Read all f64 values in a chunk. Caller owns returned slice.
    pub fn readF64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]f64 {
        if (self.value_type != .f64_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
        const result = try self.alloc.alloc(f64, num_docs);
        errdefer self.alloc.free(result);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = try decodeSerializableF64(chunk_data[off..][0..8].*);
        }
        return result;
    }

    pub fn readNumericChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]NumericValue {
        if (self.value_type != .numeric_val) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 9);
        const result = try self.alloc.alloc(NumericValue, num_docs);
        errdefer self.alloc.free(result);
        for (0..num_docs) |i| {
            result[i] = try decodeNumericValue(chunk_data[values_start + i * 9 ..][0..9].*);
        }
        return result;
    }

    /// Read all GeoPoint values in a chunk. Caller owns returned slice.
    pub fn readGeoPointChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]GeoPoint {
        if (self.value_type != .geo_point) return error.InvalidData;
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = try fixedValueSpanStart(chunk_data, num_docs, 16);
        const result = try self.alloc.alloc(GeoPoint, num_docs);
        errdefer self.alloc.free(result);
        for (0..num_docs) |i| {
            const off = values_start + i * 16;
            result[i] = .{
                .lat = try decodeSerializableF64(chunk_data[off..][0..8].*),
                .lon = try decodeSerializableF64(chunk_data[off + 8 ..][0..8].*),
            };
        }
        return result;
    }

    /// Read doc IDs in a chunk. Caller owns returned slice.
    pub fn readChunkDocIds(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u32 {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        _ = try valuesStartForDocIds(chunk_data, num_docs);
        const result = try self.alloc.alloc(u32, num_docs);
        for (0..num_docs) |i| {
            const off = 4 + i * 4;
            result[i] = std.mem.readInt(u32, chunk_data[off..][0..4], .little);
        }
        return result;
    }

    /// Read all doc IDs in a chunk after validating that the typed value
    /// payload for the same chunk is decodable. Caller owns returned slice.
    pub fn readValidatedChunkDocIds(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u32 {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        if (chunk_data.len < 4) return error.InvalidData;
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        try self.validateChunkValuePayload(chunk_data, num_docs);

        const result = try self.alloc.alloc(u32, num_docs);
        for (0..num_docs) |i| {
            const off = 4 + i * 4;
            result[i] = std.mem.readInt(u32, chunk_data[off..][0..4], .little);
        }
        return result;
    }

    fn validateChunkValuePayload(
        self: *const TypedDocValuesReader,
        chunk_data: []const u8,
        num_docs: u32,
    ) !void {
        switch (self.value_type) {
            .u64_val, .i64_val => {
                _ = try fixedValueSpanStart(chunk_data, num_docs, 8);
            },
            .f64_val => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 8);
                for (0..num_docs) |i| {
                    const off = values_start + i * 8;
                    _ = try decodeSerializableF64(chunk_data[off..][0..8].*);
                }
            },
            .geo_point => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 16);
                for (0..num_docs) |i| {
                    const off = values_start + i * 16;
                    _ = try decodeSerializableF64(chunk_data[off..][0..8].*);
                    _ = try decodeSerializableF64(chunk_data[off + 8 ..][0..8].*);
                }
            },
            .bool_val => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 1);
                for (0..num_docs) |i| {
                    _ = try decodeSerializableBool(chunk_data[values_start + i]);
                }
            },
            .bytes_val => {
                var cursor = try valuesStartForDocIds(chunk_data, num_docs);
                for (0..num_docs) |_| {
                    if (cursor + 4 > chunk_data.len) return error.InvalidData;
                    const value_len = std.mem.readInt(u32, chunk_data[cursor..][0..4], .little);
                    cursor += 4;
                    if (value_len > chunk_data.len - cursor) return error.InvalidData;
                    cursor += value_len;
                }
            },
            .numeric_val => {
                const values_start = try fixedValueSpanStart(chunk_data, num_docs, 9);
                for (0..num_docs) |i| {
                    _ = try decodeNumericValue(chunk_data[values_start + i * 9 ..][0..9].*);
                }
            },
        }
    }
};

fn decodeNumericValue(raw: [9]u8) !NumericValue {
    return switch (raw[0]) {
        0 => .{ .u64_val = std.mem.readInt(u64, raw[1..9], .little) },
        1 => .{ .i64_val = std.mem.readInt(i64, raw[1..9], .little) },
        2 => .{ .f64_val = try decodeSerializableF64(raw[1..9].*) },
        else => error.InvalidData,
    };
}

// ============================================================================
// Tests
// ============================================================================

fn buildSingleDocFixedSectionAlloc(alloc: Allocator, value_type: ValueType, doc_id: u32, value_bytes: []const u8) ![]u8 {
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 1))));
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_id))));
    try chunk.appendSlice(alloc, value_bytes);

    const compressed = try snappy.encode(alloc, chunk.items);
    defer alloc.free(compressed);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(alloc);
    try data.append(alloc, @backingInt(value_type));
    try data.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 1))));
    const chunk_end: u64 = @intCast(5 + 8 + compressed.len);
    try data.appendSlice(alloc, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, chunk_end))));
    try data.appendSlice(alloc, compressed);
    return try data.toOwnedSlice(alloc);
}

test "typed doc values u64 round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .u64_val = 100 });
    try writer.add(1, .{ .u64_val = 200 });
    try writer.add(5, .{ .u64_val = 500 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(ValueType.u64_val, reader.value_type);

    try std.testing.expectEqual(@as(?u64, 100), try reader.getU64(0));
    try std.testing.expectEqual(@as(?u64, 200), try reader.getU64(1));
    try std.testing.expectEqual(@as(?u64, 500), try reader.getU64(5));
    try std.testing.expectEqual(@as(?u64, null), try reader.getU64(3));

    const doc_ids = try reader.readValidatedChunkDocIds(0);
    defer alloc.free(doc_ids);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 5 }, doc_ids);
}

test "typed doc values exact numeric domain round-trip and comparison" {
    const alloc = std.testing.allocator;
    var writer = TypedDocValuesWriter.init(alloc, .numeric_val, 2);
    defer writer.deinit();
    try writer.add(0, .{ .numeric_val = .{ .i64_val = -9007199254740993 } });
    try writer.add(1, .{ .numeric_val = .{ .f64_val = 10.5 } });
    try writer.add(2, .{ .numeric_val = .{ .u64_val = std.math.maxInt(u64) } });

    const data = try writer.build();
    defer alloc.free(data);
    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(NumericValue{ .i64_val = -9007199254740993 }, (try reader.getNumeric(0)).?);
    try std.testing.expectEqual(NumericValue{ .f64_val = 10.5 }, (try reader.getNumeric(1)).?);
    try std.testing.expectEqual(NumericValue{ .u64_val = std.math.maxInt(u64) }, (try reader.getNumeric(2)).?);
    try std.testing.expectEqual(std.math.Order.lt, compareNumericValues(.{ .i64_val = 10 }, .{ .f64_val = 10.5 }));
    try std.testing.expectEqual(std.math.Order.gt, compareNumericValues(.{ .u64_val = 9007199254740993 }, .{ .f64_val = 9007199254740992.0 }));
}

test "typed doc values writer rejects mismatched value type" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try std.testing.expectError(error.InvalidData, writer.add(0, .{ .i64_val = -1 }));
    try std.testing.expectError(error.InvalidData, writer.add(1, .{ .bytes_val = "not-u64" }));
    try std.testing.expectEqual(@as(usize, 0), writer.entries.items.len);
}

test "typed doc values writer rejects duplicate and out-of-order doc ids" {
    const alloc = std.testing.allocator;

    var duplicate_writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer duplicate_writer.deinit();
    try duplicate_writer.add(0, .{ .u64_val = 10 });
    try std.testing.expectError(error.InvalidData, duplicate_writer.add(0, .{ .u64_val = 20 }));

    var out_of_order_writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer out_of_order_writer.deinit();
    try out_of_order_writer.add(2, .{ .u64_val = 20 });
    try std.testing.expectError(error.InvalidData, out_of_order_writer.add(1, .{ .u64_val = 10 }));
}

test "typed doc values writer rejects non-finite floating values" {
    const alloc = std.testing.allocator;

    var f64_writer = TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer f64_writer.deinit();
    try std.testing.expectError(error.InvalidData, f64_writer.add(0, .{ .f64_val = std.math.nan(f64) }));
    try std.testing.expectError(error.InvalidData, f64_writer.add(0, .{ .f64_val = std.math.inf(f64) }));
    try f64_writer.add(0, .{ .f64_val = 1.5 });

    var numeric_writer = TypedDocValuesWriter.init(alloc, .numeric_val, 1024);
    defer numeric_writer.deinit();
    try std.testing.expectError(error.InvalidData, numeric_writer.add(0, .{ .numeric_val = .{ .f64_val = std.math.nan(f64) } }));
    try std.testing.expectError(error.InvalidData, numeric_writer.add(0, .{ .numeric_val = .{ .f64_val = std.math.inf(f64) } }));
    try numeric_writer.add(0, .{ .numeric_val = .{ .f64_val = 1.5 } });

    var geo_writer = TypedDocValuesWriter.init(alloc, .geo_point, 1024);
    defer geo_writer.deinit();
    try std.testing.expectError(error.InvalidData, geo_writer.add(0, .{ .geo_point = .{ .lat = std.math.nan(f64), .lon = 10.0 } }));
    try std.testing.expectError(error.InvalidData, geo_writer.add(0, .{ .geo_point = .{ .lat = 10.0, .lon = std.math.inf(f64) } }));
    try geo_writer.add(0, .{ .geo_point = .{ .lat = 10.0, .lon = 20.0 } });
}

test "typed doc values reader rejects mismatched accessor type" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .u64_val = 42 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(@as(?u64, 42), try reader.getU64(0));

    try std.testing.expectError(error.InvalidData, reader.getI64(0));
    try std.testing.expectError(error.InvalidData, reader.getF64(0));
    try std.testing.expectError(error.InvalidData, reader.getBool(0));
    try std.testing.expectError(error.InvalidData, reader.getBytesAlloc(0));
    try std.testing.expectError(error.InvalidData, reader.readI64Chunk(0));
    try std.testing.expectError(error.InvalidData, reader.readF64Chunk(0));
    try std.testing.expectError(error.InvalidData, reader.readGeoPointChunk(0));
}

test "typed doc values reader rejects malformed headers and chunk bounds" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.InvalidData, TypedDocValuesReader.init(alloc, &.{ 255, 0, 0, 0, 0 }));

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .u64_val = 42 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectError(error.InvalidData, reader.readU64Chunk(1));

    const corrupt_data = try alloc.dupe(u8, data);
    defer alloc.free(corrupt_data);
    std.mem.writeInt(u64, corrupt_data[5..][0..8], @as(u64, @intCast(corrupt_data.len + 1)), .little);

    var corrupt_reader = try TypedDocValuesReader.init(alloc, corrupt_data);
    try std.testing.expectError(error.InvalidData, corrupt_reader.readU64Chunk(0));
}

test "typed doc values reader rejects malformed bytes value lengths" {
    const alloc = std.testing.allocator;

    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 1))));
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 0))));
    try chunk.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 8))));
    try chunk.appendSlice(alloc, "abc");

    const compressed = try snappy.encode(alloc, chunk.items);
    defer alloc.free(compressed);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(alloc);
    try data.append(alloc, @backingInt(ValueType.bytes_val));
    try data.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 1))));
    const chunk_end: u64 = @intCast(5 + 8 + compressed.len);
    try data.appendSlice(alloc, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, chunk_end))));
    try data.appendSlice(alloc, compressed);

    var reader = try TypedDocValuesReader.init(alloc, data.items);
    try std.testing.expectError(error.InvalidData, reader.getBytesAlloc(0));
    try std.testing.expectError(error.InvalidData, reader.readValidatedChunkDocIds(0));
}

test "typed doc values reader rejects non-canonical bool values" {
    const alloc = std.testing.allocator;

    const data = try buildSingleDocFixedSectionAlloc(alloc, .bool_val, 0, &.{2});
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectError(error.InvalidData, reader.getBool(0));
    try std.testing.expectError(error.InvalidData, reader.readValidatedChunkDocIds(0));
}

test "typed doc values reader rejects non-finite floating payloads" {
    const alloc = std.testing.allocator;

    const nan_bytes = @as([8]u8, @bitCast(std.math.nan(f64)));
    const f64_data = try buildSingleDocFixedSectionAlloc(alloc, .f64_val, 0, &nan_bytes);
    defer alloc.free(f64_data);

    var f64_reader = try TypedDocValuesReader.init(alloc, f64_data);
    try std.testing.expectError(error.InvalidData, f64_reader.getF64(0));
    try std.testing.expectError(error.InvalidData, f64_reader.readF64Chunk(0));
    try std.testing.expectError(error.InvalidData, f64_reader.readValidatedChunkDocIds(0));

    var geo_bytes = std.ArrayListUnmanaged(u8).empty;
    defer geo_bytes.deinit(alloc);
    try geo_bytes.appendSlice(alloc, &@as([8]u8, @bitCast(std.math.inf(f64))));
    try geo_bytes.appendSlice(alloc, &@as([8]u8, @bitCast(@as(f64, 10.0))));

    const geo_data = try buildSingleDocFixedSectionAlloc(alloc, .geo_point, 0, geo_bytes.items);
    defer alloc.free(geo_data);

    var geo_reader = try TypedDocValuesReader.init(alloc, geo_data);
    try std.testing.expectError(error.InvalidData, geo_reader.getGeoPoint(0));
    try std.testing.expectError(error.InvalidData, geo_reader.readGeoPointChunk(0));
    try std.testing.expectError(error.InvalidData, geo_reader.readValidatedChunkDocIds(0));
}

test "typed doc values i64 round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .i64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .i64_val = -100 });
    try writer.add(1, .{ .i64_val = 0 });
    try writer.add(5, .{ .i64_val = 500 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(ValueType.i64_val, reader.value_type);

    try std.testing.expectEqual(@as(?i64, -100), try reader.getI64(0));
    try std.testing.expectEqual(@as(?i64, 0), try reader.getI64(1));
    try std.testing.expectEqual(@as(?i64, 500), try reader.getI64(5));
    try std.testing.expectEqual(@as(?i64, null), try reader.getI64(3));
}

test "typed doc values f64 round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .f64_val = 3.14 });
    try writer.add(1, .{ .f64_val = 2.718 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    const v0 = (try reader.getF64(0)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), v0, 0.001);

    const v1 = (try reader.getF64(1)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 2.718), v1, 0.001);
}

test "typed doc values geo_point round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .geo_point, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .geo_point = .{ .lat = 37.7749, .lon = -122.4194 } });
    try writer.add(1, .{ .geo_point = .{ .lat = 40.7128, .lon = -74.0060 } });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    const gp0 = (try reader.getGeoPoint(0)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 37.7749), gp0.lat, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, -122.4194), gp0.lon, 0.0001);

    const gp1 = (try reader.getGeoPoint(1)).?;
    try std.testing.expectApproxEqAbs(@as(f64, 40.7128), gp1.lat, 0.0001);
}

test "typed doc values bool round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .bool_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .bool_val = true });
    try writer.add(1, .{ .bool_val = false });
    try writer.add(2, .{ .bool_val = true });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    try std.testing.expectEqual(@as(?bool, true), try reader.getBool(0));
    try std.testing.expectEqual(@as(?bool, false), try reader.getBool(1));
    try std.testing.expectEqual(@as(?bool, true), try reader.getBool(2));
}

test "typed doc values bulk chunk read" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer writer.deinit();

    for (0..5) |i| {
        try writer.add(@intCast(i), .{ .u64_val = @intCast(i * 10) });
    }

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);

    const values = try reader.readU64Chunk(0);
    defer alloc.free(values);
    try std.testing.expectEqual(@as(usize, 5), values.len);
    try std.testing.expectEqual(@as(u64, 0), values[0]);
    try std.testing.expectEqual(@as(u64, 10), values[1]);
    try std.testing.expectEqual(@as(u64, 40), values[4]);

    const doc_ids = try reader.readChunkDocIds(0);
    defer alloc.free(doc_ids);
    try std.testing.expectEqual(@as(usize, 5), doc_ids.len);
    try std.testing.expectEqual(@as(u32, 0), doc_ids[0]);
    try std.testing.expectEqual(@as(u32, 4), doc_ids[4]);
}

test "typed doc values decoded chunk iterator preserves sparse values across chunks" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .u64_val, 2);
    defer writer.deinit();
    try writer.add(1, .{ .u64_val = 10 });
    try writer.add(4, .{ .u64_val = 40 });
    try writer.add(9, .{ .u64_val = 90 });
    try writer.add(15, .{ .u64_val = 150 });
    try writer.add(31, .{ .u64_val = 310 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(@as(u32, 3), reader.num_chunks);

    const expected_doc_ids = [_]u32{ 1, 4, 9, 15, 31 };
    const expected_values = [_]u64{ 10, 40, 90, 150, 310 };
    var value_index: usize = 0;
    for (0..reader.num_chunks) |chunk_index| {
        var chunk = try reader.decodeChunk(@intCast(chunk_index));
        defer chunk.deinit();
        var it = chunk.iterator();
        while (try it.next()) |entry| {
            try std.testing.expectEqual(expected_doc_ids[value_index], entry.doc_id);
            try std.testing.expectEqual(expected_values[value_index], entry.value.u64_val);
            value_index += 1;
        }
    }
    try std.testing.expectEqual(expected_doc_ids.len, value_index);
}

test "typed doc values decoded chunk iterator borrows byte values from its chunk" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .bytes_val, 2);
    defer writer.deinit();
    try writer.add(0, .{ .bytes_val = "alpha" });
    try writer.add(3, .{ .bytes_val = "beta" });
    try writer.add(8, .{ .bytes_val = "gamma" });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    const expected_doc_ids = [_]u32{ 0, 3, 8 };
    const expected_values = [_][]const u8{ "alpha", "beta", "gamma" };
    var value_index: usize = 0;
    for (0..reader.num_chunks) |chunk_index| {
        var chunk = try reader.decodeChunk(@intCast(chunk_index));
        defer chunk.deinit();
        var it = chunk.iterator();
        while (try it.next()) |entry| {
            try std.testing.expectEqual(expected_doc_ids[value_index], entry.doc_id);
            try std.testing.expectEqualStrings(expected_values[value_index], entry.value.bytes_val);
            value_index += 1;
        }
    }
    try std.testing.expectEqual(expected_doc_ids.len, value_index);
}

test "typed doc values bytes round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .bytes_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .bytes_val = "hello" });
    try writer.add(1, .{ .bytes_val = "world" });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(ValueType.bytes_val, reader.value_type);
    try std.testing.expectEqual(@as(u32, 1), reader.num_chunks);

    const doc0 = (try reader.getBytesAlloc(0)).?;
    defer alloc.free(doc0);
    try std.testing.expectEqualStrings("hello", doc0);

    const doc1 = (try reader.getBytesAlloc(1)).?;
    defer alloc.free(doc1);
    try std.testing.expectEqualStrings("world", doc1);

    try std.testing.expect((try reader.getBytesAlloc(2)) == null);

    const doc_ids = try reader.readValidatedChunkDocIds(0);
    defer alloc.free(doc_ids);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, doc_ids);
}
