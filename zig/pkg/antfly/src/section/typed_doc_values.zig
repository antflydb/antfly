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
//!   - f64: 64-bit floating point
//!   - bytes: variable-length byte strings
//!   - geo_point: packed (lat, lon) as two f64s = 16 bytes
//!   - bool: single byte (0 or 1)
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
};

pub const GeoPoint = struct {
    lat: f64,
    lon: f64,
};

pub const TypedValue = union(enum) {
    u64_val: u64,
    f64_val: f64,
    bytes_val: []const u8,
    geo_point: GeoPoint,
    bool_val: bool,
};

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

    pub fn add(self: *TypedDocValuesWriter, doc_id: u32, value: TypedValue) !void {
        var entry = Entry{ .doc_id = doc_id, .value = value };
        // For bytes, dupe the data so we own it
        if (value == .bytes_val) {
            const owned = try self.alloc.dupe(u8, value.bytes_val);
            entry.owned_bytes = owned;
            entry.value = .{ .bytes_val = owned };
        }
        try self.entries.append(self.alloc, entry);
    }

    /// Build serialized typed doc values section. Caller owns returned bytes.
    pub fn build(self: *TypedDocValuesWriter) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);

        const num_entries: u32 = @intCast(self.entries.items.len);
        const num_chunks: u32 = if (num_entries == 0) 0 else (num_entries - 1) / self.chunk_size + 1;

        // Header: value_type + num_chunks
        try out.append(self.alloc, @intFromEnum(self.value_type));
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
        const value_type: ValueType = @enumFromInt(data[0]);
        const num_chunks = std.mem.readInt(u32, data[1..5], .little);
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

    /// Decompress a chunk and return its raw bytes. Caller owns result.
    fn decompressChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u8 {
        const start: usize = @intCast(self.chunkStartOffset(chunk_idx));
        const end: usize = @intCast(self.chunkEndOffset(chunk_idx));
        const compressed = self.data[start..end];
        return snappy.decode(self.alloc, compressed);
    }

    /// Find which chunk contains a given doc_id by scanning chunk doc IDs.
    /// Returns (chunk_idx, position_within_chunk).
    pub fn findDoc(self: *const TypedDocValuesReader, doc_id: u32) !?struct { chunk_idx: u32, pos: u32, chunk_data: []u8 } {
        for (0..self.num_chunks) |ci| {
            const chunk_data = try self.decompressChunk(@intCast(ci));
            const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
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
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const values_start = 4 + @as(usize, num_docs) * 4;
        const val_off = values_start + @as(usize, found.pos) * 8;
        return std.mem.readInt(u64, found.chunk_data[val_off..][0..8], .little);
    }

    /// Get a single f64 value for a doc.
    pub fn getF64(self: *const TypedDocValuesReader, doc_id: u32) !?f64 {
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const values_start = 4 + @as(usize, num_docs) * 4;
        const val_off = values_start + @as(usize, found.pos) * 8;
        return @bitCast(found.chunk_data[val_off..][0..8].*);
    }

    /// Get a single GeoPoint value for a doc.
    pub fn getGeoPoint(self: *const TypedDocValuesReader, doc_id: u32) !?GeoPoint {
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const values_start = 4 + @as(usize, num_docs) * 4;
        const val_off = values_start + @as(usize, found.pos) * 16;
        const lat: f64 = @bitCast(found.chunk_data[val_off..][0..8].*);
        const lon: f64 = @bitCast(found.chunk_data[val_off + 8 ..][0..8].*);
        return .{ .lat = lat, .lon = lon };
    }

    /// Get a single bool value for a doc.
    pub fn getBool(self: *const TypedDocValuesReader, doc_id: u32) !?bool {
        const found = try self.findDoc(doc_id) orelse return null;
        defer self.alloc.free(found.chunk_data);
        const num_docs = std.mem.readInt(u32, found.chunk_data[0..4], .little);
        const values_start = 4 + @as(usize, num_docs) * 4;
        const val_off = values_start + @as(usize, found.pos);
        return found.chunk_data[val_off] != 0;
    }

    /// Read all u64 values in a chunk. Caller owns returned slice.
    pub fn readU64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u64 {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = 4 + @as(usize, num_docs) * 4;
        const result = try self.alloc.alloc(u64, num_docs);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = std.mem.readInt(u64, chunk_data[off..][0..8], .little);
        }
        return result;
    }

    /// Read all f64 values in a chunk. Caller owns returned slice.
    pub fn readF64Chunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]f64 {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = 4 + @as(usize, num_docs) * 4;
        const result = try self.alloc.alloc(f64, num_docs);
        for (0..num_docs) |i| {
            const off = values_start + i * 8;
            result[i] = @bitCast(chunk_data[off..][0..8].*);
        }
        return result;
    }

    /// Read all GeoPoint values in a chunk. Caller owns returned slice.
    pub fn readGeoPointChunk(self: *const TypedDocValuesReader, chunk_idx: u32) ![]GeoPoint {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const values_start = 4 + @as(usize, num_docs) * 4;
        const result = try self.alloc.alloc(GeoPoint, num_docs);
        for (0..num_docs) |i| {
            const off = values_start + i * 16;
            result[i] = .{
                .lat = @bitCast(chunk_data[off..][0..8].*),
                .lon = @bitCast(chunk_data[off + 8 ..][0..8].*),
            };
        }
        return result;
    }

    /// Read doc IDs in a chunk. Caller owns returned slice.
    pub fn readChunkDocIds(self: *const TypedDocValuesReader, chunk_idx: u32) ![]u32 {
        const chunk_data = try self.decompressChunk(chunk_idx);
        defer self.alloc.free(chunk_data);
        const num_docs = std.mem.readInt(u32, chunk_data[0..4], .little);
        const result = try self.alloc.alloc(u32, num_docs);
        for (0..num_docs) |i| {
            const off = 4 + i * 4;
            result[i] = std.mem.readInt(u32, chunk_data[off..][0..4], .little);
        }
        return result;
    }
};

// ============================================================================
// Tests
// ============================================================================

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

test "typed doc values bytes round-trip" {
    const alloc = std.testing.allocator;

    var writer = TypedDocValuesWriter.init(alloc, .bytes_val, 1024);
    defer writer.deinit();

    try writer.add(0, .{ .bytes_val = "hello" });
    try writer.add(1, .{ .bytes_val = "world" });

    const data = try writer.build();
    defer alloc.free(data);

    // Just verify it builds without error — bytes getters would need
    // a separate API since they're variable length
    try std.testing.expect(data.len > 5);
    const reader = try TypedDocValuesReader.init(alloc, data);
    try std.testing.expectEqual(ValueType.bytes_val, reader.value_type);
    try std.testing.expectEqual(@as(u32, 1), reader.num_chunks);
}
