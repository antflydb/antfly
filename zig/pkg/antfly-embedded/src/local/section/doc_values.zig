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

//! Chunked columnar doc value storage.
//!
//! Doc values provide per-document field values for sorting, faceting, and
//! highlighting. Values are grouped into chunks (default 1024 docs) and
//! Snappy-compressed for efficient random access.
//!
//! Wire format:
//!   [numChunks: u32 LE]
//!   [chunkOffset_0: u64 LE]   — end offset of chunk 0
//!   [chunkOffset_1: u64 LE]
//!   ...
//!   [chunk_0_data: Snappy-compressed]
//!   [chunk_1_data: Snappy-compressed]
//!   ...
//!
//! Each uncompressed chunk contains:
//!   [numDocs: u32 LE]
//!   For each doc:
//!     [docID: u32 LE]
//!     [valueLen: u32 LE]
//!     [value: bytes]

const std = @import("std");
const Allocator = std.mem.Allocator;
const snappy = @import("../encoding/snappy.zig");

/// Default number of documents per chunk.
pub const default_chunk_size: u32 = 1024;

// ============================================================================
// Doc Values Writer
// ============================================================================

pub const DocValuesWriter = struct {
    alloc: Allocator,
    chunk_size: u32,
    entries: std.ArrayListUnmanaged(Entry),

    const Entry = struct {
        doc_id: u32,
        value: []u8,
    };

    pub fn init(alloc: Allocator, chunk_size: u32) DocValuesWriter {
        return .{
            .alloc = alloc,
            .chunk_size = chunk_size,
            .entries = .empty,
        };
    }

    pub fn deinit(self: *DocValuesWriter) void {
        for (self.entries.items) |*e| self.alloc.free(e.value);
        self.entries.deinit(self.alloc);
    }

    /// Add a doc value. Values must be added in docID order.
    pub fn add(self: *DocValuesWriter, doc_id: u32, value: []const u8) !void {
        try self.entries.append(self.alloc, .{
            .doc_id = doc_id,
            .value = try self.alloc.dupe(u8, value),
        });
    }

    /// Build the serialized doc values section. Caller owns returned bytes.
    pub fn build(self: *DocValuesWriter) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);

        const num_entries: u32 = @intCast(self.entries.items.len);
        const num_chunks: u32 = if (num_entries == 0) 0 else (num_entries - 1) / self.chunk_size + 1;

        // Write num_chunks
        try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, num_chunks))));

        // Reserve space for chunk offset table
        const offset_table_start = out.items.len;
        const offset_table_bytes = @as(usize, num_chunks) * 8;
        try out.appendNTimes(self.alloc, 0, offset_table_bytes);

        // Write each chunk
        const data_start = out.items.len;
        var entry_idx: usize = 0;

        for (0..num_chunks) |chunk_idx| {
            const chunk_start = entry_idx;
            const chunk_end = @min(entry_idx + self.chunk_size, num_entries);
            const chunk_entries = self.entries.items[chunk_start..chunk_end];

            // Build uncompressed chunk
            var chunk_buf = std.ArrayListUnmanaged(u8).empty;
            defer chunk_buf.deinit(self.alloc);

            // numDocs in chunk
            const chunk_docs: u32 = @intCast(chunk_entries.len);
            try chunk_buf.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, chunk_docs))));

            for (chunk_entries) |*e| {
                try chunk_buf.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, e.doc_id))));
                try chunk_buf.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(e.value.len))))));
                try chunk_buf.appendSlice(self.alloc, e.value);
            }

            // Snappy compress
            const compressed = try snappy.encode(self.alloc, chunk_buf.items);
            defer self.alloc.free(compressed);
            try out.appendSlice(self.alloc, compressed);

            // Record chunk end offset (relative to data_start)
            const chunk_end_offset: u64 = out.items.len - data_start;
            const off_pos = offset_table_start + chunk_idx * 8;
            out.items[off_pos..][0..8].* = @bitCast(std.mem.nativeToLittle(u64, chunk_end_offset));

            entry_idx = chunk_end;
        }

        return try self.alloc.dupe(u8, out.items);
    }
};

// ============================================================================
// Doc Values Reader
// ============================================================================

pub const DocValuesReader = struct {
    alloc: Allocator,
    data: []const u8,
    num_chunks: u32,
    chunk_offsets: []const u8, // raw offset table bytes
    data_start: usize,

    pub fn init(alloc: Allocator, data: []const u8) !DocValuesReader {
        if (data.len < 4) return error.InvalidData;
        const num_chunks = std.mem.readInt(u32, data[0..4], .little);
        const offset_table_end: usize = 4 + @as(usize, num_chunks) * 8;
        if (data.len < offset_table_end) return error.InvalidData;

        return .{
            .alloc = alloc,
            .data = data,
            .num_chunks = num_chunks,
            .chunk_offsets = data[4..offset_table_end],
            .data_start = offset_table_end,
        };
    }

    /// Read a specific chunk, decompress, and return all doc values in it.
    /// Caller owns returned entries.
    pub fn readChunk(self: *const DocValuesReader, chunk_idx: u32) ![]DocValue {
        if (chunk_idx >= self.num_chunks) return error.InvalidChunk;

        // Get chunk byte range
        const chunk_start: usize = if (chunk_idx > 0)
            @intCast(std.mem.readInt(u64, self.chunk_offsets[(@as(usize, chunk_idx) - 1) * 8 ..][0..8], .little))
        else
            0;
        const chunk_end: usize = @intCast(std.mem.readInt(u64, self.chunk_offsets[@as(usize, chunk_idx) * 8 ..][0..8], .little));

        const compressed = self.data[self.data_start + chunk_start .. self.data_start + chunk_end];
        const decompressed = try snappy.decode(self.alloc, compressed);
        defer self.alloc.free(decompressed);

        // Parse chunk
        var pos: usize = 0;
        const num_docs = std.mem.readInt(u32, decompressed[pos..][0..4], .little);
        pos += 4;

        var entries = try self.alloc.alloc(DocValue, num_docs);
        errdefer self.alloc.free(entries);

        for (0..num_docs) |i| {
            const doc_id = std.mem.readInt(u32, decompressed[pos..][0..4], .little);
            pos += 4;
            const value_len = std.mem.readInt(u32, decompressed[pos..][0..4], .little);
            pos += 4;
            const value = try self.alloc.dupe(u8, decompressed[pos..][0..value_len]);
            pos += value_len;
            entries[i] = .{ .doc_id = doc_id, .value = value };
        }

        return entries;
    }

    /// Look up a single doc's value by scanning chunks.
    /// Returns null if not found. Caller owns returned bytes.
    pub fn get(self: *const DocValuesReader, doc_id: u32, chunk_size: u32) !?[]u8 {
        const chunk_idx = doc_id / chunk_size;
        if (chunk_idx >= self.num_chunks) return null;

        const entries = try self.readChunk(chunk_idx);
        defer {
            for (entries) |*e| self.alloc.free(e.value);
            self.alloc.free(entries);
        }

        for (entries) |*e| {
            if (e.doc_id == doc_id) {
                // Transfer ownership of this value
                const result = try self.alloc.dupe(u8, e.value);
                return result;
            }
        }

        return null;
    }
};

pub const DocValue = struct {
    doc_id: u32,
    value: []u8,
};

// ============================================================================
// Tests
// ============================================================================

test "doc values round-trip" {
    const alloc = std.testing.allocator;
    var writer = DocValuesWriter.init(alloc, 2); // small chunk for testing
    defer writer.deinit();

    try writer.add(0, "hello");
    try writer.add(1, "world");
    try writer.add(2, "foo");

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try DocValuesReader.init(alloc, data);

    // Chunk 0: docs 0,1
    const chunk0 = try reader.readChunk(0);
    defer {
        for (chunk0) |*e| alloc.free(e.value);
        alloc.free(chunk0);
    }
    try std.testing.expectEqual(@as(usize, 2), chunk0.len);
    try std.testing.expectEqual(@as(u32, 0), chunk0[0].doc_id);
    try std.testing.expectEqualStrings("hello", chunk0[0].value);
    try std.testing.expectEqual(@as(u32, 1), chunk0[1].doc_id);
    try std.testing.expectEqualStrings("world", chunk0[1].value);

    // Chunk 1: doc 2
    const chunk1 = try reader.readChunk(1);
    defer {
        for (chunk1) |*e| alloc.free(e.value);
        alloc.free(chunk1);
    }
    try std.testing.expectEqual(@as(usize, 1), chunk1.len);
    try std.testing.expectEqualStrings("foo", chunk1[0].value);
}

test "doc values get by id" {
    const alloc = std.testing.allocator;
    var writer = DocValuesWriter.init(alloc, 1024);
    defer writer.deinit();

    try writer.add(0, "alpha");
    try writer.add(5, "beta");
    try writer.add(10, "gamma");

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try DocValuesReader.init(alloc, data);

    const v0 = (try reader.get(0, 1024)) orelse return error.TestExpectedEqual;
    defer alloc.free(v0);
    try std.testing.expectEqualStrings("alpha", v0);

    const v5 = (try reader.get(5, 1024)) orelse return error.TestExpectedEqual;
    defer alloc.free(v5);
    try std.testing.expectEqualStrings("beta", v5);

    // Non-existent doc
    try std.testing.expect(try reader.get(3, 1024) == null);
}

test "doc values empty" {
    const alloc = std.testing.allocator;
    var writer = DocValuesWriter.init(alloc, 1024);
    defer writer.deinit();

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try DocValuesReader.init(alloc, data);
    try std.testing.expectEqual(@as(u32, 0), reader.num_chunks);
}
