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

//! Columnar stored fields with projection pushdown.
//!
//! Stores each field separately so reading one field only decompresses
//! that field's data (not all fields like bleve's per-document compression).
//! Each field is chunked into groups of docs, with Snappy compression per chunk.
//!
//! Format:
//!   [magic: "COLS" 4 bytes]
//!   [version: u8 = 1]
//!   [num_docs: u32 LE]
//!   [num_fields: u16 LE]
//!   [field directory]
//!   [chunk data]
//!
//! Field directory (per field):
//!   [name_len: u16 LE][name]
//!   [num_chunks: u32 LE]
//!   [chunk_offsets: u64 LE × (num_chunks + 1)]  — start/end offsets
//!
//! Chunk data:
//!   Snappy([value_len: u32 LE][value_bytes] × docs_in_chunk)

const std = @import("std");
const Allocator = std.mem.Allocator;
const snappy = @import("encoding/snappy.zig");

const columnar_magic: [4]u8 = "COLS".*;
const columnar_version: u8 = 1;

pub const FieldValue = struct {
    field_name: []const u8,
    value: []const u8,
};

// ============================================================================
// Writer
// ============================================================================

pub const ColumnarWriter = struct {
    alloc: Allocator,
    chunk_size: u32,
    fields: std.StringArrayHashMapUnmanaged(FieldData),
    doc_count: u32,

    const FieldData = struct {
        /// Chunks of raw (uncompressed) doc values.
        chunks: std.ArrayListUnmanaged(std.ArrayListUnmanaged(u8)),
        docs_in_current: u32,

        fn init() FieldData {
            return .{ .chunks = .empty, .docs_in_current = 0 };
        }

        fn deinit(self: *FieldData, alloc: Allocator) void {
            for (self.chunks.items) |*c| c.deinit(alloc);
            self.chunks.deinit(alloc);
        }
    };

    pub fn init(alloc: Allocator, chunk_size: u32) ColumnarWriter {
        return .{
            .alloc = alloc,
            .chunk_size = chunk_size,
            .fields = .empty,
            .doc_count = 0,
        };
    }

    pub fn deinit(self: *ColumnarWriter) void {
        for (self.fields.values()) |*fd| fd.deinit(self.alloc);
        // Free owned key strings
        for (self.fields.keys()) |key| self.alloc.free(key);
        self.fields.deinit(self.alloc);
    }

    /// Add a document's field values. Must be called in doc_id order (0, 1, 2, ...).
    pub fn addDoc(self: *ColumnarWriter, field_values: []const FieldValue) !void {
        for (field_values) |fv| {
            const gop = try self.fields.getOrPut(self.alloc, fv.field_name);
            if (!gop.found_existing) {
                // Own the key
                gop.key_ptr.* = try self.alloc.dupe(u8, fv.field_name);
                gop.value_ptr.* = FieldData.init();
                // Backfill empty values for previous docs
                const full_chunks = self.doc_count / self.chunk_size;
                const remainder = self.doc_count % self.chunk_size;
                for (0..full_chunks) |_| {
                    var chunk = std.ArrayListUnmanaged(u8).empty;
                    for (0..self.chunk_size) |_| {
                        try appendU32LE(&chunk, self.alloc, 0);
                    }
                    try gop.value_ptr.chunks.append(self.alloc, chunk);
                }
                if (remainder > 0) {
                    var chunk = std.ArrayListUnmanaged(u8).empty;
                    for (0..remainder) |_| {
                        try appendU32LE(&chunk, self.alloc, 0);
                    }
                    try gop.value_ptr.chunks.append(self.alloc, chunk);
                    gop.value_ptr.docs_in_current = @intCast(remainder);
                }
            }

            const fd = gop.value_ptr;

            // Start new chunk if needed
            if (fd.chunks.items.len == 0 or fd.docs_in_current >= self.chunk_size) {
                try fd.chunks.append(self.alloc, .empty);
                fd.docs_in_current = 0;
            }

            // Append value to current chunk
            var current = &fd.chunks.items[fd.chunks.items.len - 1];
            try appendU32LE(current, self.alloc, @intCast(fv.value.len));
            try current.appendSlice(self.alloc, fv.value);
            fd.docs_in_current += 1;
        }

        // For fields NOT in this doc, append an empty value
        for (self.fields.values()) |*fd| {
            const expected_docs = self.doc_count + 1;
            const actual_docs = blk: {
                var total: u32 = 0;
                for (fd.chunks.items[0 .. fd.chunks.items.len -| 1]) |_| {
                    total += self.chunk_size;
                }
                if (fd.chunks.items.len > 0) total += fd.docs_in_current;
                break :blk total;
            };
            if (actual_docs < expected_docs) {
                if (fd.chunks.items.len == 0 or fd.docs_in_current >= self.chunk_size) {
                    try fd.chunks.append(self.alloc, .empty);
                    fd.docs_in_current = 0;
                }
                const current = &fd.chunks.items[fd.chunks.items.len - 1];
                try appendU32LE(current, self.alloc, 0); // empty value
                fd.docs_in_current += 1;
            }
        }

        self.doc_count += 1;
    }

    /// Build serialized columnar data. Caller owns result.
    pub fn build(self: *ColumnarWriter) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);

        // Header
        try out.appendSlice(self.alloc, &columnar_magic);
        try out.append(self.alloc, columnar_version);
        try appendU32LE(&out, self.alloc, self.doc_count);
        try appendU16LE(&out, self.alloc, @intCast(self.fields.count()));

        // Field directory: collect offsets as we write chunk data
        // First pass: write directory with placeholder offsets
        const dir_start = out.items.len;
        for (self.fields.keys()) |name| {
            try appendU16LE(&out, self.alloc, @intCast(name.len));
            try out.appendSlice(self.alloc, name);
            const fd = self.fields.getPtr(name).?;
            const num_chunks: u32 = @intCast(fd.chunks.items.len);
            try appendU32LE(&out, self.alloc, num_chunks);
            // Placeholder for chunk offsets (num_chunks + 1 entries)
            try out.appendNTimes(self.alloc, 0, (@as(usize, num_chunks) + 1) * 8);
        }
        _ = dir_start;

        // Second pass: write compressed chunks and fill in offsets
        const data_start = out.items.len;
        var dir_pos: usize = 4 + 1 + 4 + 2; // skip header
        for (self.fields.keys()) |name| {
            const name_overhead = 2 + name.len + 4; // name_len + name + num_chunks
            dir_pos += name_overhead;
            const offset_table_pos = dir_pos;

            const fd = self.fields.getPtr(name).?;
            const num_chunks = fd.chunks.items.len;

            for (fd.chunks.items, 0..) |*chunk, ci| {
                // Record chunk start offset (relative to data_start)
                const chunk_offset: u64 = @intCast(out.items.len - data_start);
                const off_pos = offset_table_pos + ci * 8;
                out.items[off_pos..][0..8].* = @bitCast(std.mem.nativeToLittle(u64, chunk_offset));

                // Compress and write chunk
                const compressed = try snappy.encode(self.alloc, chunk.items);
                defer self.alloc.free(compressed);
                try out.appendSlice(self.alloc, compressed);
            }

            // Record end offset
            const end_offset: u64 = @intCast(out.items.len - data_start);
            const end_off_pos = offset_table_pos + num_chunks * 8;
            out.items[end_off_pos..][0..8].* = @bitCast(std.mem.nativeToLittle(u64, end_offset));

            dir_pos += (num_chunks + 1) * 8;
        }

        return try self.alloc.dupe(u8, out.items);
    }
};

// ============================================================================
// Reader
// ============================================================================

pub const ColumnarReader = struct {
    alloc: Allocator,
    data: []const u8,
    num_docs: u32,
    chunk_size: u32,
    fields: []FieldMeta,
    data_start: usize,

    const FieldMeta = struct {
        name: []const u8,
        num_chunks: u32,
        /// Byte offsets relative to data_start for each chunk start + end.
        offsets: []const u8, // packed u64 LE × (num_chunks + 1)

        fn chunkOffset(self: *const FieldMeta, idx: usize) u64 {
            return std.mem.readInt(u64, self.offsets[idx * 8 ..][0..8], .little);
        }
    };

    pub fn init(alloc: Allocator, data: []const u8, chunk_size: u32) !ColumnarReader {
        if (data.len < 11) return error.InvalidData;
        if (!std.mem.eql(u8, data[0..4], &columnar_magic)) return error.InvalidMagic;
        if (data[4] != columnar_version) return error.UnsupportedVersion;

        const num_docs = std.mem.readInt(u32, data[5..9], .little);
        const num_fields = std.mem.readInt(u16, data[9..11], .little);

        var pos: usize = 11;
        var fields = try alloc.alloc(FieldMeta, num_fields);
        errdefer alloc.free(fields);

        for (0..num_fields) |fi| {
            const name_len = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
            const name = data[pos..][0..name_len];
            pos += name_len;
            const num_chunks = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            const offsets_len = (@as(usize, num_chunks) + 1) * 8;
            const offsets = data[pos..][0..offsets_len];
            pos += offsets_len;

            fields[fi] = .{
                .name = name,
                .num_chunks = num_chunks,
                .offsets = offsets,
            };
        }

        return .{
            .alloc = alloc,
            .data = data,
            .num_docs = num_docs,
            .chunk_size = chunk_size,
            .fields = fields,
            .data_start = pos,
        };
    }

    pub fn deinit(self: *ColumnarReader) void {
        self.alloc.free(self.fields);
    }

    /// Read a single field value for a document. Returns null if field or doc not found.
    /// Only decompresses the relevant chunk. Caller owns returned value.
    pub fn readField(self: *const ColumnarReader, doc_id: u32, field_name: []const u8) !?[]u8 {
        const meta = self.findField(field_name) orelse return null;
        if (doc_id >= self.num_docs) return null;

        const chunk_idx = doc_id / self.chunk_size;
        const doc_in_chunk = doc_id % self.chunk_size;

        if (chunk_idx >= meta.num_chunks) return null;

        const chunk_data = try self.decompressChunk(meta, chunk_idx);
        defer self.alloc.free(chunk_data);

        // Parse values in chunk to find the right doc
        var pos: usize = 0;
        for (0..doc_in_chunk + 1) |i| {
            if (pos + 4 > chunk_data.len) return null;
            const val_len = std.mem.readInt(u32, chunk_data[pos..][0..4], .little);
            pos += 4;
            if (i == doc_in_chunk) {
                if (val_len == 0) return null;
                return try self.alloc.dupe(u8, chunk_data[pos..][0..val_len]);
            }
            pos += val_len;
        }
        return null;
    }

    /// Read multiple fields for a document (projection pushdown).
    /// Only decompresses chunks for requested fields.
    pub fn readDoc(self: *const ColumnarReader, doc_id: u32, field_names: []const []const u8) ![]FieldValue {
        var results = std.ArrayListUnmanaged(FieldValue).empty;
        errdefer {
            for (results.items) |r| self.alloc.free(@constCast(r.value));
            results.deinit(self.alloc);
        }

        for (field_names) |name| {
            if (try self.readField(doc_id, name)) |value| {
                try results.append(self.alloc, .{
                    .field_name = name,
                    .value = value,
                });
            }
        }

        return try results.toOwnedSlice(self.alloc);
    }

    fn findField(self: *const ColumnarReader, name: []const u8) ?*const FieldMeta {
        for (self.fields) |*f| {
            if (std.mem.eql(u8, f.name, name)) return f;
        }
        return null;
    }

    fn decompressChunk(self: *const ColumnarReader, meta: *const FieldMeta, chunk_idx: u32) ![]u8 {
        const start = meta.chunkOffset(chunk_idx);
        const end = meta.chunkOffset(chunk_idx + 1);
        const compressed = self.data[self.data_start + @as(usize, @intCast(start)) ..][0..@intCast(end - start)];
        return snappy.decode(self.alloc, compressed);
    }
};

// ============================================================================
// Helpers
// ============================================================================

fn appendU32LE(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, val: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, val))));
}

fn appendU16LE(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, val: u16) !void {
    try out.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, val))));
}

// ============================================================================
// Tests
// ============================================================================

test "columnar round-trip basic" {
    const alloc = std.testing.allocator;

    var writer = ColumnarWriter.init(alloc, 2);
    defer writer.deinit();

    try writer.addDoc(&.{
        .{ .field_name = "title", .value = "hello" },
        .{ .field_name = "body", .value = "world" },
    });
    try writer.addDoc(&.{
        .{ .field_name = "title", .value = "foo" },
        .{ .field_name = "body", .value = "bar baz" },
    });
    try writer.addDoc(&.{
        .{ .field_name = "title", .value = "zig" },
    });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try ColumnarReader.init(alloc, data, 2);
    defer reader.deinit();

    // Read individual fields
    const v0 = (try reader.readField(0, "title")).?;
    defer alloc.free(v0);
    try std.testing.expectEqualStrings("hello", v0);

    const v1 = (try reader.readField(1, "body")).?;
    defer alloc.free(v1);
    try std.testing.expectEqualStrings("bar baz", v1);

    // Doc 2 has no "body" field → null
    const v2 = try reader.readField(2, "body");
    try std.testing.expect(v2 == null);

    // Doc 2 has "title"
    const v3 = (try reader.readField(2, "title")).?;
    defer alloc.free(v3);
    try std.testing.expectEqualStrings("zig", v3);
}

test "projection pushdown reads only requested fields" {
    const alloc = std.testing.allocator;

    var writer = ColumnarWriter.init(alloc, 1024);
    defer writer.deinit();

    try writer.addDoc(&.{
        .{ .field_name = "a", .value = "val_a" },
        .{ .field_name = "b", .value = "val_b" },
        .{ .field_name = "c", .value = "val_c" },
    });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try ColumnarReader.init(alloc, data, 1024);
    defer reader.deinit();

    // Read only field "b"
    const results = try reader.readDoc(0, &.{"b"});
    defer {
        for (results) |r| alloc.free(@constCast(r.value));
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("b", results[0].field_name);
    try std.testing.expectEqualStrings("val_b", results[0].value);
}

test "columnar cross-chunk boundary" {
    const alloc = std.testing.allocator;

    var writer = ColumnarWriter.init(alloc, 2); // chunk_size=2
    defer writer.deinit();

    // 5 docs → 3 chunks (2, 2, 1)
    for (0..5) |i| {
        var buf: [16]u8 = undefined;
        const val = std.fmt.bufPrint(&buf, "doc{d}", .{i}) catch unreachable;
        try writer.addDoc(&.{.{ .field_name = "id", .value = val }});
    }

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try ColumnarReader.init(alloc, data, 2);
    defer reader.deinit();

    // Check all 5 docs
    for (0..5) |i| {
        const val = (try reader.readField(@intCast(i), "id")).?;
        defer alloc.free(val);
        var expected: [16]u8 = undefined;
        const exp = std.fmt.bufPrint(&expected, "doc{d}", .{i}) catch unreachable;
        try std.testing.expectEqualStrings(exp, val);
    }

    // Out of range → null
    try std.testing.expect(try reader.readField(5, "id") == null);
}
