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

//! Segment file container for full-text index sections.
//!
//! A segment is a self-contained, immutable index file containing:
//!   - Stored fields (raw document data)
//!   - Inverted text index sections (per field)
//!   - Vector index sections (per field, optional)
//!
//! Designed for async I/O: sections are independent and can be
//! read/written/merged concurrently across segment files.
//!
//! File layout (footer at end, read backwards):
//!   [field sections...]
//!   [stored fields data]
//!   [section index]
//!   [footer]
//!
//! Compatible with zapx v16/v17 section-based architecture.

const std = @import("std");
const Allocator = std.mem.Allocator;
const byte_copy = @import("common/byte_copy.zig");
const platform_time = @import("platform/time.zig");
const inverted = @import("section/inverted.zig");
const typed_dv = @import("section/typed_doc_values.zig");
const snappy = @import("encoding/snappy.zig");
const roaring = @import("encoding/roaring.zig");

// ============================================================================
// Constants
// ============================================================================

const magic: [4]u8 = "AFSM".*; // AntFly SegMent
const segment_version: u32 = 3; // v3: footer checksum is optional to avoid eager full-file scans
const stored_fields_version_compressed_per_doc: u8 = 2;
const stored_fields_version_uncompressed_offsets: u8 = 3;
const stored_fields_version_block_compressed: u8 = 4;
/// Index-only segments carry document ordinals and search sections but no
/// primary-key or source record per document. The footer remains authoritative
/// for doc_count; the repeated u32 lets the stored section validate itself.
const stored_fields_version_omitted: u8 = 5;
const stored_fields_block_doc_target: usize = 128;
const stored_fields_block_raw_target: usize = 512 * 1024;
const stored_fields_v4_doc_entry_size: usize = 24;

/// Fixed footer size (big-endian, at end of segment):
///   [numDocs: u64 BE]           8
///   [storedIndexOffset: u64 BE] 8
///   [sectionsIndexOffset: u64 BE] 8
///   [chunkMode: u32 BE]         4
///   [version: u32 BE]           4
///   [CRC32: u32 BE]             4  (0 means not materialized)
///   [magic: 4 bytes]            4
const footer_size: usize = 8 + 8 + 8 + 4 + 4 + 4 + 4; // 40 bytes

pub const SectionType = enum(u16) {
    inverted_text = 0,
    vector = 1,
    synonym = 2,
    columnar_stored = 3,
    typed_doc_values = 4,
    doc_ordinals = 5,
    index_sort = 6,
    index_sort_bounds = 7,
    doc_key_range = 8,
};

pub const doc_ordinals_field = "\x00__antfly_doc_ordinals";
pub const index_sort_field = "\x00__antfly_index_sort";
pub const doc_key_range_field = "\x00__antfly_doc_key_range";

pub const SegmentIndexSortField = struct {
    field: []const u8,
    desc: bool = false,

    pub fn deinit(self: *SegmentIndexSortField, alloc: Allocator) void {
        alloc.free(@constCast(self.field));
        self.* = undefined;
    }
};

pub const SegmentIndexSortBoundValue = union(enum) {
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
    bool_val: bool,
    bytes_val: []const u8,
    id: []const u8,

    pub fn deinit(self: *SegmentIndexSortBoundValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes_val => |bytes| alloc.free(@constCast(bytes)),
            .id => |id| alloc.free(@constCast(id)),
            else => {},
        }
        self.* = undefined;
    }
};

pub const SegmentIndexSortBounds = struct {
    first: []SegmentIndexSortBoundValue,
    last: []SegmentIndexSortBoundValue,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.first) |*value| value.deinit(alloc);
        if (self.first.len > 0) alloc.free(self.first);
        for (self.last) |*value| value.deinit(alloc);
        if (self.last.len > 0) alloc.free(self.last);
        self.* = undefined;
    }
};

pub const SegmentLayoutStats = struct {
    stored_fields_bytes: u64 = 0,
    inverted_text_bytes: u64 = 0,
    inverted_header_bytes: u64 = 0,
    inverted_norm_bytes: u64 = 0,
    inverted_term_dict_bytes: u64 = 0,
    inverted_term_block_bytes: u64 = 0,
    inverted_term_index_bytes: u64 = 0,
    inverted_fst_bytes: u64 = 0,
    inverted_bloom_bytes: u64 = 0,
    inverted_postings_bytes: u64 = 0,
    inverted_postings_header_bytes: u64 = 0,
    inverted_block_max_bytes: u64 = 0,
    inverted_chunk_meta_bytes: u64 = 0,
    inverted_postings_payload_bytes: u64 = 0,
    inverted_positions_bytes: u64 = 0,
    inverted_skip_bytes: u64 = 0,
    inverted_one_hit_terms: u64 = 0,
    inverted_single_doc_postings_terms: u64 = 0,
    inverted_postings_terms: u64 = 0,
    inverted_postings_doc_frequency_total: u64 = 0,
    inverted_projected_posting_count_blocks_64: u64 = 0,
    inverted_projected_posting_count_blocks_128: u64 = 0,
    inverted_projected_posting_count_blocks_256: u64 = 0,
    typed_doc_values_bytes: u64 = 0,
    doc_ordinals_bytes: u64 = 0,
    index_sort_bytes: u64 = 0,
    index_sort_bounds_bytes: u64 = 0,
    other_section_bytes: u64 = 0,
    section_index_bytes: u64 = 0,
};

// ============================================================================
// Segment writer
// ============================================================================

/// Builds a segment file from fields and their sections.
pub const SegmentWriter = struct {
    alloc: Allocator,
    fields: std.ArrayListUnmanaged(FieldBuilder),
    stored_fields: std.ArrayListUnmanaged(StoredDoc),
    compression_bytes: std.ArrayListUnmanaged(u8),
    doc_count: u32 = 0,
    last_stored_compress_ns: u64 = 0,
    last_stored_raw_bytes: u64 = 0,
    last_stored_compressed_bytes: u64 = 0,

    pub fn init(alloc: Allocator) SegmentWriter {
        return .{
            .alloc = alloc,
            .fields = .empty,
            .stored_fields = .empty,
            .compression_bytes = .empty,
        };
    }

    pub fn deinit(self: *SegmentWriter) void {
        for (self.fields.items) |*f| f.deinit(self.alloc);
        self.fields.deinit(self.alloc);
        for (self.stored_fields.items) |*s| {
            self.alloc.free(s.id);
            if (s.owns_data) self.alloc.free(@constCast(s.data));
        }
        self.stored_fields.deinit(self.alloc);
        self.compression_bytes.deinit(self.alloc);
    }

    /// Add a field to the segment.
    pub fn addField(self: *SegmentWriter, name: []const u8) !u16 {
        const idx: u16 = @intCast(self.fields.items.len);
        try self.fields.append(self.alloc, .{
            .name = try self.alloc.dupe(u8, name),
            .sections = .empty,
        });
        return idx;
    }

    /// Attach a section to a field.
    pub fn addSection(self: *SegmentWriter, field_idx: u16, section_type: SectionType, data: []const u8) !void {
        const owned = try self.alloc.dupe(u8, data);
        errdefer self.alloc.free(owned);
        try self.addSectionOwned(field_idx, section_type, owned);
    }

    /// Attach an owned section buffer to a field.
    ///
    /// On success ownership of `data` transfers to the writer and it will be
    /// freed by `deinit`. On error, the caller still owns `data`.
    pub fn addSectionOwned(self: *SegmentWriter, field_idx: u16, section_type: SectionType, data: []u8) !void {
        try self.fields.items[field_idx].sections.append(self.alloc, .{
            .section_type = section_type,
            .data = data,
        });
    }

    /// Store a document's raw data.
    pub fn addStoredDoc(self: *SegmentWriter, doc_id: []const u8, data: []const u8) !void {
        if (self.doc_count != self.stored_fields.items.len) return error.InvalidSegment;
        const owned_id = try self.alloc.dupe(u8, doc_id);
        errdefer self.alloc.free(owned_id);
        const owned_data = try self.alloc.dupe(u8, data);
        errdefer self.alloc.free(owned_data);
        try self.stored_fields.append(self.alloc, .{
            .id = owned_id,
            .data = owned_data,
            .is_compressed = false,
            .owns_data = true,
        });
        self.doc_count += 1;
    }

    /// Store a document while borrowing raw data until `build` completes.
    pub fn addStoredDocBorrowed(self: *SegmentWriter, doc_id: []const u8, data: []const u8) !void {
        if (self.doc_count != self.stored_fields.items.len) return error.InvalidSegment;
        const owned_id = try self.alloc.dupe(u8, doc_id);
        errdefer self.alloc.free(owned_id);
        try self.stored_fields.append(self.alloc, .{
            .id = owned_id,
            .data = data,
            .is_compressed = false,
            .owns_data = false,
        });
        self.doc_count += 1;
    }

    /// Store a document with Snappy-compressed data already prepared.
    pub fn addStoredDocCompressed(self: *SegmentWriter, doc_id: []const u8, compressed_data: []const u8) !void {
        if (self.doc_count != self.stored_fields.items.len) return error.InvalidSegment;
        const owned_id = try self.alloc.dupe(u8, doc_id);
        errdefer self.alloc.free(owned_id);
        const owned_data = try self.alloc.dupe(u8, compressed_data);
        errdefer self.alloc.free(owned_data);
        try self.stored_fields.append(self.alloc, .{
            .id = owned_id,
            .data = owned_data,
            .is_compressed = true,
            .owns_data = true,
        });
        self.doc_count += 1;
    }

    /// Add one logical document without a stored primary key or source body.
    /// This is reserved for the embedded search-kernel boundary, where stable
    /// result IDs come from the ordinal sidecar. It must not be mixed with
    /// stored documents in the same segment.
    pub fn addUnstoredDoc(self: *SegmentWriter) !void {
        if (self.stored_fields.items.len != 0) return error.InvalidSegment;
        self.doc_count = std.math.add(u32, self.doc_count, 1) catch return error.InvalidSegment;
    }

    pub fn addDocOrdinals(self: *SegmentWriter, ordinals: []const u32) !void {
        if (ordinals.len != self.doc_count) return error.InvalidSegment;
        const data = try encodeDocOrdinalsAlloc(self.alloc, ordinals);
        errdefer self.alloc.free(data);
        if (data.len == 0) {
            self.alloc.free(data);
            return;
        }

        const field_idx = try self.addField(doc_ordinals_field);
        try self.addSectionOwned(field_idx, .doc_ordinals, data);
    }

    pub fn addIndexSortMetadata(self: *SegmentWriter, fields: []const SegmentIndexSortField) !void {
        if (fields.len == 0) return error.InvalidSegment;
        const data = try encodeIndexSortMetadataAlloc(self.alloc, fields);
        errdefer self.alloc.free(data);
        const field_idx = try self.addField(index_sort_field);
        try self.addSectionOwned(field_idx, .index_sort, data);
    }

    pub fn addIndexSortMetadataWithBounds(
        self: *SegmentWriter,
        fields: []const SegmentIndexSortField,
        bounds: SegmentIndexSortBounds,
    ) !void {
        if (fields.len == 0) return error.InvalidSegment;
        if (bounds.first.len != fields.len or bounds.last.len != fields.len) return error.InvalidSegment;

        const metadata = try encodeIndexSortMetadataAlloc(self.alloc, fields);
        errdefer self.alloc.free(metadata);
        const bounds_data = try encodeIndexSortBoundsMetadataAlloc(self.alloc, bounds);
        errdefer self.alloc.free(bounds_data);

        const field_idx = try self.addField(index_sort_field);
        try self.addSectionOwned(field_idx, .index_sort, metadata);
        try self.addSectionOwned(field_idx, .index_sort_bounds, bounds_data);
    }

    pub fn addDocKeyRange(self: *SegmentWriter, min_key: []const u8, max_key: []const u8) !void {
        if (std.mem.order(u8, min_key, max_key) == .gt) return error.InvalidSegment;
        const data = try encodeDocKeyRangeAlloc(self.alloc, min_key, max_key);
        errdefer self.alloc.free(data);
        const field_idx = try self.addField(doc_key_range_field);
        try self.addSectionOwned(field_idx, .doc_key_range, data);
    }

    /// Build the final segment file bytes. Caller owns result.
    ///
    /// Layout:
    ///   [stored fields data]
    ///   [field section data...]
    ///   [sections index (BE)]
    ///   [footer (40 bytes, BE)]
    pub fn build(self: *SegmentWriter) ![]u8 {
        var sink_impl = MemorySegmentSink.init(self.alloc);
        errdefer sink_impl.deinit();
        try sink_impl.out.ensureTotalCapacity(self.alloc, self.estimatedBuildSize());
        var sink = sink_impl.sink();
        try self.writeToSink(&sink);
        return try sink_impl.finishOwned();
    }

    /// Write the final segment file bytes into `sink`.
    ///
    /// This is the file-backed analogue of `build()`: it preserves the same
    /// on-disk layout while avoiding a heap allocation for the final segment
    /// buffer. As with `build()`, attached section buffers are consumed.
    pub fn writeToSink(self: *SegmentWriter, sink: *SegmentSink) !void {
        const stored_offset: u64 = @intCast(sink.len());
        try self.writeStoredFieldsToSink(sink);

        for (self.fields.items) |*field| {
            for (field.sections.items) |*section| {
                section.offset = sink.len();
                try sink.appendSlice(section.data);
                section.length = section.data.len;
                self.alloc.free(section.data);
                section.data = &.{};
            }
        }

        const sections_index_offset: u64 = @intCast(sink.len());
        try self.writeSectionIndexToSink(sink);

        try sinkAppendU64BE(sink, @intCast(self.doc_count));
        try sinkAppendU64BE(sink, stored_offset);
        try sinkAppendU64BE(sink, sections_index_offset);
        try sinkAppendU32BE(sink, 0);
        try sinkAppendU32BE(sink, segment_version);
        try sinkAppendU32BE(sink, 0);
        try sink.appendSlice(&magic);
    }

    fn estimatedBuildSize(self: *const SegmentWriter) usize {
        if (self.doc_count > 0 and self.stored_fields.items.len == 0) {
            var total: usize = 1 + 4;
            for (self.fields.items) |field| {
                total +|= 2 + field.name.len + 2;
                for (field.sections.items) |section| {
                    total +|= section.data.len;
                    total +|= 2 + 8 + 8;
                }
            }
            return total +| 40;
        }
        var total: usize = 1 + 4 + 4 + 4 + 8 + self.stored_fields.items.len * stored_fields_v4_doc_entry_size;
        total +|= @as(usize, if (self.stored_fields.items.len == 0) 0 else (self.stored_fields.items.len - 1) / stored_fields_block_doc_target + 1) * 8;
        for (self.stored_fields.items) |doc| {
            total +|= doc.id.len;
            total +|= 4 + doc.data.len;
        }

        for (self.fields.items) |field| {
            total +|= 2 + field.name.len + 2;
            for (field.sections.items) |section| {
                total +|= section.data.len;
                total +|= 2 + 8 + 8;
            }
        }
        total +|= 40;
        return total;
    }

    fn writeStoredFields(self: *SegmentWriter, out: *std.ArrayListUnmanaged(u8)) !void {
        if (self.doc_count > 0 and self.stored_fields.items.len == 0) {
            try out.append(self.alloc, stored_fields_version_omitted);
            try appendU32LE(self.alloc, out, self.doc_count);
            return;
        }
        // Format v3 (with uncompressed docs + offset table for random access):
        //   [version: u8 = 3]
        //   [num_docs: u32 LE]
        //   [offset_0: u64 LE]  — offset from start of stored section to doc 0
        //   [offset_1: u64 LE]
        //   ...
        //   [doc_0_data]  — per doc: [id_len: u16 LE][id][data_len: u32 LE][data]
        //   [doc_1_data]
        //   ...
        //
        // Small-document indexing is CPU-bound on per-document compression in
        // the write path. Lucene/Tantivy-style stored-field compression should
        // be block oriented; until the format grows block metadata, keep the
        // random-access offset table and write raw stored docs.
        const num_docs: u32 = @intCast(self.stored_fields.items.len);
        const section_start = out.items.len;

        // Write version
        try out.append(self.alloc, stored_fields_version_uncompressed_offsets);
        self.last_stored_compress_ns = 0;

        // Write num_docs
        try appendU32LE(self.alloc, out, num_docs);

        // Reserve space for offset table
        const offset_table_start = out.items.len;
        const offset_table_size = @as(usize, num_docs) * 8;
        try out.appendNTimes(self.alloc, 0, offset_table_size);

        // Write each document and record offsets
        for (self.stored_fields.items, 0..) |*doc, i| {
            const doc_offset: u64 = @intCast(out.items.len - section_start);

            // Write offset into table
            const off_pos = offset_table_start + i * 8;
            out.items[off_pos..][0..8].* = @bitCast(std.mem.nativeToLittle(u64, doc_offset));

            // Write doc ID (uncompressed for fast access)
            try appendU16LE(self.alloc, out, @intCast(doc.id.len));
            try out.appendSlice(self.alloc, doc.id);

            // Write raw stored data. `addStoredDocCompressed` is retained for
            // old callers; decode once here so v3 remains consistently raw.
            if (doc.is_compressed) {
                const compress_start = platform_time.monotonicNs();
                const decoded = try snappy.decode(self.alloc, doc.data);
                defer self.alloc.free(decoded);
                self.last_stored_compress_ns +|= platform_time.monotonicNs() - compress_start;
                try appendU32LE(self.alloc, out, @intCast(decoded.len));
                try out.appendSlice(self.alloc, decoded);
            } else {
                try appendU32LE(self.alloc, out, @intCast(doc.data.len));
                try out.appendSlice(self.alloc, doc.data);
            }
        }
    }

    fn writeStoredFieldsToSink(self: *SegmentWriter, sink: *SegmentSink) !void {
        if (self.doc_count > 0 and self.stored_fields.items.len == 0) {
            try sink.appendByte(stored_fields_version_omitted);
            try sinkAppendU32LE(sink, self.doc_count);
            self.last_stored_compress_ns = 0;
            self.last_stored_raw_bytes = 0;
            self.last_stored_compressed_bytes = 0;
            return;
        }
        const num_docs: u32 = @intCast(self.stored_fields.items.len);
        try sink.appendByte(stored_fields_version_block_compressed);
        self.last_stored_compress_ns = 0;
        self.last_stored_raw_bytes = 0;
        self.last_stored_compressed_bytes = 0;
        try sinkAppendU32LE(sink, num_docs);
        const blocks = try planStoredFieldBlocks(self.alloc, self.stored_fields.items);
        defer self.alloc.free(blocks);
        const num_blocks: u32 = @intCast(blocks.len);
        try sinkAppendU32LE(sink, num_blocks);
        try sinkAppendU32LE(sink, stored_fields_block_doc_target);
        const id_bytes_len_pos = sink.len();
        try sinkAppendU64LE(sink, 0);

        const doc_table_start = sink.len();
        try sink.appendNTimes(0, @as(usize, num_docs) * stored_fields_v4_doc_entry_size);
        const block_offsets_start = sink.len();
        try sink.appendNTimes(0, @as(usize, num_blocks) * 8);

        const id_bytes_start = sink.len();
        for (self.stored_fields.items, 0..) |*doc, i| {
            const id_offset: u64 = @intCast(sink.len() - id_bytes_start);
            try sink.appendSlice(doc.id);
            const entry_pos = doc_table_start + i * stored_fields_v4_doc_entry_size;
            try sink.writeAt(entry_pos, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, id_offset))));
            try sink.writeAt(entry_pos + 8, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(doc.id.len))))));
        }
        const id_bytes_len: u64 = @intCast(sink.len() - id_bytes_start);
        try sink.writeAt(id_bytes_len_pos, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, id_bytes_len))));

        const data_start = sink.len();
        var chunk = std.ArrayListUnmanaged(u8).empty;
        defer chunk.deinit(self.alloc);
        for (blocks, 0..) |block, block_idx| {
            chunk.clearRetainingCapacity();

            var doc_index = block.start;
            while (doc_index < block.end) : (doc_index += 1) {
                const doc = &self.stored_fields.items[doc_index];
                const doc_offset: u32 = @intCast(chunk.items.len);
                var decoded: ?[]u8 = null;
                defer if (decoded) |bytes| self.alloc.free(bytes);
                const raw_data = if (doc.is_compressed) blk: {
                    const decode_start = platform_time.monotonicNs();
                    decoded = try snappy.decode(self.alloc, doc.data);
                    self.last_stored_compress_ns +|= platform_time.monotonicNs() - decode_start;
                    break :blk decoded.?;
                } else doc.data;
                self.last_stored_raw_bytes +|= raw_data.len;
                try appendU32LE(self.alloc, &chunk, @intCast(raw_data.len));
                try chunk.appendSlice(self.alloc, raw_data);

                const entry_pos = doc_table_start + doc_index * stored_fields_v4_doc_entry_size;
                try sink.writeAt(entry_pos + 12, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(block_idx))))));
                try sink.writeAt(entry_pos + 16, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_offset))));
                try sink.writeAt(entry_pos + 20, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(raw_data.len))))));
            }

            const encode_start = platform_time.monotonicNs();
            const compressed = try snappy.encode(self.alloc, chunk.items);
            defer self.alloc.free(compressed);
            self.last_stored_compress_ns +|= platform_time.monotonicNs() - encode_start;
            self.last_stored_compressed_bytes +|= compressed.len;
            try sink.appendSlice(compressed);

            const block_end_offset: u64 = @intCast(sink.len() - data_start);
            try sink.writeAt(block_offsets_start + @as(usize, block_idx) * 8, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, block_end_offset))));
        }
    }

    fn writeSectionIndex(self: *SegmentWriter, out: *std.ArrayListUnmanaged(u8)) !void {
        // Sections index format (big-endian):
        //   [num_fields: u16 BE]
        //   For each field:
        //     [name_len: u16 BE] [name]
        //     [num_sections: u16 BE]
        //     For each section:
        //       [section_type: u16 BE]
        //       [offset: u64 BE]
        //       [length: u64 BE]
        try appendU16BE(self.alloc, out, @intCast(self.fields.items.len));
        for (self.fields.items) |*field| {
            try appendU16BE(self.alloc, out, @intCast(field.name.len));
            try out.appendSlice(self.alloc, field.name);
            try appendU16BE(self.alloc, out, @intCast(field.sections.items.len));

            for (field.sections.items) |*section| {
                try appendU16BE(self.alloc, out, @intFromEnum(section.section_type));
                try appendU64BE(self.alloc, out, @intCast(section.offset));
                try appendU64BE(self.alloc, out, @intCast(section.length));
            }
        }
    }

    fn writeSectionIndexToSink(self: *SegmentWriter, sink: *SegmentSink) !void {
        try sinkAppendU16BE(sink, @intCast(self.fields.items.len));
        for (self.fields.items) |*field| {
            try sinkAppendU16BE(sink, @intCast(field.name.len));
            try sink.appendSlice(field.name);
            try sinkAppendU16BE(sink, @intCast(field.sections.items.len));

            for (field.sections.items) |*section| {
                try sinkAppendU16BE(sink, @intFromEnum(section.section_type));
                try sinkAppendU64BE(sink, @intCast(section.offset));
                try sinkAppendU64BE(sink, @intCast(section.length));
            }
        }
    }

    const StoredDoc = struct {
        id: []u8,
        data: []const u8,
        is_compressed: bool,
        owns_data: bool,
    };

    const StoredFieldBlockPlan = struct {
        start: usize,
        end: usize,
    };

    fn storedDocRawLen(doc: *const StoredDoc) !usize {
        return if (doc.is_compressed) try snappy.decodedLen(doc.data) else doc.data.len;
    }

    fn planStoredFieldBlocks(alloc: Allocator, docs: []const StoredDoc) ![]StoredFieldBlockPlan {
        if (docs.len == 0) return try alloc.alloc(StoredFieldBlockPlan, 0);
        var blocks = std.ArrayListUnmanaged(StoredFieldBlockPlan).empty;
        errdefer blocks.deinit(alloc);

        var start: usize = 0;
        while (start < docs.len) {
            var end = start;
            var raw_bytes: usize = 0;
            while (end < docs.len) {
                const doc_raw_bytes = (try storedDocRawLen(&docs[end])) +| 4;
                if (end > start and (end - start >= stored_fields_block_doc_target or raw_bytes +| doc_raw_bytes > stored_fields_block_raw_target)) break;
                raw_bytes +|= doc_raw_bytes;
                end += 1;
            }
            try blocks.append(alloc, .{ .start = start, .end = end });
            start = end;
        }

        return try blocks.toOwnedSlice(alloc);
    }

    const SectionData = struct {
        section_type: SectionType,
        data: []u8,
        offset: usize = 0,
        length: usize = 0,

        fn deinit(self: *SectionData, alloc: Allocator) void {
            alloc.free(self.data);
        }
    };

    const FieldBuilder = struct {
        name: []u8,
        sections: std.ArrayListUnmanaged(SectionData),

        fn deinit(self: *FieldBuilder, alloc: Allocator) void {
            alloc.free(self.name);
            for (self.sections.items) |*s| s.deinit(alloc);
            self.sections.deinit(alloc);
        }
    };
};

// ============================================================================
// Segment reader
// ============================================================================

/// Reads a segment file.
pub const SegmentReader = struct {
    alloc: Allocator,
    data: []const u8,
    stored_offset: u64,
    index_offset: u64,
    doc_count: u32,
    num_fields: u16,
    fields: []FieldInfo,

    pub const FieldInfo = struct {
        name: []const u8,
        sections: []SectionInfo,
    };

    pub const SectionInfo = struct {
        section_type: SectionType,
        offset: u64,
        length: u64,
    };

    pub fn init(alloc: Allocator, data: []const u8) !SegmentReader {
        if (data.len < footer_size) return error.InvalidSegment;

        // Read footer (big-endian, 40 bytes at end)
        const end = data.len;
        if (!std.mem.eql(u8, data[end - 4 ..][0..4], &magic)) return error.InvalidMagic;

        const stored_crc = std.mem.readInt(u32, data[end - 8 ..][0..4], .big);
        const ver = std.mem.readInt(u32, data[end - 12 ..][0..4], .big);
        if (ver != segment_version) return error.UnsupportedVersion;
        // chunkMode at end-16 (ignored for now)
        const sections_index_offset = std.mem.readInt(u64, data[end - 24 ..][0..8], .big);
        const stored_offset = std.mem.readInt(u64, data[end - 32 ..][0..8], .big);
        const doc_count: u32 = @intCast(std.mem.readInt(u64, data[end - 40 ..][0..8], .big));

        // v3 writers use a zero checksum sentinel so normal mmap opens do not
        // force the whole segment resident. Non-zero legacy/diagnostic
        // checksums are still honored.
        if (stored_crc != 0) {
            const footer_data_end = end - 8; // exclude CRC + magic
            const expected_crc = std.hash.Crc32.hash(data[0..footer_data_end]);
            if (stored_crc != expected_crc) return error.CrcMismatch;
        }

        // Parse sections index (big-endian)
        var pos: usize = @intCast(sections_index_offset);
        const num_fields = std.mem.readInt(u16, data[pos..][0..2], .big);
        pos += 2;

        var fields = try alloc.alloc(FieldInfo, num_fields);
        errdefer alloc.free(fields);

        for (0..num_fields) |fi| {
            const name_len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            const name = data[pos..][0..name_len];
            pos += name_len;
            const num_sections = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;

            const sections = try alloc.alloc(SectionInfo, num_sections);
            for (0..num_sections) |si| {
                const st = std.mem.readInt(u16, data[pos..][0..2], .big);
                pos += 2;
                const offset = std.mem.readInt(u64, data[pos..][0..8], .big);
                pos += 8;
                const length = std.mem.readInt(u64, data[pos..][0..8], .big);
                pos += 8;
                sections[si] = .{
                    .section_type = @enumFromInt(st),
                    .offset = offset,
                    .length = length,
                };
            }
            fields[fi] = .{ .name = name, .sections = sections };
        }

        return .{
            .alloc = alloc,
            .data = data,
            .stored_offset = stored_offset,
            .index_offset = sections_index_offset,
            .doc_count = doc_count,
            .num_fields = num_fields,
            .fields = fields,
        };
    }

    pub fn deinit(self: *SegmentReader) void {
        for (self.fields) |*f| self.alloc.free(f.sections);
        self.alloc.free(self.fields);
    }

    /// Get section data for a field by name and type.
    pub fn getSection(self: *const SegmentReader, field_name: []const u8, section_type: SectionType) ?[]const u8 {
        for (self.fields) |*field| {
            if (std.mem.eql(u8, field.name, field_name)) {
                for (field.sections) |*section| {
                    if (section.section_type == section_type) {
                        if (section.length == 0) return null;
                        const offset: usize = @intCast(section.offset);
                        const length: usize = @intCast(section.length);
                        return self.data[offset..][0..length];
                    }
                }
            }
        }
        return null;
    }

    pub fn layoutStats(self: *const SegmentReader) SegmentLayoutStats {
        return self.layoutStatsWithInvertedDetails(false);
    }

    pub fn layoutStatsWithInvertedDetails(self: *const SegmentReader, detailed_inverted: bool) SegmentLayoutStats {
        var stats = SegmentLayoutStats{};
        var stored_end: usize = @intCast(self.index_offset);
        for (self.fields) |*field| {
            for (field.sections) |*section| {
                const offset: usize = @intCast(section.offset);
                const length: u64 = section.length;
                if (offset >= self.stored_offset and offset < stored_end) stored_end = offset;
                switch (section.section_type) {
                    .inverted_text => {
                        stats.inverted_text_bytes +|= length;
                        if (offset > self.data.len or length > self.data.len - offset) continue;
                        const section_data = self.data[offset..][0..@intCast(length)];
                        if (inverted.InvertedIndexReader.init(self.alloc, section_data)) |reader| {
                            const inverted_layout = if (detailed_inverted)
                                reader.detailedLayoutStats() catch reader.layoutStats()
                            else
                                reader.layoutStats();
                            {
                                stats.inverted_header_bytes +|= inverted_layout.header_bytes;
                                stats.inverted_norm_bytes +|= inverted_layout.norm_bytes;
                                stats.inverted_term_dict_bytes +|= inverted_layout.term_dict_bytes;
                                stats.inverted_term_block_bytes +|= inverted_layout.term_block_bytes;
                                stats.inverted_term_index_bytes +|= inverted_layout.term_index_bytes;
                                stats.inverted_fst_bytes +|= inverted_layout.fst_bytes;
                                stats.inverted_bloom_bytes +|= inverted_layout.bloom_bytes;
                                stats.inverted_postings_bytes +|= inverted_layout.postings_bytes;
                                stats.inverted_postings_header_bytes +|= inverted_layout.postings_header_bytes;
                                stats.inverted_block_max_bytes +|= inverted_layout.block_max_bytes;
                                stats.inverted_chunk_meta_bytes +|= inverted_layout.chunk_meta_bytes;
                                stats.inverted_postings_payload_bytes +|= inverted_layout.postings_payload_bytes;
                                stats.inverted_positions_bytes +|= inverted_layout.positions_bytes;
                                stats.inverted_skip_bytes +|= inverted_layout.skip_bytes;
                                stats.inverted_one_hit_terms +|= inverted_layout.one_hit_terms;
                                stats.inverted_single_doc_postings_terms +|= inverted_layout.single_doc_postings_terms;
                                stats.inverted_postings_terms +|= inverted_layout.postings_terms;
                                stats.inverted_postings_doc_frequency_total +|= inverted_layout.postings_doc_frequency_total;
                                stats.inverted_projected_posting_count_blocks_64 +|= inverted_layout.projected_posting_count_blocks_64;
                                stats.inverted_projected_posting_count_blocks_128 +|= inverted_layout.projected_posting_count_blocks_128;
                                stats.inverted_projected_posting_count_blocks_256 +|= inverted_layout.projected_posting_count_blocks_256;
                            }
                        } else |_| {}
                    },
                    .typed_doc_values => stats.typed_doc_values_bytes +|= length,
                    .doc_ordinals => stats.doc_ordinals_bytes +|= length,
                    .index_sort => stats.index_sort_bytes +|= length,
                    .index_sort_bounds => stats.index_sort_bounds_bytes +|= length,
                    else => stats.other_section_bytes +|= length,
                }
            }
        }
        const stored_start: usize = @intCast(self.stored_offset);
        if (stored_end >= stored_start) stats.stored_fields_bytes = @intCast(stored_end - stored_start);
        const footer_start = self.data.len - footer_size;
        if (footer_start >= self.index_offset) stats.section_index_bytes = @intCast(footer_start - @as(usize, @intCast(self.index_offset)));
        return stats;
    }

    /// Get an inverted index reader for a field.
    pub fn invertedIndex(self: *const SegmentReader, field_name: []const u8) !?inverted.InvertedIndexReader {
        const section_data = self.getSection(field_name, .inverted_text) orelse return null;
        return try inverted.InvertedIndexReader.init(self.alloc, section_data);
    }

    pub const StoredDocRef = struct { id: []const u8, data: []const u8 };

    /// Read stored document by index. v2 segments return Snappy-compressed
    /// data; v1/v3 segments return raw stored data. v4 block-compressed
    /// segments return the document id and an empty data slice; use
    /// `storedDocDecompressed` when stored data is required.
    pub fn storedDoc(self: *const SegmentReader, doc_idx: u32) ?StoredDocRef {
        var pos: usize = @intCast(self.stored_offset);
        const ver = self.data[pos];
        pos += 1;
        const num_docs = std.mem.readInt(u32, self.data[pos..][0..4], .little);
        pos += 4;
        if (doc_idx >= num_docs) return null;

        if (ver == stored_fields_version_omitted) return null;

        if (ver == stored_fields_version_block_compressed) {
            const loc = self.v4StoredDocLocation(pos, doc_idx) orelse return null;
            return .{ .id = loc.id, .data = &.{} };
        }

        if (ver >= stored_fields_version_compressed_per_doc) {
            // v2/v3: offset table for O(1) access
            const offset_table_start = pos;
            const doc_offset = std.mem.readInt(u64, self.data[offset_table_start + @as(usize, doc_idx) * 8 ..][0..8], .little);
            const abs_pos: usize = @intCast(self.stored_offset + doc_offset);
            return self.readStoredDocAt(abs_pos);
        } else {
            // v1: linear scan
            for (0..doc_idx + 1) |i| {
                const id_len = std.mem.readInt(u16, self.data[pos..][0..2], .little);
                pos += 2;
                const id = self.data[pos..][0..id_len];
                pos += id_len;
                const data_len = std.mem.readInt(u32, self.data[pos..][0..4], .little);
                pos += 4;
                const doc_data = self.data[pos..][0..data_len];
                pos += data_len;
                if (i == doc_idx) return .{ .id = id, .data = doc_data };
            }
            return null;
        }
    }

    fn readStoredDocAt(self: *const SegmentReader, pos: usize) ?StoredDocRef {
        var p = pos;
        const id_len = std.mem.readInt(u16, self.data[p..][0..2], .little);
        p += 2;
        const id = self.data[p..][0..id_len];
        p += id_len;
        const compressed_len = std.mem.readInt(u32, self.data[p..][0..4], .little);
        p += 4;
        const compressed_data = self.data[p..][0..compressed_len];
        return .{ .id = id, .data = compressed_data };
    }

    /// Read and decompress stored document data. Caller owns returned data.
    pub fn storedDocDecompressed(self: *const SegmentReader, doc_idx: u32) !?struct { id: []const u8, data: []u8 } {
        const raw = self.storedDoc(doc_idx) orelse return null;
        const ver = self.data[@intCast(self.stored_offset)];
        if (ver == stored_fields_version_block_compressed) {
            const loc = self.v4StoredDocLocation(@intCast(self.stored_offset + 1 + 4), doc_idx) orelse return null;
            const compressed = self.data[loc.block_start..loc.block_end];
            const block = try snappy.decode(self.alloc, compressed);
            defer self.alloc.free(block);
            if (loc.doc_offset > block.len or block.len - loc.doc_offset < 4) return error.InvalidSegment;
            const data_len = std.mem.readInt(u32, block[loc.doc_offset..][0..4], .little);
            const data_start = loc.doc_offset + 4;
            if (data_start > block.len or data_len > block.len - data_start) return error.InvalidSegment;
            if (data_len != loc.raw_len) return error.InvalidSegment;
            return .{ .id = loc.id, .data = try self.alloc.dupe(u8, block[data_start..][0..data_len]) };
        }
        if (ver == stored_fields_version_compressed_per_doc) {
            const decompressed = try snappy.decode(self.alloc, raw.data);
            return .{ .id = raw.id, .data = decompressed };
        } else {
            // v1/v3: data is not compressed, dupe for consistent ownership.
            return .{ .id = raw.id, .data = try self.alloc.dupe(u8, raw.data) };
        }
    }

    pub fn storedDocsAreCompressed(self: *const SegmentReader) bool {
        const ver = self.data[@intCast(self.stored_offset)];
        return ver == stored_fields_version_compressed_per_doc or ver == stored_fields_version_block_compressed;
    }

    pub fn storedFieldsOmitted(self: *const SegmentReader) bool {
        return self.data[@intCast(self.stored_offset)] == stored_fields_version_omitted;
    }

    pub const V4StoredDocLocation = struct {
        id: []const u8,
        block_idx: u32,
        block_start: usize,
        block_end: usize,
        doc_offset: usize,
        raw_len: u32,
    };

    fn v4StoredDocLocation(self: *const SegmentReader, header_pos_after_doc_count: usize, doc_idx: u32) ?V4StoredDocLocation {
        var pos = header_pos_after_doc_count;
        const num_blocks = std.mem.readInt(u32, self.data[pos..][0..4], .little);
        pos += 4;
        _ = std.mem.readInt(u32, self.data[pos..][0..4], .little);
        pos += 4;
        const id_bytes_len = std.mem.readInt(u64, self.data[pos..][0..8], .little);
        pos += 8;

        const doc_table_start = pos;
        const block_offsets_start = doc_table_start + @as(usize, self.doc_count) * stored_fields_v4_doc_entry_size;
        const id_bytes_start = block_offsets_start + @as(usize, num_blocks) * 8;
        const data_start = id_bytes_start + @as(usize, @intCast(id_bytes_len));
        const entry_pos = doc_table_start + @as(usize, doc_idx) * stored_fields_v4_doc_entry_size;
        if (entry_pos + stored_fields_v4_doc_entry_size > self.data.len) return null;
        const id_offset = std.mem.readInt(u64, self.data[entry_pos..][0..8], .little);
        const id_len = std.mem.readInt(u32, self.data[entry_pos + 8 ..][0..4], .little);
        const block_idx = std.mem.readInt(u32, self.data[entry_pos + 12 ..][0..4], .little);
        const doc_offset = std.mem.readInt(u32, self.data[entry_pos + 16 ..][0..4], .little);
        const raw_len = std.mem.readInt(u32, self.data[entry_pos + 20 ..][0..4], .little);
        if (block_idx >= num_blocks) return null;
        const id_start = id_bytes_start + @as(usize, @intCast(id_offset));
        if (id_start > self.data.len or id_len > self.data.len - id_start) return null;
        const block_end_offset = std.mem.readInt(u64, self.data[block_offsets_start + @as(usize, block_idx) * 8 ..][0..8], .little);
        const block_start_offset: u64 = if (block_idx == 0) 0 else std.mem.readInt(u64, self.data[block_offsets_start + (@as(usize, block_idx) - 1) * 8 ..][0..8], .little);
        const block_start = data_start + @as(usize, @intCast(block_start_offset));
        const block_end = data_start + @as(usize, @intCast(block_end_offset));
        if (block_start > self.data.len or block_end > self.data.len or block_start > block_end) return null;
        return .{
            .id = self.data[id_start..][0..id_len],
            .block_idx = block_idx,
            .block_start = block_start,
            .block_end = block_end,
            .doc_offset = @intCast(doc_offset),
            .raw_len = raw_len,
        };
    }

    pub fn docOrdinal(self: *const SegmentReader, doc_idx: u32) !?u32 {
        const section = self.getSection(doc_ordinals_field, .doc_ordinals) orelse return null;
        return try decodeDocOrdinal(section, doc_idx);
    }

    pub const DocKeyRange = struct {
        min_key: []const u8,
        max_key: []const u8,
    };

    pub fn docKeyRange(self: *const SegmentReader) !?DocKeyRange {
        const section = self.getSection(doc_key_range_field, .doc_key_range) orelse return null;
        return try decodeDocKeyRange(section);
    }

    pub fn indexSortFieldsAlloc(self: *const SegmentReader, alloc: Allocator) !?[]SegmentIndexSortField {
        const section = self.getSection(index_sort_field, .index_sort) orelse return null;
        return try decodeIndexSortMetadataAlloc(alloc, section);
    }

    pub fn indexSortBoundsAlloc(self: *const SegmentReader, alloc: Allocator) !?SegmentIndexSortBounds {
        const section = self.getSection(index_sort_field, .index_sort_bounds) orelse return null;
        return try decodeIndexSortBoundsMetadataAlloc(alloc, section);
    }
};

// ============================================================================
// Segment merger
// ============================================================================

/// Streaming destination for segment bytes.
///
/// The current segment format still requires random writes for the stored-doc
/// offset table and a final CRC pass, so sinks must support `writeAt` and
/// `crc32Prefix`. A file-backed sink can implement the same contract without
/// requiring the merge code to materialize intermediate SegmentWriter state.
pub const SegmentSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        len: *const fn (*anyopaque) usize,
        append_slice: *const fn (*anyopaque, []const u8) anyerror!void,
        append_byte: *const fn (*anyopaque, u8) anyerror!void,
        append_ntimes: *const fn (*anyopaque, u8, usize) anyerror!void,
        write_at: *const fn (*anyopaque, usize, []const u8) anyerror!void,
        crc32_prefix: *const fn (*anyopaque, usize) anyerror!u32,
    };

    pub fn len(self: *SegmentSink) usize {
        return self.vtable.len(self.ptr);
    }

    pub fn appendSlice(self: *SegmentSink, bytes: []const u8) !void {
        try self.vtable.append_slice(self.ptr, bytes);
    }

    pub fn appendByte(self: *SegmentSink, byte: u8) !void {
        try self.vtable.append_byte(self.ptr, byte);
    }

    pub fn appendNTimes(self: *SegmentSink, byte: u8, count: usize) !void {
        try self.vtable.append_ntimes(self.ptr, byte, count);
    }

    pub fn writeAt(self: *SegmentSink, offset: usize, bytes: []const u8) !void {
        try self.vtable.write_at(self.ptr, offset, bytes);
    }

    pub fn crc32Prefix(self: *SegmentSink, len_prefix: usize) !u32 {
        return try self.vtable.crc32_prefix(self.ptr, len_prefix);
    }
};

/// In-memory SegmentSink used by the current KV-backed segment representation.
/// It avoids merge-time SegmentWriter staging and transfers the final buffer to
/// the caller with `toOwnedSlice`.
pub const MemorySegmentSink = struct {
    alloc: Allocator,
    out: std.ArrayListUnmanaged(u8) = .empty,

    pub fn init(alloc: Allocator) MemorySegmentSink {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemorySegmentSink) void {
        self.out.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn sink(self: *MemorySegmentSink) SegmentSink {
        return .{
            .ptr = self,
            .vtable = &memory_segment_sink_vtable,
        };
    }

    pub fn finishOwned(self: *MemorySegmentSink) ![]u8 {
        return try self.out.toOwnedSlice(self.alloc);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        try byte_copy.appendSlicePossiblyAliased(&self.out, self.alloc, bytes);
    }

    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        try self.out.append(self.alloc, byte);
    }

    fn appendNTimes(ptr: *anyopaque, byte: u8, count: usize) !void {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        try self.out.appendNTimes(self.alloc, byte, count);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidSegment;
        byte_copy.copyPossiblyAliased(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidSegment;
        return std.hash.Crc32.hash(self.out.items[0..len_prefix]);
    }
};

const memory_segment_sink_vtable = SegmentSink.VTable{
    .len = MemorySegmentSink.len,
    .append_slice = MemorySegmentSink.appendSlice,
    .append_byte = MemorySegmentSink.appendByte,
    .append_ntimes = MemorySegmentSink.appendNTimes,
    .write_at = MemorySegmentSink.writeAt,
    .crc32_prefix = MemorySegmentSink.crc32Prefix,
};

pub const MergeInput = struct {
    reader: *const SegmentReader,
    deleted: ?roaring.RoaringBitmap = null,

    fn isDeleted(self: MergeInput, doc_id: u32) bool {
        return if (self.deleted) |deleted| deleted.contains(doc_id) else false;
    }
};

pub const MergeOptions = struct {
    index_sort: []const SegmentIndexSortField = &.{},
};

const MergeDocRef = struct {
    input_idx: usize,
    doc_id: u32,
};

const SegmentSortValue = union(enum) {
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
    bool_val: bool,
    bytes_val: []u8,
    id: []const u8,

    fn deinit(self: *SegmentSortValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes_val => |bytes| alloc.free(bytes),
            else => {},
        }
        self.* = undefined;
    }
};

const SegmentSortValueTag = enum {
    u64_val,
    i64_val,
    f64_val,
    bool_val,
    bytes_val,
    id,
};

fn segmentSortValueTag(value: SegmentSortValue) SegmentSortValueTag {
    return switch (value) {
        .u64_val => .u64_val,
        .i64_val => .i64_val,
        .f64_val => .f64_val,
        .bool_val => .bool_val,
        .bytes_val => .bytes_val,
        .id => .id,
    };
}

const SortedMergeRecord = struct {
    ref: MergeDocRef,
    keys: []SegmentSortValue,

    fn deinit(self: *SortedMergeRecord, alloc: Allocator) void {
        for (self.keys) |*key| key.deinit(alloc);
        alloc.free(self.keys);
        self.* = undefined;
    }
};

const SortedMergePlan = struct {
    records: []SortedMergeRecord,
    doc_maps: [][]u32,

    fn deinit(self: *SortedMergePlan, alloc: Allocator) void {
        for (self.records) |*record| record.deinit(alloc);
        alloc.free(self.records);
        for (self.doc_maps) |map| alloc.free(map);
        alloc.free(self.doc_maps);
        self.* = undefined;
    }
};

const BuiltSection = struct {
    section_type: SectionType,
    offset: u64,
    length: u64,
};

const BuiltField = struct {
    name: []const u8,
    sections: std.ArrayListUnmanaged(BuiltSection) = .empty,

    fn deinit(self: *BuiltField, alloc: Allocator) void {
        self.sections.deinit(alloc);
    }
};

/// Merge multiple segments into one. Merges per-field inverted indexes
/// and concatenates stored documents.
pub fn mergeSegments(alloc: Allocator, segments: []const []const u8) ![]u8 {
    // Open readers
    var readers = try alloc.alloc(SegmentReader, segments.len);
    defer {
        for (readers) |*r| r.deinit();
        alloc.free(readers);
    }
    for (segments, 0..) |seg, i| {
        readers[i] = try SegmentReader.init(alloc, seg);
    }

    var inputs = try alloc.alloc(MergeInput, readers.len);
    defer alloc.free(inputs);
    for (readers, 0..) |*reader, i| {
        inputs[i] = .{ .reader = reader };
    }
    return try mergeSegmentInputs(alloc, inputs);
}

/// Merge already-open segment readers into one output segment.
/// Deleted documents are omitted and doc IDs are compacted.
pub fn mergeSegmentInputs(alloc: Allocator, inputs: []const MergeInput) ![]u8 {
    return try mergeSegmentInputsWithOptions(alloc, inputs, .{});
}

pub fn mergeSegmentInputsWithOptions(alloc: Allocator, inputs: []const MergeInput, options: MergeOptions) ![]u8 {
    if (inputs.len == 0) return error.NoSegments;

    var sink_impl = MemorySegmentSink.init(alloc);
    errdefer sink_impl.deinit();
    var sink = sink_impl.sink();
    try writeMergedSegmentToSinkWithOptions(alloc, &sink, inputs, options);
    return try sink_impl.finishOwned();
}

pub fn writeMergedSegmentToSink(alloc: Allocator, sink: *SegmentSink, inputs: []const MergeInput) !void {
    try writeMergedSegmentToSinkWithOptions(alloc, sink, inputs, .{});
}

pub fn writeMergedSegmentToSinkWithOptions(alloc: Allocator, sink: *SegmentSink, inputs: []const MergeInput, options: MergeOptions) !void {
    if (options.index_sort.len > 0) {
        var plan = try buildSortedMergePlanAlloc(alloc, inputs, options.index_sort);
        defer plan.deinit(alloc);
        return try writeSortedMergedSegmentToSink(alloc, sink, inputs, options.index_sort, &plan);
    }
    try writeAppendMergedSegmentToSink(alloc, sink, inputs);
}

pub fn commonIndexSortForMergeInputsAlloc(alloc: Allocator, inputs: []const MergeInput) ![]SegmentIndexSortField {
    var common: []SegmentIndexSortField = &.{};
    errdefer freeIndexSortFields(alloc, common);
    var found_live_segment = false;

    for (inputs) |input| {
        if (!inputHasLiveDocs(input)) continue;
        found_live_segment = true;
        const fields = (try input.reader.indexSortFieldsAlloc(alloc)) orelse {
            freeIndexSortFields(alloc, common);
            return &.{};
        };
        defer freeIndexSortFields(alloc, fields);

        if (common.len == 0) {
            common = try cloneIndexSortFieldsAlloc(alloc, fields);
            continue;
        }
        if (!indexSortFieldsEqual(common, fields)) {
            freeIndexSortFields(alloc, common);
            return &.{};
        }
    }
    if (!found_live_segment) {
        freeIndexSortFields(alloc, common);
        return &.{};
    }
    return common;
}

pub fn freeIndexSortFields(alloc: Allocator, fields: []const SegmentIndexSortField) void {
    for (fields) |*field| {
        alloc.free(@constCast(field.field));
    }
    if (fields.len > 0) alloc.free(fields);
}

fn cloneIndexSortFieldsAlloc(alloc: Allocator, fields: []const SegmentIndexSortField) ![]SegmentIndexSortField {
    const cloned = try alloc.alloc(SegmentIndexSortField, fields.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*field| field.deinit(alloc);
        alloc.free(cloned);
    }
    for (fields, 0..) |field, i| {
        cloned[i] = .{
            .field = try alloc.dupe(u8, field.field),
            .desc = field.desc,
        };
        initialized += 1;
    }
    return cloned;
}

fn indexSortFieldsEqual(a: []const SegmentIndexSortField, b: []const SegmentIndexSortField) bool {
    if (a.len != b.len) return false;
    for (a, 0..) |field, i| {
        if (field.desc != b[i].desc) return false;
        if (!std.mem.eql(u8, field.field, b[i].field)) return false;
    }
    return true;
}

fn writeAppendMergedSegmentToSink(alloc: Allocator, sink: *SegmentSink, inputs: []const MergeInput) !void {
    if (inputs.len == 0) return error.NoSegments;

    const stored_offset: u64 = @intCast(sink.len());
    const doc_count = countLiveDocs(inputs);
    try writeMergedStoredFields(alloc, sink, inputs, doc_count);

    // Collect all unique field names
    var field_set = std.StringHashMapUnmanaged(void).empty;
    defer field_set.deinit(alloc);

    for (inputs) |input| {
        for (input.reader.fields) |*f| {
            if (std.mem.eql(u8, f.name, doc_ordinals_field)) continue;
            if (std.mem.eql(u8, f.name, index_sort_field)) continue;
            if (std.mem.eql(u8, f.name, doc_key_range_field)) continue;
            try field_set.put(alloc, f.name, {});
        }
    }

    var built_fields = std.ArrayListUnmanaged(BuiltField).empty;
    defer {
        for (built_fields.items) |*field| field.deinit(alloc);
        built_fields.deinit(alloc);
    }

    // For each field, append merged sections directly into the sink and retain
    // only compact section-index metadata.
    var field_iter = field_set.keyIterator();
    while (field_iter.next()) |field_name_ptr| {
        const field_name = field_name_ptr.*;
        var built_field = BuiltField{ .name = field_name };
        errdefer built_field.deinit(alloc);

        var has_inverted = false;
        var present_count: usize = 0;
        var first_present_index: ?usize = null;
        var only_present_deleted = false;
        const inv_sections = try alloc.alloc(?[]const u8, inputs.len);
        defer alloc.free(inv_sections);
        const doc_counts = try alloc.alloc(u32, inputs.len);
        defer alloc.free(doc_counts);
        const deleted_docs = try alloc.alloc(?roaring.RoaringBitmap, inputs.len);
        defer alloc.free(deleted_docs);

        for (inputs, 0..) |input, i| {
            const reader = input.reader;
            doc_counts[i] = reader.doc_count;
            deleted_docs[i] = input.deleted;
            inv_sections[i] = null;
            if (reader.getSection(field_name, .inverted_text)) |section_data| {
                if (section_data.len == 0) continue;
                inv_sections[i] = section_data;
                has_inverted = true;
                present_count += 1;
                if (first_present_index == null) first_present_index = i;
                if (input.deleted != null) only_present_deleted = true;
            }
        }

        if (has_inverted) {
            if (present_count == 1 and first_present_index.? == 0 and !only_present_deleted) {
                try appendBuiltSection(alloc, sink, &built_field, .inverted_text, inv_sections[0].?);
            } else {
                const offset = sink.len();
                try inverted.writeMergedInvertedSectionSlotsWithDeletes(alloc, sink, inv_sections, doc_counts, deleted_docs, inverted.productionIndexConfig());
                try built_field.sections.append(alloc, .{
                    .section_type = .inverted_text,
                    .offset = @intCast(offset),
                    .length = @intCast(sink.len() - offset),
                });
            }
        }

        if (try mergeTypedDocValuesSections(alloc, inputs, field_name)) |merged| {
            defer alloc.free(merged);
            try appendBuiltSection(alloc, sink, &built_field, .typed_doc_values, merged);
        }

        try built_fields.append(alloc, built_field);
    }

    if (try mergeDocOrdinalSectionsAlloc(alloc, inputs, doc_count)) |merged_doc_ordinals| {
        defer alloc.free(merged_doc_ordinals);
        var built_field = BuiltField{ .name = doc_ordinals_field };
        errdefer built_field.deinit(alloc);
        try appendBuiltSection(alloc, sink, &built_field, .doc_ordinals, merged_doc_ordinals);
        try built_fields.append(alloc, built_field);
    }
    if (try mergeDocKeyRangeSectionsAlloc(alloc, inputs)) |merged_key_range| {
        defer alloc.free(merged_key_range);
        var built_field = BuiltField{ .name = doc_key_range_field };
        errdefer built_field.deinit(alloc);
        try appendBuiltSection(alloc, sink, &built_field, .doc_key_range, merged_key_range);
        try built_fields.append(alloc, built_field);
    }

    const sections_index_offset: u64 = @intCast(sink.len());
    try writeMergedSectionIndex(alloc, sink, built_fields.items);

    try sinkAppendU64BE(sink, doc_count);
    try sinkAppendU64BE(sink, stored_offset);
    try sinkAppendU64BE(sink, sections_index_offset);
    try sinkAppendU32BE(sink, 0);
    try sinkAppendU32BE(sink, segment_version);
    try sinkAppendU32BE(sink, 0);
    try sink.appendSlice(&magic);
}

fn writeSortedMergedSegmentToSink(
    alloc: Allocator,
    sink: *SegmentSink,
    inputs: []const MergeInput,
    index_sort: []const SegmentIndexSortField,
    plan: *const SortedMergePlan,
) !void {
    if (inputs.len == 0) return error.NoSegments;

    const stored_offset: u64 = @intCast(sink.len());
    const doc_count: u32 = @intCast(plan.records.len);
    try writeMergedStoredFieldsInOrder(alloc, sink, inputs, plan.records, doc_count);

    var field_set = std.StringHashMapUnmanaged(void).empty;
    defer field_set.deinit(alloc);

    for (inputs) |input| {
        for (input.reader.fields) |*f| {
            if (std.mem.eql(u8, f.name, doc_ordinals_field)) continue;
            if (std.mem.eql(u8, f.name, index_sort_field)) continue;
            if (std.mem.eql(u8, f.name, doc_key_range_field)) continue;
            try field_set.put(alloc, f.name, {});
        }
    }

    var built_fields = std.ArrayListUnmanaged(BuiltField).empty;
    defer {
        for (built_fields.items) |*field| field.deinit(alloc);
        built_fields.deinit(alloc);
    }

    var field_iter = field_set.keyIterator();
    while (field_iter.next()) |field_name_ptr| {
        const field_name = field_name_ptr.*;
        var built_field = BuiltField{ .name = field_name };
        errdefer built_field.deinit(alloc);

        var has_inverted = false;
        const inv_sections = try alloc.alloc(?[]const u8, inputs.len);
        defer alloc.free(inv_sections);
        const doc_counts = try alloc.alloc(u32, inputs.len);
        defer alloc.free(doc_counts);

        for (inputs, 0..) |input, i| {
            const reader = input.reader;
            doc_counts[i] = reader.doc_count;
            inv_sections[i] = null;
            if (reader.getSection(field_name, .inverted_text)) |section_data| {
                if (section_data.len == 0) continue;
                inv_sections[i] = section_data;
                has_inverted = true;
            }
        }

        if (has_inverted) {
            const offset = sink.len();
            try inverted.writeMergedInvertedSectionSlotsWithDocMaps(alloc, sink, inv_sections, doc_counts, plan.doc_maps, doc_count, inverted.productionIndexConfig());
            try built_field.sections.append(alloc, .{
                .section_type = .inverted_text,
                .offset = @intCast(offset),
                .length = @intCast(sink.len() - offset),
            });
        }

        if (try mergeTypedDocValuesSectionsInOrder(alloc, inputs, field_name, plan.records)) |merged| {
            defer alloc.free(merged);
            try appendBuiltSection(alloc, sink, &built_field, .typed_doc_values, merged);
        }

        try built_fields.append(alloc, built_field);
    }

    if (try mergeDocOrdinalSectionsInOrderAlloc(alloc, inputs, plan.records)) |merged_doc_ordinals| {
        defer alloc.free(merged_doc_ordinals);
        var built_field = BuiltField{ .name = doc_ordinals_field };
        errdefer built_field.deinit(alloc);
        try appendBuiltSection(alloc, sink, &built_field, .doc_ordinals, merged_doc_ordinals);
        try built_fields.append(alloc, built_field);
    }
    if (try mergeDocKeyRangeSectionsAlloc(alloc, inputs)) |merged_key_range| {
        defer alloc.free(merged_key_range);
        var built_field = BuiltField{ .name = doc_key_range_field };
        errdefer built_field.deinit(alloc);
        try appendBuiltSection(alloc, sink, &built_field, .doc_key_range, merged_key_range);
        try built_fields.append(alloc, built_field);
    }

    const index_sort_data = try encodeIndexSortMetadataAlloc(alloc, index_sort);
    defer alloc.free(index_sort_data);
    {
        var sort_field = BuiltField{ .name = index_sort_field };
        errdefer sort_field.deinit(alloc);
        try appendBuiltSection(alloc, sink, &sort_field, .index_sort, index_sort_data);
        if (try sortedMergePlanIndexSortBoundsAlloc(alloc, plan)) |bounds| {
            var owned_bounds = bounds;
            defer owned_bounds.deinit(alloc);
            const bounds_data = try encodeIndexSortBoundsMetadataAlloc(alloc, owned_bounds);
            defer alloc.free(bounds_data);
            try appendBuiltSection(alloc, sink, &sort_field, .index_sort_bounds, bounds_data);
        }
        try built_fields.append(alloc, sort_field);
    }

    const sections_index_offset: u64 = @intCast(sink.len());
    try writeMergedSectionIndex(alloc, sink, built_fields.items);

    try sinkAppendU64BE(sink, doc_count);
    try sinkAppendU64BE(sink, stored_offset);
    try sinkAppendU64BE(sink, sections_index_offset);
    try sinkAppendU32BE(sink, 0);
    try sinkAppendU32BE(sink, segment_version);
    try sinkAppendU32BE(sink, 0);
    try sink.appendSlice(&magic);
}

fn countLiveDocs(inputs: []const MergeInput) u32 {
    var total: u32 = 0;
    for (inputs) |input| {
        for (0..input.reader.doc_count) |doc_id_usize| {
            if (!input.isDeleted(@intCast(doc_id_usize))) total += 1;
        }
    }
    return total;
}

fn allStoredFieldsOmitted(inputs: []const MergeInput) bool {
    var saw_live_docs = false;
    for (inputs) |input| {
        if (!inputHasLiveDocs(input)) continue;
        saw_live_docs = true;
        if (!input.reader.storedFieldsOmitted()) return false;
    }
    return saw_live_docs;
}

fn writeOmittedStoredFields(sink: *SegmentSink, doc_count: u32) !void {
    try sink.appendByte(stored_fields_version_omitted);
    try sinkAppendU32LE(sink, doc_count);
}

fn rejectMixedStoredFieldModes(inputs: []const MergeInput) !void {
    for (inputs) |input| {
        if (inputHasLiveDocs(input) and input.reader.storedFieldsOmitted()) return error.InvalidSegment;
    }
}

fn writeMergedStoredFields(alloc: Allocator, sink: *SegmentSink, inputs: []const MergeInput, doc_count: u32) !void {
    if (allStoredFieldsOmitted(inputs)) return writeOmittedStoredFields(sink, doc_count);
    try rejectMixedStoredFieldModes(inputs);
    try sink.appendByte(stored_fields_version_block_compressed);
    try sinkAppendU32LE(sink, doc_count);
    const num_blocks = try countMergedStoredBlocks(alloc, inputs);
    try sinkAppendU32LE(sink, num_blocks);
    try sinkAppendU32LE(sink, stored_fields_block_doc_target);
    const id_bytes_len_pos = sink.len();
    try sinkAppendU64LE(sink, 0);

    const doc_table_start = sink.len();
    try sink.appendNTimes(0, @as(usize, doc_count) * stored_fields_v4_doc_entry_size);
    const block_offsets_start = sink.len();
    try sink.appendNTimes(0, @as(usize, num_blocks) * 8);

    const id_bytes_start = sink.len();
    var out_doc_id: u32 = 0;
    for (inputs) |input| {
        for (0..input.reader.doc_count) |doc_id_usize| {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) continue;
            const doc = input.reader.storedDoc(doc_id) orelse continue;
            try sink.appendSlice(doc.id);
            const entry_pos = doc_table_start + @as(usize, out_doc_id) * stored_fields_v4_doc_entry_size;
            const id_offset: u64 = @intCast(sink.len() - id_bytes_start - doc.id.len);
            try sink.writeAt(entry_pos, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, id_offset))));
            try sink.writeAt(entry_pos + 8, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(doc.id.len))))));
            out_doc_id += 1;
        }
    }
    const id_bytes_len: u64 = @intCast(sink.len() - id_bytes_start);
    try sink.writeAt(id_bytes_len_pos, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, id_bytes_len))));

    const data_start = sink.len();
    out_doc_id = 0;
    var block_idx: u32 = 0;
    var docs_in_block: u32 = 0;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    for (inputs) |input| {
        var doc_id_usize: usize = 0;
        while (doc_id_usize < input.reader.doc_count) {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) {
                doc_id_usize += 1;
                continue;
            }

            if (chunk.items.len == 0) {
                if (try copyMergedStoredBlockIfPossible(sink, input, doc_id, out_doc_id, block_idx, doc_table_start, block_offsets_start, data_start)) |copied_docs| {
                    out_doc_id += copied_docs;
                    doc_id_usize += copied_docs;
                    block_idx += 1;
                    docs_in_block = 0;
                    continue;
                }
            }

            const stored = (try input.reader.storedDocDecompressed(doc_id)) orelse {
                doc_id_usize += 1;
                continue;
            };
            defer alloc.free(stored.data);
            if (chunk.items.len > 0 and (docs_in_block >= stored_fields_block_doc_target or chunk.items.len +| 4 +| stored.data.len > stored_fields_block_raw_target)) {
                try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, data_start, block_idx);
                block_idx += 1;
                docs_in_block = 0;
            }

            const doc_offset: u32 = @intCast(chunk.items.len);
            try appendU32LE(alloc, &chunk, @intCast(stored.data.len));
            try chunk.appendSlice(alloc, stored.data);
            const entry_pos = doc_table_start + @as(usize, out_doc_id) * stored_fields_v4_doc_entry_size;
            try sink.writeAt(entry_pos + 12, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, block_idx))));
            try sink.writeAt(entry_pos + 16, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_offset))));
            try sink.writeAt(entry_pos + 20, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(stored.data.len))))));
            out_doc_id += 1;
            docs_in_block += 1;
            doc_id_usize += 1;
        }
    }
    if (chunk.items.len > 0) {
        try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, data_start, block_idx);
    }
}

fn writeMergedStoredFieldsInOrder(
    alloc: Allocator,
    sink: *SegmentSink,
    inputs: []const MergeInput,
    records: []const SortedMergeRecord,
    doc_count: u32,
) !void {
    if (allStoredFieldsOmitted(inputs)) return writeOmittedStoredFields(sink, doc_count);
    try rejectMixedStoredFieldModes(inputs);
    try sink.appendByte(stored_fields_version_block_compressed);
    try sinkAppendU32LE(sink, doc_count);
    const num_blocks = try countStoredBlocksInOrder(alloc, inputs, records);
    try sinkAppendU32LE(sink, num_blocks);
    try sinkAppendU32LE(sink, stored_fields_block_doc_target);
    const id_bytes_len_pos = sink.len();
    try sinkAppendU64LE(sink, 0);

    const doc_table_start = sink.len();
    try sink.appendNTimes(0, @as(usize, doc_count) * stored_fields_v4_doc_entry_size);
    const block_offsets_start = sink.len();
    try sink.appendNTimes(0, @as(usize, num_blocks) * 8);

    const id_bytes_start = sink.len();
    for (records, 0..) |record, out_doc_id| {
        const doc = inputs[record.ref.input_idx].reader.storedDoc(record.ref.doc_id) orelse return error.InvalidSegment;
        try sink.appendSlice(doc.id);
        const entry_pos = doc_table_start + out_doc_id * stored_fields_v4_doc_entry_size;
        const id_offset: u64 = @intCast(sink.len() - id_bytes_start - doc.id.len);
        try sink.writeAt(entry_pos, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, id_offset))));
        try sink.writeAt(entry_pos + 8, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(doc.id.len))))));
    }
    const id_bytes_len: u64 = @intCast(sink.len() - id_bytes_start);
    try sink.writeAt(id_bytes_len_pos, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, id_bytes_len))));

    const data_start = sink.len();
    var block_idx: u32 = 0;
    var docs_in_block: u32 = 0;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);

    for (records, 0..) |record, out_doc_id| {
        const stored = (try inputs[record.ref.input_idx].reader.storedDocDecompressed(record.ref.doc_id)) orelse return error.InvalidSegment;
        defer alloc.free(stored.data);
        if (chunk.items.len > 0 and (docs_in_block >= stored_fields_block_doc_target or chunk.items.len +| 4 +| stored.data.len > stored_fields_block_raw_target)) {
            try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, data_start, block_idx);
            block_idx += 1;
            docs_in_block = 0;
        }

        const doc_offset: u32 = @intCast(chunk.items.len);
        try appendU32LE(alloc, &chunk, @intCast(stored.data.len));
        try chunk.appendSlice(alloc, stored.data);
        const entry_pos = doc_table_start + out_doc_id * stored_fields_v4_doc_entry_size;
        try sink.writeAt(entry_pos + 12, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, block_idx))));
        try sink.writeAt(entry_pos + 16, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_offset))));
        try sink.writeAt(entry_pos + 20, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(stored.data.len))))));
        docs_in_block += 1;
    }
    if (chunk.items.len > 0) {
        try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, data_start, block_idx);
    }
}

fn countStoredBlocksInOrder(alloc: Allocator, inputs: []const MergeInput, records: []const SortedMergeRecord) !u32 {
    var blocks: u32 = 0;
    var docs_in_block: u32 = 0;
    var raw_bytes: usize = 0;
    for (records) |record| {
        const stored = (try inputs[record.ref.input_idx].reader.storedDocDecompressed(record.ref.doc_id)) orelse return error.InvalidSegment;
        const doc_raw_bytes = 4 +| stored.data.len;
        alloc.free(stored.data);
        if (raw_bytes > 0 and (docs_in_block >= stored_fields_block_doc_target or raw_bytes +| doc_raw_bytes > stored_fields_block_raw_target)) {
            blocks += 1;
            docs_in_block = 0;
            raw_bytes = 0;
        }
        docs_in_block += 1;
        raw_bytes +|= doc_raw_bytes;
    }
    if (raw_bytes > 0) blocks += 1;
    return blocks;
}

fn countMergedStoredBlocks(alloc: Allocator, inputs: []const MergeInput) !u32 {
    var blocks: u32 = 0;
    var docs_in_block: u32 = 0;
    var raw_bytes: usize = 0;
    for (inputs) |input| {
        var doc_id_usize: usize = 0;
        while (doc_id_usize < input.reader.doc_count) {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) {
                doc_id_usize += 1;
                continue;
            }

            if (raw_bytes == 0) {
                if (copyableStoredBlockDocs(input, doc_id)) |copied_docs| {
                    blocks += 1;
                    doc_id_usize += copied_docs;
                    docs_in_block = 0;
                    raw_bytes = 0;
                    continue;
                }
            }

            const stored = (try input.reader.storedDocDecompressed(doc_id)) orelse {
                doc_id_usize += 1;
                continue;
            };
            const doc_raw_bytes = 4 +| stored.data.len;
            alloc.free(stored.data);
            if (raw_bytes > 0 and (docs_in_block >= stored_fields_block_doc_target or raw_bytes +| doc_raw_bytes > stored_fields_block_raw_target)) {
                blocks += 1;
                docs_in_block = 0;
                raw_bytes = 0;
            }
            docs_in_block += 1;
            raw_bytes +|= doc_raw_bytes;
            doc_id_usize += 1;
        }
    }
    if (raw_bytes > 0) blocks += 1;
    return blocks;
}

fn flushMergedStoredBlock(
    alloc: Allocator,
    sink: *SegmentSink,
    chunk: *std.ArrayListUnmanaged(u8),
    block_offsets_start: usize,
    data_start: usize,
    block_idx: u32,
) !void {
    const compressed = try snappy.encode(alloc, chunk.items);
    defer alloc.free(compressed);
    try sink.appendSlice(compressed);
    const block_end_offset: u64 = @intCast(sink.len() - data_start);
    try sink.writeAt(block_offsets_start + @as(usize, block_idx) * 8, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, block_end_offset))));
    chunk.clearRetainingCapacity();
}

fn copyMergedStoredBlockIfPossible(
    sink: *SegmentSink,
    input: MergeInput,
    start_doc_id: u32,
    out_doc_id: u32,
    out_block_idx: u32,
    doc_table_start: usize,
    block_offsets_start: usize,
    data_start: usize,
) !?u32 {
    const copied_docs = copyableStoredBlockDocs(input, start_doc_id) orelse return null;
    const reader = input.reader;
    const first = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), start_doc_id) orelse return null;

    if (first.block_start > first.block_end or first.block_end > reader.data.len) return null;

    try sink.appendSlice(reader.data[first.block_start..first.block_end]);
    const block_end_offset: u64 = @intCast(sink.len() - data_start);
    try sink.writeAt(block_offsets_start + @as(usize, out_block_idx) * 8, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, block_end_offset))));

    var i: u32 = 0;
    while (i < copied_docs) : (i += 1) {
        const loc = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), start_doc_id + i) orelse return null;
        const entry_pos = doc_table_start + @as(usize, out_doc_id + i) * stored_fields_v4_doc_entry_size;
        try sink.writeAt(entry_pos + 12, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, out_block_idx))));
        try sink.writeAt(entry_pos + 16, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(loc.doc_offset))))));
        try sink.writeAt(entry_pos + 20, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, loc.raw_len))));
    }

    return copied_docs;
}

fn copyableStoredBlockDocs(input: MergeInput, start_doc_id: u32) ?u32 {
    if (input.deleted != null) return null;
    const reader = input.reader;
    if (reader.data[@intCast(reader.stored_offset)] != stored_fields_version_block_compressed) return null;
    const first = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), start_doc_id) orelse return null;
    if (first.doc_offset != 0) return null;

    var count: u32 = 0;
    var raw_bytes: usize = 0;
    var doc_id = start_doc_id;
    while (doc_id < reader.doc_count) : (doc_id += 1) {
        const loc = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), doc_id) orelse return null;
        if (loc.block_idx != first.block_idx) break;
        if (loc.block_start != first.block_start or loc.block_end != first.block_end) return null;
        count += 1;
        raw_bytes +|= 4 +| @as(usize, loc.raw_len);
    }
    if (count == 0) return null;
    if (count > stored_fields_block_doc_target) return null;
    if (count > 1 and raw_bytes > stored_fields_block_raw_target) return null;
    return count;
}

fn appendBuiltSection(
    alloc: Allocator,
    sink: *SegmentSink,
    field: *BuiltField,
    section_type: SectionType,
    data: []const u8,
) !void {
    if (data.len == 0) return;
    const offset: u64 = @intCast(sink.len());
    try sink.appendSlice(data);
    try field.sections.append(alloc, .{
        .section_type = section_type,
        .offset = offset,
        .length = data.len,
    });
}

fn writeMergedSectionIndex(alloc: Allocator, sink: *SegmentSink, fields: []const BuiltField) !void {
    _ = alloc;
    try sinkAppendU16BE(sink, @intCast(fields.len));
    for (fields) |*field| {
        try sinkAppendU16BE(sink, @intCast(field.name.len));
        try sink.appendSlice(field.name);
        try sinkAppendU16BE(sink, @intCast(field.sections.items.len));
        for (field.sections.items) |section| {
            try sinkAppendU16BE(sink, @intFromEnum(section.section_type));
            try sinkAppendU64BE(sink, section.offset);
            try sinkAppendU64BE(sink, section.length);
        }
    }
}

fn buildSortedMergePlanAlloc(
    alloc: Allocator,
    inputs: []const MergeInput,
    index_sort: []const SegmentIndexSortField,
) !SortedMergePlan {
    if (index_sort.len == 0) return error.InvalidSegment;
    var records = std.ArrayListUnmanaged(SortedMergeRecord).empty;
    errdefer {
        for (records.items) |*record| record.deinit(alloc);
        records.deinit(alloc);
    }

    var doc_maps = try alloc.alloc([]u32, inputs.len);
    var doc_maps_initialized: usize = 0;
    errdefer {
        for (doc_maps[0..doc_maps_initialized]) |map| alloc.free(map);
        alloc.free(doc_maps);
    }

    for (inputs, 0..) |input, input_idx| {
        try validateInputIndexSortMetadata(alloc, input, index_sort);
        const map = try alloc.alloc(u32, input.reader.doc_count);
        @memset(map, std.math.maxInt(u32));
        doc_maps[input_idx] = map;
        doc_maps_initialized += 1;

        for (0..input.reader.doc_count) |doc_id_usize| {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) continue;
            const keys = try loadSegmentSortKeysAlloc(alloc, inputs, .{ .input_idx = input_idx, .doc_id = doc_id }, index_sort);
            errdefer {
                for (keys) |*key| key.deinit(alloc);
                alloc.free(keys);
            }
            try records.append(alloc, .{
                .ref = .{ .input_idx = input_idx, .doc_id = doc_id },
                .keys = keys,
            });
        }
    }

    try validateSortedMergeKeyDomainsAlloc(alloc, records.items, index_sort.len);
    std.sort.pdq(SortedMergeRecord, records.items, index_sort, sortedMergeRecordLessThan);
    for (records.items, 0..) |record, out_doc_id| {
        doc_maps[record.ref.input_idx][record.ref.doc_id] = @intCast(out_doc_id);
    }

    return .{
        .records = try records.toOwnedSlice(alloc),
        .doc_maps = doc_maps,
    };
}

fn validateSortedMergeKeyDomainsAlloc(
    alloc: Allocator,
    records: []const SortedMergeRecord,
    key_count: usize,
) !void {
    const expected = try alloc.alloc(?SegmentSortValueTag, key_count);
    defer alloc.free(expected);
    @memset(expected, null);

    for (records) |record| {
        if (record.keys.len != key_count) return error.InvalidSegment;
        for (record.keys, 0..) |key, i| {
            const tag = segmentSortValueTag(key);
            if (expected[i]) |existing| {
                if (existing != tag) return error.InvalidSegment;
            } else {
                expected[i] = tag;
            }
        }
    }
}

fn validateInputIndexSortMetadata(
    alloc: Allocator,
    input: MergeInput,
    index_sort: []const SegmentIndexSortField,
) !void {
    if (!inputHasLiveDocs(input)) return;
    const fields = (try input.reader.indexSortFieldsAlloc(alloc)) orelse return error.InvalidSegment;
    defer {
        for (fields) |*field| field.deinit(alloc);
        alloc.free(fields);
    }
    if (fields.len != index_sort.len) return error.InvalidSegment;
    for (fields, 0..) |field, i| {
        if (field.desc != index_sort[i].desc) return error.InvalidSegment;
        if (!std.mem.eql(u8, field.field, index_sort[i].field)) return error.InvalidSegment;
    }
}

fn inputHasLiveDocs(input: MergeInput) bool {
    for (0..input.reader.doc_count) |doc_id_usize| {
        if (!input.isDeleted(@intCast(doc_id_usize))) return true;
    }
    return false;
}

fn loadSegmentSortKeysAlloc(
    alloc: Allocator,
    inputs: []const MergeInput,
    ref: MergeDocRef,
    index_sort: []const SegmentIndexSortField,
) ![]SegmentSortValue {
    const keys = try alloc.alloc(SegmentSortValue, index_sort.len);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |*key| key.deinit(alloc);
        alloc.free(keys);
    }
    for (index_sort, 0..) |field, i| {
        keys[i] = try loadSegmentSortKeyAlloc(alloc, inputs[ref.input_idx].reader, ref.doc_id, field.field);
        initialized += 1;
    }
    return keys;
}

fn loadSegmentSortKeyAlloc(
    alloc: Allocator,
    reader: *const SegmentReader,
    doc_id: u32,
    field: []const u8,
) !SegmentSortValue {
    if (std.mem.eql(u8, field, "_id")) {
        const doc = reader.storedDoc(doc_id) orelse return error.InvalidSegment;
        return .{ .id = doc.id };
    }
    const section = reader.getSection(field, .typed_doc_values) orelse return error.UnsupportedTypedDocValues;
    var dv_reader = try typed_dv.TypedDocValuesReader.init(alloc, section);
    return switch (dv_reader.value_type) {
        .u64_val => .{ .u64_val = (try segmentSortDocValue(dv_reader.getU64(doc_id))) orelse return error.InvalidSegment },
        .i64_val => .{ .i64_val = (try segmentSortDocValue(dv_reader.getI64(doc_id))) orelse return error.InvalidSegment },
        .f64_val => blk: {
            const value = (try segmentSortDocValue(dv_reader.getF64(doc_id))) orelse return error.InvalidSegment;
            if (!std.math.isFinite(value)) return error.InvalidSegment;
            break :blk .{ .f64_val = value };
        },
        .bool_val => .{ .bool_val = (try segmentSortDocValue(dv_reader.getBool(doc_id))) orelse return error.InvalidSegment },
        .bytes_val => .{ .bytes_val = (try segmentSortDocValue(dv_reader.getBytesAlloc(doc_id))) orelse return error.InvalidSegment },
        .geo_point => return error.UnsupportedTypedDocValues,
    };
}

fn segmentSortDocValue(value: anytype) !@typeInfo(@TypeOf(value)).error_union.payload {
    return value catch |err| switch (err) {
        error.InvalidData => error.InvalidSegment,
        else => err,
    };
}

fn sortedMergeRecordLessThan(index_sort: []const SegmentIndexSortField, a: SortedMergeRecord, b: SortedMergeRecord) bool {
    for (index_sort, 0..) |field, i| {
        const order = compareSegmentSortValues(a.keys[i], b.keys[i]);
        if (order == .eq) continue;
        return if (field.desc) order == .gt else order == .lt;
    }
    if (a.ref.input_idx != b.ref.input_idx) return a.ref.input_idx < b.ref.input_idx;
    return a.ref.doc_id < b.ref.doc_id;
}

fn compareSegmentSortValues(a: SegmentSortValue, b: SegmentSortValue) std.math.Order {
    return switch (a) {
        .u64_val => |av| switch (b) {
            .u64_val => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .i64_val => |av| switch (b) {
            .i64_val => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .f64_val => |av| switch (b) {
            .f64_val => |bv| compareSegmentSortF64(av, bv),
            else => .lt,
        },
        .bool_val => |av| switch (b) {
            .bool_val => |bv| std.math.order(@intFromBool(av), @intFromBool(bv)),
            else => .lt,
        },
        .bytes_val => |av| switch (b) {
            .bytes_val => |bv| std.mem.order(u8, av, bv),
            else => .lt,
        },
        .id => |av| switch (b) {
            .id => |bv| std.mem.order(u8, av, bv),
            else => .lt,
        },
    };
}

fn compareSegmentSortF64(a: f64, b: f64) std.math.Order {
    const a_nan = std.math.isNan(a);
    const b_nan = std.math.isNan(b);
    if (a_nan and b_nan) return .eq;
    if (a_nan) return .gt;
    if (b_nan) return .lt;
    return std.math.order(a, b);
}

fn sortedMergePlanIndexSortBoundsAlloc(alloc: Allocator, plan: *const SortedMergePlan) !?SegmentIndexSortBounds {
    if (plan.records.len == 0) return null;
    const first = try segmentBoundValuesFromSortValuesAlloc(alloc, plan.records[0].keys);
    errdefer {
        for (first) |*value| value.deinit(alloc);
        alloc.free(first);
    }
    return .{
        .first = first,
        .last = try segmentBoundValuesFromSortValuesAlloc(alloc, plan.records[plan.records.len - 1].keys),
    };
}

fn segmentBoundValuesFromSortValuesAlloc(
    alloc: Allocator,
    values: []const SegmentSortValue,
) ![]SegmentIndexSortBoundValue {
    const out = try alloc.alloc(SegmentIndexSortBoundValue, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*value| value.deinit(alloc);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try segmentBoundValueFromSortValueAlloc(alloc, value);
        initialized += 1;
    }
    return out;
}

fn segmentBoundValueFromSortValueAlloc(alloc: Allocator, value: SegmentSortValue) !SegmentIndexSortBoundValue {
    return switch (value) {
        .u64_val => |v| .{ .u64_val = v },
        .i64_val => |v| .{ .i64_val = v },
        .f64_val => |v| if (std.math.isFinite(v)) .{ .f64_val = v } else error.InvalidSegment,
        .bool_val => |v| .{ .bool_val = v },
        .bytes_val => |v| .{ .bytes_val = try alloc.dupe(u8, v) },
        .id => |v| .{ .id = try alloc.dupe(u8, v) },
    };
}

fn mergeTypedDocValuesSections(
    alloc: Allocator,
    inputs: []const MergeInput,
    field_name: []const u8,
) !?[]u8 {
    var value_type: ?typed_dv.ValueType = null;
    var writer: ?typed_dv.TypedDocValuesWriter = null;
    defer if (writer) |*w| w.deinit();

    var merged_doc_id: u32 = 0;
    for (inputs) |input| {
        const reader = input.reader;
        var dv_reader: ?typed_dv.TypedDocValuesReader = null;
        if (reader.getSection(field_name, .typed_doc_values)) |section_data| {
            dv_reader = try typed_dv.TypedDocValuesReader.init(alloc, section_data);
            if (value_type == null) {
                value_type = dv_reader.?.value_type;
                writer = typed_dv.TypedDocValuesWriter.init(alloc, dv_reader.?.value_type, typed_dv.default_chunk_size);
            } else if (value_type.? != dv_reader.?.value_type) {
                if (writer) |*w| {
                    w.deinit();
                    writer = null;
                }
                return null;
            }
        }

        for (0..reader.doc_count) |doc_id_usize| {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) continue;
            if (dv_reader) |dv| {
                switch (dv.value_type) {
                    .u64_val => if (try dv.getU64(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .u64_val = value });
                    },
                    .i64_val => if (try dv.getI64(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .i64_val = value });
                    },
                    .f64_val => if (try dv.getF64(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .f64_val = value });
                    },
                    .geo_point => if (try dv.getGeoPoint(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .geo_point = value });
                    },
                    .bool_val => if (try dv.getBool(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .bool_val = value });
                    },
                    .bytes_val => if (try dv.getBytesAlloc(doc_id)) |value| {
                        defer alloc.free(value);
                        try writer.?.add(merged_doc_id, .{ .bytes_val = value });
                    },
                }
            }
            merged_doc_id += 1;
        }
    }

    if (writer) |*w| {
        if (w.entries.items.len == 0) return null;
        return try w.build();
    }
    return null;
}

fn mergeTypedDocValuesSectionsInOrder(
    alloc: Allocator,
    inputs: []const MergeInput,
    field_name: []const u8,
    records: []const SortedMergeRecord,
) !?[]u8 {
    var readers = try alloc.alloc(?typed_dv.TypedDocValuesReader, inputs.len);
    defer alloc.free(readers);
    @memset(readers, null);
    var value_type: ?typed_dv.ValueType = null;

    for (inputs, 0..) |input, i| {
        const section = input.reader.getSection(field_name, .typed_doc_values) orelse continue;
        readers[i] = try typed_dv.TypedDocValuesReader.init(alloc, section);
        if (value_type == null) {
            value_type = readers[i].?.value_type;
        } else if (value_type.? != readers[i].?.value_type) {
            return null;
        }
    }
    const vt = value_type orelse return null;

    var writer = typed_dv.TypedDocValuesWriter.init(alloc, vt, typed_dv.default_chunk_size);
    defer writer.deinit();
    for (records, 0..) |record, out_doc_id| {
        const reader = readers[record.ref.input_idx] orelse continue;
        try addTypedDocValueIfPresent(alloc, &writer, &reader, record.ref.doc_id, @intCast(out_doc_id));
    }
    if (writer.entries.items.len == 0) return null;
    return try writer.build();
}

fn addTypedDocValueIfPresent(
    alloc: Allocator,
    writer: *typed_dv.TypedDocValuesWriter,
    reader: *const typed_dv.TypedDocValuesReader,
    src_doc_id: u32,
    out_doc_id: u32,
) !void {
    switch (reader.value_type) {
        .u64_val => if (try reader.getU64(src_doc_id)) |value| {
            try writer.add(out_doc_id, .{ .u64_val = value });
        },
        .i64_val => if (try reader.getI64(src_doc_id)) |value| {
            try writer.add(out_doc_id, .{ .i64_val = value });
        },
        .f64_val => if (try reader.getF64(src_doc_id)) |value| {
            try writer.add(out_doc_id, .{ .f64_val = value });
        },
        .geo_point => if (try reader.getGeoPoint(src_doc_id)) |value| {
            try writer.add(out_doc_id, .{ .geo_point = value });
        },
        .bool_val => if (try reader.getBool(src_doc_id)) |value| {
            try writer.add(out_doc_id, .{ .bool_val = value });
        },
        .bytes_val => if (try reader.getBytesAlloc(src_doc_id)) |value| {
            defer alloc.free(value);
            try writer.add(out_doc_id, .{ .bytes_val = value });
        },
    }
}

const doc_key_range_version: u8 = 1;

fn encodeDocKeyRangeAlloc(alloc: Allocator, min_key: []const u8, max_key: []const u8) ![]u8 {
    if (min_key.len > std.math.maxInt(u32) or max_key.len > std.math.maxInt(u32)) return error.InvalidSegment;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, doc_key_range_version);
    try appendU32LE(alloc, &out, @intCast(min_key.len));
    try out.appendSlice(alloc, min_key);
    try appendU32LE(alloc, &out, @intCast(max_key.len));
    try out.appendSlice(alloc, max_key);
    return try out.toOwnedSlice(alloc);
}

fn decodeDocKeyRange(section: []const u8) !SegmentReader.DocKeyRange {
    if (section.len < 9 or section[0] != doc_key_range_version) return error.InvalidSegment;
    const min_len = std.mem.readInt(u32, section[1..5], .little);
    const min_start: usize = 5;
    const min_end = min_start + @as(usize, min_len);
    if (min_end > section.len or section.len - min_end < 4) return error.InvalidSegment;
    const max_len = std.mem.readInt(u32, section[min_end..][0..4], .little);
    const max_start = min_end + 4;
    if (max_start > section.len or max_len != section.len - max_start) return error.InvalidSegment;
    const min_key = section[min_start..min_end];
    const max_key = section[max_start..];
    if (std.mem.order(u8, min_key, max_key) == .gt) return error.InvalidSegment;
    return .{ .min_key = min_key, .max_key = max_key };
}

fn mergeDocKeyRangeSectionsAlloc(alloc: Allocator, inputs: []const MergeInput) !?[]u8 {
    var min_key: ?[]const u8 = null;
    var max_key: ?[]const u8 = null;
    var saw_omitted = false;
    for (inputs) |input| {
        if (!inputHasLiveDocs(input) or !input.reader.storedFieldsOmitted()) continue;
        saw_omitted = true;
        const range = (try input.reader.docKeyRange()) orelse return error.InvalidSegment;
        if (min_key == null or std.mem.order(u8, range.min_key, min_key.?) == .lt) min_key = range.min_key;
        if (max_key == null or std.mem.order(u8, range.max_key, max_key.?) == .gt) max_key = range.max_key;
    }
    if (!saw_omitted) return null;
    return try encodeDocKeyRangeAlloc(
        alloc,
        min_key orelse return error.InvalidSegment,
        max_key orelse return error.InvalidSegment,
    );
}

pub fn encodeDocOrdinalsAlloc(alloc: Allocator, ordinals: []const u32) ![]u8 {
    var has_ordinal = false;
    for (ordinals) |ordinal| {
        if (ordinal != 0) {
            has_ordinal = true;
            break;
        }
    }
    if (!has_ordinal) return try alloc.alloc(u8, 0);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, 1);
    try appendU32BE(alloc, &out, @intCast(ordinals.len));
    for (ordinals) |ordinal| try appendU32BE(alloc, &out, ordinal);
    return try out.toOwnedSlice(alloc);
}

fn decodeDocOrdinal(section: []const u8, doc_idx: u32) !?u32 {
    if (section.len < 5) return error.InvalidSegment;
    const version = section[0];
    if (version != 1) return error.UnsupportedVersion;
    const count = std.mem.readInt(u32, section[1..5], .big);
    if (doc_idx >= count) return null;
    const expected_len = 5 + @as(usize, count) * 4;
    if (section.len != expected_len) return error.InvalidSegment;
    const offset = 5 + @as(usize, doc_idx) * 4;
    const ordinal = std.mem.readInt(u32, section[offset..][0..4], .big);
    return if (ordinal == 0) null else ordinal;
}

fn mergeDocOrdinalSectionsAlloc(alloc: Allocator, inputs: []const MergeInput, doc_count: u32) !?[]u8 {
    if (doc_count == 0) return null;
    var ordinals = try alloc.alloc(u32, doc_count);
    defer alloc.free(ordinals);

    var out_doc_id: usize = 0;
    var has_ordinal = false;
    for (inputs) |input| {
        for (0..input.reader.doc_count) |doc_id_usize| {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) continue;
            const ordinal = (try input.reader.docOrdinal(doc_id)) orelse 0;
            ordinals[out_doc_id] = ordinal;
            has_ordinal = has_ordinal or ordinal != 0;
            out_doc_id += 1;
        }
    }
    if (!has_ordinal) return null;
    return try encodeDocOrdinalsAlloc(alloc, ordinals);
}

fn mergeDocOrdinalSectionsInOrderAlloc(
    alloc: Allocator,
    inputs: []const MergeInput,
    records: []const SortedMergeRecord,
) !?[]u8 {
    if (records.len == 0) return null;
    var ordinals = try alloc.alloc(u32, records.len);
    defer alloc.free(ordinals);

    var has_ordinal = false;
    for (records, 0..) |record, out_doc_id| {
        const ordinal = (try inputs[record.ref.input_idx].reader.docOrdinal(record.ref.doc_id)) orelse 0;
        ordinals[out_doc_id] = ordinal;
        has_ordinal = has_ordinal or ordinal != 0;
    }
    if (!has_ordinal) return null;
    return try encodeDocOrdinalsAlloc(alloc, ordinals);
}

const index_sort_metadata_version: u8 = 1;
const index_sort_bounds_metadata_version: u8 = 1;

fn encodeIndexSortMetadataAlloc(alloc: Allocator, fields: []const SegmentIndexSortField) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, index_sort_metadata_version);
    try appendU16BE(alloc, &out, @intCast(fields.len));
    for (fields) |field| {
        if (field.field.len == 0 or field.field.len > std.math.maxInt(u16)) return error.InvalidSegment;
        try out.append(alloc, if (field.desc) 1 else 0);
        try appendU16BE(alloc, &out, @intCast(field.field.len));
        try out.appendSlice(alloc, field.field);
    }
    return try out.toOwnedSlice(alloc);
}

fn decodeIndexSortMetadataAlloc(alloc: Allocator, data: []const u8) ![]SegmentIndexSortField {
    if (data.len < 3) return error.InvalidSegment;
    if (data[0] != index_sort_metadata_version) return error.UnsupportedVersion;
    var pos: usize = 1;
    const field_count = std.mem.readInt(u16, data[pos..][0..2], .big);
    pos += 2;
    const fields = try alloc.alloc(SegmentIndexSortField, field_count);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |*field| field.deinit(alloc);
        alloc.free(fields);
    }
    for (0..field_count) |i| {
        if (pos + 3 > data.len) return error.InvalidSegment;
        const desc = switch (data[pos]) {
            0 => false,
            1 => true,
            else => return error.InvalidSegment,
        };
        pos += 1;
        const name_len = std.mem.readInt(u16, data[pos..][0..2], .big);
        pos += 2;
        if (name_len == 0 or pos + name_len > data.len) return error.InvalidSegment;
        fields[i] = .{
            .field = try alloc.dupe(u8, data[pos..][0..name_len]),
            .desc = desc,
        };
        initialized += 1;
        pos += name_len;
    }
    if (pos != data.len) return error.InvalidSegment;
    return fields;
}

fn encodeIndexSortBoundsMetadataAlloc(alloc: Allocator, bounds: SegmentIndexSortBounds) ![]u8 {
    if (bounds.first.len == 0 or bounds.first.len != bounds.last.len) return error.InvalidSegment;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, index_sort_bounds_metadata_version);
    try appendU16BE(alloc, &out, @intCast(bounds.first.len));
    try encodeIndexSortBoundTuple(alloc, &out, bounds.first);
    try encodeIndexSortBoundTuple(alloc, &out, bounds.last);
    return try out.toOwnedSlice(alloc);
}

fn encodeIndexSortBoundTuple(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const SegmentIndexSortBoundValue,
) !void {
    for (values) |value| {
        switch (value) {
            .u64_val => |v| {
                try out.append(alloc, 0);
                try appendU64BE(alloc, out, v);
            },
            .i64_val => |v| {
                try out.append(alloc, 5);
                try appendU64BE(alloc, out, @bitCast(v));
            },
            .f64_val => |v| {
                try out.append(alloc, 1);
                try appendU64BE(alloc, out, @bitCast(v));
            },
            .bool_val => |v| {
                try out.append(alloc, 2);
                try out.append(alloc, if (v) 1 else 0);
            },
            .bytes_val => |v| {
                try out.append(alloc, 3);
                try appendBoundBytes(alloc, out, v);
            },
            .id => |v| {
                try out.append(alloc, 4);
                try appendBoundBytes(alloc, out, v);
            },
        }
    }
}

fn appendBoundBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    if (value.len > std.math.maxInt(u32)) return error.InvalidSegment;
    try appendU32BE(alloc, out, @intCast(value.len));
    try out.appendSlice(alloc, value);
}

fn decodeIndexSortBoundsMetadataAlloc(alloc: Allocator, data: []const u8) !SegmentIndexSortBounds {
    if (data.len < 3) return error.InvalidSegment;
    if (data[0] != index_sort_bounds_metadata_version) return error.UnsupportedVersion;
    var pos: usize = 1;
    const arity = std.mem.readInt(u16, data[pos..][0..2], .big);
    pos += 2;
    if (arity == 0) return error.InvalidSegment;

    const first = try decodeIndexSortBoundTupleAlloc(alloc, data, &pos, arity);
    errdefer {
        for (first) |*value| value.deinit(alloc);
        alloc.free(first);
    }
    const last = try decodeIndexSortBoundTupleAlloc(alloc, data, &pos, arity);
    errdefer {
        for (last) |*value| value.deinit(alloc);
        alloc.free(last);
    }
    if (pos != data.len) return error.InvalidSegment;
    return .{ .first = first, .last = last };
}

fn decodeIndexSortBoundTupleAlloc(
    alloc: Allocator,
    data: []const u8,
    pos: *usize,
    arity: usize,
) ![]SegmentIndexSortBoundValue {
    const values = try alloc.alloc(SegmentIndexSortBoundValue, arity);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |*value| value.deinit(alloc);
        alloc.free(values);
    }
    for (0..arity) |i| {
        if (pos.* >= data.len) return error.InvalidSegment;
        const tag = data[pos.*];
        pos.* += 1;
        values[i] = switch (tag) {
            0 => blk: {
                if (pos.* + 8 > data.len) return error.InvalidSegment;
                const value = std.mem.readInt(u64, data[pos.*..][0..8], .big);
                pos.* += 8;
                break :blk .{ .u64_val = value };
            },
            1 => blk: {
                if (pos.* + 8 > data.len) return error.InvalidSegment;
                const bits = std.mem.readInt(u64, data[pos.*..][0..8], .big);
                pos.* += 8;
                break :blk .{ .f64_val = @bitCast(bits) };
            },
            2 => blk: {
                if (pos.* >= data.len) return error.InvalidSegment;
                const raw = data[pos.*];
                pos.* += 1;
                break :blk .{ .bool_val = switch (raw) {
                    0 => false,
                    1 => true,
                    else => return error.InvalidSegment,
                } };
            },
            3 => .{ .bytes_val = try decodeBoundBytesAlloc(alloc, data, pos) },
            4 => .{ .id = try decodeBoundBytesAlloc(alloc, data, pos) },
            5 => blk: {
                if (pos.* + 8 > data.len) return error.InvalidSegment;
                const bits = std.mem.readInt(u64, data[pos.*..][0..8], .big);
                pos.* += 8;
                break :blk .{ .i64_val = @bitCast(bits) };
            },
            else => return error.InvalidSegment,
        };
        initialized += 1;
    }
    return values;
}

fn decodeBoundBytesAlloc(alloc: Allocator, data: []const u8, pos: *usize) ![]const u8 {
    if (pos.* + 4 > data.len) return error.InvalidSegment;
    const len = std.mem.readInt(u32, data[pos.*..][0..4], .big);
    pos.* += 4;
    if (pos.* + len > data.len) return error.InvalidSegment;
    const value = try alloc.dupe(u8, data[pos.*..][0..len]);
    pos.* += len;
    return value;
}

// ============================================================================
// Helpers
// ============================================================================

fn appendU16LE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u16) !void {
    try out.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, val))));
}

fn sinkAppendU16LE(sink: *SegmentSink, val: u16) !void {
    try sink.appendSlice(&@as([2]u8, @bitCast(std.mem.nativeToLittle(u16, val))));
}

fn appendU32LE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, val))));
}

fn sinkAppendU32LE(sink: *SegmentSink, val: u32) !void {
    try sink.appendSlice(&@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, val))));
}

fn appendU64LE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u64) !void {
    try out.appendSlice(alloc, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, val))));
}

fn sinkAppendU64LE(sink: *SegmentSink, val: u64) !void {
    try sink.appendSlice(&@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, val))));
}

fn appendU16BE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u16) !void {
    try out.appendSlice(alloc, &@as([2]u8, @bitCast(std.mem.nativeToBig(u16, val))));
}

fn sinkAppendU16BE(sink: *SegmentSink, val: u16) !void {
    try sink.appendSlice(&@as([2]u8, @bitCast(std.mem.nativeToBig(u16, val))));
}

fn appendU32BE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToBig(u32, val))));
}

fn sinkAppendU32BE(sink: *SegmentSink, val: u32) !void {
    try sink.appendSlice(&@as([4]u8, @bitCast(std.mem.nativeToBig(u32, val))));
}

fn appendU64BE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u64) !void {
    try out.appendSlice(alloc, &@as([8]u8, @bitCast(std.mem.nativeToBig(u64, val))));
}

fn sinkAppendU64BE(sink: *SegmentSink, val: u64) !void {
    try sink.appendSlice(&@as([8]u8, @bitCast(std.mem.nativeToBig(u64, val))));
}

// ============================================================================
// Tests
// ============================================================================

test "segment roundtrip" {
    const alloc = std.testing.allocator;

    // Build an inverted index
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{
        .{ .term = "hello", .freq = 1 },
        .{ .term = "world", .freq = 1 },
    });
    try inv_builder.addDocument(1, &.{
        .{ .term = "hello", .freq = 2 },
    });
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Build segment
    var seg_writer = SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    const field_idx = try seg_writer.addField("content");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    try seg_writer.addStoredDoc("doc-1", "Hello world");
    try seg_writer.addStoredDoc("doc-2", "Hello again");

    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    // Read it back
    var reader = try SegmentReader.init(alloc, seg_bytes);
    defer reader.deinit();

    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);
    try std.testing.expectEqual(@as(u16, 1), reader.num_fields);
    try std.testing.expectEqualStrings("content", reader.fields[0].name);

    // Read inverted index
    var inv_reader = (try reader.invertedIndex("content")) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), inv_reader.doc_count);

    const hello = inv_reader.lookup("hello") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), hello.docFreq());

    const world = inv_reader.lookup("world") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), world.docFreq());

    // Read stored docs (decompressed)
    const doc0 = (try reader.storedDocDecompressed(0)) orelse return error.TestExpectedEqual;
    defer alloc.free(doc0.data);
    try std.testing.expectEqualStrings("doc-1", doc0.id);
    try std.testing.expectEqualStrings("Hello world", doc0.data);

    const doc1 = (try reader.storedDocDecompressed(1)) orelse return error.TestExpectedEqual;
    defer alloc.free(doc1.data);
    try std.testing.expectEqualStrings("doc-2", doc1.id);
}

test "segment layout stats ignores invalid inverted section slice" {
    var sections = [_]SegmentReader.SectionInfo{.{
        .section_type = .inverted_text,
        .offset = 60,
        .length = 8,
    }};
    var fields = [_]SegmentReader.FieldInfo{.{
        .name = "content",
        .sections = sections[0..],
    }};
    const data = [_]u8{0} ** 64;
    const reader = SegmentReader{
        .alloc = std.testing.allocator,
        .data = &data,
        .stored_offset = data.len - footer_size,
        .index_offset = data.len - footer_size,
        .doc_count = 0,
        .num_fields = 1,
        .fields = fields[0..],
    };

    const stats = reader.layoutStatsWithInvertedDetails(true);
    try std.testing.expectEqual(@as(u64, 8), stats.inverted_text_bytes);
    try std.testing.expectEqual(@as(u64, 0), stats.inverted_postings_bytes);
}

test "segment merge" {
    const alloc = std.testing.allocator;

    // Build segment 1
    var inv1 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv1.deinit();
    try inv1.addDocument(0, &.{.{ .term = "alpha", .freq = 1 }});
    const inv1_data = try inv1.build();
    defer alloc.free(inv1_data);

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    const f1 = try sw1.addField("body");
    try sw1.addSection(f1, .inverted_text, inv1_data);
    try sw1.addStoredDoc("a", "doc A");
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    // Build segment 2
    var inv2 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv2.deinit();
    try inv2.addDocument(0, &.{.{ .term = "alpha", .freq = 2 }});
    try inv2.addDocument(1, &.{.{ .term = "beta", .freq = 1 }});
    const inv2_data = try inv2.build();
    defer alloc.free(inv2_data);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    const f2 = try sw2.addField("body");
    try sw2.addSection(f2, .inverted_text, inv2_data);
    try sw2.addStoredDoc("b", "doc B");
    try sw2.addStoredDoc("c", "doc C");
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    // Merge
    const merged = try mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var reader = try SegmentReader.init(alloc, merged);
    defer reader.deinit();

    try std.testing.expectEqual(@as(u32, 3), reader.doc_count);

    // "alpha" should be in both segments (2 docs total)
    var inv_reader = (try reader.invertedIndex("body")) orelse return error.TestExpectedEqual;
    const alpha = inv_reader.lookup("alpha") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), alpha.docFreq());

    // "beta" only in segment 2
    const beta = inv_reader.lookup("beta") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), beta.docFreq());

    // All 3 stored docs present
    try std.testing.expect(reader.storedDoc(0) != null);
    try std.testing.expect(reader.storedDoc(1) != null);
    try std.testing.expect(reader.storedDoc(2) != null);
    try std.testing.expect(reader.storedDoc(3) == null);
}

test "segment append merge preserves bytes typed doc values" {
    const alloc = std.testing.allocator;

    var tenant1_writer = typed_dv.TypedDocValuesWriter.init(alloc, .bytes_val, 1024);
    defer tenant1_writer.deinit();
    try tenant1_writer.add(0, .{ .bytes_val = "acme" });
    const tenant1_data = try tenant1_writer.build();
    defer alloc.free(tenant1_data);

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    const tenant1 = try sw1.addField("tenant");
    try sw1.addSection(tenant1, .typed_doc_values, tenant1_data);
    try sw1.addStoredDoc("doc:acme", "{\"tenant\":\"acme\"}");
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    var tenant2_writer = typed_dv.TypedDocValuesWriter.init(alloc, .bytes_val, 1024);
    defer tenant2_writer.deinit();
    try tenant2_writer.add(0, .{ .bytes_val = "beta" });
    const tenant2_data = try tenant2_writer.build();
    defer alloc.free(tenant2_data);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    const tenant2 = try sw2.addField("tenant");
    try sw2.addSection(tenant2, .typed_doc_values, tenant2_data);
    try sw2.addStoredDoc("doc:beta", "{\"tenant\":\"beta\"}");
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    const merged = try mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), merged_reader.doc_count);

    var tenant_reader = try typed_dv.TypedDocValuesReader.init(alloc, merged_reader.getSection("tenant", .typed_doc_values) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(typed_dv.ValueType.bytes_val, tenant_reader.value_type);

    const tenant0 = (try tenant_reader.getBytesAlloc(0)) orelse return error.TestExpectedEqual;
    defer alloc.free(tenant0);
    const tenant1_value = (try tenant_reader.getBytesAlloc(1)) orelse return error.TestExpectedEqual;
    defer alloc.free(tenant1_value);

    try std.testing.expectEqualStrings("acme", tenant0);
    try std.testing.expectEqualStrings("beta", tenant1_value);
}

test "index-only stored fields preserve ordinals key ranges and merges" {
    const alloc = std.testing.allocator;
    var left_writer = SegmentWriter.init(alloc);
    defer left_writer.deinit();
    try left_writer.addUnstoredDoc();
    try left_writer.addDocOrdinals(&.{11});
    try left_writer.addDocKeyRange("doc:a", "doc:a");
    const left = try left_writer.build();
    defer alloc.free(left);

    var right_writer = SegmentWriter.init(alloc);
    defer right_writer.deinit();
    try right_writer.addUnstoredDoc();
    try right_writer.addDocOrdinals(&.{12});
    try right_writer.addDocKeyRange("doc:z", "doc:z");
    const right = try right_writer.build();
    defer alloc.free(right);

    const merged = try mergeSegments(alloc, &.{ left, right });
    defer alloc.free(merged);
    var reader = try SegmentReader.init(alloc, merged);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);
    try std.testing.expect(reader.storedFieldsOmitted());
    try std.testing.expect(reader.storedDoc(0) == null);
    try std.testing.expectEqual(@as(?u32, 11), try reader.docOrdinal(0));
    try std.testing.expectEqual(@as(?u32, 12), try reader.docOrdinal(1));
    const range = (try reader.docKeyRange()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("doc:a", range.min_key);
    try std.testing.expectEqualStrings("doc:z", range.max_key);
}

test "segment block-compressed stored fields cross block boundary" {
    const alloc = std.testing.allocator;

    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();

    var id_buf: [32]u8 = undefined;
    var data_buf: [96]u8 = undefined;
    for (0..(stored_fields_block_doc_target + 7)) |i| {
        const id = try std.fmt.bufPrint(&id_buf, "doc-{d}", .{i});
        const data = try std.fmt.bufPrint(&data_buf, "{{\"ordinal\":{d},\"body\":\"stored field block boundary\"}}", .{i});
        try writer.addStoredDoc(id, data);
    }

    const bytes = try writer.build();
    defer alloc.free(bytes);

    var reader = try SegmentReader.init(alloc, bytes);
    defer reader.deinit();

    try std.testing.expect(reader.storedDocsAreCompressed());
    try std.testing.expect(writer.last_stored_raw_bytes > writer.last_stored_compressed_bytes);
    try std.testing.expectEqual(@as(u32, @intCast(stored_fields_block_doc_target + 7)), reader.doc_count);

    const first_ref = reader.storedDoc(0) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("doc-0", first_ref.id);
    try std.testing.expectEqual(@as(usize, 0), first_ref.data.len);

    const boundary = (try reader.storedDocDecompressed(@intCast(stored_fields_block_doc_target))) orelse return error.TestExpectedEqual;
    defer alloc.free(boundary.data);
    try std.testing.expectEqualStrings("doc-128", boundary.id);
    try std.testing.expect(std.mem.indexOf(u8, boundary.data, "\"ordinal\":128") != null);

    const last_doc_id: u32 = @intCast(stored_fields_block_doc_target + 6);
    const last = (try reader.storedDocDecompressed(last_doc_id)) orelse return error.TestExpectedEqual;
    defer alloc.free(last.data);
    try std.testing.expectEqualStrings("doc-134", last.id);
    try std.testing.expect(std.mem.indexOf(u8, last.data, "\"ordinal\":134") != null);
}

test "segment stored fields split large blocks by raw byte budget" {
    const alloc = std.testing.allocator;

    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();

    const large = try alloc.alloc(u8, stored_fields_block_raw_target / 3);
    defer alloc.free(large);
    @memset(large, 'x');

    try writer.addStoredDoc("doc-0", large);
    try writer.addStoredDoc("doc-1", large);
    try writer.addStoredDoc("doc-2", large);

    const bytes = try writer.build();
    defer alloc.free(bytes);

    var reader = try SegmentReader.init(alloc, bytes);
    defer reader.deinit();

    const loc0 = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), 0) orelse return error.TestExpectedEqual;
    const loc1 = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), 1) orelse return error.TestExpectedEqual;
    const loc2 = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), 2) orelse return error.TestExpectedEqual;

    try std.testing.expectEqual(@as(u32, 0), loc0.block_idx);
    try std.testing.expectEqual(@as(u32, 0), loc1.block_idx);
    try std.testing.expectEqual(@as(u32, 1), loc2.block_idx);
    try std.testing.expect(loc0.block_end - loc0.block_start < stored_fields_block_raw_target);
    try std.testing.expect(loc2.block_end > loc2.block_start);
}

test "merge copies aligned stored field blocks without recompressing" {
    const alloc = std.testing.allocator;

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    var id_buf: [32]u8 = undefined;
    var data_buf: [128]u8 = undefined;
    for (0..stored_fields_block_doc_target) |i| {
        const id = try std.fmt.bufPrint(&id_buf, "a-{d}", .{i});
        const data = try std.fmt.bufPrint(&data_buf, "{{\"segment\":\"a\",\"ordinal\":{d},\"body\":\"copy aligned stored block\"}}", .{i});
        try sw1.addStoredDoc(id, data);
    }
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    try sw2.addStoredDoc("b-0", "{\"segment\":\"b\",\"ordinal\":0}");
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    var r1 = try SegmentReader.init(alloc, seg1);
    defer r1.deinit();
    const src_loc = r1.v4StoredDocLocation(@intCast(r1.stored_offset + 1 + 4), 0) orelse return error.TestExpectedEqual;
    const src_block = r1.data[src_loc.block_start..src_loc.block_end];

    const merged = try mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    const merged_loc = merged_reader.v4StoredDocLocation(@intCast(merged_reader.stored_offset + 1 + 4), 0) orelse return error.TestExpectedEqual;
    const merged_block = merged_reader.data[merged_loc.block_start..merged_loc.block_end];
    try std.testing.expectEqualSlices(u8, src_block, merged_block);

    const last_copied = (try merged_reader.storedDocDecompressed(@intCast(stored_fields_block_doc_target - 1))) orelse return error.TestExpectedEqual;
    defer alloc.free(last_copied.data);
    try std.testing.expectEqualStrings("a-127", last_copied.id);

    const tail = (try merged_reader.storedDocDecompressed(@intCast(stored_fields_block_doc_target))) orelse return error.TestExpectedEqual;
    defer alloc.free(tail.data);
    try std.testing.expectEqualStrings("b-0", tail.id);
}

test "segment doc ordinal sidecar roundtrip and merge preserve live order" {
    const alloc = std.testing.allocator;

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    try sw1.addStoredDoc("a", "{}");
    try sw1.addStoredDoc("b", "{}");
    try sw1.addDocOrdinals(&.{ 7, 11 });
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    try sw2.addStoredDoc("c", "{}");
    try sw2.addDocOrdinals(&.{13});
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    var reader1 = try SegmentReader.init(alloc, seg1);
    defer reader1.deinit();
    try std.testing.expectEqual(@as(?u32, 7), try reader1.docOrdinal(0));
    try std.testing.expectEqual(@as(?u32, 11), try reader1.docOrdinal(1));

    var reader2 = try SegmentReader.init(alloc, seg2);
    defer reader2.deinit();
    var deleted = roaring.RoaringBitmap.init(alloc);
    defer deleted.deinit();
    try deleted.add(0);

    const merged = try mergeSegmentInputs(alloc, &.{
        .{ .reader = &reader1, .deleted = deleted },
        .{ .reader = &reader2 },
    });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), merged_reader.doc_count);
    try std.testing.expectEqual(@as(?u32, 11), try merged_reader.docOrdinal(0));
    try std.testing.expectEqual(@as(?u32, 13), try merged_reader.docOrdinal(1));
}

test "segment index sort metadata roundtrip" {
    const alloc = std.testing.allocator;

    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();
    try writer.addStoredDoc("doc:a", "{}");
    try writer.addIndexSortMetadata(&.{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    });
    const bytes = try writer.build();
    defer alloc.free(bytes);

    var reader = try SegmentReader.init(alloc, bytes);
    defer reader.deinit();
    const fields = (try reader.indexSortFieldsAlloc(alloc)) orelse return error.TestExpectedEqual;
    defer {
        for (fields) |*field| field.deinit(alloc);
        alloc.free(fields);
    }

    try std.testing.expectEqual(@as(usize, 2), fields.len);
    try std.testing.expectEqualStrings("created_at", fields[0].field);
    try std.testing.expect(fields[0].desc);
    try std.testing.expectEqualStrings("_id", fields[1].field);
    try std.testing.expect(!fields[1].desc);
}

test "segment merge drops index sort metadata until physical sort is preserved" {
    const alloc = std.testing.allocator;

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    try sw1.addStoredDoc("doc:a", "{}");
    try sw1.addIndexSortMetadata(&.{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    });
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    try sw2.addStoredDoc("doc:b", "{}");
    try sw2.addIndexSortMetadata(&.{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    });
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    const merged = try mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    try std.testing.expect((try merged_reader.indexSortFieldsAlloc(alloc)) == null);
}

test "segment sorted merge preserves index sort and remaps doc addressed sections" {
    const alloc = std.testing.allocator;

    var inv1 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv1.deinit();
    try inv1.addDocument(0, &.{.{ .term = "all", .freq = 1 }});
    try inv1.addDocument(1, &.{.{ .term = "all", .freq = 1 }});
    const inv1_data = try inv1.build();
    defer alloc.free(inv1_data);

    var price1_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer price1_writer.deinit();
    try price1_writer.add(0, .{ .u64_val = 3 });
    try price1_writer.add(1, .{ .u64_val = 1 });
    const price1_data = try price1_writer.build();
    defer alloc.free(price1_data);

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    const body1 = try sw1.addField("body");
    try sw1.addSection(body1, .inverted_text, inv1_data);
    const price1 = try sw1.addField("price");
    try sw1.addSection(price1, .typed_doc_values, price1_data);
    try sw1.addStoredDoc("doc:c", "{\"price\":3}");
    try sw1.addStoredDoc("doc:a", "{\"price\":1}");
    try sw1.addDocOrdinals(&.{ 30, 10 });
    try sw1.addIndexSortMetadata(&.{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    });
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    var inv2 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv2.deinit();
    try inv2.addDocument(0, &.{.{ .term = "all", .freq = 1 }});
    const inv2_data = try inv2.build();
    defer alloc.free(inv2_data);

    var price2_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer price2_writer.deinit();
    try price2_writer.add(0, .{ .u64_val = 2 });
    const price2_data = try price2_writer.build();
    defer alloc.free(price2_data);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    const body2 = try sw2.addField("body");
    try sw2.addSection(body2, .inverted_text, inv2_data);
    const price2 = try sw2.addField("price");
    try sw2.addSection(price2, .typed_doc_values, price2_data);
    try sw2.addStoredDoc("doc:b", "{\"price\":2}");
    try sw2.addDocOrdinals(&.{20});
    try sw2.addIndexSortMetadata(&.{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    });
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    var reader1 = try SegmentReader.init(alloc, seg1);
    defer reader1.deinit();
    var reader2 = try SegmentReader.init(alloc, seg2);
    defer reader2.deinit();

    const sort_fields = [_]SegmentIndexSortField{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    };
    const merged = try mergeSegmentInputsWithOptions(alloc, &.{
        .{ .reader = &reader1 },
        .{ .reader = &reader2 },
    }, .{ .index_sort = &sort_fields });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    try std.testing.expectEqual(@as(u32, 3), merged_reader.doc_count);
    try std.testing.expectEqualStrings("doc:a", merged_reader.storedDoc(0).?.id);
    try std.testing.expectEqualStrings("doc:b", merged_reader.storedDoc(1).?.id);
    try std.testing.expectEqualStrings("doc:c", merged_reader.storedDoc(2).?.id);
    try std.testing.expectEqual(@as(?u32, 10), try merged_reader.docOrdinal(0));
    try std.testing.expectEqual(@as(?u32, 20), try merged_reader.docOrdinal(1));
    try std.testing.expectEqual(@as(?u32, 30), try merged_reader.docOrdinal(2));

    const merged_sort = (try merged_reader.indexSortFieldsAlloc(alloc)) orelse return error.TestExpectedEqual;
    defer {
        for (merged_sort) |*field| field.deinit(alloc);
        alloc.free(merged_sort);
    }
    try std.testing.expectEqual(@as(usize, 2), merged_sort.len);
    try std.testing.expectEqualStrings("price", merged_sort[0].field);
    try std.testing.expectEqualStrings("_id", merged_sort[1].field);

    var price_reader = try typed_dv.TypedDocValuesReader.init(alloc, merged_reader.getSection("price", .typed_doc_values) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(@as(?u64, 1), try price_reader.getU64(0));
    try std.testing.expectEqual(@as(?u64, 2), try price_reader.getU64(1));
    try std.testing.expectEqual(@as(?u64, 3), try price_reader.getU64(2));

    var inv_reader = (try merged_reader.invertedIndex("body")) orelse return error.TestExpectedEqual;
    const all = inv_reader.lookup("all") orelse return error.TestExpectedEqual;
    var iter = try all.iterator(alloc);
    defer iter.deinit();
    var seen: [3]bool = .{ false, false, false };
    while (try iter.next()) |hit| {
        if (hit.doc_id >= seen.len) return error.TestExpectedEqual;
        seen[hit.doc_id] = true;
    }
    try std.testing.expect(seen[0] and seen[1] and seen[2]);
}

fn buildLegacyF64DocValuesSectionAlloc(alloc: Allocator, doc_id: u32, value: f64) ![]u8 {
    var chunk_data = std.ArrayListUnmanaged(u8).empty;
    defer chunk_data.deinit(alloc);
    try chunk_data.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 1))));
    try chunk_data.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_id))));
    try chunk_data.appendSlice(alloc, &@as([8]u8, @bitCast(value)));

    const compressed = try snappy.encode(alloc, chunk_data.items);
    defer alloc.free(compressed);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, @intFromEnum(typed_dv.ValueType.f64_val));
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, 1))));
    const offset_pos = out.items.len;
    try out.appendNTimes(alloc, 0, 8);
    try out.appendSlice(alloc, compressed);
    out.items[offset_pos..][0..8].* = @bitCast(std.mem.nativeToLittle(u64, @as(u64, @intCast(out.items.len))));
    return try out.toOwnedSlice(alloc);
}

test "segment sorted merge rejects non-finite f64 index sort values" {
    const alloc = std.testing.allocator;

    const price_data = try buildLegacyF64DocValuesSectionAlloc(alloc, 0, std.math.nan(f64));
    defer alloc.free(price_data);

    var sw = SegmentWriter.init(alloc);
    defer sw.deinit();
    const price = try sw.addField("price");
    try sw.addSection(price, .typed_doc_values, price_data);
    try sw.addStoredDoc("doc:a", "{\"price\":1}");
    try sw.addIndexSortMetadata(&.{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    });
    const seg = try sw.build();
    defer alloc.free(seg);

    var reader = try SegmentReader.init(alloc, seg);
    defer reader.deinit();

    const sort_fields = [_]SegmentIndexSortField{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    };
    try std.testing.expectError(error.InvalidSegment, mergeSegmentInputsWithOptions(alloc, &.{
        .{ .reader = &reader },
    }, .{ .index_sort = &sort_fields }));
}

test "segment sorted merge rejects mixed index sort doc value domains" {
    const alloc = std.testing.allocator;

    var u64_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer u64_writer.deinit();
    try u64_writer.add(0, .{ .u64_val = 2 });
    const u64_data = try u64_writer.build();
    defer alloc.free(u64_data);

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    const price1 = try sw1.addField("price");
    try sw1.addSection(price1, .typed_doc_values, u64_data);
    try sw1.addStoredDoc("doc:b", "{\"price\":2}");
    try sw1.addIndexSortMetadata(&.{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    });
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    var i64_writer = typed_dv.TypedDocValuesWriter.init(alloc, .i64_val, 1024);
    defer i64_writer.deinit();
    try i64_writer.add(0, .{ .i64_val = 1 });
    const i64_data = try i64_writer.build();
    defer alloc.free(i64_data);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    const price2 = try sw2.addField("price");
    try sw2.addSection(price2, .typed_doc_values, i64_data);
    try sw2.addStoredDoc("doc:a", "{\"price\":1}");
    try sw2.addIndexSortMetadata(&.{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    });
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    var reader1 = try SegmentReader.init(alloc, seg1);
    defer reader1.deinit();
    var reader2 = try SegmentReader.init(alloc, seg2);
    defer reader2.deinit();

    const sort_fields = [_]SegmentIndexSortField{
        .{ .field = "price", .desc = false },
        .{ .field = "_id", .desc = false },
    };
    try std.testing.expectError(error.InvalidSegment, mergeSegmentInputsWithOptions(alloc, &.{
        .{ .reader = &reader1 },
        .{ .reader = &reader2 },
    }, .{ .index_sort = &sort_fields }));
}

test "multi-field segment" {
    const alloc = std.testing.allocator;

    var inv_title = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_title.deinit();
    try inv_title.addDocument(0, &.{.{ .term = "zig", .freq = 1 }});
    const title_data = try inv_title.build();
    defer alloc.free(title_data);

    var inv_body = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_body.deinit();
    try inv_body.addDocument(0, &.{
        .{ .term = "zig", .freq = 3 },
        .{ .term = "fast", .freq = 2 },
    });
    const body_data = try inv_body.build();
    defer alloc.free(body_data);

    var sw = SegmentWriter.init(alloc);
    defer sw.deinit();
    const title_field = try sw.addField("title");
    try sw.addSection(title_field, .inverted_text, title_data);
    const body_field = try sw.addField("body");
    try sw.addSection(body_field, .inverted_text, body_data);
    try sw.addStoredDoc("doc-1", "{}");
    const seg = try sw.build();
    defer alloc.free(seg);

    var reader = try SegmentReader.init(alloc, seg);
    defer reader.deinit();

    try std.testing.expectEqual(@as(u16, 2), reader.num_fields);

    // "zig" in title field
    var title_reader = (try reader.invertedIndex("title")) orelse return error.TestExpectedEqual;
    try std.testing.expect(title_reader.lookup("zig") != null);
    try std.testing.expect(title_reader.lookup("fast") == null);

    // "fast" in body field
    var body_reader = (try reader.invertedIndex("body")) orelse return error.TestExpectedEqual;
    try std.testing.expect(body_reader.lookup("fast") != null);
    try std.testing.expect(body_reader.lookup("zig") != null);
}

test "merge segments with sparse field coverage" {
    // Regression: merging segments where a field's inverted section has fewer
    // documents than the segment total (some docs lack the field).
    const alloc = std.testing.allocator;

    // Segment 1: 3 docs, all have "title", only doc 2 has "category"
    var inv_title1 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_title1.deinit();
    try inv_title1.addDocument(0, &.{.{ .term = "alpha", .freq = 1, .norm = 3 }});
    try inv_title1.addDocument(1, &.{.{ .term = "beta", .freq = 1, .norm = 3 }});
    try inv_title1.addDocument(2, &.{.{ .term = "gamma", .freq = 1, .norm = 3 }});
    const title1 = try inv_title1.build();
    defer alloc.free(title1);

    var inv_cat1 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_cat1.deinit();
    // Only doc 2 has this field — inverted doc_count will be 1
    try inv_cat1.addDocument(2, &.{.{ .term = "books", .freq = 1, .norm = 1 }});
    const cat1 = try inv_cat1.build();
    defer alloc.free(cat1);

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    const f_title1 = try sw1.addField("title");
    try sw1.addSection(f_title1, .inverted_text, title1);
    const f_cat1 = try sw1.addField("category");
    try sw1.addSection(f_cat1, .inverted_text, cat1);
    try sw1.addStoredDoc("d1", "{}");
    try sw1.addStoredDoc("d2", "{}");
    try sw1.addStoredDoc("d3", "{}");
    const seg1 = try sw1.build();
    defer alloc.free(seg1);

    // Segment 2: 1 doc with both fields
    var inv_title2 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_title2.deinit();
    try inv_title2.addDocument(0, &.{.{ .term = "delta", .freq = 1, .norm = 1 }});
    const title2 = try inv_title2.build();
    defer alloc.free(title2);

    var inv_cat2 = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_cat2.deinit();
    try inv_cat2.addDocument(0, &.{.{ .term = "music", .freq = 1, .norm = 1 }});
    const cat2 = try inv_cat2.build();
    defer alloc.free(cat2);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    const f_title2 = try sw2.addField("title");
    try sw2.addSection(f_title2, .inverted_text, title2);
    const f_cat2 = try sw2.addField("category");
    try sw2.addSection(f_cat2, .inverted_text, cat2);
    try sw2.addStoredDoc("d4", "{}");
    const seg2 = try sw2.build();
    defer alloc.free(seg2);

    // Merge should not fail with InvalidData
    const merged = try mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var reader = try SegmentReader.init(alloc, merged);
    defer reader.deinit();

    try std.testing.expectEqual(@as(u32, 4), reader.doc_count);

    // All title terms should be present
    var title_inv = (try reader.invertedIndex("title")) orelse return error.TestExpectedEqual;
    try std.testing.expect(title_inv.lookup("alpha") != null);
    try std.testing.expect(title_inv.lookup("delta") != null);

    // Category terms should be present
    var cat_inv = (try reader.invertedIndex("category")) orelse return error.TestExpectedEqual;
    try std.testing.expect(cat_inv.lookup("books") != null);
    try std.testing.expect(cat_inv.lookup("music") != null);
}
