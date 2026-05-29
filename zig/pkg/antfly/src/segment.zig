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
const platform_time = @import("platform/time.zig");
const inverted = @import("section/inverted.zig");
const typed_dv = @import("section/typed_doc_values.zig");
const snappy = @import("encoding/snappy.zig");
const roaring = @import("encoding/roaring.zig");

// ============================================================================
// Constants
// ============================================================================

const magic: [4]u8 = "AFSM".*; // AntFly SegMent
const segment_version: u32 = 2; // v2: big-endian footer + CRC32 + Snappy stored fields
const stored_fields_version_compressed_per_doc: u8 = 2;
const stored_fields_version_uncompressed_offsets: u8 = 3;
const stored_fields_version_block_compressed: u8 = 4;
const stored_fields_block_doc_target: usize = 128;
const stored_fields_v4_doc_entry_size: usize = 24;

/// Fixed footer size (big-endian, at end of segment):
///   [numDocs: u64 BE]           8
///   [storedIndexOffset: u64 BE] 8
///   [sectionsIndexOffset: u64 BE] 8
///   [chunkMode: u32 BE]         4
///   [version: u32 BE]           4
///   [CRC32: u32 BE]             4
///   [magic: 4 bytes]            4
const footer_size: usize = 8 + 8 + 8 + 4 + 4 + 4 + 4; // 40 bytes

pub const SectionType = enum(u16) {
    inverted_text = 0,
    vector = 1,
    synonym = 2,
    columnar_stored = 3,
    typed_doc_values = 4,
    doc_ordinals = 5,
};

pub const doc_ordinals_field = "\x00__antfly_doc_ordinals";

pub const SegmentLayoutStats = struct {
    stored_fields_bytes: u64 = 0,
    inverted_text_bytes: u64 = 0,
    inverted_header_bytes: u64 = 0,
    inverted_fst_bytes: u64 = 0,
    inverted_bloom_bytes: u64 = 0,
    inverted_postings_header_bytes: u64 = 0,
    inverted_block_max_bytes: u64 = 0,
    inverted_chunk_meta_bytes: u64 = 0,
    inverted_postings_payload_bytes: u64 = 0,
    inverted_positions_bytes: u64 = 0,
    inverted_skip_bytes: u64 = 0,
    inverted_one_hit_terms: u64 = 0,
    inverted_postings_terms: u64 = 0,
    typed_doc_values_bytes: u64 = 0,
    doc_ordinals_bytes: u64 = 0,
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

        const footer_start = sink.len();
        try sinkAppendU64BE(sink, @intCast(self.doc_count));
        try sinkAppendU64BE(sink, stored_offset);
        try sinkAppendU64BE(sink, sections_index_offset);
        try sinkAppendU32BE(sink, 0);
        try sinkAppendU32BE(sink, segment_version);
        const crc = try sink.crc32Prefix(footer_start + 32);
        try sinkAppendU32BE(sink, crc);
        try sink.appendSlice(&magic);
    }

    fn estimatedBuildSize(self: *const SegmentWriter) usize {
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
        const num_docs: u32 = @intCast(self.stored_fields.items.len);
        try sink.appendByte(stored_fields_version_block_compressed);
        self.last_stored_compress_ns = 0;
        self.last_stored_raw_bytes = 0;
        self.last_stored_compressed_bytes = 0;
        try sinkAppendU32LE(sink, num_docs);
        const num_blocks: u32 = if (num_docs == 0) 0 else @intCast((@as(usize, num_docs) - 1) / stored_fields_block_doc_target + 1);
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
        var doc_index: usize = 0;
        for (0..num_blocks) |block_idx| {
            var chunk = std.ArrayListUnmanaged(u8).empty;
            defer chunk.deinit(self.alloc);
            chunk.clearRetainingCapacity();

            const block_end = @min(doc_index + stored_fields_block_doc_target, self.stored_fields.items.len);
            while (doc_index < block_end) : (doc_index += 1) {
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

        // Verify CRC32 (over everything up to but not including CRC + magic)
        const footer_data_end = end - 8; // exclude CRC + magic
        const expected_crc = std.hash.Crc32.hash(data[0..footer_data_end]);
        if (stored_crc != expected_crc) return error.CrcMismatch;

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
                        const section_data = self.data[offset..][0..@intCast(length)];
                        if (inverted.InvertedIndexReader.init(self.alloc, section_data)) |reader| {
                            const inverted_layout = if (detailed_inverted)
                                reader.detailedLayoutStats() catch reader.layoutStats()
                            else
                                reader.layoutStats();
                            {
                                stats.inverted_header_bytes +|= inverted_layout.header_bytes;
                                stats.inverted_fst_bytes +|= inverted_layout.fst_bytes;
                                stats.inverted_bloom_bytes +|= inverted_layout.bloom_bytes;
                                stats.inverted_postings_header_bytes +|= inverted_layout.postings_header_bytes;
                                stats.inverted_block_max_bytes +|= inverted_layout.block_max_bytes;
                                stats.inverted_chunk_meta_bytes +|= inverted_layout.chunk_meta_bytes;
                                stats.inverted_postings_payload_bytes +|= inverted_layout.postings_payload_bytes;
                                stats.inverted_positions_bytes +|= inverted_layout.positions_bytes;
                                stats.inverted_skip_bytes +|= inverted_layout.skip_bytes;
                                stats.inverted_one_hit_terms +|= inverted_layout.one_hit_terms;
                                stats.inverted_postings_terms +|= inverted_layout.postings_terms;
                            }
                        } else |_| {}
                    },
                    .typed_doc_values => stats.typed_doc_values_bytes +|= length,
                    .doc_ordinals => stats.doc_ordinals_bytes +|= length,
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

    pub const V4StoredDocLocation = struct {
        id: []const u8,
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
        try self.out.appendSlice(self.alloc, bytes);
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
        @memcpy(self.out.items[offset..][0..bytes.len], bytes);
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
    if (inputs.len == 0) return error.NoSegments;

    var sink_impl = MemorySegmentSink.init(alloc);
    errdefer sink_impl.deinit();
    var sink = sink_impl.sink();
    try writeMergedSegmentToSink(alloc, &sink, inputs);
    return try sink_impl.finishOwned();
}

pub fn writeMergedSegmentToSink(alloc: Allocator, sink: *SegmentSink, inputs: []const MergeInput) !void {
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
                const merged = try inverted.mergeInvertedSectionSlotsWithDeletes(alloc, inv_sections, doc_counts, deleted_docs, .{});
                defer alloc.free(merged);
                try appendBuiltSection(alloc, sink, &built_field, .inverted_text, merged);
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

    const sections_index_offset: u64 = @intCast(sink.len());
    try writeMergedSectionIndex(alloc, sink, built_fields.items);

    const footer_start = sink.len();
    try sinkAppendU64BE(sink, doc_count);
    try sinkAppendU64BE(sink, stored_offset);
    try sinkAppendU64BE(sink, sections_index_offset);
    try sinkAppendU32BE(sink, 0);
    try sinkAppendU32BE(sink, segment_version);
    const crc = try sink.crc32Prefix(footer_start + 32);
    try sinkAppendU32BE(sink, crc);
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

fn writeMergedStoredFields(alloc: Allocator, sink: *SegmentSink, inputs: []const MergeInput, doc_count: u32) !void {
    try sink.appendByte(stored_fields_version_block_compressed);
    try sinkAppendU32LE(sink, doc_count);
    const num_blocks: u32 = if (doc_count == 0) 0 else @intCast((@as(usize, doc_count) - 1) / stored_fields_block_doc_target + 1);
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

            if (chunk.items.len == 0 and out_doc_id % stored_fields_block_doc_target == 0) {
                if (try copyMergedStoredBlockIfPossible(sink, input, doc_id, out_doc_id, block_idx, doc_table_start, block_offsets_start, data_start)) |copied_docs| {
                    out_doc_id += copied_docs;
                    doc_id_usize += copied_docs;
                    block_idx += 1;
                    continue;
                }
            }

            if (chunk.items.len > 0 and out_doc_id > 0 and out_doc_id % stored_fields_block_doc_target == 0) {
                const compressed = try snappy.encode(alloc, chunk.items);
                defer alloc.free(compressed);
                try sink.appendSlice(compressed);
                const block_end_offset: u64 = @intCast(sink.len() - data_start);
                try sink.writeAt(block_offsets_start + @as(usize, block_idx) * 8, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, block_end_offset))));
                block_idx += 1;
                chunk.clearRetainingCapacity();
            }

            const stored = (try input.reader.storedDocDecompressed(doc_id)) orelse continue;
            defer alloc.free(stored.data);
            const doc_offset: u32 = @intCast(chunk.items.len);
            try appendU32LE(alloc, &chunk, @intCast(stored.data.len));
            try chunk.appendSlice(alloc, stored.data);
            const entry_pos = doc_table_start + @as(usize, out_doc_id) * stored_fields_v4_doc_entry_size;
            try sink.writeAt(entry_pos + 12, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, block_idx))));
            try sink.writeAt(entry_pos + 16, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, doc_offset))));
            try sink.writeAt(entry_pos + 20, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(stored.data.len))))));
            out_doc_id += 1;
            doc_id_usize += 1;
        }
    }
    if (doc_count > 0) {
        const compressed = try snappy.encode(alloc, chunk.items);
        defer alloc.free(compressed);
        try sink.appendSlice(compressed);
        const block_end_offset: u64 = @intCast(sink.len() - data_start);
        try sink.writeAt(block_offsets_start + @as(usize, block_idx) * 8, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, block_end_offset))));
    }
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
    if (input.deleted != null) return null;
    const reader = input.reader;
    if (reader.data[@intCast(reader.stored_offset)] != stored_fields_version_block_compressed) return null;
    if (start_doc_id % stored_fields_block_doc_target != 0) return null;
    if (out_doc_id % stored_fields_block_doc_target != 0) return null;
    if (start_doc_id + stored_fields_block_doc_target > reader.doc_count) return null;

    var first_loc: ?SegmentReader.V4StoredDocLocation = null;
    var i: u32 = 0;
    while (i < stored_fields_block_doc_target) : (i += 1) {
        const loc = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), start_doc_id + i) orelse return null;
        if (first_loc == null) {
            if (loc.doc_offset != 0) return null;
            first_loc = loc;
        } else if (loc.block_start != first_loc.?.block_start or loc.block_end != first_loc.?.block_end) {
            return null;
        }
    }

    const first = first_loc.?;
    if (first.block_start > first.block_end or first.block_end > reader.data.len) return null;

    try sink.appendSlice(reader.data[first.block_start..first.block_end]);
    const block_end_offset: u64 = @intCast(sink.len() - data_start);
    try sink.writeAt(block_offsets_start + @as(usize, out_block_idx) * 8, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, block_end_offset))));

    i = 0;
    while (i < stored_fields_block_doc_target) : (i += 1) {
        const loc = reader.v4StoredDocLocation(@intCast(reader.stored_offset + 1 + 4), start_doc_id + i) orelse return null;
        const entry_pos = doc_table_start + @as(usize, out_doc_id + i) * stored_fields_v4_doc_entry_size;
        try sink.writeAt(entry_pos + 12, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, out_block_idx))));
        try sink.writeAt(entry_pos + 16, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(loc.doc_offset))))));
        try sink.writeAt(entry_pos + 20, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, loc.raw_len))));
    }

    return @intCast(stored_fields_block_doc_target);
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
                    .f64_val => if (try dv.getF64(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .f64_val = value });
                    },
                    .geo_point => if (try dv.getGeoPoint(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .geo_point = value });
                    },
                    .bool_val => if (try dv.getBool(doc_id)) |value| {
                        try writer.?.add(merged_doc_id, .{ .bool_val = value });
                    },
                    .bytes_val => return error.UnsupportedTypedDocValues,
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
