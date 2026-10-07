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
//!   [stored fields data]
//!   [field sections...]
//!   [v5 page checksums: one BE CRC32 per 64 KiB of preceding data]
//!   [v5 directory descriptor, then section index]
//!   [footer]
//!
//! The 20-byte directory descriptor stores offset (u64 BE), length (u64 BE),
//! and CRC32 (u32 BE). The footer metadata checksum authenticates it along
//! with the section index. Native v5 readers verify touched immutable pages;
//! v4 readers retain whole-region checksums. Contiguous readers accept both.
//! New artifacts require a v5-capable reader; compaction rewrites old artifacts.
//!
//! The section-based layout was originally inspired by Bleve's zapx.

const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const Allocator = std.mem.Allocator;
const byte_copy = @import("common/byte_copy.zig");
const platform_time = @import("antfly_platform").time;
const inverted = @import("section/inverted.zig");
const typed_dv = @import("section/typed_doc_values.zig");
const snappy = @import("encoding/snappy.zig");
const roaring = @import("encoding/roaring.zig");

// ============================================================================
// Constants
// ============================================================================

const magic: [4]u8 = "AFSM".*; // AntFly SegMent
const segment_version: u32 = 5; // v5: authenticated page directory; v4 remains readable.
const integrity = @import("segment_integrity.zig");
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
///   [storedLength: u64 BE]      8
///   [storedMetadataLength: u64] 8
///   [sectionsIndexOffset: u64 BE] 8
///   [storedMetadataCRC32: u32]  4
///   [version: u32 BE]           4
///   [metadataCRC32: u32 BE]     4  (section index plus footer metadata)
///   [magic: 4 bytes]            4
const footer_size: usize = 8 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4; // 56 bytes
const integrity_unverified: u8 = 0;
const integrity_valid: u8 = 1;
const integrity_invalid: u8 = 2;

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
    inverted_term_count: u64 = 0,
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
            if (s.owns_id) self.alloc.free(@constCast(s.id));
            if (s.owns_data) self.alloc.free(@constCast(s.data));
        }
        self.stored_fields.deinit(self.alloc);
        self.compression_bytes.deinit(self.alloc);
    }

    /// Add a field to the segment.
    pub fn addField(self: *SegmentWriter, name: []const u8) !u16 {
        const idx: u16 = @intCast(self.fields.items.len);
        const owned_name = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(owned_name);
        try self.fields.append(self.alloc, .{
            .name = owned_name,
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

    /// Borrow both identity and body from an immutable input batch. The batch
    /// must remain alive through writeToSink; neither slice is retained after it.
    pub fn addStoredDocFromInput(self: *SegmentWriter, doc_id: []const u8, data: []const u8) !void {
        if (self.doc_count != self.stored_fields.items.len) return error.InvalidSegment;
        try self.stored_fields.append(self.alloc, .{
            .id = doc_id,
            .data = data,
            .is_compressed = false,
            .owns_data = false,
            .owns_id = false,
        });
        self.doc_count = try std.math.add(u32, self.doc_count, 1);
    }

    /// Register a producer whose context stays alive until writeToSink returns.
    /// It runs once, directly against the tracked private sink. A producer error
    /// invalidates this write; the publication owner must abort the private sink.
    pub fn addSectionBuilder(self: *SegmentWriter, field_idx: u16, section_type: SectionType, builder: SectionBuilder) !void {
        try self.fields.items[field_idx].sections.append(self.alloc, .{
            .section_type = section_type,
            .data = &.{},
            .builder = builder,
        });
    }

    pub const SectionBuilder = struct {
        context: *anyopaque,
        write: *const fn (Allocator, *anyopaque, *SegmentSink) anyerror!void,
    };

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
    ///   [footer (56 bytes, BE)]
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
    pub fn writeToSink(self: *SegmentWriter, output: *SegmentSink) !void {
        var tracked = PageChecksumSink.init(self.alloc, output);
        defer tracked.deinit();
        var sink = tracked.sink();
        try self.writeToSinkTracked(&sink);
    }
    fn writeToSinkTracked(self: *SegmentWriter, sink: *SegmentSink) !void {
        const stored_offset: u64 = @intCast(sink.len());
        const stored_metadata_length = try self.writeStoredFieldsToSink(sink);
        const stored_length: u64 = @intCast(sink.len() - @as(usize, @intCast(stored_offset)));
        const stored_metadata_crc = try sink.crc32Range(@intCast(stored_offset), @intCast(stored_metadata_length));

        for (self.fields.items) |*field| {
            for (field.sections.items) |*section| {
                section.offset = sink.len();
                if (section.builder) |builder| {
                    sink.beginSection();
                    // Consume before calling: a partially failed producer must
                    // never be retried into an already mutated private sink.
                    section.builder = null;
                    try builder.write(self.alloc, builder.context, sink);
                    section.length = sink.len() - section.offset;
                    section.checksum = try sink.crc32Range(section.offset, section.length);
                } else {
                    try sink.appendSlice(section.data);
                    section.length = section.data.len;
                    section.checksum = Crc32.hash(section.data);
                    self.alloc.free(section.data);
                    section.data = &.{};
                }
            }
        }

        const directory = try writePageDirectory(sink);
        const sections_index_offset: u64 = @intCast(sink.len());
        try writePageDirectoryDescriptor(sink, directory);
        try self.writeSectionIndexToSink(sink);

        try sinkAppendU64BE(sink, @intCast(self.doc_count));
        try sinkAppendU64BE(sink, stored_offset);
        try sinkAppendU64BE(sink, stored_length);
        try sinkAppendU64BE(sink, stored_metadata_length);
        try sinkAppendU64BE(sink, sections_index_offset);
        try sinkAppendU32BE(sink, stored_metadata_crc);
        try writeSegmentVersionChecksumAndMagic(sink, @intCast(sections_index_offset));
    }

    fn estimatedBuildSize(self: *const SegmentWriter) usize {
        if (self.doc_count > 0 and self.stored_fields.items.len == 0) {
            var total: usize = 1 + 4;
            for (self.fields.items) |field| {
                total +|= 2 + field.name.len + 2;
                for (field.sections.items) |section| {
                    total +|= section.data.len;
                    total +|= 2 + 8 + 8 + 4;
                }
            }
            return total +| footer_size;
        }
        var total: usize = 1 + 4 + 4 + 4 + 8 + self.stored_fields.items.len * stored_fields_v4_doc_entry_size;
        total +|= @as(usize, if (self.stored_fields.items.len == 0) 0 else (self.stored_fields.items.len - 1) / stored_fields_block_doc_target + 1) * 12;
        for (self.stored_fields.items) |doc| {
            total +|= doc.id.len;
            total +|= 4 + doc.data.len;
        }

        for (self.fields.items) |field| {
            total +|= 2 + field.name.len + 2;
            for (field.sections.items) |section| {
                total +|= section.data.len;
                total +|= 2 + 8 + 8 + 4;
            }
        }
        total +|= footer_size;
        return total;
    }

    fn writeStoredFieldsToSink(self: *SegmentWriter, sink: *SegmentSink) !u64 {
        const stored_start = sink.len();
        if (self.doc_count > 0 and self.stored_fields.items.len == 0) {
            try sink.appendByte(stored_fields_version_omitted);
            try sinkAppendU32LE(sink, self.doc_count);
            self.last_stored_compress_ns = 0;
            self.last_stored_raw_bytes = 0;
            self.last_stored_compressed_bytes = 0;
            return @intCast(sink.len() - stored_start);
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
        const block_checksums_start = sink.len();
        try sink.appendNTimes(0, @as(usize, num_blocks) * 4);

        const id_bytes_start = sink.len();
        for (self.stored_fields.items, 0..) |*doc, i| {
            const id_offset: u64 = @intCast(sink.len() - id_bytes_start);
            try sink.appendSlice(doc.id);
            const entry_pos = doc_table_start + i * stored_fields_v4_doc_entry_size;
            try sink.writeAt(entry_pos, &@as([8]u8, @bitCast(@as(u64, id_offset))));
            try sink.writeAt(entry_pos + 8, &@as([4]u8, @bitCast(@as(u32, @as(u32, @intCast(doc.id.len))))));
        }
        const id_bytes_len: u64 = @intCast(sink.len() - id_bytes_start);
        try sink.writeAt(id_bytes_len_pos, &@as([8]u8, @bitCast(@as(u64, id_bytes_len))));

        const metadata_length: u64 = @intCast(sink.len() - stored_start);
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
                try sink.writeAt(entry_pos + 12, &@as([4]u8, @bitCast(@as(u32, @as(u32, @intCast(block_idx))))));
                try sink.writeAt(entry_pos + 16, &@as([4]u8, @bitCast(@as(u32, doc_offset))));
                try sink.writeAt(entry_pos + 20, &@as([4]u8, @bitCast(@as(u32, @as(u32, @intCast(raw_data.len))))));
            }

            const encode_start = platform_time.monotonicNs();
            const compressed = try snappy.encodeInto(self.alloc, &self.compression_bytes, chunk.items);
            self.last_stored_compress_ns +|= platform_time.monotonicNs() - encode_start;
            self.last_stored_compressed_bytes +|= compressed.len;
            try sink.appendSlice(compressed);
            try sink.writeAt(
                block_checksums_start + @as(usize, block_idx) * 4,
                &@as([4]u8, @bitCast(@as(u32, Crc32.hash(compressed)))),
            );

            const block_end_offset: u64 = @intCast(sink.len() - data_start);
            try sink.writeAt(block_offsets_start + @as(usize, block_idx) * 8, &@as([8]u8, @bitCast(@as(u64, block_end_offset))));
            if (chunk.capacity > 512 * 1024) {
                chunk.deinit(self.alloc);
                chunk = .empty;
            }
            if (self.compression_bytes.capacity > 512 * 1024) {
                self.compression_bytes.deinit(self.alloc);
                self.compression_bytes = .empty;
            }
        }
        return metadata_length;
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
        //       [CRC32: u32 BE]
        try appendU16BE(self.alloc, out, @intCast(self.fields.items.len));
        for (self.fields.items) |*field| {
            try appendU16BE(self.alloc, out, @intCast(field.name.len));
            try out.appendSlice(self.alloc, field.name);
            try appendU16BE(self.alloc, out, @intCast(field.sections.items.len));

            for (field.sections.items) |*section| {
                try appendU16BE(self.alloc, out, @backingInt(section.section_type));
                try appendU64BE(self.alloc, out, @intCast(section.offset));
                try appendU64BE(self.alloc, out, @intCast(section.length));
                try appendU32BE(self.alloc, out, section.checksum);
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
                try sinkAppendU16BE(sink, @backingInt(section.section_type));
                try sinkAppendU64BE(sink, @intCast(section.offset));
                try sinkAppendU64BE(sink, @intCast(section.length));
                try sinkAppendU32BE(sink, section.checksum);
            }
        }
    }

    const StoredDoc = struct {
        id: []const u8,
        owns_id: bool = true,
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
        checksum: u32 = 0,
        builder: ?SectionBuilder = null,

        pub fn deinit(self: *SectionData, alloc: Allocator) void {
            alloc.free(self.data);
        }
    };

    const FieldBuilder = struct {
        name: []u8,
        sections: std.ArrayListUnmanaged(SectionData),

        pub fn deinit(self: *FieldBuilder, alloc: Allocator) void {
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
pub const PostingsLoader = inverted.PostingsLoader;
pub const SegmentReader = struct {
    postings_loader: ?PostingsLoader = null,
    alloc: Allocator,
    data: []const u8,
    stored_offset: u64,
    stored_length: u64 = 0,
    stored_metadata_length: u64 = 0,
    stored_metadata_checksum: u32 = 0,
    stored_block_validations: ?[]std.atomic.Value(u8) = null,
    index_offset: u64,
    doc_count: u32,
    num_fields: u16,
    fields: []FieldInfo,

    native: ?*Native = null,

    const Native = struct {
        range: RangeSegmentReader,
        borrowed_navigation: bool = false,
        stored_metadata: [21]u8 = @splat(0),
        identity_mutex: std.atomic.Mutex = .unlocked,
        metadata_cache: ?@import("segment_source.zig").ConcurrentBlockCache = null,
        identity_pages: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
        spanning_ids: std.AutoHashMapUnmanaged(u32, []u8) = .empty,
        identity_bytes: usize = 0,

        // Only the legacy stable-borrow API retains identity pages. Query and
        // merge decode scopes use their own bounded identity scratch instead.
        fn identity(self: *Native, allocator: Allocator, doc: u32, offset: u64, length: usize) ![]const u8 {
            if (length == 0) return &.{};
            const sync = @import("antfly_platform").sync;
            sync.lockYielding(&self.identity_mutex);
            defer self.identity_mutex.unlock();
            const page_size = 64 * 1024;
            const first = offset / page_size * page_size;
            const within: usize = @intCast(offset - first);
            if (length > page_size - within) {
                if (self.spanning_ids.get(doc)) |bytes| return bytes;
                const bytes = try allocator.alloc(u8, length);
                errdefer allocator.free(bytes);
                try self.range.source.readInto(offset, bytes);
                try self.spanning_ids.put(allocator, doc, bytes);
                self.identity_bytes += bytes.len;
                return bytes;
            }
            const bytes = self.identity_pages.get(first) orelse blk: {
                const bytes = try allocator.alloc(u8, @intCast(@min(page_size, self.range.source.len() - first)));
                errdefer allocator.free(bytes);
                try self.range.source.readInto(first, bytes);
                try self.identity_pages.put(allocator, first, bytes);
                self.identity_bytes += bytes.len;
                break :blk bytes;
            };
            return bytes[within..][0..length];
        }
    };

    pub const InvertedFieldStats = struct { doc_count: u32, total_field_len: u64 };
    pub const FieldInfo = struct {
        inverted_stats: ?InvertedFieldStats = null,
        name: []const u8,
        sections: []SectionInfo,
    };

    pub const SectionInfo = struct {
        section_type: SectionType,
        offset: u64,
        length: u64,
        cached_navigation: ?[]u8 = null,
        checksum: u32 = 0,
        validation: std.atomic.Value(u8) = .init(integrity_valid),
    };

    /// The caller owns the immutable source and keeps it alive until reader deinit.
    pub fn initSource(alloc: Allocator, input_source: SegmentSource) !SegmentReader {
        const native = try alloc.create(Native);
        errdefer alloc.destroy(native);
        native.* = .{ .range = try RangeSegmentReader.init(alloc, input_source, .{ .metadata_limit_bytes = std.math.maxInt(usize) }) };
        native.range.owns_source = false;
        errdefer native.range.deinit();
        // Stable borrowed IDs require identity navigation to remain resident.
        // Charge its actual size to the publication allocator/resource manager,
        // rather than making a fixed scratch cap a format-compatibility limit.
        // Bodies and field payloads remain native ranges.
        try input_source.readInto(native.range.stored_offset, native.stored_metadata[0..@intCast(@min(native.stored_metadata.len, native.range.stored_metadata_length))]);
        native.metadata_cache = try @import("segment_source.zig").ConcurrentBlockCache.init(alloc, native.range.source, 256 * 1024);
        errdefer native.metadata_cache.?.deinit();
        const fields = try alloc.alloc(FieldInfo, native.range.fields.len);
        errdefer alloc.free(fields);
        var initialized: usize = 0;
        errdefer for (fields[0..initialized]) |field| {
            for (field.sections) |section| if (section.cached_navigation) |bytes| alloc.free(bytes);
        };
        for (native.range.fields, fields) |field, *out| {
            out.* = .{ .name = field.name, .sections = field.sections };
            initialized += 1;
            for (out.sections) |*section| {
                if (section.section_type == .inverted_text and section.length != 0) {
                    const view = try @import("segment_source.zig").View.init(native.range.source, section.offset, section.length);
                    const stats = try inverted.RangeInvertedIndexReader.init(alloc, view, 1024 * 1024);
                    out.inverted_stats = .{ .doc_count = stats.doc_count, .total_field_len = stats.total_field_len };
                }
                switch (section.section_type) {
                    .inverted_text, .typed_doc_values, .doc_ordinals, .vector, .columnar_stored => continue,
                    else => {},
                }
                const bytes = try alloc.alloc(u8, @intCast(section.length));
                errdefer alloc.free(bytes);
                var checksum: [8192]u8 = undefined;
                _ = try native.range.readSectionInto(field.name, section.section_type, 0, bytes, &checksum);
                section.cached_navigation = bytes;
            }
        }
        const validations = try alloc.alloc(std.atomic.Value(u8), native.range.num_blocks);
        for (validations) |*validation| validation.* = .init(integrity_unverified);
        var index_bytes: [8]u8 = undefined;
        errdefer alloc.free(validations);
        try input_source.readInto(input_source.len() - 24, &index_bytes);
        return .{ .alloc = alloc, .data = &.{}, .native = native, .stored_offset = native.range.stored_offset, .stored_length = native.range.stored_length, .stored_metadata_length = native.range.stored_metadata_length, .stored_block_validations = validations, .index_offset = std.mem.readInt(u64, &index_bytes, .big), .doc_count = native.range.doc_count, .num_fields = @intCast(fields.len), .fields = fields };
    }

    /// The enclosing snapshot pins this reader's physical segment. Reuse its
    /// admitted navigation without rereading metadata under every query. Only
    /// query-local cache state and the capability-bound source are allocated.
    pub fn bindSource(self: *const SegmentReader, alloc: Allocator, input_source: SegmentSource) !SegmentReader {
        const base = self.native orelse return error.InvalidSegment;
        if (input_source.len() != base.range.source.len()) return error.InvalidSegment;
        const native = try alloc.create(Native);
        errdefer alloc.destroy(native);
        native.* = .{ .range = base.range, .borrowed_navigation = true, .stored_metadata = base.stored_metadata };
        native.range.alloc = alloc;
        native.range.owns_source = false;
        native.range.owns_fields = false;
        native.range.paged_source = null;
        native.range.source = input_source;
        if (base.range.paged_source) |paged| {
            native.range.paged_source = try paged.bind(alloc, input_source);
            native.range.source = native.range.paged_source.?.source();
        }
        errdefer native.range.deinit();
        native.metadata_cache = try @import("segment_source.zig").ConcurrentBlockCache.init(alloc, native.range.source, 256 * 1024);
        var bound = self.*;
        bound.alloc = alloc;
        bound.native = native;
        return bound;
    }

    pub fn nativeNavigationBytes(self: *const SegmentReader) usize {
        const native = self.native orelse return 0;
        @import("antfly_platform").sync.lockYielding(&native.identity_mutex);
        defer native.identity_mutex.unlock();
        var bytes = @sizeOf(Native) + native.identity_bytes + native.metadata_cache.?.retainedBytes() +
            self.fields.len * (@sizeOf(FieldInfo) + @sizeOf(RangeSegmentReader.Field)) +
            self.stored_block_validations.?.len * @sizeOf(std.atomic.Value(u8));
        if (native.range.paged_source) |paged| {
            bytes += @sizeOf(integrity.PagedSource) + paged.validations.len * @sizeOf(std.atomic.Value(u8)) + paged.retainedBytes() + paged.original.retainedBytes();
        } else bytes += native.range.source.retainedBytes();
        for (self.fields) |field| {
            bytes += field.name.len + field.sections.len * @sizeOf(SectionInfo);
            for (field.sections) |section| if (section.cached_navigation) |data| {
                bytes += data.len;
            };
        }
        return bytes;
    }

    pub fn source(self: *const SegmentReader) SegmentSource {
        return if (self.native) |native| native.range.source else .{ .contiguous = self.data };
    }

    fn storedMetadata(self: *const SegmentReader) []const u8 {
        return if (self.native) |native| &native.stored_metadata else self.data[@intCast(self.stored_offset)..][0..@intCast(self.stored_metadata_length)];
    }

    pub fn typedDocValuesScoped(self: *const SegmentReader, allocator: Allocator, field: []const u8) !?typed_dv.TypedDocValuesReader {
        if (self.native) |native| {
            const view = (try native.range.sectionView(field, .typed_doc_values)) orelse return null;
            const scoped = try typed_dv.RangeTypedDocValuesReader.init(allocator, view, std.math.maxInt(usize), std.math.maxInt(usize));
            var reader = scoped.reader;
            reader.owned_offsets = scoped.offsets;
            return reader;
        }
        return try typed_dv.TypedDocValuesReader.init(allocator, (try self.getSection(field, .typed_doc_values)) orelse return null);
    }

    pub fn init(alloc: Allocator, data: []const u8) !SegmentReader {
        if (data.len < footer_size) return error.InvalidSegment;

        // Validate the compact structural envelope before trusting any offsets.
        // Payload checksums are intentionally deferred until first access so
        // mmap-backed segment admission remains O(metadata), not O(file size).
        const end = data.len;
        const footer_start = end - footer_size;
        if (!std.mem.eql(u8, data[end - 4 ..][0..4], &magic)) return error.InvalidMagic;

        const metadata_crc = std.mem.readInt(u32, data[end - 8 ..][0..4], .big);
        const ver = std.mem.readInt(u32, data[end - 12 ..][0..4], .big);
        if (ver != 4 and ver != segment_version) return error.UnsupportedVersion;
        const stored_metadata_crc = std.mem.readInt(u32, data[end - 16 ..][0..4], .big);
        const sections_index_offset = std.mem.readInt(u64, data[end - 24 ..][0..8], .big);
        const stored_metadata_length = std.mem.readInt(u64, data[end - 32 ..][0..8], .big);
        const stored_length = std.mem.readInt(u64, data[end - 40 ..][0..8], .big);
        const stored_offset = std.mem.readInt(u64, data[end - 48 ..][0..8], .big);
        const doc_count_u64 = std.mem.readInt(u64, data[end - 56 ..][0..8], .big);
        const doc_count = std.math.cast(u32, doc_count_u64) orelse return error.InvalidSegment;

        const index_offset = std.math.cast(usize, sections_index_offset) orelse return error.InvalidSegment;
        const stored_start = std.math.cast(usize, stored_offset) orelse return error.InvalidSegment;
        const stored_len = std.math.cast(usize, stored_length) orelse return error.InvalidSegment;
        const stored_metadata_len = std.math.cast(usize, stored_metadata_length) orelse return error.InvalidSegment;
        if (index_offset > footer_start) return error.InvalidSegment;
        if (stored_start > index_offset or stored_len > index_offset - stored_start) return error.InvalidSegment;
        if (stored_metadata_len > stored_len) return error.InvalidSegment;
        const expected_metadata_crc = Crc32.hash(data[index_offset .. end - 8]);
        if (metadata_crc != expected_metadata_crc) return error.CrcMismatch;
        if (Crc32.hash(data[stored_start..][0..stored_metadata_len]) != stored_metadata_crc) return error.CrcMismatch;

        if (stored_metadata_len < 5) return error.InvalidSegment;
        const stored_mode = data[stored_start];
        const stored_doc_count = std.mem.readInt(u32, data[stored_start + 1 ..][0..4], .little);
        if (stored_doc_count != doc_count) return error.InvalidSegment;
        var stored_block_count: usize = 0;
        switch (stored_mode) {
            stored_fields_version_omitted => {
                if (stored_metadata_len != 5 or stored_len != 5) return error.InvalidSegment;
            },
            stored_fields_version_block_compressed => {
                if (stored_metadata_len < 21) return error.InvalidSegment;
                stored_block_count = std.mem.readInt(u32, data[stored_start + 5 ..][0..4], .little);
                const block_doc_target = std.mem.readInt(u32, data[stored_start + 9 ..][0..4], .little);
                if (block_doc_target != stored_fields_block_doc_target) return error.InvalidSegment;
                const id_bytes_len = std.math.cast(usize, std.mem.readInt(u64, data[stored_start + 13 ..][0..8], .little)) orelse
                    return error.InvalidSegment;
                const doc_table_len = std.math.mul(usize, doc_count, stored_fields_v4_doc_entry_size) catch
                    return error.InvalidSegment;
                const block_offsets_len = std.math.mul(usize, stored_block_count, 8) catch
                    return error.InvalidSegment;
                const block_checksums_len = std.math.mul(usize, stored_block_count, 4) catch
                    return error.InvalidSegment;
                var expected_stored_metadata_len: usize = 21;
                expected_stored_metadata_len = std.math.add(usize, expected_stored_metadata_len, doc_table_len) catch return error.InvalidSegment;
                expected_stored_metadata_len = std.math.add(usize, expected_stored_metadata_len, block_offsets_len) catch return error.InvalidSegment;
                expected_stored_metadata_len = std.math.add(usize, expected_stored_metadata_len, block_checksums_len) catch return error.InvalidSegment;
                expected_stored_metadata_len = std.math.add(usize, expected_stored_metadata_len, id_bytes_len) catch return error.InvalidSegment;
                if (expected_stored_metadata_len != stored_metadata_len) return error.InvalidSegment;
                if ((doc_count == 0) != (stored_block_count == 0)) return error.InvalidSegment;

                const block_offsets_start = stored_start + 21 + doc_table_len;
                const stored_data_len = stored_len - stored_metadata_len;
                var previous_end: u64 = 0;
                for (0..stored_block_count) |block_idx| {
                    const block_end = std.mem.readInt(u64, data[block_offsets_start + block_idx * 8 ..][0..8], .little);
                    if (block_end <= previous_end or block_end > stored_data_len) return error.InvalidSegment;
                    previous_end = block_end;
                }
                if (previous_end != stored_data_len) return error.InvalidSegment;
            },
            else => return error.InvalidSegment,
        }

        // Parse sections index (big-endian)
        var pos = index_offset;
        var payload_end = index_offset;
        if (ver == segment_version) {
            const directory = try integrity.Directory.read(.{ .contiguous = data }, index_offset);
            payload_end = @intCast(directory.offset);
            if (payload_end < stored_start + stored_len) return error.InvalidSegment;
            if (footer_start - pos < integrity.descriptor_size) return error.InvalidSegment;
            pos += integrity.descriptor_size;
        }
        if (footer_start - pos < 2) return error.InvalidSegment;
        const num_fields = std.mem.readInt(u16, data[pos..][0..2], .big);
        pos += 2;

        var fields = try alloc.alloc(FieldInfo, num_fields);
        var fields_initialized: usize = 0;
        errdefer {
            for (fields[0..fields_initialized]) |field| alloc.free(field.sections);
            alloc.free(fields);
        }

        for (0..num_fields) |fi| {
            if (footer_start - pos < 2) return error.InvalidSegment;
            const name_len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            if (name_len > footer_start - pos) return error.InvalidSegment;
            const name = data[pos..][0..name_len];
            pos += name_len;
            for (fields[0..fi]) |field| {
                if (std.mem.eql(u8, field.name, name)) return error.InvalidSegment;
            }
            if (footer_start - pos < 2) return error.InvalidSegment;
            const num_sections = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;

            const sections = try alloc.alloc(SectionInfo, num_sections);
            fields[fi] = .{ .name = name, .sections = sections };
            fields_initialized += 1;
            for (0..num_sections) |si| {
                if (footer_start - pos < 22) return error.InvalidSegment;
                const st = std.mem.readInt(u16, data[pos..][0..2], .big);
                pos += 2;
                const offset = std.mem.readInt(u64, data[pos..][0..8], .big);
                pos += 8;
                const length = std.mem.readInt(u64, data[pos..][0..8], .big);
                pos += 8;
                const checksum = std.mem.readInt(u32, data[pos..][0..4], .big);
                pos += 4;
                const section_type = std.enums.fromInt(SectionType, st) orelse return error.InvalidSegment;
                for (sections[0..si]) |section| {
                    if (section.section_type == section_type) return error.InvalidSegment;
                }
                const section_offset = std.math.cast(usize, offset) orelse return error.InvalidSegment;
                const section_len = std.math.cast(usize, length) orelse return error.InvalidSegment;
                if (section_offset < stored_start + stored_len or
                    section_offset > payload_end or
                    section_len > payload_end - section_offset)
                {
                    return error.InvalidSegment;
                }
                sections[si] = .{
                    .section_type = section_type,
                    .offset = offset,
                    .length = length,
                    .checksum = checksum,
                    .validation = .init(integrity_unverified),
                };
            }
        }
        if (pos != footer_start) return error.InvalidSegment;

        const stored_block_validations = try alloc.alloc(std.atomic.Value(u8), stored_block_count);
        errdefer alloc.free(stored_block_validations);
        for (stored_block_validations) |*validation| validation.* = .init(integrity_unverified);

        return .{
            .alloc = alloc,
            .data = data,
            .stored_offset = stored_offset,
            .stored_length = stored_length,
            .stored_metadata_length = stored_metadata_length,
            .stored_metadata_checksum = stored_metadata_crc,
            .stored_block_validations = stored_block_validations,
            .index_offset = sections_index_offset,
            .doc_count = doc_count,
            .num_fields = num_fields,
            .fields = fields,
        };
    }

    pub fn deinit(self: *SegmentReader) void {
        if (self.native) |native| {
            if (!native.borrowed_navigation) {
                for (self.fields) |field| for (field.sections) |section| {
                    if (section.cached_navigation) |bytes| self.alloc.free(bytes);
                };
                self.alloc.free(self.fields);
                self.alloc.free(self.stored_block_validations.?);
            }
            var pages = native.identity_pages.valueIterator();
            while (pages.next()) |bytes| self.alloc.free(bytes.*);
            native.identity_pages.deinit(self.alloc);
            var ids = native.spanning_ids.valueIterator();
            while (ids.next()) |bytes| self.alloc.free(bytes.*);
            native.spanning_ids.deinit(self.alloc);
            native.metadata_cache.?.deinit();
            native.range.deinit();
            self.alloc.destroy(native);
            return;
        }
        for (self.fields) |*f| self.alloc.free(f.sections);
        self.alloc.free(self.fields);
        if (self.stored_block_validations) |validations| self.alloc.free(validations);
    }

    /// Get section data for a field by name and type.
    pub fn getSection(self: *const SegmentReader, field_name: []const u8, section_type: SectionType) !?[]const u8 {
        if (self.native) |native| {
            const entry = native.range.findSection(field_name, section_type) orelse return null;
            if (entry.length == 0) return null;
            return entry.cached_navigation orelse error.NativeSectionRequiresScope;
        }
        for (self.fields) |*field| {
            if (std.mem.eql(u8, field.name, field_name)) {
                for (field.sections) |*section| {
                    if (section.section_type == section_type) {
                        if (section.length == 0) return null;
                        const offset: usize = @intCast(section.offset);
                        const length: usize = @intCast(section.length);
                        const bytes = self.data[offset..][0..length];
                        // Seekable remote payloads authenticate each immutable
                        // posting block separately; metadata is authenticated
                        // before admission. A whole-section CRC would read all
                        // postings and defeat selective access.
                        if (section_type == .inverted_text and self.postings_loader != null) return bytes;
                        const validation = @constCast(&section.validation);
                        switch (validation.load(.acquire)) {
                            integrity_valid => return bytes,
                            integrity_invalid => return error.CrcMismatch,
                            else => {},
                        }
                        if (Crc32.hash(bytes) != section.checksum) {
                            validation.store(integrity_invalid, .release);
                            return error.CrcMismatch;
                        }
                        validation.store(integrity_valid, .release);
                        return bytes;
                    }
                }
            }
        }
        return null;
    }

    fn validateStoredBlock(self: *const SegmentReader, block_idx: u32, bytes: []const u8) !u32 {
        const validations = self.stored_block_validations orelse return error.InvalidSegment;
        if (block_idx >= validations.len) return error.InvalidSegment;
        const validation = @constCast(&validations[block_idx]);
        const block_checksums_start = 21 +
            @as(usize, self.doc_count) * stored_fields_v4_doc_entry_size +
            validations.len * 8;
        const expected = std.mem.readInt(u32, self.storedMetadata()[block_checksums_start + @as(usize, block_idx) * 4 ..][0..4], .little);
        switch (validation.load(.acquire)) {
            integrity_valid => return expected,
            integrity_invalid => return error.CrcMismatch,
            else => {},
        }
        if (Crc32.hash(bytes) != expected) {
            validation.store(integrity_invalid, .release);
            return error.CrcMismatch;
        }
        validation.store(integrity_valid, .release);
        return expected;
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
                        if (self.invertedLayoutStats(field.name, section, detailed_inverted)) |inverted_layout| {
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
                                stats.inverted_term_count +|= inverted_layout.term_count;
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
        const footer_start = self.source().len() - footer_size;
        if (footer_start >= self.index_offset) stats.section_index_bytes = @intCast(footer_start - @as(usize, @intCast(self.index_offset)));
        return stats;
    }

    fn invertedLayoutStats(self: *const SegmentReader, field: []const u8, section: *const SectionInfo, detailed: bool) !inverted.InvertedIndexReader.LayoutStats {
        if (self.native != null) {
            if (detailed) {
                var scoped = (try self.invertedIndexScoped(self.alloc, field)) orelse return .{};
                defer scoped.deinit();
                return scoped.detailedLayoutStats();
            }
            const view = try @import("segment_source.zig").View.init(self.source(), section.offset, section.length);
            const reader = try inverted.RangeInvertedIndexReader.init(self.alloc, view, 1024 * 1024);
            return reader.layoutStats();
        }
        if (section.offset > self.data.len or section.length > self.data.len - section.offset) return .{};
        const reader = try inverted.InvertedIndexReader.init(self.alloc, self.data[@intCast(section.offset)..][0..@intCast(section.length)]);
        return if (detailed) reader.detailedLayoutStats() catch reader.layoutStats() else reader.layoutStats();
    }

    /// Get an inverted index reader for a field.
    pub fn invertedIndex(self: *const SegmentReader, field_name: []const u8) !?inverted.InvertedIndexReader {
        const section_data = (try self.getSection(field_name, .inverted_text)) orelse return null;
        var reader = try inverted.InvertedIndexReader.init(self.alloc, section_data);
        if (self.postings_loader) |loader| {
            reader.postings_loader = loader;
            reader.postings_loader.?.base = @intFromPtr(section_data.ptr) - @intFromPtr(self.data.ptr);
        }
        return reader;
    }

    /// Collection statistics preserve lazy payload checksum validation. Native
    /// admission reads only field descriptors, as the mapped path does.
    pub fn invertedFieldStats(self: *const SegmentReader, field: []const u8) !?InvertedFieldStats {
        if (self.native != null) {
            // Immutable admission metadata survives cache capability sealing.
            for (self.fields) |info| if (std.mem.eql(u8, info.name, field)) return info.inverted_stats;
            return null;
        }
        for (self.fields) |info| if (std.mem.eql(u8, info.name, field)) {
            for (info.sections) |section| if (section.section_type == .inverted_text) {
                if (section.offset > self.data.len or section.length > self.data.len - section.offset) return null;
                const reader = inverted.InvertedIndexReader.init(self.alloc, self.data[@intCast(section.offset)..][0..@intCast(section.length)]) catch return null;
                return .{ .doc_count = reader.doc_count, .total_field_len = reader.total_field_len };
            };
        };
        return null;
    }

    pub fn sectionView(self: *const SegmentReader, field: []const u8, kind: SectionType) !?@import("segment_source.zig").View {
        if (self.native) |native| return native.range.sectionView(field, kind);
        const bytes = (try self.getSection(field, kind)) orelse return null;
        return try @import("segment_source.zig").View.init(.{ .contiguous = bytes }, 0, bytes.len);
    }

    /// Raw rows borrow this reader/source; owned quantized state is scoped.
    /// The reader and source must outlive the returned index.
    pub fn vectorIndexScoped(self: *const SegmentReader, alloc: Allocator, field: []const u8) !?@import("section/vector_section.zig").RaBitQIndex {
        const view = (try self.sectionView(field, .vector)) orelse return null;
        return try @import("section/vector_section.zig").readRaBitQIndexRanges(alloc, view);
    }

    /// Admission summary for streaming typed compaction: directories and peak
    /// decoded chunks, including oversized values in historical artifacts.
    /// New streams authenticate an exact decoded-size summary in navigation.
    /// Historical streams scan only length prefixes; SegmentShared caches the
    /// result so subsequent admissions never revisit payload pages.
    pub fn typedMergeWorkingSetBytes(self: *const SegmentReader) !u64 {
        var total: u64 = 0;
        for (self.fields) |field| {
            const view = (try self.sectionView(field.name, .typed_doc_values)) orelse continue;
            if (view.length < 5) return error.InvalidData;
            var header: [5]u8 = undefined;
            try view.readInto(0, &header);
            _ = std.enums.fromInt(typed_dv.ValueType, header[0] & 0x3f) orelse return error.InvalidData;
            const count = std.mem.readInt(u32, header[1..5], .little);
            const navigation = @as(u64, count) * 8;
            var table: u64 = 5;
            var start: u64 = 5 + navigation;
            var limit = view.length;
            var summary: ?u64 = null;
            if (header[0] & 0x40 != 0 and header[0] & 0x80 == 0) return error.InvalidData;
            if (header[0] & 0x80 != 0) {
                const tail: u64 = if (header[0] & 0x40 != 0) 16 else 8;
                if (view.length < 5 + tail) return error.InvalidData;
                if (tail == 16) {
                    var size: [8]u8 = undefined;
                    try view.readInto(view.length - 16, &size);
                    summary = std.mem.readInt(u64, &size, .little);
                }
                var trailer: [8]u8 = undefined;
                try view.readInto(view.length - 8, &trailer);
                table = std.mem.readInt(u64, &trailer, .little);
                if (table < 5 or table > view.length - tail or navigation != view.length - tail - table) return error.InvalidData;
                start = 5;
                limit = table;
            } else if (start > limit) return error.InvalidData;
            var largest: u64 = 0;
            for (0..count) |i| {
                var offset: [8]u8 = undefined;
                try view.readInto(table + @as(u64, i) * 8, &offset);
                const end = std.mem.readInt(u64, &offset, .little);
                if (end <= start or end > limit) return error.InvalidData;
                if (summary == null) {
                    var prefix: [10]u8 = undefined;
                    const length: usize = @intCast(@min(prefix.len, end - start));
                    try view.readInto(start, prefix[0..length]);
                    const decoded = try snappy.decodedLen(prefix[0..length]);
                    if (decoded > try snappy.decodedSizeUpperBound(end - start)) return error.InvalidData;
                    largest = @max(largest, decoded);
                }
                start = end;
            }
            if (start != limit) return error.InvalidData;
            if (summary) |size| {
                if ((count == 0) != (size == 0) or size > try snappy.decodedSizeUpperBound(limit - 5)) return error.InvalidData;
                largest = size;
            }
            // Input and output directories coexist. Arena growth/retention
            // can coexist with the active chunk and sorted column cursors.
            total = try std.math.add(u64, total, try std.math.mul(u64, navigation, 2));
            total = try std.math.add(u64, total, try std.math.mul(u64, largest, 3));
        }
        return total;
    }

    /// Chunk payload borrows this reader/source; the returned reader owns its
    /// navigation directory and must be deinitialized before this reader.
    pub fn storedColumnScoped(self: *const SegmentReader, alloc: Allocator, field: []const u8) !?@import("section/doc_values.zig").DocValuesReader {
        const view = (try self.sectionView(field, .columnar_stored)) orelse return null;
        return try @import("section/doc_values.zig").DocValuesReader.initRanges(alloc, view);
    }

    pub fn invertedIndexScoped(self: *const SegmentReader, allocator: Allocator, field_name: []const u8) !?inverted.ScopedInvertedIndexReader {
        if (self.native) |native| return native.range.invertedIndexScoped(allocator, field_name, .{});
        const data = (try self.getSection(field_name, .inverted_text)) orelse return null;
        var scoped = try inverted.ScopedInvertedIndexReader.initContiguous(allocator, data);
        if (self.postings_loader) |loader| {
            scoped.contiguous.?.postings_loader = loader;
            scoped.contiguous.?.postings_loader.?.base = @intFromPtr(data.ptr) - @intFromPtr(self.data.ptr);
        }
        return scoped;
    }

    pub const StoredDocRef = struct { id: []const u8, data: []const u8 };

    /// Read a stored document identity by index. Source bodies live in
    /// independently checksummed compressed blocks and are returned by
    /// `storedDocDecompressed`.
    pub fn storedDoc(self: *const SegmentReader, doc_idx: u32) !?StoredDocRef {
        var pos: usize = 0;
        const metadata = self.storedMetadata();
        const ver = metadata[pos];
        pos += 1;
        const num_docs = std.mem.readInt(u32, metadata[pos..][0..4], .little);
        pos += 4;
        if (doc_idx >= num_docs) return null;

        if (ver == stored_fields_version_omitted) return null;

        if (ver == stored_fields_version_block_compressed) {
            const loc = (try self.v4StoredDocLocation(doc_idx)) orelse return null;
            return .{ .id = loc.id, .data = &.{} };
        }
        return error.InvalidSegment;
    }

    /// Heap IDs borrow the reader; native IDs belong to the caller's scope.
    /// Neither path admits stable segment-owned identity pages.
    pub fn storedIdScoped(self: *const SegmentReader, alloc: Allocator, doc: u32) !?[]const u8 {
        if (self.native != null) return self.storedIdAlloc(alloc, doc);
        const stored = (try self.storedDoc(doc)) orelse return null;
        return stored.id;
    }

    /// Identity owned by the caller; never admits stable borrowed-ID pages.
    pub fn storedIdAlloc(self: *const SegmentReader, alloc: Allocator, doc: u32) !?[]u8 {
        if (self.native != null) {
            return self.nativeStoredIdentity(alloc, doc);
        }
        const stored = (try self.storedDoc(doc)) orelse return null;
        return try alloc.dupe(u8, stored.id);
    }

    /// Read and decompress stored data into the caller allocator. The identity
    /// remains borrowed from this reader.
    pub fn storedDocDecompressed(self: *const SegmentReader, alloc: Allocator, doc_idx: u32) !?struct { id: []const u8, data: []u8 } {
        const raw = (try self.storedDoc(doc_idx)) orelse return null;
        const ver = self.storedMetadata()[0];
        if (ver == stored_fields_version_block_compressed) {
            const loc = (try self.v4StoredDocLocation(doc_idx)) orelse return null;
            const block = try self.decodeStoredBlock(alloc, loc);
            defer alloc.free(block);
            if (loc.doc_offset > block.len or block.len - loc.doc_offset < 4) return error.InvalidSegment;
            const data_len = std.mem.readInt(u32, block[loc.doc_offset..][0..4], .little);
            const data_start = loc.doc_offset + 4;
            if (data_start > block.len or data_len > block.len - data_start) return error.InvalidSegment;
            if (data_len != loc.raw_len) return error.InvalidSegment;
            return .{ .id = raw.id, .data = try alloc.dupe(u8, block[data_start..][0..data_len]) };
        }
        return error.InvalidSegment;
    }

    fn decodeStoredBlock(self: *const SegmentReader, allocator: Allocator, location: V4StoredDocLocation) ![]u8 {
        if (self.native != null) {
            const validations = self.stored_block_validations orelse return error.InvalidSegment;
            if (location.block_idx >= validations.len) return error.InvalidSegment;
            const validation = @constCast(&validations[location.block_idx]);
            switch (validation.load(.acquire)) {
                integrity_invalid => return error.CrcMismatch,
                integrity_valid => {},
                else => {
                    const expected = try self.storedBlockChecksum(location.block_idx);
                    var scratch: [8192]u8 = undefined;
                    const actual = try self.source().checksum(location.block_start, location.block_end - location.block_start, &scratch);
                    if (actual != expected) {
                        validation.store(integrity_invalid, .release);
                        return error.CrcMismatch;
                    }
                    validation.store(integrity_valid, .release);
                },
            }
            const view = try @import("segment_source.zig").View.init(self.source(), location.block_start, location.block_end - location.block_start);
            // Required decoded output is charged to the caller's allocator.
            // Compressed input uses only the decoder's fixed window, and
            // cursor/cache owners discard oversized working chunks on advance.
            return snappy.decodeFromView(allocator, view, std.math.maxInt(usize));
        }
        const compressed = self.data[location.block_start..location.block_end];
        _ = try self.validateStoredBlock(location.block_idx, compressed);
        return snappy.decode(allocator, compressed);
    }

    /// Length is authenticated by the stored metadata checksum at admission.
    /// Merge sizing needs no compressed payload read or decompression.
    pub fn storedDocLength(self: *const SegmentReader, doc: u32) !?u32 {
        if (self.native) |native| {
            if (doc >= self.doc_count or native.range.num_blocks == 0) return null;
            var bytes: [4]u8 = undefined;
            try native.metadata_cache.?.readInto(self.stored_offset + 21 + @as(u64, doc) * stored_fields_v4_doc_entry_size + 20, &bytes);
            return std.mem.readInt(u32, &bytes, .little);
        }
        const location = (try self.v4StoredDocLocation(doc)) orelse return null;
        return location.raw_len;
    }

    /// A caller-owned decode scope. The body is borrowed until a different
    /// block is requested or the scope closes. It retains at most one decoded
    /// block and frees oversized arena capacity before the next decode. All
    /// readers used by this cursor must remain alive until it closes.
    pub const StoredDocCursor = struct {
        scratch: SegmentReadScratch,
        identities: SegmentReadScratch,
        reader: ?*const SegmentReader = null,
        block: ?u32 = null,
        decoded: []const u8 = &.{},
        decode_count: usize = 0,

        pub fn init(allocator: Allocator) StoredDocCursor {
            return .{ .scratch = SegmentReadScratch.init(allocator, 2 * stored_fields_block_raw_target), .identities = SegmentReadScratch.init(allocator, 64 * 1024) };
        }

        pub fn deinit(self: *StoredDocCursor) void {
            self.scratch.deinit();
            self.identities.deinit();
            self.* = undefined;
        }

        pub fn get(self: *StoredDocCursor, reader: *const SegmentReader, doc: u32) !?StoredDocRef {
            self.identities.reset();
            const loc = (if (reader.native != null) try reader.nativeStoredLocation(doc, self.identities.allocator()) else try reader.v4StoredDocLocation(doc)) orelse return null;
            if (self.reader != reader or self.block != loc.block_idx) {
                self.reader = null;
                self.block = null;
                self.decoded = &.{};
                self.scratch.reset();
                const decoded = try reader.decodeStoredBlock(self.scratch.allocator(), loc);
                self.decoded = decoded;
                self.reader = reader;
                self.block = loc.block_idx;
                self.decode_count += 1;
            }
            if (loc.doc_offset > self.decoded.len or self.decoded.len - loc.doc_offset < 4) return error.InvalidSegment;
            const length = std.mem.readInt(u32, self.decoded[loc.doc_offset..][0..4], .little);
            const body_start = loc.doc_offset + 4;
            if (length != loc.raw_len or length > self.decoded.len - body_start) return error.InvalidSegment;
            return .{ .id = loc.id, .data = self.decoded[body_start..][0..length] };
        }
    };

    /// Small LRU for interleaved merge inputs. Cache bytes are charged to the
    /// task allocator. A block larger than the budget is the sole cached block
    /// and is freed before any different block is decoded. Borrowed bodies can
    /// be invalidated by the next get; readers must outlive this scope.
    pub const StoredDocBlockCache = struct {
        const Entry = struct { reader: *const SegmentReader, block: u32, decoded: []u8 };
        allocator: Allocator,
        identities: SegmentReadScratch,
        byte_budget: usize,
        entries: std.ArrayListUnmanaged(Entry) = .empty,
        count: usize = 0,
        live_bytes: usize = 0,
        decode_count: usize = 0,

        pub fn init(allocator: Allocator, byte_budget: usize) StoredDocBlockCache {
            return .{ .allocator = allocator, .byte_budget = byte_budget, .identities = SegmentReadScratch.init(allocator, 64 * 1024) };
        }

        fn evict(self: *StoredDocBlockCache) void {
            self.count -= 1;
            const bytes = self.entries.pop().?.decoded;
            self.live_bytes -= bytes.len;
            self.allocator.free(bytes);
        }

        pub fn deinit(self: *StoredDocBlockCache) void {
            while (self.count != 0) self.evict();
            self.entries.deinit(self.allocator);
            self.identities.deinit();
            self.* = undefined;
        }

        pub fn get(self: *StoredDocBlockCache, reader: *const SegmentReader, doc: u32) !?StoredDocRef {
            self.identities.reset();
            const loc = (if (reader.native != null) try reader.nativeStoredLocation(doc, self.identities.allocator()) else try reader.v4StoredDocLocation(doc)) orelse return null;
            var found: ?usize = null;
            for (self.entries.items[0..self.count], 0..) |entry, i| {
                if (entry.reader == reader and entry.block == loc.block_idx) {
                    found = i;
                    break;
                }
            }
            if (found) |position| {
                const hit = self.entries.items[position];
                std.mem.copyBackwards(Entry, self.entries.items[1 .. position + 1], self.entries.items[0..position]);
                self.entries.items[0] = hit;
            } else {
                var header: [5]u8 = undefined;
                const needed = if (reader.native != null) blk: {
                    const take = @min(header.len, loc.block_end - loc.block_start);
                    try reader.source().readInto(loc.block_start, header[0..take]);
                    break :blk try snappy.decodedLen(header[0..take]);
                } else try snappy.decodedLen(reader.data[loc.block_start..loc.block_end]);
                while (self.count != 0 and (self.count >= @max(1, @min(1024, self.byte_budget / @sizeOf(Entry))) or self.live_bytes > self.byte_budget -| needed or needed > self.byte_budget)) self.evict();
                // Metadata grows lazily with fitting blocks. Cap descriptors
                // independently to bound lookup work even for tiny blocks.
                try self.entries.ensureUnusedCapacity(self.allocator, 1);
                const decoded = try reader.decodeStoredBlock(self.allocator, loc);
                while (self.count != 0 and (self.count >= @max(1, @min(1024, self.byte_budget / @sizeOf(Entry))) or self.live_bytes > self.byte_budget -| decoded.len or decoded.len > self.byte_budget)) self.evict();
                self.entries.appendAssumeCapacity(undefined);
                std.mem.copyBackwards(Entry, self.entries.items[1 .. self.count + 1], self.entries.items[0..self.count]);
                self.entries.items[0] = .{ .reader = reader, .block = loc.block_idx, .decoded = decoded };
                self.count += 1;
                self.live_bytes += decoded.len;
                self.decode_count += 1;
            }
            const decoded = self.entries.items[0].decoded;
            if (loc.doc_offset > decoded.len or decoded.len - loc.doc_offset < 4) return error.InvalidSegment;
            const length = std.mem.readInt(u32, decoded[loc.doc_offset..][0..4], .little);
            const body_start = loc.doc_offset + 4;
            if (length != loc.raw_len or length > decoded.len - body_start) return error.InvalidSegment;
            return .{ .id = loc.id, .data = decoded[body_start..][0..length] };
        }
    };

    pub fn storedDocsAreCompressed(self: *const SegmentReader) !bool {
        const ver = self.storedMetadata()[0];
        return ver == stored_fields_version_block_compressed;
    }

    pub fn storedFieldsOmitted(self: *const SegmentReader) !bool {
        return self.storedMetadata()[0] == stored_fields_version_omitted;
    }

    pub const V4StoredDocLocation = struct {
        id: []const u8,
        id_length: usize,
        block_idx: u32,
        block_start: usize,
        block_end: usize,
        doc_offset: usize,
        raw_len: u32,
    };

    fn nativeStoredIdentity(self: *const SegmentReader, allocator: Allocator, doc: u32) !?[]u8 {
        const native = self.native.?;
        const range = &native.range;
        if (doc >= range.doc_count or range.num_blocks == 0) return null;
        const metadata = native.metadata_cache.?.borrowedSource();
        var entry: [stored_fields_v4_doc_entry_size]u8 = undefined;
        try metadata.readInto(range.stored_offset + 21 + @as(u64, doc) * entry.len, &entry);
        const offset = std.mem.readInt(u64, entry[0..8], .little);
        const length = std.mem.readInt(u32, entry[8..12], .little);
        if (std.mem.readInt(u32, entry[12..16], .little) >= range.num_blocks or offset > range.id_bytes_length or length > range.id_bytes_length - offset) return error.InvalidSegment;
        const ids_start = range.stored_offset + 21 + @as(u64, range.doc_count) * entry.len + @as(u64, range.num_blocks) * 12;
        const id = try allocator.alloc(u8, length);
        errdefer allocator.free(id);
        try metadata.readInto(ids_start + offset, id);
        return id;
    }

    fn nativeStoredLocation(self: *const SegmentReader, doc: u32, identity_allocator: ?Allocator) !?V4StoredDocLocation {
        return self.nativeStoredLocationMode(doc, identity_allocator, true);
    }

    fn storedLocationMetadata(self: *const SegmentReader, doc: u32) !?V4StoredDocLocation {
        if (self.native != null) return self.nativeStoredLocationMode(doc, null, false);
        return self.v4StoredDocLocation(doc);
    }

    fn nativeStoredLocationMode(self: *const SegmentReader, doc: u32, identity_allocator: ?Allocator, read_identity: bool) !?V4StoredDocLocation {
        const native = self.native.?;
        const range = &native.range;
        const metadata_source = native.metadata_cache.?.borrowedSource();
        if (doc >= range.doc_count or range.num_blocks == 0) return null;
        var entry: [stored_fields_v4_doc_entry_size]u8 = undefined;
        try metadata_source.readInto(range.stored_offset + 21 + @as(u64, doc) * entry.len, &entry);
        const id_offset = std.mem.readInt(u64, entry[0..8], .little);
        const id_len = std.mem.readInt(u32, entry[8..12], .little);
        const block = std.mem.readInt(u32, entry[12..16], .little);
        if (block >= range.num_blocks or id_offset > range.id_bytes_length or id_len > range.id_bytes_length - id_offset) return error.InvalidSegment;
        const offsets_start = range.stored_offset + 21 + @as(u64, range.doc_count) * stored_fields_v4_doc_entry_size;
        var offsets: [16]u8 = @splat(0);
        if (block == 0) try metadata_source.readInto(offsets_start, offsets[8..]) else try metadata_source.readInto(offsets_start + @as(u64, block - 1) * 8, &offsets);
        const start = std.mem.readInt(u64, offsets[0..8], .little);
        const end = std.mem.readInt(u64, offsets[8..16], .little);
        if (start >= end or end > range.stored_length - range.stored_metadata_length) return error.InvalidSegment;
        const ids_start = offsets_start + @as(u64, range.num_blocks) * 12;
        const id = if (!read_identity) &.{} else if (identity_allocator) |allocator| blk: {
            const bytes = try allocator.alloc(u8, id_len);
            errdefer allocator.free(bytes);
            try metadata_source.readInto(ids_start + id_offset, bytes);
            break :blk bytes;
        } else try native.identity(self.alloc, doc, ids_start + id_offset, id_len);
        return .{
            .id = id,
            .id_length = id_len,
            .block_idx = block,
            .block_start = @intCast(range.stored_offset + range.stored_metadata_length + start),
            .block_end = @intCast(range.stored_offset + range.stored_metadata_length + end),
            .doc_offset = std.mem.readInt(u32, entry[16..20], .little),
            .raw_len = std.mem.readInt(u32, entry[20..24], .little),
        };
    }

    fn storedBlockChecksum(self: *const SegmentReader, block: u32) !u32 {
        const blocks = self.stored_block_validations.?.len;
        if (block >= blocks) return error.InvalidSegment;
        const offset = 21 + @as(u64, self.doc_count) * stored_fields_v4_doc_entry_size + blocks * 8 + @as(u64, block) * 4;
        if (self.native) |native| {
            var bytes: [4]u8 = undefined;
            try native.metadata_cache.?.readInto(self.stored_offset + offset, &bytes);
            return std.mem.readInt(u32, &bytes, .little);
        }
        return std.mem.readInt(u32, self.storedMetadata()[@intCast(offset)..][0..4], .little);
    }

    fn v4StoredDocLocation(self: *const SegmentReader, doc_idx: u32) !?V4StoredDocLocation {
        if (self.native != null) return self.nativeStoredLocation(doc_idx, null);
        if (doc_idx >= self.doc_count) return null;
        const stored_start: usize = 0;
        const metadata = self.storedMetadata();
        const stored_len = std.math.cast(usize, self.stored_length) orelse
            return error.InvalidSegment;
        const stored_end = std.math.add(usize, stored_start, stored_len) catch
            return error.InvalidSegment;
        if (stored_end > self.source().len() - self.stored_offset or stored_start > stored_end or
            stored_end - stored_start < 21)
        {
            return error.InvalidSegment;
        }

        var pos = stored_start + 5;
        const num_blocks = std.mem.readInt(u32, metadata[pos..][0..4], .little);
        pos += 4;
        _ = std.mem.readInt(u32, metadata[pos..][0..4], .little);
        pos += 4;
        const id_bytes_len = std.math.cast(
            usize,
            std.mem.readInt(u64, metadata[pos..][0..8], .little),
        ) orelse return error.InvalidSegment;
        pos += 8;

        const doc_table_start = pos;
        const doc_table_len = std.math.mul(
            usize,
            self.doc_count,
            stored_fields_v4_doc_entry_size,
        ) catch return error.InvalidSegment;
        const block_offsets_len = std.math.mul(usize, num_blocks, 8) catch
            return error.InvalidSegment;
        const block_checksums_len = std.math.mul(usize, num_blocks, 4) catch
            return error.InvalidSegment;
        const block_offsets_start = std.math.add(usize, doc_table_start, doc_table_len) catch
            return error.InvalidSegment;
        const block_checksums_start = std.math.add(usize, block_offsets_start, block_offsets_len) catch
            return error.InvalidSegment;
        const id_bytes_start = std.math.add(usize, block_checksums_start, block_checksums_len) catch
            return error.InvalidSegment;
        const data_start = std.math.add(usize, id_bytes_start, id_bytes_len) catch
            return error.InvalidSegment;
        if (data_start > stored_end) return error.InvalidSegment;

        const entry_offset = std.math.mul(
            usize,
            doc_idx,
            stored_fields_v4_doc_entry_size,
        ) catch return error.InvalidSegment;
        const entry_pos = std.math.add(usize, doc_table_start, entry_offset) catch
            return error.InvalidSegment;
        if (entry_pos > data_start or stored_fields_v4_doc_entry_size > data_start - entry_pos)
            return error.InvalidSegment;
        const id_offset = std.mem.readInt(u64, metadata[entry_pos..][0..8], .little);
        const id_len = std.mem.readInt(u32, metadata[entry_pos + 8 ..][0..4], .little);
        const block_idx = std.mem.readInt(u32, metadata[entry_pos + 12 ..][0..4], .little);
        const doc_offset = std.mem.readInt(u32, metadata[entry_pos + 16 ..][0..4], .little);
        const raw_len = std.mem.readInt(u32, metadata[entry_pos + 20 ..][0..4], .little);
        if (block_idx >= num_blocks) return error.InvalidSegment;
        const id_offset_usize = std.math.cast(usize, id_offset) orelse
            return error.InvalidSegment;
        const id_start = std.math.add(usize, id_bytes_start, id_offset_usize) catch
            return error.InvalidSegment;
        if (id_start > data_start or id_len > data_start - id_start)
            return error.InvalidSegment;
        const block_end_offset = std.mem.readInt(u64, metadata[block_offsets_start + @as(usize, block_idx) * 8 ..][0..8], .little);
        const block_start_offset: u64 = if (block_idx == 0) 0 else std.mem.readInt(u64, metadata[block_offsets_start + (@as(usize, block_idx) - 1) * 8 ..][0..8], .little);
        const block_start_relative = std.math.cast(usize, block_start_offset) orelse
            return error.InvalidSegment;
        const block_end_relative = std.math.cast(usize, block_end_offset) orelse
            return error.InvalidSegment;
        const block_start = std.math.add(usize, data_start, block_start_relative) catch
            return error.InvalidSegment;
        const block_end = std.math.add(usize, data_start, block_end_relative) catch
            return error.InvalidSegment;
        if (block_start > stored_end or block_end > stored_end or block_start > block_end)
            return error.InvalidSegment;
        return .{
            .id = metadata[id_start..][0..id_len],
            .id_length = id_len,
            .block_idx = block_idx,
            .block_start = block_start + @as(usize, @intCast(self.stored_offset)),
            .block_end = block_end + @as(usize, @intCast(self.stored_offset)),
            .doc_offset = @intCast(doc_offset),
            .raw_len = raw_len,
        };
    }

    pub fn docOrdinal(self: *const SegmentReader, doc_idx: u32) !?u32 {
        if (self.native) |native| {
            const view = (try native.range.sectionView(doc_ordinals_field, .doc_ordinals)) orelse return null;
            if (view.length < 5) return error.InvalidSegment;
            var header: [5]u8 = undefined;
            try native.metadata_cache.?.readInto(view.offset, &header);
            if (header[0] != 1) return error.UnsupportedVersion;
            const count = std.mem.readInt(u32, header[1..5], .big);
            if (doc_idx >= count) return null;
            if (view.length != 5 + @as(u64, count) * 4) return error.InvalidSegment;
            var bytes: [4]u8 = undefined;
            try native.metadata_cache.?.readInto(view.offset + 5 + @as(u64, doc_idx) * 4, &bytes);
            const ordinal = std.mem.readInt(u32, &bytes, .big);
            return if (ordinal == 0) null else ordinal;
        }
        const section = (try self.getSection(doc_ordinals_field, .doc_ordinals)) orelse return null;
        return try decodeDocOrdinal(section, doc_idx);
    }

    pub const DocKeyRange = struct {
        min_key: []const u8,
        max_key: []const u8,
    };

    pub fn docKeyRange(self: *const SegmentReader) !?DocKeyRange {
        const section = (try self.getSection(doc_key_range_field, .doc_key_range)) orelse return null;
        return try decodeDocKeyRange(section);
    }

    pub fn indexSortFieldsAlloc(self: *const SegmentReader, alloc: Allocator) !?[]SegmentIndexSortField {
        const section = (try self.getSection(index_sort_field, .index_sort)) orelse return null;
        return try decodeIndexSortMetadataAlloc(alloc, section);
    }

    pub fn indexSortBoundsAlloc(self: *const SegmentReader, alloc: Allocator) !?SegmentIndexSortBounds {
        const section = (try self.getSection(index_sort_field, .index_sort_bounds)) orelse return null;
        return try decodeIndexSortBoundsMetadataAlloc(alloc, section);
    }
};

// ============================================================================
// Segment merger
// ============================================================================

/// Streaming destination for segment bytes.
///
/// The segment format requires random writes for the stored-doc offset table
/// and bounded range checksums for immutable payloads. A file-backed sink can
/// implement the same contract without materializing the final segment.
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
        crc32_range: *const fn (*anyopaque, usize, usize) anyerror!u32,
        page_directory: ?*const fn (*anyopaque) anyerror!integrity.Directory = null,
        begin_section: ?*const fn (*anyopaque) void = null,
        resident_bytes: ?*const fn (*anyopaque) usize = null,
    };

    pub fn len(self: *SegmentSink) usize {
        return self.vtable.len(self.ptr);
    }

    /// Heap bytes retained by the sink, distinct from bytes written to a file.
    /// Unknown sinks conservatively report their complete logical length.
    pub fn residentBytes(self: *SegmentSink) usize {
        return if (self.vtable.resident_bytes) |resident| resident(self.ptr) else self.len();
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

    /// Optional append-checksum checkpoint; sinks without tracking keep the
    /// existing range checksum contract.
    pub fn beginSection(self: *SegmentSink) void {
        if (self.vtable.begin_section) |begin| begin(self.ptr);
    }

    pub fn crc32Prefix(self: *SegmentSink, len_prefix: usize) !u32 {
        return try self.vtable.crc32_prefix(self.ptr, len_prefix);
    }

    pub fn crc32Range(self: *SegmentSink, offset: usize, range_len: usize) !u32 {
        return try self.vtable.crc32_range(self.ptr, offset, range_len);
    }
};

/// CRCs follow append traffic. Randomly patched pages alone are reread at
/// finalization; immutable payload pages never need a second checksum scan.
const PageChecksumSink = struct {
    alloc: Allocator,
    inner: *SegmentSink,
    checksums: std.ArrayListUnmanaged(u32) = .empty,
    dirty: std.ArrayListUnmanaged(bool) = .empty,
    partial: Crc32 = Crc32.init(),
    partial_len: usize = 0,
    partial_dirty: bool = false,
    tracking: bool = true,
    nonempty_prefix: bool = false,
    section_start: ?usize = null,
    section_checksums: std.ArrayListUnmanaged(u32) = .empty,
    section_dirty: std.ArrayListUnmanaged(bool) = .empty,
    section_partial: Crc32 = Crc32.init(),
    section_partial_len: usize = 0,
    section_partial_dirty: bool = false,
    fn init(alloc: Allocator, inner: *SegmentSink) PageChecksumSink {
        return .{ .alloc = alloc, .inner = inner, .tracking = inner.len() == 0, .nonempty_prefix = inner.len() != 0 };
    }
    fn deinit(self: *PageChecksumSink) void {
        self.checksums.deinit(self.alloc);
        self.dirty.deinit(self.alloc);
        self.section_checksums.deinit(self.alloc);
        self.section_dirty.deinit(self.alloc);
    }
    fn sink(self: *PageChecksumSink) SegmentSink {
        return .{ .ptr = self, .vtable = &vtable };
    }
    fn owner(ptr: *anyopaque) *PageChecksumSink {
        return @ptrCast(@alignCast(ptr));
    }
    fn length(ptr: *anyopaque) usize {
        return owner(ptr).inner.len();
    }
    fn track(self: *PageChecksumSink, bytes: []const u8) !void {
        if (self.section_start != null) {
            var position: usize = 0;
            while (position < bytes.len) {
                const take = @min(bytes.len - position, integrity.page_size - self.section_partial_len);
                self.section_partial.update(bytes[position..][0..take]);
                self.section_partial_len += take;
                position += take;
                if (self.section_partial_len == integrity.page_size) {
                    try self.section_checksums.append(self.alloc, self.section_partial.final());
                    try self.section_dirty.append(self.alloc, self.section_partial_dirty);
                    self.section_partial = Crc32.init();
                    self.section_partial_len = 0;
                    self.section_partial_dirty = false;
                }
            }
        }
        if (!self.tracking) return;
        var position: usize = 0;
        while (position < bytes.len) {
            const take = @min(bytes.len - position, integrity.page_size - self.partial_len);
            self.partial.update(bytes[position..][0..take]);
            self.partial_len += take;
            position += take;
            if (self.partial_len == integrity.page_size) {
                try self.checksums.append(self.alloc, self.partial.final());
                try self.dirty.append(self.alloc, self.partial_dirty);
                self.partial = Crc32.init();
                self.partial_len = 0;
                self.partial_dirty = false;
            }
        }
    }
    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self = owner(ptr);
        // Hash before append: memory sinks may relocate an aliased input.
        // Any failure aborts publication and discards this sink's state.
        try self.track(bytes);
        try self.inner.appendSlice(bytes);
    }
    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        try appendSlice(ptr, &.{byte});
    }
    fn appendNTimes(ptr: *anyopaque, byte: u8, count: usize) !void {
        const self = owner(ptr);
        try self.inner.appendNTimes(byte, count);
        var bytes: [8192]u8 = @splat(byte);
        var remaining = count;
        while (remaining > 0) {
            const take = @min(remaining, bytes.len);
            try self.track(bytes[0..take]);
            remaining -= take;
        }
    }
    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self = owner(ptr);
        try self.inner.writeAt(offset, bytes);
        if (bytes.len == 0) return;
        if (self.section_start) |start| {
            if (offset + bytes.len > start) {
                var block = (offset -| start) / integrity.page_size;
                const last_block = (offset + bytes.len - 1 - start) / integrity.page_size;
                while (block <= last_block) : (block += 1) {
                    if (block < self.section_dirty.items.len) self.section_dirty.items[block] = true else self.section_partial_dirty = true;
                }
            }
        }
        if (!self.tracking) return;
        var index = offset / integrity.page_size;
        const last = (offset + bytes.len - 1) / integrity.page_size;
        while (index <= last) : (index += 1) {
            if (index < self.dirty.items.len) self.dirty.items[index] = true else self.partial_dirty = true;
        }
    }
    fn crcPrefix(ptr: *anyopaque, count: usize) !u32 {
        return owner(ptr).inner.crc32Prefix(count);
    }
    fn beginSection(ptr: *anyopaque) void {
        const self = owner(ptr);
        self.section_start = self.inner.len();
        self.section_checksums.clearRetainingCapacity();
        self.section_dirty.clearRetainingCapacity();
        self.section_partial = Crc32.init();
        self.section_partial_len = 0;
        self.section_partial_dirty = false;
    }
    fn crcRange(ptr: *anyopaque, offset: usize, count: usize) !u32 {
        const self = owner(ptr);
        if (self.section_start == offset and count == self.inner.len() - offset) {
            var crc: u32 = 0;
            for (self.section_checksums.items, self.section_dirty.items, 0..) |value, dirty, index| {
                const block = if (dirty) try self.inner.crc32Range(offset + index * integrity.page_size, integrity.page_size) else value;
                crc = combineSectionCrc(crc, block, integrity.page_size);
            }
            if (self.section_partial_len != 0) {
                const block = if (self.section_partial_dirty) try self.inner.crc32Range(offset + self.section_checksums.items.len * integrity.page_size, self.section_partial_len) else self.section_partial.final();
                crc = combineSectionCrc(crc, block, self.section_partial_len);
            }
            return crc;
        }
        return self.inner.crc32Range(offset, count);
    }
    fn directory(ptr: *anyopaque) !integrity.Directory {
        const self = owner(ptr);
        self.tracking = false;
        self.section_start = null;
        if (self.nonempty_prefix) return writePageDirectory(self.inner);
        const end = self.inner.len();
        if (self.partial_len != 0) {
            try self.checksums.append(self.alloc, self.partial.final());
            try self.dirty.append(self.alloc, self.partial_dirty);
        }
        var buffer: [8192]u8 = undefined;
        var used: usize = 0;
        var directory_crc = Crc32.init();
        for (self.checksums.items, self.dirty.items, 0..) |crc, dirty, index| {
            const offset = index * integrity.page_size;
            const value = if (dirty) try self.inner.crc32Range(offset, @min(integrity.page_size, end - offset)) else crc;
            std.mem.writeInt(u32, buffer[used..][0..4], value, .big);
            used += 4;
            if (used == buffer.len) {
                directory_crc.update(buffer[0..used]);
                try self.inner.appendSlice(buffer[0..used]);
                used = 0;
            }
        }
        directory_crc.update(buffer[0..used]);
        try self.inner.appendSlice(buffer[0..used]);
        return .{ .offset = end, .length = self.inner.len() - end, .checksum = directory_crc.final() };
    }
    const vtable = SegmentSink.VTable{ .len = length, .append_slice = appendSlice, .append_byte = appendByte, .append_ntimes = appendNTimes, .write_at = writeAt, .crc32_prefix = crcPrefix, .crc32_range = crcRange, .page_directory = directory, .begin_section = beginSection };
};

/// Concatenate finalized IEEE CRCs without scanning either payload. Applying
/// the reflected polynomial's zero-byte operator shifts the left checksum.
fn combineSectionCrc(left: u32, right: u32, right_length: usize) u32 {
    if (right_length == 0) return left;
    const Matrix = struct {
        fn times(matrix: *const [32]u32, vector: u32) u32 {
            var bits = vector;
            var result: u32 = 0;
            var index: usize = 0;
            while (bits != 0) : (index += 1) {
                if (bits & 1 != 0) result ^= matrix[index];
                bits >>= 1;
            }
            return result;
        }
        fn square(matrix: *const [32]u32) [32]u32 {
            var result: [32]u32 = undefined;
            for (&result, 0..) |*value, index| value.* = times(matrix, matrix[index]);
            return result;
        }
    };
    // Full blocks dominate large sections. Precompute their shift operator
    // once rather than rebuilding GF(2) matrices for every 64 KiB block.
    const full_block_shift = comptime blk: {
        @setEvalBranchQuota(100_000);
        var matrix: [32]u32 = undefined;
        matrix[0] = 0xedb88320;
        var basis: u32 = 1;
        for (matrix[1..]) |*value| {
            value.* = basis;
            basis <<= 1;
        }
        for (0..19) |_| matrix = Matrix.square(&matrix);
        break :blk matrix;
    };
    if (right_length == integrity.page_size) return Matrix.times(&full_block_shift, left) ^ right;
    var odd: [32]u32 = undefined;
    odd[0] = 0xedb88320;
    var row: u32 = 1;
    for (odd[1..]) |*value| {
        value.* = row;
        row <<= 1;
    }
    var even = Matrix.square(&odd);
    odd = Matrix.square(&even);
    var length = right_length;
    var crc = left;
    while (true) {
        even = Matrix.square(&odd);
        if (length & 1 != 0) crc = Matrix.times(&even, crc);
        length >>= 1;
        if (length == 0) break;
        odd = Matrix.square(&even);
        if (length & 1 != 0) crc = Matrix.times(&odd, crc);
        length >>= 1;
        if (length == 0) break;
    }
    return crc ^ right;
}

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
        return Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn residentBytes(ptr: *anyopaque) usize {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        return self.out.capacity;
    }

    fn crc32Range(ptr: *anyopaque, offset: usize, range_len: usize) !u32 {
        const self: *MemorySegmentSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or range_len > self.out.items.len - offset) return error.InvalidSegment;
        return Crc32.hash(self.out.items[offset..][0..range_len]);
    }
};

const memory_segment_sink_vtable = SegmentSink.VTable{
    .len = MemorySegmentSink.len,
    .append_slice = MemorySegmentSink.appendSlice,
    .append_byte = MemorySegmentSink.appendByte,
    .append_ntimes = MemorySegmentSink.appendNTimes,
    .write_at = MemorySegmentSink.writeAt,
    .crc32_prefix = MemorySegmentSink.crc32Prefix,
    .crc32_range = MemorySegmentSink.crc32Range,
    .resident_bytes = MemorySegmentSink.residentBytes,
};

pub const MergeInput = struct {
    reader: *const SegmentReader,
    deleted: ?roaring.RoaringBitmap = null,

    fn isDeleted(self: MergeInput, doc_id: u32) bool {
        return if (self.deleted) |deleted| deleted.contains(doc_id) else false;
    }
};

/// Task-private provenance. Append merges retain spans; sorted native merges
/// retain private file coordinates with bounded caches. Heap callers can
/// promote shuffled spans to a four-byte table per source document. Public IDs and
/// parent ordinals never participate, so sibling chunks remain distinct.
pub const MergeSourceMap = struct {
    const Span = struct { source: u32, count: u32, output: u32 };
    const Row = struct { count: u32, spans: std.ArrayListUnmanaged(Span) = .empty, dense: ?[]u32 = null };
    pub const Location = struct { segment: u32, doc: u32 };
    const FileMappings = struct {
        allocator: Allocator,
        mapping: *FileSortedPlan.Run,
        contexts: []FileSortedPlan.MapContext,
        count: u32,
        fn deinit(self: *@This()) void {
            for (self.contexts) |*context| context.cache.deinit();
            self.allocator.free(self.contexts);
            self.mapping.deinit();
            self.allocator.destroy(self);
        }
    };
    allocator: Allocator,
    rows: []Row,
    output_ends: std.ArrayListUnmanaged(u32) = .empty,
    output_base: u32 = 0,
    file: ?*FileMappings = null,
    pub fn init(allocator: Allocator, counts: []const u32) !@This() {
        const rows = try allocator.alloc(Row, counts.len);
        for (rows, counts) |*row, count| row.* = .{ .count = count };
        return .{ .allocator = allocator, .rows = rows };
    }
    pub fn deinit(self: *@This()) void {
        if (self.file) |file| file.deinit();
        for (self.rows) |*row| {
            row.spans.deinit(self.allocator);
            if (row.dense) |dense| self.allocator.free(dense);
        }
        self.allocator.free(self.rows);
        self.output_ends.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn reset(self: *@This()) void {
        if (self.file) |file| file.deinit();
        self.file = null;
        for (self.rows) |*row| {
            row.spans.clearRetainingCapacity();
            if (row.dense) |dense| @memset(dense, std.math.maxInt(u32));
        }
        self.output_ends.clearRetainingCapacity();
        self.output_base = 0;
    }
    pub fn retainedBytes(self: *const @This()) usize {
        var bytes = self.rows.len * @sizeOf(Row) + self.output_ends.capacity * 4;
        if (comptime @import("builtin").os.tag != .freestanding) if (self.file) |file| {
            bytes += @sizeOf(FileMappings) + file.contexts.len * @sizeOf(FileSortedPlan.MapContext);
            bytes += file.mapping.buffer.capacity + file.mapping.read_buffer.len;
            for (file.contexts) |*context| bytes += context.cache.retainedBytes();
        };
        for (self.rows) |row| bytes += row.spans.capacity * @sizeOf(Span) + if (row.dense) |dense| dense.len * 4 else @as(usize, 0);
        return bytes;
    }
    pub fn record(self: *@This(), input: usize, source: u32, output: u32) !void {
        if (self.file != null) return error.InvalidSegment;
        if (input >= self.rows.len or source >= self.rows[input].count) return error.InvalidSegment;
        const row = &self.rows[input];
        const global = try std.math.add(u32, self.output_base, output);
        if (row.dense) |dense| {
            dense[source] = global;
            return;
        }
        if (row.spans.items.len > 0) {
            const last = &row.spans.items[row.spans.items.len - 1];
            if (source == last.source + last.count and global == last.output + last.count) {
                last.count += 1;
                return;
            }
        }
        const shuffled = row.spans.items.len > 0 and source < row.spans.items[row.spans.items.len - 1].source + row.spans.items[row.spans.items.len - 1].count;
        if (shuffled or (row.spans.items.len + 1) * @sizeOf(Span) > @as(usize, row.count) * 4) {
            const dense = try self.allocator.alloc(u32, row.count);
            @memset(dense, std.math.maxInt(u32));
            for (row.spans.items) |span| for (0..span.count) |i| {
                dense[span.source + i] = span.output + @as(u32, @intCast(i));
            };
            dense[source] = global;
            row.spans.deinit(self.allocator);
            row.spans = .empty;
            row.dense = dense;
        } else try row.spans.append(self.allocator, .{ .source = source, .count = 1, .output = global });
    }
    pub fn finishOutput(self: *@This(), count: u32) !void {
        if (self.file) |file| if (self.output_base != 0 or count != file.count) return error.InvalidSegment;
        const end = try std.math.add(u32, self.output_base, count);
        if (count == 0) return error.EmptySegment;
        try self.output_ends.append(self.allocator, end);
        self.output_base = end;
    }
    pub fn lookup(self: *const @This(), input: usize, source: u32) ?Location {
        // This entry point is intentionally memory-only for publication under
        // the apply lock. Native provenance is resolved off-lock with lookupRead.
        std.debug.assert(self.file == null);
        if (input >= self.rows.len or source >= self.rows[input].count) return null;
        const row = &self.rows[input];
        const global = if (row.dense) |dense| dense[source] else blk: {
            var lo: usize = 0;
            var hi = row.spans.items.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (row.spans.items[mid].source <= source) lo = mid + 1 else hi = mid;
            }
            if (lo == 0) return null;
            const span = row.spans.items[lo - 1];
            if (source - span.source >= span.count) return null;
            break :blk span.output + source - span.source;
        };
        return self.locate(global);
    }

    pub fn lookupRead(self: *const @This(), input: usize, source: u32) !?Location {
        if (self.file) |file| {
            if (input >= self.rows.len or source >= self.rows[input].count) return null;
            var bytes: [4]u8 = undefined;
            try file.contexts[input].cache.readInto(@as(u64, source) * 4, &bytes);
            return self.locate(std.mem.readInt(u32, &bytes, .little));
        }
        return self.lookup(input, source);
    }

    fn adoptFile(self: *@This(), file: *FileSortedPlan) !void {
        if (self.file != null or self.output_base != 0 or self.rows.len != file.maps.len) return error.InvalidSegment;
        for (self.rows, file.maps) |row, map| if (row.count != map.len) return error.InvalidSegment;
        const owner = try file.allocator.create(FileMappings);
        owner.* = .{ .allocator = file.allocator, .mapping = file.mapping, .contexts = file.contexts, .count = @intCast(file.count) };
        for (self.rows) |*row| {
            row.spans.deinit(self.allocator);
            row.spans = .empty;
            if (row.dense) |dense| self.allocator.free(dense);
            row.dense = null;
        }
        // Only the mapping and its stable cache contexts survive publication.
        // Output references and mapping descriptors belonged to the build.
        std.debug.assert(file.rows.len == 0 and file.unordered == null);
        file.allocator.free(file.maps);
        file.allocator.free(file.rows);
        file.records.deinit();
        file.allocator.destroy(file);
        self.file = owner;
    }

    fn locate(self: *const @This(), global: u32) ?Location {
        if (global == std.math.maxInt(u32) or global >= self.output_base) return null;
        var lo: usize = 0;
        var hi = self.output_ends.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.output_ends.items[mid] <= global) lo = mid + 1 else hi = mid;
        }
        return .{ .segment = @intCast(lo), .doc = global - (if (lo == 0) @as(u32, 0) else self.output_ends.items[lo - 1]) };
    }
};

pub const MergeScratchOptions = struct {
    io: std.Io,
    directory: []const u8,
    /// Small plans stay in memory within this fixed coordinate budget.
    in_memory_plan_bytes: usize = 256 * 1024,
    resource_manager: ?*@import("storage/resource_manager.zig").ResourceManager = null,
    external_sort_chunk_bytes: usize = 256 * 1024,
    external_sort_chunk_documents: usize = 1024,
    external_sort_fan_in: usize = 4,
};

pub const MergeOptions = struct {
    source_map: ?*MergeSourceMap = null,
    scratch: ?MergeScratchOptions = null,
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
    numeric_val: typed_dv.NumericValue,

    pub fn deinit(self: *SegmentSortValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes_val => |bytes| alloc.free(bytes),
            .id => |id| alloc.free(@constCast(id)),
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
    numeric_val,
};

fn segmentSortValueTag(value: SegmentSortValue) SegmentSortValueTag {
    return switch (value) {
        .u64_val => .u64_val,
        .i64_val => .i64_val,
        .f64_val => .f64_val,
        .bool_val => .bool_val,
        .bytes_val => .bytes_val,
        .id => .id,
        .numeric_val => .numeric_val,
    };
}

const SortedMergeRecord = struct {
    ref: MergeDocRef,
    keys: []SegmentSortValue,

    pub fn deinit(self: *SortedMergeRecord, alloc: Allocator) void {
        for (self.keys) |*key| key.deinit(alloc);
        alloc.free(self.keys);
        self.* = undefined;
    }
};

/// One query/merge owner for column navigation and a shared byte-bounded
/// payload cache. Reader addresses remain stable as new fields are opened.
/// Field names and segments must outlive this scope.
pub const TypedReadScope = struct {
    const Entry = struct { segment: *const SegmentReader, field: []const u8, reader: ?*typed_dv.TypedDocValuesReader };
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    cache: ?*typed_dv.TypedDocValuesReader.PointCache = null,

    pub fn init(allocator: Allocator) TypedReadScope {
        return .{ .allocator = allocator };
    }
    pub fn deinit(self: *TypedReadScope) void {
        if (self.cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
        }
        for (self.entries.items) |entry| if (entry.reader) |reader| {
            reader.deinit();
            self.allocator.destroy(reader);
        };
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn get(self: *TypedReadScope, segment: *const SegmentReader, field: []const u8) !?*typed_dv.TypedDocValuesReader {
        for (self.entries.items) |entry| if (entry.segment == segment and std.mem.eql(u8, entry.field, field)) return entry.reader;
        var opened = (try segment.typedDocValuesScoped(self.allocator, field)) orelse {
            try self.entries.append(self.allocator, .{ .segment = segment, .field = field, .reader = null });
            return null;
        };
        errdefer opened.deinit();
        const reader = try self.allocator.create(typed_dv.TypedDocValuesReader);
        errdefer self.allocator.destroy(reader);
        if (self.cache == null) {
            const cache = try self.allocator.create(typed_dv.TypedDocValuesReader.PointCache);
            cache.* = .{ .allocator = self.allocator, .byte_budget = 1024 * 1024 };
            self.cache = cache;
        }
        opened.point_cache = self.cache;
        opened.owns_point_cache = false;
        reader.* = opened;
        try self.entries.append(self.allocator, .{ .segment = segment, .field = field, .reader = reader });
        return reader;
    }
};

const SortedMergeDoc = struct { ref: MergeDocRef };

const SortedRecords = struct {
    memory: []const SortedMergeDoc = &.{},
    file: ?*FileSortedPlan = null,
    len: usize,
    fn batch(self: @This(), start: usize, buffer: []SortedMergeDoc) ![]const SortedMergeDoc {
        if (start > self.len) return error.InvalidSegment;
        const count = @min(buffer.len, self.len - start);
        if (self.file) |file| {
            var bytes: [64 * 8]u8 = undefined;
            if (count > 64) return error.InvalidSegment;
            try (try file.records.sealedView()).readInto(start * 8, bytes[0 .. count * 8]);
            for (buffer[0..count], 0..) |*record, i| {
                const input = std.mem.readInt(u32, bytes[i * 8 ..][0..4], .little);
                const doc = std.mem.readInt(u32, bytes[i * 8 + 4 ..][0..4], .little);
                if (input >= file.maps.len or doc >= file.maps[input].len) return error.InvalidSegment;
                record.* = .{ .ref = .{ .input_idx = input, .doc_id = doc } };
            }
            return buffer[0..count];
        }
        return self.memory[start..][0..count];
    }
    const Iterator = struct {
        records: SortedRecords,
        position: usize = 0,
        used: usize = 0,
        length: usize = 0,
        buffer: [64]SortedMergeDoc = undefined,
        fn next(self: *@This()) !?SortedMergeDoc {
            if (self.position == self.records.len) return null;
            if (self.used == self.length) {
                const loaded = try self.records.batch(self.position, &self.buffer);
                if (self.records.file == null) @memcpy(self.buffer[0..loaded.len], loaded);
                self.length = loaded.len;
                self.used = 0;
            }
            const record = self.buffer[self.used];
            self.used += 1;
            self.position += 1;
            return record;
        }
    };
    fn iterator(self: @This()) Iterator {
        return .{ .records = self };
    }
};
fn asSortedRecords(records: anytype) SortedRecords {
    if (@TypeOf(records) == SortedRecords) return records;
    return .{ .memory = records, .len = records.len };
}

/// Two private files hold output references and source mappings. Each input
/// fills its mapping in source order through a small buffer; no per-document
/// random write or document-sized heap allocation is required.
const FileSortedPlan = struct {
    const Run = @import("postings_run.zig").Run;
    const Source = @import("segment_source.zig");
    const Row = struct {
        offset: usize,
        count: u32,
        next: u32 = 0,
        used: usize = 0,
        bytes: [4096]u8 = undefined,
        fn flush(self: *@This(), file: *Run) !void {
            if (self.used == 0) return;
            try file.writeAt(self.offset + @as(usize, self.next) * 4 - self.used, self.bytes[0..self.used]);
            self.used = 0;
        }
        fn add(self: *@This(), file: *Run, value: u32) !void {
            if (self.next >= self.count) return error.InvalidSegment;
            std.mem.writeInt(u32, self.bytes[self.used..][0..4], value, .little);
            self.next += 1;
            self.used += 4;
            if (self.used == self.bytes.len) try self.flush(file);
        }
    };
    const MapContext = struct {
        view: Source.View,
        cache: Source.BlockCache,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.view.readInto(offset, out);
        }
        fn close(_: *anyopaque) void {}
    };
    allocator: Allocator,
    records: *Run,
    mapping: *Run,
    rows: []Row,
    maps: []inverted.FileDocMap = &.{},
    contexts: []MapContext = &.{},
    count: usize = 0,
    monotonic: bool = true,
    scratch: @import("spill_sort.zig").Options,
    unordered: ?*@import("spill_sort.zig").Sorter = null,
    fn create(allocator: Allocator, inputs: []const MergeInput, scratch: MergeScratchOptions) !*@This() {
        if (comptime @import("builtin").os.tag == .freestanding) return error.NativePostingsRunsUnavailable;
        const self = try allocator.create(@This());
        errdefer allocator.destroy(self);
        const records = try Run.createWithResources(allocator, scratch.io, scratch.directory, scratch.resource_manager);
        errdefer records.deinit();
        const mapping = try Run.createWithResources(allocator, scratch.io, scratch.directory, scratch.resource_manager);
        errdefer mapping.deinit();
        const rows = try allocator.alloc(Row, inputs.len);
        errdefer allocator.free(rows);
        var offset: usize = 0;
        for (rows, inputs) |*row, input| {
            row.* = .{ .offset = offset, .count = input.reader.doc_count };
            offset = try std.math.add(usize, offset, @as(usize, row.count) * 4);
        }
        var sink = mapping.sink();
        try sink.appendNTimes(0xff, offset);
        self.* = .{ .allocator = allocator, .records = records, .mapping = mapping, .rows = rows, .scratch = .{ .io = scratch.io, .directory = scratch.directory, .resource_manager = scratch.resource_manager } };
        return self;
    }
    fn deinit(self: *@This()) void {
        if (self.unordered) |sorter| {
            sorter.deinit();
            self.allocator.destroy(sorter);
        }
        for (self.contexts) |*context| context.cache.deinit();
        self.allocator.free(self.contexts);
        self.allocator.free(self.maps);
        self.allocator.free(self.rows);
        self.mapping.deinit();
        self.records.deinit();
        self.allocator.destroy(self);
    }
    fn append(self: *@This(), ref: MergeDocRef) !void {
        const row = &self.rows[ref.input_idx];
        if (self.monotonic) {
            if (ref.doc_id < row.next or ref.doc_id >= row.count) return error.NonMonotonicIndexSort;
            while (row.next < ref.doc_id) try row.add(self.mapping, std.math.maxInt(u32));
            try row.add(self.mapping, @intCast(self.count));
        } else {
            if (ref.doc_id >= row.count) return error.InvalidSegment;
            var output: [4]u8 = undefined;
            std.mem.writeInt(u32, &output, @intCast(self.count), .little);
            try self.unordered.?.add((@as(u64, @intCast(ref.input_idx)) << 32) | ref.doc_id, &output);
        }
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u32, bytes[0..4], @intCast(ref.input_idx), .little);
        std.mem.writeInt(u32, bytes[4..8], ref.doc_id, .little);
        try self.records.appendSlice(&bytes);
        self.count += 1;
    }
    fn finish(self: *@This()) !void {
        if (self.monotonic) {
            for (self.rows) |*row| {
                while (row.next < row.count) try row.add(self.mapping, std.math.maxInt(u32));
                try row.flush(self.mapping);
            }
        } else {
            const sorter = self.unordered.?;
            if (try sorter.finish()) |range| {
                var cursor = @import("spill_sort.zig").Cursor.init(self.allocator, sorter.run, range);
                defer cursor.deinit();
                var head = try cursor.next();
                for (self.rows, 0..) |*row, input| {
                    while (head) |record| {
                        const source: usize = @intCast(record.key >> 32);
                        if (source > input) break;
                        if (source != input or record.payload.len != 4) return error.InvalidSegment;
                        const doc: u32 = @truncate(record.key);
                        if (doc < row.next or doc >= row.count) return error.InvalidSegment;
                        while (row.next < doc) try row.add(self.mapping, std.math.maxInt(u32));
                        try row.add(self.mapping, std.mem.readInt(u32, record.payload[0..4], .little));
                        head = try cursor.next();
                    }
                    while (row.next < row.count) try row.add(self.mapping, std.math.maxInt(u32));
                    try row.flush(self.mapping);
                }
                if (head != null) return error.InvalidSegment;
            } else {
                for (self.rows) |*row| {
                    while (row.next < row.count) try row.add(self.mapping, std.math.maxInt(u32));
                    try row.flush(self.mapping);
                }
            }
            sorter.deinit();
            self.allocator.destroy(sorter);
            self.unordered = null;
        }
        try self.mapping.seal(0);
        try self.records.seal(0);
        const maps = try self.allocator.alloc(inverted.FileDocMap, self.rows.len);
        errdefer self.allocator.free(maps);
        const contexts = try self.allocator.alloc(MapContext, self.rows.len);
        errdefer self.allocator.free(contexts);
        var initialized: usize = 0;
        errdefer for (contexts[0..initialized]) |*context| context.cache.deinit();
        const mapping = try self.mapping.sealedView();
        const records = try self.records.sealedView();
        for (self.rows, maps, contexts) |row, *map, *context| {
            context.view = try Source.View.init(mapping.source, row.offset, @as(u64, row.count) * 4);
            context.cache = try Source.BlockCache.init(self.allocator, .{ .ranges = .{ .ptr = context, .length = context.view.length, .read_into = MapContext.read, .close = MapContext.close } }, 64 * 1024);
            initialized += 1;
            map.* = .{ .len = row.count, .ids = try Source.View.init(context.cache.borrowedSource(), 0, context.view.length), .records = records, .monotonic = self.monotonic, .scratch = self.scratch };
        }
        self.maps = maps;
        self.contexts = contexts;
        // Mapping write buffers are no longer needed after sealing.
        self.allocator.free(self.rows);
        self.rows = &.{};
    }

    fn enableUnordered(self: *@This()) !void {
        std.debug.assert(self.count == 0);
        const sorter = try self.allocator.create(@import("spill_sort.zig").Sorter);
        errdefer self.allocator.destroy(sorter);
        sorter.* = try .init(self.allocator, self.scratch);
        self.unordered = sorter;
        self.monotonic = false;
    }
};

fn deinitSortKeys(alloc: Allocator, keys: []SegmentSortValue) void {
    for (keys) |*key| key.deinit(alloc);
    alloc.free(keys);
}

const SortedMergePlan = struct {
    records: []SortedMergeDoc,
    doc_maps: [][]u32,
    first_keys: []SegmentSortValue,
    last_keys: []SegmentSortValue,
    file: ?*FileSortedPlan = null,

    fn ordered(self: *const SortedMergePlan) SortedRecords {
        return .{ .memory = self.records, .file = self.file, .len = if (self.file) |file| file.count else self.records.len };
    }

    pub fn deinit(self: *SortedMergePlan, alloc: Allocator) void {
        if (self.file) |file| file.deinit();
        alloc.free(self.records);
        deinitSortKeys(alloc, self.first_keys);
        deinitSortKeys(alloc, self.last_keys);
        for (self.doc_maps) |map| alloc.free(map);
        alloc.free(self.doc_maps);
        self.* = undefined;
    }
};

const BuiltSection = struct {
    section_type: SectionType,
    offset: u64,
    length: u64,
    checksum: u32,
};

const BuiltField = struct {
    name: []const u8,
    sections: std.ArrayListUnmanaged(BuiltSection) = .empty,

    pub fn deinit(self: *BuiltField, alloc: Allocator) void {
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

pub fn writeMergedSegmentToSinkWithOptions(alloc: Allocator, output: *SegmentSink, inputs: []const MergeInput, options: MergeOptions) !void {
    var tracked = PageChecksumSink.init(alloc, output);
    defer tracked.deinit();
    var tracked_sink = tracked.sink();
    const sink = &tracked_sink;
    if (options.index_sort.len > 0) {
        var plan = try buildSortedMergePlanWithScratch(alloc, inputs, options.index_sort, options.scratch);
        defer plan.deinit(alloc);
        try writeSortedMergedSegmentToSink(alloc, sink, inputs, options.index_sort, &plan);
        if (options.source_map) |map| {
            if (plan.file) |file| {
                try map.adoptFile(file);
                plan.file = null;
            } else {
                var iterator = plan.ordered().iterator();
                var output_doc: u32 = 0;
                while (try iterator.next()) |record| {
                    try map.record(record.ref.input_idx, record.ref.doc_id, output_doc);
                    output_doc += 1;
                }
            }
        }
        return;
    }
    try writeAppendMergedSegmentToSink(alloc, sink, inputs);
    if (options.source_map) |map| {
        var output_doc: u32 = 0;
        for (inputs, 0..) |input, input_idx| for (0..input.reader.doc_count) |source_doc| {
            if (input.isDeleted(@intCast(source_doc))) continue;
            try map.record(input_idx, @intCast(source_doc), output_doc);
            output_doc += 1;
        };
    }
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
    const stored_metadata_length = try writeMergedStoredFields(alloc, sink, inputs, doc_count);
    const stored_length: u64 = @intCast(sink.len() - @as(usize, @intCast(stored_offset)));
    const stored_metadata_crc = try sink.crc32Range(@intCast(stored_offset), @intCast(stored_metadata_length));

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

    // Freeze deletion navigation once for all fields in this merge.
    const doc_counts = try alloc.alloc(u32, inputs.len);
    defer alloc.free(doc_counts);
    const deleted_docs = try alloc.alloc(?roaring.RoaringBitmap, inputs.len);
    defer alloc.free(deleted_docs);
    for (inputs, doc_counts, deleted_docs) |input, *count, *deleted| {
        count.* = input.reader.doc_count;
        deleted.* = input.deleted;
    }
    const rank_maps = try inverted.prepareRankDocMaps(alloc, doc_counts, deleted_docs);
    defer inverted.deinitRankDocMaps(alloc, rank_maps);

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
        const inv_sections = try alloc.alloc(?@import("segment_source.zig").View, inputs.len);
        defer alloc.free(inv_sections);
        for (inputs, 0..) |input, i| {
            const reader = input.reader;
            inv_sections[i] = null;
            if (try reader.sectionView(field_name, .inverted_text)) |section_view| {
                inv_sections[i] = section_view;
                has_inverted = true;
                present_count += 1;
                if (first_present_index == null) first_present_index = i;
                if (input.deleted != null) only_present_deleted = true;
            }
        }

        if (has_inverted) {
            if (present_count == 1 and first_present_index.? == 0 and !only_present_deleted) {
                const view = inv_sections[0].?;
                const offset = sink.len();
                sink.beginSection();
                var scratch: [64 * 1024]u8 = undefined;
                var copied: u64 = 0;
                while (copied < view.length) {
                    const bytes = scratch[0..@intCast(@min(scratch.len, view.length - copied))];
                    try view.readInto(copied, bytes);
                    try sink.appendSlice(bytes);
                    copied += bytes.len;
                }
                try built_field.sections.append(alloc, .{ .section_type = .inverted_text, .offset = offset, .length = view.length, .checksum = try sink.crc32Range(offset, sink.len() - offset) });
            } else {
                const offset = sink.len();
                sink.beginSection();
                try inverted.writeMergedInvertedSectionSlotsWithRankMaps(alloc, sink, inv_sections, doc_counts, rank_maps, doc_count, inverted.productionIndexConfig());
                try built_field.sections.append(alloc, .{
                    .section_type = .inverted_text,
                    .offset = @intCast(offset),
                    .length = @intCast(sink.len() - offset),
                    .checksum = try sink.crc32Range(offset, sink.len() - offset),
                });
            }
        }

        const typed_start = sink.len();
        sink.beginSection();
        if (try writeMergeTypedDocValuesSections(alloc, sink, inputs, field_name, rank_maps)) {
            try built_field.sections.append(alloc, .{ .section_type = .typed_doc_values, .offset = typed_start, .length = sink.len() - typed_start, .checksum = try sink.crc32Range(typed_start, sink.len() - typed_start) });
        }

        try built_fields.append(alloc, built_field);
    }

    const ordinal_start = sink.len();
    sink.beginSection();
    if (try writeMergedDocOrdinals(sink, inputs, null, doc_count)) {
        var built_field = BuiltField{ .name = doc_ordinals_field };
        errdefer built_field.deinit(alloc);
        try built_field.sections.append(alloc, .{ .section_type = .doc_ordinals, .offset = ordinal_start, .length = sink.len() - ordinal_start, .checksum = try sink.crc32Range(ordinal_start, sink.len() - ordinal_start) });
        try built_fields.append(alloc, built_field);
    }
    if (try mergeDocKeyRangeSectionsAlloc(alloc, inputs)) |merged_key_range| {
        defer alloc.free(merged_key_range);
        var built_field = BuiltField{ .name = doc_key_range_field };
        errdefer built_field.deinit(alloc);
        try appendBuiltSection(alloc, sink, &built_field, .doc_key_range, merged_key_range);
        try built_fields.append(alloc, built_field);
    }

    const directory = try writePageDirectory(sink);
    const sections_index_offset: u64 = @intCast(sink.len());
    try writePageDirectoryDescriptor(sink, directory);
    try writeMergedSectionIndex(alloc, sink, built_fields.items);

    try sinkAppendU64BE(sink, doc_count);
    try sinkAppendU64BE(sink, stored_offset);
    try sinkAppendU64BE(sink, stored_length);
    try sinkAppendU64BE(sink, stored_metadata_length);
    try sinkAppendU64BE(sink, sections_index_offset);
    try sinkAppendU32BE(sink, stored_metadata_crc);
    try writeSegmentVersionChecksumAndMagic(sink, @intCast(sections_index_offset));
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
    const doc_count: u32 = @intCast(plan.ordered().len);
    const stored_metadata_length = try writeMergedStoredFieldsInOrder(alloc, sink, inputs, plan.ordered(), doc_count);
    const stored_length: u64 = @intCast(sink.len() - @as(usize, @intCast(stored_offset)));
    const stored_metadata_crc = try sink.crc32Range(@intCast(stored_offset), @intCast(stored_metadata_length));

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
        const inv_sections = try alloc.alloc(?@import("segment_source.zig").View, inputs.len);
        defer alloc.free(inv_sections);
        const doc_counts = try alloc.alloc(u32, inputs.len);
        defer alloc.free(doc_counts);

        for (inputs, 0..) |input, i| {
            const reader = input.reader;
            doc_counts[i] = reader.doc_count;
            inv_sections[i] = null;
            if (try reader.sectionView(field_name, .inverted_text)) |section_view| {
                inv_sections[i] = section_view;
                has_inverted = true;
            }
        }

        if (has_inverted) {
            const offset = sink.len();
            sink.beginSection();
            if (plan.file) |file| {
                try inverted.writeMergedInvertedSectionSlotsWithFileMaps(alloc, sink, inv_sections, doc_counts, file.maps, doc_count, inverted.productionIndexConfig());
            } else try inverted.writeMergedInvertedSectionSlotsWithDocMaps(alloc, sink, inv_sections, doc_counts, plan.doc_maps, doc_count, inverted.productionIndexConfig());
            try built_field.sections.append(alloc, .{
                .section_type = .inverted_text,
                .offset = @intCast(offset),
                .length = @intCast(sink.len() - offset),
                .checksum = try sink.crc32Range(offset, sink.len() - offset),
            });
        }

        const typed_start = sink.len();
        sink.beginSection();
        if (try writeMergeTypedDocValuesSectionsInOrder(alloc, sink, inputs, field_name, plan.ordered(), null)) {
            try built_field.sections.append(alloc, .{ .section_type = .typed_doc_values, .offset = typed_start, .length = sink.len() - typed_start, .checksum = try sink.crc32Range(typed_start, sink.len() - typed_start) });
        }

        try built_fields.append(alloc, built_field);
    }

    const ordinal_start = sink.len();
    sink.beginSection();
    if (try writeMergedDocOrdinals(sink, inputs, plan.ordered(), doc_count)) {
        var built_field = BuiltField{ .name = doc_ordinals_field };
        errdefer built_field.deinit(alloc);
        try built_field.sections.append(alloc, .{ .section_type = .doc_ordinals, .offset = ordinal_start, .length = sink.len() - ordinal_start, .checksum = try sink.crc32Range(ordinal_start, sink.len() - ordinal_start) });
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

    const directory = try writePageDirectory(sink);
    const sections_index_offset: u64 = @intCast(sink.len());
    try writePageDirectoryDescriptor(sink, directory);
    try writeMergedSectionIndex(alloc, sink, built_fields.items);

    try sinkAppendU64BE(sink, doc_count);
    try sinkAppendU64BE(sink, stored_offset);
    try sinkAppendU64BE(sink, stored_length);
    try sinkAppendU64BE(sink, stored_metadata_length);
    try sinkAppendU64BE(sink, sections_index_offset);
    try sinkAppendU32BE(sink, stored_metadata_crc);
    try writeSegmentVersionChecksumAndMagic(sink, @intCast(sections_index_offset));
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

fn allStoredFieldsOmitted(inputs: []const MergeInput) !bool {
    var saw_live_docs = false;
    for (inputs) |input| {
        if (!inputHasLiveDocs(input)) continue;
        saw_live_docs = true;
        if (!(try input.reader.storedFieldsOmitted())) return false;
    }
    return saw_live_docs;
}

fn writeOmittedStoredFields(sink: *SegmentSink, doc_count: u32) !u64 {
    const start = sink.len();
    try sink.appendByte(stored_fields_version_omitted);
    try sinkAppendU32LE(sink, doc_count);
    return @intCast(sink.len() - start);
}

fn rejectMixedStoredFieldModes(inputs: []const MergeInput) !void {
    for (inputs) |input| {
        if (inputHasLiveDocs(input) and (try input.reader.storedFieldsOmitted())) return error.InvalidSegment;
    }
}

/// Emit complete navigation rows in bounded contiguous writes. ID bytes are
/// appended first; the body pass already knows the exact output block/offset,
/// including compressed-block passthrough, and fills each row only once.
const StoredTableOutput = struct {
    start: usize,
    length: usize = 0,
    doc_count: u32 = 0,
    id_bytes: u64 = 0,
    bytes: [2730 * stored_fields_v4_doc_entry_size]u8 = undefined,

    fn append(self: *StoredTableOutput, sink: *SegmentSink, id_length: usize, block: u32, offset: u32, raw_length: usize) !void {
        if (id_length > std.math.maxInt(u32) or raw_length > std.math.maxInt(u32)) return error.InvalidSegment;
        const row = self.bytes[self.length..][0..stored_fields_v4_doc_entry_size];
        std.mem.writeInt(u64, row[0..8], self.id_bytes, .little);
        std.mem.writeInt(u32, row[8..12], @intCast(id_length), .little);
        std.mem.writeInt(u32, row[12..16], block, .little);
        std.mem.writeInt(u32, row[16..20], offset, .little);
        std.mem.writeInt(u32, row[20..24], @intCast(raw_length), .little);
        self.length += stored_fields_v4_doc_entry_size;
        self.id_bytes = try std.math.add(u64, self.id_bytes, id_length);
        self.doc_count = try std.math.add(u32, self.doc_count, 1);
        if (self.length == self.bytes.len) try self.flush(sink);
    }
    fn flush(self: *StoredTableOutput, sink: *SegmentSink) !void {
        if (self.length == 0) return;
        try sink.writeAt(self.start, self.bytes[0..self.length]);
        self.start += self.length;
        self.length = 0;
    }
};

fn writeMergedStoredFields(alloc: Allocator, sink: *SegmentSink, inputs: []const MergeInput, doc_count: u32) !u64 {
    const stored_start = sink.len();
    if (try allStoredFieldsOmitted(inputs)) return writeOmittedStoredFields(sink, doc_count);
    try rejectMixedStoredFieldModes(inputs);
    try sink.appendByte(stored_fields_version_block_compressed);
    try sinkAppendU32LE(sink, doc_count);
    const num_blocks = try countMergedStoredBlocks(inputs);
    try sinkAppendU32LE(sink, num_blocks);
    try sinkAppendU32LE(sink, stored_fields_block_doc_target);
    const id_bytes_len_pos = sink.len();
    try sinkAppendU64LE(sink, 0);

    const doc_table_start = sink.len();
    var table = StoredTableOutput{ .start = doc_table_start };
    try sink.appendNTimes(0, @as(usize, doc_count) * stored_fields_v4_doc_entry_size);
    const block_offsets_start = sink.len();
    try sink.appendNTimes(0, @as(usize, num_blocks) * 8);
    const block_checksums_start = sink.len();
    try sink.appendNTimes(0, @as(usize, num_blocks) * 4);

    var identity_scratch = SegmentReadScratch.init(alloc, 64 * 1024);
    defer identity_scratch.deinit();
    const id_bytes_start = sink.len();
    for (inputs) |input| {
        for (0..input.reader.doc_count) |doc_id_usize| {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) continue;
            identity_scratch.reset();
            const id = (try input.reader.storedIdAlloc(identity_scratch.allocator(), doc_id)) orelse continue;
            try sink.appendSlice(id);
        }
    }
    const id_bytes_len: u64 = @intCast(sink.len() - id_bytes_start);
    try sink.writeAt(id_bytes_len_pos, &@as([8]u8, @bitCast(@as(u64, id_bytes_len))));

    const metadata_length: u64 = @intCast(sink.len() - stored_start);
    const data_start = sink.len();
    var block_idx: u32 = 0;
    var docs_in_block: u32 = 0;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    var stored_cursor = SegmentReader.StoredDocBlockCache.init(alloc, 1024 * 1024);
    defer stored_cursor.deinit();
    for (inputs) |input| {
        var doc_id_usize: usize = 0;
        while (doc_id_usize < input.reader.doc_count) {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) {
                doc_id_usize += 1;
                continue;
            }

            // Flush a partial prefix before an intact source block, so one
            // damaged block does not force every subsequent block to decode.
            if (try copyableStoredBlockDocs(input, doc_id)) |copied_docs| {
                if (chunk.items.len != 0) {
                    try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, block_checksums_start, data_start, block_idx);
                    block_idx += 1;
                }
                const copied = (try copyMergedStoredBlockIfPossible(sink, input, doc_id, block_idx, &table, block_offsets_start, block_checksums_start, data_start)) orelse return error.InvalidSegment;
                if (copied != copied_docs) return error.InvalidSegment;
                doc_id_usize += copied_docs;
                block_idx += 1;
                docs_in_block = 0;
                continue;
            }

            const stored = (try stored_cursor.get(input.reader, doc_id)) orelse {
                doc_id_usize += 1;
                continue;
            };
            if (chunk.items.len > 0 and (docs_in_block >= stored_fields_block_doc_target or chunk.items.len +| 4 +| stored.data.len > stored_fields_block_raw_target)) {
                try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, block_checksums_start, data_start, block_idx);
                block_idx += 1;
                docs_in_block = 0;
            }

            const doc_offset: u32 = @intCast(chunk.items.len);
            try appendU32LE(alloc, &chunk, @intCast(stored.data.len));
            try chunk.appendSlice(alloc, stored.data);
            try table.append(sink, stored.id.len, block_idx, doc_offset, stored.data.len);
            docs_in_block += 1;
            doc_id_usize += 1;
        }
    }
    if (chunk.items.len > 0) {
        try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, block_checksums_start, data_start, block_idx);
    }
    try table.flush(sink);
    if (table.doc_count != doc_count or table.id_bytes != id_bytes_len) return error.InvalidSegment;
    return metadata_length;
}

fn copyableSortedStoredBlockDocs(inputs: []const MergeInput, records: SortedRecords, position: usize, ref: MergeDocRef) !?u32 {
    const count = (try copyableStoredBlockDocs(inputs[ref.input_idx], ref.doc_id)) orelse return null;
    if (count > records.len - position) return null;
    var buffer: [64]SortedMergeDoc = undefined;
    var offset: usize = 0;
    while (offset < count) {
        const batch = try records.batch(position + offset, buffer[0..@min(buffer.len, count - offset)]);
        for (batch, 0..) |record, i| if (record.ref.input_idx != ref.input_idx or record.ref.doc_id != ref.doc_id + offset + i) return null;
        offset += batch.len;
    }
    return count;
}

fn writeMergedStoredFieldsInOrder(
    alloc: Allocator,
    sink: *SegmentSink,
    inputs: []const MergeInput,
    records_arg: anytype,
    doc_count: u32,
) !u64 {
    return writeMergedStoredFieldsInOrderWithReuse(true, alloc, sink, inputs, records_arg, doc_count);
}

fn writeMergedStoredFieldsInOrderWithReuse(
    comptime reuse_blocks: bool,
    alloc: Allocator,
    sink: *SegmentSink,
    inputs: []const MergeInput,
    records_arg: anytype,
    doc_count: u32,
) !u64 {
    const records = asSortedRecords(records_arg);
    const stored_start = sink.len();
    if (try allStoredFieldsOmitted(inputs)) return writeOmittedStoredFields(sink, doc_count);
    try rejectMixedStoredFieldModes(inputs);
    try sink.appendByte(stored_fields_version_block_compressed);
    try sinkAppendU32LE(sink, doc_count);
    const num_blocks = try countStoredBlocksInOrder(reuse_blocks, inputs, records);
    try sinkAppendU32LE(sink, num_blocks);
    try sinkAppendU32LE(sink, stored_fields_block_doc_target);
    const id_bytes_len_pos = sink.len();
    try sinkAppendU64LE(sink, 0);

    const doc_table_start = sink.len();
    var table = StoredTableOutput{ .start = doc_table_start };
    try sink.appendNTimes(0, @as(usize, doc_count) * stored_fields_v4_doc_entry_size);
    const block_offsets_start = sink.len();
    try sink.appendNTimes(0, @as(usize, num_blocks) * 8);
    const block_checksums_start = sink.len();
    try sink.appendNTimes(0, @as(usize, num_blocks) * 4);

    var identity_scratch = SegmentReadScratch.init(alloc, 64 * 1024);
    defer identity_scratch.deinit();
    const id_bytes_start = sink.len();
    var record_iterator_1 = records.iterator();
    while (try record_iterator_1.next()) |record| {
        identity_scratch.reset();
        const id = (try inputs[record.ref.input_idx].reader.storedIdAlloc(identity_scratch.allocator(), record.ref.doc_id)) orelse return error.InvalidSegment;
        try sink.appendSlice(id);
    }
    const id_bytes_len: u64 = @intCast(sink.len() - id_bytes_start);
    try sink.writeAt(id_bytes_len_pos, &@as([8]u8, @bitCast(@as(u64, id_bytes_len))));

    const metadata_length: u64 = @intCast(sink.len() - stored_start);
    const data_start = sink.len();
    var block_idx: u32 = 0;
    var docs_in_block: u32 = 0;
    var chunk = std.ArrayListUnmanaged(u8).empty;
    defer chunk.deinit(alloc);
    var stored_cursor = SegmentReader.StoredDocBlockCache.init(alloc, 1024 * 1024);
    defer stored_cursor.deinit();

    var record_iterator_2 = records.iterator();
    while (try record_iterator_2.next()) |record| {
        if (reuse_blocks) {
            if (try copyableSortedStoredBlockDocs(inputs, records, record_iterator_2.position - 1, record.ref)) |count| {
                if (chunk.items.len != 0) {
                    try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, block_checksums_start, data_start, block_idx);
                    block_idx += 1;
                }
                docs_in_block = 0;
                const copied = (try copyMergedStoredBlockIfPossible(sink, inputs[record.ref.input_idx], record.ref.doc_id, block_idx, &table, block_offsets_start, block_checksums_start, data_start)) orelse return error.InvalidSegment;
                if (copied != count) return error.InvalidSegment;
                record_iterator_2.position += count - 1;
                record_iterator_2.used = 0;
                record_iterator_2.length = 0;
                block_idx += 1;
                continue;
            }
        }
        const stored = (try stored_cursor.get(inputs[record.ref.input_idx].reader, record.ref.doc_id)) orelse return error.InvalidSegment;
        if (chunk.items.len > 0 and (docs_in_block >= stored_fields_block_doc_target or chunk.items.len +| 4 +| stored.data.len > stored_fields_block_raw_target)) {
            try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, block_checksums_start, data_start, block_idx);
            block_idx += 1;
            docs_in_block = 0;
        }

        const doc_offset: u32 = @intCast(chunk.items.len);
        try appendU32LE(alloc, &chunk, @intCast(stored.data.len));
        try chunk.appendSlice(alloc, stored.data);
        try table.append(sink, stored.id.len, block_idx, doc_offset, stored.data.len);
        docs_in_block += 1;
    }
    if (chunk.items.len > 0) {
        try flushMergedStoredBlock(alloc, sink, &chunk, block_offsets_start, block_checksums_start, data_start, block_idx);
    }
    try table.flush(sink);
    if (table.doc_count != doc_count or table.id_bytes != id_bytes_len) return error.InvalidSegment;
    return metadata_length;
}

fn countStoredBlocksInOrder(comptime reuse_blocks: bool, inputs: []const MergeInput, records_arg: anytype) !u32 {
    const records = asSortedRecords(records_arg);
    var blocks: u32 = 0;
    var docs_in_block: u32 = 0;
    var raw_bytes: usize = 0;
    var record_iterator_1 = records.iterator();
    while (try record_iterator_1.next()) |record| {
        if (reuse_blocks) {
            if (try copyableSortedStoredBlockDocs(inputs, records, record_iterator_1.position - 1, record.ref)) |count| {
                if (raw_bytes != 0) blocks += 1;
                docs_in_block = 0;
                raw_bytes = 0;
                record_iterator_1.position += count - 1;
                record_iterator_1.used = 0;
                record_iterator_1.length = 0;
                blocks += 1;
                continue;
            }
        }
        const length = (try inputs[record.ref.input_idx].reader.storedDocLength(record.ref.doc_id)) orelse return error.InvalidSegment;
        const doc_raw_bytes = @as(usize, 4) +| length;
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

fn countMergedStoredBlocks(inputs: []const MergeInput) !u32 {
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

            if (try copyableStoredBlockDocs(input, doc_id)) |copied_docs| {
                if (raw_bytes != 0) blocks += 1;
                blocks += 1;
                doc_id_usize += copied_docs;
                docs_in_block = 0;
                raw_bytes = 0;
                continue;
            }

            const length = (try input.reader.storedDocLength(doc_id)) orelse {
                doc_id_usize += 1;
                continue;
            };
            const doc_raw_bytes = @as(usize, 4) +| length;
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
    block_checksums_start: usize,
    data_start: usize,
    block_idx: u32,
) !void {
    const compressed = try snappy.encode(alloc, chunk.items);
    defer alloc.free(compressed);
    try sink.appendSlice(compressed);
    try sink.writeAt(
        block_checksums_start + @as(usize, block_idx) * 4,
        &@as([4]u8, @bitCast(@as(u32, Crc32.hash(compressed)))),
    );
    const block_end_offset: u64 = @intCast(sink.len() - data_start);
    try sink.writeAt(block_offsets_start + @as(usize, block_idx) * 8, &@as([8]u8, @bitCast(@as(u64, block_end_offset))));
    chunk.clearRetainingCapacity();
}

fn copyMergedStoredBlockIfPossible(
    sink: *SegmentSink,
    input: MergeInput,
    start_doc_id: u32,
    out_block_idx: u32,
    table: *StoredTableOutput,
    block_offsets_start: usize,
    block_checksums_start: usize,
    data_start: usize,
) !?u32 {
    const copied_docs = (try copyableStoredBlockDocs(input, start_doc_id)) orelse return null;
    const reader = input.reader;
    const first = (try reader.storedLocationMetadata(start_doc_id)) orelse return null;

    if (first.block_start > first.block_end or first.block_end > reader.source().len()) return null;

    const block_checksum = if (reader.native != null) blk: {
        var scratch: [8192]u8 = undefined;
        var checksum = Crc32.init();
        const expected = try reader.storedBlockChecksum(first.block_idx);
        var offset = first.block_start;
        while (offset < first.block_end) {
            const bytes = scratch[0..@min(scratch.len, first.block_end - offset)];
            try reader.source().readInto(offset, bytes);
            checksum.update(bytes);
            try sink.appendSlice(bytes);
            offset += bytes.len;
        }
        // The task-private output is discarded on failure; no corrupt block
        // can reach artifact publication.
        const crc = checksum.final();
        if (crc != expected) return error.CrcMismatch;
        break :blk crc;
    } else blk: {
        const block = reader.data[first.block_start..first.block_end];
        const crc = try reader.validateStoredBlock(first.block_idx, block);
        try sink.appendSlice(block);
        break :blk crc;
    };
    try sink.writeAt(
        block_checksums_start + @as(usize, out_block_idx) * 4,
        &@as([4]u8, @bitCast(@as(u32, block_checksum))),
    );
    const block_end_offset: u64 = @intCast(sink.len() - data_start);
    try sink.writeAt(block_offsets_start + @as(usize, out_block_idx) * 8, &@as([8]u8, @bitCast(@as(u64, block_end_offset))));

    var i: u32 = 0;
    while (i < copied_docs) : (i += 1) {
        const loc = (try reader.storedLocationMetadata(start_doc_id + i)) orelse return null;
        try table.append(sink, loc.id_length, out_block_idx, @intCast(loc.doc_offset), loc.raw_len);
    }

    return copied_docs;
}

fn copyableStoredBlockDocs(input: MergeInput, start_doc_id: u32) !?u32 {
    const reader = input.reader;
    if (reader.storedMetadata()[0] != stored_fields_version_block_compressed) return null;
    const first = (try reader.storedLocationMetadata(start_doc_id)) orelse return null;
    if (first.doc_offset != 0) return null;

    var count: u32 = 0;
    var raw_bytes: usize = 0;
    var doc_id = start_doc_id;
    while (doc_id < reader.doc_count) : (doc_id += 1) {
        const loc = (try reader.storedLocationMetadata(doc_id)) orelse return null;
        if (loc.block_idx != first.block_idx) break;
        if (input.isDeleted(doc_id)) return null;
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
        .checksum = Crc32.hash(data),
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
            try sinkAppendU16BE(sink, @backingInt(section.section_type));
            try sinkAppendU64BE(sink, section.offset);
            try sinkAppendU64BE(sink, section.length);
            try sinkAppendU32BE(sink, section.checksum);
        }
    }
}

fn buildSortedMergePlanAlloc(alloc: Allocator, inputs: []const MergeInput, index_sort: []const SegmentIndexSortField) !SortedMergePlan {
    return buildSortedMergePlanWithScratch(alloc, inputs, index_sort, null);
}

fn buildSortedMergePlanWithScratch(alloc: Allocator, inputs: []const MergeInput, index_sort: []const SegmentIndexSortField, scratch: ?MergeScratchOptions) !SortedMergePlan {
    return buildStreamingSortedMergePlanWithScratch(alloc, inputs, index_sort, scratch) catch |err| switch (err) {
        // Historical callers can supply unsorted physical inputs with sort
        // metadata. Preserve their global-sort contract through a fallback.
        error.NonMonotonicIndexSort => if (scratch) |options| buildExternalSortedMergePlan(alloc, inputs, index_sort, options) else buildUnsortedMergePlanAlloc(alloc, inputs, index_sort),
        else => err,
    };
}

fn buildUnsortedMergePlanAlloc(
    alloc: Allocator,
    inputs: []const MergeInput,
    index_sort: []const SegmentIndexSortField,
) !SortedMergePlan {
    if (index_sort.len == 0) return error.InvalidSegment;
    var reads = TypedReadScope.init(alloc);
    defer reads.deinit();
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
            const keys = try loadSegmentSortKeysAlloc(alloc, &reads, inputs, .{ .input_idx = input_idx, .doc_id = doc_id }, index_sort, null, null, null);
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

    const domains = try alloc.alloc(?SegmentSortValueTag, index_sort.len);
    defer alloc.free(domains);
    @memset(domains, null);
    for (records.items) |record| try validateSortKeyDomains(record.keys, domains);
    std.sort.pdq(SortedMergeRecord, records.items, index_sort, sortedMergeRecordLessThan);
    for (records.items, 0..) |record, out_doc_id| {
        doc_maps[record.ref.input_idx][record.ref.doc_id] = @intCast(out_doc_id);
    }

    const docs = try alloc.alloc(SortedMergeDoc, records.items.len);
    errdefer alloc.free(docs);
    for (records.items, docs) |record, *doc| doc.* = .{ .ref = record.ref };
    const first_keys = try cloneSegmentSortKeysAlloc(alloc, if (records.items.len == 0) &.{} else records.items[0].keys);
    errdefer deinitSortKeys(alloc, first_keys);
    const last_keys = try cloneSegmentSortKeysAlloc(alloc, if (records.items.len == 0) &.{} else records.items[records.items.len - 1].keys);
    for (records.items) |*record| record.deinit(alloc);
    records.deinit(alloc);
    return .{ .records = docs, .doc_maps = doc_maps, .first_keys = first_keys, .last_keys = last_keys };
}

/// Historical segments may advertise an index sort without physically sorted
/// documents. Sort byte-bounded chunks, then binary-carry their coordinate
/// runs. Extract source keys once into sequential scratch records; carries
/// retain up to four byte-bounded heads, with binary fallback for large keys.
const ExternalCoordinateSort = struct {
    const Range = struct { start: usize, end: usize, count: usize, max_record_bytes: usize = 0 };
    allocator: Allocator,
    spool: *FileSortedPlan.Run,
    fields: []const SegmentIndexSortField,
    levels: [64][3]?Range = @splat(@splat(null)),
    fan_in: usize = 4,
    const Cursor = struct {
        sorter: *ExternalCoordinateSort,
        range: Range,
        position: usize,
        remaining: usize,
        payloads: @import("segment_source.zig").Scratch,
        keys: ?[]SegmentSortValue = null,
        current: ?SortedMergeRecord = null,
        encoded: []const u8 = &.{},
        fn init(sorter: *ExternalCoordinateSort, range: Range) @This() {
            return .{ .sorter = sorter, .range = range, .position = range.start, .remaining = range.count, .payloads = .init(sorter.allocator, 128 * 1024) };
        }
        fn deinit(self: *@This()) void {
            if (self.keys) |keys| self.sorter.allocator.free(keys);
            self.payloads.deinit();
        }
        fn next(self: *@This()) !void {
            self.current = null;
            self.payloads.reset();
            if (self.remaining == 0) {
                if (self.position != self.range.end) return error.InvalidSegment;
                return;
            }
            if (self.position > self.range.end or self.range.end - self.position < 8) return error.InvalidSegment;
            var header: [8]u8 = undefined;
            const view = try self.sorter.spool.sealedView();
            try view.readInto(self.position, &header);
            self.position += 8;
            const length = std.math.cast(usize, std.mem.readInt(u64, &header, .little)) orelse return error.InvalidSegment;
            if (length < 8 or length > self.range.end - self.position) return error.InvalidSegment;
            const bytes = try self.payloads.allocator().alloc(u8, length);
            try view.readInto(self.position, bytes);
            self.position += length;
            self.remaining -= 1;
            self.encoded = bytes;
            if (self.keys == null) self.keys = try self.sorter.allocator.alloc(SegmentSortValue, self.sorter.fields.len);
            var offset: usize = 8;
            for (self.keys.?) |*key| {
                if (offset >= bytes.len) return error.InvalidSegment;
                const tag = std.enums.fromInt(SegmentSortValueTag, bytes[offset]) orelse return error.InvalidSegment;
                offset += 1;
                if (tag == .bool_val) {
                    if (offset >= bytes.len or bytes[offset] > 1) return error.InvalidSegment;
                    key.* = .{ .bool_val = bytes[offset] != 0 };
                    offset += 1;
                    continue;
                }
                var numeric_tag: u8 = 0;
                if (tag == .numeric_val) {
                    if (offset >= bytes.len) return error.InvalidSegment;
                    numeric_tag = bytes[offset];
                    offset += 1;
                }
                if (offset > bytes.len or bytes.len - offset < 8) return error.InvalidSegment;
                const value = std.mem.readInt(u64, bytes[offset..][0..8], .little);
                offset += 8;
                key.* = switch (tag) {
                    .u64_val => .{ .u64_val = value },
                    .i64_val => .{ .i64_val = @bitCast(value) },
                    .f64_val => .{ .f64_val = @bitCast(value) },
                    .numeric_val => .{ .numeric_val = switch (numeric_tag) {
                        0 => .{ .u64_val = value },
                        1 => .{ .i64_val = @bitCast(value) },
                        2 => .{ .f64_val = @bitCast(value) },
                        else => return error.InvalidSegment,
                    } },
                    .bytes_val, .id => blk: {
                        const size = std.math.cast(usize, value) orelse return error.InvalidSegment;
                        if (size > bytes.len - offset) return error.InvalidSegment;
                        const payload = bytes[offset..][0..size];
                        offset += size;
                        break :blk if (tag == .id) .{ .id = payload } else .{ .bytes_val = payload };
                    },
                    .bool_val => unreachable,
                };
            }
            if (offset != bytes.len) return error.InvalidSegment;
            self.current = .{ .ref = .{ .input_idx = std.mem.readInt(u32, bytes[0..4], .little), .doc_id = std.mem.readInt(u32, bytes[4..8], .little) }, .keys = self.keys.? };
        }
    };

    fn appendRecord(self: *@This(), record: SortedMergeRecord) !void {
        var length: usize = 8;
        for (record.keys) |key| length = try std.math.add(usize, length, switch (key) {
            .bool_val => 2,
            .numeric_val => 10,
            .bytes_val => |v| 9 + v.len,
            .id => |v| 9 + v.len,
            else => 9,
        });
        var header: [16]u8 = undefined;
        std.mem.writeInt(u64, header[0..8], length, .little);
        std.mem.writeInt(u32, header[8..12], @intCast(record.ref.input_idx), .little);
        std.mem.writeInt(u32, header[12..16], record.ref.doc_id, .little);
        try self.spool.appendSlice(&header);
        for (record.keys) |key| {
            try self.spool.appendSlice(&.{@backingInt(segmentSortValueTag(key))});
            if (key == .bool_val) {
                try self.spool.appendSlice(&.{@intFromBool(key.bool_val)});
                continue;
            }
            const value: u64 = switch (key) {
                .u64_val => |v| v,
                .i64_val => |v| @bitCast(v),
                .f64_val => |v| @bitCast(v),
                .bytes_val => |v| v.len,
                .id => |v| v.len,
                .numeric_val => |v| blk: {
                    const tag: u8 = switch (v) {
                        .u64_val => 0,
                        .i64_val => 1,
                        .f64_val => 2,
                    };
                    try self.spool.appendSlice(&.{tag});
                    break :blk switch (v) {
                        .u64_val => |n| n,
                        .i64_val => |n| @bitCast(n),
                        .f64_val => |n| @bitCast(n),
                    };
                },
                .bool_val => unreachable,
            };
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, value, .little);
            try self.spool.appendSlice(&encoded);
            switch (key) {
                .bytes_val => |v| try self.spool.appendSlice(v),
                .id => |v| try self.spool.appendSlice(v),
                else => {},
            }
        }
    }
    fn appendEncoded(self: *@This(), bytes: []const u8) !void {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, bytes.len, .little);
        try self.spool.appendSlice(&length);
        try self.spool.appendSlice(bytes);
    }
    fn merge(self: *@This(), ranges: []const Range) !Range {
        var head_bytes: usize = 0;
        for (ranges) |range| head_bytes +|= range.max_record_bytes;
        if (ranges.len > 2 and head_bytes > 256 * 1024) {
            var current = ranges[0];
            for (ranges[1..]) |range| current = try self.merge(&.{ current, range });
            return current;
        }
        const start = self.spool.len();
        var cursors: [4]Cursor = undefined;
        for (ranges, 0..) |range, i| cursors[i] = Cursor.init(self, range);
        defer for (cursors[0..ranges.len]) |*cursor| cursor.deinit();
        for (cursors[0..ranges.len]) |*cursor| try cursor.next();
        while (true) {
            var selected: ?usize = null;
            for (cursors[0..ranges.len], 0..) |cursor, i| if (cursor.current) |record| {
                if (selected == null or sortedMergeRecordLessThan(self.fields, record, cursors[selected.?].current.?)) selected = i;
            };
            const i = selected orelse break;
            try self.appendEncoded(cursors[i].encoded);
            try cursors[i].next();
        }
        try self.spool.seal(start);
        var count: usize = 0;
        var maximum: usize = 0;
        for (ranges) |range| {
            count = try std.math.add(usize, count, range.count);
            maximum = @max(maximum, range.max_record_bytes);
            self.spool.releaseRange(range.start);
        }
        try self.spool.compact();
        return .{ .start = start, .end = self.spool.len(), .count = count, .max_record_bytes = maximum };
    }
    fn push(self: *@This(), records: []SortedMergeRecord) !void {
        if (records.len == 0) return;
        std.sort.pdq(SortedMergeRecord, records, self.fields, sortedMergeRecordLessThan);
        const start = self.spool.len();
        var maximum: usize = 0;
        for (records) |record| {
            const before = self.spool.len();
            try self.appendRecord(record);
            maximum = @max(maximum, self.spool.len() - before);
        }
        try self.spool.seal(start);
        var range = Range{ .start = start, .end = self.spool.len(), .count = records.len, .max_record_bytes = maximum };
        for (&self.levels) |*level| {
            for (level[0 .. self.fan_in - 1]) |*slot| if (slot.* == null) {
                slot.* = range;
                return;
            };
            var ranges: [4]Range = undefined;
            for (level[0 .. self.fan_in - 1], 0..) |*slot, i| {
                ranges[i] = slot.*.?;
                slot.* = null;
            }
            ranges[self.fan_in - 1] = range;
            range = try self.merge(ranges[0..self.fan_in]);
        }
        return error.Overflow;
    }
    fn finish(self: *@This()) !?Range {
        var range: ?Range = null;
        for (&self.levels) |*level| {
            var ranges: [4]Range = undefined;
            var count: usize = 0;
            for (level[0 .. self.fan_in - 1]) |*slot| if (slot.*) |previous| {
                ranges[count] = previous;
                count += 1;
                slot.* = null;
            };
            if (range) |current| {
                ranges[count] = current;
                count += 1;
            }
            range = if (count == 0) null else if (count == 1) ranges[0] else try self.merge(ranges[0..count]);
        }
        return range;
    }
};

fn buildExternalSortedMergePlan(alloc: Allocator, inputs: []const MergeInput, fields: []const SegmentIndexSortField, scratch: MergeScratchOptions) !SortedMergePlan {
    if (scratch.external_sort_chunk_bytes == 0 or scratch.external_sort_chunk_documents == 0 or scratch.external_sort_fan_in < 2 or scratch.external_sort_fan_in > 4) return error.InvalidSegment;
    const file = try FileSortedPlan.create(alloc, inputs, scratch);
    errdefer file.deinit();
    try file.enableUnordered();
    const spool = try FileSortedPlan.Run.createWithResources(alloc, scratch.io, scratch.directory, scratch.resource_manager);
    defer spool.deinit();
    var reads = TypedReadScope.init(alloc);
    defer reads.deinit();
    var sorter = ExternalCoordinateSort{ .allocator = alloc, .spool = spool, .fields = fields, .fan_in = scratch.external_sort_fan_in };
    var chunk = std.ArrayListUnmanaged(SortedMergeRecord).empty;
    defer chunk.deinit(alloc);
    var chunk_payloads = @import("segment_source.zig").Scratch.init(alloc, scratch.external_sort_chunk_bytes);
    defer chunk_payloads.deinit();
    const domains = try alloc.alloc(?SegmentSortValueTag, fields.len);
    defer alloc.free(domains);
    @memset(domains, null);
    var chunk_bytes: usize = 0;
    for (inputs, 0..) |input, input_idx| {
        try validateInputIndexSortMetadata(alloc, input, fields);
        for (0..input.reader.doc_count) |doc| {
            if (input.isDeleted(@intCast(doc))) continue;
            if (chunk.items.len != 0 and (chunk.items.len >= scratch.external_sort_chunk_documents or chunk_bytes >= scratch.external_sort_chunk_bytes)) {
                try sorter.push(chunk.items);
                chunk.clearRetainingCapacity();
                chunk_payloads.reset();
                chunk_bytes = 0;
            }
            const record = try sorterRecord(chunk_payloads.allocator(), &reads, inputs, .{ .input_idx = input_idx, .doc_id = @intCast(doc) }, fields);
            try validateSortKeyDomains(record.keys, domains);
            var bytes: usize = @sizeOf(SortedMergeRecord) + record.keys.len * @sizeOf(SegmentSortValue);
            for (record.keys) |key| bytes += switch (key) {
                .id => |value| value.len,
                .bytes_val => |value| value.len,
                else => 0,
            };
            try chunk.append(alloc, record);
            chunk_bytes += bytes;
        }
    }
    try sorter.push(chunk.items);
    chunk.clearRetainingCapacity();
    chunk_payloads.deinit();
    chunk_payloads = @import("segment_source.zig").Scratch.init(alloc, scratch.external_sort_chunk_bytes);
    const range = try sorter.finish();
    var first: []SegmentSortValue = &.{};
    errdefer deinitSortKeys(alloc, first);
    var last: []SegmentSortValue = &.{};
    errdefer deinitSortKeys(alloc, last);
    if (range) |ordered| {
        var cursor = ExternalCoordinateSort.Cursor.init(&sorter, ordered);
        defer cursor.deinit();
        var position: usize = 0;
        while (true) : (position += 1) {
            try cursor.next();
            const record = cursor.current orelse break;
            if (position == 0) first = try cloneSegmentSortKeysAlloc(alloc, record.keys);
            if (position + 1 == ordered.count) last = try cloneSegmentSortKeysAlloc(alloc, record.keys);
            try file.append(record.ref);
        }
    }
    try file.finish();
    return .{ .file = file, .records = &.{}, .doc_maps = &.{}, .first_keys = first, .last_keys = last };
}

fn sorterRecord(alloc: Allocator, reads: *TypedReadScope, inputs: []const MergeInput, ref: MergeDocRef, fields: []const SegmentIndexSortField) !SortedMergeRecord {
    return .{ .ref = ref, .keys = try loadSegmentSortKeysAlloc(alloc, reads, inputs, ref, fields, null, null, null) };
}

fn buildStreamingSortedMergePlanAlloc(
    alloc: Allocator,
    inputs: []const MergeInput,
    index_sort: []const SegmentIndexSortField,
) !SortedMergePlan {
    return buildStreamingSortedMergePlanWithScratch(alloc, inputs, index_sort, null);
}

fn buildStreamingSortedMergePlanWithScratch(alloc: Allocator, inputs: []const MergeInput, index_sort: []const SegmentIndexSortField, private_scratch: ?MergeScratchOptions) !SortedMergePlan {
    if (index_sort.len == 0) return error.InvalidSegment;
    var coordinate_bytes: u64 = 0;
    for (inputs) |input| {
        const live = @as(u64, input.reader.doc_count) -| if (input.deleted) |deleted| deleted.cardinality() else 0;
        coordinate_bytes = try std.math.add(u64, coordinate_bytes, @as(u64, input.reader.doc_count) * 4 + live * @sizeOf(SortedMergeDoc));
    }
    const file: ?*FileSortedPlan = if (private_scratch) |options| blk: {
        // Files add two 64 KiB run buffers and a 64 KiB mapping cache per
        // input. Require a clear saving over that fan-in-dependent cost.
        // A zero threshold explicitly forces files for tests and callers.
        const file_overhead = try std.math.add(u64, 128 * 1024, try std.math.mul(u64, inputs.len, 68 * 1024));
        const crossover = if (options.in_memory_plan_bytes == 0) 0 else @max(options.in_memory_plan_bytes, try std.math.mul(u64, file_overhead, 2));
        break :blk if (coordinate_bytes > crossover) try FileSortedPlan.create(alloc, inputs, options) else null;
    } else null;
    errdefer if (file) |owner| owner.deinit();
    var records = std.ArrayListUnmanaged(SortedMergeDoc).empty;
    errdefer records.deinit(alloc);
    var first_keys: []SegmentSortValue = &.{};
    errdefer deinitSortKeys(alloc, first_keys);
    var last_keys: []SegmentSortValue = &.{};
    errdefer deinitSortKeys(alloc, last_keys);

    var doc_maps = try alloc.alloc([]u32, if (file == null) inputs.len else 0);
    var doc_maps_initialized: usize = 0;
    errdefer {
        for (doc_maps[0..doc_maps_initialized]) |map| alloc.free(map);
        alloc.free(doc_maps);
    }

    var reads = TypedReadScope.init(alloc);
    defer reads.deinit();
    var streams = StreamingSortReads{ .allocator = alloc, .reads = &reads, .inputs = inputs };
    defer streams.deinit();
    const Scratch = @import("segment_source.zig").Scratch;
    const payloads = try alloc.alloc([2]Scratch, inputs.len);
    for (payloads) |*pair| pair.* = .{ Scratch.init(alloc, 128 * 1024), Scratch.init(alloc, 128 * 1024) };
    defer {
        for (payloads) |*pair| {
            pair[0].deinit();
            pair[1].deinit();
        }
        alloc.free(payloads);
    }
    const slots = try alloc.alloc(u1, inputs.len);
    defer alloc.free(slots);
    @memset(slots, 0);
    const Heap = std.PriorityQueue(SortedMergeRecord, []const SegmentIndexSortField, struct {
        fn compare(fields: []const SegmentIndexSortField, a: SortedMergeRecord, b: SortedMergeRecord) std.math.Order {
            return if (sortedMergeRecordLessThan(fields, a, b)) .lt else if (sortedMergeRecordLessThan(fields, b, a)) .gt else .eq;
        }
    }.compare);
    var heap = Heap.initContext(index_sort);
    defer {
        while (heap.pop()) |entry| {
            // Payloads belong to the per-input scratch owners; the heap owns
            // only its key arrays. Keep fallback records compact as well.
            alloc.free(entry.keys);
        }
        heap.deinit(alloc);
    }
    const domains = try alloc.alloc(?SegmentSortValueTag, index_sort.len);
    defer alloc.free(domains);
    @memset(domains, null);
    const spare_keys = try alloc.alloc(?[]SegmentSortValue, inputs.len);
    @memset(spare_keys, null);
    defer {
        for (spare_keys) |keys| if (keys) |reusable| alloc.free(reusable);
        alloc.free(spare_keys);
    }
    var live_count: usize = 0;
    for (inputs, 0..) |input, input_idx| {
        try validateInputIndexSortMetadata(alloc, input, index_sort);
        if (file == null) {
            const map = try alloc.alloc(u32, input.reader.doc_count);
            @memset(map, std.math.maxInt(u32));
            doc_maps[input_idx] = map;
            doc_maps_initialized += 1;
        }
        var first: ?u32 = null;
        for (0..input.reader.doc_count) |id| {
            if (!input.isDeleted(@intCast(id))) {
                live_count += 1;
                if (first == null) first = @intCast(id);
            }
        }
        if (first) |id| {
            const head = SortedMergeRecord{
                .ref = .{ .input_idx = input_idx, .doc_id = id },
                .keys = try loadSegmentSortKeysAlloc(alloc, &reads, inputs, .{ .input_idx = input_idx, .doc_id = id }, index_sort, null, &streams, payloads[input_idx][0].allocator()),
            };
            errdefer alloc.free(head.keys);
            try validateSortKeyDomains(head.keys, domains);
            try heap.push(alloc, head);
        }
    }
    if (file == null) try records.ensureTotalCapacityPrecise(alloc, live_count);
    while (heap.pop()) |entry| {
        const head = entry;
        var head_owned = true;
        defer if (head_owned) alloc.free(head.keys);
        const out_id = if (file) |owner| owner.count else records.items.len;
        if (out_id == 0) first_keys = try cloneSegmentSortKeysAlloc(alloc, head.keys);
        if (out_id + 1 == live_count) last_keys = try cloneSegmentSortKeysAlloc(alloc, head.keys);
        if (file) |owner| try owner.append(head.ref) else {
            records.appendAssumeCapacity(.{ .ref = head.ref });
            doc_maps[head.ref.input_idx][head.ref.doc_id] = @intCast(out_id);
        }
        const input = inputs[head.ref.input_idx];
        var next = @as(usize, head.ref.doc_id) + 1;
        while (next < input.reader.doc_count and input.isDeleted(@intCast(next))) : (next += 1) {}
        if (next < input.reader.doc_count) {
            const reusable = spare_keys[head.ref.input_idx];
            spare_keys[head.ref.input_idx] = null;
            const slot = slots[head.ref.input_idx] ^ 1;
            const scratch = &payloads[head.ref.input_idx][slot];
            scratch.reset();
            const successor = SortedMergeRecord{
                .ref = .{ .input_idx = head.ref.input_idx, .doc_id = @intCast(next) },
                .keys = try loadSegmentSortKeysAlloc(alloc, &reads, inputs, .{ .input_idx = head.ref.input_idx, .doc_id = @intCast(next) }, index_sort, reusable, &streams, scratch.allocator()),
            };
            errdefer alloc.free(successor.keys);
            try validateSortKeyDomains(successor.keys, domains);
            // Index-sort metadata promises monotonic input; corruption must
            // not silently produce a wrongly sorted output segment.
            if (sortedMergeRecordLessThan(index_sort, successor, head)) return error.NonMonotonicIndexSort;
            try heap.push(alloc, successor);
            slots[head.ref.input_idx] = slot;
            spare_keys[head.ref.input_idx] = head.keys;
            head_owned = false;
        }
    }

    if (file) |owner| try owner.finish();
    return .{
        .file = file,
        .records = try records.toOwnedSlice(alloc),
        .doc_maps = doc_maps,
        .first_keys = first_keys,
        .last_keys = last_keys,
    };
}

fn validateSortKeyDomains(keys: []const SegmentSortValue, expected: []?SegmentSortValueTag) !void {
    if (keys.len != expected.len) return error.InvalidSegment;
    for (keys, expected) |key, *domain| {
        const tag = segmentSortValueTag(key);
        if (domain.*) |existing| {
            if (existing != tag) return error.InvalidSegment;
        } else domain.* = tag;
    }
}

fn cloneSegmentSortKeysAlloc(alloc: Allocator, keys: []const SegmentSortValue) ![]SegmentSortValue {
    const copy = try alloc.alloc(SegmentSortValue, keys.len);
    var initialized: usize = 0;
    errdefer {
        for (copy[0..initialized]) |*key| key.deinit(alloc);
        alloc.free(copy);
    }
    for (keys, copy) |key, *out| {
        out.* = switch (key) {
            .bytes_val => |bytes| .{ .bytes_val = try alloc.dupe(u8, bytes) },
            .id => |id| .{ .id = try alloc.dupe(u8, id) },
            else => key,
        };
        initialized += 1;
    }
    return copy;
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

const StreamingSortReads = struct {
    const Entry = struct {
        input: usize,
        field: []const u8,
        cursor: *typed_dv.TypedDocValuesReader.Cursor,
        current: ?typed_dv.TypedDocValuesReader.DecodedChunk.Entry = null,
    };
    allocator: Allocator,
    reads: *TypedReadScope,
    inputs: []const MergeInput,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    fn deinit(self: *StreamingSortReads) void {
        for (self.entries.items) |entry| {
            entry.cursor.deinit();
            self.allocator.destroy(entry.cursor);
        }
        self.entries.deinit(self.allocator);
    }
    fn key(self: *StreamingSortReads, payload_alloc: Allocator, ref: MergeDocRef, field: []const u8) !SegmentSortValue {
        const input = self.inputs[ref.input_idx];
        if (std.mem.eql(u8, field, "_id")) return loadSegmentSortKeyAlloc(payload_alloc, self.reads, input.reader, ref.doc_id, field);
        const reader = (try self.reads.get(input.reader, field)) orelse return error.UnsupportedTypedDocValues;
        const live = @as(usize, input.reader.doc_count) -| if (input.deleted) |deleted| deleted.cardinality() else 0;
        if (live <= reader.num_chunks) return loadSegmentSortKeyAlloc(payload_alloc, self.reads, input.reader, ref.doc_id, field);
        var found: ?*Entry = null;
        for (self.entries.items) |*entry| if (entry.input == ref.input_idx and std.mem.eql(u8, entry.field, field)) {
            found = entry;
            break;
        };
        if (found == null) {
            const cursor = try self.allocator.create(typed_dv.TypedDocValuesReader.Cursor);
            errdefer self.allocator.destroy(cursor);
            cursor.* = typed_dv.TypedDocValuesReader.Cursor.init(reader);
            errdefer cursor.deinit();
            try self.entries.append(self.allocator, .{ .input = ref.input_idx, .field = field, .cursor = cursor });
            found = &self.entries.items[self.entries.items.len - 1];
        }
        const entry = found.?;
        while (entry.current == null or entry.current.?.doc_id < ref.doc_id) entry.current = (try segmentSortDocValue(entry.cursor.next())) orelse return error.InvalidSegment;
        if (entry.current.?.doc_id != ref.doc_id) return error.InvalidSegment;
        return switch (entry.current.?.value) {
            .u64_val => |v| .{ .numeric_val = .{ .u64_val = v } },
            .i64_val => |v| .{ .numeric_val = .{ .i64_val = v } },
            .f64_val => |v| if (std.math.isFinite(v)) .{ .numeric_val = .{ .f64_val = v } } else error.InvalidSegment,
            .numeric_val => |v| .{ .numeric_val = v },
            .bool_val => |v| .{ .bool_val = v },
            .bytes_val => |v| .{ .bytes_val = try payload_alloc.dupe(u8, v) },
            .geo_point => error.UnsupportedTypedDocValues,
        };
    }
};

fn loadSegmentSortKeysAlloc(
    alloc: Allocator,
    reads: *TypedReadScope,
    inputs: []const MergeInput,
    ref: MergeDocRef,
    index_sort: []const SegmentIndexSortField,
    reusable: ?[]SegmentSortValue,
    streams: ?*StreamingSortReads,
    payload_allocator: ?Allocator,
) ![]SegmentSortValue {
    const payload_alloc = payload_allocator orelse alloc;
    const keys = reusable orelse try alloc.alloc(SegmentSortValue, index_sort.len);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |*key| key.deinit(payload_alloc);
        alloc.free(keys);
    }
    for (index_sort, 0..) |field, i| {
        keys[i] = if (streams) |stream| try stream.key(payload_alloc, ref, field.field) else try loadSegmentSortKeyAlloc(payload_alloc, reads, inputs[ref.input_idx].reader, ref.doc_id, field.field);
        initialized += 1;
    }
    return keys;
}

fn loadSegmentSortKeyAlloc(
    alloc: Allocator,
    reads: *TypedReadScope,
    reader: *const SegmentReader,
    doc_id: u32,
    field: []const u8,
) !SegmentSortValue {
    if (std.mem.eql(u8, field, "_id")) {
        return .{ .id = (try reader.storedIdAlloc(alloc, doc_id)) orelse return error.InvalidSegment };
    }
    const dv_reader = (try reads.get(reader, field)) orelse return error.UnsupportedTypedDocValues;
    return switch (dv_reader.value_type) {
        // Normalize legacy scalar numeric columns into the exact tagged
        // domain before comparing sort keys. This keeps sorted compaction
        // valid across an on-disk format rollout without rounding integers.
        .u64_val => .{ .numeric_val = .{ .u64_val = (try segmentSortDocValue(dv_reader.getU64(doc_id))) orelse return error.InvalidSegment } },
        .i64_val => .{ .numeric_val = .{ .i64_val = (try segmentSortDocValue(dv_reader.getI64(doc_id))) orelse return error.InvalidSegment } },
        .f64_val => blk: {
            const value = (try segmentSortDocValue(dv_reader.getF64(doc_id))) orelse return error.InvalidSegment;
            if (!std.math.isFinite(value)) return error.InvalidSegment;
            break :blk .{ .numeric_val = .{ .f64_val = value } };
        },
        .bool_val => .{ .bool_val = (try segmentSortDocValue(dv_reader.getBool(doc_id))) orelse return error.InvalidSegment },
        .bytes_val => .{ .bytes_val = (try segmentSortDocValue(dv_reader.getBytesAllocWithAllocator(alloc, doc_id))) orelse return error.InvalidSegment },
        .geo_point => return error.UnsupportedTypedDocValues,
        .numeric_val => .{ .numeric_val = (try segmentSortDocValue(dv_reader.getNumeric(doc_id))) orelse return error.InvalidSegment },
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
        .numeric_val => |av| switch (b) {
            .numeric_val => |bv| typed_dv.compareNumericValues(av, bv),
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
    if (plan.ordered().len == 0) return null;
    const first = try segmentBoundValuesFromSortValuesAlloc(alloc, plan.first_keys);
    errdefer {
        for (first) |*value| value.deinit(alloc);
        alloc.free(first);
    }
    return .{
        .first = first,
        .last = try segmentBoundValuesFromSortValuesAlloc(alloc, plan.last_keys),
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
        .numeric_val => |v| switch (v) {
            .u64_val => |number| .{ .u64_val = number },
            .i64_val => |number| .{ .i64_val = number },
            .f64_val => |number| .{ .f64_val = number },
        },
    };
}

fn writeMergeTypedDocValuesSections(
    alloc: Allocator,
    sink: *SegmentSink,
    inputs: []const MergeInput,
    field_name: []const u8,
    rank_maps: ?[]const inverted.RankDocMap,
) !bool {
    if (rank_maps) |maps| if (maps.len != inputs.len) return error.InvalidSegment;
    var value_type: ?typed_dv.ValueType = null;

    for (inputs) |input| {
        var reader = (try input.reader.typedDocValuesScoped(alloc, field_name)) orelse continue;
        defer reader.deinit();
        value_type = mergeTypedDocValuesValueType(value_type, reader.value_type) orelse return false;
    }
    const target_value_type = value_type orelse return false;
    var writer = typed_dv.StreamingWriter.init(alloc, sink, target_value_type);
    defer writer.deinit();

    var merged_doc_base: u32 = 0;
    for (inputs, 0..) |input, input_idx| {
        const reader = input.reader;
        var dv_reader = try reader.typedDocValuesScoped(alloc, field_name);
        defer if (dv_reader) |*dv| dv.deinit();

        if (dv_reader) |*dv| {
            var cursor = typed_dv.TypedDocValuesReader.Cursor.init(dv);
            defer cursor.deinit();
            while (try cursor.next()) |entry| {
                if (entry.doc_id >= reader.doc_count) return error.InvalidSegment;
                if (input.isDeleted(entry.doc_id)) continue;
                const deleted_before: u32 = if (rank_maps) |maps|
                    if (maps[input_idx].rank_index) |rank| @intCast(rank.rank(entry.doc_id)) else 0
                else if (input.deleted) |deleted|
                    @intCast(deleted.rank(entry.doc_id))
                else
                    0;
                try addMergedTypedDocValue(&writer, merged_doc_base + entry.doc_id - deleted_before, entry.value);
            }
        }

        const deleted_live_range: u32 = if (input.deleted) |deleted|
            @intCast(deleted.rank(reader.doc_count))
        else
            0;
        merged_doc_base += reader.doc_count - deleted_live_range;
    }

    return try writer.finish();
}

fn typedDocValuesValueTypeIsNumeric(value_type: typed_dv.ValueType) bool {
    return switch (value_type) {
        .u64_val, .i64_val, .f64_val, .numeric_val => true,
        else => false,
    };
}

fn mergeTypedDocValuesValueType(current: ?typed_dv.ValueType, next: typed_dv.ValueType) ?typed_dv.ValueType {
    const existing = current orelse return next;
    if (existing == next) return existing;
    if (typedDocValuesValueTypeIsNumeric(existing) and typedDocValuesValueTypeIsNumeric(next)) return .numeric_val;
    return null;
}

fn addMergedTypedDocValue(writer: anytype, doc_id: u32, value: typed_dv.TypedValue) !void {
    if (writer.value_type != .numeric_val) return writer.add(doc_id, value);
    const numeric: typed_dv.NumericValue = switch (value) {
        .u64_val => |number| .{ .u64_val = number },
        .i64_val => |number| .{ .i64_val = number },
        .f64_val => |number| .{ .f64_val = number },
        .numeric_val => |number| number,
        else => return error.InvalidSegment,
    };
    return writer.add(doc_id, .{ .numeric_val = numeric });
}

fn writeMergeTypedDocValuesSectionsInOrder(
    alloc: Allocator,
    sink: *SegmentSink,
    inputs: []const MergeInput,
    field_name: []const u8,
    records_arg: anytype,
    decoded_chunks: ?*usize,
) !bool {
    const records = asSortedRecords(records_arg);
    var reads = TypedReadScope.init(alloc);
    defer reads.deinit();
    const readers = try alloc.alloc(?*typed_dv.TypedDocValuesReader, inputs.len);
    defer alloc.free(readers);
    @memset(readers, null);
    var value_type: ?typed_dv.ValueType = null;

    for (inputs, 0..) |input, i| {
        readers[i] = try reads.get(input.reader, field_name);
        if (readers[i] == null) continue;
        value_type = mergeTypedDocValuesValueType(value_type, readers[i].?.value_type) orelse return false;
    }
    const vt = value_type orelse return false;

    const Scan = struct {
        cursor: ?typed_dv.TypedDocValuesReader.Cursor = null,
        current: ?typed_dv.TypedDocValuesReader.DecodedChunk.Entry = null,
        last: ?u32 = null,
        monotonic: bool = true,
        selected: usize = 0,
    };
    const scans = try alloc.alloc(Scan, inputs.len);
    defer alloc.free(scans);
    for (scans) |*scan| scan.* = .{};
    defer for (scans) |*scan| if (scan.cursor) |*cursor| cursor.deinit();
    var record_iterator_1 = records.iterator();
    while (try record_iterator_1.next()) |record| {
        const scan = &scans[record.ref.input_idx];
        if (scan.last) |last| if (record.ref.doc_id <= last) {
            scan.monotonic = false;
        };
        scan.last = record.ref.doc_id;
        scan.selected += 1;
    }
    for (scans, readers) |*scan, reader| if (reader) |r| {
        // Sparse deletion survivors should seek rather than scan a whole
        // column. Dense monotonic inputs amortize one decode per chunk.
        if (scan.monotonic and scan.selected > r.num_chunks) scan.cursor = typed_dv.TypedDocValuesReader.Cursor.init(r);
    };
    var writer = typed_dv.StreamingWriter.init(alloc, sink, vt);
    defer writer.deinit();
    const gather_limit = 256 * 1024;
    var gather = @import("segment_source.zig").Scratch.init(alloc, gather_limit);
    defer gather.deinit();
    var needs_gather = false;
    for (scans) |scan| if (!scan.monotonic) {
        needs_gather = true;
        break;
    };
    var base: usize = 0;
    while (base < records.len) {
        var record_buffer: [64]SortedMergeDoc = undefined;
        const batch = try records.batch(base, &record_buffer);
        var values: [64]?typed_dv.TypedValue = undefined;
        var gathered = true;
        if (needs_gather) {
            gather.reset();
            @memset(&values, null);
            var order: [64]usize = undefined;
            for (order[0..batch.len], 0..) |*slot, i| slot.* = i;
            std.mem.sort(usize, order[0..batch.len], batch, struct {
                fn less(rows: []const SortedMergeDoc, left: usize, right: usize) bool {
                    const l = rows[left].ref;
                    const r = rows[right].ref;
                    return if (l.input_idx != r.input_idx) l.input_idx < r.input_idx else l.doc_id < r.doc_id;
                }
            }.less);
            var used: usize = 0;
            // Reordered inputs gather adjacent chunk lookups together. Only these
            // inputs borrow the cache; forward cursors remain untouched until emit.
            for (order[0..batch.len]) |slot| {
                const ref = batch[slot].ref;
                if (scans[ref.input_idx].monotonic) continue;
                const reader = readers[ref.input_idx] orelse continue;
                var value = (try typedDocValueBorrowed(reader, ref.doc_id)) orelse continue;
                if (value == .bytes_val) {
                    if (value.bytes_val.len > gather_limit - used) {
                        gathered = false;
                        break;
                    }
                    used += value.bytes_val.len;
                    value.bytes_val = try gather.allocator().dupe(u8, value.bytes_val);
                }
                values[slot] = value;
            }
        }
        if (!gathered) gather.reset();
        for (batch, 0..) |record, slot| {
            const out_doc_id: u32 = @intCast(base + slot);
            const reader = readers[record.ref.input_idx] orelse continue;
            const scan = &scans[record.ref.input_idx];
            if (scan.cursor) |*cursor| {
                if (scan.current == null) scan.current = try cursor.next();
                while (scan.current != null and scan.current.?.doc_id < record.ref.doc_id) scan.current = try cursor.next();
                if (scan.current) |entry| if (entry.doc_id == record.ref.doc_id) {
                    try addMergedTypedDocValue(&writer, out_doc_id, entry.value);
                };
            } else if (!scan.monotonic and gathered) {
                if (values[slot]) |value| try addMergedTypedDocValue(&writer, out_doc_id, value);
            } else try addTypedDocValueIfPresent(alloc, &writer, reader, record.ref.doc_id, out_doc_id);
        }
        base += batch.len;
    }
    if (decoded_chunks) |count| {
        count.* = if (reads.cache) |cache| cache.decode_count else 0;
        for (scans) |scan| if (scan.cursor) |cursor| {
            count.* += cursor.next_chunk;
        };
    }
    return try writer.finish();
}

/// Byte values borrow the merge scope's cache until the next lookup.
fn typedDocValueBorrowed(reader: *const typed_dv.TypedDocValuesReader, doc: u32) !?typed_dv.TypedValue {
    return switch (reader.value_type) {
        .u64_val => if (try reader.getU64(doc)) |v| .{ .u64_val = v } else null,
        .i64_val => if (try reader.getI64(doc)) |v| .{ .i64_val = v } else null,
        .f64_val => if (try reader.getF64(doc)) |v| .{ .f64_val = v } else null,
        .geo_point => if (try reader.getGeoPoint(doc)) |v| .{ .geo_point = v } else null,
        .bool_val => if (try reader.getBool(doc)) |v| .{ .bool_val = v } else null,
        .bytes_val => if (try reader.getBytesBorrowed(doc)) |v| .{ .bytes_val = v } else null,
        .numeric_val => if (try reader.getNumeric(doc)) |v| .{ .numeric_val = v } else null,
    };
}

fn addTypedDocValueIfPresent(
    alloc: Allocator,
    writer: anytype,
    reader: *const typed_dv.TypedDocValuesReader,
    src_doc_id: u32,
    out_doc_id: u32,
) !void {
    switch (reader.value_type) {
        .u64_val => if (try reader.getU64(src_doc_id)) |value| {
            try addMergedTypedDocValue(writer, out_doc_id, .{ .u64_val = value });
        },
        .i64_val => if (try reader.getI64(src_doc_id)) |value| {
            try addMergedTypedDocValue(writer, out_doc_id, .{ .i64_val = value });
        },
        .f64_val => if (try reader.getF64(src_doc_id)) |value| {
            try addMergedTypedDocValue(writer, out_doc_id, .{ .f64_val = value });
        },
        .geo_point => if (try reader.getGeoPoint(src_doc_id)) |value| {
            try writer.add(out_doc_id, .{ .geo_point = value });
        },
        .bool_val => if (try reader.getBool(src_doc_id)) |value| {
            try writer.add(out_doc_id, .{ .bool_val = value });
        },
        .bytes_val => {
            if (reader.point_cache != null) {
                if (try reader.getBytesBorrowed(src_doc_id)) |value| try writer.add(out_doc_id, .{ .bytes_val = value });
            } else if (try reader.getBytesAllocWithAllocator(alloc, src_doc_id)) |value| {
                defer alloc.free(value);
                try writer.add(out_doc_id, .{ .bytes_val = value });
            }
        },
        .numeric_val => if (try reader.getNumeric(src_doc_id)) |value| {
            try addMergedTypedDocValue(writer, out_doc_id, .{ .numeric_val = value });
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
        if (!inputHasLiveDocs(input) or !(try input.reader.storedFieldsOmitted())) continue;
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

const OrdinalOutput = struct {
    bytes: [16 * 1024]u8 = undefined,
    used: usize = 0,
    count: u32 = 0,
    fn add(self: *@This(), sink: *SegmentSink, ordinal: u32) !void {
        std.mem.writeInt(u32, self.bytes[self.used..][0..4], ordinal, .big);
        self.used += 4;
        self.count += 1;
        if (self.used == self.bytes.len) try self.flush(sink);
    }
    fn flush(self: *@This(), sink: *SegmentSink) !void {
        try sink.appendSlice(self.bytes[0..self.used]);
        self.used = 0;
    }
};
fn writeMergedDocOrdinals(sink: *SegmentSink, inputs: []const MergeInput, records: ?SortedRecords, doc_count: u32) !bool {
    var present = false;
    if (records) |ordered| {
        var it = ordered.iterator();
        while (try it.next()) |record| {
            if (((try inputs[record.ref.input_idx].reader.docOrdinal(record.ref.doc_id)) orelse 0) != 0) {
                present = true;
                break;
            }
        }
    } else outer: for (inputs) |input| {
        for (0..input.reader.doc_count) |doc| {
            if (input.isDeleted(@intCast(doc))) continue;
            if (((try input.reader.docOrdinal(@intCast(doc))) orelse 0) != 0) {
                present = true;
                break :outer;
            }
        }
    }
    if (!present) return false;
    try sink.appendByte(1);
    try sinkAppendU32BE(sink, doc_count);
    var output = OrdinalOutput{};
    if (records) |ordered| {
        var it = ordered.iterator();
        while (try it.next()) |record| try output.add(sink, (try inputs[record.ref.input_idx].reader.docOrdinal(record.ref.doc_id)) orelse 0);
    } else for (inputs) |input| {
        for (0..input.reader.doc_count) |doc| {
            if (input.isDeleted(@intCast(doc))) continue;
            try output.add(sink, (try input.reader.docOrdinal(@intCast(doc))) orelse 0);
        }
    }
    if (output.count != doc_count) return error.InvalidSegment;
    try output.flush(sink);
    return true;
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
    try out.appendSlice(alloc, &@as([2]u8, @bitCast(@as(u16, val))));
}

fn sinkAppendU16LE(sink: *SegmentSink, val: u16) !void {
    try sink.appendSlice(&@as([2]u8, @bitCast(@as(u16, val))));
}

fn appendU32LE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, val))));
}

fn sinkAppendU32LE(sink: *SegmentSink, val: u32) !void {
    try sink.appendSlice(&@as([4]u8, @bitCast(@as(u32, val))));
}

fn appendU64LE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u64) !void {
    try out.appendSlice(alloc, &@as([8]u8, @bitCast(@as(u64, val))));
}

fn sinkAppendU64LE(sink: *SegmentSink, val: u64) !void {
    try sink.appendSlice(&@as([8]u8, @bitCast(@as(u64, val))));
}

fn appendU16BE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u16) !void {
    try out.appendSlice(alloc, &@as([2]u8, @bitCast(@byteSwap(@as(u16, val)))));
}

fn sinkAppendU16BE(sink: *SegmentSink, val: u16) !void {
    try sink.appendSlice(&@as([2]u8, @bitCast(@byteSwap(@as(u16, val)))));
}

fn appendU32BE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@byteSwap(@as(u32, val)))));
}

fn sinkAppendU32BE(sink: *SegmentSink, val: u32) !void {
    try sink.appendSlice(&@as([4]u8, @bitCast(@byteSwap(@as(u32, val)))));
}

fn appendU64BE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u64) !void {
    try out.appendSlice(alloc, &@as([8]u8, @bitCast(@byteSwap(@as(u64, val)))));
}

fn sinkAppendU64BE(sink: *SegmentSink, val: u64) !void {
    try sink.appendSlice(&@as([8]u8, @bitCast(@byteSwap(@as(u64, val)))));
}

fn writePageDirectory(sink: *SegmentSink) !integrity.Directory {
    if (sink.vtable.page_directory) |directory| return directory(sink.ptr);
    const data_end = sink.len();
    var offset: usize = 0;
    while (offset < data_end) : (offset += integrity.page_size) {
        try sinkAppendU32BE(sink, try sink.crc32Range(offset, @min(integrity.page_size, data_end - offset)));
    }
    return .{ .offset = data_end, .length = sink.len() - data_end, .checksum = try sink.crc32Range(data_end, sink.len() - data_end) };
}

fn writePageDirectoryDescriptor(sink: *SegmentSink, directory: integrity.Directory) !void {
    try sinkAppendU64BE(sink, directory.offset);
    try sinkAppendU64BE(sink, directory.length);
    try sinkAppendU32BE(sink, directory.checksum);
}

fn writeSegmentVersionChecksumAndMagic(sink: *SegmentSink, metadata_offset: usize) !void {
    try sinkAppendU32BE(sink, segment_version);
    const checksum = try sink.crc32Range(metadata_offset, sink.len() - metadata_offset);
    try sinkAppendU32BE(sink, checksum);
    try sink.appendSlice(&magic);
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
    const doc0 = (try reader.storedDocDecompressed(alloc, 0)) orelse return error.TestExpectedEqual;
    defer alloc.free(doc0.data);
    try std.testing.expectEqualStrings("doc-1", doc0.id);
    try std.testing.expectEqualStrings("Hello world", doc0.data);

    const doc1 = (try reader.storedDocDecompressed(alloc, 1)) orelse return error.TestExpectedEqual;
    defer alloc.free(doc1.data);
    try std.testing.expectEqualStrings("doc-2", doc1.id);
}

test "segment readers reject stored metadata corruption at admission" {
    const alloc = std.testing.allocator;
    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();
    try writer.addStoredDoc("doc-1", "integrity matters");

    const encoded = try writer.build();
    defer alloc.free(encoded);
    const corrupted = try alloc.dupe(u8, encoded);
    defer alloc.free(corrupted);
    corrupted[0] ^= 0x40;

    try std.testing.expectError(error.CrcMismatch, SegmentReader.init(alloc, corrupted));
}

test "segment readers lazily reject stored block corruption and memoize failure" {
    const alloc = std.testing.allocator;
    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();
    try writer.addStoredDoc("doc-1", "integrity matters");

    const encoded = try writer.build();
    defer alloc.free(encoded);
    const corrupted = try alloc.dupe(u8, encoded);
    defer alloc.free(corrupted);
    var original_reader = try SegmentReader.init(alloc, corrupted);
    const location = (try original_reader.v4StoredDocLocation(0)) orelse
        return error.TestExpectedEqual;
    original_reader.deinit();
    corrupted[location.block_start] ^= 0x40;

    var reader = try SegmentReader.init(alloc, corrupted);
    defer reader.deinit();
    try std.testing.expect((try reader.storedDoc(0)) != null);
    try std.testing.expectError(error.CrcMismatch, reader.storedDocDecompressed(alloc, 0));
    try std.testing.expectError(error.CrcMismatch, reader.storedDocDecompressed(alloc, 0));
}

test "segment readers lazily reject section corruption and memoize failure" {
    const alloc = std.testing.allocator;
    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();
    try writer.addStoredDoc("doc-1", "source");
    const field_idx = try writer.addField("content");
    try writer.addSection(field_idx, .inverted_text, "immutable section payload");

    const encoded = try writer.build();
    defer alloc.free(encoded);
    const corrupted = try alloc.dupe(u8, encoded);
    defer alloc.free(corrupted);
    var original_reader = try SegmentReader.init(alloc, corrupted);
    const section_offset: usize = @intCast(original_reader.fields[0].sections[0].offset);
    original_reader.deinit();
    corrupted[section_offset] ^= 0x40;

    var reader = try SegmentReader.init(alloc, corrupted);
    defer reader.deinit();
    try std.testing.expectError(error.CrcMismatch, reader.getSection("content", .inverted_text));
    try std.testing.expectError(error.CrcMismatch, reader.getSection("content", .inverted_text));
}

test "segment readers reject corrupt structural metadata at admission" {
    const alloc = std.testing.allocator;
    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();
    try writer.addStoredDoc("doc-1", "source");

    const encoded = try writer.build();
    defer alloc.free(encoded);
    const corrupted = try alloc.dupe(u8, encoded);
    defer alloc.free(corrupted);
    const metadata_offset: usize = @intCast(std.mem.readInt(u64, corrupted[corrupted.len - 24 ..][0..8], .big));
    corrupted[metadata_offset] ^= 0x40;

    try std.testing.expectError(error.CrcMismatch, SegmentReader.init(alloc, corrupted));
}

test "segment readers fail closed on checksummed malformed document offsets" {
    const alloc = std.testing.allocator;
    var writer = SegmentWriter.init(alloc);
    defer writer.deinit();
    try writer.addStoredDoc("doc-1", "source");

    const encoded = try writer.build();
    defer alloc.free(encoded);
    const malformed = try alloc.dupe(u8, encoded);
    defer alloc.free(malformed);
    const end = malformed.len;
    const stored_start: usize = @intCast(std.mem.readInt(u64, malformed[end - 48 ..][0..8], .big));
    const stored_metadata_len: usize = @intCast(std.mem.readInt(u64, malformed[end - 32 ..][0..8], .big));
    const sections_index_offset: usize = @intCast(std.mem.readInt(u64, malformed[end - 24 ..][0..8], .big));

    // The first document's id offset lives at the beginning of the v4
    // document table. Recompute both envelope checksums to exercise structural
    // validation independently from accidental-corruption detection.
    std.mem.writeInt(u64, malformed[stored_start + 21 ..][0..8], std.math.maxInt(u64), .little);
    std.mem.writeInt(
        u32,
        malformed[end - 16 ..][0..4],
        Crc32.hash(malformed[stored_start..][0..stored_metadata_len]),
        .big,
    );
    std.mem.writeInt(
        u32,
        malformed[end - 8 ..][0..4],
        Crc32.hash(malformed[sections_index_offset .. end - 8]),
        .big,
    );

    var reader = try SegmentReader.init(alloc, malformed);
    defer reader.deinit();
    try std.testing.expectError(error.InvalidSegment, reader.storedDoc(0));
}

test "segment stored document decompression honors the caller allocator" {
    var reader_gpa: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .canary = 0x53454701 });
    defer std.debug.assert(reader_gpa.deinit() == 0);
    const reader_alloc = reader_gpa.allocator();

    var output_gpa: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .canary = 0x53454702 });
    defer std.debug.assert(output_gpa.deinit() == 0);
    const output_alloc = output_gpa.allocator();

    var writer = SegmentWriter.init(reader_alloc);
    defer writer.deinit();
    try writer.addStoredDoc("doc-1", "allocator-owned output");

    const segment = try writer.build();
    defer reader_alloc.free(segment);
    var reader = try SegmentReader.init(reader_alloc, segment);
    defer reader.deinit();

    const stored = (try reader.storedDocDecompressed(output_alloc, 0)) orelse return error.TestExpectedEqual;
    defer output_alloc.free(stored.data);
    try std.testing.expectEqualStrings("allocator-owned output", stored.data);
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
    const data = @as([64]u8, @splat(0));
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
    try std.testing.expect((try reader.storedDoc(0)) != null);
    try std.testing.expect((try reader.storedDoc(1)) != null);
    try std.testing.expect((try reader.storedDoc(2)) != null);
    try std.testing.expect((try reader.storedDoc(3)) == null);
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

    var tenant_reader = try typed_dv.TypedDocValuesReader.init(alloc, (try merged_reader.getSection("tenant", .typed_doc_values)) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(typed_dv.ValueType.bytes_val, tenant_reader.value_type);

    const tenant0 = (try tenant_reader.getBytesAlloc(0)) orelse return error.TestExpectedEqual;
    defer alloc.free(tenant0);
    const tenant1_value = (try tenant_reader.getBytesAlloc(1)) orelse return error.TestExpectedEqual;
    defer alloc.free(tenant1_value);

    try std.testing.expectEqualStrings("acme", tenant0);
    try std.testing.expectEqualStrings("beta", tenant1_value);
}

test "segment append merge normalizes legacy mixed numeric doc values" {
    const alloc = std.testing.allocator;

    var integer_writer = typed_dv.TypedDocValuesWriter.init(alloc, .i64_val, 1024);
    defer integer_writer.deinit();
    try integer_writer.add(0, .{ .i64_val = -9007199254740993 });
    const integer_data = try integer_writer.build();
    defer alloc.free(integer_data);
    var first_writer = SegmentWriter.init(alloc);
    defer first_writer.deinit();
    const first_field = try first_writer.addField("price");
    try first_writer.addSection(first_field, .typed_doc_values, integer_data);
    try first_writer.addStoredDoc("doc:integer", "{}");
    const first_segment = try first_writer.build();
    defer alloc.free(first_segment);

    var decimal_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 1024);
    defer decimal_writer.deinit();
    try decimal_writer.add(0, .{ .f64_val = 10.5 });
    const decimal_data = try decimal_writer.build();
    defer alloc.free(decimal_data);
    var second_writer = SegmentWriter.init(alloc);
    defer second_writer.deinit();
    const second_field = try second_writer.addField("price");
    try second_writer.addSection(second_field, .typed_doc_values, decimal_data);
    try second_writer.addStoredDoc("doc:decimal", "{}");
    const second_segment = try second_writer.build();
    defer alloc.free(second_segment);

    const merged = try mergeSegments(alloc, &.{ first_segment, second_segment });
    defer alloc.free(merged);
    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    var values = try typed_dv.TypedDocValuesReader.init(alloc, (try merged_reader.getSection("price", .typed_doc_values)) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(typed_dv.ValueType.numeric_val, values.value_type);
    try std.testing.expectEqual(typed_dv.NumericValue{ .i64_val = -9007199254740993 }, (try values.getNumeric(0)).?);
    try std.testing.expectEqual(typed_dv.NumericValue{ .f64_val = 10.5 }, (try values.getNumeric(1)).?);
}

test "segment append merge remaps sparse multi-chunk typed doc values around deletions" {
    const alloc = std.testing.allocator;

    var first_values = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 2);
    defer first_values.deinit();
    try first_values.add(0, .{ .u64_val = 10 });
    try first_values.add(2, .{ .u64_val = 20 });
    try first_values.add(4, .{ .u64_val = 40 });
    const first_values_data = try first_values.build();
    defer alloc.free(first_values_data);

    var sw1 = SegmentWriter.init(alloc);
    defer sw1.deinit();
    const first_field = try sw1.addField("ordinal");
    try sw1.addSection(first_field, .typed_doc_values, first_values_data);
    for (0..5) |doc_id| {
        var id_buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "first:{d}", .{doc_id});
        try sw1.addStoredDoc(id, "{}");
    }
    const first_segment = try sw1.build();
    defer alloc.free(first_segment);

    var second_values = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1);
    defer second_values.deinit();
    try second_values.add(0, .{ .u64_val = 50 });
    try second_values.add(2, .{ .u64_val = 70 });
    const second_values_data = try second_values.build();
    defer alloc.free(second_values_data);

    var sw2 = SegmentWriter.init(alloc);
    defer sw2.deinit();
    const second_field = try sw2.addField("ordinal");
    try sw2.addSection(second_field, .typed_doc_values, second_values_data);
    for (0..3) |doc_id| {
        var id_buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "second:{d}", .{doc_id});
        try sw2.addStoredDoc(id, "{}");
    }
    const second_segment = try sw2.build();
    defer alloc.free(second_segment);

    var first_reader = try SegmentReader.init(alloc, first_segment);
    defer first_reader.deinit();
    var second_reader = try SegmentReader.init(alloc, second_segment);
    defer second_reader.deinit();

    var first_deleted = roaring.RoaringBitmap.init(alloc);
    defer first_deleted.deinit();
    try first_deleted.add(1);
    try first_deleted.add(4);
    var second_deleted = roaring.RoaringBitmap.init(alloc);
    defer second_deleted.deinit();
    try second_deleted.add(1);

    const merged = try mergeSegmentInputs(alloc, &.{
        .{ .reader = &first_reader, .deleted = first_deleted },
        .{ .reader = &second_reader, .deleted = second_deleted },
    });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    try std.testing.expectEqual(@as(u32, 5), merged_reader.doc_count);
    var values = try typed_dv.TypedDocValuesReader.init(alloc, (try merged_reader.getSection("ordinal", .typed_doc_values)) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(@as(?u64, 10), try values.getU64(0));
    try std.testing.expectEqual(@as(?u64, 20), try values.getU64(1));
    try std.testing.expectEqual(@as(?u64, null), try values.getU64(2));
    try std.testing.expectEqual(@as(?u64, 50), try values.getU64(3));
    try std.testing.expectEqual(@as(?u64, 70), try values.getU64(4));
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
    try std.testing.expect(try reader.storedFieldsOmitted());
    try std.testing.expect((try reader.storedDoc(0)) == null);
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

    try std.testing.expect(try reader.storedDocsAreCompressed());
    try std.testing.expect(writer.last_stored_raw_bytes > writer.last_stored_compressed_bytes);
    try std.testing.expectEqual(@as(u32, @intCast(stored_fields_block_doc_target + 7)), reader.doc_count);

    const first_ref = (try reader.storedDoc(0)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("doc-0", first_ref.id);
    try std.testing.expectEqual(@as(usize, 0), first_ref.data.len);

    const boundary = (try reader.storedDocDecompressed(alloc, @intCast(stored_fields_block_doc_target))) orelse return error.TestExpectedEqual;
    defer alloc.free(boundary.data);
    try std.testing.expectEqualStrings("doc-128", boundary.id);
    try std.testing.expect(std.mem.indexOf(u8, boundary.data, "\"ordinal\":128") != null);

    const last_doc_id: u32 = @intCast(stored_fields_block_doc_target + 6);
    const last = (try reader.storedDocDecompressed(alloc, last_doc_id)) orelse return error.TestExpectedEqual;
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

    const loc0 = (try reader.v4StoredDocLocation(0)) orelse return error.TestExpectedEqual;
    const loc1 = (try reader.v4StoredDocLocation(1)) orelse return error.TestExpectedEqual;
    const loc2 = (try reader.v4StoredDocLocation(2)) orelse return error.TestExpectedEqual;

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
    const src_loc = (try r1.v4StoredDocLocation(0)) orelse return error.TestExpectedEqual;
    const src_block = r1.data[src_loc.block_start..src_loc.block_end];

    const merged = try mergeSegments(alloc, &.{ seg1, seg2 });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    const merged_loc = (try merged_reader.v4StoredDocLocation(0)) orelse return error.TestExpectedEqual;
    const merged_block = merged_reader.data[merged_loc.block_start..merged_loc.block_end];
    try std.testing.expectEqualSlices(u8, src_block, merged_block);

    const last_copied = (try merged_reader.storedDocDecompressed(alloc, @intCast(stored_fields_block_doc_target - 1))) orelse return error.TestExpectedEqual;
    defer alloc.free(last_copied.data);
    try std.testing.expectEqualStrings("a-127", last_copied.id);

    const tail = (try merged_reader.storedDocDecompressed(alloc, @intCast(stored_fields_block_doc_target))) orelse return error.TestExpectedEqual;
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
    try std.testing.expectEqualStrings("doc:a", (try merged_reader.storedDoc(0)).?.id);
    try std.testing.expectEqualStrings("doc:b", (try merged_reader.storedDoc(1)).?.id);
    try std.testing.expectEqualStrings("doc:c", (try merged_reader.storedDoc(2)).?.id);
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

    var price_reader = try typed_dv.TypedDocValuesReader.init(alloc, (try merged_reader.getSection("price", .typed_doc_values)) orelse return error.TestExpectedEqual);
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
    try chunk_data.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 1))));
    try chunk_data.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, doc_id))));
    try chunk_data.appendSlice(alloc, &@as([8]u8, @bitCast(value)));

    const compressed = try snappy.encode(alloc, chunk_data.items);
    defer alloc.free(compressed);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, @backingInt(typed_dv.ValueType.f64_val));
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(@as(u32, 1))));
    const offset_pos = out.items.len;
    try out.appendNTimes(alloc, 0, 8);
    try out.appendSlice(alloc, compressed);
    out.items[offset_pos..][0..8].* = @bitCast(@as(u64, @as(u64, @intCast(out.items.len))));
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

test "segment sorted merge normalizes legacy mixed numeric index sort domains" {
    const alloc = std.testing.allocator;

    var u64_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
    defer u64_writer.deinit();
    try u64_writer.add(0, .{ .u64_val = 9_007_199_254_740_993 });
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
    try i64_writer.add(0, .{ .i64_val = -9_007_199_254_740_993 });
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
    const merged = try mergeSegmentInputsWithOptions(alloc, &.{
        .{ .reader = &reader1 },
        .{ .reader = &reader2 },
    }, .{ .index_sort = &sort_fields });
    defer alloc.free(merged);

    var merged_reader = try SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();
    try std.testing.expectEqualStrings("doc:a", (try merged_reader.storedDoc(0)).?.id);
    try std.testing.expectEqualStrings("doc:b", (try merged_reader.storedDoc(1)).?.id);
    var values = try typed_dv.TypedDocValuesReader.init(alloc, (try merged_reader.getSection("price", .typed_doc_values)) orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(typed_dv.ValueType.numeric_val, values.value_type);
    try std.testing.expectEqual(typed_dv.NumericValue{ .i64_val = -9_007_199_254_740_993 }, (try values.getNumeric(0)).?);
    try std.testing.expectEqual(typed_dv.NumericValue{ .u64_val = 9_007_199_254_740_993 }, (try values.getNumeric(1)).?);
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

pub const SegmentSource = @import("segment_source.zig").Source;
pub const SegmentReadScratch = @import("segment_source.zig").Scratch;

/// Range-backed admission keeps only field/section navigation metadata. A
/// caller supplies output and scratch lifetimes; payload bytes never become
/// segment-owned heap buffers. The source must pin one immutable generation.
pub const RangeSegmentReader = struct {
    alloc: Allocator,
    source: SegmentSource,
    fields: []Field,
    doc_count: u32,
    stored_offset: u64,
    stored_length: u64,
    stored_metadata_length: u64,
    num_blocks: u32,
    id_bytes_length: u64,
    paged_source: ?*integrity.PagedSource = null,
    owns_source: bool = true,
    owns_fields: bool = true,

    pub const Field = struct { name: []u8, sections: []SegmentReader.SectionInfo };
    pub const Options = struct {
        metadata_limit_bytes: usize = 1024 * 1024,
        /// Range repair streams unrelated field navigation and retains only
        /// the key-range entry. Its memory does not grow with field metadata.
        key_range_only: bool = false,
    };

    /// Format routing only; init still authenticates and validates metadata.
    pub fn supports(source: SegmentSource) !bool {
        if (source.len() < footer_size) return error.InvalidSegment;
        var version: [4]u8 = undefined;
        try source.readInto(source.len() - 12, &version);
        const ver = std.mem.readInt(u32, &version, .big);
        return ver == 4 or ver == segment_version;
    }

    pub fn init(alloc: Allocator, input_source: SegmentSource, options: Options) !RangeSegmentReader {
        var source = input_source;
        if (source.len() < footer_size) return error.InvalidSegment;
        var footer: [footer_size]u8 = undefined;
        const footer_start = source.len() - footer_size;
        try source.readInto(footer_start, &footer);
        if (!std.mem.eql(u8, footer[52..], &magic)) return error.InvalidMagic;
        const version = std.mem.readInt(u32, footer[44..48], .big);
        if (version != 4 and version != segment_version) return error.UnsupportedVersion;
        const count = std.math.cast(u32, std.mem.readInt(u64, footer[0..8], .big)) orelse return error.InvalidSegment;
        const stored = std.mem.readInt(u64, footer[8..16], .big);
        const stored_len = std.mem.readInt(u64, footer[16..24], .big);
        const stored_meta_len = std.mem.readInt(u64, footer[24..32], .big);
        const index = std.mem.readInt(u64, footer[32..40], .big);
        if (index > footer_start or stored > index or stored_len > index - stored or stored_meta_len > stored_len or stored_meta_len < 5) return error.InvalidSegment;
        // Bound navigation allocations before reading attacker-controlled tables.
        if (!options.key_range_only and footer_start - index > options.metadata_limit_bytes) return error.SegmentMetadataTooLarge;
        var checksum_scratch: [8192]u8 = undefined;
        if (try source.checksum(index, source.len() - 8 - index, &checksum_scratch) != std.mem.readInt(u32, footer[48..52], .big)) return error.CrcMismatch;
        var paged_source: ?*integrity.PagedSource = null;
        errdefer if (paged_source) |paged| paged.deinit();
        if (version == segment_version) {
            const directory = try integrity.Directory.read(source, index);
            if (directory.offset < stored + stored_len or index + integrity.descriptor_size > footer_start) return error.InvalidSegment;
            paged_source = try integrity.PagedSource.init(alloc, source, directory);
            source = paged_source.?.source();
        } else if (try source.checksum(stored, stored_meta_len, &checksum_scratch) != std.mem.readInt(u32, footer[40..44], .big)) return error.CrcMismatch;
        const navigation_start = index + if (paged_source != null) @as(u64, integrity.descriptor_size) else 0;
        var stored_header: [21]u8 = undefined;
        try source.readInto(stored, stored_header[0..5]);
        if (std.mem.readInt(u32, stored_header[1..5], .little) != count) return error.InvalidSegment;
        var blocks: u32 = 0;
        var ids: u64 = 0;
        switch (stored_header[0]) {
            stored_fields_version_omitted => if (stored_len != 5 or stored_meta_len != 5) return error.InvalidSegment,
            stored_fields_version_block_compressed => {
                if (stored_meta_len < 21) return error.InvalidSegment;
                try source.readInto(stored + 5, stored_header[5..]);
                blocks = std.mem.readInt(u32, stored_header[5..9], .little);
                if (std.mem.readInt(u32, stored_header[9..13], .little) != stored_fields_block_doc_target) return error.InvalidSegment;
                ids = std.mem.readInt(u64, stored_header[13..21], .little);
                const expected = 21 + @as(u64, count) * stored_fields_v4_doc_entry_size + @as(u64, blocks) * 12;
                if (expected > stored_meta_len or ids != stored_meta_len - expected or (count == 0) != (blocks == 0)) return error.InvalidSegment;
                const offsets = stored + 21 + @as(u64, count) * stored_fields_v4_doc_entry_size;
                var previous: u64 = 0;
                var block: usize = 0;
                while (block < blocks) {
                    const take: usize = @min(blocks - block, checksum_scratch.len / 8);
                    try source.readInto(offsets + block * 8, checksum_scratch[0 .. take * 8]);
                    for (0..take) |i| {
                        const end = std.mem.readInt(u64, checksum_scratch[i * 8 ..][0..8], .little);
                        if (end <= previous or end > stored_len - stored_meta_len) return error.InvalidSegment;
                        previous = end;
                    }
                    block += take;
                }
                if (previous != stored_len - stored_meta_len) return error.InvalidSegment;
            },
            else => return error.InvalidSegment,
        }
        if (options.key_range_only) {
            const fields = try keyRangeNavigation(alloc, source, navigation_start, footer_start, stored + stored_len, &checksum_scratch);
            return .{ .alloc = alloc, .source = source, .fields = fields, .doc_count = count, .stored_offset = stored, .stored_length = stored_len, .stored_metadata_length = stored_meta_len, .num_blocks = blocks, .id_bytes_length = ids, .paged_source = paged_source };
        }
        var cursor = MetadataCursor{ .source = source, .pos = navigation_start, .end = footer_start, .buffer = &checksum_scratch };
        const field_count = try cursor.int(u16);
        if (@as(usize, field_count) * @sizeOf(Field) > options.metadata_limit_bytes) return error.SegmentMetadataTooLarge;
        const fields = try alloc.alloc(Field, field_count);
        var initialized: usize = 0;
        errdefer {
            for (fields[0..initialized]) |field| {
                alloc.free(field.name);
                alloc.free(field.sections);
            }
            alloc.free(fields);
        }
        var allocated: usize = fields.len * @sizeOf(Field);
        for (fields, 0..) |*field, fi| {
            const name_len = try cursor.int(u16);
            if (name_len > options.metadata_limit_bytes -| allocated) return error.SegmentMetadataTooLarge;
            const name = try alloc.alloc(u8, name_len);
            errdefer alloc.free(name);
            try cursor.read(name);
            for (fields[0..fi]) |previous| if (std.mem.eql(u8, previous.name, name)) return error.InvalidSegment;
            const sections_count = try cursor.int(u16);
            allocated += name_len;
            const section_bytes = @as(usize, sections_count) * @sizeOf(SegmentReader.SectionInfo);
            if (section_bytes > options.metadata_limit_bytes -| allocated) return error.SegmentMetadataTooLarge;
            allocated += section_bytes;
            const sections = try alloc.alloc(SegmentReader.SectionInfo, sections_count);
            // Ownership is transferred only after this iteration succeeds.
            errdefer alloc.free(sections);
            for (sections, 0..) |*section, si| {
                const kind = std.enums.fromInt(SectionType, try cursor.int(u16)) orelse return error.InvalidSegment;
                const offset = try cursor.int(u64);
                const length = try cursor.int(u64);
                const checksum = try cursor.int(u32);
                const payload_end = if (paged_source) |paged| paged.directory.offset else index;
                if (offset < stored + stored_len or offset > payload_end or length > payload_end - offset) return error.InvalidSegment;
                for (sections[0..si]) |previous| if (previous.section_type == kind) return error.InvalidSegment;
                section.* = .{ .section_type = kind, .offset = offset, .length = length, .checksum = checksum, .validation = .init(integrity_unverified) };
            }
            field.* = .{ .name = name, .sections = sections };
            initialized += 1;
        }
        if (cursor.pos != cursor.end) return error.InvalidSegment;
        return .{ .alloc = alloc, .source = source, .fields = fields, .doc_count = count, .stored_offset = stored, .stored_length = stored_len, .stored_metadata_length = stored_meta_len, .num_blocks = blocks, .id_bytes_length = ids, .paged_source = paged_source };
    }

    pub fn deinit(self: *RangeSegmentReader) void {
        if (self.owns_fields) {
            for (self.fields) |field| {
                self.alloc.free(field.name);
                self.alloc.free(field.sections);
            }
            self.alloc.free(self.fields);
        }
        // Cache registrations borrow the source's accounting owner. Release
        // them before the final source lease can destroy that owner.
        var owned_source = if (self.paged_source) |paged| paged.original else self.source;
        if (self.paged_source) |paged| paged.deinit();
        if (self.owns_source) owned_source.close();
        self.* = undefined;
    }

    pub fn findSection(self: *const RangeSegmentReader, field_name: []const u8, kind: SectionType) ?*const SegmentReader.SectionInfo {
        for (self.fields) |field| if (std.mem.eql(u8, field.name, field_name)) {
            for (field.sections) |*entry| if (entry.section_type == kind) return entry;
        };
        return null;
    }

    /// Revision 5 verifies only touched pages. Legacy revision 4 checks the
    /// whole section once with bounded scratch before exposing any payload.
    /// I/O failures remain retryable and never publish a successful validation.
    pub fn readSectionInto(self: *const RangeSegmentReader, field_name: []const u8, kind: SectionType, offset: u64, out: []u8, checksum_scratch: []u8) !bool {
        const entry = self.findSection(field_name, kind) orelse return false;
        if (offset > entry.length or out.len > entry.length - offset) return error.EndOfStream;
        if (self.paged_source != null) {
            try self.source.readInto(entry.offset + offset, out);
            return true;
        }
        const validation = @constCast(&entry.validation);
        switch (validation.load(.acquire)) {
            integrity_invalid => return error.CrcMismatch,
            integrity_valid => {},
            else => {
                if (try self.source.checksum(entry.offset, entry.length, checksum_scratch) != entry.checksum) {
                    validation.store(integrity_invalid, .release);
                    return error.CrcMismatch;
                }
                validation.store(integrity_valid, .release);
            },
        }
        try self.source.readInto(entry.offset + offset, out);
        return true;
    }

    /// Authenticate a section before exposing its bounded decoder view.
    pub fn sectionView(self: *const RangeSegmentReader, field_name: []const u8, kind: SectionType) !?@import("segment_source.zig").View {
        const entry = self.findSection(field_name, kind) orelse return null;
        if (entry.length == 0) return null;
        if (self.paged_source == null) {
            var buffer: [8192]u8 = undefined;
            _ = try self.readSectionInto(field_name, kind, 0, &.{}, &buffer);
        }
        return try @import("segment_source.zig").View.init(self.source, entry.offset, entry.length);
    }

    pub fn invertedIndex(self: *const RangeSegmentReader, field_name: []const u8, max_dictionary_block_bytes: usize) !?inverted.RangeInvertedIndexReader {
        const view = (try self.sectionView(field_name, .inverted_text)) orelse return null;
        return try inverted.RangeInvertedIndexReader.init(self.alloc, view, max_dictionary_block_bytes);
    }

    pub fn invertedIndexScoped(self: *const RangeSegmentReader, allocator: Allocator, field_name: []const u8, options: inverted.ScopedInvertedIndexReader.Options) !?inverted.ScopedInvertedIndexReader {
        const view = (try self.sectionView(field_name, .inverted_text)) orelse return null;
        return try inverted.ScopedInvertedIndexReader.initRanges(allocator, view, options);
    }

    pub fn typedDocValues(self: *const RangeSegmentReader, field_name: []const u8, max_metadata_bytes: usize, max_chunk_bytes: usize) !?typed_dv.RangeTypedDocValuesReader {
        const view = (try self.sectionView(field_name, .typed_doc_values)) orelse return null;
        return try typed_dv.RangeTypedDocValuesReader.init(self.alloc, view, max_metadata_bytes, max_chunk_bytes);
    }

    pub fn docKeyRangeOwned(self: *const RangeSegmentReader, allocator: Allocator, max_bytes: usize) !?struct { min_key: []u8, max_key: []u8 } {
        const entry = self.findSection(doc_key_range_field, .doc_key_range) orelse return null;
        if (entry.length > max_bytes) return error.SegmentReadBudgetExceeded;
        if (entry.length < 9) return error.InvalidSegment;
        var checksum: [8192]u8 = undefined;
        var header: [5]u8 = undefined;
        _ = try self.readSectionInto(doc_key_range_field, .doc_key_range, 0, &header, &checksum);
        if (header[0] != doc_key_range_version) return error.InvalidSegment;
        const min_len = std.mem.readInt(u32, header[1..5], .little);
        if (min_len > entry.length - 9) return error.InvalidSegment;
        var max_length: [4]u8 = undefined;
        try self.source.readInto(entry.offset + 5 + min_len, &max_length);
        const max_len = std.mem.readInt(u32, &max_length, .little);
        if (max_len != entry.length - 9 - min_len) return error.InvalidSegment;
        const min_key = try allocator.alloc(u8, min_len);
        errdefer allocator.free(min_key);
        const max_key = try allocator.alloc(u8, max_len);
        errdefer allocator.free(max_key);
        try self.source.readInto(entry.offset + 5, min_key);
        try self.source.readInto(entry.offset + 9 + min_len, max_key);
        if (std.mem.order(u8, min_key, max_key) == .gt) return error.InvalidSegment;
        return .{ .min_key = min_key, .max_key = max_key };
    }

    /// Identities live in the authenticated metadata region. Range repair and
    /// merge planning need no stored-body decompression or payload allocation.
    pub fn storedDocIdOwned(self: *const RangeSegmentReader, allocator: Allocator, doc: u32, max_bytes: usize) !?[]u8 {
        return self.storedDocIdOwnedFrom(allocator, doc, max_bytes, self.source);
    }

    fn storedDocIdOwnedFrom(self: *const RangeSegmentReader, allocator: Allocator, doc: u32, max_bytes: usize, source: SegmentSource) !?[]u8 {
        if (doc >= self.doc_count or self.num_blocks == 0) return null;
        var entry: [stored_fields_v4_doc_entry_size]u8 = undefined;
        try source.readInto(self.stored_offset + 21 + @as(u64, doc) * entry.len, &entry);
        const offset = std.mem.readInt(u64, entry[0..8], .little);
        const length = std.mem.readInt(u32, entry[8..12], .little);
        if (offset > self.id_bytes_length or length > self.id_bytes_length - offset) return error.InvalidSegment;
        if (length > max_bytes) return error.SegmentReadBudgetExceeded;
        const bytes = try allocator.alloc(u8, length);
        errdefer allocator.free(bytes);
        const ids_start = self.stored_offset + 21 + @as(u64, self.doc_count) * stored_fields_v4_doc_entry_size + @as(u64, self.num_blocks) * 12;
        try source.readInto(ids_start + offset, bytes);
        return bytes;
    }

    pub const StoredDoc = struct {
        id: []u8,
        data: []u8,
        pub fn deinit(self: *StoredDoc, allocator: Allocator) void {
            allocator.free(self.id);
            allocator.free(self.data);
            self.* = undefined;
        }
    };

    /// Reads just one stored block. Output owns its identity and body; scratch
    /// can be reset immediately on return. Limits apply BEFORE decompression
    /// so malformed preambles cannot expand beyond admitted memory.
    pub fn storedDocOwned(self: *const RangeSegmentReader, allocator: Allocator, scratch: *SegmentReadScratch, doc: u32, max_block_bytes: usize) !?StoredDoc {
        return self.storedDocOwnedFrom(allocator, scratch, doc, max_block_bytes, self.source);
    }

    fn storedDocOwnedFrom(self: *const RangeSegmentReader, allocator: Allocator, scratch: *SegmentReadScratch, doc: u32, max_block_bytes: usize, source: SegmentSource) !?StoredDoc {
        if (doc >= self.doc_count or self.num_blocks == 0) return null;
        var entry: [stored_fields_v4_doc_entry_size]u8 = undefined;
        try source.readInto(self.stored_offset + 21 + @as(u64, doc) * entry.len, &entry);
        const id_offset = std.mem.readInt(u64, entry[0..8], .little);
        const id_len = std.mem.readInt(u32, entry[8..12], .little);
        const block = std.mem.readInt(u32, entry[12..16], .little);
        const doc_offset = std.mem.readInt(u32, entry[16..20], .little);
        const raw_len = std.mem.readInt(u32, entry[20..24], .little);
        if (block >= self.num_blocks or id_offset > self.id_bytes_length or id_len > self.id_bytes_length - id_offset) return error.InvalidSegment;
        if (id_len > max_block_bytes or raw_len > max_block_bytes) return error.SegmentReadBudgetExceeded;
        const offsets_start = self.stored_offset + 21 + @as(u64, self.doc_count) * stored_fields_v4_doc_entry_size;
        const checksums_start = offsets_start + @as(u64, self.num_blocks) * 8;
        const ids_start = checksums_start + @as(u64, self.num_blocks) * 4;
        var offsets: [16]u8 = @splat(0);
        if (block == 0) {
            try source.readInto(offsets_start, offsets[8..]);
        } else {
            try source.readInto(offsets_start + @as(u64, block - 1) * 8, &offsets);
        }
        const block_start = std.mem.readInt(u64, offsets[0..8], .little);
        const block_end = std.mem.readInt(u64, offsets[8..16], .little);
        if (block_start > block_end or block_end > self.stored_length - self.stored_metadata_length) return error.InvalidSegment;
        const block_len = std.math.cast(usize, block_end - block_start) orelse return error.InvalidSegment;
        if (block_len > max_block_bytes) return error.SegmentReadBudgetExceeded;
        const compressed = try scratch.allocator().alloc(u8, block_len);
        try source.readInto(self.stored_offset + self.stored_metadata_length + block_start, compressed);
        var checksum: [4]u8 = undefined;
        try source.readInto(checksums_start + @as(u64, block) * 4, &checksum);
        if (Crc32.hash(compressed) != std.mem.readInt(u32, &checksum, .little)) return error.CrcMismatch;
        if (try snappy.decodedLen(compressed) > max_block_bytes) return error.SegmentReadBudgetExceeded;
        const decoded = try snappy.decode(scratch.allocator(), compressed);
        if (doc_offset > decoded.len or decoded.len - doc_offset < 4) return error.InvalidSegment;
        const length = std.mem.readInt(u32, decoded[doc_offset..][0..4], .little);
        const body_start = @as(usize, doc_offset) + 4;
        if (length != raw_len or length > decoded.len - body_start) return error.InvalidSegment;
        const id = try allocator.alloc(u8, id_len);
        errdefer allocator.free(id);
        try source.readInto(ids_start + id_offset, id);
        const body = try allocator.dupe(u8, decoded[body_start..][0..length]);
        return .{ .id = id, .data = body };
    }

    /// Task-owned native block reuse. Fields and source are borrowed from the
    /// reader; outputs own their buffers and survive cache/scratch resets.
    pub const ReadScope = struct {
        reader: *const RangeSegmentReader,
        cache: @import("segment_source.zig").BlockCache,

        pub fn init(allocator: Allocator, reader: *const RangeSegmentReader, byte_budget: usize) !ReadScope {
            return .{ .reader = reader, .cache = try @import("segment_source.zig").BlockCache.init(allocator, reader.source, byte_budget) };
        }

        pub fn deinit(self: *ReadScope) void {
            self.cache.deinit();
            self.* = undefined;
        }

        pub fn invertedIndex(self: *ReadScope, field_name: []const u8, max_dictionary_block_bytes: usize) !?inverted.RangeInvertedIndexReader {
            var view = (try self.reader.sectionView(field_name, .inverted_text)) orelse return null;
            view.source = self.cache.borrowedSource();
            return try inverted.RangeInvertedIndexReader.init(self.cache.allocator, view, max_dictionary_block_bytes);
        }

        pub fn typedDocValues(self: *ReadScope, field_name: []const u8, max_metadata_bytes: usize, max_chunk_bytes: usize) !?typed_dv.RangeTypedDocValuesReader {
            var view = (try self.reader.sectionView(field_name, .typed_doc_values)) orelse return null;
            view.source = self.cache.borrowedSource();
            return try typed_dv.RangeTypedDocValuesReader.init(self.cache.allocator, view, max_metadata_bytes, max_chunk_bytes);
        }

        pub fn storedDocIdOwned(self: *ReadScope, allocator: Allocator, doc: u32, max_bytes: usize) !?[]u8 {
            return self.reader.storedDocIdOwnedFrom(allocator, doc, max_bytes, self.cache.borrowedSource());
        }

        pub fn storedDocOwned(self: *ReadScope, allocator: Allocator, scratch: *SegmentReadScratch, doc: u32, max_block_bytes: usize) !?StoredDoc {
            return self.reader.storedDocOwnedFrom(allocator, scratch, doc, max_block_bytes, self.cache.borrowedSource());
        }
    };

    fn keyRangeNavigation(alloc: Allocator, source: SegmentSource, index: u64, end: u64, payload_start: u64, buffer: []u8) ![]Field {
        var cursor = MetadataCursor{ .source = source, .pos = index, .end = end, .buffer = buffer };
        const field_count = try cursor.int(u16);
        var selected: ?SegmentReader.SectionInfo = null;
        var found_field = false;
        for (0..field_count) |_| {
            const name_length = try cursor.int(u16);
            const matches = try cursor.matches(doc_key_range_field, name_length);
            if (matches and found_field) return error.InvalidSegment;
            if (matches) found_field = true;
            const section_count = try cursor.int(u16);
            var seen = std.EnumSet(SectionType).empty;
            for (0..section_count) |_| {
                const kind = std.enums.fromInt(SectionType, try cursor.int(u16)) orelse return error.InvalidSegment;
                const offset = try cursor.int(u64);
                const length = try cursor.int(u64);
                const checksum = try cursor.int(u32);
                if (seen.contains(kind) or offset < payload_start or offset > index or length > index - offset) return error.InvalidSegment;
                seen.insert(kind);
                if (matches and kind == .doc_key_range) selected = .{ .section_type = kind, .offset = offset, .length = length, .checksum = checksum, .validation = .init(integrity_unverified) };
            }
        }
        if (cursor.pos != end) return error.InvalidSegment;
        const fields = try alloc.alloc(Field, if (selected != null) 1 else 0);
        errdefer alloc.free(fields);
        if (selected) |entry| {
            const name = try alloc.dupe(u8, doc_key_range_field);
            errdefer alloc.free(name);
            const sections = try alloc.alloc(SegmentReader.SectionInfo, 1);
            sections[0] = entry;
            fields[0] = .{ .name = name, .sections = sections };
        }
        return fields;
    }

    const MetadataCursor = struct {
        source: SegmentSource,
        pos: u64,
        end: u64,
        buffer: []u8,
        buffer_start: u64 = 0,
        buffer_len: usize = 0,
        fn read(self: *@This(), out: []u8) !void {
            if (self.pos > self.end or out.len > self.end - self.pos) return error.InvalidSegment;
            var written: usize = 0;
            while (written < out.len) {
                if (self.buffer_len == 0 or self.pos < self.buffer_start or self.pos - self.buffer_start >= self.buffer_len) {
                    self.buffer_start = self.pos;
                    self.buffer_len = @intCast(@min(self.buffer.len, self.end - self.pos));
                    try self.source.readInto(self.pos, self.buffer[0..self.buffer_len]);
                }
                const index: usize = @intCast(self.pos - self.buffer_start);
                const take = @min(out.len - written, self.buffer_len - index);
                @memcpy(out[written..][0..take], self.buffer[index..][0..take]);
                written += take;
                self.pos += take;
            }
        }
        fn matches(self: *@This(), expected: []const u8, length: usize) !bool {
            var same = length == expected.len;
            var offset: usize = 0;
            var bytes: [256]u8 = undefined;
            while (offset < length) {
                const take = @min(bytes.len, length - offset);
                try self.read(bytes[0..take]);
                if (same and !std.mem.eql(u8, bytes[0..take], expected[offset..][0..take])) same = false;
                offset += take;
            }
            return same;
        }
        fn int(self: *@This(), comptime T: type) !T {
            var raw: [@sizeOf(T)]u8 = undefined;
            try self.read(&raw);
            return std.mem.readInt(T, &raw, .big);
        }
    };
};

test "range segment reader admits metadata only and detects payload corruption lazily" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("doc", "body");
    const field = try writer.addField("large");
    const payload = try a.alloc(u8, 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    try writer.addSection(field, .vector, payload);
    const encoded = try writer.build();
    defer a.free(encoded);
    const State = struct {
        bytes: []u8,
        bytes_read: usize = 0,
        max_read: usize = 0,
        closed: bool = false,
        fail: bool = false,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.TestIoFailure;
            self.bytes_read += out.len;
            self.max_read = @max(self.max_read, out.len);
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
        }
    };
    var state = State{ .bytes = encoded };
    const source = SegmentSource{ .ranges = .{ .ptr = &state, .length = encoded.len, .read_into = State.read, .close = State.close } };
    var reader = try RangeSegmentReader.init(a, source, .{});
    defer reader.deinit();
    try std.testing.expect(state.bytes_read <= integrity.page_size + 8192);
    const section_info = reader.findSection("large", .vector).?;
    var out: [16]u8 = undefined;
    var checksum: [1024]u8 = undefined;
    state.fail = true;
    try std.testing.expectError(error.TestIoFailure, reader.readSectionInto("large", .vector, integrity.page_size, &out, &checksum));
    try std.testing.expectEqual(integrity_unverified, section_info.validation.load(.acquire));
    state.fail = false;
    encoded[@intCast(section_info.offset + integrity.page_size)] ^= 1;
    try std.testing.expectError(error.CrcMismatch, reader.readSectionInto("large", .vector, integrity.page_size, &out, &checksum));
    try std.testing.expect(state.max_read <= integrity.page_size);
    encoded[@intCast(section_info.offset + integrity.page_size)] ^= 1;
    try std.testing.expectError(error.CrcMismatch, reader.readSectionInto("large", .vector, integrity.page_size, &out, &checksum));
}

test "segment bounded stored cursor decodes once per block and retries failures" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var body: [512]u8 = @splat('b');
    for (0..256) |doc| {
        body[0] = @intCast(doc);
        try writer.addStoredDoc("id", &body);
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var failed_cursor = SegmentReader.StoredDocCursor.init(failing.allocator());
    defer failed_cursor.deinit();
    try std.testing.expectError(error.OutOfMemory, failed_cursor.get(&reader, 0));
    try std.testing.expectEqual(@as(?u32, null), failed_cursor.block);
    var cursor = SegmentReader.StoredDocCursor.init(a);
    defer cursor.deinit();
    for (0..256) |doc| {
        try std.testing.expectEqual(@as(?u32, 512), try reader.storedDocLength(@intCast(doc)));
        const stored = (try cursor.get(&reader, @intCast(doc))).?;
        try std.testing.expectEqual(@as(u8, @intCast(doc)), stored.data[0]);
        try std.testing.expectEqual(@as(usize, 512), stored.data.len);
    }
    try std.testing.expectEqual(@as(usize, 2), cursor.decode_count);
    try std.testing.expectEqual(@as(?SegmentReader.StoredDocRef, null), try cursor.get(&reader, 256));
    // Switching reader identity must invalidate a matching block ordinal.
    var other = try SegmentReader.init(a, bytes);
    defer other.deinit();
    _ = try cursor.get(&other, 128);
    try std.testing.expectEqual(@as(usize, 3), cursor.decode_count);
    std.debug.print("LITE_STORED_CURSOR documents=256 baseline_decodes=256 cursor_decodes=2 retained_bytes={d}\n", .{cursor.scratch.arena.queryCapacity()});
}

test "range segment reader streams oversized repair metadata and key ranges" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("id", "body");
    const name = try a.alloc(u8, 60000);
    defer a.free(name);
    @memset(name, 'f');
    for (0..18) |i| {
        name[0] = @intCast(i + 1);
        _ = try writer.addField(name);
    }
    const min_key = try a.alloc(u8, 600000);
    defer a.free(min_key);
    const max_key = try a.alloc(u8, 600000);
    defer a.free(max_key);
    @memset(min_key, 'a');
    @memset(max_key, 'z');
    try writer.addDocKeyRange(min_key, max_key);
    const bytes = try writer.build();
    defer a.free(bytes);
    try std.testing.expectError(error.SegmentMetadataTooLarge, RangeSegmentReader.init(a, .{ .contiguous = bytes }, .{}));
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 1024 };
    var reader = try RangeSegmentReader.init(budget.allocator(), .{ .contiguous = bytes }, .{ .key_range_only = true });
    defer reader.deinit();
    try std.testing.expect(reader.findSection(doc_key_range_field, .doc_key_range) != null);
    var key_budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = min_key.len + max_key.len };
    const range = (try reader.docKeyRangeOwned(key_budget.allocator(), std.math.maxInt(usize))).?;
    defer key_budget.allocator().free(range.min_key);
    defer key_budget.allocator().free(range.max_key);
    try std.testing.expectEqualSlices(u8, min_key, range.min_key);
    try std.testing.expectEqualSlices(u8, max_key, range.max_key);
    try std.testing.expectEqual(min_key.len + max_key.len, key_budget.peak);
    std.debug.print("LITE_RANGE_REPAIR metadata_bytes=1080000 navigation_peak={d} key_output_peak={d}\n", .{ budget.peak, key_budget.peak });
}

test "segment bounded stored cursor allocation and time measurement" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    const body: [512]u8 = @splat('b');
    for (0..256) |_| try writer.addStoredDoc("id", &body);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    var baseline = std.testing.FailingAllocator.init(a, .{});
    const old_start = platform_time.monotonicNs();
    for (0..32) |_| {
        for (0..256) |doc| {
            const stored = (try reader.storedDocDecompressed(baseline.allocator(), @intCast(doc))).?;
            std.mem.doNotOptimizeAway(stored.data);
            baseline.allocator().free(stored.data);
        }
    }
    const old_ns = platform_time.monotonicNs() - old_start;
    var reusable = std.testing.FailingAllocator.init(a, .{});
    const new_start = platform_time.monotonicNs();
    var cursor = SegmentReader.StoredDocCursor.init(reusable.allocator());
    for (0..32) |_| {
        for (0..256) |doc| {
            const stored = (try cursor.get(&reader, @intCast(doc))).?;
            std.mem.doNotOptimizeAway(stored.data);
        }
    }
    cursor.deinit();
    const new_ns = platform_time.monotonicNs() - new_start;
    try std.testing.expectEqual(@as(usize, 0), baseline.allocated_bytes - baseline.freed_bytes);
    try std.testing.expectEqual(@as(usize, 0), reusable.allocated_bytes - reusable.freed_bytes);
    try std.testing.expect(reusable.allocations < baseline.allocations / 16);
    std.debug.print("LITE_STORED_CURSOR_BENCH documents=8192 old_allocations={d} new_allocations={d} old_allocated_bytes={d} new_allocated_bytes={d} old_ns={d} new_ns={d}\n", .{ baseline.allocations, reusable.allocations, baseline.allocated_bytes, reusable.allocated_bytes, old_ns, new_ns });
}

test "segment bounded block cache avoids interleaved merge thrashing and bounds retention" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    const body: [512]u8 = @splat('b');
    for (0..256) |_| try writer.addStoredDoc("id", &body);
    const bytes = try writer.build();
    defer a.free(bytes);
    var first = try SegmentReader.init(a, bytes);
    defer first.deinit();
    var second = try SegmentReader.init(a, bytes);
    defer second.deinit();
    var old = SegmentReader.StoredDocCursor.init(a);
    defer old.deinit();
    var cache = SegmentReader.StoredDocBlockCache.init(a, 256 * 1024);
    defer cache.deinit();
    for (0..256) |doc| {
        for ([_]*const SegmentReader{ &first, &second }) |reader| {
            const baseline = (try old.get(reader, @intCast(doc))).?;
            const cached = (try cache.get(reader, @intCast(doc))).?;
            try std.testing.expectEqualSlices(u8, baseline.data, cached.data);
            try std.testing.expect(cache.live_bytes <= cache.byte_budget);
        }
    }
    try std.testing.expectEqual(@as(usize, 512), old.decode_count);
    try std.testing.expectEqual(@as(usize, 4), cache.decode_count);
    std.debug.print("LITE_MERGE_BLOCK_CACHE reads=512 single_slot_decodes={d} bounded_cache_decodes={d} cached_bytes={d}\n", .{ old.decode_count, cache.decode_count, cache.live_bytes });
    const Scenario = struct {
        fn run(allocator: Allocator, x: *const SegmentReader, y: *const SegmentReader) !void {
            var scope = SegmentReader.StoredDocBlockCache.init(allocator, 256 * 1024);
            defer scope.deinit();
            for (0..256) |doc| {
                _ = try scope.get(x, @intCast(doc));
                _ = try scope.get(y, @intCast(doc));
            }
        }
    };
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{ &first, &second });
    var oversized_writer = SegmentWriter.init(a);
    defer oversized_writer.deinit();
    const huge = try a.alloc(u8, 300 * 1024);
    defer a.free(huge);
    @memset(huge, 'h');
    try oversized_writer.addStoredDoc("huge", huge);
    const oversized_bytes = try oversized_writer.build();
    defer a.free(oversized_bytes);
    var oversized = try SegmentReader.init(a, oversized_bytes);
    defer oversized.deinit();
    _ = try cache.get(&oversized, 0);
    try std.testing.expectEqual(@as(usize, 1), cache.count);
    _ = try cache.get(&first, 0);
    try std.testing.expect(cache.live_bytes <= cache.byte_budget);
}

test "segment range block cache bounds memory and never serves failed fills" {
    const a = std.testing.allocator;
    const State = struct {
        bytes: [160]u8,
        fail: bool = false,
        reads: usize = 0,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            if (self.fail) {
                @memset(out, 255);
                self.fail = false;
                return error.TestReadFailure;
            }
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .bytes = undefined };
    for (&state.bytes, 0..) |*byte, i| byte.* = @intCast(i);
    const source = SegmentSource{ .ranges = .{ .ptr = &state, .length = state.bytes.len, .read_into = State.read, .close = State.close } };
    var cache = try @import("segment_source.zig").BlockCache.init(a, source, 64);
    defer cache.deinit();
    var out: [10]u8 = undefined;
    for (0..10) |_| for ([_]u64{ 0, 32, 64, 96 }) |offset| {
        try cache.readInto(offset, &out);
        try std.testing.expectEqualSlices(u8, state.bytes[@intCast(offset)..][0..out.len], &out);
    };
    try std.testing.expectEqual(@as(usize, 4), state.reads);
    try std.testing.expectEqual(@as(usize, 64), cache.retainedBytes());
    state.fail = true;
    try std.testing.expectError(error.TestReadFailure, cache.readInto(128, &out));
    try cache.readInto(0, &out);
    try std.testing.expectEqualSlices(u8, state.bytes[0..10], &out);
    try cache.readInto(128, &out);
    try std.testing.expectEqualSlices(u8, state.bytes[128..138], &out);
    try cache.readInto(150, &out);
    try std.testing.expectEqualSlices(u8, state.bytes[150..160], &out);
    try std.testing.expect(cache.retainedBytes() <= 64);
    try std.testing.expectError(error.EndOfStream, cache.readInto(151, &out));
}

test "segment range typed values stream chunks with bounded navigation and propagate failures" {
    const a = std.testing.allocator;
    const State = struct {
        bytes: []const u8,
        fail: bool = false,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.TestReadFailure;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    const cases = [_]typed_dv.TypedValue{
        .{ .u64_val = 123 },                                      .{ .i64_val = -42 },   .{ .f64_val = 1.25 },
        .{ .geo_point = .{ .lat = 42, .lon = -71 } },             .{ .bool_val = true }, .{ .bytes_val = "a value" },
        .{ .numeric_val = .{ .u64_val = std.math.maxInt(u64) } },
    };
    for (cases) |value| {
        var writer = typed_dv.TypedDocValuesWriter.init(a, switch (value) {
            .u64_val => .u64_val,
            .i64_val => .i64_val,
            .f64_val => .f64_val,
            .geo_point => .geo_point,
            .bool_val => .bool_val,
            .bytes_val => .bytes_val,
            .numeric_val => .numeric_val,
        }, 64);
        defer writer.deinit();
        for (0..256) |doc| try writer.add(@intCast(doc), value);
        const bytes = try writer.build();
        defer a.free(bytes);
        var state = State{ .bytes = bytes };
        const source = SegmentSource{ .ranges = .{ .ptr = &state, .length = bytes.len, .read_into = State.read, .close = State.close } };
        const view = try @import("segment_source.zig").View.init(source, 0, bytes.len);
        var range = try typed_dv.RangeTypedDocValuesReader.init(a, view, 64, 64 * 1024);
        defer range.deinit();
        try std.testing.expectEqual(@as(usize, 32), range.offsets.len);
        var total: usize = 0;
        for (0..range.reader.num_chunks) |i| {
            var chunk = try range.reader.decodeChunk(@intCast(i));
            defer chunk.deinit();
            var it = chunk.iterator();
            while (try it.next()) |entry| {
                try std.testing.expectEqual(@as(u32, @intCast(total)), entry.doc_id);
                if (std.meta.activeTag(value) == .bytes_val) {
                    try std.testing.expectEqualStrings(value.bytes_val, entry.value.bytes_val);
                } else {
                    try std.testing.expectEqualDeep(value, entry.value);
                }
                total += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 256), total);
        state.fail = true;
        try std.testing.expectError(error.TestReadFailure, range.reader.decodeChunk(0));
        state.fail = false;
        try std.testing.expectError(error.SegmentMetadataTooLarge, typed_dv.RangeTypedDocValuesReader.init(a, view, 31, 64 * 1024));
        var bounded = try typed_dv.RangeTypedDocValuesReader.init(a, view, 64, 1);
        defer bounded.deinit();
        try std.testing.expectError(error.SegmentReadBudgetExceeded, bounded.reader.decodeChunk(0));
    }
}

test "segment range inverted dictionary reads selected blocks and propagates errors" {
    const a = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..1024) |doc| {
        var term: [32]u8 = undefined;
        try builder.addDocument(@intCast(doc), &.{ .{ .term = try std.fmt.bufPrint(&term, "term-{d:0>6}", .{doc}), .freq = 1 }, .{ .term = "common", .freq = 2 } });
    }
    const data = try builder.build();
    defer a.free(data);
    const State = struct {
        bytes: []const u8,
        fail: bool = false,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.TestReadFailure;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .bytes = data };
    const source = SegmentSource{ .ranges = .{ .ptr = &state, .length = data.len, .read_into = State.read, .close = State.close } };
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 4096 };
    const reader = try inverted.RangeInvertedIndexReader.init(budget.allocator(), try @import("segment_source.zig").View.init(source, 0, data.len), 4096);
    var scratch = SegmentReadScratch.init(budget.allocator(), 4096);
    defer scratch.deinit();
    for (0..1024) |doc| {
        var term: [32]u8 = undefined;
        const address = (try reader.lookupAddress(try std.fmt.bufPrint(&term, "term-{d:0>6}", .{doc}), &scratch)).?;
        try std.testing.expectEqual(@as(u32, @intCast(doc)), address.one_hit.doc_num);
    }
    const common = (try reader.lookupAddress("common", &scratch)).?;
    try std.testing.expect(common == .postings_offset);
    try std.testing.expect((try reader.lookupAddress("absent", &scratch)) == null);
    try std.testing.expect((try reader.lookupAddress("term-999999", &scratch)) == null);
    state.fail = true;
    try std.testing.expectError(error.TestReadFailure, reader.lookupAddress("common", &scratch));
    state.fail = false;
    try std.testing.expect((try reader.lookupAddress("common", &scratch)) != null);
    try std.testing.expect(budget.peak <= 4096);
    std.debug.print("LITE_RANGE_DICTIONARY section_bytes={d} blocks={d} dictionary_scratch_peak={d}\n", .{ data.len, reader.block_count, budget.peak });
}

test "segment range postings reuse bounded chunk buffers and propagate read failures" {
    const a = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..4096) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 3, .positions = if (doc % 3 == 0) &.{} else &.{ 1, 4, 35 } }});
    const data = try builder.build();
    defer a.free(data);
    const reader = try inverted.InvertedIndexReader.init(a, data);
    const lookup = reader.lookup("common").?;
    const native_reader = try inverted.RangeInvertedIndexReader.init(a, try @import("segment_source.zig").View.init(.{ .contiguous = data }, 0, data.len), 4096);
    var dictionary_scratch = SegmentReadScratch.init(a, 4096);
    defer dictionary_scratch.deinit();
    const address = (try native_reader.lookupAddress("common", &dictionary_scratch)).?;
    var owned = try native_reader.openPostings(address.postings_offset, 64 * 1024, 4096, 4096);
    defer owned.deinit();
    var decoded = try owned.value.iterator(a);
    defer decoded.deinit();
    var expected_iterator = try lookup.iterator(a);
    defer expected_iterator.deinit();
    while (try expected_iterator.next()) |expected| try std.testing.expectEqualDeep(expected, (try decoded.next()).?);
    try std.testing.expect((try decoded.next()) == null);
    try std.testing.expectError(error.SegmentMetadataTooLarge, native_reader.openPostings(address.postings_offset, 1, 4096, 4096));
    const AllocationSweep = struct {
        fn run(allocator: Allocator, input: []const u8, posting_offset: u64) !void {
            const native = try inverted.RangeInvertedIndexReader.init(allocator, try @import("segment_source.zig").View.init(.{ .contiguous = input }, 0, input.len), 4096);
            var term = try native.openPostings(posting_offset, 64 * 1024, 4096, 4096);
            defer term.deinit();
            var iterator = try term.value.iterator(allocator);
            defer iterator.deinit();
            _ = try iterator.next();
            _ = try iterator.advanceTo(2049);
        }
    };
    try std.testing.checkAllAllocationFailures(a, AllocationSweep.run, .{ data, address.postings_offset });
    const State = struct {
        bytes: []const u8,
        fail: bool = false,
        reads: usize = 0,
        largest: usize = 0,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.TestReadFailure;
            self.reads += 1;
            self.largest = @max(self.largest, out.len);
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .bytes = lookup.postings.payload_data };
    const source = SegmentSource{ .ranges = .{ .ptr = &state, .length = state.bytes.len, .read_into = State.read, .close = State.close } };
    var position_state = State{ .bytes = lookup.postings.positions_data.? };
    const position_source = SegmentSource{ .ranges = .{ .ptr = &position_state, .length = position_state.bytes.len, .read_into = State.read, .close = State.close } };
    var postings = lookup.postings;
    postings.positions_data = null;
    postings.positions_range = try @import("segment_source.zig").View.init(position_source, 0, position_state.bytes.len);
    postings.payload_data = &.{};
    postings.payload_range = try @import("segment_source.zig").View.init(source, 0, state.bytes.len);
    var range = try postings.iterator(a);
    defer range.deinit();
    var contiguous = try lookup.iterator(a);
    defer contiguous.deinit();
    while (try contiguous.next()) |expected| {
        const hit = (try range.next()).?;
        try std.testing.expectEqualDeep(expected, hit);
    }
    try std.testing.expect((try range.next()) == null);
    try std.testing.expect(range.payload_buffer.capacity <= postings.max_payload_chunk_bytes);
    try std.testing.expectEqual(@as(usize, lookup.postings.chunk_meta_count), state.reads);
    var advancing = try postings.iterator(a);
    defer advancing.deinit();
    var reference = try lookup.iterator(a);
    defer reference.deinit();
    for ([_]u32{ 1, 29, 127, 130, 512, 2049, 4095 }) |doc| {
        try std.testing.expectEqualDeep((try reference.advanceTo(doc)).?, (try advancing.advanceTo(doc)).?);
    }
    try std.testing.expect(advancing.position_read_buffer.capacity <= postings.max_position_record_bytes);
    position_state.fail = true;
    var position_failing = try postings.iterator(a);
    defer position_failing.deinit();
    try std.testing.expectError(error.TestReadFailure, position_failing.next());
    position_state.fail = false;
    state.fail = true;
    var failing = try postings.iterator(a);
    defer failing.deinit();
    try std.testing.expectError(error.TestReadFailure, failing.next());
    state.fail = false;
    postings.max_payload_chunk_bytes = 1;
    var bounded = try postings.iterator(a);
    defer bounded.deinit();
    try std.testing.expectError(error.SegmentReadBudgetExceeded, bounded.next());
    std.debug.print("LITE_RANGE_POSTINGS documents=4096 chunks={d} payload_bytes={d} largest_read={d} retained_buffer={d}\n", .{ state.reads, state.bytes.len, state.largest, range.payload_buffer.capacity });
}

test "segment native field scopes retain escaping iterators and share navigation" {
    const a = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..512) |doc| try builder.addDocument(@intCast(doc), &.{ .{ .term = "alpha", .freq = 2, .positions = &.{ 0, 5 } }, .{ .term = "beta", .freq = 1, .positions = &.{3} } });
    const data = try builder.build();
    defer a.free(data);
    const view = try @import("segment_source.zig").View.init(.{ .contiguous = data }, 0, data.len);
    var scope = try inverted.ScopedInvertedIndexReader.initRanges(a, view, .{});
    var scope_open = true;
    defer if (scope_open) scope.deinit();
    const alpha = (try scope.lookup("alpha")).?;
    const beta = (try scope.lookup("beta")).?;
    try std.testing.expect(alpha.postings.norms_data.ptr == beta.postings.norms_data.ptr);
    var alpha_iterator = try alpha.iterator(a);
    defer alpha_iterator.deinit();
    var beta_iterator = try beta.iterator(a);
    defer beta_iterator.deinit();
    var terms = try scope.rangeTermIterator("alpha", "gamma");
    defer terms.deinit();
    var automaton = @import("search/levenshtein.zig").LevenshteinAutomaton{ .term = "beta", .max_distance = 0, .alloc = a };
    defer automaton.deinit();
    var matching = try scope.fstSearchIterator(automaton.automaton());
    defer matching.deinit();
    try std.testing.expectEqualStrings("beta", (try matching.next()).?.term);
    try std.testing.expect((try matching.next()) == null);
    scope.deinit();
    scope_open = false;
    for (0..512) |doc| {
        const hit = (try alpha_iterator.next()).?;
        try std.testing.expectEqual(@as(u32, @intCast(doc)), hit.doc_id);
        try std.testing.expectEqualSlices(u32, &.{ 0, 5 }, hit.positions);
    }
    try std.testing.expect((try alpha_iterator.next()) == null);
    try std.testing.expectEqual(@as(u32, 501), (try beta_iterator.advanceTo(501)).?.doc_id);
    try std.testing.expectEqualStrings("alpha", (try terms.next()).?.term);
    try std.testing.expectEqualStrings("beta", (try terms.next()).?.term);
    try std.testing.expect((try terms.next()) == null);
    const Sweep = struct {
        fn run(allocator: Allocator, source_view: @import("segment_source.zig").View) !void {
            var scoped = try inverted.ScopedInvertedIndexReader.initRanges(allocator, source_view, .{});
            defer scoped.deinit();
            const found = (try scoped.lookup("alpha")).?;
            var iterator = try found.iterator(allocator);
            defer iterator.deinit();
            _ = try iterator.next();
            var keys = try scoped.termIterator();
            defer keys.deinit();
            _ = try keys.next();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{view});
    var zero_budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 0 };
    var contiguous = try inverted.ScopedInvertedIndexReader.initContiguous(zero_budget.allocator(), data);
    defer contiguous.deinit();
    try std.testing.expect((try contiguous.lookup("alpha")) != null);
    try std.testing.expectEqual(@as(usize, 0), zero_budget.peak);
}

test "segment native facade queries typed values and merges both orders without payload copies" {
    const a = std.testing.allocator;
    var text = inverted.InvertedIndexBuilder.init(a, .{});
    defer text.deinit();
    var values = typed_dv.TypedDocValuesWriter.init(a, .i64_val, 16);
    defer values.deinit();
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    for (0..128) |doc| {
        try text.addDocument(@intCast(doc), &.{ .{ .term = "alpha", .freq = 1, .positions = &.{0} }, .{ .term = "beta", .freq = 1, .positions = &.{1} } });
        try values.add(@intCast(doc), .{ .i64_val = @intCast(doc) });
        var id: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d:0>4}", .{doc}), "native stored body");
    }
    const text_bytes = try text.build();
    defer a.free(text_bytes);
    const value_bytes = try values.build();
    defer a.free(value_bytes);
    try writer.addSection(try writer.addField("body"), .inverted_text, text_bytes);
    try writer.addSection(try writer.addField("price"), .typed_doc_values, value_bytes);
    try writer.addIndexSortMetadata(&.{.{ .field = "price", .desc = false }});
    const bytes = try writer.build();
    defer a.free(bytes);
    const State = struct {
        bytes: []const u8,
        fail: bool = false,
        reads: usize = 0,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.TestReadFailure;
            self.reads += 1;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .bytes = bytes };
    const input = SegmentSource{ .ranges = .{ .ptr = &state, .length = bytes.len, .read_into = State.read, .close = State.close } };
    var native = try SegmentReader.initSource(a, input);
    defer native.deinit();
    try std.testing.expectEqual(@as(usize, 0), native.data.len);
    var contiguous = try SegmentReader.init(a, bytes);
    defer contiguous.deinit();
    try std.testing.expectEqualDeep(contiguous.layoutStats(), native.layoutStats());
    try std.testing.expectEqualDeep(contiguous.layoutStatsWithInvertedDetails(true), native.layoutStatsWithInvertedDetails(true));
    try std.testing.expectError(error.NativeSectionRequiresScope, native.getSection("body", .inverted_text));
    try std.testing.expectEqualStrings("doc-0127", (try native.storedDoc(127)).?.id);
    var cursor = SegmentReader.StoredDocCursor.init(a);
    defer cursor.deinit();
    try std.testing.expectEqualStrings("native stored body", (try cursor.get(&native, 127)).?.data);
    var dv = (try native.typedDocValuesScoped(a, "price")).?;
    defer dv.deinit();
    try std.testing.expectEqual(@as(i64, 127), (try dv.getI64(127)).?);
    const local_index = @import("index.zig");
    const query = @import("search/query.zig");
    var index = try local_index.IndexWriter.init(a);
    defer index.deinit();
    try index.addSegmentWithIdData(1, .fromNative(input));
    const entry = &index.snapshot().segments[0];
    var phrase = try (query.PhraseFilter{ .field = "body", .terms = &.{ "alpha", "beta" } }).execute(a, entry);
    defer phrase.deinit();
    try std.testing.expectEqual(@as(usize, 128), phrase.cardinality());
    const reads_before_scan = state.reads;
    var range = try (query.RangeFilter{ .field = "price", .min_val = 120 }).execute(a, entry);
    defer range.deinit();
    try std.testing.expectEqual(@as(usize, 8), range.cardinality());
    const scan_reads = state.reads - reads_before_scan;
    try std.testing.expect(scan_reads <= 12);
    std.debug.print("LITE_NATIVE_VALUE_SCAN documents=128 chunks=8 reads={d}\n", .{scan_reads});
    const inputs = [_]MergeInput{ .{ .reader = &native }, .{ .reader = &native } };
    for ([_]MergeOptions{ .{}, .{ .index_sort = &.{.{ .field = "price", .desc = false }} } }) |options| {
        var output = MemorySegmentSink.init(a);
        defer output.deinit();
        var sink = output.sink();
        try writeMergedSegmentToSinkWithOptions(a, &sink, &inputs, options);
        var merged = try SegmentReader.init(a, output.out.items);
        defer merged.deinit();
        try std.testing.expectEqual(@as(u32, 256), merged.doc_count);
        var merged_dv = (try merged.typedDocValuesScoped(a, "price")).?;
        defer merged_dv.deinit();
        try std.testing.expectEqual(@as(i64, 0), (try merged_dv.getI64(0)).?);
        try std.testing.expectEqual(@as(i64, if (options.index_sort.len == 0) 1 else 0), (try merged_dv.getI64(1)).?);
        var merged_text = (try merged.invertedIndexScoped(a, "body")).?;
        defer merged_text.deinit();
        try std.testing.expectEqual(@as(u32, 256), (try merged_text.lookup("alpha")).?.docFreq());
    }
    entry.reader.native.?.range.paged_source.?.cached_page = null;
    state.fail = true;
    try std.testing.expectError(error.TestReadFailure, (query.TermFilter{ .field = "body", .term = "alpha" }).execute(a, entry));
    state.fail = false;
    const Sweep = struct {
        fn run(allocator: Allocator, source_input: SegmentSource) !void {
            var reader = try SegmentReader.initSource(allocator, source_input);
            defer reader.deinit();
            var field = (try reader.typedDocValuesScoped(allocator, "price")).?;
            defer field.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{input});
}

test "segment dictionary scopes reuse bounded metadata and preserve escaping postings" {
    const a = std.testing.allocator;
    var builder = inverted.InvertedIndexBuilder.init(a, .{});
    defer builder.deinit();
    for (0..256) |term| {
        var key: [32]u8 = undefined;
        const word = try std.fmt.bufPrint(&key, "term-{d:0>4}", .{term});
        for (0..2) |doc| try builder.addDocument(@intCast(doc), &.{.{ .term = word, .freq = 2, .positions = &.{ 0, 3 } }});
    }
    const bytes = try builder.build();
    defer a.free(bytes);
    const view = try @import("segment_source.zig").View.init(.{ .contiguous = bytes }, 0, bytes.len);
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 128 * 1024 };
    var field = try inverted.ScopedInvertedIndexReader.initRanges(budget.allocator(), view, .{ .cache_bytes = 4096, .navigation_bytes = 16 * 1024 });
    var scope_open = true;
    defer if (scope_open) field.deinit();
    var terms = try field.termIterator();
    var terms_open = true;
    defer if (terms_open) terms.deinit();
    const first = (try terms.next()).?;
    var escaping = try first.result.iterator(budget.allocator());
    defer escaping.deinit();
    var count: usize = 1;
    while (try terms.next()) |entry| {
        var postings = try entry.result.iterator(budget.allocator());
        defer postings.deinit();
        try std.testing.expectEqualSlices(u32, &.{ 0, 3 }, (try postings.next()).?.positions);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 256), count);
    terms.deinit();
    terms_open = false;
    field.deinit();
    scope_open = false;
    try std.testing.expectEqualSlices(u32, &.{ 0, 3 }, (try escaping.next()).?.positions);
    std.debug.print("LITE_NATIVE_TERM_SCRATCH terms={d} peak_bytes={d}\n", .{ count, budget.peak });
}

test "segment native admission accepts large legacy identity navigation and offset batches" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    for (0..300_000) |doc| {
        var id: [36]u8 = @splat('x');
        std.mem.writeInt(u64, id[0..8], @intCast(doc), .little);
        try writer.addStoredDoc(&id, "");
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 300_000), reader.doc_count);
    const last = (try reader.storedDoc(299_999)).?;
    try std.testing.expectEqual(@as(u64, 299_999), std.mem.readInt(u64, last.id[0..8], .little));
    try std.testing.expect(reader.nativeNavigationBytes() < 512 * 1024);
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 8 * 1024 * 1024 };
    var bounded = try SegmentReader.initSource(budget.allocator(), .{ .contiguous = bytes });
    bounded.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "segment shared typed scope reuses readers and cleans up allocation failures" {
    const a = std.testing.allocator;
    var values = typed_dv.TypedDocValuesWriter.init(a, .u64_val, 32);
    defer values.deinit();
    for (0..128) |doc| try values.add(@intCast(doc), .{ .u64_val = doc });
    const column = try values.build();
    defer a.free(column);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    writer.doc_count = 128;
    for ([_][]const u8{ "one", "two" }) |field| try writer.addSection(try writer.addField(field), .typed_doc_values, column);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    const Harness = struct {
        fn run(alloc: Allocator, segment: *const SegmentReader) !void {
            var scope = TypedReadScope.init(alloc);
            defer scope.deinit();
            const one = (try scope.get(segment, "one")).?;
            const two = (try scope.get(segment, "two")).?;
            try std.testing.expect(one == (try scope.get(segment, "one")).?);
            for (0..128) |doc| {
                try std.testing.expectEqual(@as(u64, doc), (try one.getU64(@intCast(doc))).?);
                try std.testing.expectEqual(@as(u64, doc), (try two.getU64(@intCast(doc))).?);
            }
            try std.testing.expect((try scope.get(segment, "absent")) == null);
            try std.testing.expect((try scope.get(segment, "absent")) == null);
            try std.testing.expectEqual(@as(usize, 3), scope.entries.items.len);
            try std.testing.expect(scope.cache.?.live_bytes <= scope.cache.?.byte_budget);
        }
    };
    try Harness.run(a, &reader);
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{&reader});
}

test "segment streaming sorted merge retains heads and bounds instead of all keys" {
    const a = std.testing.allocator;
    var values = typed_dv.TypedDocValuesWriter.init(a, .u64_val, 1024);
    defer values.deinit();
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    for (0..25_000) |doc| {
        try values.add(@intCast(doc), .{ .u64_val = doc });
        var id: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d}", .{doc}), "");
    }
    const column = try values.build();
    defer a.free(column);
    try writer.addSection(try writer.addField("rank"), .typed_doc_values, column);
    const fields = [_]SegmentIndexSortField{.{ .field = "rank", .desc = false }};
    try writer.addIndexSortMetadata(&fields);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    const inputs = [_]MergeInput{ .{ .reader = &reader }, .{ .reader = &reader } };
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var streamed_budget = Budget{ .backing = a, .limit = 2 * 1024 * 1024 };
    const clock = @import("antfly_platform").time;
    const streamed_start = clock.monotonicNs();
    var streamed = try buildSortedMergePlanAlloc(streamed_budget.allocator(), &inputs, &fields);
    defer streamed.deinit(streamed_budget.allocator());
    const streamed_ns = clock.monotonicNs() - streamed_start;
    var reference_budget = Budget{ .backing = a };
    const reference_start = clock.monotonicNs();
    var reference = try buildUnsortedMergePlanAlloc(reference_budget.allocator(), &inputs, &fields);
    defer reference.deinit(reference_budget.allocator());
    const reference_ns = clock.monotonicNs() - reference_start;
    try std.testing.expectEqualDeep(reference.records, streamed.records);
    try std.testing.expectEqualDeep(reference.doc_maps, streamed.doc_maps);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var file_budget = Budget{ .backing = a, .limit = 1024 * 1024 };
    const file_start = clock.monotonicNs();
    var file_plan = try buildSortedMergePlanWithScratch(file_budget.allocator(), &inputs, &fields, .{ .io = std.testing.io, .directory = directory });
    defer file_plan.deinit(file_budget.allocator());
    const file_ns = clock.monotonicNs() - file_start;
    try std.testing.expectEqual(@as(usize, 0), file_plan.records.len);
    try std.testing.expectEqual(@as(usize, 0), file_plan.doc_maps.len);
    try std.testing.expectEqual(reference.records.len, file_plan.ordered().len);
    var file_iterator = file_plan.ordered().iterator();
    for (reference.records) |record| try std.testing.expectEqualDeep(record, (try file_iterator.next()).?);
    try std.testing.expect((try file_iterator.next()) == null);
    for (reference.doc_maps, file_plan.file.?.maps) |expected, map| {
        for (expected, 0..) |id, source_doc| {
            var encoded: [4]u8 = undefined;
            try map.ids.readInto(source_doc * 4, &encoded);
            try std.testing.expectEqual(id, std.mem.readInt(u32, &encoded, .little));
        }
    }
    try std.testing.expect(file_budget.peak < streamed_budget.peak / 2);
    try std.testing.expect(file_plan.file.?.mapping.write_calls < 200);
    std.debug.print("FILE_SORTED_PLAN documents=50000 peak={d} heap_peak={d} elapsed_ns={d} map_writes={d} record_reads={d}\n", .{ file_budget.peak, streamed_budget.peak, file_ns, file_plan.file.?.mapping.write_calls, file_plan.file.?.records.read_calls });

    try std.testing.expectEqualDeep(reference.first_keys, streamed.first_keys);
    try std.testing.expectEqualDeep(reference.last_keys, streamed.last_keys);
    try std.testing.expect(streamed_budget.peak < reference_budget.peak / 2);
    std.debug.print("LITE_SORTED_MERGE documents=50000 streamed_peak={d} reference_peak={d} streamed_ns={d} reference_ns={d} streamed_allocs={d} reference_allocs={d}\n", .{ streamed_budget.peak, reference_budget.peak, streamed_ns, reference_ns, streamed_budget.alloc_calls, reference_budget.alloc_calls });
    const Spy = struct {
        inner: *SegmentSink,
        writes: usize = 0,
        largest: usize = 0,
        fn self(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn len(ptr: *anyopaque) usize {
            return self(ptr).inner.len();
        }
        fn appendSlice(ptr: *anyopaque, input_bytes: []const u8) !void {
            return self(ptr).inner.appendSlice(input_bytes);
        }
        fn appendByte(ptr: *anyopaque, byte: u8) !void {
            return self(ptr).inner.appendByte(byte);
        }
        fn appendNTimes(ptr: *anyopaque, byte: u8, count: usize) !void {
            return self(ptr).inner.appendNTimes(byte, count);
        }
        fn writeAt(ptr: *anyopaque, offset: usize, input_bytes: []const u8) !void {
            const owner = self(ptr);
            owner.writes += 1;
            owner.largest = @max(owner.largest, input_bytes.len);
            return owner.inner.writeAt(offset, input_bytes);
        }
        fn crc32Prefix(ptr: *anyopaque, count: usize) !u32 {
            return self(ptr).inner.crc32Prefix(count);
        }
        fn crc32Range(ptr: *anyopaque, offset: usize, count: usize) !u32 {
            return self(ptr).inner.crc32Range(offset, count);
        }
        const vtable = SegmentSink.VTable{ .len = len, .append_slice = appendSlice, .append_byte = appendByte, .append_ntimes = appendNTimes, .write_at = writeAt, .crc32_prefix = crc32Prefix, .crc32_range = crc32Range };
    };
    var output = MemorySegmentSink.init(a);
    defer output.deinit();
    var inner = output.sink();
    var spy = Spy{ .inner = &inner };
    var counted = SegmentSink{ .ptr = &spy, .vtable = &Spy.vtable };
    _ = try writeMergedStoredFieldsInOrder(a, &counted, &inputs, streamed.records, 50_000);
    try std.testing.expect(spy.writes < 1000);
    try std.testing.expect(spy.largest <= 64 * 1024);
    for ([_]usize{ 0, 127, 128, 2730, 49_999 }) |doc| {
        const row = output.out.items[21 + doc * stored_fields_v4_doc_entry_size ..][0..stored_fields_v4_doc_entry_size];
        try std.testing.expectEqual(@as(u32, @intCast(doc / stored_fields_block_doc_target)), std.mem.readInt(u32, row[12..16], .little));
        try std.testing.expectEqual(@as(u32, @intCast((doc % stored_fields_block_doc_target) * 4)), std.mem.readInt(u32, row[16..20], .little));
        try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, row[20..24], .little));
    }
    std.debug.print("LITE_STORED_TABLE documents=50000 writes={d} largest_write={d}\n", .{ spy.writes, spy.largest });
    const Harness = struct {
        fn run(alloc: Allocator, sources: []const MergeInput, sort: []const SegmentIndexSortField) !void {
            var plan = try buildSortedMergePlanAlloc(alloc, sources, sort);
            defer plan.deinit(alloc);
        }
    };
    // A small input exercises head/heap/bounds ownership at every failure site.
    var small = SegmentWriter.init(a);
    defer small.deinit();
    try small.addStoredDoc("a", "");
    try small.addStoredDoc("b", "");
    try small.addStoredDoc("c", "");
    const id_fields = [_]SegmentIndexSortField{.{ .field = "_id", .desc = false }};
    try small.addIndexSortMetadata(&id_fields);
    const small_bytes = try small.build();
    defer a.free(small_bytes);
    var small_reader = try SegmentReader.init(a, small_bytes);
    defer small_reader.deinit();
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{ @as([]const MergeInput, &.{.{ .reader = &small_reader }}), @as([]const SegmentIndexSortField, &id_fields) });
}

test "segment native stored decoder streams compressed input and discards oversized working output" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 17 * 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("large", payload);
    try writer.addStoredDoc("small", "small body");
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = payload.len + payload.len / 2 + 64 * 1024 };
    var cursor = SegmentReader.StoredDocCursor.init(budget.allocator());
    defer cursor.deinit();
    const large = (try cursor.get(&reader, 0)).?;
    try std.testing.expectEqualSlices(u8, payload, large.data);
    const small = (try cursor.get(&reader, 1)).?;
    try std.testing.expectEqualStrings("small body", small.data);
    try std.testing.expect(budget.live < 1024 * 1024);
}

fn legacyV4Fixture(allocator: Allocator, bytes: []const u8) ![]u8 {
    const index: usize = @intCast(std.mem.readInt(u64, bytes[bytes.len - 24 ..][0..8], .big));
    const directory = try integrity.Directory.read(.{ .contiguous = bytes }, index);
    const payload_end: usize = @intCast(directory.offset);
    const index_body = bytes[index + integrity.descriptor_size .. bytes.len - footer_size];
    const output = try allocator.alloc(u8, payload_end + index_body.len + footer_size);
    @memcpy(output[0..payload_end], bytes[0..payload_end]);
    @memcpy(output[payload_end..][0..index_body.len], index_body);
    @memcpy(output[output.len - footer_size ..], bytes[bytes.len - footer_size ..]);
    std.mem.writeInt(u64, output[output.len - 24 ..][0..8], payload_end, .big);
    std.mem.writeInt(u32, output[output.len - 12 ..][0..4], 4, .big);
    std.mem.writeInt(u32, output[output.len - 8 ..][0..4], Crc32.hash(output[payload_end .. output.len - 8]), .big);
    return output;
}

test "paged segment integrity bounds cold selective reads and preserves v4 fallback" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("stable-id", "body");
    const payload = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 'p');
    try writer.addSection(try writer.addField("large"), .vector, payload);
    const bytes = try writer.build();
    defer a.free(bytes);
    const legacy = try legacyV4Fixture(a, bytes);
    defer a.free(legacy);
    const State = struct {
        bytes: []const u8,
        bytes_read: usize = 0,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.bytes_read += out.len;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
        fn source(self: *@This()) SegmentSource {
            return .{ .ranges = .{ .ptr = self, .length = self.bytes.len, .read_into = read, .close = close } };
        }
    };
    var paged_state = State{ .bytes = bytes };
    var legacy_state = State{ .bytes = legacy };
    var paged = try RangeSegmentReader.init(a, paged_state.source(), .{});
    defer paged.deinit();
    var old = try RangeSegmentReader.init(a, legacy_state.source(), .{});
    defer old.deinit();
    const paged_before = paged_state.bytes_read;
    const old_before = legacy_state.bytes_read;
    const view = (try paged.sectionView("large", .vector)).?;
    const old_view = (try old.sectionView("large", .vector)).?;
    var out: [16]u8 = undefined;
    try view.readInto(2 * 1024 * 1024, &out);
    try std.testing.expectEqualSlices(u8, payload[0..16], &out);
    try old_view.readInto(2 * 1024 * 1024, &out);
    const cold_bytes = paged_state.bytes_read - paged_before;
    const old_bytes = legacy_state.bytes_read - old_before;
    try std.testing.expect(cold_bytes <= integrity.page_size + 32);
    try std.testing.expect(old_bytes >= payload.len);
    const warm_before = paged_state.bytes_read;
    try view.readInto(2 * 1024 * 1024, &out);
    try std.testing.expectEqual(@as(usize, 0), paged_state.bytes_read - warm_before);
    try view.readInto(3 * 1024 * 1024, &out);
    const evicted_before = paged_state.bytes_read;
    try view.readInto(2 * 1024 * 1024, &out);
    try std.testing.expectEqual(@as(usize, 16), paged_state.bytes_read - evicted_before);
    const id = (try old.storedDocIdOwned(a, 0, 1024)).?;
    defer a.free(id);
    try std.testing.expectEqualStrings("stable-id", id);
    std.debug.print("LITE_PAGED_INTEGRITY field_bytes={d} cold_bytes={d} legacy_cold_bytes={d} warm_bytes=0\n", .{ payload.len, cold_bytes, old_bytes });
    const Harness = struct {
        fn run(allocator: Allocator, source: SegmentSource) !void {
            var reader = try RangeSegmentReader.init(allocator, source, .{});
            defer reader.deinit();
            const field = (try reader.sectionView("large", .vector)).?;
            var result: [16]u8 = undefined;
            try field.readInto(2 * 1024 * 1024, &result);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{paged_state.source()});
}

test "native document ordinals read bounded pages without retaining the column" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    writer.doc_count = 300_000;
    const ordinals = try a.alloc(u32, writer.doc_count);
    defer a.free(ordinals);
    for (ordinals, 0..) |*ordinal, i| ordinal.* = @intCast(i + 1);
    try writer.addDocOrdinals(ordinals);
    const bytes = try writer.build();
    defer a.free(bytes);
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = a, .limit = 192 * 1024 };
    var reader = try SegmentReader.initSource(budget.allocator(), .{ .contiguous = bytes });
    defer reader.deinit();
    try std.testing.expect(reader.nativeNavigationBytes() < 8192);
    try std.testing.expectEqual(@as(?u32, 1), try reader.docOrdinal(0));
    try std.testing.expectEqual(@as(?u32, 300_000), try reader.docOrdinal(299_999));
    try std.testing.expectEqual(@as(?u32, null), try reader.docOrdinal(300_000));
    try std.testing.expect(reader.nativeNavigationBytes() < 192 * 1024);
}

test "stored merges use bounded identity scopes instead of stable identity pages" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    const id: [1024]u8 = @splat('i');
    for (0..4096) |_| try writer.addStoredDoc(&id, "{}");
    const bytes = try writer.build();
    defer a.free(bytes);
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = a };
    var reader = try SegmentReader.initSource(budget.allocator(), .{ .contiguous = bytes });
    defer reader.deinit();
    const before = reader.nativeNavigationBytes();
    var output = MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    _ = try writeMergedStoredFields(a, &sink, &.{.{ .reader = &reader }}, 4096);
    const after = reader.nativeNavigationBytes();
    std.debug.print("LITE_MERGE_IDENTITIES documents=4096 id_bytes=4194304 before={d} after={d} retained_ids={d}\n", .{ before, after, reader.native.?.identity_bytes });
    try std.testing.expect(after < 512 * 1024);
}

test "native admission leaves vector payload in source" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("one", "{}");
    const payload = try a.alloc(u8, 4 * 1024 * 1024);
    @memset(payload, 9);
    try writer.addSectionOwned(try writer.addField("embedding"), .vector, payload);
    const bytes = try writer.build();
    defer a.free(bytes);
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = a };
    var reader = try SegmentReader.initSource(budget.allocator(), .{ .contiguous = bytes });
    defer reader.deinit();
    std.debug.print("LITE_NATIVE_VECTOR payload_bytes={d} admission_live={d} peak={d}\n", .{ 4 * 1024 * 1024, budget.live, budget.peak });
    try std.testing.expect(budget.live < 512 * 1024);
}

test "streamed typed byte merge bounds working memory and roundtrips trailer directories" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var values = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 128);
    defer values.deinit();
    const value: [1024]u8 = @splat('x');
    for (0..4096) |doc| {
        try writer.addStoredDoc("doc", "{}");
        try values.add(@intCast(doc), .{ .bytes_val = &value });
    }
    try writer.addSectionOwned(try writer.addField("body"), .typed_doc_values, try values.build());
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = a };
    var output = MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    try std.testing.expect(try writeMergeTypedDocValuesSections(budget.allocator(), &sink, &.{.{ .reader = &reader }}, "body", null));
    const merged = output.out.items;
    var fresh = try typed_dv.TypedDocValuesReader.init(a, merged);
    defer fresh.deinit();
    const last = (try fresh.getBytesAlloc(4095)).?;
    defer a.free(last);
    try std.testing.expectEqualSlices(u8, &value, last);
    std.debug.print("LITE_TYPED_MERGE raw_values=4194304 output={d} peak={d} allocs={d}\n", .{ merged.len, budget.peak, budget.alloc_calls });
    try std.testing.expect(budget.peak < 1024 * 1024);
}

test "incremental page CRCs reread only patched pages and unwind failures" {
    const Sweep = struct {
        const Counting = struct {
            inner: *SegmentSink,
            checksum_bytes: usize = 0,
            fn self(ptr: *anyopaque) *@This() {
                return @ptrCast(@alignCast(ptr));
            }
            fn len(ptr: *anyopaque) usize {
                return self(ptr).inner.len();
            }
            fn append(ptr: *anyopaque, bytes: []const u8) !void {
                try self(ptr).inner.appendSlice(bytes);
            }
            fn byte(ptr: *anyopaque, value: u8) !void {
                try self(ptr).inner.appendByte(value);
            }
            fn repeat(ptr: *anyopaque, value: u8, count: usize) !void {
                try self(ptr).inner.appendNTimes(value, count);
            }
            fn patch(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
                try self(ptr).inner.writeAt(offset, bytes);
            }
            fn prefix(ptr: *anyopaque, count: usize) !u32 {
                return range(ptr, 0, count);
            }
            fn range(ptr: *anyopaque, offset: usize, count: usize) !u32 {
                self(ptr).checksum_bytes += count;
                return self(ptr).inner.crc32Range(offset, count);
            }
            const table = SegmentSink.VTable{ .len = len, .append_slice = append, .append_byte = byte, .append_ntimes = repeat, .write_at = patch, .crc32_prefix = prefix, .crc32_range = range };
        };
        fn run(allocator: Allocator) !void {
            var output = MemorySegmentSink.init(allocator);
            defer output.deinit();
            var base = output.sink();
            var count = Counting{ .inner = &base };
            var counting = SegmentSink{ .ptr = &count, .vtable = &Counting.table };
            var tracked = PageChecksumSink.init(allocator, &counting);
            defer tracked.deinit();
            var sink = tracked.sink();
            try sink.appendNTimes('a', 4 * integrity.page_size + 17);
            try sink.writeAt(2, "header");
            try sink.writeAt(4 * integrity.page_size + 2, "tail");
            const directory = try writePageDirectory(&sink);
            try std.testing.expectEqual(@as(usize, integrity.page_size + 17), count.checksum_bytes);
            for (0..5) |page| {
                const start = page * integrity.page_size;
                const end = @min(start + integrity.page_size, directory.offset);
                const expected = Crc32.hash(output.out.items[start..@intCast(end)]);
                const actual = std.mem.readInt(u32, output.out.items[@as(usize, @intCast(directory.offset)) + page * 4 ..][0..4], .big);
                try std.testing.expectEqual(expected, actual);
            }
            try directory.validate(.{ .contiguous = output.out.items }, directory.offset + directory.length);
        }
    };
    try Sweep.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Sweep.run, .{});
}

test "native vector and stored column scopes decode borrowed payload ranges" {
    const a = std.testing.allocator;
    const vectors = @import("section/vector_section.zig");
    const columns = @import("section/doc_values.zig");
    var content = vectors.VectorIndexContent.init(a, 8, .l2_squared, .recall_optimized);
    defer content.deinit();
    const row = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try content.addVector(&row, 0);
    const vector_bytes = try vectors.writeVectorSection(a, &content, 42);
    defer a.free(vector_bytes);
    var column = columns.DocValuesWriter.init(a, 1);
    defer column.deinit();
    try column.add(0, "column value");
    const column_bytes = try column.build();
    defer a.free(column_bytes);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("doc", "body");
    const field = try writer.addField("payload");
    try writer.addSection(field, .vector, vector_bytes);
    try writer.addSection(field, .columnar_stored, column_bytes);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    try std.testing.expectError(error.NativeSectionRequiresScope, reader.getSection("payload", .vector));
    try std.testing.expectError(error.NativeSectionRequiresScope, reader.getSection("payload", .columnar_stored));
    var vector = (try reader.vectorIndexScoped(a, "payload")).?;
    defer vector.deinit();
    const reconstructed = try vectors.reconstructVectors(a, &vector, &.{0});
    defer a.free(reconstructed);
    try std.testing.expectEqualSlices(f32, &row, reconstructed);
    var stored = (try reader.storedColumnScoped(a, "payload")).?;
    defer stored.deinit();
    const value = (try stored.get(0, 1)).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("column value", value);
}

test "incremental page CRC tracking preserves aliased sink appends" {
    const a = std.testing.allocator;
    var output = MemorySegmentSink.init(a);
    defer output.deinit();
    var base = output.sink();
    var tracked = PageChecksumSink.init(a, &base);
    defer tracked.deinit();
    var sink = tracked.sink();
    try sink.appendNTimes('a', integrity.page_size);
    try sink.appendSlice(output.out.items);
    const directory = try writePageDirectory(&sink);
    try std.testing.expectEqual(@as(u64, 2 * integrity.page_size), directory.offset);
    const expected = Crc32.hash(output.out.items[0..integrity.page_size]);
    for (0..2) |page| {
        const actual = std.mem.readInt(u32, output.out.items[@as(usize, @intCast(directory.offset)) + page * 4 ..][0..4], .big);
        try std.testing.expectEqual(expected, actual);
    }
}

test "page backend I/O leaves cache mutex unlocked" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, integrity.page_size * 2 + 8);
    defer a.free(bytes);
    @memset(bytes, 'a');
    const page_crc = Crc32.hash(bytes[0..integrity.page_size]);
    for (0..2) |i| std.mem.writeInt(u32, bytes[2 * integrity.page_size + i * 4 ..][0..4], page_crc, .big);
    const State = struct {
        bytes: []const u8,
        page_source: ?*integrity.PagedSource = null,
        observe: bool = true,
        lock_held: bool = false,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.observe) {
                if (self.page_source.?.mutex.tryLock()) self.page_source.?.mutex.unlock() else self.lock_held = true;
            }
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .bytes = bytes };
    const original = SegmentSource{ .ranges = .{ .ptr = &state, .length = bytes.len, .read_into = State.read, .close = State.close } };
    const paged = try integrity.PagedSource.init(a, original, .{ .offset = 2 * integrity.page_size, .length = 8, .checksum = Crc32.hash(bytes[2 * integrity.page_size ..]) });
    defer paged.deinit();
    state.page_source = paged;
    const source = paged.source();
    var output: [16]u8 = undefined;
    try source.readInto(0, &output);
    try source.readInto(integrity.page_size, &output);
    state.observe = true;
    try source.readInto(0, &output);
    std.debug.print("REVIEW_WARM_PAGE backend_io_under_global_lock={any}\n", .{state.lock_held});
    try std.testing.expect(!state.lock_held);
}

test "streaming ID sort keys reuse bounded payload owners" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    for (0..4096) |doc| {
        var key: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&key, "doc-{d:0>6}", .{doc}), "");
    }
    const fields = [_]SegmentIndexSortField{.{ .field = "_id", .desc = false }};
    try writer.addIndexSortMetadata(&fields);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a };
    var plan = try buildSortedMergePlanAlloc(budget.allocator(), &.{.{ .reader = &reader }}, &fields);
    defer plan.deinit(budget.allocator());
    try std.testing.expect(budget.alloc_calls < 64);
    std.debug.print("REVIEW_ID_SORT rows=4096 allocations={d} peak={d} retained_ids={d}\n", .{ budget.alloc_calls, budget.peak, reader.native.?.identity_bytes });
}

test "sorted typed compaction streams fanin and preserves reordered sparse values" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var values = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 128);
    defer values.deinit();
    var sparse = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 128);
    defer sparse.deinit();
    var value: [1024]u8 = @splat('x');
    for (0..256) |doc| {
        try writer.addStoredDoc("doc", "{}");
        value[0] = @intCast(doc);
        // Sparse values also exercise missing records in the cursor path.
        try values.add(@intCast(doc), .{ .bytes_val = &value });
        if (doc % 3 != 0) try sparse.add(@intCast(doc), .{ .bytes_val = &value });
    }
    try writer.addSectionOwned(try writer.addField("body"), .typed_doc_values, try values.build());
    try writer.addSectionOwned(try writer.addField("sparse"), .typed_doc_values, try sparse.build());
    const bytes = try writer.build();
    defer a.free(bytes);
    var readers: [8]SegmentReader = undefined;
    var initialized: usize = 0;
    defer for (readers[0..initialized]) |*reader| reader.deinit();
    var inputs: [8]MergeInput = undefined;
    for (&readers, &inputs) |*reader, *input| {
        reader.* = try SegmentReader.init(a, bytes);
        initialized += 1;
        input.* = .{ .reader = reader };
    }
    var records: [2048]SortedMergeDoc = undefined;
    for (0..256) |doc| for (0..8) |input| {
        records[doc * 8 + input] = .{ .ref = .{ .input_idx = input, .doc_id = @intCast(doc) } };
    };
    for ([_][]const u8{ "body", "sparse" }) |field| {
        for (0..2) |reorder| {
            if (reorder != 0) std.mem.reverse(SortedMergeDoc, &records);
            var output = MemorySegmentSink.init(a);
            defer output.deinit();
            var sink = output.sink();
            var decodes: usize = 0;
            try std.testing.expect(try writeMergeTypedDocValuesSectionsInOrder(a, &sink, &inputs, field, &records, &decodes));
            if (reorder == 0) {
                std.debug.print("SORTED_TYPED_FANIN rows=2048 distinct_chunks=16 decodes={d}\n", .{decodes});
                try std.testing.expectEqual(@as(usize, 16), decodes);
            } else {
                std.debug.print("REORDERED_TYPED_FANIN rows=2048 decodes={d}\n", .{decodes});
                try std.testing.expect(decodes < 400);
            }
            var result = try typed_dv.TypedDocValuesReader.init(a, output.out.items);
            defer result.deinit();
            for (records, 0..) |record, out| {
                const actual = try result.getBytesAlloc(@intCast(out));
                if (actual) |found| {
                    defer a.free(found);
                    try std.testing.expect(std.mem.eql(u8, field, "body") or record.ref.doc_id % 3 != 0);
                    try std.testing.expectEqual(@as(u8, @intCast(record.ref.doc_id)), found[0]);
                } else {
                    try std.testing.expectEqualStrings("sparse", field);
                    try std.testing.expectEqual(@as(u32, 0), record.ref.doc_id % 3);
                }
            }
        }
        std.mem.reverse(SortedMergeDoc, &records);
    }
}

test "section CRC tracking rereads only patched blocks" {
    const a = std.testing.allocator;
    const Counting = struct {
        inner: SegmentSink,
        read_bytes: usize = 0,
        fn self(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn len(ptr: *anyopaque) usize {
            return self(ptr).inner.len();
        }
        fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
            try self(ptr).inner.appendSlice(bytes);
        }
        fn appendByte(ptr: *anyopaque, byte: u8) !void {
            try self(ptr).inner.appendByte(byte);
        }
        fn appendNTimes(ptr: *anyopaque, byte: u8, count: usize) !void {
            try self(ptr).inner.appendNTimes(byte, count);
        }
        fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
            try self(ptr).inner.writeAt(offset, bytes);
        }
        fn crcPrefix(ptr: *anyopaque, count: usize) !u32 {
            self(ptr).read_bytes += count;
            return self(ptr).inner.crc32Prefix(count);
        }
        fn crcRange(ptr: *anyopaque, offset: usize, count: usize) !u32 {
            self(ptr).read_bytes += count;
            return self(ptr).inner.crc32Range(offset, count);
        }
        const vtable = SegmentSink.VTable{ .len = len, .append_slice = appendSlice, .append_byte = appendByte, .append_ntimes = appendNTimes, .write_at = writeAt, .crc32_prefix = crcPrefix, .crc32_range = crcRange };
    };
    var memory = MemorySegmentSink.init(a);
    defer memory.deinit();
    var counted = Counting{ .inner = memory.sink() };
    var destination = SegmentSink{ .ptr = &counted, .vtable = &Counting.vtable };
    var tracker = PageChecksumSink.init(a, &destination);
    defer tracker.deinit();
    var sink = tracker.sink();
    try sink.appendSlice("prefix");
    for ([_]usize{ 0, 1, 31, 65535, 65536, 65537, 4 * 1024 * 1024 }) |size| {
        const start = sink.len();
        sink.beginSection();
        try sink.appendNTimes('a', size);
        const before = counted.read_bytes;
        try std.testing.expectEqual(Crc32.hash(memory.out.items[start..]), try sink.crc32Range(start, size));
        try std.testing.expectEqual(before, counted.read_bytes);
        if (size > 0) {
            try sink.writeAt(start, "b");
            try std.testing.expectEqual(Crc32.hash(memory.out.items[start..]), try sink.crc32Range(start, size));
            try std.testing.expectEqual(@min(size, integrity.page_size), counted.read_bytes - before);
        }
    }
}

test "streaming byte sort heads keep successor and bounds alive across chunks and allocation failures" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var values = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 2);
    defer values.deinit();
    for (0..7) |doc| {
        var key: [32]u8 = undefined;
        const id = try std.fmt.bufPrint(&key, "key-{d}", .{doc});
        try writer.addStoredDoc(id, "");
        try values.add(@intCast(doc), .{ .bytes_val = id });
    }
    try writer.addSectionOwned(try writer.addField("key"), .typed_doc_values, try values.build());
    const fields = [_]SegmentIndexSortField{.{ .field = "key", .desc = false }};
    try writer.addIndexSortMetadata(&fields);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    const Sweep = struct {
        fn run(allocator: Allocator, input: *const SegmentReader, sort: []const SegmentIndexSortField) !void {
            var plan = try buildSortedMergePlanAlloc(allocator, &.{ .{ .reader = input }, .{ .reader = input } }, sort);
            defer plan.deinit(allocator);
            try std.testing.expectEqualStrings("key-0", plan.first_keys[0].bytes_val);
            try std.testing.expectEqualStrings("key-6", plan.last_keys[0].bytes_val);
            for (plan.records, 0..) |record, i| try std.testing.expectEqual(@as(u32, @intCast(i / 2)), record.ref.doc_id);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{ &reader, @as([]const SegmentIndexSortField, &fields) });
}

test "section checksum scratch unwinds failures and handles aliased appends and boundary patches" {
    const Sweep = struct {
        fn run(allocator: Allocator) !void {
            var memory = MemorySegmentSink.init(allocator);
            defer memory.deinit();
            var inner = memory.sink();
            var tracker = PageChecksumSink.init(allocator, &inner);
            defer tracker.deinit();
            var sink = tracker.sink();
            try sink.appendSlice("prefix");
            const start = sink.len();
            sink.beginSection();
            try sink.appendSlice(memory.out.items);
            try sink.appendNTimes('x', 2 * integrity.page_size);
            try sink.writeAt(start + integrity.page_size - 2, "patch across boundary");
            try std.testing.expectEqual(Crc32.hash(memory.out.items[start..]), try sink.crc32Range(start, sink.len() - start));
            _ = try writePageDirectory(&sink);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Sweep.run, .{});
}

test "reordered typed gather bounds scratch for oversized values and cleans up allocation failures" {
    const a = std.testing.allocator;
    const Sweep = struct {
        fn run(allocator: Allocator, input: *const SegmentReader, value_len: usize) !void {
            const records = [_]SortedMergeDoc{
                .{ .ref = .{ .input_idx = 0, .doc_id = 2 } },
                .{ .ref = .{ .input_idx = 0, .doc_id = 1 } },
                .{ .ref = .{ .input_idx = 0, .doc_id = 0 } },
            };
            var memory = MemorySegmentSink.init(allocator);
            defer memory.deinit();
            var sink = memory.sink();
            try std.testing.expect(try writeMergeTypedDocValuesSectionsInOrder(allocator, &sink, &.{.{ .reader = input }}, "body", &records, null));
            var output = try typed_dv.TypedDocValuesReader.init(allocator, memory.out.items);
            defer output.deinit();
            for (records, 0..) |record, doc| {
                const value = (try output.getBytesAlloc(@intCast(doc))).?;
                defer allocator.free(value);
                try std.testing.expectEqual(value_len, value.len);
                try std.testing.expectEqual(@as(u8, @intCast(record.ref.doc_id)), value[0]);
            }
        }
    };
    for ([_]usize{ 32, 130 * 1024 }) |size| {
        var writer = SegmentWriter.init(a);
        defer writer.deinit();
        var values = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 2);
        defer values.deinit();
        const value = try a.alloc(u8, size);
        defer a.free(value);
        @memset(value, 'x');
        for (0..3) |doc| {
            try writer.addStoredDoc("id", "");
            value[0] = @intCast(doc);
            try values.add(@intCast(doc), .{ .bytes_val = value });
        }
        try writer.addSectionOwned(try writer.addField("body"), .typed_doc_values, try values.build());
        const bytes = try writer.build();
        defer a.free(bytes);
        var reader = try SegmentReader.init(a, bytes);
        defer reader.deinit();
        if (size == 32) try std.testing.checkAllAllocationFailures(a, Sweep.run, .{ &reader, size }) else try Sweep.run(a, &reader, size);
    }
}

test "shared range cache keeps warm hits live during backend fills and retries failures" {
    const a = std.testing.allocator;
    const Cache = @import("segment_source.zig").ConcurrentBlockCache;
    const State = struct {
        bytes: [2048]u8 = @splat('a'),
        cache: ?*Cache = null,
        calls: usize = 0,
        fail: bool = false,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (!self.cache.?.mutex.tryLock()) return error.BackendUnderCacheLock;
            self.cache.?.mutex.unlock();
            if (offset != 0) {
                var warm: [1]u8 = undefined;
                try self.cache.?.readInto(0, &warm);
                try std.testing.expectEqual(@as(u8, 'a'), warm[0]);
            }
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
            if (self.fail) {
                @memset(out, 'x');
                return error.TestReadFailure;
            }
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{};
    var cache = try Cache.init(a, .{ .ranges = .{ .ptr = &state, .length = state.bytes.len, .read_into = State.read, .close = State.close } }, 1280);
    defer cache.deinit();
    state.cache = &cache;
    var bulk: [512]u8 = undefined;
    try cache.readInto(0, &bulk);
    try std.testing.expectEqual(@as(usize, 0), cache.retainedBytes());
    state.calls = 0;
    var out: [16]u8 = undefined;
    try cache.readInto(0, &out);
    state.fail = true;
    try std.testing.expectError(error.TestReadFailure, cache.readInto(256, &out));
    try cache.readInto(0, &out);
    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat('a'))), &out);
    state.fail = false;
    try cache.readInto(256, &out);
    try std.testing.expectEqual(@as(usize, 3), state.calls);
    try std.testing.expect(cache.retainedBytes() <= 1280);
    const Sweep = struct {
        fn run(allocator: Allocator) !void {
            var local = State{};
            var scoped = try Cache.init(allocator, .{ .ranges = .{ .ptr = &local, .length = local.bytes.len, .read_into = State.read, .close = State.close } }, 1280);
            defer scoped.deinit();
            local.cache = &scoped;
            var result: [16]u8 = undefined;
            for ([_]u64{ 0, 256, 512 }) |offset| try scoped.readInto(offset, &result);
            try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat('a'))), &result);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{});
    var tiny = try Cache.init(a, .{ .contiguous = state.bytes[0..32] }, 160 * 1024);
    defer tiny.deinit();
    try tiny.readInto(0, &out);
    try std.testing.expectEqual(@as(usize, 64), tiny.retainedBytes());
}

test "native artifact window reuse reduces physical reads across decoder windows" {
    const a = std.testing.allocator;
    const native = @import("storage/lite/native.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/range-cache.aflite", .{tmp.sub_path});
    defer a.free(path);
    var file = try native.NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const bytes = try a.alloc(u8, 128 * 1024);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i);
    try file.putIndexCatalogRecord("/segment", bytes);
    file.page_cache_enabled.store(false, .monotonic);
    const checkpoint = file.activeCheckpoint();
    var value = try file.openIndexValue(a, "/segment", checkpoint);
    defer value.deinit(a);
    const State = struct {
        file: *native.NativeFile,
        value: native.NativeFile.IndexValue,
        checkpoint: native.CheckpointSlot,
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.file.readIndexValueInto(self.value, offset, out, self.checkpoint);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .file = &file, .value = value, .checkpoint = checkpoint };
    const source = SegmentSource{ .ranges = .{ .ptr = &state, .length = bytes.len, .read_into = State.read, .close = State.close } };
    var output: [8192]u8 = undefined;
    const before = file.test_value_read_calls.load(.monotonic);
    const bytes_before = file.test_value_read_bytes.load(.monotonic);
    for (0..16) |i| {
        try source.readInto(i * output.len, &output);
        try std.testing.expectEqualSlices(u8, bytes[i * output.len ..][0..output.len], &output);
    }
    const baseline = file.test_value_read_calls.load(.monotonic) - before;
    const baseline_bytes = file.test_value_read_bytes.load(.monotonic) - bytes_before;
    var cache = try @import("segment_source.zig").ConcurrentBlockCache.init(a, source, 160 * 1024);
    defer cache.deinit();
    const start = file.test_value_read_calls.load(.monotonic);
    const bytes_start = file.test_value_read_bytes.load(.monotonic);
    for (0..16) |i| {
        try cache.readInto(i * output.len, &output);
        try std.testing.expectEqualSlices(u8, bytes[i * output.len ..][0..output.len], &output);
    }
    const cached = file.test_value_read_calls.load(.monotonic) - start;
    const cached_bytes = file.test_value_read_bytes.load(.monotonic) - bytes_start;
    std.debug.print("LITE_RANGE_CACHE decoder_windows=16 value_io_calls_before={d} after={d} bytes_before={d} after={d} retained={d}\n", .{ baseline, cached, baseline_bytes, cached_bytes, cache.retainedBytes() });
    try std.testing.expect(cached_bytes <= baseline_bytes);
    try std.testing.expect(cached < baseline);
    try std.testing.expect(cache.retainedBytes() <= 160 * 1024);
}

test "streaming typed admission estimates peak chunks instead of complete columns" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var values = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 16);
    defer values.deinit();
    var value: [4096]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(123);
    prng.random().bytes(&value);
    for (0..2048) |i| {
        try values.add(@intCast(i), .{ .bytes_val = &value });
        try writer.addUnstoredDoc();
    }
    const column = try values.build();
    defer a.free(column);
    try writer.addSection(try writer.addField("value"), .typed_doc_values, column);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    const estimate = try reader.typedMergeWorkingSetBytes();
    try std.testing.expect(estimate < column.len);
    std.debug.print("LITE_TYPED_ADMISSION serialized={d} previous_reservation={d} bounded_estimate={d}\n", .{ column.len, column.len * 2, estimate });
}

test "typed merge admission rejects malformed legacy chunk extents" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addUnstoredDoc();
    var bad: [14]u8 = @splat(0);
    bad[0] = @backingInt(typed_dv.ValueType.bytes_val);
    std.mem.writeInt(u32, bad[1..5], 1, .little);
    std.mem.writeInt(u64, bad[5..13], std.math.maxInt(u64), .little);
    try writer.addSection(try writer.addField("value"), .typed_doc_values, &bad);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    try std.testing.expectError(error.InvalidData, reader.typedMergeWorkingSetBytes());
}

test "Snappy admission bounds cover compressed and incompressible chunks" {
    const a = std.testing.allocator;
    const unterminated: [10]u8 = @splat(0x80);
    try std.testing.expectError(error.CorruptInput, snappy.decodedLen(&unterminated));
    const overflow = [_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f };
    try std.testing.expectError(error.CorruptInput, snappy.decodedLen(&overflow));
    var prng = std.Random.DefaultPrng.init(879);
    for ([_]usize{ 0, 1, 64, 4096, 65536, 160 * 1024 }) |size| {
        const raw = try a.alloc(u8, size);
        defer a.free(raw);
        for ([_]bool{ false, true }) |random| {
            if (random) prng.random().bytes(raw) else @memset(raw, 'x');
            const encoded = try snappy.encode(a, raw);
            defer a.free(encoded);
            try std.testing.expect(try snappy.decodedSizeUpperBound(encoded.len) >= raw.len);
        }
    }
}

test "native range caches share admission and evict idle owners without blocking active fills" {
    const a = std.testing.allocator;
    const resources = @import("storage/resource_manager.zig");
    const Cache = @import("segment_source.zig").ConcurrentBlockCache;
    const Backend = struct {
        bytes: []const u8,
        reads: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 32 * 1024, .hard_limit_bytes = 48 * 1024 };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    var bytes: [256 * 1024]u8 = undefined;
    for (&bytes, 0..) |*byte, i| byte.* = @truncate(i);
    var backend = Backend{ .bytes = &bytes };
    const source: SegmentSource = .{ .ranges = .{ .ptr = &backend, .length = bytes.len, .read_into = Backend.read, .close = Backend.close, .resource_manager = &manager } };
    var caches: [12]Cache = undefined;
    for (&caches) |*cache| cache.* = try Cache.init(a, source, 80 * 1024);
    {
        defer for (&caches) |*cache| cache.deinit();
        var out: [16]u8 = undefined;
        for (&caches, 0..) |*cache, i| {
            try cache.readInto(i * 16384, &out);
            try std.testing.expectEqualSlices(u8, bytes[i * 16384 ..][0..out.len], &out);
            try std.testing.expect(manager.sliceStats(.lite_native_page_cache).used_bytes <= 48 * 1024);
        }
        try std.testing.expectEqual(@as(usize, 0), caches[0].retainedBytes());
        try caches[0].readInto(0, &out);
        try std.testing.expect(caches[0].fill_mutex.tryLock());
        {
            defer caches[0].fill_mutex.unlock();
            // At most one spare fill fits. The next slot cannot evict the
            // pinned owner and returns uncached bytes without failing reads.
            try caches[1].readInto(16384, &out);
            try std.testing.expectEqualSlices(u8, bytes[16384..][0..out.len], &out);
            try std.testing.expect(caches[0].retainedBytes() > 0);
        }
        try std.testing.expect(manager.reclaimed_bytes.load(.acquire) > 0);
    }
    const stats = manager.sliceStats(.lite_native_page_cache);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.peak_bytes <= 48 * 1024);
}

test "typed merge admission uses exact authenticated summaries and legacy prefixes" {
    const a = std.testing.allocator;
    const input = try a.alloc(u8, 1024 * 1024);
    defer a.free(input);
    var random = std.Random.DefaultPrng.init(81);
    random.random().bytes(input);
    for ([_]bool{ false, true }) |streamed| {
        var writer = SegmentWriter.init(a);
        defer writer.deinit();
        try writer.addUnstoredDoc();
        var values = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 1024);
        defer values.deinit();
        var encoded = MemorySegmentSink.init(a);
        defer encoded.deinit();
        var sink = encoded.sink();
        if (streamed) {
            var streaming = typed_dv.StreamingWriter.init(a, &sink, .bytes_val);
            defer streaming.deinit();
            for (0..1024) |doc| try streaming.add(@intCast(doc), .{ .bytes_val = input[doc * 1024 ..][0..1024] });
            try std.testing.expect(try streaming.finish());
        } else {
            for (0..1024) |doc| try values.add(@intCast(doc), .{ .bytes_val = input[doc * 1024 ..][0..1024] });
            const data = try values.build();
            defer a.free(data);
            try sink.appendSlice(data);
        }
        try writer.addSection(try writer.addField("value"), .typed_doc_values, encoded.out.items);
        const bytes = try writer.build();
        defer a.free(bytes);
        var reader = try SegmentReader.init(a, bytes);
        defer reader.deinit();
        const estimate = try reader.typedMergeWorkingSetBytes();
        try std.testing.expect(estimate < if (streamed) @as(u64, 256 * 1024) else @as(u64, 4 * 1024 * 1024));
        std.debug.print("typed merge exact streamed={} encoded={d} admission={d}\n", .{ streamed, encoded.out.items.len, estimate });
    }
}

test "native integrity authenticates without retaining payload when aggregate cache cannot fit a page" {
    const a = std.testing.allocator;
    const resources = @import("storage/resource_manager.zig");
    const Backend = struct {
        bytes: []const u8,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 2048, .hard_limit_bytes = 4096 };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    const bytes = try a.alloc(u8, 2 * integrity.page_size + 8);
    defer a.free(bytes);
    @memset(bytes, 17);
    for (0..2) |i| std.mem.writeInt(u32, bytes[2 * integrity.page_size + i * 4 ..][0..4], Crc32.hash(bytes[i * integrity.page_size ..][0..integrity.page_size]), .big);
    var backend = Backend{ .bytes = bytes };
    const original: SegmentSource = .{ .ranges = .{ .ptr = &backend, .length = bytes.len, .read_into = Backend.read, .close = Backend.close, .resource_manager = &manager } };
    const paged = try integrity.PagedSource.init(a, original, .{ .offset = 2 * integrity.page_size, .length = 8, .checksum = Crc32.hash(bytes[2 * integrity.page_size ..]) });
    defer paged.deinit();
    const source = paged.source();
    var out: [16]u8 = undefined;
    try source.readInto(0, &out);
    try std.testing.expectEqualSlices(u8, bytes[0..16], &out);
    try std.testing.expectEqual(@as(usize, 0), paged.retainedBytes());
    try std.testing.expectEqual(@as(u8, 1), paged.validations[0].load(.acquire));
    try source.readInto(32, &out);
    bytes[integrity.page_size] ^= 1;
    try std.testing.expectError(error.CrcMismatch, source.readInto(integrity.page_size, &out));
    bytes[integrity.page_size] ^= 1;
    try std.testing.expectError(error.CrcMismatch, source.readInto(integrity.page_size, &out));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_page_cache).used_bytes);
}

test "scoped native identity scans retain no stable segment ID pages" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    const id: [1024]u8 = @splat('i');
    for (0..4096) |_| try writer.addStoredDoc(&id, "{}");
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.initSource(a, .{ .contiguous = bytes });
    defer reader.deinit();
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = a };
    var scratch = SegmentReadScratch.init(budget.allocator(), 64 * 1024);
    defer scratch.deinit();
    for (0..4096) |i| {
        scratch.reset();
        const actual = (try reader.storedIdScoped(scratch.allocator(), @intCast(i))).?;
        try std.testing.expectEqualSlices(u8, &id, actual);
    }
    std.debug.print("scoped ID scan bytes=4194304 retained={d} scratch_peak={d}\n", .{ reader.native.?.identity_bytes, budget.peak });
    try std.testing.expectEqual(@as(usize, 0), reader.native.?.identity_bytes);
    try std.testing.expect(budget.peak < 128 * 1024);
}

test "merge provenance compresses append order and bounds shuffled source maps" {
    const a = std.testing.allocator;
    var append = try MergeSourceMap.init(a, &.{50_000});
    defer append.deinit();
    for (0..50_000) |i| try append.record(0, @intCast(i), @intCast(i));
    try append.finishOutput(50_000);
    try std.testing.expect(append.retainedBytes() < 1024);
    try std.testing.expectEqual(@as(u32, 49_999), append.lookup(0, 49_999).?.doc);
    var shuffled = try MergeSourceMap.init(a, &.{50_000});
    defer shuffled.deinit();
    var output: u32 = 0;
    for (0..50_000) |i| {
        const source: u32 = @intCast(49_999 - i);
        if (source == 37) continue;
        try shuffled.record(0, source, output);
        output += 1;
    }
    try shuffled.finishOutput(output);
    try std.testing.expect(shuffled.retainedBytes() < 201 * 1024);
    try std.testing.expect(shuffled.lookup(0, 37) == null);
    for (0..50_000) |i| {
        if (i == 37) continue;
        const expected: u32 = @intCast(if (i > 37) 49_999 - i else 49_998 - i);
        try std.testing.expectEqual(expected, shuffled.lookup(0, @intCast(i)).?.doc);
    }
    std.debug.print("merge provenance rows=50000 append_bytes={d} shuffled_bytes={d}\n", .{ append.retainedBytes(), shuffled.retainedBytes() });
}

test "merge provenance preserves source identities across retries and output boundaries" {
    const a = std.testing.allocator;
    var map = try MergeSourceMap.init(a, &.{ 6, 6 });
    defer map.deinit();
    // A rejected three-document candidate, followed by an accepted smaller
    // window. Remaining source documents are overwritten in later outputs.
    for (0..3) |i| try map.record(0, @intCast(i), @intCast(i));
    try map.record(0, 0, 0);
    try map.finishOutput(1);
    try map.record(0, 2, 0);
    try map.record(0, 1, 1);
    try map.record(1, 4, 2);
    try map.finishOutput(3);
    try std.testing.expectEqualDeep(MergeSourceMap.Location{ .segment = 0, .doc = 0 }, map.lookup(0, 0).?);
    try std.testing.expectEqualDeep(MergeSourceMap.Location{ .segment = 1, .doc = 0 }, map.lookup(0, 2).?);
    try std.testing.expectEqualDeep(MergeSourceMap.Location{ .segment = 1, .doc = 1 }, map.lookup(0, 1).?);
    try std.testing.expectEqualDeep(MergeSourceMap.Location{ .segment = 1, .doc = 2 }, map.lookup(1, 4).?);
    try std.testing.expect(map.lookup(0, 3) == null);
    try std.testing.expect(map.lookup(1, 0) == null);
    try std.testing.expect(map.lookup(2, 0) == null);
    map.reset();
    try std.testing.expect(map.lookup(0, 0) == null);
    try map.record(1, 1, 0);
    try map.finishOutput(1);
    try std.testing.expectEqualDeep(MergeSourceMap.Location{ .segment = 0, .doc = 0 }, map.lookup(1, 1).?);
}

test "merge provenance allocation failures release spans tables and output boundaries" {
    const Scenario = struct {
        fn run(backing: Allocator) !void {
            var stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            var map = try MergeSourceMap.init(stable.allocator(), &.{ 8, 8 });
            defer map.deinit();
            try map.record(0, 4, 0);
            try map.record(0, 0, 1);
            try map.record(1, 5, 2);
            try map.finishOutput(3);
            try map.record(1, 2, 0);
            try map.finishOutput(1);
            try std.testing.expectEqual(@as(u32, 0), map.lookup(0, 4).?.segment);
            try std.testing.expectEqual(@as(u32, 1), map.lookup(1, 2).?.segment);
        }
    };
    try Scenario.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "file sorted merge preserves deletion survivors sparse postings columns and ordinals" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const sort = [_]SegmentIndexSortField{.{ .field = "rank", .desc = false }};
    const Harness = struct {
        fn build(alloc: Allocator, parity: usize) ![]u8 {
            var writer = SegmentWriter.init(alloc);
            defer writer.deinit();
            var text = inverted.InvertedIndexBuilder.init(alloc, inverted.productionIndexConfig());
            defer text.deinit();
            var column = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 1024);
            defer column.deinit();
            var ordinals: [3200]u32 = undefined;
            for (0..3200) |doc| {
                const rank = doc * 2 + parity;
                var name: [32]u8 = undefined;
                const id = try std.fmt.bufPrint(&name, "doc-{d:0>5}", .{rank});
                try writer.addStoredDoc(id, "{}");
                try column.add(@intCast(doc), .{ .u64_val = rank });
                ordinals[doc] = @intCast(rank + 1);
                if (doc % 3 != 0) try text.addDocument(@intCast(doc), &.{
                    .{ .term = "common", .freq = 2, .norm = 3, .positions = &.{ 0, 2 } },
                    .{ .term = id, .freq = 1, .norm = 3, .positions = &.{1} },
                });
            }
            const bytes = try text.build();
            defer alloc.free(bytes);
            const values = try column.build();
            defer alloc.free(values);
            try writer.addSection(try writer.addField("body"), .inverted_text, bytes);
            try writer.addSection(try writer.addField("rank"), .typed_doc_values, values);
            try writer.addDocOrdinals(&ordinals);
            try writer.addIndexSortMetadata(&.{.{ .field = "rank", .desc = false }});
            return writer.build();
        }
    };
    const first = try Harness.build(a, 0);
    defer a.free(first);
    const second = try Harness.build(a, 1);
    defer a.free(second);
    var left = try SegmentReader.init(a, first);
    defer left.deinit();
    var right = try SegmentReader.init(a, second);
    defer right.deinit();
    var deleted = roaring.RoaringBitmap.init(a);
    defer deleted.deinit();
    for ([_]u32{ 0, 1, 1023 }) |doc| try deleted.add(doc);
    const inputs = [_]MergeInput{ .{ .reader = &left, .deleted = deleted }, .{ .reader = &right } };
    const expected = try mergeSegmentInputsWithOptions(a, &inputs, .{ .index_sort = &sort });
    defer a.free(expected);
    var output = MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 4 * 1024 * 1024 };
    var provenance = try MergeSourceMap.init(a, &.{ 3200, 3200 });
    defer provenance.deinit();
    try writeMergedSegmentToSinkWithOptions(budget.allocator(), &sink, &inputs, .{ .index_sort = &sort, .source_map = &provenance, .scratch = .{ .io = std.testing.io, .directory = directory, .in_memory_plan_bytes = 0 } });
    try provenance.finishOutput(6397);
    try std.testing.expectEqualSlices(u8, expected, output.out.items);
    try std.testing.expect((try provenance.lookupRead(0, 0)) == null);
    try std.testing.expectEqual(@as(u32, 0), (try provenance.lookupRead(1, 0)).?.doc);
    var reader = try SegmentReader.init(a, output.out.items);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), (try reader.docOrdinal(0)).?);
    std.debug.print("FILE_SORTED_MERGE documents=6397 scratch_peak={d} output_bytes={d}\n", .{ budget.peak, output.out.items.len });
}

test "file sorted merge releases files maps and scratch on allocation failures" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    try writer.addStoredDoc("a", "{}");
    try writer.addStoredDoc("b", "{}");
    try writer.addIndexSortMetadata(&.{.{ .field = "_id", .desc = false }});
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    const Harness = struct {
        fn run(backing: Allocator, input: *const SegmentReader, path: []const u8) !void {
            var stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            var output = MemorySegmentSink.init(std.testing.allocator);
            defer output.deinit();
            var sink = output.sink();
            try writeMergedSegmentToSinkWithOptions(stable.allocator(), &sink, &.{.{ .reader = input }}, .{ .index_sort = &.{.{ .field = "_id", .desc = false }}, .scratch = .{ .io = std.testing.io, .directory = path, .in_memory_plan_bytes = 0 } });
            var result = try SegmentReader.init(std.testing.allocator, output.out.items);
            defer result.deinit();
            try std.testing.expectEqualStrings("b", (try result.storedDoc(1)).?.id);
        }
    };
    var small = try buildSortedMergePlanWithScratch(a, &.{.{ .reader = &reader }}, &.{.{ .field = "_id", .desc = false }}, .{ .io = std.testing.io, .directory = "/unused-small-merge-scratch" });
    defer small.deinit(a);
    try std.testing.expect(small.file == null);
    try Harness.run(a, &reader, directory);
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{ &reader, directory });
}

test "external sorted coordinates stay bounded across shuffled document counts" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var peaks: [2]usize = undefined;
    // Both sizes fill the fixed run buffers and four mapping-cache slots, so
    // the comparison measures growth after cache warmup.
    for ([_]usize{ 25000, 50000 }, &peaks) |count, *peak| {
        var writer = SegmentWriter.init(a);
        defer writer.deinit();
        for (0..count) |doc| {
            var name: [32]u8 = undefined;
            try writer.addStoredDoc(try std.fmt.bufPrint(&name, "doc-{d:0>6}", .{(doc * 7919) % count}), "{}");
        }
        const fields = [_]SegmentIndexSortField{.{ .field = "_id", .desc = false }};
        try writer.addIndexSortMetadata(&fields);
        const bytes = try writer.build();
        defer a.free(bytes);
        var reader = try SegmentReader.init(a, bytes);
        defer reader.deinit();
        var deleted = roaring.RoaringBitmap.init(a);
        defer deleted.deinit();
        for ([_]u32{ 0, 17, 1024 }) |doc| try deleted.add(doc);
        const inputs = [_]MergeInput{.{ .reader = &reader, .deleted = deleted }};
        var reference = try buildUnsortedMergePlanAlloc(a, &inputs, &fields);
        defer reference.deinit(a);
        var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 1024 * 1024 };
        {
            var plan = try buildSortedMergePlanWithScratch(budget.allocator(), &inputs, &fields, .{ .io = std.testing.io, .directory = directory });
            defer plan.deinit(budget.allocator());
            try std.testing.expect(plan.file != null);
            try std.testing.expect(!plan.file.?.monotonic);
            try std.testing.expect(plan.file.?.mapping.write_calls < 128);
            std.debug.print("SEQUENTIAL_MAPPING documents={d} writes={d} map_bytes={d}\n", .{ count, plan.file.?.mapping.write_calls, count * 4 });
            try std.testing.expectEqualDeep(reference.first_keys, plan.first_keys);
            try std.testing.expectEqualDeep(reference.last_keys, plan.last_keys);
            var iterator = plan.ordered().iterator();
            for (reference.records, 0..) |record, output| {
                try std.testing.expectEqualDeep(record, (try iterator.next()).?);
                var mapped: [4]u8 = undefined;
                try plan.file.?.maps[0].ids.readInto(@as(u64, record.ref.doc_id) * 4, &mapped);
                try std.testing.expectEqual(@as(u32, @intCast(output)), std.mem.readInt(u32, &mapped, .little));
            }
            try std.testing.expect(try iterator.next() == null);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        peak.* = budget.peak;
    }
    std.debug.print("EXTERNAL_SORT documents=25000,50000 peaks={d},{d}\n", .{ peaks[0], peaks[1] });
    try std.testing.expect(peaks[1] <= peaks[0] + 16 * 1024);
}

test "sorted coordinate crossover includes fan in cache costs" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    for (0..2000) |doc| {
        var name: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&name, "doc-{d:0>6}", .{doc}), "{}");
    }
    const fields = [_]SegmentIndexSortField{.{ .field = "_id", .desc = false }};
    try writer.addIndexSortMetadata(&fields);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    const inputs: [8]MergeInput = @splat(.{ .reader = &reader });
    var plan = try buildSortedMergePlanWithScratch(a, &inputs, &fields, .{ .io = std.testing.io, .directory = "/unused-wide-small-merge" });
    defer plan.deinit(a);
    // 320 KiB of coordinates exceeds the old fixed cutoff, but remains
    // cheaper than eight mapping caches plus their run buffers.
    try std.testing.expect(plan.file == null);
    try std.testing.expectEqual(@as(usize, 16000), plan.records.len);
}

test "append merges share dense deletion rank navigation across text and typed fields" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var text = inverted.InvertedIndexBuilder.init(a, inverted.productionIndexConfig());
    defer text.deinit();
    var column = typed_dv.TypedDocValuesWriter.init(a, .u64_val, 1024);
    defer column.deinit();
    for (0..10000) |doc| {
        var name: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&name, "doc-{d:0>6}", .{doc}), "{}");
        try text.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 1, .norm = 1 }});
        try column.add(@intCast(doc), .{ .u64_val = doc });
    }
    const postings = try text.build();
    defer a.free(postings);
    const values = try column.build();
    defer a.free(values);
    for ([_][]const u8{ "first", "second", "third" }) |field| {
        const index = try writer.addField(field);
        try writer.addSection(index, .inverted_text, postings);
        try writer.addSection(index, .typed_doc_values, values);
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var source = try SegmentReader.init(a, bytes);
    defer source.deinit();
    var deleted = roaring.RoaringBitmap.init(a);
    defer deleted.deinit();
    for (1..9001) |doc| try deleted.add(@intCast(doc));
    const merged = try mergeSegmentInputs(a, &.{.{ .reader = &source, .deleted = deleted }});
    defer a.free(merged);
    var result = try SegmentReader.init(a, merged);
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1000), result.doc_count);
    for ([_][]const u8{ "first", "second", "third" }) |field| {
        var index = (try result.invertedIndexScoped(a, field)).?;
        defer index.deinit();
        try std.testing.expectEqual(@as(u32, 1000), (try index.lookup("common")).?.docFreq());
        var values_reader = (try result.typedDocValuesScoped(a, field)).?;
        defer values_reader.deinit();
        try std.testing.expectEqual(@as(?u64, 0), try values_reader.getU64(0));
        try std.testing.expectEqual(@as(?u64, 9001), try values_reader.getU64(1));
        try std.testing.expectEqual(@as(?u64, 9999), try values_reader.getU64(999));
    }
}

test "external sorted merge preserves artifact bytes and cleans allocation failures" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var text = inverted.InvertedIndexBuilder.init(a, inverted.productionIndexConfig());
    defer text.deinit();
    var column = typed_dv.TypedDocValuesWriter.init(a, .u64_val, 4);
    defer column.deinit();
    var ordinals: [12]u32 = undefined;
    for (0..12) |doc| {
        const rank = (doc * 5) % 12;
        var name: [32]u8 = undefined;
        const id = try std.fmt.bufPrint(&name, "doc-{d:0>3}", .{rank});
        try writer.addStoredDoc(id, "{}");
        try column.add(@intCast(doc), .{ .u64_val = rank });
        try text.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 2, .norm = 3, .positions = &.{ 0, 2 } }});
        ordinals[doc] = @intCast(rank + 1);
    }
    const values = try column.build();
    defer a.free(values);
    const postings = try text.build();
    defer a.free(postings);
    try writer.addSection(try writer.addField("rank"), .typed_doc_values, values);
    try writer.addSection(try writer.addField("body"), .inverted_text, postings);
    try writer.addDocOrdinals(&ordinals);
    try writer.addIndexSortMetadata(&.{.{ .field = "rank", .desc = false }});
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    var deleted = roaring.RoaringBitmap.init(a);
    defer deleted.deinit();
    try deleted.add(1);
    const input = MergeInput{ .reader = &reader, .deleted = deleted };
    const expected = try mergeSegmentInputsWithOptions(a, &.{input}, .{ .index_sort = &.{.{ .field = "rank", .desc = false }} });
    defer a.free(expected);
    const Harness = struct {
        fn run(backing: Allocator, source: MergeInput, path: []const u8, reference: []const u8) !void {
            var stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            var output = MemorySegmentSink.init(std.testing.allocator);
            defer output.deinit();
            var sink = output.sink();
            var provenance = try MergeSourceMap.init(stable.allocator(), &.{source.reader.doc_count});
            defer provenance.deinit();
            try writeMergedSegmentToSinkWithOptions(stable.allocator(), &sink, &.{source}, .{ .index_sort = &.{.{ .field = "rank", .desc = false }}, .source_map = &provenance, .scratch = .{ .io = std.testing.io, .directory = path, .in_memory_plan_bytes = 0, .external_sort_chunk_documents = 3 } });
            try provenance.finishOutput(11);
            try std.testing.expect(provenance.file != null);
            try std.testing.expectEqualSlices(u8, reference, output.out.items);
            try std.testing.expect((try provenance.lookupRead(0, 1)) == null);
            try std.testing.expectEqual(@as(u32, 0), (try provenance.lookupRead(0, 0)).?.doc);
        }
    };
    try Harness.run(a, input, directory, expected);
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{ input, directory, expected });
}

test "external sorted position heavy terms fit bounded native scratch" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var text = inverted.InvertedIndexBuilder.init(a, inverted.productionIndexConfig());
    defer text.deinit();
    var positions: [32]u32 = undefined;
    for (&positions, 0..) |*position, i| position.* = @intCast(i);
    const count = 50000;
    for (0..count) |doc| {
        var name: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&name, "doc-{d:0>6}", .{(doc * 7919) % count}), "{}");
        try text.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 32, .norm = 32, .positions = &positions }});
    }
    const postings = try text.build();
    defer a.free(postings);
    try writer.addSection(try writer.addField("body"), .inverted_text, postings);
    const fields = [_]SegmentIndexSortField{.{ .field = "_id", .desc = false }};
    try writer.addIndexSortMetadata(&fields);
    const bytes = try writer.build();
    defer a.free(bytes);
    var source = try SegmentReader.init(a, bytes);
    defer source.deinit();
    var output = MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 2 * 1024 * 1024 };
    try writeMergedSegmentToSinkWithOptions(budget.allocator(), &sink, &.{.{ .reader = &source }}, .{ .index_sort = &fields, .scratch = .{ .io = std.testing.io, .directory = directory } });
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    var merged = try SegmentReader.init(a, output.out.items);
    defer merged.deinit();
    var index = (try merged.invertedIndexScoped(a, "body")).?;
    defer index.deinit();
    var result = (try index.lookup("common")).?;
    try std.testing.expectEqual(@as(u32, count), result.docFreq());
    var iterator = try result.iterator(a);
    defer iterator.deinit();
    var seen: usize = 0;
    while (try iterator.next()) |hit| {
        try std.testing.expectEqual(@as(u32, @intCast(seen)), hit.doc_id);
        try std.testing.expectEqual(@as(u32, 32), hit.freq);
        try std.testing.expectEqualSlices(u32, &positions, hit.positions);
        seen += 1;
    }
    try std.testing.expectEqual(@as(usize, count), seen);
    std.debug.print("BOUNDED_UNORDERED_POSTINGS documents=50000 positions=1600000 peak={d} old_position_arrays=12800000\n", .{budget.peak});
}

test "private spill sorts preserve variable payloads and unwind allocation failures" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    const Harness = struct {
        fn run(backing: Allocator, directory: []const u8) !void {
            var stable = @import("storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = backing };
            const Spill = @import("spill_sort.zig");
            var sorter = try Spill.Sorter.init(stable.allocator(), .{ .io = std.testing.io, .directory = directory, .chunk_records = 3, .chunk_bytes = 96 });
            defer sorter.deinit();
            for (0..20) |i| {
                const key = (i * 7) % 20;
                var payload: [64]u8 = @splat(@intCast(key));
                try sorter.add(key, payload[0 .. key + 1]);
            }
            const range = (try sorter.finish()).?;
            var cursor = Spill.Cursor.init(stable.allocator(), sorter.run, range);
            defer cursor.deinit();
            for (0..20) |key| {
                const record = (try cursor.next()).?;
                try std.testing.expectEqual(@as(u64, key), record.key);
                try std.testing.expectEqual(key + 1, record.payload.len);
                for (record.payload) |byte| try std.testing.expectEqual(@as(u8, @intCast(key)), byte);
            }
            try std.testing.expect(try cursor.next() == null);
        }
    };
    try Harness.run(a, path);
    try std.testing.checkAllAllocationFailures(a, Harness.run, .{path});
}

test "external key sidecars preserve mixed exact numerics bytes booleans and descending ties" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const values = [_]typed_dv.NumericValue{ .{ .u64_val = std.math.maxInt(u64) }, .{ .i64_val = std.math.minInt(i64) }, .{ .f64_val = 0.5 }, .{ .u64_val = 1 }, .{ .i64_val = 1 }, .{ .f64_val = 1.0 }, .{ .i64_val = 0 } };
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var number = typed_dv.TypedDocValuesWriter.init(a, .numeric_val, 3);
    defer number.deinit();
    var label = typed_dv.TypedDocValuesWriter.init(a, .bytes_val, 3);
    defer label.deinit();
    var flag = typed_dv.TypedDocValuesWriter.init(a, .bool_val, 3);
    defer flag.deinit();
    for (values, 0..) |value, doc| {
        var name: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&name, "doc-{d}", .{doc}), "{}");
        try number.add(@intCast(doc), .{ .numeric_val = value });
        try label.add(@intCast(doc), .{ .bytes_val = if (doc % 2 == 0) "same" else "other" });
        try flag.add(@intCast(doc), .{ .bool_val = doc % 3 == 0 });
    }
    const columns = [_]struct { name: []const u8, column: *typed_dv.TypedDocValuesWriter }{ .{ .name = "number", .column = &number }, .{ .name = "label", .column = &label }, .{ .name = "flag", .column = &flag } };
    for (columns) |column| {
        const bytes = try column.column.build();
        defer a.free(bytes);
        try writer.addSection(try writer.addField(column.name), .typed_doc_values, bytes);
    }
    const sort = [_]SegmentIndexSortField{ .{ .field = "number", .desc = true }, .{ .field = "label", .desc = false }, .{ .field = "flag", .desc = true }, .{ .field = "_id", .desc = true } };
    try writer.addIndexSortMetadata(&sort);
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    const inputs = [_]MergeInput{.{ .reader = &reader }};
    const expected = try mergeSegmentInputsWithOptions(a, &inputs, .{ .index_sort = &sort });
    defer a.free(expected);
    const actual = try mergeSegmentInputsWithOptions(a, &inputs, .{ .index_sort = &sort, .scratch = .{ .io = std.testing.io, .directory = directory, .external_sort_chunk_documents = 2 } });
    defer a.free(actual);
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "medium frequency position heavy term spills within native budget" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var text = inverted.InvertedIndexBuilder.init(a, inverted.productionIndexConfig());
    defer text.deinit();
    var positions: [2048]u32 = undefined;
    for (&positions, 0..) |*position, i| position.* = @intCast(i);
    for (0..1000) |doc| {
        var name: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&name, "doc-{d:0>6}", .{(doc * 7919) % 1000}), "{}");
        try text.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 2048, .norm = 2048, .positions = &positions }});
    }
    const postings = try text.build();
    defer a.free(postings);
    try writer.addSection(try writer.addField("body"), .inverted_text, postings);
    const fields = [_]SegmentIndexSortField{.{ .field = "_id", .desc = false }};
    try writer.addIndexSortMetadata(&fields);
    const bytes = try writer.build();
    defer a.free(bytes);
    var source = try SegmentReader.init(a, bytes);
    defer source.deinit();
    var second = try SegmentReader.init(a, bytes);
    defer second.deinit();
    for ([_]bool{ false, true }) |sorted| {
        const inputs = [_]MergeInput{ .{ .reader = &source }, .{ .reader = &second } };
        const expected_docs: u32 = if (sorted) 1000 else 2000;
        var output = MemorySegmentSink.init(a);
        defer output.deinit();
        var sink = output.sink();
        var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 10 * 1024 * 1024 };
        try writeMergedSegmentToSinkWithOptions(budget.allocator(), &sink, inputs[0..if (sorted) 1 else 2], .{ .index_sort = if (sorted) &fields else &.{}, .scratch = .{ .io = std.testing.io, .directory = directory, .in_memory_plan_bytes = 0 } });
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        var merged = try SegmentReader.init(a, output.out.items);
        defer merged.deinit();
        const section = (try merged.getSection("body", .inverted_text)) orelse return error.TestExpectedEqual;
        const index = try inverted.InvertedIndexReader.init(a, section);
        const result = index.lookup("common") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(expected_docs, result.docFreq());
        try std.testing.expectEqual(@as(u64, expected_docs) * 2048, index.total_field_len);
        var iterator = try result.iterator(a);
        defer iterator.deinit();
        var count: u32 = 0;
        while (try iterator.next()) |hit| {
            try std.testing.expectEqual(count, hit.doc_id);
            try std.testing.expectEqualSlices(u32, &positions, hit.positions);
            count += 1;
        }
        try std.testing.expectEqual(expected_docs, count);
        std.debug.print("LITE_MEDIUM_TERM documents={d} positions_per_doc=2048 result=success budget=10485760 sorted={} peak={d}\n", .{ expected_docs, sorted, budget.peak });
    }
}

test "wide interleaved stored cache retains all fitting blocks" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var body: [2048]u8 = @splat('x');
    @memcpy(body[0..9], "{\"body\":\"");
    @memcpy(body[2046..], "\"}");
    for (0..32) |doc| {
        var id: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d}", .{doc}), &body);
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var readers: [8]SegmentReader = undefined;
    var initialized: usize = 0;
    defer for (readers[0..initialized]) |*reader| reader.deinit();
    for (&readers) |*reader| {
        reader.* = try SegmentReader.init(a, bytes);
        initialized += 1;
    }
    var cache = SegmentReader.StoredDocBlockCache.init(a, 1024 * 1024);
    defer cache.deinit();
    for (0..32) |doc| for (&readers) |*reader| {
        const stored = (try cache.get(reader, @intCast(doc))).?;
        try std.testing.expectEqualSlices(u8, &body, stored.data);
    };
    try std.testing.expectEqual(@as(usize, 8), cache.decode_count);
    try std.testing.expect(cache.live_bytes <= cache.byte_budget);
    const Scenario = struct {
        fn run(allocator: Allocator, inputs: []SegmentReader) !void {
            var scope = SegmentReader.StoredDocBlockCache.init(allocator, 1024 * 1024);
            defer scope.deinit();
            for (inputs) |*reader| _ = try scope.get(reader, 0);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{readers[0..]});
    std.debug.print("LITE_WIDE_CACHE sources=8 documents_per_source=32 decodes={d} distinct_blocks=8 cached_bytes={d} budget=1048576\n", .{ cache.decode_count, cache.live_bytes });
}

test "large positional output chunks remain bounded and preserve fixed chunk framing" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var text = inverted.InvertedIndexBuilder.init(a, inverted.productionIndexConfig());
    defer text.deinit();
    var positions: [16384]u32 = undefined;
    for (&positions, 0..) |*position, i| position.* = @intCast(i);
    for (0..256) |doc| {
        var name: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&name, "doc-{d:0>6}", .{(doc * 7919) % 256}), "{}");
        try text.addDocument(@intCast(doc), &.{.{ .term = "common", .freq = 16384, .norm = 16384, .positions = &positions }});
    }
    const postings = try text.build();
    defer a.free(postings);
    try writer.addSection(try writer.addField("body"), .inverted_text, postings);
    const fields = [_]SegmentIndexSortField{.{ .field = "_id", .desc = false }};
    try writer.addIndexSortMetadata(&fields);
    const bytes = try writer.build();
    defer a.free(bytes);
    var source = try SegmentReader.init(a, bytes);
    defer source.deinit();
    var second = try SegmentReader.init(a, bytes);
    defer second.deinit();
    for ([_]bool{ false, true }) |sorted| {
        const inputs = [_]MergeInput{ .{ .reader = &source }, .{ .reader = &second } };
        const expected_docs: u32 = if (sorted) 256 else 512;
        var output = MemorySegmentSink.init(a);
        defer output.deinit();
        var sink = output.sink();
        var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 10 * 1024 * 1024 };
        try writeMergedSegmentToSinkWithOptions(budget.allocator(), &sink, inputs[0..if (sorted) 1 else 2], .{ .index_sort = if (sorted) &fields else &.{}, .scratch = .{ .io = std.testing.io, .directory = directory, .in_memory_plan_bytes = 0 } });
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        var merged = try SegmentReader.init(a, output.out.items);
        defer merged.deinit();
        const section = (try merged.getSection("body", .inverted_text)) orelse return error.TestExpectedEqual;
        const index = try inverted.InvertedIndexReader.init(a, section);
        const result = index.lookup("common") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(expected_docs, result.docFreq());
        try std.testing.expectEqual(@as(u64, expected_docs) * 16384, index.total_field_len);
        var iterator = try result.iterator(a);
        defer iterator.deinit();
        var count: u32 = 0;
        while (try iterator.next()) |hit| {
            try std.testing.expectEqual(count, hit.doc_id);
            try std.testing.expectEqualSlices(u32, &positions, hit.positions);
            count += 1;
        }
        try std.testing.expectEqual(expected_docs, count);
        std.debug.print("LITE_LARGE_POSITION_CHUNK documents={d} positions_per_doc=16384 result=success budget=10485760 sorted={} peak={d}\n", .{ expected_docs, sorted, budget.peak });
    }
}

test "multiway spill sorting reduces rewrite bytes with bounded view heads" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const Spill = @import("spill_sort.zig");
    var rewritten: [2]usize = undefined;
    for ([_]usize{ 2, 4 }, 0..) |fan_in, trial| {
        var budget = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 2 * 1024 * 1024 };
        var sorter = try Spill.Sorter.init(budget.allocator(), .{ .io = std.testing.io, .directory = directory, .fan_in = fan_in });
        const start = platform_time.monotonicNs();
        for (0..65536) |i| {
            const key = (i * 7919) % 65536;
            var payload: [64]u8 = @splat('p');
            std.mem.writeInt(u64, payload[0..8], key, .little);
            try sorter.add(key, &payload);
        }
        const range = (try sorter.finish()).?;
        rewritten[trial] = sorter.merged_bytes;
        var cursor = Spill.Cursor.init(budget.allocator(), sorter.run, range);
        var expected: u64 = 0;
        while (try cursor.nextView()) |record| {
            try std.testing.expectEqual(expected, record.key);
            var payload: [8]u8 = undefined;
            try record.payload.readInto(0, &payload);
            try std.testing.expectEqual(expected, std.mem.readInt(u64, &payload, .little));
            expected += 1;
        }
        try std.testing.expectEqual(@as(u64, 65536), expected);
        cursor.deinit();
        std.debug.print("LITE_MULTIWAY records=65536 fan_in={d} merged_bytes={d} peak={d} elapsed_ns={d}\n", .{ fan_in, sorter.merged_bytes, budget.peak, platform_time.monotonicNs() - start });
        sorter.deinit();
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expect(rewritten[1] <= rewritten[0] / 2);
}

test "sorted intact stored blocks copy compressed bytes and reduce scratch" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var body: [4092]u8 = @splat('b');
    @memcpy(body[0..9], "{\"body\":\"");
    @memcpy(body[4090..], "\"}");
    for (0..2048) |doc| {
        var id: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d:0>6}", .{doc}), &body);
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    const records = try a.alloc(SortedMergeDoc, 2048);
    defer a.free(records);
    for (records, 0..) |*record, i| record.* = .{ .ref = .{ .input_idx = 0, .doc_id = @intCast(i) } };
    var baseline = MemorySegmentSink.init(a);
    defer baseline.deinit();
    var baseline_sink = baseline.sink();
    var old = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 8 * 1024 * 1024 };
    const old_start = platform_time.monotonicNs();
    _ = try writeMergedStoredFieldsInOrderWithReuse(false, old.allocator(), &baseline_sink, &.{.{ .reader = &reader }}, records, 2048);
    const old_ns = platform_time.monotonicNs() - old_start;
    var copied = MemorySegmentSink.init(a);
    defer copied.deinit();
    var copied_sink = copied.sink();
    var new = @import("storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 2 * 1024 * 1024 };
    const new_start = platform_time.monotonicNs();
    _ = try writeMergedStoredFieldsInOrder(new.allocator(), &copied_sink, &.{.{ .reader = &reader }}, records, 2048);
    const new_ns = platform_time.monotonicNs() - new_start;
    try std.testing.expectEqualSlices(u8, baseline.out.items, copied.out.items);
    try std.testing.expectEqual(@as(usize, 0), old.live);
    try std.testing.expectEqual(@as(usize, 0), new.live);
    try std.testing.expect(new.peak < old.peak / 4);
    std.debug.print("LITE_SORTED_STORED_COPY docs=2048 body_bytes=4092 old_peak={d} new_peak={d} old_allocations={d} new_allocations={d} old_ns={d} new_ns={d}\n", .{ old.peak, new.peak, old.alloc_calls, new.alloc_calls, old_ns, new_ns });
}

test "native compressed block copying reads payload once and rejects corruption" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    var body: [4092]u8 = undefined;
    var random: u32 = 1729;
    for (&body) |*byte_| {
        random = random *% 1664525 +% 1013904223;
        byte_.* = @truncate(random >> 24);
    }
    for (0..16) |doc| {
        var id: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d}", .{doc}), &body);
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var memory = try SegmentReader.init(a, bytes);
    defer memory.deinit();
    const first = (try memory.storedLocationMetadata(0)).?;
    const last = (try memory.storedLocationMetadata(15)).?;
    const State = struct {
        data: []u8,
        start: u64,
        end: u64,
        payload_bytes: usize = 0,
        fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset >= self.start and offset + output.len <= self.end) self.payload_bytes += output.len;
            @memcpy(output, self.data[@intCast(offset)..][0..output.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var state = State{ .data = bytes, .start = first.block_start, .end = last.block_end };
    var native = try SegmentReader.initSource(a, .{ .ranges = .{ .ptr = &state, .length = bytes.len, .read_into = State.read, .close = State.close } });
    defer native.deinit();
    var records: [16]SortedMergeDoc = undefined;
    for (&records, 0..) |*record, doc| {
        record.* = .{ .ref = .{ .input_idx = 0, .doc_id = @intCast(doc) } };
        _ = try native.storedLocationMetadata(@intCast(doc));
    }
    _ = try native.storedBlockChecksum(first.block_idx);
    // Count logical source reads after page authentication/cache effects.
    // The original range remains the cache's immutable backing source.
    const Counted = struct {
        source: SegmentSource,
        start: u64,
        end: u64,
        payload_bytes: usize = 0,
        corrupt: bool = false,
        fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.source.readInto(offset, output);
            if (offset >= self.start and offset + output.len <= self.end) {
                self.payload_bytes += output.len;
                if (self.corrupt and output.len > 0) output[0] ^= 1;
            }
        }
        fn close(_: *anyopaque) void {}
    };
    var counted = Counted{ .source = native.source(), .start = first.block_start, .end = last.block_end };
    native.native.?.range.source = .{ .ranges = .{ .ptr = &counted, .length = bytes.len, .read_into = Counted.read, .close = Counted.close } };
    var expected = MemorySegmentSink.init(a);
    defer expected.deinit();
    var expected_sink = expected.sink();
    _ = try writeMergedStoredFieldsInOrder(a, &expected_sink, &.{.{ .reader = &memory }}, &records, 16);
    state.payload_bytes = 0;
    var output = MemorySegmentSink.init(a);
    defer output.deinit();
    var sink = output.sink();
    _ = try writeMergedStoredFieldsInOrder(a, &sink, &.{.{ .reader = &native }}, &records, 16);
    try std.testing.expectEqualSlices(u8, expected.out.items, output.out.items);
    try std.testing.expectEqual(@as(usize, @intCast(counted.end - counted.start)), counted.payload_bytes);
    std.debug.print("LITE_NATIVE_BLOCK_COPY payload_bytes={d} logical_read_bytes={d}\n", .{ counted.end - counted.start, counted.payload_bytes });
    counted.corrupt = true;
    var corrupt = MemorySegmentSink.init(a);
    defer corrupt.deinit();
    var corrupt_sink = corrupt.sink();
    try std.testing.expectError(error.CrcMismatch, writeMergedStoredFieldsInOrder(a, &corrupt_sink, &.{.{ .reader = &native }}, &records, 16));
}

test "partial stored prefixes resume compressed copying in append and sorted merges" {
    const a = std.testing.allocator;
    var prefix_writer = SegmentWriter.init(a);
    defer prefix_writer.deinit();
    try prefix_writer.addStoredDoc("deleted", "{}");
    try prefix_writer.addStoredDoc("prefix", "{}");
    const prefix_bytes = try prefix_writer.build();
    defer a.free(prefix_bytes);
    var prefix = try SegmentReader.init(a, prefix_bytes);
    defer prefix.deinit();
    var deleted = roaring.RoaringBitmap.init(a);
    defer deleted.deinit();
    try deleted.add(0);
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    const body: [4092]u8 = @splat('b');
    for (0..2048) |doc| {
        var id: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d}", .{doc}), &body);
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    const inputs = [_]MergeInput{ .{ .reader = &prefix, .deleted = deleted }, .{ .reader = &reader } };
    const records = try a.alloc(SortedMergeDoc, 2049);
    defer a.free(records);
    records[0] = .{ .ref = .{ .input_idx = 0, .doc_id = 1 } };
    for (records[1..], 0..) |*record, doc| record.* = .{ .ref = .{ .input_idx = 1, .doc_id = @intCast(doc) } };
    var baseline = MemorySegmentSink.init(a);
    defer baseline.deinit();
    var baseline_sink = baseline.sink();
    const Budget = @import("storage/lite/test_allocator.zig").BudgetAllocator;
    var old = Budget{ .backing = a, .limit = 8 * 1024 * 1024 };
    _ = try writeMergedStoredFieldsInOrderWithReuse(false, old.allocator(), &baseline_sink, &inputs, records, 2049);
    for ([_]bool{ false, true }) |sorted| {
        var output = MemorySegmentSink.init(a);
        defer output.deinit();
        var sink = output.sink();
        var budget = Budget{ .backing = a, .limit = 512 * 1024 };
        const start = platform_time.monotonicNs();
        const metadata_length = if (sorted)
            try writeMergedStoredFieldsInOrder(budget.allocator(), &sink, &inputs, records, 2049)
        else
            try writeMergedStoredFields(budget.allocator(), &sink, &inputs, 2049);
        const elapsed = platform_time.monotonicNs() - start;
        var validations: [17]std.atomic.Value(u8) = @splat(.init(integrity_unverified));
        const merged = SegmentReader{ .stored_block_validations = &validations, .alloc = a, .data = output.out.items, .stored_offset = 0, .stored_length = output.out.items.len, .stored_metadata_length = metadata_length, .index_offset = 0, .doc_count = 2049, .num_fields = 0, .fields = &.{} };
        try std.testing.expectEqual(@as(u32, 17), std.mem.readInt(u32, output.out.items[5..9], .little));
        var cache = SegmentReader.StoredDocBlockCache.init(a, 1024 * 1024);
        defer cache.deinit();
        try std.testing.expectEqualStrings("prefix", (try cache.get(&merged, 0)).?.id);
        for (1..2049) |doc| {
            const actual = (try cache.get(&merged, @intCast(doc))).?;
            var id: [32]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&id, "doc-{d}", .{doc - 1}), actual.id);
            try std.testing.expectEqualSlices(u8, &body, actual.data);
            if ((doc - 1) % 128 == 0) {
                const source = (try reader.storedLocationMetadata(@intCast(doc - 1))).?;
                const dest = (try merged.storedLocationMetadata(@intCast(doc))).?;
                try std.testing.expectEqualSlices(u8, bytes[source.block_start..source.block_end], output.out.items[dest.block_start..dest.block_end]);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expect(budget.peak < old.peak / 4);
        std.debug.print("LITE_PREFIX_COPY sorted={any} docs=2049 old_peak={d} new_peak={d} elapsed_ns={d}\n", .{ sorted, old.peak, budget.peak, elapsed });
    }
}

test "stored block eligibility is local to deletions and preserves live order" {
    const a = std.testing.allocator;
    var writer = SegmentWriter.init(a);
    defer writer.deinit();
    const body: [4092]u8 = @splat('b');
    for (0..1024) |doc| {
        var id: [32]u8 = undefined;
        try writer.addStoredDoc(try std.fmt.bufPrint(&id, "doc-{d}", .{doc}), &body);
    }
    const bytes = try writer.build();
    defer a.free(bytes);
    var reader = try SegmentReader.init(a, bytes);
    defer reader.deinit();
    for ([_]u32{ 0, 129, 1023 }) |deleted_doc| {
        var deleted = roaring.RoaringBitmap.init(a);
        defer deleted.deinit();
        try deleted.add(deleted_doc);
        const input = MergeInput{ .reader = &reader, .deleted = deleted };
        for (0..8) |block| {
            const expected: ?u32 = if (deleted_doc / 128 == block) null else 128;
            try std.testing.expectEqual(expected, try copyableStoredBlockDocs(input, @intCast(block * 128)));
        }
        var records: [1023]SortedMergeDoc = undefined;
        var position: usize = 0;
        for (0..1024) |doc| {
            if (doc == deleted_doc) continue;
            records[position] = .{ .ref = .{ .input_idx = 0, .doc_id = @intCast(doc) } };
            position += 1;
        }
        for ([_]bool{ false, true }) |sorted| {
            var output = MemorySegmentSink.init(a);
            defer output.deinit();
            var sink = output.sink();
            const metadata_length = if (sorted)
                try writeMergedStoredFieldsInOrder(a, &sink, &.{input}, &records, 1023)
            else
                try writeMergedStoredFields(a, &sink, &.{input}, 1023);
            var validations: [8]std.atomic.Value(u8) = @splat(.init(integrity_unverified));
            const merged = SegmentReader{ .stored_block_validations = &validations, .alloc = a, .data = output.out.items, .stored_offset = 0, .stored_length = output.out.items.len, .stored_metadata_length = metadata_length, .index_offset = 0, .doc_count = 1023, .num_fields = 0, .fields = &.{} };
            try std.testing.expectEqual(@as(u32, 8), std.mem.readInt(u32, output.out.items[5..9], .little));
            var cache = SegmentReader.StoredDocBlockCache.init(a, 1024 * 1024);
            defer cache.deinit();
            for (records, 0..) |record, doc| {
                const actual = (try cache.get(&merged, @intCast(doc))).?;
                var id: [32]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&id, "doc-{d}", .{record.ref.doc_id}), actual.id);
                try std.testing.expectEqualSlices(u8, &body, actual.data);
                if (record.ref.doc_id % 128 == 0 and record.ref.doc_id / 128 != deleted_doc / 128) {
                    const source = (try reader.storedLocationMetadata(record.ref.doc_id)).?;
                    const dest = (try merged.storedLocationMetadata(@intCast(doc))).?;
                    try std.testing.expectEqualSlices(u8, bytes[source.block_start..source.block_end], output.out.items[dest.block_start..dest.block_end]);
                }
            }
        }
    }
}
