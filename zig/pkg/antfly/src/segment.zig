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
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);
        try out.ensureTotalCapacity(self.alloc, self.estimatedBuildSize());

        // 1. Write stored fields
        const stored_offset: u64 = out.items.len;
        try self.writeStoredFields(&out);

        // 2. Write field sections
        var section_locs = std.ArrayListUnmanaged(SectionLoc).empty;
        defer section_locs.deinit(self.alloc);

        for (self.fields.items, 0..) |*field, fi| {
            for (field.sections.items, 0..) |*section, si| {
                const offset: u64 = out.items.len;
                try out.appendSlice(self.alloc, section.data);
                try section_locs.append(self.alloc, .{
                    .field_idx = @intCast(fi),
                    .section_idx = @intCast(si),
                    .offset = offset,
                    .length = section.data.len,
                });
            }
        }

        // 3. Write sections index (big-endian)
        const sections_index_offset: u64 = out.items.len;
        try self.writeSectionIndex(&out, section_locs.items);

        // 4. Write footer (big-endian, 40 bytes)
        const footer_start = out.items.len;
        try appendU64BE(self.alloc, &out, @intCast(self.doc_count));
        try appendU64BE(self.alloc, &out, stored_offset);
        try appendU64BE(self.alloc, &out, sections_index_offset);
        try appendU32BE(self.alloc, &out, 0); // chunkMode (0 = default)
        try appendU32BE(self.alloc, &out, segment_version);
        // CRC32 over everything before the CRC + magic (last 8 bytes)
        const crc = std.hash.Crc32.hash(out.items[0 .. footer_start + 32]);
        try appendU32BE(self.alloc, &out, crc);
        try out.appendSlice(self.alloc, &magic);

        return try out.toOwnedSlice(self.alloc);
    }

    fn estimatedBuildSize(self: *const SegmentWriter) usize {
        var total: usize = 1 + 4 + self.stored_fields.items.len * 8;
        for (self.stored_fields.items) |doc| {
            total +|= 2 + doc.id.len + 4;
            if (doc.is_compressed) {
                total +|= doc.data.len;
            } else {
                // Snappy's worst case is slightly larger than input; this is
                // only a capacity hint, so keep it simple and conservative.
                total +|= doc.data.len + doc.data.len / 8 + 64;
            }
        }

        var section_count: usize = 0;
        for (self.fields.items) |field| {
            total +|= 2 + field.name.len + 2;
            section_count +|= field.sections.items.len;
            for (field.sections.items) |section| {
                total +|= section.data.len;
                total +|= 2 + 8 + 8;
            }
        }
        total +|= section_count * @sizeOf(SectionLoc);
        total +|= 40;
        return total;
    }

    fn writeStoredFields(self: *SegmentWriter, out: *std.ArrayListUnmanaged(u8)) !void {
        // Format v2 (with Snappy compression + offset table for random access):
        //   [version: u8 = 2]
        //   [num_docs: u32 LE]
        //   [offset_0: u64 LE]  — offset from start of stored section to doc 0
        //   [offset_1: u64 LE]
        //   ...
        //   [doc_0_data]  — per doc: [id_len: u16 LE][id][compressed_len: u32 LE][snappy_data]
        //   [doc_1_data]
        //   ...
        const num_docs: u32 = @intCast(self.stored_fields.items.len);
        const section_start = out.items.len;

        // Write version
        try out.append(self.alloc, 2);
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

            // Write data, reusing Snappy-compressed payloads when available.
            if (doc.is_compressed) {
                try appendU32LE(self.alloc, out, @intCast(doc.data.len));
                try out.appendSlice(self.alloc, doc.data);
            } else {
                const compress_start = platform_time.monotonicNs();
                const compressed = try snappy.encodeInto(self.alloc, &self.compression_bytes, doc.data);
                self.last_stored_compress_ns +|= platform_time.monotonicNs() - compress_start;
                try appendU32LE(self.alloc, out, @intCast(compressed.len));
                try out.appendSlice(self.alloc, compressed);
            }
        }
    }

    const SectionLoc = struct { field_idx: u16, section_idx: u16, offset: u64, length: u64 };

    fn writeSectionIndex(self: *SegmentWriter, out: *std.ArrayListUnmanaged(u8), locs: []const SectionLoc) !void {
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
        for (self.fields.items, 0..) |*field, fi| {
            try appendU16BE(self.alloc, out, @intCast(field.name.len));
            try out.appendSlice(self.alloc, field.name);
            try appendU16BE(self.alloc, out, @intCast(field.sections.items.len));

            for (field.sections.items, 0..) |*section, si| {
                try appendU16BE(self.alloc, out, @intFromEnum(section.section_type));
                // Find matching location
                var found = false;
                for (locs) |loc| {
                    if (loc.field_idx == @as(u16, @intCast(fi)) and loc.section_idx == @as(u16, @intCast(si))) {
                        try appendU64BE(self.alloc, out, loc.offset);
                        try appendU64BE(self.alloc, out, loc.length);
                        found = true;
                        break;
                    }
                }
                if (!found) return error.MissingSectionLocation;
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

    /// Get an inverted index reader for a field.
    pub fn invertedIndex(self: *const SegmentReader, field_name: []const u8) !?inverted.InvertedIndexReader {
        const section_data = self.getSection(field_name, .inverted_text) orelse return null;
        return try inverted.InvertedIndexReader.init(self.alloc, section_data);
    }

    pub const StoredDocRef = struct { id: []const u8, data: []const u8 };

    /// Read stored document by index. Returns ID and Snappy-compressed data.
    /// Use `storedDocDecompressed` for uncompressed data (requires allocation).
    pub fn storedDoc(self: *const SegmentReader, doc_idx: u32) ?StoredDocRef {
        var pos: usize = @intCast(self.stored_offset);
        const ver = self.data[pos];
        pos += 1;
        const num_docs = std.mem.readInt(u32, self.data[pos..][0..4], .little);
        pos += 4;
        if (doc_idx >= num_docs) return null;

        if (ver >= 2) {
            // v2: offset table for O(1) access
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
        if (ver >= 2) {
            const decompressed = try snappy.decode(self.alloc, raw.data);
            return .{ .id = raw.id, .data = decompressed };
        } else {
            // v1: data is not compressed, dupe for consistent ownership
            return .{ .id = raw.id, .data = try self.alloc.dupe(u8, raw.data) };
        }
    }

    pub fn storedDocsAreCompressed(self: *const SegmentReader) bool {
        return self.data[@intCast(self.stored_offset)] >= 2;
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
    const section_start = sink.len();
    try sink.appendByte(2);
    try sinkAppendU32LE(sink, doc_count);

    const offset_table_start = sink.len();
    try sink.appendNTimes(0, @as(usize, doc_count) * 8);

    var out_doc_id: u32 = 0;
    for (inputs) |input| {
        for (0..input.reader.doc_count) |doc_id_usize| {
            const doc_id: u32 = @intCast(doc_id_usize);
            if (input.isDeleted(doc_id)) continue;
            const doc = input.reader.storedDoc(doc_id) orelse continue;
            const doc_offset: u64 = @intCast(sink.len() - section_start);
            const offset_pos = offset_table_start + @as(usize, out_doc_id) * 8;
            try sink.writeAt(offset_pos, &@as([8]u8, @bitCast(std.mem.nativeToLittle(u64, doc_offset))));

            try sinkAppendU16LE(sink, @intCast(doc.id.len));
            try sink.appendSlice(doc.id);

            if (input.reader.storedDocsAreCompressed()) {
                try sinkAppendU32LE(sink, @intCast(doc.data.len));
                try sink.appendSlice(doc.data);
            } else {
                const compressed = try snappy.encode(alloc, doc.data);
                defer alloc.free(compressed);
                try sinkAppendU32LE(sink, @intCast(compressed.len));
                try sink.appendSlice(compressed);
            }
            out_doc_id += 1;
        }
    }
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
