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

//! Inverted text index section for full-text search.
//!
//! Builds and queries an inverted index using:
//!   - Blocked term dictionary indexed by a Vellum FST (block ceiling → block offset)
//!   - Roaring bitmaps for posting lists (document ID sets)
//!   - Chunked int encoder for term frequencies and field norms
//!   - BM25 scoring
//!
//! Wire-compatible with zapx SectionInvertedTextIndex.

const std = @import("std");
const Allocator = std.mem.Allocator;
const roaring = @import("../encoding/roaring.zig");
const chunked = @import("../encoding/chunked_coder.zig");
const simd_bitpack = @import("../encoding/simd_bitpack.zig");
const vellum = @import("antfly_vellum");
const bloom = @import("bloom");
const platform_time = @import("antfly_platform").time;

// ============================================================================
// Wire format versions
// ============================================================================
//
//   v11: v10 postings + fixed-prefix blocked term dictionary
//   v12: v11 + bit-packed position deltas
//   v13: v12 + variable BlockTree-style prefix-compressed term blocks
//   v14: v13 + compact postings chunk metadata
//   v15: v14 + local prefix runs inside term dictionary blocks
//   v16: v15 + per-section packed doc norms instead of per-posting norms
//   v17: v16 + cumulative-end postings chunk metadata
//   v18: v17 + varint postings headers
//   v19: v18 + sparse block-max records aligned to stored posting chunks
//   v20: v19 + compact tagged term-block dictionary values
//   v21: v20 + bit-packed postings chunk metadata columns
//   v22: v21 + postings-offset deltas in term dictionary blocks
//   v23: v22 + front-coded terms inside term dictionary blocks
//   v24: v23 + explicit postings payload length and bounded chunk checkpoints
//   v25: v24 + one-byte Tantivy-compatible quantized document field norms
//   v26: v25 + three-byte block-max records (u16 max-freq + u8 min-norm ID)
//   v27: v26 + chunk-framed positions without redundant per-document counts
//   v28: v27 + fixed-count postings blocks with block-local doc-delta payloads
//   v29: v28 payloads + separate sparse document-range impact metadata with
//        two-byte conservative impact records
//   v30: v29 metadata + contiguous bit packing within each eight-document
//        position group (no per-document byte padding)
//   v31: v30 + compact single-document posting records that retain frequency
//        and positions without allocating a chunk/header/impact envelope
//   v32: v31 + two-column posting-count chunk metadata; chunk ordinal and
//        document count are derived from the block ordinal and term frequency
//   v33: v32 + inline constant frequency/location controls for posting blocks
//        whose encoded frequency value is identical and fits in seven bits
//   v34: v33 + five-bit conservative impact max-frequency buckets while
//        retaining exact eight-bit minimum field-norm IDs
//   v35: v34 + portable vertical BP128 encoding for full postings blocks;
//        partial blocks retain the horizontal bitstream
//   v36: v35 + block-max records aligned one-for-one with 128-posting payload
//        blocks; removes the separate sparse 1,024-document impact range map
//        and its range-ID sidecar
//   v37: restores the selective 1,024-document bounds and adaptively encodes
//        repeated (frequency ceiling, minimum norm) pairs through a per-term
//        palette; v36 remains a measured, rejected branch-only experiment
//   v38: v35 query structures + a compact postings header that derives block
//        count, compact-metadata length, and skip length; single-block terms
//        also omit redundant impact count and range-ID length fields
//
// Writers emit v38. The production reader accepts v23, the exact format shipped
// by origin/main when this migration began, and v38. Versions v24-v37 are
// development-only experiments on this branch and are deliberately not part of
// the compatibility contract.

const wire_version_legacy: u8 = 23;
const wire_version_checkpoints: u8 = 24;
const wire_version_quantized_norms: u8 = 25;
const wire_version_compact_block_max: u8 = 26;
const wire_version_chunk_framed_positions: u8 = 27;
const wire_version_posting_count_blocks: u8 = 28;
const wire_version_separate_impact_ranges: u8 = 29;
const wire_version_contiguous_position_groups: u8 = 30;
const wire_version_inline_single_doc: u8 = 31;
const wire_version_compact_posting_count_meta: u8 = 32;
const wire_version_constant_block_frequency: u8 = 33;
const wire_version_packed_impact_frequency: u8 = 34;
const wire_version_vertical_bp128: u8 = 35;
const wire_version_payload_aligned_impacts: u8 = 36;
const wire_version_compact_postings_header: u8 = 38;
const wire_version_current: u8 = wire_version_compact_postings_header;
const v7_header_size: usize = 4 + 1 + 4 + 8 + 4 + 4 + 4 + 4; // 33 bytes
const postings_chunk_meta_header_size: usize = 4;
const postings_skip_record_size_v23: usize = 8;
const postings_skip_record_size_v24: usize = 16;
const postings_skip_stride_chunks: usize = 16;
const postings_skip_min_chunks: usize = postings_skip_stride_chunks * 2;
const impact_range_doc_count: u32 = 1024;
const impact_range_min_doc_freq: u32 = 1;
const position_doc_group_size: usize = 8;
const constant_frequency_marker: u8 = 0x80;
const constant_frequency_mask: u8 = 0x7f;
const vertical_bp128_marker: u8 = 0x40;
const packed_width_mask: u8 = 0x3f;
const term_dict_block_min_entries: usize = 25;
const term_dict_block_max_entries: usize = 48;
const term_dict_index_record_size: usize = 8;
const term_dict_magic = "BTD4";
const term_dict_header_size: usize = 20;

fn blockMaxRecordSize(version: u8) usize {
    if (version >= wire_version_separate_impact_ranges) return 2;
    return if (version >= wire_version_compact_block_max) 3 else 6;
}

/// v29 spends one byte on maximum term frequency per impact range. Frequencies
/// below 255 remain exact; the escape value decodes to the largest frequency
/// representable by the legacy scorer. This is deliberately an upper bound,
/// so WAND may prune less aggressively for an unusually repetitive document
/// but can never discard a competitive hit.
fn impactMaxFreqToId(freq: u16) u8 {
    return if (freq < std.math.maxInt(u8)) @intCast(freq) else std.math.maxInt(u8);
}

fn impactMaxFreqFromId(id: u8) u16 {
    return if (id == std.math.maxInt(u8)) std.math.maxInt(u16) else id;
}

const impact_freq_packed_upper_bounds = [32]u16{
    0,  1,  2,  3,  4,  5,  6,  7,  8,   9,   10,  12,  14,  16,  20,  24,
    28, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 192, 224, 254, std.math.maxInt(u16),
};

fn impactMaxFreqToPackedId(freq_id: u8) u5 {
    const freq = impactMaxFreqFromId(freq_id);
    for (impact_freq_packed_upper_bounds, 0..) |upper, packed_id| {
        if (freq <= upper) return @intCast(packed_id);
    }
    unreachable;
}

fn impactMaxFreqFromPackedId(packed_id: u5) u16 {
    return impact_freq_packed_upper_bounds[packed_id];
}

fn usesPackedImpactFrequency(version: u8) bool {
    return version >= wire_version_packed_impact_frequency;
}

const impact_ids_varint_encoding: u8 = 252;
const impact_ids_run_encoding: u8 = 253;

fn appendImpactRecord(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), max_freq: u16, min_norm: u16) !void {
    try out.appendSlice(alloc, &@as([2]u8, .{
        impactMaxFreqToId(max_freq),
        fieldNormToId(min_norm),
    }));
}

fn encodeImpactMetadata(
    alloc: Allocator,
    scratch: *PostingSerializeScratch,
    count: usize,
    version: u8,
) !void {
    scratch.impact_encoded.clearRetainingCapacity();
    if (count == 0) return;

    scratch.doc_deltas.clearRetainingCapacity();
    try scratch.doc_deltas.ensureTotalCapacity(alloc, count);
    for (0..count) |ordinal| {
        scratch.doc_deltas.appendAssumeCapacity(impactMaxFreqToPackedId(scratch.impact_block_max.items[ordinal * 2]));
    }

    _ = version;
    _ = try appendPackedU32(alloc, &scratch.impact_encoded, scratch.doc_deltas.items, 5);
    for (0..count) |ordinal| try scratch.impact_encoded.append(alloc, scratch.impact_block_max.items[ordinal * 2 + 1]);
}

fn encodeImpactChunkIds(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    chunk_ids: []const u32,
    deltas: *std.ArrayListUnmanaged(u32),
) !void {
    if (chunk_ids.len == 0) return;
    deltas.clearRetainingCapacity();
    try deltas.ensureTotalCapacity(alloc, chunk_ids.len);
    var previous: u32 = 0;
    for (chunk_ids, 0..) |chunk_id, idx| {
        deltas.appendAssumeCapacity(if (idx == 0) chunk_id else chunk_id - previous);
        previous = chunk_id;
    }

    const bits = maxBitWidth(deltas.items);
    const packed_len = 1 + packedU32ByteLen(chunk_ids.len, bits);
    var varint_len: usize = 1;
    for (deltas.items) |delta| varint_len +|= varintU32Size(delta);

    var run_count: u32 = 0;
    var run_len: usize = 1;
    var previous_run_end: u32 = 0;
    var run_encoded_len: usize = 1;
    var idx: usize = 1;
    while (idx <= chunk_ids.len) : (idx += 1) {
        if (idx < chunk_ids.len and chunk_ids[idx] == chunk_ids[idx - 1] + 1) {
            run_len += 1;
            continue;
        }
        const run_start = chunk_ids[idx - run_len];
        const start_delta = if (run_count == 0) run_start else run_start - previous_run_end - 1;
        run_encoded_len +|= varintU32Size(start_delta) + varintU32Size(@intCast(run_len));
        previous_run_end = chunk_ids[idx - 1];
        run_count += 1;
        run_len = 1;
    }
    run_encoded_len +|= varintU32Size(run_count);

    if (run_encoded_len < packed_len and run_encoded_len <= varint_len) {
        try out.append(alloc, impact_ids_run_encoding);
        try writeVarintU32(alloc, out, run_count);
        var encoded_runs: u32 = 0;
        var doc_idx: usize = 0;
        previous_run_end = 0;
        while (doc_idx < chunk_ids.len) {
            const run_start_idx = doc_idx;
            doc_idx += 1;
            while (doc_idx < chunk_ids.len and chunk_ids[doc_idx] == chunk_ids[doc_idx - 1] + 1) doc_idx += 1;
            const start = chunk_ids[run_start_idx];
            const start_delta = if (encoded_runs == 0) start else start - previous_run_end - 1;
            try writeVarintU32(alloc, out, start_delta);
            try writeVarintU32(alloc, out, @intCast(doc_idx - run_start_idx));
            previous_run_end = chunk_ids[doc_idx - 1];
            encoded_runs += 1;
        }
        return;
    }
    if (varint_len < packed_len) {
        try out.append(alloc, impact_ids_varint_encoding);
        for (deltas.items) |delta| try writeVarintU32(alloc, out, delta);
        return;
    }
    try out.append(alloc, bits);
    _ = try appendPackedU32(alloc, out, deltas.items, bits);
}

fn findEncodedImpactChunkOrdinal(data: []const u8, count: u32, wanted: u32) ?usize {
    if (data.len == 0 or count == 0) return null;
    if (data[0] <= 32) {
        const bits = data[0];
        if (data.len - 1 != packedU32ByteLen(count, bits)) return null;
        var chunk_id: u32 = 0;
        for (0..count) |ordinal| {
            chunk_id +|= readPackedU32At(data[1..], ordinal, bits) catch return null;
            if (chunk_id == wanted) return ordinal;
            if (chunk_id > wanted) return null;
        }
        return null;
    }

    var cursor: usize = 1;
    if (data[0] == impact_ids_varint_encoding) {
        var chunk_id: u32 = 0;
        for (0..count) |ordinal| {
            chunk_id +|= readVarintU32(data, &cursor) catch return null;
            if (chunk_id == wanted) return ordinal;
            if (chunk_id > wanted) return null;
        }
        return null;
    }
    if (data[0] == impact_ids_run_encoding) {
        const run_count = readVarintU32(data, &cursor) catch return null;
        var previous_end: u32 = 0;
        var ordinal_base: usize = 0;
        for (0..run_count) |run_idx| {
            const start_delta = readVarintU32(data, &cursor) catch return null;
            const run_len = readVarintU32(data, &cursor) catch return null;
            if (run_len == 0) return null;
            const start = if (run_idx == 0) start_delta else previous_end +| 1 +| start_delta;
            const end = start +| (run_len - 1);
            if (wanted >= start and wanted <= end) return ordinal_base + @as(usize, @intCast(wanted - start));
            if (wanted < start) return null;
            ordinal_base +|= run_len;
            previous_end = end;
        }
    }
    return null;
}

fn usesPostingCountBlocks(version: u8) bool {
    return version >= wire_version_posting_count_blocks;
}

fn usesSeparateImpactRanges(version: u8) bool {
    return version >= wire_version_separate_impact_ranges;
}

fn usesGroupedPositions(version: u8) bool {
    return version >= wire_version_separate_impact_ranges;
}

fn usesContiguousPositionGroups(version: u8) bool {
    return version >= wire_version_contiguous_position_groups;
}

fn usesInlineSingleDocPostings(version: u8) bool {
    return version >= wire_version_inline_single_doc;
}

fn usesCompactPostingCountMeta(version: u8) bool {
    return version >= wire_version_compact_posting_count_meta;
}

fn usesConstantBlockFrequency(version: u8) bool {
    return version >= wire_version_constant_block_frequency;
}

fn usesVerticalBp128(version: u8) bool {
    return version >= wire_version_vertical_bp128;
}

fn usesPayloadAlignedImpacts(version: u8) bool {
    return version == wire_version_payload_aligned_impacts;
}

fn usesCompactPostingsHeader(version: u8) bool {
    return version >= wire_version_compact_postings_header;
}

fn metadataChunkSize(version: u8, chunk_size: u32) u32 {
    return if (usesPostingCountBlocks(version)) 0 else chunk_size;
}

/// Skip building a per-segment bloom filter when there are fewer terms than this.
/// FST traversal is already cheap for tiny term sets, and the filter would
/// dominate the section size.
const bloom_min_terms: usize = 64;

/// Write the current section header into `dst[0..v7_header_size]`. Shared
/// between `InvertedIndexBuilder.build` and the merger's
/// `assembleMergedSection` so the wire layout lives in exactly one place.
fn writeCurrentHeader(
    dst: []u8,
    version: u8,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    vellum_len: u32,
    bloom_len: u32,
    norms_len: u32,
) void {
    std.debug.assert(dst.len >= v7_header_size);
    @memcpy(dst[0..4], "INVT");
    dst[4] = version;
    dst[5..9].* = @bitCast(std.mem.nativeToLittle(u32, doc_count));
    dst[9..17].* = @bitCast(std.mem.nativeToLittle(u64, total_field_len));
    dst[17..21].* = @bitCast(std.mem.nativeToLittle(u32, chunk_size));
    dst[21..25].* = @bitCast(std.mem.nativeToLittle(u32, vellum_len));
    dst[25..29].* = @bitCast(std.mem.nativeToLittle(u32, bloom_len));
    dst[29..33].* = @bitCast(std.mem.nativeToLittle(u32, norms_len));
}

// ============================================================================
// Varint helpers (LEB128)
// ============================================================================

fn writeVarintU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var v = value;
    while (v >= 0x80) : (v >>= 7) {
        try out.append(alloc, @as(u8, @truncate(v)) | 0x80);
    }
    try out.append(alloc, @truncate(v));
}

fn writeVarintU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var v = value;
    while (v >= 0x80) : (v >>= 7) {
        try out.append(alloc, @as(u8, @truncate(v)) | 0x80);
    }
    try out.append(alloc, @truncate(v));
}

fn varintU32Size(value: u32) usize {
    if (value < 0x80) return 1;
    if (value < 0x4000) return 2;
    if (value < 0x200000) return 3;
    if (value < 0x10000000) return 4;
    return 5;
}

fn varintU64Size(value: u64) usize {
    if (value < 0x80) return 1;
    if (value < 0x4000) return 2;
    if (value < 0x200000) return 3;
    if (value < 0x10000000) return 4;
    if (value < 0x800000000) return 5;
    if (value < 0x40000000000) return 6;
    if (value < 0x2000000000000) return 7;
    if (value < 0x100000000000000) return 8;
    if (value < 0x8000000000000000) return 9;
    return 10;
}

/// Derive two independent 64-bit bloom-filter hashes for `term` from a
/// single Wyhash pass plus a splitmix64 finalizer. The classical bloom-double-
/// hashing setup needs two uncorrelated u64s; doing two full Wyhash passes
/// (one with seed 0, one with seed 1) doubles the per-lookup hash cost
/// unnecessarily — splitmix64's finalizer applied to h1 produces an h2 that's
/// statistically independent enough for bloom membership without re-walking
/// the input bytes.
///
/// Both write paths (builder + merger) and the read path must use this exact
/// derivation; otherwise the bits set at write time won't be probed at read
/// time and the filter will report false negatives. v6 sections built before
/// this change used two-Wyhash hashes — readers running the new code will
/// not be able to use bloom on those older bitstreams (they'll fall back to
/// a full FST walk via `lookup()`). The branch hasn't been merged or shipped,
/// so no on-disk segments are affected.
fn termBloomHashes(term: []const u8) struct { h1: u64, h2: u64 } {
    const h1 = std.hash.Wyhash.hash(0, term);
    // splitmix64 finalizer (Steele/Lea, "Fast Splittable Pseudorandom Number
    // Generators"). Strong avalanche on every output bit; cheap (3 mults +
    // 3 xorshifts) compared to another full Wyhash pass over `term`.
    var h2 = h1;
    h2 ^= h2 >> 30;
    h2 *%= 0xbf58476d1ce4e5b9;
    h2 ^= h2 >> 27;
    h2 *%= 0x94d049bb133111eb;
    h2 ^= h2 >> 31;
    return .{ .h1 = h1, .h2 = h2 };
}

const TermDictEntry = struct {
    term: []const u8,
    value: u64,
};

pub const InvertedIndexBuildProfile = struct {
    sort_ns: u64 = 0,
    postings_serialize_ns: u64 = 0,
    term_dict_ns: u64 = 0,
    norms_ns: u64 = 0,
    bloom_finish_ns: u64 = 0,
    final_assembly_ns: u64 = 0,
};

fn appendLeU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, value))));
}

fn commonPrefixLen(a: []const u8, b: []const u8) usize {
    const limit = @min(a.len, b.len);
    var i: usize = 0;
    const Word = usize;
    const word_size = @sizeOf(Word);
    while (i + word_size <= limit) : (i += word_size) {
        const lhs = std.mem.readInt(Word, a[i..][0..word_size], .little);
        const rhs = std.mem.readInt(Word, b[i..][0..word_size], .little);
        const diff = lhs ^ rhs;
        if (diff != 0) return i + (@ctz(diff) / 8);
    }
    while (i < limit and a[i] == b[i]) : (i += 1) {}
    return i;
}

fn chooseTermBlockEnd(entries_len: usize, start: usize) usize {
    const remaining = entries_len - start;
    if (remaining <= term_dict_block_max_entries) return entries_len;

    var block_len = term_dict_block_max_entries;
    const tail = remaining - block_len;
    if (tail > 0 and tail < term_dict_block_min_entries) {
        const borrow = term_dict_block_min_entries - tail;
        if (block_len > term_dict_block_min_entries + borrow) {
            block_len -= borrow;
        }
    }
    return start + block_len;
}

fn estimateTermDictBlockCount(entries_len: usize) usize {
    var count: usize = 0;
    var i: usize = 0;
    while (i < entries_len) {
        i = chooseTermBlockEnd(entries_len, i);
        count += 1;
    }
    return count;
}

const TermByteStats = struct {
    total: usize = 0,
    max: usize = 0,
};

fn estimateTermBytes(entries: []const TermDictEntry) TermByteStats {
    var stats = TermByteStats{};
    for (entries) |entry| {
        stats.total +|= entry.term.len;
        stats.max = @max(stats.max, entry.term.len);
    }
    return stats;
}

fn appendTermDictIndexRecord(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    block_offset: u32,
    ceiling_term_offset: u32,
) !void {
    try appendLeU32(alloc, out, block_offset);
    try appendLeU32(alloc, out, ceiling_term_offset);
}

fn encodeTermDictBlockValueDelta(value: u64, last_postings_offset: *u64) u64 {
    if (fstValIs1Hit(value)) {
        const decoded = fstValDecode1Hit(value);
        return (decoded.doc_num << 1) | 1;
    }
    std.debug.assert(value >= last_postings_offset.*);
    const delta = value - last_postings_offset.*;
    last_postings_offset.* = value;
    std.debug.assert(delta <= (std.math.maxInt(u64) >> 1));
    return delta << 1;
}

fn decodeTermDictBlockValueDelta(encoded: u64, last_postings_offset: *u64) u64 {
    if (encoded & 1 != 0) {
        return fstValEncode1Hit(encoded >> 1, 0);
    }
    const delta = encoded >> 1;
    last_postings_offset.* +|= delta;
    return last_postings_offset.*;
}

fn encodeBlockedTermDictionary(alloc: Allocator, entries: []const TermDictEntry) ![]u8 {
    var block_data = std.ArrayListUnmanaged(u8).empty;
    defer block_data.deinit(alloc);
    var index_records = std.ArrayListUnmanaged(u8).empty;
    defer index_records.deinit(alloc);
    var index_terms = std.ArrayListUnmanaged(u8).empty;
    defer index_terms.deinit(alloc);

    const estimated_block_count = estimateTermDictBlockCount(entries.len);
    const term_bytes = estimateTermBytes(entries);
    try block_data.ensureTotalCapacity(alloc, term_bytes.total +| entries.len * 12 +| estimated_block_count * 16);
    try index_records.ensureTotalCapacity(alloc, estimated_block_count * term_dict_index_record_size);
    try index_terms.ensureTotalCapacity(alloc, estimated_block_count * (term_bytes.max +| 5));

    const fst_registry_size: usize = std.math.clamp(entries.len, 64, 65_536);
    var block_fst_builder = try vellum.Builder.init(alloc, .{
        .registry_table_size = fst_registry_size,
    });
    defer block_fst_builder.deinit();

    var i: usize = 0;
    var block_count: u32 = 0;
    while (i < entries.len) {
        const end = chooseTermBlockEnd(entries.len, i);
        const first_term = entries[i].term;
        const ceiling_term = entries[end - 1].term;
        const prefix_len = commonPrefixLen(first_term, ceiling_term);
        const prefix = first_term[0..prefix_len];
        const block_offset: u64 = @intCast(block_data.items.len);
        try block_fst_builder.insert(ceiling_term, block_offset);

        const ceiling_term_offset: u32 = @intCast(index_terms.items.len);
        try writeVarintU32(alloc, &index_terms, @intCast(ceiling_term.len));
        try index_terms.appendSlice(alloc, ceiling_term);
        try appendTermDictIndexRecord(alloc, &index_records, @intCast(block_offset), ceiling_term_offset);

        try writeVarintU32(alloc, &block_data, @intCast(prefix.len));
        try writeVarintU32(alloc, &block_data, @intCast(end - i));
        try block_data.appendSlice(alloc, prefix);

        var last_postings_offset: u64 = 0;
        var previous_suffix: []const u8 = &.{};
        for (entries[i..end]) |entry| {
            const suffix = entry.term[prefix.len..];
            const shared_len = commonPrefixLen(previous_suffix, suffix);
            const leaf = suffix[shared_len..];
            try writeVarintU32(alloc, &block_data, @intCast(shared_len));
            try writeVarintU32(alloc, &block_data, @intCast(leaf.len));
            try block_data.appendSlice(alloc, leaf);
            try writeVarintU64(alloc, &block_data, encodeTermDictBlockValueDelta(entry.value, &last_postings_offset));
            previous_suffix = suffix;
        }

        block_count += 1;
        i = end;
    }

    const block_fst = try block_fst_builder.finish();
    defer alloc.free(block_fst);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, term_dict_magic);
    try appendLeU32(alloc, &out, block_count);
    try appendLeU32(alloc, &out, @intCast(block_data.items.len));
    try appendLeU32(alloc, &out, @intCast(index_records.items.len + index_terms.items.len));
    try appendLeU32(alloc, &out, @intCast(block_fst.len));
    try out.appendSlice(alloc, block_data.items);
    try out.appendSlice(alloc, index_records.items);
    try out.appendSlice(alloc, index_terms.items);
    try out.appendSlice(alloc, block_fst);
    return try out.toOwnedSlice(alloc);
}

/// Merge-time blocked dictionary encoder.
///
/// A segment merge discovers terms in lexical order, but historically retained
/// an allocation for every term until all postings had been written. Large
/// merges therefore held the uncompressed vocabulary, an entry array, the
/// encoded dictionary, and finally a second contiguous dictionary copy at the
/// same time. This encoder keeps at most one 48-term source block and appends
/// the finished dictionary components directly to the segment sink.
const StreamingTermDictionaryBuilder = struct {
    const PendingEntry = struct {
        term_offset: u32,
        term_len: u32,
        value: u64,
    };

    alloc: Allocator,
    block_data: std.ArrayListUnmanaged(u8) = .empty,
    index_records: std.ArrayListUnmanaged(u8) = .empty,
    index_terms: std.ArrayListUnmanaged(u8) = .empty,
    pending_term_bytes: std.ArrayListUnmanaged(u8) = .empty,
    pending_entries: [term_dict_block_max_entries]PendingEntry = undefined,
    pending_count: usize = 0,
    term_count: usize = 0,
    block_count: u32 = 0,
    block_fst_builder: vellum.Builder,
    finalized_blocks: bool = false,

    fn init(alloc: Allocator) !StreamingTermDictionaryBuilder {
        return .{
            .alloc = alloc,
            // Large merges benefit from the maximum bounded registry. This is
            // independent of vocabulary size and avoids resizing the FST
            // builder while keeping its working set predictable.
            .block_fst_builder = try vellum.Builder.init(alloc, .{
                .registry_table_size = 65_536,
            }),
        };
    }

    fn deinit(self: *StreamingTermDictionaryBuilder) void {
        self.block_data.deinit(self.alloc);
        self.index_records.deinit(self.alloc);
        self.index_terms.deinit(self.alloc);
        self.pending_term_bytes.deinit(self.alloc);
        self.block_fst_builder.deinit();
        self.* = undefined;
    }

    fn add(self: *StreamingTermDictionaryBuilder, term: []const u8, value: u64) !void {
        if (self.finalized_blocks) return error.InvalidData;
        if (self.pending_count == term_dict_block_max_entries) try self.flushPendingBlock();
        if (term.len > std.math.maxInt(u32) or self.pending_term_bytes.items.len > std.math.maxInt(u32) - term.len) {
            return error.InvalidData;
        }
        const offset: u32 = @intCast(self.pending_term_bytes.items.len);
        try self.pending_term_bytes.appendSlice(self.alloc, term);
        self.pending_entries[self.pending_count] = .{
            .term_offset = offset,
            .term_len = @intCast(term.len),
            .value = value,
        };
        self.pending_count += 1;
        self.term_count += 1;
    }

    fn pendingTerm(self: *const StreamingTermDictionaryBuilder, entry: PendingEntry) []const u8 {
        return self.pending_term_bytes.items[entry.term_offset..][0..entry.term_len];
    }

    fn flushPendingBlock(self: *StreamingTermDictionaryBuilder) !void {
        if (self.pending_count == 0) return;
        const first_term = self.pendingTerm(self.pending_entries[0]);
        const ceiling_term = self.pendingTerm(self.pending_entries[self.pending_count - 1]);
        const prefix_len = commonPrefixLen(first_term, ceiling_term);
        const prefix = first_term[0..prefix_len];
        if (self.block_data.items.len > std.math.maxInt(u32) or self.index_terms.items.len > std.math.maxInt(u32)) {
            return error.InvalidData;
        }
        const block_offset: u32 = @intCast(self.block_data.items.len);
        try self.block_fst_builder.insert(ceiling_term, block_offset);

        const ceiling_term_offset: u32 = @intCast(self.index_terms.items.len);
        try writeVarintU32(self.alloc, &self.index_terms, @intCast(ceiling_term.len));
        try self.index_terms.appendSlice(self.alloc, ceiling_term);
        try appendTermDictIndexRecord(self.alloc, &self.index_records, block_offset, ceiling_term_offset);

        try writeVarintU32(self.alloc, &self.block_data, @intCast(prefix.len));
        try writeVarintU32(self.alloc, &self.block_data, @intCast(self.pending_count));
        try self.block_data.appendSlice(self.alloc, prefix);

        var last_postings_offset: u64 = 0;
        var previous_suffix: []const u8 = &.{};
        for (self.pending_entries[0..self.pending_count]) |entry| {
            const term = self.pendingTerm(entry);
            const suffix = term[prefix.len..];
            const shared_len = commonPrefixLen(previous_suffix, suffix);
            const leaf = suffix[shared_len..];
            try writeVarintU32(self.alloc, &self.block_data, @intCast(shared_len));
            try writeVarintU32(self.alloc, &self.block_data, @intCast(leaf.len));
            try self.block_data.appendSlice(self.alloc, leaf);
            try writeVarintU64(self.alloc, &self.block_data, encodeTermDictBlockValueDelta(entry.value, &last_postings_offset));
            previous_suffix = suffix;
        }

        self.block_count += 1;
        self.pending_count = 0;
        self.pending_term_bytes.clearRetainingCapacity();
    }

    fn finalizeBlocks(self: *StreamingTermDictionaryBuilder) !void {
        if (self.finalized_blocks) return;
        try self.flushPendingBlock();
        self.finalized_blocks = true;
    }

    /// Replays the compact encoded blocks after the exact merged term count is
    /// known. This replaces the former 16-byte hash retained for every term
    /// with one exact-size bloom bitset and a single reusable term buffer.
    fn encodeBloomAlloc(self: *StreamingTermDictionaryBuilder, config: IndexConfig) ![]u8 {
        try self.finalizeBlocks();
        if (!config.enable_bloom or self.term_count < bloom_min_terms) return try self.alloc.dupe(u8, &.{});

        var builder = try bloom.Builder.init(self.alloc, self.term_count, .{
            .bits_per_key = config.bloom_bits_per_key,
        });
        errdefer builder.deinit();
        var current_suffix = std.ArrayListUnmanaged(u8).empty;
        defer current_suffix.deinit(self.alloc);
        var current_term = std.ArrayListUnmanaged(u8).empty;
        defer current_term.deinit(self.alloc);

        var cursor: usize = 0;
        var blocks_seen: u32 = 0;
        var terms_seen: usize = 0;
        while (cursor < self.block_data.items.len) : (blocks_seen += 1) {
            const prefix_len = readVarintU32(self.block_data.items, &cursor) catch return error.InvalidData;
            const entry_count = readVarintU32(self.block_data.items, &cursor) catch return error.InvalidData;
            if (cursor + prefix_len > self.block_data.items.len) return error.InvalidData;
            const prefix = self.block_data.items[cursor..][0..prefix_len];
            cursor += prefix_len;
            current_suffix.clearRetainingCapacity();

            for (0..entry_count) |_| {
                const shared_len = readVarintU32(self.block_data.items, &cursor) catch return error.InvalidData;
                const leaf_len = readVarintU32(self.block_data.items, &cursor) catch return error.InvalidData;
                if (shared_len > current_suffix.items.len or cursor + leaf_len > self.block_data.items.len) return error.InvalidData;
                current_suffix.shrinkRetainingCapacity(shared_len);
                try current_suffix.appendSlice(self.alloc, self.block_data.items[cursor..][0..leaf_len]);
                cursor += leaf_len;
                _ = readVarintU64(self.block_data.items, &cursor) catch return error.InvalidData;

                current_term.clearRetainingCapacity();
                try current_term.appendSlice(self.alloc, prefix);
                try current_term.appendSlice(self.alloc, current_suffix.items);
                const hashes = termBloomHashes(current_term.items);
                builder.addHashes(hashes.h1, hashes.h2);
                terms_seen += 1;
            }
        }
        if (cursor != self.block_data.items.len or blocks_seen != self.block_count or terms_seen != self.term_count) return error.InvalidData;

        var filter = builder.finish();
        defer filter.deinit(self.alloc);
        return try filter.encodeAlloc(self.alloc);
    }

    fn finishIntoSink(self: *StreamingTermDictionaryBuilder, sink: anytype) !usize {
        try self.finalizeBlocks();
        const block_fst = try self.block_fst_builder.finish();
        defer self.alloc.free(block_fst);
        const block_index_len = self.index_records.items.len +| self.index_terms.items.len;
        const total_len = term_dict_header_size +| self.block_data.items.len +| block_index_len +| block_fst.len;
        if (self.block_data.items.len > std.math.maxInt(u32) or block_index_len > std.math.maxInt(u32) or block_fst.len > std.math.maxInt(u32)) {
            return error.InvalidData;
        }

        try sink.appendSlice(term_dict_magic);
        var header_tail: [16]u8 = undefined;
        std.mem.writeInt(u32, header_tail[0..4], self.block_count, .little);
        std.mem.writeInt(u32, header_tail[4..8], @intCast(self.block_data.items.len), .little);
        std.mem.writeInt(u32, header_tail[8..12], @intCast(block_index_len), .little);
        std.mem.writeInt(u32, header_tail[12..16], @intCast(block_fst.len), .little);
        try sink.appendSlice(&header_tail);
        try sink.appendSlice(self.block_data.items);
        try sink.appendSlice(self.index_records.items);
        try sink.appendSlice(self.index_terms.items);
        try sink.appendSlice(block_fst);
        return total_len;
    }
};

/// Decode a u32 LEB128 varint at `cursor`. Advances `cursor` past the decoded
/// bytes. Returns `error.Truncated` if the buffer ends mid-varint.
fn readVarintU32(data: []const u8, cursor: *usize) !u32 {
    var result: u32 = 0;
    var shift: u5 = 0;
    while (cursor.* < data.len) {
        const b = data[cursor.*];
        cursor.* += 1;
        result |= @as(u32, b & 0x7f) << shift;
        if (b & 0x80 == 0) return result;
        if (shift >= 28) return error.VarintOverflow;
        shift += 7;
    }
    return error.Truncated;
}

fn readVarintU64(data: []const u8, cursor: *usize) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (cursor.* < data.len) {
        const b = data[cursor.*];
        cursor.* += 1;
        result |= @as(u64, b & 0x7f) << shift;
        if (b & 0x80 == 0) return result;
        if (shift >= 63) return error.VarintOverflow;
        shift += 7;
    }
    return error.Truncated;
}

// ============================================================================
// Index builder (write path)
// ============================================================================

/// Builds an inverted text index from documents.
///
/// Usage:
///   var builder = try InvertedIndexBuilder.init(alloc, .{});
///   try builder.addDocument(0, &.{.{ .term = "hello", .freq = 1, .positions = &.{0} }});
///   try builder.addDocument(1, &.{.{ .term = "hello", .freq = 2, .positions = &.{0, 5} }});
///   const section = try builder.build();
///   defer alloc.free(section);
pub const InvertedIndexBuilder = struct {
    alloc: Allocator,
    config: IndexConfig,

    /// term -> PostingAccumulator
    terms: std.StringHashMapUnmanaged(PostingAccumulator),
    /// Page-based arena that owns the bytes backing every term-string key
    /// in `terms`. Replaces per-term `alloc.dupe` churn with bump-pointer
    /// allocation that's freed once at deinit. Pages don't relocate, so the
    /// slice headers stored as map keys remain valid for the builder's life.
    term_arena: std.heap.ArenaAllocator,
    /// Dense doc-id indexed field norms. Lucene stores norms once per field
    /// instead of repeating them in every term posting.
    doc_norms: std.ArrayListUnmanaged(u32),
    doc_count: u32 = 0,
    /// Total tokens across all documents (for avgdl)
    total_field_len: u64 = 0,

    pub fn init(alloc: Allocator, config: IndexConfig) InvertedIndexBuilder {
        return .{
            .alloc = alloc,
            .config = config,
            .terms = .empty,
            .term_arena = std.heap.ArenaAllocator.init(alloc),
            .doc_norms = .empty,
        };
    }

    pub fn deinit(self: *InvertedIndexBuilder) void {
        var it = self.terms.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.alloc);
        }
        self.terms.deinit(self.alloc);
        self.term_arena.deinit();
        self.doc_norms.deinit(self.alloc);
    }

    pub fn estimatedMemoryBytes(self: *const InvertedIndexBuilder) u64 {
        var total: u64 = @as(u64, @intCast(self.terms.capacity())) * @sizeOf(std.StringHashMapUnmanaged(PostingAccumulator).Entry);
        total +|= @as(u64, @intCast(self.doc_norms.capacity)) * @sizeOf(u32);
        var it = self.terms.iterator();
        while (it.next()) |entry| {
            total +|= @intCast(entry.key_ptr.*.len);
            total +|= entry.value_ptr.estimatedMemoryBytes();
        }
        return total;
    }

    fn recordDocNorm(self: *InvertedIndexBuilder, doc_num: u32, norm: u32) !void {
        const needed = @as(usize, doc_num) + 1;
        if (self.doc_norms.items.len < needed) {
            const old_len = self.doc_norms.items.len;
            try self.doc_norms.resize(self.alloc, needed);
            @memset(self.doc_norms.items[old_len..], 0);
        }
        if (self.doc_norms.items[doc_num] == 0 or norm > self.doc_norms.items[doc_num]) {
            self.doc_norms.items[doc_num] = norm;
        }
    }

    /// A single term occurrence in a document.
    pub const TermHit = struct {
        term: []const u8,
        freq: u32,
        norm: u32 = 0,
        positions: []const u32 = &.{},
    };

    /// Add a document's term hits to the index.
    pub fn addDocument(self: *InvertedIndexBuilder, doc_num: u32, hits: []const TermHit) !void {
        var field_len: u32 = 0;
        var doc_norm: u32 = 0;
        for (hits) |hit| {
            if (doc_norm == 0 or hit.norm > doc_norm) doc_norm = hit.norm;
            const gop = try self.terms.getOrPut(self.alloc, hit.term);
            if (!gop.found_existing) {
                // Re-key into arena-owned storage; the HashMap copied a borrowed
                // slice from the caller, but the arena copy will outlive the call.
                gop.key_ptr.* = try self.term_arena.allocator().dupe(u8, hit.term);
                gop.value_ptr.* = PostingAccumulator.init();
            }
            try gop.value_ptr.add(self.alloc, doc_num, hit.freq, hit.norm, hit.positions);
            field_len += hit.freq;
        }
        if (doc_norm == 0) doc_norm = field_len;
        try self.recordDocNorm(doc_num, doc_norm);
        self.doc_count += 1;
        self.total_field_len += field_len;
    }

    /// Add a single term hit for a document (used by merger).
    pub fn addDocumentSingle(self: *InvertedIndexBuilder, doc_num: u32, term: []const u8, freq: u32, norm_val: u32) !void {
        const gop = try self.terms.getOrPut(self.alloc, term);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.term_arena.allocator().dupe(u8, term);
            gop.value_ptr.* = PostingAccumulator.init();
        }
        try gop.value_ptr.add(self.alloc, doc_num, freq, norm_val, &.{});
        try self.recordDocNorm(doc_num, norm_val);
        self.total_field_len += freq;
    }

    /// Build the serialized inverted index section.
    /// Caller owns returned bytes.
    ///
    /// Layout (v27, with blocked dictionary, 1-hit optimization, compact
    /// block-max records, Tantivy-compatible one-byte field norms, and
    /// chunk-framed positions):
    ///   [header: 33 bytes]
    ///   [postings_data]
    ///   [vellum FST data]
    ///
    /// Header:
    ///   magic: "INVT" (4 bytes)
    ///   version: u8 = 27
    ///   doc_count: u32 LE
    ///   total_field_len: u64 LE
    ///   chunk_size: u32 LE
    ///   dictionary, bloom, and norm section lengths
    ///
    /// FST values:
    ///   - General: postings offset within postings_data
    ///   - 1-hit: packed docNum + normBits (for single-doc, freq=1 terms)
    ///
    /// Postings per term, v27:
    ///   [doc_freq: varint u32]
    ///   [stored_chunks: varint u32]
    ///   [chunk_meta_len: varint u32]
    ///   [payload_len: varint u32]
    ///   [positions_section_len: varint u32]
    ///   [skip_section_len: varint u32]
    ///   [stored_chunks × 3-byte block-max records]
    ///   [bit-packed chunk metadata columns]
    ///   [packed per-chunk doc-delta/freqHasLocs/norm payloads]
    ///   [positions: varint byte length + bit-packed records per stored chunk]
    ///   [16-byte sparse checkpoints every 16 stored chunks]
    ///
    /// Postings are followed by packed norms, an optional term bloom filter,
    /// and the blocked term dictionary.
    pub fn build(self: *InvertedIndexBuilder) ![]u8 {
        return self.buildAlloc(self.alloc);
    }

    pub fn buildAlloc(self: *InvertedIndexBuilder, output_alloc: Allocator) ![]u8 {
        return self.buildAllocProfile(output_alloc, null);
    }

    pub fn buildAllocProfile(
        self: *InvertedIndexBuilder,
        output_alloc: Allocator,
        profile: ?*InvertedIndexBuildProfile,
    ) ![]u8 {
        const profile_timings = profile != null;
        const scratch_alloc = output_alloc;
        const term_count = self.terms.count();
        if (term_count == 0) return try output_alloc.dupe(u8, &.{});

        // Step 1: Sort terms
        const sort_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        const sorted_terms = try scratch_alloc.alloc([]const u8, term_count);
        defer scratch_alloc.free(sorted_terms);
        {
            var it = self.terms.keyIterator();
            var i: usize = 0;
            while (it.next()) |key| {
                sorted_terms[i] = key.*;
                i += 1;
            }
        }
        std.mem.sort([]const u8, sorted_terms, {}, struct {
            fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);
        if (profile_timings) profile.?.sort_ns +|= platform_time.monotonicNs() - sort_start_ns;

        // Step 2: Serialize postings data directly into the final section and
        // collect dictionary entries (term -> postings offset or 1-hit). The header is
        // backpatched after bloom/FST sizes are known, avoiding a second
        // field-sized postings buffer during segment construction.
        var output = std.ArrayListUnmanaged(u8).empty;
        errdefer output.deinit(output_alloc);
        try output.appendNTimes(output_alloc, 0, v7_header_size);

        var dict_entries = try scratch_alloc.alloc(TermDictEntry, term_count);
        defer scratch_alloc.free(dict_entries);

        // Optional per-segment term bloom filter. `null` when disabled (small
        // term sets) or when the caller opted out via `IndexConfig.enable_bloom`.
        const want_bloom = self.config.enable_bloom and term_count >= bloom_min_terms;
        var bloom_builder: ?bloom.Builder = if (want_bloom)
            try bloom.Builder.init(scratch_alloc, term_count, .{
                .bits_per_key = self.config.bloom_bits_per_key,
            })
        else
            null;
        // Cleared to null below after `finish()`; the conditional makes the
        // errdefer safe whether finish() has run or not.
        errdefer if (bloom_builder) |*b| b.deinit();

        var serialize_scratch = PostingSerializeScratch{};
        defer serialize_scratch.deinit(scratch_alloc);

        const postings_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        for (sorted_terms, 0..) |term, term_idx| {
            const acc = self.terms.getPtr(term).?;
            var dict_value: u64 = 0;

            if (bloom_builder) |*b| {
                // Single Wyhash + splitmix64 derivation; the read path mirrors
                // it via `termBloomHashes` to keep the bit-set pattern the
                // same across writers and readers.
                const h = termBloomHashes(term);
                b.addHashes(h.h1, h.h2);
            }

            // 1-hit optimization: single doc, freq=1, no locs, no positions, docNum fits in 31 bits
            if (acc.doc_ids.items.len == 1 and
                acc.metas.items[0].freq == 1 and
                acc.metas.items[0].position_count == 0 and
                acc.doc_ids.items[0] <= mask_31_bits)
            {
                const doc_num: u64 = acc.doc_ids.items[0];
                dict_value = fstValEncode1Hit(doc_num, 0);
            } else {
                const postings_offset: u64 = @intCast(output.items.len - v7_header_size);
                try acc.serializeV9(output_alloc, &output, &serialize_scratch, self.config);
                dict_value = postings_offset;
            }
            dict_entries[term_idx] = .{
                .term = term,
                .value = dict_value,
            };
        }
        if (profile_timings) profile.?.postings_serialize_ns +|= platform_time.monotonicNs() - postings_start_ns;

        const term_dict_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        const term_dict_data = try encodeBlockedTermDictionary(scratch_alloc, dict_entries);
        defer scratch_alloc.free(term_dict_data);
        if (profile_timings) profile.?.term_dict_ns +|= platform_time.monotonicNs() - term_dict_start_ns;

        const norms_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        const norms_data = try encodeNormTable(scratch_alloc, self.doc_norms.items);
        defer scratch_alloc.free(norms_data);
        if (profile_timings) profile.?.norms_ns +|= platform_time.monotonicNs() - norms_start_ns;

        // Encode bloom (if any) into a single buffer that we'll inline into
        // the section. The on-disk payload is the standard `lib/bloom` magic +
        // version + bit_count + hash_count + bytes envelope.
        var bloom_bytes: []const u8 = &.{};
        defer if (bloom_bytes.len > 0) scratch_alloc.free(@constCast(bloom_bytes));
        const bloom_finish_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        if (bloom_builder) |*b| {
            var filter = b.finish();
            // `finish` consumes the builder (sets it to undefined); null out the
            // option so the errdefer above is a no-op.
            bloom_builder = null;
            defer filter.deinit(scratch_alloc);
            bloom_bytes = try filter.encodeAlloc(scratch_alloc);
        }
        if (profile_timings) profile.?.bloom_finish_ns +|= platform_time.monotonicNs() - bloom_finish_start_ns;

        // Step 3: Finish final section.
        const final_assembly_start_ns = if (profile_timings) platform_time.monotonicNs() else 0;
        try output.ensureUnusedCapacity(output_alloc, norms_data.len +| bloom_bytes.len +| term_dict_data.len);
        if (norms_data.len > 0) {
            try output.appendSlice(output_alloc, norms_data);
        }
        if (bloom_bytes.len > 0) {
            try output.appendSlice(output_alloc, bloom_bytes);
        }
        try output.appendSlice(output_alloc, term_dict_data);

        writeCurrentHeader(
            output.items[0..v7_header_size],
            self.config.wireVersion(),
            self.doc_count,
            self.total_field_len,
            self.config.chunk_size,
            @intCast(term_dict_data.len),
            @intCast(bloom_bytes.len),
            @intCast(norms_data.len),
        );

        const owned = try output.toOwnedSlice(output_alloc);
        if (profile_timings) profile.?.final_assembly_ns +|= platform_time.monotonicNs() - final_assembly_start_ns;
        return owned;
    }
};

/// Per-document posting metadata stored beside `doc_ids`.
const PostingMeta = struct {
    freq: u32,
    norm: u32,
    position_count: u32,
};

const V7ChunkMeta = struct {
    chunk_id: u32,
    max_doc: u32,
    doc_count: u32,
    doc_ctrl_off: u32,
    doc_ctrl_len: u32,
    doc_data_off: u32,
    doc_data_len: u32,
    freq_ctrl_off: u32,
    freq_ctrl_len: u32,
    freq_data_off: u32,
    freq_data_len: u32,
};

/// Minimum heap required by the v23 reader's eagerly decoded metadata arrays,
/// excluding allocator capacity rounding.
pub fn legacyDecodedChunkMetadataMinBytes(block_max_bytes: u64) usize {
    const stored_chunks: usize = @intCast(block_max_bytes / 6);
    return stored_chunks * (@sizeOf(V7ChunkMeta) + 4 * @sizeOf(u32));
}

const PostingSerializeScratch = struct {
    chunks: std.ArrayListUnmanaged(V7ChunkMeta) = .empty,
    doc_deltas: std.ArrayListUnmanaged(u32) = .empty,
    freq_values: std.ArrayListUnmanaged(u32) = .empty,
    block_max: std.ArrayListUnmanaged(u8) = .empty,
    chunk_meta: std.ArrayListUnmanaged(u8) = .empty,
    payload: std.ArrayListUnmanaged(u8) = .empty,
    positions: std.ArrayListUnmanaged(u8) = .empty,
    position_chunk: std.ArrayListUnmanaged(u8) = .empty,
    position_group_deltas: std.ArrayListUnmanaged(u32) = .empty,
    skip: std.ArrayListUnmanaged(u8) = .empty,
    svb_control: std.ArrayListUnmanaged(u8) = .empty,
    svb_data: std.ArrayListUnmanaged(u8) = .empty,
    chunk_id_deltas: std.ArrayListUnmanaged(u32) = .empty,
    max_doc_offsets: std.ArrayListUnmanaged(u32) = .empty,
    chunk_doc_counts: std.ArrayListUnmanaged(u32) = .empty,
    payload_end_deltas: std.ArrayListUnmanaged(u32) = .empty,
    impact_chunk_ids: std.ArrayListUnmanaged(u32) = .empty,
    impact_block_max: std.ArrayListUnmanaged(u8) = .empty,
    impact_ids: std.ArrayListUnmanaged(u8) = .empty,
    impact_encoded: std.ArrayListUnmanaged(u8) = .empty,

    fn reset(self: *PostingSerializeScratch) void {
        self.chunks.clearRetainingCapacity();
        self.doc_deltas.clearRetainingCapacity();
        self.freq_values.clearRetainingCapacity();
        self.block_max.clearRetainingCapacity();
        self.chunk_meta.clearRetainingCapacity();
        self.payload.clearRetainingCapacity();
        self.positions.clearRetainingCapacity();
        self.position_chunk.clearRetainingCapacity();
        self.position_group_deltas.clearRetainingCapacity();
        self.skip.clearRetainingCapacity();
        self.svb_control.clearRetainingCapacity();
        self.svb_data.clearRetainingCapacity();
        self.chunk_id_deltas.clearRetainingCapacity();
        self.max_doc_offsets.clearRetainingCapacity();
        self.chunk_doc_counts.clearRetainingCapacity();
        self.payload_end_deltas.clearRetainingCapacity();
        self.impact_chunk_ids.clearRetainingCapacity();
        self.impact_block_max.clearRetainingCapacity();
        self.impact_ids.clearRetainingCapacity();
        self.impact_encoded.clearRetainingCapacity();
    }

    fn deinit(self: *PostingSerializeScratch, alloc: Allocator) void {
        self.chunks.deinit(alloc);
        self.doc_deltas.deinit(alloc);
        self.freq_values.deinit(alloc);
        self.block_max.deinit(alloc);
        self.chunk_meta.deinit(alloc);
        self.payload.deinit(alloc);
        self.positions.deinit(alloc);
        self.position_chunk.deinit(alloc);
        self.position_group_deltas.deinit(alloc);
        self.skip.deinit(alloc);
        self.svb_control.deinit(alloc);
        self.svb_data.deinit(alloc);
        self.chunk_id_deltas.deinit(alloc);
        self.max_doc_offsets.deinit(alloc);
        self.chunk_doc_counts.deinit(alloc);
        self.payload_end_deltas.deinit(alloc);
        self.impact_chunk_ids.deinit(alloc);
        self.impact_block_max.deinit(alloc);
        self.impact_ids.deinit(alloc);
        self.impact_encoded.deinit(alloc);
    }
};

fn appendPostingSkipData(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), chunks: []const V7ChunkMeta) !void {
    out.clearRetainingCapacity();
    if (chunks.len < postings_skip_min_chunks) return;

    var chunk_index: usize = postings_skip_stride_chunks;
    while (chunk_index < chunks.len) : (chunk_index += postings_skip_stride_chunks) {
        const boundary = chunks[chunk_index - 1];
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, boundary.max_doc))));
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(chunk_index))))));
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, boundary.chunk_id))));
        const payload_end = boundary.doc_ctrl_off + boundary.doc_ctrl_len;
        try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, payload_end))));
    }
}

fn bitWidthU32(value: u32) u8 {
    if (value == 0) return 0;
    return @intCast(32 - @clz(value));
}

fn maxBitWidth(values: []const u32) u8 {
    var bits: u8 = 0;
    for (values) |value| bits = @max(bits, bitWidthU32(value));
    return bits;
}

fn packedU32ByteLen(count: usize, bits: u8) usize {
    if (bits == 0 or count == 0) return 0;
    return (count * @as(usize, bits) + 7) / 8;
}

fn appendPackedU32(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const u32,
    bits: u8,
) !struct { off: u32, len: u32 } {
    const off: u32 = @intCast(out.items.len);
    const len = packedU32ByteLen(values.len, bits);
    if (len == 0) return .{ .off = off, .len = 0 };

    const start = out.items.len;
    try out.appendNTimes(alloc, 0, len);
    var bit_pos: usize = 0;
    for (values) |value| {
        var remaining = bits;
        var shifted = value;
        while (remaining > 0) {
            const byte_index = start + bit_pos / 8;
            const bit_in_byte: u3 = @intCast(bit_pos % 8);
            const avail: u8 = 8 - @as(u8, bit_in_byte);
            const take: u8 = @min(remaining, avail);
            const mask: u32 = if (take == 32) std.math.maxInt(u32) else (@as(u32, 1) << @intCast(take)) - 1;
            out.items[byte_index] |= @as(u8, @truncate(shifted & mask)) << bit_in_byte;
            shifted >>= @intCast(take);
            remaining -= take;
            bit_pos += take;
        }
    }
    return .{ .off = off, .len = @intCast(len) };
}

fn decodePackedU32Into(data: []const u8, values: []u32, bits: u8) !void {
    return decodePackedU32Range(data, 0, values, bits);
}

/// Decode a contiguous range from the LSB-first packed stream. A small bit
/// reservoir turns the former byte-at-a-time inner loop into one mask/shift
/// per value for the common widths while still handling arbitrary contiguous
/// ranges within a packed stream.
fn decodePackedU32Range(data: []const u8, start_index: usize, values: []u32, bits: u8) !void {
    if (bits > 32) return error.InvalidData;
    if (bits == 0) {
        @memset(values, 0);
        return;
    }
    if (values.len == 0) return;
    const start_bit = std.math.mul(usize, start_index, bits) catch return error.InvalidData;
    const value_bits = std.math.mul(usize, values.len, bits) catch return error.InvalidData;
    const end_bit = std.math.add(usize, start_bit, value_bits) catch return error.InvalidData;
    const rounded_end_bit = std.math.add(usize, end_bit, 7) catch return error.InvalidData;
    const needed = rounded_end_bit / 8;
    if (data.len < needed) return error.InvalidData;

    var byte_index = start_bit / 8;
    const initial_skip: u3 = @intCast(start_bit % 8);
    var reservoir: u64 = 0;
    var reservoir_bits: u8 = 0;
    if (initial_skip != 0) {
        reservoir = @as(u64, data[byte_index]) >> initial_skip;
        reservoir_bits = 8 - @as(u8, initial_skip);
        byte_index += 1;
    }
    const mask: u64 = if (bits == 32) std.math.maxInt(u32) else (@as(u64, 1) << @intCast(bits)) - 1;
    for (values) |*value| {
        while (reservoir_bits < bits) {
            reservoir |= @as(u64, data[byte_index]) << @intCast(reservoir_bits);
            reservoir_bits += 8;
            byte_index += 1;
        }
        value.* = @intCast(reservoir & mask);
        reservoir >>= @intCast(bits);
        reservoir_bits -= bits;
    }
}

fn decodePackedU32IntoStrided(
    data: []const u8,
    out: []u32,
    count: usize,
    bits: u8,
    start: usize,
    stride: usize,
) !void {
    if (bits > 32) return error.InvalidData;
    if (count == 0) return;
    if (start + (count - 1) * stride >= out.len) return error.InvalidData;
    if (bits == 0) {
        var i: usize = 0;
        while (i < count) : (i += 1) out[start + i * stride] = 0;
        return;
    }
    const needed = packedU32ByteLen(count, bits);
    if (data.len < needed) return error.InvalidData;

    var bit_pos: usize = 0;
    var i: usize = 0;
    while (i < count) : (i += 1) {
        var value: u32 = 0;
        var shift: u8 = 0;
        var remaining = bits;
        while (remaining > 0) {
            const byte_index = bit_pos / 8;
            const bit_in_byte: u3 = @intCast(bit_pos % 8);
            const avail: u8 = 8 - @as(u8, bit_in_byte);
            const take: u8 = @min(remaining, avail);
            const mask: u8 = if (take == 8) 0xff else @as(u8, @truncate((@as(u16, 1) << @intCast(take)) - 1));
            const part: u32 = (data[byte_index] >> bit_in_byte) & mask;
            value |= part << @intCast(shift);
            shift += take;
            remaining -= take;
            bit_pos += take;
        }
        out[start + i * stride] = value;
    }
}

const CompactChunkMetaLayout = struct {
    chunk_delta_bits: u8,
    max_doc_offset_bits: u8,
    doc_count_bits: u8,
    payload_delta_bits: u8,
    chunk_delta_off: usize,
    chunk_delta_len: usize,
    max_doc_offset_off: usize,
    max_doc_offset_len: usize,
    doc_count_off: usize,
    doc_count_len: usize,
    payload_delta_off: usize,
    payload_delta_len: usize,
    total_len: usize,
};

fn compactChunkMetaLayout(data: []const u8, count: usize, version: u8) !CompactChunkMetaLayout {
    if (count == 0) {
        return .{
            .chunk_delta_bits = 0,
            .max_doc_offset_bits = 0,
            .doc_count_bits = 0,
            .payload_delta_bits = 0,
            .chunk_delta_off = 0,
            .chunk_delta_len = 0,
            .max_doc_offset_off = 0,
            .max_doc_offset_len = 0,
            .doc_count_off = 0,
            .doc_count_len = 0,
            .payload_delta_off = 0,
            .payload_delta_len = 0,
            .total_len = 0,
        };
    }
    const compact_posting_count = usesCompactPostingCountMeta(version);
    const header_size: usize = if (compact_posting_count) 2 else postings_chunk_meta_header_size;
    if (data.len < header_size) return error.InvalidData;
    const chunk_delta_bits: u8 = if (compact_posting_count) 0 else data[0];
    const max_doc_offset_bits = data[if (compact_posting_count) 0 else 1];
    const doc_count_bits: u8 = if (compact_posting_count) 0 else data[2];
    const payload_delta_bits = data[if (compact_posting_count) 1 else 3];
    if (chunk_delta_bits > 32 or max_doc_offset_bits > 32 or doc_count_bits > 32 or payload_delta_bits > 32) return error.InvalidData;

    var cursor: usize = header_size;
    const chunk_delta_len = packedU32ByteLen(count, chunk_delta_bits);
    const chunk_delta_off = cursor;
    cursor += chunk_delta_len;
    const max_doc_offset_len = packedU32ByteLen(count, max_doc_offset_bits);
    const max_doc_offset_off = cursor;
    cursor += max_doc_offset_len;
    const doc_count_len = packedU32ByteLen(count, doc_count_bits);
    const doc_count_off = cursor;
    cursor += doc_count_len;
    const payload_delta_len = packedU32ByteLen(count, payload_delta_bits);
    const payload_delta_off = cursor;
    cursor += payload_delta_len;
    if (data.len < cursor) return error.InvalidData;

    return .{
        .chunk_delta_bits = chunk_delta_bits,
        .max_doc_offset_bits = max_doc_offset_bits,
        .doc_count_bits = doc_count_bits,
        .payload_delta_bits = payload_delta_bits,
        .chunk_delta_off = chunk_delta_off,
        .chunk_delta_len = chunk_delta_len,
        .max_doc_offset_off = max_doc_offset_off,
        .max_doc_offset_len = max_doc_offset_len,
        .doc_count_off = doc_count_off,
        .doc_count_len = doc_count_len,
        .payload_delta_off = payload_delta_off,
        .payload_delta_len = payload_delta_len,
        .total_len = cursor,
    };
}

fn readPackedU32At(data: []const u8, index: usize, bits: u8) !u32 {
    if (bits > 32) return error.InvalidData;
    if (bits == 0) return 0;
    var bit_pos: usize = index * @as(usize, bits);
    const needed = (bit_pos + bits + 7) / 8;
    if (data.len < needed) return error.InvalidData;

    var value: u32 = 0;
    var shift: u8 = 0;
    var remaining = bits;
    while (remaining > 0) {
        const byte_index = bit_pos / 8;
        const bit_in_byte: u3 = @intCast(bit_pos % 8);
        const avail: u8 = 8 - @as(u8, bit_in_byte);
        const take: u8 = @min(remaining, avail);
        const mask: u8 = if (take == 8) 0xff else @as(u8, @truncate((@as(u16, 1) << @intCast(take)) - 1));
        const part: u32 = (data[byte_index] >> bit_in_byte) & mask;
        value |= part << @intCast(shift);
        shift += take;
        remaining -= take;
        bit_pos += take;
    }
    return value;
}

fn appendCompactChunkMeta(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    chunks: []const V7ChunkMeta,
    chunk_size: u32,
    version: u8,
    scratch: *PostingSerializeScratch,
) !void {
    if (chunks.len == 0) return;

    const compact_posting_count = usesCompactPostingCountMeta(version);
    if (!compact_posting_count) try scratch.chunk_id_deltas.ensureTotalCapacity(alloc, chunks.len);
    try scratch.max_doc_offsets.ensureTotalCapacity(alloc, chunks.len);
    if (!compact_posting_count) try scratch.chunk_doc_counts.ensureTotalCapacity(alloc, chunks.len);
    try scratch.payload_end_deltas.ensureTotalCapacity(alloc, chunks.len);

    var prev_chunk_id: u32 = 0;
    var prev_payload_end: u32 = 0;
    for (chunks, 0..) |chunk, i| {
        const chunk_id_delta = if (i == 0) chunk.chunk_id else chunk.chunk_id - prev_chunk_id;
        const chunk_base = if (chunk_size == 0) 0 else chunk.chunk_id * chunk_size;
        const max_doc_offset = chunk.max_doc - chunk_base;
        const payload_end = chunk.doc_ctrl_off + chunk.doc_ctrl_len;
        const payload_delta = payload_end - prev_payload_end;

        if (!compact_posting_count) scratch.chunk_id_deltas.appendAssumeCapacity(chunk_id_delta);
        scratch.max_doc_offsets.appendAssumeCapacity(max_doc_offset);
        if (!compact_posting_count) scratch.chunk_doc_counts.appendAssumeCapacity(chunk.doc_count);
        scratch.payload_end_deltas.appendAssumeCapacity(payload_delta);

        prev_chunk_id = chunk.chunk_id;
        prev_payload_end = payload_end;
    }

    const max_doc_offset_bits = maxBitWidth(scratch.max_doc_offsets.items);
    const payload_delta_bits = maxBitWidth(scratch.payload_end_deltas.items);
    if (compact_posting_count) {
        try out.appendSlice(alloc, &.{ max_doc_offset_bits, payload_delta_bits });
        _ = try appendPackedU32(alloc, out, scratch.max_doc_offsets.items, max_doc_offset_bits);
        _ = try appendPackedU32(alloc, out, scratch.payload_end_deltas.items, payload_delta_bits);
        return;
    }

    const chunk_delta_bits = maxBitWidth(scratch.chunk_id_deltas.items);
    const doc_count_bits = maxBitWidth(scratch.chunk_doc_counts.items);
    try out.appendSlice(alloc, &.{ chunk_delta_bits, max_doc_offset_bits, doc_count_bits, payload_delta_bits });
    _ = try appendPackedU32(alloc, out, scratch.chunk_id_deltas.items, chunk_delta_bits);
    _ = try appendPackedU32(alloc, out, scratch.max_doc_offsets.items, max_doc_offset_bits);
    _ = try appendPackedU32(alloc, out, scratch.chunk_doc_counts.items, doc_count_bits);
    _ = try appendPackedU32(alloc, out, scratch.payload_end_deltas.items, payload_delta_bits);
}

fn readCompactChunkMetaAt(data: []const u8, count: usize, version: u8, chunk_size: u32, doc_freq: u32, index: usize) !V7ChunkMeta {
    return readCompactChunkMetaAtCheckpoint(data, count, version, chunk_size, doc_freq, index, 0, 0, 0);
}

fn readCompactChunkMetaAtCheckpoint(
    data: []const u8,
    count: usize,
    version: u8,
    chunk_size: u32,
    doc_freq: u32,
    index: usize,
    start_index: usize,
    previous_chunk_id: u32,
    previous_payload_end: u32,
) !V7ChunkMeta {
    if (index >= count) return error.InvalidData;
    if (start_index > index) return error.InvalidData;
    const layout = try compactChunkMetaLayout(data, count, version);
    const chunk_delta_data = data[layout.chunk_delta_off..][0..layout.chunk_delta_len];
    const max_doc_offset_data = data[layout.max_doc_offset_off..][0..layout.max_doc_offset_len];
    const doc_count_data = data[layout.doc_count_off..][0..layout.doc_count_len];
    const payload_delta_data = data[layout.payload_delta_off..][0..layout.payload_delta_len];

    const compact_posting_count = usesCompactPostingCountMeta(version);
    var chunk_id = if (compact_posting_count) @as(u32, @intCast(index)) else previous_chunk_id;
    var payload_end = previous_payload_end;
    var prev_payload_end = previous_payload_end;
    var i = start_index;
    while (i <= index) : (i += 1) {
        if (!compact_posting_count) chunk_id +%= try readPackedU32At(chunk_delta_data, i, layout.chunk_delta_bits);
        const payload_delta = try readPackedU32At(payload_delta_data, i, layout.payload_delta_bits);
        prev_payload_end = payload_end;
        payload_end +%= payload_delta;
    }

    const max_doc_offset = try readPackedU32At(max_doc_offset_data, index, layout.max_doc_offset_bits);
    const doc_count = if (compact_posting_count)
        if (index + 1 < count or doc_freq == 0)
            chunk_size
        else
            doc_freq - @as(u32, @intCast(index)) * chunk_size
    else
        try readPackedU32At(doc_count_data, index, layout.doc_count_bits);
    const max_doc = if (usesPostingCountBlocks(version)) max_doc_offset else chunk_id * chunk_size + max_doc_offset;
    return .{
        .chunk_id = chunk_id,
        .max_doc = max_doc,
        .doc_count = doc_count,
        .doc_ctrl_off = prev_payload_end,
        .doc_ctrl_len = payload_end - prev_payload_end,
        .doc_data_off = 0,
        .doc_data_len = 0,
        .freq_ctrl_off = 0,
        .freq_ctrl_len = 0,
        .freq_data_off = 0,
        .freq_data_len = 0,
    };
}

fn encodeNormTable(alloc: Allocator, norms: []const u32) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try appendLeU32(alloc, &out, @intCast(norms.len));
    // 0xff is outside the legacy packed-bit-width range (0...32). Each byte
    // is the same downward-quantized fieldnorm ID used by Tantivy 0.25.
    try out.append(alloc, 0xff);
    try out.ensureUnusedCapacity(alloc, norms.len);
    for (norms) |norm| out.appendAssumeCapacity(fieldNormToId(norm));
    return try out.toOwnedSlice(alloc);
}

fn decodeNormValue(norms_data: []const u8, doc_id: u32) u32 {
    if (norms_data.len < 5) return 0;
    const count = std.mem.readInt(u32, norms_data[0..4], .little);
    if (doc_id >= count) return 0;
    const bits = norms_data[4];
    if (bits == 0xff) {
        if (norms_data.len < 5 + @as(usize, count)) return 0;
        return fieldNormFromId(norms_data[5 + @as(usize, doc_id)]);
    }
    if (bits > 32) return 0;
    const packed_bytes = norms_data[5..];
    const needed = packedU32ByteLen(@intCast(count), bits);
    if (packed_bytes.len < needed) return 0;

    if (bits == 0) return 0;
    var bit_pos: usize = @as(usize, doc_id) * @as(usize, bits);
    var value: u32 = 0;
    var shift: u8 = 0;
    var remaining = bits;
    while (remaining > 0) {
        const byte_index = bit_pos / 8;
        const bit_in_byte: u3 = @intCast(bit_pos % 8);
        const avail: u8 = 8 - @as(u8, bit_in_byte);
        const take: u8 = @min(remaining, avail);
        const mask: u8 = if (take == 8) 0xff else @as(u8, @truncate((@as(u16, 1) << @intCast(take)) - 1));
        const part: u32 = (packed_bytes[byte_index] >> bit_in_byte) & mask;
        value |= part << @intCast(shift);
        shift += take;
        remaining -= take;
        bit_pos += take;
    }
    return value;
}

/// Tantivy's fieldnorm table is a compact small-float sequence. Values 0...40
/// are exact; subsequent IDs form eight-value groups whose step doubles.
fn fieldNormFromId(id: u8) u32 {
    if (id <= 40) return id;
    const relative: u32 = @as(u32, id) - 41;
    const group: u5 = @intCast(relative / 8);
    const offset = relative % 8;
    return ((@as(u32, 18) + 2 * offset) << group) + 24;
}

fn fieldNormToId(field_norm: u32) u8 {
    if (field_norm <= 40) return @intCast(field_norm);
    if (field_norm >= fieldNormFromId(255)) return 255;
    var lo: u16 = 40;
    var hi: u16 = 256;
    while (lo + 1 < hi) {
        const mid = lo + (hi - lo) / 2;
        if (fieldNormFromId(@intCast(mid)) <= field_norm) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    return @intCast(lo);
}

const PackedPostingSlice = struct { off: u32, len: u32 };

fn appendPackedPostingChunk(
    alloc: Allocator,
    payload: *std.ArrayListUnmanaged(u8),
    doc_values: []const u32,
    freq_values: []const u32,
) !PackedPostingSlice {
    std.debug.assert(doc_values.len == freq_values.len);

    const off: u32 = @intCast(payload.items.len);
    const doc_bits = maxBitWidth(doc_values);
    const freq_bits = maxBitWidth(freq_values);
    try payload.appendSlice(alloc, &.{ doc_bits, freq_bits });
    _ = try appendPackedU32(alloc, payload, doc_values, doc_bits);
    _ = try appendPackedU32(alloc, payload, freq_values, freq_bits);
    return .{ .off = off, .len = @intCast(payload.items.len - off) };
}

/// v28 postings block payload. The first document is an absolute varint so a
/// large segment-local document ID cannot widen every delta in the block.
/// Remaining document deltas and all freq/locations values are bit-packed
/// independently using block-local widths.
fn appendPackedPostingBlock(
    alloc: Allocator,
    payload: *std.ArrayListUnmanaged(u8),
    doc_values: []const u32,
    freq_values: []const u32,
    version: u8,
) !PackedPostingSlice {
    std.debug.assert(doc_values.len == freq_values.len and doc_values.len > 0);

    const off: u32 = @intCast(payload.items.len);
    try writeVarintU32(alloc, payload, doc_values[0]);
    const doc_bits = maxBitWidth(doc_values[1..]);
    var constant_frequency: ?u8 = null;
    if (usesConstantBlockFrequency(version) and freq_values[0] <= constant_frequency_mask) {
        const candidate: u8 = @intCast(freq_values[0]);
        var all_equal = true;
        for (freq_values[1..]) |value| {
            if (value != candidate) {
                all_equal = false;
                break;
            }
        }
        if (all_equal) constant_frequency = candidate;
    }
    const freq_bits = if (constant_frequency != null) 0 else maxBitWidth(freq_values);
    const vertical_block = usesVerticalBp128(version) and doc_values.len == simd_bitpack.block_values;
    const doc_control = doc_bits | if (vertical_block) vertical_bp128_marker else 0;
    const freq_control = if (constant_frequency) |value|
        constant_frequency_marker | value
    else
        freq_bits | if (vertical_block) vertical_bp128_marker else 0;
    try payload.appendSlice(alloc, &.{ doc_control, freq_control });

    if (vertical_block) {
        var doc_deltas: [simd_bitpack.block_values]u32 = @splat(0);
        @memcpy(doc_deltas[1..], doc_values[1..]);
        var encoded: [32 * 16]u8 = undefined;
        const doc_len = try simd_bitpack.encodeBlock(&encoded, &doc_deltas, doc_bits);
        try payload.appendSlice(alloc, encoded[0..doc_len]);
        if (constant_frequency == null) {
            const frequencies: *const [simd_bitpack.block_values]u32 = freq_values[0..simd_bitpack.block_values];
            const freq_len = try simd_bitpack.encodeBlock(&encoded, frequencies, freq_bits);
            try payload.appendSlice(alloc, encoded[0..freq_len]);
        }
    } else {
        _ = try appendPackedU32(alloc, payload, doc_values[1..], doc_bits);
        if (constant_frequency == null) _ = try appendPackedU32(alloc, payload, freq_values, freq_bits);
    }
    return .{ .off = off, .len = @intCast(payload.items.len - off) };
}

fn appendPackedPositionsForDoc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    positions: []const u32,
) !void {
    try writeVarintU32(alloc, out, @intCast(positions.len));
    if (positions.len == 0) return;

    var deltas_buf: [64]u32 = undefined;
    var heap_deltas: ?[]u32 = null;
    defer if (heap_deltas) |values| alloc.free(values);
    const deltas = if (positions.len <= deltas_buf.len)
        deltas_buf[0..positions.len]
    else blk: {
        heap_deltas = try alloc.alloc(u32, positions.len);
        break :blk heap_deltas.?;
    };

    var prev: u32 = 0;
    for (positions, 0..) |p, i| {
        deltas[i] = if (p >= prev) p - prev else 0;
        prev = p;
    }

    const bits = maxBitWidth(deltas);
    try out.append(alloc, bits);
    _ = try appendPackedU32(alloc, out, deltas, bits);
}

/// v27 positions are framed once per stored postings chunk. The posting
/// frequency already supplies the number of positions for a document, so each
/// document needs only its bit width and packed deltas. The outer chunk length
/// lets phrase seeks skip an entire positions chunk without walking every
/// document record.
fn appendChunkFramedPositionsForDoc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    positions: []const u32,
) !void {
    if (positions.len == 0) return;

    var deltas_buf: [64]u32 = undefined;
    var heap_deltas: ?[]u32 = null;
    defer if (heap_deltas) |values| alloc.free(values);
    const deltas = if (positions.len <= deltas_buf.len)
        deltas_buf[0..positions.len]
    else blk: {
        heap_deltas = try alloc.alloc(u32, positions.len);
        break :blk heap_deltas.?;
    };

    var prev: u32 = 0;
    for (positions, 0..) |p, i| {
        deltas[i] = if (p >= prev) p - prev else 0;
        prev = p;
    }

    const bits = maxBitWidth(deltas);
    try out.append(alloc, bits);
    _ = try appendPackedU32(alloc, out, deltas, bits);
}

/// v30 amortizes the bit-width byte across a small group and packs all deltas
/// in that group contiguously. A phrase seek derives the selected document's
/// value offset from the already-decoded frequency column, so it still unpacks
/// only that document while avoiding up to seven padding bits per posting.
fn appendGroupedPositionsForChunk(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    metas: []const PostingMeta,
    positions: []const u32,
    group_deltas: *std.ArrayListUnmanaged(u32),
) !void {
    var positions_offset: usize = 0;
    var doc_start: usize = 0;
    while (doc_start < metas.len) {
        const doc_end = @min(metas.len, doc_start + position_doc_group_size);
        group_deltas.clearRetainingCapacity();

        var group_position_count: usize = 0;
        for (metas[doc_start..doc_end]) |meta| group_position_count +|= meta.position_count;
        try group_deltas.ensureTotalCapacity(alloc, group_position_count);

        for (metas[doc_start..doc_end]) |meta| {
            var previous: u32 = 0;
            const count: usize = @intCast(meta.position_count);
            const doc_positions = positions[positions_offset..][0..count];
            for (doc_positions) |position| {
                try group_deltas.append(alloc, if (position >= previous) position - previous else 0);
                previous = position;
            }
            positions_offset += count;
        }

        const bits = maxBitWidth(group_deltas.items);
        try out.append(alloc, bits);
        _ = try appendPackedU32(alloc, out, group_deltas.items, bits);
        doc_start = doc_end;
    }
    if (positions_offset != positions.len) return error.InvalidData;
}

/// Accumulates postings for a single term during index building.
const PostingAccumulator = struct {
    doc_ids: std.ArrayListUnmanaged(u32) = .empty,
    metas: std.ArrayListUnmanaged(PostingMeta) = .empty,
    /// Flat concatenation of all position lists.
    all_positions: std.ArrayListUnmanaged(u32) = .empty,

    fn init() PostingAccumulator {
        return .{};
    }

    fn deinit(self: *PostingAccumulator, alloc: Allocator) void {
        self.doc_ids.deinit(alloc);
        self.metas.deinit(alloc);
        self.all_positions.deinit(alloc);
    }

    fn estimatedMemoryBytes(self: *const PostingAccumulator) u64 {
        return (@as(u64, @intCast(self.doc_ids.capacity)) * @sizeOf(u32)) +
            (@as(u64, @intCast(self.metas.capacity)) * @sizeOf(PostingMeta)) +
            (@as(u64, @intCast(self.all_positions.capacity)) * @sizeOf(u32));
    }

    fn add(self: *PostingAccumulator, alloc: Allocator, doc_num: u32, freq: u32, norm_val: u32, positions: []const u32) !void {
        try self.doc_ids.append(alloc, doc_num);
        try self.metas.append(alloc, .{
            .freq = freq,
            .norm = norm_val,
            .position_count = @intCast(positions.len),
        });
        try self.all_positions.appendSlice(alloc, positions);
    }

    fn serializeV9(
        self: *const PostingAccumulator,
        alloc: Allocator,
        out: *std.ArrayListUnmanaged(u8),
        scratch: *PostingSerializeScratch,
        config: IndexConfig,
    ) !void {
        scratch.reset();
        const doc_freq: u32 = @intCast(self.doc_ids.items.len);
        if (doc_freq == 0) return error.InvalidData;
        if (config.chunk_size == 0) return error.InvalidData;
        const posting_count_blocks = config.postings_layout == .posting_count_v35;
        const separate_impact_ranges = usesSeparateImpactRanges(config.wireVersion());
        const payload_aligned_impacts = usesPayloadAlignedImpacts(config.wireVersion());

        // v31 gives the overwhelmingly common single-document term a direct
        // representation. A zero doc-frequency is the on-wire discriminator
        // (real posting lists can never have one), followed by the absolute
        // document ID, freq/locations value, and—when present—one bit width
        // plus packed position deltas. This retains exact phrase data while
        // avoiding the eight-field term header, four chunk-metadata columns,
        // payload controls, and redundant impact record.
        if (usesInlineSingleDocPostings(config.wireVersion()) and doc_freq == 1) {
            const meta = self.metas.items[0];
            if (meta.position_count != self.all_positions.items.len) return error.InvalidData;
            const has_locs = meta.position_count > 0;
            if (has_locs and meta.position_count != meta.freq) return error.InvalidData;

            try writeVarintU32(alloc, out, 0);
            try writeVarintU32(alloc, out, self.doc_ids.items[0]);
            try writeVarintU32(alloc, out, @intCast(encodeFreqHasLocs(meta.freq, has_locs)));
            if (has_locs) {
                scratch.position_group_deltas.clearRetainingCapacity();
                try scratch.position_group_deltas.ensureTotalCapacity(alloc, self.all_positions.items.len);
                var previous: u32 = 0;
                for (self.all_positions.items) |position| {
                    try scratch.position_group_deltas.append(alloc, if (position >= previous) position - previous else 0);
                    previous = position;
                }
                const bits = maxBitWidth(scratch.position_group_deltas.items);
                try out.append(alloc, bits);
                _ = try appendPackedU32(alloc, out, scratch.position_group_deltas.items, bits);
            }
            return;
        }

        if (separate_impact_ranges and !payload_aligned_impacts and doc_freq >= impact_range_min_doc_freq) {
            if (doc_freq <= config.chunk_size) {
                // One bounded posting-count payload needs only one global
                // upper bound. Its exact [first_doc, max_doc] interval already
                // lives in payload chunk metadata, so do not duplicate sparse
                // document-range IDs or one record per crossed 1K range.
                var impact_max_freq: u16 = 0;
                var impact_min_norm: u16 = std.math.maxInt(u16);
                for (self.metas.items) |meta| {
                    const freq_u16: u16 = if (meta.freq > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.freq);
                    const norm_u16: u16 = if (meta.norm > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.norm);
                    impact_max_freq = @max(impact_max_freq, freq_u16);
                    impact_min_norm = @min(impact_min_norm, norm_u16);
                }
                try appendImpactRecord(alloc, &scratch.impact_block_max, impact_max_freq, impact_min_norm);
            } else {
                var current_impact_chunk: ?u32 = null;
                var impact_max_freq: u16 = 0;
                var impact_min_norm: u16 = std.math.maxInt(u16);
                for (self.doc_ids.items, self.metas.items) |doc_id, meta| {
                    const chunk_id = doc_id / impact_range_doc_count;
                    if (current_impact_chunk != null and current_impact_chunk.? != chunk_id) {
                        try scratch.impact_chunk_ids.append(alloc, current_impact_chunk.?);
                        try appendImpactRecord(alloc, &scratch.impact_block_max, impact_max_freq, impact_min_norm);
                        impact_max_freq = 0;
                        impact_min_norm = std.math.maxInt(u16);
                    }
                    current_impact_chunk = chunk_id;
                    const freq_u16: u16 = if (meta.freq > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.freq);
                    const norm_u16: u16 = if (meta.norm > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.norm);
                    impact_max_freq = @max(impact_max_freq, freq_u16);
                    impact_min_norm = @min(impact_min_norm, norm_u16);
                }
                if (current_impact_chunk) |chunk_id| {
                    try scratch.impact_chunk_ids.append(alloc, chunk_id);
                    try appendImpactRecord(alloc, &scratch.impact_block_max, impact_max_freq, impact_min_norm);
                }
                try encodeImpactChunkIds(alloc, &scratch.impact_ids, scratch.impact_chunk_ids.items, &scratch.doc_deltas);
            }
        }

        var pos_offset: usize = 0;
        const store_positions = self.all_positions.items.len > 0;
        var doc_start: usize = 0;
        while (doc_start < self.doc_ids.items.len) {
            const chunk_id: u32 = if (posting_count_blocks)
                @intCast(scratch.chunks.items.len)
            else
                self.doc_ids.items[doc_start] / config.chunk_size;
            var doc_end = doc_start + 1;
            if (posting_count_blocks) {
                doc_end = @min(self.doc_ids.items.len, doc_start + @as(usize, config.chunk_size));
            } else {
                while (doc_end < self.doc_ids.items.len and self.doc_ids.items[doc_end] / config.chunk_size == chunk_id) : (doc_end += 1) {}
            }

            var chunk_max_freq: u16 = 0;
            var chunk_min_norm: u16 = std.math.maxInt(u16);
            scratch.doc_deltas.clearRetainingCapacity();
            scratch.freq_values.clearRetainingCapacity();
            scratch.position_chunk.clearRetainingCapacity();
            try scratch.doc_deltas.ensureTotalCapacity(alloc, doc_end - doc_start);
            try scratch.freq_values.ensureTotalCapacity(alloc, doc_end - doc_start);
            const chunk_positions_start = pos_offset;

            var prev_doc: u32 = 0;
            var i = doc_start;
            while (i < doc_end) : (i += 1) {
                const doc_id = self.doc_ids.items[i];
                const meta = self.metas.items[i];
                scratch.doc_deltas.appendAssumeCapacity(if (i == doc_start)
                    (if (posting_count_blocks) doc_id else doc_id - chunk_id * config.chunk_size)
                else
                    doc_id - prev_doc);
                const has_locs = meta.position_count > 0;
                if (has_locs and meta.position_count != meta.freq) return error.InvalidData;
                const encoded_freq_has_locs: u32 = @intCast(encodeFreqHasLocs(meta.freq, has_locs));
                scratch.freq_values.appendAssumeCapacity(encoded_freq_has_locs);
                prev_doc = doc_id;

                const freq_u16: u16 = if (meta.freq > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.freq);
                const norm_u16: u16 = if (meta.norm > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(meta.norm);
                if (freq_u16 > chunk_max_freq) chunk_max_freq = freq_u16;
                if (norm_u16 < chunk_min_norm) chunk_min_norm = norm_u16;

                const count: usize = meta.position_count;
                if (store_positions and !usesGroupedPositions(config.wireVersion())) {
                    try appendChunkFramedPositionsForDoc(
                        alloc,
                        &scratch.position_chunk,
                        self.all_positions.items[pos_offset..][0..count],
                    );
                }
                pos_offset += count;
            }

            if (store_positions and usesGroupedPositions(config.wireVersion())) {
                try appendGroupedPositionsForChunk(
                    alloc,
                    &scratch.position_chunk,
                    self.metas.items[doc_start..doc_end],
                    self.all_positions.items[chunk_positions_start..pos_offset],
                    &scratch.position_group_deltas,
                );
            }

            if (store_positions) {
                try writeVarintU32(alloc, &scratch.positions, @intCast(scratch.position_chunk.items.len));
                try scratch.positions.appendSlice(alloc, scratch.position_chunk.items);
            }

            const packed_chunk = if (posting_count_blocks)
                try appendPackedPostingBlock(alloc, &scratch.payload, scratch.doc_deltas.items, scratch.freq_values.items, config.wireVersion())
            else
                try appendPackedPostingChunk(alloc, &scratch.payload, scratch.doc_deltas.items, scratch.freq_values.items);
            try scratch.chunks.append(alloc, .{
                .chunk_id = chunk_id,
                .max_doc = self.doc_ids.items[doc_end - 1],
                .doc_count = @intCast(doc_end - doc_start),
                .doc_ctrl_off = packed_chunk.off,
                .doc_ctrl_len = packed_chunk.len,
                .doc_data_off = 0,
                .doc_data_len = 0,
                .freq_ctrl_off = 0,
                .freq_ctrl_len = 0,
                .freq_data_off = 0,
                .freq_data_len = 0,
            });
            if (payload_aligned_impacts) {
                // The scoring bound shares the exact ordinal and document
                // interval of the payload it can prune. This avoids both the
                // sparse 1K-document impact map and the range-ID translation
                // on the query path while retaining conservative BM25 bounds.
                try appendImpactRecord(alloc, &scratch.impact_block_max, chunk_max_freq, chunk_min_norm);
            } else if (!separate_impact_ranges) {
                try scratch.block_max.appendSlice(alloc, &@as([3]u8, .{
                    @truncate(chunk_max_freq),
                    @truncate(chunk_max_freq >> 8),
                    fieldNormToId(chunk_min_norm),
                }));
            }

            doc_start = doc_end;
        }

        try appendPostingSkipData(alloc, &scratch.skip, scratch.chunks.items);
        try appendCompactChunkMeta(
            alloc,
            &scratch.chunk_meta,
            scratch.chunks.items,
            if (posting_count_blocks) 0 else config.chunk_size,
            config.wireVersion(),
            scratch,
        );

        const stored_chunks: u32 = @intCast(scratch.chunks.items.len);
        const chunk_meta_len: u32 = @intCast(scratch.chunk_meta.items.len);
        const positions_len: u32 = @intCast(scratch.positions.items.len);
        const skip_len: u32 = @intCast(scratch.skip.items.len);
        const payload_len: u32 = @intCast(scratch.payload.items.len);
        const impact_count: u32 = @intCast(scratch.impact_block_max.items.len / blockMaxRecordSize(config.wireVersion()));
        const impact_ids_len: u32 = @intCast(scratch.impact_ids.items.len);
        if (usesPackedImpactFrequency(config.wireVersion())) {
            try encodeImpactMetadata(alloc, scratch, impact_count, config.wireVersion());
        }
        const impact_meta_len: usize = if (usesPackedImpactFrequency(config.wireVersion()))
            scratch.impact_encoded.items.len
        else
            scratch.impact_block_max.items.len;
        const compact_postings_header = usesCompactPostingsHeader(config.wireVersion());
        const header_len = if (compact_postings_header)
            varintU32Size(doc_freq) +
                varintU32Size(payload_len) +
                varintU32Size(positions_len) +
                (if (separate_impact_ranges and doc_freq > config.chunk_size)
                    varintU32Size(impact_count) + varintU32Size(impact_ids_len)
                else
                    0)
        else
            varintU32Size(doc_freq) +
                varintU32Size(stored_chunks) +
                varintU32Size(chunk_meta_len) +
                varintU32Size(payload_len) +
                varintU32Size(positions_len) +
                varintU32Size(skip_len) +
                (if (separate_impact_ranges) varintU32Size(impact_count) + varintU32Size(impact_ids_len) else 0);
        const total_len = header_len + scratch.block_max.items.len + scratch.chunk_meta.items.len + scratch.payload.items.len + scratch.positions.items.len + scratch.skip.items.len + impact_meta_len + scratch.impact_ids.items.len;
        try out.ensureUnusedCapacity(alloc, total_len);

        try writeVarintU32(alloc, out, doc_freq);
        if (!compact_postings_header) {
            try writeVarintU32(alloc, out, stored_chunks);
            try writeVarintU32(alloc, out, chunk_meta_len);
        }
        try writeVarintU32(alloc, out, payload_len);
        try writeVarintU32(alloc, out, positions_len);
        if (!compact_postings_header) try writeVarintU32(alloc, out, skip_len);
        if (separate_impact_ranges and (!compact_postings_header or doc_freq > config.chunk_size)) {
            try writeVarintU32(alloc, out, impact_count);
            try writeVarintU32(alloc, out, impact_ids_len);
        }

        const term_start = out.items.len - header_len;
        try out.appendSlice(alloc, scratch.block_max.items);
        try out.appendSlice(alloc, scratch.chunk_meta.items);
        try out.appendSlice(alloc, scratch.payload.items);
        try out.appendSlice(alloc, scratch.positions.items);
        try out.appendSlice(alloc, scratch.skip.items);
        if (usesPackedImpactFrequency(config.wireVersion())) {
            try out.appendSlice(alloc, scratch.impact_encoded.items);
        } else {
            try out.appendSlice(alloc, scratch.impact_block_max.items);
        }
        try out.appendSlice(alloc, scratch.impact_ids.items);
        std.debug.assert(out.items.len - term_start == total_len);
    }
};

// ============================================================================
// Index reader (query path)
// ============================================================================

/// Reads the origin/main v23 format and the current production format.
pub const InvertedIndexReader = struct {
    alloc: Allocator,
    data: []const u8,
    doc_count: u32,
    total_field_len: u64,
    chunk_size: u32,
    postings_offset: usize,
    norms_data: []const u8,
    version: u8,
    dict_block_count: u32,
    dict_blocks: []const u8,
    dict_index: []const u8,
    dict_fst: vellum.FST,
    /// Optional per-segment term bloom filter. When present, callers can
    /// reject absent terms before walking the FST. Borrows into `data`.
    term_bloom: ?bloom.BorrowedFilter,

    pub fn init(alloc: Allocator, data: []const u8) !InvertedIndexReader {
        if (data.len < v7_header_size) return error.InvalidData;
        if (!std.mem.eql(u8, data[0..4], "INVT")) return error.InvalidMagic;
        const version = data[4];
        if (version != wire_version_legacy and version != wire_version_current) return error.UnsupportedVersion;

        const doc_count = std.mem.readInt(u32, data[5..9], .little);
        const total_field_len = std.mem.readInt(u64, data[9..17], .little);
        const chunk_size = std.mem.readInt(u32, data[17..21], .little);
        const dict_len = std.mem.readInt(u32, data[21..25], .little);
        const bloom_len = std.mem.readInt(u32, data[25..29], .little);
        const norms_len = std.mem.readInt(u32, data[29..33], .little);
        const postings_offset: usize = v7_header_size;

        if (dict_len < term_dict_header_size or dict_len > data.len) return error.InvalidData;
        const dict_offset = data.len - dict_len;
        if (dict_offset < postings_offset) return error.InvalidData;
        if (@as(usize, bloom_len) + @as(usize, norms_len) > dict_offset - postings_offset) return error.InvalidData;
        const norms_offset = dict_offset - @as(usize, bloom_len) - @as(usize, norms_len);
        const norms_data = data[norms_offset..][0..norms_len];
        const dict_data = data[dict_offset..];
        if (!std.mem.eql(u8, dict_data[0..4], term_dict_magic)) return error.InvalidData;
        const block_count = std.mem.readInt(u32, dict_data[4..8], .little);
        const block_data_len = std.mem.readInt(u32, dict_data[8..12], .little);
        const block_index_len = std.mem.readInt(u32, dict_data[12..16], .little);
        const block_fst_len = std.mem.readInt(u32, dict_data[16..20], .little);
        const index_records_len = @as(usize, block_count) * term_dict_index_record_size;
        if (block_index_len < index_records_len) return error.InvalidData;
        if (term_dict_header_size + @as(usize, block_data_len) + @as(usize, block_index_len) + @as(usize, block_fst_len) != dict_data.len) return error.InvalidData;
        const block_data = dict_data[term_dict_header_size..][0..block_data_len];
        const block_index_data = dict_data[term_dict_header_size + @as(usize, block_data_len) ..][0..block_index_len];
        const block_fst_data = dict_data[term_dict_header_size + @as(usize, block_data_len) + @as(usize, block_index_len) ..];
        const dict_fst = try vellum.FST.load(block_fst_data);

        var term_bloom: ?bloom.BorrowedFilter = null;
        if (bloom_len > 0) {
            const bloom_offset = dict_offset - bloom_len;
            term_bloom = bloom.BorrowedFilter.decode(data[bloom_offset..dict_offset]) catch null;
        }

        return .{
            .alloc = alloc,
            .data = data,
            .doc_count = doc_count,
            .total_field_len = total_field_len,
            .chunk_size = chunk_size,
            .postings_offset = postings_offset,
            .norms_data = norms_data,
            .version = version,
            .dict_block_count = block_count,
            .dict_blocks = block_data,
            .dict_index = block_index_data,
            .dict_fst = dict_fst,
            .term_bloom = term_bloom,
        };
    }

    fn normForDoc(self: *const InvertedIndexReader, doc_id: u32) u32 {
        return decodeNormValue(self.norms_data, doc_id);
    }

    /// Decoded BM25 field length for one document. The value uses the same
    /// norm representation as scoring, so deletion-adjusted aggregate stats
    /// remain consistent with the lengths consumed by the scorer.
    pub fn docLength(self: *const InvertedIndexReader, doc_id: u32) u32 {
        if (doc_id >= self.doc_count) return 0;
        return self.normForDoc(doc_id);
    }

    /// Average document length for BM25.
    pub fn avgDocLen(self: *const InvertedIndexReader) f32 {
        if (self.doc_count == 0) return 0;
        return @as(f32, @floatFromInt(self.total_field_len)) / @as(f32, @floatFromInt(self.doc_count));
    }

    pub const LayoutStats = struct {
        header_bytes: u64 = 0,
        term_dict_bytes: u64 = 0,
        norm_bytes: u64 = 0,
        term_block_bytes: u64 = 0,
        term_index_bytes: u64 = 0,
        fst_bytes: u64 = 0,
        bloom_bytes: u64 = 0,
        postings_bytes: u64 = 0,
        postings_header_bytes: u64 = 0,
        projected_compact_postings_header_bytes: u64 = 0,
        block_max_bytes: u64 = 0,
        impact_record_count: u64 = 0,
        impact_range_id_bytes: u64 = 0,
        projected_adaptive_impact_bytes: u64 = 0,
        projected_adaptive_impact_terms: u64 = 0,
        projected_raw_impact_terms: u64 = 0,
        projected_impact_descriptor_header_delta: i64 = 0,
        chunk_meta_bytes: u64 = 0,
        postings_payload_bytes: u64 = 0,
        positions_bytes: u64 = 0,
        skip_bytes: u64 = 0,
        term_count: u64 = 0,
        one_hit_terms: u64 = 0,
        single_doc_postings_terms: u64 = 0,
        postings_terms: u64 = 0,
        postings_doc_frequency_total: u64 = 0,
        projected_posting_count_blocks_64: u64 = 0,
        projected_posting_count_blocks_128: u64 = 0,
        projected_posting_count_blocks_256: u64 = 0,
    };

    pub fn layoutStats(self: *const InvertedIndexReader) LayoutStats {
        const dict_len = std.mem.readInt(u32, self.data[21..25], .little);
        const bloom_len = std.mem.readInt(u32, self.data[25..29], .little);
        const norms_len = std.mem.readInt(u32, self.data[29..33], .little);
        var stats = LayoutStats{
            .header_bytes = v7_header_size,
            .term_dict_bytes = dict_len,
            .norm_bytes = norms_len,
            .bloom_bytes = bloom_len,
            .postings_bytes = if (self.data.len >= v7_header_size + @as(usize, dict_len) + @as(usize, bloom_len) + @as(usize, norms_len))
                @intCast(self.data.len - v7_header_size - @as(usize, dict_len) - @as(usize, bloom_len) - @as(usize, norms_len))
            else
                0,
        };
        // Each blocked-dictionary index record gives the corresponding block
        // offset, and every block begins with prefix length plus entry count.
        // This is O(number of 25-48 term blocks), touches dictionary metadata
        // only, and is cached in SegmentEntry at open. It avoids deriving the
        // public term count by decoding every posting on every status poll.
        stats.term_count = count_terms: {
            var total: u64 = 0;
            for (0..self.dict_block_count) |block_idx| {
                const block_offset = self.termBlockOffset(block_idx);
                if (block_offset >= self.dict_blocks.len) break :count_terms 0;
                var block_cursor: usize = block_offset;
                _ = readVarintU32(self.dict_blocks, &block_cursor) catch break :count_terms 0;
                const block_terms = readVarintU32(self.dict_blocks, &block_cursor) catch break :count_terms 0;
                total +|= @as(u64, block_terms);
            }
            break :count_terms total;
        };
        const dict_offset = self.data.len - @as(usize, dict_len);
        if (dict_len >= term_dict_header_size and dict_offset < self.data.len) {
            const dict_data = self.data[dict_offset..];
            if (std.mem.eql(u8, dict_data[0..4], term_dict_magic)) {
                const block_data_len = std.mem.readInt(u32, dict_data[8..12], .little);
                const block_index_len = std.mem.readInt(u32, dict_data[12..16], .little);
                const block_fst_len = std.mem.readInt(u32, dict_data[16..20], .little);
                if (term_dict_header_size + @as(usize, block_data_len) + @as(usize, block_index_len) + @as(usize, block_fst_len) == @as(usize, dict_len)) {
                    stats.term_block_bytes = block_data_len;
                    stats.term_index_bytes = block_index_len;
                    stats.fst_bytes = block_fst_len;
                }
            }
        }
        return stats;
    }

    pub fn detailedLayoutStats(self: *const InvertedIndexReader) !LayoutStats {
        var stats = self.layoutStats();

        var it = try self.termIterator();
        defer it.deinit();
        while (try it.next()) |entry| {
            switch (entry.result) {
                .one_hit => stats.one_hit_terms +|= 1,
                .postings => |postings| {
                    stats.postings_terms +|= 1;
                    if (postings.doc_freq == 1) stats.single_doc_postings_terms +|= 1;
                    stats.postings_doc_frequency_total +|= postings.doc_freq;
                    stats.projected_posting_count_blocks_64 +|= (@as(u64, postings.doc_freq) + 63) / 64;
                    stats.projected_posting_count_blocks_128 +|= (@as(u64, postings.doc_freq) + 127) / 128;
                    stats.projected_posting_count_blocks_256 +|= (@as(u64, postings.doc_freq) + 255) / 256;
                    stats.postings_header_bytes +|= @intCast(postings.header_len);
                    if (postings.inline_single_doc) {
                        stats.projected_compact_postings_header_bytes +|= @intCast(postings.header_len);
                    } else {
                        const positions_len = if (postings.positions_data) |positions| positions.len else 0;
                        var projected_header = varintU32Size(postings.doc_freq) +
                            varintU32Size(@intCast(postings.payload_data.len)) +
                            varintU32Size(@intCast(positions_len));
                        if (postings.doc_freq > simd_bitpack.block_values) {
                            const projection = if (postings.block_max) |block_max_info| block_max_info.adaptiveColumnProjection() else BlockMaxInfo.AdaptiveColumnProjection{};
                            const descriptor = (@as(u64, projection.records) << 1) | @intFromBool(projection.use_adaptive);
                            if (descriptor <= std.math.maxInt(u32)) projected_header += varintU32Size(@intCast(descriptor));
                            const impact_ids_len = if (postings.impact_chunk_ids_data) |ids| ids.len else 0;
                            projected_header += varintU32Size(@intCast(impact_ids_len));
                        }
                        stats.projected_compact_postings_header_bytes +|= @intCast(projected_header);
                    }
                    if (postings.inline_single_doc and postings.inline_has_locs) {
                        stats.positions_bytes +|= @as(u64, @intCast(postings.inline_positions_data.len)) + 1;
                    }
                    if (postings.block_max) |block_max_info| {
                        stats.block_max_bytes +|= @intCast(block_max_info.meta.len);
                        const projection = block_max_info.adaptiveColumnProjection();
                        stats.impact_record_count +|= projection.records;
                        stats.projected_adaptive_impact_bytes +|= projection.selected_bytes;
                        stats.projected_impact_descriptor_header_delta += projection.descriptor_header_delta;
                        if (projection.use_adaptive) {
                            stats.projected_adaptive_impact_terms +|= 1;
                        } else {
                            stats.projected_raw_impact_terms +|= 1;
                        }
                    }
                    if (postings.impact_chunk_ids_data) |ids| stats.impact_range_id_bytes +|= @intCast(ids.len);
                    stats.chunk_meta_bytes +|= @intCast(postings.chunk_meta_data.len);
                    stats.postings_payload_bytes +|= @intCast(postings.payload_data.len);
                    if (postings.positions_data) |positions_data| {
                        stats.positions_bytes +|= @intCast(positions_data.len);
                    }
                    if (postings.skip_data) |skip_data| {
                        stats.skip_bytes +|= @intCast(skip_data.len);
                    }
                },
            }
        }
        return stats;
    }

    /// Look up a term using the blocked term dictionary. Returns posting data, or null.
    /// For 1-hit terms, returns a synthetic TermPostings with the single doc.
    /// Consults the per-segment term bloom filter before walking the FST
    /// when present, so absent-term lookups skip the FST traversal entirely.
    pub fn lookup(self: *const InvertedIndexReader, term: []const u8) ?LookupResult {
        if (self.term_bloom) |filter| {
            const h = termBloomHashes(term);
            if (!filter.maybeContainsHashes(h.h1, h.h2)) return null;
        }
        const block_offset = self.findTermBlockOffset(term) catch return null;
        const dict_value = self.lookupInTermBlock(block_offset, term) catch return null;

        if (fstValIs1Hit(dict_value)) {
            const decoded = fstValDecode1Hit(dict_value);
            return .{ .one_hit = .{
                .doc_num = @intCast(decoded.doc_num),
                .norm_bits = self.normForDoc(@intCast(decoded.doc_num)),
            } };
        }

        return .{ .postings = self.readPostings(dict_value) };
    }

    /// Iterate all terms in the dictionary using the block-ceiling FST iterator.
    pub fn termIterator(self: *const InvertedIndexReader) !TermIterator {
        return .{
            .alloc = self.alloc,
            .reader = self,
            .block_iter = try self.dict_fst.iterator(self.alloc, null, null),
        };
    }

    /// Iterate terms in a lexicographic range [start, end).
    pub fn rangeTermIterator(self: *const InvertedIndexReader, start: ?[]const u8, end: ?[]const u8) !TermIterator {
        return .{
            .alloc = self.alloc,
            .reader = self,
            .block_iter = try self.dict_fst.iterator(self.alloc, start, null),
            .start = start,
            .end = end,
        };
    }

    /// Iterate terms matching an automaton. Blocks are enumerated by prefix FST,
    /// then the automaton is checked against full terms inside each block.
    pub fn fstSearchIterator(self: *const InvertedIndexReader, aut: vellum.Automaton) !TermIterator {
        return .{
            .alloc = self.alloc,
            .reader = self,
            .block_iter = try self.dict_fst.iterator(self.alloc, null, null),
            .automaton = aut,
        };
    }

    fn findTermBlockOffset(self: *const InvertedIndexReader, term: []const u8) !u32 {
        var lo: usize = 0;
        var hi: usize = self.dict_block_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const ceiling = try self.termBlockCeiling(mid);
            if (std.mem.order(u8, ceiling, term) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo >= self.dict_block_count) return error.NotFound;
        return self.termBlockOffset(lo);
    }

    fn termBlockOffset(self: *const InvertedIndexReader, block_idx: usize) u32 {
        const off = block_idx * term_dict_index_record_size;
        return std.mem.readInt(u32, self.dict_index[off..][0..4], .little);
    }

    fn termBlockCeiling(self: *const InvertedIndexReader, block_idx: usize) ![]const u8 {
        const records_len = @as(usize, self.dict_block_count) * term_dict_index_record_size;
        const record_off = block_idx * term_dict_index_record_size;
        const term_offset = std.mem.readInt(u32, self.dict_index[record_off + 4 ..][0..4], .little);
        if (term_offset >= self.dict_index.len - records_len) return error.InvalidData;
        var cursor = records_len + @as(usize, term_offset);
        const term_len = try readVarintU32(self.dict_index, &cursor);
        if (cursor + term_len > self.dict_index.len) return error.InvalidData;
        return self.dict_index[cursor..][0..term_len];
    }

    fn lookupInTermBlock(self: *const InvertedIndexReader, block_offset: u32, term: []const u8) !u64 {
        if (block_offset >= self.dict_blocks.len) return error.InvalidData;
        var cursor: usize = block_offset;
        const prefix_len = try readVarintU32(self.dict_blocks, &cursor);
        const entry_count = try readVarintU32(self.dict_blocks, &cursor);
        if (cursor + prefix_len > self.dict_blocks.len) return error.Truncated;
        const prefix = self.dict_blocks[cursor..][0..prefix_len];
        cursor += prefix_len;
        if (!std.mem.startsWith(u8, term, prefix)) return error.NotFound;
        const wanted_suffix = term[prefix.len..];

        // Query terms are overwhelmingly short. Reconstruct only the prefix
        // needed to compare against the requested suffix and keep the common
        // path entirely on the stack. A dictionary entry longer than the
        // requested suffix can never match, so retaining its tail only creates
        // allocator traffic in every segment lookup.
        var stack_suffix: [256]u8 = undefined;
        var heap_suffix: ?[]u8 = null;
        const suffix_buf: []u8 = if (wanted_suffix.len <= stack_suffix.len)
            stack_suffix[0..wanted_suffix.len]
        else blk: {
            const owned = try self.alloc.alloc(u8, wanted_suffix.len);
            heap_suffix = owned;
            break :blk owned;
        };
        defer if (heap_suffix) |owned| self.alloc.free(owned);

        var suffix_prefix_len: usize = 0;
        var previous_suffix_len: usize = 0;
        var last_postings_offset: u64 = 0;
        var remaining = entry_count;
        while (remaining > 0) : (remaining -= 1) {
            const shared_len: usize = try readVarintU32(self.dict_blocks, &cursor);
            const leaf_len: usize = try readVarintU32(self.dict_blocks, &cursor);
            if (shared_len > previous_suffix_len) return error.InvalidData;
            if (cursor + leaf_len > self.dict_blocks.len) return error.Truncated;

            const retained_len = @min(shared_len, suffix_buf.len);
            if (retained_len > suffix_prefix_len) {
                // We retain min(actual length, requested length) bytes from the
                // previous suffix. Therefore a shared prefix can exceed the
                // retained bytes only after both have reached the requested
                // length, in which case retained_len == suffix_prefix_len.
                return error.InvalidData;
            }
            const copied_leaf_len = @min(leaf_len, suffix_buf.len - retained_len);
            @memcpy(suffix_buf[retained_len..][0..copied_leaf_len], self.dict_blocks[cursor..][0..copied_leaf_len]);
            suffix_prefix_len = retained_len + copied_leaf_len;
            previous_suffix_len = shared_len +| leaf_len;
            cursor += leaf_len;
            const value = decodeTermDictBlockValueDelta(try readVarintU64(self.dict_blocks, &cursor), &last_postings_offset);

            const prefix_order = std.mem.order(u8, suffix_buf[0..suffix_prefix_len], wanted_suffix);
            const order: std.math.Order = if (prefix_order != .eq)
                prefix_order
            else
                std.math.order(previous_suffix_len, wanted_suffix.len);
            switch (order) {
                .eq => return value,
                .gt => return error.NotFound,
                .lt => {},
            }
        }
        return error.NotFound;
    }

    fn readPostings(self: *const InvertedIndexReader, offset: u64) TermPostings {
        // Dictionary values are deliberately u64. A force-merged full-text
        // section can exceed 4 GiB even though document IDs remain u32; do not
        // truncate later postings offsets when reopening such a segment.
        const base = self.postings_offset + @as(usize, @intCast(offset));
        var cursor = base;
        const doc_freq = readVarintU32(self.data, &cursor) catch unreachable;
        if (doc_freq == 0 and usesInlineSingleDocPostings(self.version)) {
            const doc_id = readVarintU32(self.data, &cursor) catch unreachable;
            const encoded_freq = readVarintU32(self.data, &cursor) catch unreachable;
            const decoded = decodeFreqHasLocs(encoded_freq);
            const inline_header_len = cursor - base;
            var position_bits: u8 = 0;
            var positions_data: []const u8 = &.{};
            if (decoded.has_locs) {
                position_bits = self.data[cursor];
                cursor += 1;
                const positions_len = packedU32ByteLen(@intCast(decoded.freq), position_bits);
                positions_data = self.data[cursor..][0..positions_len];
                cursor += positions_len;
            }
            return .{
                .doc_freq = 1,
                .serialized_data = self.data[base..cursor],
                .header_len = inline_header_len,
                .chunk_size = self.chunk_size,
                .version = self.version,
                .doc_range_aligned = false,
                .chunk_meta_data = &.{},
                .chunk_meta_count = 0,
                .payload_data = &.{},
                .norms_data = self.norms_data,
                .inline_single_doc = true,
                .inline_doc_id = doc_id,
                .inline_freq = @intCast(decoded.freq),
                .inline_has_locs = decoded.has_locs,
                .inline_position_bits = position_bits,
                .inline_positions_data = positions_data,
            };
        }
        const compact_postings_header = usesCompactPostingsHeader(self.version);
        const stored_chunks = if (compact_postings_header)
            1 + (doc_freq - 1) / self.chunk_size
        else
            readVarintU32(self.data, &cursor) catch unreachable;
        const stored_chunk_meta_len = if (compact_postings_header)
            null
        else
            readVarintU32(self.data, &cursor) catch unreachable;
        const stored_payload_len = if (self.version >= wire_version_checkpoints)
            readVarintU32(self.data, &cursor) catch unreachable
        else
            null;
        const positions_len = readVarintU32(self.data, &cursor) catch unreachable;
        const skip_len: u32 = if (compact_postings_header)
            if (stored_chunks < postings_skip_min_chunks)
                0
            else
                @intCast(((stored_chunks - 1) / postings_skip_stride_chunks) * postings_skip_record_size_v24)
        else
            readVarintU32(self.data, &cursor) catch unreachable;
        const has_explicit_impact_lengths = usesSeparateImpactRanges(self.version) and
            (!compact_postings_header or doc_freq > self.chunk_size);
        const impact_count = if (has_explicit_impact_lengths)
            readVarintU32(self.data, &cursor) catch unreachable
        else if (usesSeparateImpactRanges(self.version))
            @as(u32, 1)
        else
            @as(u32, 0);
        const impact_ids_len = if (has_explicit_impact_lengths)
            readVarintU32(self.data, &cursor) catch unreachable
        else
            @as(u32, 0);
        const header_len = cursor - base;
        const block_max_start = cursor;
        const block_max_len = if (usesSeparateImpactRanges(self.version)) 0 else @as(usize, stored_chunks) * blockMaxRecordSize(self.version);
        const chunk_meta_start = block_max_start + block_max_len;
        const chunk_meta_len: u32 = if (stored_chunk_meta_len) |length|
            length
        else
            @intCast((compactChunkMetaLayout(self.data[chunk_meta_start..], stored_chunks, self.version) catch unreachable).total_len);
        const payload_start = chunk_meta_start + @as(usize, chunk_meta_len);
        const chunk_meta_data = self.data[chunk_meta_start..][0..chunk_meta_len];
        const payload_len: usize = if (stored_payload_len) |length|
            length
        else if (stored_chunks == 0)
            0
        else blk: {
            const last_meta = readCompactChunkMetaAt(chunk_meta_data, stored_chunks, self.version, self.chunk_size, doc_freq, @as(usize, stored_chunks) - 1) catch unreachable;
            break :blk @as(usize, last_meta.doc_ctrl_off) + last_meta.doc_ctrl_len;
        };
        const positions_start = payload_start + payload_len;
        const skip_start = positions_start + positions_len;
        const impact_block_max_start = skip_start + skip_len;
        const impact_block_max_len = if (usesPackedImpactFrequency(self.version))
            packedU32ByteLen(impact_count, 5) + @as(usize, impact_count)
        else
            @as(usize, impact_count) * blockMaxRecordSize(self.version);
        const impact_ids_start = impact_block_max_start + impact_block_max_len;
        const after_postings = impact_ids_start + impact_ids_len;

        const impact_meta = self.data[impact_block_max_start..][0..impact_block_max_len];
        const packed_impact_frequency = usesPackedImpactFrequency(self.version);

        const scoring_block_max: ?BlockMaxInfo = if (usesSeparateImpactRanges(self.version))
            if (impact_count > 0) .{
                .meta = impact_meta,
                .chunk_size = if (impact_ids_len > 0) impact_range_doc_count else self.chunk_size,
                .chunk_meta_data = if (impact_ids_len > 0)
                    self.data[impact_ids_start..][0..impact_ids_len]
                else
                    chunk_meta_data,
                .chunk_meta_count = impact_count,
                .version = self.version,
                .range_ids = impact_ids_len > 0,
                .packed_impact_frequency = packed_impact_frequency,
            } else null
        else
            .{
                .meta = self.data[block_max_start..][0..block_max_len],
                .chunk_size = self.chunk_size,
                .chunk_meta_data = chunk_meta_data,
                .chunk_meta_count = stored_chunks,
                .version = self.version,
            };

        return .{
            .doc_freq = doc_freq,
            .serialized_data = self.data[base..after_postings],
            .header_len = header_len,
            .chunk_size = self.chunk_size,
            .version = self.version,
            .doc_range_aligned = !usesPostingCountBlocks(self.version) or (usesSeparateImpactRanges(self.version) and impact_ids_len > 0),
            .block_max = scoring_block_max,
            .chunk_meta_data = chunk_meta_data,
            .chunk_meta_count = stored_chunks,
            .payload_data = self.data[payload_start..][0..payload_len],
            .norms_data = self.norms_data,
            .positions_data = if (positions_len > 0) self.data[positions_start..][0..positions_len] else null,
            .skip_data = if (skip_len > 0) self.data[skip_start..][0..skip_len] else null,
            .impact_chunk_ids_data = if (impact_ids_len > 0) self.data[impact_ids_start..][0..impact_ids_len] else null,
            .impact_chunk_count = if (impact_ids_len > 0) impact_count else 0,
        };
    }
};

pub const TermIterator = struct {
    alloc: Allocator,
    reader: *const InvertedIndexReader,
    block_iter: vellum.FSTIterator,
    current_block_prefix: []const u8 = &.{},
    current_block_cursor: usize = 0,
    current_block_remaining: u32 = 0,
    current_block_last_postings_offset: u64 = 0,
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
    automaton: ?vellum.Automaton = null,
    // We must copy the key before advancing, because block parsing reuses section slices.
    current_key: std.ArrayListUnmanaged(u8) = .empty,

    pub const Entry = struct { term: []const u8, result: LookupResult };

    pub fn next(self: *TermIterator) !?Entry {
        return self.nextInternal(false, null);
    }

    /// Diagnostic iterator path. The normal next() instantiation contains no
    /// counter update or conditional branch in its term-decoding hot loop.
    pub fn nextWithDecodedCount(self: *TermIterator, decoded_count: *u64) !?Entry {
        return self.nextInternal(true, decoded_count);
    }

    fn nextInternal(self: *TermIterator, comptime track_decoded: bool, decoded_count: ?*u64) !?Entry {
        while (true) {
            if (self.current_block_remaining == 0) {
                if (!try self.loadNextBlock()) return null;
            }

            const shared_len = readVarintU32(self.reader.dict_blocks, &self.current_block_cursor) catch return error.InvalidData;
            const leaf_len = readVarintU32(self.reader.dict_blocks, &self.current_block_cursor) catch return error.InvalidData;
            if (shared_len > self.current_key.items.len) return error.InvalidData;
            if (self.current_block_cursor + leaf_len > self.reader.dict_blocks.len) return error.InvalidData;
            const leaf = self.reader.dict_blocks[self.current_block_cursor..][0..leaf_len];
            self.current_block_cursor += leaf_len;
            const value = decodeTermDictBlockValueDelta(readVarintU64(self.reader.dict_blocks, &self.current_block_cursor) catch return error.InvalidData, &self.current_block_last_postings_offset);
            self.current_block_remaining -= 1;
            if (comptime track_decoded) decoded_count.?.* += 1;

            self.current_key.shrinkRetainingCapacity(self.current_block_prefix.len + shared_len);
            try self.current_key.appendSlice(self.alloc, leaf);

            if (self.start) |start| {
                if (std.mem.order(u8, self.current_key.items, start) == .lt) continue;
            }
            if (self.end) |end| {
                if (std.mem.order(u8, self.current_key.items, end) != .lt) return null;
            }
            if (self.automaton) |aut| {
                if (!termMatchesAutomaton(aut, self.current_key.items)) continue;
            }

            const result: LookupResult = if (fstValIs1Hit(value))
                .{ .one_hit = .{
                    .doc_num = @intCast(fstValDecode1Hit(value).doc_num),
                    .norm_bits = self.reader.normForDoc(@intCast(fstValDecode1Hit(value).doc_num)),
                } }
            else
                .{ .postings = self.reader.readPostings(value) };

            return .{ .term = self.current_key.items, .result = result };
        }
    }

    fn loadNextBlock(self: *TermIterator) !bool {
        const current = self.block_iter.current() orelse return false;
        _ = try self.block_iter.nextEntry();
        const block_offset: usize = @intCast(current.val);
        if (block_offset >= self.reader.dict_blocks.len) return error.InvalidData;
        var cursor = block_offset;
        const prefix_len = readVarintU32(self.reader.dict_blocks, &cursor) catch return error.InvalidData;
        self.current_block_remaining = readVarintU32(self.reader.dict_blocks, &cursor) catch return error.InvalidData;
        if (cursor + prefix_len > self.reader.dict_blocks.len) return error.InvalidData;
        self.current_block_prefix = self.reader.dict_blocks[cursor..][0..prefix_len];
        cursor += prefix_len;
        self.current_block_cursor = cursor;
        self.current_block_last_postings_offset = 0;
        self.current_key.clearRetainingCapacity();
        try self.current_key.appendSlice(self.alloc, self.current_block_prefix);
        return true;
    }

    pub fn deinit(self: *TermIterator) void {
        self.current_key.deinit(self.alloc);
        self.block_iter.deinit();
    }
};

fn termMatchesAutomaton(aut: vellum.Automaton, term: []const u8) bool {
    var state = aut.start();
    for (term) |b| {
        if (!aut.canMatch(state)) return false;
        state = aut.accept(state, b);
    }
    return aut.isMatch(state);
}

/// Result of looking up a term. Either a full postings list or a 1-hit value.
pub const LookupResult = union(enum) {
    postings: TermPostings,
    one_hit: OneHit,

    pub const OneHit = struct {
        doc_num: u32,
        norm_bits: u32,
    };

    /// Get the document frequency for this term.
    pub fn docFreq(self: LookupResult) u32 {
        return switch (self) {
            .postings => |p| p.doc_freq,
            .one_hit => 1,
        };
    }

    /// Create a postings iterator.
    pub fn iterator(self: *const LookupResult, alloc: Allocator) !PostingsIterator {
        return switch (self.*) {
            .postings => |*p| p.iterator(alloc),
            .one_hit => |h| PostingsIterator.initOneHit(h),
        };
    }
};

/// Per-chunk block-max metadata for WAND scoring acceleration.
/// Each stored postings chunk has one block-max record. v23-v25 records use
/// `[max_freq:u16][min_norm:u16][max_norm:u16]`; v26 stores only the values the
/// scorer actually consumes as `[max_freq:u16][min_norm_id:u8]`.
pub const BlockMaxInfo = struct {
    /// Packed records aligned with `chunk_meta_data`.
    meta: []const u8,
    chunk_size: u32,
    chunk_meta_data: []const u8,
    chunk_meta_count: u32,
    version: u8,
    range_ids: bool = false,
    packed_impact_frequency: bool = false,

    pub const AdaptiveColumnProjection = struct {
        records: u64 = 0,
        current_bytes: u64 = 0,
        selected_bytes: u64 = 0,
        descriptor_header_delta: i64 = 0,
        frequency_bits: u8 = 5,
        norm_bits: u8 = 8,
        use_adaptive: bool = false,
    };

    /// Project an exact per-term column-range encoding. This is read-only
    /// format-design instrumentation: it preserves every existing frequency
    /// bucket and norm ID, unlike the rejected global low-DF bound collapse.
    pub fn adaptiveColumnProjection(self: BlockMaxInfo) AdaptiveColumnProjection {
        const count = self.chunkCount();
        var projection = AdaptiveColumnProjection{
            .records = @intCast(count),
            .current_bytes = @intCast(self.meta.len),
            .selected_bytes = @intCast(self.meta.len),
        };
        if (!self.packed_impact_frequency or count == 0) return projection;

        const frequency_bytes = packedU32ByteLen(count, 5);
        if (self.meta.len != frequency_bytes + count) return projection;
        var min_frequency: u8 = 31;
        var max_frequency: u8 = 0;
        var min_norm: u8 = std.math.maxInt(u8);
        var max_norm: u8 = 0;
        for (0..count) |ordinal| {
            const bit_position = ordinal * 5;
            const byte_index = bit_position >> 3;
            const bit_shift: u4 = @intCast(bit_position & 7);
            const window = @as(u16, self.meta[byte_index]) |
                (if (byte_index + 1 < frequency_bytes) @as(u16, self.meta[byte_index + 1]) << 8 else 0);
            const frequency: u8 = @as(u5, @truncate(window >> bit_shift));
            const norm = self.meta[frequency_bytes + ordinal];
            min_frequency = @min(min_frequency, frequency);
            max_frequency = @max(max_frequency, frequency);
            min_norm = @min(min_norm, norm);
            max_norm = @max(max_norm, norm);
        }

        const frequency_bits = bitWidthU32(max_frequency - min_frequency);
        const norm_bits = bitWidthU32(max_norm - min_norm);
        const adaptive_bytes = 1 +
            @as(usize, @intFromBool(frequency_bits < 5)) +
            @as(usize, @intFromBool(norm_bits < 8)) +
            packedU32ByteLen(count, frequency_bits) +
            packedU32ByteLen(count, norm_bits);
        projection.frequency_bits = frequency_bits;
        projection.norm_bits = norm_bits;
        projection.use_adaptive = adaptive_bytes < self.meta.len;
        if (projection.use_adaptive) projection.selected_bytes = @intCast(adaptive_bytes);

        const old_descriptor_bytes = varintU32Size(@intCast(count));
        const encoded_descriptor = (@as(u64, count) << 1) | @intFromBool(projection.use_adaptive);
        if (encoded_descriptor <= std.math.maxInt(u32)) {
            const new_descriptor_bytes = varintU32Size(@intCast(encoded_descriptor));
            projection.descriptor_header_delta = @as(i64, @intCast(new_descriptor_bytes)) - @as(i64, @intCast(old_descriptor_bytes));
        }
        return projection;
    }

    fn recordSize(self: BlockMaxInfo) usize {
        return blockMaxRecordSize(self.version);
    }

    fn minNormAt(self: BlockMaxInfo, offset: usize) u32 {
        if (self.packed_impact_frequency) {
            const freq_bytes = packedU32ByteLen(self.chunk_meta_count, 5);
            return fieldNormFromId(self.meta[freq_bytes + offset]);
        }
        return if (self.version >= wire_version_separate_impact_ranges)
            fieldNormFromId(self.meta[offset + 1])
        else if (self.version >= wire_version_compact_block_max)
            fieldNormFromId(self.meta[offset + 2])
        else
            std.mem.readInt(u16, self.meta[offset + 2 ..][0..2], .little);
    }

    fn maxFreqAt(self: BlockMaxInfo, offset: usize) u16 {
        if (self.packed_impact_frequency) {
            const freq_bytes = packedU32ByteLen(self.chunk_meta_count, 5);
            const bit_position = offset * 5;
            const byte_index = bit_position >> 3;
            if (byte_index >= freq_bytes) return std.math.maxInt(u16);
            const bit_shift: u4 = @intCast(bit_position & 7);
            const window = @as(u16, self.meta[byte_index]) |
                (if (byte_index + 1 < freq_bytes) @as(u16, self.meta[byte_index + 1]) << 8 else 0);
            const packed_id: u5 = @truncate(window >> bit_shift);
            return impactMaxFreqFromPackedId(packed_id);
        }
        return if (self.version >= wire_version_separate_impact_ranges)
            impactMaxFreqFromId(self.meta[offset])
        else
            std.mem.readInt(u16, self.meta[offset..][0..2], .little);
    }

    fn chunkCount(self: BlockMaxInfo) usize {
        if (self.packed_impact_frequency) return self.chunk_meta_count;
        return @min(self.meta.len / self.recordSize(), @as(usize, self.chunk_meta_count));
    }

    fn storedChunkOrdinal(self: BlockMaxInfo, chunk_idx: u32) ?usize {
        if (self.range_ids) {
            if (self.chunk_meta_count == 0 or self.chunk_meta_data.len == 0) return null;
            return findEncodedImpactChunkOrdinal(self.chunk_meta_data, self.chunk_meta_count, chunk_idx);
        }
        var lo: usize = 0;
        var hi = self.chunkCount();
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const meta_record = readCompactChunkMetaAt(self.chunk_meta_data, self.chunk_meta_count, self.version, self.chunk_size, 0, mid) catch return null;
            if (meta_record.chunk_id < chunk_idx) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo >= self.chunkCount()) return null;
        const meta_record = readCompactChunkMetaAt(self.chunk_meta_data, self.chunk_meta_count, self.version, self.chunk_size, 0, lo) catch return null;
        if (meta_record.chunk_id == chunk_idx) return lo;
        return null;
    }

    /// Compute the maximum possible BM25 impact for a chunk.
    /// Uses the most favorable values in the chunk: max_freq and min_norm (shortest doc).
    pub fn maxImpact(self: BlockMaxInfo, chunk_idx: u32, doc_count: u32, doc_freq: u32, avg_dl: f32, config: BM25Config) f32 {
        const ordinal = self.storedChunkOrdinal(chunk_idx) orelse return 0;
        return self.maxImpactAtOrdinal(ordinal, doc_count, doc_freq, avg_dl, config);
    }

    fn maxImpactAtOrdinal(self: BlockMaxInfo, ordinal: usize, doc_count: u32, doc_freq: u32, avg_dl: f32, config: BM25Config) f32 {
        return self.maxImpactAtOrdinalWithIdf(ordinal, avg_dl, bm25Idf(doc_count, doc_freq), config);
    }

    fn maxImpactAtOrdinalWithIdf(self: BlockMaxInfo, ordinal: usize, avg_dl: f32, idf: f32, config: BM25Config) f32 {
        return self.maxImpactAtOrdinalWithScorer(ordinal, BM25TermScorer.init(avg_dl, idf, config));
    }

    fn maxImpactAtOrdinalWithScorer(self: BlockMaxInfo, ordinal: usize, scorer: BM25TermScorer) f32 {
        if (ordinal >= self.chunkCount()) return 0;
        const offset = if (self.packed_impact_frequency) ordinal else ordinal * self.recordSize();
        const max_freq = self.maxFreqAt(offset);
        const min_norm = self.minNormAt(offset);
        if (max_freq == 0) return 0;
        // Use min_norm as doc_len (shortest doc → highest TF component)
        return scorer.score(max_freq, min_norm);
    }

    fn maxImpactAtOrdinalWithBoundTable(
        self: BlockMaxInfo,
        ordinal: usize,
        scorer: BM25TermScorer,
        idf: f32,
        table: ?*const BM25BoundTable,
    ) f32 {
        if (ordinal >= self.chunkCount()) return 0;
        if (self.packed_impact_frequency) {
            if (table) |bound_table| {
                const freq_bytes = packedU32ByteLen(self.chunk_meta_count, 5);
                const bit_position = ordinal * 5;
                const byte_index = bit_position >> 3;
                if (byte_index >= freq_bytes or freq_bytes + ordinal >= self.meta.len) return scorer.maxScore();
                const bit_shift: u4 = @intCast(bit_position & 7);
                const window = @as(u16, self.meta[byte_index]) |
                    (if (byte_index + 1 < freq_bytes) @as(u16, self.meta[byte_index + 1]) << 8 else 0);
                const packed_freq_id: u5 = @truncate(window >> bit_shift);
                const norm_id = self.meta[freq_bytes + ordinal];
                return bound_table.score(packed_freq_id, norm_id, idf);
            }
        }
        return self.maxImpactAtOrdinalWithScorer(ordinal, scorer);
    }

    /// Conservative maximum impact over every stored chunk in this postings
    /// list. This supports segment ordering/pruning without decoding postings.
    pub fn maxImpactAll(self: BlockMaxInfo, doc_count: u32, doc_freq: u32, avg_dl: f32, config: BM25Config) f32 {
        var maximum: f32 = 0;
        for (0..self.chunkCount()) |ordinal| {
            const offset = if (self.packed_impact_frequency) ordinal else ordinal * self.recordSize();
            const max_freq = self.maxFreqAt(offset);
            const min_norm = self.minNormAt(offset);
            if (max_freq == 0) continue;
            maximum = @max(maximum, bm25Score(max_freq, min_norm, doc_count, doc_freq, avg_dl, config));
        }
        return maximum;
    }
};

/// Parsed posting data for a single term (zero-copy view into section data).
pub const TermPostings = struct {
    doc_freq: u32,
    serialized_data: []const u8,
    header_len: usize,
    chunk_size: u32,
    version: u8,
    doc_range_aligned: bool,
    block_max: ?BlockMaxInfo = null,
    chunk_meta_data: []const u8,
    chunk_meta_count: u32,
    payload_data: []const u8,
    norms_data: []const u8,
    positions_data: ?[]const u8 = null,
    skip_data: ?[]const u8 = null,
    impact_chunk_ids_data: ?[]const u8 = null,
    impact_chunk_count: u32 = 0,
    inline_single_doc: bool = false,
    inline_doc_id: u32 = 0,
    inline_freq: u32 = 0,
    inline_has_locs: bool = false,
    inline_position_bits: u8 = 0,
    inline_positions_data: []const u8 = &.{},

    pub fn scoringChunkSize(self: *const TermPostings) u32 {
        return if (self.block_max) |block_max| block_max.chunk_size else self.chunk_size;
    }

    /// Decode document IDs into a roaring bitmap for callers that need set operations.
    pub fn docBitmap(self: *const TermPostings, alloc: Allocator) !roaring.RoaringBitmap {
        var bitmap = roaring.RoaringBitmap.init(alloc);
        errdefer bitmap.deinit();
        var iter = try self.iterator(alloc);
        defer iter.deinit();
        iter.decode_positions = false;
        while (try iter.next()) |hit| {
            try bitmap.add(hit.doc_id);
        }
        return bitmap;
    }

    /// Create a postings iterator that yields (doc_id, freq, norm, positions) tuples.
    pub fn iterator(self: *const TermPostings, alloc: Allocator) !PostingsIterator {
        if (self.inline_single_doc) return PostingsIterator.initInlineSingleDoc(self, alloc);
        var iter = PostingsIterator{
            .alloc = alloc,
            .doc_freq = self.doc_freq,
            .chunk_size = self.chunk_size,
            .chunk_meta_data = self.chunk_meta_data,
            .chunk_meta_count = self.chunk_meta_count,
            .payload_data = self.payload_data,
            .norms_data = self.norms_data,
            .version = self.version,
            .doc_range_aligned = self.doc_range_aligned,
            .positions_data = self.positions_data,
            .skip_data = self.skip_data,
            .impact_chunk_ids_data = self.impact_chunk_ids_data,
            .impact_chunk_count = self.impact_chunk_count,
        };
        errdefer iter.deinit();
        try iter.decodeImpactChunkIds();
        return iter;
    }
};

pub const PackedPositionView = struct {
    data: []const u8,
    start_index: usize,
    count: usize,
    bits: u8,

    pub fn cursor(self: PackedPositionView) !PackedPositionCursor {
        if (self.bits > 32) return error.InvalidData;
        const start_bit = std.math.mul(usize, self.start_index, self.bits) catch return error.InvalidData;
        const value_bits = std.math.mul(usize, self.count, self.bits) catch return error.InvalidData;
        const end_bit = std.math.add(usize, start_bit, value_bits) catch return error.InvalidData;
        if ((std.math.add(usize, end_bit, 7) catch return error.InvalidData) / 8 > self.data.len) return error.InvalidData;

        var result_cursor = PackedPositionCursor{
            .data = self.data,
            .remaining = self.count,
            .bits = self.bits,
            .byte_index = start_bit / 8,
        };
        const initial_skip: u3 = @intCast(start_bit % 8);
        if (self.bits != 0 and initial_skip != 0) {
            result_cursor.reservoir = @as(u64, self.data[result_cursor.byte_index]) >> initial_skip;
            result_cursor.reservoir_bits = 8 - @as(u8, initial_skip);
            result_cursor.byte_index += 1;
        }
        return result_cursor;
    }
};

pub const PackedPositionCursor = struct {
    data: []const u8,
    remaining: usize,
    bits: u8,
    byte_index: usize,
    reservoir: u64 = 0,
    reservoir_bits: u8 = 0,
    previous: u32 = 0,

    pub inline fn next(self: *PackedPositionCursor) !?u32 {
        if (self.remaining == 0) return null;
        var delta: u32 = 0;
        if (self.bits != 0) {
            while (self.reservoir_bits < self.bits) {
                if (self.byte_index >= self.data.len) return error.InvalidData;
                self.reservoir |= @as(u64, self.data[self.byte_index]) << @intCast(self.reservoir_bits);
                self.reservoir_bits += 8;
                self.byte_index += 1;
            }
            const mask: u64 = if (self.bits == 32) std.math.maxInt(u32) else (@as(u64, 1) << @intCast(self.bits)) - 1;
            delta = @intCast(self.reservoir & mask);
            self.reservoir >>= @intCast(self.bits);
            self.reservoir_bits -= self.bits;
        }
        self.previous +%= delta;
        self.remaining -= 1;
        return self.previous;
    }
};

/// Iterates over (doc_id, freq, norm, positions) for a term's posting list.
pub const PostingsIterator = struct {
    alloc: Allocator,
    doc_freq: u32 = 0,
    chunk_size: u32 = 0,
    chunk_meta_data: []const u8 = &.{},
    chunk_meta_count: u32 = 0,
    payload_data: []const u8 = &.{},
    norms_data: []const u8 = &.{},
    current_chunk_index: usize = std.math.maxInt(usize),
    current_chunk_meta: ?V7ChunkMeta = null,
    current_chunk_min_doc: u32 = 0,
    next_chunk_index: usize = 0,
    chunk_doc_pos: usize = 0,
    version: u8 = wire_version_current,
    doc_range_aligned: bool = false,
    positions_data: ?[]const u8 = null,
    skip_data: ?[]const u8 = null,
    impact_chunk_ids_data: ?[]const u8 = null,
    impact_chunk_count: u32 = 0,
    impact_chunk_ids: std.ArrayListUnmanaged(u32) = .empty,
    current_impact_ordinal: usize = 0,
    current_impact_valid: bool = false,
    last_returned_doc: u32 = 0,
    positions_cursor: usize = 0,
    positions_chunk_end: usize = 0,
    positions_chunk_index: usize = std.math.maxInt(usize),
    positions_group_doc_end: usize = 0,
    positions_group_bits: u8 = 0,
    positions_group_data_start: usize = 0,
    positions_group_value_offset: usize = 0,
    positions_group_value_count: usize = 0,
    doc_values: std.ArrayListUnmanaged(u32) = .empty,
    freq_values: std.ArrayListUnmanaged(u32) = .empty,
    chunk_metas: std.ArrayListUnmanaged(V7ChunkMeta) = .empty,
    chunk_meta_values: std.ArrayListUnmanaged(u32) = .empty,
    chunk_meta_decoded: bool = false,
    /// Reusable buffer for decoded positions.
    positions_buf: std.ArrayListUnmanaged(u32) = .empty,
    /// When false, `next()` skips position decoding entirely — both the
    /// varint walk and the buffer fill. The returned `Hit.positions` slice
    /// is always empty in that mode. Set by callers that only need
    /// (doc_id, freq, norm) for BM25 scoring (e.g., the WAND scorer);
    /// avoids the per-doc varint cost on positions-bearing posting lists.
    decode_positions: bool = true,
    // 1-hit fields
    is_one_hit: bool = false,
    one_hit_consumed: bool = false,
    one_hit_doc: u32 = 0,
    one_hit_norm: u32 = 0,
    one_hit_freq: u32 = 1,
    one_hit_has_locs: bool = false,
    one_hit_position_bits: u8 = 0,
    one_hit_positions_data: []const u8 = &.{},
    one_hit_owns_scratch: bool = false,
    /// A phrase approximation has selected the current document but has not
    /// yet decoded or skipped its position record. While set, chunk_doc_pos
    /// still points at that document and positions_cursor points at its record.
    deferred_position_pending: bool = false,
    position_records_decoded: u64 = 0,

    pub const Hit = struct {
        doc_id: u32,
        freq: u32,
        norm: u32,
        /// Positions of this term in the document. Valid until next call to next().
        /// Empty if positions not stored.
        positions: []const u32 = &.{},
    };

    fn initOneHit(h: LookupResult.OneHit) PostingsIterator {
        return .{
            .alloc = undefined,
            .is_one_hit = true,
            .one_hit_doc = h.doc_num,
            .one_hit_norm = h.norm_bits,
        };
    }

    fn initInlineSingleDoc(postings: *const TermPostings, alloc: Allocator) PostingsIterator {
        return .{
            .alloc = alloc,
            .norms_data = postings.norms_data,
            .version = postings.version,
            .is_one_hit = true,
            .one_hit_doc = postings.inline_doc_id,
            .one_hit_norm = decodeNormValue(postings.norms_data, postings.inline_doc_id),
            .one_hit_freq = postings.inline_freq,
            .one_hit_has_locs = postings.inline_has_locs,
            .one_hit_position_bits = postings.inline_position_bits,
            .one_hit_positions_data = postings.inline_positions_data,
            .one_hit_owns_scratch = true,
        };
    }

    fn takeOneHit(self: *PostingsIterator, with_positions: bool) !?Hit {
        if (self.one_hit_consumed) return null;
        self.one_hit_consumed = true;
        self.positions_buf.clearRetainingCapacity();
        if (with_positions and self.one_hit_has_locs) {
            const count: usize = @intCast(self.one_hit_freq);
            try self.positions_buf.ensureTotalCapacity(self.alloc, count);
            self.positions_buf.items.len = count;
            try decodePackedU32Into(
                self.one_hit_positions_data,
                self.positions_buf.items,
                self.one_hit_position_bits,
            );
            var previous: u32 = 0;
            for (self.positions_buf.items) |*delta| {
                previous +%= delta.*;
                delta.* = previous;
            }
        }
        return .{
            .doc_id = self.one_hit_doc,
            .freq = self.one_hit_freq,
            .norm = self.one_hit_norm,
            .positions = self.positions_buf.items,
        };
    }

    fn chunkCount(self: *const PostingsIterator) usize {
        return self.chunk_meta_count;
    }

    fn decodeImpactChunkIds(self: *PostingsIterator) !void {
        if (!usesSeparateImpactRanges(self.version) or self.impact_chunk_count == 0) return;
        const data = self.impact_chunk_ids_data orelse return error.InvalidData;
        if (data.len == 0) return error.InvalidData;
        const count: usize = self.impact_chunk_count;

        try self.impact_chunk_ids.ensureTotalCapacity(self.alloc, count);
        self.impact_chunk_ids.items.len = count;

        if (data[0] <= 32) {
            const bits = data[0];
            const packed_len = packedU32ByteLen(count, bits);
            if (data.len != packed_len + 1) return error.InvalidData;
            try decodePackedU32Into(data[1..], self.impact_chunk_ids.items, bits);
            var chunk_id: u32 = 0;
            for (self.impact_chunk_ids.items) |*delta| {
                chunk_id +|= delta.*;
                delta.* = chunk_id;
            }
            return;
        }

        var cursor: usize = 1;
        if (data[0] == impact_ids_varint_encoding) {
            var chunk_id: u32 = 0;
            for (self.impact_chunk_ids.items) |*value| {
                chunk_id +|= readVarintU32(data, &cursor) catch return error.InvalidData;
                value.* = chunk_id;
            }
            if (cursor != data.len) return error.InvalidData;
            return;
        }
        if (data[0] == impact_ids_run_encoding) {
            const run_count = readVarintU32(data, &cursor) catch return error.InvalidData;
            var output_idx: usize = 0;
            var previous_end: u32 = 0;
            for (0..run_count) |run_idx| {
                const start_delta = readVarintU32(data, &cursor) catch return error.InvalidData;
                const run_len = readVarintU32(data, &cursor) catch return error.InvalidData;
                if (run_len == 0 or run_len > count -| output_idx) return error.InvalidData;
                const start = if (run_idx == 0) start_delta else previous_end +| 1 +| start_delta;
                for (0..run_len) |offset| {
                    self.impact_chunk_ids.items[output_idx] = start +| @as(u32, @intCast(offset));
                    output_idx += 1;
                }
                previous_end = self.impact_chunk_ids.items[output_idx - 1];
            }
            if (output_idx != count or cursor != data.len) return error.InvalidData;
            return;
        }
        return error.InvalidData;
    }

    inline fn noteReturnedDoc(self: *PostingsIterator, doc_id: u32) void {
        if (!usesSeparateImpactRanges(self.version) or self.impact_chunk_ids.items.len == 0) return;
        const wanted_chunk = doc_id / impact_range_doc_count;
        var ordinal = if (self.current_impact_valid) self.current_impact_ordinal else 0;
        while (ordinal + 1 < self.impact_chunk_ids.items.len and self.impact_chunk_ids.items[ordinal] < wanted_chunk) ordinal += 1;
        if (self.impact_chunk_ids.items[ordinal] == wanted_chunk) {
            self.current_impact_ordinal = ordinal;
            self.current_impact_valid = true;
            self.last_returned_doc = doc_id;
        }
    }

    fn ensureChunkMetaDecoded(self: *PostingsIterator) !void {
        if (self.chunk_meta_decoded) return;
        const count: usize = self.chunk_meta_count;
        self.chunk_metas.clearRetainingCapacity();
        self.chunk_meta_values.clearRetainingCapacity();
        try self.chunk_metas.ensureTotalCapacity(self.alloc, count);
        self.chunk_metas.items.len = count;
        if (count == 0) {
            self.chunk_meta_decoded = true;
            return;
        }

        const layout = try compactChunkMetaLayout(self.chunk_meta_data, count, self.version);
        const compact_posting_count = usesCompactPostingCountMeta(self.version);
        const value_columns: usize = if (compact_posting_count) 2 else 4;
        try self.chunk_meta_values.ensureTotalCapacity(self.alloc, count * value_columns);
        self.chunk_meta_values.items.len = count * value_columns;
        const empty_values = self.chunk_meta_values.items[0..0];
        const chunk_deltas = if (compact_posting_count) empty_values else self.chunk_meta_values.items[0..count];
        const max_doc_start: usize = if (compact_posting_count) 0 else count;
        const max_doc_offsets = self.chunk_meta_values.items[max_doc_start..][0..count];
        const doc_counts = if (compact_posting_count) empty_values else self.chunk_meta_values.items[count * 2 ..][0..count];
        const payload_start: usize = if (compact_posting_count) count else count * 3;
        const payload_deltas = self.chunk_meta_values.items[payload_start..][0..count];

        if (!compact_posting_count) try decodePackedU32Into(self.chunk_meta_data[layout.chunk_delta_off..][0..layout.chunk_delta_len], chunk_deltas, layout.chunk_delta_bits);
        try decodePackedU32Into(self.chunk_meta_data[layout.max_doc_offset_off..][0..layout.max_doc_offset_len], max_doc_offsets, layout.max_doc_offset_bits);
        if (!compact_posting_count) try decodePackedU32Into(self.chunk_meta_data[layout.doc_count_off..][0..layout.doc_count_len], doc_counts, layout.doc_count_bits);
        try decodePackedU32Into(self.chunk_meta_data[layout.payload_delta_off..][0..layout.payload_delta_len], payload_deltas, layout.payload_delta_bits);

        var chunk_id: u32 = 0;
        var payload_end: u32 = 0;
        for (0..count) |i| {
            if (compact_posting_count) {
                chunk_id = @intCast(i);
            } else {
                chunk_id +%= chunk_deltas[i];
            }
            const prev_payload_end = payload_end;
            payload_end +%= payload_deltas[i];
            const max_doc = if (usesPostingCountBlocks(self.version)) max_doc_offsets[i] else chunk_id * self.chunk_size + max_doc_offsets[i];
            const doc_count = if (compact_posting_count)
                if (i + 1 < count) self.chunk_size else self.doc_freq - @as(u32, @intCast(i)) * self.chunk_size
            else
                doc_counts[i];
            self.chunk_metas.items[i] = .{
                .chunk_id = chunk_id,
                .max_doc = max_doc,
                .doc_count = doc_count,
                .doc_ctrl_off = prev_payload_end,
                .doc_ctrl_len = payload_end - prev_payload_end,
                .doc_data_off = 0,
                .doc_data_len = 0,
                .freq_ctrl_off = 0,
                .freq_ctrl_len = 0,
                .freq_data_off = 0,
                .freq_data_len = 0,
            };
        }
        self.chunk_meta_decoded = true;
    }

    fn chunkMeta(self: *PostingsIterator, index: usize) !V7ChunkMeta {
        if (self.version >= wire_version_checkpoints) {
            const checkpoint = self.chunkCheckpoint(index);
            return readCompactChunkMetaAtCheckpoint(
                self.chunk_meta_data,
                self.chunk_meta_count,
                self.version,
                self.chunk_size,
                self.doc_freq,
                index,
                checkpoint.chunk_index,
                checkpoint.previous_chunk_id,
                checkpoint.previous_payload_end,
            );
        }
        try self.ensureChunkMetaDecoded();
        if (index >= self.chunk_metas.items.len) return error.InvalidData;
        return self.chunk_metas.items[index];
    }

    const ChunkCheckpoint = struct {
        chunk_index: usize = 0,
        previous_chunk_id: u32 = 0,
        previous_payload_end: u32 = 0,
    };

    fn chunkCheckpoint(self: *const PostingsIterator, index: usize) ChunkCheckpoint {
        if (self.version < wire_version_checkpoints or index < postings_skip_stride_chunks) return .{};
        const record_index = index / postings_skip_stride_chunks - 1;
        if (record_index >= self.skipRecordCount()) return .{};
        const record = self.skipRecord(record_index);
        if (record.chunk_index > index) return .{};
        return .{
            .chunk_index = record.chunk_index,
            .previous_chunk_id = record.previous_chunk_id,
            .previous_payload_end = record.previous_payload_end,
        };
    }

    fn skipRecordSize(self: *const PostingsIterator) usize {
        return if (self.version >= wire_version_checkpoints) postings_skip_record_size_v24 else postings_skip_record_size_v23;
    }

    fn skipRecordCount(self: *const PostingsIterator) usize {
        const data = self.skip_data orelse return 0;
        return data.len / self.skipRecordSize();
    }

    fn skipRecord(self: *const PostingsIterator, index: usize) struct {
        max_doc: u32,
        chunk_index: usize,
        previous_chunk_id: u32,
        previous_payload_end: u32,
    } {
        const data = self.skip_data.?;
        const base = index * self.skipRecordSize();
        return .{
            .max_doc = std.mem.readInt(u32, data[base..][0..4], .little),
            .chunk_index = @intCast(std.mem.readInt(u32, data[base + 4 ..][0..4], .little)),
            .previous_chunk_id = if (self.version >= wire_version_checkpoints) std.mem.readInt(u32, data[base + 8 ..][0..4], .little) else 0,
            .previous_payload_end = if (self.version >= wire_version_checkpoints) std.mem.readInt(u32, data[base + 12 ..][0..4], .little) else 0,
        };
    }

    fn skipWindowForTarget(self: *PostingsIterator, target: u32) !struct { lo: usize, hi: usize } {
        const count = self.skipRecordCount();
        if (count == 0) return .{ .lo = self.next_chunk_index, .hi = self.chunkCount() };

        var lo_record: usize = 0;
        var hi_record: usize = count;
        while (lo_record < hi_record) {
            const mid = lo_record + (hi_record - lo_record) / 2;
            const record = self.skipRecord(mid);
            if (record.max_doc < target) {
                lo_record = mid + 1;
            } else {
                hi_record = mid;
            }
        }

        const start_record = if (lo_record == 0) null else lo_record - 1;
        const start = if (start_record) |idx| self.skipRecord(idx).chunk_index else self.next_chunk_index;
        const end = if (lo_record < count) self.skipRecord(lo_record).chunk_index else self.chunkCount();
        return .{
            .lo = @max(self.next_chunk_index, start),
            .hi = @min(end, self.chunkCount()),
        };
    }

    fn nextChunkIndexForTarget(self: *PostingsIterator, target: u32) !usize {
        const window = try self.skipWindowForTarget(target);
        var lo = window.lo;
        var hi = window.hi;
        if (lo >= hi) return lo;
        if (self.skipRecordCount() > 0) {
            while (lo < hi) : (lo += 1) {
                const meta = try self.chunkMeta(lo);
                if (meta.max_doc >= target) return lo;
            }
            return hi;
        }
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const meta = try self.chunkMeta(mid);
            if (meta.max_doc < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    fn loadChunk(self: *PostingsIterator, index: usize) !void {
        const meta = if (self.version >= wire_version_checkpoints and
            self.current_chunk_meta != null and
            self.current_chunk_index != std.math.maxInt(usize) and
            index == self.current_chunk_index + 1)
            try readCompactChunkMetaAtCheckpoint(
                self.chunk_meta_data,
                self.chunk_meta_count,
                self.version,
                self.chunk_size,
                self.doc_freq,
                index,
                index,
                self.current_chunk_meta.?.chunk_id,
                self.current_chunk_meta.?.doc_ctrl_off + self.current_chunk_meta.?.doc_ctrl_len,
            )
        else
            try self.chunkMeta(index);
        self.doc_values.clearRetainingCapacity();
        try self.doc_values.ensureTotalCapacity(self.alloc, meta.doc_count);
        self.doc_values.items.len = meta.doc_count;

        self.freq_values.clearRetainingCapacity();
        try self.freq_values.ensureTotalCapacity(self.alloc, meta.doc_count);
        self.freq_values.items.len = meta.doc_count;

        if (@as(usize, meta.doc_ctrl_off) + meta.doc_ctrl_len > self.payload_data.len) return error.InvalidData;
        const chunk_data = self.payload_data[meta.doc_ctrl_off..][0..meta.doc_ctrl_len];
        var payload_cursor: usize = 0;
        const first_doc = if (usesPostingCountBlocks(self.version))
            readVarintU32(chunk_data, &payload_cursor) catch return error.InvalidData
        else
            null;
        if (chunk_data.len - payload_cursor < 2) return error.InvalidData;
        const doc_control = chunk_data[payload_cursor];
        const freq_control = chunk_data[payload_cursor + 1];
        payload_cursor += 2;
        const constant_frequency = if (usesConstantBlockFrequency(self.version) and freq_control & constant_frequency_marker != 0)
            freq_control & constant_frequency_mask
        else
            null;
        const vertical_docs = usesVerticalBp128(self.version) and doc_control & vertical_bp128_marker != 0;
        const vertical_frequencies = usesVerticalBp128(self.version) and constant_frequency == null and freq_control & vertical_bp128_marker != 0;
        const doc_bits = if (vertical_docs) doc_control & packed_width_mask else doc_control;
        const freq_bits = if (constant_frequency != null) 0 else if (vertical_frequencies) freq_control & packed_width_mask else freq_control;
        if (doc_bits > 32 or freq_bits > 32) return error.InvalidData;

        const count: usize = meta.doc_count;
        const packed_doc_count = if (first_doc != null) count -| 1 else count;
        if ((vertical_docs or vertical_frequencies) and count != simd_bitpack.block_values) return error.InvalidData;
        const doc_len = if (vertical_docs) simd_bitpack.encodedLen(doc_bits) catch return error.InvalidData else packedU32ByteLen(packed_doc_count, doc_bits);
        const freq_len = if (vertical_frequencies) simd_bitpack.encodedLen(freq_bits) catch return error.InvalidData else packedU32ByteLen(count, freq_bits);
        const expected_len = payload_cursor + doc_len + freq_len;
        if (chunk_data.len < expected_len) return error.InvalidData;

        var pos = payload_cursor;
        if (first_doc) |doc_id| {
            if (count == 0) return error.InvalidData;
            if (vertical_docs) {
                const block: *[simd_bitpack.block_values]u32 = self.doc_values.items[0..simd_bitpack.block_values];
                _ = simd_bitpack.decodeBlockPrefixSum(chunk_data[pos..][0..doc_len], block, doc_bits, doc_id) catch return error.InvalidData;
            } else {
                try decodePackedU32Into(chunk_data[pos..][0..doc_len], self.doc_values.items[1..], doc_bits);
                self.doc_values.items[0] = doc_id;
            }
        } else {
            try decodePackedU32Into(chunk_data[pos..][0..doc_len], self.doc_values.items, doc_bits);
        }
        pos += doc_len;
        if (constant_frequency) |value| {
            @memset(self.freq_values.items, value);
        } else if (vertical_frequencies) {
            const block: *[simd_bitpack.block_values]u32 = self.freq_values.items[0..simd_bitpack.block_values];
            _ = simd_bitpack.decodeBlock(chunk_data[pos..][0..freq_len], block, freq_bits) catch return error.InvalidData;
        } else {
            try decodePackedU32Into(chunk_data[pos..][0..freq_len], self.freq_values.items, freq_bits);
        }

        if (self.doc_values.items.len > 0 and !vertical_docs) {
            if (!usesPostingCountBlocks(self.version)) self.doc_values.items[0] +%= meta.chunk_id * self.chunk_size;
            for (1..self.doc_values.items.len) |i| {
                self.doc_values.items[i] +%= self.doc_values.items[i - 1];
            }
        }

        self.current_chunk_index = index;
        self.current_chunk_meta = meta;
        self.current_chunk_min_doc = if (self.doc_values.items.len > 0) self.doc_values.items[0] else 0;
        self.next_chunk_index = index + 1;
        self.chunk_doc_pos = 0;
    }

    fn enterPositionChunk(self: *PostingsIterator, chunk_index: usize) !void {
        if (self.version < wire_version_chunk_framed_positions or self.positions_data == null) return;
        if (self.positions_chunk_index == chunk_index) return;
        const pd = self.positions_data.?;
        if (self.positions_cursor >= pd.len) return error.InvalidData;
        const chunk_len = readVarintU32(pd, &self.positions_cursor) catch return error.InvalidData;
        const chunk_end = self.positions_cursor + @as(usize, chunk_len);
        if (chunk_end > pd.len) return error.InvalidData;
        self.positions_chunk_end = chunk_end;
        self.positions_chunk_index = chunk_index;
        self.positions_group_doc_end = 0;
        self.positions_group_bits = 0;
        self.positions_group_data_start = 0;
        self.positions_group_value_offset = 0;
        self.positions_group_value_count = 0;
    }

    fn skipPositionChunk(self: *PostingsIterator) !void {
        const pd = self.positions_data orelse return;
        if (self.version < wire_version_chunk_framed_positions) return error.InvalidData;
        if (self.positions_cursor >= pd.len) return error.InvalidData;
        const chunk_len = readVarintU32(pd, &self.positions_cursor) catch return error.InvalidData;
        if (self.positions_cursor + @as(usize, chunk_len) > pd.len) return error.InvalidData;
        self.positions_cursor += @as(usize, chunk_len);
        self.positions_chunk_index = std.math.maxInt(usize);
        self.positions_chunk_end = self.positions_cursor;
        self.positions_group_doc_end = 0;
        self.positions_group_bits = 0;
        self.positions_group_data_start = 0;
        self.positions_group_value_offset = 0;
        self.positions_group_value_count = 0;
    }

    fn ensurePositionGroup(self: *PostingsIterator, doc_pos: usize) !u8 {
        if (!usesGroupedPositions(self.version)) return error.InvalidData;
        if (doc_pos < self.positions_group_doc_end) return self.positions_group_bits;
        if (doc_pos != self.positions_group_doc_end) return error.InvalidData;
        if (self.positions_cursor >= self.positions_chunk_end) return error.InvalidData;
        const bits = self.positions_data.?[self.positions_cursor];
        self.positions_cursor += 1;
        if (bits > 32) return error.InvalidData;
        self.positions_group_bits = bits;
        self.positions_group_doc_end = @min(self.freq_values.items.len, doc_pos + position_doc_group_size);
        self.positions_group_data_start = self.positions_cursor;
        self.positions_group_value_offset = 0;
        self.positions_group_value_count = 0;
        for (self.freq_values.items[doc_pos..self.positions_group_doc_end]) |freq_has_locs| {
            const decoded = decodeFreqHasLocs(freq_has_locs);
            if (decoded.has_locs) self.positions_group_value_count +|= @intCast(decoded.freq);
        }
        const packed_len = packedU32ByteLen(self.positions_group_value_count, bits);
        if (self.positions_group_data_start + packed_len > self.positions_chunk_end) return error.InvalidData;
        return bits;
    }

    fn advanceContiguousPositionRecord(self: *PostingsIterator, doc_pos: usize, value_count: usize) !void {
        if (!usesContiguousPositionGroups(self.version)) return error.InvalidData;
        if (self.positions_group_value_offset + value_count > self.positions_group_value_count) return error.InvalidData;
        self.positions_group_value_offset += value_count;
        if (doc_pos + 1 == self.positions_group_doc_end) {
            if (self.positions_group_value_offset != self.positions_group_value_count) return error.InvalidData;
            self.positions_cursor = self.positions_group_data_start + packedU32ByteLen(self.positions_group_value_count, self.positions_group_bits);
        }
    }

    fn skipPositionRecord(self: *PostingsIterator) !void {
        const pd = self.positions_data orelse return;
        if (self.version >= wire_version_chunk_framed_positions) {
            const decoded = decodeFreqHasLocs(self.freq_values.items[self.chunk_doc_pos]);
            const bits = if (usesGroupedPositions(self.version))
                try self.ensurePositionGroup(self.chunk_doc_pos)
            else blk: {
                if (!decoded.has_locs) return;
                if (self.positions_cursor >= self.positions_chunk_end) return error.InvalidData;
                const value = pd[self.positions_cursor];
                self.positions_cursor += 1;
                break :blk value;
            };
            if (usesContiguousPositionGroups(self.version)) {
                const count: usize = if (decoded.has_locs) @intCast(decoded.freq) else 0;
                try self.advanceContiguousPositionRecord(self.chunk_doc_pos, count);
                return;
            }
            if (!decoded.has_locs) return;
            if (bits > 32) return error.InvalidData;
            const packed_len = packedU32ByteLen(@intCast(decoded.freq), bits);
            if (self.positions_cursor + packed_len > self.positions_chunk_end) return error.InvalidData;
            self.positions_cursor += packed_len;
            return;
        }
        if (self.positions_cursor >= pd.len) return error.InvalidData;
        const num_pos = readVarintU32(pd, &self.positions_cursor) catch return error.InvalidData;
        if (num_pos == 0) return;
        if (self.positions_cursor >= pd.len) return error.InvalidData;
        const bits = pd[self.positions_cursor];
        self.positions_cursor += 1;
        if (bits > 32) return error.InvalidData;
        const packed_len = packedU32ByteLen(@intCast(num_pos), bits);
        if (self.positions_cursor + packed_len > pd.len) return error.InvalidData;
        self.positions_cursor += packed_len;
    }

    fn decodePositionRecord(self: *PostingsIterator, doc_pos: usize, expected_count: u32, has_locs: bool) !void {
        self.positions_buf.clearRetainingCapacity();
        const pd = self.positions_data orelse return;
        self.position_records_decoded +|= 1;
        const num_pos = if (self.version >= wire_version_chunk_framed_positions) blk: {
            if (!has_locs) {
                if (usesGroupedPositions(self.version)) {
                    _ = try self.ensurePositionGroup(doc_pos);
                    if (usesContiguousPositionGroups(self.version)) try self.advanceContiguousPositionRecord(doc_pos, 0);
                }
                return;
            }
            break :blk expected_count;
        } else blk: {
            if (self.positions_cursor >= pd.len) return error.InvalidData;
            break :blk readVarintU32(pd, &self.positions_cursor) catch return error.InvalidData;
        };
        const positions_end = if (self.version >= wire_version_chunk_framed_positions) self.positions_chunk_end else pd.len;
        const bits = if (usesGroupedPositions(self.version))
            try self.ensurePositionGroup(doc_pos)
        else blk: {
            if (num_pos == 0) return;
            if (self.positions_cursor >= positions_end) return error.InvalidData;
            const value = pd[self.positions_cursor];
            self.positions_cursor += 1;
            break :blk value;
        };
        if (num_pos == 0) return;
        if (bits > 32) return error.InvalidData;
        const count: usize = @intCast(num_pos);
        if (usesContiguousPositionGroups(self.version)) {
            try self.positions_buf.ensureTotalCapacity(self.alloc, count);
            self.positions_buf.items.len = count;
            try decodePackedU32Range(
                pd[self.positions_group_data_start..self.positions_chunk_end],
                self.positions_group_value_offset,
                self.positions_buf.items,
                bits,
            );
            try self.advanceContiguousPositionRecord(doc_pos, count);
            var prev: u32 = 0;
            for (self.positions_buf.items) |*delta| {
                const position = prev +% delta.*;
                delta.* = position;
                prev = position;
            }
            return;
        }
        const packed_len = packedU32ByteLen(count, bits);
        if (self.positions_cursor + packed_len > positions_end) return error.InvalidData;
        try self.positions_buf.ensureTotalCapacity(self.alloc, count);
        self.positions_buf.items.len = count;
        try decodePackedU32Into(pd[self.positions_cursor..][0..packed_len], self.positions_buf.items, bits);
        self.positions_cursor += packed_len;
        var prev: u32 = 0;
        for (self.positions_buf.items) |*delta| {
            const position = prev +% delta.*;
            delta.* = position;
            prev = position;
        }
    }

    fn takeCurrentWithPositions(self: *PostingsIterator) !Hit {
        const doc_pos = self.chunk_doc_pos;
        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        const freq_has_locs_val = self.freq_values.items[self.chunk_doc_pos];
        const norm_val = decodeNormValue(self.norms_data, doc_id);
        self.chunk_doc_pos += 1;
        const decoded = decodeFreqHasLocs(freq_has_locs_val);
        try self.decodePositionRecord(doc_pos, @intCast(decoded.freq), decoded.has_locs);
        self.noteReturnedDoc(doc_id);
        return .{ .doc_id = doc_id, .freq = @intCast(decoded.freq), .norm = norm_val, .positions = self.positions_buf.items };
    }

    inline fn takeCurrentScoring(self: *PostingsIterator) Hit {
        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        const freq_has_locs_val = self.freq_values.items[self.chunk_doc_pos];
        const norm_val = decodeNormValue(self.norms_data, doc_id);
        self.chunk_doc_pos += 1;
        self.noteReturnedDoc(doc_id);
        return .{
            .doc_id = doc_id,
            .freq = @intCast(decodeFreqHasLocs(freq_has_locs_val).freq),
            .norm = norm_val,
        };
    }

    /// Position-free ranking iterator used by WAND and conjunction scorers.
    /// Keeping this separate from `next` removes the positional branch and
    /// avoids touching positional scratch for every scored posting.
    pub fn nextScoring(self: *PostingsIterator) !?Hit {
        if (self.is_one_hit) return try self.takeOneHit(false);

        if (self.current_chunk_index == std.math.maxInt(usize) or self.chunk_doc_pos >= self.doc_values.items.len) {
            if (self.next_chunk_index >= self.chunkCount()) return null;
            try self.loadChunk(self.next_chunk_index);
        }
        return self.takeCurrentScoring();
    }

    pub fn next(self: *PostingsIterator) !?Hit {
        if (!self.decode_positions) return self.nextScoring();
        if (self.is_one_hit) return try self.takeOneHit(true);

        if (self.current_chunk_index == std.math.maxInt(usize) or self.chunk_doc_pos >= self.doc_values.items.len) {
            if (self.next_chunk_index >= self.chunkCount()) return null;
            try self.loadChunk(self.next_chunk_index);
            if (self.decode_positions) try self.enterPositionChunk(self.current_chunk_index);
        }

        return try self.takeCurrentWithPositions();
    }

    /// Seek monotonically to `target` while preserving positional alignment.
    /// Skipped documents advance over packed position records without unpacking
    /// their deltas; only the selected candidate's positions are decoded.
    pub fn advanceToWithPositions(self: *PostingsIterator, target: u32) !?Hit {
        if (self.is_one_hit) {
            if (self.one_hit_consumed or self.one_hit_doc < target) {
                self.one_hit_consumed = true;
                return null;
            }
            return try self.takeOneHit(true);
        }

        if (self.current_chunk_index != std.math.maxInt(usize)) {
            while (self.chunk_doc_pos < self.doc_values.items.len) {
                if (self.doc_values.items[self.chunk_doc_pos] >= target) return try self.takeCurrentWithPositions();
                try self.skipPositionRecord();
                self.chunk_doc_pos += 1;
            }
        }

        const target_chunk_index = try self.nextChunkIndexForTarget(target);
        if (target_chunk_index >= self.chunkCount()) return null;
        var skipped_chunk = self.next_chunk_index;
        while (skipped_chunk < target_chunk_index) : (skipped_chunk += 1) {
            if (self.version >= wire_version_chunk_framed_positions) {
                try self.skipPositionChunk();
            } else {
                const meta = try self.chunkMeta(skipped_chunk);
                for (0..meta.doc_count) |_| try self.skipPositionRecord();
            }
        }
        try self.loadChunk(target_chunk_index);
        try self.enterPositionChunk(target_chunk_index);
        while (self.chunk_doc_pos < self.doc_values.items.len and self.doc_values.items[self.chunk_doc_pos] < target) {
            try self.skipPositionRecord();
            self.chunk_doc_pos += 1;
        }
        if (self.chunk_doc_pos >= self.doc_values.items.len) return try self.advanceToWithPositions(target);
        return try self.takeCurrentWithPositions();
    }

    /// Seek to a candidate document while preserving positional alignment but
    /// deferring position decode. Rejected approximation documents are skipped
    /// by advancing their framed/grouped position records; only a subsequent
    /// `decodeDeferredPositions` call unpacks the selected document's deltas.
    /// Calling this again with a target at or below the pending document returns
    /// the same candidate without consuming it.
    pub fn advanceToDeferredPositions(self: *PostingsIterator, target: u32) !?Hit {
        if (self.is_one_hit) {
            if (self.one_hit_consumed) return null;
            if (self.one_hit_doc < target) {
                self.one_hit_consumed = true;
                self.deferred_position_pending = false;
                return null;
            }
            self.deferred_position_pending = true;
            return .{
                .doc_id = self.one_hit_doc,
                .freq = self.one_hit_freq,
                .norm = self.one_hit_norm,
            };
        }

        if (self.deferred_position_pending) {
            if (self.chunk_doc_pos >= self.doc_values.items.len) return error.InvalidData;
            const pending_doc = self.doc_values.items[self.chunk_doc_pos];
            if (pending_doc >= target) return @as(?Hit, try self.currentDeferredHit());
            try self.skipPositionRecord();
            self.chunk_doc_pos += 1;
            self.deferred_position_pending = false;
        }

        if (self.current_chunk_index != std.math.maxInt(usize)) {
            while (self.chunk_doc_pos < self.doc_values.items.len) {
                if (self.doc_values.items[self.chunk_doc_pos] >= target) {
                    self.deferred_position_pending = true;
                    return @as(?Hit, try self.currentDeferredHit());
                }
                try self.skipPositionRecord();
                self.chunk_doc_pos += 1;
            }
        }

        const target_chunk_index = try self.nextChunkIndexForTarget(target);
        if (target_chunk_index >= self.chunkCount()) return null;
        var skipped_chunk = self.next_chunk_index;
        while (skipped_chunk < target_chunk_index) : (skipped_chunk += 1) try self.skipPositionChunk();
        try self.loadChunk(target_chunk_index);
        try self.enterPositionChunk(target_chunk_index);
        while (self.chunk_doc_pos < self.doc_values.items.len and self.doc_values.items[self.chunk_doc_pos] < target) {
            try self.skipPositionRecord();
            self.chunk_doc_pos += 1;
        }
        if (self.chunk_doc_pos >= self.doc_values.items.len) return try self.advanceToDeferredPositions(target);
        self.deferred_position_pending = true;
        return @as(?Hit, try self.currentDeferredHit());
    }

    fn currentDeferredHit(self: *PostingsIterator) !Hit {
        if (!self.deferred_position_pending or self.chunk_doc_pos >= self.doc_values.items.len) return error.InvalidData;
        const doc_id = self.doc_values.items[self.chunk_doc_pos];
        const decoded = decodeFreqHasLocs(self.freq_values.items[self.chunk_doc_pos]);
        return .{
            .doc_id = doc_id,
            .freq = @intCast(decoded.freq),
            .norm = decodeNormValue(self.norms_data, doc_id),
        };
    }

    /// Decode and consume the document selected by
    /// `advanceToDeferredPositions`.
    pub fn decodeDeferredPositions(self: *PostingsIterator) !Hit {
        if (!self.deferred_position_pending) return error.InvalidData;
        self.deferred_position_pending = false;
        if (self.is_one_hit) return (try self.takeOneHit(true)) orelse error.InvalidData;
        return try self.takeCurrentWithPositions();
    }

    pub fn canTakeDeferredPackedPositions(self: *const PostingsIterator) bool {
        return !self.is_one_hit and self.positions_data != null and usesContiguousPositionGroups(self.version);
    }

    /// Consume a deferred v30+ positional record as a zero-copy packed view.
    /// The caller may stream its delta values after this iterator advances;
    /// the view references immutable segment bytes rather than iterator scratch.
    pub fn takeDeferredPackedPositions(self: *PostingsIterator) !PackedPositionView {
        if (!self.deferred_position_pending or !self.canTakeDeferredPackedPositions()) return error.InvalidData;
        if (self.chunk_doc_pos >= self.freq_values.items.len) return error.InvalidData;
        self.deferred_position_pending = false;
        const doc_pos = self.chunk_doc_pos;
        const doc_id = self.doc_values.items[doc_pos];
        const decoded = decodeFreqHasLocs(self.freq_values.items[doc_pos]);
        const bits = try self.ensurePositionGroup(doc_pos);
        const count: usize = if (decoded.has_locs) @intCast(decoded.freq) else 0;
        const view = PackedPositionView{
            .data = self.positions_data.?[self.positions_group_data_start..self.positions_chunk_end],
            .start_index = self.positions_group_value_offset,
            .count = count,
            .bits = bits,
        };
        try self.advanceContiguousPositionRecord(doc_pos, count);
        self.chunk_doc_pos += 1;
        self.position_records_decoded +|= 1;
        self.noteReturnedDoc(doc_id);
        return view;
    }

    pub fn decodedPositionRecords(self: *const PostingsIterator) u64 {
        return self.position_records_decoded;
    }

    /// Advance to the smallest doc_id >= `target` and return its (freq, norm).
    /// Returns null if no such doc exists.
    ///
    /// Hybrid strategy:
    ///   * **Same chunk**: just call `next()` in a loop. The chunked decoder
    ///     is already materialized and the per-step cost is a few ALU ops.
    ///     This avoids the per-call `RoaringBitmap.rank` overhead, which
    ///     dominates short jumps (the most common case in WAND when the
    ///     pivot moves by a handful of docs).
    ///   * **Cross-chunk**: chunk metadata stores each chunk's max doc, so we
    ///     skip whole compressed chunks, load the destination chunk once, then
    ///     scan the decoded doc deltas to the target.
    ///
    /// Positions are NOT decoded on this path. The returned `Hit.positions`
    /// slice is always empty here. This iterator must not be intermixed with
    /// `next()` in a way that requires positions to stay in sync. WAND
    /// scoring (the primary caller) doesn't read positions.
    pub fn advanceTo(self: *PostingsIterator, target: u32) !?Hit {
        if (self.is_one_hit) {
            if (self.one_hit_consumed or self.one_hit_doc < target) {
                self.one_hit_consumed = true;
                return null;
            }
            return try self.takeOneHit(false);
        }

        if (self.current_chunk_index != std.math.maxInt(usize)) {
            while (self.chunk_doc_pos < self.doc_values.items.len and self.doc_values.items[self.chunk_doc_pos] < target) {
                self.chunk_doc_pos += 1;
            }
            if (self.chunk_doc_pos < self.doc_values.items.len) return self.takeCurrentScoring();
        }

        const target_chunk_index = try self.nextChunkIndexForTarget(target);
        if (target_chunk_index >= self.chunkCount()) return null;
        try self.loadChunk(target_chunk_index);
        if (self.current_chunk_index == std.math.maxInt(usize) or self.current_chunk_index >= self.chunkCount()) return null;

        while (self.chunk_doc_pos < self.doc_values.items.len and self.doc_values.items[self.chunk_doc_pos] < target) {
            self.chunk_doc_pos += 1;
        }
        if (self.chunk_doc_pos >= self.doc_values.items.len) return try self.advanceTo(target);

        return self.takeCurrentScoring();
    }

    /// Return a chunk's conservative BM25 upper bound using the iterator's
    /// decoded chunk table. The raw compact metadata stores delta-coded chunk
    /// IDs, so random access through `BlockMaxInfo.maxImpact` must reconstruct
    /// preceding deltas. WAND already owns this iterator and decodes the table
    /// on its first chunk load; binary-searching that table keeps repeated
    /// block lookups O(log stored_chunks).
    pub fn blockMaxImpact(
        self: *PostingsIterator,
        block_max: BlockMaxInfo,
        chunk_idx: u32,
        doc_count: u32,
        doc_freq: u32,
        avg_dl: f32,
        config: BM25Config,
    ) !f32 {
        if (block_max.range_ids) {
            return block_max.maxImpact(chunk_idx, doc_count, doc_freq, avg_dl, config);
        }
        if (usesPostingCountBlocks(self.version)) {
            return block_max.maxImpactAtOrdinal(@intCast(chunk_idx), doc_count, doc_freq, avg_dl, config);
        }
        if (self.version >= wire_version_checkpoints) {
            const target_doc = @as(u64, chunk_idx) * @as(u64, self.chunk_size);
            if (target_doc > std.math.maxInt(u32)) return 0;
            const target: u32 = @intCast(target_doc);
            const checkpoint_count = self.skipRecordCount();
            var lo_record: usize = 0;
            var hi_record = checkpoint_count;
            while (lo_record < hi_record) {
                const mid = lo_record + (hi_record - lo_record) / 2;
                if (self.skipRecord(mid).max_doc < target) {
                    lo_record = mid + 1;
                } else {
                    hi_record = mid;
                }
            }
            const start = if (lo_record == 0) 0 else self.skipRecord(lo_record - 1).chunk_index;
            const end = if (lo_record < checkpoint_count) self.skipRecord(lo_record).chunk_index else self.chunkCount();
            var ordinal = start;
            while (ordinal < end) : (ordinal += 1) {
                const meta = try self.chunkMeta(ordinal);
                if (meta.chunk_id < chunk_idx) continue;
                if (meta.chunk_id != chunk_idx) return 0;
                return block_max.maxImpactAtOrdinal(ordinal, doc_count, doc_freq, avg_dl, config);
            }
            return 0;
        }
        try self.ensureChunkMetaDecoded();
        var lo: usize = 0;
        var hi = @min(self.chunk_metas.items.len, block_max.chunkCount());
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.chunk_metas.items[mid].chunk_id < chunk_idx) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo >= self.chunk_metas.items.len or self.chunk_metas.items[lo].chunk_id != chunk_idx) return 0;
        return block_max.maxImpactAtOrdinal(lo, doc_count, doc_freq, avg_dl, config);
    }

    /// Return the upper bound aligned with the iterator's currently loaded
    /// stored chunk. Block-max records and chunk metadata have identical
    /// ordinals, so WAND does not need to rediscover the ordinal from a
    /// delta-coded chunk ID on every advance.
    pub fn currentBlockMaxImpact(
        self: *const PostingsIterator,
        block_max: BlockMaxInfo,
        doc_count: u32,
        doc_freq: u32,
        avg_dl: f32,
        config: BM25Config,
    ) f32 {
        if (block_max.range_ids) {
            const cursor = self.currentBlockCursor() orelse return 0;
            return block_max.maxImpactAtOrdinal(cursor.ordinal, doc_count, doc_freq, avg_dl, config);
        }
        if (self.current_chunk_index == std.math.maxInt(usize)) return 0;
        return block_max.maxImpactAtOrdinal(self.current_chunk_index, doc_count, doc_freq, avg_dl, config);
    }

    pub fn currentBlockMaxImpactWithIdf(
        self: *const PostingsIterator,
        block_max: BlockMaxInfo,
        avg_dl: f32,
        idf: f32,
        config: BM25Config,
    ) f32 {
        if (block_max.range_ids) {
            const cursor = self.currentBlockCursor() orelse return 0;
            return block_max.maxImpactAtOrdinalWithIdf(cursor.ordinal, avg_dl, idf, config);
        }
        if (self.current_chunk_index == std.math.maxInt(usize)) return 0;
        return block_max.maxImpactAtOrdinalWithIdf(self.current_chunk_index, avg_dl, idf, config);
    }

    pub fn currentBlockMaxImpactWithScorer(
        self: *const PostingsIterator,
        block_max: BlockMaxInfo,
        scorer: BM25TermScorer,
        idf: f32,
        bound_table: ?*const BM25BoundTable,
    ) f32 {
        if (block_max.range_ids) {
            const cursor = self.currentBlockCursor() orelse return 0;
            return block_max.maxImpactAtOrdinalWithBoundTable(cursor.ordinal, scorer, idf, bound_table);
        }
        if (self.current_chunk_index == std.math.maxInt(usize)) return 0;
        return block_max.maxImpactAtOrdinalWithBoundTable(self.current_chunk_index, scorer, idf, bound_table);
    }

    pub const CompetitiveBlockAdvance = struct {
        hit: ?Hit,
        chunks_skipped: u32,
    };

    pub const BlockCursor = struct {
        ordinal: usize,
        chunk_id: u32,
        payload_end: u32,
        min_doc: u32,
        max_doc: u32,
    };

    pub fn currentBlockCursor(self: *const PostingsIterator) ?BlockCursor {
        if (self.impact_chunk_ids.items.len > 0) {
            if (!self.current_impact_valid or self.current_impact_ordinal >= self.impact_chunk_ids.items.len) return null;
            const lo = self.current_impact_ordinal;
            const wanted_chunk = self.impact_chunk_ids.items[lo];
            const min_doc = wanted_chunk * impact_range_doc_count;
            return .{
                .ordinal = lo,
                .chunk_id = wanted_chunk,
                .payload_end = 0,
                .min_doc = min_doc,
                .max_doc = min_doc +| (impact_range_doc_count - 1),
            };
        }
        const meta = self.current_chunk_meta orelse return null;
        return .{
            .ordinal = self.current_chunk_index,
            .chunk_id = meta.chunk_id,
            .payload_end = meta.doc_ctrl_off + meta.doc_ctrl_len,
            .min_doc = self.current_chunk_min_doc,
            .max_doc = meta.max_doc,
        };
    }

    pub fn advanceBlockCursor(self: *const PostingsIterator, cursor: *BlockCursor) !bool {
        const next_ordinal = cursor.ordinal + 1;
        if (self.impact_chunk_ids.items.len > 0) {
            if (next_ordinal >= self.impact_chunk_ids.items.len) return false;
            const chunk_id = self.impact_chunk_ids.items[next_ordinal];
            const min_doc = chunk_id * impact_range_doc_count;
            cursor.* = .{
                .ordinal = next_ordinal,
                .chunk_id = chunk_id,
                .payload_end = 0,
                .min_doc = min_doc,
                .max_doc = min_doc +| (impact_range_doc_count - 1),
            };
            return true;
        }
        if (next_ordinal >= self.chunkCount()) return false;
        const previous_max_doc = cursor.max_doc;
        const meta = try readCompactChunkMetaAtCheckpoint(
            self.chunk_meta_data,
            self.chunk_meta_count,
            self.version,
            self.chunk_size,
            self.doc_freq,
            next_ordinal,
            next_ordinal,
            cursor.chunk_id,
            cursor.payload_end,
        );
        cursor.* = .{
            .ordinal = next_ordinal,
            .chunk_id = meta.chunk_id,
            .payload_end = meta.doc_ctrl_off + meta.doc_ctrl_len,
            .min_doc = previous_max_doc +| 1,
            .max_doc = meta.max_doc,
        };
        return true;
    }

    pub fn blockCursorImpactWithIdf(
        _: *const PostingsIterator,
        block_max: BlockMaxInfo,
        cursor: BlockCursor,
        avg_dl: f32,
        idf: f32,
        config: BM25Config,
    ) f32 {
        return block_max.maxImpactAtOrdinalWithIdf(cursor.ordinal, avg_dl, idf, config);
    }

    pub fn blockCursorImpactWithScorer(
        _: *const PostingsIterator,
        block_max: BlockMaxInfo,
        cursor: BlockCursor,
        scorer: BM25TermScorer,
        idf: f32,
        bound_table: ?*const BM25BoundTable,
    ) f32 {
        return block_max.maxImpactAtOrdinalWithBoundTable(cursor.ordinal, scorer, idf, bound_table);
    }

    pub fn loadBlockCursor(self: *PostingsIterator, cursor: BlockCursor) !?Hit {
        if (self.impact_chunk_ids.items.len > 0) return self.advanceTo(cursor.min_doc);
        try self.loadChunk(cursor.ordinal);
        return try self.next();
    }

    /// Skip the remainder of the current chunk and scan only the aligned
    /// memory-mapped block-max records until a competitive future chunk is
    /// found. No rejected postings payload is decoded.
    pub fn advanceToCompetitiveBlock(
        self: *PostingsIterator,
        block_max: BlockMaxInfo,
        threshold: f32,
        avg_dl: f32,
        idf: f32,
        config: BM25Config,
        allow_equal_prune: bool,
    ) !CompetitiveBlockAdvance {
        return self.advanceToCompetitiveBlockWithScorer(
            block_max,
            threshold,
            BM25TermScorer.init(avg_dl, idf, config),
            idf,
            null,
            allow_equal_prune,
        );
    }

    pub fn advanceToCompetitiveBlockWithScorer(
        self: *PostingsIterator,
        block_max: BlockMaxInfo,
        threshold: f32,
        scorer: BM25TermScorer,
        idf: f32,
        bound_table: ?*const BM25BoundTable,
        allow_equal_prune: bool,
    ) !CompetitiveBlockAdvance {
        if (self.impact_chunk_ids.items.len > 0) {
            const current = self.currentBlockCursor() orelse return .{ .hit = null, .chunks_skipped = 0 };
            var ordinal = current.ordinal + 1;
            var skipped: u32 = 1;
            while (ordinal < block_max.chunkCount()) : (ordinal += 1) {
                const bound = block_max.maxImpactAtOrdinalWithBoundTable(ordinal, scorer, idf, bound_table);
                if (bound > threshold or (!allow_equal_prune and bound == threshold)) {
                    const target = self.impact_chunk_ids.items[ordinal] * impact_range_doc_count;
                    return .{ .hit = try self.advanceTo(target), .chunks_skipped = skipped };
                }
                skipped +|= 1;
            }
            self.next_chunk_index = self.chunkCount();
            self.chunk_doc_pos = self.doc_values.items.len;
            return .{ .hit = null, .chunks_skipped = skipped };
        }
        if (self.current_chunk_index == std.math.maxInt(usize)) return .{ .hit = null, .chunks_skipped = 0 };
        var ordinal = self.current_chunk_index + 1;
        var skipped: u32 = 1; // remainder of the current non-competitive chunk
        while (ordinal < block_max.chunkCount()) : (ordinal += 1) {
            const bound = block_max.maxImpactAtOrdinalWithBoundTable(ordinal, scorer, idf, bound_table);
            if (bound > threshold or (!allow_equal_prune and bound == threshold)) {
                try self.loadChunk(ordinal);
                return .{ .hit = try self.next(), .chunks_skipped = skipped };
            }
            skipped +|= 1;
        }
        self.next_chunk_index = self.chunkCount();
        self.chunk_doc_pos = self.doc_values.items.len;
        return .{ .hit = null, .chunks_skipped = skipped };
    }

    /// Discard the remainder of the loaded stored chunk and land on the first
    /// posting in the next stored chunk. Stored chunk ordinals are monotonic,
    /// so aligned front-block pruning does not need a target-doc search.
    pub fn advanceToNextStoredChunk(self: *PostingsIterator) !?Hit {
        if (self.impact_chunk_ids.items.len > 0) {
            const current = self.currentBlockCursor() orelse return try self.next();
            const next_ordinal = current.ordinal + 1;
            if (next_ordinal >= self.impact_chunk_ids.items.len) {
                self.next_chunk_index = self.chunkCount();
                self.chunk_doc_pos = self.doc_values.items.len;
                return null;
            }
            return self.advanceTo(self.impact_chunk_ids.items[next_ordinal] * impact_range_doc_count);
        }
        if (self.current_chunk_index == std.math.maxInt(usize)) return try self.next();
        const next_ordinal = self.current_chunk_index + 1;
        if (next_ordinal >= self.chunkCount()) {
            self.next_chunk_index = self.chunkCount();
            self.chunk_doc_pos = self.doc_values.items.len;
            return null;
        }
        try self.loadChunk(next_ordinal);
        return try self.next();
    }

    /// Heap retained solely for fully decoded compact chunk metadata. v24
    /// iterators should keep this at zero on normal next/advance/WAND paths;
    /// chunk payload decode buffers are intentionally excluded.
    pub fn decodedChunkMetadataHeapBytes(self: *const PostingsIterator) usize {
        return self.chunk_metas.capacity * @sizeOf(V7ChunkMeta) +
            self.chunk_meta_values.capacity * @sizeOf(u32) +
            self.impact_chunk_ids.capacity * @sizeOf(u32);
    }

    pub fn deinit(self: *PostingsIterator) void {
        if (self.is_one_hit) {
            if (self.one_hit_owns_scratch) self.positions_buf.deinit(self.alloc);
        } else {
            self.doc_values.deinit(self.alloc);
            self.freq_values.deinit(self.alloc);
            self.chunk_metas.deinit(self.alloc);
            self.chunk_meta_values.deinit(self.alloc);
            self.impact_chunk_ids.deinit(self.alloc);
            self.positions_buf.deinit(self.alloc);
        }
    }
};

// ============================================================================
// BM25 Scoring
// ============================================================================

pub const BM25Config = struct {
    k1: f32 = 1.2,
    b: f32 = 0.75,
};

/// Query-term BM25 constants shared by document scoring and conservative
/// block ceilings. Average field length, IDF, k1, and b are invariant for the
/// lifetime of one WAND term state; retaining their products avoids rebuilding
/// the same expression for every scored posting and every rejected block.
pub const BM25TermScorer = struct {
    numerator_scale: f32,
    norm_offset: f32,
    norm_length_scale: f32,

    pub fn init(avg_doc_len: f32, idf: f32, config: BM25Config) BM25TermScorer {
        return .{
            .numerator_scale = idf * (config.k1 + 1.0),
            .norm_offset = config.k1 * (1.0 - config.b),
            .norm_length_scale = config.k1 * config.b / avg_doc_len,
        };
    }

    pub inline fn score(self: BM25TermScorer, freq: u32, doc_len: u32) f32 {
        const f: f32 = @floatFromInt(freq);
        const dl: f32 = @floatFromInt(doc_len);
        return self.numerator_scale * f / (f + self.norm_offset + self.norm_length_scale * dl);
    }

    pub inline fn maxScore(self: BM25TermScorer) f32 {
        return self.numerator_scale;
    }
};

pub const bm25_bound_table_frequency_count: usize = 32;
pub const bm25_bound_table_norm_count: usize = 256;

/// IDF-independent BM25 TF ceilings for the current five-bit impact frequency
/// and one-byte norm domains. A snapshot may retain a bounded number of these
/// tables for distinct `(avg_field_length, k1, b)` configurations.
pub const BM25BoundTable = struct {
    values: [bm25_bound_table_frequency_count * bm25_bound_table_norm_count]f32,

    pub fn init(avg_doc_len: f32, config: BM25Config) BM25BoundTable {
        var table: BM25BoundTable = undefined;
        const scorer = BM25TermScorer.init(avg_doc_len, 1.0, config);
        for (0..bm25_bound_table_frequency_count) |freq_id| {
            const freq = impactMaxFreqFromPackedId(@intCast(freq_id));
            for (0..bm25_bound_table_norm_count) |norm_id| {
                const value = scorer.score(
                    freq,
                    fieldNormFromId(@intCast(norm_id)),
                );
                // Pre-bias the IDF-independent component upward so the query
                // hot path remains one indexed load and one multiply.
                table.values[freq_id * bm25_bound_table_norm_count + norm_id] =
                    std.math.nextAfter(f32, value * 1.000001, std.math.inf(f32));
            }
        }
        return table;
    }

    pub inline fn score(self: *const BM25BoundTable, packed_freq_id: u5, norm_id: u8, idf: f32) f32 {
        return idf * self.values[@as(usize, packed_freq_id) * bm25_bound_table_norm_count + norm_id];
    }
};

pub fn bm25Idf(doc_count: u32, doc_freq: u32) f32 {
    const n: f32 = @floatFromInt(doc_count);
    const df: f32 = @floatFromInt(doc_freq);
    return @log(1.0 + (n - df + 0.5) / (df + 0.5));
}

/// Frequency-independent upper bound for one BM25 term contribution. The TF
/// component approaches `k1 + 1` from below for every finite frequency.
pub fn bm25MaxScore(doc_count: u32, doc_freq: u32, config: BM25Config) f32 {
    return bm25Idf(doc_count, doc_freq) * (config.k1 + 1.0);
}

/// BM25 with a caller-supplied IDF sum. Phrase scorers use phrase occurrence
/// count as frequency and the sum of their constituent terms' IDFs.
pub fn bm25ScoreWithIdf(freq: u32, doc_len: u32, avg_doc_len: f32, idf_sum: f32, config: BM25Config) f32 {
    const f: f32 = @floatFromInt(freq);
    const dl: f32 = @floatFromInt(doc_len);
    const norm = config.k1 * (1.0 - config.b + config.b * dl / avg_doc_len);
    return idf_sum * (config.k1 + 1.0) * f / (f + norm);
}

/// Compute BM25 score for a single term-document pair.
pub fn bm25Score(
    freq: u32,
    doc_len: u32,
    doc_count: u32,
    doc_freq: u32,
    avg_doc_len: f32,
    config: BM25Config,
) f32 {
    return bm25ScoreWithIdf(freq, doc_len, avg_doc_len, bm25Idf(doc_count, doc_freq), config);
}

fn sumTermFrequenciesSimd(alloc: Allocator, freq_norm_data: []const u8) !u64 {
    var decoder = try chunked.ChunkedIntDecoder.init(alloc, freq_norm_data, 0);
    defer decoder.deinit();

    var total: u64 = 0;
    const freq_mask: @Vector(8, u32) = .{ 1, 0, 1, 0, 1, 0, 1, 0 };

    for (0..decoder.numChunks()) |chunk_idx| {
        try decoder.loadChunk(chunk_idx);

        while (decoder.remaining() >= 8) {
            const batch = decoder.readValues(8).?;
            const vals: @Vector(8, u32) = batch[0..8].*;
            const freqs = (vals >> @splat(@as(u5, 1))) * freq_mask;
            total += @reduce(.Add, @as(@Vector(8, u64), @intCast(freqs)));
        }

        while (decoder.remaining() >= 2) {
            const freq_has_locs = decoder.readValue().?;
            _ = decoder.readValue().?;
            total += decodeFreqHasLocs(freq_has_locs).freq;
        }
    }

    return total;
}

fn remapSingleContributorPostings(
    alloc: Allocator,
    postings: TermPostings,
    doc_offset: u32,
    merged_doc_count: u32,
    total_field_len: *u64,
) ![]u8 {
    var original_bitmap = try postings.docBitmap(alloc);
    defer original_bitmap.deinit();

    var shifted_bitmap = try original_bitmap.addOffset(doc_offset);
    defer shifted_bitmap.deinit();

    const bitmap_bytes = try shifted_bitmap.toBytes(alloc);
    defer alloc.free(bitmap_bytes);

    const num_chunks: u32 = if (merged_doc_count == 0) 0 else @intCast((merged_doc_count - 1) / postings.chunk_size + 1);
    total_field_len.* += try sumTermFrequenciesSimd(alloc, postings.freq_norm_data);

    const chunk_aligned = doc_offset % postings.chunk_size == 0;
    const freq_norm_bytes = if (chunk_aligned)
        try chunked.prependEmptyChunks(
            alloc,
            postings.freq_norm_data,
            @intCast(doc_offset / postings.chunk_size),
            num_chunks,
        )
    else
        try rebuildShiftedFreqNorm(
            alloc,
            postings,
            &original_bitmap,
            &shifted_bitmap,
            doc_offset,
            merged_doc_count,
        );
    defer alloc.free(freq_norm_bytes);

    const block_max_meta = if (chunk_aligned)
        try shiftBlockMaxWholeChunks(alloc, postings, @intCast(doc_offset / postings.chunk_size), num_chunks)
    else
        try rebuildShiftedBlockMax(alloc, postings, &original_bitmap, doc_offset, num_chunks);
    defer alloc.free(block_max_meta);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, postings.doc_freq))));
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(bitmap_bytes.len))))));
    try out.appendSlice(alloc, bitmap_bytes);
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(freq_norm_bytes.len))))));
    try out.appendSlice(alloc, freq_norm_bytes);
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, num_chunks))));
    try out.appendSlice(alloc, block_max_meta);

    const positions_len: u32 = if (postings.positions_data) |pd| @intCast(pd.len) else 0;
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, positions_len))));
    if (postings.positions_data) |pd| {
        try out.appendSlice(alloc, pd);
    }

    const owned = try alloc.dupe(u8, out.items);
    out.deinit(alloc);
    return owned;
}

fn rebuildShiftedFreqNorm(
    alloc: Allocator,
    postings: TermPostings,
    original_bitmap: *const roaring.RoaringBitmap,
    shifted_bitmap: *const roaring.RoaringBitmap,
    doc_offset: u32,
    merged_doc_count: u32,
) ![]u8 {
    _ = doc_offset;
    var encoder = try chunked.ChunkedIntEncoder.initWithMode(alloc, postings.chunk_size, merged_doc_count, .stream_vbyte);
    defer encoder.deinit();

    var decoder = try chunked.ChunkedIntDecoder.init(alloc, postings.freq_norm_data, 0);
    defer decoder.deinit();

    var orig_iter = original_bitmap.iterator();
    var shifted_iter = shifted_bitmap.iterator();
    var current_chunk: usize = std.math.maxInt(usize);

    while (orig_iter.next()) |orig_doc| {
        const shifted_doc = shifted_iter.next() orelse return error.InvalidData;
        const target_chunk = orig_doc / postings.chunk_size;
        if (target_chunk != current_chunk) {
            try decoder.loadChunk(target_chunk);
            current_chunk = target_chunk;
        }

        const freq_has_locs = decoder.readValue() orelse return error.InvalidData;
        const norm_val = decoder.readValue() orelse return error.InvalidData;
        try encoder.add(shifted_doc, &.{ freq_has_locs, norm_val });
    }

    try encoder.close();
    return encoder.toBytes();
}

fn shiftBlockMaxWholeChunks(
    alloc: Allocator,
    postings: TermPostings,
    chunk_delta: u32,
    num_chunks: u32,
) ![]u8 {
    const out = try alloc.alloc(u8, @as(usize, num_chunks) * 6);
    for (0..num_chunks) |chunk_idx| {
        const base = chunk_idx * 6;
        out[base..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, 0));
        out[base + 2 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, std.math.maxInt(u16)));
        out[base + 4 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, 0));
    }
    if (postings.block_max) |bm| {
        const dst_off = @as(usize, chunk_delta) * 6;
        @memcpy(out[dst_off..][0..bm.meta.len], bm.meta);
    }
    return out;
}

fn rebuildShiftedBlockMax(
    alloc: Allocator,
    postings: TermPostings,
    original_bitmap: *const roaring.RoaringBitmap,
    doc_offset: u32,
    num_chunks: u32,
) ![]u8 {
    const out = try alloc.alloc(u8, @as(usize, num_chunks) * 6);
    errdefer alloc.free(out);
    var chunk_max_freq = try alloc.alloc(u16, num_chunks);
    defer alloc.free(chunk_max_freq);
    var chunk_min_norm = try alloc.alloc(u16, num_chunks);
    defer alloc.free(chunk_min_norm);
    var chunk_max_norm = try alloc.alloc(u16, num_chunks);
    defer alloc.free(chunk_max_norm);
    @memset(chunk_max_freq, 0);
    @memset(chunk_min_norm, std.math.maxInt(u16));
    @memset(chunk_max_norm, 0);

    var decoder = try chunked.ChunkedIntDecoder.init(alloc, postings.freq_norm_data, 0);
    defer decoder.deinit();

    var orig_iter = original_bitmap.iterator();
    var current_chunk: usize = std.math.maxInt(usize);
    while (orig_iter.next()) |orig_doc| {
        const target_chunk = orig_doc / postings.chunk_size;
        if (target_chunk != current_chunk) {
            try decoder.loadChunk(target_chunk);
            current_chunk = target_chunk;
        }

        const freq_has_locs = decoder.readValue() orelse return error.InvalidData;
        const norm_val = decoder.readValue() orelse return error.InvalidData;
        const decoded = decodeFreqHasLocs(freq_has_locs);

        const shifted_doc = orig_doc + doc_offset;
        const chunk_idx = shifted_doc / postings.chunk_size;
        const freq_u16: u16 = if (decoded.freq > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(decoded.freq);
        const norm_u16: u16 = if (norm_val > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(norm_val);
        if (freq_u16 > chunk_max_freq[chunk_idx]) chunk_max_freq[chunk_idx] = freq_u16;
        if (norm_u16 < chunk_min_norm[chunk_idx]) chunk_min_norm[chunk_idx] = norm_u16;
        if (norm_u16 > chunk_max_norm[chunk_idx]) chunk_max_norm[chunk_idx] = norm_u16;
    }

    for (0..num_chunks) |chunk_idx| {
        const base = chunk_idx * 6;
        out[base..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, chunk_max_freq[chunk_idx]));
        out[base + 2 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, chunk_min_norm[chunk_idx]));
        out[base + 4 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, chunk_max_norm[chunk_idx]));
    }

    return out;
}

// ============================================================================
// 1-Hit Encoding (zapx-compatible)
// ============================================================================

/// Mask for the encoding type in FST values (bits 63-62).
pub const fst_val_encoding_mask: u64 = 0xc000000000000000;
/// General encoding: FST value is a postings offset.
pub const fst_val_encoding_general: u64 = 0x0000000000000000;
/// 1-Hit encoding: term appears in exactly 1 document, freq=1, no locs.
pub const fst_val_encoding_1hit: u64 = 0x8000000000000000;
/// 31-bit mask for docNum and normBits fields.
const mask_31_bits: u64 = 0x7fffffff;

/// Encode a 1-hit FST value: docNum (bits 30-0) + normBits (bits 61-31).
pub fn fstValEncode1Hit(doc_num: u64, norm_bits: u64) u64 {
    return fst_val_encoding_1hit |
        ((norm_bits & mask_31_bits) << 31) |
        (doc_num & mask_31_bits);
}

/// Decode a 1-hit FST value into (docNum, normBits).
pub fn fstValDecode1Hit(v: u64) struct { doc_num: u64, norm_bits: u64 } {
    return .{
        .doc_num = v & mask_31_bits,
        .norm_bits = (v >> 31) & mask_31_bits,
    };
}

/// Check if an FST value uses 1-hit encoding.
pub fn fstValIs1Hit(v: u64) bool {
    return (v & fst_val_encoding_mask) == fst_val_encoding_1hit;
}

// ============================================================================
// freqHasLocs Encoding (zapx-compatible)
// ============================================================================

/// Encode frequency and hasLocs flag into a single value.
/// Format: (freq << 1) | hasLocsBit
pub fn encodeFreqHasLocs(freq: u64, has_locs: bool) u64 {
    return (freq << 1) | @as(u64, @intFromBool(has_locs));
}

/// Decode a freqHasLocs value into (freq, hasLocs).
pub fn decodeFreqHasLocs(v: u64) struct { freq: u64, has_locs: bool } {
    return .{
        .freq = v >> 1,
        .has_locs = (v & 1) != 0,
    };
}

// ============================================================================
// Configuration
// ============================================================================

const PostingsLayout = enum {
    /// Branch-only v27 writer retained solely to construct compatibility and
    /// layout regression fixtures. Production readers reject its output.
    legacy_fixture_v27,
    /// v35: portable vertical BP128 payloads with selective document-range
    /// bounds and compact five-bit frequency ceilings.
    posting_count_v35,
};

pub const IndexConfig = struct {
    /// Documents per v27 range or postings per v28 block.
    chunk_size: u32 = 128,
    postings_layout: PostingsLayout = .posting_count_v35,
    /// Build a per-segment term bloom filter that lets readers reject absent
    /// terms before walking the FST. Defaults on for current segments and is
    /// auto-skipped when the term count falls below `bloom_min_terms`.
    enable_bloom: bool = true,
    /// Bloom filter sizing. 10 bits/key with 4 hashes → ~1% false-positive rate
    /// on the typical posting-list term distribution.
    bloom_bits_per_key: usize = 10,

    pub fn wireVersion(self: IndexConfig) u8 {
        return switch (self.postings_layout) {
            .legacy_fixture_v27 => wire_version_chunk_framed_positions,
            .posting_count_v35 => wire_version_current,
        };
    }

    pub fn postingsLayoutName(self: IndexConfig) []const u8 {
        return switch (self.postings_layout) {
            .legacy_fixture_v27 => "legacy_fixture_doc_range",
            .posting_count_v35 => "fixed_posting_count_sparse_impacts_contiguous_positions_inline_single_doc_two_column_meta_constant_frequency_five_bit_impact_frequency_vertical_bp128",
        };
    }
};

var benchmark_chunk_size_override: ?u32 = null;

/// Process-local engineering override used by the isolated search benchmark.
/// Production callers leave this unset. Keeping the override at the common
/// builder/merge configuration boundary ensures a sweep measures the real
/// production writer and merger rather than a benchmark-only codec.
pub fn setBenchmarkChunkSizeOverride(chunk_size: ?u32) void {
    benchmark_chunk_size_override = chunk_size;
}

pub fn productionIndexConfig() IndexConfig {
    var config = IndexConfig{};
    if (benchmark_chunk_size_override) |chunk_size| config.chunk_size = chunk_size;
    return config;
}

// ============================================================================
// Segment merger
// ============================================================================

/// Merge multiple inverted index sections into one.
/// Input: slice of serialized section bytes.
/// Output: merged section bytes. Caller owns result.
pub fn mergeInvertedSections(alloc: Allocator, sections: []const []const u8, config: IndexConfig) ![]u8 {
    return mergeInvertedSectionsWithDeletes(alloc, sections, null, config);
}

/// Merge with deleted document handling.
/// `deleted_docs`: optional per-segment roaring bitmaps of deleted doc IDs.
/// Deleted docs are skipped during merge and remaining docs are renumbered.
pub fn mergeInvertedSectionsWithDeletes(
    alloc: Allocator,
    sections: []const []const u8,
    deleted_docs: ?[]const ?roaring.RoaringBitmap,
    config: IndexConfig,
) ![]u8 {
    var section_slots = try alloc.alloc(?[]const u8, sections.len);
    defer alloc.free(section_slots);
    var doc_counts = try alloc.alloc(u32, sections.len);
    defer alloc.free(doc_counts);
    for (sections, 0..) |section, i| {
        section_slots[i] = section;
        const reader = try InvertedIndexReader.init(alloc, section);
        doc_counts[i] = reader.doc_count;
    }
    return mergeInvertedSectionSlotsWithDeletes(alloc, section_slots, doc_counts, deleted_docs, config);
}

fn mergedSectionCapacityHint(sections: []const ?[]const u8) usize {
    var total: usize = v7_header_size;
    for (sections) |section_opt| {
        if (section_opt) |section| total +|= section.len;
    }
    return total;
}

const MergeMemorySink = struct {
    alloc: Allocator,
    output: std.ArrayListUnmanaged(u8) = .empty,

    fn deinit(self: *MergeMemorySink) void {
        self.output.deinit(self.alloc);
    }

    fn len(self: *const MergeMemorySink) usize {
        return self.output.items.len;
    }

    fn appendSlice(self: *MergeMemorySink, bytes: []const u8) !void {
        try self.output.appendSlice(self.alloc, bytes);
    }

    fn writeAt(self: *MergeMemorySink, offset: usize, bytes: []const u8) !void {
        if (offset > self.output.items.len or bytes.len > self.output.items.len - offset) return error.InvalidData;
        @memcpy(self.output.items[offset..][0..bytes.len], bytes);
    }

    fn finishOwned(self: *MergeMemorySink) ![]u8 {
        return try self.output.toOwnedSlice(self.alloc);
    }
};

pub fn mergeInvertedSectionSlotsWithDeletes(
    alloc: Allocator,
    sections: []const ?[]const u8,
    doc_counts: []const u32,
    deleted_docs: ?[]const ?roaring.RoaringBitmap,
    config: IndexConfig,
) ![]u8 {
    var sink = MergeMemorySink{ .alloc = alloc };
    defer sink.deinit();
    try sink.output.ensureTotalCapacityPrecise(alloc, mergedSectionCapacityHint(sections));
    try writeMergedInvertedSectionSlotsWithDeletes(alloc, &sink, sections, doc_counts, deleted_docs, config);
    return try sink.finishOwned();
}

pub fn writeMergedInvertedSectionSlotsWithDeletes(
    alloc: Allocator,
    sink: anytype,
    sections: []const ?[]const u8,
    doc_counts: []const u32,
    deleted_docs: ?[]const ?roaring.RoaringBitmap,
    config: IndexConfig,
) !void {
    if (sections.len != doc_counts.len) return error.InvalidData;

    // Open readers for all present sections, preserving slot order so doc
    // offsets include segments that do not contain this field.
    var readers = try alloc.alloc(InvertedIndexReader, sections.len);
    defer alloc.free(readers);
    var reader_present = try alloc.alloc(bool, sections.len);
    defer alloc.free(reader_present);
    for (sections, 0..) |section_opt, i| {
        reader_present[i] = false;
        const section = section_opt orelse continue;
        const reader = try InvertedIndexReader.init(alloc, section);
        // The inverted section's doc_count reflects only documents that had
        // content for this field, which can be fewer than the segment's total
        // doc_count when documents have varying field structures.
        if (reader.doc_count > doc_counts[i]) return error.InvalidData;
        readers[i] = reader;
        reader_present[i] = true;
    }

    // Track document number remapping: each segment's doc IDs get offset.
    // Account for deleted docs when computing offsets.
    var doc_offsets = try alloc.alloc(u32, sections.len);
    defer alloc.free(doc_offsets);
    var running_offset: u32 = 0;
    for (doc_counts, 0..) |doc_count, i| {
        doc_offsets[i] = running_offset;
        var live_docs = doc_count;
        if (deleted_docs) |dels| {
            if (i < dels.len) {
                if (dels[i]) |del_bitmap| {
                    live_docs -|= @intCast(del_bitmap.cardinality());
                }
            }
        }
        running_offset += live_docs;
    }

    // Iterate all terms from all readers, merge postings
    // We need to renumber docs per-segment, skipping deleted ones.
    // Since multiple terms reference the same docs, we build a renumber
    // map per segment on first pass.
    var renumber_maps = try alloc.alloc(?[]u32, sections.len);
    defer {
        for (renumber_maps) |m| if (m) |map| alloc.free(map);
        alloc.free(renumber_maps);
    }
    @memset(renumber_maps, null);

    for (doc_counts, 0..) |doc_count, seg_idx| {
        // Build renumber map for this segment
        var rmap = try alloc.alloc(u32, doc_count);
        var new_id = doc_offsets[seg_idx];
        for (0..doc_count) |doc_id| {
            const is_deleted = if (deleted_docs) |dels| blk: {
                if (seg_idx < dels.len) {
                    if (dels[seg_idx]) |del_bitmap| {
                        break :blk del_bitmap.contains(@intCast(doc_id));
                    }
                }
                break :blk false;
            } else false;

            if (is_deleted) {
                rmap[doc_id] = std.math.maxInt(u32); // sentinel
            } else {
                rmap[doc_id] = new_id;
                new_id += 1;
            }
        }
        renumber_maps[seg_idx] = rmap;
    }

    var term_iters = try alloc.alloc(TermIterator, sections.len);
    defer {
        for (term_iters, 0..) |*iter, i| {
            if (reader_present[i]) iter.deinit();
        }
        alloc.free(term_iters);
    }

    var current_entries = try alloc.alloc(?TermIterator.Entry, sections.len);
    defer alloc.free(current_entries);
    @memset(current_entries, null);

    for (readers, 0..) |*reader, seg_idx| {
        if (!reader_present[seg_idx]) continue;
        term_iters[seg_idx] = try reader.termIterator();
        current_entries[seg_idx] = try nextTermIteratorEntry(term_iters, seg_idx);
    }

    const section_start = sink.len();
    var header_placeholder: [v7_header_size]u8 = undefined;
    @memset(&header_placeholder, 0);
    try sink.appendSlice(&header_placeholder);
    var term_postings = std.ArrayListUnmanaged(u8).empty;
    defer term_postings.deinit(alloc);

    var total_field_len: u64 = 0;
    const merged_norms = try alloc.alloc(u32, running_offset);
    defer alloc.free(merged_norms);
    @memset(merged_norms, 0);
    var serialize_scratch = PostingSerializeScratch{};
    defer serialize_scratch.deinit(alloc);
    var dict_builder = try StreamingTermDictionaryBuilder.init(alloc);
    defer dict_builder.deinit();
    var merged_term = std.ArrayListUnmanaged(u8).empty;
    defer merged_term.deinit(alloc);

    while (true) {
        const min_term = findMinCurrentTerm(current_entries) orelse break;
        merged_term.clearRetainingCapacity();
        try merged_term.appendSlice(alloc, min_term);

        {
            var acc = PostingAccumulator.init();
            defer acc.deinit(alloc);

            for (current_entries, 0..) |entry_opt, seg_idx| {
                const entry = entry_opt orelse continue;
                if (!std.mem.eql(u8, entry.term, merged_term.items)) continue;

                const rmap = renumber_maps[seg_idx].?;
                try appendLookupResultToAccumulator(alloc, &acc, entry.result, rmap, merged_norms, &total_field_len);
                current_entries[seg_idx] = try nextTermIteratorEntry(term_iters, seg_idx);
            }

            if (acc.doc_ids.items.len == 0) continue;
            try sortPostingAccumulatorByDocId(alloc, &acc);
            const dict_value = try appendMergedTermToSink(alloc, sink, section_start, &term_postings, &serialize_scratch, &acc, config, running_offset);
            try dict_builder.add(merged_term.items, dict_value);
        }
    }

    const norms_data = try encodeNormTable(alloc, merged_norms);
    defer alloc.free(norms_data);
    try finishStreamingMergedSectionToSink(alloc, sink, section_start, running_offset, total_field_len, norms_data, &dict_builder, config);
}

pub fn mergeInvertedSectionSlotsWithDocMaps(
    alloc: Allocator,
    sections: []const ?[]const u8,
    doc_counts: []const u32,
    doc_maps: []const []const u32,
    merged_doc_count: u32,
    config: IndexConfig,
) ![]u8 {
    var sink = MergeMemorySink{ .alloc = alloc };
    defer sink.deinit();
    try sink.output.ensureTotalCapacityPrecise(alloc, mergedSectionCapacityHint(sections));
    try writeMergedInvertedSectionSlotsWithDocMaps(alloc, &sink, sections, doc_counts, doc_maps, merged_doc_count, config);
    return try sink.finishOwned();
}

pub fn writeMergedInvertedSectionSlotsWithDocMaps(
    alloc: Allocator,
    sink: anytype,
    sections: []const ?[]const u8,
    doc_counts: []const u32,
    doc_maps: []const []const u32,
    merged_doc_count: u32,
    config: IndexConfig,
) !void {
    if (sections.len != doc_counts.len or sections.len != doc_maps.len) return error.InvalidData;

    var readers = try alloc.alloc(InvertedIndexReader, sections.len);
    defer alloc.free(readers);
    var reader_present = try alloc.alloc(bool, sections.len);
    defer alloc.free(reader_present);
    for (sections, 0..) |section_opt, i| {
        if (doc_maps[i].len != doc_counts[i]) return error.InvalidData;
        reader_present[i] = false;
        const section = section_opt orelse continue;
        const reader = try InvertedIndexReader.init(alloc, section);
        if (reader.doc_count > doc_counts[i]) return error.InvalidData;
        readers[i] = reader;
        reader_present[i] = true;
    }

    var term_iters = try alloc.alloc(TermIterator, sections.len);
    defer {
        for (term_iters, 0..) |*iter, i| {
            if (reader_present[i]) iter.deinit();
        }
        alloc.free(term_iters);
    }

    var current_entries = try alloc.alloc(?TermIterator.Entry, sections.len);
    defer alloc.free(current_entries);
    @memset(current_entries, null);

    for (readers, 0..) |*reader, seg_idx| {
        if (!reader_present[seg_idx]) continue;
        term_iters[seg_idx] = try reader.termIterator();
        current_entries[seg_idx] = try nextTermIteratorEntry(term_iters, seg_idx);
    }

    const section_start = sink.len();
    var header_placeholder: [v7_header_size]u8 = undefined;
    @memset(&header_placeholder, 0);
    try sink.appendSlice(&header_placeholder);
    var term_postings = std.ArrayListUnmanaged(u8).empty;
    defer term_postings.deinit(alloc);

    var total_field_len: u64 = 0;
    const merged_norms = try alloc.alloc(u32, merged_doc_count);
    defer alloc.free(merged_norms);
    @memset(merged_norms, 0);
    var serialize_scratch = PostingSerializeScratch{};
    defer serialize_scratch.deinit(alloc);
    var dict_builder = try StreamingTermDictionaryBuilder.init(alloc);
    defer dict_builder.deinit();
    var merged_term = std.ArrayListUnmanaged(u8).empty;
    defer merged_term.deinit(alloc);

    while (true) {
        const min_term = findMinCurrentTerm(current_entries) orelse break;
        merged_term.clearRetainingCapacity();
        try merged_term.appendSlice(alloc, min_term);

        {
            var acc = PostingAccumulator.init();
            defer acc.deinit(alloc);

            for (current_entries, 0..) |entry_opt, seg_idx| {
                const entry = entry_opt orelse continue;
                if (!std.mem.eql(u8, entry.term, merged_term.items)) continue;

                try appendLookupResultToAccumulator(alloc, &acc, entry.result, doc_maps[seg_idx], merged_norms, &total_field_len);
                current_entries[seg_idx] = try nextTermIteratorEntry(term_iters, seg_idx);
            }

            if (acc.doc_ids.items.len == 0) continue;
            try sortPostingAccumulatorByDocId(alloc, &acc);
            const dict_value = try appendMergedTermToSink(alloc, sink, section_start, &term_postings, &serialize_scratch, &acc, config, merged_doc_count);
            try dict_builder.add(merged_term.items, dict_value);
        }
    }

    const norms_data = try encodeNormTable(alloc, merged_norms);
    defer alloc.free(norms_data);
    try finishStreamingMergedSectionToSink(alloc, sink, section_start, merged_doc_count, total_field_len, norms_data, &dict_builder, config);
}

const PostingSortEntry = struct {
    doc_id: u32,
    meta: PostingMeta,
    positions_start: usize,
};

fn postingSortEntryLessThan(_: void, a: PostingSortEntry, b: PostingSortEntry) bool {
    return a.doc_id < b.doc_id;
}

fn sortPostingAccumulatorByDocId(alloc: Allocator, acc: *PostingAccumulator) !void {
    if (acc.doc_ids.items.len <= 1) return;

    const entries = try alloc.alloc(PostingSortEntry, acc.doc_ids.items.len);
    defer alloc.free(entries);
    var positions_start: usize = 0;
    for (entries, 0..) |*entry, i| {
        entry.* = .{
            .doc_id = acc.doc_ids.items[i],
            .meta = acc.metas.items[i],
            .positions_start = positions_start,
        };
        positions_start += acc.metas.items[i].position_count;
    }

    std.mem.sort(PostingSortEntry, entries, {}, postingSortEntryLessThan);

    const sorted_positions = try alloc.alloc(u32, acc.all_positions.items.len);
    defer alloc.free(sorted_positions);
    var sorted_positions_len: usize = 0;
    for (entries, 0..) |entry, i| {
        acc.doc_ids.items[i] = entry.doc_id;
        acc.metas.items[i] = entry.meta;
        const position_count: usize = @intCast(entry.meta.position_count);
        if (position_count == 0) continue;
        const positions = acc.all_positions.items[entry.positions_start..][0..position_count];
        @memcpy(sorted_positions[sorted_positions_len..][0..position_count], positions);
        sorted_positions_len += position_count;
    }
    if (sorted_positions_len != acc.all_positions.items.len) return error.InvalidData;
    @memcpy(acc.all_positions.items, sorted_positions);
}

fn nextTermIteratorEntry(term_iters: []TermIterator, idx: usize) !?TermIterator.Entry {
    return try term_iters[idx].next();
}

fn findMinCurrentTerm(current_entries: []const ?TermIterator.Entry) ?[]const u8 {
    var min_term: ?[]const u8 = null;
    for (current_entries) |entry_opt| {
        const entry = entry_opt orelse continue;
        if (min_term == null or std.mem.order(u8, entry.term, min_term.?) == .lt) {
            min_term = entry.term;
        }
    }
    return min_term;
}

fn singleContributorIndex(current_entries: []const ?TermIterator.Entry, term: []const u8) ?usize {
    var contributor: ?usize = null;
    for (current_entries, 0..) |entry_opt, idx| {
        const entry = entry_opt orelse continue;
        if (!std.mem.eql(u8, entry.term, term)) continue;
        if (contributor != null) return null;
        contributor = idx;
    }
    return contributor;
}

fn appendSingleContributorTerm(
    alloc: Allocator,
    fst_builder: *vellum.Builder,
    postings_data: *std.ArrayListUnmanaged(u8),
    entry: TermIterator.Entry,
    deleted_docs: ?[]const ?roaring.RoaringBitmap,
    seg_idx: usize,
    doc_offset: u32,
    merged_doc_count: u32,
    total_field_len: *u64,
) !bool {
    _ = alloc;
    _ = fst_builder;
    _ = postings_data;
    _ = entry;
    _ = deleted_docs;
    _ = seg_idx;
    _ = doc_offset;
    _ = merged_doc_count;
    _ = total_field_len;
    return false;
}

fn appendLookupResultToAccumulator(
    alloc: Allocator,
    acc: *PostingAccumulator,
    result: LookupResult,
    rmap: []const u32,
    doc_norms: []u32,
    total_field_len: *u64,
) !void {
    switch (result) {
        .one_hit => |hit| {
            if (hit.doc_num >= rmap.len) return;
            const remapped_doc = rmap[hit.doc_num];
            if (remapped_doc == std.math.maxInt(u32)) return;
            if (remapped_doc < doc_norms.len and (doc_norms[remapped_doc] == 0 or hit.norm_bits > doc_norms[remapped_doc])) {
                doc_norms[remapped_doc] = hit.norm_bits;
            }
            try acc.add(alloc, remapped_doc, 1, hit.norm_bits, &.{});
            total_field_len.* += 1;
        },
        .postings => {
            var result_copy = result;
            var post_iter = try result_copy.iterator(alloc);
            defer post_iter.deinit();

            while (try post_iter.next()) |hit| {
                if (hit.doc_id >= rmap.len) continue;
                const remapped_doc = rmap[hit.doc_id];
                if (remapped_doc == std.math.maxInt(u32)) continue;
                if (remapped_doc < doc_norms.len and (doc_norms[remapped_doc] == 0 or hit.norm > doc_norms[remapped_doc])) {
                    doc_norms[remapped_doc] = hit.norm;
                }
                try acc.add(alloc, remapped_doc, hit.freq, hit.norm, hit.positions);
                total_field_len.* += hit.freq;
            }
        },
    }
}

fn appendMergedTermToSink(
    alloc: Allocator,
    sink: anytype,
    section_start: usize,
    term_postings: *std.ArrayListUnmanaged(u8),
    serialize_scratch: *PostingSerializeScratch,
    acc: *const PostingAccumulator,
    config: IndexConfig,
    merged_doc_count: u32,
) !u64 {
    if (acc.doc_ids.items.len == 1 and
        acc.metas.items[0].freq == 1 and
        acc.metas.items[0].position_count == 0 and
        acc.doc_ids.items[0] <= mask_31_bits)
    {
        const doc_num: u64 = acc.doc_ids.items[0];
        return fstValEncode1Hit(doc_num, 0);
    }

    const postings_offset: u64 = @intCast(sink.len() - section_start - v7_header_size);
    _ = merged_doc_count;
    term_postings.clearRetainingCapacity();
    try acc.serializeV9(alloc, term_postings, serialize_scratch, config);
    try sink.appendSlice(term_postings.items);
    return postings_offset;
}

/// Complete a merged section after its postings have already been emitted.
/// Only compact norms, bloom, and dictionary metadata remain resident; the
/// field-sized postings stream is never assembled in heap memory.
fn finishStreamingMergedSectionToSink(
    alloc: Allocator,
    sink: anytype,
    section_start: usize,
    doc_count: u32,
    total_field_len: u64,
    norms_data: []const u8,
    dict_builder: *StreamingTermDictionaryBuilder,
    config: IndexConfig,
) !void {
    const bloom_bytes = try dict_builder.encodeBloomAlloc(config);
    defer alloc.free(bloom_bytes);

    try sink.appendSlice(norms_data);
    if (bloom_bytes.len > 0) try sink.appendSlice(bloom_bytes);
    const term_dict_len = try dict_builder.finishIntoSink(sink);

    var header: [v7_header_size]u8 = undefined;
    writeCurrentHeader(
        &header,
        config.wireVersion(),
        doc_count,
        total_field_len,
        config.chunk_size,
        @intCast(term_dict_len),
        @intCast(bloom_bytes.len),
        @intCast(norms_data.len),
    );
    try sink.writeAt(section_start, &header);
}

// ============================================================================
// Tests
// ============================================================================

test "build and query inverted index" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    // Add two documents
    try builder.addDocument(0, &.{
        .{ .term = "hello", .freq = 1 },
        .{ .term = "world", .freq = 1 },
    });
    try builder.addDocument(1, &.{
        .{ .term = "hello", .freq = 2 },
        .{ .term = "zig", .freq = 1 },
    });

    const section = try builder.build();
    defer alloc.free(section);

    // Read it back
    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);

    // Look up "hello" — should be in both docs (general encoding, not 1-hit)
    const hello = reader.lookup("hello") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), hello.docFreq());

    // Look up "world" — should be in doc 0 only (1-hit: freq=1, single doc)
    const world = reader.lookup("world") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), world.docFreq());

    // Look up "zig" — should be in doc 1 only (1-hit)
    const zig_term = reader.lookup("zig") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), zig_term.docFreq());

    // "missing" should not exist
    try std.testing.expect(reader.lookup("missing") == null);
}

test "postings iterator yields correct hits" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    try builder.addDocument(0, &.{.{ .term = "alpha", .freq = 3, .norm = 10 }});
    try builder.addDocument(1, &.{.{ .term = "alpha", .freq = 1, .norm = 5 }});
    try builder.addDocument(2, &.{.{ .term = "alpha", .freq = 7, .norm = 20 }});

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("alpha") orelse return error.TestExpectedEqual;

    var iter = try result.iterator(alloc);
    defer iter.deinit();

    // Doc 0
    const hit0 = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 0), hit0.doc_id);
    try std.testing.expectEqual(@as(u32, 3), hit0.freq);
    try std.testing.expectEqual(@as(u32, 10), hit0.norm);

    // Doc 1
    const hit1 = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), hit1.doc_id);
    try std.testing.expectEqual(@as(u32, 1), hit1.freq);

    // Doc 2
    const hit2 = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), hit2.doc_id);
    try std.testing.expectEqual(@as(u32, 7), hit2.freq);

    // No more
    try std.testing.expect(try iter.next() == null);
}

test "term iterator enumerates all terms" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    try builder.addDocument(0, &.{
        .{ .term = "charlie", .freq = 1 },
        .{ .term = "alpha", .freq = 1 },
        .{ .term = "bravo", .freq = 1 },
    });

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    var iter = try reader.termIterator();
    defer iter.deinit();

    // Should be sorted: alpha, bravo, charlie
    const t0 = try iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("alpha", t0.term);
    const t1 = try iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("bravo", t1.term);
    const t2 = try iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("charlie", t2.term);
    try std.testing.expect(try iter.next() == null);
}

fn legacyV14TermBlockDataBytesForTest(entries: []const TermDictEntry) usize {
    var total: usize = 0;
    var i: usize = 0;
    while (i < entries.len) {
        const end = chooseTermBlockEnd(entries.len, i);
        const first_term = entries[i].term;
        const ceiling_term = entries[end - 1].term;
        const prefix_len = commonPrefixLen(first_term, ceiling_term);
        total += varintU32Size(@intCast(prefix_len));
        total += varintU32Size(@intCast(end - i));
        total += prefix_len;

        for (entries[i..end]) |entry| {
            const suffix_len = entry.term.len - prefix_len;
            total += varintU32Size(@intCast(suffix_len));
            total += suffix_len;
            total += varintU64Size(entry.value);
        }

        i = end;
    }
    return total;
}

test "v23 term dictionary stores front-coded blocks indexed by block ceiling" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    var hits = std.ArrayListUnmanaged(InvertedIndexBuilder.TermHit).empty;
    defer {
        for (hits.items) |hit| alloc.free(@constCast(hit.term));
        hits.deinit(alloc);
    }
    var i: usize = 0;
    while (i < 60) : (i += 1) {
        const term = try std.fmt.allocPrint(alloc, "aa{d:0>3}", .{i});
        errdefer alloc.free(term);
        try hits.append(alloc, .{ .term = term, .freq = 1 });
    }

    try builder.addDocument(0, hits.items);

    const section = try builder.build();
    defer alloc.free(section);

    try std.testing.expectEqual(@as(u8, wire_version_current), section[4]);
    const dict_len = std.mem.readInt(u32, section[21..25], .little);
    const dict = section[section.len - dict_len ..];
    try std.testing.expectEqualStrings(term_dict_magic, dict[0..4]);
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, dict[4..8], .little));
    const block_data_len = std.mem.readInt(u32, dict[8..12], .little);
    const index_data_len = std.mem.readInt(u32, dict[12..16], .little);
    try std.testing.expect(block_data_len > 0);
    try std.testing.expect(index_data_len > 2 * term_dict_index_record_size);

    var block_cursor: usize = term_dict_header_size;
    const first_prefix_len = try readVarintU32(dict, &block_cursor);
    try std.testing.expect(first_prefix_len > 2);
    _ = try readVarintU32(dict, &block_cursor);
    block_cursor += first_prefix_len;
    const first_shared_len = try readVarintU32(dict, &block_cursor);
    const first_leaf_len = try readVarintU32(dict, &block_cursor);
    try std.testing.expectEqual(@as(u32, 0), first_shared_len);
    try std.testing.expect(first_leaf_len > 0);
    block_cursor += first_leaf_len;
    _ = try readVarintU64(dict, &block_cursor);
    const second_shared_len = try readVarintU32(dict, &block_cursor);
    try std.testing.expect(second_shared_len > 0);

    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expect(reader.lookup("aa000") != null);
    try std.testing.expect(reader.lookup("aa034") != null);
    try std.testing.expect(reader.lookup("aa035") != null);
    try std.testing.expect(reader.lookup("aa059") != null);
    try std.testing.expect(reader.lookup("aa060") == null);

    // Normal query terms must not allocate while reconstructing a front-coded
    // block. In production this lookup is repeated once per segment and was a
    // material part of the single-term setup cost.
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    failing.fail_index = failing.alloc_index;
    reader.alloc = failing.allocator();
    try std.testing.expect(reader.lookup("aa034") != null);
    const missing_block = try reader.findTermBlockOffset("aa034x");
    try std.testing.expectError(error.NotFound, reader.lookupInTermBlock(missing_block, "aa034x"));
    reader.alloc = alloc;

    var range_iter = try reader.rangeTermIterator("aa034", "aa037");
    defer range_iter.deinit();
    const r0 = try range_iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("aa034", r0.term);
    const r1 = try range_iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("aa035", r1.term);
    const r2 = try range_iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("aa036", r2.term);
    try std.testing.expect(try range_iter.next() == null);
}

test "v23 term dictionary front coding shrinks block payload" {
    const alloc = std.testing.allocator;

    var terms = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (terms.items) |term| alloc.free(@constCast(term));
        terms.deinit(alloc);
    }
    var entries = std.ArrayListUnmanaged(TermDictEntry).empty;
    defer entries.deinit(alloc);

    var i: usize = 0;
    while (i < 48) : (i += 1) {
        const term = try std.fmt.allocPrint(alloc, "group{d:0>2}_shared_component_{d:0>3}", .{ i / 8, i });
        errdefer alloc.free(term);
        try terms.append(alloc, term);
        try entries.append(alloc, .{ .term = term, .value = @intCast(i + 1) });
    }

    const dict = try encodeBlockedTermDictionary(alloc, entries.items);
    defer alloc.free(dict);

    const block_data_len = std.mem.readInt(u32, dict[8..12], .little);
    const legacy_block_bytes = legacyV14TermBlockDataBytesForTest(entries.items);
    try std.testing.expect(block_data_len < legacy_block_bytes);
}

test "v22 term dictionary block values compact one-hit terms and delta postings offsets" {
    var encode_last_postings_offset: u64 = 0;
    var decode_last_postings_offset: u64 = 0;
    const one_hit = fstValEncode1Hit(42, 0);
    const encoded_one_hit = encodeTermDictBlockValueDelta(one_hit, &encode_last_postings_offset);
    try std.testing.expectEqual(@as(u64, 85), encoded_one_hit);
    try std.testing.expect(varintU64Size(encoded_one_hit) < varintU64Size(one_hit));
    try std.testing.expect(fstValIs1Hit(decodeTermDictBlockValueDelta(encoded_one_hit, &decode_last_postings_offset)));
    try std.testing.expectEqual(@as(u64, 42), fstValDecode1Hit(decodeTermDictBlockValueDelta(encoded_one_hit, &decode_last_postings_offset)).doc_num);

    const postings_offset: u64 = 123_456;
    const encoded_postings = encodeTermDictBlockValueDelta(postings_offset, &encode_last_postings_offset);
    try std.testing.expectEqual(postings_offset << 1, encoded_postings);
    try std.testing.expectEqual(postings_offset, decodeTermDictBlockValueDelta(encoded_postings, &decode_last_postings_offset));

    const next_postings_offset: u64 = 123_500;
    const encoded_next_postings = encodeTermDictBlockValueDelta(next_postings_offset, &encode_last_postings_offset);
    try std.testing.expectEqual(@as(u64, 88), encoded_next_postings);
    try std.testing.expect(varintU64Size(encoded_next_postings) < varintU64Size(next_postings_offset << 1));
    try std.testing.expectEqual(next_postings_offset, decodeTermDictBlockValueDelta(encoded_next_postings, &decode_last_postings_offset));

    const beyond_u32 = @as(u64, std.math.maxInt(u32)) + 987_654_321;
    const encoded_beyond_u32 = encodeTermDictBlockValueDelta(beyond_u32, &encode_last_postings_offset);
    try std.testing.expectEqual(beyond_u32, decodeTermDictBlockValueDelta(encoded_beyond_u32, &decode_last_postings_offset));
}

test "BM25 scoring" {
    // doc_count=100, doc_freq=10, freq=3, doc_len=200, avg_doc_len=150
    const score = bm25Score(3, 200, 100, 10, 150.0, .{});
    // IDF = ln(1 + (100 - 10 + 0.5) / (10 + 0.5)) ≈ ln(1 + 8.619) ≈ 2.278
    // TF = (3 * 2.2) / (3 + 1.2 * (1 - 0.75 + 0.75 * 200/150))
    //    = 6.6 / (3 + 1.2 * (0.25 + 1.0)) = 6.6 / (3 + 1.5) = 6.6 / 4.5 ≈ 1.467
    // Score ≈ 2.278 * 1.467 ≈ 3.34
    try std.testing.expect(score > 3.0);
    try std.testing.expect(score < 4.0);
}

test "BM25 term scorer retains query-invariant arithmetic" {
    const avg_doc_len: f32 = 150.0;
    const idf = bm25Idf(100, 10);
    const config = BM25Config{};
    const scorer = BM25TermScorer.init(avg_doc_len, idf, config);

    for ([_]u32{ 1, 2, 3, 7, 31, 65_535 }) |freq| {
        for ([_]u32{ 1, 40, 200, 1_048, 1_000_000 }) |doc_len| {
            const reference = bm25ScoreWithIdf(freq, doc_len, avg_doc_len, idf, config);
            try std.testing.expectApproxEqRel(reference, scorer.score(freq, doc_len), 2e-6);
        }
    }
    try std.testing.expectEqual(idf * (config.k1 + 1.0), scorer.maxScore());
}

test "BM25 bound table matches packed impact and norm domains" {
    const avg_doc_len: f32 = 137.5;
    const idf: f32 = 2.25;
    const config = BM25Config{};
    const table = BM25BoundTable.init(avg_doc_len, config);
    const unit_scorer = BM25TermScorer.init(avg_doc_len, 1.0, config);

    for ([_]u5{ 0, 1, 11, 18, 30, 31 }) |freq_id| {
        for ([_]u8{ 0, 1, 40, 88, 127, 255 }) |norm_id| {
            const expected = idf * unit_scorer.score(
                impactMaxFreqFromPackedId(freq_id),
                fieldNormFromId(norm_id),
            );
            try std.testing.expect(table.score(freq_id, norm_id, idf) >= expected);
        }
    }

    for ([_]f32{ 0.01, 0.5, 1.0, 2.25, 10.0, 25.0 }) |test_idf| {
        const direct = BM25TermScorer.init(avg_doc_len, test_idf, config);
        for (0..bm25_bound_table_frequency_count) |freq_id| {
            for (0..bm25_bound_table_norm_count) |norm_id| {
                const expected = direct.score(
                    impactMaxFreqFromPackedId(@intCast(freq_id)),
                    fieldNormFromId(@intCast(norm_id)),
                );
                const bound = table.score(@intCast(freq_id), @intCast(norm_id), test_idf);
                try std.testing.expect(bound >= expected);
            }
        }
    }
}

test "v25 field norms match Tantivy quantization" {
    try std.testing.expectEqual(@as(u32, 40), fieldNormFromId(40));
    try std.testing.expectEqual(@as(u32, 42), fieldNormFromId(41));
    try std.testing.expectEqual(@as(u32, 60), fieldNormFromId(49));
    try std.testing.expectEqual(@as(u32, 1_048), fieldNormFromId(88));
    try std.testing.expectEqual(@as(u32, 1_176), fieldNormFromId(89));
    try std.testing.expectEqual(@as(u32, 2_013_265_944), fieldNormFromId(255));
    try std.testing.expectEqual(@as(u8, 40), fieldNormToId(41));
    try std.testing.expectEqual(@as(u8, 41), fieldNormToId(42));
    try std.testing.expectEqual(@as(u8, 48), fieldNormToId(59));
    try std.testing.expectEqual(@as(u8, 49), fieldNormToId(60));
    try std.testing.expectEqual(@as(u8, 255), fieldNormToId(std.math.maxInt(u32)));
}

test "v25 norm table uses one byte per document and reads legacy packed norms" {
    const alloc = std.testing.allocator;
    const norms = [_]u32{ 1, 41, 42, 59, 60, 1_049 };
    const encoded = try encodeNormTable(alloc, &norms);
    defer alloc.free(encoded);
    try std.testing.expectEqual(@as(usize, 5 + norms.len), encoded.len);
    try std.testing.expectEqual(@as(u8, 0xff), encoded[4]);
    const expected = [_]u32{ 1, 40, 42, 56, 60, 1_048 };
    for (expected, 0..) |norm, i| try std.testing.expectEqual(norm, decodeNormValue(encoded, @intCast(i)));

    // Legacy v23/v24 norm tables remain readable.
    var legacy = std.ArrayListUnmanaged(u8).empty;
    defer legacy.deinit(alloc);
    try appendLeU32(alloc, &legacy, 3);
    try legacy.append(alloc, 6);
    _ = try appendPackedU32(alloc, &legacy, &[_]u32{ 7, 42, 63 }, 6);
    try std.testing.expectEqual(@as(u32, 7), decodeNormValue(legacy.items, 0));
    try std.testing.expectEqual(@as(u32, 42), decodeNormValue(legacy.items, 1));
    try std.testing.expectEqual(@as(u32, 63), decodeNormValue(legacy.items, 2));
}

test "merge two sections" {
    const alloc = std.testing.allocator;

    // Build section 1
    var b1 = InvertedIndexBuilder.init(alloc, .{});
    defer b1.deinit();
    try b1.addDocument(0, &.{
        .{ .term = "hello", .freq = 1 },
        .{ .term = "world", .freq = 1 },
    });
    const s1 = try b1.build();
    defer alloc.free(s1);

    // Build section 2
    var b2 = InvertedIndexBuilder.init(alloc, .{});
    defer b2.deinit();
    try b2.addDocument(0, &.{
        .{ .term = "hello", .freq = 2 },
        .{ .term = "zig", .freq = 1 },
    });
    const s2 = try b2.build();
    defer alloc.free(s2);

    // Merge
    const merged = try mergeInvertedSections(alloc, &.{ s1, s2 }, .{});
    defer alloc.free(merged);

    var reader = try InvertedIndexReader.init(alloc, merged);
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);

    // "hello" should be in both docs
    const hello = reader.lookup("hello") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), hello.docFreq());

    // "world" only in doc 0 (from segment 1)
    const world = reader.lookup("world") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), world.docFreq());

    // "zig" only in doc 1 (from segment 2, remapped)
    const zig_term = reader.lookup("zig") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), zig_term.docFreq());
}

test "streaming merge dictionary spans blocks and rebuilds exact bloom" {
    const alloc = std.testing.allocator;

    var b1 = InvertedIndexBuilder.init(alloc, .{});
    defer b1.deinit();
    var b2 = InvertedIndexBuilder.init(alloc, .{});
    defer b2.deinit();
    var term_buf: [32]u8 = undefined;

    for (0..90) |i| {
        const term = try std.fmt.bufPrint(&term_buf, "term-{d:0>3}", .{i});
        try b1.addDocument(0, &.{.{ .term = term, .freq = 1 }});
    }
    for (50..140) |i| {
        const term = try std.fmt.bufPrint(&term_buf, "term-{d:0>3}", .{i});
        try b2.addDocument(0, &.{.{ .term = term, .freq = 1 }});
    }
    const s1 = try b1.build();
    defer alloc.free(s1);
    const s2 = try b2.build();
    defer alloc.free(s2);

    const merged = try mergeInvertedSections(alloc, &.{ s1, s2 }, .{ .enable_bloom = true });
    defer alloc.free(merged);
    var reader = try InvertedIndexReader.init(alloc, merged);

    try std.testing.expect(reader.dict_block_count >= 3);
    try std.testing.expect(reader.term_bloom != null);
    try std.testing.expectEqual(@as(u32, 1), (reader.lookup("term-000") orelse return error.TestExpectedEqual).docFreq());
    try std.testing.expectEqual(@as(u32, 2), (reader.lookup("term-075") orelse return error.TestExpectedEqual).docFreq());
    try std.testing.expectEqual(@as(u32, 1), (reader.lookup("term-139") orelse return error.TestExpectedEqual).docFreq());
    try std.testing.expect(reader.lookup("term-999") == null);

    var iter = try reader.termIterator();
    defer iter.deinit();
    var count: usize = 0;
    while (try iter.next()) |_| count += 1;
    try std.testing.expectEqual(@as(usize, 140), count);
}

test "merge with deleted docs" {
    const alloc = std.testing.allocator;

    // Segment 1: docs 0,1 with terms "apple","banana"
    var b1 = InvertedIndexBuilder.init(alloc, .{});
    defer b1.deinit();
    try b1.addDocument(0, &.{.{ .term = "apple", .freq = 1 }});
    try b1.addDocument(1, &.{
        .{ .term = "apple", .freq = 1 },
        .{ .term = "banana", .freq = 1 },
    });
    const s1 = try b1.build();
    defer alloc.free(s1);

    // Segment 2: doc 0 with "banana"
    var b2 = InvertedIndexBuilder.init(alloc, .{});
    defer b2.deinit();
    try b2.addDocument(0, &.{.{ .term = "banana", .freq = 2 }});
    const s2 = try b2.build();
    defer alloc.free(s2);

    // Delete doc 0 from segment 1
    var del1 = roaring.RoaringBitmap.init(alloc);
    defer del1.deinit();
    try del1.add(0);

    const deleted = [_]?roaring.RoaringBitmap{ del1, null };
    const merged = try mergeInvertedSectionsWithDeletes(alloc, &.{ s1, s2 }, &deleted, .{});
    defer alloc.free(merged);

    var reader = try InvertedIndexReader.init(alloc, merged);
    // Should have 2 live docs total (doc 1 from seg1 + doc 0 from seg2)
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);

    // "apple" should only have 1 doc (doc 0 from seg1 was deleted)
    const apple = reader.lookup("apple") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), apple.docFreq());

    // "banana" should have 2 docs
    const banana = reader.lookup("banana") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), banana.docFreq());
}

test "merge preserves 1-hit encoding for unique live terms" {
    const alloc = std.testing.allocator;

    var b1 = InvertedIndexBuilder.init(alloc, .{});
    defer b1.deinit();
    try b1.addDocument(0, &.{
        .{ .term = "alpha", .freq = 1, .norm = 11 },
        .{ .term = "shared", .freq = 2, .norm = 11 },
    });
    const s1 = try b1.build();
    defer alloc.free(s1);

    var b2 = InvertedIndexBuilder.init(alloc, .{});
    defer b2.deinit();
    try b2.addDocument(0, &.{
        .{ .term = "beta", .freq = 1, .norm = 13 },
        .{ .term = "shared", .freq = 1, .norm = 13 },
    });
    const s2 = try b2.build();
    defer alloc.free(s2);

    const merged = try mergeInvertedSections(alloc, &.{ s1, s2 }, .{});
    defer alloc.free(merged);

    var reader = try InvertedIndexReader.init(alloc, merged);

    const alpha = reader.lookup("alpha") orelse return error.TestExpectedEqual;
    switch (alpha) {
        .one_hit => |hit| {
            try std.testing.expectEqual(@as(u32, 0), hit.doc_num);
            try std.testing.expectEqual(@as(u32, 11), hit.norm_bits);
        },
        .postings => return error.TestExpectedEqual,
    }

    const beta = reader.lookup("beta") orelse return error.TestExpectedEqual;
    switch (beta) {
        .one_hit => |hit| {
            try std.testing.expectEqual(@as(u32, 1), hit.doc_num);
            try std.testing.expectEqual(@as(u32, 13), hit.norm_bits);
        },
        .postings => return error.TestExpectedEqual,
    }

    const shared = reader.lookup("shared") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), shared.docFreq());
}

test "merge direct-copies serialized postings for zero-offset unique term" {
    const alloc = std.testing.allocator;

    var b1 = InvertedIndexBuilder.init(alloc, .{});
    defer b1.deinit();
    try b1.addDocument(0, &.{.{ .term = "carry", .freq = 3, .norm = 9 }});
    try b1.addDocument(1, &.{.{ .term = "carry", .freq = 2, .norm = 11 }});
    const s1 = try b1.build();
    defer alloc.free(s1);

    var b2 = InvertedIndexBuilder.init(alloc, .{});
    defer b2.deinit();
    try b2.addDocument(0, &.{.{ .term = "later", .freq = 1, .norm = 7 }});
    const s2 = try b2.build();
    defer alloc.free(s2);

    var r1 = try InvertedIndexReader.init(alloc, s1);
    const source = r1.lookup("carry") orelse return error.TestExpectedEqual;

    const merged = try mergeInvertedSections(alloc, &.{ s1, s2 }, .{});
    defer alloc.free(merged);

    var merged_reader = try InvertedIndexReader.init(alloc, merged);
    const carry = merged_reader.lookup("carry") orelse return error.TestExpectedEqual;

    switch (source) {
        .postings => |src_postings| switch (carry) {
            .postings => |merged_postings| try std.testing.expectEqualStrings(src_postings.serialized_data, merged_postings.serialized_data),
            .one_hit => return error.TestExpectedEqual,
        },
        .one_hit => return error.TestExpectedEqual,
    }
}

test "merge remaps unique postings term from later segment" {
    const alloc = std.testing.allocator;

    var b1 = InvertedIndexBuilder.init(alloc, .{});
    defer b1.deinit();
    try b1.addDocument(0, &.{.{ .term = "first", .freq = 1, .norm = 5 }});
    try b1.addDocument(1, &.{.{ .term = "first", .freq = 1, .norm = 6 }});
    const s1 = try b1.build();
    defer alloc.free(s1);

    var b2 = InvertedIndexBuilder.init(alloc, .{});
    defer b2.deinit();
    try b2.addDocument(0, &.{.{ .term = "shifted", .freq = 3, .norm = 9 }});
    try b2.addDocument(1, &.{.{ .term = "shifted", .freq = 2, .norm = 11 }});
    const s2 = try b2.build();
    defer alloc.free(s2);

    const merged = try mergeInvertedSections(alloc, &.{ s1, s2 }, .{});
    defer alloc.free(merged);

    var reader = try InvertedIndexReader.init(alloc, merged);
    const shifted = reader.lookup("shifted") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), shifted.docFreq());

    var iter = try shifted.iterator(alloc);
    defer iter.deinit();

    const hit0 = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), hit0.doc_id);
    try std.testing.expectEqual(@as(u32, 3), hit0.freq);
    const hit1 = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 3), hit1.doc_id);
    try std.testing.expectEqual(@as(u32, 2), hit1.freq);
    try std.testing.expect(try iter.next() == null);
}

test "1-hit optimization end-to-end" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    // "unique" appears in 1 doc with freq=1 → should be 1-hit
    // "common" appears in 2 docs → should be general encoding
    try builder.addDocument(0, &.{
        .{ .term = "unique", .freq = 1, .norm = 42 },
        .{ .term = "common", .freq = 2 },
    });
    try builder.addDocument(1, &.{
        .{ .term = "common", .freq = 1 },
    });

    const section = try builder.build();
    defer alloc.free(section);

    // Builders emit the current version by default; the FST version-encoding (1-hit packing) is
    // unchanged from v3+, so the "unique" term still lands on the 1-hit path.
    try std.testing.expectEqual(@as(u8, wire_version_current), section[4]);

    var reader = try InvertedIndexReader.init(alloc, section);

    // "unique" should be a 1-hit
    const unique = reader.lookup("unique") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), unique.docFreq());
    switch (unique) {
        .one_hit => |h| {
            try std.testing.expectEqual(@as(u32, 0), h.doc_num);
            try std.testing.expectEqual(@as(u32, 42), h.norm_bits);
        },
        .postings => return error.TestExpectedEqual,
    }

    // Iterate 1-hit via PostingsIterator
    var iter = try unique.iterator(alloc);
    defer iter.deinit();
    const hit = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 0), hit.doc_id);
    try std.testing.expectEqual(@as(u32, 1), hit.freq);
    try std.testing.expectEqual(@as(u32, 42), hit.norm);
    try std.testing.expect(try iter.next() == null);

    // "common" should be general encoding
    const common = reader.lookup("common") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), common.docFreq());
    switch (common) {
        .postings => {},
        .one_hit => return error.TestExpectedEqual,
    }
}

test "1-hit encoding round-trip" {
    // Basic round-trip
    const encoded = fstValEncode1Hit(42, 12345);
    try std.testing.expect(fstValIs1Hit(encoded));
    const decoded = fstValDecode1Hit(encoded);
    try std.testing.expectEqual(@as(u64, 42), decoded.doc_num);
    try std.testing.expectEqual(@as(u64, 12345), decoded.norm_bits);

    // Max 31-bit values
    const max31: u64 = 0x7fffffff;
    const max_encoded = fstValEncode1Hit(max31, max31);
    try std.testing.expect(fstValIs1Hit(max_encoded));
    const max_decoded = fstValDecode1Hit(max_encoded);
    try std.testing.expectEqual(max31, max_decoded.doc_num);
    try std.testing.expectEqual(max31, max_decoded.norm_bits);

    // General encoding should not be detected as 1-hit
    try std.testing.expect(!fstValIs1Hit(0));
    try std.testing.expect(!fstValIs1Hit(12345));
}

test "freqHasLocs encoding round-trip" {
    // freq=5, hasLocs=true
    const v1 = encodeFreqHasLocs(5, true);
    try std.testing.expectEqual(@as(u64, 11), v1); // (5 << 1) | 1
    const d1 = decodeFreqHasLocs(v1);
    try std.testing.expectEqual(@as(u64, 5), d1.freq);
    try std.testing.expect(d1.has_locs);

    // freq=5, hasLocs=false
    const v2 = encodeFreqHasLocs(5, false);
    try std.testing.expectEqual(@as(u64, 10), v2); // (5 << 1) | 0
    const d2 = decodeFreqHasLocs(v2);
    try std.testing.expectEqual(@as(u64, 5), d2.freq);
    try std.testing.expect(!d2.has_locs);

    // freq=0
    const v3 = encodeFreqHasLocs(0, false);
    try std.testing.expectEqual(@as(u64, 0), v3);
}

test "current one posting block retains one global impact bound" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    try builder.addDocument(0, &.{.{ .term = "late", .freq = 3, .norm = 9 }});
    try builder.addDocument(1, &.{.{ .term = "pad1", .freq = 1, .norm = 10 }});
    try builder.addDocument(2, &.{.{ .term = "pad2", .freq = 1, .norm = 10 }});
    try builder.addDocument(3, &.{.{ .term = "pad3", .freq = 1, .norm = 10 }});
    try builder.addDocument(4, &.{.{ .term = "pad4", .freq = 1, .norm = 10 }});
    try builder.addDocument(5, &.{.{ .term = "late", .freq = 2, .norm = 11 }});

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("late") orelse return error.TestExpectedEqual;
    switch (result) {
        .postings => |p| {
            const bm = p.block_max orelse return error.TestExpectedEqual;
            const expected_header_len = varintU32Size(p.doc_freq) +
                varintU32Size(@intCast(p.payload_data.len)) +
                varintU32Size(0);
            try std.testing.expectEqual(expected_header_len, p.header_len);
            try std.testing.expectEqual(@as(usize, 1), bm.chunkCount());
            try std.testing.expectEqual(@as(usize, 2), bm.meta.len);
            try std.testing.expectEqual(@as(u16, 3), bm.maxFreqAt(0));
            try std.testing.expectEqual(@as(u32, 9), bm.minNormAt(0));
            try std.testing.expect(bm.maxImpact(0, 6, 2, reader.avgDocLen(), .{}) > 0);

            var iter = try p.iterator(alloc);
            defer iter.deinit();
            try std.testing.expectEqual(
                bm.maxImpact(0, 6, 2, reader.avgDocLen(), .{}),
                try iter.blockMaxImpact(bm, 0, 6, 2, reader.avgDocLen(), .{}),
            );
        },
        .one_hit => return error.TestExpectedEqual,
    }
}

test "v29 impact frequency escape remains a conservative upper bound" {
    try std.testing.expectEqual(@as(u8, 254), impactMaxFreqToId(254));
    try std.testing.expectEqual(std.math.maxInt(u8), impactMaxFreqToId(255));
    try std.testing.expectEqual(std.math.maxInt(u8), impactMaxFreqToId(4096));
    try std.testing.expectEqual(@as(u16, 254), impactMaxFreqFromId(254));
    try std.testing.expectEqual(std.math.maxInt(u16), impactMaxFreqFromId(std.math.maxInt(u8)));
}

test "v29 adaptive impact IDs use runs and round-trip" {
    const alloc = std.testing.allocator;
    var ids: [100]u32 = undefined;
    for (&ids, 0..) |*id, i| id.* = @intCast(700 + i);
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);
    var deltas = std.ArrayListUnmanaged(u32).empty;
    defer deltas.deinit(alloc);
    try encodeImpactChunkIds(alloc, &encoded, &ids, &deltas);
    try std.testing.expectEqual(impact_ids_run_encoding, encoded.items[0]);

    var iter = PostingsIterator{
        .alloc = alloc,
        .impact_chunk_ids_data = encoded.items,
        .impact_chunk_count = ids.len,
    };
    defer iter.deinit();
    try iter.decodeImpactChunkIds();
    try std.testing.expectEqualSlices(u32, &ids, iter.impact_chunk_ids.items);
    try std.testing.expectEqual(@as(?usize, 37), findEncodedImpactChunkOrdinal(encoded.items, ids.len, ids[37]));
}

test "v29 one-payload-block postings omit sparse impact range IDs" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 128 });
    defer builder.deinit();
    try builder.addDocument(0, &.{.{ .term = "sparse", .freq = 1, .norm = 4, .positions = &.{0} }});
    for (1..5000) |doc_id| try builder.addDocument(@intCast(doc_id), &.{});
    try builder.addDocument(5000, &.{.{ .term = "sparse", .freq = 2, .norm = 8, .positions = &.{ 1, 9 } }});

    const section = try builder.build();
    defer alloc.free(section);
    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("sparse") orelse return error.TestExpectedEqual;
    switch (result) {
        .postings => |p| {
            try std.testing.expect(!p.doc_range_aligned);
            try std.testing.expectEqual(@as(u32, 0), p.impact_chunk_count);
            try std.testing.expect(p.impact_chunk_ids_data == null);
            const block_max = p.block_max orelse return error.TestExpectedEqual;
            try std.testing.expect(!block_max.range_ids);
            try std.testing.expectEqual(@as(usize, 1), block_max.chunkCount());
        },
        .one_hit => return error.TestExpectedEqual,
    }
}

test "v30 contiguous grouped positions retain direct document round-trip" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 128 });
    defer builder.deinit();
    for (0..position_doc_group_size) |doc_id| {
        const positions = [_]u32{@intCast(doc_id * 3)};
        try builder.addDocument(@intCast(doc_id), &.{.{ .term = "grouped", .freq = 1, .norm = 8, .positions = &positions }});
    }
    const section = try builder.build();
    defer alloc.free(section);
    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("grouped") orelse return error.TestExpectedEqual;
    switch (result) {
        .postings => |p| {
            // One chunk-length byte, one shared width, and one byte per doc.
            try std.testing.expect(p.positions_data.?.len <= position_doc_group_size + 2);
            var iter = try p.iterator(alloc);
            defer iter.deinit();
            for (0..position_doc_group_size) |doc_id| {
                const hit = try iter.next() orelse return error.TestExpectedEqual;
                try std.testing.expectEqual(@as(u32, @intCast(doc_id)), hit.doc_id);
                try std.testing.expectEqualSlices(u32, &.{@as(u32, @intCast(doc_id * 3))}, hit.positions);
            }
        },
        .one_hit => return error.TestExpectedEqual,
    }
}

test "v21 postings keep norms in per-section table with bit-packed chunk metadata" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    try builder.addDocument(0, &.{.{ .term = "term", .freq = 3, .norm = 10 }});
    try builder.addDocument(1, &.{.{ .term = "term", .freq = 1, .norm = 20 }});
    try builder.addDocument(2, &.{.{ .term = "term", .freq = 5, .norm = 30 }});
    try builder.addDocument(3, &.{.{ .term = "term", .freq = 2, .norm = 15 }});

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const layout = reader.layoutStats();
    try std.testing.expectEqual(@as(u64, 9), layout.norm_bytes);
    try std.testing.expectEqual(@as(u64, 1), layout.term_count);

    const result = reader.lookup("term") orelse return error.TestExpectedEqual;
    switch (result) {
        .postings => |p| {
            try std.testing.expectEqual(@as(u8, wire_version_current), p.version);
            try std.testing.expect(p.chunk_meta_data.len < 24);
            try std.testing.expectEqual(@as(usize, 10), p.payload_data.len);
            var iter = try p.iterator(alloc);
            defer iter.deinit();

            const h0 = try iter.next() orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u32, 0), h0.doc_id);
            try std.testing.expectEqual(@as(u32, 3), h0.freq);
            try std.testing.expectEqual(@as(u32, 10), h0.norm);
            const h3 = try iter.advanceTo(3) orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u32, 3), h3.doc_id);
            try std.testing.expectEqual(@as(u32, 2), h3.freq);
            try std.testing.expectEqual(@as(u32, 15), h3.norm);
        },
        .one_hit => return error.TestExpectedEqual,
    }
}

test "positions round-trip v12 bit-packed deltas" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    // Doc 0: "hello" at positions [0, 5]
    // Doc 1: "hello" at positions [3]
    // Doc 0: "world" at positions [1]
    try builder.addDocument(0, &.{
        .{ .term = "hello", .freq = 2, .norm = 10, .positions = &.{ 0, 5 } },
        .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{1} },
    });
    try builder.addDocument(1, &.{
        .{ .term = "hello", .freq = 1, .norm = 8, .positions = &.{3} },
    });

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expectEqual(@as(u8, wire_version_current), reader.version);

    // Check "hello" positions
    const hello = reader.lookup("hello") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 2), hello.docFreq());
    {
        var iter = try hello.iterator(alloc);
        defer iter.deinit();

        // Doc 0: positions [0, 5]
        const hit0 = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 0), hit0.doc_id);
        try std.testing.expectEqual(@as(u32, 2), hit0.freq);
        try std.testing.expectEqual(@as(usize, 2), hit0.positions.len);
        try std.testing.expectEqual(@as(u32, 0), hit0.positions[0]);
        try std.testing.expectEqual(@as(u32, 5), hit0.positions[1]);

        // Doc 1: positions [3]
        const hit1 = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 1), hit1.doc_id);
        try std.testing.expectEqual(@as(u32, 1), hit1.freq);
        try std.testing.expectEqual(@as(usize, 1), hit1.positions.len);
        try std.testing.expectEqual(@as(u32, 3), hit1.positions[0]);

        try std.testing.expect(try iter.next() == null);
    }

    // Check "world" positions (has positions, so not 1-hit even though single doc)
    const world = reader.lookup("world") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), world.docFreq());
    {
        var iter = try world.iterator(alloc);
        defer iter.deinit();
        const hit = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 0), hit.doc_id);
        try std.testing.expectEqual(@as(usize, 1), hit.positions.len);
        try std.testing.expectEqual(@as(u32, 1), hit.positions[0]);
    }
}

test "merge with inverted doc_count less than segment doc_count" {
    // Regression: when a segment has documents that don't all contribute to
    // a field's inverted section, the inverted section's doc_count will be
    // less than the segment's doc_count. The merge must handle this.
    const alloc = std.testing.allocator;

    // Segment 1: 4 docs, but only docs 2,3 have the "parent" field.
    // The inverted section will have doc_count=2 with postings for doc IDs 2,3.
    var b1 = InvertedIndexBuilder.init(alloc, .{});
    defer b1.deinit();
    try b1.addDocument(2, &.{.{ .term = "root-a", .freq = 1, .norm = 4 }});
    try b1.addDocument(3, &.{.{ .term = "child", .freq = 1, .norm = 4 }});
    const s1 = try b1.build();
    defer alloc.free(s1);

    // Segment 2: 1 doc with the "parent" field.
    var b2 = InvertedIndexBuilder.init(alloc, .{});
    defer b2.deinit();
    try b2.addDocument(0, &.{.{ .term = "root-b", .freq = 1, .norm = 1 }});
    const s2 = try b2.build();
    defer alloc.free(s2);

    // Verify s1 has doc_count=2 (only 2 addDocument calls)
    const r1 = try InvertedIndexReader.init(alloc, s1);
    try std.testing.expectEqual(@as(u32, 2), r1.doc_count);

    // Merge with segment-level doc_counts: seg1 has 4 total docs, seg2 has 1.
    const merged = try mergeInvertedSectionSlotsWithDeletes(
        alloc,
        &.{ s1, s2 },
        &.{ 4, 1 },
        null,
        .{},
    );
    defer alloc.free(merged);

    var reader = try InvertedIndexReader.init(alloc, merged);
    // Merged doc_count should be total live docs: 4 + 1 = 5
    try std.testing.expectEqual(@as(u32, 5), reader.doc_count);

    // "root-a" should be remapped from doc 2 in seg1 to doc 2 in merged
    const root_a = reader.lookup("root-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), root_a.docFreq());

    // "root-b" should be remapped from doc 0 in seg2 to doc 4 in merged
    const root_b = reader.lookup("root-b") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 1), root_b.docFreq());
}

test "sparse field postings beyond first chunk survive merge" {
    const alloc = std.testing.allocator;

    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();
    try builder.addDocument(4, &.{.{ .term = "late", .freq = 2, .norm = 7 }});

    const section = try builder.build();
    defer alloc.free(section);

    var source_reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expectEqual(@as(u32, 1), source_reader.doc_count);

    const source_late = source_reader.lookup("late") orelse return error.TestExpectedEqual;
    var source_iter = try source_late.iterator(alloc);
    defer source_iter.deinit();
    const source_hit = (try source_iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 4), source_hit.doc_id);
    try std.testing.expectEqual(@as(u32, 2), source_hit.freq);
    try std.testing.expect(try source_iter.next() == null);

    const merged = try mergeInvertedSectionSlotsWithDeletes(
        alloc,
        &.{section},
        &.{6},
        null,
        .{ .chunk_size = 2 },
    );
    defer alloc.free(merged);

    var merged_reader = try InvertedIndexReader.init(alloc, merged);
    try std.testing.expectEqual(@as(u32, 6), merged_reader.doc_count);

    const late = merged_reader.lookup("late") orelse return error.TestExpectedEqual;
    var iter = try late.iterator(alloc);
    defer iter.deinit();
    const hit = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 4), hit.doc_id);
    try std.testing.expectEqual(@as(u32, 2), hit.freq);
    try std.testing.expectEqual(@as(u32, 7), hit.norm);
    try std.testing.expect(try iter.next() == null);
}

test "PostingsIterator advanceTo skips through chunks correctly" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 4 });
    defer builder.deinit();

    // 12 docs, "term" present in every other doc → 6 hits across 3 chunks.
    // doc_id sequence: 0, 2, 4, 6, 8, 10. Chunks:
    //   chunk 0 (docs 0..3):   doc 0,  doc 2
    //   chunk 1 (docs 4..7):   doc 4,  doc 6
    //   chunk 2 (docs 8..11):  doc 8,  doc 10
    var freq: u32 = 1;
    var i: u32 = 0;
    while (i < 12) : (i += 2) {
        try builder.addDocument(i, &.{
            .{ .term = "term", .freq = freq, .norm = 10 + freq },
        });
        freq += 1;
    }

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;

    // Seek to mid-chunk: target=5 → land on doc 6 (chunk 1, position 1).
    {
        var iter = try lookup.iterator(alloc);
        defer iter.deinit();
        const hit = (try iter.advanceTo(5)) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 6), hit.doc_id);
        // freq=4 came from doc 6 (4th addDocument call: 0→1, 2→2, 4→3, 6→4).
        try std.testing.expectEqual(@as(u32, 4), hit.freq);
        // Subsequent next() should yield doc 8 in the next chunk.
        const next_hit = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 8), next_hit.doc_id);
    }

    // Seek to a doc not in the postings: target=7 → land on doc 8.
    {
        var iter = try lookup.iterator(alloc);
        defer iter.deinit();
        const hit = (try iter.advanceTo(7)) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 8), hit.doc_id);
    }

    // Seek before any doc: target=0 → land on doc 0.
    {
        var iter = try lookup.iterator(alloc);
        defer iter.deinit();
        const hit = (try iter.advanceTo(0)) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 0), hit.doc_id);
        try std.testing.expectEqual(@as(u32, 1), hit.freq);
    }

    // Seek past last doc: target=20 → null.
    {
        var iter = try lookup.iterator(alloc);
        defer iter.deinit();
        try std.testing.expect(try iter.advanceTo(20) == null);
    }
}

test "PostingsIterator positional seek decodes only selected records" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    for (0..8) |doc| {
        const positions = [_]u32{ @intCast(doc), @intCast(doc + 10) };
        try builder.addDocument(@intCast(doc), &.{.{
            .term = "term",
            .freq = 2,
            .norm = 20,
            .positions = &positions,
        }});
    }
    const section = try builder.build();
    defer alloc.free(section);
    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;
    var iter = try lookup.iterator(alloc);
    defer iter.deinit();

    const first = (try iter.advanceToWithPositions(5)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 5), first.doc_id);
    try std.testing.expectEqualSlices(u32, &.{ 5, 15 }, first.positions);
    const second = (try iter.advanceToWithPositions(7)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 7), second.doc_id);
    try std.testing.expectEqualSlices(u32, &.{ 7, 17 }, second.positions);
    try std.testing.expect(try iter.advanceToWithPositions(9) == null);
}

test "PostingsIterator deferred positional seek decodes only accepted candidates" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2 });
    defer builder.deinit();

    for (0..8) |doc| {
        const positions = [_]u32{ @intCast(doc), @intCast(doc + 10) };
        try builder.addDocument(@intCast(doc), &.{.{
            .term = "term",
            .freq = 2,
            .norm = 20,
            .positions = &positions,
        }});
    }
    const section = try builder.build();
    defer alloc.free(section);
    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;
    var iter = try lookup.iterator(alloc);
    defer iter.deinit();

    const candidate = (try iter.advanceToDeferredPositions(5)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 5), candidate.doc_id);
    try std.testing.expectEqual(@as(usize, 0), candidate.positions.len);
    try std.testing.expectEqual(@as(u64, 0), iter.decodedPositionRecords());

    // Re-reading the pending candidate must neither consume nor decode it.
    const same = (try iter.advanceToDeferredPositions(5)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 5), same.doc_id);
    try std.testing.expectEqual(@as(u64, 0), iter.decodedPositionRecords());

    const accepted = try iter.decodeDeferredPositions();
    try std.testing.expectEqualSlices(u32, &.{ 5, 15 }, accepted.positions);
    try std.testing.expectEqual(@as(u64, 1), iter.decodedPositionRecords());

    const next_candidate = (try iter.advanceToDeferredPositions(7)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 7), next_candidate.doc_id);
    try std.testing.expectEqual(@as(u64, 1), iter.decodedPositionRecords());
    const next_accepted = try iter.decodeDeferredPositions();
    try std.testing.expectEqualSlices(u32, &.{ 7, 17 }, next_accepted.positions);
    try std.testing.expectEqual(@as(u64, 2), iter.decodedPositionRecords());
    try std.testing.expect(try iter.advanceToDeferredPositions(9) == null);
}

test "PostingsIterator streams deferred grouped positions without scratch arrays" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 8 });
    defer builder.deinit();

    for (0..8) |doc| {
        const positions = [_]u32{ @intCast(doc), @intCast(doc + 10) };
        try builder.addDocument(@intCast(doc), &.{.{
            .term = "term",
            .freq = 2,
            .norm = 20,
            .positions = &positions,
        }});
    }
    const section = try builder.build();
    defer alloc.free(section);
    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;
    var iter = try lookup.iterator(alloc);
    defer iter.deinit();

    const candidate = (try iter.advanceToDeferredPositions(5)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 5), candidate.doc_id);
    const packed_view = try iter.takeDeferredPackedPositions();
    var cursor = try packed_view.cursor();
    try std.testing.expectEqual(@as(?u32, 5), try cursor.next());
    try std.testing.expectEqual(@as(?u32, 15), try cursor.next());
    try std.testing.expectEqual(@as(?u32, null), try cursor.next());
    try std.testing.expectEqual(@as(u64, 1), iter.decodedPositionRecords());

    const next_candidate = (try iter.advanceToDeferredPositions(7)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 7), next_candidate.doc_id);
    const next_packed = try iter.takeDeferredPackedPositions();
    var next_cursor = try next_packed.cursor();
    try std.testing.expectEqual(@as(?u32, 7), try next_cursor.next());
    try std.testing.expectEqual(@as(?u32, 17), try next_cursor.next());
}

test "production reader rejects branch-only v24-v37 formats" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 4 });
    defer builder.deinit();
    try builder.addDocument(0, &.{.{
        .term = "format-contract",
        .freq = 2,
        .norm = 8,
    }});
    const section = try builder.build();
    defer alloc.free(section);

    var candidate = try alloc.dupe(u8, section);
    defer alloc.free(candidate);
    var version: u8 = wire_version_checkpoints;
    while (version < wire_version_current) : (version += 1) {
        candidate[4] = version;
        try std.testing.expectError(error.UnsupportedVersion, InvertedIndexReader.init(alloc, candidate));
    }
}

test "v31 inline single-document postings retain frequency positions and direct iteration" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 4 });
    defer builder.deinit();
    const expected_positions = [_]u32{ 2, 5, 11 };
    try builder.addDocument(0, &.{.{
        .term = "singleton-with-positions",
        .freq = expected_positions.len,
        .norm = 17,
        .positions = &expected_positions,
    }});

    const section = try builder.build();
    defer alloc.free(section);
    try std.testing.expectEqual(wire_version_current, section[4]);

    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("singleton-with-positions") orelse return error.TestExpectedEqual;
    const postings = switch (result) {
        .postings => |postings| postings,
        .one_hit => return error.TestExpectedEqual,
    };
    try std.testing.expect(postings.inline_single_doc);
    try std.testing.expectEqual(@as(u32, 1), postings.doc_freq);
    try std.testing.expectEqual(@as(u32, 3), postings.inline_freq);
    try std.testing.expectEqual(@as(usize, 0), postings.chunk_meta_data.len);
    try std.testing.expectEqual(@as(usize, 0), postings.payload_data.len);
    try std.testing.expect(postings.block_max == null);
    try std.testing.expect(postings.serialized_data.len < 10);

    var ranking_iter = try postings.iterator(alloc);
    defer ranking_iter.deinit();
    ranking_iter.decode_positions = false;
    const ranking_hit = (try ranking_iter.advanceTo(0)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 0), ranking_hit.doc_id);
    try std.testing.expectEqual(@as(u32, 3), ranking_hit.freq);
    try std.testing.expectEqual(@as(usize, 0), ranking_hit.positions.len);

    var phrase_iter = try postings.iterator(alloc);
    defer phrase_iter.deinit();
    const phrase_hit = (try phrase_iter.advanceToWithPositions(0)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualSlices(u32, &expected_positions, phrase_hit.positions);
    try std.testing.expect(try phrase_iter.next() == null);
}

test "v32 posting-count metadata derives chunk ordinal and document count" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 128 });
    defer builder.deinit();
    for (0..257) |doc_id| {
        try builder.addDocument(@intCast(doc_id), &.{.{ .term = "three-blocks", .freq = 1, .norm = 9 }});
    }

    const section = try builder.build();
    defer alloc.free(section);
    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("three-blocks") orelse return error.TestExpectedEqual;
    const postings = switch (result) {
        .postings => |postings| postings,
        .one_hit => return error.TestExpectedEqual,
    };
    try std.testing.expectEqual(@as(u32, 3), postings.chunk_meta_count);
    const layout = try compactChunkMetaLayout(postings.chunk_meta_data, postings.chunk_meta_count, postings.version);
    try std.testing.expectEqual(@as(usize, 0), layout.chunk_delta_len);
    try std.testing.expectEqual(@as(usize, 0), layout.doc_count_len);
    try std.testing.expectEqual(@as(usize, 2), layout.max_doc_offset_off);
    try std.testing.expectEqual(postings.chunk_meta_data.len, layout.total_len);

    var iter = try postings.iterator(alloc);
    defer iter.deinit();
    try std.testing.expectEqual(@as(u32, 127), (try iter.advanceTo(127)).?.doc_id);
    try std.testing.expectEqual(@as(u32, 128), (try iter.advanceTo(128)).?.doc_id);
    try std.testing.expectEqual(@as(u32, 256), (try iter.advanceTo(256)).?.doc_id);
    try std.testing.expect(try iter.next() == null);
}

test "v33 constant-frequency blocks omit packed frequency payload" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 128 });
    defer builder.deinit();
    const one_position = [_]u32{0};
    for (0..256) |doc_id| {
        try builder.addDocument(@intCast(doc_id), &.{.{
            .term = "constant-frequency",
            .freq = 1,
            .norm = 12,
            .positions = &one_position,
        }});
    }

    const section = try builder.build();
    defer alloc.free(section);
    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("constant-frequency") orelse return error.TestExpectedEqual;
    const postings = switch (result) {
        .postings => |postings| postings,
        .one_hit => return error.TestExpectedEqual,
    };
    try std.testing.expectEqual(@as(u32, 2), postings.chunk_meta_count);
    for (0..postings.chunk_meta_count) |block_index| {
        const meta = try readCompactChunkMetaAt(
            postings.chunk_meta_data,
            postings.chunk_meta_count,
            postings.version,
            postings.chunk_size,
            postings.doc_freq,
            block_index,
        );
        const block = postings.payload_data[meta.doc_ctrl_off..][0..meta.doc_ctrl_len];
        var cursor: usize = 0;
        _ = try readVarintU32(block, &cursor);
        try std.testing.expectEqual(constant_frequency_marker | @as(u8, @intCast(encodeFreqHasLocs(1, true))), block[cursor + 1]);
        const doc_control = block[cursor];
        try std.testing.expect(doc_control & vertical_bp128_marker != 0);
        const doc_bits = doc_control & packed_width_mask;
        try std.testing.expectEqual(cursor + 2 + try simd_bitpack.encodedLen(doc_bits), block.len);
    }

    var iter = try postings.iterator(alloc);
    defer iter.deinit();
    const hit = (try iter.advanceToWithPositions(200)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 200), hit.doc_id);
    try std.testing.expectEqual(@as(u32, 1), hit.freq);
    try std.testing.expectEqualSlices(u32, &one_position, hit.positions);
}

test "v35 full posting blocks use portable vertical BP128 for docs and frequencies" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 128 });
    defer builder.deinit();
    for (0..128) |doc_id| {
        try builder.addDocument(@intCast(doc_id), &.{.{
            .term = "vertical-bp128",
            .freq = @intCast(1 + doc_id % 7),
            .norm = @intCast(10 + doc_id % 5),
        }});
    }

    const section = try builder.build();
    defer alloc.free(section);
    try std.testing.expectEqual(wire_version_current, section[4]);
    var reader = try InvertedIndexReader.init(alloc, section);
    const result = reader.lookup("vertical-bp128") orelse return error.TestExpectedEqual;
    const postings = switch (result) {
        .postings => |postings| postings,
        .one_hit => return error.TestExpectedEqual,
    };
    try std.testing.expectEqual(@as(u32, 1), postings.chunk_meta_count);
    const meta = try readCompactChunkMetaAt(
        postings.chunk_meta_data,
        postings.chunk_meta_count,
        postings.version,
        postings.chunk_size,
        postings.doc_freq,
        0,
    );
    const block = postings.payload_data[meta.doc_ctrl_off..][0..meta.doc_ctrl_len];
    var cursor: usize = 0;
    _ = try readVarintU32(block, &cursor);
    const doc_control = block[cursor];
    const freq_control = block[cursor + 1];
    try std.testing.expect(doc_control & vertical_bp128_marker != 0);
    try std.testing.expect(freq_control & vertical_bp128_marker != 0);
    const doc_len = try simd_bitpack.encodedLen(doc_control & packed_width_mask);
    const freq_len = try simd_bitpack.encodedLen(freq_control & packed_width_mask);
    try std.testing.expectEqual(cursor + 2 + doc_len + freq_len, block.len);

    var iter = try postings.iterator(alloc);
    defer iter.deinit();
    iter.decode_positions = false;
    for (0..128) |doc_id| {
        const hit = try iter.nextScoring() orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, @intCast(doc_id)), hit.doc_id);
        try std.testing.expectEqual(@as(u32, @intCast(1 + doc_id % 7)), hit.freq);
    }
    try std.testing.expect(try iter.nextScoring() == null);
}

test "v34 five-bit impact frequencies are conservative upper bounds" {
    for (0..256) |raw_id| {
        const id: u8 = @intCast(raw_id);
        const decoded = impactMaxFreqFromPackedId(impactMaxFreqToPackedId(id));
        try std.testing.expect(decoded >= impactMaxFreqFromId(id));
    }
    try std.testing.expectEqual(@as(u16, 1), impactMaxFreqFromPackedId(impactMaxFreqToPackedId(1)));
    try std.testing.expectEqual(@as(u16, 5), impactMaxFreqFromPackedId(impactMaxFreqToPackedId(5)));
    try std.testing.expectEqual(@as(u16, 112), impactMaxFreqFromPackedId(impactMaxFreqToPackedId(100)));
    try std.testing.expectEqual(std.math.maxInt(u16), impactMaxFreqFromPackedId(impactMaxFreqToPackedId(255)));
}

test "PostingsIterator advanceTo uses sparse skip data for long postings" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 1 });
    defer builder.deinit();

    var doc: u32 = 0;
    while (doc < 40) : (doc += 1) {
        try builder.addDocument(doc, &.{
            .{ .term = "term", .freq = doc + 1, .norm = 10 },
        });
    }

    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;
    switch (lookup) {
        .postings => |postings| {
            try std.testing.expect(postings.skip_data != null);
            try std.testing.expectEqual(@as(usize, 2 * postings_skip_record_size_v24), postings.skip_data.?.len);
            const bm = postings.block_max orelse return error.TestExpectedEqual;
            const ids_len = (postings.impact_chunk_ids_data orelse return error.TestExpectedEqual).len;
            const expected_header_len = varintU32Size(postings.doc_freq) +
                varintU32Size(@intCast(postings.payload_data.len)) +
                varintU32Size(0) +
                varintU32Size(@intCast(bm.chunkCount())) +
                varintU32Size(@intCast(ids_len));
            try std.testing.expectEqual(expected_header_len, postings.header_len);
        },
        .one_hit => return error.TestUnexpectedResult,
    }

    var iter = try lookup.iterator(alloc);
    defer iter.deinit();
    const hit = (try iter.advanceTo(33)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 33), hit.doc_id);
    try std.testing.expectEqual(@as(u32, 34), hit.freq);
}

test "PostingsIterator decode_positions=false skips position decode" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    try builder.addDocument(0, &.{
        .{ .term = "term", .freq = 3, .norm = 10, .positions = &.{ 0, 5, 12 } },
    });
    try builder.addDocument(1, &.{
        .{ .term = "term", .freq = 2, .norm = 8, .positions = &.{ 1, 100 } },
    });
    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;

    // Default iterator: positions decoded.
    {
        var iter = try lookup.iterator(alloc);
        defer iter.deinit();
        const h = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(usize, 3), h.positions.len);
    }

    // Scoring-only iterator: positions empty, freq/norm still correct.
    {
        var iter = try lookup.iterator(alloc);
        iter.decode_positions = false;
        defer iter.deinit();
        const h0 = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 0), h0.doc_id);
        try std.testing.expectEqual(@as(u32, 3), h0.freq);
        try std.testing.expectEqual(@as(u32, 10), h0.norm);
        try std.testing.expectEqual(@as(usize, 0), h0.positions.len);
        const h1 = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 1), h1.doc_id);
        try std.testing.expectEqual(@as(u32, 2), h1.freq);
        try std.testing.expectEqual(@as(usize, 0), h1.positions.len);
    }
}

test "PostingsIterator advanceTo on 1-hit term" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();
    try builder.addDocument(42, &.{.{ .term = "unique", .freq = 1, .norm = 7 }});
    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("unique") orelse return error.TestExpectedEqual;

    // Advance to a target <= the 1-hit doc → return the doc.
    {
        var iter = try lookup.iterator(alloc);
        defer iter.deinit();
        const hit = (try iter.advanceTo(10)) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u32, 42), hit.doc_id);
        try std.testing.expectEqual(@as(u32, 7), hit.norm);
    }

    // Advance past the 1-hit doc → null.
    {
        var iter = try lookup.iterator(alloc);
        defer iter.deinit();
        try std.testing.expect(try iter.advanceTo(43) == null);
    }
}

test "PostingsIterator advanceTo: empty postings list returns null" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();
    // Single 1-hit doc; advanceTo past its id should always return null
    // and stay null on subsequent calls.
    try builder.addDocument(7, &.{.{ .term = "lonely", .freq = 1, .norm = 3 }});
    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("lonely") orelse return error.TestExpectedEqual;
    var iter = try lookup.iterator(alloc);
    defer iter.deinit();

    // advanceTo to a target larger than the only doc → null.
    try std.testing.expect(try iter.advanceTo(8) == null);
    // Repeat call still null (the iterator is stably exhausted).
    try std.testing.expect(try iter.advanceTo(8) == null);
}

test "v6 bloom is built at exactly bloom_min_terms" {
    // Boundary: writing a section with the smallest term count that still
    // qualifies for bloom must produce a non-empty bloom payload, while
    // bloom_min_terms - 1 must skip it. Catches off-by-one between the
    // builder's `term_count >= bloom_min_terms` and the reader's
    // `bloom_len > 0` decoding.
    const alloc = std.testing.allocator;

    inline for ([_]struct { count: usize, expect_bloom: bool }{
        .{ .count = bloom_min_terms - 1, .expect_bloom = false },
        .{ .count = bloom_min_terms, .expect_bloom = true },
    }) |spec| {
        var builder = InvertedIndexBuilder.init(alloc, .{});
        defer builder.deinit();
        var name_buf: [16]u8 = undefined;
        var i: usize = 0;
        while (i < spec.count) : (i += 1) {
            const term = try std.fmt.bufPrint(&name_buf, "tok{d:0>5}", .{i});
            try builder.addDocument(@intCast(i), &.{.{ .term = term, .freq = 1, .norm = 3 }});
        }
        const section = try builder.build();
        defer alloc.free(section);

        const bloom_len = std.mem.readInt(u32, section[25..29], .little);
        if (spec.expect_bloom) {
            try std.testing.expect(bloom_len > 0);
        } else {
            try std.testing.expectEqual(@as(u32, 0), bloom_len);
        }

        const reader = try InvertedIndexReader.init(alloc, section);
        try std.testing.expectEqual(spec.expect_bloom, reader.term_bloom != null);
    }
}

test "varint u32 round-trip" {
    const alloc = std.testing.allocator;
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(alloc);

    const samples = [_]u32{ 0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 0xffff_ffff };
    for (samples) |s| try writeVarintU32(alloc, &buf, s);

    var cursor: usize = 0;
    for (samples) |s| {
        const got = try readVarintU32(buf.items, &cursor);
        try std.testing.expectEqual(s, got);
    }
    try std.testing.expectEqual(buf.items.len, cursor);

    // Truncated buffer should return error.
    var truncated = try alloc.dupe(u8, buf.items[0..1]);
    defer alloc.free(truncated);
    truncated[0] |= 0x80; // force continuation but cut off
    var trunc_cursor: usize = 0;
    try std.testing.expectError(error.Truncated, readVarintU32(truncated, &trunc_cursor));
}

test "v12 positions are bit-packed smaller than raw u32" {
    // Smoke-test the shrinkage claim: dense, monotonic positions like a
    // tokenized document produces should pack much smaller as bit-packed
    // deltas than 4 bytes per position.
    const alloc = std.testing.allocator;

    var positions: [256]u32 = undefined;
    for (&positions, 0..) |*p, i| p.* = @intCast(i);

    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();
    try builder.addDocument(0, &.{
        .{ .term = "hello", .freq = positions.len, .norm = 100, .positions = &positions },
    });
    const section = try builder.build();
    defer alloc.free(section);

    // A raw u32 payload would spend 1024 bytes on the position values alone.
    // v27 emits one chunk-length varint and per-document bit-packed records
    // with no redundant per-document position count.
    try std.testing.expect(section.len < 800);

    var reader = try InvertedIndexReader.init(alloc, section);
    const lookup = reader.lookup("hello") orelse return error.TestExpectedEqual;
    switch (lookup) {
        .postings => |postings| {
            const positions_len = if (postings.inline_single_doc)
                postings.inline_positions_data.len + 1
            else
                (postings.positions_data orelse return error.TestExpectedEqual).len;
            try std.testing.expect(positions_len <= 40);
            var iter = try postings.iterator(alloc);
            defer iter.deinit();
            const hit = try iter.next() orelse return error.TestExpectedEqual;
            try std.testing.expectEqualSlices(u32, &positions, hit.positions);
            try std.testing.expect(try iter.next() == null);
        },
        .one_hit => return error.TestUnexpectedResult,
    }
}

test "v12 reads back positions with wide packed deltas" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    // Mix narrow and wide deltas so the packed payload crosses byte boundaries
    // and exercises bit widths larger than one byte.
    const positions = [_]u32{ 0, 1, 127, 200, 1000, 100_000, 100_001 };
    try builder.addDocument(0, &.{
        .{ .term = "term", .freq = positions.len, .norm = 10, .positions = &positions },
    });
    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expectEqual(@as(u8, wire_version_current), reader.version);

    const lookup = reader.lookup("term") orelse return error.TestExpectedEqual;
    var iter = try lookup.iterator(alloc);
    defer iter.deinit();
    const hit = (try iter.next()) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(positions.len, hit.positions.len);
    for (positions, hit.positions) |want, got| {
        try std.testing.expectEqual(want, got);
    }
}

test "v6 bloom rejects absent terms before walking FST" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();

    // Need at least bloom_min_terms unique keys for the filter to be built.
    var name_buf: [16]u8 = undefined;
    var i: usize = 0;
    while (i < 128) : (i += 1) {
        const term = try std.fmt.bufPrint(&name_buf, "tok{d:0>5}", .{i});
        try builder.addDocument(@intCast(i), &.{
            .{ .term = term, .freq = 1, .norm = 5 },
        });
    }
    const section = try builder.build();
    defer alloc.free(section);

    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expect(reader.term_bloom != null);

    // Present terms still resolve.
    try std.testing.expect(reader.lookup("tok00000") != null);
    try std.testing.expect(reader.lookup("tok00127") != null);

    // Absent terms return null. The bloom filter is probabilistic, so we
    // can't assert "filter rejected without FST" — but we *can* assert the
    // overall lookup result, which is what callers rely on.
    try std.testing.expect(reader.lookup("definitely-not-a-term") == null);
    try std.testing.expect(reader.lookup("tok99999") == null);
}

test "v6 below bloom threshold skips bloom payload" {
    // With fewer than `bloom_min_terms` unique terms the builder shouldn't
    // emit a bloom — the FST is already in cache and the filter would just
    // bloat the section.
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();
    try builder.addDocument(0, &.{
        .{ .term = "alpha", .freq = 1, .norm = 4 },
        .{ .term = "beta", .freq = 1, .norm = 4 },
    });
    const section = try builder.build();
    defer alloc.free(section);

    // bloom_len lives at offset 25 in the v6 header.
    const bloom_len = std.mem.readInt(u32, section[25..29], .little);
    try std.testing.expectEqual(@as(u32, 0), bloom_len);

    var reader = try InvertedIndexReader.init(alloc, section);
    try std.testing.expect(reader.term_bloom == null);
    try std.testing.expect(reader.lookup("alpha") != null);
    try std.testing.expect(reader.lookup("missing") == null);
}

test "legacy section versions are rejected by current reader" {
    const alloc = std.testing.allocator;

    var fst_builder = try vellum.Builder.init(alloc, .{});
    defer fst_builder.deinit();
    try fst_builder.insert("hello", 0);
    const fst_bytes = try fst_builder.finish();
    defer alloc.free(fst_bytes);

    const inv_header_size: usize = 25;
    const total = inv_header_size + fst_bytes.len;
    var section = try alloc.alloc(u8, total);
    defer alloc.free(section);
    @memcpy(section[0..4], "INVT");
    section[4] = 5;
    section[5..9].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, 2)));
    section[9..17].* = @bitCast(std.mem.nativeToLittle(u64, @as(u64, 3)));
    section[17..21].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, 1024)));
    section[21..25].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(fst_bytes.len))));
    @memcpy(section[25..][0..fst_bytes.len], fst_bytes);

    try std.testing.expectError(error.UnsupportedVersion, InvertedIndexReader.init(alloc, section));
}

test "current reader reopens origin-main v23 postings and block-max layout" {
    const alloc = std.testing.allocator;
    var builder = InvertedIndexBuilder.init(alloc, .{ .chunk_size = 2, .postings_layout = .legacy_fixture_v27 });
    defer builder.deinit();
    try builder.addDocument(0, &.{.{ .term = "compat", .freq = 2, .norm = 7 }});
    try builder.addDocument(1, &.{.{ .term = "compat", .freq = 3, .norm = 9 }});
    const current = try builder.build();
    defer alloc.free(current);

    var cursor: usize = v7_header_size;
    _ = try readVarintU32(current, &cursor); // doc freq
    const stored_chunks = try readVarintU32(current, &cursor);
    _ = try readVarintU32(current, &cursor); // chunk metadata length
    const payload_len_offset = cursor;
    _ = try readVarintU32(current, &cursor);
    const payload_len_end = cursor;
    _ = try readVarintU32(current, &cursor); // positions length
    _ = try readVarintU32(current, &cursor); // skip length
    const current_block_max_start = cursor;

    // Expand v27's three-byte records back to v23's six-byte
    // [max_freq,min_norm,max_norm] representation before changing the header.
    const extra_block_bytes = @as(usize, stored_chunks) * 3;
    const expanded = try alloc.alloc(u8, current.len + extra_block_bytes);
    defer alloc.free(expanded);
    @memcpy(expanded[0..current_block_max_start], current[0..current_block_max_start]);
    for (0..stored_chunks) |chunk_idx| {
        const src = current_block_max_start + chunk_idx * 3;
        const dst = current_block_max_start + chunk_idx * 6;
        @memcpy(expanded[dst..][0..2], current[src..][0..2]);
        const norm: u16 = @intCast(fieldNormFromId(current[src + 2]));
        expanded[dst + 2 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, norm));
        expanded[dst + 4 ..][0..2].* = @bitCast(std.mem.nativeToLittle(u16, norm));
    }
    const current_block_max_end = current_block_max_start + @as(usize, stored_chunks) * 3;
    const legacy_block_max_end = current_block_max_start + @as(usize, stored_chunks) * 6;
    @memcpy(expanded[legacy_block_max_end..], current[current_block_max_end..]);

    // This tiny fixture's payload length occupies one varint byte. Removing it
    // recreates the v23 postings header while leaving relative term offsets
    // and all section-length fields valid.
    try std.testing.expectEqual(payload_len_offset + 1, payload_len_end);
    const legacy = try alloc.alloc(u8, expanded.len - 1);
    defer alloc.free(legacy);
    @memcpy(legacy[0..payload_len_offset], expanded[0..payload_len_offset]);
    @memcpy(legacy[payload_len_offset..], expanded[payload_len_end..]);
    legacy[4] = wire_version_legacy;

    var reader = try InvertedIndexReader.init(alloc, legacy);
    try std.testing.expectEqual(wire_version_legacy, reader.version);
    const lookup = reader.lookup("compat") orelse return error.TestExpectedEqual;
    var iter = try lookup.iterator(alloc);
    defer iter.deinit();
    const first = try iter.next() orelse return error.TestExpectedEqual;
    const second = try iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 0), first.doc_id);
    try std.testing.expectEqual(@as(u32, 2), first.freq);
    try std.testing.expectEqual(@as(u32, 1), second.doc_id);
    try std.testing.expectEqual(@as(u32, 3), second.freq);
}
